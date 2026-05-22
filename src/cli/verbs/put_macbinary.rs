//! `rb-cli put-macbinary IMG[@N] HOST [DST]` — write a MacBinary I / II
//! file into an HFS volume in one shot.
//!
//! MacBinary archives carry both forks plus the full Finder info
//! (type/creator, fdFlags, fdLocation, fdFldr, creation + modification
//! dates) for a single Mac file. Replaces the put + setrsrc + chmeta
//! dance; without it, `rb-cli put` can only set type and creator from
//! the command line, dropping every other field the archive holds.
//!
//! HFS only today (`set_finder_info` / `set_dates` return `Unsupported`
//! on other filesystems). HFS+ support follows when those setters land
//! over there.

use anyhow::{anyhow, bail, Context, Result};
use byteorder::{BigEndian, ByteOrder};
use clap::Args;
use std::path::PathBuf;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::parse::split_mac_path;
use crate::cli::resolve::resolve_partition_rw;
use crate::fs::filesystem::{CreateFileOptions, ResourceForkSource};

#[derive(Debug, Args)]
pub struct PutMacBinaryArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,

    /// MacBinary I / II archive on the host.
    pub host_file: PathBuf,

    /// Destination directory inside the filesystem (`/` for root). The
    /// filename comes from the MacBinary header. Defaults to `/`.
    #[arg(long = "dst-dir", default_value = "/")]
    pub dst_dir: String,

    /// Override the filename from the MacBinary header.
    #[arg(long)]
    pub rename: Option<String>,

    /// Overwrite an existing entry at the destination path.
    #[arg(long)]
    pub force: bool,
}

/// Parsed MacBinary header. We don't enforce a CRC check on MacBinary II
/// (some legacy archives have garbage there but are otherwise valid);
/// instead we rely on the structural sanity checks at byte offsets 0,
/// 74, 82 and the length fields landing inside the file.
#[derive(Debug)]
struct MacBinaryHeader {
    filename: String,
    type_code: [u8; 4],
    creator_code: [u8; 4],
    /// Full 16-bit fdFlags. MacBinary I only sets the high byte at +73;
    /// MacBinary II adds the low byte at +101.
    finder_flags: u16,
    /// fdLocation `(v, h)` — Pascal Point convention (vertical first).
    finder_location: (i16, i16),
    /// fdFldr — the window/folder ID the Finder displays the file in.
    finder_folder: i16,
    data_length: u32,
    rsrc_length: u32,
    /// Mac epoch (seconds since 1904-01-01).
    create_date: u32,
    modify_date: u32,
    /// Bytes 121..123: secondary header length + version byte (129 = II,
    /// 130 = III, 0 = I).
    version: u8,
}

fn parse_macbinary_header(bytes: &[u8]) -> Result<MacBinaryHeader> {
    if bytes.len() < 128 {
        bail!("MacBinary: file too short ({} bytes)", bytes.len());
    }
    // Structural sanity. byte 0 is the "old version" byte (must be 0);
    // bytes 74 and 82 are reserved (must be 0).
    if bytes[0] != 0 || bytes[74] != 0 || bytes[82] != 0 {
        bail!(
            "MacBinary: header sanity bytes wrong (b0={:#x}, b74={:#x}, b82={:#x})",
            bytes[0],
            bytes[74],
            bytes[82]
        );
    }
    let name_len = bytes[1] as usize;
    if name_len == 0 || name_len > 63 {
        bail!("MacBinary: bad filename length {name_len}");
    }
    let filename = crate::fs::hfs::mac_roman_to_utf8(&bytes[2..2 + name_len]);

    let mut type_code = [0u8; 4];
    type_code.copy_from_slice(&bytes[65..69]);
    let mut creator_code = [0u8; 4];
    creator_code.copy_from_slice(&bytes[69..73]);

    let finder_flags_hi = bytes[73] as u16;
    let finder_flags_lo = bytes[101] as u16;
    let version = bytes[122];
    let finder_flags = (finder_flags_hi << 8) | if version >= 129 { finder_flags_lo } else { 0 };

    let loc_v = BigEndian::read_i16(&bytes[75..77]);
    let loc_h = BigEndian::read_i16(&bytes[77..79]);
    let finder_folder = BigEndian::read_i16(&bytes[79..81]);
    let data_length = BigEndian::read_u32(&bytes[83..87]);
    let rsrc_length = BigEndian::read_u32(&bytes[87..91]);
    let create_date = BigEndian::read_u32(&bytes[91..95]);
    let modify_date = BigEndian::read_u32(&bytes[95..99]);

    Ok(MacBinaryHeader {
        filename,
        type_code,
        creator_code,
        finder_flags,
        finder_location: (loc_v, loc_h),
        finder_folder,
        data_length,
        rsrc_length,
        create_date,
        modify_date,
        version,
    })
}

/// Round up to the next 128-byte boundary. MacBinary pads every section
/// (secondary header, data fork, resource fork, comment).
fn pad128(n: usize) -> usize {
    n.div_ceil(128) * 128
}

pub fn run(args: PutMacBinaryArgs) -> Result<()> {
    let bytes = std::fs::read(&args.host_file)
        .with_context(|| format!("reading {}", args.host_file.display()))?;
    let hdr = parse_macbinary_header(&bytes)?;

    // Slice out the secondary header (almost always 0), data fork, and
    // resource fork. Each is padded to a 128-byte boundary.
    let sec_hdr_len = BigEndian::read_u16(&bytes[120..122]) as usize;
    let mut off = 128 + pad128(sec_hdr_len);
    let data_end = off + hdr.data_length as usize;
    if data_end > bytes.len() {
        bail!(
            "MacBinary: data fork extends past file ({} > {})",
            data_end,
            bytes.len()
        );
    }
    let data_fork = bytes[off..data_end].to_vec();
    off = off + pad128(hdr.data_length as usize);
    let rsrc_end = off + hdr.rsrc_length as usize;
    if rsrc_end > bytes.len() {
        bail!(
            "MacBinary: resource fork extends past file ({} > {})",
            rsrc_end,
            bytes.len()
        );
    }
    let rsrc_fork = bytes[off..rsrc_end].to_vec();

    let target_name = args.rename.clone().unwrap_or_else(|| hdr.filename.clone());
    if target_name.is_empty() {
        bail!("MacBinary: empty filename");
    }

    let (file, ctx) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    let (parent_path, _) = split_mac_path(&format!(
        "{}/x",
        args.dst_dir.trim_end_matches('/').trim_start_matches('/'),
    ))?;
    let parent_path = if parent_path == "/" {
        "/".to_string()
    } else {
        parent_path
    };
    let parent = super::ls::resolve_path(&mut *fs, &parent_path)?;
    if !parent.is_directory() {
        bail!("parent is not a directory: {parent_path}");
    }

    let existing = fs
        .list_directory(&parent)
        .map_err(|e| anyhow!("list_directory: {e}"))?
        .into_iter()
        .find(|e| e.name == target_name);
    if let Some(ref e) = existing {
        if !args.force {
            bail!(
                "{}/{} already exists (pass --force to overwrite)",
                parent_path.trim_end_matches('/'),
                target_name
            );
        }
        fs.delete_entry(&parent, e)
            .map_err(|e| anyhow!("delete existing: {e}"))?;
    }

    let type_str = std::str::from_utf8(&hdr.type_code)
        .map(|s| s.to_string())
        .unwrap_or_else(|_| "BINA".to_string());
    let creator_str = std::str::from_utf8(&hdr.creator_code)
        .map(|s| s.to_string())
        .unwrap_or_else(|_| "????".to_string());

    let resource_fork = if hdr.rsrc_length > 0 {
        Some(ResourceForkSource::Data(rsrc_fork))
    } else {
        None
    };
    let options = CreateFileOptions {
        type_code: Some(type_str.clone()),
        creator_code: Some(creator_str.clone()),
        resource_fork,
        ..Default::default()
    };

    let mut reader: &[u8] = &data_fork;
    fs.create_file(
        &parent,
        &target_name,
        &mut reader,
        hdr.data_length as u64,
        &options,
    )
    .map_err(|e| anyhow!("create_file: {e}"))?;

    // Look up the entry we just created so we can apply the rest of the
    // Finder info + dates. `resolve_path` invalidates between calls but
    // the entry we want is `parent/target_name` and `list_directory`
    // returns fresh FileEntry instances.
    let created = fs
        .list_directory(&parent)
        .map_err(|e| anyhow!("list_directory after create: {e}"))?
        .into_iter()
        .find(|e| e.name == target_name)
        .ok_or_else(|| anyhow!("created entry not found after create_file"))?;

    // Build the 16-byte FInfo: type, creator, fdFlags, fdLocation v,h, fdFldr.
    let mut finfo = [0u8; 16];
    finfo[0..4].copy_from_slice(&hdr.type_code);
    finfo[4..8].copy_from_slice(&hdr.creator_code);
    BigEndian::write_u16(&mut finfo[8..10], hdr.finder_flags);
    BigEndian::write_i16(&mut finfo[10..12], hdr.finder_location.0);
    BigEndian::write_i16(&mut finfo[12..14], hdr.finder_location.1);
    BigEndian::write_i16(&mut finfo[14..16], hdr.finder_folder);
    // FXInfo stays zero — MacBinary doesn't carry those bits explicitly.
    let fxinfo = [0u8; 16];

    if let Err(e) = fs.set_finder_info(&created, finfo, fxinfo) {
        // Non-HFS filesystems don't support this. Surface as a warning so
        // the file still lands; users can run `chmeta` separately if they
        // need to.
        log_stderr(format!("warning: set_finder_info skipped: {e}"));
    }

    if hdr.create_date != 0 || hdr.modify_date != 0 {
        let backup = 0; // MacBinary doesn't carry a backup date.
        if let Err(e) = fs.set_dates(&created, hdr.create_date, hdr.modify_date, backup) {
            log_stderr(format!("warning: set_dates skipped: {e}"));
        }
    }

    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;

    log_stderr(format!(
        "put-macbinary: {} ({} data, {} rsrc, type={} creator={} fdFlags=0x{:04x}, MacBinary v{})",
        target_name,
        hdr.data_length,
        hdr.rsrc_length,
        type_str,
        creator_str,
        hdr.finder_flags,
        if hdr.version >= 129 { 2 } else { 1 },
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal MacBinary II archive for parser tests.
    fn build_archive(
        filename: &[u8],
        type_code: [u8; 4],
        creator: [u8; 4],
        finder_flags: u16,
        data: &[u8],
        rsrc: &[u8],
    ) -> Vec<u8> {
        let mut hdr = [0u8; 128];
        hdr[1] = filename.len() as u8;
        hdr[2..2 + filename.len()].copy_from_slice(filename);
        hdr[65..69].copy_from_slice(&type_code);
        hdr[69..73].copy_from_slice(&creator);
        hdr[73] = (finder_flags >> 8) as u8;
        hdr[101] = (finder_flags & 0xFF) as u8;
        BigEndian::write_i16(&mut hdr[75..77], 100); // fdLocation.v
        BigEndian::write_i16(&mut hdr[77..79], 200); // fdLocation.h
        BigEndian::write_i16(&mut hdr[79..81], -1); // fdFldr (Finder default)
        BigEndian::write_u32(&mut hdr[83..87], data.len() as u32);
        BigEndian::write_u32(&mut hdr[87..91], rsrc.len() as u32);
        BigEndian::write_u32(&mut hdr[91..95], 0xAAAAAAAA); // create
        BigEndian::write_u32(&mut hdr[95..99], 0xBBBBBBBB); // modify
        hdr[122] = 129; // MacBinary II
        hdr[123] = 129;

        let mut out = hdr.to_vec();
        out.extend_from_slice(data);
        while out.len() % 128 != 0 {
            out.push(0);
        }
        out.extend_from_slice(rsrc);
        while out.len() % 128 != 0 {
            out.push(0);
        }
        out
    }

    #[test]
    fn parse_macbinary_extracts_fields() {
        let data = b"hello mac";
        let rsrc = b"RSRC";
        let archive = build_archive(b"Greet", *b"TEXT", *b"ttxt", 0x4000, data, rsrc);
        let hdr = parse_macbinary_header(&archive).unwrap();
        assert_eq!(hdr.filename, "Greet");
        assert_eq!(&hdr.type_code, b"TEXT");
        assert_eq!(&hdr.creator_code, b"ttxt");
        assert_eq!(hdr.finder_flags, 0x4000);
        assert_eq!(hdr.finder_location, (100, 200));
        assert_eq!(hdr.finder_folder, -1);
        assert_eq!(hdr.data_length, data.len() as u32);
        assert_eq!(hdr.rsrc_length, rsrc.len() as u32);
        assert_eq!(hdr.create_date, 0xAAAAAAAA);
        assert_eq!(hdr.modify_date, 0xBBBBBBBB);
        assert_eq!(hdr.version, 129);
    }

    #[test]
    fn parse_macbinary_rejects_bad_sanity_bytes() {
        let mut archive = build_archive(b"x", *b"TEXT", *b"ttxt", 0, b"", b"");
        archive[74] = 0x55;
        assert!(parse_macbinary_header(&archive).is_err());
    }
}
