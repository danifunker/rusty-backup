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

pub fn run(args: PutMacBinaryArgs) -> Result<()> {
    let bytes = std::fs::read(&args.host_file)
        .with_context(|| format!("reading {}", args.host_file.display()))?;
    // One canonical full-fidelity parser (src/macarchive/macbinary.rs) handles
    // I/II/III and returns both forks already sliced. It is lenient (no CRC
    // enforcement) — put-macbinary is an explicit "this is a MacBinary file"
    // command, so we don't gate it behind the stricter is_macbinary heuristic.
    let hdr = crate::macarchive::macbinary::parse(&bytes)
        .with_context(|| format!("parsing {}", args.host_file.display()))?;
    let data_fork = hdr.data_fork.clone();
    let rsrc_fork = hdr.resource_fork.clone();
    let data_length = data_fork.len() as u64;
    let rsrc_length = rsrc_fork.len();

    let target_name = args.rename.clone().unwrap_or_else(|| hdr.filename.clone());
    if target_name.is_empty() {
        bail!("MacBinary: empty filename");
    }

    let (file, ctx, commit) = resolve_partition_rw(&args.image.path, args.image.partition)?;
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

    let resource_fork = if rsrc_length > 0 {
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
    fs.create_file(&parent, &target_name, &mut reader, data_length, &options)
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
    drop(fs);
    commit.commit()?;

    log_stderr(format!(
        "put-macbinary: {} ({} data, {} rsrc, type={} creator={} fdFlags=0x{:04x}, {})",
        target_name,
        data_length,
        rsrc_length,
        type_str,
        creator_str,
        hdr.finder_flags,
        hdr.version.label(),
    ));
    Ok(())
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::*;

    // MacBinary parsing is exercised in src/macarchive/macbinary.rs; this verb
    // is now a thin wrapper around `macbinary::parse`.

    #[test]
    fn delegates_to_canonical_parser() {
        // Spot-check that the shared parser is reachable and round-trips a
        // type/creator + flags through, the way `run` consumes it.
        let mut hdr = [0u8; 128];
        hdr[1] = 1;
        hdr[2] = b'x';
        hdr[65..69].copy_from_slice(b"TEXT");
        hdr[69..73].copy_from_slice(b"ttxt");
        hdr[73] = 0x20; // fdFlags high byte (hasBundle)
        byteorder::BigEndian::write_u32(&mut hdr[83..87], 1);
        hdr[122] = 129;
        hdr[123] = 129;
        let crc = crate::fs::resource_fork::macbinary_crc16(&hdr[0..124]);
        byteorder::BigEndian::write_u16(&mut hdr[124..126], crc);
        let mut buf = hdr.to_vec();
        buf.push(b'D'); // 1-byte data fork
        while buf.len() % 128 != 0 {
            buf.push(0);
        }
        let f = crate::macarchive::macbinary::parse(&buf).unwrap();
        assert_eq!(f.filename, "x");
        assert_eq!(&f.type_code, b"TEXT");
        assert_eq!(f.finder_flags, 0x2000);
        assert_eq!(f.data_fork, b"D");
    }
}
