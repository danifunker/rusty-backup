//! Rebuild a classic HFS volume at a bigger block size, for a size it cannot reach in place.
//!
//! HFS has 65535 allocation blocks and a block size fixed at format time, so a volume can outgrow
//! its own blocks. Build Disk and every Restore path share this; see docs/fs_limits.md.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};
use disk_core::display::format_size;

/// Room beyond the files for the rebuilt catalog, extents tree and bitmap.
const METADATA_MARGIN: u64 = 64 * 1024 * 1024;

/// The bytes of the volume at `from` in use, from its MDB.
fn used_bytes(src: &mut (impl Read + Seek), from: u64) -> Result<u64> {
    let mut mdb = [0u8; 512];
    src.seek(SeekFrom::Start(from + 1024))?;
    src.read_exact(&mut mdb)?;
    let be16 = |at: usize| u16::from_be_bytes([mdb[at], mdb[at + 1]]) as u64;
    let block = u32::from_be_bytes([mdb[20], mdb[21], mdb[22], mdb[23]]) as u64;
    Ok(be16(18).saturating_sub(be16(34)) * block)
}

/// A temp file beside `near`, or in the system's temp folder, once it has room for `needed`.
fn temp_file(near: Option<&Path>, needed: u64) -> Result<File> {
    let dir = near.map_or_else(std::env::temp_dir, Path::to_path_buf);
    if let Some(free) = crate::os::available_space(&dir).filter(|&free| free < needed) {
        bail!(
            "rebuilding this HFS volume needs {} of temporary space in {}, which has {} free; \
             point TMPDIR at a larger drive",
            format_size(needed),
            dir.display(),
            format_size(free),
        );
    }
    tempfile::tempfile_in(&dir).context("creating a temporary file for the rebuilt volume")
}

/// Clone the HFS volume at `from` into a temp file `size` bytes long, checked and ready to copy.
pub fn rebuild_hfs_volume<R>(
    src: &mut R,
    from: u64,
    size: u64,
    temp_near: Option<&Path>,
    log_cb: &mut impl FnMut(&str),
) -> Result<File>
where
    R: Read + Seek + Send,
{
    if size > fs_mac::hfs::HFS_MAX_VOLUME_BYTES {
        bail!(
            "classic HFS stops at {}; this row is {}",
            format_size(fs_mac::hfs::HFS_MAX_VOLUME_BYTES),
            format_size(size)
        );
    }
    let block_size = fs_mac::hfs::pick_block_size(size);
    log_cb(&format!(
        "HFS can't grow to {} at its block size: rebuilding it at {} KiB blocks",
        format_size(size),
        block_size / 1024
    ));
    if let Some(note) = fs_mac::hfs::mac_os_size_note(size) {
        log_cb(note);
    }
    let mut temp = temp_file(temp_near, used_bytes(src, from)? + METADATA_MARGIN)?;

    let mut source = fs_mac::hfs::HfsFilesystem::open(&mut *src, from)
        .map_err(|e| anyhow::anyhow!("opening the HFS volume: {e}"))?;
    let inherited: std::collections::HashSet<String> = source
        .fsck()
        .map(|r| r.errors.into_iter().map(|e| e.code).collect())
        .unwrap_or_default();
    let name = source.volume_summary().volume_name;
    let (_, report) =
        fs_mac::hfs_clone::clone_hfs_at(&mut source, &mut temp, 0, size, block_size, &name)
            .map_err(|e| anyhow::anyhow!("cloning the HFS volume: {e}"))?;
    // A block size that doesn't divide the row leaves a remainder; the alternate MDB follows it.
    temp.set_len(size)?;
    fs_mac::hfs::fit_hfs_volume_to_partition(&mut temp, 0, size, log_cb)?;
    let result = fs_mac::hfs::HfsFilesystem::open(&mut temp, 0)
        .and_then(|mut fs| fs.fsck())
        .map_err(|e| anyhow::anyhow!("checking the rebuilt volume: {e}"))?;
    let fresh: Vec<&str> = result
        .errors
        .iter()
        .filter(|e| !inherited.contains(&e.code))
        .map(|e| e.code.as_str())
        .collect();
    if !fresh.is_empty() {
        bail!("the rebuilt HFS volume fails its check: {fresh:?}");
    }
    log_cb(&format!(
        "Copied {} file(s) and {} folder(s) into the rebuilt volume",
        report.files_copied, report.dirs_copied
    ));
    Ok(temp)
}

/// Replace the HFS volume at `offset` with a rebuilt one if `size` is past its reach.
///
/// Reads go through a sector-aligned reader, since `file` may be a raw device. The old volume is
/// only overwritten once the new one has passed its check. Returns whether it rebuilt.
pub fn rebuild_outgrown_hfs<F>(
    file: &mut F,
    offset: u64,
    size: u64,
    temp_near: Option<&Path>,
    cancel: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<bool>
where
    F: Read + Write + Seek + Send,
{
    if !fs_mac::hfs::hfs_outgrows_in_place(file, offset, size) {
        return Ok(false);
    }
    let mut temp = {
        let mut reader = crate::os::SectorAlignedReader::new(&mut *file);
        rebuild_hfs_volume(&mut reader, offset, size, temp_near, log_cb)?
    };
    temp.seek(SeekFrom::Start(0))?;
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; 1 << 20];
    let mut left = size;
    while left > 0 {
        if cancel() {
            bail!("cancelled while writing the rebuilt HFS volume");
        }
        let n = left.min(buf.len() as u64) as usize;
        temp.read_exact(&mut buf[..n])?;
        file.write_all(&buf[..n])?;
        left -= n as u64;
    }
    file.flush()?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use disk_core::filesystem::{EditableFilesystem, Filesystem};
    use std::io::Cursor;

    const MIB: u64 = 1024 * 1024;

    /// An 8 MiB volume at 512-byte blocks, which tops out just past 32 MiB.
    fn small_volume(file: &str, data: &[u8]) -> Vec<u8> {
        let mut img = Cursor::new(vec![0u8; (8 * MIB) as usize]);
        fs_mac::hfs::write_blank_hfs_into(&mut img, 0, 8 * MIB, 512, "Old").unwrap();
        let mut fs = fs_mac::hfs::HfsFilesystem::open(&mut img, 0).unwrap();
        let root = fs.root().unwrap();
        fs.create_file(
            &root,
            file,
            &mut Cursor::new(data),
            data.len() as u64,
            &Default::default(),
        )
        .unwrap();
        fs.sync_metadata().unwrap();
        drop(fs);
        img.into_inner()
    }

    #[test]
    fn a_volume_within_reach_is_left_alone() {
        let img = small_volume("KEEP", b"hello");
        let mut disk = Cursor::new(img.clone());
        let rebuilt =
            rebuild_outgrown_hfs(&mut disk, 0, 8 * MIB, None, &|| false, &mut |_| {}).unwrap();
        assert!(!rebuilt);
        assert_eq!(disk.into_inner(), img);
    }

    #[test]
    fn an_outgrown_volume_is_replaced_at_its_offset_with_its_files() {
        let offset = MIB;
        let size = 48 * MIB;
        let mut raw = vec![0xAAu8; offset as usize];
        raw.extend(small_volume("KEEP", b"hello"));
        raw.resize((offset + size + MIB) as usize, 0xBB);
        let mut disk = Cursor::new(raw);
        let rebuilt =
            rebuild_outgrown_hfs(&mut disk, offset, size, None, &|| false, &mut |_| {}).unwrap();
        assert!(rebuilt);

        let mut fs = fs_mac::hfs::HfsFilesystem::open(&mut disk, offset).unwrap();
        assert_eq!(fs.volume_summary().volume_name, "Old");
        assert!(fs.fsck().unwrap().errors.is_empty());
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let entry = entries.iter().find(|e| e.name == "KEEP").unwrap();
        assert_eq!(fs.read_file(entry, 16).unwrap(), b"hello");
        drop(fs);
        // Nothing outside the partition moved.
        let raw = disk.into_inner();
        assert!(raw[..offset as usize].iter().all(|&b| b == 0xAA));
        assert!(raw[(offset + size) as usize..].iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn a_cancelled_write_says_so() {
        let mut disk = Cursor::new(small_volume("KEEP", b"hello"));
        disk.get_mut().resize((48 * MIB) as usize, 0);
        let err = rebuild_outgrown_hfs(&mut disk, 0, 48 * MIB, None, &|| true, &mut |_| {})
            .expect_err("cancel stops the write");
        assert!(format!("{err}").contains("cancelled"), "{err}");
    }
}
