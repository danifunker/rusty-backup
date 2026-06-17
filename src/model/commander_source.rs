//! Source resolution for Commander Mode panes.
//!
//! Bridges a user-picked image file to the partition list its pane offers and
//! to the [`BrowseSession`] that actually opens a chosen partition. This is the
//! same two-step the Inspect tab performs inline (parse the table, then build a
//! session per partition); lifting it here keeps the GUI panes thin and lets
//! both panes share one code path.
//!
//! Container peeling (CHD / GHO / IMZ / flat floppy wrappers, and the
//! VHD / 2MG / DMG / DiskCopy image wrappers) is handled exactly the way
//! [`BrowseSession::open`] peels them, so the partition offsets this module
//! reports line up with the offsets the session later opens at.

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};

use crate::backup::metadata::BackupMetadata;
use crate::fs::zstd_stream::ZstdStreamCache;
use crate::model::backup_loader::infer_fat_type_byte;
use crate::model::browse_session::BrowseSession;
use crate::model::source_reader;
use crate::partition::{PartitionInfo, PartitionTable};
use crate::rbformats::{self, BoxReadSeek, ImageFormat};

/// Open a reader over `path` with any container wrapper peeled off, matching
/// [`BrowseSession::open`]'s own peeling so partition offsets are consistent.
fn open_probe_reader(path: &Path) -> Result<BoxReadSeek> {
    // CHD / GHO / IMZ and the flat floppy wrappers decode to a flat sector
    // stream via the shared container reader.
    if source_reader::is_container_path(path) {
        return source_reader::open_read(path);
    }

    // Otherwise peel an image wrapper (VHD / 2MG / DMG / DiskCopy 4.2 / ...);
    // a Raw image falls through to a plain buffered file.
    let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    match rbformats::detect_image_format_with_path(file, Some(path)) {
        Ok(format) if !matches!(format, ImageFormat::Raw) => {
            let file2 =
                std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
            let (reader, _size) = rbformats::wrap_image_reader(file2, format)
                .with_context(|| format!("unwrap image {}", path.display()))?;
            Ok(reader)
        }
        _ => {
            let file =
                std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
            Ok(Box::new(std::io::BufReader::new(file)))
        }
    }
}

/// Parse the partition table of the image/container at `path` and return its
/// partition list. A partition-less (superfloppy) image yields a single
/// offset-0 entry whose `type_name` carries the detected filesystem hint.
pub fn probe_partitions(path: &Path) -> Result<Vec<PartitionInfo>> {
    let mut reader = open_probe_reader(path)?;
    let table = PartitionTable::detect(&mut reader)
        .with_context(|| format!("parsing partition table of {}", path.display()))?;
    Ok(table.partitions())
}

/// Build a [`BrowseSession`] for `part` within the image at `path`. Mirrors the
/// field assignment `BrowseView::open` performs: the absolute byte offset comes
/// from [`PartitionInfo::byte_offset`], and a zero type byte (superfloppies,
/// some GPT entries) is inferred from the partition's display name so FAT
/// volumes still dispatch to the right driver.
pub fn session_for(path: &Path, part: &PartitionInfo) -> BrowseSession {
    let partition_type = if part.partition_type_byte != 0 {
        part.partition_type_byte
    } else {
        infer_fat_type_byte(&part.type_name)
    };
    BrowseSession {
        source_path: Some(path.to_path_buf()),
        partition_offset: part.byte_offset(),
        partition_type,
        partition_type_string: part.partition_type_string.clone(),
        ..Default::default()
    }
}

/// Build a [`BrowseSession`] for the partition at `part_index` inside a native
/// rusty-backup folder (`metadata.json` already parsed into `metadata`). This
/// mirrors the Inspect tab's per-compression backup-open (the `open_browse`
/// native-folder case plus `open_browse_zstd`), but yields a session a Commander
/// pane opens off-thread instead of driving a `BrowseView`.
///
/// The data file for a per-partition backup *is* the partition (data at offset
/// 0), so the session's `partition_offset` is 0 regardless of where the
/// partition sat on the original disk. The partition type byte/string come from
/// the backup's own `PartitionMetadata`, with the same FAT-from-name inference
/// [`session_for`] uses for a zero type byte.
///
/// Supported compressions: `none` (raw, opened via `source_path`) and `zstd`
/// (streamed through a [`ZstdStreamCache`]). CHD / WOZ / Clonezilla backups need
/// the heavier reader/cache machinery the Inspect tab carries and are not yet
/// browsable from a Commander pane — those return an error the pane surfaces.
pub fn session_for_backup_partition(
    folder: &Path,
    metadata: &BackupMetadata,
    part_index: usize,
) -> Result<BrowseSession> {
    let part = metadata
        .partitions
        .iter()
        .find(|p| p.index == part_index)
        .with_context(|| format!("partition {part_index} not found in backup metadata"))?;

    if part.compressed_files.is_empty() {
        bail!("partition {part_index} has no data files listed in the backup");
    }
    if part.compressed_files.len() > 1 {
        bail!(
            "partition {part_index} is split across {} files; browsing split backups \
             is not supported",
            part.compressed_files.len()
        );
    }
    let data_path = folder.join(&part.compressed_files[0]);
    if !data_path.exists() {
        bail!("backup data file not found: {}", data_path.display());
    }

    let partition_type = if part.partition_type_byte != 0 {
        part.partition_type_byte
    } else {
        infer_fat_type_byte(&part.type_name)
    };
    let mut session = BrowseSession {
        partition_offset: 0,
        partition_type,
        partition_type_string: part.partition_type_string.clone(),
        ..Default::default()
    };

    match metadata.compression_type.as_str() {
        // Raw partition image — data starts at byte 0 of the file.
        "none" => {
            session.source_path = Some(data_path);
        }
        // Streamed zstd: decompress forward-only into a shared in-memory cache,
        // exactly the reader the Inspect tab opens with before its seekable
        // cache finishes building.
        "zstd" => {
            let cache = ZstdStreamCache::new(&data_path)
                .with_context(|| format!("opening zstd backup {}", data_path.display()))?;
            session.zstd_cache = Some(Arc::new(Mutex::new(cache)));
        }
        other => {
            bail!(
                "browsing {other} backups from a Commander pane is not supported yet \
                 (use the Inspect tab)"
            );
        }
    }
    Ok(session)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::metadata::{AlignmentMetadata, PartitionMetadata};

    /// A blank FAT12 floppy is a superfloppy: probing yields exactly one
    /// offset-0 partition, and the session built for it opens the volume.
    #[test]
    fn probe_and_session_round_trip_on_superfloppy() {
        let flat = crate::fs::fat::create_blank_fat(737280, Some("PROBE")).unwrap();
        let tmp = tempfile::Builder::new().suffix(".img").tempfile().unwrap();
        std::fs::write(tmp.path(), &flat).unwrap();

        let parts = probe_partitions(tmp.path()).expect("probe");
        assert_eq!(parts.len(), 1, "superfloppy exposes a single partition");
        assert_eq!(parts[0].byte_offset(), 0);

        let session = session_for(tmp.path(), &parts[0]);
        assert_eq!(session.partition_offset, 0);
        // A FAT type byte was inferred from the superfloppy's hint name (the
        // exact FAT12/16/32 flavor is re-detected from the BPB at open time).
        assert!(matches!(session.partition_type, 0x01 | 0x06 | 0x0B | 0x0C));

        let mut fs = session.open().expect("open volume");
        assert_eq!(fs.volume_label(), Some("PROBE"));
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
    }

    /// Minimal native-backup metadata describing one FAT12 partition stored in
    /// `data_file` with the given `compression`.
    fn one_partition_meta(compression: &str, data_file: &str, size: u64) -> BackupMetadata {
        BackupMetadata {
            version: 1,
            created: "test".into(),
            source_device: "test".into(),
            source_size_bytes: size,
            partition_table_type: "None".into(),
            checksum_type: "sha256".into(),
            compression_type: compression.into(),
            split_size_mib: None,
            sector_by_sector: false,
            layout: Default::default(),
            container: None,
            container_logical_size: None,
            container_sha1: None,
            size_policy: None,
            alignment: AlignmentMetadata {
                detected_type: "None".into(),
                first_partition_lba: 0,
                alignment_sectors: 0,
                heads: 0,
                sectors_per_track: 0,
            },
            partitions: vec![PartitionMetadata {
                index: 0,
                type_name: "FAT12".into(),
                partition_type_byte: 0x01,
                start_lba: 0,
                start_byte: None,
                original_size_bytes: size,
                imaged_size_bytes: size,
                compressed_files: vec![data_file.into()],
                checksum: String::new(),
                resized: false,
                compacted: false,
                is_logical: false,
                partition_type_string: None,
                minimum_size_bytes: None,
                defragmented_min_size_bytes: None,
                hfsplus_signature: None,
                defragmented_clone: false,
            }],
            bad_sectors: vec![],
            extended_container: None,
        }
    }

    /// A raw ("none") backup partition opens via `source_path` at offset 0.
    #[test]
    fn backup_session_opens_raw_partition() {
        let flat = crate::fs::fat::create_blank_fat(737280, Some("BKRAW")).unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("partition-0.raw"), &flat).unwrap();

        let meta = one_partition_meta("none", "partition-0.raw", flat.len() as u64);
        let session = session_for_backup_partition(dir.path(), &meta, 0).expect("session");
        assert!(session.source_path.is_some());
        assert_eq!(session.partition_offset, 0);

        let fs = session.open().expect("open volume");
        assert_eq!(fs.volume_label(), Some("BKRAW"));
    }

    /// A zstd backup partition opens through a streaming `ZstdStreamCache`.
    #[test]
    fn backup_session_opens_zstd_partition() {
        let flat = crate::fs::fat::create_blank_fat(737280, Some("BKZSTD")).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let compressed = zstd::encode_all(&flat[..], 3).unwrap();
        std::fs::write(dir.path().join("partition-0.zst"), &compressed).unwrap();

        let meta = one_partition_meta("zstd", "partition-0.zst", flat.len() as u64);
        let session = session_for_backup_partition(dir.path(), &meta, 0).expect("session");
        assert!(session.zstd_cache.is_some());
        assert!(session.source_path.is_none());

        let fs = session.open().expect("open volume");
        assert_eq!(fs.volume_label(), Some("BKZSTD"));
    }

    /// A compression we don't yet open from a Commander pane errors cleanly
    /// rather than producing a broken session.
    #[test]
    fn backup_session_rejects_unsupported_compression() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("partition-0.chd"), b"x").unwrap();
        let meta = one_partition_meta("chd", "partition-0.chd", 1);
        assert!(session_for_backup_partition(dir.path(), &meta, 0).is_err());
    }

    /// A missing data file is reported, not silently opened.
    #[test]
    fn backup_session_reports_missing_data_file() {
        let dir = tempfile::tempdir().unwrap();
        let meta = one_partition_meta("none", "partition-0.raw", 1024);
        assert!(session_for_backup_partition(dir.path(), &meta, 0).is_err());
    }
}
