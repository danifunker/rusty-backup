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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};

use crate::backup::metadata::{BackupLayout, BackupMetadata};
use crate::clonezilla::block_cache::{CacheState, PartcloneBlockCache};
use crate::clonezilla::metadata::ClonezillaImage;
use crate::fs::zstd_stream::ZstdStreamCache;
use crate::model::backup_loader::{self, infer_fat_type_byte, LoadOutcome};
use crate::model::browse_session::BrowseSession;
use crate::model::source_reader;
use crate::partition::{PartitionInfo, PartitionTable};

/// Parse the partition table of the image/container at `path` and return its
/// partition list. A partition-less (superfloppy) image yields a single
/// offset-0 entry whose `type_name` carries the detected filesystem hint.
///
/// Container / image-wrapper peeling goes through the shared
/// [`source_reader::open_peeled_read`] primitive, so the offsets reported here
/// match the offsets [`BrowseSession::open`] later opens at and every front end
/// probes CHD / GHO / IMZ / VHD / … identically.
pub fn probe_partitions(path: &Path) -> Result<Vec<PartitionInfo>> {
    let mut reader = source_reader::open_peeled_read(path, None)?;
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
/// Handles every **per-partition** compression the Inspect tab browses:
/// `none` (raw), `chd` / `chd-dvd` (per-partition CHD), and `woz` all open via
/// `source_path` — the data file *is* the partition and [`BrowseSession::open`]
/// sniffs its magic to pick the reader (plain raw, `ChdReader` for `MComprHD`,
/// `WozReader` for the WOZ signature); `zstd` is streamed through a
/// [`ZstdStreamCache`]. A *single-file-chd* CHD backup is a different shape
/// (one container holding the whole disk) and is routed by
/// [`ResolvedBackup::open_partition`] before reaching here; Clonezilla images
/// go through the partclone block cache.
/// The single data file backing partition `part_index` of a native backup,
/// validated to exist and not be split across multiple files. Shared by
/// [`session_for_backup_partition`] and the Inspect tab's zstd opener (which
/// builds its reader separately, for the streaming + seekable-cache upgrade) so
/// both reject missing / split backups with the same checks.
pub fn single_data_file(
    folder: &Path,
    metadata: &BackupMetadata,
    part_index: usize,
) -> Result<PathBuf> {
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
    Ok(data_path)
}

pub fn session_for_backup_partition(
    folder: &Path,
    metadata: &BackupMetadata,
    part_index: usize,
) -> Result<BrowseSession> {
    let data_path = single_data_file(folder, metadata, part_index)?;
    let part = metadata
        .partitions
        .iter()
        .find(|p| p.index == part_index)
        .with_context(|| format!("partition {part_index} not found in backup metadata"))?;

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
        // Raw / CHD / WOZ partition images: the data file *is* the partition
        // (data at byte 0). `BrowseSession::open` sniffs the file's magic to
        // pick the right reader — plain buffered file for raw, `ChdReader` for
        // a `MComprHD` CHD, `WozReader` for the WOZ signature — so all three
        // are the same session shape.
        "none" | "chd" | "chd-dvd" | "woz" => {
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

/// The editing flow a native-backup partition supports once its
/// [`BrowseSession`] is open, derived from the backup's compression. The view
/// layers this onto the open session — the Inspect tab applies it today;
/// Commander backup panes stay read-only and ignore it. Keeping the
/// compression→flow mapping here (not inside a view) means every front end
/// applies the same one, per CONTRIBUTING "one model, many UIs".
pub enum BackupEdit {
    /// Edit in place: a raw (`none`) partition image edits directly; a
    /// per-partition CHD edits through the `chd_edit` diff/in-place flow.
    /// Either way the view marks editing supported and lets its edit-mode
    /// toggle pick the mechanism by sniffing the source.
    InPlace,
    /// Decompress → edit → recompress archive flow (`zstd` / `woz`). Carries
    /// the parameters the view's archive-edit context needs to recompress and
    /// refresh the backup's `metadata.json` after a save.
    Archive(ArchiveEditPlan),
    /// Not editable from a backup pane (a compression we can browse but not yet
    /// write back).
    ReadOnly,
}

/// Parameters for the decompress→edit→recompress flow of a compressed backup
/// partition. Mirrors the Inspect view's `ArchiveEditContext` field-for-field
/// so the view can install it directly.
pub struct ArchiveEditPlan {
    /// Compressed data file to decompress for editing and recompress on save.
    pub archive_path: PathBuf,
    /// Backup compression (`"zstd"` / `"woz"`); selects the recompression codec.
    pub compression_type: String,
    /// Uncompressed partition size (free-space projection + zero-fill).
    pub original_size: u64,
    /// Whether the partition was compacted (imaged size < original size).
    pub compacted: bool,
    /// The backup's `metadata.json`, rewritten with the new checksum on save.
    pub metadata_path: PathBuf,
    /// Index of this partition within the backup.
    pub partition_index: usize,
    /// Checksum algorithm recorded in the backup (`"sha256"` / `"crc32"`).
    pub checksum_type: String,
}

/// The editing flow for partition `part_index` of a native backup, derived from
/// its compression (see [`BackupEdit`]). The companion of
/// [`session_for_backup_partition`]: that builds the read session, this tells
/// the view how to enable editing on top of it.
pub fn backup_edit_for(
    folder: &Path,
    metadata: &BackupMetadata,
    part_index: usize,
) -> Result<BackupEdit> {
    let part = metadata
        .partitions
        .iter()
        .find(|p| p.index == part_index)
        .with_context(|| format!("partition {part_index} not found in backup metadata"))?;
    Ok(match metadata.compression_type.as_str() {
        "none" | "chd" | "chd-dvd" => BackupEdit::InPlace,
        compression @ ("zstd" | "woz") => {
            let data_file = part
                .compressed_files
                .first()
                .with_context(|| format!("partition {part_index} has no data file listed"))?;
            BackupEdit::Archive(ArchiveEditPlan {
                archive_path: folder.join(data_file),
                compression_type: compression.to_string(),
                original_size: part.original_size_bytes,
                compacted: part.compacted,
                metadata_path: folder.join("metadata.json"),
                partition_index: part_index,
                checksum_type: metadata.checksum_type.clone(),
            })
        }
        _ => BackupEdit::ReadOnly,
    })
}

/// A backup folder resolved to its kind, partition list, and the context needed
/// to open a partition for browsing. This is the **one** entry point a UI uses
/// to open a backup, so Commander, the Inspect tab, and a future TUI share the
/// native-vs-Clonezilla routing instead of each re-deriving it (the gap that
/// left Commander unable to open Clonezilla images). Wraps
/// [`backup_loader::load_backup`], which does the native-vs-Clonezilla detection.
pub enum ResolvedBackup {
    /// A native rusty-backup folder (`metadata.json`).
    Native {
        folder: PathBuf,
        metadata: Box<BackupMetadata>,
        partitions: Vec<PartitionInfo>,
    },
    /// A Clonezilla image folder (partclone + sfdisk).
    Clonezilla {
        folder: PathBuf,
        image: Box<ClonezillaImage>,
        partitions: Vec<PartitionInfo>,
    },
}

/// How to open a resolved backup partition for browsing.
pub enum BackupPartitionOpen {
    /// A ready [`BrowseSession`] — `spawn_open` it like any image partition.
    Session(BrowseSession),
    /// A Clonezilla partition needs a partclone block cache before a session can
    /// open it. A previous scan may already be on disk ([`load_partclone_cache`]);
    /// otherwise the cache must be built by a scan (the cache runner) and then
    /// turned into a session with [`session_for_partclone_cache`].
    Clonezilla(ClonezillaOpen),
}

/// Everything needed to obtain a Clonezilla partition's block cache and then a
/// browsing session.
pub struct ClonezillaOpen {
    /// Sorted partclone split files backing this partition.
    pub partclone_files: Vec<PathBuf>,
    /// MBR partition type byte (drives filesystem dispatch).
    pub partition_type: u8,
    /// Where a prior scan's metadata cache lives / will be written
    /// (`_<device>.metadata.cache` in the image folder).
    pub cache_path: PathBuf,
}

/// Resolve a backup folder: detect native rusty-backup vs Clonezilla and return
/// its partition list + open context.
pub fn resolve_backup(folder: &Path) -> Result<ResolvedBackup> {
    match backup_loader::load_backup(folder)? {
        LoadOutcome::Backup(o) => Ok(ResolvedBackup::Native {
            folder: folder.to_path_buf(),
            metadata: Box::new(o.metadata),
            partitions: o.partitions,
        }),
        LoadOutcome::Clonezilla(o) => Ok(ResolvedBackup::Clonezilla {
            folder: folder.to_path_buf(),
            image: Box::new(o.image),
            partitions: o.partitions,
        }),
    }
}

impl ResolvedBackup {
    /// The partition list (drives the pane's partition dropdown).
    pub fn partitions(&self) -> &[PartitionInfo] {
        match self {
            ResolvedBackup::Native { partitions, .. } => partitions,
            ResolvedBackup::Clonezilla { partitions, .. } => partitions,
        }
    }

    /// True for a Clonezilla image (vs. a native rusty-backup folder).
    pub fn is_clonezilla(&self) -> bool {
        matches!(self, ResolvedBackup::Clonezilla { .. })
    }

    /// Build the opener for the partition with index `part_index`.
    pub fn open_partition(&self, part_index: usize) -> Result<BackupPartitionOpen> {
        match self {
            ResolvedBackup::Native {
                folder,
                metadata,
                partitions,
            } => {
                // A CHD backup uses the single-file-chd layout: one `.chd`
                // container holding the whole disk (partition table at sector 0,
                // partitions at their declared offsets), with no per-partition
                // data files. Browse it like a CHD *image* — open the container
                // at the partition's offset, the same redirect the Inspect tab
                // performs.
                if metadata.layout == BackupLayout::SingleFileChd {
                    let container = metadata
                        .container
                        .as_deref()
                        .context("single-file-chd backup is missing its container filename")?;
                    let chd_path = folder.join(container);
                    let part = partitions
                        .iter()
                        .find(|p| p.index == part_index)
                        .with_context(|| format!("partition {part_index} not found in backup"))?;
                    Ok(BackupPartitionOpen::Session(session_for(&chd_path, part)))
                } else {
                    Ok(BackupPartitionOpen::Session(session_for_backup_partition(
                        folder, metadata, part_index,
                    )?))
                }
            }
            ResolvedBackup::Clonezilla { folder, image, .. } => {
                let cz = image
                    .partitions
                    .iter()
                    .find(|p| p.index == part_index)
                    .with_context(|| {
                        format!("partition {part_index} not found in Clonezilla image")
                    })?;
                if cz.partclone_files.is_empty() {
                    bail!("Clonezilla partition {part_index} has no partclone data files");
                }
                let cache_path = folder.join(format!("_{}.metadata.cache", cz.device_name));
                Ok(BackupPartitionOpen::Clonezilla(ClonezillaOpen {
                    partclone_files: cz.partclone_files.clone(),
                    partition_type: cz.partition_type_byte,
                    cache_path,
                }))
            }
        }
    }
}

/// Build a read-only [`BrowseSession`] over a ready partclone block cache (the
/// reader short-circuits all disk access through the cache, so partition offset
/// is 0).
pub fn session_for_partclone_cache(
    cache: Arc<Mutex<PartcloneBlockCache>>,
    partition_type: u8,
) -> BrowseSession {
    BrowseSession {
        partclone_cache: Some(cache),
        partition_type,
        partition_offset: 0,
        ..Default::default()
    }
}

/// Try to load a Clonezilla partition's block cache from a previous on-disk scan.
/// Returns `None` when no cache file exists yet (a fresh scan is needed).
pub fn load_partclone_cache(open: &ClonezillaOpen) -> Option<Arc<Mutex<PartcloneBlockCache>>> {
    if !open.cache_path.exists() {
        return None;
    }
    PartcloneBlockCache::load_from_file(&open.cache_path, open.partclone_files.clone())
        .ok()
        .map(|c| Arc::new(Mutex::new(c)))
}

/// In-memory store of scanned partclone block caches (keyed by partition index)
/// plus the lookup decision both GUI front ends share. Browsing a Clonezilla
/// partition needs a scanned [`PartcloneBlockCache`]; the cheapest source is a
/// cache already scanned this session, then a prior scan persisted to disk
/// (`_<device>.metadata.cache`), and only failing both does a background scan
/// run. The Inspect tab inlined this whole tree; lifting it here (Straggler C
/// of the source-resolution unification) lets Commander reuse in-memory caches
/// too instead of re-loading from disk on every open.
#[derive(Default)]
pub struct PartcloneCacheStore {
    caches: HashMap<usize, Arc<Mutex<PartcloneBlockCache>>>,
}

/// Outcome of [`PartcloneCacheStore::resolve`].
pub enum PartcloneLookup {
    /// A ready, scanned cache — open a browsing session immediately with
    /// [`session_for_partclone_cache`].
    Ready(Arc<Mutex<PartcloneBlockCache>>),
    /// No cache available yet. Hand `cache` to [`cache_runner::spawn_partclone_scan`]
    /// with `cache_path`; when the scan finishes, call [`PartcloneCacheStore::insert`]
    /// to memoize it so the next open reuses it in memory.
    ///
    /// [`cache_runner::spawn_partclone_scan`]: crate::model::cache_runner::spawn_partclone_scan
    NeedsScan {
        cache: Arc<Mutex<PartcloneBlockCache>>,
        cache_path: PathBuf,
    },
}

impl PartcloneCacheStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decide how to obtain partition `part_index`'s block cache: reuse a ready
    /// in-memory cache, load a prior on-disk scan (memoizing it), or report that
    /// a fresh scan is needed. A corrupt on-disk cache is removed so the scan
    /// rewrites it.
    pub fn resolve(&mut self, part_index: usize, open: &ClonezillaOpen) -> PartcloneLookup {
        // 1. A cache scanned earlier this session — reuse it directly.
        if let Some(cache) = self.caches.get(&part_index) {
            let ready = cache
                .lock()
                .map(|c| c.state == CacheState::Ready)
                .unwrap_or(false);
            if ready {
                return PartcloneLookup::Ready(Arc::clone(cache));
            }
        }

        // 2. A prior scan persisted to disk — load + memoize it. A cache file
        //    that fails to load is stale; drop it so the scan below rewrites it.
        if open.cache_path.exists() {
            match PartcloneBlockCache::load_from_file(
                &open.cache_path,
                open.partclone_files.clone(),
            ) {
                Ok(loaded) => {
                    let cache = Arc::new(Mutex::new(loaded));
                    self.caches.insert(part_index, Arc::clone(&cache));
                    return PartcloneLookup::Ready(cache);
                }
                Err(e) => {
                    log::warn!(
                        "Failed to load metadata cache {}: {e}; will re-scan",
                        open.cache_path.display()
                    );
                    let _ = std::fs::remove_file(&open.cache_path);
                }
            }
        }

        // 3. Nothing cached yet — the caller must run a background scan.
        let cache = Arc::new(Mutex::new(PartcloneBlockCache::new(
            open.partclone_files.clone(),
        )));
        PartcloneLookup::NeedsScan {
            cache,
            cache_path: open.cache_path.clone(),
        }
    }

    /// Memoize a freshly-scanned cache (called when the scan runner finishes) so
    /// a later open of the same partition reuses it in memory.
    pub fn insert(&mut self, part_index: usize, cache: Arc<Mutex<PartcloneBlockCache>>) {
        self.caches.insert(part_index, cache);
    }

    /// Drop all cached scans (e.g. when the pane's source folder changes).
    pub fn clear(&mut self) {
        self.caches.clear();
    }
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

    /// Per-partition `chd` / `chd-dvd` / `woz` backups open via `source_path`
    /// at offset 0 (the data file *is* the partition); `BrowseSession::open`
    /// sniffs the magic to pick the reader. We only check the session shape
    /// here — decoding the container is covered by the format readers' tests.
    #[test]
    fn backup_session_opens_chd_and_woz_via_source_path() {
        for compression in ["chd", "chd-dvd", "woz"] {
            let dir = tempfile::tempdir().unwrap();
            let data_file = format!("partition-0.{compression}");
            std::fs::write(dir.path().join(&data_file), b"placeholder").unwrap();
            let meta = one_partition_meta(compression, &data_file, 11);
            let session =
                session_for_backup_partition(dir.path(), &meta, 0).expect("session built");
            assert_eq!(
                session.source_path,
                Some(dir.path().join(&data_file)),
                "{compression} opens via source_path"
            );
            assert_eq!(session.partition_offset, 0);
            assert!(session.zstd_cache.is_none());
        }
    }

    /// The edit-flow mapping: raw + CHD edit in place; zstd + woz go through the
    /// archive (decompress→edit→recompress) flow with the right parameters; an
    /// unhandled compression is read-only.
    #[test]
    fn backup_edit_for_maps_compression_to_flow() {
        let dir = tempfile::tempdir().unwrap();

        for in_place in ["none", "chd", "chd-dvd"] {
            let meta = one_partition_meta(in_place, "partition-0.bin", 4096);
            assert!(
                matches!(
                    backup_edit_for(dir.path(), &meta, 0).unwrap(),
                    BackupEdit::InPlace
                ),
                "{in_place} edits in place"
            );
        }

        for archive in ["zstd", "woz"] {
            let data_file = format!("partition-0.{archive}");
            let mut meta = one_partition_meta(archive, &data_file, 4096);
            meta.checksum_type = "crc32".into();
            meta.partitions[0].compacted = true;
            let BackupEdit::Archive(plan) = backup_edit_for(dir.path(), &meta, 0).unwrap() else {
                panic!("{archive} should use the archive edit flow");
            };
            assert_eq!(plan.archive_path, dir.path().join(&data_file));
            assert_eq!(plan.compression_type, archive);
            assert_eq!(plan.original_size, 4096);
            assert!(plan.compacted);
            assert_eq!(plan.metadata_path, dir.path().join("metadata.json"));
            assert_eq!(plan.partition_index, 0);
            assert_eq!(plan.checksum_type, "crc32");
        }

        let meta = one_partition_meta("xz", "partition-0.xz", 4096);
        assert!(matches!(
            backup_edit_for(dir.path(), &meta, 0).unwrap(),
            BackupEdit::ReadOnly
        ));
    }

    /// A compression we don't yet open from a Commander pane errors cleanly
    /// rather than producing a broken session.
    #[test]
    fn backup_session_rejects_unsupported_compression() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("partition-0.xz"), b"x").unwrap();
        let meta = one_partition_meta("xz", "partition-0.xz", 1);
        assert!(session_for_backup_partition(dir.path(), &meta, 0).is_err());
    }

    /// A missing data file is reported, not silently opened.
    #[test]
    fn backup_session_reports_missing_data_file() {
        let dir = tempfile::tempdir().unwrap();
        let meta = one_partition_meta("none", "partition-0.raw", 1024);
        assert!(session_for_backup_partition(dir.path(), &meta, 0).is_err());
    }

    /// `resolve_backup` on a native rusty-backup folder lists its partitions and
    /// `open_partition` yields a ready browsing session.
    #[test]
    fn resolve_native_backup_lists_partitions_and_opens() {
        let flat = crate::fs::fat::create_blank_fat(737280, Some("RESV")).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let compressed = zstd::encode_all(&flat[..], 3).unwrap();
        std::fs::write(dir.path().join("partition-0.zst"), &compressed).unwrap();
        let meta = one_partition_meta("zstd", "partition-0.zst", flat.len() as u64);
        std::fs::write(
            dir.path().join("metadata.json"),
            serde_json::to_string(&meta).unwrap(),
        )
        .unwrap();

        let resolved = resolve_backup(dir.path()).expect("resolve");
        assert!(!resolved.is_clonezilla());
        assert_eq!(resolved.partitions().len(), 1);
        match resolved.open_partition(0).expect("open") {
            BackupPartitionOpen::Session(s) => {
                let fs = s.open().expect("open volume");
                assert_eq!(fs.volume_label(), Some("RESV"));
            }
            _ => panic!("a native backup partition should open as a Session"),
        }
    }

    /// A single-file-chd backup (a CHD backup — the only CHD layout) opens its
    /// `.chd` container as an image at the partition's byte offset, not via
    /// per-partition data files (which it doesn't have). Regression guard for
    /// the "partition N has no data files listed" bug on CHD backups.
    #[test]
    fn single_file_chd_backup_opens_container_at_offset() {
        let dir = tempfile::tempdir().unwrap();
        let mut meta = one_partition_meta("chd", "", 4096);
        meta.layout = BackupLayout::SingleFileChd;
        meta.container = Some("disk.chd".into());
        meta.partitions[0].start_lba = 2048;
        meta.partitions[0].compressed_files = vec![]; // single-file-chd has none
        let part = PartitionInfo {
            index: 0,
            type_name: "Apple_HFS".into(),
            partition_type_byte: 0,
            start_lba: 2048,
            start_byte: None,
            size_bytes: 4096,
            bootable: false,
            is_logical: false,
            is_extended_container: false,
            partition_type_string: Some("Apple_HFS".into()),
            hfs_block_size: None,
            rdb_part_block: None,
            drv_name: None,
        };
        let resolved = ResolvedBackup::Native {
            folder: dir.path().to_path_buf(),
            metadata: Box::new(meta),
            partitions: vec![part],
        };
        match resolved.open_partition(0).expect("open") {
            BackupPartitionOpen::Session(s) => {
                assert_eq!(s.source_path, Some(dir.path().join("disk.chd")));
                assert_eq!(s.partition_offset, 2048 * 512);
                assert_eq!(s.partition_type_string.as_deref(), Some("Apple_HFS"));
            }
            _ => panic!("single-file-chd should open as a Session over the container"),
        }
    }

    /// Build a minimal `ClonezillaOpen` over a temp folder for store tests.
    fn dummy_open(dir: &Path) -> ClonezillaOpen {
        ClonezillaOpen {
            partclone_files: vec![dir.join("sda1.ext4-ptcl-img.gz.aa")],
            partition_type: 0x83,
            cache_path: dir.join("_sda1.metadata.cache"),
        }
    }

    /// With nothing cached in memory and no scan on disk, the store reports a
    /// scan is needed and hands back the path it should be persisted to.
    #[test]
    fn partclone_store_needs_scan_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        let open = dummy_open(dir.path());
        let mut store = PartcloneCacheStore::new();
        match store.resolve(1, &open) {
            PartcloneLookup::NeedsScan { cache_path, .. } => {
                assert_eq!(cache_path, open.cache_path);
            }
            _ => panic!("no cache anywhere -> NeedsScan"),
        }
    }

    /// A cache scanned earlier this session is reused in memory (no disk read);
    /// a still-scanning cache is not reused, so the store falls back to a scan.
    #[test]
    fn partclone_store_reuses_ready_in_memory_cache() {
        let dir = tempfile::tempdir().unwrap();
        let open = dummy_open(dir.path());

        let mut store = PartcloneCacheStore::new();
        let ready = Arc::new(Mutex::new(PartcloneBlockCache::new(
            open.partclone_files.clone(),
        )));
        ready.lock().unwrap().state = CacheState::Ready;
        store.insert(1, Arc::clone(&ready));
        match store.resolve(1, &open) {
            PartcloneLookup::Ready(c) => assert!(Arc::ptr_eq(&c, &ready)),
            _ => panic!("a ready in-memory cache should be reused"),
        }

        // A not-yet-ready (still Scanning) cache is ignored; with no disk cache
        // the store asks for a fresh scan.
        store.clear();
        let scanning = Arc::new(Mutex::new(PartcloneBlockCache::new(
            open.partclone_files.clone(),
        )));
        store.insert(1, scanning);
        assert!(matches!(
            store.resolve(1, &open),
            PartcloneLookup::NeedsScan { .. }
        ));
    }

    /// A Clonezilla partition resolves to a `Clonezilla` open carrying the right
    /// partclone files + `_<device>.metadata.cache` path; with no cache on disk
    /// yet, `load_partclone_cache` reports that a scan is needed.
    #[test]
    fn clonezilla_open_routes_to_cache_path() {
        use crate::clonezilla::metadata::ClonezillaPartition;
        let dir = tempfile::tempdir().unwrap();
        let files = vec![dir.path().join("sda1.ext4-ptcl-img.gz.aa")];
        let part = ClonezillaPartition {
            index: 1,
            device_name: "sda1".into(),
            start_lba: 2048,
            size_sectors: 1000,
            partition_type_byte: 0x83,
            filesystem_type: "ext4".into(),
            is_extended: false,
            is_logical: false,
            bootable: false,
            partclone_files: files.clone(),
            type_guid: None,
            unique_guid: None,
            partition_name: None,
        };
        let image = ClonezillaImage {
            disk_name: "sda".into(),
            cylinders: 0,
            heads: 0,
            sectors_per_track: 0,
            mbr_bytes: [0u8; 512],
            hidden_data_after_mbr: vec![],
            ebr_data: std::collections::HashMap::new(),
            partitions: vec![part],
            source_size_bytes: 0,
            image_info: String::new(),
            is_gpt: false,
            gpt_primary_raw: None,
            gpt_backup_raw: None,
            gpt_disk_guid: None,
            gpt_first_lba: None,
            gpt_last_lba: None,
        };
        let resolved = ResolvedBackup::Clonezilla {
            folder: dir.path().to_path_buf(),
            image: Box::new(image),
            partitions: vec![],
        };
        assert!(resolved.is_clonezilla());
        let BackupPartitionOpen::Clonezilla(open) = resolved.open_partition(1).expect("open")
        else {
            panic!("a Clonezilla partition should yield a Clonezilla open");
        };
        assert_eq!(open.partition_type, 0x83);
        assert_eq!(open.partclone_files, files);
        assert_eq!(open.cache_path, dir.path().join("_sda1.metadata.cache"));
        // No prior scan on disk -> a fresh scan is needed.
        assert!(load_partclone_cache(&open).is_none());
    }
}
