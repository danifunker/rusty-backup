pub mod disk_image_stream;
pub mod format;
pub mod metadata;
#[cfg(feature = "chd")]
pub mod single_file_chd;
/// Stub for `single_file_chd` when the `chd` feature is off — provides
/// the public surface (`is_supported`, `run_export`, `SingleFileChdExportInputs`)
/// that callers reference, all routing to a typed error or `false`.
#[cfg(not(feature = "chd"))]
pub mod single_file_chd {
    use crate::partition::PartitionTable;
    use anyhow::{bail, Result};
    use std::path::PathBuf;

    pub fn is_supported(_table: &PartitionTable) -> bool {
        false
    }

    /// Stand-in for the real input struct; the runner never reads these
    /// because [`run_export`] always errors first.
    pub struct SingleFileChdExportInputs {
        pub source_path: PathBuf,
        pub output_path: PathBuf,
    }

    pub fn run_export(_inputs: SingleFileChdExportInputs) -> Result<()> {
        bail!("chd feature not built into this binary")
    }
}
mod sizes;
pub mod verify;

use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::fs;
use crate::partition::{self, PartitionTable};
use crate::rbformats::chd_options::ChdOptions;
use metadata::{
    AlignmentMetadata, BackupLayout, BackupMetadata, ExtendedContainerMetadata, PartitionMetadata,
    SizePolicy,
};

/// Compression type for backup output.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum CompressionType {
    Chd,
    /// DVD-profile CHD (MAME 0.287+). On disk it's still a `.chd` file;
    /// the metadata tag distinguishes it. Hunk size + codecs default to
    /// chdman's DVD profile (4096-byte hunks, 2048-byte sectors).
    Dvd,
    Vhd,
    Zstd,
    None,
}

impl CompressionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            CompressionType::Chd => "chd",
            CompressionType::Dvd => "chd-dvd",
            CompressionType::Vhd => "vhd",
            CompressionType::Zstd => "zstd",
            CompressionType::None => "none",
        }
    }

    pub fn file_extension(&self) -> &'static str {
        match self {
            CompressionType::Chd | CompressionType::Dvd => "chd",
            CompressionType::Vhd => "vhd",
            CompressionType::Zstd => "zst",
            CompressionType::None => "raw",
        }
    }
}

/// Checksum algorithm choice.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ChecksumType {
    Sha256,
    Crc32,
}

impl ChecksumType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChecksumType::Sha256 => "sha256",
            ChecksumType::Crc32 => "crc32",
        }
    }
}

/// Configuration for a backup run.
#[derive(Debug, Clone)]
pub struct BackupConfig {
    pub source_path: PathBuf,
    pub destination_dir: PathBuf,
    pub backup_name: String,
    pub compression: CompressionType,
    pub checksum: ChecksumType,
    pub split_size_mib: Option<u32>,
    /// When true, copy every sector verbatim (including blank space).
    /// When false (default), skip all-zero blocks for smaller/faster output.
    pub sector_by_sector: bool,
    /// When set, only back up partitions whose index is in the list.
    /// The partition table is always exported in full regardless.
    pub partition_filter: Option<Vec<usize>>,
    /// CHD codec + hunk-size options (only consulted when `compression`
    /// is `Chd` or `Dvd`). `None` = use the profile's chdman defaults.
    pub chd_options: Option<ChdOptions>,
    /// Backup-time partition size policy (single-file CHD only). `None`
    /// keeps source sizes. The actual per-partition sizes the user picked
    /// live in `partition_target_sizes`; `size_policy` is the policy label
    /// recorded in metadata for traceability.
    pub size_policy: Option<SizePolicy>,
    /// Per-partition target sizes in bytes, keyed by partition index
    /// (single-file CHD only). Entries omitted from this map default to
    /// the partition's source size. Non-trivial entries route through
    /// the staging path's per-partition resize machinery (see
    /// `single_file_chd::run_via_staging`).
    pub partition_target_sizes: Option<Vec<(usize, u64)>>,
    /// When `true`, eligible HFS+/HFSX partitions are routed through the
    /// streamed defrag-clone path: the captured image is a fully repacked
    /// HFS+ volume sized at its `defragmented_min_size_bytes`. Other
    /// filesystems are unaffected. Ignored when [`BackupConfig::sector_by_sector`]
    /// is on. Pre-flight via [`fs::can_defrag_clone_hfsplus`] gates each
    /// partition; failure aborts the whole backup with the reason.
    pub shrink_to_minimum: bool,
    /// Pre-computed defragmented minimum sizes from the GUI's "Calc min"
    /// affordance, keyed by partition index. When present, `analyze_partitions`
    /// skips the (slow) per-partition defragmented-minimum walk and uses these
    /// values directly. Sizes that are absent here still trigger the walk.
    /// Empty / `None` = walk every eligible HFS+/HFS partition (the legacy
    /// behavior).
    pub precomputed_minimum_sizes: Option<Vec<(usize, u64)>>,
    /// Per-partition opt-in for the HFS+ defrag-clone path. When `None`,
    /// the legacy "global" behavior applies: every HFS+/HFSX partition
    /// gets defrag-cloned whenever [`BackupConfig::shrink_to_minimum`] is
    /// on. When `Some(set)`, only partitions whose index appears in
    /// `set` are routed through the clone pipeline; the rest fall back to
    /// the layout-preserving in-place trim path (same as
    /// `shrink_to_minimum=false` for that partition). Drives the new
    /// per-partition Defrag checkbox in the backup UI.
    pub defrag_partition_indices: Option<std::collections::HashSet<usize>>,
}

/// Shared progress state between background backup thread and the GUI.
pub struct BackupProgress {
    pub current_bytes: u64,
    pub total_bytes: u64,
    /// Full untrimmed partition sizes — when larger than `total_bytes`,
    /// indicates smart trimming is saving space.
    pub full_size_bytes: u64,
    pub operation: String,
    pub finished: bool,
    pub error: Option<String>,
    pub cancel_requested: bool,
    pub log_messages: VecDeque<LogMessage>,
}

/// A log message from the backup thread.
pub struct LogMessage {
    pub level: LogLevel,
    pub message: String,
}

#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Info,
    Warning,
    Error,
}

impl Default for BackupProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl BackupProgress {
    pub fn new() -> Self {
        Self {
            current_bytes: 0,
            total_bytes: 0,
            full_size_bytes: 0,
            operation: String::new(),
            finished: false,
            error: None,
            cancel_requested: false,
            log_messages: VecDeque::new(),
        }
    }
}

fn log(progress: &Arc<Mutex<BackupProgress>>, level: LogLevel, message: impl Into<String>) {
    if let Ok(mut p) = progress.lock() {
        p.log_messages.push_back(LogMessage {
            level,
            message: message.into(),
        });
    }
}

pub(super) fn set_operation(progress: &Arc<Mutex<BackupProgress>>, op: impl Into<String>) {
    if let Ok(mut p) = progress.lock() {
        p.operation = op.into();
    }
}

fn is_cancelled(progress: &Arc<Mutex<BackupProgress>>) -> bool {
    progress.lock().map(|p| p.cancel_requested).unwrap_or(false)
}

fn set_progress_bytes(progress: &Arc<Mutex<BackupProgress>>, current: u64, total: u64) {
    if let Ok(mut p) = progress.lock() {
        p.current_bytes = current;
        p.total_bytes = total;
    }
}

/// Where a backup reads its bytes from.
///
/// The backup engine is otherwise source-agnostic: it parses the partition
/// table and streams partition bytes through the same compaction / compression
/// pipeline regardless of whether the bytes come from a local file/device or a
/// remote image served over the block tier (`rb-cli serve`). The variants here
/// are the two entry points; everything downstream funnels through a
/// [`SourceFactory`].
pub enum BackupSource {
    /// A local file or raw device, opened (and elevated) by the engine.
    Path(PathBuf),
    /// A remote source served over the block tier. For an **image file**,
    /// `path` is relative to the daemon's serve root and `is_device` is false;
    /// for a **physical drive** on the remote machine, `path` is the daemon's
    /// device path (e.g. `/dev/sda`) and `is_device` is true. `display` is the
    /// label recorded in `metadata.json`'s `source_device` (e.g.
    /// `rb://host:7341/disk.img` or `rb://host:7341/dev/sda`).
    #[cfg(feature = "remote")]
    Remote {
        conn: Arc<Mutex<crate::remote::RemoteConnection>>,
        path: String,
        display: String,
        /// `true` = a raw physical device (`OpenDevice`); `false` = an image
        /// file under the serve root (`OpenBlock`).
        is_device: bool,
    },
}

/// Mints fresh, independent, seekable readers over the backup source.
///
/// The legacy path-based engine cloned the elevated `File` handle
/// (`source.get_ref().try_clone()`) every time it needed an independent reader
/// — for the per-partition compaction probe, the trim read, the defrag-clone
/// producer thread, etc. This type generalises that "mint a fresh reader"
/// operation so the same engine works over a [`crate::remote::RemoteBlockReader`]
/// (one fresh reader per call, opened on the shared connection).
///
/// Generic engine paths (partition-table parse, FS probes, compaction, trim
/// read) call [`SourceFactory::open`] and get a `Box<dyn ReadSeek>`. Two
/// advanced paths — single-file CHD and HFS+/PFS3 defrag-clone — remain
/// **local-only** (gated off for remote) and reach for the concrete `&File`
/// via [`SourceFactory::local_file`], preserving their existing `File`-specific
/// behaviour (positioned reads, `disk_image_stream`).
pub(crate) enum SourceFactory {
    Local {
        file: File,
        /// Auto-deletes the temp device image (if any) on drop. Held for the
        /// lifetime of the backup; never read directly.
        _guard: crate::os::TempFileGuard,
        path: PathBuf,
    },
    #[cfg(feature = "remote")]
    Remote {
        conn: Arc<Mutex<crate::remote::RemoteConnection>>,
        path: String,
        /// Total image length, captured once at construction (one round-trip)
        /// so `total_size` doesn't re-open a block handle.
        size: u64,
        /// `true` = open `path` as a raw physical device; `false` = an image
        /// file under the serve root.
        is_device: bool,
    },
}

impl SourceFactory {
    /// Mint a fresh, independent seekable reader positioned at offset 0.
    fn open(&self) -> Result<Box<dyn crate::rbformats::ReadSeek>> {
        match self {
            SourceFactory::Local { file, .. } => Ok(Box::new(
                file.try_clone()
                    .context("failed to clone local source handle")?,
            )),
            #[cfg(feature = "remote")]
            SourceFactory::Remote {
                conn,
                path,
                is_device,
                ..
            } => {
                let reader = if *is_device {
                    crate::remote::RemoteBlockReader::open_device(Arc::clone(conn), path)
                } else {
                    crate::remote::RemoteBlockReader::open(Arc::clone(conn), path)
                }
                .with_context(|| format!("failed to open remote source {path}"))?;
                Ok(Box::new(reader))
            }
        }
    }

    /// Total size of the source image in bytes.
    fn total_size(&self) -> Result<u64> {
        match self {
            SourceFactory::Local { file, path, .. } => crate::os::get_file_size(file, path),
            #[cfg(feature = "remote")]
            SourceFactory::Remote { size, .. } => Ok(*size),
        }
    }

    /// The concrete local `File`, if this is a local source. Used only by the
    /// local-only paths (single-file CHD, defrag-clone) that need `File`-specific
    /// behaviour; always `None` for a remote source.
    fn local_file(&self) -> Option<&File> {
        match self {
            SourceFactory::Local { file, .. } => Some(file),
            #[cfg(feature = "remote")]
            SourceFactory::Remote { .. } => None,
        }
    }

    /// Whether this source is remote (some engine paths are gated off for it).
    fn is_remote(&self) -> bool {
        #[cfg(feature = "remote")]
        {
            matches!(self, SourceFactory::Remote { .. })
        }
        #[cfg(not(feature = "remote"))]
        {
            false
        }
    }
}

/// Main backup orchestrator for a local file/device. Runs on a background
/// thread. Thin wrapper over [`run_backup_from`] with a path-based source.
pub fn run_backup(config: BackupConfig, progress: Arc<Mutex<BackupProgress>>) -> Result<()> {
    let source = BackupSource::Path(config.source_path.clone());
    run_backup_from(source, config, progress)
}

/// Backup orchestrator parameterised on the byte source. The path-based
/// [`run_backup`] and (with the `remote` feature) a remote-image source both
/// funnel here. Runs on a background thread.
pub fn run_backup_from(
    source: BackupSource,
    config: BackupConfig,
    progress: Arc<Mutex<BackupProgress>>,
) -> Result<()> {
    // Build the reader factory: a local backup opens + elevates the path; a
    // remote backup brokers ranged reads over the shared connection.
    let (factory, source_display): (SourceFactory, String) = match source {
        BackupSource::Path(path) => {
            log(
                &progress,
                LogLevel::Info,
                format!("Starting backup of {}", path.display()),
            );
            set_operation(&progress, "Opening source device...");
            log(
                &progress,
                LogLevel::Info,
                "Requesting device access (you may be prompted for administrator credentials)...",
            );
            let elevated = crate::os::open_source_for_reading(&path)
                .with_context(|| format!("cannot open source: {}", path.display()))?;
            if let Some(tmp) = elevated.temp_path() {
                log(
                    &progress,
                    LogLevel::Info,
                    format!("Created temporary device image: {}", tmp.display()),
                );
            }
            let (file, guard) = elevated.into_parts();
            let display = path.display().to_string();
            (
                SourceFactory::Local {
                    file,
                    _guard: guard,
                    path,
                },
                display,
            )
        }
        #[cfg(feature = "remote")]
        BackupSource::Remote {
            conn,
            path,
            display,
            is_device,
        } => {
            let kind = if is_device { "drive" } else { "image" };
            log(
                &progress,
                LogLevel::Info,
                format!("Starting backup of remote {kind} {display}"),
            );

            // Single-file CHD and the HFS+/PFS3 defrag-clone (shrink-to-minimum)
            // paths read the whole disk with positioned, random-access local
            // `File` reads. Rather than refuse them over the wire, stream the
            // remote disk to a local scratch file once (sequential bulk read)
            // and run the *normal local pipeline* on it — full feature parity.
            // The streaming per-partition path (Zstd/Raw/VHD, no shrink) stays
            // direct, with no whole-disk temp.
            let needs_local_copy = matches!(
                config.compression,
                CompressionType::Chd | CompressionType::Dvd
            ) || config.shrink_to_minimum;

            if needs_local_copy {
                let reason = if matches!(
                    config.compression,
                    CompressionType::Chd | CompressionType::Dvd
                ) {
                    "CHD output"
                } else {
                    "shrink-to-minimum"
                };
                log(
                    &progress,
                    LogLevel::Info,
                    format!(
                        "{reason} needs a local copy of the remote {kind}; \
                         downloading it first..."
                    ),
                );
                let (file, guard, temp_path) = materialize_remote_to_temp(
                    &conn,
                    &path,
                    is_device,
                    &config.destination_dir,
                    &config.backup_name,
                    &progress,
                )?;
                (
                    SourceFactory::Local {
                        file,
                        _guard: guard,
                        path: temp_path,
                    },
                    display,
                )
            } else {
                set_operation(&progress, "Opening remote source...");
                // One round-trip to learn the source length (and prove it opens).
                let size = {
                    let reader = if is_device {
                        crate::remote::RemoteBlockReader::open_device(Arc::clone(&conn), &path)
                    } else {
                        crate::remote::RemoteBlockReader::open(Arc::clone(&conn), &path)
                    }
                    .with_context(|| format!("failed to open remote source {path}"))?;
                    reader.len()
                };
                (
                    SourceFactory::Remote {
                        conn,
                        path,
                        size,
                        is_device,
                    },
                    display,
                )
            }
        }
    };

    run_backup_inner(&factory, source_display, config, progress)
}

/// Stream a whole remote source (image or device) to a local scratch file so
/// the `File`-specific backup paths (single-file CHD, HFS+/PFS3 defrag-clone)
/// can run on it exactly as for a local disk. Returns the read-only temp
/// `File`, its auto-deleting guard, and the path. The temp lives in `dest_dir`
/// (where the backup is written — the user already chose a roomy location) and
/// is removed on any exit. Bulk sequential download in `MAX_RANGE_READ`-sized
/// chunks (no random access), with progress + cancel.
#[cfg(feature = "remote")]
fn materialize_remote_to_temp(
    conn: &Arc<Mutex<crate::remote::RemoteConnection>>,
    path: &str,
    is_device: bool,
    dest_dir: &std::path::Path,
    backup_name: &str,
    progress: &Arc<Mutex<BackupProgress>>,
) -> Result<(File, crate::os::TempFileGuard, PathBuf)> {
    use std::io::Write;

    // 4 MiB per request — the server's per-read cap; far fewer round-trips than
    // the 256 KiB window the seekable RemoteBlockReader uses.
    const CHUNK: u32 = 4 * 1024 * 1024;

    let (handle, total) = {
        let mut c = conn
            .lock()
            .map_err(|_| anyhow::anyhow!("remote connection lock poisoned"))?;
        if is_device {
            c.open_device(path)
        } else {
            c.open_block(path)
        }
        .with_context(|| format!("failed to open remote source {path}"))?
    };

    std::fs::create_dir_all(dest_dir).ok();
    let temp_path = dest_dir.join(format!(".{backup_name}.remote-download.tmp"));
    // Build the delete-guard *before* writing so a mid-stream failure (or
    // cancel) still removes the partial scratch file.
    let guard = crate::os::TempFileGuard::deleting(temp_path.clone());

    set_operation(progress, "Downloading remote disk...");
    log(
        progress,
        LogLevel::Info,
        format!(
            "Downloading {} from the remote to a local scratch file ({})...",
            partition::format_size(total),
            temp_path.display()
        ),
    );
    if let Ok(mut p) = progress.lock() {
        p.total_bytes = total;
        p.full_size_bytes = total;
        p.current_bytes = 0;
    }

    let download = (|| -> Result<()> {
        let mut writer = std::io::BufWriter::new(
            File::create(&temp_path)
                .with_context(|| format!("creating scratch file {}", temp_path.display()))?,
        );
        let mut offset: u64 = 0;
        while offset < total {
            if is_cancelled(progress) {
                bail!("backup cancelled");
            }
            let want = (total - offset).min(CHUNK as u64) as u32;
            let chunk = {
                let mut c = conn
                    .lock()
                    .map_err(|_| anyhow::anyhow!("remote connection lock poisoned"))?;
                c.read_block(handle, offset, want)
                    .with_context(|| format!("reading remote disk at offset {offset}"))?
            };
            if chunk.is_empty() {
                break; // short of `total` — the daemon hit EOF
            }
            writer
                .write_all(&chunk)
                .context("writing the scratch file")?;
            offset += chunk.len() as u64;
            set_progress_bytes(progress, offset, total);
        }
        writer.flush().context("flushing the scratch file")?;
        Ok(())
    })();

    // Close the daemon-side handle regardless of how the download went.
    if let Ok(mut c) = conn.lock() {
        let _ = c.close_block(handle);
    }
    download?;

    let file = File::open(&temp_path)
        .with_context(|| format!("reopening scratch file {}", temp_path.display()))?;
    log(
        progress,
        LogLevel::Info,
        "Remote download complete; building the backup from the local copy...",
    );
    Ok((file, guard, temp_path))
}

/// The actual backup pipeline, source-agnostic via [`SourceFactory`].
fn run_backup_inner(
    factory: &SourceFactory,
    source_display: String,
    config: BackupConfig,
    progress: Arc<Mutex<BackupProgress>>,
) -> Result<()> {
    // CHD output and shrink-to-minimum need `File`-specific random-access
    // reads, so `run_backup_from` materializes a remote source to a local temp
    // before reaching here for those cases — the factory is `Local` by then.
    // A `Remote` factory therefore only ever runs the streaming per-partition
    // path (Zstd/Raw/VHD, no shrink); the guard keeps defrag-clone off it.
    let shrink_to_minimum = config.shrink_to_minimum && !factory.is_remote();

    set_operation(&progress, "Reading partition table...");
    // Primary sequential reader: MBR read, table detect, gpt.bin export, and
    // the trim-based per-partition read all run against this one reader.
    let mut source = factory.open()?;

    // Read the first 512 bytes for MBR export
    let mut mbr_bytes = [0u8; 512];
    source
        .read_exact(&mut mbr_bytes)
        .context("cannot read first sector")?;
    source.seek(SeekFrom::Start(0))?;

    // Log first bytes for diagnostics
    log(
        &progress,
        LogLevel::Info,
        format!(
            "First 16 bytes: {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X}, bytes 510-511: {:02X} {:02X}",
            mbr_bytes[0], mbr_bytes[1], mbr_bytes[2], mbr_bytes[3],
            mbr_bytes[4], mbr_bytes[5], mbr_bytes[6], mbr_bytes[7],
            mbr_bytes[8], mbr_bytes[9], mbr_bytes[10], mbr_bytes[11],
            mbr_bytes[12], mbr_bytes[13], mbr_bytes[14], mbr_bytes[15],
            mbr_bytes[510], mbr_bytes[511],
        ),
    );

    let mut table =
        PartitionTable::detect(&mut source).context("failed to detect partition table")?;

    // Fix up superfloppy size: seek(End(0)) returns 0 for macOS device files,
    // so we need to use platform-specific ioctl to get the real device size.
    if let PartitionTable::None { size_bytes, .. } = &mut table {
        if *size_bytes == 0 {
            if let Ok(real_size) = factory.total_size() {
                log(
                    &progress,
                    LogLevel::Info,
                    format!(
                        "Device size via seek was 0, using ioctl size: {} bytes",
                        real_size
                    ),
                );
                *size_bytes = real_size;
            }
        }
    }

    let alignment = partition::detect_alignment(&table);
    let mut partitions = table.partitions();
    let is_superfloppy = matches!(table, PartitionTable::None { .. });

    // For APM disks, probe "Apple_HFS" partitions to detect the actual HFS variant
    // (native HFS vs native HFS+ vs embedded HFS+) and update the type_name accordingly.
    if matches!(table, PartitionTable::Apm(_)) {
        for part in &mut partitions {
            if part.partition_type_string.as_deref() == Some("Apple_HFS") {
                let part_offset = part.byte_offset();
                if let Ok(clone) = factory.open() {
                    let mut br = std::io::BufReader::new(clone);
                    let detected = fs::probe_apple_hfs_type(&mut br, part_offset);
                    if detected == "HFS+" || detected == "HFSX" {
                        part.type_name = part
                            .type_name
                            .replace("Apple_HFS", &format!("Apple_HFS ({detected})"));
                    }
                }
            }
        }
    }

    // MBR type 0x83 is officially "Linux", but MSX HDD formatters (Nextor and
    // similar) reuse it for FAT12/16. Probe the VBR so backup logs/metadata
    // show the actual FS family rather than the generic "Linux" label. For
    // FAT VBRs we additionally open the BPB to tag the FAT subtype + "(MSX)".
    for part in &mut partitions {
        if part.is_extended_container {
            continue;
        }
        // MBR 0x83 (Linux native) or GPT "Linux Filesystem" GUID — both can
        // hold ext, btrfs, or XFS. Probe the VBR so the backup metadata
        // reflects the real family rather than the generic GPT label.
        let is_linux_mbr = part.partition_type_byte == 0x83;
        let is_linux_gpt =
            part.partition_type_string.as_deref() == Some("0FC63DAF-8483-4772-8E79-3D69D8477DE4");
        if !is_linux_mbr && !is_linux_gpt {
            continue;
        }
        let Ok(clone) = factory.open() else {
            continue;
        };
        let mut br = std::io::BufReader::new(clone);
        let part_offset = part.byte_offset();
        match fs::probe_0x83_fs_type(&mut br, part_offset) {
            Some("FAT") if is_linux_mbr => {
                let subtype = factory.open().ok().and_then(|c| {
                    fs::fat::FatFilesystem::open(std::io::BufReader::new(c), part_offset)
                        .ok()
                        .map(|fs| {
                            use crate::fs::Filesystem;
                            fs.fs_type().to_string()
                        })
                });
                part.type_name = match subtype {
                    Some(t) => format!("{t} (MSX)"),
                    None => "FAT (MSX)".to_string(),
                };
            }
            Some(other) => {
                part.type_name = other.to_string();
            }
            None => {}
        }
    }

    if is_superfloppy {
        log(
            &progress,
            LogLevel::Info,
            format!(
                "Detected superfloppy (no partition table) with {} partition(s)",
                partitions.len(),
            ),
        );
        for p in &partitions {
            log(
                &progress,
                LogLevel::Info,
                format!(
                    "  Partition {}: type_byte=0x{:02X} type_name={} start_lba={} size={}",
                    p.index, p.partition_type_byte, p.type_name, p.start_lba, p.size_bytes,
                ),
            );
        }
    } else {
        log(
            &progress,
            LogLevel::Info,
            format!(
                "Detected {} partition table with {} partition(s), alignment: {}",
                table.type_name(),
                partitions.len(),
                alignment.alignment_type
            ),
        );
        for p in &partitions {
            log(
                &progress,
                LogLevel::Info,
                format!(
                    "  Partition {}: type_byte=0x{:02X} type_name={} start_lba={} size={}",
                    p.index, p.partition_type_byte, p.type_name, p.start_lba, p.size_bytes,
                ),
            );
        }
    }

    if is_cancelled(&progress) {
        bail!("backup cancelled");
    }

    // Get source size
    log(&progress, LogLevel::Info, "Getting source device size...");
    let source_size = factory.total_size().context("failed to get source size")?;
    log(
        &progress,
        LogLevel::Info,
        format!("Source size: {} bytes", source_size),
    );

    // Step 2: Create backup folder
    set_operation(&progress, "Creating backup folder...");
    let backup_folder = format::create_backup_folder(&config.destination_dir, &config.backup_name)?;
    log(
        &progress,
        LogLevel::Info,
        format!("Backup folder: {}", backup_folder.display()),
    );

    // Step 3: Export partition table
    //
    // Single-file CHD backups already carry the raw partition-table sector
    // inside `disk.chd` at offset 0 — writing a separate `mbr.bin` would
    // duplicate those bytes. We still emit the parsed JSON sidecar
    // (`mbr.json`) for fast inspect loads.
    let single_file_chd_planned = matches!(
        config.compression,
        CompressionType::Chd | CompressionType::Dvd
    ) && !is_superfloppy
        && config.split_size_mib.is_none()
        && single_file_chd::is_supported(&table);

    set_operation(&progress, "Exporting partition table...");
    match &table {
        PartitionTable::Mbr(mbr) => {
            if single_file_chd_planned {
                format::export_mbr_json(mbr, &backup_folder)?;
                log(
                    &progress,
                    LogLevel::Info,
                    "Exported MBR (mbr.json only — raw bytes live in disk.chd)",
                );
            } else {
                format::export_mbr(mbr, &mbr_bytes, &backup_folder)?;
                log(
                    &progress,
                    LogLevel::Info,
                    "Exported MBR (mbr.bin + mbr.json)",
                );
            }
        }
        PartitionTable::Gpt { gpt, .. } => {
            if single_file_chd_planned {
                // Raw GPT sectors live inside disk.chd; only emit the JSON
                // sidecar for fast inspect.
                let json =
                    serde_json::to_string_pretty(gpt).context("failed to serialize GPT to JSON")?;
                std::fs::write(backup_folder.join("gpt.json"), json)
                    .context("failed to write gpt.json")?;
                log(
                    &progress,
                    LogLevel::Info,
                    "Exported GPT (gpt.json only — raw sectors live in disk.chd)",
                );
            } else {
                format::export_gpt(gpt, &mbr_bytes, &backup_folder)?;
                log(
                    &progress,
                    LogLevel::Info,
                    "Exported GPT (gpt.json + mbr.bin)",
                );
                if let Err(e) = format::export_gpt_bin(&mut source, &backup_folder) {
                    log(
                        &progress,
                        LogLevel::Warning,
                        format!("Failed to export gpt.bin: {e}"),
                    );
                } else {
                    log(
                        &progress,
                        LogLevel::Info,
                        "Exported raw GPT sectors (gpt.bin)",
                    );
                }
            }
        }
        PartitionTable::Apm(apm) => {
            if single_file_chd_planned {
                // apm.bin would duplicate the DDR + partition map already
                // embedded in disk.chd; emit only the JSON sidecar.
                let json =
                    serde_json::to_string_pretty(apm).context("failed to serialize APM to JSON")?;
                std::fs::write(backup_folder.join("apm.json"), json)
                    .context("failed to write apm.json")?;
                log(
                    &progress,
                    LogLevel::Info,
                    "Exported APM (apm.json only — raw blocks live in disk.chd)",
                );
            } else {
                format::export_apm(apm, &backup_folder)?;
                log(
                    &progress,
                    LogLevel::Info,
                    "Exported APM (apm.json + apm.bin)",
                );
            }
        }
        PartitionTable::Rdb(rdb) => {
            // Emit a JSON sidecar of the RDB layout so inspect tools and
            // future round-trip restores can read it. Per-partition data
            // backup follows the standard layout-preserving path once the
            // AFFS/PFS/SFS readers land.
            let json =
                serde_json::to_string_pretty(rdb).context("failed to serialize RDB to JSON")?;
            std::fs::write(backup_folder.join("rdb.json"), json)
                .context("failed to write rdb.json")?;
            log(&progress, LogLevel::Info, "Exported RDB (rdb.json)");
        }
        PartitionTable::Sgi(vh) => {
            // Step 2 surfaces SGI partitions in the inspect tab; backup of
            // SGI disks is a separate workflow (deferred). Emit a JSON
            // sidecar so the partition layout is recorded if a future
            // session does want to round-trip it, but bail before any
            // per-partition data write — the data path needs SGI-aware
            // sizing and the EFS/XFS readers to land first.
            let json = serde_json::to_string_pretty(vh)
                .context("failed to serialize SGI volume header to JSON")?;
            std::fs::write(backup_folder.join("sgi.json"), json)
                .context("failed to write sgi.json")?;
            log(
                &progress,
                LogLevel::Info,
                "Exported SGI volume header (sgi.json) — partition data backup not yet supported",
            );
            bail!("backing up SGI disks is not yet supported (browse only)");
        }
        PartitionTable::Ahdi(table) => {
            // Mirror the RDB / SGI sidecar shape: emit ahdi.json so a future
            // restore knows the AHDI primary slots, XGM chain, and disk-size
            // / bad-sector fields. Per-partition FAT data backup rides the
            // standard layout-preserving path through the existing FAT
            // pipeline.
            let json = serde_json::to_string_pretty(table)
                .context("failed to serialize AHDI table to JSON")?;
            std::fs::write(backup_folder.join("ahdi.json"), json)
                .context("failed to write ahdi.json")?;
            log(&progress, LogLevel::Info, "Exported AHDI (ahdi.json)");
        }
        PartitionTable::X68k { table, .. } => {
            // Mirror the AHDI sidecar shape: emit x68k.json so restore
            // knows the X68k SASI slots, disk-size field, and partition
            // start/length. Per-partition Human68k (FAT-derived) backup
            // rides the standard layout-preserving path through the
            // existing FAT pipeline.
            let json = serde_json::to_string_pretty(table)
                .context("failed to serialize X68k table to JSON")?;
            std::fs::write(backup_folder.join("x68k.json"), json)
                .context("failed to write x68k.json")?;
            log(&progress, LogLevel::Info, "Exported X68k (x68k.json)");
        }
        PartitionTable::None { fs_hint, .. } => {
            log(
                &progress,
                LogLevel::Info,
                format!("No partition table (superfloppy, filesystem: {fs_hint})"),
            );
        }
    }

    if is_cancelled(&progress) {
        bail!("backup cancelled");
    }

    // Step 4: Analyze partitions for smart sizing
    //
    // When not in sector-by-sector mode, try filesystem-aware compaction first
    // (FAT only — packs allocated clusters contiguously for a smaller, defragmented
    // image). If compaction isn't possible, fall back to trim-based sizing that
    // skips unused space beyond the last allocated cluster.
    //
    // This runs before the single-file-CHD branch so the CHD path can persist
    // `minimum_size_bytes` / `defragmented_min_size_bytes` in metadata.json,
    // which the restore-resize path uses to validate Custom shrinks and to
    // populate the "Minimum" picker.
    log(
        &progress,
        LogLevel::Info,
        format!(
            "Analyzing {} partitions for smart sizing...",
            partitions.len()
        ),
    );

    let sizes::PartitionSizing {
        mut effective_sizes,
        mut stream_sizes,
        compact_sizes,
        minimum_sizes,
        defragmented_min_sizes,
        mut is_layout_preserving_flags,
    } = sizes::analyze_partitions(
        factory,
        &partitions,
        config.sector_by_sector,
        is_superfloppy,
        config.precomputed_minimum_sizes.as_deref(),
        &progress,
    );

    // Phase-8 / Step-22f: shrink-to-minimum pre-flight + sizing override.
    //
    // For each clone-eligible HFS+/HFSX partition, run
    // [`fs::can_defrag_clone_hfsplus`] before any bytes flow. On failure abort
    // the whole backup with the human-readable reason — the user can either
    // disable shrink-to-minimum or remediate the source (clean unmount, flatten
    // the wrapper) and retry.
    //
    // Successful pre-flights cause the per-partition loop to stream the
    // partition through `stream_defragmented_hfsplus` instead of the compact
    // reader; size vectors are overridden so progress accounting and the smart-
    // sizing log line reflect the post-clone footprint.
    let mut clone_target_sizes: Vec<Option<u64>> = vec![None; partitions.len()];
    let mut clone_shapes: Vec<Option<fs::DefragCloneShape>> = vec![None; partitions.len()];
    if shrink_to_minimum && !config.sector_by_sector {
        for (i, part) in partitions.iter().enumerate() {
            if part.is_extended_container || part.partition_type_byte == 0xEE {
                continue;
            }
            if let Some(ref filter) = config.partition_filter {
                if !filter.contains(&part.index) {
                    continue;
                }
            }
            // Per-partition Defrag opt-in (new GUI). When the caller supplies
            // an explicit allowlist, only those partitions get the clone
            // pipeline; the rest fall back to the layout-preserving in-place
            // trim. When the field is `None`, behave as before (clone every
            // HFS+/HFSX partition).
            if let Some(set) = config.defrag_partition_indices.as_ref() {
                if !set.contains(&part.index) {
                    continue;
                }
            }
            // Only HFS+/HFSX and PFS3 partitions participate. Skip
            // silently for other FS types — the flag is global; they
            // keep their default compaction path.
            let pts = part.partition_type_string.as_deref();
            let is_hfsplus = factory
                .open()
                .ok()
                .and_then(|c| {
                    let mut br = BufReader::new(c);
                    fs::probe_hfsplus_signature(&mut br, part.start_lba * 512)
                })
                .is_some();
            let is_pfs3 = pts.map(fs::is_amiga_pfs3_type).unwrap_or(false);
            let is_exfat = factory
                .open()
                .ok()
                .map(|c| {
                    let mut br = BufReader::new(c);
                    fs::probe_exfat_signature(&mut br, part.start_lba * 512)
                })
                .unwrap_or(false);
            let is_ntfs = factory
                .open()
                .ok()
                .map(|c| {
                    let mut br = BufReader::new(c);
                    fs::probe_ntfs_signature(&mut br, part.start_lba * 512)
                })
                .unwrap_or(false);
            if !is_hfsplus && !is_pfs3 && !is_exfat && !is_ntfs {
                continue;
            }
            let target = match defragmented_min_sizes[i] {
                Some(t) if t > 0 && t < part.size_bytes => t,
                _ => {
                    log(
                        &progress,
                        LogLevel::Info,
                        format!(
                            "partition-{}: shrink-to-minimum skipped — no defragmented \
                             minimum size available",
                            part.index
                        ),
                    );
                    continue;
                }
            };
            // Pre-flight on a fresh reader. Returns the shape (Flat or
            // Wrapped for HFS+/HFSX, Pfs3 for PFS3) that the streaming
            // side should use.
            let pf = factory
                .open()
                .context("clone source for shrink-preflight")
                .and_then(|c| {
                    let mut br = BufReader::new(c);
                    fs::detect_defrag_clone_shape(&mut br, part.start_lba * 512, pts)
                        .map_err(|reason| anyhow::anyhow!("{reason}"))
                });
            match pf {
                Ok(shape) => {
                    let shape_label = match shape {
                        fs::DefragCloneShape::Flat => "flat HFS+",
                        fs::DefragCloneShape::Wrapped => "wrapped HFS+ (HFS shell preserved)",
                        fs::DefragCloneShape::Pfs3 => "PFS3 (Amiga)",
                        fs::DefragCloneShape::Exfat => "exFAT",
                        fs::DefragCloneShape::Ntfs => "NTFS",
                    };
                    log(
                        &progress,
                        LogLevel::Info,
                        format!(
                            "partition-{}: streaming defragmented {} — \
                             source {} -> {}",
                            part.index,
                            shape_label,
                            partition::format_size(part.size_bytes),
                            partition::format_size(target),
                        ),
                    );
                    clone_target_sizes[i] = Some(target);
                    clone_shapes[i] = Some(shape);
                    // Override sizing: the streamed image flows through the
                    // compressor verbatim at `target` bytes. `compact_sizes`
                    // is left as whatever `analyze_partitions` produced — the
                    // per-partition loop dispatches on `clone_target_sizes`
                    // first, so neither the compacted nor the trim branch
                    // runs for cloned partitions. The single-file CHD path
                    // reads `clone_target_sizes` directly and doesn't consume
                    // these size vectors, so the override is harmless there.
                    effective_sizes[i] = target;
                    stream_sizes[i] = target;
                    is_layout_preserving_flags[i] = false;
                }
                Err(e) => {
                    // Pre-flight refused this partition (typically embedded
                    // HFS+ inside an HFS wrapper, or a dirty journal). Skip
                    // the clone for this partition only — it'll fall back to
                    // the regular layout-preserving / packed-padded path at
                    // its original size, and other partitions can still be
                    // cloned. The user gets a smaller-than-no-shrink image
                    // (any cloneable partitions still shrink) without losing
                    // the whole backup to one un-cloneable volume.
                    log(
                        &progress,
                        LogLevel::Warning,
                        format!(
                            "partition-{}: skipping shrink-to-minimum — {e}. \
                             This partition keeps its original size; other \
                             partitions are unaffected.",
                            part.index,
                        ),
                    );
                }
            }
        }
    } else if shrink_to_minimum && config.sector_by_sector {
        log(
            &progress,
            LogLevel::Warning,
            "shrink-to-minimum is incompatible with sector-by-sector mode; \
             ignoring the flag.",
        );
    }

    // Single-file CHD branch: when the user asked for CHD output and the
    // source has a partition table single_file_chd can handle, synthesise a
    // whole-disk image into one CHD and skip the per-partition loop. Falls
    // through to the legacy per-partition path otherwise.
    #[cfg(feature = "chd")]
    if single_file_chd_planned {
        // CHD is local-only (remote sources bail above), so the staging path's
        // `disk_image_stream` builder gets a concrete `BufReader<File>`.
        let chd_file = factory
            .local_file()
            .context("single-file CHD backup requires a local source")?
            .try_clone()
            .context("failed to clone local source for CHD staging")?;
        let chd_source = BufReader::new(chd_file);
        return run_single_file_chd_path(
            &config,
            &progress,
            &chd_source,
            source_size,
            &mbr_bytes,
            &table,
            &partitions,
            &alignment,
            &backup_folder,
            &minimum_sizes,
            &defragmented_min_sizes,
            &clone_target_sizes,
            &clone_shapes,
        );
    }
    #[cfg(not(feature = "chd"))]
    if single_file_chd_planned {
        anyhow::bail!(
            "this binary was built without the `chd` feature; \
             single-file CHD backups are unavailable"
        );
    }
    // CHD/DVD selected on a source single_file_chd can't handle (only
    // superfloppies fit this today — every other shape is_supported).
    // Superfloppies route through the per-partition loop with
    // `effective_compression` forced to `None` (raw .img), so the user
    // ends up with a `partition-0.img` rather than a CHD. We don't emit
    // per-partition CHDs anywhere; CHD output is single-file or nothing.
    if matches!(
        config.compression,
        CompressionType::Chd | CompressionType::Dvd
    ) && config.split_size_mib.is_some()
    {
        log(
            &progress,
            LogLevel::Warning,
            "Split-size set with CHD output: splitting is incompatible with \
             chdman/MAME single-file CHDs and is ignored.",
        );
    }

    let split_bytes = config.split_size_mib.map(|mib| mib as u64 * 1024 * 1024);
    let mut partition_metadata = Vec::new();
    let mut overall_bytes_done: u64 = 0;

    // Export mbr-min.bin for MBR tables: a copy of the MBR with each
    // partition's total_sectors reduced to the effective (imaged) size.
    if let PartitionTable::Mbr(_) = &table {
        let mut min_sectors: Vec<(usize, u32)> = Vec::new();
        for (i, part) in partitions.iter().enumerate() {
            if part.is_logical {
                continue; // logical partitions live in EBRs, not the MBR
            }
            if part.is_extended_container {
                // Sum effective sizes of all logical partitions inside this container
                let logical_sum: u64 = partitions
                    .iter()
                    .enumerate()
                    .filter(|(_, p)| p.is_logical)
                    .map(|(j, _)| effective_sizes[j])
                    .sum();
                let new_sectors = (logical_sum / 512) as u32;
                min_sectors.push((part.index, new_sectors));
            } else {
                let new_sectors = (effective_sizes[i] / 512) as u32;
                min_sectors.push((part.index, new_sectors));
            }
        }
        if let Err(e) = format::export_mbr_min(&mbr_bytes, &min_sectors, &backup_folder) {
            log(
                &progress,
                LogLevel::Warning,
                format!("Failed to export mbr-min.bin: {e}"),
            );
        } else {
            log(
                &progress,
                LogLevel::Info,
                "Exported minimum-size MBR (mbr-min.bin)",
            );
        }
    }

    let sizes::BackupTotals {
        total_display_bytes,
        total_stream_bytes,
        full_partition_bytes,
    } = sizes::compute_totals(
        &partitions,
        &effective_sizes,
        &stream_sizes,
        config.partition_filter.as_deref(),
    );

    if let Ok(mut p) = progress.lock() {
        p.total_bytes = total_stream_bytes;
        p.full_size_bytes = full_partition_bytes;
        p.current_bytes = 0;
    }

    if full_partition_bytes > total_display_bytes {
        log(
            &progress,
            LogLevel::Info,
            format!(
                "Smart sizing: imaging {} of {} total (saving {})",
                partition::format_size(total_display_bytes),
                partition::format_size(full_partition_bytes),
                partition::format_size(full_partition_bytes - total_display_bytes),
            ),
        );
    }

    for (part_idx, part) in partitions.iter().enumerate() {
        if is_cancelled(&progress) {
            bail!("backup cancelled");
        }

        // Skip GPT protective partitions (0xEE) — not a real partition.
        if part.partition_type_byte == 0xEE {
            continue;
        }

        // Skip partitions not in the filter (if a filter is set)
        if let Some(ref filter) = config.partition_filter {
            if !filter.contains(&part.index) {
                log(
                    &progress,
                    LogLevel::Info,
                    format!(
                        "Skipping partition-{} (not selected for backup)",
                        part.index
                    ),
                );
                continue;
            }
        }

        log(
            &progress,
            LogLevel::Info,
            format!(
                "Processing partition index {} (partition-{})",
                part_idx, part.index
            ),
        );

        if part.is_extended_container {
            log(
                &progress,
                LogLevel::Info,
                format!(
                    "Skipping extended container partition-{} (logical partitions backed up individually)",
                    part.index
                ),
            );
            continue;
        }

        // image_size: logical data bytes (allocated blocks × block_size) for this partition.
        //   Stored in imaged_size_bytes and used for non-compacted source.take() limit.
        let image_size = effective_sizes[part_idx];
        // stream_size: actual bytes that will flow through the compressor.
        //   For LP readers this is the minimum (trimmed); equals image_size.
        let stream_size = stream_sizes[part_idx];
        let is_compacted = compact_sizes[part_idx].is_some();
        // is_layout_preserving: the compact reader emits free blocks as zeros.
        //   Tracked separately since after trimming stream_size == image_size for all modes.
        let is_layout_preserving = is_layout_preserving_flags[part_idx];

        let part_label = format!("partition-{}", part.index);
        set_operation(
            &progress,
            format!(
                "Backing up {} ({})...",
                part_label,
                partition::format_size(image_size)
            ),
        );

        if is_compacted {
            if is_layout_preserving {
                log(
                    &progress,
                    LogLevel::Info,
                    format!(
                        "Compacting {}: {} at LBA {}, trimmed {} -> {} (free blocks -> zeros)",
                        part_label,
                        part.type_name,
                        part.start_lba,
                        partition::format_size(part.size_bytes),
                        partition::format_size(stream_size),
                    ),
                );
            } else {
                log(
                    &progress,
                    LogLevel::Info,
                    format!(
                        "Compacting {}: {} at LBA {}, {} -> {} (defragmented)",
                        part_label,
                        part.type_name,
                        part.start_lba,
                        partition::format_size(part.size_bytes),
                        partition::format_size(stream_size),
                    ),
                );
            }
        } else if image_size < part.size_bytes {
            log(
                &progress,
                LogLevel::Info,
                format!(
                    "Processing {}: {} at LBA {}, trimmed from {} to {}",
                    part_label,
                    part.type_name,
                    part.start_lba,
                    partition::format_size(part.size_bytes),
                    partition::format_size(image_size),
                ),
            );
        } else {
            log(
                &progress,
                LogLevel::Info,
                format!(
                    "Processing {}: {} at LBA {}, size {}",
                    part_label,
                    part.type_name,
                    part.start_lba,
                    partition::format_size(part.size_bytes)
                ),
            );
        }

        let output_base = backup_folder.join(&part_label);
        let progress_clone = Arc::clone(&progress);
        let base_bytes = overall_bytes_done;

        // Superfloppy: force raw compression to produce a universally compatible .img file
        let effective_compression = if is_superfloppy {
            CompressionType::None
        } else {
            config.compression
        };

        log(
            &progress,
            LogLevel::Info,
            format!(
                "Starting compression for {}, is_compacted={}",
                part_label, is_compacted
            ),
        );

        let clone_target = clone_target_sizes[part_idx];
        let clone_shape = clone_shapes[part_idx];
        // When the output is a single file (no user-selected split),
        // build a shared hasher and tee it through the compressor so
        // we can read the digest back without a post-write file
        // re-read. The compressors honour the hasher only when a
        // single output file is produced (zstd / raw / vhd with
        // split_size = None). For CHD/DVD outputs, or any split
        // output, the hasher stays empty and the fall-back
        // compute_checksum read pass handles each file.
        let output_hasher: Option<crate::rbformats::OutputHasherHandle> = if split_bytes.is_none() {
            Some(std::sync::Arc::new(std::sync::Mutex::new(Some(
                verify::RunningHasher::new(config.checksum),
            ))))
        } else {
            None
        };
        let compressed_files = if let Some(target_size) = clone_target {
            // Phase-8 / Step-22f: stream a defragmented HFS+ clone directly
            // into the compressor via a bounded channel pipe. The producer
            // thread owns its own `HfsPlusFilesystem` over a cloned source
            // file handle; the consumer (this thread) is the compressor.
            // Errors from either side propagate through the join.
            //
            // For wrapped HFS+ the producer instead drives
            // `stream_wrapped_defragmented_hfsplus`, which rebuilds the
            // outer HFS shell around the shrunken inner volume.
            let shape = clone_shape.unwrap_or(fs::DefragCloneShape::Flat);
            log(
                &progress,
                LogLevel::Info,
                format!(
                    "Defrag-cloning {} into {} ({})",
                    part_label,
                    partition::format_size(target_size),
                    match shape {
                        fs::DefragCloneShape::Flat => "flat HFS+",
                        fs::DefragCloneShape::Wrapped => "wrapped HFS+",
                        fs::DefragCloneShape::Pfs3 => "PFS3 (Amiga)",
                        fs::DefragCloneShape::Exfat => "exFAT",
                        fs::DefragCloneShape::Ntfs => "NTFS",
                    },
                ),
            );
            let part_offset = part.byte_offset();
            let part_size = part.size_bytes;
            // Defrag-clone is local-only (the Wrapped sub-path does positioned
            // `&File` reads); remote sources never set a clone target.
            let producer_clone = factory
                .local_file()
                .context("defrag-clone requires a local source")?
                .try_clone()
                .context("failed to clone source for defrag-clone producer")?;
            let (mut writer, mut reader) = channel_pipe();
            let progress_log_prod = Arc::clone(&progress);
            let producer =
                std::thread::spawn(move || -> Result<fs::hfsplus_defrag::DefragReport> {
                    let report = match shape {
                        fs::DefragCloneShape::Pfs3 => {
                            let br = BufReader::new(producer_clone);
                            let mut pfs = fs::pfs3::Pfs3Filesystem::open(br, part_offset)
                                .context("open source PFS3 for defrag-clone")?;
                            let progress_log_inner = Arc::clone(&progress_log_prod);
                            let mut clone_log = |s: &str| {
                                log(&progress_log_inner, LogLevel::Info, s.to_string());
                            };
                            let mut clone_progress = |_emitted: u64| {};
                            let pr = fs::pfs3_clone::stream_defragmented_pfs3(
                                &mut pfs,
                                target_size,
                                &mut writer,
                                &mut clone_log,
                                &mut clone_progress,
                            )
                            .context("stream_defragmented_pfs3")?;
                            // Forward PFS3 warnings (skipped hardlinks,
                            // deldir contents, etc) into the GUI log.
                            for w in &pr.warnings {
                                log(
                                    &progress_log_prod,
                                    LogLevel::Warning,
                                    format!("PFS3 clone: {w}"),
                                );
                            }
                            // Map PFS3 counters onto DefragReport so
                            // the existing log line below renders.
                            fs::hfsplus_defrag::DefragReport {
                                files_copied: pr.files_copied,
                                dirs_copied: pr.dirs_copied,
                                data_bytes_copied: pr.bytes_copied,
                                rsrc_bytes_copied: 0,
                                xattrs_copied: 0,
                                hardlinks_copied: pr.hardlinks_copied,
                                dir_hardlinks_copied: 0,
                                bytes_emitted: target_size,
                            }
                        }
                        fs::DefragCloneShape::Exfat => {
                            let br = BufReader::new(producer_clone);
                            let mut ex = fs::exfat::ExfatFilesystem::open(br, part_offset)
                                .context("open source exFAT for defrag-clone")?;
                            let progress_log_inner = Arc::clone(&progress_log_prod);
                            let mut clone_log = |s: &str| {
                                log(&progress_log_inner, LogLevel::Info, s.to_string());
                            };
                            let mut clone_progress = |_emitted: u64| {};
                            let pr = fs::exfat_clone::stream_defragmented_exfat(
                                &mut ex,
                                target_size,
                                &mut writer,
                                &mut clone_log,
                                &mut clone_progress,
                            )
                            .context("stream_defragmented_exfat")?;
                            for w in &pr.warnings {
                                log(
                                    &progress_log_prod,
                                    LogLevel::Warning,
                                    format!("exFAT clone: {w}"),
                                );
                            }
                            // Map exFAT counters onto DefragReport so the
                            // existing emit log line below renders.
                            fs::hfsplus_defrag::DefragReport {
                                files_copied: pr.files_copied,
                                dirs_copied: pr.dirs_copied,
                                data_bytes_copied: pr.bytes_copied,
                                rsrc_bytes_copied: 0,
                                xattrs_copied: 0,
                                hardlinks_copied: 0,
                                dir_hardlinks_copied: 0,
                                bytes_emitted: target_size,
                            }
                        }
                        fs::DefragCloneShape::Ntfs => {
                            let br = BufReader::new(producer_clone);
                            let mut nt = fs::ntfs::NtfsFilesystem::open(br, part_offset)
                                .context("open source NTFS for defrag-clone")?;
                            let progress_log_inner = Arc::clone(&progress_log_prod);
                            let mut clone_log = |s: &str| {
                                log(&progress_log_inner, LogLevel::Info, s.to_string());
                            };
                            let mut clone_progress = |_emitted: u64| {};
                            let pr = fs::ntfs_clone::stream_defragmented_ntfs(
                                &mut nt,
                                target_size,
                                &mut writer,
                                &mut clone_log,
                                &mut clone_progress,
                            )
                            .context("stream_defragmented_ntfs")?;
                            for w in &pr.warnings {
                                log(
                                    &progress_log_prod,
                                    LogLevel::Warning,
                                    format!("NTFS clone: {w}"),
                                );
                            }
                            fs::hfsplus_defrag::DefragReport {
                                files_copied: pr.files_copied,
                                dirs_copied: pr.dirs_copied,
                                data_bytes_copied: pr.bytes_copied,
                                rsrc_bytes_copied: 0,
                                xattrs_copied: 0,
                                hardlinks_copied: 0,
                                dir_hardlinks_copied: 0,
                                bytes_emitted: target_size,
                            }
                        }
                        fs::DefragCloneShape::Flat => {
                            let br = BufReader::new(producer_clone);
                            let mut hfs = fs::hfsplus::HfsPlusFilesystem::open(br, part_offset)
                                .context("open source HFS+ for defrag-clone")?;
                            fs::hfsplus_defrag::stream_defragmented_hfsplus(
                                &mut hfs,
                                target_size,
                                &mut writer,
                                None,
                            )
                            .context("stream_defragmented_hfsplus")?
                        }
                        fs::DefragCloneShape::Wrapped => {
                            // Positioned-read MDB detection — avoids the
                            // shared-fd-offset race when producer threads
                            // run concurrently across partitions.
                            let info = fs::hfsplus_wrapper_clone::detect_wrapped_hfsplus_at(
                                &producer_clone,
                                part_offset,
                                part_size,
                            )
                            .ok_or_else(|| {
                                anyhow::anyhow!("wrapped HFS+ detection failed at backup time")
                            })?;
                            let mut br = BufReader::new(producer_clone);
                            // The clone target is a partition-level size;
                            // back-derive the inner target by stripping the
                            // wrapper overhead (alloc-block start + pre-embed
                            // wrapper alloc blocks + 1024 alt-MDB tail).
                            let outer_overhead = (info.al_block_start_sector as u64) * 512
                                + (info.embed_start_block as u64) * (info.al_block_size as u64)
                                + 1024;
                            let inner_target = target_size.saturating_sub(outer_overhead);
                            let plan =
                                fs::hfsplus_wrapper_clone::plan_wrapped_clone(&info, inner_target)
                                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                            fs::hfsplus_wrapper_clone::stream_wrapped_defragmented_hfsplus(
                                &mut br,
                                &plan,
                                &mut writer,
                                None,
                            )
                            .map_err(|e| anyhow::anyhow!("{e}"))?
                        }
                    };
                    log(
                        &progress_log_prod,
                        LogLevel::Info,
                        format!(
                            "Defrag emit: {} files / {} folders / {} data / {} hardlinks / \
                             {} xattrs",
                            report.files_copied,
                            report.dirs_copied,
                            partition::format_size(report.data_bytes_copied),
                            report.hardlinks_copied + report.dir_hardlinks_copied,
                            report.xattrs_copied,
                        ),
                    );
                    Ok(report)
                });
            let progress_log = Arc::clone(&progress);
            let compress_result = crate::rbformats::compress_partition_hashed(
                &mut reader,
                &output_base,
                effective_compression,
                target_size,
                split_bytes,
                false, // streamed image is already packed; don't skip zeros
                config.chd_options.clone(),
                output_hasher.clone(),
                &mut |bytes_read| {
                    set_progress_bytes(
                        &progress_clone,
                        base_bytes + bytes_read,
                        total_stream_bytes,
                    );
                },
                &|| is_cancelled(&progress_clone),
                &mut |msg| log(&progress_log, LogLevel::Info, msg),
            );
            // Always join the producer so its errors surface even if the
            // consumer succeeded (e.g. partial pipe).
            let producer_result = producer
                .join()
                .map_err(|_| anyhow::anyhow!("defrag-clone producer thread panicked"))?;
            let files = compress_result
                .with_context(|| format!("failed to compress defrag-cloned {part_label}"))?;
            producer_result.with_context(|| format!("defrag-clone failed for {part_label}"))?;
            files
        } else if is_compacted {
            // Use compacted reader — create a fresh compact reader for this filesystem
            log(
                &progress,
                LogLevel::Info,
                format!("Creating compact reader for {}", part_label),
            );
            let part_offset = part.byte_offset();
            let clone = factory
                .open()
                .context("failed to open source for compaction")?;
            let (compact_reader, _) = fs::compact_partition_reader(
                BufReader::new(clone),
                part_offset,
                part.partition_type_byte,
                part.partition_type_string.as_deref(),
            )
            .ok_or_else(|| anyhow::anyhow!("compaction failed for {part_label}"))?;

            // Trim the stream to stream_size.  For layout-preserving readers this
            // drops the zero-filled free tail; for packed readers stream_size equals
            // the natural end of the stream so take() is a no-op.
            let mut limited = compact_reader.take(stream_size);

            log(
                &progress,
                LogLevel::Info,
                format!("Calling compress_partition for compacted {}", part_label),
            );
            let progress_log = Arc::clone(&progress);
            crate::rbformats::compress_partition_hashed(
                &mut limited,
                &output_base,
                effective_compression,
                stream_size,
                split_bytes,
                false, // compacted image has no wasted space, don't skip zeros
                config.chd_options.clone(),
                output_hasher.clone(),
                &mut |bytes_read| {
                    set_progress_bytes(
                        &progress_clone,
                        base_bytes + bytes_read,
                        total_stream_bytes,
                    );
                },
                &|| is_cancelled(&progress_clone),
                &mut |msg| log(&progress_log, LogLevel::Info, msg),
            )
            .with_context(|| format!("failed to compress {part_label}"))?
        } else {
            // Fall back to trim-based read
            log(
                &progress,
                LogLevel::Info,
                format!("Using trim-based read for {}", part_label),
            );
            let part_offset = part.byte_offset();
            log(
                &progress,
                LogLevel::Info,
                format!("Seeking to offset {} for {}", part_offset, part_label),
            );
            source.seek(SeekFrom::Start(part_offset))?;
            log(
                &progress,
                LogLevel::Info,
                format!("Creating limited reader, image_size={}", image_size),
            );
            let part_reader = (&mut source).take(image_size);
            let mut limited = LimitedReader::new(part_reader);

            log(
                &progress,
                LogLevel::Info,
                format!("Calling compress_partition for trim-based {}", part_label),
            );
            let progress_log = Arc::clone(&progress);
            crate::rbformats::compress_partition_hashed(
                &mut limited,
                &output_base,
                effective_compression,
                image_size,
                split_bytes,
                !config.sector_by_sector,
                config.chd_options.clone(),
                output_hasher.clone(),
                &mut |bytes_read| {
                    set_progress_bytes(
                        &progress_clone,
                        base_bytes + bytes_read,
                        total_stream_bytes,
                    );
                },
                &|| is_cancelled(&progress_clone),
                &mut |msg| log(&progress_log, LogLevel::Info, msg),
            )
            .with_context(|| format!("failed to compress {part_label}"))?
        };

        // Log output file sizes for diagnostics
        for file_name in &compressed_files {
            let file_path = backup_folder.join(file_name);
            match std::fs::metadata(&file_path) {
                Ok(meta) => log(
                    &progress,
                    LogLevel::Info,
                    format!("Output file {} size: {} bytes", file_name, meta.len()),
                ),
                Err(e) => log(
                    &progress,
                    LogLevel::Warning,
                    format!("Cannot stat output file {}: {}", file_name, e),
                ),
            }
        }

        overall_bytes_done += stream_size;
        set_progress_bytes(&progress, overall_bytes_done, total_stream_bytes);

        // Superfloppy: rename .raw to .img for universally compatible raw image
        let compressed_files = if is_superfloppy {
            let mut renamed = Vec::new();
            for file_name in &compressed_files {
                let new_name = file_name.replace(".raw", ".img");
                let old_path = backup_folder.join(file_name);
                let new_path = backup_folder.join(&new_name);
                if old_path != new_path {
                    std::fs::rename(&old_path, &new_path)
                        .with_context(|| format!("failed to rename {file_name} to {new_name}"))?;
                }
                renamed.push(new_name);
            }
            renamed
        } else {
            compressed_files
        };

        // Compute checksums for each output file. When the compressor
        // tee'd a hasher during write (single non-split output, format
        // supports it), finalise that hasher and reuse the digest —
        // skips a multi-GB re-read of the just-written file. Falls
        // back to the post-write hash pass for split outputs, CHD,
        // or any other case the compressor didn't honour the hasher.
        let tee_digest = output_hasher
            .as_ref()
            .map(verify::finalize_shared_hasher)
            .filter(|s| !s.is_empty());
        let mut all_checksums = Vec::new();
        for (i, file_name) in compressed_files.iter().enumerate() {
            let file_path = backup_folder.join(file_name);
            let checksum = match (&tee_digest, i, compressed_files.len()) {
                (Some(d), 0, 1) => d.clone(),
                _ => verify::compute_checksum(&file_path, config.checksum)
                    .with_context(|| format!("failed to checksum {file_name}"))?,
            };
            verify::write_checksum_file(&checksum, &file_path, config.checksum)
                .with_context(|| format!("failed to write checksum for {file_name}"))?;
            all_checksums.push(checksum);
        }

        let combined_checksum = if all_checksums.len() == 1 {
            all_checksums[0].clone()
        } else {
            all_checksums.join(",")
        };

        log(
            &progress,
            LogLevel::Info,
            format!(
                "{}: {} file(s), checksum: {}",
                part_label,
                compressed_files.len(),
                &combined_checksum[..combined_checksum.len().min(16)]
            ),
        );

        let minimum_size = minimum_sizes[part_idx].filter(|&s| s < part.size_bytes);
        let defragmented_min = defragmented_min_sizes[part_idx]
            .filter(|&s| s < part.size_bytes)
            .filter(|&d| match minimum_size {
                Some(m) => d < m,
                None => true,
            });
        // Probe the HFS+/HFSX signature for HFS+-shaped partitions so
        // restore can warn on case-sensitivity mismatches (Step 20).
        // `probe_hfsplus_signature` returns None for non-HFS+ volumes.
        let hfsplus_signature = factory.open().ok().and_then(|c| {
            let mut br = BufReader::new(c);
            fs::probe_hfsplus_signature(&mut br, part.start_lba * 512)
        });
        partition_metadata.push(PartitionMetadata {
            index: part.index,
            type_name: part.type_name.clone(),
            partition_type_byte: part.partition_type_byte,
            start_lba: part.start_lba,
            // Persist the true byte offset only when it isn't the floored
            // `start_lba * 512` (i.e. non-512-aligned X68000 SASI partitions);
            // otherwise leave it None so 512-aligned metadata is unchanged.
            start_byte: part.start_byte.filter(|&b| b != part.start_lba * 512),
            original_size_bytes: part.size_bytes,
            imaged_size_bytes: image_size,
            compressed_files,
            checksum: combined_checksum,
            resized: false,
            compacted: is_compacted,
            is_logical: part.is_logical,
            partition_type_string: part.partition_type_string.clone(),
            minimum_size_bytes: minimum_size,
            defragmented_min_size_bytes: defragmented_min,
            hfsplus_signature,
            defragmented_clone: clone_target.is_some(),
        });
    }

    if is_cancelled(&progress) {
        bail!("backup cancelled");
    }

    // Capture extended container info for MBR tables
    let extended_container = partitions
        .iter()
        .find(|p| p.is_extended_container)
        .map(|p| ExtendedContainerMetadata {
            mbr_index: p.index,
            partition_type_byte: p.partition_type_byte,
            start_lba: p.start_lba,
            size_bytes: p.size_bytes,
        });

    // Step 5: Write metadata.json
    set_operation(&progress, "Writing metadata...");
    let metadata = BackupMetadata {
        version: 1,
        created: Utc::now().to_rfc3339(),
        source_device: source_display,
        source_size_bytes: source_size,
        partition_table_type: table.type_name().to_string(),
        checksum_type: config.checksum.as_str().to_string(),
        compression_type: if is_superfloppy {
            CompressionType::None.as_str().to_string()
        } else {
            config.compression.as_str().to_string()
        },
        split_size_mib: config.split_size_mib,
        sector_by_sector: config.sector_by_sector,
        layout: BackupLayout::PerPartition,
        container: None,
        container_logical_size: None,
        container_sha1: None,
        size_policy: config.size_policy,
        alignment: AlignmentMetadata {
            detected_type: format!("{}", alignment.alignment_type),
            first_partition_lba: alignment.first_lba,
            alignment_sectors: alignment.alignment_sectors,
            heads: alignment.heads,
            sectors_per_track: alignment.sectors_per_track,
        },
        partitions: partition_metadata,
        bad_sectors: vec![],
        extended_container,
    };

    let metadata_path = backup_folder.join("metadata.json");
    let metadata_file = File::create(&metadata_path)
        .with_context(|| format!("failed to create {}", metadata_path.display()))?;
    serde_json::to_writer_pretty(metadata_file, &metadata)
        .context("failed to write metadata.json")?;

    log(
        &progress,
        LogLevel::Info,
        format!("Backup complete: {}", backup_folder.display()),
    );

    // Step 6: Mark finished
    if let Ok(mut p) = progress.lock() {
        p.finished = true;
        p.operation = "Backup complete".to_string();
    }

    Ok(())
}

/// Single-file CHD backup path. Synthesises a whole-disk image into one
/// CHD container (chdman/MAME compatible) and writes a `single-file-chd`
/// `metadata.json` describing the byte ranges inside it. Always routes
/// through `single_file_chd::run_via_staging`, which stages each
/// partition's bytes as zstd into a temp dir, then folds the staging
/// files into the final CHD without a whole-disk scratch image.
#[allow(clippy::too_many_arguments)] // intentional — pulls run_backup's locals
#[cfg(feature = "chd")]
fn run_single_file_chd_path(
    config: &BackupConfig,
    progress: &Arc<Mutex<BackupProgress>>,
    source: &BufReader<File>,
    source_size: u64,
    mbr_bytes: &[u8; 512],
    table: &PartitionTable,
    partitions: &[partition::PartitionInfo],
    alignment: &partition::PartitionAlignment,
    backup_folder: &std::path::Path,
    minimum_sizes: &[Option<u64>],
    defragmented_min_sizes: &[Option<u64>],
    clone_target_sizes: &[Option<u64>],
    clone_shapes: &[Option<fs::DefragCloneShape>],
) -> Result<()> {
    set_operation(progress, "Building disk-image stream...");
    log(
        progress,
        LogLevel::Info,
        format!(
            "Single-file CHD layout selected ({} disk, {} bytes)",
            table.type_name(),
            source_size,
        ),
    );

    let requested_policy = config.size_policy.unwrap_or(SizePolicy::Original);

    // Output base for compress_chd (the .chd extension is appended by the
    // compressor). Use the backup folder's name so the resulting file is
    // `<backup_name>/<backup_name>.chd`. Falls back to the literal `disk`
    // when the path has no final component (shouldn't happen in practice
    // — backup_folder is always created with a name).
    let output_stem = backup_folder
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("disk");
    let output_base = backup_folder.join(output_stem);

    // Merge user-supplied per-partition target sizes with HFS+ defrag-clone
    // targets derived from `shrink_to_minimum`. User-supplied entries take
    // precedence on conflict; clone targets fill in HFS+/HFSX partitions that
    // the user left at "Original". `hfsplus_clone_targets` flags which of the
    // resulting resize entries should be produced via
    // `stream_defragmented_hfsplus` instead of in-place resize.
    let mut merged_resize: Vec<(usize, u64)> =
        config.partition_target_sizes.clone().unwrap_or_default();
    let mut hfsplus_clone_targets: Vec<(usize, u64, fs::DefragCloneShape)> = Vec::new();
    for (i, part) in partitions.iter().enumerate() {
        if let Some(target) = clone_target_sizes[i] {
            let already_set = merged_resize.iter().any(|(idx, _)| *idx == part.index);
            if !already_set {
                merged_resize.push((part.index, target));
            }
            // Even when the user picked a custom size, record the clone
            // target so the staging path knows this partition wants a
            // clone stream. If user_size != clone_target the user wins
            // for the resize plan but we still need to clone — the
            // cloned HFS+ volume will be sized to the entry in
            // `merged_resize`.
            let shape = clone_shapes[i].unwrap_or(fs::DefragCloneShape::Flat);
            hfsplus_clone_targets.push((part.index, target, shape));
        }
    }
    let resize_targets_slice: Option<&[(usize, u64)]> = if merged_resize.is_empty() {
        None
    } else {
        Some(merged_resize.as_slice())
    };
    let clone_targets_slice: Option<&[(usize, u64, fs::DefragCloneShape)]> =
        if hfsplus_clone_targets.is_empty() {
            None
        } else {
            Some(hfsplus_clone_targets.as_slice())
        };

    let inputs = single_file_chd::SingleFileChdInputs {
        source_file: source.get_ref(),
        source_size,
        source_partition_table_bytes: mbr_bytes,
        partition_table: table,
        partitions,
        partition_filter: config.partition_filter.as_deref(),
        sector_by_sector: config.sector_by_sector,
        chd_options: config.chd_options.clone(),
        is_dvd: matches!(config.compression, CompressionType::Dvd),
        output_base: &output_base,
        resize_targets: resize_targets_slice,
        hfsplus_clone_targets: clone_targets_slice,
        alignment_sectors: alignment.alignment_sectors,
        checksum_type: config.checksum,
    };

    // Compute the shrunken-envelope total BEFORE bytes flow so the GUI
    // progress bar reads "0 / 53 GiB" (not "0 / 58 GiB") on resize-active
    // backups. `build_resize_plans` is a pure-arithmetic plan derived from
    // the already-merged resize targets; it doesn't touch the device.
    let envelope_total = match single_file_chd::build_resize_plans(&inputs) {
        Ok(Some(plans)) => single_file_chd::compute_resized_envelope(
            &plans,
            alignment.alignment_sectors,
            source_size,
            table,
        ),
        _ => source_size,
    };
    {
        let mut p = progress.lock().expect("backup progress mutex poisoned");
        p.total_bytes = envelope_total;
        p.full_size_bytes = envelope_total;
        p.current_bytes = 0;
    }

    let progress_for_cb = progress.clone();
    let progress_for_cancel = progress.clone();
    let progress_for_log = progress.clone();
    let progress_for_checksum = progress.clone();
    let mut progress_cb = move |n: u64| {
        if let Ok(mut p) = progress_for_cb.lock() {
            p.current_bytes = n;
        }
    };
    let cancel_check = move || {
        progress_for_cancel
            .lock()
            .map(|p| p.cancel_requested)
            .unwrap_or(false)
    };
    let mut log_cb = move |s: &str| {
        if let Ok(mut p) = progress_for_log.lock() {
            p.log_messages.push_back(LogMessage {
                level: LogLevel::Info,
                message: s.to_string(),
            });
        }
    };
    // Post-write checksum phase: swap the progress bar over to the
    // hashing total so the GUI doesn't sit at "100% / 100%" while we
    // re-read the CHD. The first invocation (current=0, total=N) flips
    // the operation label too.
    let mut first_checksum_tick = true;
    let progress_for_checksum_outer = progress_for_checksum;
    let mut checksum_phase_cb = move |current: u64, total: u64| {
        if let Ok(mut p) = progress_for_checksum_outer.lock() {
            if first_checksum_tick {
                p.operation = "Computing per-partition checksums...".to_string();
                first_checksum_tick = false;
            }
            p.current_bytes = current;
            p.total_bytes = total;
        }
    };

    // Install a thread-local progress sink so the HFS+ defrag-clone
    // pipeline can pipe its [defrag-build] tick messages into the GUI
    // log queue (in addition to stderr). Cleared after
    // `single_file_chd::run_via_staging` returns. The sink closure owns
    // its own `Arc<Mutex<BackupProgress>>` clone so it stays 'static +
    // Send (a requirement for the thread-local Box-erased fn type).
    let progress_for_sink = progress.clone();
    fs::hfsplus_defrag::set_progress_sink(Some(Box::new(move |msg: &str| {
        if let Ok(mut p) = progress_for_sink.lock() {
            p.log_messages.push_back(LogMessage {
                level: LogLevel::Info,
                message: msg.to_string(),
            });
        }
    })));

    // Route to the new two-phase staging path when its gates accept the
    // current inputs (MBR/GPT, no HFS+ clone targets, no FS-level
    // resize). Otherwise fall back to the legacy direct-stream /
    // scratch-tempfile path. The router lives here rather than inside
    // `single_file_chd` so the log line surfaces in the GUI before any
    // bytes flow.
    // `phase_cb` lets the staging path swap the top-bar operation
    // label as each stage starts ("Staging partition-N", "Assembling
    // CHD", ...) — without it the user would see "Building disk-image
    // stream..." for the entire multi-hour run.
    let progress_for_phase = progress.clone();
    let mut phase_cb = move |label: &str| {
        if let Ok(mut p) = progress_for_phase.lock() {
            p.operation = label.to_string();
        }
    };
    // Send-able sink for HFS+ [defrag-build] ticks: the producer
    // thread spawned by the staging path can clone this Arc and
    // install it as its own thread-local sink, so catalog-walk
    // progress reaches the GUI log even though the work runs on a
    // worker thread.
    let progress_for_defrag = progress.clone();
    let defrag_log_sink: std::sync::Arc<dyn Fn(&str) + Send + Sync> =
        std::sync::Arc::new(move |msg: &str| {
            if let Ok(mut p) = progress_for_defrag.lock() {
                p.log_messages.push_back(LogMessage {
                    level: LogLevel::Info,
                    message: msg.to_string(),
                });
            }
        });
    let result = single_file_chd::run_via_staging(
        inputs,
        &mut progress_cb,
        &cancel_check,
        &mut log_cb,
        &mut checksum_phase_cb,
        &mut phase_cb,
        Some(defrag_log_sink),
    );

    fs::hfsplus_defrag::set_progress_sink(None);
    let result = result?;

    // Build per-partition metadata. The byte range inside the CHD lives in
    // `result.partition_ranges`; we cross-reference each partition's
    // PartitionInfo for the type-name + alignment data.
    set_operation(progress, "Writing metadata...");
    let partition_metadata: Vec<PartitionMetadata> = result
        .partition_ranges
        .iter()
        .filter_map(|range| {
            let (part_idx, part) = partitions
                .iter()
                .enumerate()
                .find(|(_, p)| p.index == range.partition_index)?;
            // `start_lba` records where the partition lives inside the CHD's
            // synthesised disk image — that's the new offset when the
            // backup-time resize plan moved it, otherwise the source LBA.
            let new_start_lba = range.offset_in_disk / 512;
            // Pull the per-partition minimum sizes computed by
            // `analyze_partitions` upstream. Restore-resize uses these to
            // populate the "Minimum" picker and to validate Custom shrinks.
            // After a backup-time resize the partition has already been
            // shrunk to ~minimum, so the stored minimum should not exceed
            // the new imaged size.
            let minimum_size = minimum_sizes
                .get(part_idx)
                .copied()
                .flatten()
                .map(|m| m.min(range.length));
            let defragmented_min = defragmented_min_sizes
                .get(part_idx)
                .copied()
                .flatten()
                .map(|m| m.min(range.length));
            let hfsplus_signature = source.get_ref().try_clone().ok().and_then(|c| {
                let mut br = BufReader::new(c);
                fs::probe_hfsplus_signature(&mut br, part.start_lba * 512)
            });
            Some(PartitionMetadata {
                index: range.partition_index,
                type_name: part.type_name.clone(),
                partition_type_byte: part.partition_type_byte,
                start_lba: new_start_lba,
                // Single-file CHD relocates partitions and is not used for
                // X68000 sources (the only non-512-aligned scheme), so the
                // floored 512-LBA offset is authoritative here.
                start_byte: None,
                original_size_bytes: part.size_bytes,
                imaged_size_bytes: range.length,
                compressed_files: vec![], // data is inside the container
                checksum: range.checksum.clone(),
                resized: range.length != part.size_bytes || new_start_lba != part.start_lba,
                compacted: !config.sector_by_sector,
                is_logical: part.is_logical,
                partition_type_string: part.partition_type_string.clone(),
                minimum_size_bytes: minimum_size,
                defragmented_min_size_bytes: defragmented_min,
                hfsplus_signature,
                defragmented_clone: false,
            })
        })
        .collect();

    let metadata = BackupMetadata {
        version: 1,
        created: Utc::now().to_rfc3339(),
        source_device: config.source_path.display().to_string(),
        source_size_bytes: source_size,
        partition_table_type: table.type_name().to_string(),
        checksum_type: config.checksum.as_str().to_string(),
        compression_type: config.compression.as_str().to_string(),
        split_size_mib: None, // splitting is rejected on this path
        sector_by_sector: config.sector_by_sector,
        layout: BackupLayout::SingleFileChd,
        container: Some(result.container_filename.clone()),
        container_logical_size: Some(result.container_logical_size),
        container_sha1: Some(result.container_sha1.clone()),
        size_policy: Some(requested_policy),
        alignment: AlignmentMetadata {
            detected_type: format!("{}", alignment.alignment_type),
            first_partition_lba: alignment.first_lba,
            alignment_sectors: alignment.alignment_sectors,
            heads: alignment.heads,
            sectors_per_track: alignment.sectors_per_track,
        },
        partitions: partition_metadata,
        bad_sectors: vec![],
        extended_container: None,
    };

    let metadata_path = backup_folder.join("metadata.json");
    let metadata_file = File::create(&metadata_path)
        .with_context(|| format!("failed to create {}", metadata_path.display()))?;
    serde_json::to_writer_pretty(metadata_file, &metadata)
        .context("failed to write metadata.json")?;

    log(
        progress,
        LogLevel::Info,
        format!(
            "Single-file CHD backup complete: {} ({}, SHA1 {})",
            backup_folder.display(),
            result.container_filename,
            result.container_sha1,
        ),
    );

    if let Ok(mut p) = progress.lock() {
        p.finished = true;
        p.operation = "Backup complete".to_string();
    }

    Ok(())
}

/// A reader wrapper that limits reads to exactly `limit` bytes.
struct LimitedReader<R> {
    inner: R,
}

impl<R: Read> LimitedReader<R> {
    fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R: Read> Read for LimitedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

/// Bounded in-memory pipe used to feed a `Write`-driven producer (e.g.
/// [`fs::stream_defragmented_hfsplus`]) into a `Read`-consuming sink (e.g.
/// `compress_partition`). Each `write` call enqueues one owned chunk; the
/// reader serves bytes from a single pending chunk before pulling the next.
/// `sync_channel(capacity=4)` bounds peak memory to a few hundred KiB at
/// the chunk sizes the defrag emitter produces, with the producer blocked
/// on backpressure when the consumer falls behind.
pub(crate) fn channel_pipe() -> (ChannelPipeWriter, ChannelPipeReader) {
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(4);
    (
        ChannelPipeWriter { tx: Some(tx) },
        ChannelPipeReader {
            rx,
            chunk: Vec::new(),
            pos: 0,
        },
    )
}

pub(crate) struct ChannelPipeWriter {
    tx: Option<std::sync::mpsc::SyncSender<Vec<u8>>>,
}

impl std::io::Write for ChannelPipeWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let tx = self.tx.as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer already closed")
        })?;
        tx.send(buf.to_vec()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "consumer dropped pipe reader",
            )
        })?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for ChannelPipeWriter {
    fn drop(&mut self) {
        // Closing the sender wakes the reader, which then sees EOF.
        self.tx = None;
    }
}

pub(crate) struct ChannelPipeReader {
    rx: std::sync::mpsc::Receiver<Vec<u8>>,
    chunk: Vec<u8>,
    pos: usize,
}

impl Read for ChannelPipeReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.chunk.len() {
            match self.rx.recv() {
                Ok(next) => {
                    self.chunk = next;
                    self.pos = 0;
                }
                Err(_) => return Ok(0), // sender dropped — clean EOF
            }
        }
        let avail = self.chunk.len() - self.pos;
        let n = avail.min(buf.len());
        buf[..n].copy_from_slice(&self.chunk[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

#[cfg(test)]
mod pipe_tests {
    use super::{channel_pipe, ChannelPipeReader, ChannelPipeWriter};
    use std::io::{Read, Write};

    #[test]
    fn channel_pipe_round_trips_in_order() {
        let (mut w, mut r) = channel_pipe();
        let producer = std::thread::spawn(move || -> std::io::Result<()> {
            for i in 0u8..16 {
                let chunk = vec![i; 1024];
                w.write_all(&chunk)?;
            }
            Ok(())
        });
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        producer.join().unwrap().unwrap();
        assert_eq!(got.len(), 16 * 1024);
        for i in 0u8..16 {
            let off = i as usize * 1024;
            assert!(got[off..off + 1024].iter().all(|&b| b == i));
        }
    }

    #[test]
    fn channel_pipe_reader_eof_on_writer_drop() {
        let (w, mut r) = channel_pipe();
        drop(w);
        let mut buf = [0u8; 8];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(n, 0, "expected immediate EOF when writer dropped");
    }

    fn _assert_send_for_pipe_ends() {
        fn assert_send<T: Send>() {}
        assert_send::<ChannelPipeWriter>();
        assert_send::<ChannelPipeReader>();
    }
}
