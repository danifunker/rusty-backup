//! Browse session: the parameters that locate a filesystem to browse, plus
//! the read-only and editable openers built on top of them.
//!
//! The GUI's filesystem browser opens a fresh `Filesystem` (or
//! `EditableFilesystem`) instance for nearly every operation — see the
//! `MEMORY.md` note about *not* caching state on the filesystem struct. That
//! leaves a lot of "where to open from" parameters that have to travel
//! together: the source path or pre-opened device handle, the partition
//! offset/type, plus the optional partclone / seekable-zstd block caches that
//! short-circuit reading from disk. `BrowseSession` bundles those parameters,
//! exposes [`open`](BrowseSession::open) / [`open_editable`](BrowseSession::open_editable),
//! and is `Clone` so background workers (extraction, cache builds) can be
//! handed a session value rather than a long-argument-list builder call.
//!
//! Extracted from `gui/browse_view.rs` per §5 of `docs/codecleanup.md`.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read as _};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::clonezilla::block_cache::{PartcloneBlockCache, PartcloneBlockReader};
use crate::fs::entry::FileEntry;
use crate::fs::filesystem::{EditableFilesystem, Filesystem, FilesystemError};
use crate::fs::zstd_stream::{ZstdStreamCache, ZstdStreamReader};
use crate::fs::{self};
use crate::rbformats::chd::ChdReader;
use crate::rbformats::chd_edit::{ChdEditHandle, ChdEditSession};

/// Parameters and caches that locate a filesystem to browse.
#[derive(Default, Clone)]
pub struct BrowseSession {
    /// Path to the backing image, device, or seekable-zstd cache file.
    pub source_path: Option<PathBuf>,
    /// Byte offset into the source where this partition starts.
    pub partition_offset: u64,
    /// Partition type byte (MBR-style).
    pub partition_type: u8,
    /// Partition type string for table types that carry one (APM, GPT GUID).
    pub partition_type_string: Option<String>,
    /// An already-opened (possibly elevated) device handle. When present,
    /// every `open()` clones a fresh fd from it instead of opening
    /// `source_path`. Used on macOS where raw disk access is gated through
    /// a privileged helper.
    pub preopen_file: Option<Arc<File>>,
    /// Clonezilla partclone block cache. When present, the filesystem reads
    /// through the cache instead of touching `source_path`.
    pub partclone_cache: Option<Arc<Mutex<PartcloneBlockCache>>>,
    /// Streaming zstd cache for native zstd backups.
    pub zstd_cache: Option<Arc<Mutex<ZstdStreamCache>>>,
    /// Live CHD edit session. When set, every `open` and `open_editable`
    /// call clones this handle as the backing reader instead of touching
    /// `source_path` — the GUI's CHD edit-mode flow installs it for the
    /// duration of editing and clears it on apply/discard.
    pub chd_edit_session: Option<Arc<Mutex<ChdEditSession>>>,
    /// Password for encrypted containers (currently IMZ only). When set,
    /// `open()` passes it to the decryption API. The GUI sets this after
    /// prompting the user.
    pub password: Option<String>,
}

/// True when the session is opening an HFS+ (or HFSX, or APM Apple_HFS that
/// may resolve to HFS+) partition. Used to selectively disable F_NOCACHE on
/// macOS so the catalog B-tree reads can hit the buffer cache.
#[cfg(target_os = "macos")]
fn is_hfs_plus_partition(partition_type: u8, partition_type_string: Option<&str>) -> bool {
    if let Some(s) = partition_type_string {
        if matches!(s, "Apple_HFS" | "Apple_HFSX" | "Apple_HFS+") {
            return true;
        }
    }
    partition_type == 0xAF
}

impl BrowseSession {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a read-only filesystem from this session.
    pub fn open(&self) -> Result<Box<dyn Filesystem>, FilesystemError> {
        // Live CHD edit session — read-through preserves any unflushed
        // writes the editor has staged in memory.
        if let Some(arc) = &self.chd_edit_session {
            let handle = ChdEditHandle::from_arc(Arc::clone(arc));
            return fs::open_filesystem(
                handle,
                self.partition_offset,
                self.partition_type,
                self.partition_type_string.as_deref(),
            );
        }

        // Partclone cache short-circuits all other paths.
        if let Some(cache) = &self.partclone_cache {
            let reader = PartcloneBlockReader::new(Arc::clone(cache));
            return fs::open_filesystem(reader, 0, self.partition_type, None);
        }

        // Streaming zstd cache.
        if let Some(cache) = &self.zstd_cache {
            let reader = ZstdStreamReader::new(Arc::clone(cache));
            return fs::open_filesystem(
                reader,
                self.partition_offset,
                self.partition_type,
                self.partition_type_string.as_deref(),
            );
        }

        // Pre-opened device handle (already elevated, e.g. macOS raw disk).
        // try_clone() gives an independent fd so each open() call can seek
        // freely. Wrapped in SectorAlignedReader because macOS raw character
        // devices (/dev/rdiskN) require sector-aligned seeks/reads.
        if let Some(arc) = &self.preopen_file {
            let file = arc.try_clone().map_err(FilesystemError::Io)?;
            #[cfg(target_os = "macos")]
            {
                if is_hfs_plus_partition(self.partition_type, self.partition_type_string.as_deref())
                {
                    if let Err(e) = crate::os::macos::clear_nocache(&file) {
                        log::debug!("clear_nocache failed (ignored): {e}");
                    } else {
                        log::debug!(
                            "[HFS+ open] cleared F_NOCACHE on inspect fd to enable buffer cache"
                        );
                    }
                }
            }
            let reader = crate::os::SectorAlignedReader::new(file);
            return fs::open_filesystem(
                reader,
                self.partition_offset,
                self.partition_type,
                self.partition_type_string.as_deref(),
            );
        }

        let path = self
            .source_path
            .as_ref()
            .ok_or_else(|| FilesystemError::Parse("no source path set".into()))?;

        // Sniff first 8 bytes to detect content-addressable formats regardless
        // of file extension (users often have mislabeled files).
        let mut magic = [0u8; 8];
        if let Ok(mut f) = File::open(path) {
            let _ = f.read(&mut magic);
        }

        // CHD — 8-byte magic "MComprHD" at offset 0.
        if &magic == b"MComprHD" {
            let chd_reader = ChdReader::open(path)
                .map_err(|e| FilesystemError::Parse(format!("failed to open CHD: {e}")))?;
            return fs::open_filesystem(
                chd_reader,
                self.partition_offset,
                self.partition_type,
                self.partition_type_string.as_deref(),
            );
        }

        // GHO/GHS — 2-byte magic FE EF. GhoReader handles both
        // SECTOR-mode (streaming block cache) and file-aware (in-RAM
        // virtual FAT image).
        if magic[0] == 0xFE && magic[1] == 0xEF {
            let gho_reader = crate::rbformats::gho::GhoReader::open(path)
                .map_err(|e| FilesystemError::Parse(format!("failed to open GHO: {e:#}")))?;
            return fs::open_filesystem(
                gho_reader,
                self.partition_offset,
                self.partition_type,
                self.partition_type_string.as_deref(),
            );
        }

        // IMZ — 4-byte magic "PK\x03\x04" (ZIP local file header).
        if &magic[..4] == b"PK\x03\x04" {
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            if ext.eq_ignore_ascii_case("imz") {
                let pw = self.password.as_deref().map(|s| s.as_bytes());
                let imz_reader = crate::rbformats::imz::ImzReader::open_with_password(path, pw)
                    .map_err(|e| FilesystemError::Parse(format!("failed to open IMZ: {e:#}")))?;
                return fs::open_filesystem(
                    imz_reader,
                    self.partition_offset,
                    self.partition_type,
                    self.partition_type_string.as_deref(),
                );
            }
        }

        // Seekable zstd cache files — keep extension-based detection since
        // there is no reliable content-level signal distinguishing them from
        // other zstd files.
        let is_seekable_zst = path.extension().map(|e| e == "zst").unwrap_or(false)
            && path
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.ends_with(".seekable"))
                .unwrap_or(false);

        if is_seekable_zst {
            let file = File::open(path).map_err(FilesystemError::Io)?;
            let decoder = match zeekstd::Decoder::new(file) {
                Ok(d) => d,
                Err(e) => {
                    let _ = std::fs::remove_file(path);
                    return Err(FilesystemError::Parse(format!(
                        "stale seekable zstd cache removed ({e}). Click Browse again to rebuild."
                    )));
                }
            };
            return fs::open_filesystem(
                decoder,
                self.partition_offset,
                self.partition_type,
                self.partition_type_string.as_deref(),
            );
        }

        // For all other formats (raw images, VHD, 2MG, DiskCopy 4.2,
        // DOS-order, etc.), use the unified format detection pipeline so
        // container wrappers are peeled off.
        let file = File::open(path).map_err(FilesystemError::Io)?;
        match crate::rbformats::detect_image_format_with_path(file, Some(path)) {
            Ok(format) if !matches!(format, crate::rbformats::ImageFormat::Raw) => {
                let file2 = File::open(path).map_err(FilesystemError::Io)?;
                let (reader, _size) = crate::rbformats::wrap_image_reader(file2, format)
                    .map_err(|e| FilesystemError::Parse(format!("failed to unwrap image: {e}")))?;
                // Container formats present unwrapped data starting at offset
                // 0; use partition_offset=0 for superfloppies.
                let effective_offset = if self.partition_type == 0 {
                    0
                } else {
                    self.partition_offset
                };
                fs::open_filesystem(
                    reader,
                    effective_offset,
                    self.partition_type,
                    self.partition_type_string.as_deref(),
                )
            }
            _ => {
                let file = File::open(path).map_err(FilesystemError::Io)?;
                let reader = BufReader::new(file);
                fs::open_filesystem(
                    reader,
                    self.partition_offset,
                    self.partition_type,
                    self.partition_type_string.as_deref(),
                )
            }
        }
    }

    /// Spawn a worker that opens the filesystem and reads the root directory
    /// listing on a background thread, so the GUI can keep painting (with a
    /// spinner / phase label) while a slow open is in progress.
    ///
    /// Volumes with very large catalogs — HFS+ on a heavily-used volume can
    /// have a 200+ MB catalog file — take seconds to load over a slow source
    /// (SD-card raw I/O, network mounts). Doing it inline froze the UI; this
    /// hands the work to a thread and surfaces phase strings via the shared
    /// status struct.
    pub fn spawn_open(&self) -> Arc<Mutex<BrowseOpenStatus>> {
        let session = self.clone();
        let status = Arc::new(Mutex::new(BrowseOpenStatus::starting()));
        let status_thread = Arc::clone(&status);
        thread::spawn(move || {
            let set_phase = |s: &str| {
                if let Ok(mut g) = status_thread.lock() {
                    g.phase = s.to_string();
                }
            };

            set_phase("Opening filesystem...");
            let mut fs = match session.open() {
                Ok(fs) => fs,
                Err(e) => {
                    let msg = format!("{e}");
                    let is_pw = msg.to_lowercase().contains("password-protected");
                    if let Ok(mut g) = status_thread.lock() {
                        g.error = Some(format!("Cannot open filesystem: {e}"));
                        g.needs_password = is_pw;
                        g.finished = true;
                    }
                    return;
                }
            };

            let fs_type = fs.fs_type().to_string();
            let volume_label = fs.volume_label().unwrap_or("").to_string();
            let blessed_folder = fs.blessed_system_folder();

            set_phase("Reading root directory...");
            let (root, root_entries, list_err) = match fs.root() {
                Ok(root) => match fs.list_directory(&root) {
                    Ok(entries) => (Some(root), Some(entries), None),
                    Err(e) => (
                        Some(root),
                        None,
                        Some(format!("Failed to read root directory: {e}")),
                    ),
                },
                Err(e) => (None, None, Some(format!("Failed to get root: {e}"))),
            };

            if let Ok(mut g) = status_thread.lock() {
                g.phase = "Done".to_string();
                g.fs_type = fs_type;
                g.volume_label = volume_label;
                g.blessed_folder = blessed_folder;
                g.root = root;
                g.root_entries = root_entries;
                g.error = list_err;
                g.fs = Some(fs);
                g.finished = true;
            }
        });
        status
    }

    /// Open the filesystem read-write for editing operations.
    ///
    /// Unlike [`open`](Self::open) this requires a real `source_path` (or a
    /// live CHD edit session) — partclone / zstd / preopen paths are
    /// read-only.
    pub fn open_editable(&self) -> Result<Box<dyn EditableFilesystem>, FilesystemError> {
        // Live CHD edit session: writes go through the diff (compressed
        // parent) or in-place (uncompressed). No File handle to source_path
        // is opened.
        if let Some(arc) = &self.chd_edit_session {
            let handle = ChdEditHandle::from_arc(Arc::clone(arc));
            return fs::open_editable_filesystem(
                handle,
                self.partition_offset,
                self.partition_type,
                self.partition_type_string.as_deref(),
            );
        }

        let path = self
            .source_path
            .as_ref()
            .ok_or_else(|| FilesystemError::Parse("no source path set".into()))?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(FilesystemError::Io)?;

        fs::open_editable_filesystem(
            file,
            self.partition_offset,
            self.partition_type,
            self.partition_type_string.as_deref(),
        )
    }
}

/// Shared state between the GUI and the [`BrowseSession::spawn_open`] worker.
///
/// The worker fills these fields as it makes progress: phase strings while it
/// runs, then the parsed metadata + the live `Filesystem` instance on
/// completion. The GUI clones the `Arc` and polls each frame until `finished`
/// flips true, then drains the fields into its own `BrowseView` state.
///
/// The `fs` slot lets the GUI keep one open `Filesystem` alive across reads
/// (per-file previews, directory listings) instead of re-opening — which on
/// HFS+ volumes with hundreds of thousands of files means re-reading the
/// entire 100+ MB catalog every time. Edits invalidate the cached instance
/// because they go through `open_editable` and write back to disk.
pub struct BrowseOpenStatus {
    pub phase: String,
    pub finished: bool,
    pub fs_type: String,
    pub volume_label: String,
    pub blessed_folder: Option<(u64, String)>,
    pub root: Option<FileEntry>,
    pub root_entries: Option<Vec<FileEntry>>,
    pub error: Option<String>,
    /// Set when the open fails because the container is password-protected.
    /// The GUI checks this to show a password prompt instead of a plain error.
    pub needs_password: bool,
    /// The opened filesystem itself, handed back to the UI thread so it can
    /// cache it. `None` if the open failed.
    pub fs: Option<Box<dyn Filesystem>>,
}

impl BrowseOpenStatus {
    fn starting() -> Self {
        Self {
            phase: "Starting...".to_string(),
            finished: false,
            fs_type: String::new(),
            volume_label: String::new(),
            blessed_folder: None,
            root: None,
            root_entries: None,
            error: None,
            needs_password: false,
            fs: None,
        }
    }
}
