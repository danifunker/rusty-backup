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

use crate::clonezilla::block_cache::{PartcloneBlockCache, PartcloneBlockReader};
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
