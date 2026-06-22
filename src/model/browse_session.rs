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
    /// Remote image over the block tier: `(shared connection, remote path)`.
    /// When set, `open()` builds a fresh read-only `RemoteBlockReader` on the
    /// connection and `open_editable()` a read-write one (the daemon keeps the
    /// image open and serves / patches byte ranges).
    #[cfg(feature = "remote")]
    pub remote: Option<(Arc<Mutex<crate::remote::RemoteConnection>>, String)>,
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
        // Remote image over the block tier — a fresh RemoteBlockReader per open
        // (the daemon keeps the image open and serves byte ranges).
        #[cfg(feature = "remote")]
        if let Some((conn, rpath)) = &self.remote {
            let reader = crate::remote::RemoteBlockReader::open(Arc::clone(conn), rpath)
                .map_err(|e| FilesystemError::Io(std::io::Error::other(e.to_string())))?;
            return fs::open_filesystem(
                reader,
                self.partition_offset,
                self.partition_type,
                self.partition_type_string.as_deref(),
            );
        }

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

        // A gzip-wrapped Alto/Pilot pack (`.pdi.gz`, gzipped CopyDisk) shows gzip
        // magic on the raw file. Sniff the *decompressed* prefix so the Alto
        // branch below sees the real container magic; the pack itself is small,
        // so it's decompressed in full only when that magic matches (further
        // down). Flat gzip images (`.adz`/`.hdz`) keep streaming through the
        // dedicated gzip branch later in this function.
        let is_gzip = magic[0] == 0x1f && magic[1] == 0x8b;
        let probe_magic: [u8; 8] = if is_gzip {
            crate::model::source_reader::gunzip_prefix(path, 8)
                .ok()
                .and_then(|b| <[u8; 8]>::try_from(b).ok())
                .unwrap_or(magic)
        } else {
            magic
        };

        // Xerox Alto disk packs (PARC Disk Image, or a CopyDisk stream). These
        // are label-bearing disks that can't be represented as a flat sector
        // stream the way every other source here can, so they bypass
        // open_filesystem: read the whole pack, decode it into an in-memory
        // Disk, and hand back a BFS filesystem view. PDI starts with the magic
        // "PARCDISK"; a CopyDisk stream opens with bytes 00 07 00 03 00 0a
        // (params block: length=7, type=3, diskType=10 AltoDiablo).
        // Salto cooked `.dsk` images carry no magic; recognize them by the exact
        // Diablo-31 image size (open_pack does the real validation).
        let alto_size = std::fs::metadata(path).map(|m| m.len() as usize).ok();
        let is_salto_dsk = alto_size == Some(crate::fs::alto::salto::IMAGE_BYTES);
        // Trident (TFS) pack image: recognized by the exact T-80 / T-300 size.
        let is_trident = matches!(
            alto_size,
            Some(crate::fs::alto::trident::T80_BYTES) | Some(crate::fs::alto::trident::T300_BYTES)
        );
        // Dwarf Draco 6085 ".zdisk"/".zdelta" is a zlib stream (a Pilot pack);
        // gate the full read on the zlib magic byte, then confirm by inflating
        // the prefix to the DAAD signature so we don't slurp unrelated files.
        let zdisk_bytes = if magic[0] == 0x78 {
            std::fs::read(path)
                .ok()
                .filter(|b| crate::fs::alto::zdisk::is_zdisk(b))
        } else {
            None
        };
        if &probe_magic == b"PARCDISK"
            || probe_magic[0..6] == [0x00, 0x07, 0x00, 0x03, 0x00, 0x0a]
            || is_salto_dsk
            || is_trident
            || zdisk_bytes.is_some()
        {
            let bytes = match zdisk_bytes {
                Some(b) => b,
                None if is_gzip => {
                    crate::model::source_reader::gunzip_to_vec(path).map_err(FilesystemError::Io)?
                }
                None => std::fs::read(path).map_err(FilesystemError::Io)?,
            };
            let disk = crate::fs::alto::open_pack(&bytes)?;
            // A PDI with fsFamily=2 (or any .zdisk) is a Pilot/Cedar volume
            // (structurally unrelated to BFS); everything else is an Alto BFS pack.
            if disk.geometry.family == crate::fs::alto::FsFamily::Pilot {
                use crate::fs::alto::pilot::{Generation, PilotFilesystem};
                // File-ID generation comes from PDI flags bit 2; a 6085 .zdisk
                // carries no flags, so default to the original-Pilot generation
                // (these are classic Pilot 12.3 volumes). Generation does not
                // affect the read path, only how a written ID is interpreted.
                let generation = crate::fs::alto::pdi::read_header(&bytes)
                    .map(|h| Generation::from_pdi_flag_bit2(h.flags & 0x0004 != 0))
                    .unwrap_or(Generation::OriginalPilot);
                return Ok(Box::new(PilotFilesystem::open(disk, generation)?));
            }
            return Ok(Box::new(crate::fs::alto::bfs::BfsFilesystem::open(disk)));
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
            let pw = self.password.as_deref().map(|s| s.as_bytes());
            let gho_reader = crate::rbformats::gho::GhoReader::open_with_password(path, pw)
                .map_err(|e| FilesystemError::Parse(format!("failed to open GHO: {e:#}")))?;
            return fs::open_filesystem(
                gho_reader,
                self.partition_offset,
                self.partition_type,
                self.partition_type_string.as_deref(),
            );
        }

        // IMZ / .zip — 4-byte magic "PK\x03\x04" (ZIP local file header).
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
            } else if ext.eq_ignore_ascii_case("zip") {
                // A plain .zip holding a RAW disk image. open_read inflates the
                // chosen entry to a temp flat image; the inner image may be a
                // superfloppy (offset 0) or a partitioned disk, so honor
                // partition_offset exactly like a raw image. The GUI auto-picks
                // the disk entry (no `--inside`); an ambiguous archive surfaces
                // the multi-image error.
                let pw = self.password.as_deref().map(|s| s.as_bytes());
                let reader = crate::model::source_reader::open_read_with_password(path, pw)
                    .map_err(|e| {
                        FilesystemError::Parse(format!("failed to open ZIP disk: {e:#}"))
                    })?;
                let effective_offset = if self.partition_type == 0 {
                    0
                } else {
                    self.partition_offset
                };
                return fs::open_filesystem(
                    reader,
                    effective_offset,
                    self.partition_type,
                    self.partition_type_string.as_deref(),
                );
            }
        }

        // Gzip-wrapped Amiga images (.adz/.hdz) — magic 1f 8b. open_read peels
        // the gzip to a temp flat image; the inner image may be a superfloppy
        // (.adf, AFFS at offset 0) or RDB-partitioned (.hdf), so honor the
        // partition offset just like a raw image (offset 0 for a superfloppy).
        if magic[0] == 0x1f && magic[1] == 0x8b {
            let reader = crate::model::source_reader::open_read(path)
                .map_err(|e| FilesystemError::Parse(format!("failed to open gzip image: {e:#}")))?;
            let effective_offset = if self.partition_type == 0 {
                0
            } else {
                self.partition_offset
            };
            return fs::open_filesystem(
                reader,
                effective_offset,
                self.partition_type,
                self.partition_type_string.as_deref(),
            );
        }

        // Flat floppy containers (MSA / EDSK / D88 / DIM / XDF / HDM /
        // Arculator HDF / 140 KB Apple-II). open_read sniffs the format and
        // decodes it into an in-memory flat sector stream; the decoded image
        // is a superfloppy, so it opens at offset 0. CHD / GHO / IMZ are
        // handled above with their password-aware streaming readers, so this
        // only covers the plain in-memory decoders.
        if crate::model::source_reader::is_flat_floppy_container_path(path) {
            let reader = crate::model::source_reader::open_read(path)
                .map_err(|e| FilesystemError::Parse(format!("failed to open container: {e:#}")))?;
            let effective_offset = if self.partition_type == 0 {
                0
            } else {
                self.partition_offset
            };
            return fs::open_filesystem(
                reader,
                effective_offset,
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
            // Native (C) zstd has a seekable random-access decoder (zeekstd).
            #[cfg(feature = "native-zstd")]
            {
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
            // Pure-Rust zstd has no seekable reader; a seekable-zstd file is
            // still a valid sequence of zstd frames (+ a skippable seek-table
            // frame), so fully decompress it to an anonymous tempfile that
            // gives Read + Seek. Correct, just not random-access. Gate on
            // pure-zstd actually being on (matching zstd_compat.rs) — plain
            // `not(native-zstd)` also matched a no-backend build, where the
            // `libzstd_bitexact_rs` crate isn't linked.
            #[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
            {
                let file = File::open(path).map_err(FilesystemError::Io)?;
                let mut dec = libzstd_bitexact_rs::StreamDecoder::new(file);
                let mut tmp = tempfile::tempfile().map_err(FilesystemError::Io)?;
                if let Err(e) = std::io::copy(&mut dec, &mut tmp) {
                    let _ = std::fs::remove_file(path);
                    return Err(FilesystemError::Parse(format!(
                        "stale seekable zstd cache removed ({e}). Click Browse again to rebuild."
                    )));
                }
                std::io::Seek::seek(&mut tmp, std::io::SeekFrom::Start(0))
                    .map_err(FilesystemError::Io)?;
                return fs::open_filesystem(
                    tmp,
                    self.partition_offset,
                    self.partition_type,
                    self.partition_type_string.as_deref(),
                );
            }
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
            // Feed the carve scanner's per-chunk progress into the shared
            // status so a long full scan paints a determinate bar. Harmless
            // for every other filesystem (their `open` never reports). The
            // sink lives on this worker thread, where `session.open()` runs.
            let progress_status = Arc::clone(&status_thread);
            crate::fs::carve::set_scan_progress(Some(Box::new(move |done, total| {
                if let Ok(mut g) = progress_status.lock() {
                    g.phase = "Scanning image for recoverable data...".to_string();
                    g.scan_done = done;
                    g.scan_total = total;
                }
            })));
            let opened = session.open();
            crate::fs::carve::set_scan_progress(None);
            let mut fs = match opened {
                Ok(fs) => fs,
                Err(e) => {
                    let msg = format!("{e}");
                    // Detect password-related open failures (missing or wrong
                    // password) so the GUI re-shows the password prompt. Match
                    // on specific phrases rather than the bare word "password"
                    // to avoid false positives from image paths that happen to
                    // contain it (e.g. a folder named "GH11-password").
                    let lower = msg.to_lowercase();
                    let is_pw = lower.contains("password-protected")
                        || lower.contains("password is required")
                        || lower.contains("password must be specified")
                        || lower.contains("incorrect password")
                        || lower.contains("wrong password");
                    if let Ok(mut g) = status_thread.lock() {
                        g.error = Some(format!("Cannot open filesystem: {e}"));
                        g.needs_password = is_pw;
                        g.finished = true;
                    }
                    return;
                }
            };

            // Scan done — drop back to the plain spinner for the (instant)
            // root read so the progress bar doesn't linger at 100%.
            if let Ok(mut g) = status_thread.lock() {
                g.scan_done = 0;
                g.scan_total = 0;
            }

            let fs_type = fs.fs_type().to_string();
            let volume_label = fs.volume_label().unwrap_or("").to_string();
            let total_size = fs.total_size();
            let used_size = fs.used_size();
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
                g.total_size = total_size;
                g.used_size = used_size;
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
    ///
    /// Returns the editable filesystem **and** a [`ContainerEditCommit`] guard.
    /// After a successful, synced mutation the caller must call
    /// [`ContainerEditCommit::commit`] to persist it. For a raw image / device
    /// / CHD that's a no-op (writes already landed); for a floppy container
    /// (.d88 / .xdf / .hdm / .dim) the returned handle edits a decoded temp
    /// flat and `commit` re-encodes it back into the container. A caller that
    /// only reads (e.g. a free-space probe) or only checks that edit mode can
    /// be entered just drops the guard.
    pub fn open_editable(
        &self,
    ) -> Result<(Box<dyn EditableFilesystem>, ContainerEditCommit), FilesystemError> {
        // Remote image over the block tier — a read-write RemoteBlockReader
        // patches byte ranges over the wire; the daemon opens the file
        // read+write and syncs on flush/close. The commit guard is a no-op
        // (writes land directly over the wire, like a raw image / device).
        #[cfg(feature = "remote")]
        if let Some((conn, rpath)) = &self.remote {
            let reader = crate::remote::RemoteBlockReader::open_rw(Arc::clone(conn), rpath)
                .map_err(|e| FilesystemError::Io(std::io::Error::other(e.to_string())))?;
            let fs = fs::open_editable_filesystem(
                reader,
                self.partition_offset,
                self.partition_type,
                self.partition_type_string.as_deref(),
            )?;
            return Ok((fs, ContainerEditCommit { session: None }));
        }

        // Live CHD edit session: writes go through the diff (compressed
        // parent) or in-place (uncompressed). No File handle to source_path
        // is opened.
        if let Some(arc) = &self.chd_edit_session {
            let handle = ChdEditHandle::from_arc(Arc::clone(arc));
            let fs = fs::open_editable_filesystem(
                handle,
                self.partition_offset,
                self.partition_type,
                self.partition_type_string.as_deref(),
            )?;
            return Ok((fs, ContainerEditCommit { session: None }));
        }

        let path = self
            .source_path
            .as_ref()
            .ok_or_else(|| FilesystemError::Parse("no source path set".into()))?;

        // Xerox Alto packs: read the whole pack, decode to an in-memory Disk,
        // and hand back an editable BFS view. Edits rebuild the volume and
        // sync writes it back as a PDI (the no-op container commit applies).
        // PDI magic "PARCDISK"; CopyDisk params block 00 07 00 03 00 0a.
        {
            let mut magic = [0u8; 8];
            if let Ok(mut f) = File::open(path) {
                let _ = f.read(&mut magic);
            }
            let is_salto_dsk = std::fs::metadata(path)
                .map(|m| m.len() as usize == crate::fs::alto::salto::IMAGE_BYTES)
                .unwrap_or(false);
            // Dwarf Draco 6085 ".zdisk" (a Pilot pack); see open() for the gating.
            let is_zdisk = magic[0] == 0x78
                && std::fs::read(path)
                    .map(|b| crate::fs::alto::zdisk::is_zdisk(&b))
                    .unwrap_or(false);
            if &magic == b"PARCDISK"
                || magic[0..6] == [0x00, 0x07, 0x00, 0x03, 0x00, 0x0a]
                || is_salto_dsk
                || is_zdisk
            {
                let bytes = std::fs::read(path).map_err(FilesystemError::Io)?;
                let disk = crate::fs::alto::open_pack(&bytes)?;
                // Pilot/Cedar volumes (fsFamily=2, and every .zdisk) are
                // read-only for now.
                if disk.geometry.family == crate::fs::alto::FsFamily::Pilot {
                    return Err(FilesystemError::Unsupported(
                        "Pilot/Cedar volumes are read-only".into(),
                    ));
                }
                let efs = Box::new(crate::fs::alto::bfs::BfsFilesystem::open_editable(
                    disk,
                    path.clone(),
                ));
                return Ok((efs, ContainerEditCommit { session: None }));
            }
        }

        // Floppy container: edit a decoded temp flat, re-encode on commit.
        if crate::model::source_reader::is_editable_container_path(path) {
            let session =
                crate::model::container_edit::ContainerEditSession::open(path).map_err(|e| {
                    FilesystemError::Parse(format!("opening container for edit: {e:#}"))
                })?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(session.flat_path())
                .map_err(FilesystemError::Io)?;
            let fs = fs::open_editable_filesystem(
                file,
                self.partition_offset,
                self.partition_type,
                self.partition_type_string.as_deref(),
            )?;
            return Ok((
                fs,
                ContainerEditCommit {
                    session: Some(session),
                },
            ));
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(FilesystemError::Io)?;

        let fs = fs::open_editable_filesystem(
            file,
            self.partition_offset,
            self.partition_type,
            self.partition_type_string.as_deref(),
        )?;
        Ok((fs, ContainerEditCommit { session: None }))
    }
}

/// Returned alongside the editable filesystem from
/// [`BrowseSession::open_editable`]. Call [`commit`](Self::commit) after a
/// successful, synced mutation to persist it. No-op for raw images / devices /
/// CHD; for a floppy container it re-encodes the decoded temp flat back into
/// the container and atomically replaces the original. Dropping without
/// committing discards container edits (so a failed/aborted edit leaves the
/// original container untouched).
#[must_use = "call commit() to persist edits to a container"]
pub struct ContainerEditCommit {
    session: Option<crate::model::container_edit::ContainerEditSession>,
}

impl ContainerEditCommit {
    /// Persist the edit. No-op for raw sources; re-encodes for containers.
    pub fn commit(self) -> Result<(), FilesystemError> {
        match self.session {
            Some(session) => session
                .commit()
                .map_err(|e| FilesystemError::Parse(format!("re-encoding container: {e:#}"))),
            None => Ok(()),
        }
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
    /// Bytes scanned so far during a synthetic carve open. `scan_total == 0`
    /// means "not a carve scan" (or not started) — the GUI then shows a plain
    /// spinner; when `scan_total > 0` it paints a determinate progress bar.
    pub scan_done: u64,
    pub scan_total: u64,
    pub finished: bool,
    pub fs_type: String,
    pub volume_label: String,
    /// Total filesystem size and used bytes, for the browser's free-space line.
    pub total_size: u64,
    pub used_size: u64,
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
            scan_done: 0,
            scan_total: 0,
            finished: false,
            fs_type: String::new(),
            volume_label: String::new(),
            total_size: 0,
            used_size: 0,
            blessed_folder: None,
            root: None,
            root_entries: None,
            error: None,
            needs_password: false,
            fs: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::CreateFileOptions;

    /// GUI edit-mode write path: opening a floppy container for editing, adding
    /// a file, and committing must persist the file back INTO the container
    /// (not into a discarded temp). Regression guard for the bug where GUI
    /// edits to .d88/.xdf/.hdm/.dim were silently lost.
    #[test]
    fn open_editable_persists_edits_into_floppy_container() {
        // Blank 720K FAT12 floppy wrapped as a headerless .xdf. (num_heads=2
        // lands at offset 0x1A, so the d88 header sniffer doesn't claim it and
        // detection routes by the .xdf extension.) Human68k floppies are
        // standard little-endian FAT12, which Human68kFilesystem opens.
        let flat = crate::fs::fat::create_blank_fat(737280, Some("TEST")).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let xdf = dir.path().join("disk.xdf");
        std::fs::write(&xdf, &flat).unwrap();

        let session = BrowseSession {
            source_path: Some(xdf.clone()),
            partition_type_string: Some("human68k".to_string()),
            ..Default::default()
        };

        // Edit via the editable handle, sync, then commit (re-encode -> .xdf).
        let (mut efs, commit) = session.open_editable().expect("open_editable");
        let root = efs.root().unwrap();
        let mut data = std::io::Cursor::new(b"hello container".to_vec());
        efs.create_file(
            &root,
            "HELLO.TXT",
            &mut data,
            15,
            &CreateFileOptions::default(),
        )
        .expect("create_file");
        efs.sync_metadata().expect("sync");
        drop(efs);
        commit.commit().expect("commit re-encode");

        // The file must be visible when the container is re-opened read-only,
        // and the container must still be its original fixed size.
        let mut fs = session.open().expect("re-open");
        let root = fs.root().unwrap();
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(
            names.iter().any(|n| n == "HELLO.TXT"),
            "edit persisted into .xdf container: {names:?}"
        );
        assert_eq!(std::fs::metadata(&xdf).unwrap().len(), 737280);
    }

    /// A raw (non-container) image returns a no-op commit and edits land
    /// directly in the file — committing must not error or alter behavior.
    #[test]
    fn open_editable_raw_image_commit_is_noop() {
        let flat = crate::fs::fat::create_blank_fat(737280, Some("RAW")).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img"); // not a container extension
        std::fs::write(&img, &flat).unwrap();

        let session = BrowseSession {
            source_path: Some(img.clone()),
            partition_type_string: Some("human68k".to_string()),
            ..Default::default()
        };
        let (mut efs, commit) = session.open_editable().expect("open_editable");
        let root = efs.root().unwrap();
        let mut data = std::io::Cursor::new(b"x".to_vec());
        efs.create_file(&root, "R.TXT", &mut data, 1, &CreateFileOptions::default())
            .unwrap();
        efs.sync_metadata().unwrap();
        drop(efs);
        commit.commit().expect("no-op commit");

        let mut fs = session.open().expect("re-open");
        let root = fs.root().unwrap();
        assert!(fs
            .list_directory(&root)
            .unwrap()
            .iter()
            .any(|e| e.name == "R.TXT"));
    }

    /// A Xerox Alto pack (here a PARC Disk Image) must route through the Alto
    /// branch of `open()` and come back as a BFS filesystem view — this is the
    /// exact path the GUI browse view takes when a user opens a `.pdi`/`.bfs`.
    #[test]
    fn open_routes_alto_pdi_to_bfs_filesystem() {
        use crate::fs::alto::{Disk, FsFamily, Geometry, Sector};
        use std::io::Write as _;

        let geometry = Geometry {
            family: FsFamily::Diablo,
            disk_model: 31,
            n_disks: 1,
            n_cylinders: 1,
            n_heads: 1,
            n_sectors: 4,
            label_bytes: 16,
            data_bytes: 512,
        };
        let total = geometry.total_sectors();
        let sectors = (0..total).map(|_| Sector::zeroed(16, 512)).collect();
        let pdi = crate::fs::alto::pdi::write(&Disk { geometry, sectors });

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&pdi).unwrap();

        let mut session = BrowseSession::new();
        session.source_path = Some(tmp.path().to_path_buf());
        let fs = session
            .open()
            .expect("BrowseSession should open an Alto PDI pack");
        assert_eq!(fs.fs_type(), "Alto BFS");
    }

    #[test]
    fn open_routes_salto_dsk_to_bfs_filesystem() {
        use crate::fs::alto::{salto, Disk, FsFamily, Geometry, Sector};
        use std::io::Write as _;

        // A blank Salto-shaped Diablo-31 pack written in Salto's .dsk container.
        let geometry = Geometry {
            family: FsFamily::Diablo,
            disk_model: 31,
            n_disks: 1,
            n_cylinders: 203,
            n_heads: 2,
            n_sectors: 12,
            label_bytes: 16,
            data_bytes: 512,
        };
        let total = geometry.total_sectors();
        let sectors = (0..total).map(|_| Sector::zeroed(16, 512)).collect();
        let dsk = salto::write(&Disk { geometry, sectors }).expect("salto write");
        assert_eq!(dsk.len(), salto::IMAGE_BYTES);

        let mut tmp = tempfile::Builder::new().suffix(".dsk").tempfile().unwrap();
        tmp.write_all(&dsk).unwrap();

        let mut session = BrowseSession::new();
        session.source_path = Some(tmp.path().to_path_buf());
        let fs = session
            .open()
            .expect("BrowseSession should open a Salto .dsk pack");
        assert_eq!(fs.fs_type(), "Alto BFS");
    }

    #[test]
    fn open_routes_pilot_pdi_to_pilot_filesystem() {
        use crate::fs::alto::pilot::{self, Generation};
        use std::io::Write as _;

        // A blank Cedar-nucleus Pilot volume, written as a PDI (fsFamily=2).
        let disk = pilot::create_blank(
            pilot::pilot_geometry(128),
            Generation::CedarNucleus,
            "RouteTest",
        )
        .expect("create blank pilot volume");
        let pdi = crate::fs::alto::pdi::write(&disk);

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&pdi).unwrap();

        let mut session = BrowseSession::new();
        session.source_path = Some(tmp.path().to_path_buf());
        let mut fs = session
            .open()
            .expect("BrowseSession should open a Pilot PDI volume");
        assert_eq!(fs.fs_type(), "Pilot/Cedar");
        assert_eq!(fs.volume_label(), Some("RouteTest"));
        // Blank volume: browsable root, zero files.
        let root = fs.root().expect("root");
        assert!(fs.list_directory(&root).expect("list").is_empty());

        // Editing a Pilot volume is refused (read-only).
        assert!(session.open_editable().is_err());
    }

    #[test]
    fn open_routes_zdisk_to_pilot_filesystem() {
        use crate::fs::alto::pilot::{self, Generation};
        use crate::fs::alto::{zdisk, FsFamily, Geometry};
        use std::io::Write as _;

        // A blank original-Pilot volume on a 6085-shaped geometry (16
        // sectors/track), serialized to the Dwarf Draco `.zdisk` container.
        let geo = Geometry {
            family: FsFamily::Pilot,
            disk_model: 0,
            n_disks: 1,
            n_cylinders: 40,
            n_heads: 1,
            n_sectors: 16,
            label_bytes: 20,
            data_bytes: 512,
        };
        let disk = pilot::create_blank(geo, Generation::OriginalPilot, "ZDiskVol")
            .expect("create blank pilot volume");
        let zbytes = zdisk::write(&disk).expect("zdisk write");

        let mut tmp = tempfile::Builder::new()
            .suffix(".zdisk")
            .tempfile()
            .unwrap();
        tmp.write_all(&zbytes).unwrap();

        let mut session = BrowseSession::new();
        session.source_path = Some(tmp.path().to_path_buf());
        let fs = session
            .open()
            .expect("BrowseSession should open a Dwarf .zdisk Pilot volume");
        assert_eq!(fs.fs_type(), "Pilot/Cedar");
        assert_eq!(fs.volume_label(), Some("ZDiskVol"));

        // A .zdisk Pilot volume is read-only.
        assert!(session.open_editable().is_err());
    }
}
