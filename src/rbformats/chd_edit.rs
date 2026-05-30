//! Edit-mode plumbing for CHD hard-disk images.
//!
//! [`ChdEditSession`] is a sector-buffered `Read + Write + Seek` adapter over
//! [`libchdman_rs::hd::HdImage`]. It lets the existing `EditableFilesystem`
//! code mutate a CHD without first decompressing it to a raw temp file.
//!
//! Two open modes:
//!
//! - [`ChdEditSession::open_uncompressed`] — modifies the CHD in place. Use
//!   when [`is_compressed_chd`] returns false.
//! - [`ChdEditSession::open_with_diff`] — opens a compressed parent CHD
//!   read-only and routes all writes into a fresh, uncompressed child diff
//!   (the same scheme MAME uses at runtime). [`flatten_to_parent`] then
//!   merges the diff back into a single compressed CHD that overwrites the
//!   parent.
//!
//! Before any edit, callers should run [`make_backup_copy`] so the original
//! parent file is preserved as `<name>.chd_backup` for manual revert.

use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use libchdman_rs::hd::HdImage;

use super::chd::compress_chd;
use super::chd_options::ChdOptions;

/// Sector-buffered `Read + Write + Seek` view over a CHD.
///
/// Caches one sector at a time. A partial-sector write is implemented as
/// read-modify-write; a full-sector overwrite skips the read. The cached
/// sector is flushed lazily — on a different-sector access, on
/// [`Write::flush`], or on drop.
pub struct ChdEditSession {
    hd: HdImage,
    sector_size: usize,
    sector_count: u64,
    logical_size: u64,
    position: u64,
    buf: Vec<u8>,
    cached_lba: Option<u64>,
    dirty: bool,
}

// SAFETY: same reasoning as `ChdReader` in chd.rs — the underlying CHD handle
// is single-threaded, but ownership can be moved between threads as long as
// only one thread accesses it at a time. We never share a session across
// threads concurrently.
unsafe impl Send for ChdEditSession {}

impl ChdEditSession {
    /// Open an uncompressed CHD writeable; mutations land directly in `path`.
    /// Fails if the CHD is compressed — use [`Self::open_with_diff`] instead.
    pub fn open_uncompressed(path: &Path) -> Result<Self> {
        let hd = HdImage::open(path).map_err(|e| {
            anyhow::anyhow!(
                "failed to open uncompressed CHD {}: {:?}",
                path.display(),
                e
            )
        })?;
        Ok(Self::from_hd(hd))
    }

    /// Open compressed `parent_path` read-only and create `diff_path` as a
    /// fresh uncompressed child. All writes route into the diff; the parent
    /// is never modified. `diff_path` must not already exist — pass a path
    /// inside a temp directory or delete the previous diff first.
    pub fn open_with_diff(parent_path: &Path, diff_path: &Path) -> Result<Self> {
        let hd = HdImage::open_with_diff(parent_path, diff_path).map_err(|e| {
            anyhow::anyhow!(
                "failed to open CHD {} with diff {}: {:?}",
                parent_path.display(),
                diff_path.display(),
                e
            )
        })?;
        Ok(Self::from_hd(hd))
    }

    /// Re-attach to an existing diff that was previously created via
    /// [`Self::open_with_diff`] but is now closed.
    pub fn reopen_with_diff(parent_path: &Path, diff_path: &Path) -> Result<Self> {
        let hd = HdImage::reopen_diff(parent_path, diff_path).map_err(|e| {
            anyhow::anyhow!(
                "failed to reopen diff {} against parent {}: {:?}",
                diff_path.display(),
                parent_path.display(),
                e
            )
        })?;
        Ok(Self::from_hd(hd))
    }

    fn from_hd(hd: HdImage) -> Self {
        let sector_size = hd.sector_size() as usize;
        let sector_count = hd.sector_count();
        let logical_size = sector_size as u64 * sector_count;
        Self {
            hd,
            sector_size,
            sector_count,
            logical_size,
            position: 0,
            buf: vec![0u8; sector_size],
            cached_lba: None,
            dirty: false,
        }
    }

    /// Total addressable bytes — `sector_count * sector_size`.
    pub fn logical_size(&self) -> u64 {
        self.logical_size
    }

    pub fn sector_size(&self) -> usize {
        self.sector_size
    }

    pub fn sector_count(&self) -> u64 {
        self.sector_count
    }

    fn flush_cached(&mut self) -> io::Result<()> {
        if self.dirty {
            let lba = self
                .cached_lba
                .expect("dirty buffer must have a cached_lba");
            self.hd
                .write_sector(lba, &self.buf)
                .map_err(|e| io::Error::other(format!("CHD write_sector(lba={lba}): {e:?}")))?;
            self.dirty = false;
        }
        Ok(())
    }

    fn load_sector(&mut self, lba: u64) -> io::Result<()> {
        if self.cached_lba == Some(lba) {
            return Ok(());
        }
        self.flush_cached()?;
        self.hd
            .read_sector(lba, &mut self.buf)
            .map_err(|e| io::Error::other(format!("CHD read_sector(lba={lba}): {e:?}")))?;
        self.cached_lba = Some(lba);
        Ok(())
    }
}

impl Read for ChdEditSession {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.logical_size {
            return Ok(0);
        }
        let remaining = self.logical_size - self.position;
        let want = (out.len() as u64).min(remaining) as usize;
        if want == 0 {
            return Ok(0);
        }
        let lba = self.position / self.sector_size as u64;
        let off = (self.position % self.sector_size as u64) as usize;
        let n = (self.sector_size - off).min(want);
        self.load_sector(lba)?;
        out[..n].copy_from_slice(&self.buf[off..off + n]);
        self.position += n as u64;
        Ok(n)
    }
}

impl Write for ChdEditSession {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if self.position >= self.logical_size {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "write past end of CHD",
            ));
        }
        let remaining = self.logical_size - self.position;
        let want = (data.len() as u64).min(remaining) as usize;
        if want == 0 {
            return Ok(0);
        }
        let lba = self.position / self.sector_size as u64;
        let off = (self.position % self.sector_size as u64) as usize;
        let n = (self.sector_size - off).min(want);
        if off != 0 || n != self.sector_size {
            // Partial-sector write: read-modify-write.
            self.load_sector(lba)?;
        } else {
            // Full-sector overwrite: skip the read.
            if self.cached_lba != Some(lba) {
                self.flush_cached()?;
                self.cached_lba = Some(lba);
            }
        }
        self.buf[off..off + n].copy_from_slice(&data[..n]);
        self.dirty = true;
        self.position += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_cached()
    }
}

impl Seek for ChdEditSession {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::Current(n) => self.position as i64 + n,
            SeekFrom::End(n) => self.logical_size as i64 + n,
        };
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before start",
            ));
        }
        // Cap at logical_size so subsequent writes return WriteZero rather
        // than corrupting state.
        self.position = (new_pos as u64).min(self.logical_size);
        Ok(self.position)
    }
}

impl Drop for ChdEditSession {
    fn drop(&mut self) {
        if self.dirty {
            // Last-chance flush. Drop can't propagate errors; surface in log
            // so a forgotten flush() call is still recoverable evidence.
            if let Err(e) = self.flush_cached() {
                log::error!("ChdEditSession dropped with unflushed write: {e}");
            }
        }
    }
}

/// `Read + Write + Seek` handle backed by an `Arc<Mutex<ChdEditSession>>`.
///
/// The filesystem opener (`fs::open_filesystem` / `open_editable_filesystem`)
/// takes ownership of its reader, but during edit mode we want one
/// long-lived `ChdEditSession` shared across multiple opens (browse refresh,
/// staged-edit replay, etc.). `ChdEditHandle` clones the `Arc` cheaply and
/// locks per I/O call. The mutex is uncontended in practice — the GUI is
/// single-threaded — but Rust requires the `Send` bound for the worker
/// threads that run apply / flatten.
#[derive(Clone)]
pub struct ChdEditHandle {
    inner: Arc<Mutex<ChdEditSession>>,
}

impl ChdEditHandle {
    pub fn new(session: ChdEditSession) -> Self {
        Self {
            inner: Arc::new(Mutex::new(session)),
        }
    }

    pub fn from_arc(inner: Arc<Mutex<ChdEditSession>>) -> Self {
        Self { inner }
    }

    pub fn arc(&self) -> Arc<Mutex<ChdEditSession>> {
        Arc::clone(&self.inner)
    }

    pub fn logical_size(&self) -> u64 {
        self.inner
            .lock()
            .expect("ChdEditSession poisoned")
            .logical_size()
    }

    /// Force a flush of the cached sector. Call before dropping the last
    /// handle if you can't tolerate a silent error in `Drop`.
    pub fn flush(&self) -> io::Result<()> {
        self.inner.lock().expect("ChdEditSession poisoned").flush()
    }
}

impl Read for ChdEditHandle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner
            .lock()
            .expect("ChdEditSession poisoned")
            .read(buf)
    }
}

impl Write for ChdEditHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner
            .lock()
            .expect("ChdEditSession poisoned")
            .write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.lock().expect("ChdEditSession poisoned").flush()
    }
}

impl Seek for ChdEditHandle {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner
            .lock()
            .expect("ChdEditSession poisoned")
            .seek(pos)
    }
}

/// True if the CHD at `path` uses any compression codec (i.e. cannot be
/// written to in place). Compressed CHDs must use the
/// [`ChdEditSession::open_with_diff`] flow.
pub fn is_compressed_chd(path: &Path) -> Result<bool> {
    let path_str = path
        .to_str()
        .with_context(|| format!("CHD path is not valid UTF-8: {}", path.display()))?;
    let chd = libchdman_rs::Chd::open(path_str, false, None)
        .map_err(|e| anyhow::anyhow!("failed to open CHD {}: {:?}", path.display(), e))?;
    let info = chd
        .info()
        .map_err(|e| anyhow::anyhow!("failed to read CHD info: {:?}", e))?;
    Ok(info.compressed)
}

/// Copy `chd_path` to `<chd_path>_backup` so the user has an easy revert
/// point before any edits happen. No-op if the backup already exists —
/// the first edit's backup is preserved across subsequent edit sessions.
///
/// Returns the backup path either way.
pub fn make_backup_copy(chd_path: &Path) -> Result<PathBuf> {
    let backup = backup_path_for(chd_path);
    if backup.exists() {
        return Ok(backup);
    }
    fs::copy(chd_path, &backup).with_context(|| {
        format!(
            "failed to copy {} -> {}",
            chd_path.display(),
            backup.display()
        )
    })?;
    Ok(backup)
}

/// `foo.chd` -> `foo.chd_backup`. The trailing `_backup` (no dot) keeps the
/// `.chd` extension visible so file managers still recognise the format.
pub fn backup_path_for(chd_path: &Path) -> PathBuf {
    let mut name = chd_path.file_name().unwrap_or_default().to_os_string();
    name.push("_backup");
    chd_path.parent().unwrap_or(Path::new(".")).join(name)
}

/// Read all logical bytes of an [`HdImage`] sequentially, sector by sector.
/// Used as the input to [`compress_chd`] when flattening a diff back into
/// a fresh standalone CHD.
struct HdImageSequentialReader {
    session: ChdEditSession,
}

impl Read for HdImageSequentialReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.session.read(buf)
    }
}

/// Merge the parent + diff into a fresh compressed CHD that replaces
/// `parent_path`, then delete the diff. Geometry, sector size, and logical
/// size are inherited from the parent; codecs/hunk size come from `opts`
/// (or chdman's defaults if `None`).
///
/// The diff is opened read-only via [`HdImage::reopen_diff`], streamed into
/// [`compress_chd`] which writes a sibling file `<stem>.flattening.chd`,
/// and the result is renamed atomically over `parent_path`. On error the
/// staging file is removed and the parent is left untouched.
pub fn flatten_to_parent(
    parent_path: &Path,
    diff_path: &Path,
    opts: Option<ChdOptions>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    let session = ChdEditSession::reopen_with_diff(parent_path, diff_path)?;
    let logical_size = session.logical_size();

    // compress_chd derives the output filename from `output_base.file_stem()`
    // + ".chd". Use a sibling `<stem>.flattening` base so we get
    // `<stem>.flattening.chd` and don't clobber the parent until the rename.
    let parent_dir = parent_path.parent().unwrap_or(Path::new("."));
    let parent_stem = parent_path
        .file_stem()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
    let mut staging_stem = parent_stem;
    staging_stem.push(".flattening");
    let staging_base = parent_dir.join(staging_stem);
    let staging_chd = staging_base.with_extension("chd");

    // Defensive: leftover staging file from a previous crashed flatten.
    if staging_chd.exists() {
        let _ = fs::remove_file(&staging_chd);
    }

    // Stage the merged parent+diff view into a flat scratch file BEFORE
    // calling compress_chd. libchdman-rs's create_from_reader cannot be
    // re-entered from its own reader callback — when the reader is backed by
    // ChdEditSession (which itself calls libchdman's HdImage::read_sector to
    // resolve each merged read), the nested libchdman calls produce a
    // corrupted output CHD (all-zero hunks in practice). Feeding a plain
    // File-backed reader to compress_chd decouples the two libchdman
    // invocations entirely. Memory usage stays bounded by the I/O buffer
    // size, not by logical_size, so this works for multi-GB images too.
    let scratch_path = parent_dir.join({
        let mut s = parent_path
            .file_stem()
            .map(|s| s.to_os_string())
            .unwrap_or_default();
        s.push(".flattening.scratch");
        s
    });
    if scratch_path.exists() {
        let _ = fs::remove_file(&scratch_path);
    }
    {
        let mut reader = HdImageSequentialReader { session };
        let mut scratch = std::io::BufWriter::with_capacity(
            1024 * 1024,
            fs::File::create(&scratch_path).with_context(|| {
                format!(
                    "failed to create flatten scratch file {}",
                    scratch_path.display()
                )
            })?,
        );
        std::io::copy(&mut reader, &mut scratch)
            .context("failed to stage merged view for flatten")?;
        std::io::Write::flush(&mut scratch).context("failed to flush flatten scratch file")?;
        // reader (and the wrapped session) drops here — close the read-side
        // libchdman handle BEFORE compress_chd opens its write-side one.
    }

    let mut scratch_reader = std::io::BufReader::with_capacity(
        1024 * 1024,
        fs::File::open(&scratch_path).with_context(|| {
            format!(
                "failed to reopen flatten scratch file {}",
                scratch_path.display()
            )
        })?,
    );
    let result = compress_chd(
        &mut scratch_reader,
        &staging_base,
        logical_size,
        None,
        opts,
        progress_cb,
        cancel_check,
        log_cb,
    );
    drop(scratch_reader);
    let _ = fs::remove_file(&scratch_path);

    match result {
        Ok(_files) => {
            fs::rename(&staging_chd, parent_path).with_context(|| {
                format!(
                    "failed to replace {} with {}",
                    parent_path.display(),
                    staging_chd.display()
                )
            })?;
            let _ = fs::remove_file(diff_path);
            Ok(())
        }
        Err(e) => {
            let _ = fs::remove_file(&staging_chd);
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libchdman_rs::hd::{create_from_reader, HdCreateOptions};
    use std::io::Cursor;
    use tempfile::TempDir;

    /// Build an uncompressed HD CHD (codecs all-zero). `data.len()` must be
    /// a multiple of 512.
    fn make_uncompressed_chd(path: &Path, data: &[u8]) {
        let opts = HdCreateOptions {
            logical_size: data.len() as u64,
            hunk_size: 4096,
            unit_size: 512,
            codecs: [0u32; 4],
            geometry: None,
            ident: None,
        };
        let mut reader = Cursor::new(data);
        create_from_reader(&mut reader, path, opts, &mut |_p| {}, &|| false)
            .expect("create_from_reader (uncompressed)");
    }

    /// Build a compressed HD CHD via the same `compress_chd` path the
    /// production code uses. `output_base` should not include the `.chd`
    /// extension.
    fn make_compressed_chd(path: &Path, data: &[u8]) {
        let parent_dir = path.parent().unwrap();
        let stem = path.file_stem().unwrap();
        let base = parent_dir.join(stem);
        let mut reader = Cursor::new(data);
        compress_chd(
            &mut reader,
            &base,
            data.len() as u64,
            None,
            None,
            &mut |_| {},
            &|| false,
            &mut |_| {},
        )
        .expect("compress_chd");
    }

    fn pseudo_random(len: usize, seed: usize) -> Vec<u8> {
        (0..len)
            .map(|i| ((i.wrapping_mul(seed.max(1) * 31)) ^ (i >> 7)) as u8)
            .collect()
    }

    #[test]
    fn uncompressed_in_place_round_trip() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("disk.chd");
        let mut data = pseudo_random(64 * 1024, 1);
        make_uncompressed_chd(&path, &data);

        // Mutate bytes spanning two sectors via the seek-able session.
        let mut session = ChdEditSession::open_uncompressed(&path).unwrap();
        assert_eq!(session.sector_size(), 512);
        session.seek(SeekFrom::Start(500)).unwrap();
        let patch = [0xAB; 100];
        session.write_all(&patch).unwrap();
        session.flush().unwrap();
        drop(session);

        // Mirror the change in `data` and verify the on-disk CHD now matches.
        data[500..600].copy_from_slice(&patch);
        let chd = libchdman_rs::Chd::open(path.to_str().unwrap(), false, None).unwrap();
        let mut got = vec![0u8; data.len()];
        chd.read_bytes(0, &mut got).unwrap();
        assert_eq!(got, data, "in-place edit not persisted");
    }

    #[test]
    fn compressed_diff_then_flatten() {
        let tmp = TempDir::new().unwrap();
        let parent = tmp.path().join("disk.chd");
        let diff = tmp.path().join("edit.diff");
        // 1 MiB matches the size used by the chd.rs round-trip test; smaller
        // inputs have tripped chdman's compress_v5_map bitmap path in practice.
        let mut data = pseudo_random(1024 * 1024, 2);
        make_compressed_chd(&parent, &data);

        // Snapshot the parent's bytes BEFORE editing — used to verify the
        // parent file is untouched while writes route into the diff.
        let parent_bytes_before = fs::read(&parent).unwrap();

        // Edit through the diff session.
        {
            let mut session = ChdEditSession::open_with_diff(&parent, &diff).unwrap();
            session.seek(SeekFrom::Start(2048)).unwrap();
            let patch = [0x5A; 1024];
            session.write_all(&patch).unwrap();
            session.flush().unwrap();
            data[2048..3072].copy_from_slice(&patch);
        }
        // Diff exists; parent file bytes unchanged.
        assert!(diff.exists(), "diff should have been created");
        let parent_bytes_after_edit = fs::read(&parent).unwrap();
        assert_eq!(
            parent_bytes_before, parent_bytes_after_edit,
            "parent CHD must not be modified during diff editing"
        );

        // Reopen as merged view and verify the patch is visible.
        {
            let session = ChdEditSession::reopen_with_diff(&parent, &diff).unwrap();
            let mut got = vec![0u8; data.len()];
            let mut r = HdImageSequentialReader { session };
            r.read_exact(&mut got).unwrap();
            assert_eq!(got, data, "merged view does not reflect diff write");
        }

        // Flatten back into the parent.
        flatten_to_parent(&parent, &diff, None, &mut |_| {}, &|| false, &mut |_| {}).unwrap();
        assert!(!diff.exists(), "diff should be removed after flatten");
        assert!(parent.exists(), "parent should still exist after flatten");

        // Final bytes match `data` and the file is still a valid (compressed) CHD.
        let chd = libchdman_rs::Chd::open(parent.to_str().unwrap(), false, None).unwrap();
        let mut got = vec![0u8; data.len()];
        chd.read_bytes(0, &mut got).unwrap();
        assert_eq!(got, data, "flattened CHD content mismatch");
        assert!(
            chd.info().unwrap().compressed,
            "flattened output should still be compressed"
        );
    }

    #[test]
    fn make_backup_copy_preserves_original() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("disk.chd");
        let data = pseudo_random(8 * 1024, 3);
        make_uncompressed_chd(&path, &data);

        let backup = make_backup_copy(&path).unwrap();
        assert_eq!(backup, tmp.path().join("disk.chd_backup"));
        assert!(backup.exists());
        assert_eq!(fs::read(&backup).unwrap(), fs::read(&path).unwrap());

        // Calling again is a no-op and does not overwrite the existing backup
        // (even if the working file has changed since).
        fs::write(&path, b"corrupted").unwrap();
        let backup2 = make_backup_copy(&path).unwrap();
        assert_eq!(backup, backup2);
        assert_ne!(
            fs::read(&backup).unwrap(),
            b"corrupted",
            "existing backup must not be overwritten"
        );
    }

    #[test]
    fn is_compressed_chd_reports_correctly() {
        let tmp = TempDir::new().unwrap();
        let plain = tmp.path().join("plain.chd");
        let compressed = tmp.path().join("squashed.chd");
        let data = pseudo_random(1024 * 1024, 4);
        make_uncompressed_chd(&plain, &data);
        make_compressed_chd(&compressed, &data);

        assert!(!is_compressed_chd(&plain).unwrap());
        assert!(is_compressed_chd(&compressed).unwrap());
    }

    #[test]
    fn write_past_end_returns_error() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("disk.chd");
        let data = pseudo_random(2048, 5);
        make_uncompressed_chd(&path, &data);

        let mut session = ChdEditSession::open_uncompressed(&path).unwrap();
        session
            .seek(SeekFrom::Start(session.logical_size()))
            .unwrap();
        let err = session.write(&[0u8; 16]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WriteZero);
    }
}
