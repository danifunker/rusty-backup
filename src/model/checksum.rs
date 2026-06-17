//! Multi-algorithm checksums for Commander Mode's "Calculate Checksums" action.
//!
//! [`hash_reader`] streams a reader **once** through four hashers in parallel —
//! CRC32, MD5, SHA1, and SHA256 — and returns a [`ChecksumSet`] with hex
//! accessors. The same streaming core ([`ChecksumHasher`], a `std::io::Write`
//! sink) backs the image path, where data is produced by
//! [`Filesystem::write_file_to`](crate::fs::filesystem::Filesystem::write_file_to)
//! rather than read from a file handle.
//!
//! This deliberately does **not** reuse `backup::verify::RunningHasher` (CRC32 /
//! SHA256 only, and backup-specific): Commander shows all four algorithms a user
//! would compare against an external tool's output.
//!
//! [`spawn`] runs a whole selection on a worker thread behind a
//! [`ChecksumStatus`] (the established Status pattern), re-opening an image
//! source on the worker exactly like
//! [`commander_ops::spawn_host_copy`](crate::model::commander_ops::spawn_host_copy).

use std::io::{self, Read, Write};
use std::sync::{Arc, Mutex};
use std::thread;

use md5::{Digest, Md5};
use sha1::Sha1;
use sha2::Sha256;

use crate::fs::entry::FileEntry;
use crate::model::browse_session::BrowseSession;

/// The four computed digests for one file. Raw bytes; use the `*_hex`
/// accessors for display.
#[derive(Clone, PartialEq, Eq)]
pub struct ChecksumSet {
    pub crc32: u32,
    pub md5: [u8; 16],
    pub sha1: [u8; 20],
    pub sha256: [u8; 32],
}

impl ChecksumSet {
    /// CRC32 as 8 lowercase hex digits.
    pub fn crc32_hex(&self) -> String {
        format!("{:08x}", self.crc32)
    }
    pub fn md5_hex(&self) -> String {
        hex_lower(&self.md5)
    }
    pub fn sha1_hex(&self) -> String {
        hex_lower(&self.sha1)
    }
    pub fn sha256_hex(&self) -> String {
        hex_lower(&self.sha256)
    }
}

/// A `std::io::Write` sink that feeds every byte written to all four hashers and
/// counts the running total. Both the image path (via `write_file_to`) and
/// [`hash_reader`] funnel through this.
pub struct ChecksumHasher {
    crc: crc32fast::Hasher,
    md5: Md5,
    sha1: Sha1,
    sha256: Sha256,
    total: u64,
}

impl Default for ChecksumHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl ChecksumHasher {
    pub fn new() -> Self {
        Self {
            crc: crc32fast::Hasher::new(),
            md5: Md5::new(),
            sha1: Sha1::new(),
            sha256: Sha256::new(),
            total: 0,
        }
    }

    /// Feed `data` to all four hashers and add to the running byte count.
    pub fn update(&mut self, data: &[u8]) {
        self.crc.update(data);
        self.md5.update(data);
        self.sha1.update(data);
        self.sha256.update(data);
        self.total += data.len() as u64;
    }

    /// Total bytes hashed so far.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Finish all four digests.
    pub fn finalize(self) -> ChecksumSet {
        ChecksumSet {
            crc32: self.crc.finalize(),
            md5: self.md5.finalize().into(),
            sha1: self.sha1.finalize().into(),
            sha256: self.sha256.finalize().into(),
        }
    }
}

impl Write for ChecksumHasher {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.update(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Stream `reader` once through all four hashers, invoking `progress` with the
/// running byte total after each chunk. Reads in 1 MiB blocks (never buffers the
/// whole input).
pub fn hash_reader<R: Read>(reader: &mut R, progress: &dyn Fn(u64)) -> io::Result<ChecksumSet> {
    let mut hasher = ChecksumHasher::new();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        progress(hasher.total());
    }
    Ok(hasher.finalize())
}

/// Lowercase hex for a byte slice.
fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// One file's checksum result (or the error that stopped it).
pub struct FileChecksum {
    pub name: String,
    pub size: u64,
    /// `Some` on success; `None` with `error` set on failure.
    pub set: Option<ChecksumSet>,
    pub error: Option<String>,
}

/// Shared state between the GUI and the [`spawn`] worker.
#[derive(Default)]
pub struct ChecksumStatus {
    pub finished: bool,
    /// Fatal error that aborted the whole job (e.g. the image wouldn't open).
    pub error: Option<String>,
    /// One entry per hashed file, appended as each completes.
    pub results: Vec<FileChecksum>,
    /// Name of the file currently being hashed.
    pub current_file: String,
    /// Bytes hashed in the current file, and its total size (for a progress bar).
    pub current_bytes: u64,
    pub current_total: u64,
    /// Files completed / total selected (directories excluded).
    pub done_files: usize,
    pub total_files: usize,
}

/// What to checksum, and where the bytes come from.
pub enum ChecksumJob {
    /// Files on a disk-image volume; re-opened read-only on the worker thread.
    Image {
        session: BrowseSession,
        entries: Vec<FileEntry>,
    },
    /// Host-OS files referenced by their absolute path.
    Host { entries: Vec<FileEntry> },
}

/// Run a [`ChecksumJob`] on a worker thread. Directories in the selection are
/// skipped (only files are hashed). The returned status flips `finished` when
/// done.
pub fn spawn(job: ChecksumJob) -> Arc<Mutex<ChecksumStatus>> {
    let status = Arc::new(Mutex::new(ChecksumStatus::default()));
    let status_thread = Arc::clone(&status);
    thread::spawn(move || {
        run(job, &status_thread);
        if let Ok(mut g) = status_thread.lock() {
            g.finished = true;
        }
    });
    status
}

fn run(job: ChecksumJob, status: &Arc<Mutex<ChecksumStatus>>) {
    match job {
        ChecksumJob::Image { session, entries } => {
            let files: Vec<FileEntry> = entries.into_iter().filter(|e| e.is_file()).collect();
            set_total(status, files.len());
            let mut fs = match session.open() {
                Ok(fs) => fs,
                Err(e) => {
                    if let Ok(mut g) = status.lock() {
                        g.error = Some(format!("opening source image: {e:#}"));
                    }
                    return;
                }
            };
            for entry in files {
                begin_file(status, &entry);
                let mut hasher = ChecksumHasher::new();
                let progress = |b: u64| {
                    if let Ok(mut g) = status.lock() {
                        g.current_bytes = b;
                    }
                };
                let mut sink = ProgressSink {
                    hasher: &mut hasher,
                    progress: &progress,
                };
                let result = fs.write_file_to(&entry, &mut sink);
                finish_file(status, &entry, result.map(|_| hasher.finalize()));
            }
        }
        ChecksumJob::Host { entries } => {
            let files: Vec<FileEntry> = entries.into_iter().filter(|e| e.is_file()).collect();
            set_total(status, files.len());
            for entry in files {
                begin_file(status, &entry);
                let progress = |b: u64| {
                    if let Ok(mut g) = status.lock() {
                        g.current_bytes = b;
                    }
                };
                let result = std::fs::File::open(&entry.path)
                    .and_then(|mut f| hash_reader(&mut f, &progress));
                finish_file(status, &entry, result);
            }
        }
    }
}

/// A `Write` sink wrapping a [`ChecksumHasher`] that reports progress after each
/// chunk, for the image path where bytes arrive via `write_file_to`.
struct ProgressSink<'a> {
    hasher: &'a mut ChecksumHasher,
    progress: &'a dyn Fn(u64),
}

impl Write for ProgressSink<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hasher.update(buf);
        (self.progress)(self.hasher.total());
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn set_total(status: &Arc<Mutex<ChecksumStatus>>, total: usize) {
    if let Ok(mut g) = status.lock() {
        g.total_files = total;
    }
}

fn begin_file(status: &Arc<Mutex<ChecksumStatus>>, entry: &FileEntry) {
    if let Ok(mut g) = status.lock() {
        g.current_file = entry.name.clone();
        g.current_bytes = 0;
        g.current_total = entry.size;
    }
}

fn finish_file<E: std::fmt::Display>(
    status: &Arc<Mutex<ChecksumStatus>>,
    entry: &FileEntry,
    result: Result<ChecksumSet, E>,
) {
    if let Ok(mut g) = status.lock() {
        let (set, error) = match result {
            Ok(s) => (Some(s), None),
            Err(e) => (None, Some(format!("{e}"))),
        };
        g.results.push(FileChecksum {
            name: entry.name.clone(),
            size: entry.size,
            set,
            error,
        });
        g.done_files += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn set_of(data: &[u8]) -> ChecksumSet {
        let mut r = Cursor::new(data.to_vec());
        hash_reader(&mut r, &|_| {}).unwrap()
    }

    #[test]
    fn empty_input_matches_known_vectors() {
        let s = set_of(b"");
        assert_eq!(s.crc32_hex(), "00000000");
        assert_eq!(s.md5_hex(), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(s.sha1_hex(), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(
            s.sha256_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn abc_matches_known_vectors() {
        let s = set_of(b"abc");
        // CRC32 of "abc" (IEEE) = 0x352441c2.
        assert_eq!(s.crc32_hex(), "352441c2");
        assert_eq!(s.md5_hex(), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(s.sha1_hex(), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(
            s.sha256_hex(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn progress_reports_running_total() {
        // 3 MiB of data -> progress called with a monotonic, increasing total
        // ending at the full length.
        let data = vec![0xABu8; 3 * 1024 * 1024];
        let mut r = Cursor::new(data.clone());
        let seen = std::cell::RefCell::new(Vec::new());
        let _ = hash_reader(&mut r, &|b| seen.borrow_mut().push(b)).unwrap();
        let seen = seen.into_inner();
        assert!(!seen.is_empty());
        assert_eq!(*seen.last().unwrap(), data.len() as u64);
        assert!(seen.windows(2).all(|w| w[0] < w[1]), "monotonic: {seen:?}");
    }

    #[test]
    fn writer_and_reader_paths_agree() {
        // The ChecksumHasher Write sink must produce the same digests as the
        // hash_reader path for the same bytes (the image path uses the writer).
        let data = b"the quick brown fox";
        let from_reader = set_of(data);
        let mut hasher = ChecksumHasher::new();
        hasher.write_all(data).unwrap();
        let from_writer = hasher.finalize();
        assert!(from_reader == from_writer);
    }
}
