//! Carve — a read-only *synthetic* filesystem for raw / unstructured images.
//!
//! Some disks carry no filesystem we can mount: custom bootblock disks
//! (Amiga demos, intros, and diagnostic tools that boot straight from the
//! boot block and write results to raw sectors), wiped media with leftover
//! data, or images whose filesystem we simply don't recognize. There is no
//! directory to browse — but there is often recoverable *content* sitting in
//! the bytes.
//!
//! `CarveFilesystem` doesn't pretend the disk has a structure it lacks. It
//! scans the image once and surfaces what is actually recognizable:
//!
//!   - `whole-disk.img` — the entire image, always present, so the user can
//!     always extract a byte-for-byte copy.
//!   - `bootblock.bin` — the first 1024 bytes, when they look like an Amiga
//!     boot block (`DOS` magic). Lets the user grab the boot code on its own.
//!   - `carved-blkNNNNNN.{jsonl,json,txt}` — one entry per maximal run of
//!     contiguous printable text found on the disk, named by the 512-byte
//!     block where the run starts. This is how the embedded `results.jsonl`
//!     of an NDOS diagnostic disk becomes extractable in-app.
//!
//! This is a recovery / inspection aid, not a real filesystem: the carve is
//! heuristic (printable-ASCII runs above a length threshold), read-only, and
//! makes no claim of completeness. It is reached for two superfloppy cases:
//! the `Amiga-NDOS` classification (a valid Amiga boot block with no AmigaDOS
//! root block) and any superfloppy whose filesystem we couldn't detect.

use std::cell::RefCell;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, Ordering};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

/// Streaming scan block; large enough to amortize syscalls, small enough to
/// keep memory flat regardless of image size (per the project's large-image
/// I/O guidance).
const SCAN_CHUNK: usize = 1 << 20; // 1 MiB

/// Default cap on how far into the image carve scans for text runs. Bounds
/// the cost of opening a large, filesystem-less device (a multi-GB SD card
/// whose filesystem we didn't recognize would otherwise be read end-to-end
/// just to list). The full image is always extractable via `whole-disk.img`
/// regardless of this cap — only the *text-run scan* is bounded. Override
/// per-process with [`set_full_scan`].
pub const DEFAULT_SCAN_LIMIT: u64 = 10 * 1024 * 1024; // 10 MiB

/// Process-wide "full scan" switch. When set, carve scans the entire image
/// instead of stopping at [`DEFAULT_SCAN_LIMIT`]. Driven by the GUI browse
/// view's "Full scan" toggle and the CLI `--carve-full` flag; tests use
/// [`CarveFilesystem::open_with_limit`] directly so they never touch it.
static FULL_SCAN: AtomicBool = AtomicBool::new(false);

/// Enable or disable whole-image carve scanning for this process.
pub fn set_full_scan(on: bool) {
    FULL_SCAN.store(on, Ordering::Relaxed);
}

/// Whether whole-image carve scanning is currently enabled.
pub fn full_scan_enabled() -> bool {
    FULL_SCAN.load(Ordering::Relaxed)
}

/// A carve-scan progress callback, invoked as `(bytes_scanned, bytes_to_scan)`.
pub type ProgressSink = Box<dyn FnMut(u64, u64)>;

thread_local! {
    /// Per-thread progress sink for the carve scan. Set by a caller that runs
    /// the open on a worker thread (the GUI browse-open worker) so a long full
    /// scan can paint a determinate progress bar instead of an indefinite
    /// spinner. `None` (the default) means progress is not reported.
    static SCAN_PROGRESS: RefCell<Option<ProgressSink>> = const { RefCell::new(None) };
}

/// Install a carve-scan progress sink on the **current thread**. The sink is
/// invoked as `(bytes_scanned, bytes_to_scan)` roughly once per [`SCAN_CHUNK`]
/// (1 MiB). Pass `None` to clear it. The carve scan must run on the same
/// thread the sink was installed on (it does — `open` is synchronous).
pub fn set_scan_progress(sink: Option<ProgressSink>) {
    SCAN_PROGRESS.with(|c| *c.borrow_mut() = sink);
}

fn report_progress(done: u64, total: u64) {
    SCAN_PROGRESS.with(|c| {
        if let Some(sink) = c.borrow_mut().as_mut() {
            sink(done, total);
        }
    });
}

/// Minimum length (bytes) of a printable-text run before we surface it as a
/// carved pseudo-file. Keeps short ASCII fragments inside machine code (a few
/// bytes of an error string in a code section) from cluttering the listing,
/// while still catching real payloads like an embedded JSONL log.
const MIN_TEXT_RUN: u64 = 64;

/// Upper bound on carved text entries, so a pathological image (e.g. a huge
/// all-text disk fragmented by stray non-text bytes) can't produce an
/// unbounded listing. `whole-disk.img` is always present regardless, so the
/// raw bytes are never out of reach even when this cap is hit.
const MAX_TEXT_ENTRIES: usize = 4096;

/// Classification of a carved text run, which picks the pseudo-file extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TextKind {
    /// Starts with `{` and spans multiple lines — newline-delimited JSON.
    JsonLines,
    /// Starts with `{` or `[` — a single JSON document.
    Json,
    /// Any other printable-text run.
    Text,
}

impl TextKind {
    fn ext(self) -> &'static str {
        match self {
            TextKind::JsonLines => "jsonl",
            TextKind::Json => "json",
            TextKind::Text => "txt",
        }
    }
}

/// A recoverable region of the image surfaced as one pseudo-file.
#[derive(Debug, Clone)]
struct CarveRegion {
    name: String,
    /// Absolute byte offset within the underlying reader.
    offset: u64,
    /// Length in bytes.
    size: u64,
}

pub struct CarveFilesystem<R: Read + Seek + Send> {
    reader: R,
    /// Total volume size in bytes.
    size: u64,
    /// How many bytes were actually scanned for text runs (≤ `size`).
    scanned: u64,
    /// All pseudo-files, in display order (whole-disk first).
    regions: Vec<CarveRegion>,
    /// Sum of carved text-run sizes — what we report as "used".
    used: u64,
    /// True when carved-text entries were capped at [`MAX_TEXT_ENTRIES`].
    truncated: bool,
}

/// A byte is "text" for carving purposes if it is printable ASCII or common
/// whitespace. Deliberately ASCII-only: JSON/JSONL and log text are ASCII,
/// and admitting arbitrary high bytes would carve noise out of binary code.
#[inline]
fn is_text_byte(b: u8) -> bool {
    matches!(b, b'\t' | b'\n' | b'\r') || (0x20..=0x7e).contains(&b)
}

/// Classify a run from a short prefix and whether it contains an interior
/// newline (a newline before the final byte).
fn classify(prefix: &[u8], has_interior_newline: bool) -> TextKind {
    let first = prefix.iter().copied().find(|b| !b.is_ascii_whitespace());
    match first {
        Some(b'{') if has_interior_newline => TextKind::JsonLines,
        Some(b'{') | Some(b'[') => TextKind::Json,
        _ => TextKind::Text,
    }
}

impl<R: Read + Seek + Send> CarveFilesystem<R> {
    /// Open with the process-wide scan policy: the whole image when
    /// [`full_scan_enabled`] is set, otherwise the first [`DEFAULT_SCAN_LIMIT`]
    /// bytes.
    pub fn open(reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let limit = if full_scan_enabled() {
            None
        } else {
            Some(DEFAULT_SCAN_LIMIT)
        };
        Self::open_with_limit(reader, partition_offset, limit)
    }

    /// Open, scanning at most `scan_limit` bytes for text runs (`None` = whole
    /// image). `whole-disk.img` and `bootblock.bin` are unaffected — only the
    /// carved-text listing is bounded.
    pub fn open_with_limit(
        mut reader: R,
        partition_offset: u64,
        scan_limit: Option<u64>,
    ) -> Result<Self, FilesystemError> {
        let end = reader.seek(SeekFrom::End(0))?;
        let size = end.saturating_sub(partition_offset);
        let scanned = match scan_limit {
            Some(limit) => limit.min(size),
            None => size,
        };

        // Peek the first sector to decide whether a `bootblock.bin` entry is
        // worth surfacing (Amiga `DOS` boot-block magic).
        reader.seek(SeekFrom::Start(partition_offset))?;
        let mut head = [0u8; 512];
        let head_read = fill(&mut reader, &mut head)?;
        let amiga_bootblock = head_read >= 4 && &head[0..3] == b"DOS" && head[3] <= 7;

        let (text_regions, used, truncated) =
            Self::scan_text_runs(&mut reader, partition_offset, scanned)?;

        // Assemble the listing: whole-disk first, then bootblock, then the
        // carved runs in on-disk order.
        let mut regions = Vec::with_capacity(text_regions.len() + 2);
        regions.push(CarveRegion {
            name: "whole-disk.img".to_string(),
            offset: partition_offset,
            size,
        });
        if amiga_bootblock {
            regions.push(CarveRegion {
                name: "bootblock.bin".to_string(),
                offset: partition_offset,
                size: 1024.min(size),
            });
        }
        regions.extend(text_regions);

        Ok(Self {
            reader,
            size,
            scanned,
            regions,
            used,
            truncated,
        })
    }

    /// Single streaming pass over `[partition_offset, partition_offset+size)`,
    /// coalescing maximal printable-text runs (across chunk boundaries) into
    /// carved regions. Returns the regions, their total byte count, and
    /// whether the entry cap was hit.
    fn scan_text_runs(
        reader: &mut R,
        partition_offset: u64,
        size: u64,
    ) -> Result<(Vec<CarveRegion>, u64, bool), FilesystemError> {
        reader.seek(SeekFrom::Start(partition_offset))?;

        let mut regions: Vec<CarveRegion> = Vec::new();
        let mut used: u64 = 0;
        let mut truncated = false;

        // In-progress run state, carried across chunks.
        let mut run_start: u64 = 0;
        let mut run_len: u64 = 0;
        let mut run_prefix: Vec<u8> = Vec::new();
        let mut run_interior_newline = false;
        let mut run_saw_newline = false;

        let mut buf = vec![0u8; SCAN_CHUNK];
        let mut pos: u64 = 0; // bytes consumed within the volume
        report_progress(0, size);

        // Finalize a completed run if it clears the length bar.
        let finalize = |start: u64,
                        len: u64,
                        prefix: &[u8],
                        interior_nl: bool,
                        regions: &mut Vec<CarveRegion>,
                        used: &mut u64|
         -> bool {
            if len < MIN_TEXT_RUN {
                return false;
            }
            if regions.len() >= MAX_TEXT_ENTRIES {
                return true; // signal truncation; stop emitting
            }
            let kind = classify(prefix, interior_nl);
            let block = (start - partition_offset) / 512;
            regions.push(CarveRegion {
                name: format!("carved-blk{:06}.{}", block, kind.ext()),
                offset: start,
                size: len,
            });
            *used += len;
            false
        };

        while pos < size {
            let want = SCAN_CHUNK.min((size - pos) as usize);
            let got = fill(reader, &mut buf[..want])?;
            if got == 0 {
                break; // short image / truncated reader
            }
            for &b in &buf[..got] {
                let abs = partition_offset + pos;
                if is_text_byte(b) {
                    if run_len == 0 {
                        run_start = abs;
                        run_prefix.clear();
                        run_saw_newline = false;
                        run_interior_newline = false;
                    } else if b == b'\n' {
                        // A newline that isn't the very first byte and has
                        // text after it later marks an interior newline; we
                        // approximate by recording any newline seen before a
                        // subsequent text byte.
                        run_saw_newline = true;
                    } else if run_saw_newline {
                        run_interior_newline = true;
                    }
                    if run_prefix.len() < 128 {
                        run_prefix.push(b);
                    }
                    run_len += 1;
                } else if run_len > 0 {
                    if finalize(
                        run_start,
                        run_len,
                        &run_prefix,
                        run_interior_newline,
                        &mut regions,
                        &mut used,
                    ) {
                        truncated = true;
                    }
                    run_len = 0;
                }
                pos += 1;
            }
            report_progress(pos, size);
        }
        // Flush a run that extends to EOF.
        if run_len > 0
            && finalize(
                run_start,
                run_len,
                &run_prefix,
                run_interior_newline,
                &mut regions,
                &mut used,
            )
        {
            truncated = true;
        }

        Ok((regions, used, truncated))
    }

    fn region_for(&self, entry: &FileEntry) -> Option<&CarveRegion> {
        self.regions
            .iter()
            .find(|r| r.offset == entry.location && r.size == entry.size && r.name == entry.name)
    }
}

impl<R: Read + Seek + Send> Filesystem for CarveFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        // Flat namespace: only the root holds entries.
        if entry.path != "/" {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        Ok(self
            .regions
            .iter()
            .map(|r| FileEntry::new_file(r.name.clone(), format!("/{}", r.name), r.size, r.offset))
            .collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let region = self
            .region_for(entry)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        let want = (region.size as usize).min(max_bytes);
        let mut out = vec![0u8; want];
        self.reader.seek(SeekFrom::Start(region.offset))?;
        let got = fill(&mut self.reader, &mut out)?;
        out.truncate(got);
        Ok(out)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let (offset, size) = {
            let region = self
                .region_for(entry)
                .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
            (region.offset, region.size)
        };
        self.reader.seek(SeekFrom::Start(offset))?;
        let mut remaining = size;
        let mut buf = vec![0u8; SCAN_CHUNK];
        let mut written = 0u64;
        while remaining > 0 {
            let want = SCAN_CHUNK.min(remaining as usize);
            let got = fill(&mut self.reader, &mut buf[..want])?;
            if got == 0 {
                break;
            }
            writer.write_all(&buf[..got])?;
            written += got as u64;
            remaining -= got as u64;
        }
        Ok(written)
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn fs_type(&self) -> &str {
        "Raw carve (no filesystem)"
    }

    fn total_size(&self) -> u64 {
        self.size
    }

    fn used_size(&self) -> u64 {
        self.used
    }
}

impl<R: Read + Seek + Send> CarveFilesystem<R> {
    /// True when the carved-text *entry list* was capped at
    /// [`MAX_TEXT_ENTRIES`] (raw bytes still reachable via `whole-disk.img`).
    pub fn was_truncated(&self) -> bool {
        self.truncated
    }

    /// Bytes actually scanned for text runs.
    pub fn scanned_bytes(&self) -> u64 {
        self.scanned
    }

    /// True when the text scan stopped before the end of the image (because
    /// the scan limit was below the image size). The user can re-open with
    /// full scan to cover the rest.
    pub fn scan_was_capped(&self) -> bool {
        self.scanned < self.size
    }
}

/// Read until `buf` is full or EOF; returns the number of bytes read. Unlike
/// `read_exact`, a short read at EOF is not an error — carve must tolerate
/// images whose length isn't a clean multiple of anything.
fn fill<R: Read>(reader: &mut R, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a 64 KiB image: an Amiga `DOS\0` boot block, a JSONL run at
    /// block 64, and a plain-text run at block 100.
    fn sample_image() -> (Vec<u8>, String, String) {
        let mut img = vec![0u8; 64 * 1024];
        img[0..4].copy_from_slice(b"DOS\0");

        let jsonl = "{\"reloc\":{\"root\":880}}\n{\"name\":\"PMOVE TC\",\"vec\":0}\n{\"name\":\"PFLUSHA\"}\n";
        let joff = 64 * 512;
        img[joff..joff + jsonl.len()].copy_from_slice(jsonl.as_bytes());

        let text =
            "This is a plain text region long enough to clear the threshold for carving. ABCDEFG.";
        let toff = 100 * 512;
        img[toff..toff + text.len()].copy_from_slice(text.as_bytes());

        (img, jsonl.to_string(), text.to_string())
    }

    #[test]
    fn carve_lists_whole_disk_and_bootblock() {
        let (img, _, _) = sample_image();
        let mut fs = CarveFilesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.contains(&"whole-disk.img".to_string()));
        assert!(names.contains(&"bootblock.bin".to_string()));
    }

    #[test]
    fn carve_finds_and_extracts_jsonl() {
        let (img, jsonl, _) = sample_image();
        let mut fs = CarveFilesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let jentry = entries
            .iter()
            .find(|e| e.name == "carved-blk000064.jsonl")
            .expect("JSONL run should be carved at block 64 with .jsonl extension");
        let bytes = fs.read_file(jentry, usize::MAX).unwrap();
        assert_eq!(String::from_utf8(bytes).unwrap(), jsonl);
    }

    #[test]
    fn carve_classifies_plain_text() {
        let (img, _, text) = sample_image();
        let mut fs = CarveFilesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let tentry = entries
            .iter()
            .find(|e| e.name == "carved-blk000100.txt")
            .expect("plain-text run should be carved at block 100 with .txt extension");
        // write_file_to should stream the exact bytes too.
        let mut out = Vec::new();
        fs.write_file_to(tentry, &mut out).unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), text);
    }

    #[test]
    fn carve_skips_short_runs() {
        // A 4 KiB image with only a tiny ASCII fragment (< MIN_TEXT_RUN)
        // should carve nothing but still expose whole-disk.img.
        let mut img = vec![0u8; 4096];
        img[100..110].copy_from_slice(b"shortbits!");
        let mut fs = CarveFilesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(names, vec!["whole-disk.img".to_string()]);
    }

    #[test]
    fn carve_scan_limit_bounds_the_scan() {
        // 16 KiB image with a text run starting at byte 8192. A 4 KiB scan
        // limit must NOT carve it (but whole-disk.img is still present and
        // the cap is reported); a full scan must find it.
        let text =
            "A recoverable text region living past the scan limit, long enough to clear MIN_TEXT_RUN.";
        let mut img = vec![0u8; 16 * 1024];
        img[8192..8192 + text.len()].copy_from_slice(text.as_bytes());

        // Capped scan: 4 KiB.
        let mut capped =
            CarveFilesystem::open_with_limit(Cursor::new(img.clone()), 0, Some(4096)).unwrap();
        assert!(capped.scan_was_capped());
        assert_eq!(capped.scanned_bytes(), 4096);
        let root = capped.root().unwrap();
        let names: Vec<String> = capped
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(names, vec!["whole-disk.img".to_string()]);

        // Full scan: limit None.
        let mut full = CarveFilesystem::open_with_limit(Cursor::new(img), 0, None).unwrap();
        assert!(!full.scan_was_capped());
        let root = full.root().unwrap();
        let found = full
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .any(|e| e.name.ends_with(".txt"));
        assert!(found, "full scan should carve the text past the cap");
    }

    #[test]
    fn carve_reports_scan_progress() {
        use std::rc::Rc;
        // 4 MiB image so the scan spans multiple 1 MiB chunks.
        let total = 4 * 1024 * 1024u64;
        let img = vec![0u8; total as usize];

        let calls: Rc<RefCell<Vec<(u64, u64)>>> = Rc::new(RefCell::new(Vec::new()));
        let sink_calls = Rc::clone(&calls);
        set_scan_progress(Some(Box::new(move |done, t| {
            sink_calls.borrow_mut().push((done, t));
        })));
        let _fs = CarveFilesystem::open_with_limit(Cursor::new(img), 0, None).unwrap();
        set_scan_progress(None); // clear for any later test on this thread

        let recorded = calls.borrow();
        assert!(!recorded.is_empty(), "progress sink should be called");
        // `total` is constant and equals the scanned size.
        assert!(recorded.iter().all(|&(_, t)| t == total));
        // `done` is non-decreasing and reaches the total.
        let mut prev = 0;
        for &(done, _) in recorded.iter() {
            assert!(done >= prev, "progress must be non-decreasing");
            prev = done;
        }
        assert_eq!(recorded.last().unwrap().0, total);
    }
}
