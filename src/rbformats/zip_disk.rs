//! Read-only opener for a RAW disk image stored inside a plain `.zip`
//! archive.
//!
//! Unlike WinImage `.imz` (a ZIP holding a *floppy* image, decompressed
//! into RAM by [`crate::rbformats::imz`]), a `.zip` here may hold a full
//! hard-disk image that is hundreds of MB or several GB. So the chosen
//! entry is inflated to a temp file once at open time and random-accessed
//! from there — the same shape as `source_reader`'s `GzipTempReader`.
//! ZIP deflate streams are not seekable, so the one-time inflate is
//! unavoidable; the temp file is removed when the reader drops.
//!
//! Entry selection — "which file in the archive is the disk?":
//!   * exactly one entry whose name has a disk-image extension  -> pick it
//!   * more than one such entry                                 -> error,
//!     list candidates, suggest `--inside <name>`
//!   * no disk-extension entry but exactly one regular file     -> pick it
//!   * otherwise                                                -> error
//!
//! Passing `inside = Some(name)` overrides the heuristic and selects the
//! named entry (exact, then case-insensitive, then basename match).

use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};

/// ZIP local-file-header signature.
pub const ZIP_MAGIC: &[u8; 4] = b"PK\x03\x04";

/// Block size for zero-run detection when writing the decompressed image
/// sparsely. 64 KiB balances sparseness — a hole of this size skips physical
/// allocation on APFS / ext4 / NTFS / XFS — against syscall overhead.
const SPARSE_BLOCK: usize = 64 * 1024;

/// Fill `buf` from `src`, looping over short reads (and EINTR) until `buf` is
/// full or EOF. Returns the number of bytes read (`buf.len()` unless EOF).
fn read_block(src: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match src.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

/// Copy the decompressed disk image `src` into `dst`, punching holes for runs
/// of zeros instead of writing them. Vintage disk images are mostly free space
/// (the motivating IRIX image is ~89% zeros), so this keeps physical
/// allocation to the non-zero content while the file's logical size stays
/// full. Returns the total logical length (== the decompressed size).
///
/// On a filesystem without sparse-file support the holes simply become real
/// zero bytes — same result as a plain copy, no regression.
fn sparse_copy(mut src: impl Read, dst: &mut File) -> io::Result<u64> {
    let mut buf = vec![0u8; SPARSE_BLOCK];
    let mut pos: u64 = 0;
    loop {
        let n = read_block(&mut src, &mut buf)?;
        if n == 0 {
            break;
        }
        let chunk = &buf[..n];
        if !chunk.iter().all(|&b| b == 0) {
            dst.seek(io::SeekFrom::Start(pos))?;
            dst.write_all(chunk)?;
        }
        pos += n as u64;
        if n < buf.len() {
            break;
        }
    }
    // Extend to the full logical length so a trailing zero run is represented
    // as a hole rather than truncating the image.
    dst.set_len(pos)?;
    Ok(pos)
}

/// Disk-image extensions recognised *inside* a `.zip` when auto-picking.
/// Whatever we pick is handed to partition detection, which validates it,
/// so this only has to be good enough to disambiguate the disk from
/// sidecar files (readme, checksum, ...). Matched case-insensitively.
fn is_disk_image_entry(name: &str) -> bool {
    const EXTS: &[&str] = &[
        ".img", ".raw", ".dd", ".iso", ".hdd", ".hda", ".hdv", ".dsk", ".vhd", ".hdf", ".hds",
        ".ima", ".vmdk", ".qcow2", ".chd",
    ];
    let lower = name.to_ascii_lowercase();
    EXTS.iter().any(|e| lower.ends_with(e))
}

#[derive(Clone)]
struct EntryInfo {
    index: usize,
    name: String,
    size: u64,
}

fn collect_entries<R: Read + Seek>(archive: &mut zip::ZipArchive<R>) -> Vec<EntryInfo> {
    let mut out = Vec::new();
    for i in 0..archive.len() {
        if let Ok(e) = archive.by_index_raw(i) {
            if e.is_dir() {
                continue;
            }
            out.push(EntryInfo {
                index: i,
                name: e.name().to_string(),
                size: e.size(),
            });
        }
    }
    out
}

fn list_entries(entries: &[EntryInfo]) -> String {
    entries
        .iter()
        .map(|e| format!("  {} ({} bytes)", e.name, e.size))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Resolve which archive entry to open. See the module docs for the rules.
fn pick_entry<R: Read + Seek>(
    archive: &mut zip::ZipArchive<R>,
    inside: Option<&str>,
) -> Result<usize> {
    let entries = collect_entries(archive);
    if entries.is_empty() {
        bail!("ZIP archive contains no files");
    }

    if let Some(want) = inside {
        let want_base = want.rsplit(['/', '\\']).next().unwrap_or(want);
        let found = entries
            .iter()
            .find(|e| e.name == want)
            .or_else(|| entries.iter().find(|e| e.name.eq_ignore_ascii_case(want)))
            .or_else(|| {
                entries.iter().find(|e| {
                    let base = e.name.rsplit(['/', '\\']).next().unwrap_or(&e.name);
                    base.eq_ignore_ascii_case(want_base)
                })
            });
        return match found {
            Some(e) => Ok(e.index),
            None => bail!(
                "--inside \"{want}\" not found in archive. Entries:\n{}",
                list_entries(&entries)
            ),
        };
    }

    let disk: Vec<&EntryInfo> = entries
        .iter()
        .filter(|e| is_disk_image_entry(&e.name))
        .collect();
    match disk.len() {
        1 => Ok(disk[0].index),
        0 => {
            if entries.len() == 1 {
                Ok(entries[0].index)
            } else {
                bail!(
                    "ZIP has no obvious disk image and contains multiple files; \
                     pass --inside <name> to pick one. Entries:\n{}",
                    list_entries(&entries)
                )
            }
        }
        _ => {
            let cands: Vec<EntryInfo> = disk.into_iter().cloned().collect();
            bail!(
                "ZIP contains multiple disk images; pass --inside <name> to pick one. \
                 Candidates:\n{}",
                list_entries(&cands)
            )
        }
    }
}

/// A `Read + Seek` view over a disk image extracted from a `.zip`. The
/// chosen entry is inflated to a temp file at [`ZipDiskReader::open`] time;
/// the temp file is removed when this reader is dropped.
#[derive(Debug)]
pub struct ZipDiskReader {
    file: File,
    _temp: tempfile::TempPath,
    entry_name: String,
    logical_size: u64,
}

impl ZipDiskReader {
    /// Open `path`, auto-picking the disk-image entry.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with(path, None, None)
    }

    /// Open `path`, optionally decrypting with `password` and/or selecting
    /// a specific entry with `inside`.
    pub fn open_with(path: &Path, password: Option<&[u8]>, inside: Option<&str>) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("opening ZIP {}", path.display()))?;
        let mut archive = zip::ZipArchive::new(file)
            .with_context(|| format!("parsing {} as ZIP", path.display()))?;
        let idx = pick_entry(&mut archive, inside)?;

        let (encrypted, raw_name) = {
            let e = archive.by_index_raw(idx)?;
            (e.encrypted(), e.name().to_string())
        };
        if encrypted && password.is_none() {
            bail!(
                "ZIP entry \"{raw_name}\" is password-protected; supply a password \
                 (--password on the CLI, or the GUI password prompt)"
            );
        }

        // Inflate the chosen entry into a temp file. Disk images can be large
        // (and mostly zeros), so we never hold it in RAM and we write it
        // sparsely — only the non-zero blocks consume physical space.
        let mut temp = tempfile::Builder::new()
            .prefix(".rb-zip-disk-")
            .tempfile()
            .context("create zip-decode tempfile")?;

        let (entry_name, logical_size) = if encrypted {
            let pw = password.expect("checked above");
            let mut entry = archive
                .by_index_decrypt(idx, pw)
                .map_err(|e| anyhow!("decrypting ZIP entry \"{raw_name}\": {e}"))?;
            let name = entry.name().to_string();
            let size = entry.size();
            sparse_copy(&mut entry, temp.as_file_mut())
                .with_context(|| format!("inflating ZIP entry \"{name}\""))?;
            (name, size)
        } else {
            let mut entry = archive.by_index(idx)?;
            let name = entry.name().to_string();
            let size = entry.size();
            sparse_copy(&mut entry, temp.as_file_mut())
                .with_context(|| format!("inflating ZIP entry \"{name}\""))?;
            (name, size)
        };

        temp.as_file().sync_all().ok();
        let temp_path = temp.into_temp_path();
        let file = File::open(&temp_path).context("reopen decoded ZIP disk image")?;

        log::info!(
            "Opened ZIP {} -> entry \"{}\" ({} bytes)",
            path.display(),
            entry_name,
            logical_size
        );

        Ok(Self {
            file,
            _temp: temp_path,
            entry_name,
            logical_size,
        })
    }

    pub fn entry_name(&self) -> &str {
        &self.entry_name
    }

    pub fn logical_size(&self) -> u64 {
        self.logical_size
    }
}

impl Read for ZipDiskReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for ZipDiskReader {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::SeekFrom;
    use std::path::PathBuf;
    use zip::write::SimpleFileOptions;

    fn build_zip(entries: &[(&str, &[u8])]) -> PathBuf {
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("test.zip");
        let f = File::create(&path).unwrap();
        let mut zw = zip::ZipWriter::new(f);
        let opts =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        for (name, payload) in entries {
            zw.start_file(*name, opts).unwrap();
            zw.write_all(payload).unwrap();
        }
        zw.finish().unwrap();
        path
    }

    #[test]
    fn single_disk_entry_is_auto_picked() {
        let payload: Vec<u8> = (0..4096).map(|i| (i * 37) as u8).collect();
        let zip = build_zip(&[("backup.img", &payload)]);
        let mut r = ZipDiskReader::open(&zip).unwrap();
        assert_eq!(r.entry_name(), "backup.img");
        assert_eq!(r.logical_size(), payload.len() as u64);
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn disk_picked_over_sidecar_files() {
        let payload = b"disk bytes here";
        let zip = build_zip(&[
            ("README.txt", b"notes"),
            ("disk.RAW", payload),
            ("disk.raw.sha256", b"deadbeef"),
        ]);
        let r = ZipDiskReader::open(&zip).unwrap();
        assert_eq!(r.entry_name(), "disk.RAW");
    }

    #[test]
    fn single_unknown_extension_entry_is_picked() {
        let payload = b"some flat image";
        let zip = build_zip(&[("mystery.bin", payload)]);
        let r = ZipDiskReader::open(&zip).unwrap();
        assert_eq!(r.entry_name(), "mystery.bin");
    }

    #[test]
    fn multiple_disk_images_error_with_hint() {
        let zip = build_zip(&[("a.img", b"aaaa"), ("b.img", b"bbbb")]);
        let err = ZipDiskReader::open(&zip).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("multiple disk images"), "{msg}");
        assert!(msg.contains("--inside"), "{msg}");
        assert!(msg.contains("a.img") && msg.contains("b.img"), "{msg}");
    }

    #[test]
    fn inside_selects_named_entry_case_insensitively() {
        let want: Vec<u8> = (0..1024).map(|i| (i ^ 0x5a) as u8).collect();
        let zip = build_zip(&[("a.img", b"aaaa"), ("b.img", &want)]);
        let mut r = ZipDiskReader::open_with(&zip, None, Some("B.IMG")).unwrap();
        assert_eq!(r.entry_name(), "b.img");
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got, want);
    }

    #[test]
    fn inside_matches_basename_inside_subdir() {
        let zip = build_zip(&[("dumps/disk.img", b"xyz"), ("notes.txt", b"hi")]);
        let r = ZipDiskReader::open_with(&zip, None, Some("disk.img")).unwrap();
        assert_eq!(r.entry_name(), "dumps/disk.img");
    }

    #[test]
    fn inside_not_found_lists_entries() {
        let zip = build_zip(&[("a.img", b"aaaa"), ("b.img", b"bbbb")]);
        let err = ZipDiskReader::open_with(&zip, None, Some("c.img")).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("not found"), "{msg}");
        assert!(msg.contains("a.img") && msg.contains("b.img"), "{msg}");
    }

    #[test]
    fn empty_archive_errors() {
        let zip = build_zip(&[]);
        let err = ZipDiskReader::open(&zip).unwrap_err();
        assert!(format!("{err}").contains("no files"));
    }

    #[test]
    fn reader_is_seekable() {
        let payload: Vec<u8> = (0..8192).map(|i| (i * 11) as u8).collect();
        let zip = build_zip(&[("d.img", &payload)]);
        let mut r = ZipDiskReader::open(&zip).unwrap();
        r.seek(SeekFrom::Start(4096)).unwrap();
        let mut buf = vec![0u8; 256];
        r.read_exact(&mut buf).unwrap();
        assert_eq!(buf, payload[4096..4096 + 256]);
    }

    #[test]
    fn sparse_copy_preserves_interior_zero_run() {
        // An image with a large interior hole: [data][256 KiB zeros][data].
        // The sparse writer must skip the zeros yet read them back as zeros,
        // and the surrounding data must land at the correct offsets.
        let head: Vec<u8> = (0..1000u32).map(|i| (i % 251 + 1) as u8).collect();
        let gap = vec![0u8; 256 * 1024];
        let tail: Vec<u8> = (0..1000u32).map(|i| (i % 239 + 3) as u8).collect();
        let mut payload = Vec::new();
        payload.extend_from_slice(&head);
        payload.extend_from_slice(&gap);
        payload.extend_from_slice(&tail);

        let zip = build_zip(&[("sparse.img", &payload)]);
        let mut r = ZipDiskReader::open(&zip).unwrap();
        assert_eq!(r.logical_size(), payload.len() as u64);

        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got.len(), payload.len());
        assert_eq!(got, payload, "interior hole corrupted the image");

        // Spot-check a read that starts inside the hole and crosses into tail.
        let tail_start = (head.len() + gap.len()) as u64;
        r.seek(SeekFrom::Start(tail_start - 16)).unwrap();
        let mut buf = vec![0u8; 32];
        r.read_exact(&mut buf).unwrap();
        assert_eq!(&buf[..16], &[0u8; 16][..], "hole tail should read zeros");
        assert_eq!(&buf[16..], &tail[..16], "data after hole misaligned");
    }

    #[test]
    fn temp_file_removed_on_drop() {
        let zip = build_zip(&[("d.img", b"hello world")]);
        let r = ZipDiskReader::open(&zip).unwrap();
        // The reader holds an open fd on a tempfile; once dropped the
        // backing TempPath is removed. We can't easily observe the path,
        // so just confirm a second independent open still works (no lock
        // contention / leftover-state issues).
        drop(r);
        let r2 = ZipDiskReader::open(&zip).unwrap();
        assert_eq!(r2.entry_name(), "d.img");
    }
}
