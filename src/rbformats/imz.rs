//! WinImage IMZ (`.imz`) reader.
//!
//! IMZ is a tiny ZIP archive containing a single floppy image (`.ima` /
//! `.img`). The reader walks the archive, picks the first non-directory
//! entry, and extracts it to a fresh tempdir. Callers hold the returned
//! [`tempfile::TempDir`] guard for the lifetime of the open image so the
//! decoded file lives long enough to be inspected/browsed.
//!
//! Password-protected archives are detected and rejected with a precise
//! error. WinImage 6.0+ encrypts the (already-deflated) entry with its own
//! scheme — MD5(password) -> 128-bit key, then Rijndael, preceded by a
//! 135-byte header — which is NOT PKWARE ZipCrypto and NOT WinZip-AES, so
//! the `zip` crate cannot decrypt it. A known-plaintext analysis ruled out
//! every standard Rijndael mode/key-derivation; see `docs/imz_encryption.md`.
//! Until the scheme is reverse-engineered from the WinImage binary, encrypted
//! IMZ files are unsupported.

use std::fs::File;
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};

/// IMZ magic = ZIP local file header signature.
pub const IMZ_MAGIC: &[u8; 4] = b"PK\x03\x04";

/// Sniff whether `file` *might* be an IMZ. Cheap check used by the detection
/// ladder before paying for `ZipArchive::new`.
///
/// Leaves the cursor undefined (caller should reset).
pub fn looks_like_imz(file: &mut File) -> bool {
    if file.seek(SeekFrom::Start(0)).is_err() {
        return false;
    }
    let mut magic = [0u8; 4];
    matches!(file.read_exact(&mut magic), Ok(())) && &magic == IMZ_MAGIC
}

/// Result of materializing an IMZ.
#[derive(Debug)]
pub struct ImzMaterialized {
    /// Path to the extracted floppy image inside the tempdir.
    pub temp_path: PathBuf,
    /// Logical size of the decoded image (== file size at `temp_path`).
    pub logical_size: u64,
    /// Tempdir guard. When dropped, the materialized file is removed.
    /// Callers (GUI tab / inspect flow) must keep this alive for as long
    /// as they need the materialized file.
    pub guard: tempfile::TempDir,
    /// Name of the entry inside the archive (for logging).
    pub entry_name: String,
}

fn is_image_entry(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.ends_with(".ima")
        || lower.ends_with(".img")
        || lower.ends_with(".dsk")
        || lower.ends_with(".flp")
        || lower.ends_with(".vfd")
}

/// Returns `true` if the archive's chosen entry is encrypted.
pub fn imz_needs_password(path: &Path) -> Result<bool> {
    let file = File::open(path).with_context(|| format!("opening IMZ {}", path.display()))?;
    let mut archive = zip::ZipArchive::new(file)
        .with_context(|| format!("parsing IMZ {} as ZIP", path.display()))?;
    let idx = pick_entry_index(&mut archive)?;
    let entry = archive.by_index_raw(idx)?;
    Ok(entry.encrypted())
}

fn pick_entry_index<R: Read + Seek>(archive: &mut zip::ZipArchive<R>) -> Result<usize> {
    if archive.is_empty() {
        bail!("IMZ contains no entries");
    }
    let mut chosen: Option<usize> = None;
    let mut fallback: Option<usize> = None;
    for i in 0..archive.len() {
        if let Ok(entry) = archive.by_index_raw(i) {
            if entry.is_dir() {
                continue;
            }
            if is_image_entry(entry.name()) {
                chosen = Some(i);
                break;
            }
            if fallback.is_none() {
                fallback = Some(i);
            }
        }
    }
    chosen
        .or(fallback)
        .ok_or_else(|| anyhow!("IMZ has no extractable file entry"))
}

/// Open the chosen entry from the archive, optionally decrypting with
/// `password`. Returns the decompressed entry + its metadata.
fn open_entry<'a>(
    archive: &'a mut zip::ZipArchive<File>,
    idx: usize,
    password: Option<&[u8]>,
    path: &Path,
) -> Result<(zip::read::ZipFile<'a, File>, String, u64)> {
    if let Some(pw) = password {
        // The `zip` crate only knows ZipCrypto and WinZip-AES. Real WinImage
        // IMZ files use a proprietary MD5+Rijndael scheme that neither matches
        // (see docs/imz_encryption.md), so this only succeeds on the rare
        // standards-compliant IMZ. On failure, say so plainly rather than
        // implying the password was wrong.
        let entry = archive.by_index_decrypt(idx, pw).map_err(|e| {
            anyhow!(
                "IMZ {} could not be decrypted ({e}). WinImage's own \
                 password encryption (MD5 + Rijndael) is not yet supported; \
                 this is unrelated to whether the password is correct.",
                path.display(),
            )
        })?;
        let name = entry.name().to_string();
        let size = entry.size();
        Ok((entry, name, size))
    } else {
        // Check encryption via raw access first (by_index errors on
        // encrypted entries in zip 2.x before we can inspect the flag).
        {
            let raw = archive.by_index_raw(idx)?;
            if raw.encrypted() {
                let entry_name = raw.name().to_string();
                bail!(
                    "IMZ {} is password-protected (entry \"{entry_name}\")",
                    path.display(),
                );
            }
        }
        let entry = archive.by_index(idx)?;
        let name = entry.name().to_string();
        let size = entry.size();
        Ok((entry, name, size))
    }
}

/// Open `path` as an IMZ archive and extract its floppy image to a fresh
/// tempdir. Pass `password` to decrypt password-protected archives.
/// The returned [`ImzMaterialized::guard`] must be kept alive for
/// the lifetime of the materialized file.
pub fn materialize_imz_to_temp(path: &Path) -> Result<ImzMaterialized> {
    materialize_imz_to_temp_with_password(path, None)
}

pub fn materialize_imz_to_temp_with_password(
    path: &Path,
    password: Option<&[u8]>,
) -> Result<ImzMaterialized> {
    let file = File::open(path).with_context(|| format!("opening IMZ {}", path.display()))?;
    let mut archive = zip::ZipArchive::new(file)
        .with_context(|| format!("parsing IMZ {} as ZIP", path.display()))?;
    let idx = pick_entry_index(&mut archive)?;

    let (mut entry, entry_name, logical_size) = open_entry(&mut archive, idx, password, path)?;

    let guard = tempfile::tempdir().with_context(|| "creating tempdir for IMZ materialization")?;
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("imz");
    let out_path = guard.path().join(format!("{}.ima", stem));
    let mut out =
        File::create(&out_path).with_context(|| format!("creating {}", out_path.display()))?;
    let copied = io::copy(&mut entry, &mut out)
        .with_context(|| format!("decompressing IMZ entry \"{}\"", entry_name))?;
    out.sync_all().ok();

    log::info!(
        "Materialized IMZ {} -> {} ({} bytes, entry \"{}\")",
        path.display(),
        out_path.display(),
        copied,
        entry_name
    );

    Ok(ImzMaterialized {
        temp_path: out_path,
        logical_size,
        guard,
        entry_name,
    })
}

// ---------------------------------------------------------------------------
// ImzReader — streaming Read+Seek over an IMZ (eager-decompress into memory)
// ---------------------------------------------------------------------------
//
// Floppy images are tiny (1.44 / 2.88 MB), so the entire entry is
// decompressed into a Vec<u8> at open time. The win versus
// `materialize_imz_to_temp` is the absent tempfile/TempDir lifecycle:
// callers no longer have to keep a TempDir guard alive, and there's no
// filesystem write/sync. Matches the shape of `GhoReader` / `ChdReader`
// so `model::source_reader::open_read` can route uniformly.

/// `Read + Seek` over the decoded floppy image inside an IMZ.
///
/// Memory-backed (`Cursor<Vec<u8>>`); the entire entry is decompressed
/// at [`ImzReader::open`] time. Floppy images don't exceed a few MB,
/// so this is cheaper than writing to a tempfile.
pub struct ImzReader {
    cursor: Cursor<Vec<u8>>,
    entry_name: String,
    logical_size: u64,
}

impl ImzReader {
    /// Open an IMZ and eagerly decompress its single floppy-image entry
    /// into memory. Errors mirror [`materialize_imz_to_temp`] (empty
    /// archive, no extractable entry, password-protected without
    /// supplying a password).
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_password(path, None)
    }

    pub fn open_with_password(path: &Path, password: Option<&[u8]>) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("opening IMZ {}", path.display()))?;
        let mut archive = zip::ZipArchive::new(file)
            .with_context(|| format!("parsing IMZ {} as ZIP", path.display()))?;
        let idx = pick_entry_index(&mut archive)?;

        let (mut entry, entry_name, logical_size) = open_entry(&mut archive, idx, password, path)?;
        let mut buf = Vec::with_capacity(logical_size as usize);
        io::copy(&mut entry, &mut buf)
            .with_context(|| format!("decompressing IMZ entry \"{}\"", entry_name))?;

        Ok(Self {
            cursor: Cursor::new(buf),
            entry_name,
            logical_size,
        })
    }

    pub fn logical_size(&self) -> u64 {
        self.logical_size
    }

    pub fn entry_name(&self) -> &str {
        &self.entry_name
    }
}

impl Read for ImzReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.cursor.read(buf)
    }
}

impl Seek for ImzReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.cursor.seek(pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use zip::write::SimpleFileOptions;

    fn build_imz(entry_name: &str, payload: &[u8]) -> PathBuf {
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("test.imz");
        let f = File::create(&path).unwrap();
        let mut zw = zip::ZipWriter::new(f);
        let opts =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        zw.start_file(entry_name, opts).unwrap();
        zw.write_all(payload).unwrap();
        zw.finish().unwrap();
        path
    }

    #[test]
    fn round_trip_imz_with_ima_entry() {
        let payload: Vec<u8> = (0..4096).map(|i| (i * 37) as u8).collect();
        let imz = build_imz("disk.ima", &payload);
        let out = materialize_imz_to_temp(&imz).unwrap();
        assert_eq!(out.logical_size, payload.len() as u64);
        assert!(out.temp_path.exists());
        let got = std::fs::read(&out.temp_path).unwrap();
        assert_eq!(got, payload);
        // Guard drop removes the tempdir.
        let p = out.temp_path.clone();
        drop(out);
        assert!(!p.exists(), "tempdir should be cleaned up when guard drops");
    }

    #[test]
    fn picks_image_entry_over_other_files() {
        // IMZ-in-the-wild has exactly one entry, but if a future archive
        // ever carried a sidecar txt we should still pick the .ima.
        let payload = b"floppy bytes";
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("test.imz");
        let f = File::create(&path).unwrap();
        let mut zw = zip::ZipWriter::new(f);
        let opts = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
        zw.start_file("README.txt", opts).unwrap();
        zw.write_all(b"a sidecar").unwrap();
        zw.start_file("floppy.IMA", opts).unwrap();
        zw.write_all(payload).unwrap();
        zw.finish().unwrap();
        let out = materialize_imz_to_temp(&path).unwrap();
        assert_eq!(out.entry_name, "floppy.IMA");
        assert_eq!(std::fs::read(&out.temp_path).unwrap(), payload);
    }

    #[test]
    fn empty_archive_errors() {
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("empty.imz");
        let f = File::create(&path).unwrap();
        let zw = zip::ZipWriter::new(f);
        zw.finish().unwrap();
        let err = materialize_imz_to_temp(&path).unwrap_err();
        assert!(format!("{err}").contains("no entries"));
    }

    #[test]
    fn imz_reader_round_trips_payload() {
        let payload: Vec<u8> = (0..4096).map(|i| (i * 37) as u8).collect();
        let imz = build_imz("disk.ima", &payload);
        let mut r = ImzReader::open(&imz).unwrap();
        assert_eq!(r.logical_size(), payload.len() as u64);
        assert_eq!(r.entry_name(), "disk.ima");

        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got, payload);

        // Random-access seek.
        r.seek(SeekFrom::Start(2048)).unwrap();
        let mut sample = vec![0u8; 256];
        r.read_exact(&mut sample).unwrap();
        assert_eq!(sample, payload[2048..2048 + 256]);
    }

    #[test]
    fn imz_reader_matches_materialize_to_temp_byte_for_byte() {
        let payload: Vec<u8> = (0..8192).map(|i| (i ^ 0xA5) as u8).collect();
        let imz = build_imz("disk.IMG", &payload);
        let via_reader = {
            let mut r = ImzReader::open(&imz).unwrap();
            let mut buf = Vec::new();
            r.read_to_end(&mut buf).unwrap();
            buf
        };
        let via_temp = {
            let mat = materialize_imz_to_temp(&imz).unwrap();
            std::fs::read(&mat.temp_path).unwrap()
        };
        assert_eq!(via_reader, via_temp);
    }

    #[test]
    fn imz_reader_rejects_empty_archive() {
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("empty.imz");
        let f = File::create(&path).unwrap();
        let zw = zip::ZipWriter::new(f);
        zw.finish().unwrap();
        let err = ImzReader::open(&path).err().expect("must error");
        assert!(format!("{err}").contains("no entries"));
    }

    #[test]
    fn looks_like_imz_matches_pk_magic() {
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("d.imz");
        std::fs::write(&path, b"PK\x03\x04rest").unwrap();
        let mut f = File::open(&path).unwrap();
        assert!(looks_like_imz(&mut f));

        let path2 = dir.join("not.imz");
        std::fs::write(&path2, b"\x00\x00not zip").unwrap();
        let mut f2 = File::open(&path2).unwrap();
        assert!(!looks_like_imz(&mut f2));
    }
}
