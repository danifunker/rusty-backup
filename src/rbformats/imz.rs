//! WinImage IMZ (`.imz`) reader.
//!
//! IMZ is a tiny ZIP archive containing a single floppy image (`.ima` /
//! `.img`). The reader walks the archive, picks the first non-directory
//! entry, and extracts it to a fresh tempdir. Callers hold the returned
//! [`tempfile::TempDir`] guard for the lifetime of the open image so the
//! decoded file lives long enough to be inspected/browsed.
//!
//! Password-protected archives are handled with WinImage's own scheme
//! (AES-128-CBC, key = MD5(password.utf-16-le), 32 KiB chunks with IV=0
//! reset per chunk, plus an 8-byte plaintext header and a 4-byte size
//! sidetable). The format was reverse-engineered from the WinImage 6.1
//! binary; see `docs/imz_encryption.md` for the full spec and the RE
//! narrative. Files that present as encrypted but don't follow that
//! framing (or where the password is wrong) surface as an explicit error.

use std::fs::File;
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use aes::Aes128;
use anyhow::{anyhow, bail, Context, Result};
// `cipher` crate 0.6 split the old `BlockDecryptMut` trait into
// `BlockModeDecrypt` (for block-mode types like cbc::Decryptor) and
// `BlockCipherDecrypt` (for the raw cipher). For CBC we want the former.
use cbc::cipher::{BlockModeDecrypt, KeyIvInit};
use md5::{Digest, Md5};

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

/// AES-128 key derivation per the WinImage IMZ spec:
/// `K = MD5(password.encode("utf-16-le"))`.
/// Password bytes are encoded as UTF-16LE *without* a trailing nul.
fn derive_winimage_key(password: &[u8]) -> [u8; 16] {
    // Treat the input as UTF-8; on decode failure (rare for "password"-style
    // ASCII inputs) fall back to interpreting each byte as a code point so we
    // still hash *something* deterministic rather than silently truncating.
    let s = std::str::from_utf8(password)
        .map(|s| s.to_string())
        .unwrap_or_else(|_| password.iter().map(|&b| b as char).collect());
    let mut utf16le = Vec::with_capacity(s.encode_utf16().count() * 2);
    for u in s.encode_utf16() {
        utf16le.push((u & 0xff) as u8);
        utf16le.push((u >> 8) as u8);
    }
    let digest = Md5::digest(&utf16le);
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest);
    out
}

/// Decrypt a WinImage password-protected entry body. Returns the raw
/// deflate stream embedded inside (which the caller should inflate with
/// `flate2::read::DeflateDecoder` to recover the floppy image).
///
/// Body layout (from `docs/imz_encryption.md`):
/// ```text
/// +0      8 bytes   per-file header (random; ignored on decrypt)
/// +8      u32 LE    chunk_size_0 in bytes (multiple of 16; typically 32768)
/// +12     chunk_size_0 bytes  chunk 0 ciphertext
/// ...     u32 LE + ct slice per chunk
/// end-4   u32 LE    real un-padded plaintext byte count of the last chunk
/// ```
/// Each chunk is AES-128-CBC with IV = 16 zeros (reset per chunk).
fn decrypt_winimage_body(body: &[u8], password: &[u8]) -> Result<Vec<u8>> {
    type Aes128CbcDec = cbc::Decryptor<Aes128>;
    const PREFIX: usize = 8;
    const TRAILER: usize = 4;

    if body.len() < PREFIX + 4 + 16 + TRAILER {
        bail!(
            "WinImage-encrypted IMZ body too short ({} bytes) to hold any chunks",
            body.len()
        );
    }

    let key = derive_winimage_key(password);
    let iv = [0u8; 16];
    let real_last_pt_len =
        u32::from_le_bytes(body[body.len() - TRAILER..].try_into().unwrap()) as usize;

    let end = body.len() - TRAILER;
    let mut out: Vec<u8> = Vec::with_capacity(body.len());
    let mut pos = PREFIX;
    while pos + 4 <= end {
        let ct_len = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if ct_len == 0 || !ct_len.is_multiple_of(16) {
            bail!(
                "WinImage IMZ: chunk ciphertext length {} at body[{}] is not a positive multiple of 16",
                ct_len,
                pos - 4
            );
        }
        if pos + ct_len > end {
            bail!(
                "WinImage IMZ: chunk at body[{}] of length {} overruns body (end={})",
                pos,
                ct_len,
                end
            );
        }
        let mut buf = body[pos..pos + ct_len].to_vec();
        pos += ct_len;
        let mut cipher = Aes128CbcDec::new(&key.into(), &iv.into());
        // decrypt_padded_mut requires PKCS#7-compatible padding which we
        // don't have here; decrypt the blocks in-place via the raw block API.
        // `aes::Block` is a type alias for `cipher::array::Array<u8, U16>` —
        // same memory layout as the old generic-array GenericArray<u8, U16>,
        // so the raw transmute is still sound.
        let blocks: &mut [aes::Block] = unsafe {
            std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut aes::Block, ct_len / 16)
        };
        cipher.decrypt_blocks(blocks);
        if pos >= end {
            // Last chunk: trim padding to the real plaintext length.
            if real_last_pt_len > buf.len() {
                bail!(
                    "WinImage IMZ: declared last-chunk pt length {} exceeds decrypted size {}",
                    real_last_pt_len,
                    buf.len()
                );
            }
            buf.truncate(real_last_pt_len);
            out.extend_from_slice(&buf);
        } else {
            out.extend_from_slice(&buf);
        }
    }
    if pos != end {
        bail!(
            "WinImage IMZ: {} bytes left in body before trailer (parser misaligned)",
            end - pos
        );
    }
    Ok(out)
}

/// Open the chosen entry from the archive, optionally decrypting with
/// `password`. Returns the decompressed entry + its metadata.
fn open_entry<'a>(
    archive: &'a mut zip::ZipArchive<File>,
    idx: usize,
    password: Option<&[u8]>,
    path: &Path,
) -> Result<(Box<dyn Read + 'a>, String, u64)> {
    if let Some(pw) = password {
        // Capture the entry's raw (still-encrypted) body up front. Almost all
        // password-protected IMZ files in the wild use WinImage's proprietary
        // scheme (see docs/imz_encryption.md); the `zip` crate's built-in
        // ZipCrypto / WinZip-AES paths are kept as a fallback in case
        // someone ever produces a standards-compliant encrypted IMZ.
        let (csz, name, size) = {
            let raw = archive.by_index_raw(idx)?;
            (raw.compressed_size(), raw.name().to_string(), raw.size())
        };
        let mut raw_body = Vec::with_capacity(csz as usize);
        archive.by_index_raw(idx)?.read_to_end(&mut raw_body)?;

        if let Ok(deflate_bytes) = decrypt_winimage_body(&raw_body, pw) {
            let dec = flate2::read::DeflateDecoder::new(Cursor::new(deflate_bytes));
            return Ok((Box::new(dec) as Box<dyn Read + 'a>, name, size));
        }
        // Fall back to standards-compliant ZipCrypto / WinZip-AES.
        let entry = archive.by_index_decrypt(idx, pw).map_err(|e| {
            anyhow!(
                "IMZ {} could not be decrypted ({e}). \
                 Wrong password, or an encryption scheme this build does not yet support.",
                path.display(),
            )
        })?;
        let name = entry.name().to_string();
        let size = entry.size();
        Ok((Box::new(entry) as Box<dyn Read + 'a>, name, size))
    } else {
        // Check encryption via raw access first (by_index errors on
        // encrypted entries in zip 2.x before we can inspect the flag).
        {
            let raw = archive.by_index_raw(idx)?;
            if raw.encrypted() {
                let entry_name = raw.name().to_string();
                bail!(
                    "IMZ {} is password-protected (entry \"{entry_name}\"); \
                     password must be specified (use the GUI password prompt, \
                     or pass --password on the CLI)",
                    path.display(),
                );
            }
        }
        let entry = archive.by_index(idx)?;
        let name = entry.name().to_string();
        let size = entry.size();
        Ok((Box::new(entry) as Box<dyn Read + 'a>, name, size))
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
    fn winimage_key_derivation_matches_known_vector() {
        // From the reverse-engineering trace: MD5 of UTF-16LE "password"
        // (16 bytes, no trailing nul) = b081dbe85e1ec3ffc3d4e7d0227400cd.
        let k = derive_winimage_key(b"password");
        let expected = [
            0xb0u8, 0x81, 0xdb, 0xe8, 0x5e, 0x1e, 0xc3, 0xff, 0xc3, 0xd4, 0xe7, 0xd0, 0x22, 0x74,
            0x00, 0xcd,
        ];
        assert_eq!(k, expected, "WinImage AES key derivation broke");
    }

    #[test]
    fn winimage_decrypt_roundtrips_a_synthetic_body() {
        // Build a small WinImage-format body around a known plaintext, then
        // feed it through decrypt_winimage_body and confirm we get the
        // plaintext back. Uses the same key derivation as the real format.
        use aes::cipher::{BlockModeEncrypt, KeyIvInit};
        type Aes128CbcEnc = cbc::Encryptor<aes::Aes128>;

        let key = derive_winimage_key(b"password");
        let iv = [0u8; 16];

        // Two chunks: chunk 0 is full (32768 bytes); chunk 1 is partial (100 bytes).
        let pt0: Vec<u8> = (0..32768).map(|i| (i * 31 + 7) as u8).collect();
        let pt1_real: Vec<u8> = (0..100).map(|i| (i * 13 + 91) as u8).collect();
        // Pad chunk 1 to multiple of 16.
        let pt1_padded_len = 112usize;
        let mut pt1_padded = pt1_real.clone();
        pt1_padded.resize(pt1_padded_len, 0xAA);

        let encrypt_chunk = |pt: &[u8]| -> Vec<u8> {
            let mut buf = pt.to_vec();
            let mut cipher = Aes128CbcEnc::new(&key.into(), &iv.into());
            let blocks: &mut [aes::Block] = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut aes::Block, pt.len() / 16)
            };
            cipher.encrypt_blocks(blocks);
            buf
        };
        let ct0 = encrypt_chunk(&pt0);
        let ct1 = encrypt_chunk(&pt1_padded);

        // Assemble body: 8-byte mystery + u32 size + ct0 + u32 size + ct1 + u32 trailer.
        let mut body = Vec::new();
        body.extend_from_slice(&[0u8; 8]);
        body.extend_from_slice(&(ct0.len() as u32).to_le_bytes());
        body.extend_from_slice(&ct0);
        body.extend_from_slice(&(ct1.len() as u32).to_le_bytes());
        body.extend_from_slice(&ct1);
        body.extend_from_slice(&(pt1_real.len() as u32).to_le_bytes());

        let got = decrypt_winimage_body(&body, b"password").unwrap();
        let mut want = Vec::new();
        want.extend_from_slice(&pt0);
        want.extend_from_slice(&pt1_real);
        assert_eq!(got, want);
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
