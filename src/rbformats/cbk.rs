//! `.cbk` — the chunked backup container: the single-file form of the native
//! PerPartition backup folder.
//!
//! This is the on-disk artifact specced in `docs/cb_dos_network_and_state.md`
//! §2 (the deferred network transport's container), brought to the desktop
//! first so a backup *folder* can be packed into one portable `.cbk` and a
//! `.cbk` restored/materialized back. Building it here **freezes the v1 format**
//! the future DOS network client (Family B, phase 7b) must match.
//!
//! ## Format (v1, big-endian throughout — the `RBK` network convention)
//!
//! ```text
//! [chunk][chunk]...[chunk] [index] [footer:16]
//! ```
//!
//! **Chunk** = 24-byte header + payload:
//! ```text
//!   magic       u32  = "RBKC" (0x5242_4B43)
//!   version     u16  = 1
//!   logical_id  u16  — which logical member (index into the index's table)
//!   src_offset  u64  — uncompressed offset of this span within the member
//!   len         u32  — payload byte length
//!   crc32       u32  — CRC32 of the payload bytes
//!   payload     [len] — an independent gzip member (multi-member per spec)
//! ```
//!
//! **Index** (trailer): `"RBKI"` u32, version u16, member_count u16, then per
//! member: `name_len` u16, `name` (UTF-8, folder-relative), `kind` u8
//! (0 = `Gz`: chunk payloads concatenated *are* the file verbatim — e.g.
//! `partition-N.gz`, already a gzip member; 1 = `Raw`: the file is
//! `gunzip(concatenated payloads)` — e.g. `metadata.json`), `chunk_count` u32,
//! then `chunk_count` × `chunk_file_offset` u64.
//!
//! **Footer** (last 16 bytes): `"RBKF"` u32, `index_offset` u64, `index_len` u32.
//! A reader seeks to EOF-16, reads it, and jumps to the index — no full scan.
//!
//! The desktop converter emits one chunk per member (the simplest valid
//! chunking); the network producer will append many chunks per partition for
//! finer resume. Both yield a valid `.cbk` this module materializes.

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

pub const CHUNK_MAGIC: u32 = 0x5242_4B43; // "RBKC"
pub const INDEX_MAGIC: u32 = 0x5242_4B49; // "RBKI"
pub const FOOTER_MAGIC: u32 = 0x5242_4B46; // "RBKF"
pub const CBK_VERSION: u16 = 1;
const CHUNK_HEADER_LEN: usize = 24;
const FOOTER_LEN: u64 = 16;

/// How a member's chunk payloads map back to its folder file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// Payloads concatenated *are* the file (already a gzip member on disk,
    /// e.g. `partition-N.gz`). Written verbatim on materialize.
    Gz = 0,
    /// The file is `gunzip(concatenated payloads)` (e.g. `metadata.json`,
    /// `mbr.bin`). gzip-wrapped on pack, decompressed on materialize.
    Raw = 1,
}

impl Kind {
    fn from_byte(b: u8) -> Result<Kind> {
        match b {
            0 => Ok(Kind::Gz),
            1 => Ok(Kind::Raw),
            other => bail!("cbk: unknown member kind {other}"),
        }
    }
    /// Files already stored as a gzip member in the folder are carried verbatim;
    /// everything else is gzip-wrapped so every chunk payload is a gzip member.
    fn for_file(name: &str) -> Kind {
        if name.to_ascii_lowercase().ends_with(".gz") {
            Kind::Gz
        } else {
            Kind::Raw
        }
    }
}

struct MemberOut {
    name: String,
    kind: Kind,
    logical_id: u16,
    chunk_offsets: Vec<u64>,
}

fn write_chunk_header(
    w: &mut impl Write,
    logical_id: u16,
    src_offset: u64,
    len: u32,
    crc32: u32,
) -> Result<()> {
    let mut hdr = [0u8; CHUNK_HEADER_LEN];
    hdr[0..4].copy_from_slice(&CHUNK_MAGIC.to_be_bytes());
    hdr[4..6].copy_from_slice(&CBK_VERSION.to_be_bytes());
    hdr[6..8].copy_from_slice(&logical_id.to_be_bytes());
    hdr[8..16].copy_from_slice(&src_offset.to_be_bytes());
    hdr[16..20].copy_from_slice(&len.to_be_bytes());
    hdr[20..24].copy_from_slice(&crc32.to_be_bytes());
    w.write_all(&hdr)?;
    Ok(())
}

/// Pack a native backup folder into a single `.cbk` container.
///
/// One chunk per file: `*.gz` files are carried verbatim (their bytes already
/// are a gzip member); every other file is gzip-wrapped. Large `*.gz` payloads
/// stream through without being held in RAM.
pub fn pack_folder_to_cbk(folder: &Path, out: &Path) -> Result<()> {
    let meta = folder.join("metadata.json");
    if !meta.is_file() {
        bail!(
            "{} is not a backup folder (no metadata.json)",
            folder.display()
        );
    }

    // Deterministic order: metadata.json first, then the rest sorted. (Order is
    // cosmetic — the index records exact offsets — but determinism is nice.)
    let mut files: Vec<String> = fs::read_dir(folder)
        .with_context(|| format!("reading {}", folder.display()))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .filter_map(|e| e.file_name().into_string().ok())
        .collect();
    files.sort();
    files.sort_by_key(|n| n != "metadata.json"); // metadata.json to the front

    let mut w =
        BufWriter::new(File::create(out).with_context(|| format!("creating {}", out.display()))?);
    let mut members: Vec<MemberOut> = Vec::new();

    for (i, name) in files.iter().enumerate() {
        let logical_id = i as u16;
        let kind = Kind::for_file(name);
        let path = folder.join(name);
        let chunk_off = w.stream_position()?;

        match kind {
            Kind::Gz => {
                // Verbatim: header (len from stat, crc back-patched after streaming).
                let len = fs::metadata(&path)?.len();
                let len_u32: u32 = len
                    .try_into()
                    .with_context(|| format!("{name}: gz member exceeds 4 GiB chunk limit"))?;
                write_chunk_header(&mut w, logical_id, 0, len_u32, 0)?;
                let mut f = BufReader::new(File::open(&path)?);
                let mut hasher = crc32fast::Hasher::new();
                let mut buf = vec![0u8; 256 * 1024];
                loop {
                    let n = f.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                    w.write_all(&buf[..n])?;
                }
                let crc = hasher.finalize();
                // Back-patch the crc field of this chunk header.
                let end = w.stream_position()?;
                w.seek(SeekFrom::Start(chunk_off + 20))?;
                w.write_all(&crc.to_be_bytes())?;
                w.seek(SeekFrom::Start(end))?;
            }
            Kind::Raw => {
                // gzip-wrap the raw file bytes into one member.
                let raw = fs::read(&path)?;
                let mut enc = GzEncoder::new(Vec::new(), Compression::default());
                enc.write_all(&raw)?;
                let payload = enc.finish()?;
                let crc = crc32fast::hash(&payload);
                let len_u32: u32 = payload
                    .len()
                    .try_into()
                    .with_context(|| format!("{name}: payload exceeds 4 GiB chunk limit"))?;
                write_chunk_header(&mut w, logical_id, 0, len_u32, crc)?;
                w.write_all(&payload)?;
            }
        }

        members.push(MemberOut {
            name: name.clone(),
            kind,
            logical_id,
            chunk_offsets: vec![chunk_off],
        });
    }

    // Index.
    let index_offset = w.stream_position()?;
    w.write_all(&INDEX_MAGIC.to_be_bytes())?;
    w.write_all(&CBK_VERSION.to_be_bytes())?;
    let member_count: u16 = members
        .len()
        .try_into()
        .context("cbk: too many members for a u16 count")?;
    w.write_all(&member_count.to_be_bytes())?;
    for m in &members {
        let name_bytes = m.name.as_bytes();
        let name_len: u16 = name_bytes
            .len()
            .try_into()
            .context("cbk: member name too long")?;
        w.write_all(&name_len.to_be_bytes())?;
        w.write_all(name_bytes)?;
        w.write_all(&[m.kind as u8])?;
        w.write_all(&m.logical_id.to_be_bytes())?;
        let chunk_count: u32 = m.chunk_offsets.len() as u32;
        w.write_all(&chunk_count.to_be_bytes())?;
        for &off in &m.chunk_offsets {
            w.write_all(&off.to_be_bytes())?;
        }
    }
    let index_len: u32 = (w.stream_position()? - index_offset)
        .try_into()
        .context("cbk: index larger than 4 GiB")?;

    // Footer (fixed 16 bytes at EOF).
    w.write_all(&FOOTER_MAGIC.to_be_bytes())?;
    w.write_all(&index_offset.to_be_bytes())?;
    w.write_all(&index_len.to_be_bytes())?;
    w.flush()?;
    Ok(())
}

struct MemberIn {
    name: String,
    kind: Kind,
    chunk_offsets: Vec<u64>,
}

fn read_u16(p: &[u8], at: usize) -> u16 {
    u16::from_be_bytes([p[at], p[at + 1]])
}
fn read_u32(p: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([p[at], p[at + 1], p[at + 2], p[at + 3]])
}
fn read_u64(p: &[u8], at: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&p[at..at + 8]);
    u64::from_be_bytes(b)
}

fn read_index(f: &mut File) -> Result<Vec<MemberIn>> {
    let file_len = f.metadata()?.len();
    if file_len < FOOTER_LEN {
        bail!("cbk: file too small to be a container");
    }
    f.seek(SeekFrom::Start(file_len - FOOTER_LEN))?;
    let mut footer = [0u8; FOOTER_LEN as usize];
    f.read_exact(&mut footer)?;
    if read_u32(&footer, 0) != FOOTER_MAGIC {
        bail!("cbk: bad footer magic (not a .cbk container?)");
    }
    let index_offset = read_u64(&footer, 4);
    let index_len = read_u32(&footer, 12) as usize;

    f.seek(SeekFrom::Start(index_offset))?;
    let mut idx = vec![0u8; index_len];
    f.read_exact(&mut idx)?;
    if read_u32(&idx, 0) != INDEX_MAGIC {
        bail!("cbk: bad index magic");
    }
    let version = read_u16(&idx, 4);
    if version != CBK_VERSION {
        bail!("cbk: unsupported container version {version}");
    }
    let member_count = read_u16(&idx, 6) as usize;
    let mut pos = 8;
    let mut members = Vec::with_capacity(member_count);
    for _ in 0..member_count {
        let name_len = read_u16(&idx, pos) as usize;
        pos += 2;
        let name = String::from_utf8(idx[pos..pos + name_len].to_vec())
            .context("cbk: member name not UTF-8")?;
        pos += name_len;
        let kind = Kind::from_byte(idx[pos])?;
        pos += 1;
        let _logical_id = read_u16(&idx, pos);
        pos += 2;
        let chunk_count = read_u32(&idx, pos) as usize;
        pos += 4;
        let mut chunk_offsets = Vec::with_capacity(chunk_count);
        for _ in 0..chunk_count {
            chunk_offsets.push(read_u64(&idx, pos));
            pos += 8;
        }
        members.push(MemberIn {
            name,
            kind,
            chunk_offsets,
        });
    }
    Ok(members)
}

/// Read one chunk's payload at `offset`, verifying magic + CRC. Returns the
/// payload bytes.
fn read_chunk_payload(f: &mut File, offset: u64) -> Result<Vec<u8>> {
    f.seek(SeekFrom::Start(offset))?;
    let mut hdr = [0u8; CHUNK_HEADER_LEN];
    f.read_exact(&mut hdr)?;
    if read_u32(&hdr, 0) != CHUNK_MAGIC {
        bail!("cbk: bad chunk magic at offset {offset}");
    }
    let len = read_u32(&hdr, 16) as usize;
    let want_crc = read_u32(&hdr, 20);
    let mut payload = vec![0u8; len];
    f.read_exact(&mut payload)?;
    let got_crc = crc32fast::hash(&payload);
    if got_crc != want_crc {
        bail!("cbk: chunk CRC mismatch at offset {offset} (corrupt container)");
    }
    Ok(payload)
}

/// Materialize a `.cbk` container back into a native backup folder at
/// `out_folder` (created if absent). The result is byte-identical to the folder
/// that was packed.
pub fn materialize_cbk_to_folder(cbk: &Path, out_folder: &Path) -> Result<()> {
    let mut f = File::open(cbk).with_context(|| format!("opening {}", cbk.display()))?;
    let members = read_index(&mut f)?;
    fs::create_dir_all(out_folder).with_context(|| format!("creating {}", out_folder.display()))?;

    for m in &members {
        // Guard against path traversal in member names.
        if m.name.contains('/') || m.name.contains('\\') || m.name.contains("..") {
            bail!("cbk: refusing unsafe member name {:?}", m.name);
        }
        let out_path = out_folder.join(&m.name);
        let mut out = BufWriter::new(
            File::create(&out_path).with_context(|| format!("creating {}", out_path.display()))?,
        );
        match m.kind {
            Kind::Gz => {
                // Concatenated payloads ARE the file (a multi-member gzip).
                for &off in &m.chunk_offsets {
                    let payload = read_chunk_payload(&mut f, off)?;
                    out.write_all(&payload)?;
                }
            }
            Kind::Raw => {
                // File = decompress(concatenated gzip members).
                let mut concat = Vec::new();
                for &off in &m.chunk_offsets {
                    concat.extend_from_slice(&read_chunk_payload(&mut f, off)?);
                }
                let mut dec = MultiGzDecoder::new(&concat[..]);
                std::io::copy(&mut dec, &mut out)
                    .with_context(|| format!("decompressing member {}", m.name))?;
            }
        }
        out.flush()?;
    }
    Ok(())
}

/// True if `path` looks like a `.cbk` container (by extension or footer magic).
pub fn is_cbk(path: &Path) -> bool {
    if path
        .extension()
        .map(|e| e.eq_ignore_ascii_case("cbk"))
        .unwrap_or(false)
    {
        return true;
    }
    // Fall back to the footer magic so an extensionless container still works.
    (|| -> Result<bool> {
        let mut f = File::open(path)?;
        let len = f.metadata()?.len();
        if len < FOOTER_LEN {
            return Ok(false);
        }
        f.seek(SeekFrom::Start(len - FOOTER_LEN))?;
        let mut m = [0u8; 4];
        f.read_exact(&mut m)?;
        Ok(u32::from_be_bytes(m) == FOOTER_MAGIC)
    })()
    .unwrap_or(false)
}

/// A `.cbk` index entry: a member file and the file-offsets of its chunks.
/// Enough for a lazy reader to stream a single member without materializing the
/// whole container.
pub struct CbkMember {
    pub name: String,
    chunk_offsets: Vec<u64>,
}

/// Read a `.cbk`'s index (no payloads) — the member list with each member's
/// chunk offsets. Cheap: one footer read + one index read.
pub fn read_cbk_index(path: &Path) -> Result<Vec<CbkMember>> {
    let mut f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    Ok(read_index(&mut f)?
        .into_iter()
        .map(|m| CbkMember {
            name: m.name,
            chunk_offsets: m.chunk_offsets,
        })
        .collect())
}

/// A `Read` over a member's concatenated chunk payloads (the raw member bytes),
/// streamed one chunk at a time (no full-member buffering). Each payload's CRC
/// is verified as it's read. Wrap in `MultiGzDecoder` to get the member's
/// *logical content*: the decompressed partition for a `*.gz` (`Gz`) member, or
/// the file itself for a gzip-wrapped (`Raw`) member — so
/// `MultiGzDecoder::new(CbkPayloadReader::open(..))` yields the right bytes for
/// either kind.
pub struct CbkPayloadReader {
    file: File,
    chunk_offsets: Vec<u64>,
    next_chunk: usize,
    buf: Vec<u8>,
    buf_pos: usize,
}

impl CbkPayloadReader {
    pub fn open(path: &Path, member: &CbkMember) -> Result<Self> {
        Ok(Self {
            file: File::open(path)?,
            chunk_offsets: member.chunk_offsets.clone(),
            next_chunk: 0,
            buf: Vec::new(),
            buf_pos: 0,
        })
    }
}

impl Read for CbkPayloadReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        while self.buf_pos >= self.buf.len() {
            if self.next_chunk >= self.chunk_offsets.len() {
                return Ok(0); // all chunks consumed
            }
            let off = self.chunk_offsets[self.next_chunk];
            self.next_chunk += 1;
            self.buf = read_chunk_payload(&mut self.file, off)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            self.buf_pos = 0;
        }
        let n = (self.buf.len() - self.buf_pos).min(out.len());
        out[..n].copy_from_slice(&self.buf[self.buf_pos..self.buf_pos + n]);
        self.buf_pos += n;
        Ok(n)
    }
}

/// A streaming reader over a member's logical content (decompressed partition,
/// or the raw small file), via `MultiGzDecoder` over its chunk payloads.
pub fn cbk_member_content_reader(
    path: &Path,
    member: &CbkMember,
) -> Result<MultiGzDecoder<CbkPayloadReader>> {
    Ok(MultiGzDecoder::new(CbkPayloadReader::open(path, member)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Build a tiny native-ish backup folder: metadata.json (raw) + a real
    /// gzip member (partition-0.gz) + a crc32 sidecar.
    fn sample_folder(dir: &Path) {
        fs::write(dir.join("metadata.json"), b"{\n  \"version\": 1\n}\n").unwrap();
        fs::write(dir.join("mbr.bin"), vec![0xAAu8; 512]).unwrap();
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(&vec![0u8; 100_000]).unwrap();
        enc.write_all(b"some real bytes in the middle").unwrap();
        enc.write_all(&vec![0u8; 50_000]).unwrap();
        let gz = enc.finish().unwrap();
        fs::write(dir.join("partition-0.gz"), &gz).unwrap();
        fs::write(dir.join("partition-0.gz.crc32"), b"deadbeef").unwrap();
    }

    #[test]
    fn pack_then_materialize_round_trips_every_file() {
        let tmp = TempDir::new().unwrap();
        let src = tmp.path().join("MYDISK");
        fs::create_dir(&src).unwrap();
        sample_folder(&src);

        let cbk = tmp.path().join("MYDISK.cbk");
        pack_folder_to_cbk(&src, &cbk).unwrap();
        assert!(is_cbk(&cbk), "packed file should be recognized as .cbk");

        let out = tmp.path().join("restored");
        materialize_cbk_to_folder(&cbk, &out).unwrap();

        for name in [
            "metadata.json",
            "mbr.bin",
            "partition-0.gz",
            "partition-0.gz.crc32",
        ] {
            let a = fs::read(src.join(name)).unwrap();
            let b = fs::read(out.join(name)).unwrap();
            assert_eq!(a, b, "member {name} did not round-trip byte-for-byte");
        }
    }

    #[test]
    fn partition_member_is_a_valid_gzip_stream_in_the_container() {
        // The partition member's chunk payload must itself be a decodable gzip
        // member (so the desktop could read partitions straight out of .cbk).
        let tmp = TempDir::new().unwrap();
        let src = tmp.path().join("D");
        fs::create_dir(&src).unwrap();
        sample_folder(&src);
        let cbk = tmp.path().join("D.cbk");
        pack_folder_to_cbk(&src, &cbk).unwrap();

        let mut f = File::open(&cbk).unwrap();
        let members = read_index(&mut f).unwrap();
        let part = members.iter().find(|m| m.name == "partition-0.gz").unwrap();
        assert_eq!(part.kind, Kind::Gz);
        let payload = read_chunk_payload(&mut f, part.chunk_offsets[0]).unwrap();
        let mut dec = MultiGzDecoder::new(&payload[..]);
        let mut out = Vec::new();
        dec.read_to_end(&mut out).unwrap();
        assert_eq!(out.len(), 150_029, "decoded partition image length");
    }

    #[test]
    fn bad_footer_is_rejected() {
        let tmp = TempDir::new().unwrap();
        let bogus = tmp.path().join("nope.cbk");
        fs::write(&bogus, b"not a container at all, just some bytes here.").unwrap();
        let out = tmp.path().join("x");
        assert!(materialize_cbk_to_folder(&bogus, &out).is_err());
    }
}
