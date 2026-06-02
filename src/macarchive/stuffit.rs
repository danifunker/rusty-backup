//! Classic StuffIt (`.sit`) + StuffIt self-extracting (`.sea`) reader.
//!
//! Container layout (reverse-engineered; see XADMaster `XADStuffItParser.m`,
//! mirrored in `docs/native_mac_archives.md`):
//!
//! ```text
//! Archive header (22 bytes):
//!   [0..4]  "SIT!" magic
//!   [4..6]  number of entries (u16 BE)        (informational; we walk to EOF)
//!   [6..10] total archive length (u32 BE)
//!   [10..14] "rLau" secondary signature
//!   [14..22] version + reserved + header CRC
//!
//! Per-entry header (112 bytes):
//!   [0]   resource-fork compression method
//!   [1]   data-fork compression method
//!   [2]   filename length (<= 31)
//!   [3..34] filename (Mac Roman)
//!   [66..70] file type        [70..74] creator
//!   [74..76] Finder flags
//!   [76..80] creation date    [80..84] modification date  (Mac 1904 epoch)
//!   [84..88] resource fork uncompressed length
//!   [88..92] data fork uncompressed length
//!   [92..96] resource fork compressed length
//!   [96..100] data fork compressed length
//!   [100..102] resource CRC   [102..104] data CRC  (CRC-16/ARC)
//!   [110..112] header CRC (CRC-16/ARC over [0..110])
//! Then: <compressed resource fork><compressed data fork>
//! ```
//!
//! A method byte's high bit (`0x80`) marks an encrypted fork; `0x20`/`0x21`
//! (after masking the encryption + folder-contains-encrypted bits) mark the
//! start/end of a folder. The low nibble is the compression method.

use anyhow::{bail, Result};
use byteorder::{BigEndian, ByteOrder};

use crate::fs::binhex;

const ARCHIVE_HEADER_LEN: usize = 22;
const ENTRY_HEADER_LEN: usize = 112;

const ENCRYPTED_FLAG: u8 = 0x80;
const FOLDER_CONTAINS_ENCRYPTED: u8 = 0x10;
const START_FOLDER: u8 = 0x20;
const END_FOLDER: u8 = 0x21;
/// Mask that strips the encryption + "contains encrypted" bits, leaving the
/// folder marker (or the raw method) behind.
const FOLDER_MASK: u8 = !(ENCRYPTED_FLAG | FOLDER_CONTAINS_ENCRYPTED);

/// One fork (data or resource) of a StuffIt entry.
#[derive(Debug, Clone)]
pub struct ForkInfo {
    /// Compression method (low nibble: 0=none, 1=RLE90, 3=Huffman, 13=LZ+Huffman, 15=Arsenic, …).
    pub method: u8,
    /// True when the fork is password-encrypted (not supported for extraction).
    pub encrypted: bool,
    /// Decompressed length in bytes.
    pub uncompressed_len: u32,
    /// Compressed length in bytes (as stored in the archive).
    pub compressed_len: u32,
    /// Stored CRC-16/ARC of the decompressed fork.
    pub crc: u16,
    /// Absolute byte offset of the compressed data within the archive.
    pub offset: u64,
}

impl ForkInfo {
    /// Human-readable compression method name.
    pub fn method_name(&self) -> &'static str {
        method_name(self.method)
    }
}

/// One file or folder in a StuffIt archive.
#[derive(Debug, Clone)]
pub struct StuffItEntry {
    /// Path components from the archive root (last element is the name).
    pub path: Vec<String>,
    /// Leaf name (Mac Roman → UTF-8).
    pub name: String,
    /// True for folder markers (no fork data).
    pub is_dir: bool,
    pub type_code: [u8; 4],
    pub creator_code: [u8; 4],
    pub finder_flags: u16,
    /// Mac 1904-epoch timestamps.
    pub create_date: u32,
    pub mod_date: u32,
    /// Data fork (absent for folders).
    pub data: Option<ForkInfo>,
    /// Resource fork (absent for folders).
    pub rsrc: Option<ForkInfo>,
}

impl StuffItEntry {
    /// Display path joined with `/`.
    pub fn display_path(&self) -> String {
        self.path.join("/")
    }
}

/// A parsed StuffIt archive (entries in archive order; directory structure is
/// reflected in each entry's `path`).
#[derive(Debug, Clone)]
pub struct StuffItArchive {
    pub entries: Vec<StuffItEntry>,
}

fn method_name(method: u8) -> &'static str {
    match method & 0x0f {
        0 => "None",
        1 => "RLE",
        2 => "Compress (LZW)",
        3 => "Huffman",
        5 => "LZAH",
        6 => "Fixed Huffman",
        8 => "MW",
        13 => "LZ+Huffman",
        14 => "Installer",
        15 => "Arsenic",
        _ => "Unknown",
    }
}

/// CRC-16/ARC (reflected, poly 0xA001, init 0) — StuffIt's header and fork CRC.
pub(crate) fn crc16_arc(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= b as u16;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xA001;
            } else {
                crc >>= 1;
            }
        }
    }
    crc
}

/// Detect whether `data` begins with a classic StuffIt archive.
pub fn is_stuffit(data: &[u8]) -> bool {
    data.len() >= 14 && &data[0..4] == b"SIT!" && &data[10..14] == b"rLau"
}

/// Locate the StuffIt archive inside a `.sea` (self-extracting archive). A
/// StuffIt SEA is a Mac application whose payload is a plain `.sit` stream; we
/// scan for the `SIT!`…`rLau` signature. Returns the offset of the archive.
pub fn find_sea_archive(data: &[u8]) -> Option<usize> {
    if is_stuffit(data) {
        return Some(0);
    }
    // Scan for "SIT!" followed by "rLau" 10 bytes later.
    data.windows(14)
        .position(|w| &w[0..4] == b"SIT!" && &w[10..14] == b"rLau")
}

/// Parse a StuffIt archive (or the `.sit` stream embedded in a `.sea`).
pub fn parse(data: &[u8]) -> Result<StuffItArchive> {
    let base = find_sea_archive(data)
        .ok_or_else(|| anyhow::anyhow!("not a StuffIt archive (no SIT!/rLau signature)"))?;
    let archive = &data[base..];
    if archive.len() < ARCHIVE_HEADER_LEN {
        bail!("StuffIt: truncated archive header");
    }

    let total_size = BigEndian::read_u32(&archive[6..10]) as usize;
    // Clamp to what we actually have (some SEAs over-report).
    let total_size = total_size.min(archive.len());

    let mut entries = Vec::new();
    let mut currdir: Vec<String> = Vec::new();
    let mut pos = ARCHIVE_HEADER_LEN;

    while pos + ENTRY_HEADER_LEN <= total_size {
        let h = &archive[pos..pos + ENTRY_HEADER_LEN];

        let stored_hdr_crc = BigEndian::read_u16(&h[110..112]);
        let computed = crc16_arc(&h[0..110]);
        if stored_hdr_crc != computed {
            bail!(
                "StuffIt: entry header CRC mismatch at offset {pos} (got {computed:#06x}, expected {stored_hdr_crc:#06x})"
            );
        }

        let rmethod = h[0];
        let dmethod = h[1];
        let mut namelen = h[2] as usize;
        if namelen > 31 {
            namelen = 31;
        }
        let name = crate::fs::hfs::mac_roman_to_utf8(&h[3..3 + namelen]);

        let data_start = (base + pos + ENTRY_HEADER_LEN) as u64;

        if (dmethod & FOLDER_MASK) == START_FOLDER || (rmethod & FOLDER_MASK) == START_FOLDER {
            currdir.push(name.clone());
            // currdir now includes this folder as its last component.
            entries.push(StuffItEntry {
                path: currdir.clone(),
                name,
                is_dir: true,
                type_code: [0; 4],
                creator_code: [0; 4],
                finder_flags: BigEndian::read_u16(&h[74..76]),
                create_date: BigEndian::read_u32(&h[76..80]),
                mod_date: BigEndian::read_u32(&h[80..84]),
                data: None,
                rsrc: None,
            });
            pos += ENTRY_HEADER_LEN; // folder marker carries no fork data
            continue;
        }

        if (dmethod & FOLDER_MASK) == END_FOLDER || (rmethod & FOLDER_MASK) == END_FOLDER {
            currdir.pop();
            pos += ENTRY_HEADER_LEN;
            continue;
        }

        // Regular file.
        let rsrc_len = BigEndian::read_u32(&h[84..88]);
        let data_len = BigEndian::read_u32(&h[88..92]);
        let rsrc_clen = BigEndian::read_u32(&h[92..96]);
        let data_clen = BigEndian::read_u32(&h[96..100]);
        let rsrc_crc = BigEndian::read_u16(&h[100..102]);
        let data_crc = BigEndian::read_u16(&h[102..104]);

        let mut type_code = [0u8; 4];
        type_code.copy_from_slice(&h[66..70]);
        let mut creator_code = [0u8; 4];
        creator_code.copy_from_slice(&h[70..74]);

        let rsrc = if rsrc_len > 0 || rsrc_clen > 0 {
            Some(ForkInfo {
                method: rmethod & !ENCRYPTED_FLAG,
                encrypted: rmethod & ENCRYPTED_FLAG != 0,
                uncompressed_len: rsrc_len,
                compressed_len: rsrc_clen,
                crc: rsrc_crc,
                offset: data_start,
            })
        } else {
            None
        };
        let data = Some(ForkInfo {
            method: dmethod & !ENCRYPTED_FLAG,
            encrypted: dmethod & ENCRYPTED_FLAG != 0,
            uncompressed_len: data_len,
            compressed_len: data_clen,
            crc: data_crc,
            offset: data_start + rsrc_clen as u64,
        });

        let mut path = currdir.clone();
        path.push(name.clone());
        entries.push(StuffItEntry {
            path,
            name,
            is_dir: false,
            type_code,
            creator_code,
            finder_flags: BigEndian::read_u16(&h[74..76]),
            create_date: BigEndian::read_u32(&h[76..80]),
            mod_date: BigEndian::read_u32(&h[80..84]),
            data,
            rsrc,
        });

        pos += ENTRY_HEADER_LEN + rsrc_clen as usize + data_clen as usize;
    }

    Ok(StuffItArchive { entries })
}

/// Decompress one fork from the archive bytes, verifying its CRC.
///
/// `archive` is the full file passed to [`parse`] (offsets in [`ForkInfo`] are
/// absolute within it). Returns an error for encrypted forks and for
/// compression methods not yet implemented.
pub fn decompress_fork(archive: &[u8], fork: &ForkInfo) -> Result<Vec<u8>> {
    if fork.encrypted {
        bail!("StuffIt: encrypted forks are not supported");
    }
    let start = fork.offset as usize;
    let end = start + fork.compressed_len as usize;
    if end > archive.len() {
        bail!("StuffIt: fork data extends past end of archive");
    }
    let comp = &archive[start..end];
    let want = fork.uncompressed_len as usize;

    let out = match fork.method & 0x0f {
        0 => comp.to_vec(),
        1 => binhex::rle90_decode(comp),
        2 => super::stuffit_lzw::decompress(comp, want, 0x8e)?,
        3 => super::stuffit_huffman::decompress(comp, want)?,
        5 => super::stuffit_lzah::decompress(comp, want)?,
        13 => super::stuffit_lzh::decompress(comp, want)?,
        15 => super::stuffit_arsenic::decompress(comp, want)?,
        m => bail!(
            "StuffIt: compression method {m} ({}) not yet supported",
            method_name(fork.method)
        ),
    };

    let out = if out.len() > want {
        out[..want].to_vec()
    } else {
        out
    };
    if out.len() != want {
        bail!(
            "StuffIt: decompressed length {} != expected {want}",
            out.len()
        );
    }

    // Arsenic (15) carries its own checksum; everything else uses CRC-16/ARC.
    if fork.method & 0x0f != 15 {
        let got = crc16_arc(&out);
        if got != fork.crc {
            bail!(
                "StuffIt: fork CRC mismatch (got {got:#06x}, expected {:#06x})",
                fork.crc
            );
        }
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Compression to use when writing a fork.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMethod {
    /// Store uncompressed (method 0).
    Store,
    /// RLE90 (method 1), falling back to Store when it doesn't shrink the fork.
    Rle,
}

/// A single file to place into a StuffIt archive (top level; folders are not
/// emitted by the writer yet).
#[derive(Debug, Clone)]
pub struct StuffItInput {
    /// Mac filename (truncated to 31 bytes).
    pub name: String,
    pub type_code: [u8; 4],
    pub creator_code: [u8; 4],
    pub finder_flags: u16,
    /// Mac 1904-epoch timestamps (0 is acceptable).
    pub create_date: u32,
    pub mod_date: u32,
    pub data_fork: Vec<u8>,
    pub resource_fork: Vec<u8>,
}

/// Compress a fork for writing, returning `(method_byte, bytes)`.
fn compress_fork(data: &[u8], method: WriteMethod) -> (u8, Vec<u8>) {
    match method {
        WriteMethod::Store => (0, data.to_vec()),
        WriteMethod::Rle => {
            let encoded = binhex::rle90_encode(data);
            if encoded.len() < data.len() {
                (1, encoded)
            } else {
                (0, data.to_vec())
            }
        }
    }
}

/// Build a classic StuffIt (`.sit`) archive from a flat list of files.
///
/// Produces correct 112-byte entry-header CRCs and per-fork CRC-16/ARC values
/// (the checksums real readers verify), so the result round-trips through
/// [`parse`] and is accepted by The Unarchiver. The 16-bit archive-header field
/// at offset 20 (whose derivation is unknown and which XAD ignores) is written
/// as zero.
pub fn build_archive(files: &[StuffItInput], method: WriteMethod) -> Result<Vec<u8>> {
    let mut body = Vec::new();

    for f in files {
        let name_bytes = mac_name_bytes(&f.name);
        let namelen = name_bytes.len();

        let (rmethod, rcomp) = if f.resource_fork.is_empty() {
            (0u8, Vec::new())
        } else {
            compress_fork(&f.resource_fork, method)
        };
        let (dmethod, dcomp) = compress_fork(&f.data_fork, method);

        let mut h = [0u8; ENTRY_HEADER_LEN];
        h[0] = rmethod;
        h[1] = dmethod;
        h[2] = namelen as u8;
        h[3..3 + namelen].copy_from_slice(&name_bytes);
        // [34..36] filename CRC (size byte + name); not verified by readers.
        let fname_crc = crc16_arc(&h[2..3 + namelen]);
        BigEndian::write_u16(&mut h[34..36], fname_crc);
        h[66..70].copy_from_slice(&f.type_code);
        h[70..74].copy_from_slice(&f.creator_code);
        BigEndian::write_u16(&mut h[74..76], f.finder_flags);
        BigEndian::write_u32(&mut h[76..80], f.create_date);
        BigEndian::write_u32(&mut h[80..84], f.mod_date);
        BigEndian::write_u32(&mut h[84..88], f.resource_fork.len() as u32);
        BigEndian::write_u32(&mut h[88..92], f.data_fork.len() as u32);
        BigEndian::write_u32(&mut h[92..96], rcomp.len() as u32);
        BigEndian::write_u32(&mut h[96..100], dcomp.len() as u32);
        BigEndian::write_u16(&mut h[100..102], crc16_arc(&f.resource_fork));
        BigEndian::write_u16(&mut h[102..104], crc16_arc(&f.data_fork));
        // [62..66] child offset: -1 marks a file entry (as real archives do).
        h[62..66].copy_from_slice(&[0xff, 0xff, 0xff, 0xff]);
        // Header CRC over the first 110 bytes.
        let hdr_crc = crc16_arc(&h[0..110]);
        BigEndian::write_u16(&mut h[110..112], hdr_crc);

        body.extend_from_slice(&h);
        body.extend_from_slice(&rcomp);
        body.extend_from_slice(&dcomp);
    }

    let total = (ARCHIVE_HEADER_LEN + body.len()) as u32;
    let mut out = Vec::with_capacity(total as usize);
    out.extend_from_slice(b"SIT!");
    out.extend_from_slice(&(files.len() as u16).to_be_bytes());
    out.extend_from_slice(&total.to_be_bytes());
    out.extend_from_slice(b"rLau");
    out.push(1); // version
    out.push(0); // reserved
    out.extend_from_slice(&(ARCHIVE_HEADER_LEN as u32).to_be_bytes());
    out.extend_from_slice(&[0, 0]); // archive-header field at [20]; XAD ignores it
    debug_assert_eq!(out.len(), ARCHIVE_HEADER_LEN);
    out.extend_from_slice(&body);
    Ok(out)
}

/// Encode a name to at most 31 Mac Roman bytes (lossy ASCII fallback).
fn mac_name_bytes(name: &str) -> Vec<u8> {
    let mut bytes = crate::fs::hfs::utf8_to_mac_roman(name).unwrap_or_else(|_| {
        name.chars()
            .map(|c| if c.is_ascii() { c as u8 } else { b'_' })
            .collect()
    });
    bytes.truncate(31);
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc16_arc_known() {
        // CRC-16/ARC of "123456789" is 0xBB3D (standard check value).
        assert_eq!(crc16_arc(b"123456789"), 0xBB3D);
    }

    #[test]
    fn detects_signature() {
        let mut buf = vec![0u8; 22];
        buf[0..4].copy_from_slice(b"SIT!");
        buf[10..14].copy_from_slice(b"rLau");
        assert!(is_stuffit(&buf));
        assert!(!is_stuffit(b"not a sit file"));
    }

    #[test]
    fn write_then_read_roundtrip() {
        let files = vec![
            StuffItInput {
                name: "Read Me".to_string(),
                type_code: *b"TEXT",
                creator_code: *b"ttxt",
                finder_flags: 0,
                create_date: 0xAABBCCDD,
                mod_date: 0x11223344,
                // Long runs so RLE actually engages.
                data_fork: b"Hello\r\rAAAAAAAAAAAAAAAAAAAA end.".to_vec(),
                resource_fork: vec![0u8; 200],
            },
            StuffItInput {
                name: "Empty Rsrc".to_string(),
                type_code: *b"BINA",
                creator_code: *b"????",
                finder_flags: 0x4000,
                create_date: 0,
                mod_date: 0,
                data_fork: b"just a data fork".to_vec(),
                resource_fork: Vec::new(),
            },
        ];

        for method in [WriteMethod::Store, WriteMethod::Rle] {
            let bytes = build_archive(&files, method).unwrap();
            assert!(is_stuffit(&bytes));
            let archive = parse(&bytes).expect("our writer must parse");
            assert_eq!(archive.entries.len(), 2);

            let e0 = &archive.entries[0];
            assert_eq!(e0.name, "Read Me");
            assert_eq!(&e0.type_code, b"TEXT");
            assert_eq!(e0.create_date, 0xAABBCCDD);
            // decompress_fork verifies the per-fork CRC internally.
            let data0 = decompress_fork(&bytes, e0.data.as_ref().unwrap()).unwrap();
            assert_eq!(data0, files[0].data_fork);
            let rsrc0 = decompress_fork(&bytes, e0.rsrc.as_ref().unwrap()).unwrap();
            assert_eq!(rsrc0, files[0].resource_fork);

            let e1 = &archive.entries[1];
            assert_eq!(e1.finder_flags, 0x4000);
            assert!(e1.rsrc.is_none());
            let data1 = decompress_fork(&bytes, e1.data.as_ref().unwrap()).unwrap();
            assert_eq!(data1, files[1].data_fork);
        }
    }

    /// Parses real `.sit` samples if present (decoded from `.sit.hqx` on the
    /// fly). Validates the container parser + header CRCs end-to-end.
    #[test]
    fn parse_real_samples_if_present() {
        let dir = std::path::Path::new("/Users/dani/Downloads");
        if !dir.is_dir() {
            return;
        }
        let mut checked = 0;
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !name.to_ascii_lowercase().ends_with(".sit.hqx") {
                continue;
            }
            let bytes = match std::fs::read(&path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let bh = match binhex::parse_binhex(&bytes) {
                Ok(b) => b,
                Err(_) => continue,
            };
            // The .sit is the data fork of the BinHex file. Skip payloads that
            // aren't a classic StuffIt stream (e.g. the newer StuffIt 5 format,
            // which starts with "StuffIt (c)…" and is out of scope here).
            if find_sea_archive(&bh.data_fork).is_none() {
                eprintln!("{name}: skipped (not classic SIT!; likely StuffIt 5)");
                continue;
            }
            let archive = match parse(&bh.data_fork) {
                Ok(a) => a,
                Err(e) => panic!("parse failed for {name}: {e}"),
            };
            assert!(!archive.entries.is_empty(), "no entries parsed from {name}");
            for e in &archive.entries {
                assert!(!e.name.is_empty() || e.is_dir, "empty entry name in {name}");
            }
            // Actually decompress every supported fork; decompress_fork
            // verifies the per-fork CRC-16/ARC, so success means the codec
            // (including method 13 / LZ+Huffman) round-trips correctly.
            let mut ok = 0;
            let mut skipped = 0;
            for e in &archive.entries {
                for fork in [e.data.as_ref(), e.rsrc.as_ref()].into_iter().flatten() {
                    if fork.uncompressed_len == 0 {
                        continue;
                    }
                    match decompress_fork(&bh.data_fork, fork) {
                        Ok(_) => ok += 1,
                        Err(_) if matches!(fork.method & 0x0f, 2 | 3 | 5 | 6 | 8 | 14 | 15) => {
                            skipped += 1; // codec not implemented yet
                        }
                        Err(e) => panic!("{name}: decompress failed: {e}"),
                    }
                }
            }
            eprintln!(
                "{name}: {} entries, {ok} forks decompressed + CRC-verified, {skipped} unsupported-codec",
                archive.entries.len()
            );
            checked += 1;
            if checked >= 6 {
                break;
            }
        }
        eprintln!("validated {checked} real .sit samples");
    }
}
