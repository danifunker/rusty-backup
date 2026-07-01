//! MAR archive reader + writer (classic Mac, Hampa Hug's `mar` tool).
//!
//! A `.mar` archive stores a **single** Mac item — a file or a folder tree —
//! with both forks, type/creator, Finder flags, and 1904-epoch dates. It is the
//! native format of the `mar` MPW/Mac tool. Layout (all fields big-endian):
//!
//! ```text
//! [128-byte record header] [data block] [rsrc block] [comment block] ...
//! ```
//!
//! Each record header is 128 bytes:
//!   0   u32  magic 'MAR\x80' (0x4d415280)
//!   4   u32  id: 'file' (file) | 'fold' (folder start) | '----' (folder end)
//!   8   pstr name (length byte + up to 63 Mac-Roman bytes, 64 bytes total)
//!   72  u32  data fork size        76  u32  rsrc fork size
//!   80  u32  comment size          84  u32  file flags (bit0 = locked)
//!   88  u32  create date (Mac 1904)  92  u32  mod date
//!   96  16   FInfo/DInfo: type@96 creator@100 finderFlags@104 ...
//!   124 u32  header CRC (over bytes 0..124)
//!
//! A `file` header is followed by its data, resource, and comment blocks (each
//! present only when its size > 0). A block is `[payload][u32 CRC][zero pad]`,
//! padded so `payload + 4 + pad` is a multiple of 128. A `fold` header is
//! followed by its children (more records) terminated by a `----` header.
//!
//! The whole byte stream may be wrapped in LZW (Unix `compress`, magic `1f 9d`)
//! or an LZ4 frame (magic `04 22 4d 18`); an unwrapped archive starts directly
//! with the first record's `MAR\x80`. We read/write the **stored** (unwrapped)
//! form; a compressed archive is detected and reported (not silently mis-read).
//!
//! Both CRCs use a non-reflected CRC-32 with the Castagnoli polynomial
//! 0x1edc6f41 (init 0, no xorout) — `mar`'s own checksum, distinct from the
//! StuffIt/Compact-Pro fork checksums.
//!
//! Clean-room Rust from the format as described by the GPL-2.0 `mar-0.2.0`
//! sources (Hampa Hug); no code is copied. Forks are decoded eagerly into a
//! buffer and exposed through the shared [`StuffItArchive`] model (stored forks,
//! offsets into the returned buffer) so the existing extract / browse / CLI path
//! handles MAR with no special case — exactly like the loose-BinHex synthesis in
//! [`super::extract`].

use anyhow::{bail, Context, Result};

use crate::fs::hfs::{decode_mac_filename, utf8_to_mac_roman};

use super::stuffit::{
    crc16_arc, ForkCodec, ForkInfo, StuffItArchive, StuffItEntry, StuffItInputNode,
};

const MAR_MAGIC: u32 = 0x4d41_5280; // 'MAR\x80'
const MAR_FILE: u32 = 0x6669_6c65; // 'file'
const MAR_FOLDER_START: u32 = 0x666f_6c64; // 'fold'
const MAR_FOLDER_END: u32 = 0x2d2d_2d2d; // '----'
const HEADER_LEN: usize = 128;
const NAME_OFF: usize = 8;
const NAME_MAX: usize = 63;

/// LZW (`compress`) wrapper magic.
const LZW_MAGIC: [u8; 2] = [0x1f, 0x9d];
/// LZ4 frame wrapper magic.
const LZ4_MAGIC: [u8; 4] = [0x04, 0x22, 0x4d, 0x18];

// ---------------------------------------------------------------------------
// CRC-32 (poly 0x1edc6f41, MSB-first, init 0) — mar's header + fork checksum.
// ---------------------------------------------------------------------------

const fn build_crc_table() -> [u32; 256] {
    let mut tab = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut reg = (i as u32) << 24;
        let mut j = 0;
        while j < 8 {
            if reg & 0x8000_0000 != 0 {
                reg = (reg << 1) ^ 0x1edc_6f41;
            } else {
                reg <<= 1;
            }
            j += 1;
        }
        tab[i] = reg;
        i += 1;
    }
    tab
}

static CRC_TABLE: [u32; 256] = build_crc_table();

fn mar_crc32(data: &[u8]) -> u32 {
    let mut reg: u32 = 0;
    for &b in data {
        let idx = ((reg >> 24) ^ b as u32) & 0xff;
        reg = (reg << 8) ^ CRC_TABLE[idx as usize];
    }
    reg
}

fn u16_be(buf: &[u8], i: usize) -> u16 {
    u16::from_be_bytes([buf[i], buf[i + 1]])
}

fn u32_be(buf: &[u8], i: usize) -> u32 {
    u32::from_be_bytes([buf[i], buf[i + 1], buf[i + 2], buf[i + 3]])
}

/// True if `buf` begins with a valid 128-byte MAR record header (magic + a
/// header CRC that checks out). This is the unwrapped (stored) form.
fn is_mar_header(buf: &[u8]) -> bool {
    buf.len() >= HEADER_LEN
        && u32_be(buf, 0) == MAR_MAGIC
        && mar_crc32(&buf[..124]) == u32_be(buf, 124)
}

/// Detect a stored (unwrapped) MAR archive by its first record header.
/// Compressed wrappers are intentionally **not** reported here — `parse`
/// surfaces them with a clear "not yet supported" message rather than a
/// detection false-positive on the generic LZW/LZ4 magics.
pub fn is_mar(data: &[u8]) -> bool {
    is_mar_header(data)
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// Parse a MAR archive. Returns the decoded byte buffer (fork offsets in the
/// entries are absolute within it) plus the parsed directory — the same
/// `(Vec<u8>, StuffItArchive)` shape every other Mac-archive parser returns, so
/// `super::extract` handles it unchanged. Forks are modelled as **stored**
/// (method 0) over the returned buffer; each fork's MAR CRC-32 is verified here.
pub fn parse(data: &[u8]) -> Result<(Vec<u8>, StuffItArchive)> {
    if !is_mar_header(data) {
        if data.len() >= 2 && data[..2] == LZW_MAGIC {
            bail!(
                "MAR archive is LZW-compressed (Unix `compress`); only stored MAR is \
                 supported so far. Decompress it first (e.g. `uncompress`) or open it \
                 with the `mar` tool."
            );
        }
        if data.len() >= 4 && data[..4] == LZ4_MAGIC {
            bail!(
                "MAR archive is LZ4-compressed; only stored MAR is supported so far. \
                 Decompress the LZ4 frame first."
            );
        }
        bail!("not a MAR archive (bad magic or header CRC)");
    }

    let mut entries: Vec<StuffItEntry> = Vec::new();
    let mut pos = 0usize;
    parse_record(data, &mut pos, &[], &mut entries).context("parsing MAR archive")?;

    Ok((data.to_vec(), StuffItArchive { entries }))
}

/// Parse one record at `*pos` (advancing it past the record and any fork
/// blocks / nested children), appending the entry (and, for folders, its
/// descendants) to `entries`.
fn parse_record(
    data: &[u8],
    pos: &mut usize,
    parent_path: &[String],
    entries: &mut Vec<StuffItEntry>,
) -> Result<()> {
    if *pos + HEADER_LEN > data.len() {
        bail!("truncated MAR record header");
    }
    let hdr = &data[*pos..*pos + HEADER_LEN];
    if !is_mar_header(hdr) {
        bail!("bad MAR record header (magic/CRC)");
    }
    let id = u32_be(hdr, 4);
    *pos += HEADER_LEN;

    let name = read_name(hdr);
    let mut path = parent_path.to_vec();
    path.push(name.clone());

    let create_date = u32_be(hdr, 88);
    let mod_date = u32_be(hdr, 92);
    let type_code = [hdr[96], hdr[97], hdr[98], hdr[99]];
    let creator_code = [hdr[100], hdr[101], hdr[102], hdr[103]];
    let finder_flags = u16_be(hdr, 104);

    match id {
        MAR_FILE => {
            let data_size = u32_be(hdr, 72);
            let rsrc_size = u32_be(hdr, 76);
            let comment_size = u32_be(hdr, 80);
            let data_fork = read_block(data, pos, data_size, "data")?;
            let rsrc_fork = read_block(data, pos, rsrc_size, "resource")?;
            // Comments are decoded + CRC-verified for integrity but dropped
            // (the archive model has nowhere to put a Finder comment).
            let _ = read_block(data, pos, comment_size, "comment")?;
            entries.push(StuffItEntry {
                path,
                name,
                is_dir: false,
                type_code,
                creator_code,
                finder_flags,
                create_date,
                mod_date,
                data: data_fork,
                rsrc: rsrc_fork,
            });
        }
        MAR_FOLDER_START => {
            entries.push(StuffItEntry {
                path: path.clone(),
                name,
                is_dir: true,
                type_code,
                creator_code,
                finder_flags,
                create_date,
                mod_date,
                data: None,
                rsrc: None,
            });
            loop {
                if *pos + HEADER_LEN > data.len() {
                    bail!("MAR folder is not terminated (missing '----' record)");
                }
                let child = &data[*pos..*pos + HEADER_LEN];
                if !is_mar_header(child) {
                    bail!("bad MAR record header inside folder");
                }
                if u32_be(child, 4) == MAR_FOLDER_END {
                    *pos += HEADER_LEN;
                    break;
                }
                parse_record(data, pos, &path, entries)?;
            }
        }
        MAR_FOLDER_END => bail!("unexpected MAR '----' (folder end) record"),
        other => bail!("unknown MAR record id {other:#010x}"),
    }
    Ok(())
}

/// Read the name P-string out of a record header (Mac-Roman → UTF-8).
fn read_name(hdr: &[u8]) -> String {
    let len = (hdr[NAME_OFF] as usize).min(NAME_MAX);
    decode_mac_filename(&hdr[NAME_OFF + 1..NAME_OFF + 1 + len])
}

/// Read one fork/comment block: `size` payload bytes, a 4-byte big-endian
/// MAR CRC-32 (verified), then zero padding to a 128-byte boundary. Advances
/// `*pos` past the whole block. Returns a stored [`ForkInfo`] pointing at the
/// payload, or `None` for an empty block.
fn read_block(data: &[u8], pos: &mut usize, size: u32, what: &str) -> Result<Option<ForkInfo>> {
    if size == 0 {
        return Ok(None);
    }
    let size = size as usize;
    let payload_start = *pos;
    let crc_at = payload_start + size;
    let pad = (128 - ((size + 4) & 127)) & 127;
    let block_end = crc_at + 4 + pad;
    if block_end > data.len() {
        bail!("MAR {what} block extends past end of archive");
    }
    let payload = &data[payload_start..crc_at];
    let want = u32_be(data, crc_at);
    let got = mar_crc32(payload);
    if got != want {
        bail!("MAR {what} fork CRC mismatch (got {got:#010x}, expected {want:#010x})");
    }
    *pos = block_end;
    Ok(Some(ForkInfo {
        method: 0,
        codec: ForkCodec::StuffIt,
        encrypted: false,
        uncompressed_len: size as u32,
        compressed_len: size as u32,
        // The shared store-path decoder (`stuffit::decompress_fork`) re-checks
        // CRC-16/ARC, so seed it with the value of the verbatim payload.
        crc: crc16_arc(payload),
        crc32: 0,
        offset: payload_start as u64,
    }))
}

// ---------------------------------------------------------------------------
// Writer (stored)
// ---------------------------------------------------------------------------

/// Build a stored MAR archive. A MAR holds a single root item: with one input
/// node the node becomes the root; with several, they're wrapped in a folder
/// named `root_name`. Forks are stored uncompressed (this tool never writes the
/// optional LZW/LZ4 wrapper). The result round-trips through [`parse`] and is
/// read by the `mar` tool.
pub fn build_archive(root_name: &str, tree: &[StuffItInputNode]) -> Result<Vec<u8>> {
    if tree.is_empty() {
        bail!("cannot build an empty MAR archive");
    }
    let mut out = Vec::new();
    if tree.len() == 1 {
        emit_node(&mut out, &tree[0])?;
    } else {
        emit_folder(&mut out, root_name, 0, 0, 0, tree)?;
    }
    Ok(out)
}

fn emit_node(out: &mut Vec<u8>, node: &StuffItInputNode) -> Result<()> {
    match node {
        StuffItInputNode::File(f) => {
            let mut hdr = [0u8; HEADER_LEN];
            put_u32(&mut hdr, 0, MAR_MAGIC);
            put_u32(&mut hdr, 4, MAR_FILE);
            write_name(&mut hdr, &f.name)?;
            put_u32(&mut hdr, 72, f.data_fork.len() as u32);
            put_u32(&mut hdr, 76, f.resource_fork.len() as u32);
            // comment_size (80) and file_flags (84) stay 0.
            put_u32(&mut hdr, 88, f.create_date);
            put_u32(&mut hdr, 92, f.mod_date);
            hdr[96..100].copy_from_slice(&f.type_code);
            hdr[100..104].copy_from_slice(&f.creator_code);
            put_u16(&mut hdr, 104, f.finder_flags);
            seal_header(&mut hdr);
            out.extend_from_slice(&hdr);
            write_block(out, &f.data_fork);
            write_block(out, &f.resource_fork);
            // No comment block (size 0 -> omitted, matching the mar encoder).
            Ok(())
        }
        StuffItInputNode::Folder {
            name,
            finder_flags,
            create_date,
            mod_date,
            children,
        } => emit_folder(out, name, *finder_flags, *create_date, *mod_date, children),
    }
}

fn emit_folder(
    out: &mut Vec<u8>,
    name: &str,
    finder_flags: u16,
    create_date: u32,
    mod_date: u32,
    children: &[StuffItInputNode],
) -> Result<()> {
    let mut hdr = [0u8; HEADER_LEN];
    put_u32(&mut hdr, 0, MAR_MAGIC);
    put_u32(&mut hdr, 4, MAR_FOLDER_START);
    write_name(&mut hdr, name)?;
    // sizes (72/76/80) and flags (84) stay 0 for a folder.
    put_u32(&mut hdr, 88, create_date);
    put_u32(&mut hdr, 92, mod_date);
    put_u16(&mut hdr, 104, finder_flags);
    seal_header(&mut hdr);
    out.extend_from_slice(&hdr);

    for child in children {
        emit_node(out, child)?;
    }

    let mut end = [0u8; HEADER_LEN];
    put_u32(&mut end, 0, MAR_MAGIC);
    put_u32(&mut end, 4, MAR_FOLDER_END);
    seal_header(&mut end);
    out.extend_from_slice(&end);
    Ok(())
}

/// Stamp a header's CRC-32 over bytes 0..124 into bytes 124..128.
fn seal_header(hdr: &mut [u8; HEADER_LEN]) {
    let crc = mar_crc32(&hdr[..124]);
    put_u32(hdr, 124, crc);
}

/// Write a fork/comment block: payload, its CRC-32, then zero padding to a
/// 128-byte boundary. An empty fork writes nothing (matching the reader).
fn write_block(out: &mut Vec<u8>, payload: &[u8]) {
    if payload.is_empty() {
        return;
    }
    out.extend_from_slice(payload);
    out.extend_from_slice(&mar_crc32(payload).to_be_bytes());
    let pad = (128 - ((payload.len() + 4) & 127)) & 127;
    out.resize(out.len() + pad, 0);
}

/// Write a Mac filename as a Pascal string into a header's name field
/// (offset 8, length byte + up to 63 Mac-Roman bytes).
fn write_name(hdr: &mut [u8; HEADER_LEN], name: &str) -> Result<()> {
    let mut bytes = utf8_to_mac_roman(name).unwrap_or_else(|_| {
        name.chars()
            .map(|c| if c.is_ascii() { c as u8 } else { b'_' })
            .collect()
    });
    bytes.truncate(NAME_MAX);
    hdr[NAME_OFF] = bytes.len() as u8;
    hdr[NAME_OFF + 1..NAME_OFF + 1 + bytes.len()].copy_from_slice(&bytes);
    Ok(())
}

fn put_u16(buf: &mut [u8], i: usize, v: u16) {
    buf[i..i + 2].copy_from_slice(&v.to_be_bytes());
}

fn put_u32(buf: &mut [u8], i: usize, v: u32) {
    buf[i..i + 4].copy_from_slice(&v.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::macarchive::stuffit::{decompress_fork, StuffItInput};

    fn file_node(
        name: &str,
        ty: &[u8; 4],
        cr: &[u8; 4],
        data: &[u8],
        rsrc: &[u8],
    ) -> StuffItInputNode {
        StuffItInputNode::File(StuffItInput {
            name: name.to_string(),
            type_code: *ty,
            creator_code: *cr,
            finder_flags: 0,
            create_date: 0xA7E1_1134,
            mod_date: 0xE116_2A21,
            data_fork: data.to_vec(),
            resource_fork: rsrc.to_vec(),
        })
    }

    #[test]
    fn crc32_known_header_passes_roundtrip() {
        // A sealed header's CRC must validate via the reader's check.
        let mut hdr = [0u8; HEADER_LEN];
        put_u32(&mut hdr, 0, MAR_MAGIC);
        put_u32(&mut hdr, 4, MAR_FILE);
        seal_header(&mut hdr);
        assert!(is_mar_header(&hdr));
        // Corrupting any covered byte breaks the header CRC.
        hdr[8] ^= 0x01;
        assert!(!is_mar_header(&hdr));
    }

    #[test]
    fn single_file_roundtrip() {
        let data = vec![0xABu8; 5000]; // multi-block, exercises padding
        let rsrc = b"a resource fork".to_vec();
        let node = file_node("Test File", b"APPL", b"TEST", &data, &rsrc);
        let bytes = build_archive("ignored", &[node]).unwrap();

        assert!(is_mar(&bytes), "writer output is detected as MAR");
        let (buf, arc) = parse(&bytes).unwrap();
        assert_eq!(arc.entries.len(), 1);
        let e = &arc.entries[0];
        assert_eq!(e.name, "Test File");
        assert!(!e.is_dir);
        assert_eq!(&e.type_code, b"APPL");
        assert_eq!(&e.creator_code, b"TEST");
        assert_eq!(e.create_date, 0xA7E1_1134);
        let df = decompress_fork(&buf, e.data.as_ref().unwrap()).unwrap();
        assert_eq!(df, data, "data fork round-trips byte-exact");
        let rf = decompress_fork(&buf, e.rsrc.as_ref().unwrap()).unwrap();
        assert_eq!(rf, rsrc, "resource fork round-trips byte-exact");
    }

    #[test]
    fn data_only_and_rsrc_only_files() {
        // A file with no resource fork, and one with no data fork.
        let nodes = vec![
            file_node("DataOnly", b"TEXT", b"ttxt", b"hello", &[]),
            file_node("RsrcOnly", b"rsrc", b"RSED", &[], b"only rsrc"),
        ];
        let bytes = build_archive("Holder", &nodes).unwrap();
        let (buf, arc) = parse(&bytes).unwrap();
        // Two files wrapped in the synthetic "Holder" folder.
        assert!(arc.entries.iter().any(|e| e.is_dir && e.name == "Holder"));
        let d = arc.entries.iter().find(|e| e.name == "DataOnly").unwrap();
        assert!(d.data.is_some() && d.rsrc.is_none());
        assert_eq!(
            decompress_fork(&buf, d.data.as_ref().unwrap()).unwrap(),
            b"hello"
        );
        let r = arc.entries.iter().find(|e| e.name == "RsrcOnly").unwrap();
        assert!(r.data.is_none() && r.rsrc.is_some());
        assert_eq!(
            decompress_fork(&buf, r.rsrc.as_ref().unwrap()).unwrap(),
            b"only rsrc"
        );
    }

    #[test]
    fn nested_folder_roundtrip() {
        let inner = StuffItInputNode::Folder {
            name: "Inner".to_string(),
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            children: vec![file_node("Leaf", b"TEXT", b"ttxt", b"leaf data", &[])],
        };
        let root = StuffItInputNode::Folder {
            name: "Root".to_string(),
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            children: vec![file_node("Top", b"TEXT", b"ttxt", b"top data", &[]), inner],
        };
        let bytes = build_archive("ignored", &[root]).unwrap();
        let (_buf, arc) = parse(&bytes).unwrap();
        let leaf = arc.entries.iter().find(|e| e.name == "Leaf").unwrap();
        assert_eq!(
            leaf.path,
            vec!["Root", "Inner", "Leaf"],
            "nested path reconstructed"
        );
        let top = arc.entries.iter().find(|e| e.name == "Top").unwrap();
        assert_eq!(top.path, vec!["Root", "Top"]);
    }

    #[test]
    fn rejects_non_mar() {
        assert!(parse(b"not a mar at all, definitely not 128 bytes of header....").is_err());
        // LZW/LZ4-wrapped MAR is recognized but reported as unsupported.
        let lzw = parse(&[0x1f, 0x9d, 0x90, 0x00]).unwrap_err().to_string();
        assert!(lzw.contains("LZW"), "LZW wrapper reported: {lzw}");
        let lz4 = parse(&[0x04, 0x22, 0x4d, 0x18, 0x00])
            .unwrap_err()
            .to_string();
        assert!(lz4.contains("LZ4"), "LZ4 wrapper reported: {lz4}");
    }

    /// Validate against a real `.mar` if one is present locally (not committed).
    /// Mirrors `stuffit::parse_real_samples_if_present`. `parse` verifies every
    /// fork's MAR CRC-32, so a clean parse proves byte-correct decoding.
    #[test]
    fn parse_real_mar_if_present() {
        let path = std::path::Path::new("/Users/dani/Downloads/Bumpaint_1.0.mar");
        let Ok(raw) = std::fs::read(path) else {
            eprintln!("skipping: {} not present", path.display());
            return;
        };
        assert!(is_mar(&raw), "real sample detected as MAR");
        let (buf, arc) = parse(&raw).expect("real MAR parses (all fork CRCs verified)");
        assert_eq!(arc.entries.len(), 1, "Bumpaint is a single application");
        let e = &arc.entries[0];
        assert_eq!(e.name, "Bumpaint 1.0");
        assert_eq!(&e.type_code, b"APPL");
        assert_eq!(&e.creator_code, b"BUMP");
        let data = e.data.as_ref().expect("APPL has a data fork");
        let rsrc = e.rsrc.as_ref().expect("APPL has a resource fork");
        // 0x0002983a / 0x000143ee from the header (parse already CRC-checked).
        assert_eq!(data.uncompressed_len, 170_042);
        assert_eq!(rsrc.uncompressed_len, 82_926);
        assert_eq!(decompress_fork(&buf, data).unwrap().len(), 170_042);
        assert_eq!(decompress_fork(&buf, rsrc).unwrap().len(), 82_926);
    }
}
