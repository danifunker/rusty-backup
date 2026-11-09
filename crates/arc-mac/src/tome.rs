// SPDX-License-Identifier: MIT
// InstaCompOne's length and literal codes started from ResDecompress (MIT); see THIRD_PARTY_NOTICES.md.
//! Apple Installer Tome reader (file type `idcp`, creator `kakc`).
//!
//! A Tome is the archive Apple's Installer (System 6 through Mac OS 9) reads files from: a flat
//! list of Mac files, each with a data fork, a resource fork and Finder info. Forks are stored
//! raw or compressed with one of two LZ77 + static-prefix-code codecs: InstaCompOne (binary) and
//! a text variant this module calls codec 0 (7-bit literals, its own tables). Neither format has
//! a public spec; the layout below was reverse-engineered against installed copies of the same
//! files. See `docs/installer_tome.md` in rusty-backup for the full write-up.
//!
//! Layout:
//! - Header, 0x24 bytes: magic `6b 63 00 01` (`kc\0\x01`), u16 record count at 0x1a.
//! - One 0x80-byte record per file: Str31 name at +0x06, type +0x26, creator +0x2a, creation
//!   date +0x2e, modification date +0x32, Finder flags +0x36, data fork at +0x3c and resource
//!   fork at +0x4c (each u32 length, u32 offset, u32 compressed length, u32 checksum).
//! - A fork's stream is a sequence of blocks, one per absolute 64 KiB of output, each with a
//!   4-byte header: byte 0 = 1 stored, else byte 1 = 1 InstaCompOne, 0 codec 0.
//!
//! The per-fork checksum is not understood yet, so a decode is checked by the stream ending
//! exactly at the stated compressed length ([`DecodedFork::consumed`]).

use anyhow::{bail, Context, Result};
use byteorder::{BigEndian, ByteOrder};
use fs_mac::hfs::decode_mac_filename;

/// `kc\0\x01` at offset 0.
pub const TOME_MAGIC: [u8; 4] = [0x6b, 0x63, 0x00, 0x01];
const HEADER_LEN: usize = 0x24;
const RECORD_LEN: usize = 0x80;
/// Each block of a fork stream covers output up to the next multiple of this.
const BLOCK_LEN: usize = 65536;
/// Largest fork decoded; real forks are a few MB, and a crafted length must not commit 4 GB.
pub const MAX_FORK_LEN: usize = 256 << 20;

/// One fork of a Tome entry, as stored in its record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TomeFork {
    /// Decoded length. Can be an allocation size larger than the real content; the tail is zeros.
    pub len: u32,
    /// Absolute offset of the fork's stream in the Tome.
    pub offset: u32,
    /// Length of the stream in the Tome. 0 means no fork.
    pub compressed_len: u32,
    /// Stored checksum. Its algorithm is unknown; not verified.
    pub checksum: u32,
}

impl TomeFork {
    fn read(r: &[u8]) -> Self {
        TomeFork {
            len: BigEndian::read_u32(&r[0..4]),
            offset: BigEndian::read_u32(&r[4..8]),
            compressed_len: BigEndian::read_u32(&r[8..12]),
            checksum: BigEndian::read_u32(&r[12..16]),
        }
    }

    /// True when the record has no stream for this fork.
    pub fn is_empty(&self) -> bool {
        self.compressed_len == 0
    }
}

/// One file in a Tome.
#[derive(Debug, Clone)]
pub struct TomeEntry {
    /// Position in the record table.
    pub index: usize,
    /// Name bytes as stored (Mac Roman, at most 31).
    pub name_raw: Vec<u8>,
    /// Name decoded to UTF-8.
    pub name: String,
    pub type_code: [u8; 4],
    pub creator_code: [u8; 4],
    /// Mac 1904-epoch timestamps.
    pub create_date: u32,
    pub mod_date: u32,
    pub finder_flags: u16,
    pub data: TomeFork,
    pub rsrc: TomeFork,
}

/// How one block of a fork stream is coded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockKind {
    Stored,
    InstaCompOne,
    /// Codec 0: InstaCompOne's grammar with 7-bit literals and its own tables. Used for text.
    Text,
}

/// A decoded fork.
#[derive(Debug, Clone)]
pub struct DecodedFork {
    /// Exactly [`TomeFork::len`] bytes.
    pub data: Vec<u8>,
    /// Stream bytes the decoder used. Equal to [`TomeFork::compressed_len`] for a good fork.
    pub consumed: usize,
    /// Kind of each block, in order.
    pub blocks: Vec<BlockKind>,
}

/// True when `bytes` starts with a Tome header whose record table fits in the file.
pub fn is_tome(bytes: &[u8]) -> bool {
    if bytes.len() < HEADER_LEN || bytes[0..4] != TOME_MAGIC {
        return false;
    }
    let count = BigEndian::read_u16(&bytes[0x1a..0x1c]) as usize;
    count > 0 && HEADER_LEN + count * RECORD_LEN <= bytes.len()
}

/// Read the record table.
pub fn parse(bytes: &[u8]) -> Result<Vec<TomeEntry>> {
    if bytes.len() < HEADER_LEN || bytes[0..4] != TOME_MAGIC {
        bail!("not an Installer Tome (no kc 00 01 magic)");
    }
    let count = BigEndian::read_u16(&bytes[0x1a..0x1c]) as usize;
    let table_end = HEADER_LEN + count * RECORD_LEN;
    if table_end > bytes.len() {
        bail!("Tome record table ({count} records) runs past the end of the file");
    }
    let mut entries = Vec::with_capacity(count);
    for index in 0..count {
        let r = &bytes[HEADER_LEN + index * RECORD_LEN..HEADER_LEN + (index + 1) * RECORD_LEN];
        // Str31; the bytes after the string are often uninitialized memory.
        let name_len = (r[6] as usize).min(31);
        let name_raw = r[7..7 + name_len].to_vec();
        entries.push(TomeEntry {
            index,
            name: decode_mac_filename(&name_raw),
            name_raw,
            type_code: r[0x26..0x2a].try_into().unwrap(),
            creator_code: r[0x2a..0x2e].try_into().unwrap(),
            create_date: BigEndian::read_u32(&r[0x2e..0x32]),
            mod_date: BigEndian::read_u32(&r[0x32..0x36]),
            finder_flags: BigEndian::read_u16(&r[0x36..0x38]),
            data: TomeFork::read(&r[0x3c..0x4c]),
            rsrc: TomeFork::read(&r[0x4c..0x5c]),
        });
    }
    Ok(entries)
}

/// Decode one fork of a Tome held in `bytes`.
pub fn decode_fork(bytes: &[u8], fork: &TomeFork) -> Result<DecodedFork> {
    let start = fork.offset as usize;
    let end = start
        .checked_add(fork.compressed_len as usize)
        .filter(|&e| e <= bytes.len())
        .with_context(|| {
            format!(
                "fork stream at {start} (+{}) runs past the end of the Tome",
                fork.compressed_len
            )
        })?;
    decode_stream(&bytes[start..end], fork.len as usize)
}

/// Decode a fork stream (exactly its compressed bytes) to `len` bytes, zero-filling a short end.
/// A token that would read past the input is end-of-stream padding and is dropped.
pub fn decode_stream(input: &[u8], len: usize) -> Result<DecodedFork> {
    if len > MAX_FORK_LEN {
        bail!("Tome fork of {len} bytes is larger than any real one ({MAX_FORK_LEN} max)");
    }
    let limit = input.len() * 8;
    let mut out: Vec<u8> = Vec::with_capacity(len + 2100);
    let mut blocks = Vec::new();
    let mut p = 0usize; // byte position of the next block header
    let mut block_end = 0usize;

    while out.len() < len && p < input.len() {
        block_end += BLOCK_LEN;
        let target = block_end.min(len);
        let hdr = input
            .get(p..p + 4)
            .with_context(|| format!("truncated block header at stream offset {p}"))?;
        p += 4;
        if hdr[0] == 1 {
            blocks.push(BlockKind::Stored);
            let n = target.saturating_sub(out.len());
            let raw = input
                .get(p..p + n)
                .with_context(|| format!("short stored block at output {}", out.len()))?;
            out.extend_from_slice(raw);
            p += n;
            continue;
        }
        let codec = match hdr[1] {
            1 => &INSTACOMP_ONE,
            0 => &TEXT,
            _ => bail!(
                "unknown Tome block codec {:02x}{:02x}{:02x}{:02x} at output {}",
                hdr[0],
                hdr[1],
                hdr[2],
                hdr[3],
                out.len()
            ),
        };
        blocks.push(codec.kind);

        // Bit position resumes at the next byte boundary after a compressed block.
        let mut br = BitReader {
            data: input,
            pos: p * 8,
        };
        let ended = decode_block(&mut br, codec, &mut out, target, limit)?;
        p = br.pos.div_ceil(8);
        if ended {
            break;
        }
    }
    let consumed = p.min(input.len());
    out.resize(len, 0);
    Ok(DecodedFork {
        data: out,
        consumed,
        blocks,
    })
}

/// Decode tokens until `out` reaches `target` or the input ends. Returns true when a padding
/// token was dropped (end of stream).
fn decode_block(
    br: &mut BitReader,
    codec: &Codec,
    out: &mut Vec<u8>,
    target: usize,
    limit: usize,
) -> Result<bool> {
    // After a literal run shorter than 63, the next token is always a copy one byte longer.
    let mut mode = 1;
    while out.len() < target && br.pos < limit {
        let c = br.prefix_code(codec.lengths)?;
        if c > 0 || mode == 0 {
            let n = c as usize + 2 + usize::from(mode == 0);
            let d = distance(br, out.len(), codec);
            if br.pos > limit {
                br.pos = limit;
                return Ok(true);
            }
            if d > out.len() {
                bail!(
                    "copy distance {d} reaches before the start at output {}",
                    out.len()
                );
            }
            let from = out.len() - d;
            for i in 0..n {
                out.push(out[from + i]); // may overlap
            }
            mode = 1;
        } else {
            let n = br.prefix_code(LITERAL_COUNTS)? as usize;
            if br.pos + n * codec.literal_bits as usize > limit {
                br.pos = limit;
                return Ok(true);
            }
            for _ in 0..n {
                out.push(br.bits(codec.literal_bits) as u8);
            }
            mode = if n < 63 { 0 } else { 1 };
        }
    }
    Ok(false)
}

/// One prefix code: `width` bits equal to `code`, then `extra` value bits added to `base`.
struct Code {
    code: u16,
    width: u8,
    extra: u8,
    base: u16,
}

const fn k(code: u16, width: u8, extra: u8, base: u16) -> Code {
    Code {
        code,
        width,
        extra,
        base,
    }
}

/// InstaCompOne copy-length codes (the value c; a copy is c + 2 bytes).
const LENGTHS_IC1: &[Code] = &[
    k(0b00, 2, 0, 0),
    k(0b01, 2, 0, 1),
    k(0b100, 3, 0, 2),
    k(0b1010, 4, 0, 3),
    k(0b1011, 4, 0, 4),
    k(0b1110, 4, 3, 11),
    k(0b11000, 5, 0, 5),
    k(0b11001, 5, 0, 6),
    k(0b11110, 5, 3, 19),
    k(0b110100, 6, 0, 7),
    k(0b110101, 6, 0, 8),
    k(0b110110, 6, 0, 9),
    k(0b110111, 6, 0, 10),
    k(0b111110, 6, 5, 27),
    k(0b1111110, 7, 6, 59),
    k(0b11111110, 8, 7, 123),
    k(0b111111110, 9, 8, 251),
    k(0b1111111110, 10, 9, 507),
    // ResDecompress has 11111111110 + 10 bits here.
    k(0b1111111111, 10, 10, 1019),
];

/// Codec 0 copy-length codes. Codes from 111110 up follow the pattern of the shorter ones; they
/// decode every known stream with exact lengths but no single long copy has confirmed them.
const LENGTHS_TEXT: &[Code] = &[
    k(0b000, 3, 0, 0),
    k(0b001, 3, 0, 1),
    k(0b010, 3, 0, 2),
    k(0b011, 3, 0, 3),
    k(0b110, 3, 2, 8),
    k(0b1000, 4, 0, 4),
    k(0b1001, 4, 0, 5),
    k(0b1010, 4, 0, 6),
    k(0b1011, 4, 0, 7),
    k(0b1110, 4, 3, 12),
    k(0b11110, 5, 4, 20),
    k(0b111110, 6, 5, 36),
    k(0b1111110, 7, 6, 68),
    k(0b11111110, 8, 7, 132),
    k(0b111111110, 9, 8, 260),
    k(0b1111111110, 10, 9, 516),
    k(0b1111111111, 10, 10, 1028),
];

/// Literal-run counts, shared by both codecs.
const LITERAL_COUNTS: &[Code] = &[
    k(0b0, 1, 0, 1),
    k(0b100, 3, 0, 2),
    k(0b101, 3, 0, 3),
    k(0b11000, 5, 0, 4),
    k(0b11001, 5, 0, 5),
    k(0b11010, 5, 0, 6),
    k(0b11011, 5, 0, 7),
    k(0b11110, 5, 4, 16),
    k(0b11111, 5, 5, 32),
    k(0b1110000, 7, 0, 8),
    k(0b1110001, 7, 0, 9),
    k(0b1110010, 7, 0, 10),
    k(0b1110011, 7, 0, 11),
    k(0b1110100, 7, 0, 12),
    k(0b1110101, 7, 0, 13),
    k(0b1110110, 7, 0, 14),
    k(0b1110111, 7, 0, 15),
];

struct Codec {
    kind: BlockKind,
    lengths: &'static [Code],
    literal_bits: u8,
    /// Distance level = how many of these are below the output position (0..=11).
    level_limits: [usize; 11],
    /// InstaCompOne widens the long distance field to 11 bits early (see [`long_bits`]).
    early_11: bool,
}

const INSTACOMP_ONE: Codec = Codec {
    kind: BlockKind::InstaCompOne,
    lengths: LENGTHS_IC1,
    literal_bits: 8,
    level_limits: [10, 20, 40, 80, 160, 672, 1000, 2688, 5376, 10752, 21504],
    early_11: true,
};

const TEXT: Codec = Codec {
    kind: BlockKind::Text,
    lengths: LENGTHS_TEXT,
    literal_bits: 7,
    level_limits: [10, 20, 40, 80, 160, 320, 832, 1280, 2560, 5120, 10240],
    early_11: false,
};

fn level(pos: usize, limits: &[usize; 11]) -> u32 {
    limits.iter().take_while(|&&l| pos > l).count() as u32
}

/// Long distance field width for x = pos - 5 * 2^level: ceil(log2 x) in 1..=15.
/// InstaCompOne widens to 11 bits at x = 1005, not 1025.
fn long_bits(x: i64, early_11: bool) -> u32 {
    let mut n = if x < 2 {
        1
    } else {
        64 - ((x - 1) as u64).leading_zeros()
    };
    if early_11 && 1004 < x && x <= 1024 {
        n = 11;
    }
    n.min(15)
}

/// Read a copy distance at output position `pos`.
fn distance(br: &mut BitReader, pos: usize, codec: &Codec) -> usize {
    let s = level(pos, &codec.level_limits);
    if br.bits(1) == 0 {
        return br.bits(s as u8) as usize + 1;
    }
    if br.bits(1) == 0 {
        return br.bits(s as u8 + 2) as usize + (1 << s) + 1;
    }
    let near = 5usize << s;
    let n = long_bits(pos as i64 - near as i64, codec.early_11);
    br.bits(n as u8) as usize + near + 1
}

/// MSB-first. Bits past the end of the input read as 0; callers compare `pos` with the limit.
struct BitReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl BitReader<'_> {
    /// Next `n` (at most 24) bits without consuming them.
    fn peek(&self, n: u8) -> u32 {
        if n == 0 {
            return 0;
        }
        let byte = self.pos >> 3;
        let mut word = 0u32;
        for i in 0..4 {
            word = (word << 8) | u32::from(self.data.get(byte + i).copied().unwrap_or(0));
        }
        (word << (self.pos & 7)) >> (32 - u32::from(n))
    }

    fn bits(&mut self, n: u8) -> u32 {
        let v = self.peek(n);
        self.pos += n as usize;
        v
    }

    fn prefix_code(&mut self, table: &[Code]) -> Result<u32> {
        for c in table {
            if self.peek(c.width) == u32::from(c.code) {
                self.pos += c.width as usize;
                return Ok(u32::from(c.base) + self.bits(c.extra));
            }
        }
        bail!("no prefix code matches at bit {}", self.pos) // unreachable: the tables are complete
    }
}

/// One test file for [`build_stored_tome`]: name, type, creator, data fork, resource fork.
#[cfg(test)]
pub(crate) type TestFile<'a> = (&'a str, [u8; 4], [u8; 4], &'a [u8], &'a [u8]);

/// A Tome of `files` with stored forks, for tests.
#[cfg(test)]
pub(crate) fn build_stored_tome(files: &[TestFile]) -> Vec<u8> {
    let mut t = vec![0u8; HEADER_LEN + files.len() * RECORD_LEN];
    t[0..4].copy_from_slice(&TOME_MAGIC);
    BigEndian::write_u16(&mut t[0x1a..0x1c], files.len() as u16);
    for (i, (name, ty, cr, data, rsrc)) in files.iter().enumerate() {
        let mut forks = [TomeFork::default(); 2];
        for (fork, bytes) in forks.iter_mut().zip([*data, *rsrc]) {
            if bytes.is_empty() {
                continue;
            }
            fork.len = bytes.len() as u32;
            fork.offset = t.len() as u32;
            for chunk in bytes.chunks(BLOCK_LEN) {
                t.extend_from_slice(&[1, 0, 0, 0]);
                t.extend_from_slice(chunk);
            }
            fork.compressed_len = t.len() as u32 - fork.offset;
        }
        let r = HEADER_LEN + i * RECORD_LEN;
        t[r + 6] = name.len() as u8;
        t[r + 7..r + 7 + name.len()].copy_from_slice(name.as_bytes());
        t[r + 0x26..r + 0x2a].copy_from_slice(ty);
        t[r + 0x2a..r + 0x2e].copy_from_slice(cr);
        for (at, f) in [(0x3c, forks[0]), (0x4c, forks[1])] {
            for (k, v) in [f.len, f.offset, f.compressed_len, f.checksum]
                .into_iter()
                .enumerate()
            {
                BigEndian::write_u32(&mut t[r + at + 4 * k..r + at + 4 * k + 4], v);
            }
        }
    }
    t
}

#[cfg(test)]
mod tests {
    use super::*;

    /// MSB-first bit writer for building test streams.
    struct BitWriter {
        out: Vec<u8>,
        nbits: usize,
    }

    impl BitWriter {
        fn new(header: [u8; 4]) -> Self {
            BitWriter {
                out: header.to_vec(),
                nbits: 32,
            }
        }
        fn put(&mut self, v: u32, n: u32) {
            for i in (0..n).rev() {
                if self.nbits % 8 == 0 {
                    self.out.push(0);
                }
                let bit = ((v >> i) & 1) as u8;
                *self.out.last_mut().unwrap() |= bit << (7 - (self.nbits % 8));
                self.nbits += 1;
            }
        }
    }

    #[test]
    fn tables_are_complete_prefix_codes() {
        for table in [LENGTHS_IC1, LENGTHS_TEXT, LITERAL_COUNTS] {
            let kraft: f64 = table.iter().map(|c| 0.5f64.powi(c.width as i32)).sum();
            assert!((kraft - 1.0).abs() < 1e-12);
            for a in table {
                for b in table {
                    if std::ptr::eq(a, b) || a.width > b.width {
                        continue;
                    }
                    assert_ne!(b.code >> (b.width - a.width), a.code, "prefix clash");
                }
            }
        }
    }

    #[test]
    fn levels_and_long_field() {
        let l = &INSTACOMP_ONE.level_limits;
        assert_eq!(level(10, l), 0);
        assert_eq!(level(11, l), 1);
        assert_eq!(level(1001, l), 7);
        assert_eq!(level(100_000, l), 11);
        assert_eq!(long_bits(-3, true), 1);
        assert_eq!(long_bits(1004, true), 10);
        assert_eq!(long_bits(1005, true), 11);
        assert_eq!(long_bits(1005, false), 10);
        assert_eq!(long_bits(1025, false), 11);
        assert_eq!(long_bits(1 << 20, false), 15);
    }

    #[test]
    fn instacomp_one_literals_then_copy() {
        let mut w = BitWriter::new([0, 1, 0, 0]);
        w.put(0b00, 2); // c = 0 in mode 1: literal run
        w.put(0b101, 3); // 3 literals
        for &b in b"abc" {
            w.put(b as u32, 8);
        }
        w.put(0b00, 2); // mode 0: copy of 0 + 2 + 1
        w.put(0b0, 1); // distance: level 0, '0' + 0 bits = 1
        let d = decode_stream(&w.out, 8).unwrap();
        assert_eq!(d.data, b"abcccc\0\0");
        assert_eq!(d.consumed, w.out.len());
        assert_eq!(d.blocks, [BlockKind::InstaCompOne]);
    }

    #[test]
    fn text_codec_uses_7_bit_literals() {
        let mut w = BitWriter::new([0, 0, 0, 0]);
        w.put(0b000, 3); // c = 0: literal run
        w.put(0b100, 3); // 2 literals
        w.put(b'H' as u32, 7);
        w.put(b'i' as u32, 7);
        w.put(0b001, 3); // mode 0: copy of 1 + 2 + 1 = 4
        w.put(0b10, 2); // distance '10' + (0 + 2) bits: d = v + 1 + 1
        w.put(0b00, 2); // d = 2
        let d = decode_stream(&w.out, 6).unwrap();
        assert_eq!(d.data, b"HiHiHi");
        assert_eq!(d.blocks, [BlockKind::Text]);
    }

    #[test]
    fn padding_token_is_dropped() {
        let mut w = BitWriter::new([0, 1, 0, 0]);
        w.put(0b0, 2);
        w.put(0b0, 1); // 1 literal
        w.put(b'x' as u32, 8);
        w.put(0b11111, 5); // mode 0 copy whose distance runs past the end: padding
        let d = decode_stream(&w.out, 4).unwrap();
        assert_eq!(d.data, b"x\0\0\0");
        assert_eq!(d.consumed, w.out.len());
    }

    #[test]
    fn stored_block() {
        let mut s = vec![1, 0, 0, 0];
        s.extend_from_slice(b"plain");
        let d = decode_stream(&s, 5).unwrap();
        assert_eq!(d.data, b"plain");
        assert_eq!(d.consumed, s.len());
        assert!(decode_stream(&s, 6).is_err());
    }

    #[test]
    fn a_built_tome_round_trips_across_blocks() {
        let big: Vec<u8> = (0..BLOCK_LEN + 300).map(|i| (i % 251) as u8).collect();
        let t = build_stored_tome(&[
            ("A", *b"TEXT", *b"ttxt", b"hello", b""),
            ("B", *b"APPL", *b"????", &big, b"rs"),
        ]);
        let e = parse(&t).unwrap();
        assert_eq!(decode_fork(&t, &e[0].data).unwrap().data, b"hello");
        let d = decode_fork(&t, &e[1].data).unwrap();
        assert_eq!(
            (d.data == big, d.consumed, d.blocks.len()),
            (true, e[1].data.compressed_len as usize, 2)
        );
        assert_eq!(decode_fork(&t, &e[1].rsrc).unwrap().data, b"rs");
    }

    #[test]
    fn an_oversized_fork_is_refused_before_allocating() {
        let err = decode_stream(&[1, 0, 0, 0], MAX_FORK_LEN + 1).unwrap_err();
        assert!(
            err.to_string().contains("larger than any real one"),
            "{err}"
        );
    }

    #[test]
    fn unknown_codec_is_an_error() {
        assert!(decode_stream(&[0, 2, 0, 0, 0], 4).is_err());
    }

    #[test]
    fn parses_records() {
        let mut t = vec![0u8; HEADER_LEN + RECORD_LEN];
        t[0..4].copy_from_slice(&TOME_MAGIC);
        t[0x1b] = 1;
        let r = &mut t[HEADER_LEN..];
        r[6] = 4;
        r[7..11].copy_from_slice(b"Beep");
        r[0x26..0x2e].copy_from_slice(b"sfilmovr");
        r[0x36] = 0x01;
        r[0x4c + 3] = 9; // rsrc len 9
        r[0x4c + 11] = 5; // rsrc clen 5
        assert!(is_tome(&t));
        let e = &parse(&t).unwrap()[0];
        assert_eq!(e.name, "Beep");
        assert_eq!(&e.type_code, b"sfil");
        assert_eq!(&e.creator_code, b"movr");
        assert_eq!(e.finder_flags, 0x0100);
        assert!(e.data.is_empty());
        assert_eq!((e.rsrc.len, e.rsrc.compressed_len), (9, 5));
        assert!(!is_tome(&t[..HEADER_LEN]));
    }
}
