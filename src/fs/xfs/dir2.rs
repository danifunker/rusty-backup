//! XFS dir2 directory parsers: shortform (`xfs_dir2_sf_*`), single-block
//! (`xfs_dir2_data_*` with `XD2B` magic), and multi-block data blocks
//! (`XD2D` magic) used by leaf/node format directories. Sufficient for the
//! dir2 fixture, the dir2 IRIX 6.5 root directory, and any leaf/node
//! directory we encounter (the hash btree itself is skipped — for browse
//! we only need to enumerate entries, not look them up by name).
//!
//! References:
//! - `~/efs-xfs-refs/xfs-modern/xfs/libxfs/xfs_da_format.h`
//! - "XFS Algorithms & Data Structures" §5.

use byteorder::{BigEndian, ByteOrder};

use crate::fs::filesystem::FilesystemError;

/// `XD2B` — single-block dir2 magic (v4, no CRC).
pub const XFS_DIR2_BLOCK_MAGIC: u32 = 0x5844_3242;
/// `XD2D` — multi-block data block magic (v4, no CRC). Same body layout as
/// `XD2B` but without the trailing leaf-entry / tail section.
pub const XFS_DIR2_DATA_MAGIC: u32 = 0x5844_3244;
/// `XDB3` — single-block dir2 magic (v5/CRC). We reject v5 in the sb, but
/// surface the constant so detection messages can name what we saw.
pub const XFS_DIR3_BLOCK_MAGIC: u32 = 0x5844_4233;

/// Free-tag at the start of an unused-record entry in a dir2 data block.
const XFS_DIR2_DATA_FREE_TAG: u16 = 0xFFFF;

/// One directory entry decoded from any dir2 variant.
#[derive(Debug, Clone)]
pub struct Dir2Entry {
    pub inumber: u64,
    pub name: String,
}

/// Parse shortform-dir2 fork data. The fork layout for v4 (no FT feature):
///   header: count(u8), i8count(u8), parent[4 or 8]
///   entries: namelen(u8), offset[2], name[namelen], inumber[4 or 8]
/// Inumber width is 8 when `i8count > 0`, else 4. Entries do NOT include
/// `.` or `..` (the parent is stored once in the header).
pub fn parse_shortform(
    fork: &[u8],
    has_ftype: bool,
) -> Result<(u64, Vec<Dir2Entry>), FilesystemError> {
    if fork.len() < 6 {
        return Err(FilesystemError::Parse(format!(
            "dir2 shortform fork too small: {} bytes",
            fork.len()
        )));
    }
    let count = fork[0] as usize;
    let i8count = fork[1] as usize;
    let ino_width = if i8count > 0 { 8 } else { 4 };
    let parent_width = ino_width;
    let hdr_size = 2 + parent_width;
    if fork.len() < hdr_size {
        return Err(FilesystemError::Parse(format!(
            "dir2 shortform header overflows fork: need {hdr_size}, have {}",
            fork.len()
        )));
    }
    let parent = read_inumber(&fork[2..2 + parent_width], parent_width);

    let mut pos = hdr_size;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 3 > fork.len() {
            return Err(FilesystemError::Parse(
                "dir2 shortform entry truncated at header".into(),
            ));
        }
        let namelen = fork[pos] as usize;
        // pos+1..pos+3 is the 2-byte offset cookie (unused for browse).
        let name_off = pos + 3;
        let name_end = name_off + namelen;
        // FTYPE byte sits between the name and the inumber when the
        // feature is enabled. See `xfs_dir2_sf_get_ftype` in libxfs/
        // xfs_dir2_sf.c: `ftype = sfep->name[sfep->namelen]`.
        let ftype_extra = usize::from(has_ftype);
        let ino_off = name_end + ftype_extra;
        let next = ino_off + ino_width;
        if next > fork.len() {
            return Err(FilesystemError::Parse(format!(
                "dir2 shortform entry truncated: needs {next}, fork {}",
                fork.len()
            )));
        }
        let name = decode_name(&fork[name_off..name_end]);
        let inumber = read_inumber(&fork[ino_off..ino_off + ino_width], ino_width);
        out.push(Dir2Entry { inumber, name });
        pos = next;
    }
    Ok((parent, out))
}

/// Parse a single-block dir2 buffer (`XD2B` magic). `buf.len()` must equal
/// the directory block size. Returns the contained entries in on-disk
/// order, skipping `.` and `..` (the caller already knows the parent
/// inumber).
pub fn parse_block(buf: &[u8], has_ftype: bool) -> Result<Vec<Dir2Entry>, FilesystemError> {
    if buf.len() < 24 {
        return Err(FilesystemError::Parse(format!(
            "dir2 block buffer too small: {} bytes",
            buf.len()
        )));
    }
    let magic = BigEndian::read_u32(&buf[0..4]);
    if magic == XFS_DIR3_BLOCK_MAGIC {
        return Err(FilesystemError::Unsupported(
            "XFS dir3 (v5/CRC) directory blocks not supported".into(),
        ));
    }
    if magic != XFS_DIR2_BLOCK_MAGIC {
        return Err(FilesystemError::Parse(format!(
            "bad dir2 block magic: 0x{magic:08X} (expected 0x{XFS_DIR2_BLOCK_MAGIC:08X})"
        )));
    }
    // Header: magic(4) + bestfree[3]*(off:2 + len:2) = 4 + 12 = 16 bytes for v4.
    // Tail at end of block: count(4) + stale(4) = 8 bytes. Leaf entries
    // (8 bytes each) precede the tail.
    let block_end = buf.len();
    let tail_off = block_end - 8;
    let leaf_count = BigEndian::read_u32(&buf[tail_off..tail_off + 4]) as usize;
    let leaf_section_size = leaf_count.saturating_mul(8) + 8;
    if leaf_section_size > block_end - 16 {
        return Err(FilesystemError::Parse(format!(
            "dir2 block tail count {leaf_count} overflows block size {block_end}"
        )));
    }
    let data_end = block_end - leaf_section_size;
    parse_data_entries(buf, has_ftype, 16, data_end)
}

/// Parse a multi-block `XD2D` data block. Body layout matches `XD2B` minus
/// the trailing leaf-entry section — entries fill the block right up to the
/// end (free regions are recorded as `XFS_DIR2_DATA_FREE_TAG` unused records
/// inline). Used by leaf-format and node-format dir2 directories.
pub fn parse_data_block(buf: &[u8], has_ftype: bool) -> Result<Vec<Dir2Entry>, FilesystemError> {
    if buf.len() < 16 {
        return Err(FilesystemError::Parse(format!(
            "dir2 data block buffer too small: {} bytes",
            buf.len()
        )));
    }
    let magic = BigEndian::read_u32(&buf[0..4]);
    if magic != XFS_DIR2_DATA_MAGIC {
        return Err(FilesystemError::Parse(format!(
            "bad dir2 data block magic: 0x{magic:08X} (expected 0x{XFS_DIR2_DATA_MAGIC:08X})"
        )));
    }
    parse_data_entries(buf, has_ftype, 16, buf.len())
}

/// Walk the entry/free-tag region of a dir2 data block from `start` up to
/// (but not including) `data_end`. Shared between `XD2B` (single-block) and
/// `XD2D` (multi-block data) parsers — the only difference is whether a
/// leaf+tail section is reserved at the end of the block.
fn parse_data_entries(
    buf: &[u8],
    has_ftype: bool,
    start: usize,
    data_end: usize,
) -> Result<Vec<Dir2Entry>, FilesystemError> {
    let mut pos = start;
    let mut out = Vec::new();
    while pos + 4 <= data_end {
        let freetag = BigEndian::read_u16(&buf[pos..pos + 2]);
        if freetag == XFS_DIR2_DATA_FREE_TAG {
            // Unused record: length is the be16 at pos+2.
            let length = BigEndian::read_u16(&buf[pos + 2..pos + 4]) as usize;
            if length == 0 || pos + length > data_end {
                return Err(FilesystemError::Parse(format!(
                    "dir2 unused entry at {pos} has bad length {length}"
                )));
            }
            pos += length;
            continue;
        }
        // Active data entry: inumber(8) + namelen(1) + name[namelen] + ...
        // entry total padded to 8 bytes.
        if pos + 9 > data_end {
            return Err(FilesystemError::Parse(format!(
                "dir2 data entry header overflows at {pos}"
            )));
        }
        let inumber = BigEndian::read_u64(&buf[pos..pos + 8]);
        let namelen = buf[pos + 8] as usize;
        if namelen == 0 {
            return Err(FilesystemError::Parse(format!(
                "dir2 data entry at {pos} has zero namelen"
            )));
        }
        let name_off = pos + 9;
        let name_end = name_off + namelen;
        if name_end > data_end {
            return Err(FilesystemError::Parse(format!(
                "dir2 data entry name overflows at {pos}"
            )));
        }
        let name = decode_name(&buf[name_off..name_end]);
        // Entry total length:
        //   no FTYPE: align8(8 + 1 + namelen + 2)
        //   FTYPE:    align8(8 + 1 + namelen + 1 + 2)
        let ftype_extra = usize::from(has_ftype);
        let raw_len = 8 + 1 + namelen + ftype_extra + 2;
        let aligned = (raw_len + 7) & !7;
        // Skip `.` and `..` — the FileEntry tree models the hierarchy via paths.
        if name != "." && name != ".." {
            out.push(Dir2Entry { inumber, name });
        }
        pos += aligned;
    }
    Ok(out)
}

fn read_inumber(buf: &[u8], width: usize) -> u64 {
    match width {
        4 => BigEndian::read_u32(buf) as u64,
        8 => BigEndian::read_u64(buf),
        _ => 0,
    }
}

fn decode_name(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a synthetic shortform-dir2 fork with `entries` (each is
    /// (name, inumber)) and the given parent inumber. Uses 4-byte inumbers
    /// (i8count = 0) — sufficient for the tests we need.
    fn build_shortform(parent: u32, entries: &[(&str, u32)]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(entries.len() as u8); // count
        buf.push(0u8); // i8count
        buf.extend_from_slice(&parent.to_be_bytes()); // parent[4]
        for (name, ino) in entries {
            buf.push(name.len() as u8);
            buf.extend_from_slice(&[0, 0]); // offset cookie (unused on read)
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(&(*ino).to_be_bytes());
        }
        buf
    }

    #[test]
    fn shortform_three_entries() {
        let buf = build_shortform(2, &[("alpha", 100), ("beta", 101), ("gamma", 102)]);
        let (parent, entries) = parse_shortform(&buf, false).expect("parse");
        assert_eq!(parent, 2);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].name, "alpha");
        assert_eq!(entries[0].inumber, 100);
        assert_eq!(entries[2].name, "gamma");
        assert_eq!(entries[2].inumber, 102);
    }

    #[test]
    fn shortform_eight_entries() {
        let names = ["a", "bb", "ccc", "dddd", "eeeee", "ffffff", "ggg", "hhhh"];
        let pairs: Vec<(&str, u32)> = names
            .iter()
            .enumerate()
            .map(|(i, n)| (*n, 200 + i as u32))
            .collect();
        let buf = build_shortform(42, &pairs);
        let (parent, entries) = parse_shortform(&buf, false).expect("parse");
        assert_eq!(parent, 42);
        assert_eq!(entries.len(), 8);
        for (i, e) in entries.iter().enumerate() {
            assert_eq!(e.name, names[i]);
            assert_eq!(e.inumber, 200 + i as u64);
        }
    }

    #[test]
    fn shortform_long_name() {
        let long = "a".repeat(255);
        let buf = build_shortform(2, &[(&long, 999)]);
        let (_, entries) = parse_shortform(&buf, false).expect("parse");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name.len(), 255);
        assert_eq!(entries[0].inumber, 999);
    }

    #[test]
    fn shortform_i8count_uses_8byte_inumbers() {
        // Hand-build header with i8count=1 and parent=0x0000_0000_0123_4567.
        let mut buf = Vec::new();
        buf.push(1u8); // count
        buf.push(1u8); // i8count -> 8-byte ino
        buf.extend_from_slice(&0x0123_4567u64.to_be_bytes()); // parent
        buf.push(3); // namelen
        buf.extend_from_slice(&[0, 0]); // offset
        buf.extend_from_slice(b"xyz");
        buf.extend_from_slice(&0x1122_3344_5566_7788u64.to_be_bytes()); // ino
        let (parent, entries) = parse_shortform(&buf, false).expect("parse");
        assert_eq!(parent, 0x0123_4567);
        assert_eq!(entries[0].inumber, 0x1122_3344_5566_7788);
    }

    #[test]
    fn shortform_with_ftype_skips_filetype_byte() {
        // Layout per entry (FT enabled, 4-byte ino):
        //   namelen(1) + offset[2] + name + inumber(4) + ftype(1)
        let mut buf = Vec::new();
        buf.push(2u8); // count
        buf.push(0u8); // i8count
        buf.extend_from_slice(&128u32.to_be_bytes()); // parent[4]
        for (name, ino, ftype) in [("hello.txt", 131u32, 1u8), ("readme.txt", 132u32, 1u8)] {
            buf.push(name.len() as u8);
            buf.extend_from_slice(&[0, 0]); // offset
            buf.extend_from_slice(name.as_bytes());
            buf.push(ftype); // FTYPE byte sits between name and inumber
            buf.extend_from_slice(&ino.to_be_bytes());
        }
        let (parent, entries) = parse_shortform(&buf, true).expect("parse");
        assert_eq!(parent, 128);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "hello.txt");
        assert_eq!(entries[0].inumber, 131);
        assert_eq!(entries[1].name, "readme.txt");
        assert_eq!(entries[1].inumber, 132);
    }

    #[test]
    fn block_with_three_entries_round_trip() {
        // Hand-build a 512-byte dir2 block with header + 3 data entries +
        // tail. The leaf entries themselves don't matter for browse, but
        // tail.count must match the leaf-entry slot count we leave room for.
        let block_size = 512usize;
        let leaf_count = 3u32;
        let leaf_section = (leaf_count as usize) * 8 + 8; // leaf + tail
        let mut buf = vec![0u8; block_size];
        // header
        BigEndian::write_u32(&mut buf[0..4], XFS_DIR2_BLOCK_MAGIC);
        // bestfree[3] zeroed.

        let entries = [(10u64, "alpha"), (11u64, "bb"), (12u64, "longername")];
        let mut pos = 16usize;
        for (ino, name) in entries {
            BigEndian::write_u64(&mut buf[pos..pos + 8], ino);
            buf[pos + 8] = name.len() as u8;
            buf[pos + 9..pos + 9 + name.len()].copy_from_slice(name.as_bytes());
            let raw = 8 + 1 + name.len() + 2;
            let aligned = (raw + 7) & !7;
            // tag at last 2 bytes of entry (unused by parse).
            let tag_off = pos + aligned - 2;
            BigEndian::write_u16(&mut buf[tag_off..tag_off + 2], pos as u16);
            pos += aligned;
        }
        // Fill the gap between data and leaf section with a single unused
        // record so the parser can skip over the free area cleanly.
        let data_end = block_size - leaf_section;
        if pos < data_end {
            BigEndian::write_u16(&mut buf[pos..pos + 2], XFS_DIR2_DATA_FREE_TAG);
            BigEndian::write_u16(&mut buf[pos + 2..pos + 4], (data_end - pos) as u16);
        }
        // tail
        let tail_off = block_size - 8;
        BigEndian::write_u32(&mut buf[tail_off..tail_off + 4], leaf_count);
        BigEndian::write_u32(&mut buf[tail_off + 4..tail_off + 8], 0);

        let parsed = parse_block(&buf, false).expect("parse block");
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].name, "alpha");
        assert_eq!(parsed[0].inumber, 10);
        assert_eq!(parsed[2].name, "longername");
        assert_eq!(parsed[2].inumber, 12);
    }

    #[test]
    fn block_rejects_dir3_magic() {
        let mut buf = vec![0u8; 512];
        BigEndian::write_u32(&mut buf[0..4], XFS_DIR3_BLOCK_MAGIC);
        match parse_block(&buf, false) {
            Err(FilesystemError::Unsupported(msg)) => assert!(msg.contains("dir3")),
            Err(e) => panic!("expected Unsupported, got {e}"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn data_block_parses_xd2d_without_tail() {
        // Multi-block `XD2D` data: header + entries fill the whole block,
        // no trailing leaf/tail. Trailing free-space is one giant unused
        // record (free tag at start). Verifies that `parse_data_block`
        // walks right up to buf.len() and recognises XD2D — not XD2B.
        let block_size = 512usize;
        let mut buf = vec![0u8; block_size];
        BigEndian::write_u32(&mut buf[0..4], XFS_DIR2_DATA_MAGIC);
        let entries = [(50u64, "first"), (51u64, "second.txt"), (52u64, "third")];
        let mut pos = 16usize;
        for (ino, name) in entries {
            BigEndian::write_u64(&mut buf[pos..pos + 8], ino);
            buf[pos + 8] = name.len() as u8;
            buf[pos + 9..pos + 9 + name.len()].copy_from_slice(name.as_bytes());
            let raw = 8 + 1 + name.len() + 2;
            let aligned = (raw + 7) & !7;
            pos += aligned;
        }
        // Fill the rest of the block with a single unused record so the
        // walker doesn't read garbage past the last entry.
        if pos < block_size {
            BigEndian::write_u16(&mut buf[pos..pos + 2], XFS_DIR2_DATA_FREE_TAG);
            BigEndian::write_u16(&mut buf[pos + 2..pos + 4], (block_size - pos) as u16);
        }
        let parsed = parse_data_block(&buf, false).expect("parse");
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].name, "first");
        assert_eq!(parsed[0].inumber, 50);
        assert_eq!(parsed[2].name, "third");
        assert_eq!(parsed[2].inumber, 52);
    }

    #[test]
    fn data_block_rejects_wrong_magic() {
        let mut buf = vec![0u8; 256];
        BigEndian::write_u32(&mut buf[0..4], XFS_DIR2_BLOCK_MAGIC);
        match parse_data_block(&buf, false) {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("magic")),
            other => panic!("expected Parse error, got {other:?}"),
        }
    }

    #[test]
    fn block_filters_dot_and_dotdot() {
        let block_size = 256usize;
        let leaf_count = 0u32;
        let leaf_section = 8;
        let mut buf = vec![0u8; block_size];
        BigEndian::write_u32(&mut buf[0..4], XFS_DIR2_BLOCK_MAGIC);
        let entries = [(2u64, "."), (3u64, "..")];
        let mut pos = 16usize;
        for (ino, name) in entries {
            BigEndian::write_u64(&mut buf[pos..pos + 8], ino);
            buf[pos + 8] = name.len() as u8;
            buf[pos + 9..pos + 9 + name.len()].copy_from_slice(name.as_bytes());
            let aligned = (8 + 1 + name.len() + 2 + 7) & !7;
            pos += aligned;
        }
        let data_end = block_size - leaf_section;
        if pos < data_end {
            BigEndian::write_u16(&mut buf[pos..pos + 2], XFS_DIR2_DATA_FREE_TAG);
            BigEndian::write_u16(&mut buf[pos + 2..pos + 4], (data_end - pos) as u16);
        }
        let tail_off = block_size - 8;
        BigEndian::write_u32(&mut buf[tail_off..tail_off + 4], leaf_count);

        let parsed = parse_block(&buf, false).expect("parse");
        assert!(parsed.is_empty(), "expected . and .. filtered: {parsed:?}");
    }
}
