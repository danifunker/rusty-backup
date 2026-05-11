//! XFS dir1 directory parsers — the original IRIX directory format used by
//! disks whose superblock `DIRV2` bit is clear. Layouts are documented only
//! in `kernel-source-2.4.2-SGI_XFS_1.0/fs/xfs/xfs_dir_*.h`; the modern XFS
//! Algorithms PDF and the Linux v5.15 kernel both pre-date their removal.
//!
//! Two formats are handled here:
//! - **shortform-dir1** (`xfs_dir_shortform_t`): used when the inode is
//!   `di_format == Local`. Fork layout: `parent[8] + count(u8)` followed by
//!   `inumber[8] + namelen(u8) + name[namelen]` entries.
//! - **leaf-dir1** (`xfs_dir_leafblock_t`, magic `0xFEEB`): a single block
//!   used when `di_format == Extents` and the directory data fits in the
//!   directory block size. The directory entries (including "." and "..")
//!   live in a slotted region; entries are pointed at by `nameidx` offsets
//!   from the leaf entries.
//!
//! Multi-block (node-dir1) directories are handled by the caller iterating
//! over every file block, parsing each leaf, and skipping the intermediate
//! node blocks (`xfs_da_intnode_t`, magic `0xFEBE`). We never traverse the
//! hash btree — directory listing only needs to enumerate names, not look
//! them up.

use byteorder::{BigEndian, ByteOrder};

use super::dir2::Dir2Entry;
use crate::fs::filesystem::FilesystemError;

/// `xfs_dir_leafblock` magic.
pub const XFS_DIR1_LEAF_MAGIC: u16 = 0xFEEB;
/// `xfs_da_intnode` magic (intermediate-node block, dir1 and dir2 share it).
pub const XFS_DA_NODE_MAGIC: u16 = 0xFEBE;

/// Header sizes per the 2.4.2-SGI source. `xfs_da_blkinfo_t` is 12 bytes
/// (forw + back + magic + pad); the dir1 leaf header is 32 bytes total
/// (`blkinfo` + count + namebytes + firstused + holes + pad + 3 freemap
/// entries). Each leaf entry is 8 bytes (hashval + nameidx + namelen + pad).
const XFS_DIR1_LEAF_HDR_SIZE: usize = 32;
const XFS_DIR1_LEAF_ENTRY_SIZE: usize = 8;

/// Parse shortform-dir1 fork data. Returns the parent inumber and the list
/// of entries (entries do not include "." or "..").
pub fn parse_shortform(fork: &[u8]) -> Result<(u64, Vec<Dir2Entry>), FilesystemError> {
    if fork.len() < 9 {
        return Err(FilesystemError::Parse(format!(
            "dir1 shortform fork too small: {} bytes",
            fork.len()
        )));
    }
    let parent = BigEndian::read_u64(&fork[0..8]);
    let count = fork[8] as usize;
    let mut pos = 9usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 9 > fork.len() {
            return Err(FilesystemError::Parse(
                "dir1 shortform entry truncated at header".into(),
            ));
        }
        let inumber = BigEndian::read_u64(&fork[pos..pos + 8]);
        let namelen = fork[pos + 8] as usize;
        let name_off = pos + 9;
        let next = name_off + namelen;
        if next > fork.len() {
            return Err(FilesystemError::Parse(format!(
                "dir1 shortform name overflows fork: needs {next}, have {}",
                fork.len()
            )));
        }
        let name = String::from_utf8_lossy(&fork[name_off..next]).into_owned();
        out.push(Dir2Entry { inumber, name });
        pos = next;
    }
    Ok((parent, out))
}

/// Identify a directory block as either a leaf or a node by reading the
/// `xfs_da_blkinfo_t.magic` field at offset 8. Returns `None` if the magic
/// matches neither — callers should treat that as "skip this block".
pub fn block_magic(buf: &[u8]) -> Option<u16> {
    if buf.len() < 10 {
        return None;
    }
    Some(BigEndian::read_u16(&buf[8..10]))
}

/// Parse one leaf-dir1 block buffer. `buf.len()` must equal the directory
/// block size (typically the filesystem block size; dir1 doesn't multiply
/// block sizes the way dir2 can).
///
/// Returns entries in on-disk slot order, skipping "." and "..".
pub fn parse_leaf_block(buf: &[u8]) -> Result<Vec<Dir2Entry>, FilesystemError> {
    if buf.len() < XFS_DIR1_LEAF_HDR_SIZE {
        return Err(FilesystemError::Parse(format!(
            "dir1 leaf buffer too small: {} bytes",
            buf.len()
        )));
    }
    let magic = BigEndian::read_u16(&buf[8..10]);
    if magic != XFS_DIR1_LEAF_MAGIC {
        return Err(FilesystemError::Parse(format!(
            "bad dir1 leaf magic: 0x{magic:04X} (expected 0x{XFS_DIR1_LEAF_MAGIC:04X})"
        )));
    }
    let count = BigEndian::read_u16(&buf[12..14]) as usize;
    let entries_end = XFS_DIR1_LEAF_HDR_SIZE + count.saturating_mul(XFS_DIR1_LEAF_ENTRY_SIZE);
    if entries_end > buf.len() {
        return Err(FilesystemError::Parse(format!(
            "dir1 leaf count {count} overflows block size {}",
            buf.len()
        )));
    }

    let mut out = Vec::with_capacity(count);
    for slot in 0..count {
        let off = XFS_DIR1_LEAF_HDR_SIZE + slot * XFS_DIR1_LEAF_ENTRY_SIZE;
        let nameidx = BigEndian::read_u16(&buf[off + 4..off + 6]) as usize;
        let namelen = buf[off + 6] as usize;
        if namelen == 0 || nameidx + 8 + namelen > buf.len() {
            return Err(FilesystemError::Parse(format!(
                "dir1 leaf entry {slot} bad nameidx={nameidx} namelen={namelen}"
            )));
        }
        let inumber = BigEndian::read_u64(&buf[nameidx..nameidx + 8]);
        let name_off = nameidx + 8;
        let name = String::from_utf8_lossy(&buf[name_off..name_off + namelen]).into_owned();
        if name == "." || name == ".." {
            continue;
        }
        out.push(Dir2Entry { inumber, name });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a shortform-dir1 fork with `entries = [(name, inumber)]` and
    /// the given parent inumber.
    fn build_shortform(parent: u64, entries: &[(&str, u64)]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&parent.to_be_bytes());
        buf.push(entries.len() as u8);
        for (name, ino) in entries {
            buf.extend_from_slice(&(*ino).to_be_bytes());
            buf.push(name.len() as u8);
            buf.extend_from_slice(name.as_bytes());
        }
        buf
    }

    #[test]
    fn shortform_zero_entries() {
        let buf = build_shortform(2, &[]);
        let (parent, entries) = parse_shortform(&buf).expect("parse");
        assert_eq!(parent, 2);
        assert!(entries.is_empty());
    }

    #[test]
    fn shortform_one_entry() {
        let buf = build_shortform(128, &[("hello", 200)]);
        let (parent, entries) = parse_shortform(&buf).expect("parse");
        assert_eq!(parent, 128);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "hello");
        assert_eq!(entries[0].inumber, 200);
    }

    #[test]
    fn shortform_five_entries() {
        let pairs: Vec<(&str, u64)> = ["a", "bb", "ccc", "dddd", "eeeee"]
            .iter()
            .enumerate()
            .map(|(i, n)| (*n, 1000 + i as u64))
            .collect();
        let buf = build_shortform(42, &pairs);
        let (parent, entries) = parse_shortform(&buf).expect("parse");
        assert_eq!(parent, 42);
        assert_eq!(entries.len(), 5);
        for (i, e) in entries.iter().enumerate() {
            assert_eq!(e.inumber, 1000 + i as u64);
        }
    }

    #[test]
    fn shortform_long_name_roundtrip() {
        let long = "a".repeat(255);
        let buf = build_shortform(2, &[(&long, 999)]);
        let (parent, entries) = parse_shortform(&buf).expect("parse");
        assert_eq!(parent, 2);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name.len(), 255);
        assert_eq!(entries[0].inumber, 999);
    }

    #[test]
    fn shortform_uses_8byte_inumbers_unlike_dir2() {
        // 4 bytes worth of high bits in the inumber must survive parsing.
        let buf = build_shortform(0x0102_0304_0506_0708, &[("x", 0xAABB_CCDD_EEFF_0011)]);
        let (parent, entries) = parse_shortform(&buf).expect("parse");
        assert_eq!(parent, 0x0102_0304_0506_0708);
        assert_eq!(entries[0].inumber, 0xAABB_CCDD_EEFF_0011);
    }

    /// Build a leaf-dir1 block of `block_size` bytes from a list of
    /// (name, inumber) entries. Layout the names from the top of the block
    /// downward and the leaf entries directly after the header — the parser
    /// doesn't care about hash ordering or freemap completeness.
    fn build_leaf(block_size: usize, entries: &[(&str, u64)]) -> Vec<u8> {
        let mut buf = vec![0u8; block_size];
        // forw, back left zero. magic at offset 8.
        BigEndian::write_u16(&mut buf[8..10], XFS_DIR1_LEAF_MAGIC);
        BigEndian::write_u16(&mut buf[12..14], entries.len() as u16);

        // Pack name structs from the end downward. Each name struct is
        // inumber(8) + name[namelen].
        let mut name_top = block_size;
        let mut slots = Vec::with_capacity(entries.len());
        for (name, ino) in entries {
            let size = 8 + name.len();
            name_top -= size;
            buf[name_top..name_top + 8].copy_from_slice(&(*ino).to_be_bytes());
            buf[name_top + 8..name_top + 8 + name.len()].copy_from_slice(name.as_bytes());
            slots.push((name_top as u16, name.len() as u8));
        }

        // Leaf entries directly after the 32-byte header.
        for (slot, (nameidx, namelen)) in slots.iter().enumerate() {
            let off = XFS_DIR1_LEAF_HDR_SIZE + slot * XFS_DIR1_LEAF_ENTRY_SIZE;
            // hashval at off+0..off+4 — unused by the parser.
            BigEndian::write_u16(&mut buf[off + 4..off + 6], *nameidx);
            buf[off + 6] = *namelen;
        }
        // firstused — informative for compaction; not consulted.
        BigEndian::write_u16(&mut buf[16..18], name_top as u16);
        buf
    }

    #[test]
    fn leaf_block_three_entries_plus_dot_dotdot() {
        // Real dir1 leaf blocks store "." and ".." as the first two entries.
        // Build one and verify they get filtered out.
        let buf = build_leaf(
            512,
            &[
                (".", 128),
                ("..", 128),
                ("alpha", 200),
                ("beta", 201),
                ("gamma", 202),
            ],
        );
        let entries = parse_leaf_block(&buf).expect("parse");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].name, "alpha");
        assert_eq!(entries[0].inumber, 200);
        assert_eq!(entries[2].name, "gamma");
        assert_eq!(entries[2].inumber, 202);
        assert!(!entries.iter().any(|e| e.name == "."));
        assert!(!entries.iter().any(|e| e.name == ".."));
    }

    #[test]
    fn leaf_block_rejects_wrong_magic() {
        let mut buf = build_leaf(512, &[(".", 1), ("..", 1)]);
        // Stomp the magic.
        BigEndian::write_u16(&mut buf[8..10], 0x1234);
        match parse_leaf_block(&buf) {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("magic")),
            Err(e) => panic!("expected Parse, got {e}"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn block_magic_identifies_node_blocks() {
        // A node block (xfs_da_intnode) shares the blkinfo header; magic at
        // offset 8 == 0xFEBE.
        let mut buf = vec![0u8; 512];
        BigEndian::write_u16(&mut buf[8..10], XFS_DA_NODE_MAGIC);
        assert_eq!(block_magic(&buf), Some(XFS_DA_NODE_MAGIC));
        assert_ne!(block_magic(&buf), Some(XFS_DIR1_LEAF_MAGIC));
    }

    #[test]
    fn leaf_with_long_name() {
        let long = "z".repeat(255);
        let buf = build_leaf(1024, &[(".", 1), ("..", 1), (&long, 5000)]);
        let entries = parse_leaf_block(&buf).expect("parse");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name.len(), 255);
        assert_eq!(entries[0].inumber, 5000);
    }
}
