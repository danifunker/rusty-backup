//! `dir_hashname` against hashes written by other XFS implementations.
//!
//! The pinned table was read, with this module's [`disk_pairs`] walker, out
//! of two images that xfsprogs' `mkfs.xfs` 6.18 populated from a protofile
//! (`-p`) on the oracle host: one v5 (`-m crc=1`), one v4 without ftype
//! (`-m crc=0 -n ftype=0`). Each holds `/blk` (block form), `/leaf` (300
//! entries, leaf form) and `/node` (2500 entries, node form), and every one of
//! their 2824 leaf entries per image matched; the rows below are a sample.
//! The repo's SGI fixtures only hold short-form directories (no hash on disk),
//! so [`sgi_fixture_directories`] has nothing to compare there; [`mkfs_directory_forms`]
//! walks the two mkfs images themselves, kept as fixtures under `tests/fixtures/sgi`.
//!
//! The document rows come from XFS Algorithms & Data Structures, ch.
//! Directories, sec. Block Directories (the `xfs_db` dump of an `XD2B` block),
//! which [`document_block_directory_dump`] rebuilds byte for byte and walks.
//! Set `XFS_HASH_IMAGES=a.img.zst,b.img.zst` to walk further images live.

use super::*;
use std::io::{Cursor, Read};

/// `(name, hashval, where it was read)`.
const PINNED: &[(&[u8], u32, &str)] = &[
    (b".", 0x0000_002e, "mkfs v5 /blk, v4 /blk"),
    (b"..", 0x0000_172e, "mkfs v5 /blk, v4 /blk"),
    (b"0", 0x0000_0030, "mkfs v5 /blk"),
    (b"Z", 0x0000_005a, "mkfs v5 /blk"),
    (b"a", 0x0000_0061, "mkfs v5 /blk"),
    (b"99", 0x0000_1cb9, "mkfs v5 /blk"),
    (b"ab", 0x0000_30e2, "mkfs v5 /blk"),
    (b"xy", 0x0000_3c79, "mkfs v4 /blk"),
    (b"Foo", 0x0011_b7ef, "mkfs v5 /blk"),
    (b"abc", 0x0018_7163, "mkfs v4 /blk"),
    (b"abcd", 0x0c38_b1e4, "mkfs v5 /blk"),
    (b"read", 0x0e59_70e4, "mkfs v4 /blk"),
    (b"abcde", 0x1c58_f263, "mkfs v5 /blk"),
    (b"hello", 0x8cbb_3669, "mkfs v4 /blk"),
    (b"1234567", 0x368d_9ea4, "mkfs v5 /blk"),
    (b"Makefile", 0x5c41_f13b, "mkfs v5 /blk"),
    (b"README.txt", 0x105e_2e31, "mkfs v4 /blk"),
    ("\u{e9}t\u{e9}".as_bytes(), 0x253d_6125, "mkfs v5 /blk"),
    (&[b'x'; 40], 0x8fef_dfbf, "mkfs v5 /blk"),
    (&[b'q'; 255], 0xe200_0000, "mkfs v5 /blk, v4 /blk"),
    (b"frame000000.tst", 0xa3a0_40b4, "mkfs v5 /leaf; document"),
    (b"frame000001.tst", 0xb3a0_40b4, "mkfs v5 /leaf; document"),
    (b"frame000002.tst", 0x83a0_40b4, "mkfs v5 /leaf; document"),
    (b"frame000004.tst", 0xe3a0_40b4, "mkfs v4 /leaf; document"),
    (b"frame000007.tst", 0xd3a0_40b4, "mkfs v5 /leaf; document"),
    (b"frame000099.tst", 0x33a0_40fc, "mkfs v5 /leaf"),
    (b"frame000255.tst", 0xf3a0_489c, "mkfs v5 /leaf"),
    (b"node-entry-0", 0x2b08_1a35, "mkfs v5 /node"),
    (b"node-entry-7", 0x2b08_1a32, "mkfs v5 /node"),
    (b"node-entry-42", 0x840d_18a7, "mkfs v5 /node"),
    (b"node-entry-999", 0x068f_167b, "mkfs v4 /node"),
    (b"node-entry-2499", 0x46e8_7dba, "mkfs v5 /node"),
    (
        b"frame001845.tst",
        0xf3a2_6094,
        "document, Node Directories xfs_db hash",
    ),
];

#[test]
fn pinned_hashes_match() {
    for &(name, want, from) in PINNED {
        let got = dir_hashname(name);
        assert_eq!(
            got,
            want,
            "{:?} ({from}): {got:#010x}",
            String::from_utf8_lossy(name)
        );
    }
    assert_eq!(dir_hashname(b""), 0);
}

/// The `XD2B` block of the document's block-directory example: rows 0x000-0x130
/// and 0xfa0-0xff0 of its dump; everything between is zero.
const DOC_BLOCK_ROWS: &[(usize, &str)] = &[
    (0x000, "58 44 32 42 01 30 0e 78 00 00 00 00 00 00 00 00"),
    (0x010, "00 00 00 00 02 00 00 80 01 2e 00 00 00 00 00 10"),
    (0x020, "00 00 00 00 00 00 00 80 02 2e 2e 00 00 00 00 20"),
    (0x030, "00 00 00 00 02 00 00 81 0f 66 72 61 6d 65 30 30"),
    (0x040, "30 30 30 30 2e 74 73 74 80 8e 59 00 00 00 00 30"),
    (0x050, "00 00 00 00 02 00 00 82 0f 66 72 61 6d 65 30 30"),
    (0x060, "30 30 30 31 2e 74 73 74 d0 ca 5c 00 00 00 00 50"),
    (0x070, "00 00 00 00 02 00 00 83 0f 66 72 61 6d 65 30 30"),
    (0x080, "30 30 30 32 2e 74 73 74 00 00 00 00 00 00 00 70"),
    (0x090, "00 00 00 00 02 00 00 84 0f 66 72 61 6d 65 30 30"),
    (0x0a0, "30 30 30 33 2e 74 73 74 00 00 00 00 00 00 00 90"),
    (0x0b0, "00 00 00 00 02 00 00 85 0f 66 72 61 6d 65 30 30"),
    (0x0c0, "30 30 30 34 2e 74 73 74 00 00 00 00 00 00 00 b0"),
    (0x0d0, "00 00 00 00 02 00 00 86 0f 66 72 61 6d 65 30 30"),
    (0x0e0, "30 30 30 35 2e 74 73 74 00 00 00 00 00 00 00 d0"),
    (0x0f0, "00 00 00 00 02 00 00 87 0f 66 72 61 6d 65 30 30"),
    (0x100, "30 30 30 36 2e 74 73 74 00 00 00 00 00 00 00 f0"),
    (0x110, "00 00 00 00 02 00 00 88 0f 66 72 61 6d 65 30 30"),
    (0x120, "30 30 30 37 2e 74 73 74 00 00 00 00 00 00 01 10"),
    (0x130, "ff ff 0e 78 00 00 00 00 00 00 00 00 00 00 00 00"),
    (0xfa0, "00 00 00 00 00 00 01 30 00 00 00 2e 00 00 00 02"),
    (0xfb0, "00 00 17 2e 00 00 00 04 83 a0 40 b4 00 00 00 0e"),
    (0xfc0, "93 a0 40 b4 00 00 00 12 a3 a0 40 b4 00 00 00 06"),
    (0xfd0, "b3 a0 40 b4 00 00 00 0a c3 a0 40 b4 00 00 00 1e"),
    (0xfe0, "d3 a0 40 b4 00 00 00 22 e3 a0 40 b4 00 00 00 16"),
    (0xff0, "f3 a0 40 b4 00 00 00 1a 00 00 00 0a 00 00 00 00"),
];

/// Data entries of directory block `db` between `start` and `end`, keyed by
/// their leaf address (byte offset in the directory's data space / 8).
fn data_names(
    blk: &[u8],
    start: usize,
    end: usize,
    db: u64,
    dirblk: u64,
    has_ftype: bool,
) -> std::collections::BTreeMap<u32, (Vec<u8>, u64)> {
    let mut out = std::collections::BTreeMap::new();
    let mut pos = start;
    while pos + 8 <= end {
        if BigEndian::read_u16(&blk[pos..pos + 2]) == 0xFFFF {
            pos += BigEndian::read_u16(&blk[pos + 2..pos + 4]) as usize;
            continue;
        }
        let child = BigEndian::read_u64(&blk[pos..pos + 8]);
        let nl = blk[pos + 8] as usize;
        let addr = ((db * dirblk + pos as u64) / 8) as u32;
        out.insert(addr, (blk[pos + 9..pos + 9 + nl].to_vec(), child));
        pos += dir2_data_entsize(nl, has_ftype);
    }
    out
}

/// The block tail's leaf entries `(hashval, address)` and where they start.
fn block_tail(blk: &[u8]) -> (Vec<(u32, u32)>, usize) {
    let end = blk.len();
    let n = BigEndian::read_u32(&blk[end - 8..end - 4]) as usize;
    let start = end - 8 - n * 8;
    let ents = (0..n)
        .map(|k| {
            let o = start + k * 8;
            (
                BigEndian::read_u32(&blk[o..o + 4]),
                BigEndian::read_u32(&blk[o + 4..o + 8]),
            )
        })
        .collect();
    (ents, start)
}

#[test]
fn document_block_directory_dump() {
    let mut blk = vec![0u8; 4096];
    for &(off, row) in DOC_BLOCK_ROWS {
        for (i, b) in row.split(' ').enumerate() {
            blk[off + i] = u8::from_str_radix(b, 16).unwrap();
        }
    }
    let (ents, leaf_start) = block_tail(&blk);
    let names = data_names(&blk, XFS_DIR2_DATA_HDR_LEN, leaf_start, 0, 4096, true);
    assert_eq!(ents.len(), 10, "the dump's tail counts ten leaf entries");
    for (hash, addr) in ents {
        let name = &names[&addr].0;
        assert_eq!(
            dir_hashname(name),
            hash,
            "{:?}",
            String::from_utf8_lossy(name)
        );
    }
}

/// `(name, hashval, directory inode)` for every live leaf entry of every
/// block / leaf / node-form directory reachable from the root of `img`.
fn disk_pairs(img: Vec<u8>) -> Vec<(Vec<u8>, u32, u64)> {
    use super::super::types::DiFormat;
    let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
    let sb = fs.superblock().clone();
    let has_ftype = sb.has_ftype();
    let bpd = blocks_per_dir_block(&sb) as u64;
    let leaf_off = dir2_leaf_offset_fb(sb.blocksize);
    let free_off = dir2_free_offset_fb(sb.blocksize);
    let dirblk = sb.dirblksize() as u64;
    let hdr = dir2_data_hdr_len(sb.is_v5());
    let (cnt_off, leaf_hdr) = if sb.is_v5() { (56, 64) } else { (12, 16) };
    let mut out = Vec::new();
    let mut todo = vec![sb.rootino];
    let mut seen = std::collections::BTreeSet::new();
    while let Some(ino) = todo.pop() {
        if !seen.insert(ino) {
            continue;
        }
        let (core, buf) = fs.read_inode_buf(ino).expect("inode");
        let children: Vec<u64> = match core.format {
            DiFormat::Local => {
                super::super::dir2::parse_shortform(&buf[sb.fork_offset()..], has_ftype)
                    .map(|(_, e)| e.into_iter().map(|e| e.inumber).collect())
                    .unwrap_or_default()
            }
            DiFormat::Extents | DiFormat::Btree => {
                let extents = fs.decode_data_extents(&core, &buf).expect("extents");
                let mut names = std::collections::BTreeMap::new();
                let mut leaf: Vec<(u32, u32)> = Vec::new();
                let dablks: Vec<u64> = extents
                    .iter()
                    .flat_map(|e| e.startoff..e.startoff + e.blockcount)
                    .filter(|fb| fb % bpd == 0 && *fb < free_off)
                    .collect();
                for d in dablks {
                    let blk = fs
                        .read_mapped_dir_block(&sb, &extents, d)
                        .expect("read")
                        .expect("mapped");
                    let magic32 = BigEndian::read_u32(&blk[0..4]);
                    let magic16 = BigEndian::read_u16(&blk[8..10]);
                    if d < leaf_off {
                        let mut end = blk.len();
                        if matches!(magic32, 0x5844_3242 | 0x5844_4233) {
                            let (ents, start) = block_tail(&blk);
                            leaf.extend(ents);
                            end = start;
                        }
                        names.extend(data_names(&blk, hdr, end, d / bpd, dirblk, has_ftype));
                    } else if matches!(magic16, 0xD2F1 | 0xD2FF | 0x3DF1 | 0x3DFF) {
                        let n = BigEndian::read_u16(&blk[cnt_off..cnt_off + 2]) as usize;
                        for k in 0..n {
                            let o = leaf_hdr + k * 8;
                            leaf.push((
                                BigEndian::read_u32(&blk[o..o + 4]),
                                BigEndian::read_u32(&blk[o + 4..o + 8]),
                            ));
                        }
                    }
                }
                for (hash, addr) in leaf {
                    if addr == 0 {
                        continue;
                    }
                    let (name, _) = names.get(&addr).expect("leaf address names an entry");
                    out.push((name.clone(), hash, ino));
                }
                names.values().map(|(_, c)| *c).collect()
            }
            _ => Vec::new(),
        };
        for c in children {
            if fs.read_inode(c).map(|c| c.is_dir()).unwrap_or(false) {
                todo.push(c);
            }
        }
    }
    out
}

/// Decompress a zstd image file.
fn unzstd(path: &std::path::Path) -> Vec<u8> {
    let compressed = std::fs::read(path).expect("image present");
    let mut dec = zstd::stream::read::Decoder::new(Cursor::new(compressed)).expect("zstd");
    let mut out = Vec::new();
    dec.read_to_end(&mut out).expect("decompress");
    out
}

/// Assert every leaf entry of `img` against `dir_hashname`; returns the count.
fn check_image(img: Vec<u8>, label: &str) -> usize {
    let pairs = disk_pairs(img);
    for (name, hash, ino) in &pairs {
        assert_eq!(
            dir_hashname(name),
            *hash,
            "{label} dir {ino}: {:?}",
            String::from_utf8_lossy(name)
        );
    }
    pairs.len()
}

#[test]
#[cfg_attr(
    target_pointer_width = "32",
    ignore = "512 MiB SGI fixtures exhaust the 32-bit address space"
)]
fn sgi_fixture_directories() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/sgi");
    for f in ["xfs_v2_dir2_small.img.zst", "xfs_v5_modern_small.img.zst"] {
        check_image(unzstd(&dir.join(f)), f);
    }
}

/// mkfs.xfs-made v5 and v4 images with block, leaf (300) and node (2500 entry) directories.
#[test]
fn mkfs_directory_forms() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/sgi");
    for f in [
        "xfs_v5_mkfs_dirforms.img.zst",
        "xfs_v4_mkfs_dirforms.img.zst",
    ] {
        let n = check_image(unzstd(&dir.join(f)), f);
        assert!(n >= 2800, "{f}: only {n} leaf entries walked");
    }
}

#[test]
fn extra_images_from_env() {
    let Ok(list) = std::env::var("XFS_HASH_IMAGES") else {
        return;
    };
    for f in list.split(',').filter(|f| !f.is_empty()) {
        let n = check_image(unzstd(std::path::Path::new(f)), f);
        eprintln!("{f}: {n} leaf entries match");
        assert!(n > 0, "{f} has no block / leaf / node directory");
    }
}
