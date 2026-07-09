//! ext4 `metadata_csum` (crc32c) — the checksum discipline shared by the ext4
//! formatter, fsck repair, and the checksum-aware editor.
//!
//! Every algorithm here was verified byte-for-byte against `mke2fs` output (see
//! the fixture test below): the superblock, group descriptor, block/inode bitmap,
//! inode (split lo/hi), and directory-block tail checksums.
//!
//! **The e2fsprogs CRC-32C convention** is the reflected Castagnoli LFSR started
//! from a seed with **no final inversion** — subtly different from the standard
//! CRC-32C the `crc32c` crate exposes (which inverts at both ends). The mapping is
//! `ext_crc(seed, data) = crc32c_append(seed ^ !0, data) ^ !0`, which also
//! composes for chaining: `ext_crc(ext_crc(s, a), b) == ext_crc(s, a ++ b)`.
//!
//! Checksums are **UUID-seeded** (`seed = ext_crc(!0, s_uuid)`) — the volume
//! `s_checksum_seed` caches the same value when the `metadata_csum_seed` feature
//! is set; we always derive it from the UUID. The superblock's own checksum is
//! the one exception: it is seeded from `!0` (the UUID is already in its bytes).

// ---- field offsets (all little-endian on disk) ----

/// Superblock `s_checksum`.
pub const SB_CSUM_OFF: usize = 0x3FC;

// group descriptor
const GD_BBM_CSUM_LO: usize = 0x18;
const GD_IBM_CSUM_LO: usize = 0x1A;
const GD_BG_CHECKSUM: usize = 0x1E;
const GD_BBM_CSUM_HI: usize = 0x38;
const GD_IBM_CSUM_HI: usize = 0x3A;

// inode
const INO_GENERATION: usize = 0x64;
const INO_CSUM_LO: usize = 0x7C;
const INO_EXTRA_ISIZE: usize = 0x80;
const INO_CSUM_HI: usize = 0x82;

/// e2fsprogs' raw seeded CRC-32C (reflected Castagnoli, init = `seed`, no final
/// inversion). Verified against mke2fs; composes for chaining.
pub fn ext_crc(seed: u32, data: &[u8]) -> u32 {
    crc32c::crc32c_append(seed ^ 0xFFFF_FFFF, data) ^ 0xFFFF_FFFF
}

/// The volume checksum seed derived from the 16-byte filesystem UUID.
pub fn csum_seed(uuid: &[u8; 16]) -> u32 {
    ext_crc(0xFFFF_FFFF, uuid)
}

// ---- superblock ----

/// The superblock checksum: `ext_crc(!0, sb[0..0x3FC])` (not UUID-seeded).
pub fn superblock_csum(sb: &[u8]) -> u32 {
    ext_crc(0xFFFF_FFFF, &sb[..SB_CSUM_OFF])
}

/// Write `s_checksum` into a 1024-byte superblock buffer.
pub fn stamp_superblock(sb: &mut [u8]) {
    let c = superblock_csum(sb);
    sb[SB_CSUM_OFF..SB_CSUM_OFF + 4].copy_from_slice(&c.to_le_bytes());
}

// ---- group descriptor + bitmap checksums (both stored IN the descriptor) ----

/// Group-descriptor checksum (`bg_checksum`, low 16 bits): covers
/// `le32(group) ++ desc[0..0x1E] ++ 0u16 ++ desc[0x20..]`.
///
/// The bitmap-csum fields live inside this coverage, so callers must stamp the
/// bitmap checksums **before** the descriptor checksum.
pub fn group_desc_csum(seed: u32, group: u32, desc: &[u8]) -> u16 {
    let mut c = ext_crc(seed, &group.to_le_bytes());
    c = ext_crc(c, &desc[..GD_BG_CHECKSUM]);
    c = ext_crc(c, &[0u8, 0u8]); // bg_checksum treated as zero
    if desc.len() > 32 {
        c = ext_crc(c, &desc[0x20..]);
    }
    (c & 0xFFFF) as u16
}

/// Write `bg_checksum` into a descriptor slice (32- or 64-byte).
pub fn stamp_group_desc(seed: u32, group: u32, desc: &mut [u8]) {
    let c = group_desc_csum(seed, group, desc);
    desc[GD_BG_CHECKSUM..GD_BG_CHECKSUM + 2].copy_from_slice(&c.to_le_bytes());
}

/// `(lo, hi)` halves of a bitmap checksum over its covered bytes.
pub fn bitmap_csum(seed: u32, bitmap: &[u8]) -> (u16, u16) {
    let c = ext_crc(seed, bitmap);
    ((c & 0xFFFF) as u16, ((c >> 16) & 0xFFFF) as u16)
}

/// Stamp the block-bitmap checksum into the descriptor. `bitmap` must be the full
/// block-size bitmap block (its covered length).
pub fn stamp_block_bitmap_csum(seed: u32, desc: &mut [u8], bitmap: &[u8]) {
    let (lo, hi) = bitmap_csum(seed, bitmap);
    desc[GD_BBM_CSUM_LO..GD_BBM_CSUM_LO + 2].copy_from_slice(&lo.to_le_bytes());
    if desc.len() > 32 {
        desc[GD_BBM_CSUM_HI..GD_BBM_CSUM_HI + 2].copy_from_slice(&hi.to_le_bytes());
    }
}

/// Stamp the inode-bitmap checksum into the descriptor. `bitmap` must be the first
/// `inodes_per_group / 8` bytes (its covered length).
pub fn stamp_inode_bitmap_csum(seed: u32, desc: &mut [u8], bitmap: &[u8]) {
    let (lo, hi) = bitmap_csum(seed, bitmap);
    desc[GD_IBM_CSUM_LO..GD_IBM_CSUM_LO + 2].copy_from_slice(&lo.to_le_bytes());
    if desc.len() > 32 {
        desc[GD_IBM_CSUM_HI..GD_IBM_CSUM_HI + 2].copy_from_slice(&hi.to_le_bytes());
    }
}

// ---- inode ----

/// True when the inode is large enough (and `i_extra_isize` reaches far enough)
/// to carry the high half of its checksum at 0x82.
fn inode_has_hi(inode: &[u8]) -> bool {
    inode.len() > INO_CSUM_HI + 2
        && u16::from_le_bytes([inode[INO_EXTRA_ISIZE], inode[INO_EXTRA_ISIZE + 1]]) >= 4
}

/// Compute an inode's checksum halves. `inode` should have both csum fields set to
/// their real (or zero) values — this zeroes them in a copy before computing.
pub fn inode_csum(seed: u32, inode: &[u8], inum: u32) -> (u16, u16) {
    let mut tmp = inode.to_vec();
    tmp[INO_CSUM_LO..INO_CSUM_LO + 2].fill(0);
    let hi = inode_has_hi(inode);
    if hi {
        tmp[INO_CSUM_HI..INO_CSUM_HI + 2].fill(0);
    }
    let gen = u32::from_le_bytes(
        inode[INO_GENERATION..INO_GENERATION + 4]
            .try_into()
            .unwrap(),
    );
    let mut c = ext_crc(seed, &inum.to_le_bytes());
    c = ext_crc(c, &gen.to_le_bytes());
    c = ext_crc(c, &tmp);
    (
        (c & 0xFFFF) as u16,
        if hi { ((c >> 16) & 0xFFFF) as u16 } else { 0 },
    )
}

/// Write an inode's checksum into `l_i_checksum_lo` (+ `i_checksum_hi` when the
/// inode is large enough).
pub fn stamp_inode(seed: u32, inode: &mut [u8], inum: u32) {
    let (lo, hi) = inode_csum(seed, inode, inum);
    inode[INO_CSUM_LO..INO_CSUM_LO + 2].copy_from_slice(&lo.to_le_bytes());
    if inode_has_hi(inode) {
        inode[INO_CSUM_HI..INO_CSUM_HI + 2].copy_from_slice(&hi.to_le_bytes());
    }
}

// ---- directory block ----

/// A directory block's tail checksum: covers `le32(inum) ++ le32(gen) ++
/// dirblock[0..len-12]` (everything but the 12-byte tail).
pub fn dir_block_csum(seed: u32, dirblock: &[u8], inum: u32, generation: u32) -> u32 {
    let n = dirblock.len() - 12;
    let mut c = ext_crc(seed, &inum.to_le_bytes());
    c = ext_crc(c, &generation.to_le_bytes());
    ext_crc(c, &dirblock[..n])
}

/// Write the `ext4_dir_entry_tail` (a fake entry: inode 0, rec_len 12, file_type
/// 0xDE, then the checksum) into the last 12 bytes of a directory block. The
/// block's real entries must already stop at `len - 12`.
pub fn stamp_dir_block(seed: u32, dirblock: &mut [u8], inum: u32, generation: u32) {
    let c = dir_block_csum(seed, dirblock, inum, generation);
    let t = dirblock.len() - 12;
    dirblock[t..t + 4].copy_from_slice(&0u32.to_le_bytes()); // det_reserved_zero1 (inode = 0)
    dirblock[t + 4..t + 6].copy_from_slice(&12u16.to_le_bytes()); // det_rec_len
    dirblock[t + 6] = 0; // det_reserved_zero2 (name_len)
    dirblock[t + 7] = 0xDE; // det_reserved_ft (EXT4_FT_DIR_CSUM)
    dirblock[t + 8..t + 12].copy_from_slice(&c.to_le_bytes()); // det_checksum
}

// ---- extent tree block (separate, non-inline) ----

/// The per-inode checksum seed: `ext_crc(ext_crc(fs_seed, inum), generation)`.
///
/// ext4 seeds an inode's extent-tree-block checksums (and, in the kernel, its
/// inode/dir-block checksums) from this per-inode value rather than the raw
/// volume seed. `dir_block_csum` / `inode_csum` inline the same construction.
pub fn inode_seed(seed: u32, inum: u32, generation: u32) -> u32 {
    ext_crc(
        ext_crc(seed, &inum.to_le_bytes()),
        &generation.to_le_bytes(),
    )
}

/// Byte length an extent-tree block's checksum covers: `sizeof(header) +
/// sizeof(extent) * eh_max` (`EXT4_EXTENT_TAIL_OFFSET`). `eh_max` is the 16-bit
/// field at offset 4 of the extent header. The 4-byte `et_checksum` tail sits at
/// exactly this offset (the last 4 bytes of a standard-geometry block).
fn extent_tail_offset(block: &[u8]) -> usize {
    let eh_max = u16::from_le_bytes([block[4], block[5]]) as usize;
    12 + 12 * eh_max
}

/// `ext4_extent_tail.et_checksum` for a separate (non-inline) extent index/leaf
/// block. Covers the header + all `eh_max` slots, seeded by the owning inode's
/// seed (`iseed = inode_seed(fs_seed, inum, generation)`). The inline root in an
/// inode's 60-byte `i_block` has **no** tail — it is covered by the inode csum.
pub fn extent_block_csum(iseed: u32, block: &[u8]) -> u32 {
    let tail = extent_tail_offset(block).min(block.len());
    ext_crc(iseed, &block[..tail])
}

/// Write `et_checksum` into a separate extent block's 4-byte tail. No-op if the
/// computed tail offset doesn't leave room (malformed `eh_max`).
pub fn stamp_extent_block_csum(iseed: u32, block: &mut [u8]) {
    let tail = extent_tail_offset(block);
    if tail + 4 <= block.len() {
        let c = extent_block_csum(iseed, block);
        block[tail..tail + 4].copy_from_slice(&c.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read};

    fn le(b: &[u8], o: usize, n: usize) -> u64 {
        let mut v = 0u64;
        for i in 0..n {
            v |= (b[o + i] as u64) << (8 * i);
        }
        v
    }

    #[test]
    fn ext_crc_matches_known_vectors() {
        // Standard CRC-32C canonical vector, via the crate directly.
        assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283);
        // Seed derived from UUID 11111111-2222-3333-4444-555555555555 — the value
        // mke2fs stored in s_checksum_seed for that UUID (verified externally).
        let uuid: [u8; 16] = [
            0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55,
        ];
        assert_eq!(csum_seed(&uuid), 0x643d_bf5b);
    }

    fn load_ext4() -> Vec<u8> {
        let path = "tests/fixtures/test_ext4.img.zst";
        let compressed = std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        let mut dec = zstd::stream::read::Decoder::new(Cursor::new(compressed)).expect("zstd");
        let mut out = Vec::new();
        dec.read_to_end(&mut out).expect("decompress");
        out
    }

    /// Verify every stamper reproduces the stored checksums in the committed
    /// metadata_csum ext4 fixture (a real mke2fs image).
    #[test]
    fn stampers_reproduce_fixture_checksums() {
        let d = load_ext4();
        let sb = &d[1024..2048];
        let bs = 1024usize << le(sb, 0x18, 4);
        let ipg = le(sb, 0x28, 4) as usize;
        let isz = le(sb, 0x58, 2) as usize;
        let fdb = le(sb, 0x14, 4) as usize;
        let incompat = le(sb, 0x60, 4);
        let desc_size = if incompat & 0x80 != 0 {
            le(sb, 0xFE, 2) as usize
        } else {
            32
        };
        let uuid: [u8; 16] = sb[0x68..0x78].try_into().unwrap();
        let seed = csum_seed(&uuid);

        // Superblock.
        assert_eq!(
            superblock_csum(sb),
            le(sb, SB_CSUM_OFF, 4) as u32,
            "superblock csum"
        );

        // Group descriptor 0 + its bitmap checksums.
        let gdt_off = (fdb + 1) * bs;
        let desc = &d[gdt_off..gdt_off + desc_size];
        assert_eq!(
            group_desc_csum(seed, 0, desc) as u64,
            le(desc, GD_BG_CHECKSUM, 2),
            "bg_checksum"
        );
        let bb = le(desc, 0x00, 4) as usize;
        let ib = le(desc, 0x04, 4) as usize;
        let it = le(desc, 0x08, 4) as usize;
        let (bbm_lo, _) = bitmap_csum(seed, &d[bb * bs..bb * bs + bs]);
        assert_eq!(
            bbm_lo as u64,
            le(desc, GD_BBM_CSUM_LO, 2),
            "block bitmap csum"
        );
        let (ibm_lo, _) = bitmap_csum(seed, &d[ib * bs..ib * bs + ipg / 8]);
        assert_eq!(
            ibm_lo as u64,
            le(desc, GD_IBM_CSUM_LO, 2),
            "inode bitmap csum"
        );

        // Root inode (#2, index 1) — lo + hi.
        let ino_off = it * bs + isz; // index 1
        let inode = &d[ino_off..ino_off + isz];
        let (lo, hi) = inode_csum(seed, inode, 2);
        assert_eq!(lo as u64, le(inode, INO_CSUM_LO, 2), "inode csum lo");
        assert_eq!(hi as u64, le(inode, INO_CSUM_HI, 2), "inode csum hi");

        // Root directory block via root's inline extent, tail checksum.
        let ib60 = &inode[0x28..0x28 + 60];
        assert_eq!(le(ib60, 0, 2), 0xF30A, "root should be extent-mapped");
        let ee_start = (le(ib60, 12 + 8, 4) | (le(ib60, 12 + 6, 2) << 32)) as usize;
        let gen = le(inode, INO_GENERATION, 4) as u32;
        let dblk = &d[ee_start * bs..ee_start * bs + bs];
        assert_eq!(
            dir_block_csum(seed, dblk, 2, gen) as u64,
            le(dblk, bs - 4, 4),
            "root dir tail csum"
        );
    }
}
