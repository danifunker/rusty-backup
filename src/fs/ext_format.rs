//! ext2 blank-volume formatter — the write side of `rb-cli new --fs ext`.
//!
//! Produces a **plain rev-1 ext2** image (128-byte inodes; feature set limited to
//! `filetype` + `sparse_super`; no journal / extents / metadata checksums) that
//!
//!   * `ExtFilesystem::open` round-trips,
//!   * our own [`super::ext_fsck`] reports clean, and
//!   * the external `e2fsck -fn` accepts as clean.
//!
//! The formatter deliberately reuses the metadata-marking and bitmap-building
//! helpers from `ext_fsck` (`mark_metadata`, `build_block_bitmap`,
//! `build_inode_bitmap`, `has_super_backup`, `BlockMap`) and the inode serializer
//! from `ext` (`build_inode_bytes`), so the bytes we lay down are, by
//! construction, exactly what the checker recomputes and verifies.
//!
//! ## Layout produced
//!
//! One or more block groups (sparse-super backups in groups 0, 1, 3, 5, 7, 9,
//! 25, …). Reserved inodes 1..=11 are marked used; inode 2 is the root directory
//! and inode 11 is `lost+found` — both laid down by hand because
//! `ExtFilesystem::allocate_inode` never hands out reserved inodes. Everything
//! else (empty inode tables, all data blocks) stays sparse in a streamed write.

use std::io::{Cursor, Seek, SeekFrom, Write};

use super::ext::{build_inode_bytes, ExtGeom, ExtGroup};
use super::ext_fsck::{
    build_block_bitmap, build_inode_bitmap, has_super_backup, mark_metadata, BlockMap,
};
use super::filesystem::FilesystemError;

// ---- ext2 on-disk constants (mirrors of the private ones in ext.rs) ----

const EXT_MAGIC: u16 = 0xEF53;
const INODE_SIZE: u16 = 128;
const DESC_SIZE: u16 = 32;
/// First non-reserved inode. mke2fs stamps 11 and uses inode 11 for lost+found.
const FIRST_INO: u32 = 11;
const ROOT_INO: u32 = 2;
const LOST_FOUND_INO: u32 = 11;
const FT_DIR: u8 = 2;

// Feature bits set on the "plainest ext2": directory entries carry a file-type
// byte, and superblock/GDT backups follow the sparse-super schedule.
const INCOMPAT_FILETYPE: u32 = 0x0002;
const RO_COMPAT_SPARSE_SUPER: u32 = 0x0001;
/// ext3 = ext2 + this COMPAT feature (an on-disk jbd2 journal, inode 8).
const COMPAT_HAS_JOURNAL: u32 = 0x0004;

// ---- ext3 journal (jbd2) constants ----

/// The journal is reserved inode 8 (`EXT2_JOURNAL_INO`).
const EXT_JOURNAL_INO: u32 = 8;
/// Journal length in fs blocks. 1024 is the jbd2 minimum (`JBD2_MIN_JOURNAL_BLOCKS`)
/// and — with ext3 forced to 4 KiB blocks — fits in 12 direct + 1 single-indirect
/// block, so the formatter never needs a double-indirect writer.
const JOURNAL_BLOCKS: u64 = 1024;
/// jbd2 journal-superblock magic (stored **big-endian**, like all jbd2 fields).
const JBD2_MAGIC: u32 = 0xc03b_3998;
/// `JBD2_SUPERBLOCK_V2` block type — what mke2fs stamps for a fresh journal.
const JBD2_SUPERBLOCK_V2: u32 = 4;
/// `EXT3_JNL_BACKUP_BLOCKS` — the superblock carries a copy of the journal
/// inode's block map so e2fsck can find the journal if inode 8 is damaged.
const JNL_BACKUP_BLOCKS: u8 = 1;

/// Default inode density — one inode per 16 KiB (the mke2fs default).
const DEFAULT_BYTES_PER_INODE: u64 = 16384;

/// Smallest ext2 volume we'll format. Below this the fixed metadata dominates and
/// the result isn't useful; `new`'s default 800K floppy is comfortably above it.
const MIN_SIZE: u64 = 256 * 1024;
/// Smallest ext3 volume — must hold the 4 MiB journal (1024 × 4 KiB) plus
/// metadata with room to spare.
const MIN_SIZE_EXT3: u64 = 8 * 1024 * 1024;

/// Resolved geometry for a blank ext2 volume.
struct Ext2Layout {
    block_size: u64,
    total_blocks: u64,
    blocks_per_group: u32,
    first_data_block: u32,
    inodes_per_group: u32,
    inodes_count: u32,
    group_count: u32,
    /// One descriptor per group; `free_blocks` / `free_inodes` filled during the
    /// write once the allocation is known.
    groups: Vec<ExtGroup>,
    root_dir_block: u64,
    lost_found_block: u64,
    /// ext3 journal: number of journal blocks (0 for ext2), plus the locations of
    /// its single-indirect block and its first data block (which holds the jbd2
    /// journal superblock). All within block group 0.
    journal_blocks: u64,
    journal_indirect_block: u64,
    journal_data_start: u64,
}

impl Ext2Layout {
    /// Byte length of the formatted image (`total_blocks * block_size`).
    fn disk_bytes(&self) -> u64 {
        self.total_blocks * self.block_size
    }

    /// An `ExtGeom` snapshot for the reused `ext_fsck` builders.
    fn geom(&self, part_off: u64) -> ExtGeom {
        ExtGeom {
            partition_offset: part_off,
            block_size: self.block_size,
            total_blocks: self.total_blocks,
            blocks_per_group: self.blocks_per_group,
            first_data_block: self.first_data_block,
            inodes_count: self.inodes_count,
            inodes_per_group: self.inodes_per_group,
            inode_size: INODE_SIZE,
            first_ino: FIRST_INO,
            group_count: self.group_count,
            desc_size: DESC_SIZE,
            sparse_super: true,
            checksummed: false,
            free_blocks: 0,
            free_inodes: 0,
        }
    }
}

/// 1 KiB blocks below 8 MiB, 4 KiB above — a small subset of mke2fs's size table
/// that keeps small volumes roomy and large ones cheap on metadata.
fn choose_block_size(size_bytes: u64) -> u64 {
    if size_bytes <= 8 * 1024 * 1024 {
        1024
    } else {
        4096
    }
}

/// Resolve geometry from a requested byte size. May format slightly fewer bytes
/// than requested if the trailing block group would be too small to hold its own
/// metadata (that runt group is dropped, mke2fs-style).
fn compute_layout(size_bytes: u64, journal: bool) -> Result<Ext2Layout, FilesystemError> {
    let min = if journal { MIN_SIZE_EXT3 } else { MIN_SIZE };
    if size_bytes < min {
        let kind = if journal { "ext3" } else { "ext2" };
        return Err(FilesystemError::InvalidData(format!(
            "{kind} volume must be at least {min} bytes, got {size_bytes}"
        )));
    }
    // ext3 is forced to 4 KiB blocks so the 1024-block journal fits in a single
    // level of indirection (direct + single-indirect).
    let block_size = if journal {
        4096
    } else {
        choose_block_size(size_bytes)
    };
    let first_data_block: u32 = if block_size == 1024 { 1 } else { 0 };
    let blocks_per_group: u32 = (block_size * 8) as u32; // bits in one block bitmap

    let mut total_blocks = size_bytes / block_size;

    // Inodes-per-group is fixed up front from the initial group estimate so the
    // runt-drop loop below doesn't chase a moving inode-table size.
    let initial_groups =
        ((total_blocks - first_data_block as u64).div_ceil(blocks_per_group as u64)).max(1);
    let inodes_target = (size_bytes / DEFAULT_BYTES_PER_INODE).max(FIRST_INO as u64 + 5);
    let inodes_per_group = ((inodes_target.div_ceil(initial_groups)) as u32)
        .next_multiple_of(8)
        .clamp(16, blocks_per_group); // <= bits in one inode bitmap block
    let inode_table_blocks = (inodes_per_group as u64 * INODE_SIZE as u64).div_ceil(block_size);

    // Drop a trailing runt group that can't hold its own metadata + a data block.
    let mut group_count =
        ((total_blocks - first_data_block as u64).div_ceil(blocks_per_group as u64)) as u32;
    loop {
        let gdt_blocks = (group_count as u64 * DESC_SIZE as u64).div_ceil(block_size);
        let last = group_count - 1;
        let last_start = first_data_block as u64 + last as u64 * blocks_per_group as u64;
        let last_blocks = total_blocks - last_start;
        let backup_overhead = if has_super_backup(last, true) {
            1 + gdt_blocks
        } else {
            0
        };
        let overhead = backup_overhead + 2 /* block+inode bitmap */ + inode_table_blocks;
        if group_count >= 2 && last_blocks < overhead + 1 {
            total_blocks = last_start;
            group_count -= 1;
            continue;
        }
        break;
    }

    let gdt_blocks = (group_count as u64 * DESC_SIZE as u64).div_ceil(block_size);
    let inodes_count = inodes_per_group * group_count;

    // Per-group descriptor block locations: [super + GDT backup?] then block
    // bitmap, inode bitmap, inode table.
    let mut groups = Vec::with_capacity(group_count as usize);
    for gi in 0..group_count {
        let group_start = first_data_block as u64 + gi as u64 * blocks_per_group as u64;
        let prefix = if has_super_backup(gi, true) {
            1 + gdt_blocks
        } else {
            0
        };
        let block_bitmap = group_start + prefix;
        let inode_bitmap = block_bitmap + 1;
        let inode_table = inode_bitmap + 1;
        groups.push(ExtGroup {
            block_bitmap,
            inode_bitmap,
            inode_table,
            free_blocks: 0,
            free_inodes: 0,
        });
    }

    // Root + lost+found directory data: the first two data blocks in group 0.
    let group0_data_start = groups[0].inode_table + inode_table_blocks;
    let root_dir_block = group0_data_start;
    let lost_found_block = group0_data_start + 1;

    // ext3 journal: single-indirect block then a contiguous run of journal data
    // blocks, all in group 0 straight after lost+found.
    let (journal_blocks, journal_indirect_block, journal_data_start) = if journal {
        let indirect = lost_found_block + 1;
        let data_start = indirect + 1;
        let journal_end = data_start + JOURNAL_BLOCKS;
        let group0_end = first_data_block as u64 + blocks_per_group as u64;
        if journal_end > total_blocks || journal_end > group0_end {
            return Err(FilesystemError::InvalidData(format!(
                "volume too small for an ext3 journal ({JOURNAL_BLOCKS} blocks); use a larger --size"
            )));
        }
        (JOURNAL_BLOCKS, indirect, data_start)
    } else {
        (0, 0, 0)
    };

    Ok(Ext2Layout {
        block_size,
        total_blocks,
        blocks_per_group,
        first_data_block,
        inodes_per_group,
        inodes_count,
        group_count,
        groups,
        root_dir_block,
        lost_found_block,
        journal_blocks,
        journal_indirect_block,
        journal_data_start,
    })
}

/// Format a blank ext (ext2, or ext3 when `journal`) volume of (approximately)
/// `size_bytes` into `sink` at byte offset `part_off`, writing only the populated
/// metadata regions so the bulk of the volume stays sparse. Returns the exact
/// formatted byte length.
fn write_blank_ext<W: Write + Seek>(
    sink: &mut W,
    part_off: u64,
    size_bytes: u64,
    label: &str,
    journal: bool,
) -> Result<u64, FilesystemError> {
    let mut layout = compute_layout(size_bytes, journal)?;
    let block_size = layout.block_size;
    let geom = layout.geom(part_off);

    // ---- Compute the block allocation and per-group free counts. ----
    let mut map = BlockMap::new(layout.total_blocks);
    mark_metadata(&geom, &layout.groups, &mut map);
    map.set(layout.root_dir_block);
    map.set(layout.lost_found_block);
    // ext3: the journal's single-indirect block + all its data blocks are used.
    if layout.journal_blocks > 0 {
        map.set(layout.journal_indirect_block);
        for i in 0..layout.journal_blocks {
            map.set(layout.journal_data_start + i);
        }
    }

    let mut total_free_blocks = 0u64;
    for gi in 0..layout.group_count as usize {
        let group_start =
            layout.first_data_block as u64 + gi as u64 * layout.blocks_per_group as u64;
        let blocks_in_group =
            (layout.total_blocks - group_start).min(layout.blocks_per_group as u64);
        let used = (0..blocks_in_group)
            .filter(|j| map.get(group_start + j))
            .count() as u64;
        let free = blocks_in_group - used;
        layout.groups[gi].free_blocks = free as u32;
        total_free_blocks += free;
    }

    // Reserved inodes 1..=11 live in group 0; every other inode is free.
    let mut inode_used = vec![false; layout.inodes_count as usize + 1];
    for used in inode_used.iter_mut().take(FIRST_INO as usize + 1).skip(1) {
        *used = true;
    }
    for gi in 0..layout.group_count as usize {
        let used_inodes = if gi == 0 { FIRST_INO } else { 0 };
        layout.groups[gi].free_inodes = layout.inodes_per_group - used_inodes;
    }
    let total_free_inodes = layout.inodes_count - FIRST_INO;

    // ext3: build the journal inode's block map (12 direct + single-indirect) and
    // the superblock's backup copy of it (`s_jnl_blocks`).
    let journal = if layout.journal_blocks > 0 {
        Some(build_journal_map(&layout))
    } else {
        None
    };

    // ---- Serialize the fixed structures. ----
    let sb_primary = build_superblock(
        &layout,
        label,
        0,
        total_free_blocks,
        total_free_inodes,
        journal.as_ref().map(|j| &j.jnl_backup),
    );
    let gdt = build_gdt(&layout);

    // Primary superblock is always at byte offset 1024; its GDT follows in the
    // next block (block 2 for 1 KiB blocks, block 1 for 4 KiB).
    write_at(sink, part_off + 1024, &sb_primary)?;
    let sb_block: u64 = if block_size == 1024 { 1 } else { 0 };
    write_at(sink, part_off + (sb_block + 1) * block_size, &gdt)?;

    // Sparse-super backups: a superblock copy at the group's first block (offset
    // 0), GDT copy in the next block. Only s_block_group_nr differs.
    for gi in 1..layout.group_count {
        if !has_super_backup(gi, true) {
            continue;
        }
        let group_start =
            layout.first_data_block as u64 + gi as u64 * layout.blocks_per_group as u64;
        let mut sb_copy = sb_primary.clone();
        le16w(&mut sb_copy, 0x5A, gi as u16); // s_block_group_nr
        write_at(sink, part_off + group_start * block_size, &sb_copy)?;
        write_at(sink, part_off + (group_start + 1) * block_size, &gdt)?;
    }

    // Per-group block + inode bitmaps.
    for gi in 0..layout.group_count as usize {
        let bb = build_block_bitmap(&geom, gi, &map);
        write_at(
            sink,
            part_off + layout.groups[gi].block_bitmap * block_size,
            &bb,
        )?;
        let ib = build_inode_bitmap(&geom, gi, &inode_used);
        write_at(
            sink,
            part_off + layout.groups[gi].inode_bitmap * block_size,
            &ib,
        )?;
    }

    // Root (#2) and lost+found (#11) inodes in group 0's inode table.
    let it0 = layout.groups[0].inode_table * block_size;
    let root_inode = build_inode_bytes(
        INODE_SIZE,
        0o040755,
        0,
        0,
        block_size,
        3, // ".", ".." and lost+found's ".."
        0,
        &direct_iblock(layout.root_dir_block),
    );
    write_at(
        sink,
        part_off + it0 + (ROOT_INO as u64 - 1) * INODE_SIZE as u64,
        &root_inode,
    )?;
    let lf_inode = build_inode_bytes(
        INODE_SIZE,
        0o040700,
        0,
        0,
        block_size,
        2, // "." and root's entry
        0,
        &direct_iblock(layout.lost_found_block),
    );
    write_at(
        sink,
        part_off + it0 + (LOST_FOUND_INO as u64 - 1) * INODE_SIZE as u64,
        &lf_inode,
    )?;

    // Directory data blocks.
    write_at(
        sink,
        part_off + layout.root_dir_block * block_size,
        &build_root_dir(block_size),
    )?;
    write_at(
        sink,
        part_off + layout.lost_found_block * block_size,
        &build_lost_found_dir(block_size),
    )?;

    // ext3 journal: inode 8, its single-indirect block, and the jbd2 superblock.
    if let Some(j) = &journal {
        let jsize = layout.journal_blocks * block_size;
        // Reserved inode 8 is a regular file (mode 0600). i_blocks must count the
        // single-indirect block too, which `build_inode_bytes` (size-derived)
        // doesn't — so patch i_blocks_lo directly.
        let mut jinode = build_inode_bytes(INODE_SIZE, 0o100600, 0, 0, jsize, 1, 0, &j.iblock);
        let sectors = ((layout.journal_blocks + 1) * (block_size / 512)) as u32;
        jinode[0x1C..0x20].copy_from_slice(&sectors.to_le_bytes()); // i_blocks_lo
        write_at(
            sink,
            part_off + it0 + (EXT_JOURNAL_INO as u64 - 1) * INODE_SIZE as u64,
            &jinode,
        )?;

        // Single-indirect block: pointers to journal data blocks [12..].
        let mut indblk = vec![0u8; block_size as usize];
        for k in 0..(layout.journal_blocks as usize - 12) {
            let b = (layout.journal_data_start + 12 + k as u64) as u32;
            indblk[k * 4..k * 4 + 4].copy_from_slice(&b.to_le_bytes());
        }
        write_at(
            sink,
            part_off + layout.journal_indirect_block * block_size,
            &indblk,
        )?;

        // jbd2 journal superblock at journal block 0 (empty: s_start = 0).
        let jsb = build_jbd2_superblock(block_size, layout.journal_blocks);
        write_at(
            sink,
            part_off + layout.journal_data_start * block_size,
            &jsb,
        )?;
    }

    Ok(layout.disk_bytes())
}

/// Stream a blank **ext2** volume into `sink` at `part_off`. Returns the exact
/// formatted byte length.
pub fn write_blank_ext2<W: Write + Seek>(
    sink: &mut W,
    part_off: u64,
    size_bytes: u64,
    label: &str,
) -> Result<u64, FilesystemError> {
    write_blank_ext(sink, part_off, size_bytes, label, false)
}

/// Stream a blank **ext3** volume (ext2 + an empty jbd2 journal) into `sink`.
pub fn write_blank_ext3<W: Write + Seek>(
    sink: &mut W,
    part_off: u64,
    size_bytes: u64,
    label: &str,
) -> Result<u64, FilesystemError> {
    write_blank_ext(sink, part_off, size_bytes, label, true)
}

/// Format a blank ext2/ext3 volume and return the whole image in memory. Used by
/// the `batch` multi-partition builder and tests; the CLI `new` verb streams
/// straight to a file instead.
fn create_blank_ext(
    size_bytes: u64,
    label: &str,
    journal: bool,
) -> Result<Vec<u8>, FilesystemError> {
    let layout = compute_layout(size_bytes, journal)?;
    let mut img = vec![0u8; layout.disk_bytes() as usize];
    write_blank_ext(
        &mut Cursor::new(&mut img[..]),
        0,
        size_bytes,
        label,
        journal,
    )?;
    Ok(img)
}

/// Format a blank **ext2** volume as an in-memory image.
pub fn create_blank_ext2(size_bytes: u64, label: &str) -> Result<Vec<u8>, FilesystemError> {
    create_blank_ext(size_bytes, label, false)
}

/// Format a blank **ext3** volume (ext2 + an empty jbd2 journal) as an in-memory
/// image.
pub fn create_blank_ext3(size_bytes: u64, label: &str) -> Result<Vec<u8>, FilesystemError> {
    create_blank_ext(size_bytes, label, true)
}

// ---- Serializers ----

fn build_superblock(
    l: &Ext2Layout,
    label: &str,
    block_group_nr: u16,
    free_blocks: u64,
    free_inodes: u32,
    journal_backup: Option<&[u32; 17]>,
) -> Vec<u8> {
    let mut sb = vec![0u8; 1024];
    let log_bs = l.block_size.trailing_zeros() - 10; // 1024 -> 0, 4096 -> 2
    let now = now_secs();
    let feature_compat = if journal_backup.is_some() {
        COMPAT_HAS_JOURNAL
    } else {
        0
    };

    le32w(&mut sb, 0x00, l.inodes_count); // s_inodes_count
    le32w(&mut sb, 0x04, l.total_blocks as u32); // s_blocks_count_lo
    le32w(&mut sb, 0x08, (l.total_blocks * 5 / 100) as u32); // s_r_blocks_count_lo (5%)
    le32w(&mut sb, 0x0C, free_blocks as u32); // s_free_blocks_count_lo
    le32w(&mut sb, 0x10, free_inodes); // s_free_inodes_count
    le32w(&mut sb, 0x14, l.first_data_block); // s_first_data_block
    le32w(&mut sb, 0x18, log_bs); // s_log_block_size
    le32w(&mut sb, 0x1C, log_bs); // s_log_cluster_size (== block size; no bigalloc)
    le32w(&mut sb, 0x20, l.blocks_per_group); // s_blocks_per_group
    le32w(&mut sb, 0x24, l.blocks_per_group); // s_clusters_per_group
    le32w(&mut sb, 0x28, l.inodes_per_group); // s_inodes_per_group
    le32w(&mut sb, 0x30, now); // s_wtime
    le16w(&mut sb, 0x36, 0xFFFF); // s_max_mnt_count = -1 (no forced check)
    le16w(&mut sb, 0x38, EXT_MAGIC); // s_magic
    le16w(&mut sb, 0x3A, 1); // s_state = EXT2_VALID_FS (cleanly unmounted)
    le16w(&mut sb, 0x3C, 1); // s_errors = continue
    le32w(&mut sb, 0x40, now); // s_lastcheck
    le32w(&mut sb, 0x4C, 1); // s_rev_level = DYNAMIC_REV (honours inode_size/first_ino)
    le32w(&mut sb, 0x54, FIRST_INO); // s_first_ino
    le16w(&mut sb, 0x58, INODE_SIZE); // s_inode_size
    le16w(&mut sb, 0x5A, block_group_nr); // s_block_group_nr
    le32w(&mut sb, 0x5C, feature_compat); // s_feature_compat
    le32w(&mut sb, 0x60, INCOMPAT_FILETYPE); // s_feature_incompat
    le32w(&mut sb, 0x64, RO_COMPAT_SPARSE_SUPER); // s_feature_ro_compat
    sb[0x68..0x78].copy_from_slice(&make_uuid()); // s_uuid

    let name = label.as_bytes();
    let n = name.len().min(16);
    sb[0x78..0x78 + n].copy_from_slice(&name[..n]); // s_volume_name

    // ext3: journal inode number + a backup of its block map (so e2fsck -fn is
    // clean rather than wanting to add the backup).
    if let Some(jnl) = journal_backup {
        le32w(&mut sb, 0xE0, EXT_JOURNAL_INO); // s_journal_inum
        sb[0xFD] = JNL_BACKUP_BLOCKS; // s_jnl_backup_type
        for (i, v) in jnl.iter().enumerate() {
            le32w(&mut sb, 0x10C + i * 4, *v); // s_jnl_blocks[17]
        }
    }

    sb
}

fn build_gdt(l: &Ext2Layout) -> Vec<u8> {
    let mut gdt = vec![0u8; l.group_count as usize * DESC_SIZE as usize];
    for (gi, g) in l.groups.iter().enumerate() {
        let o = gi * DESC_SIZE as usize;
        le32w(&mut gdt, o, g.block_bitmap as u32); // bg_block_bitmap
        le32w(&mut gdt, o + 0x04, g.inode_bitmap as u32); // bg_inode_bitmap
        le32w(&mut gdt, o + 0x08, g.inode_table as u32); // bg_inode_table
        le16w(&mut gdt, o + 0x0C, g.free_blocks as u16); // bg_free_blocks_count
        le16w(&mut gdt, o + 0x0E, g.free_inodes as u16); // bg_free_inodes_count
        let used_dirs = if gi == 0 { 2 } else { 0 }; // root + lost+found
        le16w(&mut gdt, o + 0x10, used_dirs); // bg_used_dirs_count
    }
    gdt
}

/// The ext3 journal inode's block map, in two forms: the 60-byte `i_block`
/// (12 direct + single-indirect) for inode 8, and the 17-entry `s_jnl_blocks`
/// backup for the superblock.
struct JournalMap {
    iblock: [u8; 60],
    jnl_backup: [u32; 17],
}

/// Build the journal inode's block map. The journal occupies a contiguous run of
/// `journal_blocks` blocks from `journal_data_start`, with block 12.. reached via
/// the single-indirect block. (ext3 is forced to 4 KiB blocks, so one indirect
/// level covers the 1024-block journal.)
fn build_journal_map(l: &Ext2Layout) -> JournalMap {
    let mut iblock = [0u8; 60];
    let mut jnl_backup = [0u32; 17];
    let ndirect = 12.min(l.journal_blocks) as usize;
    for j in 0..ndirect {
        let b = (l.journal_data_start + j as u64) as u32;
        iblock[j * 4..j * 4 + 4].copy_from_slice(&b.to_le_bytes());
        jnl_backup[j] = b;
    }
    // i_block[12] = single-indirect pointer (i_block[13]/[14] double/triple = 0).
    let ind = l.journal_indirect_block as u32;
    iblock[48..52].copy_from_slice(&ind.to_le_bytes());
    jnl_backup[12] = ind;
    // s_jnl_blocks[15] = i_size_high, [16] = i_size_low.
    let jsize = l.journal_blocks * l.block_size;
    jnl_backup[15] = (jsize >> 32) as u32;
    jnl_backup[16] = jsize as u32;
    JournalMap { iblock, jnl_backup }
}

/// Build an empty jbd2 journal superblock (all fields **big-endian**). `s_start`
/// = 0 marks the log empty, so e2fsck sees nothing to recover.
fn build_jbd2_superblock(block_size: u64, journal_blocks: u64) -> Vec<u8> {
    let mut jsb = vec![0u8; block_size as usize];
    be32w(&mut jsb, 0x00, JBD2_MAGIC); // h_magic
    be32w(&mut jsb, 0x04, JBD2_SUPERBLOCK_V2); // h_blocktype
                                               // h_sequence (0x08) = 0
    be32w(&mut jsb, 0x0C, block_size as u32); // s_blocksize
    be32w(&mut jsb, 0x10, journal_blocks as u32); // s_maxlen
    be32w(&mut jsb, 0x14, 1); // s_first (log starts after the superblock)
    be32w(&mut jsb, 0x18, 1); // s_sequence (first commit id)
                              // s_start (0x1C) = 0 -> empty journal
                              // s_feature_* (0x24/0x28/0x2C) = 0
    jsb[0x30..0x40].copy_from_slice(&make_uuid()); // s_uuid
    be32w(&mut jsb, 0x40, 1); // s_nr_users
    jsb
}

/// Root directory block: `.` and `..` both point at root (2), then `lost+found`
/// (11) fills the rest of the block.
fn build_root_dir(block_size: u64) -> Vec<u8> {
    let bs = block_size as usize;
    let mut d = vec![0u8; bs];
    put_dirent(&mut d, 0, ROOT_INO, 12, ".", FT_DIR);
    put_dirent(&mut d, 12, ROOT_INO, 12, "..", FT_DIR);
    put_dirent(
        &mut d,
        24,
        LOST_FOUND_INO,
        (bs - 24) as u16,
        "lost+found",
        FT_DIR,
    );
    d
}

/// lost+found block: `.` -> itself (11), `..` -> root (2) filling the block.
fn build_lost_found_dir(block_size: u64) -> Vec<u8> {
    let bs = block_size as usize;
    let mut d = vec![0u8; bs];
    put_dirent(&mut d, 0, LOST_FOUND_INO, 12, ".", FT_DIR);
    put_dirent(&mut d, 12, ROOT_INO, (bs - 12) as u16, "..", FT_DIR);
    d
}

fn put_dirent(buf: &mut [u8], off: usize, inode: u32, rec_len: u16, name: &str, ft: u8) {
    let nb = name.as_bytes();
    le32w(buf, off, inode);
    le16w(buf, off + 4, rec_len);
    buf[off + 6] = nb.len() as u8;
    buf[off + 7] = ft;
    buf[off + 8..off + 8 + nb.len()].copy_from_slice(nb);
}

/// A 60-byte `i_block` with a single direct pointer (plain ext2, no extents).
fn direct_iblock(block: u64) -> [u8; 60] {
    let mut ib = [0u8; 60];
    ib[0..4].copy_from_slice(&(block as u32).to_le_bytes());
    ib
}

// ---- small helpers ----

fn write_at<W: Write + Seek>(
    sink: &mut W,
    offset: u64,
    data: &[u8],
) -> Result<(), FilesystemError> {
    sink.seek(SeekFrom::Start(offset))?;
    sink.write_all(data)?;
    Ok(())
}

fn le32w(buf: &mut [u8], off: usize, val: u32) {
    buf[off..off + 4].copy_from_slice(&val.to_le_bytes());
}

fn le16w(buf: &mut [u8], off: usize, val: u16) {
    buf[off..off + 2].copy_from_slice(&val.to_le_bytes());
}

/// Big-endian u32 write — jbd2 journal structures are big-endian on disk.
fn be32w(buf: &mut [u8], off: usize, val: u32) {
    buf[off..off + 4].copy_from_slice(&val.to_be_bytes());
}

fn now_secs() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32
}

/// A best-effort unique 16-byte UUID (version-4 shape). e2fsck is content with
/// any non-zero UUID; uniqueness is nice-to-have, not load-bearing.
fn make_uuid() -> [u8; 16] {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut u = nanos.to_le_bytes();
    u[6] = (u[6] & 0x0F) | 0x40; // version 4
    u[8] = (u[8] & 0x3F) | 0x80; // variant
    u
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::entry::FileEntry;
    use crate::fs::ext::ExtFilesystem;
    use crate::fs::ext_fsck::{e2fsprogs_bin, fsck_ext};
    use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
    use std::process::Command;
    use std::sync::atomic::{AtomicU32, Ordering};

    // Sizes chosen to exercise every geometry path: 1 KiB blocks / single group
    // (1 & 8 MiB), 4 KiB blocks / single group (64 MiB), 4 KiB blocks / multi
    // group (160 MiB = 2 groups, the second non-runt).
    const SIZES: &[u64] = &[1 << 20, 8 << 20, 64 << 20, 160 << 20];

    fn lost_found(fs: &mut ExtFilesystem<Cursor<Vec<u8>>>) -> Option<FileEntry> {
        let root = fs.root().unwrap();
        fs.list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "lost+found")
    }

    #[test]
    fn blank_ext2_opens_and_lists_lost_found() {
        for &size in SIZES {
            let img = create_blank_ext2(size, "testvol").unwrap();
            let mut fs = ExtFilesystem::open(Cursor::new(img), 0).unwrap();
            assert_eq!(fs.volume_label(), Some("testvol"), "label at size {size}");
            let lf = lost_found(&mut fs).unwrap_or_else(|| panic!("no lost+found at size {size}"));
            assert!(lf.is_directory(), "lost+found is a directory (size {size})");
        }
    }

    #[test]
    fn blank_ext2_is_clean_per_our_fsck() {
        for &size in SIZES {
            let img = create_blank_ext2(size, "rusty").unwrap();
            let mut fs = ExtFilesystem::open(Cursor::new(img), 0).unwrap();
            let r = fsck_ext(&mut fs).unwrap();
            assert!(
                r.is_clean(),
                "size {size}: our fsck reported {:?}",
                r.errors
            );
        }
    }

    #[test]
    fn blank_ext2_below_minimum_is_rejected() {
        assert!(create_blank_ext2(64 * 1024, "x").is_err());
    }

    #[test]
    fn blank_ext2_supports_create_file() {
        let img = create_blank_ext2(8 << 20, "rusty").unwrap();
        let mut fs = ExtFilesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        let data = b"Hello from a freshly formatted ext2 volume!";
        let mut src = Cursor::new(data.to_vec());
        fs.create_file(
            &root,
            "hello.txt",
            &mut src,
            data.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        fs.sync_metadata().unwrap();

        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let hello = entries
            .iter()
            .find(|e| e.name == "hello.txt")
            .expect("hello.txt present after create_file");
        let back = fs.read_file(hello, usize::MAX).unwrap();
        assert_eq!(&back[..data.len()], data);

        // The volume must still reconcile after the write.
        let r = fsck_ext(&mut fs).unwrap();
        assert!(r.is_clean(), "post-write fsck: {:?}", r.errors);
    }

    // ---- Oracle cross-check against e2fsprogs. ----

    // One shared counter so temp-file names are unique across ALL oracle tests
    // (they run in parallel and share this process's pid).
    static E2FSCK_NONCE: AtomicU32 = AtomicU32::new(0);

    fn e2fsck_clean(bin: &str, img: &[u8]) -> bool {
        let n = E2FSCK_NONCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("rb_extfmt_{}_{}.img", std::process::id(), n));
        std::fs::write(&path, img).unwrap();
        // -f force a full check, -n answer no (read-only). Exit 0 == clean.
        let out = Command::new(bin).args(["-fn"]).arg(&path).output();
        let _ = std::fs::remove_file(&path);
        match out {
            Ok(o) if o.status.success() => true,
            Ok(o) => {
                eprintln!(
                    "e2fsck NOT clean (exit {:?}):\n--stdout--\n{}\n--stderr--\n{}",
                    o.status.code(),
                    String::from_utf8_lossy(&o.stdout),
                    String::from_utf8_lossy(&o.stderr)
                );
                false
            }
            Err(_) => false,
        }
    }

    #[test]
    fn blank_ext2_passes_e2fsck() {
        let Some(e2fsck) = e2fsprogs_bin("e2fsck") else {
            eprintln!("skipping: e2fsck not available");
            return;
        };
        for &size in SIZES {
            let img = create_blank_ext2(size, "rusty").unwrap();
            assert!(
                e2fsck_clean(&e2fsck, &img),
                "e2fsck faulted a blank {size}-byte ext2 volume"
            );
        }

        // And after writing files through the editor — including a non-block-
        // aligned size on a 4 KiB volume, which guards the i_blocks fix.
        for (size, flen) in [(8u64 << 20, 5000u64), (64 << 20, 9000)] {
            let mut img = create_blank_ext2(size, "rusty").unwrap();
            {
                let mut fs = ExtFilesystem::open(Cursor::new(&mut img[..]), 0).unwrap();
                let root = fs.root().unwrap();
                let data = vec![0xABu8; flen as usize];
                let mut src = Cursor::new(data);
                fs.create_file(
                    &root,
                    "blob.bin",
                    &mut src,
                    flen,
                    &CreateFileOptions::default(),
                )
                .unwrap();
                fs.sync_metadata().unwrap();
            }
            assert!(
                e2fsck_clean(&e2fsck, &img),
                "e2fsck faulted after create_file ({flen} B into {size} B volume)"
            );
        }
    }

    #[test]
    fn mke2fs_reference_opens_with_our_reader() {
        let Some(mke2fs) = e2fsprogs_bin("mke2fs") else {
            eprintln!("skipping: mke2fs not available");
            return;
        };
        let path = std::env::temp_dir().join(format!("rb_extref_{}.img", std::process::id()));
        std::fs::write(&path, vec![0u8; 16 << 20]).unwrap();
        let out = Command::new(mke2fs)
            .args(["-F", "-q", "-t", "ext2", "-b", "1024"])
            .arg(&path)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "mke2fs failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let bytes = std::fs::read(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        let mut fs = ExtFilesystem::open(Cursor::new(bytes), 0).unwrap();
        assert!(
            lost_found(&mut fs).is_some(),
            "our reader should see mke2fs's lost+found"
        );
    }

    // ---- ext3 (ext2 + jbd2 journal) ----

    // ext3 is forced to 4 KiB blocks and needs room for the 4 MiB journal: 8 &
    // 16 MiB single-group, 160 MiB multi-group.
    const EXT3_SIZES: &[u64] = &[8 << 20, 16 << 20, 64 << 20, 160 << 20];

    #[test]
    fn blank_ext3_opens_as_ext3_and_is_clean() {
        for &size in EXT3_SIZES {
            let img = create_blank_ext3(size, "journaled").unwrap();
            let mut fs = ExtFilesystem::open(Cursor::new(img), 0).unwrap();
            assert_eq!(fs.fs_type(), "ext3", "has_journal -> ext3 (size {size})");
            assert!(
                lost_found(&mut fs).is_some(),
                "lost+found present (size {size})"
            );
            let r = fsck_ext(&mut fs).unwrap();
            assert!(
                r.is_clean(),
                "size {size}: our fsck reported {:?}",
                r.errors
            );
        }
    }

    #[test]
    fn blank_ext3_below_minimum_is_rejected() {
        // 1 MiB can't hold the 4 MiB journal.
        assert!(create_blank_ext3(1 << 20, "x").is_err());
    }

    #[test]
    fn blank_ext3_supports_create_file() {
        let img = create_blank_ext3(16 << 20, "j").unwrap();
        let mut fs = ExtFilesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        let data = b"written into a journaled ext3 volume";
        let mut src = Cursor::new(data.to_vec());
        fs.create_file(
            &root,
            "note.txt",
            &mut src,
            data.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        fs.sync_metadata().unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let note = entries.iter().find(|e| e.name == "note.txt").unwrap();
        assert_eq!(&fs.read_file(note, usize::MAX).unwrap()[..data.len()], data);
        let r = fsck_ext(&mut fs).unwrap();
        assert!(r.is_clean(), "post-write fsck: {:?}", r.errors);
    }

    #[test]
    fn blank_ext3_passes_e2fsck() {
        let Some(e2fsck) = e2fsprogs_bin("e2fsck") else {
            eprintln!("skipping: e2fsck not available");
            return;
        };
        for &size in EXT3_SIZES {
            let img = create_blank_ext3(size, "journaled").unwrap();
            assert!(
                e2fsck_clean(&e2fsck, &img),
                "e2fsck faulted a blank {size}-byte ext3 volume"
            );
        }
        // And after writing a file through the editor.
        let mut img = create_blank_ext3(16 << 20, "journaled").unwrap();
        {
            let mut fs = ExtFilesystem::open(Cursor::new(&mut img[..]), 0).unwrap();
            let root = fs.root().unwrap();
            let data = vec![0xCDu8; 9000];
            let mut src = Cursor::new(data);
            fs.create_file(
                &root,
                "blob.bin",
                &mut src,
                9000,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.sync_metadata().unwrap();
        }
        assert!(
            e2fsck_clean(&e2fsck, &img),
            "e2fsck faulted an ext3 volume after create_file"
        );
    }
}
