//! SGI / IRIX XFS v4 read-only support.
//!
//! Step 5 brought up superblock + inode lookup. Step 6 added dir2 shortform
//! and single-block directory listing plus extent-list file reads. Step 6b
//! adds dir1 support (shortform + leaf blocks, including the node-dir1 case
//! handled implicitly by walking every file block and parsing only those
//! whose blkinfo magic is `0xFEEB`). Step 7 adds leaf/node dir2 directories
//! (walk the data-block address space, parse every `XD2D` block, ignore the
//! hash leaf since we only enumerate) and the bmap btree walker so files
//! with `di_format == 3` can be read.
//!
//! References:
//! - `~/xfs-efs/refs/xfs-modern/xfs/libxfs/xfs_format.h`
//! - `~/xfs-efs/refs/xfs-modern/xfs/libxfs/xfs_da_format.h`
//! - `~/xfs-efs/refs/xfs-modern/xfs/libxfs/xfs_bmap_btree.c`
//! - "XFS Algorithms & Data Structures", 3rd ed. (SGI).
//! - `docs/SGI_Filesystems.md` Steps 5 and 6.

pub mod ag;
pub mod bmap;
pub mod btree_build;
pub mod dir1;
pub mod dir2;
pub mod dir_repair;
pub mod edit;
pub mod freespace_rebuild;
pub mod fsck;
pub mod inobt_repair;
pub mod inode;
pub mod inode_repair;
pub mod repair;
pub mod sb;
pub mod symlink;
pub mod types;
pub mod v5_crc;

use std::io::{Read, Seek, SeekFrom, Write};

use byteorder::{BigEndian, ByteOrder};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{Filesystem, FilesystemError};
use bmap::{
    decode_extent, fsblock_to_partition_byte, XfsBmbtIrec, NULLFSBLOCK, XFS_BMAP_CRC_MAGIC,
    XFS_BMAP_MAGIC, XFS_BTREE_LBLOCK_CRC_LEN, XFS_BTREE_LBLOCK_LEN,
};
use inode::{fork_offset, inode_byte_offset, XfsDinodeCore};
use sb::XfsSuperblock;
use types::{DiFormat, DirFormat};

const SECTOR: u64 = 512;

/// 1 MiB streaming chunk size — matches the rest of the codebase.
const STREAM_CHUNK: usize = 1024 * 1024;

/// XFS filesystem reader.
pub struct XfsFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    sb: XfsSuperblock,
    dir_format: DirFormat,
    label: String,
    fs_type_name: &'static str,
}

impl<R: Read + Seek + Send> XfsFilesystem<R> {
    pub fn open(reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        Self::open_with_log(reader, partition_offset, |_| {})
    }

    pub fn open_with_log<F: FnMut(&str)>(
        mut reader: R,
        partition_offset: u64,
        mut log_cb: F,
    ) -> Result<Self, FilesystemError> {
        let mut sector = [0u8; SECTOR as usize];
        read_at_aligned(&mut reader, partition_offset, SECTOR, &mut sector)?;
        let sb = XfsSuperblock::parse(&sector)?;

        let dir_format = sb.dir_format();
        let fs_type_name = if sb.is_v5() {
            "XFS v5"
        } else {
            match sb.versionnum & types::XFS_SB_VERSION_NUMBITS {
                1 => "XFS v1",
                _ => "XFS v2",
            }
        };
        log_cb(&format!(
            "XFS dir format: {} ({})",
            match dir_format {
                DirFormat::Dir1 => "dir1",
                DirFormat::Dir2 => "dir2",
            },
            if sb.is_v5() { "v5/dir3" } else { "v4" }
        ));

        let label = sb.label();
        Ok(XfsFilesystem {
            reader,
            partition_offset,
            sb,
            dir_format,
            label,
            fs_type_name,
        })
    }

    pub fn dir_format(&self) -> DirFormat {
        self.dir_format
    }

    pub fn superblock(&self) -> &XfsSuperblock {
        &self.sb
    }

    /// Read a full on-disk inode buffer (sized to `sb.inodesize`). Returns
    /// the parsed core and the raw bytes so callers can also slice into the
    /// fork without a second seek.
    pub(crate) fn read_inode_buf(
        &mut self,
        ino: u64,
    ) -> Result<(XfsDinodeCore, Vec<u8>), FilesystemError> {
        let part_off = inode_byte_offset(
            ino,
            self.sb.agblocks,
            self.sb.agblklog,
            self.sb.inopblog,
            self.sb.blocksize,
            self.sb.inodesize,
        );
        let isz = self.sb.inodesize as u64;
        let sector_start = (part_off / SECTOR) * SECTOR;
        let span_end = part_off + isz;
        let span_len = (span_end - sector_start).div_ceil(SECTOR) * SECTOR;
        let mut buf = vec![0u8; span_len as usize];
        read_at_aligned(
            &mut self.reader,
            self.partition_offset + sector_start,
            span_len,
            &mut buf,
        )?;
        let off_in = (part_off - sector_start) as usize;
        let inode_buf = buf[off_in..off_in + isz as usize].to_vec();
        let core = XfsDinodeCore::parse(ino, &inode_buf)?;
        Ok((core, inode_buf))
    }

    pub fn read_inode(&mut self, ino: u64) -> Result<XfsDinodeCore, FilesystemError> {
        Ok(self.read_inode_buf(ino)?.0)
    }

    /// Slice the data-fork bytes out of a full inode buffer. The fork starts
    /// at offset 100 on v4 (offsetof(`di_crc`)) and at offset 176 on v5/v3
    /// (offsetof(`di_literal_area`)). If `di_forkoff > 0`, the data fork ends
    /// at `fork_start + (forkoff << 3)`; otherwise it consumes the rest of
    /// the literal area.
    fn data_fork<'a>(&self, core: &XfsDinodeCore, inode_buf: &'a [u8]) -> &'a [u8] {
        let fork_start = fork_offset(self.sb.is_v5());
        let end = if core.forkoff > 0 {
            fork_start + (core.forkoff as usize) * 8
        } else {
            self.sb.inodesize as usize
        };
        let end = end.min(inode_buf.len());
        &inode_buf[fork_start..end]
    }

    /// Decode `di_nextents` inline extent records from a fork buffer, sorted
    /// by file offset.
    fn decode_inline_extents(
        &self,
        fork: &[u8],
        n: usize,
    ) -> Result<Vec<XfsBmbtIrec>, FilesystemError> {
        let need = n.saturating_mul(16);
        if need > fork.len() {
            return Err(FilesystemError::Parse(format!(
                "XFS extents fork too small: need {need}, have {}",
                fork.len()
            )));
        }
        let mut recs = Vec::with_capacity(n);
        for i in 0..n {
            let off = i * 16;
            let chunk: &[u8; 16] = fork[off..off + 16].try_into().expect("16-byte chunk");
            recs.push(decode_extent(chunk));
        }
        recs.sort_by_key(|e| e.startoff);
        Ok(recs)
    }

    /// Decode the full extent list from either an inline-extents inode
    /// (`di_format == Extents`) or a bmap-btree inode (`di_format == Btree`).
    /// Returned records are sorted by file offset.
    pub(crate) fn decode_data_extents(
        &mut self,
        core: &XfsDinodeCore,
        inode_buf: &[u8],
    ) -> Result<Vec<XfsBmbtIrec>, FilesystemError> {
        match core.format {
            DiFormat::Extents => {
                let fork = self.data_fork(core, inode_buf);
                self.decode_inline_extents(fork, core.nextents as usize)
            }
            DiFormat::Btree => {
                let fork_len = self.data_fork(core, inode_buf).len();
                let (root_level, first_child) = {
                    let fork = self.data_fork(core, inode_buf);
                    parse_bmbt_root(fork, fork_len)?
                };
                self.walk_bmbt(root_level, first_child)
            }
            DiFormat::Local => Err(FilesystemError::InvalidData(
                "XFS local-format inode has no extent list".into(),
            )),
            DiFormat::Other(v) => Err(FilesystemError::Parse(format!(
                "unknown XFS di_format {v} on inode {}",
                core.ino
            ))),
        }
    }

    /// Walk a bmap btree to its leaves and collect every extent record.
    ///
    /// `root_level` is the level of the in-inode root (from `bb_level`).
    /// `first_child` is the leftmost child pointer in the root. We descend
    /// along the leftmost branch to level 0, then traverse the leaf chain
    /// via `bb_rightsib` until we hit `NULLFSBLOCK`.
    fn walk_bmbt(
        &mut self,
        root_level: u16,
        first_child: u64,
    ) -> Result<Vec<XfsBmbtIrec>, FilesystemError> {
        if root_level == 0 {
            return Err(FilesystemError::Parse(
                "XFS bmap btree root claims level 0 — should be inline extents".into(),
            ));
        }
        let bs = self.sb.blocksize as u64;
        let hdr_len = if self.sb.is_v5() {
            XFS_BTREE_LBLOCK_CRC_LEN
        } else {
            XFS_BTREE_LBLOCK_LEN
        };
        if (bs as usize) < hdr_len + 16 {
            return Err(FilesystemError::Parse(format!(
                "XFS blocksize {bs} too small for bmap btree block (hdr {hdr_len})"
            )));
        }
        let max_intermediate_recs = ((bs as usize) - hdr_len) / 16;

        let mut block = vec![0u8; bs as usize];
        let mut current = first_child;
        let mut expected_level = root_level - 1;
        loop {
            if current == NULLFSBLOCK {
                return Err(FilesystemError::Parse(
                    "XFS bmap btree: NULL child pointer during descent".into(),
                ));
            }
            self.read_fsblock(current, &mut block)?;
            check_bmbt_header(&block, Some(expected_level), self.sb.is_v5())?;
            if expected_level == 0 {
                break;
            }
            // Intermediate node: pick the leftmost child (slot 0). Keys
            // occupy `max_intermediate_recs * 8` bytes after the header;
            // pointers follow.
            let ptrs_off = hdr_len + max_intermediate_recs * 8;
            if ptrs_off + 8 > block.len() {
                return Err(FilesystemError::Parse(format!(
                    "XFS bmap btree intermediate block truncated (ptrs_off={ptrs_off}, bs={bs})"
                )));
            }
            current = BigEndian::read_u64(&block[ptrs_off..ptrs_off + 8]);
            expected_level -= 1;
        }

        // Leaf level: walk via rightsib, collecting records.
        // numrecs lives at offset 6 in both v4 and v5 long-form btree blocks.
        // Siblings: v4 at 8 / 16, v5 at 8 / 16 too (the order is leftsib,
        // rightsib at the top of the long-form union — see xfs_btree_block).
        let mut out = Vec::new();
        loop {
            let numrecs = BigEndian::read_u16(&block[6..8]) as usize;
            let max_leaf_recs = ((bs as usize) - hdr_len) / 16;
            if numrecs > max_leaf_recs {
                return Err(FilesystemError::Parse(format!(
                    "XFS bmap btree leaf numrecs {numrecs} > max {max_leaf_recs}"
                )));
            }
            for i in 0..numrecs {
                let off = hdr_len + i * 16;
                let chunk: &[u8; 16] = block[off..off + 16]
                    .try_into()
                    .expect("16-byte extent record");
                out.push(decode_extent(chunk));
            }
            let rightsib = BigEndian::read_u64(&block[16..24]);
            if rightsib == NULLFSBLOCK {
                break;
            }
            self.read_fsblock(rightsib, &mut block)?;
            check_bmbt_header(&block, Some(0), self.sb.is_v5())?;
        }
        out.sort_by_key(|e| e.startoff);
        Ok(out)
    }

    /// Collect every block a bmap btree physically occupies — every internal
    /// node from the descent plus every leaf reached via `rightsib`. Mirrors
    /// `walk_bmbt`'s structural traversal but returns block numbers instead of
    /// extents. Used by R2 freespace rebuild (`mark_inode_blocks`) to account
    /// the bmbt's own blocks when an inode is `DiFormat::Btree`.
    pub(crate) fn collect_bmbt_blocks(
        &mut self,
        root_level: u16,
        first_child: u64,
    ) -> Result<Vec<u64>, FilesystemError> {
        if root_level == 0 {
            return Err(FilesystemError::Parse(
                "XFS bmap btree root claims level 0 — should be inline extents".into(),
            ));
        }
        let bs = self.sb.blocksize as u64;
        let hdr_len = if self.sb.is_v5() {
            XFS_BTREE_LBLOCK_CRC_LEN
        } else {
            XFS_BTREE_LBLOCK_LEN
        };
        if (bs as usize) < hdr_len + 16 {
            return Err(FilesystemError::Parse(format!(
                "XFS blocksize {bs} too small for bmap btree block (hdr {hdr_len})"
            )));
        }
        let max_intermediate_recs = ((bs as usize) - hdr_len) / 16;

        let mut out = Vec::new();
        let mut block = vec![0u8; bs as usize];
        let mut current = first_child;
        let mut expected_level = root_level - 1;
        loop {
            if current == NULLFSBLOCK {
                return Err(FilesystemError::Parse(
                    "XFS bmap btree: NULL child pointer during descent".into(),
                ));
            }
            out.push(current);
            self.read_fsblock(current, &mut block)?;
            check_bmbt_header(&block, Some(expected_level), self.sb.is_v5())?;
            if expected_level == 0 {
                break;
            }
            let ptrs_off = hdr_len + max_intermediate_recs * 8;
            if ptrs_off + 8 > block.len() {
                return Err(FilesystemError::Parse(format!(
                    "XFS bmap btree intermediate block truncated (ptrs_off={ptrs_off}, bs={bs})"
                )));
            }
            current = BigEndian::read_u64(&block[ptrs_off..ptrs_off + 8]);
            expected_level -= 1;
        }
        // Walk the leaf sibling chain, collecting every leaf block.
        loop {
            let rightsib = BigEndian::read_u64(&block[16..24]);
            if rightsib == NULLFSBLOCK {
                break;
            }
            out.push(rightsib);
            self.read_fsblock(rightsib, &mut block)?;
            check_bmbt_header(&block, Some(0), self.sb.is_v5())?;
        }
        Ok(out)
    }

    /// Read the bytes of one filesystem block at `fsblock` into `out`.
    pub(crate) fn read_fsblock(
        &mut self,
        fsblock: u64,
        out: &mut [u8],
    ) -> Result<(), FilesystemError> {
        let bs = self.sb.blocksize as u64;
        let part_byte = fsblock_to_partition_byte(
            fsblock,
            self.sb.agblocks,
            self.sb.agblklog,
            self.sb.blocksize,
        );
        read_at_aligned(&mut self.reader, self.partition_offset + part_byte, bs, out)
    }

    /// Build a `FileEntry` from a directory entry's name + child inode.
    fn child_entry(
        &mut self,
        parent_path: &str,
        name: String,
        ino: u64,
    ) -> Result<FileEntry, FilesystemError> {
        let path = if parent_path == "/" {
            format!("/{name}")
        } else {
            format!("{parent_path}/{name}")
        };
        let (core, inode_buf) = self.read_inode_buf(ino)?;
        let entry_type = if core.is_dir() {
            EntryType::Directory
        } else if core.is_symlink() {
            EntryType::Symlink
        } else if core.is_regular() {
            EntryType::File
        } else {
            EntryType::Special
        };
        let symlink_target = if core.is_symlink() {
            self.read_symlink_target(&core, &inode_buf).ok()
        } else {
            None
        };
        let mut size = core.size;
        if matches!(entry_type, EntryType::Directory) {
            size = 0;
        }
        Ok(FileEntry {
            name,
            path,
            entry_type,
            size,
            location: ino,
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target,
            special_type: None,
            mode: Some(core.mode as u32),
            uid: Some(core.uid),
            gid: Some(core.gid),
            resource_fork_size: None,
            aux_type: None,
            link_target_cnid: None,
            amiga_protection: None,
            amiga_comment: None,
            amiga_date: None,
        })
    }

    /// Dispatch dir2 directory listing. Routing:
    ///   - `Local`: shortform parser.
    ///   - `Extents` with `core.size <= dirblksize`: one directory block
    ///     (`XD2B` magic, single-block dir2).
    ///   - `Extents` with `core.size > dirblksize`: leaf or node directory.
    ///     We walk every file-block range in the inline extent map and
    ///     parse only those whose 32-bit magic is `XD2D` (data block) —
    ///     the leaf-1 / leaf-N / freespace / da-node blocks live at file
    ///     offsets >= `XFS_DIR2_LEAF_OFFSET` and so will fail the magic
    ///     check naturally. The hash btree is not consulted; for browse
    ///     we only need to enumerate entries.
    ///   - `Btree`: a directory whose extent map alone has spilled into
    ///     a bmap btree. Deferred — IRIX disks don't produce these in
    ///     practice.
    fn list_dir2(
        &mut self,
        core: &XfsDinodeCore,
        inode_buf: &[u8],
    ) -> Result<Vec<dir2::Dir2Entry>, FilesystemError> {
        let dirblksize = self.sb.dirblksize() as u64;
        match core.format {
            DiFormat::Local => {
                let fork = self.data_fork(core, inode_buf);
                let (_parent, entries) = dir2::parse_shortform(fork, self.sb.has_ftype())?;
                Ok(entries)
            }
            DiFormat::Extents => {
                let fork = self.data_fork(core, inode_buf);
                let recs = self.decode_inline_extents(fork, core.nextents as usize)?;
                self.walk_dir2_data_blocks(&recs, dirblksize, core.size)
            }
            DiFormat::Btree => Err(FilesystemError::Unsupported(
                "XFS btree-format directory extent map (deferred)".into(),
            )),
            DiFormat::Other(v) => Err(FilesystemError::Parse(format!(
                "unknown XFS di_format {v} on directory inode {}",
                core.ino
            ))),
        }
    }

    /// Walk dir2 data-block address space. Each directory block is
    /// `dirblksize` bytes (one or more fsblocks). The first directory
    /// block carries `XD2B` magic if and only if the whole directory fits
    /// in it (single-block format); otherwise data blocks carry `XD2D`
    /// magic. Leaf / freespace / node blocks live above
    /// `XFS_DIR2_LEAF_OFFSET` in the file address space and either fall
    /// outside our extent walk's reachable file offsets (sparse) or fail
    /// the magic check (we treat unknown magics as "skip").
    fn walk_dir2_data_blocks(
        &mut self,
        extents: &[XfsBmbtIrec],
        dirblksize: u64,
        dir_size: u64,
    ) -> Result<Vec<dir2::Dir2Entry>, FilesystemError> {
        if extents.is_empty() {
            return Ok(Vec::new());
        }
        let bs = self.sb.blocksize as u64;
        let blocks_per_dir = (dirblksize / bs).max(1);

        // Build a sorted lookup from file fsblock → disk fsblock so we can
        // assemble directory blocks even when their fsblocks come from
        // different extents.
        let mut max_data_fb: u64 = 0;
        for e in extents {
            let end = e.startoff.saturating_add(e.blockcount);
            if end > max_data_fb {
                max_data_fb = end;
            }
        }
        // dir2 leaf/free address spaces start at 32 GiB / blocksize. We
        // cap the walk there so a malformed extent list with a leaf-space
        // entry doesn't make us read garbage.
        let leaf_first_fb = (1u64 << 32) / bs;
        let walk_end_fb = max_data_fb.min(leaf_first_fb);

        let has_ftype = self.sb.has_ftype();
        let mut block_buf = vec![0u8; dirblksize as usize];
        let mut out = Vec::new();
        let mut fb = 0u64;
        while fb < walk_end_fb {
            // Assemble one directory block from `blocks_per_dir` fsblocks.
            let mut got_all = true;
            for i in 0..blocks_per_dir {
                let want_fb = fb + i;
                let Some((disk_fb, unwritten)) = fb_to_disk(extents, want_fb) else {
                    got_all = false;
                    break;
                };
                let slot = &mut block_buf[(i * bs) as usize..((i + 1) * bs) as usize];
                if unwritten {
                    slot.fill(0);
                } else {
                    self.read_fsblock(disk_fb, slot)?;
                }
            }
            if got_all {
                let magic = if block_buf.len() >= 4 {
                    BigEndian::read_u32(&block_buf[0..4])
                } else {
                    0
                };
                match magic {
                    dir2::XFS_DIR2_BLOCK_MAGIC | dir2::XFS_DIR3_BLOCK_MAGIC => {
                        out.extend(dir2::parse_block(&block_buf, has_ftype)?);
                    }
                    dir2::XFS_DIR2_DATA_MAGIC | dir2::XFS_DIR3_DATA_MAGIC => {
                        out.extend(dir2::parse_data_block(&block_buf, has_ftype)?);
                    }
                    _ => {
                        // Unknown / leaf / freespace / node — skip silently.
                    }
                }
            }
            fb += blocks_per_dir;
        }
        // `dir_size` is informational; some empty trailing space is normal
        // and we don't need to clamp by it for browse.
        let _ = dir_size;
        Ok(out)
    }

    /// Dispatch dir1 directory listing — shortform when the inode is Local,
    /// otherwise walk every file block in the extent list, parsing each block
    /// whose `xfs_da_blkinfo_t.magic` is `0xFEEB` (leaf). Intermediate-node
    /// blocks (`0xFEBE`) are skipped — for browse we only need to enumerate
    /// entries, not perform hash-based lookups.
    fn list_dir1(
        &mut self,
        core: &XfsDinodeCore,
        inode_buf: &[u8],
    ) -> Result<Vec<dir2::Dir2Entry>, FilesystemError> {
        match core.format {
            DiFormat::Local => {
                let fork = self.data_fork(core, inode_buf);
                let (_parent, entries) = dir1::parse_shortform(fork)?;
                Ok(entries)
            }
            DiFormat::Extents => {
                let fork = self.data_fork(core, inode_buf);
                let recs = self.decode_inline_extents(fork, core.nextents as usize)?;
                let bs = self.sb.blocksize as u64;
                let mut block = vec![0u8; bs as usize];
                let mut out: Vec<dir2::Dir2Entry> = Vec::new();
                for rec in recs {
                    if rec.unwritten {
                        continue;
                    }
                    for i in 0..rec.blockcount {
                        self.read_fsblock(rec.startblock + i, &mut block)?;
                        match dir1::block_magic(&block) {
                            Some(dir1::XFS_DIR1_LEAF_MAGIC) => {
                                let leaf_entries = dir1::parse_leaf_block(&block)?;
                                out.extend(leaf_entries);
                            }
                            Some(dir1::XFS_DA_NODE_MAGIC) => {
                                // Intermediate node — skip; browse iterates
                                // every block and picks up the leaves directly.
                            }
                            _ => {
                                // Unknown magic — skip rather than fail; the
                                // directory may have a freespace block we
                                // don't recognise.
                            }
                        }
                    }
                }
                Ok(out)
            }
            DiFormat::Btree => Err(FilesystemError::Unsupported(
                "XFS dir1 directory with btree-format extents (Step 7)".into(),
            )),
            DiFormat::Other(v) => Err(FilesystemError::Parse(format!(
                "unknown XFS di_format {v} on directory inode {}",
                core.ino
            ))),
        }
    }

    fn read_symlink_target(
        &mut self,
        core: &XfsDinodeCore,
        inode_buf: &[u8],
    ) -> Result<String, FilesystemError> {
        match core.format {
            DiFormat::Local => {
                let fork = self.data_fork(core, inode_buf);
                Ok(symlink::decode_local_target(fork, core.size))
            }
            DiFormat::Extents => {
                let fork = self.data_fork(core, inode_buf);
                let recs = self.decode_inline_extents(fork, core.nextents as usize)?;
                let Some(first) = recs.first() else {
                    return Ok(String::new());
                };
                let bs = self.sb.blocksize as usize;
                let mut buf = vec![0u8; bs];
                self.read_fsblock(first.startblock, &mut buf)?;
                Ok(symlink::decode_extent_target(&buf, core.size))
            }
            other => Err(FilesystemError::Unsupported(format!(
                "XFS symlink di_format {other:?} not supported"
            ))),
        }
    }

    /// Read the file's data into a `Vec<u8>` capped at `max_bytes`. The
    /// extent list (`recs`) must be sorted by file offset and may originate
    /// from either the inline extent fork (`DiFormat::Extents`) or the
    /// bmap btree (`DiFormat::Btree`).
    fn read_extents_file(
        &mut self,
        core: &XfsDinodeCore,
        recs: &[XfsBmbtIrec],
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let size = core.size as usize;
        let want = size.min(max_bytes);
        let mut out = Vec::with_capacity(want);
        if want == 0 {
            return Ok(out);
        }
        let bs = self.sb.blocksize as u64;
        let mut produced: u64 = 0;
        let want64 = want as u64;
        let mut block = vec![0u8; bs as usize];
        for rec in recs.iter().copied() {
            // Zero-fill any hole between the running file offset and this extent.
            let extent_byte_off = rec.startoff * bs;
            if extent_byte_off > produced {
                let hole = (extent_byte_off - produced).min(want64 - produced);
                out.resize(out.len() + hole as usize, 0);
                produced += hole;
                if produced >= want64 {
                    break;
                }
            }
            for i in 0..rec.blockcount {
                if produced >= want64 {
                    break;
                }
                if rec.unwritten {
                    let remaining = want64 - produced;
                    let take = remaining.min(bs);
                    out.resize(out.len() + take as usize, 0);
                    produced += take;
                } else {
                    self.read_fsblock(rec.startblock + i, &mut block)?;
                    let remaining = want64 - produced;
                    let take = remaining.min(bs) as usize;
                    out.extend_from_slice(&block[..take]);
                    produced += take as u64;
                }
            }
            if produced >= want64 {
                break;
            }
        }
        // Final tail of holes past the last extent up to di_size (if any).
        if (out.len() as u64) < want64 {
            out.resize(want, 0);
        }
        Ok(out)
    }

    /// Streaming variant of `read_extents_file`. Emits the file bytes via
    /// `writer`, 1 MiB at a time, and returns total bytes written.
    fn stream_extents_file(
        &mut self,
        core: &XfsDinodeCore,
        recs: &[XfsBmbtIrec],
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let size = core.size;
        if size == 0 {
            return Ok(0);
        }
        let bs = self.sb.blocksize as u64;
        let mut written: u64 = 0;
        let mut block = vec![0u8; bs as usize];
        let zero_chunk = vec![0u8; STREAM_CHUNK];
        for rec in recs.iter().copied() {
            let extent_byte_off = rec.startoff * bs;
            if extent_byte_off > written {
                let hole = (extent_byte_off - written).min(size - written);
                written += emit_zeros(writer, &zero_chunk, hole)?;
                if written >= size {
                    return Ok(written);
                }
            }
            for i in 0..rec.blockcount {
                if written >= size {
                    return Ok(written);
                }
                let remaining_file = size - written;
                let take = remaining_file.min(bs) as usize;
                if rec.unwritten {
                    written += emit_zeros(writer, &zero_chunk, take as u64)?;
                } else {
                    self.read_fsblock(rec.startblock + i, &mut block)?;
                    writer.write_all(&block[..take])?;
                    written += take as u64;
                }
            }
        }
        if written < size {
            written += emit_zeros(writer, &zero_chunk, size - written)?;
        }
        Ok(written)
    }
}

impl<R: Read + Seek + Send> Filesystem for XfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let rootino = self.sb.rootino;
        let (core, _) = self.read_inode_buf(rootino)?;
        if !core.is_dir() {
            return Err(FilesystemError::InvalidData(format!(
                "XFS root inode {rootino} is not a directory (mode=0o{:o})",
                core.mode
            )));
        }
        Ok(FileEntry {
            name: "/".into(),
            path: "/".into(),
            entry_type: EntryType::Directory,
            size: 0,
            location: rootino,
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target: None,
            special_type: None,
            mode: Some(core.mode as u32),
            uid: Some(core.uid),
            gid: Some(core.gid),
            resource_fork_size: None,
            aux_type: None,
            link_target_cnid: None,
            amiga_protection: None,
            amiga_comment: None,
            amiga_date: None,
        })
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let (core, inode_buf) = self.read_inode_buf(entry.location)?;
        if !core.is_dir() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let raw_entries: Vec<dir2::Dir2Entry> = match self.dir_format {
            DirFormat::Dir2 => self.list_dir2(&core, &inode_buf)?,
            DirFormat::Dir1 => self.list_dir1(&core, &inode_buf)?,
        };
        let parent_path = entry.path.clone();
        let mut out = Vec::with_capacity(raw_entries.len());
        for de in raw_entries {
            match self.child_entry(&parent_path, de.name, de.inumber) {
                Ok(child) => out.push(child),
                Err(_) => {
                    // Damaged metadata: surface the entry as a plain file with
                    // size 0 so the user still sees the name.
                    let path = if parent_path == "/" {
                        format!("/<unreadable inode {}>", de.inumber)
                    } else {
                        format!("{parent_path}/<unreadable inode {}>", de.inumber)
                    };
                    out.push(FileEntry::new_file(
                        format!("<unreadable inode {}>", de.inumber),
                        path,
                        0,
                        de.inumber,
                    ));
                }
            }
        }
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "XFS read_file on directory: {}",
                entry.path
            )));
        }
        let (core, inode_buf) = self.read_inode_buf(entry.location)?;
        match core.format {
            DiFormat::Local => {
                let fork = self.data_fork(&core, &inode_buf);
                let want = (core.size as usize).min(max_bytes);
                let take = want.min(fork.len());
                Ok(fork[..take].to_vec())
            }
            DiFormat::Extents | DiFormat::Btree => {
                let recs = self.decode_data_extents(&core, &inode_buf)?;
                self.read_extents_file(&core, &recs, max_bytes)
            }
            DiFormat::Other(v) => Err(FilesystemError::Parse(format!(
                "unknown XFS di_format {v} on inode {}",
                core.ino
            ))),
        }
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "XFS write_file_to on directory: {}",
                entry.path
            )));
        }
        let (core, inode_buf) = self.read_inode_buf(entry.location)?;
        match core.format {
            DiFormat::Local => {
                let fork = self.data_fork(&core, &inode_buf);
                let take = (core.size as usize).min(fork.len());
                writer.write_all(&fork[..take])?;
                Ok(take as u64)
            }
            DiFormat::Extents | DiFormat::Btree => {
                let recs = self.decode_data_extents(&core, &inode_buf)?;
                self.stream_extents_file(&core, &recs, writer)
            }
            DiFormat::Other(v) => Err(FilesystemError::Parse(format!(
                "unknown XFS di_format {v} on inode {}",
                core.ino
            ))),
        }
    }

    fn volume_label(&self) -> Option<&str> {
        if self.label.is_empty() {
            None
        } else {
            Some(&self.label)
        }
    }

    fn fs_type(&self) -> &str {
        self.fs_type_name
    }

    fn total_size(&self) -> u64 {
        self.sb.dblocks * self.sb.blocksize as u64
    }

    fn used_size(&self) -> u64 {
        self.total_size()
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.total_size())
    }

    fn fsck(&mut self) -> Option<Result<crate::fs::fsck::FsckResult, FilesystemError>> {
        Some(self.run_fsck())
    }
}

/// Look up an extent record covering file fsblock `fb`. Returns
/// `(disk_fsblock, unwritten)` or `None` if `fb` falls in a hole.
fn fb_to_disk(extents: &[XfsBmbtIrec], fb: u64) -> Option<(u64, bool)> {
    // Records are sorted by startoff; linear scan is fine for the small
    // extent counts directories tend to have. A binary search is the
    // obvious upgrade if this ever shows up in a profile.
    for e in extents {
        if fb >= e.startoff && fb < e.startoff + e.blockcount {
            return Some((e.startblock + (fb - e.startoff), e.unwritten));
        }
    }
    None
}

/// Parse a bmap-btree root header out of an inode's data fork (`di_format
/// == Btree`). Returns `(root_level, first_child)`. `forklen` is the fork
/// length in bytes (used to compute how many key+ptr slots the root
/// reserves).
fn parse_bmbt_root(fork: &[u8], forklen: usize) -> Result<(u16, u64), FilesystemError> {
    if fork.len() < 4 {
        return Err(FilesystemError::Parse(format!(
            "XFS bmap btree root fork too small: {} bytes",
            fork.len()
        )));
    }
    let level = BigEndian::read_u16(&fork[0..2]);
    let numrecs = BigEndian::read_u16(&fork[2..4]) as usize;
    if level == 0 {
        return Err(FilesystemError::Parse(
            "XFS bmap btree root claims level 0 — should be inline extents".into(),
        ));
    }
    if numrecs == 0 {
        return Err(FilesystemError::Parse(
            "XFS bmap btree root has zero records".into(),
        ));
    }
    // `xfs_bmdr_maxrecs(forklen, leaf=false) = (forklen - 4) / 16` — the
    // root reserves space for `maxrecs` (key, ptr) slots even though only
    // `numrecs` are populated. Pointers start after `maxrecs` keys.
    let maxrecs = (forklen.saturating_sub(4)) / 16;
    if numrecs > maxrecs {
        return Err(FilesystemError::Parse(format!(
            "XFS bmap btree root numrecs {numrecs} > maxrecs {maxrecs}"
        )));
    }
    let ptrs_off = 4 + maxrecs * 8;
    if ptrs_off + 8 > fork.len() {
        return Err(FilesystemError::Parse(format!(
            "XFS bmap btree root ptr region truncated: ptrs_off={ptrs_off}, fork={}",
            fork.len()
        )));
    }
    let first_child = BigEndian::read_u64(&fork[ptrs_off..ptrs_off + 8]);
    Ok((level, first_child))
}

/// Verify a bmap-btree disk block carries the expected magic and (if
/// supplied) level. Header layout is the long-form `xfs_btree_block`:
///   magic(4) + level(2) + numrecs(2) + leftsib(8) + rightsib(8).
fn check_bmbt_header(
    block: &[u8],
    expected_level: Option<u16>,
    is_v5: bool,
) -> Result<(), FilesystemError> {
    if block.len() < XFS_BTREE_LBLOCK_LEN {
        return Err(FilesystemError::Parse(format!(
            "XFS bmap btree block too small: {} bytes",
            block.len()
        )));
    }
    let magic = BigEndian::read_u32(&block[0..4]);
    let want = if is_v5 {
        XFS_BMAP_CRC_MAGIC
    } else {
        XFS_BMAP_MAGIC
    };
    if magic != want {
        return Err(FilesystemError::Parse(format!(
            "bad XFS bmap btree magic: 0x{magic:08X} (expected 0x{want:08X})"
        )));
    }
    if let Some(want) = expected_level {
        let got = BigEndian::read_u16(&block[4..6]);
        if got != want {
            return Err(FilesystemError::Parse(format!(
                "XFS bmap btree level mismatch: expected {want}, got {got}"
            )));
        }
    }
    Ok(())
}

fn emit_zeros(
    writer: &mut dyn Write,
    zero_chunk: &[u8],
    count: u64,
) -> Result<u64, FilesystemError> {
    let mut remaining = count;
    while remaining > 0 {
        let take = remaining.min(zero_chunk.len() as u64) as usize;
        writer.write_all(&zero_chunk[..take])?;
        remaining -= take as u64;
    }
    Ok(count)
}

/// Sector-aligned read helper. Reads `len` bytes starting at byte offset
/// `byte_off` into `out`. Both `byte_off` and `len` must be 512-byte aligned.
/// A short read surfaces as `Io(UnexpectedEof)`.
fn read_at_aligned<R: Read + Seek>(
    reader: &mut R,
    byte_off: u64,
    len: u64,
    out: &mut [u8],
) -> Result<(), FilesystemError> {
    debug_assert!(byte_off.is_multiple_of(SECTOR));
    debug_assert!(len.is_multiple_of(SECTOR));
    let len = len as usize;
    if out.len() < len {
        return Err(FilesystemError::InvalidData(format!(
            "XFS read_at_aligned: output buffer {} < {len}",
            out.len()
        )));
    }
    reader.seek(SeekFrom::Start(byte_off))?;
    let mut filled = 0;
    while filled < len {
        let n = reader.read(&mut out[filled..len])?;
        if n == 0 {
            return Err(FilesystemError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("XFS short read at byte {byte_off}: got {filled} of {len}"),
            )));
        }
        filled += n;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{BigEndian, ByteOrder};
    use std::io::Cursor;

    /// Decompress `name` from tests/fixtures/sgi/ once and hand out a shared
    /// `&'static [u8]`. The dir2 fixture inflates to 512 MiB; returning a shared
    /// slice (rather than a fresh Vec per call) keeps the cargo-test thread pool
    /// from holding N independent copies at once, which would exhaust a 32-bit
    /// address space (the i686 Windows CI leg). Tests that need to mutate the
    /// image take an owned `.to_vec()` copy.
    fn load_sgi_fixture(name: &str) -> &'static [u8] {
        use std::sync::{Mutex, OnceLock};
        static CACHE: OnceLock<Mutex<std::collections::HashMap<String, &'static [u8]>>> =
            OnceLock::new();
        let cache = CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
        let mut map = cache.lock().unwrap();
        if let Some(bytes) = map.get(name) {
            return bytes;
        }
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/sgi")
            .join(name);
        let compressed = std::fs::read(&path).expect("fixture present");
        let mut decoder =
            zstd::stream::read::Decoder::new(Cursor::new(compressed)).expect("zstd decoder");
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).expect("decompress");
        // Leak the decompressed image so the slice lives for the whole test run;
        // it's shared by every test that asks for this fixture.
        let leaked: &'static [u8] = Vec::leak(out);
        map.insert(name.to_string(), leaked);
        leaked
    }

    fn load_fixture() -> &'static [u8] {
        load_sgi_fixture("xfs_v2_dir2_small.img.zst")
    }

    #[test]
    fn parses_dir2_fixture_superblock() {
        let img = load_fixture();
        let fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        assert_eq!(fs.sb.blocksize, 4096);
        assert_eq!(fs.sb.dblocks, 131_072);
        assert_eq!(fs.sb.rootino, 128);
        assert_eq!(fs.sb.agblocks, 65_536);
        assert_eq!(fs.sb.agcount, 2);
        assert_eq!(fs.sb.versionnum, 0xb4a4);
        assert_eq!(fs.sb.sectsize, 512);
        assert_eq!(fs.sb.inodesize, 256);
        assert_eq!(fs.sb.inopblock, 16);
        assert_eq!(fs.sb.blocklog, 12);
        assert_eq!(fs.sb.inopblog, 4);
        assert_eq!(fs.sb.agblklog, 16);
        assert_eq!(fs.dir_format, DirFormat::Dir2);
        assert_eq!(fs.fs_type(), "XFS v2");
        assert_eq!(fs.volume_label(), Some("RUSTYTEST"));
        assert_eq!(fs.total_size(), 131_072u64 * 4096);
    }

    #[test]
    fn fsck_clean_v4_fixture_reports_no_errors() {
        let img = load_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let res = fs.run_fsck().expect("fsck runs");
        assert!(
            res.errors.is_empty(),
            "clean fixture should have no errors, got: {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
        assert!(!res.repairable);
        // Phase 3 ran and found no orphans on a healthy volume.
        assert!(res.orphaned_entries.is_empty());
        assert!(res.stats.extra.iter().any(|(k, _)| k == "Reachable inodes"));
        // Stats should report the two allocation groups.
        let ags = res
            .stats
            .extra
            .iter()
            .find(|(k, _)| k == "Allocation groups")
            .map(|(_, v)| v.as_str());
        assert_eq!(ags, Some("2"));
    }

    #[test]
    fn fsck_phase2_enumerates_inodes_and_claims_blocks() {
        // Proves the Phase 2 inode-btree walk + block-ownership map ran end
        // to end on real mkfs output: the fixture has hello.txt + readme.txt
        // + a symlink in root and a nested file under subdir/, so we expect
        // at least 2 files and 2 directories scanned and some claimed blocks.
        let img = load_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let res = fs.run_fsck().expect("fsck runs");
        assert!(
            res.errors.is_empty(),
            "{:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
        assert!(
            res.stats.files_checked >= 2,
            "expected >=2 files, got {}",
            res.stats.files_checked
        );
        assert!(
            res.stats.directories_checked >= 2,
            "expected >=2 dirs (root + subdir), got {}",
            res.stats.directories_checked
        );
        let scanned = res
            .stats
            .extra
            .iter()
            .any(|(k, v)| k == "Blocks scanned" && v != "0 claimed");
        assert!(scanned, "expected non-zero claimed blocks");
    }

    #[test]
    fn fsck_tolerates_null_rootino_in_secondary_superblock() {
        // Real mkfs.xfs leaves rootino/rbmino/rsumino as NULLFSINO in the
        // secondary superblocks (verified via `xfs_db -c 'sb 2'`). The
        // verifier must NOT flag that as a ReplicaSbMismatch — only geometry
        // fields are replicated. Set AG 1's secondary rootino to NULLFSINO
        // (offset 56..64) and confirm no mismatch is reported.
        let mut img = load_fixture().to_vec();
        let ag1 = 65_536usize * 4096;
        img[ag1 + 56..ag1 + 64].copy_from_slice(&u64::MAX.to_be_bytes());
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let res = fs.run_fsck().expect("fsck runs");
        assert!(
            !res.errors.iter().any(|e| e.code == "ReplicaSbMismatch"),
            "null rootino in a secondary must not be a mismatch, got: {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn fsck_detects_unaccounted_blocks() {
        // Shrink AG 0's bnobt first free record's blockcount so the freed
        // blocks vanish from the free list while staying unclaimed -> leaked.
        // (This also trips AgfFreeblksMismatch; we only assert the leak.)
        let mut img = load_fixture().to_vec();
        let bno_root = u32::from_be_bytes(img[512 + 16..512 + 20].try_into().unwrap()) as usize;
        let rec0 = bno_root * 4096 + 16;
        let orig = u32::from_be_bytes(img[rec0 + 4..rec0 + 8].try_into().unwrap());
        assert!(orig > 10, "fixture first free extent should be sizable");
        img[rec0 + 4..rec0 + 8].copy_from_slice(&(orig - 10).to_be_bytes());
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let res = fs.run_fsck().expect("fsck runs");
        assert!(
            res.errors.iter().any(|e| e.code == "UnaccountedBlocks"),
            "expected UnaccountedBlocks, got: {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn fsck_detects_free_block_also_claimed_by_inode() {
        // Patch AG 0's bnobt so a free extent overlaps the root inode chunk
        // (agbno 8..12, marked S_INO). Read bno_root from the AGF (byte
        // 512+16), find the leaf's first record (block*4096 + 16 header), and
        // rewrite it to (startblock=8, blockcount=4). Expect FreeBlockClaimed.
        let mut img = load_fixture().to_vec();
        let bno_root = u32::from_be_bytes(img[512 + 16..512 + 20].try_into().unwrap()) as usize;
        let rec0 = bno_root * 4096 + 16; // v4 short-form btree header = 16 bytes
        img[rec0..rec0 + 4].copy_from_slice(&8u32.to_be_bytes()); // startblock
        img[rec0 + 4..rec0 + 8].copy_from_slice(&4u32.to_be_bytes()); // blockcount
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let res = fs.run_fsck().expect("fsck runs");
        assert!(
            res.errors.iter().any(|e| e.code == "FreeBlockClaimed"),
            "expected FreeBlockClaimed, got: {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn fsck_detects_corrupted_ag_superblock_replica() {
        // Corrupt AG 1's superblock replica: flip its dblocks field so it
        // disagrees with the primary. AG 1 starts at agblocks*blocksize =
        // 65536 * 4096. The replica's dblocks is at offset 8..16.
        let mut img = load_fixture().to_vec();
        let ag1 = 65_536usize * 4096;
        img[ag1 + 8..ag1 + 16].copy_from_slice(&0xDEADu64.to_be_bytes());
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let res = fs.run_fsck().expect("fsck runs");
        assert!(
            res.errors.iter().any(|e| e.code == "ReplicaSbMismatch"),
            "expected ReplicaSbMismatch, got: {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
        // A replica mismatch is repairable (R4 rewrites it from primary).
        assert!(res.repairable);
    }

    #[test]
    fn fsck_detects_agf_freeblks_mismatch() {
        // Corrupt AG 0's AGF freeblks summary so it disagrees with the
        // free-space btree walk. AGF is sector 1 (byte 512); freeblks is at
        // AGF offset 52..56 (see ag.rs). Expect AgfFreeblksMismatch.
        let mut img = load_fixture().to_vec();
        let agf_freeblks = 512 + 52;
        img[agf_freeblks..agf_freeblks + 4].copy_from_slice(&0u32.to_be_bytes());
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let res = fs.run_fsck().expect("fsck runs");
        assert!(
            res.errors.iter().any(|e| e.code == "AgfFreeblksMismatch"),
            "expected AgfFreeblksMismatch, got: {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn repair_r4b_rewrites_agf_agi_summaries() {
        // R4b round-trip: corrupt AG 0's AGF freeblks (byte 512+52) and AGI
        // count/freecount (byte 1024+16, 1024+28), run repair(), confirm the
        // verifier reports clean. Validates the summary-rewrite path.
        use crate::fs::filesystem::EditableFilesystem;
        let mut img = load_fixture().to_vec();
        img[512 + 52..512 + 56].copy_from_slice(&0u32.to_be_bytes()); // AGF freeblks
        img[1024 + 16..1024 + 20].copy_from_slice(&111u32.to_be_bytes()); // AGI count
        img[1024 + 28..1024 + 32].copy_from_slice(&111u32.to_be_bytes()); // AGI freecount

        // Pre-repair: verifier flags the freeblks mismatch.
        {
            let mut fs = XfsFilesystem::open(Cursor::new(img.as_slice()), 0).expect("open");
            let res = fs.run_fsck().expect("fsck");
            assert!(res.errors.iter().any(|e| e.code == "AgfFreeblksMismatch"));
        }

        let mut cursor = Cursor::new(img);
        let report = {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            fs.repair().expect("repair runs")
        };
        assert_eq!(report.fixes_failed.len(), 0, "{:?}", report.fixes_failed);
        assert!(
            report
                .fixes_applied
                .iter()
                .any(|f| f.contains("AGF freeblks")),
            "expected AGF freeblks fix, got: {:?}",
            report.fixes_applied
        );
        assert!(
            report.fixes_applied.iter().any(|f| f.contains("AGI count")),
            "expected AGI count fix, got: {:?}",
            report.fixes_applied
        );

        let repaired = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(repaired), 0).expect("reopen");
        let res = fs.run_fsck().expect("fsck after repair");
        assert!(
            res.errors.is_empty(),
            "expected clean after repair, got: {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn repair_r4_rewrites_corrupted_secondary_superblock() {
        // R4 round-trip: corrupt AG 1's secondary superblock geometry
        // (agblocks @ 84..88 and dblocks @ 8..16), run repair(), then confirm
        // the verifier reports clean. Proves the replica-rewrite end to end
        // without any btree writes.
        use crate::fs::filesystem::EditableFilesystem;
        let mut img = load_fixture().to_vec();
        let ag1 = 65_536usize * 4096;
        img[ag1 + 8..ag1 + 16].copy_from_slice(&0xDEADu64.to_be_bytes());
        img[ag1 + 84..ag1 + 88].copy_from_slice(&0x1234u32.to_be_bytes());

        // Pre-repair: verifier flags the mismatch.
        {
            let mut fs = XfsFilesystem::open(Cursor::new(img.as_slice()), 0).expect("open");
            let res = fs.run_fsck().expect("fsck");
            assert!(res.errors.iter().any(|e| e.code == "ReplicaSbMismatch"));
        }

        // Repair in place (Cursor<Vec<u8>> is Read + Write + Seek).
        let mut cursor = Cursor::new(img);
        let report = {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            fs.repair().expect("repair runs")
        };
        assert_eq!(report.fixes_failed.len(), 0, "{:?}", report.fixes_failed);
        assert!(
            report.fixes_applied.iter().any(|f| f.contains("AG 1")),
            "expected an AG 1 fix, got: {:?}",
            report.fixes_applied
        );

        // Post-repair: verifier is clean.
        let repaired = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(repaired), 0).expect("reopen");
        let res = fs.run_fsck().expect("fsck after repair");
        assert!(
            res.errors.is_empty(),
            "expected clean after repair, got: {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn repair_r5_recomputes_inode_nblocks() {
        // R5 round-trip: hello.txt (inode 131) is an inline-extents regular
        // file. Corrupt its di_nblocks (core offset 64) to a bogus value, run
        // repair(), and confirm R5 recomputes it back to the true block count
        // (the extent-list sum) — and that the verifier stays clean.
        use crate::fs::filesystem::EditableFilesystem;
        let mut img = load_fixture().to_vec();

        // Locate inode 131 and snapshot its true nblocks before corrupting.
        let (ino_byte, true_nblocks) = {
            let mut fs = XfsFilesystem::open(Cursor::new(img.as_slice()), 0).expect("open");
            let sb = fs.superblock().clone();
            let byte = inode_byte_offset(
                131,
                sb.agblocks,
                sb.agblklog,
                sb.inopblog,
                sb.blocksize,
                sb.inodesize,
            ) as usize;
            let core = fs.read_inode(131).expect("read inode 131");
            assert!(core.is_regular(), "inode 131 should be a regular file");
            assert!(core.nblocks > 0, "expected a real block count");
            (byte, core.nblocks)
        };

        // Corrupt di_nblocks (u64 at core offset 64).
        img[ino_byte + 64..ino_byte + 72].copy_from_slice(&9999u64.to_be_bytes());

        let mut cursor = Cursor::new(img);
        let report = {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            fs.repair().expect("repair runs")
        };
        assert_eq!(report.fixes_failed.len(), 0, "{:?}", report.fixes_failed);
        assert!(
            report
                .fixes_applied
                .iter()
                .any(|f| f.contains("di_nblocks")),
            "expected an inode-core fix, got: {:?}",
            report.fixes_applied
        );

        let repaired = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(repaired), 0).expect("reopen");
        let core = fs.read_inode(131).expect("reread inode 131");
        assert_eq!(core.nblocks, true_nblocks, "di_nblocks not restored");
        let res = fs.run_fsck().expect("fsck after repair");
        assert!(
            res.errors.is_empty(),
            "expected clean after repair, got: {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn repair_r7_reconnects_orphan_into_lost_found() {
        // Create a file, then orphan it by dropping the root's last short-form
        // entry (decrement count + di_size in the inode bytes) while leaving the
        // inode allocated. repair() should reconnect it into /lost+found and the
        // verifier should be clean.
        use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem};
        use std::io::Cursor as IoCursor;

        let img = load_fixture().to_vec();
        let mut cursor = Cursor::new(img);
        // Snapshot the pre-create root sf size + entry count, then add a file.
        let (root_byte, root_size0, orphan_ino) = {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            let sb = fs.superblock().clone();
            let byte = inode_byte_offset(
                128,
                sb.agblocks,
                sb.agblklog,
                sb.inopblog,
                sb.blocksize,
                sb.inodesize,
            ) as usize;
            let size0 = fs.read_inode(128).expect("root").size;
            let root = fs.root().expect("root");
            let mut data = IoCursor::new(vec![7u8; 2000]);
            let e = fs
                .create_file(
                    &root,
                    "orphanme",
                    &mut data,
                    2000,
                    &CreateFileOptions::default(),
                )
                .expect("create");
            fs.sync_metadata().expect("sync");
            (byte, size0, e.location)
        };
        let count0 = 4u8; // fixture root: hello.txt, readme.txt, subdir, link

        // Orphan it: drop the just-added last entry by restoring the original
        // count byte (fork offset 100) and di_size (core offset 56).
        let mut img = cursor.into_inner();
        img[root_byte + 100] = count0;
        img[root_byte + 56..root_byte + 64].copy_from_slice(&root_size0.to_be_bytes());

        // Pre-repair: the verifier flags the orphan.
        {
            let mut fs = XfsFilesystem::open(Cursor::new(img.as_slice()), 0).expect("open");
            let res = fs.run_fsck().expect("fsck");
            assert!(
                res.errors.iter().any(|e| e.code == "OrphanInode"),
                "expected OrphanInode, got {:?}",
                res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
            );
        }

        let mut cursor = Cursor::new(img);
        let report = {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            fs.repair().expect("repair")
        };
        assert!(
            report
                .fixes_applied
                .iter()
                .any(|f| f.contains("lost+found")),
            "expected a reconnect fix, got {:?}",
            report.fixes_applied
        );

        let repaired = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(repaired), 0).expect("reopen");
        let root = fs.root().expect("root");
        let lf = fs
            .list_directory(&root)
            .expect("list")
            .into_iter()
            .find(|e| e.name == "lost+found")
            .expect("lost+found created");
        assert!(lf.is_directory());
        let reconnected: Vec<u64> = fs
            .list_directory(&lf)
            .expect("list lf")
            .into_iter()
            .map(|e| e.location)
            .collect();
        assert!(reconnected.contains(&orphan_ino), "orphan not reconnected");
        let res = fs.run_fsck().expect("fsck after");
        assert!(
            res.errors.is_empty(),
            "expected clean, got {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn shortform_to_block_dir_conversion() {
        // Add enough files to overflow the inline root directory, forcing a
        // short-form -> single-block conversion (exercises the dir2 block
        // builder + hash leaf index on the ftype-enabled fixture). All entries
        // must list, the originals survive, a sample reads back, and the volume
        // stays clean.
        use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem};
        use std::io::Cursor as IoCursor;
        let sample: Vec<u8> = (0..1234u32).map(|i| (i % 251) as u8).collect();

        let img = load_fixture().to_vec();
        let mut cursor = Cursor::new(img);
        {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            let root = fs.root().expect("root");
            for i in 0..20 {
                let mut data = IoCursor::new(sample.clone());
                fs.create_file(
                    &root,
                    &format!("added_{i:02}.dat"),
                    &mut data,
                    sample.len() as u64,
                    &CreateFileOptions::default(),
                )
                .unwrap_or_else(|e| panic!("create_file {i}: {e}"));
            }
            fs.sync_metadata().expect("sync");
        }

        let repaired = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(repaired), 0).expect("reopen");
        let root = fs.root().expect("root");
        // Root must now be a block (extents) directory, not short-form.
        assert_ne!(
            fs.read_inode(root.location).expect("root core").format,
            DiFormat::Local,
            "root should have converted out of short-form"
        );
        let entries = fs.list_directory(&root).expect("list");
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        for i in 0..20 {
            let want = format!("added_{i:02}.dat");
            assert!(
                names.contains(&want.as_str()),
                "missing {want} in {names:?}"
            );
        }
        // Originals survive.
        assert!(names.contains(&"hello.txt"), "original lost: {names:?}");
        // A sample reads back intact.
        let e = entries.iter().find(|e| e.name == "added_07.dat").unwrap();
        let got = fs.read_file(e, sample.len() + 4096).expect("read");
        assert_eq!(&got[..sample.len()], &sample[..], "data mismatch");
        let res = fs.run_fsck().expect("fsck");
        assert!(
            res.errors.is_empty(),
            "expected clean, got {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn delete_entry_frees_inode_and_blocks() {
        // Create a file + an empty dir, then delete both; the volume returns to
        // a clean state with the entries gone and free space recovered.
        use crate::fs::filesystem::{
            CreateDirectoryOptions, CreateFileOptions, EditableFilesystem,
        };
        use std::io::Cursor as IoCursor;
        let payload: Vec<u8> = (0..6000u32).map(|i| (i % 251) as u8).collect();

        let img = load_fixture().to_vec();
        let mut cursor = Cursor::new(img);
        let free_before;
        {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            free_before = fs.free_space().expect("free");
            let root = fs.root().expect("root");
            let mut data = IoCursor::new(payload.clone());
            fs.create_file(
                &root,
                "tmp.bin",
                &mut data,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create_file");
            fs.create_directory(&root, "tmpdir", &CreateDirectoryOptions::default())
                .expect("mkdir");
            fs.sync_metadata().expect("sync");
        }
        // Reopen and delete both.
        {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("reopen editable");
            let root = fs.root().expect("root");
            let entries = fs.list_directory(&root).expect("list");
            for nm in ["tmp.bin", "tmpdir"] {
                let e = entries.iter().find(|e| e.name == nm).expect("entry");
                fs.delete_entry(&root, e).expect("delete");
            }
            fs.sync_metadata().expect("sync");
        }

        let repaired = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(repaired), 0).expect("reopen");
        let root = fs.root().expect("root");
        let names: Vec<String> = fs
            .list_directory(&root)
            .expect("list")
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(
            !names.contains(&"tmp.bin".to_string()),
            "file not removed: {names:?}"
        );
        assert!(
            !names.contains(&"tmpdir".to_string()),
            "dir not removed: {names:?}"
        );
        // The original fixture entries survive.
        assert!(
            names.contains(&"hello.txt".to_string()),
            "sibling lost: {names:?}"
        );
        // Free space is fully reclaimed (delete is the inverse of create).
        let free_after = fs.free_space().expect("free");
        assert_eq!(free_after, free_before, "free space not fully reclaimed");
        let res = fs.run_fsck().expect("fsck");
        assert!(
            res.errors.is_empty(),
            "expected clean, got {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn block_dir_recompacts_to_shortform_on_shrink() {
        // §2.1 hole (B): grow root to block form by adding 20 files, then
        // delete them all. The block_remove_entry path is supposed to detect
        // that the surviving (original) entries fit the inode literal area
        // and re-compact back to short-form — di_format goes back to Local,
        // di_nblocks/nextents back to 0, and the directory's data block is
        // returned to free space. Originals survive, free space is fully
        // reclaimed, the volume stays fsck-clean.
        use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem};
        use std::io::Cursor as IoCursor;

        let img = load_fixture().to_vec();
        let mut cursor = Cursor::new(img);

        let (free_before, originals);
        {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            free_before = fs.free_space().expect("free");
            let root = fs.root().expect("root");
            originals = fs
                .list_directory(&root)
                .expect("list")
                .into_iter()
                .map(|e| e.name)
                .collect::<Vec<_>>();
            assert_eq!(
                fs.read_inode(root.location).expect("root core").format,
                DiFormat::Local,
                "root starts short-form"
            );
            for i in 0..20 {
                let mut data = IoCursor::new(Vec::<u8>::new());
                fs.create_file(
                    &root,
                    &format!("added_{i:02}.dat"),
                    &mut data,
                    0,
                    &CreateFileOptions::default(),
                )
                .unwrap_or_else(|e| panic!("create_file {i}: {e}"));
            }
            fs.sync_metadata().expect("sync");
        }
        // After 20 adds the root should have overflowed to block form.
        {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("reopen editable");
            let root = fs.root().expect("root");
            assert_ne!(
                fs.read_inode(root.location).expect("root core").format,
                DiFormat::Local,
                "root should be block form after 20 adds"
            );

            // Delete all 20 added files. The last delete should recompact.
            let entries = fs.list_directory(&root).expect("list");
            for i in 0..20 {
                let want = format!("added_{i:02}.dat");
                let e = entries
                    .iter()
                    .find(|e| e.name == want)
                    .unwrap_or_else(|| panic!("missing {want}"));
                fs.delete_entry(&root, e).expect("delete");
            }
            fs.sync_metadata().expect("sync");
        }

        // The root must be back in short-form, with originals intact, free
        // space fully reclaimed, and the volume clean.
        let repaired = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(repaired), 0).expect("reopen");
        let root = fs.root().expect("root");
        assert_eq!(
            fs.read_inode(root.location).expect("root core").format,
            DiFormat::Local,
            "root should have re-compacted to short-form"
        );
        let core = fs.read_inode(root.location).expect("root core");
        assert_eq!(core.nblocks, 0, "di_nblocks should be 0 after recompaction");
        assert_eq!(
            core.nextents, 0,
            "di_nextents should be 0 after recompaction"
        );

        let entries = fs.list_directory(&root).expect("list");
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        for want in &originals {
            assert!(
                names.contains(&want.as_str()),
                "original lost: {want} in {names:?}",
            );
        }
        for i in 0..20 {
            let unwanted = format!("added_{i:02}.dat");
            assert!(
                !names.contains(&unwanted.as_str()),
                "added file not removed: {unwanted}"
            );
        }
        let free_after = fs.free_space().expect("free");
        assert_eq!(
            free_after, free_before,
            "free space not fully reclaimed after add+delete cycle"
        );
        let res = fs.run_fsck().expect("fsck");
        assert!(
            res.errors.is_empty(),
            "expected clean after re-compaction, got: {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn dir_overflow_converts_block_to_leaf_form() {
        // §2.1 hole (C): when a single-block directory would overflow on the
        // next insert, convert it to leaf form — two XD2D data blocks plus an
        // XD2F leaf1 index block at file offset XFS_DIR2_LEAF_OFFSET. Adding
        // 150 files (~32 bytes per entry × 150 = ~4800 bytes data + ~1200
        // bytes leaf = > 4 KiB) to the 4-entry fixture root forces the
        // conversion mid-sequence. After: di_format is still Extents (leaf
        // form lives in extents, not bmbt), the inode has 3 extents, all
        // entries enumerate, originals survive, the volume stays fsck-clean.
        use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem};
        use std::io::Cursor as IoCursor;

        let img = load_fixture().to_vec();
        let mut cursor = Cursor::new(img);

        let originals;
        {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            let root = fs.root().expect("root");
            originals = fs
                .list_directory(&root)
                .expect("list")
                .into_iter()
                .map(|e| e.name)
                .collect::<Vec<_>>();
            assert_eq!(
                fs.read_inode(root.location).expect("root core").format,
                DiFormat::Local,
                "root starts short-form"
            );
            // Add files one at a time until the inserter rejects with the
            // post-conversion "leaf-form insert not implemented" message
            // (v1 only supports block→leaf conversion, not further growth).
            // Stop there — the conversion succeeded and the directory is
            // now in leaf form holding everything written so far.
            let mut added = 0u32;
            for i in 0..150u32 {
                let mut data = IoCursor::new(Vec::<u8>::new());
                match fs.create_file(
                    &root,
                    &format!("f{i:04}.b"),
                    &mut data,
                    0,
                    &CreateFileOptions::default(),
                ) {
                    Ok(_) => added += 1,
                    Err(FilesystemError::Unsupported(msg))
                        if msg.contains("leaf/node-form directory") =>
                    {
                        break;
                    }
                    Err(e) => panic!("create_file {i}: {e}"),
                }
            }
            assert!(
                added > 100,
                "expected the block→leaf conversion to absorb at least 100 adds before \
                 hitting the (not-yet-implemented) leaf-form insert path; got {added}"
            );
            fs.sync_metadata().expect("sync");
        }

        let repaired = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(repaired), 0).expect("reopen");
        let root = fs.root().expect("root");
        let core = fs.read_inode(root.location).expect("root core");
        assert_eq!(
            core.format,
            DiFormat::Extents,
            "root should be in extents (leaf or block) form"
        );
        assert!(
            core.nextents >= 3,
            "leaf-form root should have at least 3 extents (data0, data1, leaf1); got {}",
            core.nextents
        );
        let entries = fs.list_directory(&root).expect("list root");
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        for want in &originals {
            assert!(
                names.contains(&want.as_str()),
                "original lost: {want} in {} names",
                names.len()
            );
        }
        // Every file that was successfully created must list back. Count
        // how many we expect by re-running the same admission loop in dry
        // form (any name f0000.b..f0149.b that's actually present counts).
        let added_present: usize = (0..150u32)
            .filter(|i| names.contains(&format!("f{i:04}.b").as_str()))
            .count();
        assert!(
            added_present > 100,
            "expected > 100 added files listable from leaf-form dir, got {added_present}"
        );
        // The volume must remain fsck-clean: data blocks + leaf1 block must
        // form a coherent leaf-form directory.
        let res = fs.run_fsck().expect("fsck");
        assert!(
            res.errors.is_empty(),
            "expected clean after leaf-form conversion, got: {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn create_file_writes_bmbt_format_when_extents_exceed_inline_cap() {
        // §2.1 hole (D): when an allocation produces more extent records than
        // fit the inline data fork (`max_inline = (inodesize - fork_offset) /
        // 16 = 9` for our 256-byte v4 inode), do_create_file must lay out the
        // data fork as a single-leaf bmap btree rather than failing. The
        // fixture's AG 0 is one big contiguous free extent (~64k blocks) — to
        // force 11 runs of 100 blocks each, this test forges AG 0's bnobt/cntbt
        // to a 12-extent fragmented free set via rebuild_ag_freespace. After
        // creation: di_format = Btree (3), reading the file produces the
        // original payload byte-for-byte, and walk_bmbt enumerates the same
        // extents the writer laid down.
        use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem};
        use crate::fs::xfs::btree_build::FreeExtent;
        use std::io::Cursor as IoCursor;

        let img = load_fixture().to_vec();
        let mut cursor = Cursor::new(img);
        let sb_clone;
        let original_sb_fdblocks: u64;
        {
            let fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            sb_clone = fs.superblock().clone();
        }
        let sectsize = sb_clone.sectsize as u64;
        let blocksize = sb_clone.blocksize as u64;
        let agblocks = sb_clone.agblocks as u64;

        // Capture each AG's pre-forge freeblks + sb_fdblocks so we can adjust
        // sb_fdblocks for the blocks our forge "leaks". Both AGs must be
        // fragmented or the fast-path alloc_blocks would find a contiguous
        // 1100-block extent in the un-forged AG and inline format would win.
        let mut original_ag_free: Vec<u64> = Vec::with_capacity(sb_clone.agcount as usize);
        for agno in 0..sb_clone.agcount as u64 {
            let agf_byte = agno * agblocks * blocksize + sectsize;
            let mut agf_sec = vec![0u8; sectsize as usize];
            read_at_aligned(
                &mut Cursor::new(cursor.get_ref().as_slice()),
                agf_byte,
                sectsize,
                &mut agf_sec,
            )
            .expect("read AGF");
            original_ag_free.push(BigEndian::read_u32(&agf_sec[52..56]) as u64);
        }
        {
            let mut sb_primary = vec![0u8; sectsize as usize];
            read_at_aligned(
                &mut Cursor::new(cursor.get_ref().as_slice()),
                0,
                sectsize,
                &mut sb_primary,
            )
            .expect("read primary sb");
            original_sb_fdblocks = BigEndian::read_u64(&sb_primary[144..152]);
        }
        // Forge EVERY AG's freespace into 12 fragments of 100 blocks each, with
        // 100-block gaps — placed in the lower portion (above metadata + the
        // initial inode chunk). Both bnobt and cntbt get rebuilt by
        // rebuild_ag_freespace; sb_fdblocks is patched to absorb the leak.
        let frag_count = 12u32;
        let frag_blocksize = 100u32;
        let frag_stride = 200u32;
        let frag_base = 4_000u32; // safely above all metadata + initial chunks
        let frag_extents: Vec<FreeExtent> = (0..frag_count)
            .map(|i| FreeExtent {
                startblock: frag_base + i * frag_stride,
                blockcount: frag_blocksize,
            })
            .collect();
        let forged_ag_free: u64 = (frag_count as u64) * (frag_blocksize as u64);

        {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("reopen editable");
            for agno in 0..sb_clone.agcount as u64 {
                let expected_len = if agno == sb_clone.agcount as u64 - 1 {
                    sb_clone.dblocks - agno * agblocks
                } else {
                    agblocks
                };
                fs.rebuild_ag_freespace(&sb_clone, agno, expected_len, &frag_extents)
                    .unwrap_or_else(|e| panic!("rebuild AG {agno} freespace: {e}"));
            }
        }

        // sb_fdblocks: subtract the sum of (original - forged) deltas across AGs.
        let delta: u64 = original_ag_free
            .iter()
            .map(|&orig| orig - forged_ag_free)
            .sum();
        let mut sb_primary = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut Cursor::new(cursor.get_ref().as_slice()),
            0,
            sectsize,
            &mut sb_primary,
        )
        .expect("read primary sb again");
        BigEndian::write_u64(&mut sb_primary[144..152], original_sb_fdblocks - delta);
        {
            let writer = cursor.get_mut();
            writer[0..sectsize as usize].copy_from_slice(&sb_primary);
        }

        // Create a file that needs 1100 blocks. The fast path (single
        // contiguous extent of 1100 blocks) fails because no free extent that
        // big exists; the multi-run path lights up and lays out 11 runs of
        // 100 blocks each from extents 0..10, exceeding the 9-extent inline
        // cap and forcing btree format.
        let nblocks = 1100u64;
        let payload_len = nblocks * blocksize - blocksize / 2; // slightly less than full
        let payload: Vec<u8> = (0..payload_len as usize)
            .map(|i| ((i * 7 + 13) % 251) as u8)
            .collect();
        {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("reopen for create");
            let root = fs.root().expect("root");
            let mut data = IoCursor::new(payload.clone());
            fs.create_file(
                &root,
                "big.bin",
                &mut data,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create_file in btree mode");
            fs.sync_metadata().expect("sync");
        }

        // Reopen, verify the inode is in btree format and the file reads back.
        let repaired = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(repaired), 0).expect("reopen");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        let big = entries
            .iter()
            .find(|e| e.name == "big.bin")
            .expect("big.bin present");
        let core = fs.read_inode(big.location).expect("core");
        assert_eq!(
            core.format,
            DiFormat::Btree,
            "expected btree format, got {:?}",
            core.format
        );
        assert!(
            core.nextents > 9,
            "expected > 9 extents (inline cap), got {}",
            core.nextents
        );
        // nblocks should include data + 1 bmbt leaf block.
        assert_eq!(
            core.nblocks,
            nblocks + 1,
            "expected nblocks = data ({nblocks}) + 1 bmbt leaf"
        );

        let got = fs
            .read_file(big, payload.len() + (blocksize as usize))
            .expect("read big.bin");
        assert_eq!(
            got.len(),
            payload.len(),
            "file size differs from payload size"
        );
        assert_eq!(&got[..], &payload[..], "data round-trip mismatch");
    }

    #[test]
    fn create_file_in_shortform_root_roundtrips_data() {
        // Allocate an inode + data blocks + insert: create /payload.bin with a
        // known pattern, then read it back byte-for-byte and confirm the volume
        // stays clean.
        use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem};
        use std::io::Cursor as IoCursor;
        let payload: Vec<u8> = (0..9000u32).map(|i| (i % 251) as u8).collect();

        let img = load_fixture().to_vec();
        let mut cursor = Cursor::new(img);
        {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            let root = fs.root().expect("root");
            let mut data = IoCursor::new(payload.clone());
            fs.create_file(
                &root,
                "payload.bin",
                &mut data,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create_file");
            fs.sync_metadata().expect("sync");
        }

        let repaired = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(repaired), 0).expect("reopen");
        let root = fs.root().expect("root");
        let entry = fs
            .list_directory(&root)
            .expect("list")
            .into_iter()
            .find(|e| e.name == "payload.bin")
            .expect("file listed");
        assert!(!entry.is_directory());
        assert_eq!(entry.size, payload.len() as u64, "size mismatch");
        let read_back = fs
            .read_file(&entry, payload.len() + 4096)
            .expect("read_file");
        assert_eq!(&read_back[..payload.len()], &payload[..], "data mismatch");
        let res = fs.run_fsck().expect("fsck");
        assert!(
            res.errors.is_empty(),
            "expected clean, got {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn create_directory_in_shortform_root() {
        // Allocate an inode + insert a short-form entry: create /made_dir under
        // the root, then confirm it lists, is an empty directory, the root link
        // count bumped, and the verifier stays clean.
        use crate::fs::filesystem::{CreateDirectoryOptions, EditableFilesystem};
        let img = load_fixture().to_vec();
        let mut cursor = Cursor::new(img);
        let new_ino = {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            let root = fs.root().expect("root");
            let root_nlink = fs.read_inode(root.location).expect("root inode").nlink;
            let entry = fs
                .create_directory(&root, "made_dir", &CreateDirectoryOptions::default())
                .expect("create_directory");
            fs.sync_metadata().expect("sync");
            // Root gained a subdirectory -> nlink + 1.
            let after = fs.read_inode(root.location).expect("root inode").nlink;
            assert_eq!(after, root_nlink + 1, "root nlink not bumped");
            entry.location
        };

        let repaired = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(repaired), 0).expect("reopen");
        let root = fs.root().expect("root");
        let names: Vec<String> = fs
            .list_directory(&root)
            .expect("list")
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(
            names.contains(&"made_dir".to_string()),
            "not listed: {names:?}"
        );
        let core = fs.read_inode(new_ino).expect("new dir inode");
        assert!(core.is_dir(), "new inode is not a directory");
        assert_eq!(core.nlink, 2, "empty dir should have nlink 2");
        // The new directory is empty and listable.
        let made = fs
            .list_directory(&root)
            .expect("list")
            .into_iter()
            .find(|e| e.name == "made_dir")
            .expect("entry");
        assert!(fs.list_directory(&made).expect("list new dir").is_empty());
        let res = fs.run_fsck().expect("fsck");
        assert!(
            res.errors.is_empty(),
            "expected clean, got {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn repair_r7_recomputes_inode_nlink() {
        // R7 link-count half: corrupt the root directory's nlink (v2 inodes ->
        // di_nlink at core offset 16), repair(), and confirm R7 recomputes it
        // from the directory graph (root has one subdir -> nlink 3) and the
        // verifier stays clean.
        use crate::fs::filesystem::EditableFilesystem;
        let mut img = load_fixture().to_vec();
        let (ino_byte, true_nlink) = {
            let mut fs = XfsFilesystem::open(Cursor::new(img.as_slice()), 0).expect("open");
            let sb = fs.superblock().clone();
            let byte = inode_byte_offset(
                128,
                sb.agblocks,
                sb.agblklog,
                sb.inopblog,
                sb.blocksize,
                sb.inodesize,
            ) as usize;
            let core = fs.read_inode(128).expect("root inode");
            (byte, core.nlink)
        };
        // Corrupt di_nlink (u32 at core offset 16).
        img[ino_byte + 16..ino_byte + 20].copy_from_slice(&99u32.to_be_bytes());

        let mut cursor = Cursor::new(img);
        let report = {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            fs.repair().expect("repair runs")
        };
        assert_eq!(report.fixes_failed.len(), 0, "{:?}", report.fixes_failed);
        assert!(
            report
                .fixes_applied
                .iter()
                .any(|f| f.contains("link counts")),
            "expected an nlink fix, got: {:?}",
            report.fixes_applied
        );

        let repaired = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(repaired), 0).expect("reopen");
        assert_eq!(fs.read_inode(128).expect("reread").nlink, true_nlink);
        let res = fs.run_fsck().expect("fsck after repair");
        assert!(
            res.errors.is_empty(),
            "expected clean, got {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn repair_r6_drops_dangling_shortform_entry() {
        // R6 round-trip: free hello.txt (inode 131) by zeroing its mode, then
        // repair(). R3 frees it in the inobt, R2 reclaims its blocks, and R6
        // drops the now-dangling 'hello.txt' entry from the short-form root.
        // Afterwards the verifier is clean and the entry is gone.
        use crate::fs::filesystem::EditableFilesystem;
        let mut img = load_fixture().to_vec();
        let ino_byte = {
            let fs = XfsFilesystem::open(Cursor::new(img.as_slice()), 0).expect("open");
            let sb = fs.superblock().clone();
            inode_byte_offset(
                131,
                sb.agblocks,
                sb.agblklog,
                sb.inopblog,
                sb.blocksize,
                sb.inodesize,
            ) as usize
        };
        // Zero di_mode (u16 at core offset 2) -> inode reads as free.
        img[ino_byte + 2..ino_byte + 4].copy_from_slice(&0u16.to_be_bytes());

        // Pre-repair: zeroing only the mode (not the inobt) makes the verifier
        // flag the inode as allocated-but-zeroed; R3 then frees it in the inobt
        // and R6 drops the entry that pointed at it.
        {
            let mut fs = XfsFilesystem::open(Cursor::new(img.as_slice()), 0).expect("open");
            let res = fs.run_fsck().expect("fsck");
            assert!(
                res.errors.iter().any(|e| e.code == "AllocatedInodeZeroed"),
                "expected AllocatedInodeZeroed, got {:?}",
                res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
            );
        }

        let mut cursor = Cursor::new(img);
        let report = {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            fs.repair().expect("repair runs")
        };
        assert_eq!(report.fixes_failed.len(), 0, "{:?}", report.fixes_failed);
        assert!(
            report
                .fixes_applied
                .iter()
                .any(|f| f.contains("dangling shortform")),
            "expected a dangling-entry fix, got: {:?}",
            report.fixes_applied
        );

        let repaired = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(repaired), 0).expect("reopen");
        let root = fs.root().expect("root");
        let names: Vec<String> = fs
            .list_directory(&root)
            .expect("list")
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(
            !names.contains(&"hello.txt".to_string()),
            "entry not dropped: {names:?}"
        );
        assert!(
            names.contains(&"readme.txt".to_string()),
            "sibling lost: {names:?}"
        );
        let res = fs.run_fsck().expect("fsck after repair");
        assert!(
            res.errors.is_empty(),
            "expected clean after repair, got: {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn root_inode_is_directory() {
        let img = load_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let root = fs.root().expect("root");
        assert!(root.is_directory());
        assert_eq!(root.location, 128);
        let mode = root.mode.expect("mode");
        assert_eq!(mode & 0o170000, 0o040000);
    }

    #[test]
    fn lists_root_directory_from_fixture() {
        // From `xfs_db -c 'inode 128' -c 'p u'` on /tmp/xfs.img:
        //   hello.txt -> 131 (file)
        //   readme.txt -> 132 (file)
        //   subdir -> 1310848 (directory, in AG1)
        //   link -> 133 (symlink)
        // FTYPE feature is on (features2 = 0x28a).
        let img = load_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        assert!(fs.sb.has_ftype());
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        for expected in ["hello.txt", "readme.txt", "subdir", "link"] {
            assert!(names.contains(&expected), "missing {expected} in {names:?}");
        }
        assert!(!names.contains(&"."));
        assert!(!names.contains(&".."));
        let link = entries.iter().find(|e| e.name == "link").unwrap();
        assert!(link.is_symlink());
        let subdir = entries.iter().find(|e| e.name == "subdir").unwrap();
        assert!(subdir.is_directory());
    }

    #[test]
    fn reads_known_files_from_fixture() {
        // hello.txt (131) and readme.txt (132) are extent-format regular
        // files. xfs_db doesn't tell us their contents directly, but the
        // file sizes are recorded on disk. We just verify the read round-
        // trips byte-for-byte through write_file_to.
        let img = load_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        for name in ["hello.txt", "readme.txt"] {
            let entry = entries.iter().find(|e| e.name == name).expect(name);
            let direct = fs.read_file(entry, usize::MAX).expect("read");
            assert_eq!(direct.len() as u64, entry.size);
            let mut streamed = Vec::new();
            let n = fs.write_file_to(entry, &mut streamed).expect("stream");
            assert_eq!(n, entry.size);
            assert_eq!(streamed, direct);
        }
    }

    #[test]
    fn hello_txt_contents_match_ground_truth() {
        // Verified via `xxd -s 49152` on /tmp/xfs.img: hello.txt is 41 bytes
        // at fsblock 12 (AG0 block 12 = byte 0xC000).
        let img = load_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();
        let data = fs.read_file(hello, usize::MAX).expect("read");
        assert_eq!(data, b"hello from rusty-backup XFS dir2 fixture\n",);
    }

    #[test]
    fn descends_into_subdir_and_reads_nested_file() {
        // subdir (1310848) is shortform-local with one entry: nested.txt
        // → 1310849. Descending must work across AG boundaries (subdir
        // lives in AG1).
        let img = load_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let subdir = entries.iter().find(|e| e.name == "subdir").unwrap();
        assert!(subdir.is_directory());
        let children = fs.list_directory(subdir).expect("list subdir");
        let nested = children
            .iter()
            .find(|e| e.name == "nested.txt")
            .expect("nested.txt");
        assert_eq!(nested.path, "/subdir/nested.txt");
        // read_file should succeed (extents-format file).
        let data = fs.read_file(nested, usize::MAX).expect("read nested");
        assert_eq!(data.len() as u64, nested.size);
    }

    #[test]
    fn symlink_target_is_populated() {
        let img = load_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let link = entries.iter().find(|e| e.name == "link").unwrap();
        assert!(link.is_symlink());
        // Verified via xfs_db: u.symlink = "readme.txt".
        assert_eq!(link.symlink_target.as_deref(), Some("readme.txt"));
    }

    #[test]
    fn truncated_image_produces_unexpected_eof() {
        let img = load_fixture();
        let truncated: Vec<u8> = img[..256].to_vec();
        match XfsFilesystem::open(Cursor::new(truncated), 0) {
            Err(FilesystemError::Io(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof);
            }
            Err(e) => panic!("expected UnexpectedEof, got error {e}"),
            Ok(_) => panic!("expected UnexpectedEof, got Ok"),
        }
    }

    #[test]
    fn rejects_v5_filesystem_with_nrext64_feature() {
        // Minimal v5 sb with NREXT64 bit (incompat 0x20) set — must be
        // rejected because it changes the on-disk inode layout.
        let mut img = vec![0u8; 4096];
        img[0..4].copy_from_slice(&0x5846_5342u32.to_be_bytes());
        img[4..8].copy_from_slice(&4096u32.to_be_bytes());
        img[100..102].copy_from_slice(&0x0005u16.to_be_bytes());
        img[102..104].copy_from_slice(&512u16.to_be_bytes());
        img[216..220].copy_from_slice(&(1u32 << 5).to_be_bytes());
        match XfsFilesystem::open(Cursor::new(img), 0) {
            Err(FilesystemError::Unsupported(msg)) => assert!(msg.contains("NREXT64")),
            Err(e) => panic!("expected NREXT64 rejection, got error {e}"),
            Ok(_) => panic!("expected NREXT64 rejection, got Ok"),
        }
    }

    /// Build a tiny but structurally valid XFS v4 dir1 image: one AG of
    /// 256 blocks, blocksize 512, inodesize 256, root inode (128) with a
    /// dir1-shortform fork carrying three children. Children are not really
    /// referenced (we set their inumbers to 0 so child_entry returns the
    /// fallback placeholder), so we don't have to populate child inodes.
    /// This exercises the dir1 dispatch end-to-end without needing
    /// mkfs.xfs (which no longer produces dir1).
    fn build_dir1_image() -> Vec<u8> {
        // Layout: AG0 of 256 blocks × 512 bytes = 128 KiB.
        let blocksize: u32 = 512;
        let blocks: u32 = 256;
        let img_size = (blocks as usize) * blocksize as usize;
        let mut img = vec![0u8; img_size];

        // Superblock at byte 0.
        BigEndian::write_u32(&mut img[0..4], 0x5846_5342); // XFSB
        BigEndian::write_u32(&mut img[4..8], blocksize);
        BigEndian::write_u64(&mut img[8..16], blocks as u64); // dblocks
                                                              // sb_rootino = 128
        BigEndian::write_u64(&mut img[56..64], 128);
        BigEndian::write_u32(&mut img[84..88], blocks); // agblocks
        BigEndian::write_u32(&mut img[88..92], 1); // agcount
                                                   // versionnum: v4, DIRV2 clear → dir1.
        BigEndian::write_u16(&mut img[100..102], 0x0004);
        BigEndian::write_u16(&mut img[102..104], 512); // sectsize
        BigEndian::write_u16(&mut img[104..106], 256); // inodesize
        BigEndian::write_u16(&mut img[106..108], 2); // inopblock (512/256)
        img[120] = 9; // blocklog (2^9 = 512)
        img[121] = 9; // sectlog
        img[122] = 8; // inodelog (2^8 = 256)
        img[123] = 1; // inopblog (2^1 = 2)
        img[124] = 8; // agblklog (ceil(log2(256)) = 8)

        // Root inode 128:
        //   agno = 128 >> (8+1) = 0
        //   agbno = (128 >> 1) & 0xFF = 64
        //   off = 128 & 0x1 = 0
        //   byte = 64 * 512 = 0x8000
        let ino_off = 64 * 512;
        // di_magic
        BigEndian::write_u16(&mut img[ino_off..ino_off + 2], 0x494E);
        // di_mode: directory + 0755
        BigEndian::write_u16(&mut img[ino_off + 2..ino_off + 4], 0o040755);
        img[ino_off + 4] = 2; // version
        img[ino_off + 5] = 1; // format = Local
                              // di_size at offset +56 — set to fork length after we build it.
                              // Build a dir1 shortform fork: parent=128, 3 entries pointing at
                              // inumbers 0 (so child_entry hits the fallback and returns a
                              // placeholder).
        let mut fork = Vec::new();
        fork.extend_from_slice(&128u64.to_be_bytes()); // parent
        fork.push(3u8); // count
        for (name, ino) in [("alpha", 200u64), ("beta", 201), ("gamma", 202)] {
            fork.extend_from_slice(&ino.to_be_bytes());
            fork.push(name.len() as u8);
            fork.extend_from_slice(name.as_bytes());
        }
        BigEndian::write_u64(&mut img[ino_off + 56..ino_off + 64], fork.len() as u64);
        // Fork starts at offset +100 in the inode.
        let fork_off = ino_off + 100;
        img[fork_off..fork_off + fork.len()].copy_from_slice(&fork);

        // Now write inodes for children 200, 201, 202 so child_entry can
        // read their mode and report Directory / File / Symlink.
        // Layout: agno=0, agbno=(ino>>1)&0xFF, off=ino&1
        let mut write_child = |ino: u64, mode: u16, size: u64| {
            let agbno = ((ino >> 1) & 0xFF) as usize;
            let off_in_block = ((ino & 1) as usize) * 256;
            let byte = agbno * 512 + off_in_block;
            BigEndian::write_u16(&mut img[byte..byte + 2], 0x494E);
            BigEndian::write_u16(&mut img[byte + 2..byte + 4], mode);
            img[byte + 4] = 2;
            img[byte + 5] = 1;
            BigEndian::write_u64(&mut img[byte + 56..byte + 64], size);
        };
        write_child(200, 0o100644, 0); // alpha — regular file
        write_child(201, 0o040755, 0); // beta — directory
        write_child(202, 0o120777, 0); // gamma — symlink (empty target)

        img
    }

    /// Build a synthetic XFS v4 dir2 image containing a single regular file
    /// in btree-format. Layout: one AG of 256 × 512-byte blocks. Root inode
    /// 128 holds a dir2 shortform fork with one entry → inode 200. Inode
    /// 200 is `di_format == Btree`; its bmap btree root has level=1 and one
    /// pointer to a leaf block at fsblock 80, which holds one extent record
    /// pointing at fsblock 90 (the file's single data block). The data
    /// block contains `BTREE_FILE_PAYLOAD` and the file's `di_size` is set
    /// to that string's length.
    ///
    /// This exercises the Step 7 BMBT walker end-to-end without needing a
    /// large fixture.
    const BTREE_FILE_PAYLOAD: &[u8] = b"Hello, btree XFS!\n";

    fn build_btree_file_image() -> Vec<u8> {
        let blocksize: u32 = 512;
        let blocks: u32 = 256;
        let img_size = (blocks as usize) * blocksize as usize;
        let mut img = vec![0u8; img_size];

        // Superblock at byte 0.
        BigEndian::write_u32(&mut img[0..4], 0x5846_5342); // XFSB
        BigEndian::write_u32(&mut img[4..8], blocksize);
        BigEndian::write_u64(&mut img[8..16], blocks as u64); // dblocks
        BigEndian::write_u64(&mut img[56..64], 128); // rootino
        BigEndian::write_u32(&mut img[84..88], blocks); // agblocks
        BigEndian::write_u32(&mut img[88..92], 1); // agcount
                                                   // versionnum: v4 + DIRV2 → dir2.
        BigEndian::write_u16(&mut img[100..102], 0x2004);
        BigEndian::write_u16(&mut img[102..104], 512); // sectsize
        BigEndian::write_u16(&mut img[104..106], 256); // inodesize
        BigEndian::write_u16(&mut img[106..108], 2); // inopblock
        img[120] = 9; // blocklog
        img[121] = 9; // sectlog
        img[122] = 8; // inodelog
        img[123] = 1; // inopblog
        img[124] = 8; // agblklog

        // Root inode 128: agno=0, agbno=64, off=0 → byte 0x8000.
        let root_off = 64 * 512;
        BigEndian::write_u16(&mut img[root_off..root_off + 2], 0x494E); // di_magic
        BigEndian::write_u16(&mut img[root_off + 2..root_off + 4], 0o040755);
        img[root_off + 4] = 2; // version
        img[root_off + 5] = 1; // format = Local

        // Build dir2 shortform fork: count=1, i8count=0, parent[4]=128,
        // entry: namelen=9, offset[2]=0, name="btreefile", inumber[4]=200.
        let mut fork = Vec::new();
        fork.push(1u8); // count
        fork.push(0u8); // i8count
        fork.extend_from_slice(&128u32.to_be_bytes()); // parent
        fork.push(9u8); // namelen
        fork.extend_from_slice(&[0u8, 0u8]); // offset
        fork.extend_from_slice(b"btreefile");
        fork.extend_from_slice(&200u32.to_be_bytes()); // inumber
        BigEndian::write_u64(&mut img[root_off + 56..root_off + 64], fork.len() as u64);
        img[root_off + 100..root_off + 100 + fork.len()].copy_from_slice(&fork);

        // File inode 200: agno=0, agbno=(200>>1)&0xFF=100, off_in_block=
        // (200 & 1) * 256 = 0. So this inode sits at the start of block
        // 100 → byte 100*512 = 51200 = 0xC800.
        let file_off = 100 * 512;
        BigEndian::write_u16(&mut img[file_off..file_off + 2], 0x494E);
        BigEndian::write_u16(&mut img[file_off + 2..file_off + 4], 0o100644);
        img[file_off + 4] = 2; // version
        img[file_off + 5] = 3; // format = Btree
        BigEndian::write_u64(
            &mut img[file_off + 56..file_off + 64],
            BTREE_FILE_PAYLOAD.len() as u64,
        );
        BigEndian::write_u32(&mut img[file_off + 76..file_off + 80], 1); // nextents
                                                                         // Fork at file_off+100..file_off+256 (156 bytes). Lay out bmbt root:
                                                                         // header(4) + 9 keys + 9 ptrs (only slot 0 populated). maxrecs=9.
        let fork_off = file_off + 100;
        BigEndian::write_u16(&mut img[fork_off..fork_off + 2], 1); // level
        BigEndian::write_u16(&mut img[fork_off + 2..fork_off + 4], 1); // numrecs
                                                                       // key0 at fork+4..12 (startoff=0).
                                                                       // ptr0 at fork+4+9*8 = fork+76..fork+84 → fsblock 80.
        BigEndian::write_u64(&mut img[fork_off + 76..fork_off + 84], 80);

        // Leaf btree block at fsblock 80 = byte 40960 = 0xA000.
        let leaf_off = 80 * 512;
        BigEndian::write_u32(&mut img[leaf_off..leaf_off + 4], XFS_BMAP_MAGIC);
        BigEndian::write_u16(&mut img[leaf_off + 4..leaf_off + 6], 0); // level = leaf
        BigEndian::write_u16(&mut img[leaf_off + 6..leaf_off + 8], 1); // numrecs
        BigEndian::write_u64(&mut img[leaf_off + 8..leaf_off + 16], NULLFSBLOCK); // leftsib
        BigEndian::write_u64(&mut img[leaf_off + 16..leaf_off + 24], NULLFSBLOCK); // rightsib
                                                                                   // Extent record at offset 24: startoff=0, startblock=90, blockcount=1,
                                                                                   // unwritten=0. l0=0, l1=(90<<21)|1.
        BigEndian::write_u64(&mut img[leaf_off + 24..leaf_off + 32], 0);
        BigEndian::write_u64(&mut img[leaf_off + 32..leaf_off + 40], (90u64 << 21) | 1);

        // Data block at fsblock 90 = byte 46080 = 0xB400.
        let data_off = 90 * 512;
        img[data_off..data_off + BTREE_FILE_PAYLOAD.len()].copy_from_slice(BTREE_FILE_PAYLOAD);

        img
    }

    #[test]
    fn btree_format_file_round_trips_through_bmbt_walker() {
        let img = build_btree_file_image();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let file = entries
            .iter()
            .find(|e| e.name == "btreefile")
            .expect("btreefile entry");
        assert!(file.is_file());
        assert_eq!(file.size, BTREE_FILE_PAYLOAD.len() as u64);
        let direct = fs.read_file(file, usize::MAX).expect("read");
        assert_eq!(direct, BTREE_FILE_PAYLOAD);
        let mut streamed = Vec::new();
        let n = fs.write_file_to(file, &mut streamed).expect("stream");
        assert_eq!(n as usize, BTREE_FILE_PAYLOAD.len());
        assert_eq!(streamed, BTREE_FILE_PAYLOAD);
    }

    #[test]
    fn dir1_synthetic_image_lists_shortform_entries() {
        let img = build_dir1_image();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open dir1 xfs");
        assert_eq!(fs.dir_format, DirFormat::Dir1);
        assert!(!fs.sb.is_dir2());
        let root = fs.root().expect("root");
        assert!(root.is_directory());
        let entries = fs.list_directory(&root).expect("list root");
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["alpha", "beta", "gamma"]);
        assert!(entries[0].is_file());
        assert!(entries[1].is_directory());
        assert!(entries[2].is_symlink());
    }

    #[test]
    fn bmbt_root_parses_level_and_first_child() {
        // 8-byte fork would only hold the header; use a 32-byte fork — fits
        // header(4) + 1 key(8) + 1 ptr(8) = 20 bytes, plus padding.
        // maxrecs = (32 - 4) / 16 = 1.
        let mut fork = vec![0u8; 32];
        BigEndian::write_u16(&mut fork[0..2], 1); // level
        BigEndian::write_u16(&mut fork[2..4], 1); // numrecs
                                                  // key at fork+4..12, ptr at fork+12..20.
        BigEndian::write_u64(&mut fork[4..12], 0);
        BigEndian::write_u64(&mut fork[12..20], 0xDEAD_BEEF_CAFE_BABE);
        let (level, first_child) = parse_bmbt_root(&fork, fork.len()).expect("parse");
        assert_eq!(level, 1);
        assert_eq!(first_child, 0xDEAD_BEEF_CAFE_BABE);
    }

    #[test]
    fn bmbt_root_rejects_level_zero() {
        let mut fork = vec![0u8; 32];
        BigEndian::write_u16(&mut fork[0..2], 0); // level=0 invalid for btree root
        BigEndian::write_u16(&mut fork[2..4], 1);
        match parse_bmbt_root(&fork, fork.len()) {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("level 0")),
            other => panic!("expected Parse error, got {other:?}"),
        }
    }

    #[test]
    fn bmbt_root_uses_maxrecs_not_numrecs_for_ptr_offset() {
        // A real btree root reserves space for `maxrecs` keys before the
        // first pointer, even when only `numrecs` slots are populated.
        // Fork of 36 bytes → maxrecs = 2. Header(4) + 2 keys(16) + ptr(8)
        // = 28 bytes. Ptr should be read at offset 4 + 2*8 = 20, NOT at
        // 4 + 1*8 = 12 (where the second key sits).
        let mut fork = vec![0u8; 36];
        BigEndian::write_u16(&mut fork[0..2], 2); // level
        BigEndian::write_u16(&mut fork[2..4], 1); // numrecs (1 of 2 populated)
                                                  // key0 at +4..12, key1 (unused) at +12..20.
        BigEndian::write_u64(&mut fork[4..12], 0);
        BigEndian::write_u64(&mut fork[12..20], 0xDEAD); // would be wrong ptr
                                                         // ptr0 at +20..28 (= 4 + maxrecs*8).
        BigEndian::write_u64(&mut fork[20..28], 0xBEEF);
        let (level, first_child) = parse_bmbt_root(&fork, fork.len()).expect("parse");
        assert_eq!(level, 2);
        assert_eq!(first_child, 0xBEEF);
    }

    #[test]
    fn bmbt_header_check_accepts_correct_block() {
        let mut block = vec![0u8; XFS_BTREE_LBLOCK_LEN];
        BigEndian::write_u32(&mut block[0..4], XFS_BMAP_MAGIC);
        BigEndian::write_u16(&mut block[4..6], 0); // level
        check_bmbt_header(&block, Some(0), false).expect("ok");
        check_bmbt_header(&block, None, false).expect("ok");
    }

    #[test]
    fn bmbt_header_check_accepts_v5_bma3_magic() {
        let mut block = vec![0u8; XFS_BTREE_LBLOCK_CRC_LEN];
        BigEndian::write_u32(&mut block[0..4], XFS_BMAP_CRC_MAGIC);
        BigEndian::write_u16(&mut block[4..6], 0);
        check_bmbt_header(&block, Some(0), true).expect("ok");
        // v4 magic must be rejected when caller said is_v5.
        BigEndian::write_u32(&mut block[0..4], XFS_BMAP_MAGIC);
        assert!(check_bmbt_header(&block, None, true).is_err());
    }

    #[test]
    fn bmbt_header_check_rejects_bad_magic_and_level() {
        let mut block = vec![0u8; XFS_BTREE_LBLOCK_LEN];
        BigEndian::write_u32(&mut block[0..4], 0xDEAD_BEEF);
        match check_bmbt_header(&block, None, false) {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("magic")),
            other => panic!("expected magic error, got {other:?}"),
        }
        BigEndian::write_u32(&mut block[0..4], XFS_BMAP_MAGIC);
        BigEndian::write_u16(&mut block[4..6], 3);
        match check_bmbt_header(&block, Some(1), false) {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("level")),
            other => panic!("expected level error, got {other:?}"),
        }
    }

    #[test]
    fn fb_to_disk_handles_extents_and_holes() {
        let extents = vec![
            XfsBmbtIrec {
                startoff: 0,
                startblock: 100,
                blockcount: 4,
                unwritten: false,
            },
            XfsBmbtIrec {
                startoff: 10,
                startblock: 200,
                blockcount: 2,
                unwritten: true,
            },
        ];
        assert_eq!(fb_to_disk(&extents, 0), Some((100, false)));
        assert_eq!(fb_to_disk(&extents, 3), Some((103, false)));
        // Hole at fb=4..10.
        assert_eq!(fb_to_disk(&extents, 4), None);
        assert_eq!(fb_to_disk(&extents, 9), None);
        // Unwritten extent at fb=10..12.
        assert_eq!(fb_to_disk(&extents, 10), Some((200, true)));
        assert_eq!(fb_to_disk(&extents, 11), Some((201, true)));
        // Past the last extent.
        assert_eq!(fb_to_disk(&extents, 12), None);
    }

    /// Decompress the bundled modern-XFS fixture (300 MiB of mostly zeros,
    /// zstd → ~18 KiB on disk). Produced by a stock Linux `mkfs.xfs`
    /// (v5/CRC, dir3, blocksize=4096, inodesize=512, 4 AGs).
    fn load_modern_fixture() -> &'static [u8] {
        load_sgi_fixture("xfs_v5_modern_small.img.zst")
    }

    #[test]
    fn opens_modern_xfs_v5_fixture() {
        let img = load_modern_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs v5");
        assert!(fs.sb.is_v5(), "expected v5 fs");
        assert_eq!(fs.sb.blocksize, 4096);
        assert_eq!(fs.sb.inodesize, 512);
        assert_eq!(fs.sb.rootino, 128);
        assert_eq!(fs.sb.agcount, 4);
        assert_eq!(fs.fs_type(), "XFS v5");
        // dir3 carries FTYPE via features_incompat bit 0.
        assert!(fs.sb.has_ftype());

        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        for n in [
            "data_1.bin",
            "data_2.bin",
            "data_3.bin",
            "data_4.bin",
            "data_5.bin",
            "hostname",
        ] {
            assert!(names.contains(&n), "missing {n} in {names:?}");
        }

        // hostname is 5 bytes ("m900\n") — verified via xfs_db. Confirm read
        // and write_file_to both produce the same bytes.
        let hostname = entries.iter().find(|e| e.name == "hostname").unwrap();
        let direct = fs.read_file(hostname, 4096).expect("read hostname");
        assert_eq!(direct.len() as u64, hostname.size);
        assert_eq!(direct, b"m900\n");
        let mut streamed = Vec::new();
        let n = fs.write_file_to(hostname, &mut streamed).expect("stream");
        assert_eq!(n, hostname.size);
        assert_eq!(streamed, direct);
    }

    /// Cross-check the E.1 CRC primitive against on-disk v5 metadata blocks:
    /// the root inode (v3 core) and the AGF/AGI/SB CRC fields. If our
    /// `stamp_crc` produced anything other than what mkfs.xfs put there, the
    /// equality fails and every v5 write would write garbage.
    #[test]
    fn v5_crc_primitive_matches_on_disk_root_inode_and_ag_headers() {
        use crate::fs::xfs::v5_crc::{
            crc_valid, AGF_CRC_OFF, AGI_CRC_OFF, INODE_V3_DI_CRC_OFF, SB_CRC_OFF,
        };

        let img = load_modern_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs v5");
        let sb = fs.superblock().clone();
        assert!(sb.is_v5());

        // Superblock CRC over the first sector.
        let sb_sec = img[..sb.sectsize as usize].to_vec();
        assert!(crc_valid(&sb_sec, SB_CRC_OFF), "superblock CRC mismatch");

        // AG 0 AGF (sector 1) + AGI (sector 2).
        let sectsize = sb.sectsize as usize;
        let agf = img[sectsize..2 * sectsize].to_vec();
        assert!(crc_valid(&agf, AGF_CRC_OFF), "AGF CRC mismatch");
        let agi = img[2 * sectsize..3 * sectsize].to_vec();
        assert!(crc_valid(&agi, AGI_CRC_OFF), "AGI CRC mismatch");

        // Root inode v3 core CRC — sized to inodesize, located via the
        // existing inode reader.
        let (_core, ibuf) = fs.read_inode_buf(sb.rootino).expect("read root inode");
        assert_eq!(ibuf.len(), sb.inodesize as usize);
        assert!(
            crc_valid(&ibuf, INODE_V3_DI_CRC_OFF),
            "root inode di_crc mismatch"
        );
    }

    /// §2.1 hole (E.5a): round-trip the AGI, AGF, and primary superblock
    /// through the new sector-stamper helpers on the v5 fixture. The test
    /// mutates a non-CRC field in each (AGI `count` / AGF `freeblks` / SB
    /// `sb_icount`), writes back through `write_agi_sector` /
    /// `write_agf_sector` / `write_sb_primary`, then re-reads and asserts
    /// (a) the mutation survived and (b) the CRC at the corresponding
    /// `*_CRC_OFF` re-verifies. Without the v5 stamping path inside each
    /// helper, the CRC after the write would be stale.
    #[test]
    fn ag_header_stamp_helpers_round_trip_on_v5_fixture() {
        use crate::fs::xfs::v5_crc::{
            crc_valid, AGF_CRC_OFF, AGF_LSN_OFF, AGI_CRC_OFF, AGI_LSN_OFF, SB_CRC_OFF,
        };

        let img = load_modern_fixture().to_vec();
        let mut cursor = Cursor::new(img);
        let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open xfs v5");
        let sb = fs.superblock().clone();
        assert!(sb.is_v5());

        let sectsize = sb.sectsize as u64;

        // AGI sector for AG 0.
        let agi_byte = 2 * sectsize;
        let mut agi_sec = vec![0u8; sectsize as usize];
        read_at_aligned(&mut fs.reader, agi_byte, sectsize, &mut agi_sec).unwrap();
        assert!(crc_valid(&agi_sec, AGI_CRC_OFF), "pre-write AGI CRC valid");
        // Bump AGI count by 0 (no-op mutation) — still triggers a CRC restamp.
        let count_off = 16;
        let cur = BigEndian::read_u32(&agi_sec[count_off..count_off + 4]);
        BigEndian::write_u32(&mut agi_sec[count_off..count_off + 4], cur); // identity
                                                                           // Mark LSN to NULL sentinel so the stamper has work to do.
        BigEndian::write_u64(&mut agi_sec[AGI_LSN_OFF..AGI_LSN_OFF + 8], 0xCAFE);
        fs.write_agi_sector(&sb, agi_byte, &mut agi_sec).unwrap();
        let mut agi_re = vec![0u8; sectsize as usize];
        read_at_aligned(&mut fs.reader, agi_byte, sectsize, &mut agi_re).unwrap();
        assert!(
            crc_valid(&agi_re, AGI_CRC_OFF),
            "post-write AGI CRC must verify"
        );
        // LSN was overwritten with NULL sentinel by stamper.
        assert_eq!(
            BigEndian::read_u64(&agi_re[AGI_LSN_OFF..AGI_LSN_OFF + 8]),
            u64::MAX
        );

        // AGF sector for AG 0.
        let agf_byte = sectsize;
        let mut agf_sec = vec![0u8; sectsize as usize];
        read_at_aligned(&mut fs.reader, agf_byte, sectsize, &mut agf_sec).unwrap();
        assert!(crc_valid(&agf_sec, AGF_CRC_OFF), "pre-write AGF CRC valid");
        BigEndian::write_u64(&mut agf_sec[AGF_LSN_OFF..AGF_LSN_OFF + 8], 0xBEEF);
        fs.write_agf_sector(&sb, agf_byte, &mut agf_sec).unwrap();
        let mut agf_re = vec![0u8; sectsize as usize];
        read_at_aligned(&mut fs.reader, agf_byte, sectsize, &mut agf_re).unwrap();
        assert!(
            crc_valid(&agf_re, AGF_CRC_OFF),
            "post-write AGF CRC must verify"
        );

        // Primary superblock.
        let mut sb_sec = vec![0u8; sectsize as usize];
        read_at_aligned(&mut fs.reader, 0, sectsize, &mut sb_sec).unwrap();
        assert!(crc_valid(&sb_sec, SB_CRC_OFF), "pre-write SB CRC valid");
        // Mutate sb_icount (bytes 128..136).
        let cur_ic = BigEndian::read_u64(&sb_sec[128..136]);
        BigEndian::write_u64(&mut sb_sec[128..136], cur_ic + 1);
        fs.write_sb_primary(&sb, &mut sb_sec).unwrap();
        let mut sb_re = vec![0u8; sectsize as usize];
        read_at_aligned(&mut fs.reader, 0, sectsize, &mut sb_re).unwrap();
        assert!(
            crc_valid(&sb_re, SB_CRC_OFF),
            "post-write SB CRC must verify"
        );
        assert_eq!(
            BigEndian::read_u64(&sb_re[128..136]),
            cur_ic + 1,
            "sb_icount mutation round-tripped"
        );
    }

    /// §2.1 hole (E.2): round-trip the root inode through `write_inode_region`
    /// on the v5 fixture. The caller mutates a non-CRC field (`di_atime`),
    /// the writer stamps `di_crc` over the entire on-disk inode buffer, and
    /// a re-read sees the mutation AND a CRC that re-verifies. If the v5
    /// stamp didn't fire, `crc_valid` would catch it.
    #[test]
    fn write_inode_region_stamps_v3_crc_on_v5_round_trip() {
        use crate::fs::xfs::v5_crc::{crc_valid, INODE_V3_DI_CRC_OFF, INODE_V3_DI_INO_OFF};

        let img = load_modern_fixture().to_vec();
        let mut cursor = Cursor::new(img);
        let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open xfs v5");
        let sb = fs.superblock().clone();
        assert!(sb.is_v5());

        // Read the root inode, mutate di_atime.tv_sec (offset 32..36, BE32),
        // write through the editable surface, then re-read and verify both
        // the field round-trip AND the CRC.
        let (_core, mut ibuf) = fs.read_inode_buf(sb.rootino).expect("read root inode");
        assert!(crc_valid(&ibuf, INODE_V3_DI_CRC_OFF), "pre-write CRC valid");
        let pre_atime = BigEndian::read_u32(&ibuf[32..36]);
        let new_atime = pre_atime.wrapping_add(1);
        BigEndian::write_u32(&mut ibuf[32..36], new_atime);

        // Sanity: di_ino on disk matches sb.rootino (the stamper writes it,
        // so we'd notice if the offset constants drifted).
        assert_eq!(
            BigEndian::read_u64(&ibuf[INODE_V3_DI_INO_OFF..INODE_V3_DI_INO_OFF + 8]),
            sb.rootino
        );

        fs.write_inode_region(&sb, sb.rootino, &ibuf)
            .expect("write_inode_region");

        // Re-read the inode buffer and check (1) the mutation landed,
        // (2) the di_crc still verifies. This is the whole point of E.2.
        let (_core2, ibuf2) = fs.read_inode_buf(sb.rootino).expect("re-read root inode");
        assert_eq!(
            BigEndian::read_u32(&ibuf2[32..36]),
            new_atime,
            "di_atime mutation round-tripped"
        );
        assert!(
            crc_valid(&ibuf2, INODE_V3_DI_CRC_OFF),
            "post-write di_crc invalid — v5 stamp didn't fire"
        );
    }

    #[test]
    fn alloc_new_inode_chunk_extends_inobt_and_keeps_volume_clean() {
        // §2.1 hole (A): when every existing inobt chunk is full,
        // alloc_new_inode_chunk should carve fresh aligned blocks, write 64
        // free dinodes, splice a new record into AG 0's single-leaf inobt,
        // and bump AGI count/freecount + sb_icount/sb_ifree by 64 — with the
        // verifier remaining clean afterward.
        //
        // Driving it through alloc_inode_slot would require exhausting every
        // existing chunk first (~16k file creations on this 512 MiB fixture);
        // instead we call alloc_new_inode_chunk directly to exercise the
        // pipeline once, then check both the AGI/superblock counter deltas
        // and the on-disk shape of the new chunk.
        use crate::fs::filesystem::EditableFilesystem;
        use crate::fs::xfs::ag::XfsAgi;
        use crate::fs::xfs::types::XFS_INODES_PER_CHUNK;

        let img = load_fixture().to_vec();
        let mut cursor = Cursor::new(img);

        // Snapshot pre-state: AGI 0 count/freecount/root, sb_icount/sb_ifree.
        let (
            sb_clone,
            agi_byte0,
            sectsize,
            blocksize,
            agi_before,
            agbno_root,
            sb_icount_before,
            sb_ifree_before,
        ) = {
            let fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            let sb = fs.superblock().clone();
            let sectsize = sb.sectsize as u64;
            let agblocks = sb.agblocks as u64;
            let blocksize = sb.blocksize as u64;
            let agi_byte0 = 2 * sectsize; // AG 0, AGI is the third sector.
            let mut agi_sec = vec![0u8; sectsize as usize];
            read_at_aligned(
                &mut Cursor::new(cursor.get_ref().as_slice()),
                agi_byte0,
                sectsize,
                &mut agi_sec,
            )
            .expect("read AGI 0");
            let agi = XfsAgi::parse(&agi_sec).expect("parse AGI 0");
            let agbno_root = agi.root;
            let _ = agblocks;
            // Read sb_icount / sb_ifree from the primary superblock.
            let mut primary = vec![0u8; sectsize as usize];
            read_at_aligned(
                &mut Cursor::new(cursor.get_ref().as_slice()),
                0,
                sectsize,
                &mut primary,
            )
            .expect("read primary");
            let sb_icount = BigEndian::read_u64(&primary[128..136]);
            let sb_ifree = BigEndian::read_u64(&primary[136..144]);
            (
                sb, agi_byte0, sectsize, blocksize, agi, agbno_root, sb_icount, sb_ifree,
            )
        };

        let leaf_fsblock = agbno_root as u64; // AG 0 → fsblock == agbno
        let leaf_byte = leaf_fsblock * blocksize;
        let leaf_before = {
            let mut buf = vec![0u8; blocksize as usize];
            read_at_aligned(
                &mut Cursor::new(cursor.get_ref().as_slice()),
                leaf_byte,
                blocksize,
                &mut buf,
            )
            .expect("read inobt leaf");
            buf
        };
        let numrecs_before = BigEndian::read_u16(&leaf_before[6..8]) as usize;

        // Drive the new-chunk path once.
        {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            fs.alloc_new_inode_chunk(&sb_clone)
                .expect("alloc_new_inode_chunk");
            fs.sync_metadata().expect("sync");
        }

        // Snapshot post-state and verify the deltas.
        let mut agi_sec_after = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut Cursor::new(cursor.get_ref().as_slice()),
            agi_byte0,
            sectsize,
            &mut agi_sec_after,
        )
        .expect("read AGI 0 after");
        let agi_after = XfsAgi::parse(&agi_sec_after).expect("parse AGI 0 after");
        assert_eq!(
            agi_after.count,
            agi_before.count + XFS_INODES_PER_CHUNK as u32,
            "AGI count should bump by 64",
        );
        assert_eq!(
            agi_after.freecount,
            agi_before.freecount + XFS_INODES_PER_CHUNK as u32,
            "AGI freecount should bump by 64",
        );

        let mut primary_after = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut Cursor::new(cursor.get_ref().as_slice()),
            0,
            sectsize,
            &mut primary_after,
        )
        .expect("read primary after");
        assert_eq!(
            BigEndian::read_u64(&primary_after[128..136]),
            sb_icount_before + XFS_INODES_PER_CHUNK as u64,
            "sb_icount should bump by 64",
        );
        assert_eq!(
            BigEndian::read_u64(&primary_after[136..144]),
            sb_ifree_before + XFS_INODES_PER_CHUNK as u64,
            "sb_ifree should bump by 64",
        );

        // Inobt root leaf: one extra record, sorted by start_agino.
        let mut leaf_after = vec![0u8; blocksize as usize];
        read_at_aligned(
            &mut Cursor::new(cursor.get_ref().as_slice()),
            leaf_byte,
            blocksize,
            &mut leaf_after,
        )
        .expect("read inobt leaf after");
        let numrecs_after = BigEndian::read_u16(&leaf_after[6..8]) as usize;
        assert_eq!(
            numrecs_after,
            numrecs_before + 1,
            "inobt grew by one record"
        );
        let mut prev_start: i64 = -1;
        let mut new_record_start: Option<u32> = None;
        for r in 0..numrecs_after {
            let off = 16 + r * 16; // INOBT_HDR_LEN + r * INOBT_REC_SIZE
            let s = BigEndian::read_u32(&leaf_after[off..off + 4]);
            let fc = BigEndian::read_u32(&leaf_after[off + 4..off + 8]);
            let free = BigEndian::read_u64(&leaf_after[off + 8..off + 16]);
            assert!(
                (s as i64) > prev_start,
                "records must be sorted by start_agino"
            );
            prev_start = s as i64;
            if fc == XFS_INODES_PER_CHUNK as u32 && free == u64::MAX {
                assert!(
                    new_record_start.is_none(),
                    "more than one all-free chunk record after extend"
                );
                new_record_start = Some(s);
            }
        }
        let start_agino = new_record_start.expect("new all-free chunk record present");

        // Every dinode in the new chunk should carry XFS_DINODE_MAGIC, the
        // volume's di_version, and di_next_unlinked = NULLAGINO (the on-disk
        // "free slot" initialization).
        let inodesize = sb_clone.inodesize as u64;
        let blocks_per_chunk =
            ((XFS_INODES_PER_CHUNK as u64) * inodesize).div_ceil(blocksize) as usize;
        let chunk_agbno = (start_agino as u64) >> sb_clone.inopblog;
        let chunk_byte = chunk_agbno * blocksize;
        let span = blocks_per_chunk * blocksize as usize;
        let mut chunk_bytes = vec![0u8; span];
        read_at_aligned(
            &mut Cursor::new(cursor.get_ref().as_slice()),
            chunk_byte,
            span as u64,
            &mut chunk_bytes,
        )
        .expect("read new chunk blocks");
        // Sample the root inode for di_version so we know what to expect.
        let mut root_inode_buf = vec![0u8; inodesize as usize];
        {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open for root version");
            let (_core, buf) = fs.read_inode_buf(sb_clone.rootino).expect("root inode");
            root_inode_buf[..buf.len()].copy_from_slice(&buf);
        }
        let expected_version = root_inode_buf[4];
        for slot in 0..XFS_INODES_PER_CHUNK {
            let base = slot * inodesize as usize;
            assert_eq!(
                BigEndian::read_u16(&chunk_bytes[base..base + 2]),
                super::types::XFS_DINODE_MAGIC,
                "slot {slot} missing dinode magic",
            );
            assert_eq!(
                chunk_bytes[base + 4],
                expected_version,
                "slot {slot} wrong di_version",
            );
            assert_eq!(
                BigEndian::read_u32(&chunk_bytes[base + 96..base + 100]),
                0xFFFF_FFFF,
                "slot {slot} di_next_unlinked != NULLAGINO",
            );
            assert_eq!(
                BigEndian::read_u16(&chunk_bytes[base + 2..base + 4]),
                0,
                "slot {slot} di_mode should be 0 (free)",
            );
        }

        // End-to-end: the verifier stays clean over the extended inobt.
        let repaired = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(repaired), 0).expect("reopen");
        let res = fs.run_fsck().expect("fsck after new-chunk allocation");
        assert!(
            res.errors.is_empty(),
            "expected clean after new-chunk allocation, got: {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    /// §2.1 hole (E.5b): drive `alloc_new_inode_chunk` on the v5 modern
    /// fixture. Mirrors the v4 single-leaf test (`alloc_new_inode_chunk_
    /// extends_inobt_and_keeps_volume_clean`) but checks every v5
    /// invariant — AGI/AGF/SB CRCs re-verify, the inobt leaf's sblock-crc
    /// re-verifies, and every fresh dinode in the new chunk carries the
    /// v3 core (di_version=3, di_ino, di_uuid, valid di_crc).
    #[test]
    fn alloc_new_inode_chunk_on_v5_stamps_every_block_and_stays_clean() {
        use crate::fs::xfs::ag::{XfsAgf, XfsAgi};
        use crate::fs::xfs::types::XFS_INODES_PER_CHUNK;
        use crate::fs::xfs::v5_crc::{
            crc_valid, AGF_CRC_OFF, AGI_CRC_OFF, INODE_V3_DI_CRC_OFF, INODE_V3_DI_INO_OFF,
            INODE_V3_DI_UUID_OFF, SBLOCK_CRC_CRC_OFF, SB_CRC_OFF,
        };

        let img = load_modern_fixture().to_vec();
        let mut cursor = Cursor::new(img);

        let (sb, sectsize, blocksize, agi_byte0, agf_byte0, agbno_root, counts_before) = {
            let fs = XfsFilesystem::open(&mut cursor, 0).expect("open v5");
            let sb = fs.superblock().clone();
            assert!(sb.is_v5(), "expected v5 fixture");
            let sectsize = sb.sectsize as u64;
            let blocksize = sb.blocksize as u64;
            let agi_byte0 = 2 * sectsize;
            let agf_byte0 = sectsize;
            let mut agi_sec = vec![0u8; sectsize as usize];
            read_at_aligned(
                &mut Cursor::new(cursor.get_ref().as_slice()),
                agi_byte0,
                sectsize,
                &mut agi_sec,
            )
            .expect("AGI 0 pre");
            let agi = XfsAgi::parse(&agi_sec).unwrap();
            let mut primary = vec![0u8; sectsize as usize];
            read_at_aligned(
                &mut Cursor::new(cursor.get_ref().as_slice()),
                0,
                sectsize,
                &mut primary,
            )
            .expect("SB pre");
            let sb_icount = BigEndian::read_u64(&primary[128..136]);
            let sb_ifree = BigEndian::read_u64(&primary[136..144]);
            (
                sb,
                sectsize,
                blocksize,
                agi_byte0,
                agf_byte0,
                agi.root,
                (agi.count, agi.freecount, sb_icount, sb_ifree),
            )
        };
        let (agi_count_before, agi_freecount_before, sb_icount_before, sb_ifree_before) =
            counts_before;

        // Drive the new-chunk allocation. With the v5 gate lifted this
        // should now succeed against the modern fixture.
        {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable v5");
            fs.alloc_new_inode_chunk(&sb)
                .expect("alloc_new_inode_chunk on v5");
            use crate::fs::filesystem::EditableFilesystem;
            fs.sync_metadata().expect("sync");
        }

        // AGI / AGF / SB still CRC-valid after every write site stamped on
        // the way out (E.5a path).
        let mut agi_after = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut Cursor::new(cursor.get_ref().as_slice()),
            agi_byte0,
            sectsize,
            &mut agi_after,
        )
        .expect("AGI 0 post");
        assert!(
            crc_valid(&agi_after, AGI_CRC_OFF),
            "AGI CRC invalid after chunk alloc"
        );
        let agi_parsed = XfsAgi::parse(&agi_after).unwrap();
        assert_eq!(
            agi_parsed.count,
            agi_count_before + XFS_INODES_PER_CHUNK as u32
        );
        assert_eq!(
            agi_parsed.freecount,
            agi_freecount_before + XFS_INODES_PER_CHUNK as u32
        );

        let mut agf_after = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut Cursor::new(cursor.get_ref().as_slice()),
            agf_byte0,
            sectsize,
            &mut agf_after,
        )
        .expect("AGF 0 post");
        assert!(
            crc_valid(&agf_after, AGF_CRC_OFF),
            "AGF CRC invalid after chunk alloc"
        );
        let _ = XfsAgf::parse(&agf_after).expect("AGF parses after rebuild");

        let mut sb_after = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut Cursor::new(cursor.get_ref().as_slice()),
            0,
            sectsize,
            &mut sb_after,
        )
        .expect("SB post");
        assert!(
            crc_valid(&sb_after, SB_CRC_OFF),
            "SB CRC invalid after chunk alloc"
        );
        assert_eq!(
            BigEndian::read_u64(&sb_after[128..136]),
            sb_icount_before + XFS_INODES_PER_CHUNK as u64
        );
        assert_eq!(
            BigEndian::read_u64(&sb_after[136..144]),
            sb_ifree_before + XFS_INODES_PER_CHUNK as u64
        );

        // The single-leaf inobt at the AGI root must still parse via the v5
        // 56-byte hdr and have its sblock-crc re-verified.
        let leaf_fsblock = agbno_root as u64; // AG 0 → fsblock == agbno
        let leaf_byte = leaf_fsblock * blocksize;
        let mut leaf_after = vec![0u8; blocksize as usize];
        read_at_aligned(
            &mut Cursor::new(cursor.get_ref().as_slice()),
            leaf_byte,
            blocksize,
            &mut leaf_after,
        )
        .expect("inobt leaf post");
        assert!(
            crc_valid(&leaf_after, SBLOCK_CRC_CRC_OFF),
            "inobt leaf sblock CRC invalid after splice"
        );
        // Find a record matching `(64, u64::MAX)` — our new chunk.
        let hdr_len = 56; // v5 XFS_BTREE_SBLOCK_CRC_LEN
        let numrecs = BigEndian::read_u16(&leaf_after[6..8]) as usize;
        let mut found_new_record = false;
        let mut new_start_agino = 0u32;
        for r in 0..numrecs {
            let off = hdr_len + r * 16; // INOBT_REC_SIZE
            let fc = BigEndian::read_u32(&leaf_after[off + 4..off + 8]);
            let free = BigEndian::read_u64(&leaf_after[off + 8..off + 16]);
            if fc == XFS_INODES_PER_CHUNK as u32 && free == u64::MAX {
                assert!(!found_new_record, "more than one all-free chunk record");
                found_new_record = true;
                new_start_agino = BigEndian::read_u32(&leaf_after[off..off + 4]);
            }
        }
        assert!(
            found_new_record,
            "new all-free chunk record missing from inobt leaf"
        );

        // Every fresh dinode in the new chunk: di_magic + di_version=3 +
        // di_ino = start_ino + slot + di_uuid = sb_meta_uuid + valid di_crc.
        let inodesize = sb.inodesize as u64;
        let blocks_per_chunk =
            ((XFS_INODES_PER_CHUNK as u64) * inodesize).div_ceil(blocksize) as usize;
        let chunk_agbno = (new_start_agino as u64) >> sb.inopblog;
        let chunk_byte = chunk_agbno * blocksize;
        let span = blocks_per_chunk * blocksize as usize;
        let mut chunk_bytes = vec![0u8; span];
        read_at_aligned(
            &mut Cursor::new(cursor.get_ref().as_slice()),
            chunk_byte,
            span as u64,
            &mut chunk_bytes,
        )
        .expect("new chunk bytes");

        let ino_shift = sb.agblklog + sb.inopblog;
        let start_ino = (0u64 << ino_shift) | new_start_agino as u64;
        for slot in 0..XFS_INODES_PER_CHUNK {
            let base = slot * inodesize as usize;
            let slot_buf = &chunk_bytes[base..base + inodesize as usize];
            assert_eq!(
                BigEndian::read_u16(&slot_buf[0..2]),
                super::types::XFS_DINODE_MAGIC,
                "slot {slot} missing di_magic"
            );
            assert_eq!(slot_buf[4], 3, "slot {slot} di_version should be 3 on v5");
            assert_eq!(
                BigEndian::read_u64(&slot_buf[INODE_V3_DI_INO_OFF..INODE_V3_DI_INO_OFF + 8]),
                start_ino + slot as u64,
                "slot {slot} di_ino mismatch"
            );
            assert_eq!(
                &slot_buf[INODE_V3_DI_UUID_OFF..INODE_V3_DI_UUID_OFF + 16],
                sb.meta_uuid(),
                "slot {slot} di_uuid != sb_meta_uuid"
            );
            assert!(
                crc_valid(slot_buf, INODE_V3_DI_CRC_OFF),
                "slot {slot} di_crc invalid"
            );
        }

        // End-to-end: our own verifier stays clean over the extended v5
        // volume. This catches structural issues across AGI / AGF / inobt /
        // SB that the per-stamp checks above might miss.
        let extended = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(extended), 0).expect("reopen v5");
        let res = fs.run_fsck().expect("fsck after v5 new-chunk allocation");
        assert!(
            res.errors.is_empty(),
            "expected clean v5 volume after new-chunk alloc, got: {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    /// §2.1 hole (E.5c): end-to-end create + read on the v5 modern fixture.
    /// Drives `EditableFilesystem::create_file` (which threads through
    /// `do_create_file` → `alloc_inode_slot` → `alloc_blocks` →
    /// `write_file_data_extents` → `init_file_inode` → `dir_insert_entry`),
    /// then re-opens read-only and reads the new file back. The on-disk
    /// stamps are validated indirectly: `run_fsck` walks every metadata
    /// block and would refuse a v5 inode / dir3 / sblock with a stale CRC.
    #[test]
    fn create_file_on_v5_fixture_round_trips_through_fsck() {
        use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};

        let img = load_modern_fixture().to_vec();
        let mut cursor = Cursor::new(img);

        let payload: &[u8] = b"hello, v5/CRC XFS world\n";
        let new_name = "v5_newfile.bin";

        {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable v5");
            assert!(fs.superblock().is_v5(), "expected v5 fixture");
            let root = fs.root().expect("root");
            let mut reader = std::io::Cursor::new(payload);
            fs.create_file(
                &root,
                new_name,
                &mut reader,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create_file on v5");
            fs.sync_metadata().expect("sync");
        }

        // Re-open and read the file back.
        let bytes = cursor.into_inner();
        let mut fs = XfsFilesystem::open(Cursor::new(bytes), 0).expect("reopen v5");
        let root = fs.root().expect("root post-create");
        let entries = fs.list_directory(&root).expect("list root post-create");
        let child = entries
            .iter()
            .find(|e| e.name == new_name)
            .unwrap_or_else(|| {
                panic!(
                    "new file {new_name} missing; got names {:?}",
                    entries.iter().map(|e| &e.name).collect::<Vec<_>>()
                )
            });
        assert_eq!(child.size, payload.len() as u64);
        let read_back = fs.read_file(child, 4096).expect("read newly-created file");
        assert_eq!(read_back, payload, "v5 round-trip preserved file bytes");

        // run_fsck walks every metadata block (inode CRCs, dir3 CRCs,
        // sblock-crc btrees, AGF/AGI/SB) and would surface any stale CRC
        // from the create path.
        let res = fs.run_fsck().expect("fsck after v5 create_file");
        assert!(
            res.errors.is_empty(),
            "expected clean v5 volume after create_file, got: {:?}",
            res.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn alloc_new_inode_chunk_promotes_full_leaf_to_multi_level() {
        // §2.1 hole (A) follow-up: when the AG's only inobt leaf is full
        // (numrecs == max_leaf), alloc_new_inode_chunk must switch from the
        // single-leaf splice path to the multi-level grow path — rebuild the
        // inobt as a 2-level tree on freshly carved blocks (reclaiming the old
        // leaf in the same freespace rebuild via swap_inobt_blocks_in_ag),
        // relocate AGI root + level, and bump AGI count/freecount + sb
        // counters by +64.
        //
        // Forging the leaf to max_leaf records (existing real + phantom
        // records with no backing dinodes) shortcuts the alternative — 254
        // real allocations to legitimately fill the leaf — at the cost of
        // leaving inobt vs actual-inodes inconsistent; a follow-up `run_fsck`
        // is therefore omitted. The growth ALGORITHM is what's under test.
        use crate::fs::filesystem::EditableFilesystem;
        use crate::fs::xfs::ag::XfsAgi;
        use crate::fs::xfs::types::XFS_INODES_PER_CHUNK;

        let img = load_fixture().to_vec();
        let mut cursor = Cursor::new(img);

        let (sb_clone, agi_byte0, sectsize, blocksize, agi_before, root_agbno) = {
            let fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            let sb = fs.superblock().clone();
            let sectsize = sb.sectsize as u64;
            let blocksize = sb.blocksize as u64;
            let agi_byte0 = 2 * sectsize;
            let mut agi_sec = vec![0u8; sectsize as usize];
            read_at_aligned(
                &mut Cursor::new(cursor.get_ref().as_slice()),
                agi_byte0,
                sectsize,
                &mut agi_sec,
            )
            .expect("read AGI 0");
            let agi = XfsAgi::parse(&agi_sec).expect("parse AGI 0");
            let root = agi.root;
            (sb, agi_byte0, sectsize, blocksize, agi, root)
        };
        assert_eq!(
            agi_before.level, 1,
            "fixture starts with a single-level inobt"
        );

        let bs = blocksize as usize;
        let max_leaf = (bs - 16) / 16; // INOBT_HDR_LEN, INOBT_REC_SIZE

        // Read the existing root leaf so we can preserve real records and
        // their start_aginos.
        let leaf_byte = root_agbno as u64 * blocksize;
        let mut leaf = vec![0u8; bs];
        read_at_aligned(
            &mut Cursor::new(cursor.get_ref().as_slice()),
            leaf_byte,
            blocksize,
            &mut leaf,
        )
        .expect("read leaf");
        let existing_numrecs = BigEndian::read_u16(&leaf[6..8]) as usize;
        assert!(
            existing_numrecs < max_leaf,
            "fixture leaf should not already be full"
        );

        let mut existing: Vec<(u32, u32, u64)> = Vec::with_capacity(existing_numrecs);
        let mut max_existing_start: u32 = 0;
        for r in 0..existing_numrecs {
            let off = 16 + r * 16;
            let s = BigEndian::read_u32(&leaf[off..off + 4]);
            let fc = BigEndian::read_u32(&leaf[off + 4..off + 8]);
            let free = BigEndian::read_u64(&leaf[off + 8..off + 16]);
            existing.push((s, fc, free));
            if s > max_existing_start {
                max_existing_start = s;
            }
        }

        // Phantom records: start_aginos strictly above the existing chunks
        // and well below where `alloc_blocks_aligned`'s tail-anchored carve
        // will land the new chunk. Phantoms claim inodes in the inobt
        // accounting (forcing numrecs == max_leaf) but no backing blocks,
        // so freespace is left untouched.
        let phantom_count = max_leaf - existing_numrecs;
        let phantom_stride: u32 = XFS_INODES_PER_CHUNK as u32; // 64
        let phantom_base: u32 =
            (max_existing_start + phantom_stride * 2).next_multiple_of(phantom_stride);
        let mut all_records: Vec<(u32, u32, u64)> = Vec::with_capacity(max_leaf);
        all_records.extend(existing.iter().copied());
        for i in 0..phantom_count {
            all_records.push((phantom_base + i as u32 * phantom_stride, 0, 0));
        }
        all_records.sort_by_key(|&(s, _, _)| s);
        BigEndian::write_u16(&mut leaf[6..8], max_leaf as u16);
        for (i, &(s, fc, free)) in all_records.iter().enumerate() {
            let off = 16 + i * 16;
            BigEndian::write_u32(&mut leaf[off..off + 4], s);
            BigEndian::write_u32(&mut leaf[off + 4..off + 8], fc);
            BigEndian::write_u64(&mut leaf[off + 8..off + 16], free);
        }
        {
            let writer = cursor.get_mut();
            let lo = leaf_byte as usize;
            writer[lo..lo + bs].copy_from_slice(&leaf);
        }

        // Patch AGI count and sb_icount to match the forge: each phantom
        // claims 64 inodes, all allocated (freecount unchanged).
        let phantom_inodes = (phantom_count as u32) * XFS_INODES_PER_CHUNK as u32;
        let new_count_pre = agi_before.count + phantom_inodes;
        let mut agi_sec = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut Cursor::new(cursor.get_ref().as_slice()),
            agi_byte0,
            sectsize,
            &mut agi_sec,
        )
        .expect("read AGI for patch");
        BigEndian::write_u32(&mut agi_sec[16..20], new_count_pre);
        {
            let writer = cursor.get_mut();
            let lo = agi_byte0 as usize;
            writer[lo..lo + sectsize as usize].copy_from_slice(&agi_sec);
        }
        let mut primary = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut Cursor::new(cursor.get_ref().as_slice()),
            0,
            sectsize,
            &mut primary,
        )
        .expect("read primary for patch");
        let sb_icount_pre = BigEndian::read_u64(&primary[128..136]);
        let sb_ifree_pre = BigEndian::read_u64(&primary[136..144]);
        BigEndian::write_u64(
            &mut primary[128..136],
            sb_icount_pre + phantom_inodes as u64,
        );
        {
            let writer = cursor.get_mut();
            writer[0..sectsize as usize].copy_from_slice(&primary);
        }

        // Drive the new-chunk path. A full root leaf must route through the
        // multi-level grow branch.
        {
            let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("open editable");
            fs.alloc_new_inode_chunk(&sb_clone)
                .expect("alloc_new_inode_chunk in multi-level grow path");
            fs.sync_metadata().expect("sync");
        }

        // AGI: level >= 2 (was 1), root moved, count/freecount += 64.
        let mut agi_after_sec = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut Cursor::new(cursor.get_ref().as_slice()),
            agi_byte0,
            sectsize,
            &mut agi_after_sec,
        )
        .expect("read AGI 0 after");
        let agi_after = XfsAgi::parse(&agi_after_sec).expect("parse AGI 0 after");
        assert!(
            agi_after.level >= 2,
            "AGI level should be >= 2 after multi-level grow; got {}",
            agi_after.level
        );
        assert_ne!(
            agi_after.root, root_agbno,
            "AGI root should have moved off the old single-leaf block"
        );
        assert_eq!(
            agi_after.count,
            new_count_pre + XFS_INODES_PER_CHUNK as u32,
            "AGI count should bump by 64 (one new all-free chunk)",
        );
        assert_eq!(
            agi_after.freecount,
            agi_before.freecount + XFS_INODES_PER_CHUNK as u32,
            "AGI freecount should bump by 64 (one new all-free chunk)",
        );

        // sb_icount += 64, sb_ifree += 64.
        let mut primary_after = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut Cursor::new(cursor.get_ref().as_slice()),
            0,
            sectsize,
            &mut primary_after,
        )
        .expect("read primary after");
        assert_eq!(
            BigEndian::read_u64(&primary_after[128..136]),
            sb_icount_pre + phantom_inodes as u64 + XFS_INODES_PER_CHUNK as u64,
            "sb_icount should bump by 64",
        );
        assert_eq!(
            BigEndian::read_u64(&primary_after[136..144]),
            sb_ifree_pre + XFS_INODES_PER_CHUNK as u64,
            "sb_ifree should bump by 64",
        );

        // Walk the new multi-level tree and gather all leaf records. There
        // should be max_leaf + 1 records (existing real + phantoms + the new
        // all-free chunk) and exactly one all-free record.
        let mut fs = XfsFilesystem::open(&mut cursor, 0).expect("reopen");
        let leaves = fs
            .collect_inobt_leaf_blocks(&sb_clone, 0, agi_after.root)
            .expect("walk new inobt");
        assert!(
            leaves.len() >= 2,
            "multi-level tree should have >= 2 leaves; got {}",
            leaves.len()
        );

        let mut all_after: Vec<(u32, u32, u64)> = Vec::new();
        for leaf_agbno in &leaves {
            let leaf_fsblock = *leaf_agbno as u64; // AG 0
            let mut buf = vec![0u8; bs];
            fs.read_fsblock(leaf_fsblock, &mut buf).expect("read leaf");
            let nr = BigEndian::read_u16(&buf[6..8]) as usize;
            for r in 0..nr {
                let off = 16 + r * 16;
                let s = BigEndian::read_u32(&buf[off..off + 4]);
                let fc = BigEndian::read_u32(&buf[off + 4..off + 8]);
                let free = BigEndian::read_u64(&buf[off + 8..off + 16]);
                all_after.push((s, fc, free));
            }
        }
        assert_eq!(
            all_after.len(),
            max_leaf + 1,
            "new tree should contain old records + new all-free chunk"
        );
        let all_free_records: Vec<&(u32, u32, u64)> = all_after
            .iter()
            .filter(|&&(_, fc, free)| fc == XFS_INODES_PER_CHUNK as u32 && free == u64::MAX)
            .collect();
        assert_eq!(
            all_free_records.len(),
            1,
            "exactly one all-free chunk should be in the new tree"
        );

        // Each individual leaf must be internally sorted by start_agino; the
        // DFS walker visits leaves in stack-pop (right-to-left) order, so we
        // check each leaf independently rather than the gathered sequence.
        for leaf_agbno in &leaves {
            let leaf_fsblock = *leaf_agbno as u64;
            let mut buf = vec![0u8; bs];
            fs.read_fsblock(leaf_fsblock, &mut buf).expect("read leaf");
            let nr = BigEndian::read_u16(&buf[6..8]) as usize;
            let mut prev: i64 = -1;
            for r in 0..nr {
                let off = 16 + r * 16;
                let s = BigEndian::read_u32(&buf[off..off + 4]);
                assert!(
                    (s as i64) > prev,
                    "leaf {leaf_agbno}: records must be sorted by start_agino"
                );
                prev = s as i64;
            }
        }

        // Globally, the multiset of records survives the rebuild. Sort and
        // verify the global set is also strictly increasing (no duplicates).
        let mut sorted = all_after.clone();
        sorted.sort_by_key(|&(s, _, _)| s);
        let mut prev: i64 = -1;
        for &(s, _, _) in &sorted {
            assert!(
                (s as i64) > prev,
                "globally: records must be unique start_aginos"
            );
            prev = s as i64;
        }
    }

    #[test]
    fn dir1_dispatch_uses_dir1_shortform_parser() {
        // Patch the fixture's versionnum to clear the DIRV2 bit, then assert
        // the dir1 dispatch path is taken. The fixture's root inode is dir2
        // shortform on disk; under the dir1 parser the byte layout no longer
        // makes sense, so we expect a Parse error (not an Unsupported one) —
        // proof that we routed through `parse_shortform` in dir1.rs.
        let mut img = load_fixture().to_vec();
        img[100..102].copy_from_slice(&0x94a4u16.to_be_bytes());
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        assert_eq!(fs.dir_format, DirFormat::Dir1);
        let root = fs.root().expect("root");
        // Either we successfully (mis-)parse some entries — those will
        // have invalid inumbers and child_entry() will surface fallback
        // placeholders — or we hit a Parse error. Either is acceptable
        // proof that the dir1 path executed. What must NOT happen is
        // dir2 succeeding silently against dir1 bytes.
        if let Ok(entries) = fs.list_directory(&root) {
            // The (mis-)parse will yield entries whose names are not the
            // expected fixture names — verify we didn't accidentally
            // produce the dir2 result set.
            let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
            assert!(
                !names.contains(&"hello.txt"),
                "dir1 dispatch unexpectedly produced dir2 names: {names:?}"
            );
        }
    }
}
