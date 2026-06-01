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
pub mod dir1;
pub mod dir2;
pub mod fsck;
pub mod inode;
pub mod repair;
pub mod sb;
pub mod symlink;
pub mod types;

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
    fn read_inode_buf(&mut self, ino: u64) -> Result<(XfsDinodeCore, Vec<u8>), FilesystemError> {
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
    fn decode_data_extents(
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

    /// Read the bytes of one filesystem block at `fsblock` into `out`.
    fn read_fsblock(&mut self, fsblock: u64, out: &mut [u8]) -> Result<(), FilesystemError> {
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
            let mut fs = XfsFilesystem::open(Cursor::new(img.clone()), 0).expect("open");
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
            let mut fs = XfsFilesystem::open(Cursor::new(img.clone()), 0).expect("open");
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
