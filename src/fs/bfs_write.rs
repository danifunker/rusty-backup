//! BFS writing — block allocator, inode construction, and B+tree maintenance.
//!
//! Reading lives in [`crate::fs::bfs`]; this module adds the
//! [`EditableFilesystem`] half.
//!
//! Three facts shape everything here:
//!
//! - **The bitmap says allocated, not free.** A set bit means the block is in
//!   use — the opposite of AFFS / PFS3 / SFS. Blocks are indexed into an array
//!   of 32-bit words in the volume's byte order, LSB first within a word.
//! - **A `block_run` cannot cross an allocation group**, because `start` is a
//!   `u16` counted from the group base. Every allocation is therefore made
//!   inside one group, and a long file simply gets more runs.
//! - **We do not write the journal.** BFS logs metadata, and replaying a stale
//!   log over our changes would corrupt the volume, so writes are refused
//!   unless `log_start == log_end` (nothing pending) and the volume is clean.
//!
//! Directory B+trees are read whole, mutated in memory, and written back.
//! Leaf splits promote a separator into the parent and grow a new root when the
//! old one splits; a leaf emptied by deletion is left linked in place rather
//! than merged, which is a legal (if untidy) B+tree and keeps deletion O(1) in
//! tree height. See `src/fs/README.md` § "BFS".

use std::io::{Read, Seek, SeekFrom, Write};

use super::bfs::{
    key_lengths_offset, validate_bfs_name, values_offset, BfsEndian, BfsFilesystem, BfsInode,
    BlockRun, DataStream, BFS_CLEAN, BFS_INODE_MAGIC, BPLUSTREE_MAGIC,
};
use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

/// `INODE_IN_USE | INODE_LOGGED` — what BeOS stamps on a live inode.
const INODE_FLAGS_DEFAULT: u32 = 0x0000_0001 | 0x0000_0008;
const S_IFREG: u32 = 0o100_000;
const S_IFDIR: u32 = 0o040_000;
const S_IFLNK: u32 = 0o120_000;

/// `'CSTR'`, and the one-byte `small_data` name a file's own name is filed
/// under.
const FILE_NAME_TYPE: u32 = 0x4353_5452;
const FILE_NAME_NAME: u8 = 0x13;

const INODE_OFF_DATA: usize = 0x48;
const INODE_OFF_SMALL_DATA: usize = 0xE8;
const DATA_STREAM_SIZE: usize = 144;
const NUM_DIRECT_BLOCKS: usize = 12;
const BLOCK_RUN_SIZE: usize = 8;
const BPLUSTREE_NODE_HEADER: usize = 28;
const BPLUSTREE_HEADER_SIZE: usize = 40;
const BPLUSTREE_NULL: i64 = -1;
const INODE_TIME_SHIFT: u32 = 16;
/// A run's `len` is a `u16`, so no single extent can be longer.
const MAX_RUN_LEN: u64 = 65535;
/// Direct-only streams cap out here; past it a file needs the indirect level.
const MAX_DIRECT_RUNS: usize = NUM_DIRECT_BLOCKS;

impl<R: Read + Write + Seek + Send> BfsFilesystem<R> {
    // ---- raw block I/O ----

    pub(crate) fn write_blocks(&mut self, block: u64, data: &[u8]) -> Result<(), FilesystemError> {
        let bs = self.sb.block_size as u64;
        let count = (data.len() as u64).div_ceil(bs);
        if block.saturating_add(count) > self.sb.num_blocks as u64 {
            return Err(FilesystemError::InvalidData(format!(
                "bfs: write of {count} blocks at {block} runs past the volume"
            )));
        }
        let byte = self.block_byte(block);
        self.reader.seek(SeekFrom::Start(byte))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    /// Refuse to touch a volume whose journal still has work in it — we do not
    /// replay or extend the log, and BeOS would roll it forward over us.
    pub(crate) fn ensure_writable(&self) -> Result<(), FilesystemError> {
        if !self.sb.log_is_empty() {
            return Err(FilesystemError::Unsupported(
                "BFS: the volume's journal is not empty (log_start != log_end); mount and \
                 unmount it cleanly in BeOS/Haiku, or run chkbfs, before editing"
                    .into(),
            ));
        }
        if self.sb.flags != BFS_CLEAN {
            return Err(FilesystemError::Unsupported(
                "BFS: the volume is marked dirty ('DIRT'); run chkbfs on it before editing".into(),
            ));
        }
        Ok(())
    }

    // ---- allocation bitmap ----

    fn bitmap_blocks(&self) -> u64 {
        self.sb.num_ags as u64 * self.sb.blocks_per_ag as u64
    }

    fn read_bitmap(&mut self) -> Result<Vec<u8>, FilesystemError> {
        self.read_blocks(1, self.bitmap_blocks())
    }

    fn write_bitmap(&mut self, bitmap: &[u8]) -> Result<(), FilesystemError> {
        self.write_blocks(1, bitmap)
    }

    fn bit_is_set(&self, bitmap: &[u8], block: u64) -> bool {
        let word_at = (block / 32) as usize * 4;
        if word_at + 4 > bitmap.len() {
            return true;
        }
        let word = self.sb.endian.read_u32(bitmap, word_at);
        word & (1 << (block % 32)) != 0
    }

    fn set_bits(&self, bitmap: &mut [u8], block: u64, count: u64, on: bool) {
        for b in block..block + count {
            let word_at = (b / 32) as usize * 4;
            if word_at + 4 > bitmap.len() {
                continue;
            }
            let mut word = self.sb.endian.read_u32(bitmap, word_at);
            let mask = 1u32 << (b % 32);
            if on {
                word |= mask;
            } else {
                word &= !mask;
            }
            self.sb.endian.put_u32(bitmap, word_at, word);
        }
    }

    /// Find `count` contiguous free blocks that stay inside one allocation
    /// group, preferring the group `hint` sits in.
    fn find_free_run(&self, bitmap: &[u8], count: u64, hint: u64) -> Option<u64> {
        let ag_blocks = 1u64 << self.sb.ag_shift;
        let total = self.sb.num_blocks as u64;
        let first = self.sb.first_data_block();
        let ags = self.sb.num_ags as u64;
        let start_ag = (hint >> self.sb.ag_shift).min(ags.saturating_sub(1));

        for step in 0..ags {
            let ag = (start_ag + step) % ags;
            let ag_start = (ag * ag_blocks).max(first);
            let ag_end = ((ag + 1) * ag_blocks).min(total);
            if ag_start >= ag_end || ag_end - ag_start < count {
                continue;
            }
            let mut run_start = ag_start;
            let mut run_len = 0u64;
            for b in ag_start..ag_end {
                if self.bit_is_set(bitmap, b) {
                    run_start = b + 1;
                    run_len = 0;
                    continue;
                }
                run_len += 1;
                if run_len == count {
                    return Some(run_start);
                }
            }
        }
        None
    }

    /// Allocate `count` blocks as one run. Returns the run and marks the bitmap.
    fn alloc_run(&mut self, count: u64, hint: u64) -> Result<BlockRun, FilesystemError> {
        if count == 0 || count > MAX_RUN_LEN {
            return Err(FilesystemError::InvalidData(format!(
                "bfs: cannot allocate a {count}-block run (limit {MAX_RUN_LEN})"
            )));
        }
        let mut bitmap = self.read_bitmap()?;
        let start = self.find_free_run(&bitmap, count, hint).ok_or_else(|| {
            FilesystemError::DiskFull(format!(
                "bfs: no free run of {count} blocks inside a single allocation group"
            ))
        })?;
        self.set_bits(&mut bitmap, start, count, true);
        self.write_bitmap(&bitmap)?;
        self.sb.used_blocks += count as i64;
        self.sb_dirty = true;
        Ok(self.block_to_run(start, count as u16))
    }

    /// Allocate `count` blocks as however many runs it takes.
    fn alloc_runs(&mut self, count: u64, hint: u64) -> Result<Vec<BlockRun>, FilesystemError> {
        let mut runs = Vec::new();
        let mut remaining = count;
        let mut hint = hint;
        while remaining > 0 {
            // Back off to smaller runs rather than failing outright when the
            // volume is fragmented — BFS files are extent lists, not one span.
            let mut want = remaining.min(MAX_RUN_LEN);
            let run = loop {
                match self.alloc_run(want, hint) {
                    Ok(r) => break r,
                    Err(FilesystemError::DiskFull(_)) if want > 1 => want = (want / 2).max(1),
                    Err(e) => {
                        for r in &runs {
                            let _ = self.free_run(*r);
                        }
                        return Err(e);
                    }
                }
            };
            hint = run.to_block(self.sb.ag_shift) + run.len as u64;
            remaining -= run.len as u64;
            runs.push(run);
        }
        Ok(runs)
    }

    fn free_run(&mut self, run: BlockRun) -> Result<(), FilesystemError> {
        if run.len == 0 {
            return Ok(());
        }
        let mut bitmap = self.read_bitmap()?;
        self.set_bits(
            &mut bitmap,
            run.to_block(self.sb.ag_shift),
            run.len as u64,
            false,
        );
        self.write_bitmap(&bitmap)?;
        self.sb.used_blocks -= run.len as i64;
        self.sb_dirty = true;
        Ok(())
    }

    fn block_to_run(&self, block: u64, len: u16) -> BlockRun {
        let ag_mask = (1u64 << self.sb.ag_shift) - 1;
        BlockRun {
            allocation_group: (block >> self.sb.ag_shift) as u32,
            start: (block & ag_mask) as u16,
            len,
        }
    }

    /// Flush the in-memory superblock counters back to disk.
    pub(crate) fn sync_superblock(&mut self) -> Result<(), FilesystemError> {
        if !self.sb_dirty {
            return Ok(());
        }
        let at = self.partition_offset + self.sb_offset;
        self.reader.seek(SeekFrom::Start(at))?;
        let mut buf = [0u8; 164];
        self.reader.read_exact(&mut buf)?;
        self.sb.endian.put_i64(&mut buf, 56, self.sb.used_blocks);
        self.reader.seek(SeekFrom::Start(at))?;
        self.reader.write_all(&buf)?;
        self.sb_dirty = false;
        Ok(())
    }

    // ---- data streams ----

    /// Lay `runs` into a fresh stream, filling direct slots then the indirect
    /// array. Sets every `max_*_range` the way BeOS does.
    fn build_stream(
        &mut self,
        runs: &[BlockRun],
        size: u64,
        hint: u64,
    ) -> Result<DataStream, FilesystemError> {
        let bs = self.sb.block_size as u64;
        let mut ds = DataStream {
            size: size as i64,
            ..Default::default()
        };
        let direct_count = runs.len().min(MAX_DIRECT_RUNS);
        let mut covered = 0u64;
        for (i, run) in runs.iter().take(direct_count).enumerate() {
            ds.direct[i] = *run;
            covered += run.len as u64 * bs;
        }
        ds.max_direct_range = covered as i64;
        ds.max_indirect_range = covered as i64;
        ds.max_double_indirect_range = covered as i64;

        if runs.len() > MAX_DIRECT_RUNS {
            let rest = &runs[MAX_DIRECT_RUNS..];
            let runs_per_block = bs as usize / BLOCK_RUN_SIZE;
            let need = (rest.len().div_ceil(runs_per_block)) as u64;
            let indirect = self.alloc_run(need, hint)?;
            let mut raw = vec![0u8; (need * bs) as usize];
            for (i, run) in rest.iter().enumerate() {
                run.write(self.sb.endian, &mut raw, i * BLOCK_RUN_SIZE);
                covered += run.len as u64 * bs;
            }
            self.write_blocks(indirect.to_block(self.sb.ag_shift), &raw)?;
            ds.indirect = indirect;
            ds.max_indirect_range = covered as i64;
            ds.max_double_indirect_range = covered as i64;
        }
        Ok(ds)
    }

    /// Release every block a stream owns, including its indirect array.
    fn free_stream(&mut self, ds: &DataStream) -> Result<(), FilesystemError> {
        for run in &ds.direct {
            if run.len > 0 {
                self.free_run(*run)?;
            }
        }
        if ds.indirect.len > 0 {
            let bs = self.sb.block_size as u64;
            let raw = self.read_blocks(
                ds.indirect.to_block(self.sb.ag_shift),
                ds.indirect.len as u64,
            )?;
            for off in (0..raw.len()).step_by(BLOCK_RUN_SIZE) {
                let run = BlockRun::read(self.sb.endian, &raw, off);
                if run.len > 0 {
                    self.free_run(run)?;
                }
            }
            let _ = bs;
            self.free_run(ds.indirect)?;
        }
        if ds.double_indirect.len > 0 {
            return Err(FilesystemError::Unsupported(
                "bfs: freeing a double-indirect stream is not implemented".into(),
            ));
        }
        Ok(())
    }

    /// Write `data` over a stream's blocks, zero-padding the last block.
    fn write_stream_bytes(&mut self, ds: &DataStream, data: &[u8]) -> Result<(), FilesystemError> {
        let bs = self.sb.block_size as usize;
        let extents = self.stream_extents(ds)?;
        let mut pos = 0usize;
        for (block, count) in extents {
            if pos >= data.len() {
                break;
            }
            let span = count as usize * bs;
            let take = span.min(data.len() - pos);
            let mut buf = vec![0u8; take.div_ceil(bs) * bs];
            buf[..take].copy_from_slice(&data[pos..pos + take]);
            self.write_blocks(block, &buf)?;
            pos += take;
        }
        if pos < data.len() {
            return Err(FilesystemError::InvalidData(
                "bfs: stream is shorter than the data written into it".into(),
            ));
        }
        Ok(())
    }

    // ---- inodes ----

    fn inode_blocks(&self) -> u64 {
        (self.sb.inode_size as u64)
            .div_ceil(self.sb.block_size as u64)
            .max(1)
    }

    /// Serialise an inode, including its inline name attribute.
    fn encode_inode(&self, inode: &BfsInode, name: &str) -> Vec<u8> {
        let e = self.sb.endian;
        let mut buf = vec![0u8; self.sb.inode_size as usize];
        e.put_u32(&mut buf, 0, BFS_INODE_MAGIC);
        inode.inode_num.write(e, &mut buf, 0x04);
        e.put_u32(&mut buf, 0x0C, inode.uid);
        e.put_u32(&mut buf, 0x10, inode.gid);
        e.put_u32(&mut buf, 0x14, inode.mode);
        e.put_u32(&mut buf, 0x18, inode.flags);
        e.put_i64(&mut buf, 0x1C, inode.create_time);
        e.put_i64(&mut buf, 0x24, inode.last_modified_time);
        inode.parent.write(e, &mut buf, 0x2C);
        inode.attributes.write(e, &mut buf, 0x34);
        e.put_u32(&mut buf, 0x3C, inode.type_code);
        e.put_u32(&mut buf, 0x40, self.sb.inode_size);
        // The union arm holds a symlink target only on an inline symlink; on
        // anything else those bytes are the data stream itself.
        if inode.mode & 0o170_000 == S_IFLNK && !inode.short_symlink.is_empty() {
            let n = inode.short_symlink.len().min(DATA_STREAM_SIZE);
            buf[INODE_OFF_DATA..INODE_OFF_DATA + n].copy_from_slice(&inode.short_symlink[..n]);
        } else {
            inode.data.write(e, &mut buf, INODE_OFF_DATA);
        }

        // The name is a `small_data` of type 'CSTR' under the one-byte key 0x13:
        // header(8) name(1) pad(3) data(len) NUL.
        let bytes = name.as_bytes();
        let off = INODE_OFF_SMALL_DATA;
        let entry_end = off + 8 + 1 + 3 + bytes.len() + 1;
        if entry_end <= buf.len() {
            e.put_u32(&mut buf, off, FILE_NAME_TYPE);
            e.put_u16(&mut buf, off + 4, 1);
            e.put_u16(&mut buf, off + 6, bytes.len() as u16);
            buf[off + 8] = FILE_NAME_NAME;
            let at = off + 8 + 1 + 3;
            buf[at..at + bytes.len()].copy_from_slice(bytes);
        }
        buf
    }

    fn write_inode(&mut self, inode: &BfsInode, name: &str) -> Result<(), FilesystemError> {
        let buf = self.encode_inode(inode, name);
        self.write_blocks(inode.block, &buf)
    }

    /// Rewrite an existing inode, preserving whatever `small_data` it carries.
    fn rewrite_inode_preserving_name(&mut self, inode: &BfsInode) -> Result<(), FilesystemError> {
        let name = inode.name().unwrap_or_default();
        self.write_inode(inode, &name)
    }

    // ---- directory trees ----

    /// Read a directory's whole B+tree into memory.
    fn load_tree(&mut self, dir: &BfsInode) -> Result<BTree, FilesystemError> {
        let data = self.read_stream(&dir.data, 0, dir.data.size.max(0) as usize)?;
        BTree::open(self.sb.endian, data)
    }

    /// Write a tree back, growing the directory's stream first when it needs
    /// more blocks than it currently owns.
    fn store_tree(&mut self, dir: &mut BfsInode, tree: &BTree) -> Result<(), FilesystemError> {
        let bs = self.sb.block_size as u64;
        let want_blocks = (tree.data.len() as u64).div_ceil(bs);
        let have_blocks = self
            .stream_extents(&dir.data)?
            .iter()
            .map(|(_, c)| *c)
            .sum::<u64>();
        if want_blocks > have_blocks {
            let hint = dir.block;
            let extra = self.alloc_runs(want_blocks - have_blocks, hint)?;
            let mut runs: Vec<BlockRun> = dir
                .data
                .direct
                .iter()
                .copied()
                .filter(|r| r.len > 0)
                .collect();
            if dir.data.indirect.len > 0 {
                let raw = self.read_blocks(
                    dir.data.indirect.to_block(self.sb.ag_shift),
                    dir.data.indirect.len as u64,
                )?;
                for off in (0..raw.len()).step_by(BLOCK_RUN_SIZE) {
                    let run = BlockRun::read(self.sb.endian, &raw, off);
                    if run.len > 0 {
                        runs.push(run);
                    }
                }
                let old = dir.data.indirect;
                self.free_run(old)?;
            }
            runs.extend(extra);
            dir.data = self.build_stream(&runs, tree.data.len() as u64, dir.block)?;
        } else {
            dir.data.size = tree.data.len() as i64;
        }
        self.write_stream_bytes(&dir.data, &tree.data)?;
        self.rewrite_inode_preserving_name(dir)
    }

    /// Build the two-block tree a brand-new directory starts life with.
    fn new_directory_tree(&self, self_block: u64, parent_block: u64) -> BTree {
        let node_size = self.sb.block_size as usize;
        let mut tree = BTree::blank(self.sb.endian, node_size);
        tree.leaf_insert_raw(node_size as i64, ".", self_block as i64);
        tree.leaf_insert_raw(node_size as i64, "..", parent_block as i64);
        tree
    }

    fn fresh_inode(
        &self,
        block: u64,
        mode: u32,
        uid: u32,
        gid: u32,
        parent: BlockRun,
        mtime: i64,
    ) -> BfsInode {
        BfsInode {
            block,
            inode_num: self.block_to_run(block, self.inode_blocks() as u16),
            uid,
            gid,
            mode,
            flags: INODE_FLAGS_DEFAULT,
            create_time: mtime,
            last_modified_time: mtime,
            parent,
            attributes: BlockRun::default(),
            type_code: 0,
            inode_size: self.sb.inode_size,
            data: DataStream::default(),
            short_symlink: Vec::new(),
            small_data: Vec::new(),
        }
    }

    /// Shared body of `create_file` / `create_directory` / `create_symlink`.
    fn create_entry(
        &mut self,
        parent: &FileEntry,
        name: &str,
        attrs: NewEntryAttrs,
        payload: Payload<'_>,
    ) -> Result<FileEntry, FilesystemError> {
        let NewEntryAttrs {
            mode,
            uid,
            gid,
            mtime,
        } = attrs;
        self.ensure_writable()?;
        validate_bfs_name(name)?;
        let mut parent_inode = self.read_inode(parent.location)?;
        if !parent_inode.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        let mut tree = self.load_tree(&parent_inode)?;
        if tree.find(name).is_some() {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }

        let inode_run = self.alloc_run(self.inode_blocks(), parent.location)?;
        let inode_block = inode_run.to_block(self.sb.ag_shift);
        let parent_run = self.block_to_run(parent.location, self.inode_blocks() as u16);
        let mut inode = self.fresh_inode(inode_block, mode, uid, gid, parent_run, mtime);

        // Roll the allocations back on any failure so a half-made entry never
        // leaks blocks that no directory references.
        let built = (|| -> Result<u64, FilesystemError> {
            match payload {
                Payload::Bytes(data) => {
                    if data.is_empty() {
                        return Ok(0);
                    }
                    let bs = self.sb.block_size as u64;
                    let need = (data.len() as u64).div_ceil(bs);
                    let runs = self.alloc_runs(need, inode_block)?;
                    inode.data = self.build_stream(&runs, data.len() as u64, inode_block)?;
                    self.write_stream_bytes(&inode.data, data)?;
                    Ok(data.len() as u64)
                }
                Payload::ShortSymlink(target) => {
                    let mut raw = vec![0u8; DATA_STREAM_SIZE];
                    raw[..target.len()].copy_from_slice(target);
                    inode.short_symlink = raw;
                    Ok(target.len() as u64)
                }
                Payload::Directory => {
                    let tree = self.new_directory_tree(inode_block, parent.location);
                    let bs = self.sb.block_size as u64;
                    let need = (tree.data.len() as u64).div_ceil(bs);
                    let runs = self.alloc_runs(need, inode_block)?;
                    inode.data = self.build_stream(&runs, tree.data.len() as u64, inode_block)?;
                    self.write_stream_bytes(&inode.data, &tree.data)?;
                    Ok(0)
                }
            }
        })();
        let size = match built {
            Ok(n) => n,
            Err(e) => {
                let _ = self.free_run(inode_run);
                return Err(e);
            }
        };

        if let Err(e) = self.write_inode(&inode, name) {
            let _ = self.free_stream(&inode.data);
            let _ = self.free_run(inode_run);
            return Err(e);
        }

        tree.insert(name, inode_block as i64)?;
        if let Err(e) = self.store_tree(&mut parent_inode, &tree) {
            let _ = self.free_stream(&inode.data);
            let _ = self.free_run(inode_run);
            return Err(e);
        }
        self.sync_superblock()?;

        let parent_path = if parent.path == "/" {
            String::new()
        } else {
            parent.path.clone()
        };
        let path = format!("{parent_path}/{name}");
        let mut entry = if mode & 0o170_000 == S_IFDIR {
            FileEntry::new_directory(name.to_string(), path, inode_block)
        } else if mode & 0o170_000 == S_IFLNK {
            let target =
                String::from_utf8_lossy(&inode.short_symlink[..size as usize]).into_owned();
            FileEntry::new_symlink(name.to_string(), path, size, inode_block, target)
        } else {
            FileEntry::new_file(name.to_string(), path, size, inode_block)
        };
        entry.mode = Some(mode & 0xFFFF);
        entry.uid = Some(uid);
        entry.gid = Some(gid);
        Ok(entry)
    }
}

/// Mode, ownership, and timestamp for a newly created inode.
struct NewEntryAttrs {
    mode: u32,
    uid: u32,
    gid: u32,
    /// Already shifted into BFS's `seconds << 16` form.
    mtime: i64,
}

/// What a freshly created inode is filled with.
enum Payload<'a> {
    Bytes(&'a [u8]),
    ShortSymlink(&'a [u8]),
    Directory,
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for BfsFilesystem<R> {
    fn as_filesystem(&self) -> &dyn Filesystem {
        self
    }
    fn as_filesystem_mut(&mut self) -> &mut dyn Filesystem {
        self
    }

    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn Read,
        data_len: u64,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let mut bytes = Vec::with_capacity(data_len.min(64 << 20) as usize);
        data.take(data_len).read_to_end(&mut bytes)?;
        if bytes.len() as u64 != data_len {
            return Err(FilesystemError::InvalidData(format!(
                "bfs create_file: source gave {} bytes, expected {data_len}",
                bytes.len()
            )));
        }
        let mtime = super::times::resolve_or_now(options.unix_times).mtime_or_now() as i64;
        self.create_entry(
            parent,
            name,
            NewEntryAttrs {
                mode: options.mode.unwrap_or(S_IFREG | 0o644) | S_IFREG,
                uid: options.uid.unwrap_or(0),
                gid: options.gid.unwrap_or(0),
                mtime: mtime << INODE_TIME_SHIFT,
            },
            Payload::Bytes(&bytes),
        )
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let mtime = super::times::resolve_or_now(options.unix_times).mtime_or_now() as i64;
        self.create_entry(
            parent,
            name,
            NewEntryAttrs {
                mode: options.mode.unwrap_or(S_IFDIR | 0o755) | S_IFDIR,
                uid: options.uid.unwrap_or(0),
                gid: options.gid.unwrap_or(0),
                mtime: mtime << INODE_TIME_SHIFT,
            },
            Payload::Directory,
        )
    }

    fn create_symlink(
        &mut self,
        parent: &FileEntry,
        name: &str,
        target: &str,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if target.len() >= DATA_STREAM_SIZE {
            return Err(FilesystemError::Unsupported(format!(
                "bfs: symlink target is {} bytes; only inline targets under {DATA_STREAM_SIZE} \
                 are supported (long symlinks need a data stream)",
                target.len()
            )));
        }
        let mtime = super::times::resolve_or_now(options.unix_times).mtime_or_now() as i64;
        self.create_entry(
            parent,
            name,
            NewEntryAttrs {
                mode: S_IFLNK | 0o777,
                uid: options.uid.unwrap_or(0),
                gid: options.gid.unwrap_or(0),
                mtime: mtime << INODE_TIME_SHIFT,
            },
            Payload::ShortSymlink(target.as_bytes()),
        )
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        self.ensure_writable()?;
        let mut parent_inode = self.read_inode(parent.location)?;
        if !parent_inode.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        let target = self.read_inode(entry.location)?;
        if target.is_directory() {
            let children = self.read_directory(&target)?;
            if !children.is_empty() {
                return Err(FilesystemError::InvalidData(format!(
                    "bfs: directory '{}' is not empty",
                    entry.path
                )));
            }
        }

        let mut tree = self.load_tree(&parent_inode)?;
        if !tree.remove(&entry.name)? {
            return Err(FilesystemError::NotFound(entry.path.clone()));
        }
        self.store_tree(&mut parent_inode, &tree)?;

        self.free_stream(&target.data)?;
        let inode_run = self.block_to_run(target.block, self.inode_blocks() as u16);
        self.free_run(inode_run)?;
        self.sync_superblock()
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        self.ensure_writable()?;
        validate_bfs_name(new_name)?;
        if new_name == entry.name {
            return Ok(());
        }
        let mut parent_inode = self.read_inode(parent.location)?;
        let mut tree = self.load_tree(&parent_inode)?;
        if tree.find(new_name).is_some() {
            return Err(FilesystemError::AlreadyExists(new_name.to_string()));
        }
        if !tree.remove(&entry.name)? {
            return Err(FilesystemError::NotFound(entry.path.clone()));
        }
        tree.insert(new_name, entry.location as i64)?;
        self.store_tree(&mut parent_inode, &tree)?;

        // The inode carries its own name in `small_data`; a stale copy there
        // is what makes a renamed file show its old name in BeOS's Tracker.
        let inode = self.read_inode(entry.location)?;
        self.write_inode(&inode, new_name)?;
        self.sync_superblock()
    }

    fn set_permissions(&mut self, entry: &FileEntry, mode: u32) -> Result<(), FilesystemError> {
        self.ensure_writable()?;
        let mut inode = self.read_inode(entry.location)?;
        inode.mode = (inode.mode & !0o7777) | (mode & 0o7777);
        self.rewrite_inode_preserving_name(&inode)
    }

    fn set_owner(&mut self, entry: &FileEntry, uid: u32, gid: u32) -> Result<(), FilesystemError> {
        self.ensure_writable()?;
        let mut inode = self.read_inode(entry.location)?;
        inode.uid = uid;
        inode.gid = gid;
        self.rewrite_inode_preserving_name(&inode)
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.sync_superblock()
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        let free = (self.sb.num_blocks - self.sb.used_blocks).max(0) as u64;
        Ok(free * self.sb.block_size as u64)
    }

    fn repair(&mut self) -> Result<super::fsck::RepairReport, FilesystemError> {
        self.repair_bfs()
    }
}

// ---- the B+tree ----

/// A directory's B+tree, held whole in memory while it is edited.
pub(crate) struct BTree {
    e: BfsEndian,
    pub(crate) data: Vec<u8>,
    node_size: usize,
}

impl BTree {
    fn open(e: BfsEndian, data: Vec<u8>) -> Result<Self, FilesystemError> {
        if data.len() < BPLUSTREE_HEADER_SIZE || e.read_u32(&data, 0) != BPLUSTREE_MAGIC {
            return Err(FilesystemError::Parse(
                "bfs: directory stream has no B+tree header".into(),
            ));
        }
        let node_size = e.read_u32(&data, 4) as usize;
        if node_size < BPLUSTREE_NODE_HEADER + 32 || node_size > data.len() {
            return Err(FilesystemError::Parse(format!(
                "bfs: B+tree node size {node_size} does not fit a {}-byte stream",
                data.len()
            )));
        }
        Ok(BTree { e, data, node_size })
    }

    /// A fresh header plus one empty leaf, which is what `mkdir` starts from.
    fn blank(e: BfsEndian, node_size: usize) -> Self {
        let mut data = vec![0u8; node_size * 2];
        e.put_u32(&mut data, 0, BPLUSTREE_MAGIC);
        e.put_u32(&mut data, 4, node_size as u32);
        e.put_u32(&mut data, 8, 1);
        e.put_u32(&mut data, 12, 0);
        e.put_i64(&mut data, 16, node_size as i64);
        e.put_i64(&mut data, 24, BPLUSTREE_NULL);
        e.put_i64(&mut data, 32, (node_size * 2) as i64);
        let mut tree = BTree { e, data, node_size };
        tree.init_node(node_size as i64, true);
        tree
    }

    fn root(&self) -> i64 {
        self.e.read_i64(&self.data, 16)
    }
    fn set_root(&mut self, v: i64) {
        self.e.put_i64(&mut self.data, 16, v);
    }
    fn free_head(&self) -> i64 {
        self.e.read_i64(&self.data, 24)
    }
    fn set_free_head(&mut self, v: i64) {
        self.e.put_i64(&mut self.data, 24, v);
    }
    fn set_levels(&mut self, v: u32) {
        self.e.put_u32(&mut self.data, 8, v);
    }
    fn levels(&self) -> u32 {
        self.e.read_u32(&self.data, 8)
    }
    fn set_maximum_size(&mut self, v: i64) {
        self.e.put_i64(&mut self.data, 32, v);
    }

    fn init_node(&mut self, off: i64, leaf: bool) {
        let at = off as usize;
        for b in &mut self.data[at..at + self.node_size] {
            *b = 0;
        }
        self.e.put_i64(&mut self.data, at, BPLUSTREE_NULL);
        self.e.put_i64(&mut self.data, at + 8, BPLUSTREE_NULL);
        self.e.put_i64(
            &mut self.data,
            at + 16,
            if leaf { BPLUSTREE_NULL } else { 0 },
        );
        self.e.put_u16(&mut self.data, at + 24, 0);
        self.e.put_u16(&mut self.data, at + 26, 0);
    }

    fn node(&self, off: i64) -> Result<NodeView, FilesystemError> {
        if off < 0 {
            return Err(FilesystemError::Parse("bfs: null B+tree node".into()));
        }
        let at = off as usize;
        if at + self.node_size > self.data.len() {
            return Err(FilesystemError::Parse(format!(
                "bfs: B+tree node at {at} runs past the {}-byte stream",
                self.data.len()
            )));
        }
        let n = &self.data[at..at + self.node_size];
        let count = self.e.read_u16(n, 24) as usize;
        let key_len = self.e.read_u16(n, 26) as usize;
        let values_off = values_offset(key_len, count);
        if BPLUSTREE_NODE_HEADER + key_len > self.node_size
            || values_off + count * 8 > self.node_size
        {
            return Err(FilesystemError::Parse(
                "bfs: B+tree node's arrays do not fit its node size".into(),
            ));
        }
        let lengths_off = key_lengths_offset(key_len);
        let mut keys = Vec::with_capacity(count);
        let mut values = Vec::with_capacity(count);
        let mut prev = 0usize;
        for i in 0..count {
            let end = self.e.read_u16(n, lengths_off + i * 2) as usize;
            if end < prev || end > key_len {
                return Err(FilesystemError::Parse(
                    "bfs: B+tree key-length array is not monotonic".into(),
                ));
            }
            keys.push(n[BPLUSTREE_NODE_HEADER + prev..BPLUSTREE_NODE_HEADER + end].to_vec());
            values.push(self.e.read_i64(n, values_off + i * 8));
            prev = end;
        }
        Ok(NodeView {
            offset: off,
            left: self.e.read_i64(n, 0),
            right: self.e.read_i64(n, 8),
            overflow: self.e.read_i64(n, 16),
            keys,
            values,
        })
    }

    fn store(&mut self, view: &NodeView) -> Result<(), FilesystemError> {
        let total_key_len: usize = view.keys.iter().map(|k| k.len()).sum();
        let need = values_offset(total_key_len, view.keys.len()) + view.keys.len() * 8;
        if need > self.node_size {
            return Err(FilesystemError::InvalidData(
                "bfs: B+tree node overfilled (caller should have split)".into(),
            ));
        }
        let at = view.offset as usize;
        let ns = self.node_size;
        for b in &mut self.data[at..at + ns] {
            *b = 0;
        }
        self.e.put_i64(&mut self.data, at, view.left);
        self.e.put_i64(&mut self.data, at + 8, view.right);
        self.e.put_i64(&mut self.data, at + 16, view.overflow);
        self.e
            .put_u16(&mut self.data, at + 24, view.keys.len() as u16);
        self.e
            .put_u16(&mut self.data, at + 26, total_key_len as u16);
        let lengths_off = key_lengths_offset(total_key_len);
        let values_off = values_offset(total_key_len, view.keys.len());
        let mut cursor = 0usize;
        for (i, key) in view.keys.iter().enumerate() {
            let dst = at + BPLUSTREE_NODE_HEADER + cursor;
            self.data[dst..dst + key.len()].copy_from_slice(key);
            cursor += key.len();
            self.e
                .put_u16(&mut self.data, at + lengths_off + i * 2, cursor as u16);
            self.e
                .put_i64(&mut self.data, at + values_off + i * 8, view.values[i]);
        }
        Ok(())
    }

    /// Bytes one node would need with these keys.
    fn size_with(keys: &[Vec<u8>]) -> usize {
        let total: usize = keys.iter().map(|k| k.len()).sum();
        values_offset(total, keys.len()) + keys.len() * 8
    }

    /// Take a node off the free list, or grow the stream by one node.
    fn alloc_node(&mut self, leaf: bool) -> Result<i64, FilesystemError> {
        let head = self.free_head();
        if head != BPLUSTREE_NULL {
            let next = self.node(head)?.left;
            self.set_free_head(next);
            self.init_node(head, leaf);
            return Ok(head);
        }
        let off = self.data.len() as i64;
        self.data.resize(self.data.len() + self.node_size, 0);
        self.init_node(off, leaf);
        self.set_maximum_size(self.data.len() as i64);
        Ok(off)
    }

    /// BFS orders string keys by bytes, then by length.
    fn cmp(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        let n = a.len().min(b.len());
        match a[..n].cmp(&b[..n]) {
            std::cmp::Ordering::Equal => a.len().cmp(&b.len()),
            other => other,
        }
    }

    /// Descend to the leaf a key belongs in, recording the internal nodes.
    fn descend(&self, key: &[u8]) -> Result<(NodeView, Vec<i64>), FilesystemError> {
        let mut path = Vec::new();
        let mut off = self.root();
        for _ in 0..64 {
            let node = self.node(off)?;
            if node.overflow == BPLUSTREE_NULL {
                return Ok((node, path));
            }
            path.push(off);
            let mut next = node.overflow;
            for (i, k) in node.keys.iter().enumerate() {
                if Self::cmp(key, k) != std::cmp::Ordering::Greater {
                    next = node.values[i];
                    break;
                }
            }
            off = next;
        }
        Err(FilesystemError::Parse(
            "bfs: B+tree descent exceeded 64 levels".into(),
        ))
    }

    pub(crate) fn find(&self, name: &str) -> Option<i64> {
        let key = name.as_bytes();
        let (leaf, _) = self.descend(key).ok()?;
        leaf.keys
            .iter()
            .position(|k| k.as_slice() == key)
            .map(|i| leaf.values[i])
    }

    /// Insert into a specific leaf without any split handling — only safe on a
    /// freshly built node, which is why it is private to `new_directory_tree`.
    fn leaf_insert_raw(&mut self, off: i64, name: &str, value: i64) {
        let mut node = self.node(off).expect("blank tree node");
        let key = name.as_bytes().to_vec();
        let at = node
            .keys
            .iter()
            .position(|k| Self::cmp(&key, k) == std::cmp::Ordering::Less)
            .unwrap_or(node.keys.len());
        node.keys.insert(at, key);
        node.values.insert(at, value);
        self.store(&node).expect("blank tree node has room");
    }

    pub(crate) fn insert(&mut self, name: &str, value: i64) -> Result<(), FilesystemError> {
        let key = name.as_bytes().to_vec();
        let (mut leaf, path) = self.descend(&key)?;
        let at = leaf
            .keys
            .iter()
            .position(|k| Self::cmp(&key, k) == std::cmp::Ordering::Less)
            .unwrap_or(leaf.keys.len());
        leaf.keys.insert(at, key);
        leaf.values.insert(at, value);

        if Self::size_with(&leaf.keys) <= self.node_size {
            return self.store(&leaf);
        }
        self.split_and_promote(leaf, path)
    }

    /// Split an overfull node, then push its separator up, splitting ancestors
    /// as needed and growing a new root when the old one goes.
    fn split_and_promote(
        &mut self,
        mut node: NodeView,
        mut path: Vec<i64>,
    ) -> Result<(), FilesystemError> {
        loop {
            let is_leaf = node.overflow == BPLUSTREE_NULL;
            let half = node.keys.len() / 2;
            if half == 0 || half == node.keys.len() {
                return Err(FilesystemError::Unsupported(
                    "bfs: a single directory entry does not fit one B+tree node".into(),
                ));
            }
            let right_keys = node.keys.split_off(half);
            let right_values = node.values.split_off(half);

            let new_off = self.alloc_node(is_leaf)?;
            let mut right = self.node(new_off)?;
            right.keys = right_keys;
            right.values = right_values;
            right.overflow = if is_leaf {
                BPLUSTREE_NULL
            } else {
                node.overflow
            };
            if !is_leaf {
                // The left half's new overflow is the child of its last key,
                // which stops being a separator when it becomes the boundary.
                node.overflow = node.values.pop().unwrap_or(BPLUSTREE_NULL);
                node.keys.pop();
            }
            right.left = node.offset;
            right.right = node.right;
            let old_right = node.right;
            node.right = new_off;

            if old_right != BPLUSTREE_NULL {
                let mut sibling = self.node(old_right)?;
                sibling.left = new_off;
                self.store(&sibling)?;
            }
            let separator =
                node.keys.last().cloned().ok_or_else(|| {
                    FilesystemError::Parse("bfs: split left half has no keys".into())
                })?;
            self.store(&node)?;
            self.store(&right)?;

            match path.pop() {
                None => {
                    // The root split: build a new root above the two halves.
                    let root_off = self.alloc_node(false)?;
                    let mut root = self.node(root_off)?;
                    root.keys = vec![separator];
                    root.values = vec![node.offset];
                    root.overflow = new_off;
                    self.store(&root)?;
                    self.set_root(root_off);
                    self.set_levels(self.levels() + 1);
                    return Ok(());
                }
                Some(parent_off) => {
                    let mut parent = self.node(parent_off)?;
                    let at = parent
                        .values
                        .iter()
                        .position(|&v| v == node.offset)
                        .unwrap_or(parent.keys.len());
                    if at < parent.keys.len() {
                        parent.keys.insert(at, separator);
                        parent.values.insert(at, node.offset);
                        parent.values[at + 1] = new_off;
                    } else {
                        parent.keys.push(separator);
                        parent.values.push(node.offset);
                        parent.overflow = new_off;
                    }
                    if Self::size_with(&parent.keys) <= self.node_size {
                        return self.store(&parent);
                    }
                    node = parent;
                }
            }
        }
    }

    pub(crate) fn remove(&mut self, name: &str) -> Result<bool, FilesystemError> {
        let key = name.as_bytes().to_vec();
        let (mut leaf, _) = self.descend(&key)?;
        let Some(at) = leaf.keys.iter().position(|k| *k == key) else {
            return Ok(false);
        };
        leaf.keys.remove(at);
        leaf.values.remove(at);
        self.store(&leaf)?;
        Ok(true)
    }
}

/// A node decoded into owned keys and values.
struct NodeView {
    offset: i64,
    left: i64,
    right: i64,
    overflow: i64,
    keys: Vec<Vec<u8>>,
    values: Vec<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree() -> BTree {
        BTree::blank(BfsEndian::Little, 1024)
    }

    #[test]
    fn blank_tree_holds_dot_and_dotdot() {
        let mut t = tree();
        t.leaf_insert_raw(1024, ".", 100);
        t.leaf_insert_raw(1024, "..", 200);
        assert_eq!(t.find("."), Some(100));
        assert_eq!(t.find(".."), Some(200));
        assert_eq!(t.find("nope"), None);
    }

    #[test]
    fn keys_come_back_in_bfs_order() {
        let mut t = tree();
        for (i, name) in ["zebra", "Apple", "apple", "Beta"].iter().enumerate() {
            t.insert(name, i as i64 + 1).unwrap();
        }
        let root = t.node(t.root()).unwrap();
        let names: Vec<String> = root
            .keys
            .iter()
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .collect();
        assert_eq!(names, vec!["Apple", "Beta", "apple", "zebra"]);
    }

    /// Enough entries to overflow a 1 KiB node several times over — the split
    /// path, the new root, and the leaf chain all have to hold.
    #[test]
    fn splitting_keeps_every_key_findable() {
        let mut t = tree();
        let count = 400;
        for i in 0..count {
            t.insert(&format!("entry-{i:04}"), i as i64 + 1000).unwrap();
        }
        assert!(t.levels() >= 2, "the tree should have grown a root");
        for i in 0..count {
            assert_eq!(
                t.find(&format!("entry-{i:04}")),
                Some(i as i64 + 1000),
                "key {i} went missing after splits"
            );
        }
    }

    #[test]
    fn leaf_chain_visits_every_key_once_after_splits() {
        let mut t = tree();
        for i in 0..300 {
            t.insert(&format!("f{i:04}"), i as i64 + 1).unwrap();
        }
        // Walk to the leftmost leaf, then follow right links.
        let mut off = t.root();
        loop {
            let n = t.node(off).unwrap();
            if n.overflow == BPLUSTREE_NULL {
                break;
            }
            off = if n.values.is_empty() {
                n.overflow
            } else {
                n.values[0]
            };
        }
        let mut seen = Vec::new();
        loop {
            let n = t.node(off).unwrap();
            for k in &n.keys {
                seen.push(String::from_utf8_lossy(k).into_owned());
            }
            if n.right == BPLUSTREE_NULL {
                break;
            }
            off = n.right;
        }
        assert_eq!(seen.len(), 300);
        let mut sorted = seen.clone();
        sorted.sort();
        assert_eq!(seen, sorted, "leaf chain is out of order");
    }

    #[test]
    fn remove_then_reinsert_round_trips() {
        let mut t = tree();
        for i in 0..120 {
            t.insert(&format!("k{i:03}"), i as i64 + 1).unwrap();
        }
        assert!(t.remove("k050").unwrap());
        assert_eq!(t.find("k050"), None);
        assert!(!t.remove("k050").unwrap());
        t.insert("k050", 999).unwrap();
        assert_eq!(t.find("k050"), Some(999));
    }

    #[test]
    fn big_endian_trees_round_trip_too() {
        let mut t = BTree::blank(BfsEndian::Big, 1024);
        for i in 0..200 {
            t.insert(&format!("ppc-{i:03}"), i as i64 + 7).unwrap();
        }
        for i in 0..200 {
            assert_eq!(t.find(&format!("ppc-{i:03}")), Some(i as i64 + 7));
        }
    }
}

/// Format a blank BFS volume in memory.
///
/// The layout matches what BeOS's `mkbfs` produces for a small disk: boot
/// block + superblock in block 0, the allocation bitmap from block 1, a
/// 2048-block log after it, the index directory next, and the root directory
/// at the start of allocation group 8 — which is where every real volume we
/// have puts it, and why `root_dir` is `{ag: 8}` rather than a low block.
///
/// `size_bytes` is rounded down to a whole block. Returns the image bytes.
pub fn create_blank_bfs(
    size_bytes: u64,
    block_size: u32,
    name: &str,
    endian: BfsEndian,
) -> Result<Vec<u8>, FilesystemError> {
    if !block_size.is_power_of_two() || !(1024..=8192).contains(&block_size) {
        return Err(FilesystemError::InvalidData(format!(
            "bfs: block size {block_size} must be a power of two in 1024..=8192"
        )));
    }
    let bs = block_size as u64;
    let block_shift = block_size.trailing_zeros();
    // One bitmap block covers `block_size * 8` blocks, and BFS sizes an
    // allocation group to exactly that so `blocks_per_ag` stays 1.
    let ag_shift = block_shift + 3;
    let ag_blocks = 1u64 << ag_shift;

    let num_blocks = size_bytes / bs;
    let num_ags = num_blocks.div_ceil(ag_blocks);
    // Root lives in AG 8, so the volume has to reach that far.
    let min_ags = 9u64;
    if num_ags < min_ags {
        return Err(FilesystemError::InvalidData(format!(
            "bfs: {size_bytes} bytes is too small — a {block_size}-byte-block volume needs at \
             least {} bytes ({min_ags} allocation groups)",
            min_ags * ag_blocks * bs
        )));
    }
    if name.len() > 31 {
        return Err(FilesystemError::InvalidData(
            "bfs: volume name is capped at 31 bytes".into(),
        ));
    }

    let bitmap_blocks = num_ags;
    let log_start_block = 1 + bitmap_blocks;
    let log_blocks = 2048u64.min(ag_blocks - 1);
    let indices_block = log_start_block + log_blocks;
    let root_block = 8 * ag_blocks;
    // The directory tree is a header block plus one leaf node.
    let root_tree_block = root_block + 1;
    let root_tree_blocks = 2u64;
    let indices_tree_block = indices_block + 1;

    let mut img = vec![0u8; (num_blocks * bs) as usize];
    let ag_mask = ag_blocks - 1;
    let run = |block: u64, len: u16| BlockRun {
        allocation_group: (block >> ag_shift) as u32,
        start: (block & ag_mask) as u16,
        len,
    };

    // ---- superblock ----
    let sb_at = 512usize;
    let sb = &mut img[sb_at..sb_at + 164];
    sb[..name.len()].copy_from_slice(name.as_bytes());
    endian.put_u32(sb, 32, super::bfs::BFS_MAGIC1);
    endian.put_u32(sb, 36, super::bfs::BFS_BYTE_ORDER);
    endian.put_u32(sb, 40, block_size);
    endian.put_u32(sb, 44, block_shift);
    endian.put_i64(sb, 48, num_blocks as i64);
    endian.put_u32(sb, 64, block_size);
    endian.put_u32(sb, 68, super::bfs::BFS_MAGIC2);
    endian.put_u32(sb, 72, 1);
    endian.put_u32(sb, 76, ag_shift);
    endian.put_u32(sb, 80, num_ags as u32);
    endian.put_u32(sb, 84, BFS_CLEAN);
    run(log_start_block, log_blocks as u16).write(endian, sb, 88);
    endian.put_i64(sb, 96, indices_block as i64);
    endian.put_i64(sb, 104, indices_block as i64);
    endian.put_u32(sb, 112, super::bfs::BFS_MAGIC3);
    run(root_block, 1).write(endian, sb, 116);
    run(indices_block, 1).write(endian, sb, 124);

    // ---- allocation bitmap: everything laid out above ----
    let mut used = 0u64;
    let mut mark = |img: &mut [u8], start: u64, count: u64| {
        for b in start..start + count {
            // The bitmap starts at block 1, so its byte 0 is at `bs`.
            let at = bs as usize + (b / 32) as usize * 4;
            let mut word = endian.read_u32(img, at);
            word |= 1 << (b % 32);
            endian.put_u32(img, at, word);
        }
        used += count;
    };
    mark(&mut img, 0, 1 + bitmap_blocks);
    mark(&mut img, log_start_block, log_blocks);
    mark(&mut img, indices_block, 1 + root_tree_blocks);
    mark(&mut img, root_block, 1 + root_tree_blocks);
    endian.put_i64(&mut img[sb_at..sb_at + 164], 56, used as i64);

    // ---- the two directories ----
    let write_dir =
        |img: &mut [u8], inode_block: u64, tree_block: u64, parent: u64, dir_name: &str| {
            let tree = {
                let node_size = block_size as usize;
                let mut t = BTree::blank(endian, node_size);
                t.leaf_insert_raw(node_size as i64, ".", inode_block as i64);
                t.leaf_insert_raw(node_size as i64, "..", parent as i64);
                t
            };
            let at = (tree_block * bs) as usize;
            img[at..at + tree.data.len()].copy_from_slice(&tree.data);

            let mut inode = vec![0u8; block_size as usize];
            endian.put_u32(&mut inode, 0, BFS_INODE_MAGIC);
            run(inode_block, 1).write(endian, &mut inode, 4);
            endian.put_u32(&mut inode, 0x14, S_IFDIR | 0o755);
            endian.put_u32(&mut inode, 0x18, INODE_FLAGS_DEFAULT);
            run(parent, 1).write(endian, &mut inode, 0x2C);
            endian.put_u32(&mut inode, 0x40, block_size);
            let mut ds = DataStream {
                size: (root_tree_blocks * bs) as i64,
                ..Default::default()
            };
            ds.direct[0] = run(tree_block, root_tree_blocks as u16);
            ds.max_direct_range = ds.size;
            ds.max_indirect_range = ds.size;
            ds.max_double_indirect_range = ds.size;
            ds.write(endian, &mut inode, INODE_OFF_DATA);
            let off = INODE_OFF_SMALL_DATA;
            endian.put_u32(&mut inode, off, FILE_NAME_TYPE);
            endian.put_u16(&mut inode, off + 4, 1);
            endian.put_u16(&mut inode, off + 6, dir_name.len() as u16);
            inode[off + 8] = FILE_NAME_NAME;
            let name_at = off + 8 + 1 + 3;
            inode[name_at..name_at + dir_name.len()].copy_from_slice(dir_name.as_bytes());
            let iat = (inode_block * bs) as usize;
            img[iat..iat + inode.len()].copy_from_slice(&inode);
        };
    write_dir(&mut img, root_block, root_tree_block, root_block, name);
    write_dir(
        &mut img,
        indices_block,
        indices_tree_block,
        root_block,
        "indices",
    );

    Ok(img)
}

#[cfg(test)]
mod format_tests {
    use super::*;
    use crate::fs::filesystem::CreateFileOptions;
    use std::io::Cursor;

    fn volume(endian: BfsEndian) -> BfsFilesystem<Cursor<Vec<u8>>> {
        // 9 allocation groups of 8192 blocks each at 1 KiB = 72 MiB.
        let img = create_blank_bfs(72 * 1024 * 1024, 1024, "Blank", endian).expect("format");
        BfsFilesystem::open(Cursor::new(img), 0).expect("open the volume we just formatted")
    }

    #[test]
    fn a_formatted_volume_opens_with_an_empty_root() {
        let mut fs = volume(BfsEndian::Little);
        assert_eq!(fs.volume_label(), Some("Blank"));
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
    }

    #[test]
    fn a_volume_too_small_for_allocation_group_eight_is_refused() {
        assert!(create_blank_bfs(4 * 1024 * 1024, 1024, "Tiny", BfsEndian::Little).is_err());
    }

    /// Create, read back, delete — through the same public path `rb-cli put`
    /// uses — on a volume we formatted ourselves, in both byte orders.
    #[test]
    fn files_round_trip_on_a_fresh_volume_in_both_orders() {
        for endian in [BfsEndian::Little, BfsEndian::Big] {
            let mut fs = volume(endian);
            let root = fs.root().unwrap();
            let body = b"round trip".to_vec();
            fs.create_file(
                &root,
                "hello.txt",
                &mut body.as_slice(),
                body.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();

            let listed = fs.list_directory(&root).unwrap();
            assert_eq!(listed.len(), 1, "{endian:?}");
            assert_eq!(fs.read_file(&listed[0], usize::MAX).unwrap(), body);

            fs.delete_entry(&root, &listed[0]).unwrap();
            assert!(fs.list_directory(&root).unwrap().is_empty(), "{endian:?}");
        }
    }

    #[test]
    fn a_directory_created_here_holds_its_own_children() {
        let mut fs = volume(BfsEndian::Little);
        let root = fs.root().unwrap();
        let dir = fs
            .create_directory(&root, "sub", &CreateDirectoryOptions::default())
            .unwrap();
        fs.create_file(
            &dir,
            "inner",
            &mut b"nested".as_slice(),
            6,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let inner = fs.list_directory(&dir).unwrap();
        assert_eq!(inner.len(), 1);
        assert_eq!(inner[0].name, "inner");
        assert_eq!(fs.read_file(&inner[0], usize::MAX).unwrap(), b"nested");
    }

    /// Enough entries to split the root's B+tree several times, then read the
    /// whole listing back from the leaf chain.
    #[test]
    fn a_directory_that_splits_its_btree_still_lists_completely() {
        let mut fs = volume(BfsEndian::Big);
        let root = fs.root().unwrap();
        for i in 0..300 {
            let name = format!("file-{i:04}.txt");
            fs.create_file(
                &root,
                &name,
                &mut name.as_bytes(),
                name.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        }
        let listed = fs.list_directory(&root).unwrap();
        assert_eq!(listed.len(), 300);
        let mut names: Vec<&str> = listed.iter().map(|e| e.name.as_str()).collect();
        names.sort();
        assert_eq!(names[0], "file-0000.txt");
        assert_eq!(names[299], "file-0299.txt");
        for e in &listed {
            assert_eq!(fs.read_file(e, usize::MAX).unwrap(), e.name.as_bytes());
        }
    }

    /// A file big enough to need more than the 12 direct runs, so the indirect
    /// array is exercised end to end.
    #[test]
    fn a_file_spanning_many_runs_round_trips() {
        let mut fs = volume(BfsEndian::Little);
        let root = fs.root().unwrap();
        let body: Vec<u8> = (0..600_000u32).map(|i| (i % 251) as u8).collect();
        fs.create_file(
            &root,
            "big.bin",
            &mut body.as_slice(),
            body.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let listed = fs.list_directory(&root).unwrap();
        assert_eq!(listed[0].size, body.len() as u64);
        assert_eq!(fs.read_file(&listed[0], usize::MAX).unwrap(), body);
    }

    #[test]
    fn delete_returns_every_block_it_took() {
        let mut fs = volume(BfsEndian::Little);
        let root = fs.root().unwrap();
        let before = fs.free_space().unwrap();
        let body = vec![9u8; 200_000];
        fs.create_file(
            &root,
            "blob",
            &mut body.as_slice(),
            body.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        assert!(fs.free_space().unwrap() < before);
        let listed = fs.list_directory(&root).unwrap();
        fs.delete_entry(&root, &listed[0]).unwrap();
        assert_eq!(fs.free_space().unwrap(), before, "blocks leaked on delete");
    }

    #[test]
    fn rename_keeps_the_inode_and_its_bytes() {
        let mut fs = volume(BfsEndian::Little);
        let root = fs.root().unwrap();
        fs.create_file(
            &root,
            "before",
            &mut b"payload".as_slice(),
            7,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let listed = fs.list_directory(&root).unwrap();
        let block = listed[0].location;
        fs.rename(&root, &listed[0], "after").unwrap();
        let listed = fs.list_directory(&root).unwrap();
        assert_eq!(listed[0].name, "after");
        assert_eq!(listed[0].location, block, "rename moved the inode");
        assert_eq!(fs.read_file(&listed[0], usize::MAX).unwrap(), b"payload");
        // The inode carries its own name too; a stale one shows in Tracker.
        let inode = fs.read_inode(block).unwrap();
        assert_eq!(inode.name().as_deref(), Some("after"));
    }

    #[test]
    fn symlinks_store_their_target_inline() {
        let mut fs = volume(BfsEndian::Big);
        let root = fs.root().unwrap();
        fs.create_symlink(&root, "link", "/boot/beos", &CreateFileOptions::default())
            .unwrap();
        let listed = fs.list_directory(&root).unwrap();
        assert_eq!(listed[0].symlink_target.as_deref(), Some("/boot/beos"));
    }
}
