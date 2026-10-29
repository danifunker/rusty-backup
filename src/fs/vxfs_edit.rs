//! VxFS disk layout 4 editing: create, delete and rename files and directories.
//!
//! The on-disk facts are in `docs/vxfs-layout4-write-notes.md`, read off volumes
//! made by HP-UX 11.11 and checked with its `fsck -F vxfs`. The shape of an edit:
//!
//! - Free space lives in the extent map's level-0 bitmap. Every other level, the
//!   per-AU summary and the superblock's `free` / `efree[]` are a pure function of
//!   it, so edits flip level-0 bits and [`regen_au`] rebuilds the rest on sync.
//! - Inodes come from the primary fileset's IAU bitmap (set = free). When it has
//!   none left the primary inode list grows by one extent, mirrored into the
//!   list's replica inode, and `fsh_ninode` follows in both fileset-header copies.
//! - Directories are immediate (in the inode, `nhash` 0) until they outgrow 96
//!   bytes, then hashed blocks: chains keyed by the name's byte sum mod `nhash`.
//!
//! Only a cleanly unmounted volume is edited; the intent log is left untouched.
//! Files are written with direct extents (`ORG_EXT4`, at most ten); anything
//! needing indirect or typed extents is refused rather than half-handled.

#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::vxfs::{
    Inode, VxfsFilesystem, INODE_SIZE, ORG_EXT4, ORG_IMMED, ROOT_INO, S_IFDIR, S_IFMT, S_IFREG,
};

const SB_FREE: usize = 0xc0;
const SB_EFREE: usize = 0xc8;
const SB_AULEN: usize = 0x78;
const SB_CLEAN: usize = 0x14d;
const SB_TIME: usize = 0x154;
const VX_CLEAN: u8 = 0x5a;
const FSH_TIME: usize = 0x08;
const FSH_NINODE: usize = 0x14;
const FSH_IAUINO: usize = 0x2c;
const FSH_ILISTINO: usize = 0x30;
const FSH_CHECKSUM: usize = 0x64;
const FSH_MTIME: usize = 0x140;
/// The fileset header's words that `fsh_checksum` sums.
const FSH_LEN: usize = 0x150;
const IAU_COUNTS_RIFREE: usize = 0x0c;
const IAU_COUNTS_RIBFREE: usize = 0x10;
/// Structural inode types, in the mode's high bits.
const IFEMP: u32 = 0o2400000000;
const IFAUS: u32 = 0o2600000000;
const IFTYPE: u32 = !0o7777;
/// One extent-map piece: 512 bytes describing 2048 blocks; an AU's map is 8 KiB.
const PIECE_BLOCKS: u64 = 2048;
const PIECE_BYTES: usize = 512;
const AU_MAP_BYTES: usize = 8192;
const IMMED_LEN: usize = 96;
const DIRENT_HDR: usize = 10;
const NDADDR: usize = 10;
/// Inodes the primary list grows by when the IAU runs dry.
const ILIST_GROW_INODES: u64 = 256;

/// Inode field offsets.
const I_MODE: usize = 0;
const I_NLINK: usize = 4;
const I_UID: usize = 8;
const I_GID: usize = 12;
const I_SIZE: usize = 16;
const I_ATIME: usize = 24;
const I_MTIME: usize = 32;
const I_CTIME: usize = 40;
const I_ORG: usize = 49;
/// The union at +0x38: `matchino` for structural inodes, `dotdot` for directories.
const I_FTAREA: usize = 0x38;
const I_BLOCKS: usize = 64;
const I_ORGAREA: usize = 80;

/// Rebuild one AU's extent map from its level-0 bitmaps; returns the map and the
/// counts of maximal free aligned extents per order (0-15).
pub(crate) fn regen_au(map: &[u8]) -> (Vec<u8>, [u16; 16]) {
    const LV: [usize; 9] = [0, 0x100, 0x180, 0x1c0, 0x1e0, 0x1f0, 0x1f8, 0x1fc, 0x1fe];
    let mut out = vec![0u8; AU_MAP_BYTES];
    let mut cnt = [0u16; 16];
    let mut whole = [false; 16];
    for p in 0..16 {
        let src = &map[p * PIECE_BYTES..p * PIECE_BYTES + 256];
        let piece = &mut out[p * PIECE_BYTES..(p + 1) * PIECE_BYTES];
        piece[..256].copy_from_slice(src);
        let mut full: Vec<Vec<bool>> = vec![(0..2048)
            .map(|i| src[i / 8] & (0x80 >> (i % 8)) != 0)
            .collect()];
        for k in 1..12 {
            let prev = &full[k - 1];
            full.push(
                (0..prev.len() / 2)
                    .map(|i| prev[2 * i] && prev[2 * i + 1])
                    .collect(),
            );
        }
        for k in 0..12 {
            for i in 0..full[k].len() {
                if !full[k][i] || (k < 11 && full[k + 1][i / 2]) {
                    continue;
                }
                cnt[k] += u16::from(k < 11);
                if k == 0 {
                    continue;
                }
                let bit = if k <= 8 {
                    LV[k] * 8 + i
                } else {
                    0x1ff * 8 + [0, 4, 6][k - 9] + i
                };
                piece[bit / 8] |= 0x80 >> (bit % 8);
            }
        }
        whole[p] = full[11][0];
    }
    // Orders 11-15 combine wholly free pieces; they are counted, never stored.
    let mut lvl: Vec<bool> = whole.to_vec();
    for (k, slot) in cnt.iter_mut().enumerate().skip(11) {
        let next: Vec<bool> = (0..lvl.len() / 2)
            .map(|i| lvl[2 * i] && lvl[2 * i + 1])
            .collect();
        for (i, &f) in lvl.iter().enumerate() {
            if f && (k == 15 || !next[i / 2]) {
                *slot += 1;
            }
        }
        lvl = next;
    }
    (out, cnt)
}

/// The minimum record length of a directory entry with a `namelen`-byte name.
fn dirent_len(namelen: usize) -> usize {
    (DIRENT_HDR + namelen + 3) & !3
}

fn name_hash(name: &[u8], nhash: usize) -> usize {
    name.iter().map(|&b| b as usize).sum::<usize>() % nhash
}

/// One directory chunk: an immediate area or one directory block.
struct Chunk<'a> {
    buf: &'a mut [u8],
    big: bool,
}

impl<'a> Chunk<'a> {
    fn u16(&self, o: usize) -> u16 {
        let a = [self.buf[o], self.buf[o + 1]];
        if self.big {
            u16::from_be_bytes(a)
        } else {
            u16::from_le_bytes(a)
        }
    }
    fn u32(&self, o: usize) -> u32 {
        let a = [
            self.buf[o],
            self.buf[o + 1],
            self.buf[o + 2],
            self.buf[o + 3],
        ];
        if self.big {
            u32::from_be_bytes(a)
        } else {
            u32::from_le_bytes(a)
        }
    }
    fn put16(&mut self, o: usize, v: u16) {
        let b = if self.big {
            v.to_be_bytes()
        } else {
            v.to_le_bytes()
        };
        self.buf[o..o + 2].copy_from_slice(&b);
    }
    fn put32(&mut self, o: usize, v: u32) {
        let b = if self.big {
            v.to_be_bytes()
        } else {
            v.to_le_bytes()
        };
        self.buf[o..o + 4].copy_from_slice(&b);
    }
    fn nhash(&self) -> usize {
        self.u16(2) as usize
    }
    fn first(&self) -> usize {
        4 + 2 * self.nhash()
    }

    /// `(pos, ino, reclen, namelen)` of every record.
    fn records(&self) -> Vec<(usize, u32, usize, usize)> {
        let mut out = Vec::new();
        let mut pos = self.first();
        while pos + DIRENT_HDR <= self.buf.len() {
            let reclen = self.u16(pos + 4) as usize;
            if reclen == 0 || pos + reclen > self.buf.len() {
                break;
            }
            out.push((pos, self.u32(pos), reclen, self.u16(pos + 6) as usize));
            pos += reclen;
        }
        out
    }

    fn find(&self, name: &[u8]) -> Option<(usize, u32)> {
        self.records().into_iter().find_map(|(pos, ino, _, nl)| {
            (ino != 0 && &self.buf[pos + DIRENT_HDR..pos + DIRENT_HDR + nl] == name)
                .then_some((pos, ino))
        })
    }

    /// An empty chunk: header and one hole record spanning the rest.
    fn init(&mut self, nhash: usize) {
        self.buf.fill(0);
        self.put16(2, nhash as u16);
        let first = self.first();
        let rest = self.buf.len() - first;
        self.put16(first + 4, rest as u16);
        self.put16(0, rest as u16);
    }

    fn refresh_tfree(&mut self) {
        let free: usize = self
            .records()
            .iter()
            .map(|&(_, ino, rl, nl)| if ino == 0 { rl } else { rl - dirent_len(nl) })
            .sum();
        self.put16(0, free as u16);
    }

    /// Place `name` -> `ino` in the first record with room; false when there is none.
    fn insert(&mut self, name: &[u8], ino: u32) -> bool {
        let need = dirent_len(name.len());
        let slot = self.records().into_iter().find_map(|(pos, i, rl, nl)| {
            if i == 0 && rl >= need {
                Some((pos, rl))
            } else if i != 0 && rl - dirent_len(nl) >= need {
                self.put16(pos + 4, dirent_len(nl) as u16);
                Some((pos + dirent_len(nl), rl - dirent_len(nl)))
            } else {
                None
            }
        });
        let Some((pos, reclen)) = slot else {
            return false;
        };
        self.buf[pos..pos + reclen].fill(0);
        self.put32(pos, ino);
        self.put16(pos + 4, reclen as u16);
        self.put16(pos + 6, name.len() as u16);
        self.buf[pos + DIRENT_HDR..pos + DIRENT_HDR + name.len()].copy_from_slice(name);
        if self.nhash() > 0 {
            let h = name_hash(name, self.nhash());
            let head = self.u16(4 + 2 * h);
            self.put16(pos + 8, head);
            self.put16(4 + 2 * h, pos as u16);
        }
        self.refresh_tfree();
        true
    }

    /// Drop the record at `pos`, merging it into the record before it.
    fn remove(&mut self, pos: usize) {
        let nl = self.u16(pos + 6) as usize;
        if self.nhash() > 0 {
            let name = self.buf[pos + DIRENT_HDR..pos + DIRENT_HDR + nl].to_vec();
            let slot = 4 + 2 * name_hash(&name, self.nhash());
            let next = self.u16(pos + 8);
            let mut link = slot;
            loop {
                let at = self.u16(link) as usize;
                if at == 0 {
                    break;
                }
                if at == pos {
                    self.put16(link, next);
                    break;
                }
                link = at + 8;
            }
        }
        let recs = self.records();
        let reclen = self.u16(pos + 4) as usize;
        match recs.iter().position(|r| r.0 == pos) {
            Some(i) if i > 0 => {
                let prev = recs[i - 1].0;
                let merged = self.u16(prev + 4) as usize + reclen;
                self.put16(prev + 4, merged as u16);
                self.buf[pos..pos + reclen].fill(0);
            }
            _ => {
                self.buf[pos..pos + reclen].fill(0);
                self.put16(pos + 4, reclen as u16);
            }
        }
        self.refresh_tfree();
    }

    fn is_empty(&self) -> bool {
        self.records().iter().all(|r| r.1 == 0)
    }
}

/// An editable VxFS layout-4 volume.
pub struct VxfsEditor<R: Read + Write + Seek + Send> {
    fs: VxfsFilesystem<R>,
    sb: Vec<u8>,
    aulen: u64,
    nau: u64,
    emap: Inode,
    aus: Inode,
    /// Every AU's extent map, as read, with level-0 edits applied.
    maps: Vec<u8>,
    dirty: Vec<bool>,
    iau: Inode,
    iau_bitmap: Vec<u8>,
    iau_counts: Vec<u8>,
    ninode: u64,
    /// Structural inode numbers of the primary inode list and its replica.
    ilist_inos: [u32; 2],
    /// Physical blocks of every copy of the primary fileset header.
    fsh_blocks: Vec<u64>,
    dir_nhash: usize,
    touched: bool,
}

impl<R: Read + Write + Seek + Send> VxfsEditor<R> {
    pub fn open(reader: R, offset: u64) -> Result<Self, FilesystemError> {
        let mut fs = VxfsFilesystem::open(reader, offset)?;
        if fs.version != 4 {
            return Err(FilesystemError::Unsupported(format!(
                "VxFS editing supports disk layout 4 only (this volume is layout {})",
                fs.version
            )));
        }
        let sb = fs.read_bytes(fs.sb_off, 1024)?;
        if sb[SB_CLEAN] != VX_CLEAN {
            return Err(FilesystemError::Unsupported(
                "VxFS volume was not cleanly unmounted; run fsck on it first".into(),
            ));
        }
        let aulen = fs.u32_at(&sb, SB_AULEN) as u64;
        if aulen == 0 || !aulen.is_multiple_of(PIECE_BLOCKS) || aulen > 16 * PIECE_BLOCKS {
            return Err(FilesystemError::Unsupported(format!(
                "VxFS allocation units of {aulen} blocks are not supported"
            )));
        }
        let nau = fs.total_blocks.div_ceil(aulen);

        // The fileset-header inode and its replica each hold a copy of every header.
        let st = fs.st_ilist.clone();
        let fsh_raw = fs.raw_inode(&st, fs.fsh_ino)?;
        let fsh_replica = fs.u32_at(&fsh_raw, I_FTAREA);
        let mut fsh_blocks = Vec::new();
        for ino in [fs.fsh_ino, fsh_replica] {
            let inode = {
                let r = fs.raw_inode(&st, ino)?;
                fs.decode_inode(&r)
            };
            if let Some(b) = fs.bmap(&inode, 1)? {
                if !fsh_blocks.contains(&b) {
                    fsh_blocks.push(b);
                }
            }
        }
        let fsh = fs.read_blocks(fsh_blocks[0], 1)?;
        let ninode = fs.u32_at(&fsh, FSH_NINODE) as u64;
        let iau_ino = fs.u32_at(&fsh, FSH_IAUINO);
        let ilist_inos = [
            fs.u32_at(&fsh, FSH_ILISTINO),
            fs.u32_at(&fsh, FSH_ILISTINO + 4),
        ];

        // The extent map and AU summary are found by type in the structural list.
        let st_count = st.size / INODE_SIZE;
        let (mut emap, mut aus) = (None, None);
        for ino in 0..st_count as u32 {
            let i = {
                let r = fs.raw_inode(&st, ino)?;
                fs.decode_inode(&r)
            };
            match i.mode & IFTYPE {
                IFEMP if emap.is_none() => emap = Some(i),
                IFAUS if aus.is_none() => aus = Some(i),
                _ => {}
            }
        }
        let (emap, aus) = emap.zip(aus).ok_or_else(|| {
            FilesystemError::Parse(
                "vxfs: no extent map or AU summary in the structural list".into(),
            )
        })?;
        let mut maps = vec![0u8; nau as usize * AU_MAP_BYTES];
        fs.read_inode_range(&emap, 0, &mut maps)?;

        let iau = {
            let r = fs.raw_inode(&st, iau_ino)?;
            fs.decode_inode(&r)
        };
        let bs = fs.bsize as usize;
        let mut iau_counts = vec![0u8; bs];
        fs.read_inode_range(&iau, fs.bsize, &mut iau_counts)?;
        let mut iau_bitmap = vec![0u8; bs];
        fs.read_inode_range(&iau, 2 * fs.bsize, &mut iau_bitmap)?;

        let mut ed = Self {
            fs,
            sb,
            aulen,
            nau,
            emap,
            aus,
            maps,
            dirty: vec![false; nau as usize],
            iau,
            iau_bitmap,
            iau_counts,
            ninode,
            ilist_inos,
            fsh_blocks,
            dir_nhash: (bs / 32).max(1),
            touched: false,
        };
        ed.dir_nhash = ed.probe_dir_nhash().unwrap_or(ed.dir_nhash);
        Ok(ed)
    }

    /// `nhash` of an existing block directory, so new ones match what the OS writes.
    fn probe_dir_nhash(&mut self) -> Option<usize> {
        let root = self.fs.inode(ROOT_INO).ok()?;
        if root.org == ORG_IMMED {
            return None;
        }
        let pb = self.fs.bmap(&root, 0).ok()??;
        let blk = self.fs.read_blocks(pb, 1).ok()?;
        Some(self.fs.u16_at(&blk, 2) as usize).filter(|&n| n > 0)
    }

    fn put32(&self, b: &mut [u8], o: usize, v: u32) {
        let x = if self.fs.big {
            v.to_be_bytes()
        } else {
            v.to_le_bytes()
        };
        b[o..o + 4].copy_from_slice(&x);
    }

    fn put16(&self, b: &mut [u8], o: usize, v: u16) {
        let x = if self.fs.big {
            v.to_be_bytes()
        } else {
            v.to_le_bytes()
        };
        b[o..o + 2].copy_from_slice(&x);
    }

    fn put64(&self, b: &mut [u8], o: usize, v: u64) {
        let x = if self.fs.big {
            v.to_be_bytes()
        } else {
            v.to_le_bytes()
        };
        b[o..o + 8].copy_from_slice(&x);
    }

    fn write_phys(&mut self, block: u64, within: u64, data: &[u8]) -> Result<(), FilesystemError> {
        let at = self.fs.offset + block * self.fs.bsize + within;
        self.fs.reader.seek(SeekFrom::Start(at))?;
        self.fs.reader.write_all(data)?;
        Ok(())
    }

    /// Write `data` at byte `pos` of `inode`'s file, which must already be allocated there.
    fn write_file(&mut self, inode: &Inode, pos: u64, data: &[u8]) -> Result<(), FilesystemError> {
        let bs = self.fs.bsize;
        let mut done = 0usize;
        while done < data.len() {
            let at = pos + done as u64;
            let (bn, within) = (at / bs, at % bs);
            let take = ((bs - within) as usize).min(data.len() - done);
            let pb = self.fs.bmap(inode, bn)?.ok_or_else(|| {
                FilesystemError::InvalidData(format!("vxfs: write into a hole at block {bn}"))
            })?;
            self.write_phys(pb, within, &data[done..done + take])?;
            done += take;
        }
        Ok(())
    }

    fn user_raw(&mut self, ino: u32) -> Result<Vec<u8>, FilesystemError> {
        let il = self.fs.ilist.clone();
        self.fs.raw_inode(&il, ino)
    }

    fn put_user_raw(&mut self, ino: u32, raw: &[u8]) -> Result<(), FilesystemError> {
        let il = self.fs.ilist.clone();
        self.write_file(&il, ino as u64 * INODE_SIZE, raw)
    }

    // ---- blocks ----

    fn map_bit(&self, block: u64) -> (usize, u8) {
        let (au, within) = (block / self.aulen, block % self.aulen);
        let (piece, idx) = (within / PIECE_BLOCKS, within % PIECE_BLOCKS);
        let byte = au as usize * AU_MAP_BYTES + piece as usize * PIECE_BYTES + idx as usize / 8;
        (byte, 0x80 >> (idx % 8))
    }

    fn is_free(&self, block: u64) -> bool {
        let (byte, mask) = self.map_bit(block);
        self.maps[byte] & mask != 0
    }

    fn mark(&mut self, start: u64, len: u64, free: bool) {
        for b in start..start + len {
            let (byte, mask) = self.map_bit(b);
            if free {
                self.maps[byte] |= mask;
            } else {
                self.maps[byte] &= !mask;
            }
            self.dirty[(b / self.aulen) as usize] = true;
        }
        self.touched = true;
    }

    /// Up to `max_runs` free runs totalling `count` blocks: one run when one fits.
    fn alloc(&mut self, count: u64, max_runs: usize) -> Result<Vec<(u64, u64)>, FilesystemError> {
        let total = self.fs.total_blocks;
        let mut runs = Vec::new();
        let mut b = 0;
        while b < total {
            if !self.is_free(b) {
                b += 1;
                continue;
            }
            let s = b;
            while b < total && self.is_free(b) {
                b += 1;
            }
            runs.push((s, b - s));
        }
        let pick = if let Some(&(s, _)) = runs.iter().find(|r| r.1 >= count) {
            vec![(s, count)]
        } else {
            runs.sort_by_key(|r| std::cmp::Reverse(r.1));
            let mut out = Vec::new();
            let mut left = count;
            for (s, l) in runs.into_iter().take(max_runs) {
                let take = l.min(left);
                out.push((s, take));
                left -= take;
                if left == 0 {
                    break;
                }
            }
            if left > 0 {
                return Err(FilesystemError::DiskFull(format!(
                    "VxFS: no room for {count} blocks in {max_runs} extents"
                )));
            }
            out
        };
        for &(s, l) in &pick {
            self.mark(s, l, false);
        }
        Ok(pick)
    }

    // ---- inodes ----

    fn inode_free(&self, ino: u64) -> bool {
        self.iau_bitmap[(ino / 8) as usize] & (0x80 >> (ino % 8)) != 0
    }

    fn set_inode_free(&mut self, ino: u64, free: bool) {
        let (byte, mask) = ((ino / 8) as usize, 0x80u8 >> (ino % 8));
        if free {
            self.iau_bitmap[byte] |= mask;
        } else {
            self.iau_bitmap[byte] &= !mask;
        }
        self.touched = true;
    }

    fn alloc_inode(&mut self) -> Result<u32, FilesystemError> {
        if let Some(ino) = (3..self.ninode).find(|&i| self.inode_free(i)) {
            self.set_inode_free(ino, false);
            return Ok(ino as u32);
        }
        self.grow_ilist()?;
        self.alloc_inode()
    }

    /// Add one extent of free inodes to the primary list and both of its inodes.
    fn grow_ilist(&mut self) -> Result<(), FilesystemError> {
        let bs = self.fs.bsize;
        let new_total = self.ninode + ILIST_GROW_INODES;
        if new_total > self.iau_bitmap.len() as u64 * 8 {
            return Err(FilesystemError::DiskFull(
                "VxFS inode list is at its single-IAU limit".into(),
            ));
        }
        let blocks = ILIST_GROW_INODES * INODE_SIZE / bs;
        let run = self.alloc(blocks, 1)?[0];
        let zero = vec![0u8; (blocks * bs) as usize];
        self.write_phys(run.0, 0, &zero)?;
        let st = self.fs.st_ilist.clone();
        for ino in self.ilist_inos {
            let mut raw = self.fs.raw_inode(&st, ino)?;
            if raw[I_ORG] != ORG_EXT4 {
                return Err(FilesystemError::Unsupported(
                    "VxFS inode list is not in direct-extent form".into(),
                ));
            }
            let slot = (0..NDADDR)
                .find(|&i| self.fs.u32_at(&raw, I_ORGAREA + 20 + i * 8) == 0)
                .ok_or_else(|| {
                    FilesystemError::Unsupported("VxFS inode list has no free extent slot".into())
                })?;
            self.put32(&mut raw, I_ORGAREA + 16 + slot * 8, run.0 as u32);
            self.put32(&mut raw, I_ORGAREA + 20 + slot * 8, run.1 as u32);
            let size = self.fs.u64_at(&raw, I_SIZE) + blocks * bs;
            self.put64(&mut raw, I_SIZE, size);
            let held = self.fs.u32_at(&raw, I_BLOCKS) as u64 + blocks;
            self.put32(&mut raw, I_BLOCKS, held as u32);
            self.write_file(&st, ino as u64 * INODE_SIZE, &raw)?;
        }
        let primary = self.fs.raw_inode(&st, self.ilist_inos[0])?;
        self.fs.ilist = self.fs.decode_inode(&primary);
        for ino in self.ninode..new_total {
            self.set_inode_free(ino, true);
        }
        self.ninode = new_total;
        Ok(())
    }

    // ---- inode records ----

    fn new_inode_raw(&self, mode: u32, uid: u32, gid: u32, nlink: u32) -> Vec<u8> {
        let mut raw = vec![0u8; INODE_SIZE as usize];
        let (secs, usec) = now();
        self.put32(&mut raw, I_MODE, mode);
        self.put32(&mut raw, I_NLINK, nlink);
        self.put32(&mut raw, I_UID, uid);
        self.put32(&mut raw, I_GID, gid);
        for t in [I_ATIME, I_MTIME, I_CTIME] {
            self.put32(&mut raw, t, secs);
            self.put32(&mut raw, t + 4, usec);
        }
        raw
    }

    fn touch_times(&self, raw: &mut [u8]) {
        let (secs, usec) = now();
        for t in [I_MTIME, I_CTIME] {
            self.put32(raw, t, secs);
            self.put32(raw, t + 4, usec);
        }
    }

    /// Direct extents of an inode in `ORG_EXT4` form, refusing indirect ones.
    fn direct_extents(&self, raw: &[u8]) -> Result<Vec<(u64, u64)>, FilesystemError> {
        if raw[I_ORG] != ORG_EXT4 {
            return Err(FilesystemError::Unsupported(format!(
                "VxFS inodes with organisation type {} cannot be edited yet",
                raw[I_ORG]
            )));
        }
        if self.fs.u32_at(raw, I_ORGAREA + 4) != 0 || self.fs.u32_at(raw, I_ORGAREA + 8) != 0 {
            return Err(FilesystemError::Unsupported(
                "VxFS files with indirect extents cannot be edited yet".into(),
            ));
        }
        Ok((0..NDADDR)
            .map(|i| {
                (
                    self.fs.u32_at(raw, I_ORGAREA + 16 + i * 8) as u64,
                    self.fs.u32_at(raw, I_ORGAREA + 20 + i * 8) as u64,
                )
            })
            .filter(|e| e.1 > 0)
            .collect())
    }

    fn set_direct_extents(&self, raw: &mut [u8], exts: &[(u64, u64)]) {
        raw[I_ORGAREA..I_ORGAREA + IMMED_LEN].fill(0);
        raw[I_ORG] = ORG_EXT4;
        for (i, &(s, l)) in exts.iter().enumerate() {
            self.put32(raw, I_ORGAREA + 16 + i * 8, s as u32);
            self.put32(raw, I_ORGAREA + 20 + i * 8, l as u32);
        }
        let held: u64 = exts.iter().map(|e| e.1).sum();
        self.put32(raw, I_BLOCKS, held as u32);
    }

    /// Free whatever blocks an inode holds.
    fn release_blocks(&mut self, raw: &[u8]) -> Result<(), FilesystemError> {
        match raw[I_ORG] {
            ORG_IMMED => Ok(()),
            ORG_EXT4 => {
                for (s, l) in self.direct_extents(raw)? {
                    self.mark(s, l, true);
                }
                Ok(())
            }
            o => Err(FilesystemError::Unsupported(format!(
                "VxFS inodes with organisation type {o} cannot be removed yet"
            ))),
        }
    }

    // ---- directories ----

    /// Run `f` over every chunk of directory `ino` until it returns true; writes back the chunk it changed.
    fn with_dir<F>(&mut self, ino: u32, mut f: F) -> Result<bool, FilesystemError>
    where
        F: FnMut(&mut Chunk) -> bool,
    {
        let raw = self.user_raw(ino)?;
        let big = self.fs.big;
        if raw[I_ORG] == ORG_IMMED {
            let mut raw = raw;
            let hit = f(&mut Chunk {
                buf: &mut raw[I_ORGAREA..I_ORGAREA + IMMED_LEN],
                big,
            });
            if hit {
                self.touch_times(&mut raw);
                self.put_user_raw(ino, &raw)?;
            }
            return Ok(hit);
        }
        let inode = self.fs.decode_inode(&raw);
        let bs = self.fs.bsize;
        for bn in 0..inode.size / bs {
            let Some(pb) = self.fs.bmap(&inode, bn)? else {
                continue;
            };
            let mut blk = self.fs.read_blocks(pb, 1)?;
            if f(&mut Chunk { buf: &mut blk, big }) {
                self.write_phys(pb, 0, &blk)?;
                let mut raw = raw;
                self.touch_times(&mut raw);
                self.put_user_raw(ino, &raw)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn lookup(&mut self, dir: u32, name: &[u8]) -> Result<Option<u32>, FilesystemError> {
        let mut found = None;
        self.with_dir(dir, |c| {
            found = c.find(name).map(|(_, ino)| ino);
            false
        })?;
        Ok(found)
    }

    fn dir_insert(&mut self, dir: u32, name: &[u8], ino: u32) -> Result<(), FilesystemError> {
        if self.with_dir(dir, |c| c.insert(name, ino))? {
            return Ok(());
        }
        // No room: move an immediate directory to a block, or add a block.
        let mut raw = self.user_raw(dir)?;
        let bs = self.fs.bsize as usize;
        let big = self.fs.big;
        let mut blk = vec![0u8; bs];
        Chunk { buf: &mut blk, big }.init(self.dir_nhash);
        let mut exts = Vec::new();
        if raw[I_ORG] == ORG_IMMED {
            let area = raw[I_ORGAREA..I_ORGAREA + IMMED_LEN].to_vec();
            let mut old = area.clone();
            let old = Chunk { buf: &mut old, big };
            for (pos, i, _, nl) in old.records() {
                if i != 0 {
                    let n = area[pos + DIRENT_HDR..pos + DIRENT_HDR + nl].to_vec();
                    Chunk { buf: &mut blk, big }.insert(&n, i);
                }
            }
        } else {
            exts = self.direct_extents(&raw)?;
        }
        if !(Chunk { buf: &mut blk, big }).insert(name, ino) {
            return Err(FilesystemError::InvalidData(format!(
                "VxFS: a {}-byte name does not fit an empty directory block",
                name.len()
            )));
        }
        let next = exts.last().map(|&(s, l)| s + l);
        if let Some(n) = next.filter(|&n| n < self.fs.total_blocks && self.is_free(n)) {
            // Grow the last extent in place, as HP-UX does when the next block is free.
            self.mark(n, 1, false);
            self.write_phys(n, 0, &blk)?;
            if let Some(e) = exts.last_mut() {
                e.1 += 1;
            }
        } else if exts.len() < NDADDR {
            let run = self.alloc(1, 1)?[0];
            self.write_phys(run.0, 0, &blk)?;
            exts.push(run);
        } else {
            exts = vec![self.relocate_dir(&exts, &blk)?];
        }
        let size = exts.iter().map(|e| e.1).sum::<u64>() * bs as u64;
        self.set_direct_extents(&mut raw, &exts);
        self.put64(&mut raw, I_SIZE, size);
        self.touch_times(&mut raw);
        self.put_user_raw(dir, &raw)
    }

    /// Move a directory whose extent slots are full into one contiguous run, plus `extra`.
    fn relocate_dir(
        &mut self,
        exts: &[(u64, u64)],
        extra: &[u8],
    ) -> Result<(u64, u64), FilesystemError> {
        let held: u64 = exts.iter().map(|e| e.1).sum();
        let run = self.alloc(held + 1, 1)?[0];
        let mut at = run.0;
        for &(s, l) in exts {
            let data = self.fs.read_blocks(s, l)?;
            self.write_phys(at, 0, &data)?;
            self.mark(s, l, true);
            at += l;
        }
        self.write_phys(at, 0, extra)?;
        Ok(run)
    }

    fn dir_remove(&mut self, dir: u32, name: &[u8]) -> Result<u32, FilesystemError> {
        let mut gone = None;
        self.with_dir(dir, |c| match c.find(name) {
            Some((pos, ino)) => {
                c.remove(pos);
                gone = Some(ino);
                true
            }
            None => false,
        })?;
        gone.ok_or_else(|| FilesystemError::NotFound(String::from_utf8_lossy(name).into_owned()))
    }

    fn dir_is_empty(&mut self, dir: u32) -> Result<bool, FilesystemError> {
        let mut nonempty = false;
        self.with_dir(dir, |c| {
            nonempty |= !c.is_empty();
            false
        })?;
        Ok(!nonempty)
    }

    fn adjust_nlink(&mut self, ino: u32, delta: i32) -> Result<(), FilesystemError> {
        let mut raw = self.user_raw(ino)?;
        let n = (self.fs.u32_at(&raw, I_NLINK) as i64 + delta as i64).max(0) as u32;
        self.put32(&mut raw, I_NLINK, n);
        self.put_user_raw(ino, &raw)
    }

    fn check_name(name: &str) -> Result<(), FilesystemError> {
        if name.is_empty() || name.len() > 255 || name.contains('/') || name == "." || name == ".."
        {
            return Err(FilesystemError::InvalidData(format!(
                "invalid VxFS name {name:?}"
            )));
        }
        Ok(())
    }

    fn entry_of(
        &mut self,
        parent: &FileEntry,
        name: &str,
        ino: u32,
    ) -> Result<FileEntry, FilesystemError> {
        let path = format!("{}/{}", parent.path.trim_end_matches('/'), name);
        self.fs.entry_for(name.to_string(), path, ino)
    }

    // ---- sync ----

    fn flush(&mut self) -> Result<(), FilesystemError> {
        if !self.touched {
            return Ok(());
        }
        let (secs, usec) = now();
        let mut efree = [0u64; 32];
        let mut free = 0u64;
        for au in 0..self.nau as usize {
            let range = au * AU_MAP_BYTES..(au + 1) * AU_MAP_BYTES;
            let (map, cnt) = regen_au(&self.maps[range.clone()]);
            free += (0..16)
                .map(|p| {
                    map[p * PIECE_BYTES..p * PIECE_BYTES + 256]
                        .iter()
                        .map(|b| b.count_ones() as u64)
                        .sum::<u64>()
                })
                .sum::<u64>();
            for (k, c) in cnt.iter().enumerate() {
                efree[k] += *c as u64;
            }
            if self.dirty[au] {
                self.maps[range].copy_from_slice(&map);
                let emap = self.emap.clone();
                self.write_file(&emap, (au * AU_MAP_BYTES) as u64, &map)?;
                let mut sum = [0u8; 32];
                for (k, c) in cnt.iter().enumerate() {
                    self.put16(&mut sum, k * 2, *c);
                }
                let aus = self.aus.clone();
                self.write_file(&aus, au as u64 * 32, &sum)?;
                self.dirty[au] = false;
            }
        }
        let mut sb = self.sb.clone();
        self.put32(&mut sb, SB_FREE, free as u32);
        for (k, n) in efree.iter().enumerate() {
            self.put32(&mut sb, SB_EFREE + k * 4, *n as u32);
        }
        self.put32(&mut sb, SB_TIME, secs);
        self.put32(&mut sb, SB_TIME + 4, usec);
        let sb_at = self.fs.sb_off;
        self.fs
            .reader
            .seek(SeekFrom::Start(self.fs.offset + sb_at))?;
        self.fs.reader.write_all(&sb)?;
        self.sb = sb;
        self.fs.free_blocks = free;

        // IAU: bitmap and its counters (free inodes, wholly free 8-inode groups).
        let rifree = (0..self.ninode).filter(|&i| self.inode_free(i)).count() as u32;
        let ribfree = (0..self.ninode / 8)
            .filter(|&g| (g * 8..g * 8 + 8).all(|i| self.inode_free(i)))
            .count() as u32;
        let mut counts = self.iau_counts.clone();
        self.put32(&mut counts, IAU_COUNTS_RIFREE, rifree);
        self.put32(&mut counts, IAU_COUNTS_RIBFREE, ribfree);
        let iau = self.iau.clone();
        let bs = self.fs.bsize;
        self.write_file(&iau, bs, &counts)?;
        let bitmap = self.iau_bitmap.clone();
        self.write_file(&iau, 2 * bs, &bitmap)?;
        self.iau_counts = counts;

        for b in self.fsh_blocks.clone() {
            let mut h = self.fs.read_blocks(b, 1)?;
            self.put32(&mut h, FSH_NINODE, self.ninode as u32);
            self.put32(&mut h, FSH_TIME, secs);
            self.put32(&mut h, FSH_TIME + 4, usec);
            self.put32(&mut h, FSH_MTIME, secs);
            self.put32(&mut h, FSH_MTIME + 4, usec);
            self.put32(&mut h, FSH_CHECKSUM, 0);
            let sum = (0..FSH_LEN / 4).fold(0u32, |a, i| a.wrapping_add(self.fs.u32_at(&h, i * 4)));
            self.put32(&mut h, FSH_CHECKSUM, sum);
            self.write_phys(b, 0, &h)?;
        }
        self.fs.reader.flush()?;
        self.touched = false;
        Ok(())
    }
}

fn now() -> (u32, u32) {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (d.as_secs() as u32, d.subsec_micros())
}

impl<R: Read + Write + Seek + Send> Filesystem for VxfsEditor<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        self.fs.root()
    }
    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        self.fs.list_directory(entry)
    }
    fn read_file(&mut self, entry: &FileEntry, max: usize) -> Result<Vec<u8>, FilesystemError> {
        self.fs.read_file(entry, max)
    }
    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        w: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        self.fs.write_file_to(entry, w)
    }
    fn volume_label(&self) -> Option<&str> {
        self.fs.volume_label()
    }
    fn fs_type(&self) -> &str {
        self.fs.fs_type()
    }
    fn total_size(&self) -> u64 {
        self.fs.total_size()
    }
    fn used_size(&self) -> u64 {
        self.fs.used_size()
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for VxfsEditor<R> {
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
        Self::check_name(name)?;
        let dir = parent.location as u32;
        if self.lookup(dir, name.as_bytes())?.is_some() {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }
        let bs = self.fs.bsize;
        let blocks = data_len.div_ceil(bs);
        let exts = if blocks > 0 {
            self.alloc(blocks, NDADDR)?
        } else {
            Vec::new()
        };
        let mut buf = vec![0u8; (1 << 20).max(bs as usize)];
        let mut left = data_len;
        for &(start, len) in &exts {
            let mut pos = 0u64;
            while pos < len * bs && left > 0 {
                let n = ((len * bs - pos).min(buf.len() as u64)).min(left.next_multiple_of(bs))
                    as usize;
                let real = (n as u64).min(left) as usize;
                data.read_exact(&mut buf[..real])?;
                buf[real..n].fill(0);
                self.write_phys(start, pos, &buf[..n])?;
                pos += n as u64;
                left -= real as u64;
            }
        }
        let ino = self.alloc_inode()?;
        let mode = S_IFREG | options.mode.unwrap_or(0o644) & 0o7777;
        let mut raw =
            self.new_inode_raw(mode, options.uid.unwrap_or(0), options.gid.unwrap_or(0), 1);
        self.set_direct_extents(&mut raw, &exts);
        self.put64(&mut raw, I_SIZE, data_len);
        if let Some(t) = options.unix_times.and_then(|t| t.mtime) {
            self.put32(&mut raw, I_MTIME, t as u32);
            self.put32(&mut raw, I_MTIME + 4, 0);
        }
        self.put_user_raw(ino, &raw)?;
        self.dir_insert(dir, name.as_bytes(), ino)?;
        self.entry_of(parent, name, ino)
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Self::check_name(name)?;
        let dir = parent.location as u32;
        if self.lookup(dir, name.as_bytes())?.is_some() {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }
        let ino = self.alloc_inode()?;
        let mode = S_IFDIR | options.mode.unwrap_or(0o755) & 0o7777;
        let mut raw =
            self.new_inode_raw(mode, options.uid.unwrap_or(0), options.gid.unwrap_or(0), 2);
        raw[I_ORG] = ORG_IMMED;
        self.put64(&mut raw, I_SIZE, IMMED_LEN as u64);
        self.put32(&mut raw, I_FTAREA, dir);
        Chunk {
            buf: &mut raw[I_ORGAREA..I_ORGAREA + IMMED_LEN],
            big: self.fs.big,
        }
        .init(0);
        self.put_user_raw(ino, &raw)?;
        self.dir_insert(dir, name.as_bytes(), ino)?;
        self.adjust_nlink(dir, 1)?;
        self.entry_of(parent, name, ino)
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let dir = parent.location as u32;
        let ino = entry.location as u32;
        let raw = self.user_raw(ino)?;
        let is_dir = self.fs.u32_at(&raw, I_MODE) & S_IFMT == S_IFDIR;
        if is_dir && !self.dir_is_empty(ino)? {
            return Err(FilesystemError::InvalidData(format!(
                "directory {} is not empty",
                entry.path
            )));
        }
        self.direct_extents(&raw).or_else(|e| {
            if raw[I_ORG] == ORG_IMMED {
                Ok(Vec::new())
            } else {
                Err(e)
            }
        })?;
        let removed = self.dir_remove(dir, entry.name.as_bytes())?;
        if removed != ino {
            return Err(FilesystemError::InvalidData(format!(
                "vxfs: {} names inode {removed}, expected {ino}",
                entry.path
            )));
        }
        let links = self.fs.u32_at(&raw, I_NLINK);
        if is_dir || links <= 1 {
            self.release_blocks(&raw)?;
            self.put_user_raw(ino, &vec![0u8; INODE_SIZE as usize])?;
            self.set_inode_free(ino as u64, true);
        } else {
            self.adjust_nlink(ino, -1)?;
        }
        if is_dir {
            self.adjust_nlink(dir, -1)?;
        }
        Ok(())
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        Self::check_name(new_name)?;
        let dir = parent.location as u32;
        if self.lookup(dir, new_name.as_bytes())?.is_some() {
            return Err(FilesystemError::AlreadyExists(new_name.into()));
        }
        let ino = self.dir_remove(dir, entry.name.as_bytes())?;
        self.dir_insert(dir, new_name.as_bytes(), ino)
    }

    fn set_permissions(&mut self, entry: &FileEntry, mode: u32) -> Result<(), FilesystemError> {
        let ino = entry.location as u32;
        let mut raw = self.user_raw(ino)?;
        let m = self.fs.u32_at(&raw, I_MODE) & !0o7777 | mode & 0o7777;
        self.put32(&mut raw, I_MODE, m);
        self.put_user_raw(ino, &raw)
    }

    fn set_owner(&mut self, entry: &FileEntry, uid: u32, gid: u32) -> Result<(), FilesystemError> {
        let ino = entry.location as u32;
        let mut raw = self.user_raw(ino)?;
        self.put32(&mut raw, I_UID, uid);
        self.put32(&mut raw, I_GID, gid);
        self.put_user_raw(ino, &raw)
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.flush()
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        let free = (0..self.fs.total_blocks)
            .filter(|&b| self.is_free(b))
            .count() as u64;
        Ok(free * self.fs.bsize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regen_of_a_wholly_free_au_is_one_order_15_extent() {
        let mut map = vec![0u8; AU_MAP_BYTES];
        for p in 0..16 {
            map[p * PIECE_BYTES..p * PIECE_BYTES + 256].fill(0xff);
        }
        let (out, cnt) = regen_au(&map);
        assert_eq!(cnt[15], 1);
        assert_eq!(cnt.iter().map(|&c| c as u32).sum::<u32>(), 1);
        assert!(
            (0..16).all(|p| out[p * PIECE_BYTES + 0x1ff] == 0b0000_0010),
            "level-11 bit per piece"
        );
        assert_eq!(out[0x100], 0, "levels below a whole piece stay clear");
    }

    #[test]
    fn regen_counts_split_buddies() {
        let mut map = vec![0u8; AU_MAP_BYTES];
        // Blocks 4-7 and 9 free in piece 0.
        map[0] = 0b0000_1111;
        map[1] = 0b0100_0000;
        let (out, cnt) = regen_au(&map);
        assert_eq!((cnt[0], cnt[2]), (1, 1));
        assert_eq!(
            out[0x180] & 0b0100_0000,
            0b0100_0000,
            "level-2 bit for blocks 4-7"
        );
        assert_eq!(out[0x100], 0, "no free pair");
    }

    #[test]
    fn chunk_insert_hashes_and_remove_merges() {
        let mut buf = vec![0u8; 1024];
        let mut c = Chunk {
            buf: &mut buf,
            big: true,
        };
        c.init(32);
        assert!(c.insert(b"file-number-10", 7));
        assert!(c.insert(b"file-number-20", 8));
        assert_eq!(
            c.u16(4 + 2 * 4),
            68,
            "byte sum of file-number-10 is bucket 4"
        );
        let (pos, ino) = c.find(b"file-number-20").unwrap();
        assert_eq!((ino, c.u16(4 + 2 * 5) as usize), (8, pos));
        c.remove(pos);
        assert!(c.find(b"file-number-20").is_none());
        assert_eq!(c.u16(4 + 2 * 5), 0, "chain unlinked");
        assert_eq!(c.u16(0) as usize, 1024 - 68 - dirent_len(14));
    }
}
