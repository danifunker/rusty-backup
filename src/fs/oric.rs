//! Oric Jasmin filesystem — the disk filesystem of the Jasmin floppy
//! controller for the Oric-1 / Atmos / Telestrat (and the MiSTer Oric core).
//!
//! Flat 256-byte-sector images, 17 sectors per track. A "block" is the linear
//! 0-based sector index; a "ref" is a big-endian `(track << 8) | sector` word
//! (sector 1..17). The host OS can't mount these, so browse / extract / add /
//! delete is the whole point.
//!
//! On-disk layout (from MAME `fs_oric_jasmin.cpp`, the authoritative reference
//! and — via `floptool` — the create + read oracle this module is tested
//! against):
//! - **Free map** at block `20*17 = 340`: a 3-byte little-endian word per
//!   track. Sector `s` (1..17) is free iff `word & (0x20000 >> s)`. `0x800000`
//!   marks a non-existent track. `word[0xf6]==word[0xf7]==0x80`, volume name
//!   (8 bytes) at offset `0xf8`.
//! - **Directory** chain from block `341`: 14 entries of 18 bytes at offset
//!   `4 + i*18`; entry = inode ref (`r16b` @0), U/L lock (@2), `NAME.EXT`
//!   (12 bytes @3), S/D seq/direct (@0xf), sector count (LE @0x10). The
//!   next-directory-sector ref is `r16b` @2.
//! - **Inode** (sector list, first block ref'd by a dir entry): next-inode ref
//!   (@0, `0xff00` = end), load address (LE @2), file length (LE @4, first
//!   inode only), then data-sector refs (`r16b`) from @6 in steps of 2.
//! - The **`.SYS`** pseudo-file (ref 0, name `        .SYS`) is the 0x3e00-byte
//!   boot area (blocks 0..61).

use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

const SECTOR: usize = 256;
const SPT: u32 = 17; // sectors per track
const FMAP_BLOCK: u32 = 20 * SPT; // 340
const DIR_BLOCK: u32 = 20 * SPT + 1; // 341
const MAX_TRACKS: u32 = 2 * 41; // 82 — MAME's fmap span
const ENTRIES_PER_DIR: usize = 14;
const ENTRY_LEN: usize = 18;
const SYS_LEN: usize = 0x3e00; // boot area, blocks 0..61

/// SS = 178432 bytes (697 blocks / 41 tracks); DS = 356864 (1394 / 82).
const SS_BYTES: u64 = 178_432;
const DS_BYTES: u64 = 356_864;

#[inline]
fn ref_track(r: u16) -> u32 {
    (r >> 8) as u32
}
#[inline]
fn ref_sector(r: u16) -> u32 {
    (r & 0xff) as u32
}
#[inline]
fn cs_to_block(r: u16) -> u32 {
    ref_track(r) * SPT + ref_sector(r) - 1
}
fn ref_valid(r: u16, block_count: u32) -> bool {
    let s = ref_sector(r);
    if !(1..=17).contains(&s) {
        return false;
    }
    ref_track(r) < block_count / SPT
}

/// Structural detector: an Oric Jasmin pure-sector image is exactly SS or DS
/// size, with the free map's `0x80` markers at fmap offsets 0xf6/0xf7.
pub fn looks_like_oric_jasmin<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> bool {
    let len = match reader.seek(SeekFrom::End(0)) {
        Ok(end) => end.saturating_sub(partition_offset),
        Err(_) => return false,
    };
    if len != SS_BYTES && len != DS_BYTES {
        return false;
    }
    let mut fmap = [0u8; SECTOR];
    if reader
        .seek(SeekFrom::Start(
            partition_offset + FMAP_BLOCK as u64 * SECTOR as u64,
        ))
        .is_err()
        || reader.read_exact(&mut fmap).is_err()
    {
        return false;
    }
    // Format markers + a plausible first free-map entry.
    if fmap[0xf6] != 0x80 || fmap[0xf7] != 0x80 {
        return false;
    }
    // Track 0's map word must not be the "absent" sentinel on a real volume.
    let w0 = u32::from_le_bytes([fmap[0], fmap[1], fmap[2], 0]);
    w0 != 0x80_0000
}

pub struct OricFilesystem<R> {
    reader: R,
    partition_offset: u64,
    data: Vec<u8>,
    block_count: u32,
    volume_label: String,
    dirty: bool,
}

impl<R: Read + Seek> OricFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let len = reader
            .seek(SeekFrom::End(0))?
            .saturating_sub(partition_offset);
        if len != SS_BYTES && len != DS_BYTES {
            return Err(FilesystemError::InvalidData(
                "not an Oric Jasmin image (size)".into(),
            ));
        }
        let mut data = vec![0u8; len as usize];
        reader.seek(SeekFrom::Start(partition_offset))?;
        reader.read_exact(&mut data)?;
        let block_count = (len / SECTOR as u64) as u32;

        // Structural validation: the free-map format markers must be present.
        let fmap_off = FMAP_BLOCK as usize * SECTOR;
        if data[fmap_off + 0xf6] != 0x80 || data[fmap_off + 0xf7] != 0x80 {
            return Err(FilesystemError::InvalidData(
                "not an Oric Jasmin image (free-map markers)".into(),
            ));
        }

        // Volume name from the free-map block (offset 0xf8, 8 bytes).
        let fmap = &data[FMAP_BLOCK as usize * SECTOR..];
        let name_raw = &fmap[0xf8..0x100];
        let volume_label = String::from_utf8_lossy(name_raw).trim_end().to_string();

        Ok(OricFilesystem {
            reader,
            partition_offset,
            data,
            block_count,
            volume_label,
            dirty: false,
        })
    }

    fn block(&self, b: u32) -> &[u8] {
        let o = b as usize * SECTOR;
        &self.data[o..o + SECTOR]
    }
    fn block_mut(&mut self, b: u32) -> &mut [u8] {
        let o = b as usize * SECTOR;
        self.dirty = true;
        &mut self.data[o..o + SECTOR]
    }
    fn r16b(&self, b: u32, off: usize) -> u16 {
        let s = self.block(b);
        u16::from_be_bytes([s[off], s[off + 1]])
    }
    fn r16l(&self, b: u32, off: usize) -> u16 {
        let s = self.block(b);
        u16::from_le_bytes([s[off], s[off + 1]])
    }

    /// Decode a 12-byte `NAME.EXT` field (space-padded, `.` at offset 8) into a
    /// clean display name (no trailing `.` when there is no extension).
    fn read_name(&self, b: u32, off: usize) -> String {
        let s = self.block(b);
        let p = &s[off..off + 12];
        let mut main_len = 8;
        while main_len > 0 && p[main_len - 1] == b' ' {
            main_len -= 1;
        }
        // Extension is the 3 chars after the '.' at offset 8.
        let mut ext_len = 3;
        while ext_len > 0 && p[9 + ext_len - 1] == b' ' {
            ext_len -= 1;
        }
        let mut name = String::new();
        for &c in &p[..main_len] {
            name.push(c as char);
        }
        if ext_len > 0 {
            name.push('.');
            for &c in &p[9..9 + ext_len] {
                name.push(c as char);
            }
        }
        name
    }

    fn is_system(&self, b: u32, off: usize) -> bool {
        let ref0 = self.r16b(b, off);
        ref0 == 0 && &self.block(b)[off + 0xb..off + 0xf] == b".SYS"
    }

    /// Walk the directory chain, yielding `(dir_block, entry_offset)` for each
    /// occupied slot.
    fn dir_entries(&self) -> Result<Vec<(u32, usize)>, FilesystemError> {
        let mut out = Vec::new();
        let mut bdir = DIR_BLOCK;
        let mut seen = HashSet::new();
        loop {
            if !seen.insert(bdir) {
                break; // cycle guard
            }
            for i in 0..ENTRIES_PER_DIR {
                let off = 4 + i * ENTRY_LEN;
                let fref = self.r16b(bdir, off);
                if ref_valid(fref, self.block_count) || self.is_system(bdir, off) {
                    out.push((bdir, off));
                }
            }
            let nref = self.r16b(bdir, 2);
            if nref == 0 || !ref_valid(nref, self.block_count) {
                break;
            }
            bdir = cs_to_block(nref);
        }
        Ok(out)
    }

    fn entry_to_file(&self, bdir: u32, off: usize) -> FileEntry {
        let name = self.read_name(bdir, off + 3);
        let size_blocks = self.r16l(bdir, off + 0x10) as u64;
        let locked = self.block(bdir)[off + 2] == b'L';
        let (size, _load) = if self.is_system(bdir, off) {
            (SYS_LEN as u64, 0u16)
        } else {
            let iref = self.r16b(bdir, off);
            let iblk = cs_to_block(iref);
            (self.r16l(iblk, 4) as u64, self.r16l(iblk, 2))
        };
        // `location` encodes the dir block + entry offset for later lookup.
        let location = ((bdir as u64) << 16) | off as u64;
        let mut e = FileEntry::new_file(name.clone(), format!("/{name}"), size, location);
        e.dos_attributes = Some(if locked { 0x01 } else { 0 });
        // Stash block count in aux (used by du/space projection) — not exposed.
        let _ = size_blocks;
        e
    }

    fn read_file_bytes(&self, bdir: u32, off: usize, max: usize) -> Vec<u8> {
        if self.is_system(bdir, off) {
            let mut data = Vec::with_capacity(SYS_LEN.min(max));
            for i in 0..62u32 {
                if data.len() >= max {
                    break;
                }
                data.extend_from_slice(self.block(i));
            }
            data.truncate(SYS_LEN.min(max));
            return data;
        }
        let mut data = Vec::new();
        let iref0 = self.r16b(bdir, off);
        let length = self.r16l(cs_to_block(iref0), 4) as usize;
        let want = length.min(max);
        let mut iref = iref0;
        let mut guard = 0;
        'outer: while ref_valid(iref, self.block_count) {
            guard += 1;
            if guard > 4096 {
                break;
            }
            let iblk = cs_to_block(iref);
            let mut pos = 6;
            while pos != SECTOR {
                if data.len() >= want {
                    break 'outer;
                }
                let dref = self.r16b(iblk, pos);
                if !ref_valid(dref, self.block_count) {
                    break 'outer;
                }
                data.extend_from_slice(self.block(cs_to_block(dref)));
                pos += 2;
            }
            // Next inode sector is at offset 0 (offset 2 is the load address).
            let nref = self.r16b(iblk, 0);
            if !ref_valid(nref, self.block_count) {
                break;
            }
            iref = nref;
        }
        data.truncate(want);
        data
    }
}

impl<R: Read + Seek + Send> Filesystem for OricFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        if entry.path != "/" {
            return Ok(Vec::new()); // Jasmin is flat (no subdirectories)
        }
        Ok(self
            .dir_entries()?
            .into_iter()
            .map(|(b, o)| self.entry_to_file(b, o))
            .collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "Oric read_file on directory: {}",
                entry.path
            )));
        }
        let bdir = (entry.location >> 16) as u32;
        let off = (entry.location & 0xffff) as usize;
        Ok(self.read_file_bytes(bdir, off, max_bytes))
    }

    fn volume_label(&self) -> Option<&str> {
        if self.volume_label.is_empty() {
            None
        } else {
            Some(&self.volume_label)
        }
    }

    fn fs_type(&self) -> &str {
        "Oric Jasmin"
    }

    fn total_size(&self) -> u64 {
        self.block_count as u64 * SECTOR as u64
    }

    fn used_size(&self) -> u64 {
        (self.block_count as u64 - self.free_block_count() as u64) * SECTOR as u64
    }

    fn allocation_unit(&self) -> Option<u64> {
        Some(SECTOR as u64)
    }

    fn validate_name(&self, name: &str) -> Result<(), FilesystemError> {
        validate_oric_name(name)
    }

    fn fsck(&mut self) -> Option<Result<super::fsck::FsckResult, FilesystemError>> {
        Some(self.run_fsck())
    }
}

impl<R: Read + Seek> OricFilesystem<R> {
    fn free_block_count(&self) -> u32 {
        let mut nf = 0;
        for track in 0..MAX_TRACKS {
            let map = self.fmap_word(track);
            if map != 0x80_0000 {
                for sect in 1..=17u32 {
                    if map & (0x20000 >> sect) != 0 {
                        nf += 1;
                    }
                }
            }
        }
        nf
    }

    fn fmap_word(&self, track: u32) -> u32 {
        let s = self.block(FMAP_BLOCK);
        let o = track as usize * 3;
        u32::from_le_bytes([s[o], s[o + 1], s[o + 2], 0])
    }
}

// ============================= create =============================

/// Prepare a 12-byte `NAME.EXT` field (mirrors MAME `file_name_prepare`).
fn prepare_name(name: &str) -> [u8; 12] {
    let b = name.as_bytes();
    let mut out = [b' '; 12];
    let mut i = 0;
    while i < 8 && i < b.len() && b[i] != b'.' {
        out[i] = b[i];
        i += 1;
    }
    out[8] = b'.';
    // Skip to after the '.'.
    while i < b.len() && b[i] != b'.' {
        i += 1;
    }
    if i < b.len() {
        i += 1;
    }
    let mut o = 9;
    while i < b.len() && o < 12 {
        out[o] = b[i];
        i += 1;
        o += 1;
    }
    out
}

/// Format a blank Oric Jasmin volume. `dual_sided` selects the DS (1394-block)
/// geometry; otherwise SS (697). Byte-identical to MAME's `format`.
pub fn create_blank_oric(dual_sided: bool, volume_name: &str) -> Vec<u8> {
    let block_count: u32 = if dual_sided { 1394 } else { 697 };
    let mut data = vec![0x6cu8; block_count as usize * SECTOR];

    let fmap_off = FMAP_BLOCK as usize * SECTOR;
    let put24 = |data: &mut [u8], base: usize, idx: usize, v: u32| {
        let o = base + idx * 3;
        data[o] = (v & 0xff) as u8;
        data[o + 1] = ((v >> 8) & 0xff) as u8;
        data[o + 2] = ((v >> 16) & 0xff) as u8;
    };
    let n_tracks = block_count / SPT;
    for track in 0..n_tracks {
        let v = if track * SPT == FMAP_BLOCK {
            0x07fff
        } else {
            0x1ffff
        };
        put24(&mut data, fmap_off, track as usize, v);
    }
    for track in n_tracks..(SPT * 42 * 2 / SPT) {
        put24(&mut data, fmap_off, track as usize, 0x80_0000);
    }
    data[fmap_off + 0xf6] = 0x80;
    data[fmap_off + 0xf7] = 0x80;
    let mut nm = [b' '; 8];
    for (i, c) in volume_name.bytes().take(8).enumerate() {
        nm[i] = c;
    }
    data[fmap_off + 0xf8..fmap_off + 0x100].copy_from_slice(&nm);

    // Empty directory sector.
    let dir_off = DIR_BLOCK as usize * SECTOR;
    for b in &mut data[dir_off..dir_off + SECTOR] {
        *b = 0xff;
    }
    data[dir_off] = 0;
    data[dir_off + 1] = 0;
    data[dir_off + 2] = 0;
    data[dir_off + 3] = 0;
    data
}

// ============================= editing =============================

impl<R: Read + Seek> OricFilesystem<R> {
    fn w16b(&mut self, b: u32, off: usize, v: u16) {
        let s = self.block_mut(b);
        s[off] = (v >> 8) as u8;
        s[off + 1] = (v & 0xff) as u8;
    }
    fn w16l(&mut self, b: u32, off: usize, v: u16) {
        let s = self.block_mut(b);
        s[off] = (v & 0xff) as u8;
        s[off + 1] = (v >> 8) as u8;
    }
    fn fmap_set_word(&mut self, track: u32, v: u32) {
        let o = track as usize * 3;
        let s = self.block_mut(FMAP_BLOCK);
        s[o] = (v & 0xff) as u8;
        s[o + 1] = ((v >> 8) & 0xff) as u8;
        s[o + 2] = ((v >> 16) & 0xff) as u8;
    }

    /// Allocate `count` free blocks, returning their refs (mirrors MAME
    /// `allocate_blocks`). Returns `None` if not enough free space.
    fn allocate_blocks(&mut self, count: u32) -> Option<Vec<u16>> {
        if self.free_block_count() < count {
            return None;
        }
        let mut blocks = Vec::new();
        let mut nf = 0;
        for track in 0..MAX_TRACKS {
            if nf == count {
                break;
            }
            let mut map = self.fmap_word(track);
            if map == 0x80_0000 {
                continue;
            }
            for sect in 1..=17u32 {
                if nf == count {
                    break;
                }
                if map & (0x20000 >> sect) != 0 {
                    blocks.push(((track << 8) | sect) as u16);
                    map &= !(0x20000 >> sect);
                    nf += 1;
                }
            }
            if map == 0 {
                map = 0x80_0000;
            }
            self.fmap_set_word(track, map);
        }
        Some(blocks)
    }

    fn free_blocks(&mut self, refs: &[u16]) {
        for &r in refs {
            let track = ref_track(r);
            let sect = ref_sector(r);
            let mut map = self.fmap_word(track);
            if map == 0x80_0000 {
                map = 0;
            }
            map |= 0x20000 >> sect;
            self.fmap_set_word(track, map);
        }
    }

    fn file_find(&self, name: &str) -> Option<(u32, usize)> {
        let prep = prepare_name(name);
        for (bdir, off) in self.dir_entries().ok()? {
            if self.block(bdir)[off + 3..off + 15] == prep {
                return Some((bdir, off));
            }
        }
        None
    }

    /// Collect every block owned by the file at `(bdir, off)`: its inode
    /// sectors and data sectors (as refs).
    fn file_blocks(&self, bdir: u32, off: usize) -> Vec<u16> {
        let mut refs = Vec::new();
        if self.is_system(bdir, off) {
            return refs;
        }
        let mut iref = self.r16b(bdir, off);
        let mut guard = 0;
        while ref_valid(iref, self.block_count) {
            guard += 1;
            if guard > 4096 {
                break;
            }
            refs.push(iref);
            let iblk = cs_to_block(iref);
            let mut pos = 6;
            while pos != SECTOR {
                let dref = self.r16b(iblk, pos);
                if !ref_valid(dref, self.block_count) {
                    break;
                }
                refs.push(dref);
                pos += 2;
            }
            // Next inode sector is at offset 0 (offset 2 is the load address).
            let nref = self.r16b(iblk, 0);
            if !ref_valid(nref, self.block_count) {
                break;
            }
            iref = nref;
        }
        refs
    }

    fn create_file_internal(&mut self, name: &str, data: &[u8]) -> Result<u64, FilesystemError> {
        validate_oric_name(name)?;
        if self.file_find(name).is_some() {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }
        let dsecs = data.len().div_ceil(SECTOR).max(1) as u32;
        let isecs = dsecs.div_ceil(125);
        let need_ns = dsecs + isecs;

        // Find a free directory slot; note whether a new dir sector is needed.
        let mut target: Option<(u32, usize)> = None;
        let mut last_dir = DIR_BLOCK;
        let mut seen = HashSet::new();
        'find: loop {
            if !seen.insert(last_dir) {
                break;
            }
            for i in 0..ENTRIES_PER_DIR {
                let off = 4 + i * ENTRY_LEN;
                let fref = self.r16b(last_dir, off);
                if !ref_valid(fref, self.block_count) && !self.is_system(last_dir, off) {
                    target = Some((last_dir, off));
                    break 'find;
                }
            }
            let nref = self.r16b(last_dir, 2);
            if nref == 0 || !ref_valid(nref, self.block_count) {
                break; // dir full — need a new sector
            }
            last_dir = cs_to_block(nref);
        }

        let new_dir_needed = target.is_none();
        let alloc = need_ns + if new_dir_needed { 1 } else { 0 };
        let blocks = self
            .allocate_blocks(alloc)
            .ok_or_else(|| FilesystemError::DiskFull("Oric: not enough free blocks".into()))?;

        let (bdir, off) = if new_dir_needed {
            let ndir_ref = *blocks.last().unwrap();
            let ndir = cs_to_block(ndir_ref);
            // Link from the last dir sector and initialize the new one.
            self.w16b(last_dir, 2, ndir_ref);
            {
                let s = self.block_mut(ndir);
                s.fill(0xff);
            }
            self.w16b(ndir, 0, ndir_ref); // self ref
            self.w16b(ndir, 2, 0xff00); // no next
            (ndir, 4usize)
        } else {
            target.unwrap()
        };

        // Write the inode chain + data blocks (blocks[0..need_ns]).
        let mut i = 0u32;
        while i < need_ns {
            let iref = blocks[i as usize];
            let iblk = cs_to_block(iref);
            {
                let s = self.block_mut(iblk);
                s.fill(0xff);
            }
            if i == 0 {
                self.w16l(iblk, 2, 0x0500); // load address
                self.w16l(iblk, 4, data.len() as u16);
            }
            if i + 126 < need_ns {
                self.w16b(iblk, 0, blocks[(i + 126) as usize]);
            } else {
                self.w16b(iblk, 0, 0xff00);
            }
            let mut j = 0u32;
            while j != 125 && i + j + 1 != need_ns {
                let dpos = SECTOR * (j as usize + (i as usize / 126) * 125);
                let dref = blocks[(i + j + 1) as usize];
                self.w16b(iblk, 6 + j as usize * 2, dref);
                let dblk = cs_to_block(dref);
                let remaining = data.len().saturating_sub(dpos);
                let s = self.block_mut(dblk);
                if remaining >= SECTOR {
                    s.copy_from_slice(&data[dpos..dpos + SECTOR]);
                } else {
                    s[..remaining].copy_from_slice(&data[dpos..]);
                    for b in &mut s[remaining..] {
                        *b = 0x55;
                    }
                }
                j += 1;
            }
            i += 126;
        }

        // Directory entry.
        self.w16b(bdir, off, blocks[0]);
        self.block_mut(bdir)[off + 2] = b'U';
        let prep = prepare_name(name);
        self.block_mut(bdir)[off + 3..off + 15].copy_from_slice(&prep);
        self.block_mut(bdir)[off + 0xf] = b'S';
        self.w16l(bdir, off + 0x10, need_ns as u16);

        Ok(((bdir as u64) << 16) | off as u64)
    }

    fn delete_file_internal(&mut self, bdir: u32, off: usize) -> Result<(), FilesystemError> {
        let refs = self.file_blocks(bdir, off);
        self.free_blocks(&refs);
        let s = self.block_mut(bdir);
        for b in &mut s[off..off + ENTRY_LEN] {
            *b = 0xff;
        }
        Ok(())
    }

    fn run_fsck(&mut self) -> Result<super::fsck::FsckResult, FilesystemError> {
        use super::fsck::{FsckIssue, FsckResult, FsckStats};
        let mut errors = Vec::new();
        let mut files = 0u32;
        let mut owned: HashSet<u16> = HashSet::new();
        let err = |code: &str, msg: String| FsckIssue {
            code: code.into(),
            message: msg,
            repairable: false,
            debug: false,
        };
        let claim = |errors: &mut Vec<FsckIssue>, owned: &mut HashSet<u16>, r: u16, what: &str| {
            if !owned.insert(r) {
                errors.push(FsckIssue {
                    code: "CrossLink".into(),
                    message: format!(
                        "block {}/{} used by more than one object ({what})",
                        ref_track(r),
                        ref_sector(r)
                    ),
                    repairable: false,
                    debug: false,
                });
            }
        };

        let dir_list = match self.dir_entries() {
            Ok(d) => d,
            Err(e) => {
                errors.push(err("DirWalk", e.to_string()));
                Vec::new()
            }
        };
        for (bdir, off) in &dir_list {
            if self.is_system(*bdir, *off) {
                files += 1;
                continue;
            }
            files += 1;
            let iref0 = self.r16b(*bdir, *off);
            if !ref_valid(iref0, self.block_count) {
                errors.push(err(
                    "BadInodeRef",
                    format!("{}: invalid inode ref", self.read_name(*bdir, *off + 3)),
                ));
                continue;
            }
            for r in self.file_blocks(*bdir, *off) {
                if !ref_valid(r, self.block_count) {
                    errors.push(err(
                        "BadDataRef",
                        format!(
                            "{}: out-of-range block ref",
                            self.read_name(*bdir, *off + 3)
                        ),
                    ));
                } else {
                    claim(&mut errors, &mut owned, r, "file");
                }
            }
        }

        // Every owned block must be marked used (free bit clear) in the fmap.
        let mut alloc_but_free = 0u32;
        for &r in &owned {
            let track = ref_track(r);
            let sect = ref_sector(r);
            let map = self.fmap_word(track);
            if map != 0x80_0000 && map & (0x20000 >> sect) != 0 {
                alloc_but_free += 1;
            }
        }
        if alloc_but_free > 0 {
            errors.push(err(
                "AllocatedButFree",
                format!("{alloc_but_free} in-use block(s) marked free in the map"),
            ));
        }

        let free = self.free_block_count();
        Ok(FsckResult {
            errors,
            warnings: Vec::new(),
            stats: FsckStats {
                files_checked: files,
                directories_checked: 1,
                extra: vec![
                    ("free_blocks".into(), free.to_string()),
                    ("total_blocks".into(), self.block_count.to_string()),
                ],
            },
            repairable: false,
            orphaned_entries: Vec::new(),
        })
    }
}

impl<R: Read + Write + Seek + Send> super::filesystem::EditableFilesystem for OricFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn Read,
        _data_len: u64,
        _options: &super::filesystem::CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "Oric Jasmin has no subdirectories".into(),
            ));
        }
        let mut buf = Vec::new();
        data.read_to_end(&mut buf)?;
        let location = self.create_file_internal(name, &buf)?;
        Ok(FileEntry::new_file(
            name.to_string(),
            format!("/{name}"),
            buf.len() as u64,
            location,
        ))
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &super::filesystem::CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "Oric Jasmin has no subdirectories".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        _parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let bdir = (entry.location >> 16) as u32;
        let off = (entry.location & 0xffff) as usize;
        self.delete_file_internal(bdir, off)
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        if self.dirty {
            self.reader.seek(SeekFrom::Start(self.partition_offset))?;
            self.reader.write_all(&self.data)?;
            self.reader.flush().map_err(FilesystemError::Io)?;
            self.dirty = false;
        }
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.free_block_count() as u64 * SECTOR as u64)
    }
}

/// Validate a filename against Oric Jasmin rules (8.3, mirrors MAME's
/// `validate_filename`).
pub fn validate_oric_name(name: &str) -> Result<(), FilesystemError> {
    if !name.is_ascii() {
        return Err(FilesystemError::InvalidData(
            "Oric names must be ASCII".into(),
        ));
    }
    match name.find('.') {
        Some(pos) => {
            if pos == 0 || pos > 8 || name.len() - pos - 1 > 3 {
                return Err(FilesystemError::InvalidData(
                    "Oric name must be 8.3 (name 1-8, ext 0-3)".into(),
                ));
            }
        }
        None => {
            if name.is_empty() || name.len() > 8 {
                return Err(FilesystemError::InvalidData(
                    "Oric name must be 1-8 chars".into(),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::filesystem::{CreateFileOptions, EditableFilesystem};
    use super::*;
    use std::io::Cursor;
    use std::process::Command;

    fn floptool() -> bool {
        Command::new("floptool").arg("version").output().is_ok()
    }

    fn rootent() -> FileEntry {
        FileEntry::new_directory("/".into(), "/".into(), 0)
    }

    fn tmp(suffix: &str) -> std::path::PathBuf {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!("rb_oric_{}_{}_{}", std::process::id(), n, suffix))
    }

    /// `floptool flopdir` output over an image's bytes.
    fn floptool_dir(img: &[u8]) -> String {
        let p = tmp("dir.dsk");
        std::fs::write(&p, img).unwrap();
        let out = Command::new("floptool")
            .args(["flopdir", "oric_jasmin", "oric_jasmin", p.to_str().unwrap()])
            .output()
            .unwrap();
        let _ = std::fs::remove_file(&p);
        assert!(
            out.status.success(),
            "floptool flopdir failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    fn floptool_blank() -> Option<Vec<u8>> {
        if !floptool() {
            return None;
        }
        let p = tmp("blank.dsk");
        let ok = Command::new("floptool")
            .args([
                "flopcreate",
                "oric_jasmin",
                "oric_jasmin_ss",
                p.to_str().unwrap(),
            ])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        let bytes = if ok { std::fs::read(&p).ok() } else { None };
        let _ = std::fs::remove_file(&p);
        bytes
    }

    #[test]
    fn our_blank_matches_floptool_and_reads_in_it() {
        // floptool's flopcreate defaults the volume name to "UNTITLED"; match
        // it so the comparison is byte-for-byte across the whole image.
        let img = create_blank_oric(false, "UNTITLED");
        assert_eq!(img.len(), SS_BYTES as usize);
        if let Some(ft) = floptool_blank() {
            assert_eq!(
                img, ft,
                "our create_blank differs from floptool's flopcreate"
            );
        }
        // And Rust reads its own blank as an empty volume.
        let mut fs = OricFilesystem::open(Cursor::new(img), 0).unwrap();
        assert_eq!(fs.volume_label(), Some("UNTITLED"));
        assert!(fs.list_directory(&rootent()).unwrap().is_empty());
        assert!(fs.fsck().unwrap().unwrap().is_clean());
    }

    #[test]
    fn reads_floptool_blank() {
        let Some(img) = floptool_blank() else {
            eprintln!("skipping: floptool unavailable");
            return;
        };
        let mut fs = OricFilesystem::open(Cursor::new(img), 0).unwrap();
        assert_eq!(fs.total_size(), SS_BYTES);
        assert!(fs.list_directory(&rootent()).unwrap().is_empty());
    }

    #[test]
    fn create_files_verified_by_floptool() {
        let img = create_blank_oric(false, "DATA");
        let mut fs = OricFilesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        let small = b"HELLO ORIC\n";
        let big: Vec<u8> = (0..5000u32).map(|i| (i * 7) as u8).collect();
        fs.create_file(
            &root,
            "HELLO",
            &mut Cursor::new(small.to_vec()),
            small.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        fs.create_file(
            &root,
            "DATA.BIN",
            &mut Cursor::new(big.clone()),
            big.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        fs.sync_metadata().unwrap();
        let img = fs.data.clone();

        // Rust reads them back.
        let mut fs2 = OricFilesystem::open(Cursor::new(img.clone()), 0).unwrap();
        let names: Vec<String> = fs2
            .list_directory(&rootent())
            .unwrap()
            .iter()
            .map(|e| e.name.clone())
            .collect();
        assert!(names.contains(&"HELLO".to_string()));
        assert!(names.contains(&"DATA.BIN".to_string()));
        let e = fs2
            .list_directory(&rootent())
            .unwrap()
            .into_iter()
            .find(|e| e.name == "DATA.BIN")
            .unwrap();
        assert_eq!(fs2.read_file(&e, usize::MAX).unwrap(), big);
        assert!(fs2.fsck().unwrap().unwrap().is_clean());

        // The independent oracle (MAME's oric_jasmin driver, via floptool
        // flopdir) parses our directory + inodes and reports each file's name
        // and length — confirming the entries + sector-list inodes are
        // correctly structured. (floptool 0.264's flopread has a name-matching
        // bug for this fs, so content is validated by the Rust round-trip
        // above; flopdir independently confirms name + length + block count.)
        if floptool() {
            let dir = floptool_dir(&img);
            // HELLO (11 bytes = 0xb), DATA.BIN (5000 = 0x1388).
            assert!(dir.contains("HELLO"), "floptool dir missing HELLO:\n{dir}");
            assert!(dir.contains("0xb"), "floptool wrong HELLO length:\n{dir}");
            assert!(
                dir.contains("DATA.BIN"),
                "floptool dir missing DATA.BIN:\n{dir}"
            );
            assert!(
                dir.contains("0x1388"),
                "floptool wrong DATA.BIN length:\n{dir}"
            );
        }
    }

    #[test]
    fn delete_verified_by_floptool() {
        let img = create_blank_oric(false, "DEL");
        let mut fs = OricFilesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        for n in ["KEEP", "GONE.DAT", "KEEP2"] {
            let d = vec![0xAAu8; 900];
            fs.create_file(
                &root,
                n,
                &mut Cursor::new(d.clone()),
                d.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        }
        let free_before = fs.free_space().unwrap();
        let gone = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "GONE.DAT")
            .unwrap();
        fs.delete_entry(&root, &gone).unwrap();
        fs.sync_metadata().unwrap();
        assert!(fs.free_space().unwrap() > free_before);
        let img = fs.data.clone();

        let mut fs2 = OricFilesystem::open(Cursor::new(img.clone()), 0).unwrap();
        let names: Vec<String> = fs2
            .list_directory(&rootent())
            .unwrap()
            .iter()
            .map(|e| e.name.clone())
            .collect();
        assert_eq!(names, vec!["KEEP", "KEEP2"]);
        assert!(fs2.fsck().unwrap().unwrap().is_clean());
        if floptool() {
            let dir = floptool_dir(&img);
            assert!(!dir.contains("GONE"), "deleted file still listed:\n{dir}");
            assert!(dir.contains("KEEP"));
        }
    }

    #[test]
    fn many_files_fill_second_dir_sector() {
        // 14 entries per dir sector; 20 files force a second directory sector.
        let img = create_blank_oric(false, "MANY");
        let mut fs = OricFilesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        for i in 0..20 {
            let d = format!("file {i}").into_bytes();
            fs.create_file(
                &root,
                &format!("F{i:02}"),
                &mut Cursor::new(d.clone()),
                d.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        }
        fs.sync_metadata().unwrap();
        let img = fs.data.clone();
        let mut fs2 = OricFilesystem::open(Cursor::new(img.clone()), 0).unwrap();
        assert_eq!(fs2.list_directory(&rootent()).unwrap().len(), 20);
        assert!(fs2.fsck().unwrap().unwrap().is_clean());
        if floptool() {
            let dir = floptool_dir(&img);
            for i in [0, 13, 14, 19] {
                assert!(dir.contains(&format!("F{i:02}")), "missing F{i:02}:\n{dir}");
            }
        }
    }

    #[test]
    fn name_validation() {
        assert!(validate_oric_name("HELLO").is_ok());
        assert!(validate_oric_name("HELLO.BIN").is_ok());
        assert!(validate_oric_name("").is_err());
        assert!(validate_oric_name("TOOLONGNAME").is_err());
        assert!(validate_oric_name("A.TOOLONG").is_err());
    }

    #[test]
    fn rejects_non_oric() {
        assert!(OricFilesystem::open(Cursor::new(vec![0u8; 178_432]), 0).is_err());
        assert!(OricFilesystem::open(Cursor::new(vec![0u8; 1000]), 0).is_err());
    }
}
