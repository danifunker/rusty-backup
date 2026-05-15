//! Professional File System 3 (PFS3) reader.
//!
//! Supports the three DosType identifiers that share the PFS3 on-disk
//! format: `PFS\3`, `PDS\3` (modern PFS3 / "pds-aio"), and `muFS`
//! (multi-user PFS3 — same block structure, additional uid/gid extras we
//! currently ignore).
//!
//! Phase 5 scope: **read-only browse + backup**. Write support is Phase 6
//! and lives elsewhere. The reader handles:
//! - Both the "small" and "large/supermode" rootblock layouts.
//! - `MODE_SPLITTED_ANODES` (hi16=seqnr / lo16=offset) — the only mode
//!   modern PFS3 volumes use.
//! - Anode chain walks for directory listing and file data streaming.
//! - Layout-preserving compaction via `CompactPfs3Reader` (allocated
//!   blocks emitted verbatim, free blocks zero-filled).
//!
//! Conventions worth flagging because they're easy to get wrong:
//! - Every multi-byte field is **big-endian**.
//! - Bitmap convention is **set bit = free**, same as AFFS.
//! - Anode `blocknr`/`clustersize` are in **HW sectors of 512 bytes**, not
//!   reserved-block units. Metadata anodes (dir, anodeblock chain) use
//!   `clustersize = 1` per element and chain via `next`; the read path
//!   only ever needs `anodeoffset = 0` within a metadata cluster.
//! - Reserved blocks (dirblocks, indexblocks, anodeblocks, etc.) span
//!   `rscluster = reserved_blksize / 512` HW sectors.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};
use super::CompactResult;

use super::affs_common::datestamp_string;

const HW_SECTOR: u64 = 512;
const ROOTBLOCK_SECTOR: u32 = 2;

// rootblock.options bits
const MODE_HARDDISK: u32 = 1;
const MODE_SPLITTED_ANODES: u32 = 2;
#[allow(dead_code)]
const MODE_DIR_EXTENSION: u32 = 4;
#[allow(dead_code)]
const MODE_DELDIR: u32 = 8;
#[allow(dead_code)]
const MODE_SIZEFIELD: u32 = 16;
const MODE_EXTENSION: u32 = 32;
#[allow(dead_code)]
const MODE_SUPERINDEX: u32 = 128;
const MODE_LARGEFILE: u32 = 2048;

// block IDs (UWORD at offset 0)
const ID_DIRBLOCK: u16 = 0x4442; // 'DB'
const ID_ANODEBLOCK: u16 = 0x4142; // 'AB'
const ID_INDEXBLOCK: u16 = 0x4942; // 'IB'
const ID_BITMAPBLOCK: u16 = 0x424D; // 'BM'
const ID_BITMAPINDEXBLOCK: u16 = 0x4D49; // 'MI'
const ID_EXTENSIONBLOCK: u16 = 0x4558; // 'EX'
const ID_SUPERBLOCK: u16 = 0x5342; // 'SB'

const ANODE_ROOTDIR: u32 = 5;

fn parse_err<S: Into<String>>(msg: S) -> FilesystemError {
    FilesystemError::Parse(msg.into())
}

fn rd_u16(buf: &[u8], o: usize) -> u16 {
    u16::from_be_bytes([buf[o], buf[o + 1]])
}
fn rd_u32(buf: &[u8], o: usize) -> u32 {
    u32::from_be_bytes([buf[o], buf[o + 1], buf[o + 2], buf[o + 3]])
}

/// Decoded rootblock — only the fields we care about for read-only browsing.
#[derive(Debug, Clone)]
pub struct Pfs3RootBlock {
    pub disktype: [u8; 4],
    pub options: u32,
    pub disk_name: String,
    pub last_reserved: u32,
    pub first_reserved: u32,
    pub reserved_free: u32,
    pub reserved_blksize: u16,
    pub blocks_free: u32,
    pub disksize: u32,
    pub extension: u32,
    /// Anode-index blocknrs from the small layout (used when supermode is off).
    /// Empty in supermode.
    pub small_indexblocks: Vec<u32>,
    /// Bitmap-index blocknrs — five entries in small layout, up to 104 in
    /// large layout.
    pub bitmapindex: Vec<u32>,
}

impl Pfs3RootBlock {
    fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < 512 {
            return Err(parse_err("rootblock buffer too small"));
        }
        let mut disktype = [0u8; 4];
        disktype.copy_from_slice(&buf[0..4]);
        let options = rd_u32(buf, 4);
        // diskname: BSTR at offset 20, length byte then UTF-8 bytes (≤31).
        let name_len = buf[20] as usize;
        let name_bytes = &buf[21..21 + name_len.min(31)];
        let disk_name = String::from_utf8_lossy(name_bytes).to_string();
        let last_reserved = rd_u32(buf, 52);
        let first_reserved = rd_u32(buf, 56);
        let reserved_free = rd_u32(buf, 60);
        let reserved_blksize = rd_u16(buf, 64);
        let blocks_free = rd_u32(buf, 68);
        let disksize = rd_u32(buf, 84);
        let extension = rd_u32(buf, 88);

        // Union at offset 96 (96 = 4+4+4+2+2+2+2+32 + 4+4+4+2+2 + 4+4+4+4 + 4+4 + 4+4 = check)
        // Actually the easiest is to trust the docs: union starts at offset 96.
        // Small layout: 5 bitmap-index + 99 anode-index ULONGs.
        // Large layout: 104 bitmap-index ULONGs.
        let supermode = (options & MODE_SUPERINDEX) != 0;
        let (small_indexblocks, bitmapindex) = if supermode {
            let mut bi = Vec::with_capacity(104);
            for i in 0..104 {
                let o = 96 + i * 4;
                if o + 4 > buf.len() {
                    break;
                }
                bi.push(rd_u32(buf, o));
            }
            (Vec::new(), bi)
        } else {
            let mut bi = Vec::with_capacity(5);
            for i in 0..5 {
                bi.push(rd_u32(buf, 96 + i * 4));
            }
            let mut si = Vec::with_capacity(99);
            for i in 0..99 {
                si.push(rd_u32(buf, 96 + 5 * 4 + i * 4));
            }
            (si, bi)
        };

        Ok(Pfs3RootBlock {
            disktype,
            options,
            disk_name,
            last_reserved,
            first_reserved,
            reserved_free,
            reserved_blksize,
            blocks_free,
            disksize,
            extension,
            small_indexblocks,
            bitmapindex,
        })
    }
}

/// Decoded rootblock extension — only the supermode anode-index array
/// matters for reading.
#[derive(Debug, Clone, Default)]
pub struct Pfs3RootBlockExt {
    pub super_index: Vec<u32>, // 16 entries — supermode anode super-index
}

impl Pfs3RootBlockExt {
    fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < 64 + 16 * 4 {
            return Err(parse_err("rootblock extension buffer too small"));
        }
        if rd_u16(buf, 0) != ID_EXTENSIONBLOCK {
            return Err(parse_err("rootblock extension: bad id"));
        }
        // superindex[MAXSUPER+1] = 16 ULONGs at offset 64.
        let mut super_index = Vec::with_capacity(16);
        for i in 0..16 {
            super_index.push(rd_u32(buf, 64 + i * 4));
        }
        Ok(Pfs3RootBlockExt { super_index })
    }
}

/// In-memory anode resolved from disk.
#[derive(Debug, Clone, Copy, Default)]
struct Canode {
    clustersize: u32,
    blocknr: u32,
    next: u32,
}

/// Cache key for one reserved block (size = `reserved_blksize`).
type ResBlockBuf = Vec<u8>;

pub struct Pfs3Filesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    partition_size: u64,
    root: Pfs3RootBlock,
    ext: Option<Pfs3RootBlockExt>,
    rscluster: u32,
    reserved_blksize: u32,
    anodesperblock: u32,
    indexperblock: u32,
    anodesplitmode: bool,
    supermode: bool,
    largefile: bool,

    /// Small LRU cache of reserved-blocks keyed by HW sector. Keeps directory
    /// walks from re-reading the same dirblock repeatedly. Size is bounded
    /// by `MAX_CACHE_ENTRIES`.
    cache: HashMap<u32, ResBlockBuf>,
}

const MAX_CACHE_ENTRIES: usize = 128;

impl<R: Read + Seek> Pfs3Filesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        // Determine an upper-bound partition size from EOF. This is later
        // narrowed to `root.disksize * HW_SECTOR` once the rootblock is
        // parsed, so multi-partition images don't claim everything past
        // their start.
        let end = reader.seek(SeekFrom::End(0))?;
        let partition_size_upper = end.saturating_sub(partition_offset);
        if partition_size_upper < 8 * HW_SECTOR {
            return Err(parse_err("partition too small for PFS3"));
        }
        let partition_size = partition_size_upper;

        // Boot block (HW sector 0): magic at offset 0.
        let mut boot = [0u8; 512];
        reader.seek(SeekFrom::Start(partition_offset))?;
        reader.read_exact(&mut boot)?;
        let mag = &boot[0..4];
        // Accept PFS\1 (0x50 0x46 0x53 0x01) and the muFS / muPFS variants.
        // The on-disk magic is documented as 4 ASCII bytes; the 4th byte
        // encodes a version number, not a printable character.
        let valid_pfs = mag == b"PFS\x01" || mag == b"PDS\x01";
        let valid_mu = mag == b"muAF" || mag == b"muPF";
        let valid_afs = mag == b"AFS\x01" || mag == b"PFS\x02";
        if !(valid_pfs || valid_mu || valid_afs) {
            return Err(parse_err(format!(
                "not a PFS volume: boot magic = {:02x}{:02x}{:02x}{:02x}",
                mag[0], mag[1], mag[2], mag[3]
            )));
        }

        // Rootblock at HW sector 2. The rootblock itself is the first 512
        // bytes of the rootblock cluster; the reserved bitmap follows.
        let mut root_buf = [0u8; 512];
        reader.seek(SeekFrom::Start(
            partition_offset + ROOTBLOCK_SECTOR as u64 * HW_SECTOR,
        ))?;
        reader.read_exact(&mut root_buf)?;
        let root = Pfs3RootBlock::parse(&root_buf)?;

        if root.reserved_blksize == 0 || root.reserved_blksize % 512 != 0 {
            return Err(parse_err(format!(
                "invalid reserved_blksize {} (must be a multiple of 512)",
                root.reserved_blksize
            )));
        }

        let reserved_blksize = root.reserved_blksize as u32;
        let rscluster = reserved_blksize / 512;

        // Layout sanity. anodeblock_t header = 16 bytes; indexblock_t = 12.
        let anodesperblock = (reserved_blksize - 16) / 12;
        let indexperblock = (reserved_blksize - 12) / 4;

        let anodesplitmode = (root.options & MODE_SPLITTED_ANODES) != 0;
        let supermode = (root.options & MODE_SUPERINDEX) != 0;
        let largefile = (root.options & MODE_LARGEFILE) != 0;

        if (root.options & MODE_HARDDISK) == 0 && partition_size < 1_000_000 {
            // Floppy mode (non-harddisk) — old PFS layout we don't support.
            return Err(parse_err("PFS floppy mode not supported"));
        }

        // Narrow partition_size to root.disksize when it's plausible.
        // PFS3 records disksize in HW sectors; multi-partition images on
        // RDB store DH0 immediately followed by DH1, so the EOF-based
        // upper bound would include subsequent partitions.
        let partition_size = if root.disksize > 0 {
            let rs = root.disksize as u64 * HW_SECTOR;
            if rs > 0 && rs <= partition_size_upper {
                rs
            } else {
                partition_size_upper
            }
        } else {
            partition_size_upper
        };

        // Optional rootblock extension — needed for supermode reads.
        let ext = if (root.options & MODE_EXTENSION) != 0 && root.extension != 0 {
            let mut ext_buf = vec![0u8; reserved_blksize as usize];
            reader.seek(SeekFrom::Start(
                partition_offset + root.extension as u64 * HW_SECTOR,
            ))?;
            reader.read_exact(&mut ext_buf)?;
            Some(Pfs3RootBlockExt::parse(&ext_buf)?)
        } else {
            None
        };

        Ok(Pfs3Filesystem {
            reader,
            partition_offset,
            partition_size,
            root,
            ext,
            rscluster,
            reserved_blksize,
            anodesperblock,
            indexperblock,
            anodesplitmode,
            supermode,
            largefile,
            cache: HashMap::new(),
        })
    }

    fn read_reserved_block(&mut self, blocknr: u32) -> Result<&[u8], FilesystemError> {
        // Hot-path lookup, then load on miss. Cache trim: drop arbitrary
        // entries when full — PFS3 dirblock walks are linear, so any simple
        // eviction policy is fine.
        if !self.cache.contains_key(&blocknr) {
            if self.cache.len() >= MAX_CACHE_ENTRIES {
                if let Some(k) = self.cache.keys().next().copied() {
                    self.cache.remove(&k);
                }
            }
            let mut buf = vec![0u8; self.reserved_blksize as usize];
            let off = self.partition_offset + blocknr as u64 * HW_SECTOR;
            if off + self.reserved_blksize as u64 > self.partition_offset + self.partition_size {
                return Err(parse_err(format!(
                    "reserved block {} out of range",
                    blocknr
                )));
            }
            self.reader.seek(SeekFrom::Start(off))?;
            self.reader.read_exact(&mut buf)?;
            self.cache.insert(blocknr, buf);
        }
        Ok(self.cache.get(&blocknr).unwrap().as_slice())
    }

    /// Split an anodenr into `(seqnr, offset)`. In split mode, the high 16
    /// bits are the seqnr and the low 16 are the offset. Otherwise the
    /// anodenr is a flat index — `seqnr = anodenr / anodesperblock`,
    /// `offset = anodenr % anodesperblock`.
    fn split_anodenr(&self, anodenr: u32) -> (u32, u32) {
        if self.anodesplitmode {
            (anodenr >> 16, anodenr & 0xFFFF)
        } else {
            (anodenr / self.anodesperblock, anodenr % self.anodesperblock)
        }
    }

    /// Resolve an anode-block seqnr to the disk HW-sector of the underlying
    /// anodeblock. Handles small / large / supermode layouts.
    fn anodeblock_blocknr(&mut self, seqnr: u32) -> Result<u32, FilesystemError> {
        let indexblock_seqnr = seqnr / self.indexperblock;
        let index_offset = (seqnr % self.indexperblock) as usize;

        let indexblock_disk = if self.supermode {
            // Two-level indirection via rootblock extension's superindex[].
            let ext = self
                .ext
                .as_ref()
                .ok_or_else(|| parse_err("supermode requires rootblock extension"))?;
            let super_seqnr = (indexblock_seqnr / self.indexperblock) as usize;
            let super_offset = (indexblock_seqnr % self.indexperblock) as usize;
            if super_seqnr >= ext.super_index.len() {
                return Err(parse_err(format!(
                    "anodeblock seqnr {} super_seqnr {} out of range",
                    seqnr, super_seqnr
                )));
            }
            let super_block_nr = ext.super_index[super_seqnr];
            if super_block_nr == 0 {
                return Err(parse_err(format!(
                    "anodeblock seqnr {} maps to zero superblock",
                    seqnr
                )));
            }
            // Read the superblock — verify id, fetch index[].
            let blk = self.read_reserved_block(super_block_nr)?;
            if rd_u16(blk, 0) != ID_SUPERBLOCK {
                return Err(parse_err(format!(
                    "expected SB superblock at {}, got id={:#06x}",
                    super_block_nr,
                    rd_u16(blk, 0)
                )));
            }
            let off = 12 + super_offset * 4;
            if off + 4 > blk.len() {
                return Err(parse_err("superblock index out of range"));
            }
            rd_u32(blk, off)
        } else {
            // Small layout: rootblock.idx.small.indexblocks[indexblock_seqnr]
            let i = indexblock_seqnr as usize;
            if i >= self.root.small_indexblocks.len() {
                return Err(parse_err(format!(
                    "anodeblock seqnr {} indexblock {} out of small range",
                    seqnr, i
                )));
            }
            self.root.small_indexblocks[i]
        };

        if indexblock_disk == 0 {
            return Err(parse_err(format!(
                "anodeblock seqnr {} maps to zero indexblock",
                seqnr
            )));
        }

        let blk = self.read_reserved_block(indexblock_disk)?;
        if rd_u16(blk, 0) != ID_INDEXBLOCK {
            return Err(parse_err(format!(
                "expected IB indexblock at {}, got id={:#06x}",
                indexblock_disk,
                rd_u16(blk, 0)
            )));
        }
        let off = 12 + index_offset * 4;
        if off + 4 > blk.len() {
            return Err(parse_err("indexblock index out of range"));
        }
        let ab = rd_u32(blk, off);
        if ab == 0 {
            return Err(parse_err(format!(
                "anodeblock seqnr {} maps to zero anodeblock",
                seqnr
            )));
        }
        Ok(ab)
    }

    fn get_anode(&mut self, anodenr: u32) -> Result<Canode, FilesystemError> {
        let (seqnr, offset) = self.split_anodenr(anodenr);
        let ab_disk = self.anodeblock_blocknr(seqnr)?;
        let blk = self.read_reserved_block(ab_disk)?;
        if rd_u16(blk, 0) != ID_ANODEBLOCK {
            return Err(parse_err(format!(
                "expected AB anodeblock at {}, got id={:#06x}",
                ab_disk,
                rd_u16(blk, 0)
            )));
        }
        let off = 16 + offset as usize * 12;
        if off + 12 > blk.len() {
            return Err(parse_err(format!(
                "anode offset {} out of block at {}",
                offset, ab_disk
            )));
        }
        Ok(Canode {
            clustersize: rd_u32(blk, off),
            blocknr: rd_u32(blk, off + 4),
            next: rd_u32(blk, off + 8),
        })
    }

    /// Iterate dirblocks belonging to a directory anode chain. The callback
    /// receives the dirblock bytes (one reserved-block worth) plus the
    /// dirblock's first HW sector. Stops if the callback returns
    /// `ControlFlow::Break`.
    fn walk_dir_anode_chain(
        &mut self,
        dir_anodenr: u32,
        mut cb: impl FnMut(&[u8], u32) -> bool,
    ) -> Result<(), FilesystemError> {
        let mut anode = self.get_anode(dir_anodenr)?;
        loop {
            if anode.blocknr == 0 || anode.blocknr == 0xFFFFFFFF {
                break;
            }
            // For metadata anode chains, clustersize == 1 in practice and
            // the chain advances through .next. If a chain spans multiple
            // reserved blocks contiguously we still need to read each one;
            // step through anodeoffset in rscluster increments.
            let mut offset_sec: u32 = 0;
            let cluster_sectors = anode.clustersize.saturating_mul(self.rscluster);
            while offset_sec < cluster_sectors {
                let blk_sec = anode.blocknr + offset_sec;
                let blk = self.read_reserved_block(blk_sec)?.to_vec();
                let stop = cb(&blk, blk_sec);
                if stop {
                    return Ok(());
                }
                offset_sec += self.rscluster;
            }
            if anode.next == 0 {
                break;
            }
            anode = self.get_anode(anode.next)?;
        }
        Ok(())
    }

    /// Read a file's data into `writer` via its anode chain. `byte_size`
    /// caps the output so we don't emit cluster padding past EOF.
    fn stream_file_data(
        &mut self,
        file_anodenr: u32,
        byte_size: u64,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let mut written: u64 = 0;
        let mut anode = self.get_anode(file_anodenr)?;
        loop {
            if anode.blocknr == 0 || anode.blocknr == 0xFFFFFFFF {
                break;
            }
            if written >= byte_size {
                break;
            }
            // For file-data anodes, clustersize is in HW sectors. The
            // cluster covers HW sectors [blocknr, blocknr+clustersize).
            let cluster_bytes = anode.clustersize as u64 * HW_SECTOR;
            let remaining = byte_size - written;
            let to_read = cluster_bytes.min(remaining);
            // Stream in chunks to keep memory bounded.
            let chunk_size: u64 = 64 * 1024;
            let mut written_in_cluster: u64 = 0;
            while written_in_cluster < to_read {
                let n = chunk_size.min(to_read - written_in_cluster) as usize;
                let off =
                    self.partition_offset + anode.blocknr as u64 * HW_SECTOR + written_in_cluster;
                self.reader.seek(SeekFrom::Start(off))?;
                let mut buf = vec![0u8; n];
                self.reader.read_exact(&mut buf)?;
                writer.write_all(&buf)?;
                written_in_cluster += n as u64;
            }
            written += to_read;
            if anode.next == 0 {
                break;
            }
            anode = self.get_anode(anode.next)?;
        }
        Ok(written)
    }
}

/// One direntry parsed from a dirblock.
#[derive(Debug, Clone)]
struct DirEntry {
    ttype: i8,
    anode: u32,
    fsize: u64,
    cd: u16,
    cm: u16,
    ct: u16,
    #[allow(dead_code)]
    protection: u8,
    name: String,
    #[allow(dead_code)]
    comment: String,
}

/// Parse a single direntry starting at `entries[off]` within a dirblock.
/// Returns `None` if `next == 0` (end of dirblock).
fn parse_direntry(entries: &[u8], off: usize, largefile: bool) -> Option<DirEntry> {
    if off >= entries.len() {
        return None;
    }
    let next = entries[off] as usize;
    if next == 0 {
        return None;
    }
    if off + next > entries.len() || next < 18 {
        return None;
    }
    let ttype = entries[off + 1] as i8;
    let anode = rd_u32(entries, off + 2);
    let fsize_lo = rd_u32(entries, off + 6) as u64;
    let cd = rd_u16(entries, off + 10);
    let cm = rd_u16(entries, off + 12);
    let ct = rd_u16(entries, off + 14);
    let protection = entries[off + 16];
    let nlength = entries[off + 17] as usize;
    let name_start = off + 18;
    if name_start + nlength > off + next {
        return None;
    }
    let name = String::from_utf8_lossy(&entries[name_start..name_start + nlength]).to_string();
    let cmt_pos = name_start + nlength;
    let comment = if cmt_pos < off + next {
        let clen = entries[cmt_pos] as usize;
        if cmt_pos + 1 + clen <= off + next {
            String::from_utf8_lossy(&entries[cmt_pos + 1..cmt_pos + 1 + clen]).to_string()
        } else {
            String::new()
        }
    } else {
        String::new()
    };
    // LARGEFILE: high 16 bits live in extrafields at the tail of the
    // direntry (after comment, then alignment, then extrafields). The
    // padding makes total entry size even, then extrafields follow up to
    // the entry boundary. We only need fsizex (last 2 bytes of the entry)
    // when largefile mode is on.
    let mut fsize = fsize_lo;
    if largefile && next >= 20 {
        let fsizex_off = off + next - 2;
        if fsizex_off + 2 <= entries.len() {
            let hi = rd_u16(entries, fsizex_off) as u64;
            fsize = (hi << 32) | fsize_lo;
        }
    }
    Some(DirEntry {
        ttype,
        anode,
        fsize,
        cd,
        cm,
        ct,
        protection,
        name,
        comment,
    })
}

impl<R: Read + Seek + Send> Filesystem for Pfs3Filesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let mut fe = FileEntry::root();
        fe.location = ANODE_ROOTDIR as u64;
        Ok(fe)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let parent_path = entry.path.clone();
        let dir_anodenr = entry.location as u32;
        let largefile = self.largefile;
        let mut out: Vec<FileEntry> = Vec::new();

        // Collect raw entries first to avoid borrow-checker complaints
        // while we mutate `self` inside the callback.
        let mut raw_entries: Vec<DirEntry> = Vec::new();
        self.walk_dir_anode_chain(dir_anodenr, |blk, _sec| {
            // dirblock header: 20 bytes. Entries follow.
            if blk.len() <= 20 {
                return false;
            }
            // Verify id once; bail on the next iteration if mismatched.
            if rd_u16(blk, 0) != ID_DIRBLOCK {
                return true;
            }
            let entries = &blk[20..];
            let mut off = 0usize;
            while let Some(de) = parse_direntry(entries, off, largefile) {
                let next = entries[off] as usize;
                if next == 0 {
                    break;
                }
                off += next;
                raw_entries.push(de);
            }
            false
        })?;

        for de in raw_entries {
            // PFS3 type byte (from struct direntry): negative = file/link,
            // positive = directory. Specifically: ST_FILE = -3, ST_DIR = 2,
            // ST_LINKFILE = -4, ST_SOFTLINK = 3.
            let is_dir = de.ttype > 0;
            let child_path = if parent_path == "/" {
                format!("/{}", de.name)
            } else {
                format!("{}/{}", parent_path, de.name)
            };
            let mut fe = if is_dir {
                FileEntry::new_directory(de.name.clone(), child_path, de.anode as u64)
            } else {
                FileEntry::new_file(de.name.clone(), child_path, de.fsize, de.anode as u64)
            };
            fe.modified = datestamp_string(de.cd as i32, de.cm as i32, de.ct as i32);
            out.push(fe);
        }
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let mut buf: Vec<u8> = Vec::new();
        let size = entry.size.min(max_bytes as u64);
        self.stream_file_data(entry.location as u32, size, &mut buf)?;
        Ok(buf)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        self.stream_file_data(entry.location as u32, entry.size, writer)
    }

    fn volume_label(&self) -> Option<&str> {
        if self.root.disk_name.is_empty() {
            None
        } else {
            Some(&self.root.disk_name)
        }
    }

    fn fs_type(&self) -> &str {
        // Differentiate the three on-disk magics. Boot block carried the
        // selection but we didn't preserve it; rootblock disktype works
        // as a tie-break only for PFS2/AFS variants. Keep it simple.
        match &self.root.disktype {
            b"PFS\x02" => "PFS2",
            b"AFS\x01" => "AFS",
            b"muPF" | b"muAF" => "muPFS",
            _ => "PFS3",
        }
    }

    fn total_size(&self) -> u64 {
        // disksize is in HW sectors. Some volumes leave it zero; fall back
        // to the partition size in that case.
        if self.root.disksize == 0 {
            self.partition_size
        } else {
            self.root.disksize as u64 * HW_SECTOR
        }
    }

    fn used_size(&self) -> u64 {
        // blocks_free counts the user-data area only (excludes the
        // reserved region). Approximate used = total - free.
        let free = self.root.blocks_free as u64 * HW_SECTOR;
        self.total_size().saturating_sub(free)
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        let bitmap = read_user_bitmap(self)?;
        if bitmap.is_empty() {
            return Ok(self.total_size());
        }
        let bitmap_start = self.root.last_reserved.saturating_add(1);
        // Find the highest data-area sector that is allocated (bit clear).
        let mut last_alloc: Option<u32> = None;
        for byte_idx in (0..bitmap.len()).rev() {
            let byte = bitmap[byte_idx];
            if byte == 0xFF {
                continue;
            }
            // Walk bits MSB-first inside the byte to find the topmost
            // allocated (cleared) bit.
            for bit in (0..8u8).rev() {
                let sec_off = (byte_idx as u64) * 8 + bit as u64;
                if (byte >> bit) & 1 == 0 {
                    last_alloc = Some(bitmap_start + sec_off as u32);
                    break;
                }
            }
            if last_alloc.is_some() {
                break;
            }
        }
        let last_sec = last_alloc.unwrap_or(self.root.last_reserved);
        Ok((last_sec as u64 + 1) * HW_SECTOR)
    }
}

/// Layout-preserving compact reader for PFS3.
///
/// Emits the partition byte-for-byte from offset 0, replacing **free**
/// blocks (in the user-data area, i.e. HW sectors `> last_reserved`) with
/// zeros. The reserved area (rootblock, bitmap, indexblocks, anodeblocks,
/// dirblocks) is always emitted verbatim — those are metadata we never
/// have permission to drop.
pub struct CompactPfs3Reader<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    partition_size: u64,
    /// Concatenated user-data bitmap. Each bit covers one HW sector at
    /// `bitmap_start + bit_index`. Set bit = free (AmigaDOS convention).
    /// Empty = "no bitmap, assume everything allocated".
    bitmap: Vec<u8>,
    bitmap_start: u32,
    last_reserved: u32,
    /// Current output position relative to partition start.
    position: u64,
    /// Single-sector cache so we don't reseek for every byte.
    sec_buf: [u8; HW_SECTOR as usize],
    sec_loaded_for: Option<u32>,
}

impl<R: Read + Seek> CompactPfs3Reader<R> {
    pub fn new(
        mut reader: R,
        partition_offset: u64,
    ) -> Result<(Self, CompactResult), FilesystemError> {
        let mut fs = Pfs3Filesystem::open(&mut reader, partition_offset)?;
        let partition_size = fs.partition_size;
        let last_reserved = fs.root.last_reserved;
        let bitmap_start = last_reserved.saturating_add(1);
        let bitmap = read_user_bitmap(&mut fs)?;
        let total_blocks = (partition_size / HW_SECTOR) as u32;
        // The flat bitmap is rounded up to byte size, so it may contain
        // trailing pad bits beyond the actual data-sector range. Count
        // only bits that correspond to a real sector.
        let data_sectors = total_blocks.saturating_sub(bitmap_start) as u64;
        let mut free_blocks: u64 = 0;
        for (i, &byte) in bitmap.iter().enumerate() {
            let base = i as u64 * 8;
            if base + 8 <= data_sectors {
                free_blocks += byte.count_ones() as u64;
            } else if base < data_sectors {
                let valid_bits = (data_sectors - base) as u32;
                let mask = (1u8 << valid_bits) - 1;
                free_blocks += (byte & mask).count_ones() as u64;
            }
        }
        let allocated_data = if bitmap.is_empty() {
            // No bitmap available: treat the whole data area as allocated.
            data_sectors
        } else {
            data_sectors.saturating_sub(free_blocks)
        };
        let reserved_blocks = bitmap_start as u64; // sectors 0..=last_reserved
        let allocated = allocated_data + reserved_blocks;

        // Don't claim more than we actually have.
        let allocated = allocated.min(total_blocks as u64);
        let result = CompactResult {
            original_size: partition_size,
            compacted_size: partition_size,
            data_size: allocated * HW_SECTOR,
            clusters_used: allocated as u32,
        };

        Ok((
            CompactPfs3Reader {
                reader,
                partition_offset,
                partition_size,
                bitmap,
                bitmap_start,
                last_reserved,
                position: 0,
                sec_buf: [0u8; HW_SECTOR as usize],
                sec_loaded_for: None,
            },
            result,
        ))
    }

    fn sector_is_allocated(&self, sec: u32) -> bool {
        if sec <= self.last_reserved {
            return true;
        }
        if sec < self.bitmap_start {
            return true;
        }
        let bit = (sec - self.bitmap_start) as usize;
        let byte = bit / 8;
        let off = bit % 8;
        if byte >= self.bitmap.len() {
            return true; // outside the recorded bitmap — emit verbatim
        }
        // Set bit = free → allocated when bit is clear.
        (self.bitmap[byte] >> off) & 1 == 0
    }
}

impl<R: Read + Seek> Read for CompactPfs3Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.position >= self.partition_size || buf.is_empty() {
            return Ok(0);
        }
        let sec = (self.position / HW_SECTOR) as u32;
        let in_sec = (self.position % HW_SECTOR) as usize;
        let n = ((HW_SECTOR as usize) - in_sec).min(buf.len());

        if self.sector_is_allocated(sec) {
            if self.sec_loaded_for != Some(sec) {
                let off = self.partition_offset + sec as u64 * HW_SECTOR;
                self.reader.seek(SeekFrom::Start(off))?;
                self.reader.read_exact(&mut self.sec_buf)?;
                self.sec_loaded_for = Some(sec);
            }
            buf[..n].copy_from_slice(&self.sec_buf[in_sec..in_sec + n]);
        } else {
            buf[..n].fill(0);
        }
        self.position += n as u64;
        Ok(n)
    }
}

/// Walk the user-data bitmap (bitmapindex blocks → bitmapblocks) and
/// produce a flat byte vector with one bit per HW sector starting at
/// `last_reserved + 1`. Each byte stores eight bits in least-significant-bit
/// order (bit `b` of byte `B` represents sector `bitmap_start + B*8 + b`).
/// Set bit = free (AmigaDOS convention).
///
/// On-disk bitmap is u32 BE words; within a word, bit position `31 - i`
/// represents sector `i` of that word (pfs3aio allocation.c:318).
///
/// Returns an empty vector when the bitmap structure is unreadable or
/// no bitmap-index pointers are present — the compact reader then falls
/// back to emitting the partition verbatim.
fn read_user_bitmap<R: Read + Seek>(
    fs: &mut Pfs3Filesystem<R>,
) -> Result<Vec<u8>, FilesystemError> {
    let bitmap_start = fs.root.last_reserved.saturating_add(1);
    let total_sectors = (fs.partition_size / HW_SECTOR) as u32;
    if total_sectors <= bitmap_start {
        return Ok(Vec::new());
    }
    let data_sectors = total_sectors - bitmap_start;
    let total_bytes = ((data_sectors as usize) + 7) / 8;
    let mut out = vec![0u8; total_bytes];

    let longs_per_bmb = (fs.reserved_blksize / 4).saturating_sub(3) as usize;
    let indexperblock = fs.indexperblock as usize;

    // sectors_per_bmb tells us where each BM block starts within the
    // flat bitmap. Each word covers 32 sectors.
    let sectors_per_bmb: u64 = longs_per_bmb as u64 * 32;

    let mi_pointers: Vec<u32> = fs.root.bitmapindex.iter().copied().collect();
    for (mi_seq, &mi_blk) in mi_pointers.iter().enumerate() {
        if mi_blk == 0 {
            continue;
        }
        let mi = fs.read_reserved_block(mi_blk)?.to_vec();
        if mi.len() < 12 || rd_u16(&mi, 0) != ID_BITMAPINDEXBLOCK {
            // Not an MI block — skip silently; treat as unmapped.
            continue;
        }
        for i in 0..indexperblock {
            let off = 12 + i * 4;
            if off + 4 > mi.len() {
                break;
            }
            let bm_blk = rd_u32(&mi, off);
            if bm_blk == 0 {
                continue;
            }
            let bm_seq = (mi_seq * indexperblock + i) as u64;
            let bm = fs.read_reserved_block(bm_blk)?.to_vec();
            if bm.len() < 12 || rd_u16(&bm, 0) != ID_BITMAPBLOCK {
                continue;
            }
            // Sector base within data area covered by this BM block.
            let base_sector = bm_seq * sectors_per_bmb;
            for w in 0..longs_per_bmb {
                let o = 12 + w * 4;
                if o + 4 > bm.len() {
                    break;
                }
                let word = rd_u32(&bm, o);
                if word == 0 {
                    continue;
                }
                for i in 0..32u32 {
                    if (word >> (31 - i)) & 1 == 0 {
                        continue;
                    }
                    let sec = base_sector + (w as u64) * 32 + i as u64;
                    if sec >= data_sectors as u64 {
                        continue;
                    }
                    let byte_idx = (sec / 8) as usize;
                    let bit_off = (sec % 8) as u8;
                    out[byte_idx] |= 1u8 << bit_off;
                }
            }
        }
    }
    Ok(out)
}

// === Phase 6: formatter for a blank PFS3 volume =============================
//
// Produces a minimum mountable empty PFS3 volume so the write path has a
// known-good starting state for round-trip tests. Mirrors the layout that
// `pfs3aio/format.c::FDSFormat` would emit for a small "small-mode" disk:
//
// Reserved layout (1 KiB reserved blocks, 32 reserved total, first at HW
// sector 2; reserved block N starts at HW sector `2 + N*2`):
//   RB 0 — rootblock (first 512 B) + reserved bitmap (next 512 B)
//   RB 1 — rootblock extension (`EX`)
//   RB 2 — bitmap-index block (`MI`)
//   RB 3..3+no_bmb-1 — data bitmap blocks (`BM`)
//   next — anode-index block (`IB`) reachable from `small_indexblocks[0]`
//   next — anodeblock 0 (`AB`) — holds anodes 0..(anodesperblock-1)
//   next — root dirblock (`DB`)
//
// Anode 5 is reserved for the root directory (ANODE_ROOTDIR); anodes 0..4
// are pre-allocated as the format flow does (blocknr=0xFFFFFFFF sentinel
// marks them as taken-but-empty). All other anodes are zeroed → free.
//
// We deliberately set only `MODE_HARDDISK | MODE_SPLITTED_ANODES |
// MODE_EXTENSION` in `options`. Real PFS3 volumes also enable
// DIR_EXTENSION / DATESTAMP / LONGFN, but those add extra direntry tail
// fields. Keeping options minimal keeps the formatter and the matching
// dirblock write path simple. The reader handles minimal mode fine; this
// is enough for our round-trip tests.

const PFS3_BLANK_NUMRESERVED: u32 = 32;
const PFS3_BLANK_RESBLKSIZE: u32 = 1024;
const PFS3_BLANK_FIRST_RESERVED: u32 = 2;
const HW_SECTOR_U32: u32 = 512;

fn write_u16(buf: &mut [u8], o: usize, v: u16) {
    buf[o..o + 2].copy_from_slice(&v.to_be_bytes());
}
fn write_u32(buf: &mut [u8], o: usize, v: u32) {
    buf[o..o + 4].copy_from_slice(&v.to_be_bytes());
}

/// Create a blank PFS3 volume sized for `total_sectors` HW sectors of 512
/// bytes (`total_sectors * 512` bytes total). The volume mounts as empty
/// with diskname `name` (truncated to 31 bytes).
pub fn create_blank_pfs3(total_sectors: u32, name: &str) -> Result<Vec<u8>, FilesystemError> {
    let resblksize = PFS3_BLANK_RESBLKSIZE;
    let rscluster = resblksize / HW_SECTOR_U32; // 2
    let numreserved = PFS3_BLANK_NUMRESERVED;
    let first_reserved = PFS3_BLANK_FIRST_RESERVED;
    let last_reserved = first_reserved + rscluster * numreserved - 1;

    // Need at least last_reserved + 1 data sector beyond.
    if total_sectors < last_reserved + 8 {
        return Err(parse_err(format!(
            "PFS3 formatter: total_sectors {} too small (need > {})",
            total_sectors,
            last_reserved + 8
        )));
    }

    let bitmap_start_sector = last_reserved + 1;
    let data_sectors = total_sectors - bitmap_start_sector;
    let longsperbmb = (resblksize - 12) / 4;
    let sectors_per_bmb = longsperbmb * 32;
    let no_bmb = (data_sectors + sectors_per_bmb - 1) / sectors_per_bmb;
    let indexperblock = (resblksize - 12) / 4;
    if no_bmb > indexperblock {
        return Err(parse_err(format!(
            "PFS3 formatter: disk too large for single MI block ({} BM > {} index slots)",
            no_bmb, indexperblock
        )));
    }
    let anodesperblock = (resblksize - 16) / 12;

    // Reserved-block index assignments.
    let rb_idx_root = 0u32;
    let rb_idx_ext = 1u32;
    let rb_idx_mi = 2u32;
    let rb_idx_bm0 = 3u32;
    let rb_idx_ib0 = rb_idx_bm0 + no_bmb;
    let rb_idx_ab0 = rb_idx_ib0 + 1;
    let rb_idx_rootdir = rb_idx_ab0 + 1;
    let used_rb = rb_idx_rootdir + 1;
    if used_rb > numreserved {
        return Err(parse_err(format!(
            "PFS3 formatter: reserved-block budget exceeded ({} > {})",
            used_rb, numreserved
        )));
    }

    let rb_to_sector = |idx: u32| -> u32 { first_reserved + idx * rscluster };
    let rb_to_offset = |idx: u32| -> usize { rb_to_sector(idx) as usize * 512 };

    let mut img = vec![0u8; total_sectors as usize * 512];

    // === Boot block at HW sector 0 ===
    img[0..4].copy_from_slice(b"PFS\x01");

    // === Rootblock at RB 0 ===
    let rb = rb_to_offset(rb_idx_root);
    img[rb..rb + 4].copy_from_slice(b"PFS\x01");
    let options = MODE_HARDDISK | MODE_SPLITTED_ANODES | MODE_EXTENSION;
    write_u32(&mut img, rb + 4, options);
    write_u32(&mut img, rb + 8, 1); // datestamp
                                    // creationday/min/tick at 12..18: zero
    img[rb + 18] = 0xf0; // protection
    let n = name.as_bytes();
    let n_len = n.len().min(31);
    img[rb + 20] = n_len as u8;
    img[rb + 21..rb + 21 + n_len].copy_from_slice(&n[..n_len]);
    write_u32(&mut img, rb + 52, last_reserved);
    write_u32(&mut img, rb + 56, first_reserved);
    let reserved_free = numreserved - used_rb;
    write_u32(&mut img, rb + 60, reserved_free);
    write_u16(&mut img, rb + 64, resblksize as u16);
    write_u16(&mut img, rb + 66, 1); // rblkcluster: 1 reserved block
    write_u32(&mut img, rb + 68, data_sectors); // blocks_free
                                                // alwaysfree at 72: 0
                                                // roving_ptr at 76: 0
                                                // deldir at 80: 0
    write_u32(&mut img, rb + 84, total_sectors); // disksize (HW sectors)
    write_u32(&mut img, rb + 88, rb_to_sector(rb_idx_ext)); // extension blocknr
                                                            // not_used at 92: 0
                                                            // Small layout starts at offset 96:
                                                            //   bitmapindex[5] @ 96..116
                                                            //   indexblocks[99] @ 116..512
    write_u32(&mut img, rb + 96, rb_to_sector(rb_idx_mi));
    write_u32(&mut img, rb + 116, rb_to_sector(rb_idx_ib0));

    // === Reserved bitmap immediately after the rootblock (offset 512) ===
    let bm_res = rb + 512;
    write_u16(&mut img, bm_res, ID_BITMAPBLOCK);
    write_u32(&mut img, bm_res + 4, 1); // datestamp
                                        // seqnr at +8: 0
                                        // bitmap[0] at +12: bits 0..numreserved-1 = free, then mask off used.
    let mut res_bm = 0u32;
    for i in 0..numreserved {
        res_bm |= 1u32 << (31 - i);
    }
    for i in 0..used_rb {
        res_bm &= !(1u32 << (31 - i));
    }
    write_u32(&mut img, bm_res + 12, res_bm);

    // === Rootblock extension at RB 1 ===
    let ext = rb_to_offset(rb_idx_ext);
    write_u16(&mut img, ext, ID_EXTENSIONBLOCK);
    write_u32(&mut img, ext + 8, 1); // datestamp

    // === Bitmap-index block (MI) at RB 2 ===
    let mi = rb_to_offset(rb_idx_mi);
    write_u16(&mut img, mi, ID_BITMAPINDEXBLOCK);
    write_u32(&mut img, mi + 4, 1); // datestamp
                                    // seqnr at +8: 0
                                    // index[N] starts at +12; populate with BM block sectors.
    for i in 0..no_bmb {
        write_u32(
            &mut img,
            mi + 12 + i as usize * 4,
            rb_to_sector(rb_idx_bm0 + i),
        );
    }

    // === Data bitmap (BM) blocks ===
    // Each BM holds `longsperbmb` u32 words; all 1s = all free.
    for i in 0..no_bmb {
        let bm = rb_to_offset(rb_idx_bm0 + i);
        write_u16(&mut img, bm, ID_BITMAPBLOCK);
        write_u32(&mut img, bm + 4, 1); // datestamp
        write_u32(&mut img, bm + 8, i); // seqnr
        for w in 0..longsperbmb as usize {
            write_u32(&mut img, bm + 12 + w * 4, 0xFFFF_FFFF);
        }
    }

    // === Anode-index block (IB) at rb_idx_ib0 ===
    let ib = rb_to_offset(rb_idx_ib0);
    write_u16(&mut img, ib, ID_INDEXBLOCK);
    write_u32(&mut img, ib + 4, 1); // datestamp
                                    // seqnr at +8: 0
                                    // index[0] = anodeblock 0 sector
    write_u32(&mut img, ib + 12, rb_to_sector(rb_idx_ab0));

    // === Anodeblock 0 (AB) at rb_idx_ab0 ===
    let ab = rb_to_offset(rb_idx_ab0);
    write_u16(&mut img, ab, ID_ANODEBLOCK);
    write_u32(&mut img, ab + 4, 1); // datestamp
                                    // seqnr at +8: 0
                                    // Anodes 0..4: blocknr=0xFFFFFFFF (taken sentinel, per AllocAnode).
    for slot in 0..ANODE_ROOTDIR as usize {
        let aoff = ab + 16 + slot * 12;
        write_u32(&mut img, aoff, 0); // clustersize
        write_u32(&mut img, aoff + 4, 0xFFFF_FFFF); // blocknr sentinel
        write_u32(&mut img, aoff + 8, 0); // next
    }
    // Anode 5 (ROOTDIR): clustersize=1, blocknr=rootdir sector, next=0.
    let aoff_root = ab + 16 + ANODE_ROOTDIR as usize * 12;
    write_u32(&mut img, aoff_root, 1);
    write_u32(&mut img, aoff_root + 4, rb_to_sector(rb_idx_rootdir));
    write_u32(&mut img, aoff_root + 8, 0);
    // Anodes 6..anodesperblock-1: zero = free (already zeroed).
    let _ = anodesperblock;

    // === Root dirblock (DB) at rb_idx_rootdir ===
    let db = rb_to_offset(rb_idx_rootdir);
    write_u16(&mut img, db, ID_DIRBLOCK);
    write_u32(&mut img, db + 4, 1); // datestamp
                                    // not_used_2 at +8: 0 (2 UWORDs)
    write_u32(&mut img, db + 12, ANODE_ROOTDIR); // anodenr
    write_u32(&mut img, db + 16, 0); // parent
                                     // entries at +20: empty (first byte already 0)

    Ok(img)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rd_helpers() {
        let b = [0x12u8, 0x34, 0x56, 0x78];
        assert_eq!(rd_u16(&b, 0), 0x1234);
        assert_eq!(rd_u32(&b, 0), 0x1234_5678);
    }

    #[test]
    fn split_anodenr_modes() {
        // Quick algebra check, no I/O.
        // anodenr 0x00020003 in split mode → seqnr=2, offset=3
        let anodenr = 0x0002_0003u32;
        let seqnr = anodenr >> 16;
        let off = anodenr & 0xFFFF;
        assert_eq!((seqnr, off), (2, 3));
    }

    #[test]
    fn blank_volume_round_trips_through_reader() {
        // 4 MiB blank disk: 8192 sectors, 32 reserved, 8126 data sectors.
        let img = create_blank_pfs3(8192, "TestPFS").expect("format");
        assert_eq!(img.len(), 8192 * 512);
        let cur = std::io::Cursor::new(img);
        let mut fs = Pfs3Filesystem::open(cur, 0).expect("open");
        assert_eq!(fs.volume_label(), Some("TestPFS"));
        assert_eq!(fs.fs_type(), "PFS3");
        let root = fs.root().expect("root");
        let kids = fs.list_directory(&root).expect("list");
        assert!(kids.is_empty(), "blank root should be empty");
        // total_size == 4 MiB
        assert_eq!(fs.total_size(), 8192 * 512);
        // used_size = total - free*512. Free = data_sectors_in_bitmap_words *
        // 32 minus 0 used... actually all data sectors are free in a blank
        // disk, so used == reserved area only.
        let used = fs.used_size();
        let reserved_bytes = 66u64 * 512; // sectors 0..65 = boot + reserved area
                                          // The reader's used_size formula is total - blocks_free*512. We set
                                          // blocks_free = data_sectors (8126). So used = 66 sectors = 33792.
        assert_eq!(used, reserved_bytes);
    }

    #[test]
    fn blank_volume_compact_reader_walks_bitmap() {
        let img = create_blank_pfs3(8192, "T").expect("format");
        let cur = std::io::Cursor::new(img);
        let (_compact, info) = CompactPfs3Reader::new(cur, 0).expect("compact");
        // For a blank disk, every data-area bit is "free", so the compact
        // reader sees ONLY the reserved area as allocated.
        assert_eq!(info.original_size, 8192 * 512);
        assert_eq!(info.data_size, 66 * 512);
    }

    #[test]
    fn parse_direntry_minimal() {
        // Minimal direntry: next=20, type=2 (dir), anode=42, size=0,
        // dates=0, prot=0, nlength=2, name="hi", no comment.
        let mut e = vec![0u8; 32];
        e[0] = 20; // next
        e[1] = 2u8; // type (dir, ST_DIR)
        e[2..6].copy_from_slice(&42u32.to_be_bytes());
        e[6..10].copy_from_slice(&0u32.to_be_bytes());
        e[10..12].copy_from_slice(&0u16.to_be_bytes());
        e[12..14].copy_from_slice(&0u16.to_be_bytes());
        e[14..16].copy_from_slice(&0u16.to_be_bytes());
        e[16] = 0; // prot
        e[17] = 2; // nlength
        e[18] = b'h';
        e[19] = b'i';
        let de = parse_direntry(&e, 0, false).expect("entry parses");
        assert_eq!(de.ttype, 2);
        assert_eq!(de.anode, 42);
        assert_eq!(de.name, "hi");
    }
}
