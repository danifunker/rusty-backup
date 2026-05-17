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
// Gates whether tools honor the per-direntry datestamps at offsets 10..16.
// pfs3aio auto-sets it on first write-mount (`volume.c:546`), so any real
// PFS3 volume that's been touched has it set. We mirror that for blank
// volumes so dates we write into direntries are honored without needing a
// real-Amiga mount to flip the bit.
const MODE_DATESTAMP: u32 = 64;
/// Long-filename support flag. When set, tooling honors the
/// `fnsize` field in the rootblock extension (default 32 / max 107)
/// to allow names beyond the standard AmigaDOS 31-char limit. Real
/// AmigaVision-style volumes commonly enable this with `fnsize=107`.
const MODE_LONGFN: u32 = 1024;
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
    pub(crate) reader: R,
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

    /// Reserved-block writes pending flush, keyed by HW sector (must be the
    /// first sector of a reserved-block cluster). Value buffers are exactly
    /// `reserved_blksize` bytes. The cache mirror is updated in lockstep so
    /// subsequent reads see the in-memory state.
    dirty_reserved: HashMap<u32, Vec<u8>>,
    /// Data-area sector writes pending flush, keyed by HW sector. Each value
    /// is 512 bytes. Kept separate from `dirty_reserved` to make
    /// snapshot/rollback granular.
    dirty_data: HashMap<u32, Vec<u8>>,

    /// Rolling allocator hints. The allocators may safely start their
    /// scan from these positions and treat anything before as fully
    /// allocated. `free_*` paths lower them when blocks become available
    /// again. Persisted in [`Pfs3Snapshot`] so rollback restores hints
    /// alongside the rest of the mutable state.
    alloc_anode_hint_seqnr: u32,
    alloc_data_hint_local: u32,
    /// Per-directory tail-of-chain hint: `dir_anodenr -> chain_anodenr`
    /// of the dirblock most recently used for an append. Lets
    /// `add_direntry_to_dir` skip the full head-of-chain walk on dirs
    /// with thousands of entries (which made bulk clones O(N^2)).
    /// Always safe — the insertion path still validates space and
    /// walks forward from the hint if it's stale.
    dir_append_hint: HashMap<u32, u32>,
}

/// Snapshot of mutable state for `EditableFilesystem` rollback. Captured
/// at the start of every mutation and restored on error so a partial
/// failure leaves the in-memory volume exactly as it was before the call.
#[derive(Clone)]
struct Pfs3Snapshot {
    root: Pfs3RootBlock,
    dirty_reserved: HashMap<u32, Vec<u8>>,
    dirty_data: HashMap<u32, Vec<u8>>,
    cache_overrides: HashMap<u32, Vec<u8>>,
    alloc_anode_hint_seqnr: u32,
    alloc_data_hint_local: u32,
    dir_append_hint: HashMap<u32, u32>,
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
            dirty_reserved: HashMap::new(),
            dirty_data: HashMap::new(),
            alloc_anode_hint_seqnr: 0,
            alloc_data_hint_local: 0,
            dir_append_hint: HashMap::new(),
        })
    }

    fn read_reserved_block(&mut self, blocknr: u32) -> Result<&[u8], FilesystemError> {
        // Pending writes shadow the on-disk state so subsequent reads
        // observe the in-memory mutations.
        if let Some(v) = self.dirty_reserved.get(&blocknr) {
            return Ok(v.as_slice());
        }
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

    /// Read a `ST_SOFTLINK` direntry's target path. The link's anode
    /// points at one or more data blocks holding a NUL-terminated path
    /// string (per pfs3aio `CreateSoftLink`: a 1024-byte buffer is
    /// allocated, `strcpy`'d, and written verbatim). We follow the
    /// anode, read up to 2048 bytes (well past any practical Amiga path
    /// length), and truncate at the first NUL.
    fn read_softlink_target(&mut self, link_anodenr: u32) -> Result<String, FilesystemError> {
        let mut buf: Vec<u8> = Vec::with_capacity(2048);
        let mut sink = std::io::Cursor::new(&mut buf);
        self.stream_file_data(link_anodenr, 2048, &mut sink)?;
        let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        Ok(String::from_utf8_lossy(&buf[..end]).into_owned())
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
    /// `extrafields.link` — for ST_LINKFILE / ST_LINKDIR this is the
    /// target object's anode. Zero for non-link entries (no link bits
    /// set in the packed extrafields flags).
    extrafields_link: u32,
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
    // Extrafields layout (pfs3aio `AddExtraFields`/`GetExtraFields`):
    // packed at the END of the direntry, terminated by a u16 `flags`
    // bitmap. Each set bit (LSB first) prepends one u16 field, walking
    // backwards from the flags word. Field order matches the `struct
    // extrafields` u16-stream:
    //     bit 0 = link[hi], bit 1 = link[lo],
    //     bit 2 = uid, bit 3 = gid,
    //     bit 4 = prot[hi], bit 5 = prot[lo],
    //     bit 6 = virtualsize[hi], bit 7 = virtualsize[lo],
    //     bit 8 = rollpointer[hi], bit 9 = rollpointer[lo],
    //     bit 10 = fsizex (LARGEFILE high 16 bits of fsize).
    // We extract `link` (for ST_LINKFILE / ST_LINKDIR hardlinks) and
    // `fsizex` (for LARGEFILE mode); other fields are not currently
    // surfaced on the read path.
    let entries_origin_off = name_start - 18; // = off
    let extra_start_unaligned = cmt_pos + 1 + comment.len();
    let extra_start = (extra_start_unaligned + 1) & !1;
    let entry_end = entries_origin_off + next;
    let mut link_hi: u16 = 0;
    let mut link_lo: u16 = 0;
    let mut fsizex_hi: u16 = 0;
    if extra_start + 2 <= entry_end && entry_end <= entries.len() {
        let mut flags = rd_u16(entries, entry_end - 2);
        // Cursor walks backwards through the packed u16 fields.
        let mut cursor = entry_end - 2;
        for bit_idx in 0..11u32 {
            let present = (flags & 1) == 1;
            flags >>= 1;
            if present {
                if cursor < extra_start + 2 {
                    break;
                }
                cursor -= 2;
                let val = rd_u16(entries, cursor);
                match bit_idx {
                    0 => link_hi = val,
                    1 => link_lo = val,
                    10 => fsizex_hi = val,
                    _ => {}
                }
            }
        }
    }
    let extrafields_link = ((link_hi as u32) << 16) | (link_lo as u32);
    // LARGEFILE: fsize high 16 bits come from extrafields.fsizex.
    let fsize = if largefile {
        ((fsizex_hi as u64) << 32) | fsize_lo
    } else {
        fsize_lo
    };
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
        extrafields_link,
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
            let child_path = if parent_path == "/" {
                format!("/{}", de.name)
            } else {
                format!("{}/{}", parent_path, de.name)
            };
            // Classify by exact ST_* value (positive does NOT imply
            // directory — ST_SOFTLINK is positive too).
            let mut fe = match de.ttype {
                ST_DIR => FileEntry::new_directory(de.name.clone(), child_path, de.anode as u64),
                ST_LINKDIR => {
                    let mut e =
                        FileEntry::new_directory(de.name.clone(), child_path, de.anode as u64);
                    e.link_target_cnid = Some(de.extrafields_link as u64);
                    e
                }
                ST_SOFTLINK => {
                    // Resolve the target string from the anode's block.
                    let target = self
                        .read_softlink_target(de.anode)
                        .unwrap_or_else(|_| String::new());
                    FileEntry::new_symlink(
                        de.name.clone(),
                        child_path,
                        de.fsize,
                        de.anode as u64,
                        target,
                    )
                }
                ST_LINKFILE => {
                    let mut e =
                        FileEntry::new_file(de.name.clone(), child_path, de.fsize, de.anode as u64);
                    e.link_target_cnid = Some(de.extrafields_link as u64);
                    e
                }
                _ => {
                    // ST_FILE, ST_ROLLOVERFILE, anything else defaults
                    // to a regular file entry.
                    FileEntry::new_file(de.name.clone(), child_path, de.fsize, de.anode as u64)
                }
            };
            fe.modified = datestamp_string(de.cd as i32, de.cm as i32, de.ct as i32);
            fe.amiga_protection = Some(de.protection as u32);
            if !de.comment.is_empty() {
                fe.amiga_comment = Some(de.comment.clone());
            }
            fe.amiga_date = Some((de.cd as i32, de.cm as i32, de.ct as i32));
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

    /// Floor that the defragmenting clone (`clone_pfs3_volume`) can
    /// shrink this volume to. Computed from the actual used bytes
    /// rather than the highest-allocated-sector high-water mark — the
    /// rovingPointer allocator scatters blocks throughout the volume,
    /// so `last_data_byte` typically equals the partition size on any
    /// non-trivial PFS3 volume and reports no savings.
    ///
    /// Conservative: takes the source's `used_size` and adds a small
    /// margin for the target's new reserved area + bitmap pages. The
    /// actual clone sizes precisely via `create_blank_pfs3`; this
    /// number is for "preview the shrink" UI gating.
    fn defragmented_minimum_size(&mut self) -> Result<u64, FilesystemError> {
        let used = self.used_size();
        // 256 KiB headroom covers the blank-volume reserved area (32 KiB
        // for PFS3_BLANK_NUMRESERVED * resblksize) plus extra bitmap
        // pages for plausible target sizes. Far smaller than the
        // partition size, so the user still sees a useful saving
        // estimate.
        const MARGIN_BYTES: u64 = 256 * 1024;
        Ok(used.saturating_add(MARGIN_BYTES))
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

// === Phase 6: editable PFS3 (allocator + EditableFilesystem impl) ===========
//
// **Atomicity model.** Mutations stage edits into per-block dirty buffers
// (`dirty_reserved` for reserved-area blocks, `dirty_data` for individual
// data-area HW sectors). Reads consult dirty buffers first, then the LRU
// cache, then the disk. `sync_metadata` flushes both maps in ascending
// HW-sector order and clears them.
//
// Each mutation method takes a `Pfs3Snapshot` at entry and restores it on
// error before returning, so a partial failure leaves the in-memory volume
// indistinguishable from the pre-call state. `sync_metadata` is the only
// place where bytes actually hit the disk, so the on-disk volume is
// consistent at every successful sync boundary.
//
// AmigaDOS "set bit = free" applies to BOTH the reserved bitmap (within
// the rootblock cluster at offset 512) and every BM block in the data
// area. Bit positions are MSB-first within each u32: sector `i` of word
// `w` is at bit `31 - i`.
//
// Anodes 0..5 are PFS3-reserved (ANODE_EOF=0, ANODE_RESERVED_1..3=1..3,
// ANODE_BADBLOCKS=4, ANODE_ROOTDIR=5). User allocations start at
// `ANODE_USERFIRST = 6`.

const PFS3_ANODE_USERFIRST: u32 = 6;
const PFS3_ANODE_EOF_SENTINEL: u32 = 0xFFFFFFFF;

/// Direntry type byte (i8 on disk). Values per pfs3aio / amitools:
/// `ST_USERDIR=2`, `ST_SOFTLINK=3`, `ST_LINKDIR=4`, `ST_FILE=-3`,
/// `ST_LINKFILE=-4`, `ST_ROLLOVERFILE=-16` (PFS extension).
/// Positive does NOT imply "is a directory" — `ST_SOFTLINK=3` is a
/// symlink to a file or dir; classify by exact value.
const ST_FILE: i8 = -3;
const ST_DIR: i8 = 2;
const ST_SOFTLINK: i8 = 3;
const ST_LINKDIR: i8 = 4;
const ST_LINKFILE: i8 = -4;

impl Pfs3RootBlock {
    /// Serialize the rootblock into the first 512 bytes of the cluster.
    /// The reserved bitmap that follows immediately after stays
    /// untouched; callers must update the reserved bitmap separately by
    /// modifying the cluster buffer directly.
    fn write_into(&self, buf: &mut [u8]) {
        assert!(buf.len() >= 512);
        buf[0..4].copy_from_slice(&self.disktype);
        buf[4..8].copy_from_slice(&self.options.to_be_bytes());
        // datestamp at 8: bump it (callers may want to update separately).
        // creationday/min/tick at 12..18: leave existing bytes alone.
        let n = self.disk_name.as_bytes();
        let n_len = n.len().min(31);
        buf[20] = n_len as u8;
        buf[21..21 + n_len].copy_from_slice(&n[..n_len]);
        // Zero the leftover diskname tail so a shorter name doesn't
        // leak bytes from the previous longer name.
        for b in &mut buf[21 + n_len..52] {
            *b = 0;
        }
        buf[52..56].copy_from_slice(&self.last_reserved.to_be_bytes());
        buf[56..60].copy_from_slice(&self.first_reserved.to_be_bytes());
        buf[60..64].copy_from_slice(&self.reserved_free.to_be_bytes());
        buf[64..66].copy_from_slice(&self.reserved_blksize.to_be_bytes());
        // rblkcluster at 66 is preserved (we don't change it).
        buf[68..72].copy_from_slice(&self.blocks_free.to_be_bytes());
        buf[84..88].copy_from_slice(&self.disksize.to_be_bytes());
        buf[88..92].copy_from_slice(&self.extension.to_be_bytes());
        // Index arrays at offset 96 stay intact; we only mutate them
        // through targeted callers, not via this whole-block rewrite.
    }
}

impl<R: Read + Seek> Pfs3Filesystem<R> {
    /// HW sector of the rootblock cluster (always sector 2 in our writer).
    fn rootblock_cluster_sec(&self) -> u32 {
        ROOTBLOCK_SECTOR
    }

    /// Take a snapshot of mutable state for rollback. Captures rootblock,
    /// both dirty maps, and the cache entries we might overwrite. Restore
    /// with `restore_snapshot`.
    fn snapshot(&self) -> Pfs3Snapshot {
        Pfs3Snapshot {
            root: self.root.clone(),
            dirty_reserved: self.dirty_reserved.clone(),
            dirty_data: self.dirty_data.clone(),
            // The LRU cache is content-addressed by HW sector, so the
            // simplest correct snapshot is a full clone of the cache
            // entries we might evict. Mutation methods stay small so
            // this is cheap in practice.
            cache_overrides: self.cache.clone(),
            alloc_anode_hint_seqnr: self.alloc_anode_hint_seqnr,
            alloc_data_hint_local: self.alloc_data_hint_local,
            dir_append_hint: self.dir_append_hint.clone(),
        }
    }

    fn restore_snapshot(&mut self, snap: Pfs3Snapshot) {
        self.root = snap.root;
        self.dirty_reserved = snap.dirty_reserved;
        self.dirty_data = snap.dirty_data;
        self.cache = snap.cache_overrides;
        self.alloc_anode_hint_seqnr = snap.alloc_anode_hint_seqnr;
        self.alloc_data_hint_local = snap.alloc_data_hint_local;
        self.dir_append_hint = snap.dir_append_hint;
    }

    /// Return a mutable reference to the rootblock cluster buffer (loaded
    /// into the dirty_reserved map on first call). Width is exactly
    /// `reserved_blksize` bytes — the rootblock itself in the first 512
    /// bytes followed by the reserved-bitmap block at offset 512.
    fn rootblock_cluster_mut(&mut self) -> Result<&mut [u8], FilesystemError> {
        let sec = self.rootblock_cluster_sec();
        self.ensure_reserved_dirty(sec)?;
        Ok(self.dirty_reserved.get_mut(&sec).unwrap().as_mut_slice())
    }

    /// Ensure the reserved-block cluster at HW sector `sec` is staged in
    /// `dirty_reserved`. If absent, copy from the read cache (loading
    /// from disk first if necessary).
    fn ensure_reserved_dirty(&mut self, sec: u32) -> Result<(), FilesystemError> {
        if self.dirty_reserved.contains_key(&sec) {
            return Ok(());
        }
        let bytes = self.read_reserved_block(sec)?.to_vec();
        self.dirty_reserved.insert(sec, bytes);
        Ok(())
    }

    /// Find a free reserved block (set bit in reserved bitmap), clear
    /// the bit, decrement `reserved_free`, and return the HW sector of
    /// the newly-allocated block.
    fn alloc_reserved_block(&mut self) -> Result<u32, FilesystemError> {
        let numreserved = (self.root.last_reserved - self.root.first_reserved + 1) / self.rscluster;
        let cluster = self.rootblock_cluster_mut()?;
        // Reserved bitmap lives at offset 512 of the rootblock cluster;
        // bitmap[] starts at +12 within that block. We support exactly
        // the format our blank-volume layout emits (one BM word covers
        // up to 32 reserved blocks); for numreserved > 32 we'd need to
        // scan a second word, which is fine since the format allows it.
        let bm_off = 512 + 12;
        for word_idx in 0..((numreserved + 31) / 32) {
            let o = bm_off + word_idx as usize * 4;
            if o + 4 > cluster.len() {
                break;
            }
            let mut word =
                u32::from_be_bytes([cluster[o], cluster[o + 1], cluster[o + 2], cluster[o + 3]]);
            if word == 0 {
                continue;
            }
            for bit_pos in 0..32u32 {
                if (word >> (31 - bit_pos)) & 1 == 1 {
                    let res_idx = word_idx * 32 + bit_pos;
                    if res_idx >= numreserved {
                        break;
                    }
                    word &= !(1u32 << (31 - bit_pos));
                    cluster[o..o + 4].copy_from_slice(&word.to_be_bytes());
                    self.root.reserved_free = self.root.reserved_free.saturating_sub(1);
                    let cluster_sec = self.rootblock_cluster_sec();
                    let sec = self.root.first_reserved + res_idx * self.rscluster;
                    // Persist updated rootblock fields (reserved_free) to
                    // the on-cluster bytes too.
                    let root_clone = self.root.clone();
                    let cluster_buf = self.dirty_reserved.get_mut(&cluster_sec).unwrap();
                    root_clone.write_into(&mut cluster_buf[..512]);
                    // Zero-out the freshly allocated reserved block in
                    // memory so the caller starts from a clean slate.
                    self.dirty_reserved
                        .insert(sec, vec![0u8; self.reserved_blksize as usize]);
                    return Ok(sec);
                }
            }
        }
        Err(FilesystemError::DiskFull(
            "PFS3: out of free reserved blocks".into(),
        ))
    }

    /// Free a previously-allocated reserved block: set its bit in the
    /// reserved bitmap and bump `reserved_free`.
    fn free_reserved_block(&mut self, sec: u32) -> Result<(), FilesystemError> {
        if sec < self.root.first_reserved
            || sec > self.root.last_reserved
            || (sec - self.root.first_reserved) % self.rscluster != 0
        {
            return Err(parse_err(format!(
                "free_reserved_block: sec {} not aligned to a reserved cluster",
                sec
            )));
        }
        let res_idx = (sec - self.root.first_reserved) / self.rscluster;
        let cluster = self.rootblock_cluster_mut()?;
        let bm_off = 512 + 12;
        let o = bm_off + (res_idx / 32) as usize * 4;
        if o + 4 > cluster.len() {
            return Err(parse_err(format!(
                "free_reserved_block: bitmap word for index {} out of range",
                res_idx
            )));
        }
        let mut word =
            u32::from_be_bytes([cluster[o], cluster[o + 1], cluster[o + 2], cluster[o + 3]]);
        word |= 1u32 << (31 - (res_idx % 32));
        cluster[o..o + 4].copy_from_slice(&word.to_be_bytes());
        self.root.reserved_free += 1;
        let root_clone = self.root.clone();
        let cluster_sec = self.rootblock_cluster_sec();
        let cluster_buf = self.dirty_reserved.get_mut(&cluster_sec).unwrap();
        root_clone.write_into(&mut cluster_buf[..512]);
        // Drop any pending edits inside the now-free block; further
        // reads will fault from disk if anyone touches the stale block.
        self.dirty_reserved.remove(&sec);
        self.cache.remove(&sec);
        Ok(())
    }

    /// Look up which anodeblock holds `anodenr` and return its HW sector
    /// plus the byte offset of the anode within that block. The anodeblock
    /// must already exist; this does not allocate new anodeblocks.
    fn anode_location(&mut self, anodenr: u32) -> Result<(u32, usize), FilesystemError> {
        let (seqnr, offset) = self.split_anodenr(anodenr);
        let ab_sec = self.anodeblock_blocknr(seqnr)?;
        let off = 16 + offset as usize * 12;
        Ok((ab_sec, off))
    }

    /// Stage an anode update in `dirty_reserved`.
    fn save_anode_in_memory(&mut self, anodenr: u32, anode: Canode) -> Result<(), FilesystemError> {
        let (ab_sec, off) = self.anode_location(anodenr)?;
        self.ensure_reserved_dirty(ab_sec)?;
        let blk = self.dirty_reserved.get_mut(&ab_sec).unwrap();
        blk[off..off + 4].copy_from_slice(&anode.clustersize.to_be_bytes());
        blk[off + 4..off + 8].copy_from_slice(&anode.blocknr.to_be_bytes());
        blk[off + 8..off + 12].copy_from_slice(&anode.next.to_be_bytes());
        Ok(())
    }

    /// Find an unused anode slot (clustersize=0 AND blocknr=0 AND
    /// next=0) anywhere in the anodeblock chain and mark it allocated
    /// by writing the `(0, 0xFFFFFFFF, 0)` sentinel pfs3aio uses.
    /// Returns the anodenr.
    ///
    /// Walks anodeblock seqnrs 0, 1, 2, … resolving each via
    /// `anodeblock_blocknr`. The first AB that has a free slot wins.
    /// If every existing AB is full, allocates a fresh AB (and a
    /// fresh IB to host it, if the current IB is full), then returns
    /// slot 0 of the new AB.
    ///
    /// Slots `0..PFS3_ANODE_USERFIRST` of seqnr 0 are reserved for
    /// PFS3 bookkeeping (root anode, etc.) and are skipped during
    /// search. Higher-seqnr ABs have no reserved slots — slot 0 is
    /// usable.
    fn alloc_anode_in_memory(&mut self) -> Result<u32, FilesystemError> {
        let anodesperblock = self.anodesperblock;
        // Start where the previous successful allocation left off. Skips
        // the linear walk over already-full anodeblocks that made bulk
        // clones O(N^2). `free_anode_in_memory` lowers this hint so we
        // never miss a slot that came free again.
        let mut seqnr: u32 = self.alloc_anode_hint_seqnr;
        loop {
            let ab_sec_or_err = self.anodeblock_blocknr(seqnr);
            match ab_sec_or_err {
                Ok(ab_sec) => {
                    self.ensure_reserved_dirty(ab_sec)?;
                    let blk = self.dirty_reserved.get_mut(&ab_sec).unwrap();
                    let start_slot = if seqnr == 0 {
                        PFS3_ANODE_USERFIRST as usize
                    } else {
                        0
                    };
                    for slot in start_slot..anodesperblock as usize {
                        let o = 16 + slot * 12;
                        if o + 12 > blk.len() {
                            break;
                        }
                        let cs = u32::from_be_bytes([blk[o], blk[o + 1], blk[o + 2], blk[o + 3]]);
                        let bn =
                            u32::from_be_bytes([blk[o + 4], blk[o + 5], blk[o + 6], blk[o + 7]]);
                        let nx =
                            u32::from_be_bytes([blk[o + 8], blk[o + 9], blk[o + 10], blk[o + 11]]);
                        if cs == 0 && bn == 0 && nx == 0 {
                            blk[o..o + 4].copy_from_slice(&0u32.to_be_bytes());
                            blk[o + 4..o + 8]
                                .copy_from_slice(&PFS3_ANODE_EOF_SENTINEL.to_be_bytes());
                            blk[o + 8..o + 12].copy_from_slice(&0u32.to_be_bytes());
                            self.alloc_anode_hint_seqnr = seqnr;
                            return Ok(self.encode_anodenr(seqnr, slot as u32));
                        }
                    }
                    seqnr += 1;
                }
                Err(_) => {
                    // No anodeblock at this seqnr — allocate one.
                    let new_ab_sec = self.alloc_new_anodeblock(seqnr)?;
                    self.ensure_reserved_dirty(new_ab_sec)?;
                    let blk = self.dirty_reserved.get_mut(&new_ab_sec).unwrap();
                    let slot = 0usize;
                    let o = 16 + slot * 12;
                    blk[o..o + 4].copy_from_slice(&0u32.to_be_bytes());
                    blk[o + 4..o + 8].copy_from_slice(&PFS3_ANODE_EOF_SENTINEL.to_be_bytes());
                    blk[o + 8..o + 12].copy_from_slice(&0u32.to_be_bytes());
                    self.alloc_anode_hint_seqnr = seqnr;
                    return Ok(self.encode_anodenr(seqnr, slot as u32));
                }
            }
        }
    }

    /// Encode `(ab_seqnr, slot)` into an anodenr per the volume's
    /// `MODE_SPLITTED_ANODES` setting.
    fn encode_anodenr(&self, seqnr: u32, slot: u32) -> u32 {
        if self.anodesplitmode {
            (seqnr << 16) | slot
        } else {
            seqnr * self.anodesperblock + slot
        }
    }

    /// Allocate a fresh anodeblock at sequence number `seqnr`. Wires
    /// it into the appropriate index block (allocating a new IB +
    /// updating `small_indexblocks` if needed). Returns the new AB's
    /// HW-sector.
    fn alloc_new_anodeblock(&mut self, seqnr: u32) -> Result<u32, FilesystemError> {
        let indexperblock = self.indexperblock;
        let indexblock_seqnr = seqnr / indexperblock;
        let index_offset = (seqnr % indexperblock) as usize;

        // Get-or-create the IB.
        let ib_sec = match self
            .root
            .small_indexblocks
            .get(indexblock_seqnr as usize)
            .copied()
        {
            Some(s) if s != 0 => s,
            Some(_) => {
                // Slot exists but is zero — allocate a new IB and
                // record it.
                let new_ib_sec = self.alloc_new_indexblock(indexblock_seqnr)?;
                new_ib_sec
            }
            None => {
                return Err(FilesystemError::DiskFull(format!(
                    "PFS3: indexblock seqnr {} exceeds small_indexblocks capacity ({})",
                    indexblock_seqnr,
                    self.root.small_indexblocks.len()
                )));
            }
        };

        // Allocate a reserved block for the new AB; init header.
        let new_ab_sec = self.alloc_reserved_block()?;
        {
            let ab_blk = self.dirty_reserved.get_mut(&new_ab_sec).unwrap();
            write_u16(ab_blk, 0, ID_ANODEBLOCK);
            write_u32(ab_blk, 4, 1); // datestamp
            write_u32(ab_blk, 8, seqnr); // seqnr
        }

        // Write the new AB's sector into the IB's index[index_offset].
        self.ensure_reserved_dirty(ib_sec)?;
        let ib_blk = self.dirty_reserved.get_mut(&ib_sec).unwrap();
        let o = 12 + index_offset * 4;
        if o + 4 > ib_blk.len() {
            return Err(parse_err(format!(
                "alloc_new_anodeblock: IB index_offset {} out of range for IB at sec {}",
                index_offset, ib_sec
            )));
        }
        ib_blk[o..o + 4].copy_from_slice(&new_ab_sec.to_be_bytes());

        Ok(new_ab_sec)
    }

    /// Allocate a fresh indexblock at sequence number `indexblock_seqnr`,
    /// record its HW-sector in `small_indexblocks[indexblock_seqnr]`,
    /// and persist the updated rootblock cluster bytes.
    fn alloc_new_indexblock(&mut self, indexblock_seqnr: u32) -> Result<u32, FilesystemError> {
        let new_ib_sec = self.alloc_reserved_block()?;
        {
            let ib_blk = self.dirty_reserved.get_mut(&new_ib_sec).unwrap();
            write_u16(ib_blk, 0, ID_INDEXBLOCK);
            write_u32(ib_blk, 4, 1); // datestamp
            write_u32(ib_blk, 8, indexblock_seqnr); // seqnr
        }
        // Update in-memory rootblock.
        self.root.small_indexblocks[indexblock_seqnr as usize] = new_ib_sec;
        // Persist directly into the on-disk rootblock cluster — only
        // the one slot we changed. `Pfs3RootBlock::write_into`
        // intentionally skips the index arrays at offset 96+ (see its
        // doc), so we patch them surgically here. Small-mode layout:
        // bitmapindex[5] @ 96..116, small_indexblocks[99] @ 116..512.
        let slot_offset = 116 + indexblock_seqnr as usize * 4;
        let cluster = self.rootblock_cluster_mut()?;
        cluster[slot_offset..slot_offset + 4].copy_from_slice(&new_ib_sec.to_be_bytes());
        Ok(new_ib_sec)
    }

    /// Free an anode: zero its slot. Caller is responsible for first
    /// freeing any blocks the anode chain referenced.
    fn free_anode_in_memory(&mut self, anodenr: u32) -> Result<(), FilesystemError> {
        if anodenr < PFS3_ANODE_USERFIRST {
            return Err(parse_err(format!(
                "free_anode_in_memory: anode {} is PFS3-reserved",
                anodenr
            )));
        }
        let (ab_sec, off) = self.anode_location(anodenr)?;
        self.ensure_reserved_dirty(ab_sec)?;
        let blk = self.dirty_reserved.get_mut(&ab_sec).unwrap();
        for b in &mut blk[off..off + 12] {
            *b = 0;
        }
        let (seqnr, _) = self.split_anodenr(anodenr);
        if seqnr < self.alloc_anode_hint_seqnr {
            self.alloc_anode_hint_seqnr = seqnr;
        }
        Ok(())
    }

    /// Allocate `count` contiguous data-area HW sectors by scanning the
    /// data bitmap (across all BM blocks the rootblock points at). Clears
    /// the bits in the bitmap and returns the first sector.
    fn alloc_data_blocks(&mut self, count: u32) -> Result<u32, FilesystemError> {
        if count == 0 {
            return Err(parse_err("alloc_data_blocks: count must be > 0"));
        }
        let bitmap_start = self.root.last_reserved + 1;
        let data_sectors = (self.partition_size / HW_SECTOR) as u32 - bitmap_start;
        let longsperbmb = (self.reserved_blksize / 4) - 3;
        let sectors_per_bmb = longsperbmb * 32;

        // Search across BM blocks via the rootblock.bitmapindex[] list.
        // Each MI block holds `indexperblock` BM pointers; we follow the
        // small layout convention.
        let mi_pointers: Vec<u32> = self.root.bitmapindex.iter().copied().collect();
        // Start the search at the rolling hint to avoid an O(N) walk
        // over already-allocated regions on every call. The hint
        // monotonically advances as runs are consumed and is lowered by
        // `free_data_blocks` when sectors come free again, so we never
        // miss a run permanently.
        let hint_local = self.alloc_data_hint_local.min(data_sectors);
        let bits_per_mi = self.indexperblock.saturating_mul(sectors_per_bmb);
        let hint_mi_seq = (hint_local / bits_per_mi.max(1)) as usize;
        let hint_remainder_in_mi = hint_local % bits_per_mi.max(1);
        let hint_i = (hint_remainder_in_mi / sectors_per_bmb.max(1)) as usize;
        let hint_remainder_in_bm = hint_remainder_in_mi % sectors_per_bmb.max(1);
        let hint_w = (hint_remainder_in_bm / 32) as u32;
        let hint_bit = hint_remainder_in_bm % 32;
        // Linear scan to find a run of `count` consecutive free bits.
        let mut run_start: Option<u32> = None;
        let mut run_len: u32 = 0;
        let mut found: Option<u32> = None;
        let mut current_data_sector: u32 = 0;
        'outer: for (mi_seq, &mi_blk) in mi_pointers.iter().enumerate() {
            if mi_seq < hint_mi_seq {
                continue;
            }
            if mi_blk == 0 {
                continue;
            }
            // Walk this MI block to enumerate BM pointers.
            let mi_buf = self.read_reserved_block(mi_blk)?.to_vec();
            if rd_u16(&mi_buf, 0) != ID_BITMAPINDEXBLOCK {
                continue;
            }
            for i in 0..self.indexperblock {
                if mi_seq == hint_mi_seq && (i as usize) < hint_i {
                    continue;
                }
                let off = 12 + i as usize * 4;
                if off + 4 > mi_buf.len() {
                    break;
                }
                let bm_blk = rd_u32(&mi_buf, off);
                if bm_blk == 0 {
                    current_data_sector += sectors_per_bmb;
                    run_start = None;
                    run_len = 0;
                    continue;
                }
                let bm = self.read_reserved_block(bm_blk)?.to_vec();
                if rd_u16(&bm, 0) != ID_BITMAPBLOCK {
                    current_data_sector += sectors_per_bmb;
                    run_start = None;
                    run_len = 0;
                    continue;
                }
                for w in 0..longsperbmb {
                    if mi_seq == hint_mi_seq && (i as usize) == hint_i && w < hint_w {
                        continue;
                    }
                    let o = 12 + w as usize * 4;
                    if o + 4 > bm.len() {
                        break;
                    }
                    let word = rd_u32(&bm, o);
                    for bit in 0..32u32 {
                        if mi_seq == hint_mi_seq
                            && (i as usize) == hint_i
                            && w == hint_w
                            && bit < hint_bit
                        {
                            continue;
                        }
                        let local_sec = (mi_seq as u32 * self.indexperblock + i) * sectors_per_bmb
                            + w * 32
                            + bit;
                        if local_sec >= data_sectors {
                            break;
                        }
                        let is_free = (word >> (31 - bit)) & 1 == 1;
                        if is_free {
                            if run_start.is_none() {
                                run_start = Some(local_sec);
                                run_len = 1;
                            } else {
                                run_len += 1;
                            }
                            if run_len >= count {
                                found = run_start;
                                break 'outer;
                            }
                        } else {
                            run_start = None;
                            run_len = 0;
                        }
                    }
                }
                let _ = current_data_sector;
            }
        }

        let first_local = match found {
            Some(v) => v,
            None if self.alloc_data_hint_local > 0 => {
                // Hint-forward search missed (likely due to
                // fragmentation that pushed the run before the hint).
                // Reset and retry from sector 0.
                self.alloc_data_hint_local = 0;
                return self.alloc_data_blocks(count);
            }
            None => {
                return Err(FilesystemError::DiskFull(format!(
                    "PFS3: no contiguous run of {} free data sectors",
                    count
                )))
            }
        };
        // Clear the bits for sectors first_local..first_local+count.
        self.toggle_data_bitmap_bits(first_local, count, /*set_free=*/ false)?;
        // Advance the rolling hint past the run we just consumed.
        self.alloc_data_hint_local = first_local.saturating_add(count);
        self.root.blocks_free = self.root.blocks_free.saturating_sub(count);
        // Persist blocks_free into rootblock cluster.
        let root_clone = self.root.clone();
        let cluster_sec = self.rootblock_cluster_sec();
        self.ensure_reserved_dirty(cluster_sec)?;
        let cluster_buf = self.dirty_reserved.get_mut(&cluster_sec).unwrap();
        root_clone.write_into(&mut cluster_buf[..512]);
        Ok(bitmap_start + first_local)
    }

    /// Free `count` data-area HW sectors starting at `first_sec`.
    fn free_data_blocks(&mut self, first_sec: u32, count: u32) -> Result<(), FilesystemError> {
        let bitmap_start = self.root.last_reserved + 1;
        if first_sec < bitmap_start {
            return Err(parse_err(format!(
                "free_data_blocks: sec {} below data area start {}",
                first_sec, bitmap_start
            )));
        }
        let local = first_sec - bitmap_start;
        self.toggle_data_bitmap_bits(local, count, /*set_free=*/ true)?;
        if local < self.alloc_data_hint_local {
            self.alloc_data_hint_local = local;
        }
        self.root.blocks_free = self.root.blocks_free.saturating_add(count);
        // Also drop any pending data writes that referenced the freed
        // sectors so we don't flush stale bytes after a delete.
        for i in 0..count {
            self.dirty_data.remove(&(first_sec + i));
        }
        let root_clone = self.root.clone();
        let cluster_sec = self.rootblock_cluster_sec();
        self.ensure_reserved_dirty(cluster_sec)?;
        let cluster_buf = self.dirty_reserved.get_mut(&cluster_sec).unwrap();
        root_clone.write_into(&mut cluster_buf[..512]);
        Ok(())
    }

    /// Flip a contiguous range of bits in the data bitmap. `set_free`
    /// chooses the direction (true = set bit = free, false = clear).
    fn toggle_data_bitmap_bits(
        &mut self,
        first_local: u32,
        count: u32,
        set_free: bool,
    ) -> Result<(), FilesystemError> {
        let longsperbmb = (self.reserved_blksize / 4) - 3;
        let sectors_per_bmb = longsperbmb * 32;
        for offset in 0..count {
            let local = first_local + offset;
            let bm_seq = local / sectors_per_bmb;
            let bit_within_bmb = local % sectors_per_bmb;
            let word_idx = bit_within_bmb / 32;
            let bit_pos = bit_within_bmb % 32;
            let mi_seq = bm_seq / self.indexperblock;
            let mi_offset = bm_seq % self.indexperblock;
            let mi_blk = *self
                .root
                .bitmapindex
                .get(mi_seq as usize)
                .ok_or_else(|| parse_err("toggle_data_bitmap_bits: MI out of range"))?;
            let mi_buf = self.read_reserved_block(mi_blk)?.to_vec();
            let bm_off = 12 + mi_offset as usize * 4;
            let bm_blk = rd_u32(&mi_buf, bm_off);
            if bm_blk == 0 {
                return Err(parse_err(
                    "toggle_data_bitmap_bits: BM block pointer is zero",
                ));
            }
            self.ensure_reserved_dirty(bm_blk)?;
            let bm = self.dirty_reserved.get_mut(&bm_blk).unwrap();
            let o = 12 + word_idx as usize * 4;
            let mut word = u32::from_be_bytes([bm[o], bm[o + 1], bm[o + 2], bm[o + 3]]);
            let mask = 1u32 << (31 - bit_pos);
            if set_free {
                word |= mask;
            } else {
                word &= !mask;
            }
            bm[o..o + 4].copy_from_slice(&word.to_be_bytes());
        }
        Ok(())
    }
}

/// Encode a single direntry into a byte vector. Returns the bytes; the
/// caller is responsible for splicing them into the dirblock entries
/// area. Length is always even (so the next entry starts on a u16
/// boundary).
fn build_direntry(
    ttype: i8,
    anode: u32,
    fsize: u64,
    name: &str,
    comment: &str,
    protection: u8,
    dates: Option<(i32, i32, i32)>,
    extra_link: Option<u32>,
) -> Result<Vec<u8>, FilesystemError> {
    let name_bytes = name.as_bytes();
    let comment_bytes = comment.as_bytes();
    // Practical cap matches pfs3aio's `fnsize` ceiling (107 bytes —
    // the LONGFN-enabled max; standard AmigaDOS is 30 bytes). We set
    // `fnsize = 107` in the blank-volume's rootblock extension so
    // tools accept long names without truncation. The hard cap from
    // direntry total = u8 max (255) leaves ~230 bytes after headers /
    // comment / extrafields, but `fnsize` is the conventional limit.
    if name.is_empty() || name_bytes.len() > 107 {
        return Err(parse_err(format!(
            "build_direntry: name '{}' ({} bytes) must be 1..107 bytes",
            name,
            name_bytes.len()
        )));
    }
    if comment_bytes.len() > 79 {
        return Err(parse_err("build_direntry: comment exceeds 79 bytes"));
    }
    // Compute extrafields tail size: each present u16 word costs 2 bytes
    // plus a single trailing 2-byte `flags` word when any field is set.
    // We only ever set `link` (split into link_hi + link_lo), so the
    // tail is 0, 4, or 6 bytes.
    let extra_tail = match extra_link {
        None => 0usize,
        Some(v) => {
            let link_hi = (v >> 16) & 0xFFFF;
            let link_lo = v & 0xFFFF;
            // Non-zero u16s consume 2 bytes each. flags itself is 2.
            let mut t = 2usize;
            if link_hi != 0 {
                t += 2;
            }
            if link_lo != 0 {
                t += 2;
            }
            t
        }
    };
    let header_and_payload_unaligned = 18 + name_bytes.len() + 1 + comment_bytes.len();
    let aligned = (header_and_payload_unaligned + 1) & !1;
    let total = aligned + extra_tail;
    if total > 255 {
        return Err(parse_err("build_direntry: entry exceeds 255 bytes"));
    }
    let mut buf = vec![0u8; total];
    buf[0] = total as u8;
    buf[1] = ttype as u8;
    buf[2..6].copy_from_slice(&anode.to_be_bytes());
    buf[6..10].copy_from_slice(&(fsize as u32).to_be_bytes());
    // creation_day/min/tick at 10..16. Honored by tooling only when the
    // rootblock has MODE_DATESTAMP set; `create_blank_pfs3` always sets
    // it, so the dates we write here round-trip on our targets.
    if let Some((cd, cm, ct)) = dates {
        write_u16(&mut buf, 10, cd as u16);
        write_u16(&mut buf, 12, cm as u16);
        write_u16(&mut buf, 14, ct as u16);
    }
    buf[16] = protection;
    buf[17] = name_bytes.len() as u8;
    buf[18..18 + name_bytes.len()].copy_from_slice(name_bytes);
    let cmt_pos = 18 + name_bytes.len();
    buf[cmt_pos] = comment_bytes.len() as u8;
    buf[cmt_pos + 1..cmt_pos + 1 + comment_bytes.len()].copy_from_slice(comment_bytes);
    // Pack extrafields at the tail. Layout (see GetExtraFields back-
    // walker): from the END going backwards: flags, link_hi, link_lo
    // (each only present when its flag bit is set). LSB-first bit
    // order: bit 0 = link_hi, bit 1 = link_lo.
    if let Some(v) = extra_link {
        let link_hi = ((v >> 16) & 0xFFFF) as u16;
        let link_lo = (v & 0xFFFF) as u16;
        // Order matches the back-walker: link_hi is the FIRST u16
        // read (immediately before flags), link_lo is the second.
        // Skip zero fields — those flag bits stay clear and no u16 is
        // emitted for them.
        let mut flags: u16 = 0;
        let mut cursor = total - 2;
        if link_hi != 0 {
            cursor -= 2;
            write_u16(&mut buf, cursor, link_hi);
            flags |= 0b01;
        }
        if link_lo != 0 {
            cursor -= 2;
            write_u16(&mut buf, cursor, link_lo);
            flags |= 0b10;
        }
        write_u16(&mut buf, total - 2, flags);
    }
    Ok(buf)
}

impl<R: Read + Write + Seek + Send> Pfs3Filesystem<R> {
    /// Register `new_link_anode` as a hardlink pointing at the entry
    /// whose direntry lives in `target_parent_anode`'s dirblock chain
    /// and whose own anode is `target_anode`. Updates the back-link
    /// chain so tools that enumerate "all hardlinks to this file" by
    /// walking `target.extrafields.link` + `linknode.next` find the
    /// new link.
    ///
    /// Behavior matches pfs3aio `CreateLink` (`directory.c:2703-2727`):
    /// - If target has no extrafields.link yet, patch its direntry
    ///   in place to add `extrafields.link = new_link_anode`. The new
    ///   linknode's `next` stays 0 (the linknode itself was already
    ///   set up by `do_create_hardlink`).
    /// - If target already has a link chain, walk it via `anode.next`
    ///   and set the tail's `next` to `new_link_anode`.
    ///
    /// Wrapped in snapshot/rollback so a mid-operation error leaves
    /// the volume unchanged.
    pub fn register_hardlink_in_target_chain(
        &mut self,
        target_parent_anode: u32,
        target_anode: u32,
        new_link_anode: u32,
    ) -> Result<(), FilesystemError> {
        let snap = self.snapshot();
        match self.do_register_hardlink_in_target_chain(
            target_parent_anode,
            target_anode,
            new_link_anode,
        ) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.restore_snapshot(snap);
                Err(e)
            }
        }
    }

    /// Test/debug accessor: return the current `extrafields.link`
    /// value stored on the direntry whose anode is `target_anode`
    /// inside `parent_anode`'s dirblock chain. `0` means "no link
    /// extrafield set." Returns `Err` if the target isn't found.
    pub fn peek_direntry_extra_link(
        &mut self,
        parent_anode: u32,
        target_anode: u32,
    ) -> Result<u32, FilesystemError> {
        let (db_sec, off) =
            find_direntry_in_dir(self, parent_anode, target_anode)?.ok_or_else(|| {
                parse_err(format!(
                    "peek_direntry_extra_link: anode {target_anode} not found in dir {parent_anode}"
                ))
            })?;
        read_direntry_extra_link_at(self, db_sec, off)
    }

    /// Test/debug accessor: return the `next` anode in a linknode's
    /// chain. Used by the bidirectional hardlink test to walk the
    /// back-link chain without leaking internals.
    pub fn peek_anode_next(&mut self, anodenr: u32) -> Result<u32, FilesystemError> {
        Ok(self.get_anode(anodenr)?.next)
    }

    fn do_register_hardlink_in_target_chain(
        &mut self,
        target_parent_anode: u32,
        target_anode: u32,
        new_link_anode: u32,
    ) -> Result<(), FilesystemError> {
        let (db_sec, off) = find_direntry_in_dir(self, target_parent_anode, target_anode)?
            .ok_or_else(|| {
                parse_err(format!(
                    "register_hardlink: target anode {target_anode} not found in parent \
                     dir anode {target_parent_anode}"
                ))
            })?;
        let existing = read_direntry_extra_link_at(self, db_sec, off)?;
        if existing == 0 {
            append_link_extrafield_to_direntry(self, db_sec, off, new_link_anode)?;
        } else {
            append_to_linknode_chain(self, existing, new_link_anode)?;
        }
        Ok(())
    }

    /// Implementation of `EditableFilesystem::create_directory` body —
    /// kept as a free function so the trait impl below only has to
    /// snapshot/restore around it.
    fn do_create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        comment: &str,
        protection: u8,
        dates: Option<(i32, i32, i32)>,
    ) -> Result<FileEntry, FilesystemError> {
        check_no_duplicate(self, parent, name)?;
        let parent_anode = parent.location as u32;
        let new_anode = self.alloc_anode_in_memory()?;
        let new_dirblock_sec = self.alloc_reserved_block()?;
        // Initialize the new dirblock.
        {
            let blk = self.dirty_reserved.get_mut(&new_dirblock_sec).unwrap();
            write_u16(blk, 0, ID_DIRBLOCK);
            write_u32(blk, 4, 1); // datestamp
            write_u32(blk, 12, new_anode); // anodenr
            write_u32(blk, 16, parent_anode); // parent
                                              // entries empty (first byte = 0 already)
        }
        // Set the new anode to point at the dirblock.
        self.save_anode_in_memory(
            new_anode,
            Canode {
                clustersize: 1,
                blocknr: new_dirblock_sec,
                next: 0,
            },
        )?;
        // Add direntry to parent.
        add_direntry_to_dir(
            self,
            parent_anode,
            ST_DIR,
            new_anode,
            0,
            name,
            comment,
            protection,
            dates,
            None,
        )?;
        let path = if parent.path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent.path, name)
        };
        Ok(FileEntry::new_directory(
            name.to_string(),
            path,
            new_anode as u64,
        ))
    }

    fn do_create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
        comment: &str,
        protection: u8,
        dates: Option<(i32, i32, i32)>,
    ) -> Result<FileEntry, FilesystemError> {
        check_no_duplicate(self, parent, name)?;
        let parent_anode = parent.location as u32;
        let new_anode = self.alloc_anode_in_memory()?;
        let (first_sec, sectors) = if data_len == 0 {
            (0u32, 0u32)
        } else {
            let sectors = ((data_len + HW_SECTOR - 1) / HW_SECTOR) as u32;
            let first = self.alloc_data_blocks(sectors)?;
            (first, sectors)
        };

        // Stream data into dirty_data, sector by sector, zero-padding
        // the final sector if data_len isn't a multiple of HW_SECTOR.
        if sectors > 0 {
            let mut written: u64 = 0;
            for i in 0..sectors {
                let mut buf = vec![0u8; HW_SECTOR as usize];
                let remaining = data_len - written;
                let to_read = remaining.min(HW_SECTOR) as usize;
                data.read_exact(&mut buf[..to_read])?;
                self.dirty_data.insert(first_sec + i, buf);
                written += to_read as u64;
            }
        }

        // For file anodes: clustersize is in HW sectors, blocknr is
        // the first sector of the run, next chains to subsequent anodes
        // (we allocate contiguously so next=0).
        self.save_anode_in_memory(
            new_anode,
            Canode {
                clustersize: sectors,
                blocknr: if sectors == 0 { 0 } else { first_sec },
                next: 0,
            },
        )?;
        add_direntry_to_dir(
            self,
            parent_anode,
            ST_FILE,
            new_anode,
            data_len,
            name,
            comment,
            protection,
            dates,
            None,
        )?;
        let path = if parent.path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent.path, name)
        };
        Ok(FileEntry::new_file(
            name.to_string(),
            path,
            data_len,
            new_anode as u64,
        ))
    }

    /// Create a softlink direntry. Allocates one data block (HW sector)
    /// for the target path, writes it NUL-terminated, registers the
    /// anode pointing at that block, then adds an `ST_SOFTLINK`
    /// direntry to the parent with `fsize = target.len()`. Matches
    /// pfs3aio `CreateSoftLink` semantics; capped at 510 bytes target
    /// (one HW sector minus a NUL terminator).
    fn do_create_symlink(
        &mut self,
        parent: &FileEntry,
        name: &str,
        target: &str,
        comment: &str,
        protection: u8,
        dates: Option<(i32, i32, i32)>,
    ) -> Result<FileEntry, FilesystemError> {
        check_no_duplicate(self, parent, name)?;
        let target_bytes = target.as_bytes();
        if target_bytes.len() >= HW_SECTOR as usize {
            return Err(parse_err(format!(
                "create_symlink: target path '{}' exceeds {} bytes",
                target,
                HW_SECTOR - 1
            )));
        }
        let parent_anode = parent.location as u32;
        let new_anode = self.alloc_anode_in_memory()?;
        let target_sec = self.alloc_data_blocks(1)?;
        // Stage the target-string block: NUL-terminate then zero-fill.
        let mut buf = vec![0u8; HW_SECTOR as usize];
        buf[..target_bytes.len()].copy_from_slice(target_bytes);
        self.dirty_data.insert(target_sec, buf);
        self.save_anode_in_memory(
            new_anode,
            Canode {
                clustersize: 1,
                blocknr: target_sec,
                next: 0,
            },
        )?;
        add_direntry_to_dir(
            self,
            parent_anode,
            ST_SOFTLINK,
            new_anode,
            target_bytes.len() as u64,
            name,
            comment,
            protection,
            dates,
            None,
        )?;
        let path = if parent.path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent.path, name)
        };
        Ok(FileEntry::new_symlink(
            name.to_string(),
            path,
            target_bytes.len() as u64,
            new_anode as u64,
            target.to_string(),
        ))
    }

    /// Create a hardlink direntry pointing at `target` (which must
    /// already exist on this volume). Sets the link's
    /// `extrafields.link` to the target's anode so resolvers can
    /// follow it directly to the target data. Builds a placeholder
    /// linknode at the new anode (clustersize/blocknr describe the
    /// link's location, `next=0`).
    ///
    /// **Limitation (v1):** does NOT patch the target's direntry to
    /// register this link in the target's extrafields chain. Tools
    /// that enumerate "all hardlinks to this file" by walking the
    /// target's linknode chain will miss it. The link resolves
    /// correctly to target data; only the reverse query is affected.
    /// Acceptable for clone-from-source workflows where the target
    /// volume is read-only after construction.
    fn do_create_hardlink(
        &mut self,
        parent: &FileEntry,
        name: &str,
        target: &FileEntry,
        comment: &str,
        protection: u8,
        dates: Option<(i32, i32, i32)>,
    ) -> Result<FileEntry, FilesystemError> {
        check_no_duplicate(self, parent, name)?;
        let parent_anode = parent.location as u32;
        let target_anode = target.location as u32;
        if target_anode == 0 {
            return Err(parse_err(
                "create_hardlink: target has no anode (uninitialized FileEntry)",
            ));
        }
        let ttype = if target.is_directory() {
            ST_LINKDIR
        } else {
            ST_LINKFILE
        };
        let new_anode = self.alloc_anode_in_memory()?;
        // Linknode: clustersize is target-parent anode in pfs3aio
        // (used for "which dir holds the target" lookups), blocknr is
        // link-parent anode (used to find the link's location).
        // Without scanning every dirblock to find target's parent, we
        // record only the link side here. See struct-doc above.
        self.save_anode_in_memory(
            new_anode,
            Canode {
                clustersize: 0,
                blocknr: parent_anode,
                next: 0,
            },
        )?;
        add_direntry_to_dir(
            self,
            parent_anode,
            ttype,
            new_anode,
            target.size,
            name,
            comment,
            protection,
            dates,
            Some(target_anode),
        )?;
        let path = if parent.path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent.path, name)
        };
        let mut fe = if target.is_directory() {
            FileEntry::new_directory(name.to_string(), path, new_anode as u64)
        } else {
            FileEntry::new_file(name.to_string(), path, target.size, new_anode as u64)
        };
        fe.link_target_cnid = Some(target_anode as u64);
        Ok(fe)
    }

    fn do_delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let parent_anode = parent.location as u32;
        let entry_anode = entry.location as u32;
        // Free the anode chain. For directories, also free the dirblock(s)
        // and reject if non-empty.
        let mut anode = self.get_anode(entry_anode)?;
        if entry.is_directory() {
            // Walk dirblocks looking for any non-zero `next` byte at the
            // start of entries[] (offset 20). All-empty means safe to free.
            let mut a = anode;
            loop {
                if a.blocknr == 0 || a.blocknr == PFS3_ANODE_EOF_SENTINEL {
                    break;
                }
                let buf = self.read_reserved_block(a.blocknr)?.to_vec();
                if buf.len() > 20 && buf[20] != 0 {
                    return Err(parse_err(format!(
                        "directory '{}' is not empty",
                        entry.name
                    )));
                }
                if a.next == 0 {
                    break;
                }
                a = self.get_anode(a.next)?;
            }
            // Free the chain of dirblocks (one per anode in the chain).
            let mut a = anode;
            loop {
                if a.blocknr != 0 && a.blocknr != PFS3_ANODE_EOF_SENTINEL {
                    self.free_reserved_block(a.blocknr)?;
                }
                if a.next == 0 {
                    break;
                }
                let next_an = a.next;
                a = self.get_anode(next_an)?;
                self.free_anode_in_memory(next_an)?;
            }
        } else {
            // File: free all data blocks across the anode chain.
            loop {
                if anode.blocknr != 0
                    && anode.blocknr != PFS3_ANODE_EOF_SENTINEL
                    && anode.clustersize > 0
                {
                    self.free_data_blocks(anode.blocknr, anode.clustersize)?;
                }
                if anode.next == 0 {
                    break;
                }
                let next_an = anode.next;
                anode = self.get_anode(next_an)?;
                self.free_anode_in_memory(next_an)?;
            }
        }
        // Remove the direntry from the parent dir.
        remove_direntry_from_dir(self, parent_anode, entry_anode)?;
        // Free the entry's own anode last.
        self.free_anode_in_memory(entry_anode)?;
        Ok(())
    }

    fn do_sync_metadata(&mut self) -> Result<(), FilesystemError> {
        // Flush in ascending HW-sector order so a partial failure mid-flush
        // produces a forward-progress disk state. We flush reserved first,
        // then data — reserved-area writes always describe data-area state,
        // so seeing fresh metadata + stale data is worse than seeing fresh
        // metadata + fresh data. (Best effort: a real Amiga write-cache
        // protocol would order this differently.)
        let mut reserved_keys: Vec<u32> = self.dirty_reserved.keys().copied().collect();
        reserved_keys.sort_unstable();
        for k in reserved_keys {
            let off = self.partition_offset + k as u64 * HW_SECTOR;
            let buf = self.dirty_reserved.remove(&k).unwrap();
            // Refresh the read cache so subsequent reads see the new bytes.
            self.cache.insert(k, buf.clone());
            self.reader.seek(SeekFrom::Start(off))?;
            self.reader.write_all(&buf)?;
        }
        let mut data_keys: Vec<u32> = self.dirty_data.keys().copied().collect();
        data_keys.sort_unstable();
        for k in data_keys {
            let off = self.partition_offset + k as u64 * HW_SECTOR;
            let buf = self.dirty_data.remove(&k).unwrap();
            self.reader.seek(SeekFrom::Start(off))?;
            self.reader.write_all(&buf)?;
        }
        self.reader.flush()?;
        Ok(())
    }
}

/// Verify no entry with `name` already exists in the directory at `parent`.
/// Returns `Err(AlreadyExists)` if a duplicate is found, otherwise `Ok`.
fn check_no_duplicate<R: Read + Seek + Send>(
    fs: &mut Pfs3Filesystem<R>,
    parent: &FileEntry,
    name: &str,
) -> Result<(), FilesystemError> {
    use super::filesystem::Filesystem;
    let kids = fs.list_directory(parent)?;
    if kids.iter().any(|c| c.name.eq_ignore_ascii_case(name)) {
        return Err(FilesystemError::AlreadyExists(name.to_string()));
    }
    Ok(())
}

/// Append a direntry to a directory. Walks the directory's anode chain
/// looking for a dirblock with enough room; if none has space, returns
/// `DiskFull` (chain growth is deferred — see Phase 6 follow-ups).
fn add_direntry_to_dir<R: Read + Seek>(
    fs: &mut Pfs3Filesystem<R>,
    dir_anodenr: u32,
    ttype: i8,
    new_anode: u32,
    fsize: u64,
    name: &str,
    comment: &str,
    protection: u8,
    dates: Option<(i32, i32, i32)>,
    extra_link: Option<u32>,
) -> Result<(), FilesystemError> {
    let entry = build_direntry(
        ttype, new_anode, fsize, name, comment, protection, dates, extra_link,
    )?;
    // Start the walk at the last anode we appended to for this dir, if
    // any. The forward-walk below validates space, so a stale hint just
    // means we scan a few extra blocks (matching old behavior). A
    // current hint turns each insert from O(chain length) into O(1).
    let start_anodenr = fs
        .dir_append_hint
        .get(&dir_anodenr)
        .copied()
        .unwrap_or(dir_anodenr);
    let mut a = fs.get_anode(start_anodenr)?;
    let mut anode_for_chain = start_anodenr;
    loop {
        if a.blocknr == 0 || a.blocknr == PFS3_ANODE_EOF_SENTINEL {
            break;
        }
        let db_sec = a.blocknr;
        // Find current free space in this dirblock and append if possible.
        let blk = fs.read_reserved_block(db_sec)?.to_vec();
        let mut end = 20usize;
        while end < blk.len() {
            let n = blk[end] as usize;
            if n == 0 {
                break;
            }
            end += n;
        }
        let avail = blk.len() - end - 1; // leave one byte for the trailing 0 terminator
        if avail >= entry.len() {
            fs.ensure_reserved_dirty(db_sec)?;
            let db = fs.dirty_reserved.get_mut(&db_sec).unwrap();
            db[end..end + entry.len()].copy_from_slice(&entry);
            // The next byte must be 0 (end marker). It already is, since
            // build_direntry padded to an even length and the trailing
            // bytes were 0 in the source buffer.
            fs.dir_append_hint.insert(dir_anodenr, anode_for_chain);
            return Ok(());
        }
        if a.next == 0 {
            break;
        }
        anode_for_chain = a.next;
        a = fs.get_anode(a.next)?;
    }
    // Chain is full — extend it by allocating a new dirblock + anode.
    let new_anode_for_chain = fs.alloc_anode_in_memory()?;
    let new_db_sec = fs.alloc_reserved_block()?;
    {
        let blk = fs.dirty_reserved.get_mut(&new_db_sec).unwrap();
        write_u16(blk, 0, ID_DIRBLOCK);
        write_u32(blk, 4, 1);
        write_u32(blk, 12, dir_anodenr);
        // parent = parent of this directory; we don't have it handy. Use 0
        // for additional dirblocks in the chain (they're not first blocks).
        write_u32(blk, 16, 0);
        blk[20..20 + entry.len()].copy_from_slice(&entry);
    }
    // Append the new chain anode and link from the previous tail.
    fs.save_anode_in_memory(
        new_anode_for_chain,
        Canode {
            clustersize: 1,
            blocknr: new_db_sec,
            next: 0,
        },
    )?;
    let mut tail = fs.get_anode(anode_for_chain)?;
    tail.next = new_anode_for_chain;
    fs.save_anode_in_memory(anode_for_chain, tail)?;
    fs.dir_append_hint.insert(dir_anodenr, new_anode_for_chain);
    Ok(())
}

/// Remove the direntry whose `anode` field matches `target_anode` from
/// the directory's chain. Returns an error if the entry isn't found.
fn remove_direntry_from_dir<R: Read + Seek>(
    fs: &mut Pfs3Filesystem<R>,
    dir_anodenr: u32,
    target_anode: u32,
) -> Result<(), FilesystemError> {
    let mut a = fs.get_anode(dir_anodenr)?;
    loop {
        if a.blocknr == 0 || a.blocknr == PFS3_ANODE_EOF_SENTINEL {
            break;
        }
        let db_sec = a.blocknr;
        let blk = fs.read_reserved_block(db_sec)?.to_vec();
        let mut off = 20usize;
        while off < blk.len() {
            let n = blk[off] as usize;
            if n == 0 {
                break;
            }
            if off + 6 <= blk.len() {
                let anr =
                    u32::from_be_bytes([blk[off + 2], blk[off + 3], blk[off + 4], blk[off + 5]]);
                if anr == target_anode {
                    fs.ensure_reserved_dirty(db_sec)?;
                    let db = fs.dirty_reserved.get_mut(&db_sec).unwrap();
                    // Shift everything past this entry up by `n` bytes,
                    // zero-fill the tail.
                    let start = off;
                    let after = off + n;
                    let end = db.len();
                    db.copy_within(after..end, start);
                    let new_tail = end - n;
                    for b in &mut db[new_tail..] {
                        *b = 0;
                    }
                    return Ok(());
                }
            }
            off += n;
        }
        if a.next == 0 {
            break;
        }
        a = fs.get_anode(a.next)?;
    }
    Err(parse_err(format!(
        "remove_direntry_from_dir: anode {} not found in dir chain",
        target_anode
    )))
}

/// Locate the direntry whose `anode` field matches `target_anode`
/// within a directory's dirblock chain. Returns `(dirblock_sec,
/// offset_within_block)` on hit or `None` if the entry isn't found.
/// Matches the scan in `remove_direntry_from_dir`; factored out so
/// other in-place mutators can reuse it.
fn find_direntry_in_dir<R: Read + Seek>(
    fs: &mut Pfs3Filesystem<R>,
    dir_anodenr: u32,
    target_anode: u32,
) -> Result<Option<(u32, usize)>, FilesystemError> {
    let mut a = fs.get_anode(dir_anodenr)?;
    loop {
        if a.blocknr == 0 || a.blocknr == PFS3_ANODE_EOF_SENTINEL {
            break;
        }
        let db_sec = a.blocknr;
        let blk = fs.read_reserved_block(db_sec)?.to_vec();
        let mut off = 20usize;
        while off < blk.len() {
            let n = blk[off] as usize;
            if n == 0 {
                break;
            }
            if off + 6 <= blk.len() {
                let anr =
                    u32::from_be_bytes([blk[off + 2], blk[off + 3], blk[off + 4], blk[off + 5]]);
                if anr == target_anode {
                    return Ok(Some((db_sec, off)));
                }
            }
            off += n;
        }
        if a.next == 0 {
            break;
        }
        a = fs.get_anode(a.next)?;
    }
    Ok(None)
}

/// Read the current `extrafields.link` value from a direntry at
/// `(db_sec, off)`. Returns 0 if the direntry has no extrafields tail
/// (no `flags` word) or no link bits set in its flags.
fn read_direntry_extra_link_at<R: Read + Seek>(
    fs: &mut Pfs3Filesystem<R>,
    db_sec: u32,
    off: usize,
) -> Result<u32, FilesystemError> {
    let blk = fs.read_reserved_block(db_sec)?.to_vec();
    let total = blk[off] as usize;
    let nlength = blk[off + 17] as usize;
    let cmt_pos = off + 18 + nlength;
    let comment_len = blk[cmt_pos] as usize;
    let aligned_end_local = (cmt_pos + 1 + comment_len + 1) & !1;
    let aligned_end = aligned_end_local - off; // size of header+name+comment+pad
    if total <= aligned_end {
        return Ok(0); // no extrafields tail
    }
    let mut flags = rd_u16(&blk, off + total - 2);
    let mut cursor = off + total - 2;
    let mut link_hi = 0u16;
    let mut link_lo = 0u16;
    for bit_idx in 0..11u32 {
        let present = (flags & 1) == 1;
        flags >>= 1;
        if present {
            if cursor < aligned_end_local + 2 {
                break;
            }
            cursor -= 2;
            let val = rd_u16(&blk, cursor);
            match bit_idx {
                0 => link_hi = val,
                1 => link_lo = val,
                _ => {}
            }
        }
    }
    Ok(((link_hi as u32) << 16) | (link_lo as u32))
}

/// Grow the direntry at `(db_sec, off)` by appending an
/// `extrafields.link` tail pointing at `new_link_anode`. The caller
/// must guarantee the direntry currently has NO extrafields tail
/// (i.e. it was created via our own `build_direntry` with
/// `extra_link=None`). Shifts following entries in the dirblock right
/// by the tail size (4 or 6 bytes). Errors if the dirblock has
/// insufficient trailing zero space — chain extension for that case
/// is a future enhancement (rare in practice given typical dirblock
/// occupancy).
fn append_link_extrafield_to_direntry<R: Read + Write + Seek>(
    fs: &mut Pfs3Filesystem<R>,
    db_sec: u32,
    off: usize,
    new_link_anode: u32,
) -> Result<(), FilesystemError> {
    let blk = fs.read_reserved_block(db_sec)?.to_vec();
    let total = blk[off] as usize;
    let nlength = blk[off + 17] as usize;
    let cmt_pos = off + 18 + nlength;
    let comment_len = blk[cmt_pos] as usize;
    let aligned_end_off = ((cmt_pos + 1 + comment_len + 1) & !1) - off;
    if aligned_end_off != total {
        return Err(parse_err(format!(
            "append_link_extrafield: direntry at db {db_sec} off {off} already has \
             extrafields tail ({} bytes); v1 patcher only handles bare entries",
            total - aligned_end_off
        )));
    }
    let link_hi = ((new_link_anode >> 16) & 0xFFFF) as u16;
    let link_lo = (new_link_anode & 0xFFFF) as u16;
    let mut tail_bytes = 2usize; // flags word always present
    if link_hi != 0 {
        tail_bytes += 2;
    }
    if link_lo != 0 {
        tail_bytes += 2;
    }
    let new_total = total + tail_bytes;
    if new_total > 255 {
        return Err(parse_err(format!(
            "append_link_extrafield: new direntry size {new_total} exceeds 255 bytes"
        )));
    }

    // Find current end-of-entries: the first 0 byte at or past
    // `off + total`. The dirblock keeps at least one trailing zero
    // as the entries terminator.
    let next_off = off + total;
    let mut scan = next_off;
    while scan < blk.len() {
        if blk[scan] == 0 {
            break;
        }
        let n = blk[scan] as usize;
        if n == 0 || scan + n > blk.len() {
            break;
        }
        scan += n;
    }
    let end_of_entries = scan;
    // Need `tail_bytes` more bytes of room. The zero terminator must
    // survive, so reserve 1 trailing zero.
    let avail = blk.len() - end_of_entries - 1;
    if avail < tail_bytes {
        return Err(parse_err(format!(
            "append_link_extrafield: dirblock at sec {db_sec} full ({} byte tail needed, \
             {} avail); chain extension for this case is not implemented",
            tail_bytes, avail
        )));
    }

    fs.ensure_reserved_dirty(db_sec)?;
    let db = fs.dirty_reserved.get_mut(&db_sec).unwrap();

    // Shift following entries (and the trailing 0 marker) right by
    // `tail_bytes` to make room for the new extrafields tail.
    let shift_len = end_of_entries - next_off;
    if shift_len > 0 {
        db.copy_within(next_off..next_off + shift_len, next_off + tail_bytes);
    }
    // Zero the freshly-vacated tail region (the old start of the
    // next direntry, now duplicated at the shifted position).
    for i in next_off..next_off + tail_bytes {
        db[i] = 0;
    }
    // Pack: walking back from the flags word, write link_hi then
    // link_lo (matching the back-walker convention in parse_direntry).
    let flags_pos = next_off + tail_bytes - 2;
    let mut flags: u16 = 0;
    let mut cursor = flags_pos;
    if link_hi != 0 {
        cursor -= 2;
        write_u16(db, cursor, link_hi);
        flags |= 0b01;
    }
    if link_lo != 0 {
        cursor -= 2;
        write_u16(db, cursor, link_lo);
        flags |= 0b10;
    }
    write_u16(db, flags_pos, flags);
    db[off] = new_total as u8;
    Ok(())
}

/// Walk the linknode chain via `anode.next` starting at `first_anode`
/// and set the tail's `next` to `new_link_anode`. Returns the tail's
/// own anode for diagnostics. Errors if the chain is cyclic (defensive
/// — pfs3aio doesn't emit cycles but on-disk corruption shouldn't loop
/// us forever).
fn append_to_linknode_chain<R: Read + Write + Seek>(
    fs: &mut Pfs3Filesystem<R>,
    first_anode: u32,
    new_link_anode: u32,
) -> Result<u32, FilesystemError> {
    let mut cur = first_anode;
    let mut seen = std::collections::HashSet::new();
    loop {
        if !seen.insert(cur) {
            return Err(parse_err(format!(
                "append_to_linknode_chain: cycle detected at anode {cur}"
            )));
        }
        let mut node = fs.get_anode(cur)?;
        if node.next == 0 {
            node.next = new_link_anode;
            fs.save_anode_in_memory(cur, node)?;
            return Ok(cur);
        }
        cur = node.next;
    }
}

impl<R: Read + Write + Seek + Send> super::filesystem::EditableFilesystem for Pfs3Filesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
        options: &super::filesystem::CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let comment = options.amiga_comment.as_deref().unwrap_or("");
        let protection = options.amiga_protection.unwrap_or(0) as u8;
        let dates = options.amiga_dates;
        let snap = self.snapshot();
        match self.do_create_file(parent, name, data, data_len, comment, protection, dates) {
            Ok(fe) => Ok(fe),
            Err(e) => {
                self.restore_snapshot(snap);
                Err(e)
            }
        }
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &super::filesystem::CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let comment = options.amiga_comment.as_deref().unwrap_or("");
        let protection = options.amiga_protection.unwrap_or(0) as u8;
        let dates = options.amiga_dates;
        let snap = self.snapshot();
        match self.do_create_directory(parent, name, comment, protection, dates) {
            Ok(fe) => Ok(fe),
            Err(e) => {
                self.restore_snapshot(snap);
                Err(e)
            }
        }
    }

    fn create_symlink(
        &mut self,
        parent: &FileEntry,
        name: &str,
        target: &str,
        options: &super::filesystem::CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let comment = options.amiga_comment.as_deref().unwrap_or("");
        let protection = options.amiga_protection.unwrap_or(0) as u8;
        let dates = options.amiga_dates;
        let snap = self.snapshot();
        match self.do_create_symlink(parent, name, target, comment, protection, dates) {
            Ok(fe) => Ok(fe),
            Err(e) => {
                self.restore_snapshot(snap);
                Err(e)
            }
        }
    }

    fn create_hardlink(
        &mut self,
        parent: &FileEntry,
        name: &str,
        target: &FileEntry,
        options: &super::filesystem::CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let comment = options.amiga_comment.as_deref().unwrap_or("");
        let protection = options.amiga_protection.unwrap_or(0) as u8;
        let dates = options.amiga_dates;
        let snap = self.snapshot();
        match self.do_create_hardlink(parent, name, target, comment, protection, dates) {
            Ok(fe) => Ok(fe),
            Err(e) => {
                self.restore_snapshot(snap);
                Err(e)
            }
        }
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let snap = self.snapshot();
        match self.do_delete_entry(parent, entry) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.restore_snapshot(snap);
                Err(e)
            }
        }
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.do_sync_metadata()
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.root.blocks_free as u64 * HW_SECTOR)
    }
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
// We set `MODE_HARDDISK | MODE_SPLITTED_ANODES | MODE_EXTENSION |
// MODE_DATESTAMP` in `options`. DATESTAMP is required so tooling
// (pfs3aio, real Amigas) honors the per-direntry dates we write — the
// date bytes at direntry offsets 10..16 always exist regardless of the
// mode bit, but without it set they're ignored. DIR_EXTENSION / LONGFN
// add extra direntry tail fields and remain off to keep the formatter +
// dirblock writer simple; the reader handles minimal mode fine.

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
///
/// `resblksize` and `numreserved` scale with the target size:
/// - Volumes ≤ ~100 MB use `resblksize = 1024` (matching the existing
///   test layouts at 8192 sectors = 4 MB).
/// - Larger volumes use `resblksize = 4096` so a single MI block can
///   address every BM block via the rootblock's 5-slot `bitmapindex`
///   without needing multi-MI chaining or supermode.
/// - `numreserved` is computed from the actual BM-block count needed,
///   floored at 32 (the original budget) and with 8 blocks of headroom.
pub fn create_blank_pfs3(total_sectors: u32, name: &str) -> Result<Vec<u8>, FilesystemError> {
    // Bump resblksize when the volume is large enough that BM blocks
    // exceed `indexperblock` at the small size. 1024-byte reserved
    // blocks give indexperblock=253; at 8096 sectors per BM that
    // covers volumes up to ~1 GB in a single MI. Beyond that, jump
    // to 4096-byte reserved blocks (indexperblock=1021, 32672 sectors
    // per BM, single MI handles ~33 GB volumes).
    const RESBLK_BUMP_THRESHOLD_SECTORS: u32 = 2_000_000; // ~1 GB
    let resblksize: u32 = if total_sectors > RESBLK_BUMP_THRESHOLD_SECTORS {
        4096
    } else {
        PFS3_BLANK_RESBLKSIZE
    };
    let rscluster = resblksize / HW_SECTOR_U32;
    let first_reserved = PFS3_BLANK_FIRST_RESERVED;
    let longsperbmb = (resblksize - 12) / 4;
    let sectors_per_bmb = longsperbmb * 32;
    let indexperblock = (resblksize - 12) / 4;
    let anodesperblock = (resblksize - 16) / 12;

    // Compute numreserved from estimated need. The dependency on
    // data_sectors (= total - last_reserved - 1) is weak —
    // last_reserved grows linearly with numreserved but reserved
    // overhead lands well under 1% of total_sectors at the budgets
    // below — so a one-shot upper-bound estimate is safe.
    //
    // Budget categories:
    //   6 fixed: root, ext, mi, ib0, ab0, rootdir
    //   no_bmb:  one BM block per `sectors_per_bmb` data sectors
    //   no_ab:   estimated anodeblocks for the file/dir population.
    //            Worst-case Amiga workload: ~1 entry per 4 KB of data
    //            (real images run 5-10 KB/entry; 4 KB is conservative).
    //   no_ib_extra: indexblocks beyond the first one (added by
    //                `alloc_new_indexblock` when an AB chain crosses
    //                an IB boundary).
    //   headroom: 16 blocks of allocator slack.
    let approx_no_bmb =
        ((total_sectors as u64) + sectors_per_bmb as u64 - 1) / sectors_per_bmb as u64;
    let est_anodes = (total_sectors as u64).div_ceil(8); // sectors / 8 = ~1 per 4 KB
    let approx_no_ab = est_anodes.div_ceil(anodesperblock as u64);
    let approx_no_ib_extra = approx_no_ab
        .div_ceil(indexperblock as u64)
        .saturating_sub(1);
    let required_reserved = 6u64 + approx_no_bmb + approx_no_ab + approx_no_ib_extra + 16;
    // small_indexblocks has 99 slots; if `approx_no_ib_extra + 1 > 99`
    // we'd need supermode (not yet implemented in the writer). Cap
    // accordingly and error out below if the math demands more.
    let numreserved = required_reserved.max(PFS3_BLANK_NUMRESERVED as u64) as u32;
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
    let no_bmb = (data_sectors + sectors_per_bmb - 1) / sectors_per_bmb;
    if no_bmb > indexperblock {
        return Err(parse_err(format!(
            "PFS3 formatter: disk too large for single MI block ({} BM > {} index slots); \
             multi-MI / supermode formatting not yet implemented",
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
    let options =
        MODE_HARDDISK | MODE_SPLITTED_ANODES | MODE_EXTENSION | MODE_DATESTAMP | MODE_LONGFN;
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
                                        // bitmap[] starts at +12: bit i (MSB-first within each u32) = reserved
                                        // block i is FREE when 1, ALLOCATED when 0 (AmigaDOS convention).
                                        // We mark blocks 0..used_rb allocated and the rest free.
    let words = ((numreserved + 31) / 32) as usize;
    for w in 0..words {
        let mut word: u32 = 0;
        for bit in 0..32u32 {
            let res_idx = w as u32 * 32 + bit;
            if res_idx >= numreserved {
                break;
            }
            if res_idx >= used_rb {
                word |= 1u32 << (31 - bit);
            }
        }
        write_u32(&mut img, bm_res + 12 + w * 4, word);
    }

    // === Rootblock extension at RB 1 ===
    let ext = rb_to_offset(rb_idx_ext);
    write_u16(&mut img, ext, ID_EXTENSIONBLOCK);
    write_u32(&mut img, ext + 8, 1); // datestamp
                                     // fnsize at +56: long-filename ceiling. Standard PFS3 is 32
                                     // (=> 31 chars + NUL); we use the LONGFN max of 107 to match
                                     // real-world AmigaVision-style volumes whose direntries carry
                                     // names > 30 bytes. `build_direntry` enforces the same cap.
    write_u16(&mut img, ext + 56, 107);

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

// === PFS3 in-place resize ===================================================
//
// Mirrors the pattern used by AFFS / HFS / HFS+ / FAT / NTFS / SFS:
// `resize_pfs3_in_place` adjusts the rootblock's `disksize` (in HW sectors
// of 512 bytes) and `blocks_free` fields, and for grows marks the newly
// visible sectors as free in the data bitmap. Shrink path refuses to
// throw away allocated data; grow path refuses to extend past the
// existing bitmap-block capacity (Phase R.3 territory).
//
// No backup root exists in PFS3, so the commit point is a single
// rootblock write at HW sector `ROOTBLOCK_SECTOR` (2).
//
// The function is a no-op when the volume at `partition_offset` isn't
// a PFS3 volume (boot magic doesn't match), so it's safe to call from
// the dispatch in `resize_filesystem_for` regardless of FS type.

const PFS3_RB_DISKSIZE_OFFSET: usize = 84;
const PFS3_RB_BLOCKS_FREE_OFFSET: usize = 68;
const PFS3_RB_OPTIONS_OFFSET: usize = 4;

/// Resize a PFS3 volume at `partition_offset` to `new_size_bytes`.
///
/// - **Shrink**: rejects if any allocated user-data sector lies at or
///   beyond `new_size_bytes / 512`; otherwise stamps the new
///   `disksize` and adjusts `blocks_free` for the lost bits.
/// - **Grow**: marks the new sectors free in the data bitmap, bumps
///   `blocks_free`, and stamps the new `disksize`. Refuses to grow
///   past the capacity of the existing BM-block chain (adding BM
///   blocks needs reserved-block allocation, which we defer).
/// - **No-op** when the volume isn't PFS3, or when `new_size_bytes`
///   rounds to the existing block count.
pub fn resize_pfs3_in_place(
    device: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_size_bytes: u64,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    // --- 1. Probe boot magic at HW sector 0. ---
    device.seek(SeekFrom::Start(partition_offset))?;
    let mut boot = [0u8; 512];
    if device.read_exact(&mut boot).is_err() {
        return Ok(());
    }
    let mag = &boot[0..4];
    let valid = mag == b"PFS\x01"
        || mag == b"PDS\x01"
        || mag == b"muAF"
        || mag == b"muPF"
        || mag == b"AFS\x01"
        || mag == b"PFS\x02";
    if !valid {
        return Ok(());
    }

    // --- 2. Read rootblock (first 512 bytes of the rootblock cluster). ---
    let rb_offset = partition_offset + ROOTBLOCK_SECTOR as u64 * HW_SECTOR;
    let mut rb_buf = [0u8; 512];
    device.seek(SeekFrom::Start(rb_offset))?;
    device.read_exact(&mut rb_buf)?;
    let root = match Pfs3RootBlock::parse(&rb_buf) {
        Ok(r) => r,
        Err(e) => {
            log(&format!(
                "PFS3 resize: rootblock parse failed: {e}; skipping"
            ));
            return Ok(());
        }
    };

    let reserved_blksize = root.reserved_blksize as u32;
    if reserved_blksize == 0 || reserved_blksize % 512 != 0 {
        log(&format!(
            "PFS3 resize: invalid reserved_blksize {}, skipping",
            reserved_blksize
        ));
        return Ok(());
    }

    let new_total_u64 = new_size_bytes / HW_SECTOR;
    if new_total_u64 == 0 || new_total_u64 > u32::MAX as u64 {
        anyhow::bail!(
            "PFS3 resize: target {} bytes resolves to {} sectors, out of u32 range",
            new_size_bytes,
            new_total_u64,
        );
    }
    let new_total = new_total_u64 as u32;

    // `disksize == 0` on older volumes (MODE_SIZEFIELD off) means
    // "fills the partition"; in that case derive the effective old
    // size from the partition extent we just read from. We can still
    // grow / shrink the FS by stamping disksize during this resize.
    let old_total = if root.disksize > 0 {
        root.disksize
    } else {
        // Compute from EOF — but `partition_offset` might be 0 inside
        // a larger container, so use the explicit new_size as a
        // fallback. We can't trust EOF here.
        log("PFS3 resize: rootblock disksize==0 (pre-SIZEFIELD volume); will stamp disksize");
        // Treat the current rootblock as authoritative for "old" by
        // reading partition end via seek.
        let end = device.seek(SeekFrom::End(0))?;
        let bytes = end.saturating_sub(partition_offset);
        (bytes / HW_SECTOR).min(u32::MAX as u64) as u32
    };

    if new_total == old_total {
        log("PFS3 resize: no size change, skipping");
        return Ok(());
    }

    // Reserved area must remain inside the volume.
    let min_total = root.last_reserved + 2;
    if new_total < min_total {
        anyhow::bail!(
            "PFS3 resize: target {} sectors below the metadata-region minimum {}",
            new_total,
            min_total,
        );
    }

    let bitmap_start = root.last_reserved + 1;
    let longsperbmb = (reserved_blksize / 4) - 3; // u32 words per BM block
    let sectors_per_bmb = longsperbmb * 32;

    if new_total > old_total {
        resize_pfs3_grow(
            device,
            partition_offset,
            &root,
            old_total,
            new_total,
            bitmap_start,
            sectors_per_bmb,
            log,
        )
    } else {
        resize_pfs3_shrink(
            device,
            partition_offset,
            &root,
            old_total,
            new_total,
            bitmap_start,
            sectors_per_bmb,
            log,
        )
    }
}

/// Shrink path: refuse if any sector >= new_total is allocated,
/// otherwise update `disksize` + `blocks_free` and re-stamp the
/// rootblock.
fn resize_pfs3_shrink(
    device: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    root: &Pfs3RootBlock,
    old_total: u32,
    new_total: u32,
    bitmap_start: u32,
    sectors_per_bmb: u32,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    let reserved_blksize = root.reserved_blksize as u32;
    // Walk the data bitmap and find the highest allocated sector.
    let (highest, freed_inside_old_visible) = pfs3_highest_alloc_and_free_count(
        device,
        partition_offset,
        root,
        bitmap_start,
        sectors_per_bmb,
        new_total,
        old_total,
    )?;
    if let Some(h) = highest {
        if h >= new_total {
            anyhow::bail!(
                "PFS3 resize: highest allocated user sector {} >= target {} sectors, \
                 cannot shrink to {} bytes. Shrink to at least {} sectors (~{} bytes), \
                 or pick a larger target.",
                h,
                new_total,
                new_total as u64 * HW_SECTOR,
                h + 1,
                (h as u64 + 1) * HW_SECTOR,
            );
        }
    }

    // blocks_free originally counted free sectors in the user-data area.
    // We're dropping the range [new_total, old_total) from view; subtract
    // the free bits that lived in that range.
    let new_blocks_free = root.blocks_free.saturating_sub(freed_inside_old_visible);

    let mut new_root = root.clone();
    new_root.disksize = new_total;
    new_root.blocks_free = new_blocks_free;
    pfs3_write_rootblock(device, partition_offset, &new_root, reserved_blksize)?;

    log(&format!(
        "PFS3 resize: shrunk from {} to {} HW sectors ({} -> {} bytes), blocks_free {} -> {}",
        old_total,
        new_total,
        old_total as u64 * HW_SECTOR,
        new_total as u64 * HW_SECTOR,
        root.blocks_free,
        new_blocks_free,
    ));
    Ok(())
}

/// Grow path: mark the new sectors free in the data bitmap, update
/// `disksize` + `blocks_free`, re-stamp the rootblock.
#[allow(clippy::too_many_arguments)]
fn resize_pfs3_grow(
    device: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    root: &Pfs3RootBlock,
    old_total: u32,
    new_total: u32,
    bitmap_start: u32,
    sectors_per_bmb: u32,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    let reserved_blksize = root.reserved_blksize as u32;
    if new_total <= bitmap_start {
        anyhow::bail!(
            "PFS3 resize: target {} sectors leaves no room for data area",
            new_total,
        );
    }
    // Capacity check: count how many BM blocks the existing
    // bitmapindex chain references. Each BM covers `sectors_per_bmb`
    // sectors of the data area.
    let existing_bms = pfs3_count_bm_blocks(device, partition_offset, root)?;
    let bitmap_capacity_sectors = existing_bms.saturating_mul(sectors_per_bmb) as u64;
    let new_data_sectors = (new_total - bitmap_start) as u64;
    if new_data_sectors > bitmap_capacity_sectors {
        anyhow::bail!(
            "PFS3 resize: growing from {} to {} sectors needs {} data sectors \
             but the existing {} BM block(s) cover only {} sectors. Allocating \
             more BM blocks requires reserved-block headroom and is not \
             implemented. Maximum grow without extending the bitmap chain: \
             {} sectors (~{} bytes).",
            old_total,
            new_total,
            new_data_sectors,
            existing_bms,
            bitmap_capacity_sectors,
            bitmap_start as u64 + bitmap_capacity_sectors,
            (bitmap_start as u64 + bitmap_capacity_sectors) * HW_SECTOR,
        );
    }

    // Mark [old_total, new_total) free in the bitmap. Old volumes with
    // disksize==0 have already had the bitmap fully populated by the
    // formatter, so this might be a no-op for them; that's fine — the
    // operation is idempotent.
    let first_to_free = old_total.max(bitmap_start);
    if first_to_free < new_total {
        pfs3_set_data_bitmap_range(
            device,
            partition_offset,
            root,
            bitmap_start,
            sectors_per_bmb,
            first_to_free,
            new_total - first_to_free,
            /*set_free=*/ true,
        )?;
    }

    let added = new_total - old_total;
    let new_blocks_free = root.blocks_free.saturating_add(added);

    let mut new_root = root.clone();
    new_root.disksize = new_total;
    new_root.blocks_free = new_blocks_free;
    pfs3_write_rootblock(device, partition_offset, &new_root, reserved_blksize)?;

    log(&format!(
        "PFS3 resize: grew from {} to {} HW sectors ({} -> {} bytes), blocks_free {} -> {}",
        old_total,
        new_total,
        old_total as u64 * HW_SECTOR,
        new_total as u64 * HW_SECTOR,
        root.blocks_free,
        new_blocks_free,
    ));
    Ok(())
}

/// Walk every BM block referenced from the rootblock's bitmapindex.
/// Returns:
/// - `highest`: highest allocated sector across the bitmap, ignoring
///   sectors >= `old_total` (which may carry stale "allocated" bits
///   left over from format-time fill).
/// - `freed_inside_dropped_range`: count of bits that were *free* (=1)
///   in the range `[new_total, old_total)`. We subtract these from
///   `blocks_free` when shrinking.
#[allow(clippy::too_many_arguments)]
fn pfs3_highest_alloc_and_free_count(
    device: &mut (impl Read + Seek),
    partition_offset: u64,
    root: &Pfs3RootBlock,
    bitmap_start: u32,
    sectors_per_bmb: u32,
    new_total: u32,
    old_total: u32,
) -> anyhow::Result<(Option<u32>, u32)> {
    let reserved_blksize = root.reserved_blksize as usize;
    let longsperbmb = ((reserved_blksize / 4) - 3) as u32;
    let mut highest: Option<u32> = None;
    let mut freed_inside_dropped_range: u32 = 0;
    let mut buf = vec![0u8; reserved_blksize];

    for (mi_seq, &mi_blk) in root.bitmapindex.iter().enumerate() {
        if mi_blk == 0 {
            continue;
        }
        // Read MI block.
        device.seek(SeekFrom::Start(
            partition_offset + mi_blk as u64 * HW_SECTOR,
        ))?;
        device.read_exact(&mut buf)?;
        if rd_u16(&buf, 0) != ID_BITMAPINDEXBLOCK {
            anyhow::bail!(
                "PFS3 resize: bitmapindex block {} has bad id at MI seq {}",
                mi_blk,
                mi_seq,
            );
        }
        let mi_bytes = buf.clone();
        let indexperblock = ((reserved_blksize - 12) / 4) as u32;
        for i in 0..indexperblock {
            let off = 12 + i as usize * 4;
            if off + 4 > mi_bytes.len() {
                break;
            }
            let bm_blk = rd_u32(&mi_bytes, off);
            if bm_blk == 0 {
                continue;
            }
            let bm_seq = mi_seq as u32 * indexperblock + i;
            // Read BM block.
            device.seek(SeekFrom::Start(
                partition_offset + bm_blk as u64 * HW_SECTOR,
            ))?;
            device.read_exact(&mut buf)?;
            if rd_u16(&buf, 0) != ID_BITMAPBLOCK {
                anyhow::bail!("PFS3 resize: bitmap block {} has bad id", bm_blk);
            }
            let base_sector = bitmap_start as u64 + bm_seq as u64 * sectors_per_bmb as u64;
            for w in 0..longsperbmb {
                let o = 12 + w as usize * 4;
                if o + 4 > buf.len() {
                    break;
                }
                let word = rd_u32(&buf, o);
                if word == 0xFFFF_FFFF {
                    // All free — only matters for the dropped-range count.
                    let word_first = base_sector + w as u64 * 32;
                    let word_last = word_first + 31;
                    if word_first >= new_total as u64 && word_last < old_total as u64 {
                        freed_inside_dropped_range += 32;
                        continue;
                    }
                }
                for i in 0..32u32 {
                    let sec = base_sector + (w as u64) * 32 + i as u64;
                    if sec >= old_total as u64 {
                        break;
                    }
                    let is_free = (word >> (31 - i)) & 1 == 1;
                    if !is_free {
                        // Allocated.
                        if highest.map_or(true, |h| sec as u32 > h) {
                            highest = Some(sec as u32);
                        }
                    } else if sec >= new_total as u64 && sec < old_total as u64 {
                        freed_inside_dropped_range += 1;
                    }
                }
            }
        }
    }
    Ok((highest, freed_inside_dropped_range))
}

/// Count the number of non-zero BM-block pointers in the rootblock's
/// bitmapindex array (i.e. how many BM blocks currently exist).
fn pfs3_count_bm_blocks(
    device: &mut (impl Read + Seek),
    partition_offset: u64,
    root: &Pfs3RootBlock,
) -> anyhow::Result<u32> {
    let reserved_blksize = root.reserved_blksize as usize;
    let indexperblock = ((reserved_blksize - 12) / 4) as u32;
    let mut buf = vec![0u8; reserved_blksize];
    let mut total = 0u32;
    for &mi_blk in &root.bitmapindex {
        if mi_blk == 0 {
            continue;
        }
        device.seek(SeekFrom::Start(
            partition_offset + mi_blk as u64 * HW_SECTOR,
        ))?;
        device.read_exact(&mut buf)?;
        if rd_u16(&buf, 0) != ID_BITMAPINDEXBLOCK {
            continue;
        }
        for i in 0..indexperblock {
            let off = 12 + i as usize * 4;
            if off + 4 > buf.len() {
                break;
            }
            if rd_u32(&buf, off) != 0 {
                total += 1;
            }
        }
    }
    Ok(total)
}

/// Toggle a contiguous range of bits in the data bitmap. The
/// implementation reads each affected BM block exactly once, applies
/// all bit flips, then writes the block back.
#[allow(clippy::too_many_arguments)]
fn pfs3_set_data_bitmap_range(
    device: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    root: &Pfs3RootBlock,
    bitmap_start: u32,
    sectors_per_bmb: u32,
    first_sector: u32,
    count: u32,
    set_free: bool,
) -> anyhow::Result<()> {
    if count == 0 {
        return Ok(());
    }
    let reserved_blksize = root.reserved_blksize as usize;
    let indexperblock = ((reserved_blksize - 12) / 4) as u32;
    // Translate sector range to BM-block-relative range.
    let first_local = first_sector
        .checked_sub(bitmap_start)
        .ok_or_else(|| anyhow::anyhow!("PFS3 resize: first_sector below bitmap_start"))?;
    let last_local = first_local
        .checked_add(count - 1)
        .ok_or_else(|| anyhow::anyhow!("PFS3 resize: bitmap range overflow"))?;
    let first_bm_seq = first_local / sectors_per_bmb;
    let last_bm_seq = last_local / sectors_per_bmb;

    let mut mi_buf = vec![0u8; reserved_blksize];
    let mut bm_buf = vec![0u8; reserved_blksize];

    for bm_seq in first_bm_seq..=last_bm_seq {
        let mi_idx = (bm_seq / indexperblock) as usize;
        let mi_offset = (bm_seq % indexperblock) as usize;
        let mi_blk = *root.bitmapindex.get(mi_idx).ok_or_else(|| {
            anyhow::anyhow!(
                "PFS3 resize: bm_seq {} maps to MI index {} out of range",
                bm_seq,
                mi_idx,
            )
        })?;
        if mi_blk == 0 {
            anyhow::bail!(
                "PFS3 resize: bm_seq {} references zero MI pointer at index {}",
                bm_seq,
                mi_idx,
            );
        }
        device.seek(SeekFrom::Start(
            partition_offset + mi_blk as u64 * HW_SECTOR,
        ))?;
        device.read_exact(&mut mi_buf)?;
        if rd_u16(&mi_buf, 0) != ID_BITMAPINDEXBLOCK {
            anyhow::bail!("PFS3 resize: MI block {} bad id", mi_blk);
        }
        let bm_blk = rd_u32(&mi_buf, 12 + mi_offset * 4);
        if bm_blk == 0 {
            anyhow::bail!(
                "PFS3 resize: bm_seq {} has zero BM pointer at MI {} slot {}",
                bm_seq,
                mi_blk,
                mi_offset,
            );
        }
        let bm_off = partition_offset + bm_blk as u64 * HW_SECTOR;
        device.seek(SeekFrom::Start(bm_off))?;
        device.read_exact(&mut bm_buf)?;
        if rd_u16(&bm_buf, 0) != ID_BITMAPBLOCK {
            anyhow::bail!("PFS3 resize: BM block {} bad id", bm_blk);
        }
        let range_start_local = bm_seq * sectors_per_bmb;
        let range_end_local = range_start_local + sectors_per_bmb;
        let lo = first_local.max(range_start_local);
        let hi = (last_local + 1).min(range_end_local);
        for local in lo..hi {
            let within = local - range_start_local;
            let word_idx = within / 32;
            let bit_pos = within % 32;
            let o = 12 + word_idx as usize * 4;
            let mut word = rd_u32(&bm_buf, o);
            let mask = 1u32 << (31 - bit_pos);
            if set_free {
                word |= mask;
            } else {
                word &= !mask;
            }
            bm_buf[o..o + 4].copy_from_slice(&word.to_be_bytes());
        }
        device.seek(SeekFrom::Start(bm_off))?;
        device.write_all(&bm_buf)?;
    }
    Ok(())
}

/// Write the updated rootblock back to the first 512 bytes of the
/// rootblock cluster, then flush. Sets `MODE_SIZEFIELD` so future
/// readers honour our `disksize` field even on volumes that didn't
/// originally have it set.
fn pfs3_write_rootblock(
    device: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_root: &Pfs3RootBlock,
    _reserved_blksize: u32,
) -> anyhow::Result<()> {
    let rb_offset = partition_offset + ROOTBLOCK_SECTOR as u64 * HW_SECTOR;
    let mut rb_buf = [0u8; 512];
    device.seek(SeekFrom::Start(rb_offset))?;
    device.read_exact(&mut rb_buf)?;
    // Update the in-memory bytes directly — `write_into` rewrites a
    // handful of fields but leaves the rest of the block untouched,
    // which preserves anything we don't model (creation timestamps,
    // protection bits, index-table heads, etc).
    rb_buf[PFS3_RB_DISKSIZE_OFFSET..PFS3_RB_DISKSIZE_OFFSET + 4]
        .copy_from_slice(&new_root.disksize.to_be_bytes());
    rb_buf[PFS3_RB_BLOCKS_FREE_OFFSET..PFS3_RB_BLOCKS_FREE_OFFSET + 4]
        .copy_from_slice(&new_root.blocks_free.to_be_bytes());
    // Set MODE_SIZEFIELD so future opens trust the `disksize` we just
    // stamped. Leave every other option bit untouched.
    let mut options =
        u32::from_be_bytes(rb_buf[PFS3_RB_OPTIONS_OFFSET..PFS3_RB_OPTIONS_OFFSET + 4].try_into()?);
    options |= MODE_SIZEFIELD;
    rb_buf[PFS3_RB_OPTIONS_OFFSET..PFS3_RB_OPTIONS_OFFSET + 4]
        .copy_from_slice(&options.to_be_bytes());

    device.seek(SeekFrom::Start(rb_offset))?;
    device.write_all(&rb_buf)?;
    device.flush()?;
    Ok(())
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
        // used_size = total - blocks_free*512. blocks_free is the
        // data-area sector count; reserved area + boot are excluded.
        // Derive the expected from `last_reserved` rather than
        // hardcoding (the formatter's reserve estimate scales with
        // size + anode budget — a hardcoded number would lock the
        // test against future budget tweaks).
        let used = fs.used_size();
        let expected_used = (fs.root.last_reserved as u64 + 1) * 512;
        assert_eq!(used, expected_used);
    }

    #[test]
    fn blank_volume_compact_reader_walks_bitmap() {
        let img = create_blank_pfs3(8192, "T").expect("format");
        // Open once to read the actual reserved-area size from the
        // rootblock, since the formatter scales it with the disk size
        // + anode budget.
        let last_reserved = {
            let cur = std::io::Cursor::new(img.clone());
            Pfs3Filesystem::open(cur, 0)
                .expect("open for reserved-area probe")
                .root
                .last_reserved
        };
        let cur = std::io::Cursor::new(img);
        let (_compact, info) = CompactPfs3Reader::new(cur, 0).expect("compact");
        // For a blank disk, every data-area bit is "free", so the compact
        // reader sees ONLY the reserved area as allocated.
        assert_eq!(info.original_size, 8192 * 512);
        assert_eq!(info.data_size, (last_reserved as u64 + 1) * 512);
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
        assert_eq!(de.extrafields_link, 0);
    }

    /// Verify the extrafields back-walker extracts `link` from a packed
    /// trailer. Layout per pfs3aio: u16 fields packed at the tail,
    /// terminated by a u16 `flags` bitmap (LSB-first; bit 0 = link_hi,
    /// bit 1 = link_lo).
    #[test]
    fn parse_direntry_extracts_extrafields_link() {
        // Direntry total = 24 bytes:
        //   off  0: next=24
        //   off  1: type=-4 (ST_LINKFILE)
        //   off  2..6: anode=100 (BE)
        //   off  6..10: fsize_lo=0
        //   off 10..16: dates=0
        //   off 16: protection=0
        //   off 17: nlength=1
        //   off 18: 'X'
        //   off 19: comment_len=0
        //   off 20: pad to even -> off 20 is already even after 19+1=20
        //   extrafields region: off 20..22 = link_lo=0xCAFE, off 22..24 = flags=0b10 (bit1 set)
        let mut e = vec![0u8; 24];
        e[0] = 24;
        e[1] = (-4i8) as u8;
        e[2..6].copy_from_slice(&100u32.to_be_bytes());
        e[17] = 1;
        e[18] = b'X';
        e[19] = 0;
        e[20..22].copy_from_slice(&0xCAFEu16.to_be_bytes()); // link_lo
        e[22..24].copy_from_slice(&0b10u16.to_be_bytes()); // flags: bit 1 set
        let de = parse_direntry(&e, 0, false).expect("parse");
        assert_eq!(de.ttype, -4);
        assert_eq!(de.extrafields_link, 0xCAFE);
    }

    /// End-to-end Phase 6 smoke test: format a blank volume, create a
    /// directory and a file inside it, sync, reopen, and verify the
    /// content survives.
    #[test]
    fn write_round_trip_create_dir_and_file() {
        use super::super::filesystem::{
            CreateDirectoryOptions, CreateFileOptions, EditableFilesystem,
        };
        let img = create_blank_pfs3(8192, "TestVol").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = Pfs3Filesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");

        let dopts = CreateDirectoryOptions::default();
        let new_dir = fs
            .create_directory(&root, "MyDir", &dopts)
            .expect("create_directory");
        assert_eq!(new_dir.name, "MyDir");
        assert!(new_dir.is_directory());

        let payload: &[u8] = b"hello PFS3 write\n";
        let mut cur_payload = std::io::Cursor::new(payload);
        let fopts = CreateFileOptions::default();
        let new_file = fs
            .create_file(
                &root,
                "readme.txt",
                &mut cur_payload,
                payload.len() as u64,
                &fopts,
            )
            .expect("create_file");
        assert_eq!(new_file.size, payload.len() as u64);

        EditableFilesystem::sync_metadata(&mut fs).expect("sync");

        // Reopen the (mutated) image and verify.
        let img2 = fs.reader.into_inner();
        let cur2 = std::io::Cursor::new(img2);
        let mut fs2 = Pfs3Filesystem::open(cur2, 0).expect("reopen");
        let root2 = fs2.root().expect("root2");
        let kids = fs2.list_directory(&root2).expect("list");
        assert_eq!(kids.len(), 2, "expected 2 children, got {:?}", kids);
        let names: Vec<&str> = kids.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"MyDir"));
        assert!(names.contains(&"readme.txt"));

        let file_entry = kids.iter().find(|c| c.name == "readme.txt").unwrap();
        let data = fs2
            .read_file(file_entry, payload.len() * 2)
            .expect("read_file");
        assert_eq!(data, payload);

        let dir_entry = kids.iter().find(|c| c.name == "MyDir").unwrap();
        let dir_kids = fs2.list_directory(dir_entry).expect("list new dir");
        assert!(dir_kids.is_empty(), "new dir should start empty");
    }

    #[test]
    fn write_round_trip_delete_entry() {
        use super::super::filesystem::{
            CreateDirectoryOptions, CreateFileOptions, EditableFilesystem,
        };
        let img = create_blank_pfs3(8192, "T").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = Pfs3Filesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");

        let payload: &[u8] = b"to be deleted";
        let mut p = std::io::Cursor::new(payload);
        let file_fe = fs
            .create_file(
                &root,
                "tmp.txt",
                &mut p,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create_file");
        let dir_fe = fs
            .create_directory(&root, "doomed", &CreateDirectoryOptions::default())
            .expect("create_directory");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync after create");

        let free_after_create = EditableFilesystem::free_space(&mut fs).expect("free_space");

        // Delete both. After sync, the parent dir should be empty again
        // and free_space should bump back up.
        let root_again = fs.root().expect("root");
        EditableFilesystem::delete_entry(&mut fs, &root_again, &file_fe).expect("del file");
        EditableFilesystem::delete_entry(&mut fs, &root_again, &dir_fe).expect("del dir");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync after delete");

        let free_after_delete = EditableFilesystem::free_space(&mut fs).expect("free_space2");
        assert!(
            free_after_delete > free_after_create,
            "free_space should grow after delete (after={}, before={})",
            free_after_delete,
            free_after_create
        );

        let img2 = fs.reader.into_inner();
        let cur2 = std::io::Cursor::new(img2);
        let mut fs2 = Pfs3Filesystem::open(cur2, 0).expect("reopen");
        let root2 = fs2.root().expect("root2");
        let kids = fs2.list_directory(&root2).expect("list");
        assert!(
            kids.is_empty(),
            "root should be empty after deletes, got {:?}",
            kids
        );
    }

    #[test]
    fn create_directory_rolls_back_on_duplicate() {
        use super::super::filesystem::{CreateDirectoryOptions, EditableFilesystem};
        let img = create_blank_pfs3(8192, "T").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = Pfs3Filesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");
        fs.create_directory(&root, "Dir", &CreateDirectoryOptions::default())
            .expect("first");
        let before_free = EditableFilesystem::free_space(&mut fs).expect("free");
        let res = fs.create_directory(&root, "Dir", &CreateDirectoryOptions::default());
        assert!(matches!(res, Err(FilesystemError::AlreadyExists(_))));
        // Rollback should restore free_space (and dirty maps).
        let after_free = EditableFilesystem::free_space(&mut fs).expect("free2");
        assert_eq!(
            before_free, after_free,
            "rollback should restore free space"
        );
    }

    /// Shrink a blank 8192-sector volume to 4096 sectors. The data area
    /// is entirely free, so the resize should succeed and the
    /// rootblock should reflect the new size.
    #[test]
    fn resize_shrink_blank_volume() {
        let img = create_blank_pfs3(8192, "Shrink").expect("format");
        let mut cur = std::io::Cursor::new(img);
        let mut log: Vec<String> = Vec::new();
        let new_size = 4096u64 * HW_SECTOR;
        resize_pfs3_in_place(&mut cur, 0, new_size, &mut |s| log.push(s.to_string()))
            .expect("shrink");
        assert!(log.iter().any(|l| l.contains("shrunk")), "log: {log:?}");
        // Re-open at the new size and verify the rootblock now reports it.
        let img2 = cur.into_inner();
        let img_truncated = img2[..new_size as usize].to_vec();
        let cur2 = std::io::Cursor::new(img_truncated);
        let fs = Pfs3Filesystem::open(cur2, 0).expect("reopen");
        assert_eq!(fs.total_size(), new_size);
    }

    /// Shrink should reject if a file would be lost.
    #[test]
    fn resize_shrink_refuses_when_data_lost() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem};
        let img = create_blank_pfs3(8192, "Reject").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = Pfs3Filesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");
        // Create a file large enough that it lives near the tail.
        // Data allocations come from the end of the data bitmap walked
        // forward in `alloc_data_blocks`, so even a small file lands
        // near `bitmap_start` of the blank volume. To force a
        // late-sector allocation we instead pad with a bigger file.
        let payload: Vec<u8> = vec![0xCDu8; 1024 * 1024];
        let mut c = std::io::Cursor::new(&payload);
        fs.create_file(
            &root,
            "big.bin",
            &mut c,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create_file");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");

        // Try to shrink small enough that the new tail truncates the
        // file. The file occupies sectors low in the data area, but
        // our allocator may place data anywhere; pick a target that's
        // below `bitmap_start + ceil(1 MiB / 512)` = 66 + 2048 = 2114.
        // 200 sectors is well below that and forces a rejection.
        let img2 = fs.reader.into_inner();
        let mut cur2 = std::io::Cursor::new(img2);
        let mut log: Vec<String> = Vec::new();
        let result = resize_pfs3_in_place(&mut cur2, 0, 200 * HW_SECTOR, &mut |s| {
            log.push(s.to_string())
        });
        assert!(result.is_err(), "expected refusal, got log: {log:?}");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("highest allocated"),
            "unexpected error: {err_msg}"
        );
    }

    /// Grow within the existing BM-block capacity should succeed.
    /// Blank 8192-sector volume has 1 BM block covering 8096 data
    /// sectors, so growing to 8192 -> 9000 stays within the existing
    /// BM (still 1 block, capacity 8162 data sectors with
    /// bitmap_start=66). 9000 - 66 = 8934 — exceeds. Pick 8160 -> 8160.
    /// Instead grow from 4096 -> 8192 after a prior shrink so we
    /// know we're well within bitmap capacity.
    #[test]
    fn resize_grow_after_shrink_round_trips() {
        let img = create_blank_pfs3(8192, "GrowBack").expect("format");
        let mut cur = std::io::Cursor::new(img);
        let mut log: Vec<String> = Vec::new();
        // Shrink to 4096 sectors.
        resize_pfs3_in_place(&mut cur, 0, 4096 * HW_SECTOR, &mut |s| {
            log.push(s.to_string())
        })
        .expect("shrink");
        // Re-pad the buffer to 8192 sectors so the grow has bytes to
        // work with (the underlying medium needs to be at least that
        // big).
        let mut img = cur.into_inner();
        img.resize(8192 * HW_SECTOR as usize, 0);
        let mut cur = std::io::Cursor::new(img);
        resize_pfs3_in_place(&mut cur, 0, 8000 * HW_SECTOR, &mut |s| {
            log.push(s.to_string())
        })
        .expect("grow");
        assert!(
            log.iter().any(|l| l.contains("grew")),
            "log missing grew: {log:?}"
        );
        let img = cur.into_inner();
        let cur = std::io::Cursor::new(img);
        let fs = Pfs3Filesystem::open(cur, 0).expect("reopen after grow");
        assert_eq!(fs.total_size(), 8000 * HW_SECTOR);
    }

    /// Resize on a non-PFS volume should be a silent no-op.
    #[test]
    fn resize_skips_non_pfs_volume() {
        let mut img = vec![0u8; 8192 * HW_SECTOR as usize];
        // Stamp something that isn't PFS at boot magic.
        img[0..4].copy_from_slice(b"DEAD");
        let mut cur = std::io::Cursor::new(img);
        let mut log: Vec<String> = Vec::new();
        resize_pfs3_in_place(&mut cur, 0, 4096 * HW_SECTOR, &mut |s| {
            log.push(s.to_string())
        })
        .expect("noop");
        assert!(log.is_empty(), "expected silent no-op, got: {log:?}");
    }

    /// Equal sizes should log "no size change" and exit clean.
    #[test]
    fn resize_no_op_when_unchanged() {
        let img = create_blank_pfs3(8192, "Same").expect("format");
        let mut cur = std::io::Cursor::new(img);
        let mut log: Vec<String> = Vec::new();
        resize_pfs3_in_place(&mut cur, 0, 8192 * HW_SECTOR, &mut |s| {
            log.push(s.to_string())
        })
        .expect("noop");
        assert!(
            log.iter().any(|l| l.contains("no size change")),
            "log: {log:?}"
        );
    }

    /// `CreateFileOptions::amiga_protection` / `amiga_comment` /
    /// `amiga_dates` round-trip through create_file -> sync -> reopen ->
    /// list_directory.
    #[test]
    fn create_file_persists_amiga_metadata() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem};
        let img = create_blank_pfs3(8192, "Meta").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = Pfs3Filesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");
        let payload = b"protected".to_vec();
        let mut src = std::io::Cursor::new(payload.clone());
        let opts = CreateFileOptions {
            amiga_protection: Some(0x0000_00A5),
            amiga_comment: Some("a test filenote".to_string()),
            amiga_dates: Some((12345, 678, 90)),
            ..Default::default()
        };
        fs.create_file(&root, "secret", &mut src, payload.len() as u64, &opts)
            .expect("create_file");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");

        let img2 = fs.reader.into_inner();
        let cur2 = std::io::Cursor::new(img2);
        let mut fs2 = Pfs3Filesystem::open(cur2, 0).expect("reopen");
        let root2 = fs2.root().expect("root");
        let kids = fs2.list_directory(&root2).expect("list");
        let fe = kids
            .iter()
            .find(|e| e.name == "secret")
            .expect("created entry visible");
        assert_eq!(fe.amiga_protection, Some(0xA5));
        assert_eq!(fe.amiga_comment.as_deref(), Some("a test filenote"));
        assert_eq!(fe.amiga_date, Some((12345, 678, 90)));
    }

    /// `alloc_anode_in_memory` extends past anodeblock 0 when full.
    /// At resblksize=1024 each AB holds 84 anodes (slots 0..83); the
    /// first 6 slots of seqnr-0 are reserved for PFS3 bookkeeping, so
    /// 78 user slots in seqnr 0. Creating 100 files forces the
    /// allocator to spill into seqnr 1.
    #[test]
    fn anodeblock_chain_extends_when_first_ab_is_full() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem};
        let img = create_blank_pfs3(8192, "Chain").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = Pfs3Filesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");
        let payload = b"x".to_vec();
        for i in 0..100 {
            let name = format!("f{:03}", i);
            let mut p = std::io::Cursor::new(payload.clone());
            fs.create_file(
                &root,
                &name,
                &mut p,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap_or_else(|e| panic!("create_file f{i} failed: {e}"));
        }
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");

        // Reopen and confirm all 100 files survive.
        let img2 = fs.reader.into_inner();
        let cur2 = std::io::Cursor::new(img2);
        let mut fs2 = Pfs3Filesystem::open(cur2, 0).expect("reopen");
        let root2 = fs2.root().expect("root");
        let kids = fs2.list_directory(&root2).expect("list");
        assert_eq!(kids.len(), 100, "all 100 files should round-trip");
    }

    /// `create_file` accepts long filenames (PFS3 LONGFN mode, fnsize=107).
    /// Real Amiga volumes routinely carry names like "Sensible World of
    /// Soccer 24-25.iff" (34 bytes) — well past the standard 30-byte
    /// AmigaDOS cap.
    #[test]
    fn create_file_with_long_name_round_trips() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem};
        let img = create_blank_pfs3(8192, "Long").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = Pfs3Filesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");
        let long_name = "Sensible World of Soccer 24-25.iff";
        assert!(long_name.len() > 30, "test premise: name > standard cap");
        let payload = b"data".to_vec();
        let mut p = std::io::Cursor::new(payload.clone());
        fs.create_file(
            &root,
            long_name,
            &mut p,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create with long name");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");

        let img2 = fs.reader.into_inner();
        let cur2 = std::io::Cursor::new(img2);
        let mut fs2 = Pfs3Filesystem::open(cur2, 0).expect("reopen");
        let root2 = fs2.root().expect("root");
        let kids = fs2.list_directory(&root2).expect("list");
        let fe = kids
            .iter()
            .find(|e| e.name == long_name)
            .expect("long name visible after reopen");
        assert_eq!(fe.size, payload.len() as u64);
    }

    /// `create_blank_pfs3` must scale `resblksize` and `numreserved`
    /// with the target size. The 4 MB layout in
    /// `blank_volume_round_trips_through_reader` only exercises the
    /// small-volume path; this test covers 650 MB (forces
    /// numreserved >= ~180 at resblksize=1024) and 9 GB (forces
    /// resblksize=4096 to keep no_bmb within a single MI).
    #[test]
    fn blank_volume_scales_to_real_amiga_partition_sizes() {
        // 650 MB target (DH0-style): single MI, 1024-byte reserved
        // blocks, numreserved scaled past 32 to ~180.
        let img_650 = create_blank_pfs3(1_331_576, "DH0").expect("format 650 MB");
        let mut cur = std::io::Cursor::new(img_650);
        let fs = Pfs3Filesystem::open(&mut cur, 0).expect("open 650 MB");
        assert_eq!(fs.volume_label(), Some("DH0"));
        assert_eq!(fs.root.reserved_blksize, 1024);

        // 9 GB target (DH1-style): forces resblksize=4096 so a single
        // MI's 1021 index slots cover all BM blocks.
        let img_9g = create_blank_pfs3(18_740_736, "DH1").expect("format 9 GB");
        let mut cur = std::io::Cursor::new(img_9g);
        let fs = Pfs3Filesystem::open(&mut cur, 0).expect("open 9 GB");
        assert_eq!(fs.volume_label(), Some("DH1"));
        assert_eq!(fs.root.reserved_blksize, 4096);
    }

    /// `create_symlink` round-trips through `sync_metadata` -> reopen
    /// -> `list_directory`, surfacing the target path on the rebuilt
    /// `FileEntry`.
    #[test]
    fn create_symlink_round_trips() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem};
        let img = create_blank_pfs3(8192, "Sym").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = Pfs3Filesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");
        let opts = CreateFileOptions::default();
        fs.create_symlink(&root, "ptr", "Sys:Foo/bar", &opts)
            .expect("create_symlink");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");

        let img2 = fs.reader.into_inner();
        let cur2 = std::io::Cursor::new(img2);
        let mut fs2 = Pfs3Filesystem::open(cur2, 0).expect("reopen");
        let root2 = fs2.root().expect("root");
        let kids = fs2.list_directory(&root2).expect("list");
        let fe = kids
            .iter()
            .find(|e| e.name == "ptr")
            .expect("symlink visible");
        assert!(fe.is_symlink());
        assert_eq!(fe.symlink_target.as_deref(), Some("Sys:Foo/bar"));
        assert_eq!(fe.size, "Sys:Foo/bar".len() as u64);
    }

    /// `create_hardlink` round-trips: the link direntry surfaces with
    /// `link_target_cnid` pointing at the original file's anode.
    #[test]
    fn create_hardlink_round_trips() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem};
        let img = create_blank_pfs3(8192, "Hard").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = Pfs3Filesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");
        let opts = CreateFileOptions::default();
        let payload = b"original data".to_vec();
        let mut p = std::io::Cursor::new(payload.clone());
        let target_fe = fs
            .create_file(&root, "orig", &mut p, payload.len() as u64, &opts)
            .expect("create_file");
        let target_anode = target_fe.location;
        fs.create_hardlink(&root, "ln", &target_fe, &opts)
            .expect("create_hardlink");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");

        let img2 = fs.reader.into_inner();
        let cur2 = std::io::Cursor::new(img2);
        let mut fs2 = Pfs3Filesystem::open(cur2, 0).expect("reopen");
        let root2 = fs2.root().expect("root");
        let kids = fs2.list_directory(&root2).expect("list");
        let link = kids
            .iter()
            .find(|e| e.name == "ln")
            .expect("hardlink visible");
        assert_eq!(link.link_target_cnid, Some(target_anode));
        // The link's size mirrors the target's size (per pfs3aio).
        assert_eq!(link.size, payload.len() as u64);
    }

    /// `CreateDirectoryOptions` metadata round-trips the same way.
    #[test]
    fn create_directory_persists_amiga_metadata() {
        use super::super::filesystem::{CreateDirectoryOptions, EditableFilesystem};
        let img = create_blank_pfs3(8192, "Meta").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = Pfs3Filesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");
        let opts = CreateDirectoryOptions {
            amiga_protection: Some(0xF0),
            amiga_comment: Some("dir note".to_string()),
            amiga_dates: Some((100, 200, 300)),
            ..Default::default()
        };
        fs.create_directory(&root, "Notes", &opts)
            .expect("create_directory");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");

        let img2 = fs.reader.into_inner();
        let cur2 = std::io::Cursor::new(img2);
        let mut fs2 = Pfs3Filesystem::open(cur2, 0).expect("reopen");
        let root2 = fs2.root().expect("root");
        let kids = fs2.list_directory(&root2).expect("list");
        let fe = kids
            .iter()
            .find(|e| e.name == "Notes")
            .expect("created dir visible");
        assert_eq!(fe.amiga_protection, Some(0xF0));
        assert_eq!(fe.amiga_comment.as_deref(), Some("dir note"));
        assert_eq!(fe.amiga_date, Some((100, 200, 300)));
    }
}
