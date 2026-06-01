//! HFS+ journal: on-disk parse, replay, and a transactional write recorder.
//!
//! The journal is a physical-block redo log living inside the volume (the
//! common case — `kJIJournalInFSMask`) or, rarely, on an external device
//! (`kJIJournalOnOtherDeviceMask`, which we refuse). Its on-disk layout is
//! documented in Apple TN1150 ("Journaled HFS Plus") and implemented in the
//! Apple `hfs` source tree:
//!
//!   * `core/hfs_journal.{c,h}` — the kernel journal (write + replay)
//!   * `lib_fsck_hfs/fsck_journal.c` — the userland replay used by `fsck_hfs`
//!   * `core/hfs_format.h` — `struct JournalInfoBlock` + the `kJI*` flags
//!
//! Every structure and the checksum routine here mirror those sources
//! byte-for-byte so that transactions we write are replayable by macOS and
//! that we can replay transactions macOS wrote. See
//! `docs/hfsplus_enhancements.md` Phase 9 for the staged plan.
//!
//! Endianness: the journal is written in the *native* byte order of the
//! machine that created it (big-endian on PPC, little-endian on Intel/ARM),
//! disambiguated by the `endian` magic in the journal header. The
//! `JournalInfoBlock`, by contrast, is an HFS+ on-disk structure and is
//! therefore always big-endian like the volume header.

use byteorder::{BigEndian, ByteOrder, LittleEndian};
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};

/// `JOURNAL_HEADER_MAGIC` — `'JNLx'`.
pub const JOURNAL_HEADER_MAGIC: u32 = 0x4a4e_4c78;
/// `OLD_JOURNAL_HEADER_MAGIC` — `'JHDR'`, still accepted on read.
pub const OLD_JOURNAL_HEADER_MAGIC: u32 = 0x4a48_4452;
/// `ENDIAN_MAGIC` — the `endian` field reads as this in the journal's own
/// byte order, which is how we detect that order.
pub const ENDIAN_MAGIC: u32 = 0x1234_5678;

/// Bytes of the journal header covered by its checksum, with the `checksum`
/// field itself zeroed: `offsetof(struct journal_header, sequence_num)`.
const JOURNAL_HEADER_CKSUM_SIZE: usize = 44;
/// Bytes of a `block_list_header` covered by its checksum: the 16-byte
/// fixed header plus `binfo[0]` (16 bytes). `BLHDR_CHECKSUM_SIZE` in xnu.
const BLHDR_CHECKSUM_SIZE: usize = 32;
/// On-disk size of one `block_info` entry: `bnum(8) + bsize(4) + next(4)`.
const BLOCK_INFO_SIZE: usize = 16;
/// On-disk size of the fixed `block_list_header` prefix (before `binfo[]`).
const BLOCK_LIST_HEADER_PREFIX: usize = 16;

/// `block_list_header.flags`: per-block data checksums present in `binfo[i].next`.
/// We tolerate it on read but never set it on our own writes.
#[allow(dead_code)]
const BLHDR_CHECK_CHECKSUMS: u32 = 0x0001;
/// `block_list_header.flags`: this is the first header of a transaction.
const BLHDR_FIRST_HEADER: u32 = 0x0002;

/// `JournalInfoBlock.flags`: journal lives inside this filesystem (common).
pub const KJI_JOURNAL_IN_FS_MASK: u32 = 0x0000_0001;
/// `JournalInfoBlock.flags`: journal lives on a separate device (we refuse).
pub const KJI_JOURNAL_ON_OTHER_DEVICE_MASK: u32 = 0x0000_0002;
/// `JournalInfoBlock.flags`: journal needs initialization.
pub const KJI_JOURNAL_NEED_INIT_MASK: u32 = 0x0000_0004;

/// Sentinel `bnum` for a "killed" block that should be skipped during replay.
const KILLED_BLOCK: u64 = u64::MAX;

/// Errors from journal parsing / replay / commit.
#[derive(Debug)]
pub enum JournalError {
    /// The volume header's journaled bit is clear — there is no journal.
    NotJournaled,
    /// The journal lives on an external device, which is out of scope.
    ExternalJournal,
    /// The journal header magic did not match either accepted value.
    BadMagic(u32),
    /// The endian field matched neither byte order.
    BadEndian(u32),
    /// A structure was shorter than required, or a field pointed out of range.
    OutOfRange(String),
    /// A transaction's block-list-header checksum did not verify.
    CorruptTransaction { sequence_num: u32 },
    /// Underlying I/O failure.
    Io(std::io::Error),
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JournalError::NotJournaled => write!(f, "volume is not journaled"),
            JournalError::ExternalJournal => {
                write!(f, "journal lives on an external device (unsupported)")
            }
            JournalError::BadMagic(m) => write!(f, "bad journal header magic 0x{m:08x}"),
            JournalError::BadEndian(e) => write!(f, "bad journal endian magic 0x{e:08x}"),
            JournalError::OutOfRange(msg) => write!(f, "journal field out of range: {msg}"),
            JournalError::CorruptTransaction { sequence_num } => {
                write!(f, "corrupt journal transaction (sequence {sequence_num})")
            }
            JournalError::Io(e) => write!(f, "journal I/O error: {e}"),
        }
    }
}

impl std::error::Error for JournalError {}

impl From<std::io::Error> for JournalError {
    fn from(e: std::io::Error) -> Self {
        JournalError::Io(e)
    }
}

type Result<T> = std::result::Result<T, JournalError>;

/// Byte order of the journal, selected from the header's `endian` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalEndian {
    Big,
    Little,
}

impl JournalEndian {
    fn read_u16(self, b: &[u8]) -> u16 {
        match self {
            JournalEndian::Big => BigEndian::read_u16(b),
            JournalEndian::Little => LittleEndian::read_u16(b),
        }
    }
    fn read_u32(self, b: &[u8]) -> u32 {
        match self {
            JournalEndian::Big => BigEndian::read_u32(b),
            JournalEndian::Little => LittleEndian::read_u32(b),
        }
    }
    fn read_u64(self, b: &[u8]) -> u64 {
        match self {
            JournalEndian::Big => BigEndian::read_u64(b),
            JournalEndian::Little => LittleEndian::read_u64(b),
        }
    }
    fn write_u16(self, b: &mut [u8], v: u16) {
        match self {
            JournalEndian::Big => BigEndian::write_u16(b, v),
            JournalEndian::Little => LittleEndian::write_u16(b, v),
        }
    }
    fn write_u32(self, b: &mut [u8], v: u32) {
        match self {
            JournalEndian::Big => BigEndian::write_u32(b, v),
            JournalEndian::Little => LittleEndian::write_u32(b, v),
        }
    }
    fn write_u64(self, b: &mut [u8], v: u64) {
        match self {
            JournalEndian::Big => BigEndian::write_u64(b, v),
            JournalEndian::Little => LittleEndian::write_u64(b, v),
        }
    }
}

/// The xnu journal checksum: a byte-at-a-time accumulator, bit-inverted at
/// the end. Lifted verbatim from `calc_checksum` in `core/hfs_journal.c`.
pub fn calc_checksum(data: &[u8]) -> u32 {
    let mut cksum: u32 = 0;
    for &byte in data {
        cksum = cksum.wrapping_shl(8) ^ cksum.wrapping_add(byte as u32);
    }
    !cksum
}

/// The relevant fields of a `JournalInfoBlock` (TN1150 / `hfs_format.h`).
/// Parsed big-endian. The device signature, UUID, and serial number are not
/// needed for in-FS journals and are skipped.
#[derive(Debug, Clone)]
pub struct JournalInfoBlock {
    pub flags: u32,
    /// Byte offset of the journal header, relative to the partition start.
    pub offset: u64,
    /// Total journal size in bytes (header block + circular buffer).
    pub size: u64,
}

impl JournalInfoBlock {
    /// Parse from the 512+ byte block the volume header's `journalInfoBlock`
    /// points at. Layout: `flags@0`, `device_signature[8]@4`, `offset@36`,
    /// `size@44` — all big-endian.
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < 52 {
            return Err(JournalError::OutOfRange(
                "journal info block shorter than 52 bytes".into(),
            ));
        }
        Ok(JournalInfoBlock {
            flags: BigEndian::read_u32(&data[0..4]),
            offset: BigEndian::read_u64(&data[36..44]),
            size: BigEndian::read_u64(&data[44..52]),
        })
    }

    /// True when the journal lives inside this filesystem (the case we handle).
    pub fn is_in_fs(&self) -> bool {
        self.flags & KJI_JOURNAL_IN_FS_MASK != 0
    }

    /// True when the journal lives on a separate device (out of scope).
    pub fn is_external(&self) -> bool {
        self.flags & KJI_JOURNAL_ON_OTHER_DEVICE_MASK != 0
    }
}

/// The journal header at `JournalInfoBlock.offset` (`struct journal_header`).
/// All multi-byte fields use `endian`.
#[derive(Debug, Clone)]
pub struct JournalHeader {
    pub endian: JournalEndian,
    pub magic: u32,
    /// Byte offset (relative to the journal device base = `jib.offset`) of the
    /// first live transaction.
    pub start: u64,
    /// Byte offset of the first free byte. `start == end` means empty/clean.
    pub end: u64,
    /// Journal size in bytes (matches `JournalInfoBlock.size`).
    pub size: u64,
    /// Size of each `block_list_header` region (data follows it).
    pub blhdr_size: u32,
    pub checksum: u32,
    /// Journal header block size, and the unit `binfo.bnum` is multiplied by.
    pub jhdr_size: u32,
    pub sequence_num: u32,
}

impl JournalHeader {
    /// Parse the journal header, auto-detecting byte order from `endian`.
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < JOURNAL_HEADER_CKSUM_SIZE + 4 {
            return Err(JournalError::OutOfRange(
                "journal header shorter than 48 bytes".into(),
            ));
        }
        // The endian field reads as ENDIAN_MAGIC in the journal's own order.
        let endian = if LittleEndian::read_u32(&data[4..8]) == ENDIAN_MAGIC {
            JournalEndian::Little
        } else if BigEndian::read_u32(&data[4..8]) == ENDIAN_MAGIC {
            JournalEndian::Big
        } else {
            return Err(JournalError::BadEndian(BigEndian::read_u32(&data[4..8])));
        };

        let magic = endian.read_u32(&data[0..4]);
        if magic != JOURNAL_HEADER_MAGIC && magic != OLD_JOURNAL_HEADER_MAGIC {
            return Err(JournalError::BadMagic(magic));
        }

        let hdr = JournalHeader {
            endian,
            magic,
            start: endian.read_u64(&data[8..16]),
            end: endian.read_u64(&data[16..24]),
            size: endian.read_u64(&data[24..32]),
            blhdr_size: endian.read_u32(&data[32..36]),
            checksum: endian.read_u32(&data[36..40]),
            jhdr_size: endian.read_u32(&data[40..44]),
            sequence_num: endian.read_u32(&data[44..48]),
        };

        // Verify the header checksum (computed over the first 44 bytes with
        // the checksum field zeroed) for the modern magic. The old magic
        // predates header checksums.
        if magic == JOURNAL_HEADER_MAGIC {
            let mut tmp = data[..JOURNAL_HEADER_CKSUM_SIZE].to_vec();
            tmp[36..40].copy_from_slice(&[0u8; 4]);
            let computed = calc_checksum(&tmp);
            if computed != hdr.checksum {
                return Err(JournalError::OutOfRange(format!(
                    "journal header checksum mismatch (computed 0x{computed:08x} != stored 0x{:08x})",
                    hdr.checksum
                )));
            }
        }

        hdr.validate()?;
        Ok(hdr)
    }

    fn validate(&self) -> Result<()> {
        if self.jhdr_size == 0 || (self.jhdr_size as u64) > self.size {
            return Err(JournalError::OutOfRange(format!(
                "jhdr_size {} invalid for journal size {}",
                self.jhdr_size, self.size
            )));
        }
        if self.blhdr_size < BLHDR_CHECKSUM_SIZE as u32 || (self.blhdr_size as u64) > self.size {
            return Err(JournalError::OutOfRange(format!(
                "blhdr_size {} invalid",
                self.blhdr_size
            )));
        }
        if self.start > self.size || self.end > self.size {
            return Err(JournalError::OutOfRange(format!(
                "start {} / end {} out of range for size {}",
                self.start, self.end, self.size
            )));
        }
        Ok(())
    }

    /// True when the journal has no pending transactions.
    pub fn is_empty(&self) -> bool {
        self.normalized_start() == self.normalized_end()
    }

    /// `start`, wrapped past the very end of the buffer to the first byte after
    /// the header (matches the kernel's replay entry fixup).
    fn normalized_start(&self) -> u64 {
        if self.start == self.size {
            self.jhdr_size as u64
        } else {
            self.start
        }
    }

    fn normalized_end(&self) -> u64 {
        if self.end == self.size {
            self.jhdr_size as u64
        } else {
            self.end
        }
    }

    /// Serialize the header back into a `jhdr_size`-byte block, recomputing the
    /// checksum. The bytes past the fixed header are preserved from `original`
    /// when supplied (so we don't clobber any vendor padding macOS wrote).
    pub fn serialize(&self, original: Option<&[u8]>) -> Vec<u8> {
        let len = self.jhdr_size as usize;
        let mut out = vec![0u8; len];
        if let Some(orig) = original {
            let copy = orig.len().min(len);
            out[..copy].copy_from_slice(&orig[..copy]);
        }
        let e = self.endian;
        e.write_u32(&mut out[0..4], self.magic);
        // endian magic is byte-order agnostic by construction.
        e.write_u32(&mut out[4..8], ENDIAN_MAGIC);
        e.write_u64(&mut out[8..16], self.start);
        e.write_u64(&mut out[16..24], self.end);
        e.write_u64(&mut out[24..32], self.size);
        e.write_u32(&mut out[32..36], self.blhdr_size);
        e.write_u32(&mut out[36..40], 0); // checksum placeholder
        e.write_u32(&mut out[40..44], self.jhdr_size);
        e.write_u32(&mut out[44..48], self.sequence_num);
        let cksum = calc_checksum(&out[..JOURNAL_HEADER_CKSUM_SIZE]);
        e.write_u32(&mut out[36..40], cksum);
        out
    }
}

/// One decoded `block_info` entry describing where a journaled block lands.
#[derive(Debug, Clone)]
pub struct BlockInfo {
    /// Target block number on the filesystem device (multiply by `jhdr_size`
    /// for a byte offset relative to the partition). `u64::MAX` = killed.
    pub bnum: u64,
    /// Size of this block in bytes.
    pub bsize: u32,
    /// For `binfo[0]`: the transaction sequence number. For data blocks: the
    /// per-block checksum (only meaningful when `BLHDR_CHECK_CHECKSUMS`).
    pub next: u32,
}

/// One transaction read from the journal: its decoded block list plus the
/// block payloads, in order.
#[derive(Debug, Clone)]
pub struct TransactionView {
    pub sequence_num: u32,
    pub flags: u32,
    pub bytes_used: u32,
    /// `(target byte offset relative to partition, payload bytes)` for every
    /// non-killed data block. Offset is `bnum * jhdr_size`.
    pub blocks: Vec<(u64, Vec<u8>)>,
    /// Total bytes across all retained payloads.
    pub total_payload: u64,
}

/// A lightweight per-transaction summary for the history viewer / fsck.
#[derive(Debug, Clone)]
pub struct TransactionSummary {
    pub sequence_num: u32,
    pub block_count: u32,
    pub total_bytes: u64,
    /// `(min, max)` target byte offset across the transaction's blocks, or
    /// `None` when every block was killed.
    pub target_range: Option<(u64, u64)>,
}

/// Read-only state of a parsed journal — enough for the history viewer and the
/// fsck journal phase.
#[derive(Debug, Clone)]
pub struct JournalState {
    pub info: JournalInfoBlock,
    pub header: JournalHeader,
    pub transactions: Vec<TransactionSummary>,
    /// True when the walk stopped early on a corrupt/short transaction.
    pub partial: bool,
    /// Sequence number of the transaction whose block-list-header checksum
    /// failed (the walk stops there, like macOS replay), if any.
    pub checksum_mismatch: Option<u32>,
    /// `(from, to)` for each point where the sequence number did not advance
    /// by one. macOS stops replay at the first such jump.
    pub sequence_jumps: Vec<(u32, u32)>,
}

impl JournalState {
    /// Total bytes of block payloads across all pending transactions.
    pub fn pending_bytes(&self) -> u64 {
        self.transactions.iter().map(|t| t.total_bytes).sum()
    }
}

/// A named byte range of one volume metadata structure, used to classify which
/// subsystem a journaled block targets. Offsets are relative to the partition.
#[derive(Debug, Clone)]
pub struct MetadataRegion {
    pub name: &'static str,
    pub start: u64,
    pub end: u64,
}

/// Classify a target byte offset against a set of metadata regions, returning
/// the region name or `"fork data"` when it falls outside all of them.
pub fn classify_target(regions: &[MetadataRegion], target: u64) -> &'static str {
    for r in regions {
        if target >= r.start && target < r.end {
            return r.name;
        }
    }
    "fork data"
}

/// One journaled block, trimmed for display: where it lands, its size, and a
/// short hex preview of its leading bytes.
#[derive(Debug, Clone)]
pub struct JournalBlockDetail {
    /// Target byte offset relative to the partition (`bnum * jhdr_size`).
    pub target: u64,
    pub bsize: u32,
    /// First up-to-64 payload bytes, for a hex preview in the viewer.
    pub preview: Vec<u8>,
}

/// One transaction, fully decoded for the history viewer (block previews only,
/// not full payloads, to bound memory on large journals).
#[derive(Debug, Clone)]
pub struct JournalTxnDetail {
    pub sequence_num: u32,
    pub flags: u32,
    pub bytes_used: u32,
    pub total_bytes: u64,
    pub blocks: Vec<JournalBlockDetail>,
}

/// Everything the GUI journal viewer needs: the JIB + header, the decoded
/// transactions (newest-last, as walked), the metadata regions used to
/// classify block targets, and any consistency findings.
#[derive(Debug, Clone)]
pub struct JournalDetail {
    pub info: JournalInfoBlock,
    pub header: JournalHeader,
    pub transactions: Vec<JournalTxnDetail>,
    pub regions: Vec<MetadataRegion>,
    pub checksum_mismatch: Option<u32>,
    pub sequence_jumps: Vec<(u32, u32)>,
    pub partial: bool,
}

/// Largest preview kept per block.
const PREVIEW_BYTES: usize = 64;

/// Walk a journal into a [`JournalDetail`], capturing bounded per-block
/// previews. `regions` is supplied by the caller (it needs the volume header's
/// fork extents). Used by the GUI history viewer (Step 29).
pub fn read_journal_detail<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    jib: JournalInfoBlock,
    header: JournalHeader,
    regions: Vec<MetadataRegion>,
) -> JournalDetail {
    let mut transactions = Vec::new();
    let mut checksum_mismatch = None;
    let mut sequence_jumps = Vec::new();
    let mut partial = false;
    {
        let mut walker = JournalWalker::new(reader, partition_offset, &jib, &header);
        let mut prev_seq: Option<u32> = None;
        while let Some(item) = walker.next() {
            match item {
                Ok(view) => {
                    if let Some(prev) = prev_seq {
                        if view.sequence_num != prev && view.sequence_num != prev.wrapping_add(1) {
                            sequence_jumps.push((prev, view.sequence_num));
                        }
                    }
                    prev_seq = Some(view.sequence_num);
                    let blocks = view
                        .blocks
                        .iter()
                        .map(|(target, data)| JournalBlockDetail {
                            target: *target,
                            bsize: data.len() as u32,
                            preview: data[..data.len().min(PREVIEW_BYTES)].to_vec(),
                        })
                        .collect();
                    transactions.push(JournalTxnDetail {
                        sequence_num: view.sequence_num,
                        flags: view.flags,
                        bytes_used: view.bytes_used,
                        total_bytes: view.total_payload,
                        blocks,
                    });
                }
                Err(JournalError::CorruptTransaction { sequence_num }) => {
                    checksum_mismatch = Some(sequence_num);
                    partial = true;
                    break;
                }
                Err(_) => {
                    partial = true;
                    break;
                }
            }
        }
    }
    JournalDetail {
        info: jib,
        header,
        transactions,
        regions,
        checksum_mismatch,
        sequence_jumps,
        partial,
    }
}

/// Walks the live transactions of a journal (`start -> end`, wrapping at
/// `size`). Each `next` decodes one transaction. The walker reads straight
/// from the disk so it works on any `Read + Seek` source.
pub struct JournalWalker<'a, R: Read + Seek> {
    reader: &'a mut R,
    partition_offset: u64,
    jib_offset: u64,
    jhdr_size: u64,
    blhdr_size: usize,
    journal_size: u64,
    endian: JournalEndian,
    /// Current journal-relative read position; advances per transaction.
    cursor: u64,
    end: u64,
    /// Once a corrupt/short transaction is hit we stop yielding.
    done: bool,
}

impl<'a, R: Read + Seek> JournalWalker<'a, R> {
    pub fn new(
        reader: &'a mut R,
        partition_offset: u64,
        jib: &JournalInfoBlock,
        jh: &JournalHeader,
    ) -> Self {
        JournalWalker {
            reader,
            partition_offset,
            jib_offset: jib.offset,
            jhdr_size: jh.jhdr_size as u64,
            blhdr_size: jh.blhdr_size as usize,
            journal_size: jh.size,
            endian: jh.endian,
            cursor: jh.normalized_start(),
            end: jh.normalized_end(),
            done: jh.is_empty(),
        }
    }

    /// Wrap a journal-relative offset that has reached/passed the end of the
    /// circular buffer back to the first byte after the header block.
    fn wrap(&self, off: u64) -> u64 {
        if off >= self.journal_size {
            self.jhdr_size + (off - self.journal_size)
        } else {
            off
        }
    }

    /// Read `len` bytes starting at journal-relative `rel`, wrapping at
    /// `journal_size` back to `jhdr_size` (the circular buffer convention).
    fn read_wrapped(&mut self, rel: u64, len: usize) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(len);
        let mut pos = self.wrap(rel);
        while out.len() < len {
            let space = (self.journal_size - pos) as usize;
            let chunk = (len - out.len()).min(space);
            let abs = self.partition_offset + self.jib_offset + pos;
            self.reader.seek(SeekFrom::Start(abs))?;
            let mut buf = vec![0u8; chunk];
            self.reader.read_exact(&mut buf)?;
            out.extend_from_slice(&buf);
            pos += chunk as u64;
            if pos >= self.journal_size {
                pos = self.jhdr_size;
            }
        }
        Ok(out)
    }

    /// Decode the next transaction, or `None` at end / after a stop. On a
    /// corrupt block-list-header checksum, yields `Some(Err(..))` once and then
    /// stops.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<TransactionView>> {
        if self.done || self.cursor == self.end {
            return None;
        }
        match self.read_transaction() {
            Ok(view) => Some(Ok(view)),
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }

    fn read_transaction(&mut self) -> Result<TransactionView> {
        let start = self.cursor;
        // Read the block-list-header region.
        let hdr = self.read_wrapped(start, self.blhdr_size)?;

        let num_blocks = self.endian.read_u16(&hdr[2..4]) as usize;
        let bytes_used = self.endian.read_u32(&hdr[4..8]);
        let stored_cksum = self.endian.read_u32(&hdr[8..12]);
        let flags = self.endian.read_u32(&hdr[12..16]);
        let max_blocks = self.endian.read_u16(&hdr[0..2]) as usize;
        let sequence_num = self.endian.read_u32(&hdr[28..32]); // binfo[0].next

        // Verify the block-list-header checksum over the first 32 bytes with
        // the checksum field (offset 8) zeroed.
        let mut cksum_buf = hdr[..BLHDR_CHECKSUM_SIZE].to_vec();
        cksum_buf[8..12].copy_from_slice(&[0u8; 4]);
        if calc_checksum(&cksum_buf) != stored_cksum {
            return Err(JournalError::CorruptTransaction { sequence_num });
        }

        if num_blocks == 0 || num_blocks > max_blocks {
            return Err(JournalError::OutOfRange(format!(
                "num_blocks {num_blocks} out of range (max {max_blocks})"
            )));
        }
        if (bytes_used as usize) < self.blhdr_size || (bytes_used as u64) > self.journal_size {
            return Err(JournalError::OutOfRange(format!(
                "bytes_used {bytes_used} invalid"
            )));
        }

        // Decode the binfo array (skip binfo[0]).
        let mut infos: Vec<BlockInfo> = Vec::with_capacity(num_blocks.saturating_sub(1));
        for i in 1..num_blocks {
            let off = BLOCK_LIST_HEADER_PREFIX + i * BLOCK_INFO_SIZE;
            if off + BLOCK_INFO_SIZE > hdr.len() {
                return Err(JournalError::OutOfRange(
                    "binfo entry beyond block list header".into(),
                ));
            }
            infos.push(BlockInfo {
                bnum: self.endian.read_u64(&hdr[off..off + 8]),
                bsize: self.endian.read_u32(&hdr[off + 8..off + 12]),
                next: self.endian.read_u32(&hdr[off + 12..off + 16]),
            });
        }

        // Read the contiguous data region that follows the header.
        let data_len = bytes_used as usize - self.blhdr_size;
        let data = self.read_wrapped(self.wrap(start + self.blhdr_size as u64), data_len)?;

        // Slice the data per block, honoring killed entries and the
        // end-of-list sentinel.
        let mut blocks = Vec::new();
        let mut total_payload: u64 = 0;
        let mut data_pos = 0usize;
        for info in &infos {
            let bsize = info.bsize as usize;
            if data_pos + bsize > data.len() {
                return Err(JournalError::OutOfRange(
                    "block payload beyond transaction data".into(),
                ));
            }
            if info.bnum != KILLED_BLOCK && info.bsize != 0 {
                let target = info.bnum * self.jhdr_size;
                blocks.push((target, data[data_pos..data_pos + bsize].to_vec()));
                total_payload += bsize as u64;
            }
            data_pos += bsize;
        }

        // Advance to the next transaction.
        self.cursor = self.wrap(start + bytes_used as u64);

        Ok(TransactionView {
            sequence_num,
            flags,
            bytes_used,
            blocks,
            total_payload,
        })
    }
}

impl TransactionView {
    /// Reduce to a summary for display / statistics.
    pub fn summary(&self) -> TransactionSummary {
        let target_range = self.blocks.iter().fold(None, |acc, (off, data)| {
            let lo = *off;
            let hi = off + data.len() as u64;
            match acc {
                None => Some((lo, hi)),
                Some((amin, amax)) => Some((amin.min(lo), amax.max(hi))),
            }
        });
        TransactionSummary {
            sequence_num: self.sequence_num,
            block_count: self.blocks.len() as u32,
            total_bytes: self.total_payload,
            target_range,
        }
    }
}

/// Report from [`replay_journal`].
#[derive(Debug, Clone, Default)]
pub struct ReplayReport {
    pub transactions_applied: u32,
    pub blocks_written: u64,
    pub bytes_written: u64,
    pub last_sequence_num: u32,
    /// True when replay stopped early at a corrupt transaction (data up to the
    /// last good transaction was still applied — matches macOS behavior).
    pub partial: bool,
    pub summaries: Vec<TransactionSummary>,
}

/// Replay a dirty journal in place. For each transaction from `start -> end`:
/// verify its checksum, write every non-killed block to its real location
/// (`partition_offset + bnum * jhdr_size`), then advance `jh.start` past it and
/// rewrite the header. Advancing the header per transaction makes replay
/// idempotent — a crash mid-replay leaves the volume recoverable on retry.
///
/// Replay does **not** clear the journaled bit. The catalog is simply brought
/// current; the volume stays journaled.
pub fn replay_journal<RW: Read + Write + Seek>(
    disk: &mut RW,
    jib: &JournalInfoBlock,
    jh: &mut JournalHeader,
    partition_offset: u64,
) -> Result<ReplayReport> {
    if jib.is_external() {
        return Err(JournalError::ExternalJournal);
    }

    let mut report = ReplayReport {
        last_sequence_num: jh.sequence_num,
        ..Default::default()
    };

    // Normalize the wrapped start/end once so the header we persist is sane.
    jh.start = jh.normalized_start();
    jh.end = jh.normalized_end();

    loop {
        if jh.start == jh.end {
            break;
        }

        // Decode exactly one transaction at jh.start.
        let view = {
            let mut walker = JournalWalker::new(disk, partition_offset, jib, jh);
            match walker.next() {
                Some(Ok(view)) => {
                    // Capture where the walker advanced to (the next start).
                    let next_start = walker.cursor;
                    (view, next_start)
                }
                Some(Err(JournalError::CorruptTransaction { .. })) | None => {
                    report.partial = true;
                    break;
                }
                Some(Err(e)) => return Err(e),
            }
        };
        let (view, next_start) = view;

        // Apply each block to its real on-disk location.
        for (target, payload) in &view.blocks {
            disk.seek(SeekFrom::Start(partition_offset + *target))?;
            disk.write_all(payload)?;
            report.blocks_written += 1;
            report.bytes_written += payload.len() as u64;
        }
        disk.flush()?;

        report.transactions_applied += 1;
        report.last_sequence_num = view.sequence_num;
        report.summaries.push(view.summary());

        // Advance start past this transaction and persist the header so a
        // crash here doesn't re-apply the same transaction.
        jh.start = next_start;
        jh.sequence_num = view.sequence_num;
        write_journal_header(disk, jib, jh, partition_offset)?;
    }

    // Final: start == end (clean). Persist once more in case the loop body
    // didn't (e.g. partial stop).
    if jh.start != jh.end {
        jh.end = jh.start;
    }
    write_journal_header(disk, jib, jh, partition_offset)?;
    Ok(report)
}

/// Write the journal header block back to disk, recomputing its checksum.
fn write_journal_header<RW: Read + Write + Seek>(
    disk: &mut RW,
    jib: &JournalInfoBlock,
    jh: &JournalHeader,
    partition_offset: u64,
) -> Result<()> {
    let abs = partition_offset + jib.offset;
    // Preserve any vendor padding past the fixed header.
    disk.seek(SeekFrom::Start(abs))?;
    let mut orig = vec![0u8; jh.jhdr_size as usize];
    disk.read_exact(&mut orig)?;
    let bytes = jh.serialize(Some(&orig));
    disk.seek(SeekFrom::Start(abs))?;
    disk.write_all(&bytes)?;
    disk.flush()?;
    Ok(())
}

/// Buffers dirty 512-byte-aligned blocks during one high-level mutation and
/// emits them as a single journal transaction at commit time. Mirrors the
/// idempotent last-write-wins semantics of xnu's
/// `journal_modify_block_start/end`.
#[derive(Debug, Default)]
pub struct TransactionBuilder {
    /// `target byte offset (relative to partition) -> block bytes`. Keyed by
    /// offset so repeat writes to the same block coalesce (last write wins).
    dirty: BTreeMap<u64, Vec<u8>>,
    block_size: u32,
}

impl TransactionBuilder {
    /// `block_size` is the journal's `jhdr_size` — the unit `bnum` is scaled by
    /// and the granularity at which blocks are recorded.
    pub fn new(block_size: u32) -> Self {
        TransactionBuilder {
            dirty: BTreeMap::new(),
            block_size: block_size.max(1),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.dirty.is_empty()
    }

    pub fn block_count(&self) -> usize {
        self.dirty.len()
    }

    /// Record one `block_size`-aligned block. `offset` must be a multiple of
    /// `block_size`; `bytes` must be exactly `block_size` long.
    pub fn record_block(&mut self, offset: u64, bytes: &[u8]) {
        debug_assert_eq!(offset % self.block_size as u64, 0, "unaligned record_block");
        debug_assert_eq!(bytes.len(), self.block_size as usize, "wrong block length");
        self.dirty.insert(offset, bytes.to_vec());
    }

    /// Record an arbitrary byte range, splitting it into `block_size` chunks.
    /// The range must be block-aligned in both offset and length.
    pub fn record_range(&mut self, offset: u64, bytes: &[u8]) {
        let bs = self.block_size as usize;
        debug_assert_eq!(offset % self.block_size as u64, 0, "unaligned record_range");
        debug_assert_eq!(bytes.len() % bs, 0, "record_range length not block-aligned");
        for (i, chunk) in bytes.chunks(bs).enumerate() {
            self.dirty.insert(offset + (i * bs) as u64, chunk.to_vec());
        }
    }

    /// A preview of the blocks this builder would write, for the GUI.
    pub fn summary(&self, sequence_num: u32) -> TransactionSummary {
        let total: u64 = self.dirty.values().map(|v| v.len() as u64).sum();
        let target_range = self.dirty.iter().fold(None, |acc, (off, data)| {
            let hi = off + data.len() as u64;
            match acc {
                None => Some((*off, hi)),
                Some((amin, amax)) => Some((amin.min(*off), amax.max(hi))),
            }
        });
        TransactionSummary {
            sequence_num,
            block_count: self.dirty.len() as u32,
            total_bytes: total,
            target_range,
        }
    }

    /// Commit the buffered blocks as one transaction. The six-phase ordering
    /// matches xnu (TN1150): write the transaction body at `jh.end`, advance
    /// `jh.end`/`sequence_num` and persist the header, apply the blocks to
    /// their real locations, then advance `jh.start = jh.end` and persist
    /// again. A crash between header writes is recoverable by [`replay_journal`].
    pub fn commit<RW: Read + Write + Seek>(
        self,
        disk: &mut RW,
        jib: &JournalInfoBlock,
        jh: &mut JournalHeader,
        partition_offset: u64,
    ) -> Result<()> {
        if jib.is_external() {
            return Err(JournalError::ExternalJournal);
        }
        if self.dirty.is_empty() {
            return Ok(());
        }

        // Phases 1-4: write the transaction body into the journal and advance
        // `end`/`sequence_num`. This is the commit point — after the header is
        // persisted the transaction is durable and replay will re-apply it.
        self.write_to_journal(disk, jib, jh, partition_offset)?;

        // Phase 5: apply blocks to their real locations.
        for (offset, bytes) in &self.dirty {
            disk.seek(SeekFrom::Start(partition_offset + *offset))?;
            disk.write_all(bytes)?;
        }
        disk.flush()?;

        // Phase 6: advance start to end — transaction fully applied, clean.
        jh.start = jh.end;
        write_journal_header(disk, jib, jh, partition_offset)?;
        Ok(())
    }

    /// Phases 1-4 of [`commit`]: build the transaction body, write it at
    /// `jh.end` (wrapping), advance `jh.end`/`sequence_num`, and persist the
    /// header. Leaves the journal *dirty* (`start != end`) — the blocks have
    /// not yet been applied to their real locations. [`replay_journal`] (or
    /// the rest of [`commit`]) finishes the job. Exposed for tests that need a
    /// dirty journal and for callers that batch the checkpoint separately.
    pub fn write_to_journal<RW: Read + Write + Seek>(
        &self,
        disk: &mut RW,
        jib: &JournalInfoBlock,
        jh: &mut JournalHeader,
        partition_offset: u64,
    ) -> Result<()> {
        if jib.is_external() {
            return Err(JournalError::ExternalJournal);
        }
        if self.dirty.is_empty() {
            return Ok(());
        }

        let blhdr_size = jh.blhdr_size as usize;
        let endian = jh.endian;
        let num_data = self.dirty.len();
        let num_blocks = num_data + 1; // + binfo[0]
        let max_blocks = blhdr_size / BLOCK_INFO_SIZE - 1;
        if num_data > max_blocks {
            return Err(JournalError::OutOfRange(format!(
                "transaction has {num_data} blocks, exceeds journal max {max_blocks}"
            )));
        }

        // Build the block-list-header region.
        let mut hdr = vec![0u8; blhdr_size];
        // Fill bytes past the checksum region with 0x5a like xnu does (purely
        // cosmetic / for visibility in dumps).
        for b in hdr.iter_mut().skip(BLHDR_CHECKSUM_SIZE) {
            *b = 0x5a;
        }

        let bs = self.block_size;
        let mut data_region: Vec<u8> = Vec::new();
        // binfo[0] is the header entry, so data blocks start at index 1.
        for (idx, (offset, bytes)) in (1usize..).zip(self.dirty.iter()) {
            let bnum = offset / bs as u64;
            let info_off = BLOCK_LIST_HEADER_PREFIX + idx * BLOCK_INFO_SIZE;
            endian.write_u64(&mut hdr[info_off..info_off + 8], bnum);
            endian.write_u32(&mut hdr[info_off + 8..info_off + 12], bytes.len() as u32);
            endian.write_u32(&mut hdr[info_off + 12..info_off + 16], 0); // no per-block cksum
            data_region.extend_from_slice(bytes);
        }

        let bytes_used = (blhdr_size + data_region.len()) as u32;

        // Fixed header fields.
        endian.write_u16(&mut hdr[0..2], num_blocks as u16); // max_blocks
        endian.write_u16(&mut hdr[2..4], num_blocks as u16); // num_blocks
        endian.write_u32(&mut hdr[4..8], bytes_used);
        endian.write_u32(&mut hdr[8..12], 0); // checksum placeholder
        endian.write_u32(&mut hdr[12..16], BLHDR_FIRST_HEADER); // no CHECK_CHECKSUMS
                                                                // binfo[0]: bnum/bsize unused on replay; next = sequence number.
        let seq = jh.sequence_num.wrapping_add(1);
        endian.write_u64(&mut hdr[16..24], 0); // binfo[0].bnum
        endian.write_u32(&mut hdr[24..28], 0); // binfo[0].bsize
        endian.write_u32(&mut hdr[28..32], seq); // binfo[0].next = sequence_num

        // Checksum over the first 32 bytes with the checksum field zeroed.
        let mut cksum_buf = hdr[..BLHDR_CHECKSUM_SIZE].to_vec();
        cksum_buf[8..12].copy_from_slice(&[0u8; 4]);
        let cksum = calc_checksum(&cksum_buf);
        endian.write_u32(&mut hdr[8..12], cksum);

        // Assemble the full transaction body and write it at jh.end, wrapping.
        let mut body = hdr;
        body.extend_from_slice(&data_region);
        let write_start = if jh.end == jh.size {
            jh.jhdr_size as u64
        } else {
            jh.end
        };
        write_journal_wrapped(disk, jib, jh, partition_offset, write_start, &body)?;

        // Advance end + sequence, persist header (commit point).
        let mut new_end = write_start + body.len() as u64;
        if new_end >= jh.size {
            new_end = jh.jhdr_size as u64 + (new_end - jh.size);
        }
        jh.end = new_end;
        jh.sequence_num = seq;
        write_journal_header(disk, jib, jh, partition_offset)?;
        Ok(())
    }
}

/// Write `body` into the circular journal buffer starting at journal-relative
/// `rel`, wrapping at `jh.size` back to `jhdr_size`.
fn write_journal_wrapped<RW: Read + Write + Seek>(
    disk: &mut RW,
    jib: &JournalInfoBlock,
    jh: &JournalHeader,
    partition_offset: u64,
    rel: u64,
    body: &[u8],
) -> Result<()> {
    let mut pos = rel;
    let mut written = 0usize;
    while written < body.len() {
        let space = (jh.size - pos) as usize;
        let chunk = (body.len() - written).min(space);
        let abs = partition_offset + jib.offset + pos;
        disk.seek(SeekFrom::Start(abs))?;
        disk.write_all(&body[written..written + chunk])?;
        written += chunk;
        pos += chunk as u64;
        if pos >= jh.size {
            pos = jh.jhdr_size as u64;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A minimal synthetic in-FS journal: a JIB at block 0, a journal header,
    /// and a circular buffer, all packed into one `Vec<u8>` "partition".
    struct SyntheticJournal {
        disk: Vec<u8>,
        partition_offset: u64,
        jib: JournalInfoBlock,
        header: JournalHeader,
    }

    const JHDR_SIZE: u32 = 512;
    const BLHDR_SIZE: u32 = 512;
    const JOURNAL_SIZE: u64 = 64 * 1024;
    const JIB_OFFSET: u64 = 4096; // where the journal header lives

    fn build_journal(endian: JournalEndian, part_off: u64) -> SyntheticJournal {
        let total = part_off + JIB_OFFSET + JOURNAL_SIZE + 4096;
        let disk = vec![0u8; total as usize];
        let jib = JournalInfoBlock {
            flags: KJI_JOURNAL_IN_FS_MASK,
            offset: JIB_OFFSET,
            size: JOURNAL_SIZE,
        };
        let header = JournalHeader {
            endian,
            magic: JOURNAL_HEADER_MAGIC,
            start: JHDR_SIZE as u64,
            end: JHDR_SIZE as u64,
            size: JOURNAL_SIZE,
            blhdr_size: BLHDR_SIZE,
            checksum: 0,
            jhdr_size: JHDR_SIZE,
            sequence_num: 0,
        };
        let mut s = SyntheticJournal {
            disk,
            partition_offset: part_off,
            jib,
            header,
        };
        s.write_header();
        s
    }

    impl SyntheticJournal {
        fn write_header(&mut self) {
            let abs = (self.partition_offset + self.jib.offset) as usize;
            let bytes = self.header.serialize(None);
            self.disk[abs..abs + bytes.len()].copy_from_slice(&bytes);
        }

        fn cursor(&mut self) -> Cursor<&mut Vec<u8>> {
            Cursor::new(&mut self.disk)
        }

        /// Append one transaction writing `(target_byte_offset, payload)` pairs
        /// to the journal only (leaves the journal *dirty* — not yet applied),
        /// via the production `write_to_journal` path.
        fn dirty_blocks(&mut self, blocks: &[(u64, Vec<u8>)]) {
            let mut builder = TransactionBuilder::new(self.header.jhdr_size);
            for (off, data) in blocks {
                builder.record_block(*off, data);
            }
            let jib = self.jib.clone();
            let mut jh = self.header.clone();
            let part = self.partition_offset;
            {
                let mut cur = self.cursor();
                builder
                    .write_to_journal(&mut cur, &jib, &mut jh, part)
                    .unwrap();
            }
            self.header = jh;
        }

        /// Full `commit` lifecycle (write + apply + checkpoint), leaving the
        /// journal clean.
        #[allow(dead_code)]
        fn commit_blocks(&mut self, blocks: &[(u64, Vec<u8>)]) {
            let mut builder = TransactionBuilder::new(self.header.jhdr_size);
            for (off, data) in blocks {
                builder.record_block(*off, data);
            }
            let jib = self.jib.clone();
            let mut jh = self.header.clone();
            let part = self.partition_offset;
            {
                let mut cur = self.cursor();
                builder.commit(&mut cur, &jib, &mut jh, part).unwrap();
            }
            self.header = jh;
        }

        fn read_block_at(&self, target: u64, len: usize) -> Vec<u8> {
            let abs = (self.partition_offset + target) as usize;
            self.disk[abs..abs + len].to_vec()
        }
    }

    #[test]
    fn header_roundtrip_both_endians() {
        for endian in [JournalEndian::Big, JournalEndian::Little] {
            let s = build_journal(endian, 0);
            let abs = (s.partition_offset + s.jib.offset) as usize;
            let parsed = JournalHeader::parse(&s.disk[abs..abs + JHDR_SIZE as usize]).unwrap();
            assert_eq!(parsed.endian, endian);
            assert_eq!(parsed.magic, JOURNAL_HEADER_MAGIC);
            assert_eq!(parsed.size, JOURNAL_SIZE);
            assert_eq!(parsed.jhdr_size, JHDR_SIZE);
        }
    }

    #[test]
    fn walk_two_transactions_both_endians() {
        for endian in [JournalEndian::Big, JournalEndian::Little] {
            let mut s = build_journal(endian, 8192);
            // Two transactions, each one block, targeting different locations.
            let blk_a = vec![0xAA; JHDR_SIZE as usize];
            let blk_b = vec![0xBB; JHDR_SIZE as usize];
            s.dirty_blocks(&[(JIB_OFFSET + JOURNAL_SIZE + 1024, blk_a.clone())]);
            s.dirty_blocks(&[(JIB_OFFSET + JOURNAL_SIZE + 2048, blk_b.clone())]);

            // Re-read the header from disk and walk.
            let abs = (s.partition_offset + s.jib.offset) as usize;
            let jh = JournalHeader::parse(&s.disk[abs..abs + JHDR_SIZE as usize]).unwrap();
            let jib = s.jib.clone();
            let part = s.partition_offset;
            let mut cur = s.cursor();
            let mut walker = JournalWalker::new(&mut cur, part, &jib, &jh);

            let t1 = walker.next().unwrap().unwrap();
            assert_eq!(t1.sequence_num, 1);
            assert_eq!(t1.blocks.len(), 1);
            assert_eq!(t1.blocks[0].1, blk_a);

            let t2 = walker.next().unwrap().unwrap();
            assert_eq!(t2.sequence_num, 2);
            assert_eq!(t2.blocks[0].1, blk_b);

            assert!(walker.next().is_none());
        }
    }

    #[test]
    fn bad_magic_rejected() {
        let mut s = build_journal(JournalEndian::Big, 0);
        let abs = (s.partition_offset + s.jib.offset) as usize;
        s.disk[abs] ^= 0xFF; // corrupt magic
        let err = JournalHeader::parse(&s.disk[abs..abs + JHDR_SIZE as usize]).unwrap_err();
        assert!(matches!(
            err,
            JournalError::BadMagic(_) | JournalError::OutOfRange(_) | JournalError::BadEndian(_)
        ));
    }

    #[test]
    fn truncated_header_rejected() {
        let s = build_journal(JournalEndian::Big, 0);
        let abs = (s.partition_offset + s.jib.offset) as usize;
        let err = JournalHeader::parse(&s.disk[abs..abs + 10]).unwrap_err();
        assert!(matches!(err, JournalError::OutOfRange(_)));
    }

    #[test]
    fn out_of_range_end_rejected() {
        let mut s = build_journal(JournalEndian::Big, 0);
        s.header.end = JOURNAL_SIZE + 4096; // past the buffer
        s.write_header();
        let abs = (s.partition_offset + s.jib.offset) as usize;
        let err = JournalHeader::parse(&s.disk[abs..abs + JHDR_SIZE as usize]).unwrap_err();
        assert!(matches!(err, JournalError::OutOfRange(_)));
    }

    #[test]
    fn jib_parse_flags() {
        let mut block = vec![0u8; 512];
        BigEndian::write_u32(&mut block[0..4], KJI_JOURNAL_IN_FS_MASK);
        BigEndian::write_u64(&mut block[36..44], 4096);
        BigEndian::write_u64(&mut block[44..52], 64 * 1024);
        let jib = JournalInfoBlock::parse(&block).unwrap();
        assert!(jib.is_in_fs());
        assert!(!jib.is_external());
        assert_eq!(jib.offset, 4096);
        assert_eq!(jib.size, 64 * 1024);
    }

    // ---- Step 25: replay -------------------------------------------------

    #[test]
    fn replay_applies_dirty_transaction() {
        for endian in [JournalEndian::Big, JournalEndian::Little] {
            let mut s = build_journal(endian, 8192);
            let target = JIB_OFFSET + JOURNAL_SIZE + 1024;
            let payload = vec![0x42; JHDR_SIZE as usize];
            // Write to journal but DON'T apply — a dirty volume.
            s.dirty_blocks(&[(target, payload.clone())]);
            // The real location is still zero.
            assert_eq!(
                s.read_block_at(target, JHDR_SIZE as usize),
                vec![0u8; JHDR_SIZE as usize]
            );

            let jib = s.jib.clone();
            let mut jh = s.header.clone();
            let part = s.partition_offset;
            let report = {
                let mut cur = s.cursor();
                replay_journal(&mut cur, &jib, &mut jh, part).unwrap()
            };
            assert_eq!(report.transactions_applied, 1);
            assert_eq!(report.blocks_written, 1);
            assert!(!report.partial);
            // Now the real location carries the journaled bytes, and the
            // journal is clean (start == end).
            assert_eq!(s.read_block_at(target, JHDR_SIZE as usize), payload);
            assert_eq!(jh.start, jh.end);
        }
    }

    #[test]
    fn replay_is_idempotent() {
        let mut s = build_journal(JournalEndian::Big, 0);
        let target = JIB_OFFSET + JOURNAL_SIZE + 512;
        let payload = vec![0x7e; JHDR_SIZE as usize];
        s.dirty_blocks(&[(target, payload.clone())]);

        let jib = s.jib.clone();
        let part = s.partition_offset;

        // First replay.
        let mut jh = s.header.clone();
        {
            let mut cur = s.cursor();
            replay_journal(&mut cur, &jib, &mut jh, part).unwrap();
        }
        s.header = jh;
        // Re-read header from disk, replay again — nothing to do, no double-apply.
        let abs = (part + jib.offset) as usize;
        let mut jh2 = JournalHeader::parse(&s.disk[abs..abs + JHDR_SIZE as usize]).unwrap();
        let report2 = {
            let mut cur = s.cursor();
            replay_journal(&mut cur, &jib, &mut jh2, part).unwrap()
        };
        assert_eq!(report2.transactions_applied, 0);
        assert_eq!(s.read_block_at(target, JHDR_SIZE as usize), payload);
    }

    #[test]
    fn replay_stops_at_corrupt_transaction() {
        let mut s = build_journal(JournalEndian::Big, 0);
        let t1 = JIB_OFFSET + JOURNAL_SIZE + 512;
        let t2 = JIB_OFFSET + JOURNAL_SIZE + 1024;
        let p1 = vec![0x11; JHDR_SIZE as usize];
        let p2 = vec![0x22; JHDR_SIZE as usize];
        s.dirty_blocks(&[(t1, p1.clone())]);
        // Record where the second transaction's block-list-header lands, then
        // corrupt its checksum after writing it.
        let second_blhdr = (s.partition_offset + s.jib.offset + s.header.end) as usize;
        s.dirty_blocks(&[(t2, p2.clone())]);
        // Flip a byte inside the second header's checksum-covered region.
        s.disk[second_blhdr + 4] ^= 0xFF;

        let jib = s.jib.clone();
        let mut jh = s.header.clone();
        let part = s.partition_offset;
        let report = {
            let mut cur = s.cursor();
            replay_journal(&mut cur, &jib, &mut jh, part).unwrap()
        };
        assert_eq!(report.transactions_applied, 1);
        assert!(report.partial);
        // First transaction applied; second did not land.
        assert_eq!(s.read_block_at(t1, JHDR_SIZE as usize), p1);
        assert_eq!(
            s.read_block_at(t2, JHDR_SIZE as usize),
            vec![0u8; JHDR_SIZE as usize]
        );
    }

    // ---- Step 26: transaction recorder ----------------------------------

    #[test]
    fn recorder_roundtrips_three_blocks() {
        let mut s = build_journal(JournalEndian::Big, 4096);
        let mut builder = TransactionBuilder::new(s.header.jhdr_size);
        let base = JIB_OFFSET + JOURNAL_SIZE;
        let blocks: Vec<(u64, Vec<u8>)> = vec![
            (base, vec![1u8; JHDR_SIZE as usize]),
            (base + 512, vec![2u8; JHDR_SIZE as usize]),
            (base + 1024, vec![3u8; JHDR_SIZE as usize]),
        ];
        for (off, data) in &blocks {
            builder.record_block(*off, data);
        }
        assert_eq!(builder.block_count(), 3);

        let jib = s.jib.clone();
        let mut jh = s.header.clone();
        let part = s.partition_offset;
        {
            let mut cur = s.cursor();
            // write_to_journal so we can walk it back as a dirty txn.
            builder
                .write_to_journal(&mut cur, &jib, &mut jh, part)
                .unwrap();
        }

        let mut cur = s.cursor();
        let mut walker = JournalWalker::new(&mut cur, part, &jib, &jh);
        let txn = walker.next().unwrap().unwrap();
        assert_eq!(txn.sequence_num, 1);
        assert_eq!(txn.blocks.len(), 3);
        for (i, (off, data)) in blocks.iter().enumerate() {
            assert_eq!(txn.blocks[i].0, *off);
            assert_eq!(&txn.blocks[i].1, data);
        }
        assert!(walker.next().is_none());
    }

    #[test]
    fn recorder_last_write_wins() {
        let mut s = build_journal(JournalEndian::Big, 0);
        let mut builder = TransactionBuilder::new(s.header.jhdr_size);
        let target = JIB_OFFSET + JOURNAL_SIZE;
        builder.record_block(target, &vec![0xAA; JHDR_SIZE as usize]);
        builder.record_block(target, &vec![0xBB; JHDR_SIZE as usize]); // overwrite
        assert_eq!(builder.block_count(), 1);

        let jib = s.jib.clone();
        let mut jh = s.header.clone();
        let part = s.partition_offset;
        {
            let mut cur = s.cursor();
            builder.commit(&mut cur, &jib, &mut jh, part).unwrap();
        }
        assert_eq!(
            s.read_block_at(target, JHDR_SIZE as usize),
            vec![0xBB; JHDR_SIZE as usize]
        );
    }

    #[test]
    fn recorder_wraparound_splits_and_replays() {
        // Small journal so the transaction body straddles the wrap point.
        let part = 0u64;
        let jhdr = 512u32;
        let jsize = 8 * 1024u64;
        let jib_off = 4096u64;
        let disk = vec![0u8; (part + jib_off + jsize + 8192) as usize];
        let jib = JournalInfoBlock {
            flags: KJI_JOURNAL_IN_FS_MASK,
            offset: jib_off,
            size: jsize,
        };
        let mut header = JournalHeader {
            endian: JournalEndian::Big,
            magic: JOURNAL_HEADER_MAGIC,
            // Start near the end so the body wraps.
            start: jsize - 1024,
            end: jsize - 1024,
            size: jsize,
            blhdr_size: 512,
            checksum: 0,
            jhdr_size: jhdr,
            sequence_num: 0,
        };
        let mut s = SyntheticJournal {
            disk,
            partition_offset: part,
            jib: jib.clone(),
            header: header.clone(),
        };
        s.write_header();
        header = s.header.clone();

        // A two-block transaction = 512 (hdr) + 2*512 = 1536 bytes, which from
        // end = jsize-1024 runs past jsize and wraps.
        let target = jib_off + jsize; // beyond journal region, into the tail
        let mut builder = TransactionBuilder::new(jhdr);
        builder.record_block(target, &vec![0xC1; 512]);
        builder.record_block(target + 512, &vec![0xC2; 512]);
        {
            let mut cur = s.cursor();
            builder
                .write_to_journal(&mut cur, &jib, &mut header, part)
                .unwrap();
        }
        s.header = header.clone();

        // Walk it back across the wrap.
        {
            let mut cur = s.cursor();
            let mut walker = JournalWalker::new(&mut cur, part, &jib, &header);
            let txn = walker.next().unwrap().unwrap();
            assert_eq!(txn.blocks.len(), 2);
            assert_eq!(txn.blocks[0].1, vec![0xC1; 512]);
            assert_eq!(txn.blocks[1].1, vec![0xC2; 512]);
        }

        // And replay applies both across the wrap.
        let mut jh = header.clone();
        let report = {
            let mut cur = s.cursor();
            replay_journal(&mut cur, &jib, &mut jh, part).unwrap()
        };
        assert_eq!(report.blocks_written, 2);
        assert_eq!(s.read_block_at(target, 512), vec![0xC1; 512]);
        assert_eq!(s.read_block_at(target + 512, 512), vec![0xC2; 512]);
    }
}
