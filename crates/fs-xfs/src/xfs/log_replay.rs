//! XFS log recovery: replay the committed transactions in the internal log, as a mount would,
//! or (`-L` style) throw the log away. See `docs/xfs_log_replay.md` for the format notes.
//!
//! Written clean-room from the public "XFS Algorithms & Data Structures" format description,
//! with the numeric flag values and the recovered byte layout pinned by black-box comparison
//! against `xfs_logprint` and the kernel's own recovery of the same images.
//!
//! The walk runs twice over the records from the tail to the head. The first pass writes
//! nothing: it collects buffer cancellations and intent / done pairs, and refuses a log holding
//! anything this module cannot finish (unfinished rmap / refcount / bmap / attr / exchange
//! intents). The second pass applies each transaction when its commit record arrives.
//! Only open transactions are held in memory. Unfinished extent-free intents are reported, not
//! applied: the blocks they name are left allocated but unowned, which the repair that follows
//! (the free-space rebuild) returns to the free space.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom, Write};

use disk_core::filesystem::FilesystemError;

use super::ag::{XfsAgf, XfsAgi};
use super::bmap::fsblock_to_partition_byte;
use super::sb::XfsSuperblock;
use super::v5_crc::stamp_crc;

const BB: u64 = 512;
const HEADER_MAGIC: u32 = 0xFEED_BABE;
/// `h_cycle_data` slots per header sector (`XLOG_HEADER_CYCLE_SIZE / BBSIZE`).
const CYCLE_SLOTS: usize = 64;
const HEADER_CYCLE_SIZE: u32 = 32 * 1024;
const MAX_RECORD_BYTES: u32 = 256 * 1024;
/// Cap on the bytes of uncommitted transactions held while walking (no more than the log).
const MAX_OPEN_BYTES: u64 = 1 << 30;
/// Largest buffer a log item may name: 1 MiB, past any XFS metadata block.
const MAX_BUF_SECTORS: u64 = 2048;
const INODES_PER_CHUNK: u64 = 64;

const CLIENT_TRANS: u8 = 0x69;
const OP_START: u8 = 0x01;
const OP_COMMIT: u8 = 0x02;
const OP_WAS_CONT: u8 = 0x08;
const OP_UNMOUNT: u8 = 0x20;

const TRANS_MAGIC: u32 = 0x5452_414E;
const LI_EFI: u16 = 0x1236;
const LI_EFD: u16 = 0x1237;
const LI_INODE: u16 = 0x123b;
const LI_BUF: u16 = 0x123c;
const LI_DQUOT: u16 = 0x123d;
const LI_QUOTAOFF: u16 = 0x123e;
const LI_ICREATE: u16 = 0x123f;
const LI_RT_EFI: u16 = 0x124a;
const LI_RT_EFD: u16 = 0x124b;

const BLF_INODE_BUF: u16 = 0x1;
const BLF_CANCEL: u16 = 0x2;
/// Quota-buffer flags (user, project, group); see `quota_kind`.
const BLF_DQUOT_BUFS: u16 = 0x4 | 0x8 | 0x10;

/// `sb_qflags` accounting bits for user, project and group quotas, and every "checked" bit.
const QF_UACCT: u16 = 0x1;
const QF_PACCT: u16 = 0x8;
const QF_GACCT: u16 = 0x40;
const QF_ALL_CHKD: u16 = 0x4 | 0x20 | 0x100 | 0x400;
/// `xfs_dqblk` size on v5, with its CRC and LSN offsets.
const DQBLK_V5: usize = 136;
const DQ_CRC: usize = 108;
const DQ_LSN: usize = 112;
/// Size of the logged `xfs_disk_dquot`.
const DISK_DQUOT: usize = 104;
const BLF_CHUNK: usize = 128;

const ILOG_CORE: u32 = 0x1;
const ILOG_DDATA: u32 = 0x2;
const ILOG_DEXT: u32 = 0x4;
const ILOG_DBROOT: u32 = 0x8;
const ILOG_DEV: u32 = 0x10;
const ILOG_UUID: u32 = 0x20;
const ILOG_ADATA: u32 = 0x40;
const ILOG_AEXT: u32 = 0x80;
const ILOG_ABROOT: u32 = 0x100;
const ILOG_DFORK: u32 = ILOG_DDATA | ILOG_DEXT | ILOG_DBROOT;
const ILOG_AFORK: u32 = ILOG_ADATA | ILOG_AEXT | ILOG_ABROOT;

/// Offset of `di_next_unlinked` in an on-disk inode.
const DI_NEXT_UNLINKED: usize = 96;
const DI_CRC: usize = 100;
const DI_LSN: usize = 112;

/// What replay did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplayReport {
    /// Log records walked, tail to head.
    pub records: usize,
    /// Committed transactions applied.
    pub transactions: usize,
    /// Transactions with no commit record at the head; a mount drops these too.
    pub incomplete: usize,
    pub buffers: usize,
    /// Buffer writes skipped because the blocks were freed later in the log.
    pub cancelled: usize,
    pub inodes: usize,
    pub inode_chunks: usize,
    /// Extent-free intents with no done record: blocks the repair's free-space rebuild reclaims.
    pub unfinished_frees: usize,
    /// Quota records written back.
    pub quotas: usize,
    /// Quota items not replayed (malformed, or for a quota turned off later in the log); the
    /// "checked" flags are cleared instead, so the next quota mount recounts usage.
    pub quota_recount: bool,
}

/// What [`replay_log`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayOutcome {
    /// Nothing to replay: the head is an unmount record, or the log is blank or external.
    Clean,
    /// The log was replayed and a clean unmount record written in its place.
    Replayed(ReplayReport),
    /// The log cannot be replayed faithfully; nothing was written. The reason says why.
    Unrecoverable(String),
}

/// Byte order of the log's host-endian fields, from `h_fmt`.
#[derive(Clone, Copy)]
struct En {
    big: bool,
}

impl En {
    fn u16(self, b: &[u8], o: usize) -> u16 {
        let a = [b[o], b[o + 1]];
        if self.big {
            u16::from_be_bytes(a)
        } else {
            u16::from_le_bytes(a)
        }
    }
    fn u32(self, b: &[u8], o: usize) -> u32 {
        let a = [b[o], b[o + 1], b[o + 2], b[o + 3]];
        if self.big {
            u32::from_be_bytes(a)
        } else {
            u32::from_le_bytes(a)
        }
    }
    fn u64(self, b: &[u8], o: usize) -> u64 {
        let a: [u8; 8] = b[o..o + 8].try_into().expect("8 bytes");
        if self.big {
            u64::from_be_bytes(a)
        } else {
            u64::from_le_bytes(a)
        }
    }
}

fn be32(b: &[u8], o: usize) -> u32 {
    u32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}

fn be64(b: &[u8], o: usize) -> u64 {
    u64::from_be_bytes(b[o..o + 8].try_into().expect("8 bytes"))
}

fn corrupt(msg: String) -> FilesystemError {
    FilesystemError::Parse(msg)
}

/// The internal log's place on the device.
struct Log {
    start: u64,
    bbs: u64,
}

impl Log {
    fn new(partition_offset: u64, sb: &XfsSuperblock) -> Self {
        Log {
            start: partition_offset
                + fsblock_to_partition_byte(sb.logstart, sb.agblocks, sb.agblklog, sb.blocksize),
            bbs: u64::from(sb.logblocks) * u64::from(sb.blocksize) / BB,
        }
    }

    /// Read `n` basic blocks from `blk`, wrapping at the end of the log.
    fn read<R: Read + Seek>(&self, dev: &mut R, blk: u64, n: u64) -> std::io::Result<Vec<u8>> {
        let mut out = vec![0u8; (n * BB) as usize];
        let mut done = 0u64;
        while done < n {
            let at = (blk + done) % self.bbs;
            let run = (self.bbs - at).min(n - done);
            dev.seek(SeekFrom::Start(self.start + at * BB))?;
            dev.read_exact(&mut out[(done * BB) as usize..((done + run) * BB) as usize])?;
            done += run;
        }
        Ok(out)
    }
}

/// One log record with its cycle stamps undone.
struct Record {
    blk: u64,
    lsn: u64,
    tail_lsn: u64,
    num_ops: u32,
    en: En,
    data: Vec<u8>,
    /// Where the next record starts.
    next: u64,
}

/// Read the record whose header sits at `blk`; `None` if there is none or it was torn.
fn read_record<R: Read + Seek>(
    dev: &mut R,
    log: &Log,
    blk: u64,
) -> Result<Option<Record>, FilesystemError> {
    let h = log.read(dev, blk, 1)?;
    if be32(&h, 0) != HEADER_MAGIC {
        return Ok(None);
    }
    let cycle = be32(&h, 4);
    let version = be32(&h, 8);
    let len = be32(&h, 12);
    let size = be32(&h, 320);
    if !(version == 1 || version == 2) || len > MAX_RECORD_BYTES || cycle == 0 {
        return Ok(None);
    }
    if version == 2 && size > MAX_RECORD_BYTES {
        return Ok(None);
    }
    let header_bbs = if version == 2 && size > HEADER_CYCLE_SIZE {
        u64::from(size.div_ceil(HEADER_CYCLE_SIZE))
    } else {
        1
    };
    let data_bbs = u64::from(len).div_ceil(BB);
    // Every data sector needs a cycle-data slot in one of the headers.
    if data_bbs > header_bbs * CYCLE_SLOTS as u64 || header_bbs + data_bbs > log.bbs {
        return Ok(None);
    }
    let headers = log.read(dev, blk, header_bbs)?;
    let mut data = log.read(dev, blk + header_bbs, data_bbs)?;
    for i in 0..data_bbs as usize {
        let (hdr, slot) = (i / CYCLE_SLOTS, i % CYCLE_SLOTS);
        let hb = &headers[hdr * BB as usize..(hdr + 1) * BB as usize];
        // Slot data starts at h_cycle_data (44) in the first header, after xh_cycle (4) in the rest.
        let base = if hdr == 0 { 44 } else { 4 };
        if hdr > 0 && be32(hb, 0) != cycle {
            return Ok(None);
        }
        let s = &mut data[i * BB as usize..i * BB as usize + 4];
        if be32(s, 0) != cycle {
            return Ok(None); // a sector from an older pass: the write never finished
        }
        s.copy_from_slice(&hb[base + 4 * slot..base + 4 * slot + 4]);
    }
    data.truncate(len as usize);
    Ok(Some(Record {
        blk,
        lsn: be64(&h, 16),
        tail_lsn: be64(&h, 24),
        num_ops: be32(&h, 40),
        // h_fmt: 1 little-endian Linux, 2 big-endian Linux, 3 big-endian IRIX.
        en: En {
            big: matches!(be32(&h, 300), 2 | 3),
        },
        data,
        next: (blk + header_bbs + data_bbs) % log.bbs,
    }))
}

/// The newest complete record: the highest-LSN header whose sectors all carry its cycle.
fn find_head<R: Read + Seek>(dev: &mut R, log: &Log) -> Result<Head, FilesystemError> {
    let mut found = scan_headers(dev, log)?;
    found.sort_unstable_by(|a, b| b.cmp(a));
    let newest = found.first().map(|&(lsn, _)| lsn);
    for (_, blk) in found {
        if let Some(r) = read_record(dev, log, blk)? {
            return Ok(Head::Record(Box::new(r)));
        }
    }
    Ok(match newest {
        Some(lsn) => Head::Garbled { newest_lsn: lsn },
        None => Head::Blank,
    })
}

/// What the head search found.
enum Head {
    Record(Box<Record>),
    /// Headers exist but none is a complete record: the log is damaged, not clean.
    Garbled {
        newest_lsn: u64,
    },
    /// No headers at all: a log that was never written.
    Blank,
}

/// Every block that looks like a record header, as (LSN, block).
fn scan_headers<R: Read + Seek>(
    dev: &mut R,
    log: &Log,
) -> Result<Vec<(u64, u64)>, FilesystemError> {
    let mut found: Vec<(u64, u64)> = Vec::new();
    let mut buf = vec![0u8; 1 << 20];
    let per = buf.len() as u64 / BB;
    let mut bb = 0u64;
    dev.seek(SeekFrom::Start(log.start))?;
    while bb < log.bbs {
        let n = (log.bbs - bb).min(per);
        dev.read_exact(&mut buf[..(n * BB) as usize])?;
        for i in 0..n {
            let s = &buf[(i * BB) as usize..((i + 1) * BB) as usize];
            let lsn = be64(s, 16);
            // A real header names its own block in the low half of its LSN.
            if be32(s, 0) == HEADER_MAGIC && lsn & 0xFFFF_FFFF == bb + i {
                found.push((lsn, bb + i));
            }
        }
        bb += n;
    }
    Ok(found)
}

fn is_unmount(r: &Record) -> bool {
    r.num_ops == 1 && r.data.len() >= 12 && r.data[9] & OP_UNMOUNT != 0
}

/// A transaction being reassembled from its operations.
#[derive(Default)]
struct Trans {
    regions: Vec<Vec<u8>>,
    /// LSN of the record holding the start operation.
    lsn: u64,
}

/// Walk the records from the tail to `head`, handing each committed transaction to `on_commit`.
fn walk<R: Read + Seek>(
    dev: &mut R,
    log: &Log,
    head: &Record,
    mut on_commit: impl FnMut(&mut R, &Trans, En) -> Result<(), FilesystemError>,
) -> Result<(usize, usize), FilesystemError> {
    let mut open: HashMap<u32, Trans> = HashMap::new();
    let mut open_bytes = 0usize;
    let mut blk = head.tail_lsn & 0xFFFF_FFFF;
    let mut records = 0usize;
    let mut last_lsn: Option<u64> = None;
    loop {
        if records as u64 > log.bbs {
            return Err(corrupt("XFS log walk did not reach the head".into()));
        }
        let rec = read_record(dev, log, blk)?.ok_or_else(|| {
            corrupt(format!(
                "XFS log record at block {blk}, between the tail and the head, is missing or torn"
            ))
        })?;
        records += 1;
        // The walk must start at the tail's record and climb in LSN to the head.
        let in_order = match last_lsn {
            None => rec.lsn == head.tail_lsn,
            Some(prev) => rec.lsn > prev && rec.lsn <= head.lsn,
        };
        if !in_order {
            return Err(corrupt(format!(
                "XFS log record at block {blk} is out of sequence between the tail and the head"
            )));
        }
        last_lsn = Some(rec.lsn);
        let d = &rec.data;
        let mut p = 0usize;
        for _ in 0..rec.num_ops {
            if p + 12 > d.len() {
                return Err(corrupt(format!(
                    "XFS log record {} overruns its length",
                    rec.blk
                )));
            }
            let tid = be32(d, p);
            let len = be32(d, p + 4) as usize;
            let (client, flags) = (d[p + 8], d[p + 9]);
            let body = p + 12;
            if len > d.len() - body {
                return Err(corrupt(format!(
                    "XFS log op in record {} overruns it",
                    rec.blk
                )));
            }
            p = body + len;
            if client != CLIENT_TRANS {
                continue;
            }
            if flags & OP_START != 0 {
                open.insert(
                    tid,
                    Trans {
                        regions: Vec::new(),
                        lsn: rec.lsn,
                    },
                );
            }
            // Ops of a transaction that began before the tail were already written back.
            let Some(t) = open.get_mut(&tid) else {
                continue;
            };
            if len > 0 {
                open_bytes += len;
                if open_bytes as u64 > MAX_OPEN_BYTES.min(log.bbs * BB) {
                    return Err(corrupt(
                        "XFS log holds more uncommitted data than a replay will buffer".into(),
                    ));
                }
                let payload = &d[body..body + len];
                match t.regions.last_mut() {
                    Some(last) if flags & OP_WAS_CONT != 0 => last.extend_from_slice(payload),
                    _ => t.regions.push(payload.to_vec()),
                }
            }
            if flags & OP_COMMIT != 0 {
                let t = open.remove(&tid).expect("present");
                open_bytes -= t.regions.iter().map(Vec::len).sum::<usize>();
                on_commit(dev, &t, rec.en)?;
            }
        }
        if rec.blk == head.blk {
            break;
        }
        blk = rec.next;
    }
    Ok((records, open.len()))
}

/// One log item: its format region and the data regions after it.
struct Item<'a> {
    kind: u16,
    regions: &'a [Vec<u8>],
}

/// Split a transaction's regions into items, skipping the leading transaction header.
fn items(t: &Trans, en: En) -> Result<Vec<Item<'_>>, FilesystemError> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let r = &t.regions;
    while i < r.len() {
        let f = &r[i];
        if f.len() >= 4 && en.u32(f, 0) == TRANS_MAGIC {
            i += 1;
            continue;
        }
        if f.len() < 4 {
            return Err(corrupt("XFS log item shorter than its header".into()));
        }
        let kind = en.u16(f, 0);
        let size = usize::from(en.u16(f, 2)).max(1);
        if i + size > r.len() {
            return Err(corrupt(format!(
                "XFS log item 0x{kind:x} is missing regions"
            )));
        }
        out.push(Item {
            kind,
            regions: &r[i..i + size],
        });
        i += size;
    }
    Ok(out)
}

/// The id an intent or done item carries at offset 8; done items name their intent there.
fn item_id(f: &[u8], en: En) -> Option<u64> {
    (f.len() >= 16).then(|| en.u64(f, 8))
}

fn intent_name(kind: u16) -> Option<&'static str> {
    Some(match kind {
        0x1240 => "reverse-mapping update",
        0x1242 => "reference-count update",
        0x1244 => "file block-mapping update",
        0x1246 => "extended-attribute update",
        0x1248 => "file-mapping exchange",
        _ => return None,
    })
}

/// The done item that completes each intent kind.
fn done_for(intent: u16) -> u16 {
    intent + 1
}

/// Buffer key for cancellation: (daddr, sectors).
type BufKey = (u64, u32);

/// First-pass findings.
#[derive(Default)]
struct Survey {
    cancels: HashMap<BufKey, u32>,
    intents: HashMap<u64, u16>,
    done: HashSet<(u16, u64)>,
    problem: Option<String>,
    transactions: usize,
    /// `sb_qflags` accounting bits of quotas a quota-off item turned off.
    quota_off: u16,
    /// A quota item this module cannot read: replay none of them and force a recount.
    quota_unreadable: bool,
}

/// The `sb_qflags` accounting bit a quota buffer item (by `blf_flags`) belongs to.
fn quota_buf_kind(blf_flags: u16) -> u16 {
    match blf_flags & BLF_DQUOT_BUFS {
        0x4 => QF_UACCT,
        0x8 => QF_PACCT,
        0x10 => QF_GACCT,
        _ => 0,
    }
}

/// The accounting bit a logged dquot (by `d_flags`) belongs to, or `None` if it is not one.
fn quota_kind(dq: &[u8]) -> Option<u16> {
    if dq.len() < DISK_DQUOT || dq[0..2] != *b"DQ" {
        return None;
    }
    match dq[3] & 0x7 {
        1 => Some(QF_UACCT),
        2 => Some(QF_PACCT),
        4 => Some(QF_GACCT),
        _ => None,
    }
}

fn buf_key(f: &[u8], en: En) -> BufKey {
    (en.u64(f, 8), u32::from(en.u16(f, 6)))
}

/// Check one buffer item fully, so applying it can only fail on I/O.
fn check_buffer(it: &Item<'_>, en: En, g: &Geo) -> Result<(), String> {
    let f = &it.regions[0];
    if f.len() < 20 {
        return Err("a buffer log item is too short".into());
    }
    let (daddr, sectors) = buf_key(f, en);
    if sectors == 0
        || u64::from(sectors) > MAX_BUF_SECTORS
        || !g.inside(daddr, u64::from(sectors) * BB)
    {
        return Err(format!(
            "a buffer log item names sectors {daddr}+{sectors}, outside the volume"
        ));
    }
    if en.u16(f, 4) & BLF_CANCEL != 0 {
        return Ok(());
    }
    let words = en.u32(f, 16) as usize;
    if words > (f.len() - 20) / 4 {
        return Err("a buffer log item's dirty map overruns it".into());
    }
    let runs = map_runs(f, en, words);
    let len = sectors as usize * BB as usize;
    if runs.len() > it.regions.len() - 1 {
        return Err("a buffer log item has fewer regions than its dirty map".into());
    }
    if runs.iter().any(|&(_, end)| end * BLF_CHUNK > len) {
        return Err("a buffer log item's dirty map runs past the buffer".into());
    }
    Ok(())
}

/// The runs of set bits in a buffer item's dirty map, as `[start, end)` chunk indexes.
fn map_runs(f: &[u8], en: En, words: usize) -> Vec<(usize, usize)> {
    let bit = |i: usize| en.u32(f, 20 + 4 * (i / 32)) >> (i % 32) & 1 != 0;
    let nbits = words * 32;
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < nbits {
        if !bit(i) {
            i += 1;
            continue;
        }
        let start = i;
        while i < nbits && bit(i) {
            i += 1;
        }
        out.push((start, i));
    }
    out
}

/// Byte offsets of an inode item's daddr and buffer offset, by format size (64- or 32-bit).
fn inode_offsets(f: &[u8]) -> Option<(usize, usize, usize, usize)> {
    match f.len() {
        56 => Some((16, 24, 40, 52)),
        52 => Some((12, 20, 36, 48)),
        _ => None,
    }
}

fn check_inode(it: &Item<'_>, en: En, g: &Geo) -> Result<(), String> {
    let f = &it.regions[0];
    let Some((_, _, blk_o, boff_o)) = inode_offsets(f) else {
        return Err(format!("an inode log item is {} bytes", f.len()));
    };
    let fields = en.u32(f, 4);
    let at = en.u64(f, blk_o);
    let boff = u64::from(en.u32(f, boff_o));
    if !g.inside(at, boff.saturating_add(g.isize as u64)) {
        return Err(format!(
            "an inode log item names sector {at}+{boff}, outside the volume"
        ));
    }
    let needed = 1
        + usize::from(fields & ILOG_CORE != 0)
        + usize::from(fields & ILOG_DFORK != 0)
        + usize::from(fields & ILOG_AFORK != 0);
    if it.regions.len() < needed {
        return Err("an inode log item is missing regions".into());
    }
    if fields & ILOG_CORE != 0 {
        let core = &it.regions[1];
        let want = if core.len() > 4 && core[4] == 3 {
            176
        } else {
            96
        };
        if core.len() < want || (want == 176) != g.v5 {
            return Err("an inode log item's core is short or of the wrong version".into());
        }
    }
    Ok(())
}

fn check_icreate(it: &Item<'_>, g: &Geo) -> Result<(), String> {
    let f = &it.regions[0];
    if f.len() < 28 {
        return Err("an inode-create log item is too short".into());
    }
    let (ag, agbno, count) = (
        u64::from(be32(f, 4)),
        u64::from(be32(f, 8)),
        u64::from(be32(f, 12)),
    );
    let (isize, length) = (u64::from(be32(f, 16)), u64::from(be32(f, 20)));
    let chunk = (INODES_PER_CHUNK * g.isize as u64).max(g.blocksize);
    let daddr = ag
        .checked_mul(g.agblocks)
        .and_then(|b| b.checked_add(agbno))
        .and_then(|b| b.checked_mul(g.blocksize / BB));
    // The last AG is shorter than `agblocks`, so the volume bound is checked too.
    let ok = ag < g.agcount
        && daddr.is_some_and(|d| g.inside(d, length.saturating_mul(g.blocksize)))
        && length > 0
        && agbno + length <= g.agblocks
        && isize == g.isize as u64
        && (1..=INODES_PER_CHUNK).contains(&count)
        && count * isize <= length * g.blocksize
        && length * g.blocksize <= chunk;
    if ok {
        Ok(())
    } else {
        Err("an inode-create log item does not describe an inode chunk of this volume".into())
    }
}

fn dquot_ok(it: &Item<'_>, en: En, g: &Geo) -> bool {
    let f = &it.regions[0];
    let rec = if g.v5 { DQBLK_V5 } else { DISK_DQUOT } as u64;
    f.len() >= 24
        && it.regions.get(1).and_then(|r| quota_kind(r)).is_some()
        && g.inside(en.u64(f, 8), u64::from(en.u32(f, 20)) + rec)
}

fn survey(t: &Trans, en: En, g: &Geo, s: &mut Survey) -> Result<(), FilesystemError> {
    s.transactions += 1;
    for it in items(t, en)? {
        let f = &it.regions[0];
        let checked = match it.kind {
            LI_BUF => check_buffer(&it, en, g),
            LI_INODE => check_inode(&it, en, g),
            LI_ICREATE => check_icreate(&it, g),
            _ => Ok(()),
        };
        if let Err(why) = checked {
            s.problem.get_or_insert(why);
            continue;
        }
        match it.kind {
            LI_BUF => {
                if en.u16(f, 4) & BLF_CANCEL != 0 {
                    *s.cancels.entry(buf_key(f, en)).or_insert(0) += 1;
                }
            }
            LI_INODE | LI_ICREATE => {}
            LI_EFI | LI_RT_EFI => {
                if let Some(id) = item_id(f, en) {
                    s.intents.insert(id, it.kind);
                }
            }
            LI_EFD | LI_RT_EFD => {
                if let Some(id) = item_id(f, en) {
                    s.done.insert((it.kind, id));
                }
            }
            LI_DQUOT => {
                if !dquot_ok(&it, en, g) {
                    s.quota_unreadable = true;
                }
            }
            LI_QUOTAOFF => {
                // qf_flags holds the accounting bits being turned off.
                s.quota_off |= if f.len() >= 8 {
                    en.u32(f, 4) as u16
                } else {
                    u16::MAX
                };
            }
            k if intent_name(k).is_some() => {
                if let Some(id) = item_id(f, en) {
                    s.intents.insert(id, k);
                }
            }
            k if intent_name(k.wrapping_sub(1)).is_some() => {
                if let Some(id) = item_id(f, en) {
                    s.done.insert((k, id));
                }
            }
            k => {
                s.problem.get_or_insert_with(|| {
                    format!("the log holds an item of unknown type 0x{k:x}")
                });
            }
        }
    }
    Ok(())
}

/// Volume facts the apply pass needs.
struct Geo {
    off: u64,
    v5: bool,
    /// Volume size in 512-byte sectors, the bound for every write.
    sectors: u64,
    agcount: u64,
    sectsize: usize,
    isize: usize,
    blocksize: u64,
    agblocks: u64,
    agblklog: u8,
    inopblog: u8,
    meta_uuid: [u8; 16],
}

impl Geo {
    fn new(off: u64, sb: &XfsSuperblock) -> Self {
        Geo {
            off,
            v5: sb.versionnum & 0xF == 5,
            sectors: sb.dblocks.saturating_mul(u64::from(sb.blocksize)) / BB,
            agcount: u64::from(sb.agcount),
            sectsize: usize::from(sb.sectsize),
            isize: usize::from(sb.inodesize),
            blocksize: u64::from(sb.blocksize),
            agblocks: u64::from(sb.agblocks),
            agblklog: sb.agblklog,
            inopblog: sb.inopblog,
            meta_uuid: *sb.meta_uuid(),
        }
    }

    fn byte(&self, daddr: u64) -> u64 {
        self.off + daddr * BB
    }

    /// Whether `bytes` from sector `daddr` lie inside the volume.
    fn inside(&self, daddr: u64, bytes: u64) -> bool {
        daddr
            .checked_mul(BB)
            .and_then(|b| b.checked_add(bytes))
            .is_some_and(|end| end <= self.sectors * BB)
    }
}

fn read_exact_at<D: Read + Seek>(dev: &mut D, at: u64, len: usize) -> std::io::Result<Vec<u8>> {
    let mut b = vec![0u8; len];
    dev.seek(SeekFrom::Start(at))?;
    dev.read_exact(&mut b)?;
    Ok(b)
}

fn write_at<D: Write + Seek>(dev: &mut D, at: u64, b: &[u8]) -> std::io::Result<()> {
    dev.seek(SeekFrom::Start(at))?;
    dev.write_all(b)
}

/// Set a v5 metadata block's LSN and checksum, picked by its magic; other blocks are left alone.
fn restamp_v5_block(b: &mut [u8], lsn: u64, sectsize: usize) {
    if b.len() < 64 {
        return;
    }
    if b[0..2] == *b"DQ" {
        for dq in b.chunks_mut(DQBLK_V5) {
            if dq.len() == DQBLK_V5 && dq[0..2] == *b"DQ" {
                restamp_dqblk(dq, lsn);
            }
        }
        return;
    }
    let m32 = be32(b, 0);
    let (lsn_off, crc_off) = match &m32.to_be_bytes() {
        b"XFSB" => (240, 224),
        b"XAGF" => (208, 216),
        b"XAGI" => (320, 312),
        b"XAFL" => (24, 32),
        b"AB3B" | b"AB3C" | b"IAB3" | b"FIB3" | b"RMB3" | b"R3FC" => (24, 52),
        b"BMA3" => (32, 64),
        b"XDB3" | b"XDD3" | b"XDF3" => (16, 4),
        b"XSLM" => (48, 12),
        _ => match u16::from_be_bytes([b[8], b[9]]) {
            // xfs_da3_blkinfo magics: dir leaf1 / leafn, da node, attr leaf.
            0x3DF1 | 0x3DFF | 0x3EBE | 0x3BEE => (24, 12),
            _ => return,
        },
    };
    // Superblock, AG headers and the AGFL checksum only their sector.
    let span = match &m32.to_be_bytes() {
        b"XFSB" | b"XAGF" | b"XAGI" | b"XAFL" => b.len().min(sectsize),
        _ => b.len(),
    };
    b[lsn_off..lsn_off + 8].copy_from_slice(&lsn.to_be_bytes());
    stamp_crc(&mut b[..span], crc_off);
}

/// Set a v5 quota record's LSN and checksum.
fn restamp_dqblk(dq: &mut [u8], lsn: u64) {
    dq[DQ_LSN..DQ_LSN + 8].copy_from_slice(&lsn.to_be_bytes());
    stamp_crc(&mut dq[..DQBLK_V5], DQ_CRC);
}

/// Write a logged quota record back into its dquot block.
fn apply_dquot<D: Read + Write + Seek>(
    dev: &mut D,
    g: &Geo,
    it: &Item<'_>,
    en: En,
    lsn: u64,
) -> Result<(), FilesystemError> {
    let f = &it.regions[0];
    let dq = &it.regions[1][..DISK_DQUOT];
    let at = g.byte(en.u64(f, 8)) + u64::from(en.u32(f, 20));
    if g.v5 {
        let mut rec = read_exact_at(dev, at, DQBLK_V5)?;
        rec[..DISK_DQUOT].copy_from_slice(dq);
        restamp_dqblk(&mut rec, lsn);
        write_at(dev, at, &rec)?;
    } else {
        write_at(dev, at, dq)?;
    }
    Ok(())
}

/// Restamp every v3 inode in an inode-cluster buffer.
fn restamp_inodes(b: &mut [u8], isize: usize, lsn: u64) {
    for ino in b.chunks_mut(isize) {
        if ino.len() == isize && ino[0..2] == *b"IN" && ino[4] == 3 {
            ino[DI_LSN..DI_LSN + 8].copy_from_slice(&lsn.to_be_bytes());
            stamp_crc(ino, DI_CRC);
        }
    }
}

fn apply_buffer<D: Read + Write + Seek>(
    dev: &mut D,
    g: &Geo,
    it: &Item<'_>,
    en: En,
    lsn: u64,
) -> Result<(), FilesystemError> {
    let f = &it.regions[0];
    let flags = en.u16(f, 4);
    let (daddr, sectors) = buf_key(f, en);
    // `check_buffer` has bounded the map, its runs and the buffer against the volume.
    let runs = map_runs(f, en, en.u32(f, 16) as usize);
    let len = sectors as usize * BB as usize;
    let mut b = read_exact_at(dev, g.byte(daddr), len)?;
    for (k, &(start, end)) in runs.iter().enumerate() {
        let data = &it.regions[1 + k];
        let at = start * BLF_CHUNK;
        let n = data.len().min((end - start) * BLF_CHUNK).min(len - at);
        if flags & BLF_INODE_BUF != 0 {
            // Only the unlinked-list pointers in an inode buffer are logged this way.
            let mut o = 0usize;
            while o < len {
                let field = o + DI_NEXT_UNLINKED;
                if field >= at && field + 4 <= at + n {
                    b[field..field + 4].copy_from_slice(&data[field - at..field - at + 4]);
                }
                o += g.isize;
            }
        } else {
            b[at..at + n].copy_from_slice(&data[..n]);
        }
    }
    if g.v5 {
        if b.len() >= 2 && b[0..2] == *b"IN" {
            restamp_inodes(&mut b, g.isize, lsn);
        } else {
            restamp_v5_block(&mut b, lsn, g.sectsize);
        }
    }
    write_at(dev, g.byte(daddr), &b)?;
    Ok(())
}

/// (offset, width) of each multi-byte field of the inode core, which the log holds host-endian.
fn core_fields(v3: bool) -> Vec<(usize, usize)> {
    let mut f = vec![
        (2, 2),
        (6, 2),
        (8, 4),
        (12, 4),
        (16, 4),
        (20, 2),
        (22, 2),
        (56, 8),
        (64, 8),
        (72, 4),
        (76, 4),
        (80, 2),
        (84, 4),
        (88, 2),
        (90, 2),
        (92, 4),
    ];
    if v3 {
        f.extend_from_slice(&[
            (24, 8),
            (96, 4),
            (104, 8),
            (112, 8),
            (120, 8),
            (128, 4),
            (152, 8),
        ]);
    } else {
        f.push((30, 2));
    }
    f
}

/// Turn a logged (host-endian) inode core into its big-endian on-disk form.
fn core_to_disk(log: &[u8], en: En) -> Vec<u8> {
    let mut d = log.to_vec();
    let v3 = log.len() >= 176 && log[4] == 3;
    let swap = |d: &mut [u8], o: usize, w: usize| {
        if !en.big {
            d[o..o + w].reverse();
        }
    };
    d[0..2].copy_from_slice(&en.u16(log, 0).to_be_bytes());
    for (o, w) in core_fields(v3) {
        swap(&mut d, o, w);
    }
    // Timestamps: one 64-bit count with bigtime (di_flags2 bit 3), else two 32-bit halves.
    let bigtime = v3 && en.u64(log, 120) & (1 << 3) != 0;
    let mut stamps = vec![32, 40, 48];
    if v3 {
        stamps.push(144);
    }
    for o in stamps {
        if bigtime {
            swap(&mut d, o, 8);
        } else {
            swap(&mut d, o, 4);
            swap(&mut d, o + 4, 4);
        }
    }
    d
}

/// Copy a logged fork into the inode. A btree root is logged as an in-memory long-format block
/// (`hdr` bytes of header, keys and pointers sized to the logged length) and stored as a `bmdr`.
fn put_fork(fork: &mut [u8], logged: &[u8], broot: bool, hdr: usize) {
    if broot && fork.len() < 4 {
        return;
    }
    if !broot || logged.len() < hdr {
        let n = logged.len().min(fork.len());
        fork[..n].copy_from_slice(&logged[..n]);
        return;
    }
    let numrecs = usize::from(u16::from_be_bytes([logged[6], logged[7]]));
    let in_max = (logged.len() - hdr) / 16;
    let disk_max = fork.len().saturating_sub(4) / 16;
    let n = numrecs.min(in_max).min(disk_max);
    fork.fill(0);
    fork[0..2].copy_from_slice(&logged[4..6]);
    fork[2..4].copy_from_slice(&(n as u16).to_be_bytes());
    fork[4..4 + 8 * n].copy_from_slice(&logged[hdr..hdr + 8 * n]);
    let (src, dst) = (hdr + 8 * in_max, 4 + 8 * disk_max);
    fork[dst..dst + 8 * n].copy_from_slice(&logged[src..src + 8 * n]);
}

fn apply_inode<D: Read + Write + Seek>(
    dev: &mut D,
    g: &Geo,
    it: &Item<'_>,
    en: En,
    lsn: u64,
) -> Result<(), FilesystemError> {
    let f = &it.regions[0];
    // The 32-bit format lacks ilf_pad, so everything after ilf_dsize sits 4 bytes earlier.
    let Some((_, u_o, blk_o, boff_o)) = inode_offsets(f) else {
        return Err(corrupt(format!("XFS inode log item of {} bytes", f.len())));
    };
    let fields = en.u32(f, 4);
    let blkno = en.u64(f, blk_o);
    let boff = en.u32(f, boff_o) as u64;
    let at = g.byte(blkno) + boff;
    let mut ino = read_exact_at(dev, at, g.isize)?;
    let mut next = 1usize;
    let mut take = || {
        let r = it.regions.get(next);
        next += 1;
        r
    };
    if fields & ILOG_CORE != 0 {
        let core = take().ok_or_else(|| corrupt("XFS inode log item lacks its core".into()))?;
        let disk = core_to_disk(core, en);
        let n = disk.len().min(g.isize);
        // The on-disk unlinked pointer is kept; inode-buffer items carry its changes.
        let unlinked: [u8; 4] = ino[DI_NEXT_UNLINKED..DI_NEXT_UNLINKED + 4]
            .try_into()
            .expect("4 bytes");
        ino[..n].copy_from_slice(&disk[..n]);
        ino[DI_NEXT_UNLINKED..DI_NEXT_UNLINKED + 4].copy_from_slice(&unlinked);
    }
    let v3 = ino[4] == 3;
    let dfork = if v3 { 176 } else { 100 };
    // A fork offset past the inode leaves no attribute fork rather than a slice panic.
    let aoff = (dfork + usize::from(ino[82]) * 8).min(g.isize);
    let dend = if ino[82] == 0 { g.isize } else { aoff };
    let bhdr = if v3 { 72 } else { 24 };
    if fields & ILOG_DFORK != 0 {
        if let Some(r) = take() {
            put_fork(&mut ino[dfork..dend], r, fields & ILOG_DBROOT != 0, bhdr);
        }
    } else if fields & ILOG_DEV != 0 {
        ino[dfork..dfork + 4].copy_from_slice(&en.u32(f, u_o).to_be_bytes());
    } else if fields & ILOG_UUID != 0 {
        ino[dfork..dfork + 16].copy_from_slice(&f[u_o..u_o + 16]);
    }
    if fields & ILOG_AFORK != 0 && ino[82] != 0 && aoff < g.isize {
        if let Some(r) = take() {
            let end = g.isize;
            put_fork(&mut ino[aoff..end], r, fields & ILOG_ABROOT != 0, bhdr);
        }
    }
    if g.v5 && v3 {
        ino[DI_LSN..DI_LSN + 8].copy_from_slice(&lsn.to_be_bytes());
        stamp_crc(&mut ino, DI_CRC);
    }
    write_at(dev, at, &ino)?;
    Ok(())
}

fn apply_icreate<D: Read + Write + Seek>(
    dev: &mut D,
    g: &Geo,
    it: &Item<'_>,
) -> Result<(u64, u32), FilesystemError> {
    let f = &it.regions[0];
    // `check_icreate` has bounded the chunk to this volume and one inode chunk.
    let (ag, agbno, count) = (u64::from(be32(f, 4)), u64::from(be32(f, 8)), be32(f, 12));
    let (isize, length, gen) = (be32(f, 16) as usize, u64::from(be32(f, 20)), be32(f, 24));
    let daddr = (ag * g.agblocks + agbno) * (g.blocksize / BB);
    let mut chunk = vec![0u8; count as usize * isize];
    let first_agino = agbno << g.inopblog;
    for (i, ino) in chunk.chunks_mut(isize).enumerate() {
        ino[0..2].copy_from_slice(b"IN");
        ino[4] = if g.v5 { 3 } else { 2 };
        ino[92..96].copy_from_slice(&gen.to_be_bytes());
        ino[DI_NEXT_UNLINKED..DI_NEXT_UNLINKED + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        if g.v5 {
            let agino = first_agino + i as u64;
            let inum = (ag << (u32::from(g.agblklog) + u32::from(g.inopblog))) | agino;
            ino[152..160].copy_from_slice(&inum.to_be_bytes());
            ino[160..176].copy_from_slice(&g.meta_uuid);
            stamp_crc(ino, DI_CRC);
        }
    }
    write_at(dev, g.byte(daddr), &chunk)?;
    Ok((daddr, (length * g.blocksize / BB) as u32))
}

/// What the first pass decided.
enum Plan {
    Clean,
    Unrecoverable(String),
    Ready {
        head: Box<Record>,
        survey: Survey,
        unfinished_frees: usize,
    },
}

/// Find the head and survey the log without writing anything.
fn plan<R: Read + Seek>(
    dev: &mut R,
    log: &Log,
    partition_offset: u64,
    sb: &XfsSuperblock,
) -> Result<Plan, FilesystemError> {
    if sb.logstart == 0 {
        return Ok(Plan::Clean);
    }
    let head = match find_head(dev, log)? {
        Head::Record(r) => *r,
        Head::Blank => return Ok(Plan::Clean),
        Head::Garbled { .. } => {
            return Ok(Plan::Unrecoverable(
                "the log has record headers but no complete record".into(),
            ))
        }
    };
    // An unmount record is written after everything reached the disk, whatever its tail says.
    if is_unmount(&head) {
        return Ok(Plan::Clean);
    }
    let mut s = Survey::default();
    let g = Geo::new(partition_offset, sb);
    if let Err(e) = walk(dev, log, &head, |_, t, en| survey(t, en, &g, &mut s)) {
        return Ok(Plan::Unrecoverable(e.to_string()));
    }
    if let Some(p) = s.problem.take() {
        return Ok(Plan::Unrecoverable(p));
    }
    let mut unfinished_frees = 0usize;
    for (id, kind) in &s.intents {
        if s.done.contains(&(done_for(*kind), *id)) {
            continue;
        }
        match intent_name(*kind) {
            Some(name) => {
                return Ok(Plan::Unrecoverable(format!(
                    "the log holds an unfinished {name}, which this tool cannot complete"
                )))
            }
            None => unfinished_frees += 1,
        }
    }
    Ok(Plan::Ready {
        head: Box::new(head),
        survey: s,
        unfinished_frees,
    })
}

/// What a read-only look at the log found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogCheck {
    Clean,
    /// Committed transactions a replay would write to the volume.
    Replayable {
        transactions: usize,
    },
    /// The volume was not cleanly unmounted and the log cannot be replayed; the reason says why.
    Unrecoverable(String),
}

/// The log's state as edits see it, from the same head [`replay_log`] would use.
pub(crate) fn head_state<R: Read + Seek>(
    dev: &mut R,
    partition_offset: u64,
    sb: &XfsSuperblock,
) -> Result<super::log::LogState, FilesystemError> {
    use super::log::LogState;
    if sb.logstart == 0 {
        return Ok(LogState::External);
    }
    let log = Log::new(partition_offset, sb);
    Ok(match find_head(dev, &log)? {
        Head::Record(h) if !is_unmount(&h) => LogState::Dirty { head_lsn: h.lsn },
        Head::Garbled { newest_lsn } => LogState::Dirty {
            head_lsn: newest_lsn,
        },
        _ => LogState::Clean,
    })
}

/// Say whether the log is clean, replayable or beyond replay, writing nothing.
pub fn check_log<R: Read + Seek>(
    dev: &mut R,
    partition_offset: u64,
    sb: &XfsSuperblock,
) -> Result<LogCheck, FilesystemError> {
    let log = Log::new(partition_offset, sb);
    Ok(match plan(dev, &log, partition_offset, sb)? {
        Plan::Clean => LogCheck::Clean,
        Plan::Unrecoverable(why) => LogCheck::Unrecoverable(why),
        Plan::Ready { survey, .. } => LogCheck::Replayable {
            transactions: survey.transactions,
        },
    })
}

/// Replay the internal log of the volume at `partition_offset` and leave it clean.
///
/// Nothing is written unless the whole log can be replayed; [`ReplayOutcome::Unrecoverable`]
/// says why not, and [`zero_log`] is then the only way to make the volume mountable.
pub fn replay_log<D: Read + Write + Seek>(
    dev: &mut D,
    partition_offset: u64,
    sb: &XfsSuperblock,
) -> Result<ReplayOutcome, FilesystemError> {
    let log = Log::new(partition_offset, sb);
    let (head, s, unfinished_frees) = match plan(dev, &log, partition_offset, sb)? {
        Plan::Clean => return Ok(ReplayOutcome::Clean),
        Plan::Unrecoverable(why) => return Ok(ReplayOutcome::Unrecoverable(why)),
        Plan::Ready {
            head,
            survey,
            unfinished_frees,
        } => (head, survey, unfinished_frees),
    };

    let g = Geo::new(partition_offset, sb);
    let mut report = ReplayReport {
        unfinished_frees,
        ..ReplayReport::default()
    };
    let mut cancels = s.cancels;
    // Quota items of a quota turned off later, or any at all if one is unreadable, are skipped.
    let quota_skip = if s.quota_unreadable {
        u16::MAX
    } else {
        s.quota_off
    };
    report.quota_recount = s.quota_unreadable || s.quota_off != 0;
    let cancelled_at =
        |c: &HashMap<BufKey, u32>, daddr: u64| c.iter().any(|(k, n)| *n > 0 && k.0 == daddr);
    let (records, incomplete) = walk(dev, &log, &head, |dev, t, en| {
        // v5 recovery stamps each block with the LSN of the record the transaction started in.
        let lsn = t.lsn;
        for it in items(t, en)? {
            match it.kind {
                LI_BUF => {
                    let f = &it.regions[0];
                    let key = buf_key(f, en);
                    if en.u16(f, 4) & BLF_CANCEL != 0 {
                        if let Some(n) = cancels.get_mut(&key) {
                            *n = n.saturating_sub(1);
                        }
                        report.cancelled += 1;
                    } else if cancels.get(&key).copied().unwrap_or(0) > 0 {
                        report.cancelled += 1;
                    } else if quota_buf_kind(en.u16(f, 4)) & quota_skip != 0 {
                        // A quota buffer for a quota whose records are not replayed.
                    } else {
                        apply_buffer(dev, &g, &it, en, lsn)?;
                        report.buffers += 1;
                    }
                }
                LI_INODE => {
                    let f = &it.regions[0];
                    let blk_o = if f.len() == 56 { 40 } else { 36 };
                    if cancelled_at(&cancels, en.u64(f, blk_o)) {
                        report.cancelled += 1;
                    } else {
                        apply_inode(dev, &g, &it, en, lsn)?;
                        report.inodes += 1;
                    }
                }
                LI_ICREATE => {
                    let f = &it.regions[0];
                    let ag = u64::from(be32(f, 4));
                    let agbno = u64::from(be32(f, 8));
                    let daddr = (ag * g.agblocks + agbno) * (g.blocksize / BB);
                    if cancelled_at(&cancels, daddr) {
                        report.cancelled += 1;
                    } else {
                        apply_icreate(dev, &g, &it)?;
                        report.inode_chunks += 1;
                    }
                }
                LI_DQUOT => {
                    let kind = it.regions.get(1).and_then(|r| quota_kind(r)).unwrap_or(0);
                    if kind != 0 && kind & quota_skip == 0 {
                        apply_dquot(dev, &g, &it, en, lsn)?;
                        report.quotas += 1;
                    }
                }
                _ => {}
            }
        }
        report.transactions += 1;
        Ok(())
    })?;
    report.records = records;
    report.incomplete = incomplete;
    resync_sb_counters(dev, &g, sb)?;
    if report.quota_recount {
        clear_quota_checked(dev, &g, sb)?;
    }
    write_clean_log(dev, &log, sb, head.lsn >> 32)?;
    Ok(ReplayOutcome::Replayed(report))
}

/// Discard the log (`xfs_repair -L`): every change it holds that is not yet on disk is lost.
pub fn zero_log<D: Read + Write + Seek>(
    dev: &mut D,
    partition_offset: u64,
    sb: &XfsSuperblock,
) -> Result<(), FilesystemError> {
    if sb.logstart == 0 {
        return Ok(());
    }
    let log = Log::new(partition_offset, sb);
    // The newest cycle any header claims, complete or not: a garbled log is what this is for.
    let cycle = scan_headers(dev, &log)?
        .iter()
        .map(|(lsn, _)| lsn >> 32)
        .max()
        .unwrap_or(0);
    let g = Geo::new(partition_offset, sb);
    resync_sb_counters(dev, &g, sb)?;
    // The quota records on disk need not match the files once the log is gone.
    clear_quota_checked(dev, &g, sb)?;
    write_clean_log(dev, &log, sb, cycle)
}

/// Rewrite the log as one unmount record at its start, a cycle past the old head so no v5
/// metadata LSN is ahead of the log.
fn write_clean_log<D: Write + Seek>(
    dev: &mut D,
    log: &Log,
    sb: &XfsSuperblock,
    old_cycle: u64,
) -> Result<(), FilesystemError> {
    let cycle = (old_cycle + 1).min(u64::from(u32::MAX)) as u32;
    // Every other sector carries the previous cycle, so the log reads as wrapped once, with the
    // head right after the unmount record.
    let mut fill = vec![0u8; 1 << 20];
    for s in fill.chunks_mut(BB as usize) {
        s[..4].copy_from_slice(&(cycle - 1).to_be_bytes());
    }
    let total = log.bbs * BB;
    let mut done = 0u64;
    dev.seek(SeekFrom::Start(log.start))?;
    while done < total {
        let n = (total - done).min(fill.len() as u64) as usize;
        dev.write_all(&fill[..n])?;
        done += n as u64;
    }
    let head = super::format::build_log_head(&sb.sb_uuid, cycle);
    write_at(dev, log.start, &head)?;
    Ok(())
}

/// Superblock offsets of the lazily kept counters a mount recomputes after recovery.
const SB_ICOUNT: usize = 128;
const SB_IFREE: usize = 136;
const SB_FDBLOCKS: usize = 144;
const SB_CRC: usize = 224;
const SB_QFLAGS: usize = 176;

/// Clear every `sb_qflags` "checked" bit, so the next quota mount recounts usage.
fn clear_quota_checked<D: Read + Write + Seek>(
    dev: &mut D,
    g: &Geo,
    sb: &XfsSuperblock,
) -> Result<(), FilesystemError> {
    let mut p = read_exact_at(dev, g.off, usize::from(sb.sectsize))?;
    let q = u16::from_be_bytes([p[SB_QFLAGS], p[SB_QFLAGS + 1]]) & !QF_ALL_CHKD;
    p[SB_QFLAGS..SB_QFLAGS + 2].copy_from_slice(&q.to_be_bytes());
    if g.v5 {
        stamp_crc(&mut p, SB_CRC);
    }
    write_at(dev, g.off, &p)?;
    Ok(())
}

/// Set `sb_icount`, `sb_ifree` and `sb_fdblocks` from the AG headers, as a mount does.
fn resync_sb_counters<D: Read + Write + Seek>(
    dev: &mut D,
    g: &Geo,
    sb: &XfsSuperblock,
) -> Result<(), FilesystemError> {
    let sect = usize::from(sb.sectsize);
    let (mut icount, mut ifree, mut fdblocks) = (0u64, 0u64, 0u64);
    for agno in 0..u64::from(sb.agcount) {
        let ag = g.off + agno * g.agblocks * g.blocksize;
        let agf = XfsAgf::parse(&read_exact_at(dev, ag + sect as u64, sect)?)?;
        let agi = XfsAgi::parse(&read_exact_at(dev, ag + 2 * sect as u64, sect)?)?;
        fdblocks += u64::from(agf.freeblks) + u64::from(agf.flcount) + u64::from(agf.btreeblks);
        icount += u64::from(agi.count);
        ifree += u64::from(agi.freecount);
    }
    let mut p = read_exact_at(dev, g.off, sect)?;
    p[SB_ICOUNT..SB_ICOUNT + 8].copy_from_slice(&icount.to_be_bytes());
    p[SB_IFREE..SB_IFREE + 8].copy_from_slice(&ifree.to_be_bytes());
    p[SB_FDBLOCKS..SB_FDBLOCKS + 8].copy_from_slice(&fdblocks.to_be_bytes());
    if g.v5 {
        stamp_crc(&mut p, SB_CRC);
    }
    write_at(dev, g.off, &p)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::xfs::XfsFilesystem;
    use disk_core::entry::{EntryType, FileEntry};
    use disk_core::filesystem::{EditableFilesystem, Filesystem};
    use std::io::Cursor;

    /// A dirty fixture decoded to disk: 300 MB is too much for a 32-bit `Vec`.
    fn dirty_fixture(name: &str) -> std::fs::File {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/sgi")
            .join(name);
        let mut out = tempfile::tempfile().unwrap();
        let src = std::fs::File::open(path).expect("fixture present");
        decode_sparse(src, &mut out);
        out
    }

    /// Decode a zstd fixture into `out`, seeking over all-zero runs so the file stays sparse.
    fn decode_sparse(src: std::fs::File, out: &mut std::fs::File) {
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut dec = zstd::stream::read::Decoder::new(src).unwrap();
        let mut buf = vec![0u8; 1 << 16];
        let mut len = 0u64;
        loop {
            let mut n = 0;
            while n < buf.len() {
                match dec.read(&mut buf[n..]).unwrap() {
                    0 => break,
                    k => n += k,
                }
            }
            if n == 0 {
                break;
            }
            if buf[..n].iter().all(|&b| b == 0) {
                out.seek(SeekFrom::Current(n as i64)).unwrap();
            } else {
                out.write_all(&buf[..n]).unwrap();
            }
            len += n as u64;
        }
        out.set_len(len).unwrap();
    }

    fn child<R: Read + Seek + Send>(
        fs: &mut XfsFilesystem<R>,
        dir: &FileEntry,
        n: &str,
    ) -> Option<FileEntry> {
        fs.list_directory(dir)
            .unwrap()
            .into_iter()
            .find(|e| e.name == n)
    }

    /// A crash after deletes, a sparse btree file, xattrs, a device node and a hard link;
    /// `docs/xfs_log_replay.md` lists the workload. The kernel's recovery is the reference.
    fn replays_like_the_kernel(name: &str) {
        let mut fs = XfsFilesystem::open(dirty_fixture(name), 0).unwrap();
        let before = fs.run_fsck().unwrap();
        assert!(before.repairable, "{:?}", before.errors);
        assert_eq!(before.errors[0].code, "LogDirty");

        let report = fs.repair().unwrap();
        assert!(
            report.fixes_applied[0].starts_with("log: replayed 101"),
            "{report:?}"
        );
        assert_eq!(fs.log_state().unwrap(), crate::xfs::log::LogState::Clean);
        let after = fs.run_fsck().unwrap();
        assert!(after.errors.is_empty(), "{:?}", after.errors);

        let root = fs.root().unwrap();
        let big = child(&mut fs, &root, "big").unwrap();
        let names = fs.list_directory(&big).unwrap();
        assert_eq!(
            names
                .iter()
                .filter(|e| e.name.starts_with("entry_"))
                .count(),
            300
        );
        let last = child(&mut fs, &big, "entry_with_a_long_name_1500").unwrap();
        assert_eq!(fs.read_file(&last, 64).unwrap(), b"k1500\n");
        assert!(child(&mut fs, &big, "entry_with_a_long_name_1200").is_none());

        let keep = child(&mut fs, &root, "keep").unwrap();
        assert!(child(&mut fs, &keep, "old.txt").is_none());
        let hard = child(&mut fs, &keep, "hard.txt").unwrap();
        assert_eq!(fs.read_file(&hard, 64).unwrap(), b"data\n");
        assert_eq!(child(&mut fs, &keep, "blob").unwrap().size, 100_000);
        let null = child(&mut fs, &keep, "null").unwrap();
        assert_eq!(null.mode.map(|m| m & 0o170000), Some(0o020000));

        // The sparse file's bmap went to a btree: 4 KiB written every 8 KiB.
        let frag = child(&mut fs, &root, "frag.bin").unwrap();
        assert_eq!(frag.entry_type, EntryType::File);
        assert_eq!(frag.size, 99 * 8192 + 4096);
        let data = fs.read_file(&frag, frag.size as usize).unwrap();
        for i in [0usize, 50, 99] {
            assert!(
                data[i * 8192..i * 8192 + 4096].iter().all(|&b| b == 0xCD),
                "extent {i}"
            );
        }
        assert!(data[8192 - 4096..8192].iter().all(|&b| b == 0));
    }

    #[test]
    fn a_v5_log_replays_like_the_kernel() {
        replays_like_the_kernel("xfs_v5_dirty_log.img.zst");
    }

    #[test]
    fn a_v4_log_replays_like_the_kernel() {
        replays_like_the_kernel("xfs_v4_dirty_log.img.zst");
    }

    /// A 640 KiB file deleted while still open at the crash sits on the AGI unlinked list.
    #[test]
    fn repair_frees_what_was_deleted_while_open() {
        let mut fs =
            XfsFilesystem::open(dirty_fixture("xfs_v4_dirty_log_unlinked.img.zst"), 0).unwrap();
        let free_before = fs.free_space().unwrap();
        let report = fs.repair().unwrap();
        assert!(
            report
                .fixes_applied
                .iter()
                .any(|f| f.contains("freed 1 inodes")),
            "{report:?}"
        );
        assert!(fs.run_fsck().unwrap().errors.is_empty());
        // 640 KiB comes back, less the block more.txt took.
        assert!(fs.free_space().unwrap() >= free_before + 600 * 1024);
        let root = fs.root().unwrap();
        assert!(child(&mut fs, &root, "open.bin").is_none());
        let more = child(&mut fs, &root, "more.txt").unwrap();
        assert_eq!(fs.read_file(&more, 64).unwrap(), b"more\n");
    }

    #[test]
    fn discarding_the_log_leaves_the_pre_crash_volume() {
        let mut fs = XfsFilesystem::open(dirty_fixture("xfs_v5_dirty_log.img.zst"), 0).unwrap();
        fs.discard_log().unwrap();
        assert_eq!(fs.log_state().unwrap(), crate::xfs::log::LogState::Clean);
        // The deletes never reached the disk.
        let root = fs.root().unwrap();
        let big = child(&mut fs, &root, "big").unwrap();
        assert!(child(&mut fs, &big, "entry_with_a_long_name_1").is_some());
        assert!(child(&mut fs, &root, "frag.bin").is_none());
    }

    fn blank() -> (Vec<u8>, XfsSuperblock) {
        let mut img = Cursor::new(Vec::new());
        crate::xfs::format::write_blank_xfs(&mut img, 0, 64 * 1024 * 1024, "LOG").unwrap();
        let mut img = img.into_inner();
        let sb = XfsSuperblock::parse(&img[..512]).unwrap();
        img.resize((sb.dblocks * u64::from(sb.blocksize)) as usize, 0);
        (img, sb)
    }

    /// Write one v2 log record of `ops` (tid, flags, payload) at log block `blk`, cycle 1.
    fn put_record(img: &mut [u8], sb: &XfsSuperblock, blk: u64, ops: &[(u32, u8, Vec<u8>)]) {
        let mut data = Vec::new();
        for (tid, flags, payload) in ops {
            data.extend_from_slice(&tid.to_be_bytes());
            data.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            data.extend_from_slice(&[CLIENT_TRANS, *flags, 0, 0]);
            data.extend_from_slice(payload);
        }
        let len = data.len();
        data.resize(len.div_ceil(512) * 512, 0);
        let mut h = vec![0u8; 512];
        h[0..4].copy_from_slice(&HEADER_MAGIC.to_be_bytes());
        h[4..8].copy_from_slice(&1u32.to_be_bytes());
        h[8..12].copy_from_slice(&2u32.to_be_bytes());
        h[12..16].copy_from_slice(&(len as u32).to_be_bytes());
        h[16..24].copy_from_slice(&(1u64 << 32 | blk).to_be_bytes());
        h[24..32].copy_from_slice(&(1u64 << 32 | blk).to_be_bytes());
        h[40..44].copy_from_slice(&(ops.len() as u32).to_be_bytes());
        h[300..304].copy_from_slice(&1u32.to_be_bytes());
        h[320..324].copy_from_slice(&32768u32.to_be_bytes());
        for (i, s) in data.chunks_mut(512).enumerate() {
            h[44 + 4 * i..48 + 4 * i].copy_from_slice(&s[..4]);
            s[..4].copy_from_slice(&1u32.to_be_bytes());
        }
        let start = fsblock_to_partition_byte(sb.logstart, sb.agblocks, sb.agblklog, sb.blocksize)
            as usize
            + blk as usize * 512;
        img[start..start + 512].copy_from_slice(&h);
        img[start + 512..start + 512 + data.len()].copy_from_slice(&data);
    }

    fn trans_header(tid: u32, items: u32) -> Vec<u8> {
        [TRANS_MAGIC, 1, tid, items]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect()
    }

    /// A buffer item logging the first 128 bytes of the `sectors`-long buffer at `daddr`.
    fn buf_item(daddr: u64, sectors: u16) -> Vec<u8> {
        let mut f = Vec::new();
        f.extend_from_slice(&LI_BUF.to_le_bytes());
        f.extend_from_slice(&2u16.to_le_bytes());
        f.extend_from_slice(&0u16.to_le_bytes());
        f.extend_from_slice(&sectors.to_le_bytes());
        f.extend_from_slice(&daddr.to_le_bytes());
        f.extend_from_slice(&1u32.to_le_bytes());
        f.extend_from_slice(&1u32.to_le_bytes());
        f
    }

    #[test]
    fn only_committed_transactions_are_replayed() {
        let (mut img, sb) = blank();
        let last = (sb.dblocks - 1) * u64::from(sb.blocksize) / 512;
        let spare = last - 8;
        put_record(
            &mut img,
            &sb,
            2,
            &[
                (7, OP_START, vec![]),
                (7, 0, trans_header(7, 1)),
                (7, 0, buf_item(last, 8)),
                (7, 0, vec![0xAB; 128]),
                (7, OP_COMMIT, vec![]),
                (9, OP_START, vec![]),
                (9, 0, trans_header(9, 1)),
                (9, 0, buf_item(spare, 8)),
                (9, 0, vec![0xEE; 128]),
            ],
        );
        let mut dev = Cursor::new(img);
        assert_eq!(
            check_log(&mut dev, 0, &sb).unwrap(),
            LogCheck::Replayable { transactions: 1 }
        );
        let ReplayOutcome::Replayed(r) = replay_log(&mut dev, 0, &sb).unwrap() else {
            panic!("not replayed");
        };
        assert_eq!((r.transactions, r.incomplete, r.buffers), (1, 1, 1));
        let img = dev.into_inner();
        let at = (last * 512) as usize;
        assert!(img[at..at + 128].iter().all(|&b| b == 0xAB));
        assert!(img[(spare * 512) as usize..][..128].iter().all(|&b| b == 0));
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).unwrap();
        assert_eq!(fs.log_state().unwrap(), crate::xfs::log::LogState::Clean);
        assert!(fs.run_fsck().unwrap().errors.is_empty());
    }

    #[test]
    fn a_log_with_an_unfinished_rmap_intent_is_refused_untouched() {
        let (mut img, sb) = blank();
        let mut rui = Vec::new();
        rui.extend_from_slice(&0x1240u16.to_le_bytes());
        rui.extend_from_slice(&1u16.to_le_bytes());
        rui.extend_from_slice(&1u32.to_le_bytes());
        rui.extend_from_slice(&77u64.to_le_bytes());
        rui.resize(48, 0);
        put_record(
            &mut img,
            &sb,
            2,
            &[
                (5, OP_START, vec![]),
                (5, 0, trans_header(5, 1)),
                (5, 0, rui),
                (5, OP_COMMIT, vec![]),
            ],
        );
        let before = img.clone();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).unwrap();
        let res = fs.run_fsck().unwrap();
        assert_eq!(res.errors[0].code, "LogUnrecoverable");
        assert!(!res.repairable);
        let err = fs.repair().unwrap_err().to_string();
        assert!(
            err.contains("cannot be replayed") && err.contains("reverse-mapping"),
            "{err}"
        );
        assert!(fs.reader.get_ref() == &before, "nothing may be written");

        fs.discard_log().unwrap();
        assert!(fs.run_fsck().unwrap().errors.is_empty());
    }

    /// A quota item this module cannot read is skipped, and the next quota mount recounts.
    #[test]
    fn an_unreadable_quota_item_forces_a_recount() {
        let (mut img, sb) = blank();
        img[SB_QFLAGS..SB_QFLAGS + 2].copy_from_slice(&0x7u16.to_be_bytes());
        let mut dq = Vec::new();
        dq.extend_from_slice(&LI_DQUOT.to_le_bytes());
        dq.extend_from_slice(&2u16.to_le_bytes());
        dq.resize(24, 0);
        put_record(
            &mut img,
            &sb,
            2,
            &[
                (5, OP_START, vec![]),
                (5, 0, trans_header(5, 1)),
                (5, 0, dq),
                (5, 0, vec![0; 104]),
                (5, OP_COMMIT, vec![]),
            ],
        );
        let mut dev = Cursor::new(img);
        let ReplayOutcome::Replayed(r) = replay_log(&mut dev, 0, &sb).unwrap() else {
            panic!("not replayed");
        };
        assert!(r.quota_recount);
        assert_eq!(r.quotas, 0);
        let img = dev.into_inner();
        assert_eq!(
            u16::from_be_bytes([img[SB_QFLAGS], img[SB_QFLAGS + 1]]),
            0x3
        );
    }

    /// Made on a quota-enabled mount: files created, chowned and deleted, and a limit set.
    fn replays_quota(name: &str, quotas: usize, qflags: u16) {
        let mut dev = dirty_fixture(name);
        let mut sec = [0u8; 512];
        dev.seek(SeekFrom::Start(0)).unwrap();
        dev.read_exact(&mut sec).unwrap();
        let sb = XfsSuperblock::parse(&sec).unwrap();
        let ReplayOutcome::Replayed(r) = replay_log(&mut dev, 0, &sb).unwrap() else {
            panic!("not replayed");
        };
        assert_eq!((r.quotas, r.quota_recount), (quotas, false));
        // User 1002's record (block 384 + 1632 on v5): 40 blocks used, a 1 MiB soft limit.
        let at = if sb.versionnum & 0xF == 5 {
            384 * 512 + 1632
        } else {
            0
        };
        if at != 0 {
            let rec = read_exact_at(&mut dev, at, DQBLK_V5).unwrap();
            assert_eq!(be32(&rec, 4), 1002);
            assert_eq!(be64(&rec, 16), 256);
            assert_eq!(be64(&rec, 40), 40);
            assert!(super::super::v5_crc::crc_valid(&rec, DQ_CRC));
        }
        let sec = read_exact_at(&mut dev, 0, 512).unwrap();
        assert_eq!(
            u16::from_be_bytes([sec[SB_QFLAGS], sec[SB_QFLAGS + 1]]),
            qflags
        );
        let mut fs = XfsFilesystem::open(dev, 0).unwrap();
        assert!(fs.run_fsck().unwrap().errors.is_empty());
    }

    #[test]
    fn v5_quota_records_replay_like_the_kernel() {
        replays_quota("xfs_v5_dirty_log_quota.img.zst", 18, 0x7cf);
    }

    #[test]
    fn v4_quota_records_replay_like_the_kernel() {
        replays_quota("xfs_v4_dirty_log_quota.img.zst", 8, 0x77);
    }

    /// A v1 header claiming more data sectors than it has cycle slots for (review blocker 1).
    #[test]
    fn a_header_without_enough_cycle_slots_is_skipped_not_a_panic() {
        let (mut img, sb) = blank();
        let start =
            fsblock_to_partition_byte(sb.logstart, sb.agblocks, sb.agblklog, sb.blocksize) as usize;
        let blk = 4u64;
        let h = &mut img[start + 2048..start + 2560];
        h[0..4].copy_from_slice(&HEADER_MAGIC.to_be_bytes());
        h[4..8].copy_from_slice(&9u32.to_be_bytes());
        h[8..12].copy_from_slice(&1u32.to_be_bytes());
        h[12..16].copy_from_slice(&(65u32 * 512).to_be_bytes());
        h[16..24].copy_from_slice(&(9u64 << 32 | blk).to_be_bytes());
        for i in 0..65 {
            let o = start + 2560 + i * 512;
            img[o..o + 4].copy_from_slice(&9u32.to_be_bytes());
        }
        let mut dev = Cursor::new(img);
        assert_eq!(check_log(&mut dev, 0, &sb).unwrap(), LogCheck::Clean);
    }

    /// Each malformed item must be refused by the first pass, before anything is written.
    fn refused_untouched(item: Vec<Vec<u8>>, why: &str) {
        let (mut img, sb) = blank();
        let mut ops = vec![(3, OP_START, vec![]), (3, 0, trans_header(3, 1))];
        ops.extend(item.into_iter().map(|r| (3u32, 0u8, r)));
        ops.push((3, OP_COMMIT, vec![]));
        put_record(&mut img, &sb, 2, &ops);
        let before = img.clone();
        let mut dev = Cursor::new(img);
        match replay_log(&mut dev, 0, &sb).unwrap() {
            ReplayOutcome::Unrecoverable(m) => assert!(m.contains(why), "{m}"),
            other => panic!("{other:?}"),
        }
        assert!(dev.into_inner() == before, "nothing may be written");
    }

    #[test]
    fn a_buffer_map_with_more_runs_than_regions_is_refused() {
        let mut f = buf_item(16, 8);
        // Two runs (chunks 0 and 2) but one data region.
        f[20..24].copy_from_slice(&0b101u32.to_le_bytes());
        refused_untouched(vec![f, vec![0xAB; 128]], "fewer regions");
    }

    #[test]
    fn a_buffer_map_past_its_buffer_is_refused() {
        let mut f = buf_item(16, 1);
        f[20..24].copy_from_slice(&(1u32 << 31).to_le_bytes());
        refused_untouched(vec![f, vec![0xAB; 128]], "runs past the buffer");
    }

    #[test]
    fn a_write_outside_the_volume_is_refused() {
        refused_untouched(
            vec![buf_item(u64::MAX / 256, 8), vec![0; 128]],
            "outside the volume",
        );
    }

    #[test]
    fn an_oversized_inode_create_is_refused() {
        let mut f = Vec::new();
        f.extend_from_slice(&LI_ICREATE.to_le_bytes());
        f.extend_from_slice(&1u16.to_le_bytes());
        for v in [0u32, 64, u32::MAX, 512, u32::MAX, 1] {
            f.extend_from_slice(&v.to_be_bytes());
        }
        refused_untouched(vec![f], "inode chunk");
    }

    #[test]
    fn a_short_inode_item_is_refused() {
        let mut f = Vec::new();
        f.extend_from_slice(&LI_INODE.to_le_bytes());
        f.extend_from_slice(&1u16.to_le_bytes());
        f.resize(30, 0);
        refused_untouched(vec![f], "inode log item is 30 bytes");
    }

    /// Headers that never form a complete record mean a damaged log, not a clean one.
    #[test]
    fn a_log_of_only_broken_headers_is_unrecoverable_not_clean() {
        let (mut img, sb) = blank();
        let start =
            fsblock_to_partition_byte(sb.logstart, sb.agblocks, sb.agblklog, sb.blocksize) as usize;
        img[start + 8..start + 12].copy_from_slice(&7u32.to_be_bytes()); // no such version
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).unwrap();
        assert!(matches!(
            fs.log_state().unwrap(),
            crate::xfs::log::LogState::Dirty { .. }
        ));
        assert_eq!(fs.run_fsck().unwrap().errors[0].code, "LogUnrecoverable");
        fs.discard_log().unwrap();
        assert_eq!(fs.log_state().unwrap(), crate::xfs::log::LogState::Clean);
    }

    /// The last AG can be shorter than `agblocks`; an inode chunk past the volume's end is refused.
    #[test]
    fn an_inode_create_past_the_end_of_a_short_last_ag_is_refused() {
        let (mut img, mut sb) = blank();
        let last = u64::from(sb.agcount - 1);
        let agbno = sb.agblocks - 16;
        sb.dblocks = last * u64::from(sb.agblocks) + u64::from(agbno) + 4;
        let mut f = Vec::new();
        f.extend_from_slice(&LI_ICREATE.to_le_bytes());
        f.extend_from_slice(&1u16.to_le_bytes());
        let blocks = 64 * u32::from(sb.inodesize) / sb.blocksize;
        for v in [last as u32, agbno, 64, u32::from(sb.inodesize), blocks, 1] {
            f.extend_from_slice(&v.to_be_bytes());
        }
        put_record(
            &mut img,
            &sb,
            2,
            &[
                (3, OP_START, vec![]),
                (3, 0, trans_header(3, 1)),
                (3, 0, f),
                (3, OP_COMMIT, vec![]),
            ],
        );
        let mut dev = Cursor::new(img);
        match replay_log(&mut dev, 0, &sb).unwrap() {
            ReplayOutcome::Unrecoverable(m) => assert!(m.contains("inode chunk"), "{m}"),
            other => panic!("{other:?}"),
        }
    }
}
