//! Whether an XFS volume's internal log is clean, so an edit cannot be overwritten by recovery.
//!
//! The log is a ring of 512-byte basic blocks. Each record starts with an `xlog_rec_header`
//! (magic `0xFEEDBABE`) carrying its LSN (cycle << 32 | block); the record with the highest LSN
//! is the head. A cleanly unmounted volume's head record is the unmount transaction: one log
//! operation whose `oh_flags` has `XLOG_UNMOUNT_TRANS`. Anything else means the kernel would
//! replay transactions on the next mount, over whatever we wrote. A log of zeros (never written)
//! counts as clean. The scan streams the log 1 MiB at a time.

use std::io::{Read, Seek, SeekFrom};

use disk_core::filesystem::FilesystemError;

use super::bmap::fsblock_to_partition_byte;
use super::sb::XfsSuperblock;

const XLOG_HEADER_MAGIC: u32 = 0xFEED_BABE;
const XLOG_UNMOUNT_TRANS: u8 = 0x20;
const BB: usize = 512;
/// Bytes of record data each header basic block describes (`XLOG_HEADER_CYCLE_SIZE`).
const HEADER_CYCLE_SIZE: u32 = 32 * 1024;

/// What the head of the log says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogState {
    /// The last record is an unmount record, or the log was never written.
    Clean,
    /// The log holds transactions a mount would replay.
    Dirty { head_lsn: u64 },
    /// The log is on another device; there is nothing here to check.
    External,
}

fn be32(b: &[u8], o: usize) -> u32 {
    u32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}

/// Read the internal log at `partition_offset` and say whether it is clean.
pub fn log_state<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    sb: &XfsSuperblock,
) -> Result<LogState, FilesystemError> {
    if sb.logstart == 0 {
        return Ok(LogState::External);
    }
    let start = partition_offset
        + fsblock_to_partition_byte(sb.logstart, sb.agblocks, sb.agblklog, sb.blocksize);
    let len = u64::from(sb.logblocks) * u64::from(sb.blocksize);
    let total_bbs = (len / BB as u64) as usize;

    // The head: the record header with the highest LSN, and where it sits.
    let mut head: Option<(u64, usize, Vec<u8>)> = None;
    let mut buf = vec![0u8; 1 << 20];
    let mut bb = 0usize;
    reader.seek(SeekFrom::Start(start))?;
    while bb < total_bbs {
        let n = (total_bbs - bb).min(buf.len() / BB);
        reader.read_exact(&mut buf[..n * BB])?;
        for i in 0..n {
            let blk = &buf[i * BB..(i + 1) * BB];
            if be32(blk, 0) != XLOG_HEADER_MAGIC {
                continue;
            }
            let lsn = u64::from_be_bytes(blk[16..24].try_into().expect("8 bytes"));
            if !matches!(&head, Some((h, _, _)) if lsn <= *h) {
                head = Some((lsn, bb + i, blk.to_vec()));
            }
        }
        bb += n;
    }
    let Some((lsn, at, header)) = head else {
        return Ok(LogState::Clean);
    };

    // A clean head holds one operation, flagged as the unmount transaction.
    let version = be32(&header, 8);
    let size = be32(&header, 320);
    let header_bbs = if version == 2 && size > HEADER_CYCLE_SIZE {
        size.div_ceil(HEADER_CYCLE_SIZE) as usize
    } else {
        1
    };
    let ops = be32(&header, 40);
    let op_bb = (at + header_bbs) % total_bbs.max(1);
    reader.seek(SeekFrom::Start(start + (op_bb * BB) as u64))?;
    let mut op = [0u8; BB];
    reader.read_exact(&mut op)?;
    // The op header's first word is the cycle stamp; its flags byte (offset 9) is untouched.
    if ops == 1 && op[9] & XLOG_UNMOUNT_TRANS != 0 {
        Ok(LogState::Clean)
    } else {
        Ok(LogState::Dirty { head_lsn: lsn })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::xfs::XfsFilesystem;
    use disk_core::filesystem::{EditableFilesystem, Filesystem};
    use std::io::Cursor;

    fn blank() -> Vec<u8> {
        let mut img = Cursor::new(Vec::new());
        crate::xfs::format::write_blank_xfs(&mut img, 0, 64 * 1024 * 1024, "LOG").unwrap();
        img.into_inner()
    }

    /// Byte offset of the head record's op header (the record mkfs writes at the log's start).
    fn op_flags_at(img: &[u8]) -> usize {
        let sb = XfsSuperblock::parse(&img[..512]).unwrap();
        fsblock_to_partition_byte(sb.logstart, sb.agblocks, sb.agblklog, sb.blocksize) as usize
            + BB
            + 9
    }

    #[test]
    fn a_fresh_volume_is_clean_and_editable() {
        let mut fs = XfsFilesystem::open(Cursor::new(blank()), 0).unwrap();
        assert_eq!(fs.log_state().unwrap(), LogState::Clean);
        let root = fs.root().unwrap();
        fs.create_directory(&root, "ok", &Default::default())
            .unwrap();
    }

    #[test]
    fn a_dirty_log_refuses_edits() {
        let mut img = blank();
        let at = op_flags_at(&img);
        assert_eq!(img[at], XLOG_UNMOUNT_TRANS);
        img[at] = 0; // the head record is now an ordinary transaction
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).unwrap();
        assert!(matches!(fs.log_state().unwrap(), LogState::Dirty { .. }));
        let root = fs.root().unwrap();
        let err = fs
            .create_directory(&root, "no", &Default::default())
            .unwrap_err();
        assert!(err.to_string().contains("not cleanly unmounted"), "{err}");
        // Browsing still works.
        assert!(fs.list_directory(&root).is_ok());
    }

    #[test]
    fn a_zeroed_log_counts_as_clean() {
        let mut img = blank();
        let sb = XfsSuperblock::parse(&img[..512]).unwrap();
        let start =
            fsblock_to_partition_byte(sb.logstart, sb.agblocks, sb.agblklog, sb.blocksize) as usize;
        let len = sb.logblocks as usize * sb.blocksize as usize;
        img[start..start + len].fill(0);
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).unwrap();
        assert_eq!(fs.log_state().unwrap(), LogState::Clean);
    }
}
