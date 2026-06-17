//! Reader for the Xerox CopyDisk stream format, as stored in the period
//! `.bfs` / `.copydisk` / `.altodisk` disk-image files.
//!
//! A CopyDisk file is a verbatim recording of the CopyDisk network protocol
//! (there is no separate file wrapper). It is a sequence of big-endian blocks:
//!
//! ```text
//! block = [ length: u16 ][ type: u16 ][ payload: (length-2) words ]
//! ```
//!
//! where `length` is the block length in 16-bit words **including the length
//! word**, so a block occupies `length * 2` bytes. The stream is:
//!
//! 1. one `hereAreDiskParams` block (type 3): `diskType, nCyl, nHeads, nSec,
//!    nDisks`;
//! 2. one `hereIsDiskPage` block (type 6) per sector, in disk-address order.
//!    A full page's payload is `header(2w) + label(Lw) + data(Dw)`; a free
//!    page is stored compressed as `header(2w) + label(Lw)` with the data
//!    words dropped;
//! 3. one `endOfTransfer` block (type 7).
//!
//! Block types and the page-payload layout follow `CopyDisk.decl` /
//! `CopyDiskBfs.decl` in the PARC archive. Diablo packs (`diskType = 10`) are
//! supported; the Trident page shape differs and is not yet handled.

use super::super::filesystem::FilesystemError;
use super::{be16, Disk, FsFamily, Geometry, Sector};

const BLOCK_PARAMS: u16 = 3; // hereAreDiskParams
const BLOCK_PAGE: u16 = 6; // hereIsDiskPage
const BLOCK_EOT: u16 = 7; // endOfTransfer

const DISKTYPE_DIABLO: u16 = 10; // "AltoDiablo"

const DIABLO_LABEL_BYTES: u16 = 16; // 8 words
const DIABLO_DATA_BYTES: u16 = 512; // 256 words
const HEADER_WORDS: usize = 2;

/// A parsed CopyDisk block: its type and the byte range of its payload within
/// the source buffer.
struct Block {
    block_type: u16,
    payload_start: usize,
    payload_end: usize,
    /// Total bytes the block occupied (for advancing the cursor).
    total_bytes: usize,
}

fn read_block(bytes: &[u8], pos: usize) -> Result<Block, FilesystemError> {
    if pos + 4 > bytes.len() {
        return Err(FilesystemError::Parse(
            "CopyDisk: truncated block header".into(),
        ));
    }
    let length_words = be16(bytes, pos) as usize;
    if length_words < 2 {
        return Err(FilesystemError::Parse(format!(
            "CopyDisk: block length {length_words} words is too small at offset {pos}"
        )));
    }
    let total_bytes = length_words * 2;
    if pos + total_bytes > bytes.len() {
        return Err(FilesystemError::Parse(format!(
            "CopyDisk: block at offset {pos} runs past end of file"
        )));
    }
    Ok(Block {
        block_type: be16(bytes, pos + 2),
        payload_start: pos + 4,
        payload_end: pos + total_bytes,
        total_bytes,
    })
}

/// Parse a CopyDisk stream into an in-memory [`Disk`].
pub fn read(bytes: &[u8]) -> Result<Disk, FilesystemError> {
    // First block: disk parameters.
    let params = read_block(bytes, 0)?;
    if params.block_type != BLOCK_PARAMS {
        return Err(FilesystemError::Parse(format!(
            "CopyDisk: expected params block (type {BLOCK_PARAMS}), got type {}",
            params.block_type
        )));
    }
    let p = &bytes[params.payload_start..params.payload_end];
    if p.len() < 10 {
        return Err(FilesystemError::Parse(
            "CopyDisk: params block too short".into(),
        ));
    }
    let disk_type = be16(p, 0);
    let n_cylinders = be16(p, 2);
    let n_heads = be16(p, 4);
    let n_sectors = be16(p, 6);
    let n_disks = be16(p, 8);

    if disk_type != DISKTYPE_DIABLO {
        return Err(FilesystemError::Unsupported(format!(
            "CopyDisk: diskType {disk_type} (only Diablo, type {DISKTYPE_DIABLO}, is supported)"
        )));
    }

    let geometry = Geometry {
        family: FsFamily::Diablo,
        disk_model: match n_cylinders {
            203 => 31,
            406 => 44,
            _ => 0,
        },
        n_disks,
        n_cylinders,
        n_heads,
        n_sectors,
        label_bytes: DIABLO_LABEL_BYTES,
        data_bytes: DIABLO_DATA_BYTES,
    };

    let total = geometry.total_sectors();
    if total == 0 {
        return Err(FilesystemError::Parse(
            "CopyDisk: degenerate geometry (zero sectors)".into(),
        ));
    }
    let label_bytes = geometry.label_bytes as usize;
    let data_bytes = geometry.data_bytes as usize;
    let header_bytes = HEADER_WORDS * 2;

    let mut sectors: Vec<Sector> = (0..total)
        .map(|_| Sector::zeroed(label_bytes, data_bytes))
        .collect();
    let mut placed = vec![false; total];
    let mut n_full = 0usize;
    let mut n_free = 0usize;

    let mut pos = params.total_bytes;
    loop {
        let block = read_block(bytes, pos)?;
        pos += block.total_bytes;
        match block.block_type {
            BLOCK_EOT => break,
            BLOCK_PAGE => {}
            other => {
                return Err(FilesystemError::Parse(format!(
                    "CopyDisk: unexpected block type {other}"
                )));
            }
        }

        let payload = &bytes[block.payload_start..block.payload_end];
        if payload.len() < header_bytes + label_bytes {
            return Err(FilesystemError::Parse(
                "CopyDisk: page block shorter than header+label".into(),
            ));
        }
        // header word 1 holds the disk address; word 0 is the pack id (0).
        let da = be16(payload, 2);
        let vda = geometry.vda_from_da(da);
        if vda >= total {
            return Err(FilesystemError::Parse(format!(
                "CopyDisk: page disk-address {da:#06x} -> VDA {vda} out of range (total {total})"
            )));
        }

        let label = payload[header_bytes..header_bytes + label_bytes].to_vec();
        let data_region = &payload[header_bytes + label_bytes..];
        let data = if data_region.len() >= data_bytes {
            // Full page.
            n_full += 1;
            data_region[..data_bytes].to_vec()
        } else if data_region.is_empty() {
            // Compressed free page: header+label only, data dropped.
            n_free += 1;
            vec![0u8; data_bytes]
        } else {
            return Err(FilesystemError::Parse(format!(
                "CopyDisk: page at VDA {vda} has {} trailing bytes (expected 0 or {data_bytes})",
                data_region.len()
            )));
        };

        sectors[vda] = Sector { label, data };
        placed[vda] = true;
    }

    let missing = placed.iter().filter(|&&b| !b).count();
    if missing != 0 {
        // The stream is expected to cover the whole pack; tolerate gaps but
        // surface them so a malformed pack doesn't masquerade as complete.
        log::warn!(
            "CopyDisk: {missing} of {total} sectors were not present in the stream (left zeroed)"
        );
    }
    log::debug!(
        "CopyDisk: parsed {total} sectors ({n_full} full, {n_free} free-compressed), model {}",
        geometry.disk_model
    );

    Ok(Disk { geometry, sectors })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal one-page CopyDisk stream and confirm it parses to the
    /// right geometry and places the page at the expected VDA.
    #[test]
    fn parse_synthetic_single_page() {
        // Geometry: 1 disk, 2 cylinders, 2 heads, 3 sectors = 12 sectors.
        let mut buf: Vec<u8> = Vec::new();
        let push_word = |b: &mut Vec<u8>, w: u16| b.extend_from_slice(&w.to_be_bytes());

        // params block: len=7, type=3, diskType=10, cyl=2, heads=2, sec=3, disks=1
        push_word(&mut buf, 7);
        push_word(&mut buf, BLOCK_PARAMS);
        push_word(&mut buf, DISKTYPE_DIABLO);
        push_word(&mut buf, 2);
        push_word(&mut buf, 2);
        push_word(&mut buf, 3);
        push_word(&mut buf, 1);

        // one full page at DA encoding (sector=2, track=1, head=0, disk=0).
        // DA bits: sector<<12 | track<<3 | head<<2 | disk<<1
        let da: u16 = (2 << 12) | (1 << 3);
        // VDA = (((0*2 + 1)*2 + 0)*3 + 2) = (2*3 + 2) = 8
        let page_words = 2 /*framing*/ + 2 /*header*/ + 8 /*label*/ + 256 /*data*/;
        push_word(&mut buf, page_words as u16);
        push_word(&mut buf, BLOCK_PAGE);
        push_word(&mut buf, 0); // header word0 packID
        push_word(&mut buf, da); // header word1 DA
        for _ in 0..8 {
            push_word(&mut buf, 0xabcd); // label
        }
        for _ in 0..256 {
            push_word(&mut buf, 0x1234); // data
        }

        // endOfTransfer: len=2, type=7
        push_word(&mut buf, 2);
        push_word(&mut buf, BLOCK_EOT);

        let disk = read(&buf).expect("parse");
        assert_eq!(disk.geometry.total_sectors(), 12);
        assert_eq!(disk.geometry.family, FsFamily::Diablo);
        assert_eq!(disk.geometry.vda_from_da(da), 8);
        let s = disk.sector(8).unwrap();
        assert_eq!(be16(&s.label, 0), 0xabcd);
        assert_eq!(be16(&s.data, 0), 0x1234);
        // a sector the stream didn't mention is zeroed
        assert_eq!(be16(&disk.sector(0).unwrap().data, 0), 0);
    }
}
