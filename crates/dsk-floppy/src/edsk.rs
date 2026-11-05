//! EDSK / DSK container — the canonical CPCEMU floppy format used by every
//! Amstrad CPC / PCW / Tatung Einstein emulator, and by the MiSTer
//! AmstradPCW / Amstrad / TatungEinstein / Oric cores. Also used by some
//! CP/M floppies on other 8-bit micros.
//!
//! Two flavours of the wrapper share a near-identical layout:
//!
//! - **DSK** (standard CPCEMU) — every track is the same size, advertised
//!   in the disk-info header at offset 0x32. Used by most Amstrad CPC
//!   floppy dumps.
//! - **EDSK** (extended CPCEMU) — per-track size (in 256-byte units) is
//!   carried in the trailing 204-byte table at 0x34..0x100 of the disk-info
//!   header, allowing copy-protected / weird-track disks.
//!
//! The 256-byte disk-info header at offset 0:
//!
//! ```text
//! 0x00..0x22  identifier — "MV - CPCEMU Disk-File\r\n" (DSK) or
//!             "EXTENDED CPC DSK File\r\n" (EDSK), space-padded to 34 B
//! 0x22..0x30  creator string (14 B)
//! 0x30        number of tracks
//! 0x31        number of sides
//! 0x32..0x34  track size in bytes (LE u16) — DSK only; EDSK has 0 here
//! 0x34..0x100 per-track size in 256-B units (1 B per track) — EDSK only
//! ```
//!
//! Each track block starts on a 256-B-aligned boundary and contains a
//! 256-byte track-info header followed by the raw sector data:
//!
//! ```text
//! 0x00..0x0D  identifier — "Track-Info\r\n", space-padded
//! 0x0D..0x10  unused
//! 0x10        track number
//! 0x11        side number
//! 0x12..0x14  unused
//! 0x14        sector size code (0=128, 1=256, 2=512, 3=1024, ...)
//! 0x15        number of sectors on this track
//! 0x16        GAP3 length
//! 0x17        filler byte
//! 0x18..0x100 per-sector info (8 B each, up to 29 sectors):
//!     +0  track
//!     +1  side
//!     +2  sector ID
//!     +3  size code (0=128, 1=256, 2=512, 3=1024)
//!     +4  FDC status register 1
//!     +5  FDC status register 2
//!     +6..+8  actual sector data length in bytes (EDSK only; LE u16)
//! ```
//!
//! Sector data follows the track-info header, in sector-ID order as listed.
//! Decoded output: a flat byte stream where sectors are laid down in
//! **logical (CHS-ordered)** sequence — track 0 side 0 sector 1, track 0
//! side 0 sector 2, ..., track 0 side 1 sector 1, ..., track 1 side 0
//! sector 1, etc. This matches what the CP/M / FAT / GEMDOS drivers
//! downstream expect.
//!
//! Reference: <http://www.cpctech.org.uk/docs/extdsk.html> + the CPCEMU
//! `cpcemu.dsk` source.

use anyhow::{anyhow, bail, Result};

/// DSK / EDSK header always occupies exactly 0x100 bytes.
pub const HEADER_SIZE: usize = 0x100;

/// Track-info header always occupies exactly 0x100 bytes.
pub const TRACK_INFO_SIZE: usize = 0x100;

/// Maximum sectors per track the format can describe (per the 8-B
/// per-sector table that runs 0x18..0x100 of the track-info header).
pub const MAX_SECTORS_PER_TRACK: usize = (TRACK_INFO_SIZE - 0x18) / 8;

/// True if `head` starts with the standard or extended CPCEMU magic.
/// The longer magic is 21 bytes, so any buffer ≥ 21 B can be checked.
pub fn looks_like_edsk_header(head: &[u8]) -> bool {
    head.starts_with(b"MV - CPCEMU Disk-File") || head.starts_with(b"EXTENDED CPC DSK File")
}

/// Decoded geometry — useful for inspect / debugging.
#[derive(Debug, Clone)]
pub struct EdskGeometry {
    pub is_extended: bool,
    pub creator: String,
    pub num_tracks: u8,
    pub num_sides: u8,
    /// For DSK: every track shares this byte size. For EDSK: 0.
    pub track_size_dsk: u16,
    /// For EDSK only: each entry is the track size in 256-B units. 0 means
    /// the track is absent (unformatted side B, for example).
    pub track_sizes_edsk: Vec<u8>,
}

/// One decoded sector, after the on-disk re-ordering.
#[derive(Debug, Clone)]
struct DecodedSector {
    cyl: u8,
    head: u8,
    sector_id: u8,
    data: Vec<u8>,
}

/// Decode an EDSK / DSK byte stream into a flat sector image.
///
/// Sector layout in the output: tracks in cylinder order, within each
/// track sides in head order, within each side sectors in **logical**
/// sector-ID order (ascending). Sector size is taken from the per-
/// sector size code in the track-info header so weird-size disks still
/// round-trip correctly.
pub fn decode_edsk_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    if !looks_like_edsk_header(bytes) {
        bail!("not an EDSK / DSK image (header magic mismatch)");
    }
    if bytes.len() < HEADER_SIZE {
        bail!("EDSK truncated: only {} bytes", bytes.len());
    }
    let geom = parse_header(&bytes[..HEADER_SIZE])?;

    // Walk every track block.
    let mut cursor = HEADER_SIZE;
    let mut all_sectors: Vec<DecodedSector> = Vec::new();
    let total_tracks = geom.num_tracks as usize * geom.num_sides as usize;
    for ti in 0..total_tracks {
        let track_size = if geom.is_extended {
            *geom
                .track_sizes_edsk
                .get(ti)
                .ok_or_else(|| anyhow!("EDSK header missing track {ti}'s size"))?
                as usize
                * 256
        } else {
            geom.track_size_dsk as usize
        };
        if track_size == 0 {
            // Unformatted track — skip past nothing; the EDSK layout
            // simply omits it.
            continue;
        }
        if cursor + track_size > bytes.len() {
            bail!(
                "EDSK truncated mid-track: cursor={cursor} +{track_size} > {}",
                bytes.len()
            );
        }
        let block = &bytes[cursor..cursor + track_size];
        decode_one_track(block, geom.is_extended, &mut all_sectors)?;
        cursor += track_size;
    }

    // Re-order: ascending (cyl, head, sector_id).
    all_sectors.sort_by_key(|s| (s.cyl, s.head, s.sector_id));

    // Pack flat output.
    let mut out = Vec::with_capacity(all_sectors.iter().map(|s| s.data.len()).sum());
    for s in &all_sectors {
        out.extend_from_slice(&s.data);
    }
    Ok(out)
}

fn parse_header(buf: &[u8]) -> Result<EdskGeometry> {
    debug_assert_eq!(buf.len(), HEADER_SIZE);
    let is_extended = buf.starts_with(b"EXTENDED CPC DSK File");
    let creator = ascii_trim(&buf[0x22..0x30]);
    let num_tracks = buf[0x30];
    let num_sides = buf[0x31];
    if num_sides == 0 || num_sides > 2 {
        bail!("EDSK has bogus side count {num_sides}");
    }
    let track_size_dsk = u16::from_le_bytes([buf[0x32], buf[0x33]]);
    let track_sizes_edsk: Vec<u8> = if is_extended {
        buf[0x34..0x100].to_vec()
    } else {
        Vec::new()
    };
    Ok(EdskGeometry {
        is_extended,
        creator,
        num_tracks,
        num_sides,
        track_size_dsk,
        track_sizes_edsk,
    })
}

fn decode_one_track(block: &[u8], is_extended: bool, out: &mut Vec<DecodedSector>) -> Result<()> {
    if block.len() < TRACK_INFO_SIZE {
        bail!("EDSK track block too small: {}", block.len());
    }
    let hdr = &block[..TRACK_INFO_SIZE];
    if !hdr.starts_with(b"Track-Info") {
        bail!("EDSK track header missing 'Track-Info' magic");
    }
    let track = hdr[0x10];
    let side = hdr[0x11];
    let _sector_size_code = hdr[0x14];
    let num_sectors = hdr[0x15] as usize;
    if num_sectors > MAX_SECTORS_PER_TRACK {
        bail!(
            "EDSK track {track}/{side} claims {num_sectors} sectors (max {MAX_SECTORS_PER_TRACK})"
        );
    }
    // Sector blob starts after the 256-byte track-info header. Walk the
    // per-sector table to figure out each sector's actual byte length
    // (EDSK can describe holes / oversize sectors via the +6..+8 entry).
    let mut data_off = TRACK_INFO_SIZE;
    for i in 0..num_sectors {
        let entry_off = 0x18 + i * 8;
        let cyl_id = hdr[entry_off];
        let head_id = hdr[entry_off + 1];
        let sector_id = hdr[entry_off + 2];
        let size_code = hdr[entry_off + 3];
        let logical_size = size_code_to_bytes(size_code);
        let actual_size = if is_extended {
            let raw = u16::from_le_bytes([hdr[entry_off + 6], hdr[entry_off + 7]]) as usize;
            // 0 in EDSK means "use the size-code default" per the
            // common interpretation.
            if raw == 0 {
                logical_size
            } else {
                raw
            }
        } else {
            logical_size
        };
        if data_off + actual_size > block.len() {
            bail!(
                "EDSK sector {sector_id} on T{track}/H{side} truncated: \
                 want {actual_size} B at +{data_off}, block is {} B",
                block.len()
            );
        }
        let data = block[data_off..data_off + actual_size].to_vec();
        // If the sector is oversized (weird-disk / copy-protection), we
        // keep only the logical-size prefix when emitting flat output —
        // the downstream filesystem driver expects logical-size sectors.
        let mut emit = data;
        if emit.len() > logical_size {
            emit.truncate(logical_size);
        } else if emit.len() < logical_size {
            // Short sector: zero-pad to logical size.
            emit.resize(logical_size, 0);
        }
        out.push(DecodedSector {
            cyl: cyl_id,
            head: head_id,
            sector_id,
            data: emit,
        });
        data_off += actual_size;
    }
    Ok(())
}

/// Convert an FDC size-code (0=128, 1=256, 2=512, 3=1024, 4=2048, ...) to
/// bytes. Caps at 6 (8192) which is the practical CPC limit.
fn size_code_to_bytes(code: u8) -> usize {
    128usize << (code.min(6) as usize)
}

fn ascii_trim(buf: &[u8]) -> String {
    let s: String = buf
        .iter()
        .map(|&b| {
            if (0x20..=0x7E).contains(&b) {
                b as char
            } else {
                ' '
            }
        })
        .collect();
    s.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal DSK image: 2 tracks × 1 side × 9 sectors × 512 B.
    /// Each sector is filled with a recognisable pattern (track_id ^ 0x80
    /// in byte 0, sector_id ^ 0x40 in byte 1, then zeros).
    fn build_simple_dsk() -> Vec<u8> {
        const TRACKS: usize = 2;
        const SIDES: usize = 1;
        const SECTORS: usize = 9;
        const SECT_BYTES: usize = 512;
        const TRACK_DATA: usize = SECTORS * SECT_BYTES; // 4608
        const TRACK_TOTAL: usize = TRACK_INFO_SIZE + TRACK_DATA; // 4864

        let mut img = vec![0u8; HEADER_SIZE + TRACKS * SIDES * TRACK_TOTAL];

        // Disk-info header
        img[0..21].copy_from_slice(b"MV - CPCEMU Disk-File");
        img[21] = 0x0D;
        img[22] = 0x0A;
        img[0x22..0x2C].copy_from_slice(b"rb-cli-tst");
        img[0x30] = TRACKS as u8;
        img[0x31] = SIDES as u8;
        let track_total_le = (TRACK_TOTAL as u16).to_le_bytes();
        img[0x32] = track_total_le[0];
        img[0x33] = track_total_le[1];

        // Per-track blocks
        for ti in 0..TRACKS * SIDES {
            let block_off = HEADER_SIZE + ti * TRACK_TOTAL;
            let block = &mut img[block_off..block_off + TRACK_TOTAL];
            block[0..10].copy_from_slice(b"Track-Info");
            block[10] = 0x0D;
            block[11] = 0x0A;
            #[allow(clippy::modulo_one)]
            let head_id = (ti % SIDES) as u8;
            block[0x10] = (ti / SIDES) as u8; // cyl
            block[0x11] = head_id;
            block[0x14] = 2; // 512 B / sector
            block[0x15] = SECTORS as u8;
            for si in 0..SECTORS {
                let entry = 0x18 + si * 8;
                block[entry] = (ti / SIDES) as u8;
                #[allow(clippy::modulo_one)]
                let h = (ti % SIDES) as u8;
                block[entry + 1] = h;
                // Use sector IDs 1..=N with a deliberate non-ordered layout
                // to prove the sort works.
                block[entry + 2] = (SECTORS - si) as u8;
                block[entry + 3] = 2;
            }
            // Sector data, written in *physical* on-disk order (matching
            // the per-sector entry order, NOT logical sort).
            for si in 0..SECTORS {
                let sector_id = SECTORS - si;
                let data_off = TRACK_INFO_SIZE + si * SECT_BYTES;
                let payload = &mut block[data_off..data_off + SECT_BYTES];
                payload[0] = (ti / SIDES) as u8 ^ 0x80;
                payload[1] = sector_id as u8 ^ 0x40;
            }
        }
        img
    }

    #[test]
    fn detect_recognises_dsk_and_edsk() {
        let head = b"MV - CPCEMU Disk-File\r\n";
        assert!(looks_like_edsk_header(head));
        let head = b"EXTENDED CPC DSK File\r\n";
        assert!(looks_like_edsk_header(head));
        let head = b"NOT A DISK FILE";
        assert!(!looks_like_edsk_header(head));
    }

    #[test]
    fn decode_simple_dsk_reorders_sectors_by_id() {
        let img = build_simple_dsk();
        let flat = decode_edsk_bytes(&img).unwrap();
        // 2 tracks × 1 side × 9 sectors × 512 B = 9216 bytes.
        assert_eq!(flat.len(), 9216);
        // First sector in the flat output should be track 0 / sector ID 1.
        // build_simple_dsk seeded byte 0 = track ^ 0x80 = 0x80, byte 1 =
        // sector_id ^ 0x40 = 1 ^ 0x40 = 0x41.
        assert_eq!(flat[0], 0x80);
        assert_eq!(flat[1], 0x41);
        // Sector 2 of track 0 starts at byte 512.
        assert_eq!(flat[512], 0x80);
        assert_eq!(flat[513], 2 ^ 0x40);
        // Track 1 starts at byte 9 * 512 = 4608.
        assert_eq!(flat[4608], 1 ^ 0x80);
        assert_eq!(flat[4609], 1 ^ 0x40);
    }

    #[test]
    fn decode_rejects_non_edsk_header() {
        let buf = b"random garbage data here ........................".to_vec();
        let err = decode_edsk_bytes(&buf).unwrap_err();
        assert!(err.to_string().contains("not an EDSK"));
    }

    #[test]
    fn size_code_table() {
        assert_eq!(size_code_to_bytes(0), 128);
        assert_eq!(size_code_to_bytes(1), 256);
        assert_eq!(size_code_to_bytes(2), 512);
        assert_eq!(size_code_to_bytes(3), 1024);
        assert_eq!(size_code_to_bytes(4), 2048);
        // Cap.
        assert_eq!(size_code_to_bytes(100), 8192);
    }
}
