//! Sharp `.d88` floppy container — the canonical Japanese-emulator format
//! used by every X68000, PC-88, PC-98, MSX, FM-7, and FM Towns emulator,
//! and by the MiSTer X68000 + PC88 cores.
//!
//! ## Disk-info header (32 bytes)
//!
//! ```text
//! 0x00..0x10  disk name, ASCII, NUL-padded (rarely set)
//! 0x10..0x1A  reserved, 10 bytes
//! 0x1A        write-protect flag (0x00 = R/W, 0x10 = R/O)
//! 0x1B        media type (0x00 = 2D, 0x10 = 2DD, 0x20 = 2HD)
//! 0x1C..0x20  total disk size in bytes (LE u32, including this header)
//! ```
//!
//! ## Track-offset table (164 × LE u32 immediately after the header)
//!
//! Each entry is the byte offset of a track's data within the file, or
//! 0 if the track is absent. Tracks are indexed in (cyl, head) order:
//!   track_index = cyl × 2 + head.
//! 164 entries covers the original 82-cylinder × 2-side maximum. Modern
//! .d88 dumps from MiSTer-grade tooling rarely exceed 84 entries (42 cyl
//! × 2 sides for 2D, 77 cyl × 2 sides for 2HD, 80 cyl × 2 sides for 2DD).
//!
//! ## Per-track layout
//!
//! Each non-zero track-offset points at a sequence of sectors. Every
//! sector starts with a 16-byte sector-info header:
//!
//! ```text
//! 0x00  C  cylinder ID (logical)
//! 0x01  H  head ID    (logical)
//! 0x02  R  sector ID  (logical; sector-sort key)
//! 0x03  N  size code  (0=128, 1=256, 2=512, 3=1024, 4=2048, 5=4096, 6=8192)
//! 0x04  num_sectors_in_track  LE u16  (same value repeated in every sector)
//! 0x06  density               (0=double, 0x40=single)
//! 0x07  deleted-data mark     (0=normal, 0x10=deleted)
//! 0x08  status                (0=ok, 0xA0=no record, 0xB0=CRC data,
//!                              0xE0=CRC ID, 0xF0=no record, …)
//! 0x09..0x0E  reserved (5 bytes)
//! 0x0E..0x10  sector data length  LE u16
//! ```
//!
//! Then `data_length` bytes of sector data follow. Sectors inside a track
//! are written in physical (recorded) order; we re-sort by `(C, H, R)`
//! ascending so the output matches what an OS BIOS reads.
//!
//! Reference: <http://www.geocities.jp/origin1900/tools/d88format.txt> and
//! the BurnedHurr / D88List source. The spec is small and stable; hand-
//! written here per `docs/mister_filesystem_implementation_plan.md` §3.2
//! (no new crate dependencies).

use anyhow::{bail, Result};

/// Disk-info header is always 32 bytes.
pub const HEADER_SIZE: usize = 0x20;

/// Track-offset table has 164 LE u32 entries.
pub const MAX_TRACKS: usize = 164;

/// Per-sector header is always 16 bytes (followed by `data_length` bytes
/// of sector data).
pub const SECTOR_INFO_SIZE: usize = 0x10;

/// Maximum sector size we'll accept (N=6 = 8192 bytes). Anything larger
/// indicates corruption or a malformed `.d88`.
const MAX_SECTOR_DATA: usize = 8192;

/// Per-track absolute sanity cap. A real `.d88` track holds at most ~26
/// sectors of 1024 B each (~26 KB); 64 KB is a safe upper bound that
/// catches obviously bogus headers without rejecting weird-disk dumps.
const MAX_TRACK_BYTES: usize = 0x10_000;

/// Decoded media kind. Only used for diagnostics today.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum D88Media {
    /// 2D — 360 KB single-sided / 720 KB double-sided 40-cylinder.
    Dd2,
    /// 2DD — 720 KB / 1.44 MB 80-cylinder double-density.
    Dd2dd,
    /// 2HD — 1.2 MB / 1.6 MB high-density (X68000 default).
    Dd2hd,
}

/// Decoded `.d88` geometry.
#[derive(Debug, Clone)]
pub struct D88Geometry {
    pub name: String,
    pub write_protected: bool,
    pub media: D88Media,
    /// Total disk size in bytes as recorded in the disk-info header.
    pub disk_size: u32,
    /// Per-track byte offsets. `Some(off)` if the track is present,
    /// `None` if absent. Length is `MAX_TRACKS`.
    pub track_offsets: Vec<Option<u32>>,
}

impl D88Geometry {
    /// Highest-numbered present track + 1. Useful for diagnostics; not
    /// used during decode (we walk every non-zero entry).
    pub fn track_count(&self) -> usize {
        self.track_offsets
            .iter()
            .rposition(|o| o.is_some())
            .map(|p| p + 1)
            .unwrap_or(0)
    }
}

/// True if `head` looks like the start of a `.d88` file. The signature
/// is weak (no magic string) — we accept any buffer whose media-type
/// byte is one of the three recognised values AND whose first track
/// offset (LE u32 at byte 0x20) lands inside the file's header region
/// (≥ HEADER_SIZE, ≤ HEADER_SIZE + MAX_TRACKS*4).
pub fn looks_like_d88_header(head: &[u8]) -> bool {
    if head.len() < HEADER_SIZE + 4 {
        return false;
    }
    if !matches!(head[0x1B], 0x00 | 0x10 | 0x20) {
        return false;
    }
    // Write-protect byte must be 0 or 0x10.
    if !matches!(head[0x1A], 0x00 | 0x10) {
        return false;
    }
    // Reserved region (bytes 0x10..0x1A) is conventionally zero in
    // real-world `.d88` dumps; loose enough to allow a stray byte.
    let reserved_nonzero = head[0x10..0x1A].iter().filter(|b| **b != 0).count();
    if reserved_nonzero > 2 {
        return false;
    }
    // First track offset (LE u32) must point at or past the end of the
    // 656-byte track-offset table, AND a zero value (no tracks at all)
    // is technically valid — but then it's an empty disk and we'd rather
    // refuse than treat random bytes as `.d88`. So insist track 0 is
    // present unless explicitly an empty disk.
    let first_off = u32::from_le_bytes([head[0x20], head[0x21], head[0x22], head[0x23]]);
    let min_after_table = HEADER_SIZE as u32 + (MAX_TRACKS as u32 * 4);
    // Allow track 0 to be exactly at the end-of-table OR absent (== 0).
    // Anything in between is suspicious.
    first_off == 0 || first_off >= min_after_table
}

/// One decoded sector, kept until we sort tracks by (C, H, R).
#[derive(Debug, Clone)]
struct DecodedSector {
    cyl: u8,
    head: u8,
    sector_id: u8,
    data: Vec<u8>,
}

/// Decode a `.d88` byte stream into a flat sector image.
///
/// Output layout: tracks in (cylinder, head) order, sectors within a
/// track in ascending sector-id order. Each sector contributes its
/// declared data length; if a track holds sectors of mixed sizes the
/// flat output preserves them in order (unusual, but allowed by the
/// format).
pub fn decode_d88_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let geom = parse_header(bytes)?;
    let mut out = Vec::new();
    for (track_index, off) in geom.track_offsets.iter().enumerate() {
        let Some(track_off) = off else { continue };
        let track_off = *track_off as usize;
        if track_off >= bytes.len() {
            bail!(
                "d88 track {track_index}: offset {track_off} past end of \
                 {}-byte file",
                bytes.len()
            );
        }
        // Determine track length: from this offset to the next non-zero
        // offset (or EOF).
        let next_off = geom
            .track_offsets
            .iter()
            .skip(track_index + 1)
            .flatten()
            .copied()
            .next()
            .map(|v| v as usize)
            .unwrap_or(bytes.len());
        if next_off <= track_off {
            bail!(
                "d88 track {track_index}: next-track offset {next_off} not \
                 after current {track_off}"
            );
        }
        if next_off - track_off > MAX_TRACK_BYTES {
            bail!(
                "d88 track {track_index}: claimed size {} exceeds {MAX_TRACK_BYTES}-byte cap",
                next_off - track_off
            );
        }
        let block = &bytes[track_off..next_off];
        let mut sectors = decode_one_track(block, track_index)?;
        sectors.sort_by_key(|s| (s.cyl, s.head, s.sector_id));
        for s in &sectors {
            out.extend_from_slice(&s.data);
        }
    }
    Ok(out)
}

fn parse_header(bytes: &[u8]) -> Result<D88Geometry> {
    if bytes.len() < HEADER_SIZE + MAX_TRACKS * 4 {
        bail!(
            "d88 truncated: need {} header bytes, got {}",
            HEADER_SIZE + MAX_TRACKS * 4,
            bytes.len()
        );
    }
    let name = ascii_trim(&bytes[0x00..0x10]);
    let write_protected = match bytes[0x1A] {
        0x00 => false,
        0x10 => true,
        b => bail!("d88 unknown write-protect byte {:#x}", b),
    };
    let media = match bytes[0x1B] {
        0x00 => D88Media::Dd2,
        0x10 => D88Media::Dd2dd,
        0x20 => D88Media::Dd2hd,
        b => bail!("d88 unknown media-type byte {:#x}", b),
    };
    let disk_size = u32::from_le_bytes([bytes[0x1C], bytes[0x1D], bytes[0x1E], bytes[0x1F]]);
    // Track-offset table follows immediately.
    let mut track_offsets = Vec::with_capacity(MAX_TRACKS);
    let table = &bytes[HEADER_SIZE..HEADER_SIZE + MAX_TRACKS * 4];
    for chunk in table.chunks_exact(4) {
        let raw = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        track_offsets.push(if raw == 0 { None } else { Some(raw) });
    }
    Ok(D88Geometry {
        name,
        write_protected,
        media,
        disk_size,
        track_offsets,
    })
}

fn decode_one_track(block: &[u8], track_index: usize) -> Result<Vec<DecodedSector>> {
    if block.len() < SECTOR_INFO_SIZE {
        bail!("d88 track {track_index}: block too small ({})", block.len());
    }
    // Read the first sector header to learn how many sectors live in the
    // track. Every sector in the same track repeats the same num_sectors
    // value, so the first one is authoritative.
    let num_sectors = u16::from_le_bytes([block[0x04], block[0x05]]) as usize;
    if num_sectors == 0 || num_sectors > 256 {
        bail!("d88 track {track_index}: implausible sector count {num_sectors}");
    }
    let mut sectors = Vec::with_capacity(num_sectors);
    let mut cursor = 0usize;
    for sector_index in 0..num_sectors {
        if cursor + SECTOR_INFO_SIZE > block.len() {
            bail!(
                "d88 track {track_index} sector {sector_index}: header runs \
                 off end of track block"
            );
        }
        let hdr = &block[cursor..cursor + SECTOR_INFO_SIZE];
        let cyl = hdr[0x00];
        let head = hdr[0x01];
        let sector_id = hdr[0x02];
        let size_code = hdr[0x03];
        let logical_size = size_code_to_bytes(size_code);
        let data_size = u16::from_le_bytes([hdr[0x0E], hdr[0x0F]]) as usize;
        let actual_size = if data_size == 0 {
            logical_size
        } else {
            data_size
        };
        if actual_size > MAX_SECTOR_DATA {
            bail!(
                "d88 track {track_index} sector {sector_index}: data size \
                 {actual_size} exceeds {MAX_SECTOR_DATA}-byte cap"
            );
        }
        let data_off = cursor + SECTOR_INFO_SIZE;
        if data_off + actual_size > block.len() {
            bail!(
                "d88 track {track_index} sector {sector_index}: data runs \
                 off end of track block (off={data_off} +{actual_size} > {})",
                block.len()
            );
        }
        let data = block[data_off..data_off + actual_size].to_vec();
        sectors.push(DecodedSector {
            cyl,
            head,
            sector_id,
            data,
        });
        cursor = data_off + actual_size;
    }
    Ok(sectors)
}

/// Map a `.d88` N-byte size code to the number of bytes per sector.
/// Caps at N=6 = 8192 (the largest the format describes); values >6
/// are clamped to 0 so a malformed code can't generate a huge default.
fn size_code_to_bytes(code: u8) -> usize {
    match code {
        0 => 128,
        1 => 256,
        2 => 512,
        3 => 1024,
        4 => 2048,
        5 => 4096,
        6 => 8192,
        _ => 0,
    }
}

/// Encode a flat sector stream as a `.d88` byte stream.
///
/// Convenience wrapper for fixture-builders and the (eventual) write-back
/// path. Geometry has to be supplied since the flat input doesn't tell us
/// how to slice it into tracks.
///
/// Arguments:
/// - `flat`: raw sector data, with sectors in (cyl, head, sector_id)
///   ascending order — the same shape `decode_d88_bytes` produces, so
///   `encode_d88_bytes(decode_d88_bytes(x), geom)` round-trips.
/// - `cyls` / `heads` / `secs_per_track` / `sec_size`: physical disk
///   geometry. Sector IDs are written as 1..=secs_per_track.
/// - `media`: D88 media-type byte.
///
/// Sector IDs start at 1 (the FDC convention). Status / density / deleted
/// fields are all zero (normal sectors). Sector size code is derived from
/// `sec_size` (must be a power-of-two in {128, 256, 512, 1024, 2048,
/// 4096, 8192}).
pub fn encode_d88_bytes(
    flat: &[u8],
    cyls: u8,
    heads: u8,
    secs_per_track: u8,
    sec_size: usize,
    media: D88Media,
) -> Result<Vec<u8>> {
    let n_code = match sec_size {
        128 => 0u8,
        256 => 1,
        512 => 2,
        1024 => 3,
        2048 => 4,
        4096 => 5,
        8192 => 6,
        _ => bail!("d88 encode: sec_size {sec_size} is not a power-of-two FDC size"),
    };
    let total_tracks = cyls as usize * heads as usize;
    if total_tracks > MAX_TRACKS {
        bail!("d88 encode: {total_tracks} tracks exceeds the 164-entry table cap");
    }
    let track_bytes = secs_per_track as usize * (SECTOR_INFO_SIZE + sec_size);
    let expected_flat = total_tracks * secs_per_track as usize * sec_size;
    if flat.len() != expected_flat {
        bail!(
            "d88 encode: flat size {} mismatches geometry-derived {} \
             ({}cyl × {}heads × {}sec × {}B)",
            flat.len(),
            expected_flat,
            cyls,
            heads,
            secs_per_track,
            sec_size
        );
    }
    let total_size = HEADER_SIZE + MAX_TRACKS * 4 + total_tracks * track_bytes;
    let mut out = vec![0u8; total_size];
    // Header.
    out[0x1B] = match media {
        D88Media::Dd2 => 0x00,
        D88Media::Dd2dd => 0x10,
        D88Media::Dd2hd => 0x20,
    };
    out[0x1C..0x20].copy_from_slice(&(total_size as u32).to_le_bytes());
    // Track-offset table.
    let mut track_off = (HEADER_SIZE + MAX_TRACKS * 4) as u32;
    for t in 0..total_tracks {
        let entry_off = HEADER_SIZE + t * 4;
        out[entry_off..entry_off + 4].copy_from_slice(&track_off.to_le_bytes());
        track_off += track_bytes as u32;
    }
    // Per-track sectors. Tracks are emitted in (cyl, head) index order;
    // sectors within each track are 1..=secs_per_track.
    let mut flat_cursor = 0usize;
    let mut out_cursor = HEADER_SIZE + MAX_TRACKS * 4;
    for t in 0..total_tracks {
        let cyl = (t / heads as usize) as u8;
        let head = (t % heads as usize) as u8;
        for s in 0..secs_per_track {
            let sector_id = s + 1;
            out[out_cursor] = cyl;
            out[out_cursor + 1] = head;
            out[out_cursor + 2] = sector_id;
            out[out_cursor + 3] = n_code;
            out[out_cursor + 0x04..out_cursor + 0x06]
                .copy_from_slice(&(secs_per_track as u16).to_le_bytes());
            // Density / deleted / status / reserved all default 0.
            out[out_cursor + 0x0E..out_cursor + 0x10]
                .copy_from_slice(&(sec_size as u16).to_le_bytes());
            out[out_cursor + SECTOR_INFO_SIZE..out_cursor + SECTOR_INFO_SIZE + sec_size]
                .copy_from_slice(&flat[flat_cursor..flat_cursor + sec_size]);
            flat_cursor += sec_size;
            out_cursor += SECTOR_INFO_SIZE + sec_size;
        }
    }
    Ok(out)
}

fn ascii_trim(bytes: &[u8]) -> String {
    let end = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end])
        .trim_end()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal valid 2HD `.d88` with one cylinder × one head
    /// × `num_sectors` sectors of `sector_size` bytes each. Sector
    /// content is `0xAA + index` so the sort can be observed.
    fn build_minimal_d88(num_sectors: u8, size_code: u8) -> Vec<u8> {
        let sector_size = size_code_to_bytes(size_code);
        let track_bytes = num_sectors as usize * (SECTOR_INFO_SIZE + sector_size);
        let total = HEADER_SIZE + MAX_TRACKS * 4 + track_bytes;
        let mut buf = vec![0u8; total];
        // Header.
        buf[0x1B] = 0x20; // 2HD
        buf[0x1C..0x20].copy_from_slice(&(total as u32).to_le_bytes());
        // Track 0 offset.
        let track_off = (HEADER_SIZE + MAX_TRACKS * 4) as u32;
        buf[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&track_off.to_le_bytes());
        // Sectors — write in physical order [3,1,2] to exercise the sort.
        let physical_order = match num_sectors {
            3 => vec![3u8, 1, 2],
            n => (1..=n).collect(),
        };
        let mut cursor = track_off as usize;
        for (i, sector_id) in physical_order.iter().enumerate() {
            buf[cursor] = 0; // cyl
            buf[cursor + 1] = 0; // head
            buf[cursor + 2] = *sector_id;
            buf[cursor + 3] = size_code;
            buf[cursor + 4..cursor + 6].copy_from_slice(&(num_sectors as u16).to_le_bytes());
            buf[cursor + 0x0E..cursor + 0x10].copy_from_slice(&(sector_size as u16).to_le_bytes());
            // Data: 0xAA + index of the SECTOR ID (not the physical
            // position) so the post-sort output is monotonic.
            for j in 0..sector_size {
                buf[cursor + SECTOR_INFO_SIZE + j] =
                    0xAAu8.wrapping_add(*sector_id).wrapping_add(j as u8);
            }
            cursor += SECTOR_INFO_SIZE + sector_size;
            let _ = i;
        }
        buf
    }

    #[test]
    fn looks_like_d88_accepts_valid_header_only() {
        let buf = build_minimal_d88(8, 3);
        assert!(looks_like_d88_header(&buf[..HEADER_SIZE + 4]));
        // Wrong media byte -> rejected.
        let mut bad = buf.clone();
        bad[0x1B] = 0x55;
        assert!(!looks_like_d88_header(&bad[..HEADER_SIZE + 4]));
        // Wrong write-protect byte -> rejected.
        let mut bad = buf.clone();
        bad[0x1A] = 0x42;
        assert!(!looks_like_d88_header(&bad[..HEADER_SIZE + 4]));
        // Truncated -> rejected.
        assert!(!looks_like_d88_header(&buf[..10]));
    }

    #[test]
    fn decodes_minimal_2hd_disk_and_sorts_sectors() {
        let buf = build_minimal_d88(3, 3); // 3 sectors × 1024 B, physical order [3,1,2]
        let flat = decode_d88_bytes(&buf).unwrap();
        assert_eq!(flat.len(), 3 * 1024);
        // Sector 1 first (id=1), then 2, then 3 — content starts at
        // 0xAA + sector_id at byte 0 of each sector.
        assert_eq!(flat[0], 0xAAu8.wrapping_add(1));
        assert_eq!(flat[1024], 0xAAu8.wrapping_add(2));
        assert_eq!(flat[2048], 0xAAu8.wrapping_add(3));
    }

    #[test]
    fn parse_header_extracts_media_and_size_fields() {
        let buf = build_minimal_d88(8, 3);
        let geom = parse_header(&buf).unwrap();
        assert_eq!(geom.media, D88Media::Dd2hd);
        assert!(!geom.write_protected);
        assert_eq!(geom.track_count(), 1);
        assert!(geom.disk_size as usize == buf.len());
    }

    #[test]
    fn rejects_implausible_sector_count() {
        let mut buf = build_minimal_d88(8, 3);
        let track_off = HEADER_SIZE + MAX_TRACKS * 4;
        // Zero the num_sectors field in the first sector header.
        buf[track_off + 4] = 0;
        buf[track_off + 5] = 0;
        let err = decode_d88_bytes(&buf).unwrap_err().to_string();
        assert!(err.contains("implausible sector count"), "got: {err}");
    }

    #[test]
    fn encode_then_decode_round_trips_flat_bytes() {
        // 2 cyls × 2 heads × 8 sectors × 1024 B = 32 KiB
        let total = 2 * 2 * 8 * 1024;
        let flat: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
        let encoded = encode_d88_bytes(&flat, 2, 2, 8, 1024, D88Media::Dd2hd).unwrap();
        let round_trip = decode_d88_bytes(&encoded).unwrap();
        assert_eq!(round_trip, flat, "encode → decode must round-trip");
    }

    #[test]
    fn encode_rejects_geometry_mismatch() {
        // Pretend we have 1024 B but pass 512 B of flat data.
        let flat = vec![0u8; 512];
        let err = encode_d88_bytes(&flat, 1, 1, 1, 1024, D88Media::Dd2hd)
            .unwrap_err()
            .to_string();
        assert!(err.contains("mismatches"), "got: {err}");
    }

    #[test]
    fn rejects_oversized_sector_data() {
        let mut buf = build_minimal_d88(1, 3);
        let track_off = HEADER_SIZE + MAX_TRACKS * 4;
        // Claim a 9000-byte sector. Past MAX_SECTOR_DATA = 8192.
        let bogus = 9000u16.to_le_bytes();
        buf[track_off + 0x0E] = bogus[0];
        buf[track_off + 0x0F] = bogus[1];
        let err = decode_d88_bytes(&buf).unwrap_err().to_string();
        assert!(err.contains("exceeds"), "got: {err}");
    }
}
