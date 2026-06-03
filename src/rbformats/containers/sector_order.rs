//! Apple-II sector-order container — converts `.po` (ProDOS-order)
//! 140 KB floppy images to `.do` (DOS-order) on the fly.
//!
//! Background: the original Apple ][ Disk II GCR encoding doesn't write
//! sectors sequentially around the track — it interleaves them so that
//! DOS 3.3 (which buffers one sector at a time) doesn't miss the next
//! sector while it's processing the previous one. ProDOS, which reads
//! 512-byte blocks, uses a different interleave.
//!
//! Disk-image files come in two equivalent on-disk byte-orderings:
//!
//! - **DOS-order** (`.do`, sometimes `.dsk`) — sectors stored in DOS
//!   *logical* order. DOS sector S on track T is at byte
//!   `(T*16 + S) * 256`.
//! - **ProDOS-order** (`.po`, sometimes `.dsk`) — sectors stored in the
//!   physical-skew order ProDOS expects. DOS sector S on track T is at
//!   byte `(T*16 + DOS_TO_PO[S]) * 256`.
//!
//! Same disk content, same bytes, different ordering. Both file formats
//! are 35 tracks × 16 sectors × 256 B = 143,360 B (140 KB).
//!
//! The interleave table is the canonical one used by every Apple-II tool
//! (CiderPress2, a2kit, AppleSauce, AppleCommander). It's symmetric
//! ("involutive") except for sectors 0 and 15, which map to themselves:
//!
//! ```text
//! DOS sector :  0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15
//! .po slot   :  0 14 13 12 11 10  9  8  7  6  5  4  3  2  1 15
//! ```
//!
//! Our convention: the rest of the filesystem stack (`apple_dos.rs`,
//! `prodos.rs`) reads **DOS-order bytes**. A `.po` source flows through
//! `convert_po_to_do_bytes` at container-open time; a `.do` source is
//! identity / passthrough. A `.dsk` source is content-sniffed: we look
//! for a valid DOS 3.3 VTOC at the DOS-order track-17 sector-0 offset
//! `0x11000`; if that fails we re-interleave and try the same offset
//! in the converted bytes.
//!
//! Reference: Apple, *Beneath Apple DOS* and *Beneath Apple ProDOS*
//! (Worth & Lechner, 1981/1985), plus the a2kit MIT source which we use
//! as a port reference per `docs/mister_filesystem_implementation_
//! plan.md` §2.3.

use anyhow::{anyhow, Result};

/// Apple-II floppy geometry: 35 tracks × 16 sectors × 256 B.
pub const APPLE_II_TRACKS: usize = 35;
pub const APPLE_II_SECTORS_PER_TRACK: usize = 16;
pub const APPLE_II_SECTOR_BYTES: usize = 256;
pub const APPLE_II_TRACK_BYTES: usize = APPLE_II_SECTORS_PER_TRACK * APPLE_II_SECTOR_BYTES;
/// 140 KB — the canonical Apple-II floppy image size.
pub const APPLE_II_DISK_BYTES: usize = APPLE_II_TRACKS * APPLE_II_TRACK_BYTES;

/// Sector-skew table: index = DOS logical sector S; value = byte slot in
/// a `.po` file where that DOS sector lives.
///
/// Symmetric (the inverse mapping is itself), so the same table converts
/// in both directions.
pub const DOS_TO_PO: [usize; 16] = [0, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 15];

/// Which on-disk sector order an Apple-II `.dsk`-family file uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectorOrder {
    /// `.do` / `.dsk` written by DOS 3.3 — identity, no conversion needed.
    Dos,
    /// `.po` / `.dsk` written by ProDOS — needs interleave to flatten to
    /// DOS-order.
    ProDos,
}

/// Re-order a `.po` byte stream into a `.do` byte stream. Both are
/// 143,360 B for a standard 140 KB Apple-II floppy. Inputs of other
/// sizes are rejected with a clear error since the skew assumes the
/// 35×16 geometry.
pub fn convert_po_to_do_bytes(po_bytes: &[u8]) -> Result<Vec<u8>> {
    if po_bytes.len() != APPLE_II_DISK_BYTES {
        return Err(anyhow!(
            ".po source must be exactly {} bytes (got {})",
            APPLE_II_DISK_BYTES,
            po_bytes.len()
        ));
    }
    let mut out = vec![0u8; APPLE_II_DISK_BYTES];
    for t in 0..APPLE_II_TRACKS {
        let track_off = t * APPLE_II_TRACK_BYTES;
        for (s, &po_slot) in DOS_TO_PO.iter().enumerate() {
            let src_off = track_off + po_slot * APPLE_II_SECTOR_BYTES;
            let dst_off = track_off + s * APPLE_II_SECTOR_BYTES;
            out[dst_off..dst_off + APPLE_II_SECTOR_BYTES]
                .copy_from_slice(&po_bytes[src_off..src_off + APPLE_II_SECTOR_BYTES]);
        }
    }
    Ok(out)
}

/// Re-order DOS-order bytes back into ProDOS-order. Inverse of
/// [`convert_po_to_do_bytes`]. Used to produce `.po` from `.do` for
/// fixture round-trip tests.
pub fn convert_do_to_po_bytes(do_bytes: &[u8]) -> Result<Vec<u8>> {
    if do_bytes.len() != APPLE_II_DISK_BYTES {
        return Err(anyhow!(
            ".do source must be exactly {} bytes (got {})",
            APPLE_II_DISK_BYTES,
            do_bytes.len()
        ));
    }
    let mut out = vec![0u8; APPLE_II_DISK_BYTES];
    for t in 0..APPLE_II_TRACKS {
        let track_off = t * APPLE_II_TRACK_BYTES;
        for (s, &po_slot) in DOS_TO_PO.iter().enumerate() {
            let src_off = track_off + s * APPLE_II_SECTOR_BYTES;
            let dst_off = track_off + po_slot * APPLE_II_SECTOR_BYTES;
            out[dst_off..dst_off + APPLE_II_SECTOR_BYTES]
                .copy_from_slice(&do_bytes[src_off..src_off + APPLE_II_SECTOR_BYTES]);
        }
    }
    Ok(out)
}

/// True if a 140 KB byte buffer, interpreted as DOS-order, has a valid
/// DOS 3.3 VTOC at track 17 sector 0.
///
/// VTOC sanity fields (DOS 3.3 spec):
/// - byte 0x01: first catalog track (must be 17)
/// - byte 0x02: first catalog sector (must be 15)
/// - byte 0x03: DOS version (typically 3, can be 2 or 4)
/// - byte 0x27: max T/S pairs per T/S list sector (must be 122 = 0x7A)
/// - byte 0x34: tracks per disk (must be 35)
/// - byte 0x35: sectors per track (must be 16)
/// - bytes 0x36-0x37: bytes per sector LE (must be 256 = 0x00 0x01)
///
/// NOTE: this returns the same answer for `.do` and `.po` files of the
/// same DOS 3.3 disk because DOS sector 0 (where the VTOC lives) is a
/// fixed point of the interleave (`DOS_TO_PO[0] == 0`). To actually
/// distinguish the two orderings, walk the catalog chain too — see
/// [`detect_sector_order`].
pub fn looks_like_dos_order_vtoc(bytes: &[u8]) -> bool {
    if bytes.len() < APPLE_II_DISK_BYTES {
        return false;
    }
    // Track 17, sector 0, DOS-order: byte offset 17 * 16 * 256 = 0x11000.
    let vtoc = &bytes[0x11000..0x11000 + APPLE_II_SECTOR_BYTES];
    vtoc[0x01] == 17
        && vtoc[0x02] == 15
        && (1..=4).contains(&vtoc[0x03])
        && vtoc[0x27] == 122
        && vtoc[0x34] == APPLE_II_TRACKS as u8
        && vtoc[0x35] == APPLE_II_SECTORS_PER_TRACK as u8
        && vtoc[0x36] == 0x00
        && vtoc[0x37] == 0x01
}

/// Read a 256-byte sector from a DOS-order byte stream at (track, sector).
/// Returns `None` if out of bounds or the buffer is the wrong size.
fn read_dos_sector(bytes: &[u8], track: u8, sector: u8) -> Option<&[u8]> {
    if bytes.len() != APPLE_II_DISK_BYTES {
        return None;
    }
    if (track as usize) >= APPLE_II_TRACKS || (sector as usize) >= APPLE_II_SECTORS_PER_TRACK {
        return None;
    }
    let off = (track as usize) * APPLE_II_TRACK_BYTES + (sector as usize) * APPLE_II_SECTOR_BYTES;
    Some(&bytes[off..off + APPLE_II_SECTOR_BYTES])
}

/// Walk the catalog chain starting at the VTOC's `first catalog` pointer,
/// hopping next-sector pointers until end-of-chain (track 0) is reached
/// or the cap is hit. Returns `Some(hop_count)` if the chain reaches
/// end-of-chain cleanly within [`MAX_CATALOG_HOPS`], with every visited
/// sector on track 17 (DOS 3.3 puts the entire catalog on track 17) and
/// no cycles. Returns `None` on any failure.
///
/// In DOS-order, every catalog sector is read at the correct byte offset
/// and its next-pointer points to the next catalog — DOS 3.3 always
/// reserves the **full 15 catalog sectors** on track 17 regardless of how
/// many files the disk holds, so a real DO disk's chain hop-count is 15.
/// In ProDOS-order, the bytes at non-fixed-point sector slots (every slot
/// except 0 and 15) hold the wrong DOS sector's contents, so the chain
/// either derails (off-track-17 / cycle) or terminates short.
const MAX_CATALOG_HOPS: usize = 16;

/// Minimum hop count we accept for "this is DOS-order". DOS 3.3 reserves
/// the full 15 catalog sectors on track 17 unconditionally.
const MIN_CATALOG_HOPS_FOR_DOS: usize = 15;

fn catalog_chain_hops(bytes: &[u8]) -> Option<usize> {
    let vtoc = read_dos_sector(bytes, 17, 0)?;
    let mut track = vtoc[0x01];
    let mut sector = vtoc[0x02];
    let mut seen = std::collections::HashSet::new();
    for hop in 0..MAX_CATALOG_HOPS {
        if track == 0 {
            // End-of-chain.
            return Some(hop);
        }
        // DOS 3.3 keeps the entire catalog on track 17.
        if track != 17 {
            return None;
        }
        if !seen.insert((track, sector)) {
            // Cycle.
            return None;
        }
        let sec = read_dos_sector(bytes, track, sector)?;
        track = sec[0x01];
        sector = sec[0x02];
    }
    None
}

/// Detect whether a 140 KB Apple-II disk image is DOS- or ProDOS-order.
///
/// Strategy: first check the VTOC sanity at the DOS-order offset 0x11000.
/// That validates equally on both orderings (sector 0 is a fixed point
/// of the interleave), so we can't disambiguate yet. Then walk the
/// catalog chain in DOS-order arithmetic. In DOS-order the chain ends
/// cleanly; in ProDOS-order the chain derails at the first non-fixed-
/// point hop. If neither orientation walks cleanly, return `None` —
/// likely a ProDOS-formatted disk or a damaged DOS disk.
pub fn detect_sector_order(bytes: &[u8]) -> Option<SectorOrder> {
    if bytes.len() != APPLE_II_DISK_BYTES {
        return None;
    }
    let dos_hops = if looks_like_dos_order_vtoc(bytes) {
        catalog_chain_hops(bytes).unwrap_or(0)
    } else {
        0
    };
    let converted = convert_po_to_do_bytes(bytes).ok()?;
    let po_hops = if looks_like_dos_order_vtoc(&converted) {
        catalog_chain_hops(&converted).unwrap_or(0)
    } else {
        0
    };
    // Both orderings will show *some* chain because sectors 0 and 15 are
    // fixed points (the VTOC's "first catalog" pointer at T17S15 reads
    // identically). The disambiguator is depth: a real DOS 3.3 disk
    // walks the full 15 sectors; a PO-misinterpreted disk derails much
    // sooner because the rest of track 17's slots hold the wrong DOS
    // sector data.
    if dos_hops >= MIN_CATALOG_HOPS_FOR_DOS && dos_hops >= po_hops {
        return Some(SectorOrder::Dos);
    }
    if po_hops >= MIN_CATALOG_HOPS_FOR_DOS {
        return Some(SectorOrder::ProDos);
    }
    None
}

/// Pick a sector order from the file extension when content sniffing
/// can't disambiguate (e.g. a ProDOS-formatted disk where the DOS 3.3
/// VTOC sniff fails on both orderings). `.po` → `ProDos`, `.do` → `Dos`,
/// `.dsk` or unknown → falls back to `default_order`.
pub fn sector_order_from_extension(ext: Option<&str>, default_order: SectorOrder) -> SectorOrder {
    match ext.map(|e| e.to_ascii_lowercase()) {
        Some(ext) => match ext.as_str() {
            "po" => SectorOrder::ProDos,
            "do" => SectorOrder::Dos,
            _ => default_order,
        },
        None => default_order,
    }
}

/// Decode a `.do` / `.po` / `.dsk` file into a flat DOS-order byte stream.
/// Magic-first: content sniffing decides between DOS- and ProDOS-order.
/// If neither interpretation has a valid DOS 3.3 VTOC, the function falls
/// back to the extension hint, and if that's `.dsk` / unknown, passes the
/// bytes through as DOS-order (the historical default).
pub fn open_apple_ii_dsk(bytes: Vec<u8>, ext_hint: Option<&str>) -> Result<Vec<u8>> {
    if bytes.len() != APPLE_II_DISK_BYTES {
        return Err(anyhow!(
            "Apple-II disk image must be exactly {} bytes (got {}); \
             only standard 35×16×256 floppies are supported",
            APPLE_II_DISK_BYTES,
            bytes.len()
        ));
    }
    let order = detect_sector_order(&bytes)
        .unwrap_or_else(|| sector_order_from_extension(ext_hint, SectorOrder::Dos));
    match order {
        SectorOrder::Dos => Ok(bytes),
        SectorOrder::ProDos => convert_po_to_do_bytes(&bytes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a 140 KB byte buffer whose DOS-order track 17 carries a
    /// minimally valid DOS 3.3 directory: VTOC at sector 0 plus a
    /// catalog chain at sectors 15 -> 14 -> 13 -> ... -> 1 -> 0 (end),
    /// mirroring the layout DOS 3.3 produces on a freshly INIT'd disk.
    /// This is what's needed for `detect_sector_order` to disambiguate
    /// `.do` from `.po`.
    fn build_dos_order_disk_with_vtoc() -> Vec<u8> {
        let mut disk = vec![0u8; APPLE_II_DISK_BYTES];
        // VTOC at T17S0.
        let vtoc = &mut disk[0x11000..0x11000 + APPLE_II_SECTOR_BYTES];
        vtoc[0x01] = 17;
        vtoc[0x02] = 15;
        vtoc[0x03] = 3;
        vtoc[0x27] = 122;
        vtoc[0x34] = APPLE_II_TRACKS as u8;
        vtoc[0x35] = APPLE_II_SECTORS_PER_TRACK as u8;
        vtoc[0x36] = 0x00;
        vtoc[0x37] = 0x01;
        // Catalog chain: T17 S15 -> S14 -> ... -> S1 -> end. Standard
        // DOS 3.3 puts 15 catalog sectors on track 17 in descending
        // sector order. We populate just the next-track / next-sector
        // pointers; the per-entry slots stay zero (empty catalog).
        for next_idx in 1..=15 {
            // Sector that points at sector `next_idx - 1`.
            let cur_sector = next_idx;
            let prev_sector = next_idx - 1;
            let off = 17 * APPLE_II_TRACK_BYTES + cur_sector * APPLE_II_SECTOR_BYTES;
            let cat = &mut disk[off..off + APPLE_II_SECTOR_BYTES];
            cat[0x00] = 0; // unused
                           // Sector 1 points at (0, 0) = end-of-chain. Higher sectors
                           // chain to sector_idx - 1.
            if cur_sector == 1 {
                cat[0x01] = 0;
                cat[0x02] = 0;
            } else {
                cat[0x01] = 17;
                cat[0x02] = prev_sector as u8;
            }
        }
        // Stamp a recognisable byte at every sector start so the interleave
        // tests can prove the right sectors got swapped (outside track 17
        // so we don't clobber the catalog).
        for t in 0..APPLE_II_TRACKS {
            if t == 17 {
                continue;
            }
            for s in 0..APPLE_II_SECTORS_PER_TRACK {
                let off = t * APPLE_II_TRACK_BYTES + s * APPLE_II_SECTOR_BYTES;
                disk[off] = (t * 16 + s) as u8;
            }
        }
        disk
    }

    #[test]
    fn po_to_do_round_trip_returns_original() {
        let do_disk = build_dos_order_disk_with_vtoc();
        let po_disk = convert_do_to_po_bytes(&do_disk).unwrap();
        let back = convert_po_to_do_bytes(&po_disk).unwrap();
        assert_eq!(back, do_disk);
    }

    #[test]
    fn dos_to_po_table_is_involutive_on_inner_sectors() {
        // Sectors 1..14 should swap symmetrically: DOS_TO_PO[DOS_TO_PO[s]] == s.
        for s in 1..15 {
            assert_eq!(DOS_TO_PO[DOS_TO_PO[s]], s, "non-involutive at s={s}");
        }
        // Sectors 0 and 15 are fixed points.
        assert_eq!(DOS_TO_PO[0], 0);
        assert_eq!(DOS_TO_PO[15], 15);
    }

    #[test]
    fn looks_like_dos_order_vtoc_accepts_valid() {
        let disk = build_dos_order_disk_with_vtoc();
        assert!(looks_like_dos_order_vtoc(&disk));
    }

    #[test]
    fn looks_like_dos_order_vtoc_rejects_zeros() {
        let disk = vec![0u8; APPLE_II_DISK_BYTES];
        assert!(!looks_like_dos_order_vtoc(&disk));
    }

    #[test]
    fn detect_sector_order_finds_dos() {
        let disk = build_dos_order_disk_with_vtoc();
        assert_eq!(detect_sector_order(&disk), Some(SectorOrder::Dos));
    }

    #[test]
    fn detect_sector_order_finds_prodos() {
        let do_disk = build_dos_order_disk_with_vtoc();
        let po_disk = convert_do_to_po_bytes(&do_disk).unwrap();
        assert_eq!(detect_sector_order(&po_disk), Some(SectorOrder::ProDos));
    }

    #[test]
    fn detect_sector_order_returns_none_for_garbage() {
        let disk = vec![0xFFu8; APPLE_II_DISK_BYTES];
        assert_eq!(detect_sector_order(&disk), None);
    }

    #[test]
    fn open_apple_ii_dsk_passes_through_dos() {
        let disk = build_dos_order_disk_with_vtoc();
        let out = open_apple_ii_dsk(disk.clone(), None).unwrap();
        assert_eq!(out, disk);
    }

    #[test]
    fn open_apple_ii_dsk_converts_prodos() {
        let do_disk = build_dos_order_disk_with_vtoc();
        let po_disk = convert_do_to_po_bytes(&do_disk).unwrap();
        let out = open_apple_ii_dsk(po_disk, Some("po")).unwrap();
        // After conversion the VTOC must validate in the same DOS-order
        // offset as the original.
        assert!(looks_like_dos_order_vtoc(&out));
        assert_eq!(out, do_disk);
    }

    #[test]
    fn open_apple_ii_dsk_rejects_wrong_size() {
        let err = open_apple_ii_dsk(vec![0u8; 4096], None).unwrap_err();
        assert!(err
            .to_string()
            .contains("Apple-II disk image must be exactly"));
    }

    #[test]
    fn sector_order_from_extension_dispatches() {
        assert_eq!(
            sector_order_from_extension(Some("po"), SectorOrder::Dos),
            SectorOrder::ProDos
        );
        assert_eq!(
            sector_order_from_extension(Some("PO"), SectorOrder::Dos),
            SectorOrder::ProDos
        );
        assert_eq!(
            sector_order_from_extension(Some("do"), SectorOrder::ProDos),
            SectorOrder::Dos
        );
        assert_eq!(
            sector_order_from_extension(Some("dsk"), SectorOrder::Dos),
            SectorOrder::Dos
        );
        assert_eq!(
            sector_order_from_extension(None, SectorOrder::Dos),
            SectorOrder::Dos
        );
    }
}
