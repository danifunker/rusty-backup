//! Acorn `.hdf` Hard-Disk File — the emulator-side wrapper for an ADFS
//! / FileCore hard-disk image. Three variants exist in the wild:
//!
//! - **Bare HDF** — the file IS the raw ADFS volume, no wrapping. The
//!   MiSTer Archie core, RPCEmu (when configured), and most modern
//!   tools use this form. ADFS detection succeeds at byte 0xDC0 (Disc
//!   Record offset per the FileCore spec).
//! - **Arculator HDF** — early Arculator builds prepended a 512-byte
//!   header carrying geometry and version metadata. Byte 0xDC0 of the
//!   file holds junk; the real Disc Record lives at 0xFC0 (= 0x200 +
//!   0xDC0). Strip the header and the remaining bytes are a normal
//!   ADFS volume.
//! - **Acorn HostFS variants** — partition tables or non-FileCore
//!   wrappers; not handled here.
//!
//! This module implements the first two. Detection is structural:
//! whichever of byte 0xDC0 / 0xFC0 looks like a plausible Disc Record
//! wins; if neither does, pass the bytes through verbatim and let the
//! downstream layers complain.

use anyhow::Result;

/// Size of an Arculator-style HDF wrapper header.
pub const ARCULATOR_HEADER_SIZE: usize = 0x200;

/// Returns `true` if the 32-byte buffer at the given offset looks like
/// a FileCore Disc Record. We only check the four fields with tight
/// natural bounds — log2 sector size, secs-per-track, heads, and
/// nzones — which catches every real Disc Record and rejects all-zero
/// or junk regions with overwhelming probability.
fn looks_like_disc_record(buf: &[u8]) -> bool {
    if buf.len() < 32 {
        return false;
    }
    let log2_sec = buf[0];
    let secs_per_track = buf[1];
    let heads = buf[2];
    let nzones = buf[9];
    (8..=11).contains(&log2_sec) && secs_per_track >= 1 && (1..=4).contains(&heads) && nzones >= 1
}

/// Where in the input bytes does the ADFS Disc Record live? Returns
/// `Some(0)` for a bare HDF (Disc Record at 0xDC0), `Some(0x200)` for
/// an Arculator-wrapped HDF (Disc Record at 0xFC0), or `None` if
/// neither offset looks like a Disc Record.
pub fn detect_hdf_offset(bytes: &[u8]) -> Option<usize> {
    if bytes.len() >= 0xDC0 + 32 && looks_like_disc_record(&bytes[0xDC0..0xDC0 + 32]) {
        return Some(0);
    }
    if bytes.len() >= ARCULATOR_HEADER_SIZE + 0xDC0 + 32
        && looks_like_disc_record(
            &bytes[ARCULATOR_HEADER_SIZE + 0xDC0..ARCULATOR_HEADER_SIZE + 0xDC0 + 32],
        )
    {
        return Some(ARCULATOR_HEADER_SIZE);
    }
    None
}

/// Decode an HDF byte stream into a flat ADFS volume. If the image is
/// Arculator-wrapped (512-byte header before the Disc Record), strip
/// the header. Otherwise pass through.
///
/// Returning `Vec<u8>` rather than a slice keeps the API symmetric
/// with the other container decoders (`decode_msa_bytes`, etc.) so
/// callers can drop the result into a `Cursor`. A future refinement
/// could return a `Box<dyn ReadSeek>` over the original bytes to
/// avoid the second allocation, but HDF images are at most a few
/// hundred MB and the existing pipeline already copies through
/// `source_reader::open_read`.
pub fn decode_hdf_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    match detect_hdf_offset(bytes) {
        Some(0) => Ok(bytes.to_vec()),
        Some(off) => Ok(bytes[off..].to_vec()),
        None => {
            // Neither offset looked like a Disc Record. Could still be
            // a valid HDF (e.g. unusual sector size we didn't whitelist)
            // or an Amiga HDF — pass through and let the partition /
            // FS layer make the decision.
            Ok(bytes.to_vec())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal bare ADFS volume (E-format shape) just big
    /// enough that `looks_like_disc_record` finds the Disc Record at
    /// byte 0xDC0. Same byte layout the existing ADFS unit tests use.
    fn build_bare_adfs() -> Vec<u8> {
        let mut disk = vec![0u8; 800 * 1024];
        let dr_off = 0xC00 + 0x1C0;
        disk[dr_off] = 10; // log2_sec_size = 10 → 1024-B sectors
        disk[dr_off + 1] = 5; // secs/track
        disk[dr_off + 2] = 2; // heads
        disk[dr_off + 9] = 2; // nzones
        disk
    }

    fn build_arculator_wrapped_adfs() -> Vec<u8> {
        let bare = build_bare_adfs();
        let mut wrapped = vec![0u8; ARCULATOR_HEADER_SIZE];
        // Stamp some non-Disc-Record-looking bytes into the header so
        // the bare-offset check fails first.
        wrapped[..16].copy_from_slice(b"ARCHEADER 0x0001");
        wrapped.extend_from_slice(&bare);
        wrapped
    }

    #[test]
    fn detect_hdf_offset_picks_zero_for_bare_adfs() {
        let disk = build_bare_adfs();
        assert_eq!(detect_hdf_offset(&disk), Some(0));
    }

    #[test]
    fn detect_hdf_offset_picks_arculator_header_for_wrapped_adfs() {
        let disk = build_arculator_wrapped_adfs();
        assert_eq!(detect_hdf_offset(&disk), Some(ARCULATOR_HEADER_SIZE));
    }

    #[test]
    fn detect_hdf_offset_returns_none_for_random_bytes() {
        let disk = vec![0xCDu8; 0x10000];
        assert_eq!(detect_hdf_offset(&disk), None);
    }

    #[test]
    fn decode_hdf_strips_arculator_header() {
        let bare = build_bare_adfs();
        let wrapped = build_arculator_wrapped_adfs();
        let decoded = decode_hdf_bytes(&wrapped).unwrap();
        assert_eq!(decoded, bare);
    }

    #[test]
    fn decode_hdf_passes_bare_through_verbatim() {
        let bare = build_bare_adfs();
        let decoded = decode_hdf_bytes(&bare).unwrap();
        assert_eq!(decoded, bare);
    }
}
