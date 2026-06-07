//! DIM — DiskExplorer "DIFC HEADER" floppy image (X68000 / PC-98).
//!
//! ## On-disk layout (256-byte header + raw payload)
//!
//! | Offset | Size | Field          | Notes                                  |
//! |-------:|-----:|----------------|----------------------------------------|
//! | `0x00` |   1  | media byte     | encodes the geometry (see [`DimMedia`]) |
//! | `0x01` | 170  | track-exist    | one byte per logical (cyl, head) track |
//! | `0xAB` |  12  | DIFC signature | ASCII `DIFC HEADER\0`                  |
//! | `0xB7` |  73  | comment / pad  | ignored on read, zero on write         |
//! | `0x100`|  …   | payload        | concatenated tracks, in (cyl, head)
//! |        |      |                | order, sectors 1..=spt per track       |
//!
//! Track-existence bytes: **zero = track absent, non-zero = present**. Absent
//! tracks are zero-filled in the decoded flat output.
//!
//! ## IBM-style and "generic" DIM
//!
//! A second `.dim` family (OS/2 XDF DIM and friends) uses a 256-byte header
//! without the DIFC signature. We don't have a public spec for those, but
//! every variant we've seen pairs the 256-byte header with a raw flat
//! payload that matches one of the supported floppy geometries. The
//! [`decode_dim_bytes`] read path falls back to this layout when the DIFC
//! signature is absent — header bytes are read past and the payload is
//! treated as flat sectors. We only **write** the DIFC variant.

use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};

use super::floppy_geom::{
    infer_media_from_size, require_media_from_size, FloppyGeometry, FloppyMedia,
};

const HEADER_SIZE: usize = 0x100;
const TRACK_BITMAP_OFFSET: usize = 0x01;
const TRACK_BITMAP_LEN: usize = 170;
const DIFC_SIG_OFFSET: usize = 0xAB;
const DIFC_SIG: &[u8] = b"DIFC HEADER\0";

/// Header media-type byte. The DIFC convention does not include a 640 KB
/// code, so the X68000 + PC-98 set is the four-element subset we currently
/// recognise here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DimMedia {
    /// `0x00` — 2HD 1.2 MB (Sharp X68000 standard).
    Hd1232,
    /// `0x01` — 2HD 1.44 MB.
    Hd1440,
    /// `0x09` — 2DD 720 KB.
    Dd720,
}

impl DimMedia {
    pub const fn byte(self) -> u8 {
        match self {
            DimMedia::Hd1232 => 0x00,
            DimMedia::Hd1440 => 0x01,
            DimMedia::Dd720 => 0x09,
        }
    }

    pub fn from_byte(b: u8) -> Result<Self> {
        match b {
            0x00 => Ok(DimMedia::Hd1232),
            0x01 => Ok(DimMedia::Hd1440),
            0x09 => Ok(DimMedia::Dd720),
            other => bail!(
                "DIM media byte {other:#04x} is not one of the recognised DIFC \
                 codes (0x00=1.2MB 2HD, 0x01=1.44MB 2HD, 0x09=720KB 2DD)"
            ),
        }
    }

    pub const fn media(self) -> FloppyMedia {
        match self {
            DimMedia::Hd1232 => FloppyMedia::Hd1232,
            DimMedia::Hd1440 => FloppyMedia::Hd1440,
            DimMedia::Dd720 => FloppyMedia::Dd720,
        }
    }

    pub fn from_media(m: FloppyMedia) -> Result<Self> {
        match m {
            FloppyMedia::Hd1232 => Ok(DimMedia::Hd1232),
            FloppyMedia::Hd1440 => Ok(DimMedia::Hd1440),
            FloppyMedia::Dd720 => Ok(DimMedia::Dd720),
            FloppyMedia::Dd640 => Err(anyhow!(
                "DIFC DIM does not define a media byte for 640 KB 2DD — use \
                 XDF / HDM / D88 instead for this geometry"
            )),
        }
    }
}

/// True if the buffer starts with a DIFC DIM header (signature at byte 0xAB).
pub fn looks_like_dim_header(head: &[u8]) -> bool {
    head.len() >= DIFC_SIG_OFFSET + DIFC_SIG.len()
        && &head[DIFC_SIG_OFFSET..DIFC_SIG_OFFSET + DIFC_SIG.len()] == DIFC_SIG
}

/// True if the buffer is a plausible "generic" DIM: 256-byte header + a
/// payload whose length matches one of the four supported floppy geometries.
/// Used as a fallback for non-DIFC `.dim` files (e.g. IBM XDF DIM).
pub fn looks_like_generic_dim_size(total_size: usize) -> bool {
    total_size > HEADER_SIZE && infer_media_from_size(total_size - HEADER_SIZE).is_some()
}

/// Cheap detection used by [`super::detect_container_kind`]: extension is
/// `.dim` (case-insensitive) AND either the DIFC signature is present OR
/// the size matches the generic header+flat layout.
pub fn looks_like_dim(head: &[u8], path: Option<&Path>, total_size: usize) -> bool {
    if looks_like_dim_header(head) {
        return true;
    }
    let ext_matches = path
        .and_then(|p| p.extension())
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("dim"))
        .unwrap_or(false);
    ext_matches && looks_like_generic_dim_size(total_size)
}

/// Decode a DIM (DIFC or generic) into a flat sector stream + inferred media.
pub fn decode_dim_bytes(bytes: &[u8]) -> Result<(Vec<u8>, FloppyMedia)> {
    if bytes.len() < HEADER_SIZE {
        bail!(
            "DIM truncated: need at least {} header bytes, got {}",
            HEADER_SIZE,
            bytes.len()
        );
    }
    let payload = &bytes[HEADER_SIZE..];
    if looks_like_dim_header(bytes) {
        decode_difc_dim(bytes, payload)
    } else {
        decode_generic_dim(payload)
    }
}

fn decode_difc_dim(header: &[u8], payload: &[u8]) -> Result<(Vec<u8>, FloppyMedia)> {
    let dim_media = DimMedia::from_byte(header[0])?;
    let media = dim_media.media();
    let geom = media.geometry();
    let track_bytes = geom.spt as usize * geom.sec_size as usize;
    let total_tracks = geom.cyls as usize * geom.heads as usize;

    let bitmap = &header[TRACK_BITMAP_OFFSET..TRACK_BITMAP_OFFSET + TRACK_BITMAP_LEN];
    let present_tracks: Vec<bool> = (0..total_tracks).map(|t| bitmap[t] != 0).collect();
    let present_count = present_tracks.iter().filter(|p| **p).count();
    let expected_payload = present_count * track_bytes;
    if payload.len() < expected_payload {
        bail!(
            "DIM (DIFC) payload truncated: header advertises {} present tracks \
             ({} bytes) but only {} bytes follow the header",
            present_count,
            expected_payload,
            payload.len()
        );
    }
    if payload.len() > expected_payload && payload.len() != geom.flat_size() {
        // A few writers emit all tracks regardless of the bitmap. Accept that
        // shape too if the payload exactly matches the full-disk size.
    }

    let mut flat = vec![0u8; geom.flat_size()];
    let mut src = 0usize;
    let all_present_full_dump = payload.len() == geom.flat_size();
    for (t, &present) in present_tracks.iter().enumerate() {
        let dst = t * track_bytes;
        if all_present_full_dump || present {
            flat[dst..dst + track_bytes].copy_from_slice(&payload[src..src + track_bytes]);
            src += track_bytes;
        }
        // Absent tracks stay zero-filled.
    }
    Ok((flat, media))
}

fn decode_generic_dim(payload: &[u8]) -> Result<(Vec<u8>, FloppyMedia)> {
    let media = require_media_from_size(payload.len()).with_context(|| {
        "DIM (non-DIFC) decode failed: payload after the 256-byte header does \
         not match a supported floppy geometry"
            .to_string()
    })?;
    Ok((payload.to_vec(), media))
}

/// Encode a flat sector stream as a DIFC DIM. Always emits all tracks present
/// (bitmap byte = 1) and zero-fills the comment region. 640 KB 2DD is not
/// representable in DIFC and returns an error — use XDF / HDM / D88 instead.
pub fn encode_dim_bytes(flat: &[u8], geom: FloppyGeometry) -> Result<Vec<u8>> {
    let media = require_media_from_size(flat.len()).with_context(|| {
        format!(
            "DIM encode: flat size {} does not match a supported floppy geometry",
            flat.len()
        )
    })?;
    if media.geometry() != geom {
        bail!(
            "DIM encode: provided geometry ({}cyl x {}heads x {}sec x {}B) \
             does not match the inferred media {} (flat size {} bytes)",
            geom.cyls,
            geom.heads,
            geom.spt,
            geom.sec_size,
            media.display_label(),
            flat.len()
        );
    }
    let dim_media = DimMedia::from_media(media)?;
    let total_tracks = geom.cyls as usize * geom.heads as usize;
    if total_tracks > TRACK_BITMAP_LEN {
        bail!(
            "DIM encode: geometry produces {total_tracks} tracks, exceeding the \
             {TRACK_BITMAP_LEN}-entry DIFC track bitmap"
        );
    }

    let mut out = vec![0u8; HEADER_SIZE + flat.len()];
    out[0] = dim_media.byte();
    for t in 0..total_tracks {
        out[TRACK_BITMAP_OFFSET + t] = 0x01;
    }
    out[DIFC_SIG_OFFSET..DIFC_SIG_OFFSET + DIFC_SIG.len()].copy_from_slice(DIFC_SIG);
    out[HEADER_SIZE..].copy_from_slice(flat);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn pattern(len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| (i.wrapping_mul(31) & 0xFF) as u8)
            .collect()
    }

    #[test]
    fn detect_difc_signature() {
        let mut head = vec![0u8; HEADER_SIZE];
        head[0] = 0x00;
        head[DIFC_SIG_OFFSET..DIFC_SIG_OFFSET + DIFC_SIG.len()].copy_from_slice(DIFC_SIG);
        assert!(looks_like_dim_header(&head));

        // Wrong signature -> no.
        head[DIFC_SIG_OFFSET] = b'X';
        assert!(!looks_like_dim_header(&head));

        // Truncated -> no.
        assert!(!looks_like_dim_header(&head[..10]));
    }

    #[test]
    fn dim_media_byte_round_trips() {
        for m in [DimMedia::Hd1232, DimMedia::Hd1440, DimMedia::Dd720] {
            assert_eq!(DimMedia::from_byte(m.byte()).unwrap(), m);
        }
        assert!(DimMedia::from_byte(0x42).is_err());
    }

    #[test]
    fn dim_media_640kb_rejected() {
        let err = DimMedia::from_media(FloppyMedia::Dd640)
            .unwrap_err()
            .to_string();
        assert!(err.contains("640 KB"));
    }

    #[test]
    fn encode_emits_difc_signature_and_round_trips() {
        let geom = FloppyMedia::Hd1232.geometry();
        let flat = pattern(geom.flat_size());
        let bytes = encode_dim_bytes(&flat, geom).unwrap();
        assert!(looks_like_dim_header(&bytes));
        assert_eq!(bytes[0], DimMedia::Hd1232.byte());
        // All track bitmap entries set.
        let total_tracks = (geom.cyls * geom.heads) as usize;
        for t in 0..total_tracks {
            assert_eq!(bytes[TRACK_BITMAP_OFFSET + t], 0x01);
        }
        // Round-trip.
        let (decoded, media) = decode_dim_bytes(&bytes).unwrap();
        assert_eq!(media, FloppyMedia::Hd1232);
        assert_eq!(decoded, flat);
    }

    #[test]
    fn encode_round_trips_all_supported_difc_media() {
        for m in [FloppyMedia::Hd1232, FloppyMedia::Hd1440, FloppyMedia::Dd720] {
            let geom = m.geometry();
            let flat = pattern(geom.flat_size());
            let bytes = encode_dim_bytes(&flat, geom).unwrap();
            let (decoded, media) = decode_dim_bytes(&bytes).unwrap();
            assert_eq!(media, m);
            assert_eq!(decoded, flat);
        }
    }

    #[test]
    fn decode_zero_fills_absent_tracks() {
        let geom = FloppyMedia::Hd1232.geometry();
        let track_bytes = (geom.spt as usize) * (geom.sec_size as usize);
        let total_tracks = (geom.cyls as usize) * (geom.heads as usize);
        // Header advertising only the first 10 tracks present.
        let mut bytes = vec![0u8; HEADER_SIZE + 10 * track_bytes];
        bytes[0] = DimMedia::Hd1232.byte();
        bytes[DIFC_SIG_OFFSET..DIFC_SIG_OFFSET + DIFC_SIG.len()].copy_from_slice(DIFC_SIG);
        for t in 0..10 {
            bytes[TRACK_BITMAP_OFFSET + t] = 0x01;
        }
        // First-10-tracks payload filled with 0xAB.
        for byte in bytes.iter_mut().skip(HEADER_SIZE) {
            *byte = 0xAB;
        }
        let (decoded, _) = decode_dim_bytes(&bytes).unwrap();
        assert_eq!(decoded.len(), geom.flat_size());
        // Tracks 0..10 are 0xAB, tracks 10..end are zero.
        assert!(decoded[0..10 * track_bytes].iter().all(|&b| b == 0xAB));
        assert!(decoded[10 * track_bytes..].iter().all(|&b| b == 0));
        let _ = total_tracks;
    }

    #[test]
    fn decode_generic_dim_handles_header_plus_flat() {
        let geom = FloppyMedia::Hd1232.geometry();
        let flat = pattern(geom.flat_size());
        // Non-DIFC header — set media byte to garbage and clear the signature.
        let mut bytes = vec![0u8; HEADER_SIZE + flat.len()];
        bytes[0] = 0xDE;
        bytes[1] = 0xAD;
        // No DIFC signature at 0xAB (left zeroed).
        bytes[HEADER_SIZE..].copy_from_slice(&flat);
        let (decoded, media) = decode_dim_bytes(&bytes).unwrap();
        assert_eq!(media, FloppyMedia::Hd1232);
        assert_eq!(decoded, flat);
    }

    #[test]
    fn decode_rejects_truncated_header() {
        let err = decode_dim_bytes(&[0u8; 100]).unwrap_err().to_string();
        assert!(err.contains("truncated"));
    }

    #[test]
    fn decode_rejects_generic_with_unknown_payload_size() {
        // 256 + 99 bytes — non-DIFC, and 99 bytes doesn't match any geometry.
        let bytes = vec![0u8; HEADER_SIZE + 99];
        let err = decode_dim_bytes(&bytes).unwrap_err().to_string();
        assert!(
            err.contains("does not match a supported floppy geometry"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn looks_like_dim_detects_signature_and_extension() {
        // DIFC signature path: classified regardless of path.
        let mut head = vec![0u8; HEADER_SIZE];
        head[DIFC_SIG_OFFSET..DIFC_SIG_OFFSET + DIFC_SIG.len()].copy_from_slice(DIFC_SIG);
        assert!(looks_like_dim(&head, None, 100_000));

        // Generic path: extension required.
        let head2 = vec![0u8; HEADER_SIZE];
        let geom = FloppyMedia::Hd1232.geometry();
        let total = HEADER_SIZE + geom.flat_size();
        assert!(!looks_like_dim(&head2, None, total));
        assert!(looks_like_dim(&head2, Some(Path::new("disk.dim")), total));
        assert!(looks_like_dim(&head2, Some(Path::new("DISK.DIM")), total));
        // Wrong size -> no.
        assert!(!looks_like_dim(&head2, Some(Path::new("disk.dim")), 99));
    }
}
