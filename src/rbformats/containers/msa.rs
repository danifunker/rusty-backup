//! MSA (Magic Shadow Archiver) — Atari ST / TT / Falcon floppy container.
//!
//! MSA wraps a raw `.st` sector stream with a small header plus an optional
//! per-track RLE compression layer. The format is small, fully specified, and
//! used by every Atari ST emulator (Hatari, Steem, SainT, MiSTer AtariST).
//!
//! ## Header (10 bytes, big-endian)
//!
//! | Offset | Size | Field            | Notes                                  |
//! |-------:|-----:|------------------|----------------------------------------|
//! |  0     |  2   | id               | always `$0E 0F`                        |
//! |  2     |  2   | sectors_per_track| typically 9, 10, or 11                 |
//! |  4     |  2   | sides            | 0 = single-sided, 1 = double-sided     |
//! |  6     |  2   | start_track      | first track in image (often 0)         |
//! |  8     |  2   | end_track        | last track (inclusive)                 |
//!
//! ## Track records
//!
//! Tracks appear in order `(track, side)` with `side` cycling through
//! `0..=sides`. For each track:
//!
//! - `length: u16` big-endian — number of bytes that follow in the payload.
//! - `payload: [u8; length]` — the track bytes.
//!
//! If `length == sectors_per_track * 512` the track is **uncompressed** (raw
//! sectors). Otherwise the payload is **RLE-compressed** using the
//! sentinel byte `$E5`:
//!
//! - `$E5 byte runlen_be_u16` — repeat `byte` `runlen` times.
//! - any non-`$E5` byte — literal.
//! - a literal `$E5` is encoded as `$E5 $E5 $00 $01` (run of one).
//!
//! ## Output
//!
//! The decoded image is a flat raw sector stream of size
//! `(end_track - start_track + 1) * (sides + 1) * sectors_per_track * 512`.
//! Atari ST floppies are 720 KB (9 spt × 80 trk × 2 sides) or 800 KB
//! (10 spt × 80 trk × 2 sides); this decoder caps at 4 MiB to refuse
//! pathological inputs.
//!
//! Reference: §8 of `docs/mister_filesystem_implementation_plan.md`
//! (verified spec, DrCoolZic confirms format details).

use anyhow::{bail, Result};
use byteorder::{BigEndian, ByteOrder};

/// MSA magic — first 2 bytes of every valid `.msa` file.
pub const MSA_MAGIC: [u8; 2] = [0x0E, 0x0F];

/// MSA header is exactly 10 bytes.
pub const MSA_HEADER_LEN: usize = 10;

/// RLE marker / sentinel byte inside a compressed track.
const MSA_RLE_MARKER: u8 = 0xE5;

/// Standard Atari ST sector size. MSA does not encode this — it is fixed.
const MSA_SECTOR_SIZE: usize = 512;

/// Hard ceiling for a decoded MSA image. The largest real Atari ST floppy is
/// ~1.5 MiB (HD 1.44, rare); 4 MiB rejects any pathological / corrupted
/// header without affecting genuine media.
const MSA_MAX_DECODED_SIZE: usize = 4 * 1024 * 1024;

/// Maximum allowed values per field, derived from real-world floppy
/// hardware. Stricter bounds catch corrupted headers before allocation.
const MSA_MAX_SPT: u16 = 36; // ED floppy upper bound
const MSA_MAX_SIDES: u16 = 1; // sides field is 0 or 1
const MSA_MAX_TRACKS: u16 = 86; // 80-track standard plus a few extra

/// Parsed MSA header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MsaHeader {
    pub sectors_per_track: u16,
    pub sides: u16,
    pub start_track: u16,
    pub end_track: u16,
}

impl MsaHeader {
    /// Number of tracks in the image (inclusive of both endpoints).
    pub fn track_count(self) -> u16 {
        self.end_track - self.start_track + 1
    }

    /// Decoded image size in bytes.
    pub fn decoded_size(self) -> usize {
        self.track_count() as usize
            * (self.sides as usize + 1)
            * self.sectors_per_track as usize
            * MSA_SECTOR_SIZE
    }
}

/// Lightweight magic check used by [`crate::rbformats::containers::detect_container_kind`].
/// Only consults the first 2 bytes — full validation happens during decode.
pub fn looks_like_msa_header(buf: &[u8]) -> bool {
    buf.len() >= 2 && buf[0..2] == MSA_MAGIC
}

/// Parse and validate the MSA header from the first 10 bytes of the file.
pub fn parse_header(buf: &[u8]) -> Result<MsaHeader> {
    if buf.len() < MSA_HEADER_LEN {
        bail!(
            "MSA header truncated: {} bytes (need {MSA_HEADER_LEN})",
            buf.len()
        );
    }
    if buf[0..2] != MSA_MAGIC {
        bail!(
            "not an MSA file: header magic is {:02X} {:02X} (expected 0E 0F)",
            buf[0],
            buf[1]
        );
    }
    let header = MsaHeader {
        sectors_per_track: BigEndian::read_u16(&buf[2..4]),
        sides: BigEndian::read_u16(&buf[4..6]),
        start_track: BigEndian::read_u16(&buf[6..8]),
        end_track: BigEndian::read_u16(&buf[8..10]),
    };
    if header.sectors_per_track == 0 || header.sectors_per_track > MSA_MAX_SPT {
        bail!(
            "MSA sectors_per_track out of range: {}",
            header.sectors_per_track
        );
    }
    if header.sides > MSA_MAX_SIDES {
        bail!("MSA sides out of range: {} (expected 0 or 1)", header.sides);
    }
    if header.start_track > header.end_track || header.end_track >= MSA_MAX_TRACKS {
        bail!(
            "MSA track range invalid: start={} end={}",
            header.start_track,
            header.end_track
        );
    }
    if header.decoded_size() > MSA_MAX_DECODED_SIZE {
        bail!(
            "MSA decoded size {} exceeds {} byte cap",
            header.decoded_size(),
            MSA_MAX_DECODED_SIZE
        );
    }
    Ok(header)
}

/// Decode a full MSA byte buffer to a flat raw sector stream.
///
/// Caller should already have sniffed the magic via [`looks_like_msa_header`]
/// (the dispatcher does this). This function re-validates the header and
/// returns a clear error on any malformed track payload.
pub fn decode_msa_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let header = parse_header(bytes)?;
    let track_size = header.sectors_per_track as usize * MSA_SECTOR_SIZE;
    let num_tracks = header.track_count() as usize * (header.sides as usize + 1);

    let mut out = Vec::with_capacity(header.decoded_size());
    let mut cursor = MSA_HEADER_LEN;

    for track_idx in 0..num_tracks {
        if cursor + 2 > bytes.len() {
            bail!(
                "MSA truncated: track {}/{} length field past EOF",
                track_idx + 1,
                num_tracks
            );
        }
        let payload_len = BigEndian::read_u16(&bytes[cursor..cursor + 2]) as usize;
        cursor += 2;
        if cursor + payload_len > bytes.len() {
            bail!(
                "MSA truncated: track {}/{} payload of {} bytes past EOF",
                track_idx + 1,
                num_tracks,
                payload_len
            );
        }
        let payload = &bytes[cursor..cursor + payload_len];
        cursor += payload_len;

        if payload_len == track_size {
            // Uncompressed track — copy verbatim.
            out.extend_from_slice(payload);
        } else {
            decode_rle_track_into(payload, track_size, &mut out).map_err(|e| {
                anyhow::anyhow!("MSA track {}/{}: {}", track_idx + 1, num_tracks, e)
            })?;
        }
    }

    if out.len() != header.decoded_size() {
        bail!(
            "MSA decoded length {} != expected {} (header inconsistency)",
            out.len(),
            header.decoded_size()
        );
    }
    Ok(out)
}

/// Decode one RLE-compressed track into `out`. Must append exactly
/// `track_size` bytes or return an error.
fn decode_rle_track_into(
    payload: &[u8],
    track_size: usize,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let mut produced: usize = 0;
    let mut i: usize = 0;
    while i < payload.len() {
        let b = payload[i];
        if b != MSA_RLE_MARKER {
            out.push(b);
            produced += 1;
            i += 1;
            continue;
        }
        // RLE escape: $E5 <byte> <runlen u16 BE>
        if i + 4 > payload.len() {
            return Err(format!(
                "RLE escape at byte {i} truncated (need 4 more bytes, have {})",
                payload.len() - i
            ));
        }
        let value = payload[i + 1];
        let runlen = BigEndian::read_u16(&payload[i + 2..i + 4]) as usize;
        if produced + runlen > track_size {
            return Err(format!(
                "RLE run at byte {i} overruns track (produced {produced} + runlen {runlen} > track_size {track_size})",
            ));
        }
        out.resize(out.len() + runlen, value);
        produced += runlen;
        i += 4;
    }
    if produced != track_size {
        return Err(format!(
            "track decode produced {produced} bytes, expected {track_size}"
        ));
    }
    Ok(())
}

/// Encode a flat raw sector stream into an MSA buffer. This is the inverse of
/// [`decode_msa_bytes`] and is used by tests; a write path through the
/// container layer is out of scope for now.
#[cfg(test)]
pub fn encode_msa_bytes(header: MsaHeader, raw: &[u8]) -> Vec<u8> {
    assert_eq!(raw.len(), header.decoded_size(), "raw size mismatch");
    let track_size = header.sectors_per_track as usize * MSA_SECTOR_SIZE;
    let num_tracks = header.track_count() as usize * (header.sides as usize + 1);

    let mut out = Vec::with_capacity(raw.len() + 64);
    // Header.
    out.extend_from_slice(&MSA_MAGIC);
    let mut hdr = [0u8; 8];
    BigEndian::write_u16(&mut hdr[0..2], header.sectors_per_track);
    BigEndian::write_u16(&mut hdr[2..4], header.sides);
    BigEndian::write_u16(&mut hdr[4..6], header.start_track);
    BigEndian::write_u16(&mut hdr[6..8], header.end_track);
    out.extend_from_slice(&hdr);

    for t in 0..num_tracks {
        let start = t * track_size;
        let track = &raw[start..start + track_size];
        // Emit uncompressed: encoders that always pick the raw path are still
        // spec-compliant, since the decoder accepts both forms. Tests below
        // also exercise the RLE path via hand-crafted payloads.
        let mut len_buf = [0u8; 2];
        BigEndian::write_u16(&mut len_buf, track_size as u16);
        out.extend_from_slice(&len_buf);
        out.extend_from_slice(track);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_st_floppy_pattern(header: MsaHeader) -> Vec<u8> {
        // Each sector filled with a unique byte derived from its global
        // sector index, so any bit-flip is detectable on round-trip.
        let total = header.decoded_size();
        let mut raw = vec![0u8; total];
        for (i, byte) in raw.iter_mut().enumerate() {
            let sector = i / MSA_SECTOR_SIZE;
            *byte = ((sector * 7 + 13) & 0xFF) as u8;
        }
        raw
    }

    #[test]
    fn detects_magic() {
        assert!(looks_like_msa_header(&[0x0E, 0x0F, 0x00]));
        assert!(!looks_like_msa_header(&[0x0E]));
        assert!(!looks_like_msa_header(&[0x00, 0x0F]));
        assert!(!looks_like_msa_header(&[0xCA, 0xFE]));
    }

    #[test]
    fn rejects_bad_header_fields() {
        // sectors_per_track = 0
        let bad = [0x0E, 0x0F, 0, 0, 0, 0, 0, 0, 0, 79];
        assert!(parse_header(&bad).is_err());
        // sides = 2
        let bad = [0x0E, 0x0F, 0, 9, 0, 2, 0, 0, 0, 79];
        assert!(parse_header(&bad).is_err());
        // start > end
        let bad = [0x0E, 0x0F, 0, 9, 0, 1, 0, 80, 0, 5];
        assert!(parse_header(&bad).is_err());
    }

    #[test]
    fn round_trip_uncompressed_720k_floppy() {
        let header = MsaHeader {
            sectors_per_track: 9,
            sides: 1,
            start_track: 0,
            end_track: 79,
        };
        let raw = build_st_floppy_pattern(header);
        let msa = encode_msa_bytes(header, &raw);
        let decoded = decode_msa_bytes(&msa).unwrap();
        assert_eq!(decoded.len(), 720 * 1024);
        assert_eq!(decoded, raw);
    }

    #[test]
    fn round_trip_uncompressed_800k_floppy() {
        let header = MsaHeader {
            sectors_per_track: 10,
            sides: 1,
            start_track: 0,
            end_track: 79,
        };
        let raw = build_st_floppy_pattern(header);
        let msa = encode_msa_bytes(header, &raw);
        let decoded = decode_msa_bytes(&msa).unwrap();
        assert_eq!(decoded.len(), 800 * 1024);
        assert_eq!(decoded, raw);
    }

    #[test]
    fn decodes_rle_run() {
        // Build a 9-sector (4608 B) track that's mostly 0x00 with a single
        // 0xAB at the end. RLE encoding: $E5 $00 <4607 BE u16> 0xAB.
        let header = MsaHeader {
            sectors_per_track: 9,
            sides: 0,
            start_track: 0,
            end_track: 0,
        };
        let track_size = 9 * MSA_SECTOR_SIZE;
        let mut payload = Vec::new();
        payload.push(MSA_RLE_MARKER);
        payload.push(0x00);
        let mut runlen_buf = [0u8; 2];
        BigEndian::write_u16(&mut runlen_buf, (track_size - 1) as u16);
        payload.extend_from_slice(&runlen_buf);
        payload.push(0xAB);

        // Build the MSA file.
        let mut msa = Vec::new();
        msa.extend_from_slice(&MSA_MAGIC);
        let mut hdr = [0u8; 8];
        BigEndian::write_u16(&mut hdr[0..2], header.sectors_per_track);
        BigEndian::write_u16(&mut hdr[2..4], header.sides);
        BigEndian::write_u16(&mut hdr[4..6], header.start_track);
        BigEndian::write_u16(&mut hdr[6..8], header.end_track);
        msa.extend_from_slice(&hdr);
        let mut plen = [0u8; 2];
        BigEndian::write_u16(&mut plen, payload.len() as u16);
        msa.extend_from_slice(&plen);
        msa.extend_from_slice(&payload);

        let decoded = decode_msa_bytes(&msa).unwrap();
        assert_eq!(decoded.len(), track_size);
        assert!(decoded[..track_size - 1].iter().all(|b| *b == 0));
        assert_eq!(decoded[track_size - 1], 0xAB);
    }

    #[test]
    fn decodes_literal_e5_via_run_of_one() {
        // Spec encodes a literal 0xE5 as `$E5 $E5 $00 $01`.
        let header = MsaHeader {
            sectors_per_track: 1,
            sides: 0,
            start_track: 0,
            end_track: 0,
        };
        let track_size = MSA_SECTOR_SIZE;
        let mut payload = Vec::new();
        // First 511 bytes 0x00 via RLE: $E5 $00 $01FF.
        payload.push(MSA_RLE_MARKER);
        payload.push(0x00);
        let mut buf = [0u8; 2];
        BigEndian::write_u16(&mut buf, 511);
        payload.extend_from_slice(&buf);
        // Then a literal 0xE5 via $E5 $E5 $00 $01.
        payload.push(MSA_RLE_MARKER);
        payload.push(MSA_RLE_MARKER);
        BigEndian::write_u16(&mut buf, 1);
        payload.extend_from_slice(&buf);

        let mut msa = Vec::new();
        msa.extend_from_slice(&MSA_MAGIC);
        let mut hdr = [0u8; 8];
        BigEndian::write_u16(&mut hdr[0..2], header.sectors_per_track);
        BigEndian::write_u16(&mut hdr[2..4], header.sides);
        BigEndian::write_u16(&mut hdr[4..6], header.start_track);
        BigEndian::write_u16(&mut hdr[6..8], header.end_track);
        msa.extend_from_slice(&hdr);
        let mut plen = [0u8; 2];
        BigEndian::write_u16(&mut plen, payload.len() as u16);
        msa.extend_from_slice(&plen);
        msa.extend_from_slice(&payload);

        let decoded = decode_msa_bytes(&msa).unwrap();
        assert_eq!(decoded.len(), track_size);
        assert_eq!(decoded[track_size - 1], MSA_RLE_MARKER);
        assert!(decoded[..track_size - 1].iter().all(|b| *b == 0));
    }

    #[test]
    fn rejects_truncated_payload() {
        let mut msa = Vec::new();
        msa.extend_from_slice(&MSA_MAGIC);
        let mut hdr = [0u8; 8];
        BigEndian::write_u16(&mut hdr[0..2], 9);
        BigEndian::write_u16(&mut hdr[2..4], 1);
        BigEndian::write_u16(&mut hdr[4..6], 0);
        BigEndian::write_u16(&mut hdr[6..8], 79);
        msa.extend_from_slice(&hdr);
        // Claim a 4608 B track but provide no bytes.
        let mut plen = [0u8; 2];
        BigEndian::write_u16(&mut plen, 4608);
        msa.extend_from_slice(&plen);
        assert!(decode_msa_bytes(&msa).is_err());
    }

    #[test]
    fn rejects_rle_overrun() {
        // 1-sector header; payload's RLE asks for 600 bytes on a 512-byte
        // track. Decode must refuse.
        let mut msa = Vec::new();
        msa.extend_from_slice(&MSA_MAGIC);
        let mut hdr = [0u8; 8];
        BigEndian::write_u16(&mut hdr[0..2], 1);
        BigEndian::write_u16(&mut hdr[2..4], 0);
        BigEndian::write_u16(&mut hdr[4..6], 0);
        BigEndian::write_u16(&mut hdr[6..8], 0);
        msa.extend_from_slice(&hdr);
        let mut payload = Vec::new();
        payload.push(MSA_RLE_MARKER);
        payload.push(0x00);
        let mut buf = [0u8; 2];
        BigEndian::write_u16(&mut buf, 600);
        payload.extend_from_slice(&buf);
        let mut plen = [0u8; 2];
        BigEndian::write_u16(&mut plen, payload.len() as u16);
        msa.extend_from_slice(&plen);
        msa.extend_from_slice(&payload);
        assert!(decode_msa_bytes(&msa).is_err());
    }
}
