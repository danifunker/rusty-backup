//! G64 — raw GCR track image for the Commodore 1541 (and `.g71` for the
//! 1571). Unlike `.d64` (clean 256-byte logical sectors), a `.g64` stores
//! the **low-level GCR bitstream** of each track: sync marks, header and
//! data blocks, gaps — exactly what the read head sees. This is the
//! preservation-grade format used for copy-protected discs.
//!
//! We decode `.g64` down to a flat `.d64` so the [`crate::fs::cbm`] engine
//! (which speaks logical sectors) can read it. Decode is read-only: we do
//! not re-encode GCR (an editor would round-trip through `.d64`).
//!
//! ## Container framing (VICE G64 spec)
//!
//! ```text
//! 0x00  8   signature "GCR-1541" (or "GCR-1571" for .g71)
//! 0x08  1   version (0)
//! 0x09  1   number of half-tracks in the image (typically 84)
//! 0x0A  2   max track size in bytes, LE (typically 7928)
//! 0x0C  4*N track-offset table: LE u32 file offset per half-track (0 = none)
//! ...   4*N speed-zone table (ignored here)
//! ...       per track: [2-byte LE actual GCR length][GCR bytes, padded]
//! ```
//!
//! Half-track index for whole track `T` (1-based) is `(T - 1) * 2`.
//!
//! ## GCR sector encoding (1541 ROM)
//!
//! Within a track, each sector is `SYNC · header · gap · SYNC · data · gap`.
//! GCR is a 4-bit→5-bit code (the 16 codes each have no more than two
//! consecutive zero bits, so long zero runs never appear in valid data and
//! a run of ≥10 one-bits unambiguously marks a SYNC). Five GCR bytes
//! decode to four data bytes.
//!
//! - **Header block** (8 decoded bytes): `[0x08, cksum, sector, track,
//!   id2, id1, 0x0F, 0x0F]`, `cksum = sector ^ track ^ id2 ^ id1`.
//! - **Data block** (260 decoded bytes): `[0x07, data[256], cksum, 0, 0]`,
//!   `cksum = XOR of the 256 data bytes`.

use anyhow::{anyhow, bail, Context, Result};

pub const G64_SIGNATURE: &[u8; 8] = b"GCR-1541";
pub const G71_SIGNATURE: &[u8; 8] = b"GCR-1571";

const CBM_SECTOR: usize = 256;

/// GCR encode table: 4-bit nibble -> 5-bit code (1541 ROM standard).
const GCR_ENCODE: [u8; 16] = [
    0x0A, 0x0B, 0x12, 0x13, 0x0E, 0x0F, 0x16, 0x17, 0x09, 0x19, 0x1A, 0x1B, 0x0D, 0x1D, 0x1E, 0x15,
];

/// GCR decode table: 5-bit code -> nibble, or 0xFF for an invalid code.
fn gcr_decode_nibble(code: u8) -> u8 {
    match code & 0x1F {
        0x0A => 0,
        0x0B => 1,
        0x12 => 2,
        0x13 => 3,
        0x0E => 4,
        0x0F => 5,
        0x16 => 6,
        0x17 => 7,
        0x09 => 8,
        0x19 => 9,
        0x1A => 10,
        0x1B => 11,
        0x0D => 12,
        0x1D => 13,
        0x1E => 14,
        0x15 => 15,
        _ => 0xFF,
    }
}

/// 1541 zone map: sectors on a 1-based track.
fn sectors_in_track(track: u8) -> u8 {
    match track {
        1..=17 => 21,
        18..=24 => 19,
        25..=30 => 18,
        _ => 17,
    }
}

/// Byte offset of (track, sector) in a flat D64.
fn d64_offset(track: u8, sector: u8) -> usize {
    let before: usize = (1..track).map(|t| sectors_in_track(t) as usize).sum();
    (before + sector as usize) * CBM_SECTOR
}

/// True when `head` begins with the G64 (or G71) signature.
pub fn looks_like_g64_header(head: &[u8]) -> bool {
    head.len() >= 8 && (&head[0..8] == G64_SIGNATURE || &head[0..8] == G71_SIGNATURE)
}

// ---------------------------------------------------------------------------
// Bitstream decode
// ---------------------------------------------------------------------------

/// Expand GCR bytes into a MSB-first bit vector.
fn bits_of(track: &[u8]) -> Vec<bool> {
    let mut bits = Vec::with_capacity(track.len() * 8);
    for &b in track {
        for i in (0..8).rev() {
            bits.push((b >> i) & 1 == 1);
        }
    }
    bits
}

/// Read one GCR-decoded nibble (5 bits) at `pos`, advancing it. Returns
/// `None` on an invalid GCR code or if the stream runs out.
fn read_nibble(bits: &[bool], pos: &mut usize) -> Option<u8> {
    if *pos + 5 > bits.len() {
        return None;
    }
    let mut code = 0u8;
    for _ in 0..5 {
        code = (code << 1) | (bits[*pos] as u8);
        *pos += 1;
    }
    let n = gcr_decode_nibble(code);
    (n != 0xFF).then_some(n)
}

/// Read `n` GCR-decoded bytes starting at `pos`.
fn read_bytes(bits: &[bool], pos: &mut usize, n: usize) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let hi = read_nibble(bits, pos)?;
        let lo = read_nibble(bits, pos)?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

/// Decode all sectors found on one track's GCR bytes into a
/// `sector -> 256 bytes` map.
fn decode_track(track_gcr: &[u8], expected_track: u8) -> std::collections::HashMap<u8, Vec<u8>> {
    let bits = bits_of(track_gcr);
    let n = bits.len();
    let mut sectors = std::collections::HashMap::new();
    let mut last_header_sector: Option<u8> = None;

    let mut i = 0usize;
    while i < n {
        // Find the next SYNC: a run of >= 10 one-bits.
        if !bits[i] {
            i += 1;
            continue;
        }
        let run_start = i;
        while i < n && bits[i] {
            i += 1;
        }
        if i - run_start < 10 {
            continue; // not a sync; keep scanning
        }
        // `i` now points just past the sync, at the first block bit.
        let mut pos = i;
        // Peek the block ID byte.
        let Some(id) = read_bytes(&bits, &mut pos, 1) else {
            break;
        };
        match id[0] {
            0x08 => {
                // Header: 7 more decoded bytes follow the ID.
                if let Some(rest) = read_bytes(&bits, &mut pos, 7) {
                    let sector = rest[1];
                    let track = rest[2];
                    if track == expected_track {
                        last_header_sector = Some(sector);
                    }
                }
            }
            0x07 => {
                // After the consumed ID byte, the data block is
                // 256 payload + 1 checksum + 2 trailing = 259 bytes.
                if let Some(block) = read_bytes(&bits, &mut pos, 259) {
                    let data = &block[0..CBM_SECTOR];
                    let cksum = block[CBM_SECTOR];
                    let computed = data.iter().fold(0u8, |a, &b| a ^ b);
                    if cksum == computed {
                        if let Some(s) = last_header_sector.take() {
                            sectors.entry(s).or_insert_with(|| data.to_vec());
                        }
                    }
                }
            }
            _ => {}
        }
    }
    sectors
}

/// Decode a `.g64` image into a flat `.d64`. `.g71` (GCR-1571) is detected
/// but not yet supported (the side-1 half-track mapping needs a real
/// sample to validate against).
pub fn decode_g64_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() < 12 {
        bail!("G64 too small ({} bytes)", bytes.len());
    }
    if &bytes[0..8] == G71_SIGNATURE {
        bail!("G71 (1571 GCR) images are not yet supported; convert to .d71 first");
    }
    if &bytes[0..8] != G64_SIGNATURE {
        bail!("not a G64 image (bad signature)");
    }
    let num_halftracks = bytes[9] as usize;
    let max_track_size = u16::from_le_bytes([bytes[0x0A], bytes[0x0B]]) as usize;
    if max_track_size == 0 || max_track_size > 16384 {
        bail!("implausible G64 max track size {max_track_size}");
    }

    let table_base = 0x0C;
    let read_offset = |ht: usize| -> Option<u32> {
        let o = table_base + ht * 4;
        if o + 4 > bytes.len() {
            return None;
        }
        Some(u32::from_le_bytes([
            bytes[o],
            bytes[o + 1],
            bytes[o + 2],
            bytes[o + 3],
        ]))
    };

    // Decode whole tracks 1..=40 (D64 tops out at 40 tracks). Track T lives
    // at half-track index (T-1)*2.
    let mut decoded: Vec<(u8, std::collections::HashMap<u8, Vec<u8>>)> = Vec::new();
    let mut highest_track = 0u8;
    for track in 1..=40u8 {
        let ht = (track as usize - 1) * 2;
        if ht >= num_halftracks {
            break;
        }
        let Some(off) = read_offset(ht) else { break };
        if off == 0 {
            continue; // no data on this track
        }
        let off = off as usize;
        if off + 2 > bytes.len() {
            bail!("G64 track {track} offset {off} past EOF");
        }
        let len = u16::from_le_bytes([bytes[off], bytes[off + 1]]) as usize;
        let start = off + 2;
        let end = (start + len).min(bytes.len());
        if start >= end {
            continue;
        }
        let sectors = decode_track(&bytes[start..end], track);
        if !sectors.is_empty() {
            highest_track = track;
            decoded.push((track, sectors));
        }
    }

    if highest_track == 0 {
        bail!("G64 decoded no readable sectors");
    }
    // D64 is 35-track unless the image clearly uses the 40-track extension.
    let total_tracks = if highest_track > 35 { 40 } else { 35 };
    let total_sectors: usize = (1..=total_tracks)
        .map(|t| sectors_in_track(t) as usize)
        .sum();
    let mut d64 = vec![0u8; total_sectors * CBM_SECTOR];

    for (track, sectors) in decoded {
        if track > total_tracks {
            continue;
        }
        for (sector, data) in sectors {
            if sector >= sectors_in_track(track) {
                continue;
            }
            let off = d64_offset(track, sector);
            d64[off..off + CBM_SECTOR].copy_from_slice(&data);
        }
    }
    Ok(d64)
}

/// Convenience: read a file and decode it.
pub fn decode_g64_file(path: &std::path::Path) -> Result<Vec<u8>> {
    let bytes = std::fs::read(path).with_context(|| format!("read G64 {}", path.display()))?;
    decode_g64_bytes(&bytes)
}

// ---------------------------------------------------------------------------
// GCR encode (used only by tests to build a G64 from a known D64; the
// production path is decode-only). Kept here so encode/decode round-trip in
// one place and a future G64 writer has a starting point.
// ---------------------------------------------------------------------------

/// GCR-encode a byte buffer (length must be a multiple of 4) into GCR
/// bytes. Four input bytes (eight nibbles) become five GCR bytes.
pub fn gcr_encode(data: &[u8]) -> Result<Vec<u8>> {
    if !data.len().is_multiple_of(4) {
        return Err(anyhow!("gcr_encode needs a multiple of 4 bytes"));
    }
    let mut out = Vec::with_capacity(data.len() / 4 * 5);
    for group in data.chunks_exact(4) {
        // 8 nibbles -> 8 five-bit codes -> 40 bits -> 5 bytes.
        let mut acc: u64 = 0;
        for &b in group {
            let hi = GCR_ENCODE[(b >> 4) as usize] as u64;
            let lo = GCR_ENCODE[(b & 0x0F) as usize] as u64;
            acc = (acc << 5) | hi;
            acc = (acc << 5) | lo;
        }
        // acc holds 40 bits, MSB first.
        for shift in (0..5).rev() {
            out.push(((acc >> (shift * 8)) & 0xFF) as u8);
        }
    }
    Ok(out)
}

/// Build a minimal but spec-faithful G64 from a flat D64.
///
/// Emits the standard 1541 sector layout (`SYNC · header · gap · SYNC ·
/// data · gap`) with correct GCR encoding and checksums, a fixed disk ID
/// (`0xA0 0xA0` — the decode path ignores it), and a 84-half-track offset
/// table. The result is a valid `.g64` that round-trips through
/// [`decode_g64_bytes`] and is readable by VICE / the cbm engine.
/// `tracks` is the source track count (35 or 40).
pub fn encode_g64_from_d64(d64: &[u8], tracks: u8) -> Result<Vec<u8>> {
    const MAX_TRACK_SIZE: usize = 7928;
    const NUM_HALFTRACKS: usize = 84;
    let id1 = 0xA0u8;
    let id2 = 0xA0u8;

    // Build each track's GCR bytes.
    let mut track_blobs: Vec<Option<Vec<u8>>> = vec![None; NUM_HALFTRACKS];
    for track in 1..=tracks {
        let spt = sectors_in_track(track);
        let mut gcr = Vec::new();
        for sector in 0..spt {
            // Header block.
            let cksum = sector ^ track ^ id2 ^ id1;
            let header = [0x08, cksum, sector, track, id2, id1, 0x0F, 0x0F];
            // Data block.
            let off = d64_offset(track, sector);
            let payload = &d64[off..off + CBM_SECTOR];
            let dcksum = payload.iter().fold(0u8, |a, &b| a ^ b);
            let mut data = Vec::with_capacity(260);
            data.push(0x07);
            data.extend_from_slice(payload);
            data.push(dcksum);
            data.push(0x00);
            data.push(0x00);
            // SYNC (5 x 0xFF) + header GCR + gap + SYNC + data GCR + gap.
            gcr.extend_from_slice(&[0xFF; 5]);
            gcr.extend_from_slice(&gcr_encode(&header)?);
            gcr.extend_from_slice(&[0x55; 9]);
            gcr.extend_from_slice(&[0xFF; 5]);
            gcr.extend_from_slice(&gcr_encode(&data)?);
            gcr.extend_from_slice(&[0x55; 8]);
        }
        if gcr.len() > MAX_TRACK_SIZE {
            bail!(
                "track {track} GCR {} exceeds max {MAX_TRACK_SIZE}",
                gcr.len()
            );
        }
        track_blobs[(track as usize - 1) * 2] = Some(gcr);
    }

    // Assemble the container.
    let mut out = Vec::new();
    out.extend_from_slice(G64_SIGNATURE);
    out.push(0); // version
    out.push(NUM_HALFTRACKS as u8);
    out.extend_from_slice(&(MAX_TRACK_SIZE as u16).to_le_bytes());

    // Reserve the two tables; fill offsets after we lay out track data.
    let table_bytes = NUM_HALFTRACKS * 4 * 2;
    let data_start = out.len() + table_bytes;
    let mut offsets = vec![0u32; NUM_HALFTRACKS];
    let mut blob_area = Vec::new();
    for (ht, blob) in track_blobs.iter().enumerate() {
        if let Some(gcr) = blob {
            offsets[ht] = (data_start + blob_area.len()) as u32;
            blob_area.extend_from_slice(&(gcr.len() as u16).to_le_bytes());
            blob_area.extend_from_slice(gcr);
            // Pad to max track size + the 2-byte length prefix.
            let padded = MAX_TRACK_SIZE + 2;
            let cur = 2 + gcr.len();
            blob_area.extend(std::iter::repeat_n(0u8, padded - cur));
        }
    }
    // Track-offset table.
    for &o in &offsets {
        out.extend_from_slice(&o.to_le_bytes());
    }
    // Speed-zone table (zone value per half-track; coarse, unused on decode).
    for _ in 0..NUM_HALFTRACKS {
        out.extend_from_slice(&0u32.to_le_bytes());
    }
    out.extend_from_slice(&blob_area);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gcr_table_is_self_inverse() {
        for nib in 0u8..16 {
            let code = GCR_ENCODE[nib as usize];
            assert_eq!(gcr_decode_nibble(code), nib, "GCR table mismatch at {nib}");
        }
        // A couple of invalid codes decode to 0xFF.
        assert_eq!(gcr_decode_nibble(0x00), 0xFF);
        assert_eq!(gcr_decode_nibble(0x1F), 0xFF);
    }

    #[test]
    fn looks_like_header_matches_both_signatures() {
        assert!(looks_like_g64_header(b"GCR-1541\x00\x54\xF8\x1E"));
        assert!(looks_like_g64_header(b"GCR-1571\x00\x54\xF8\x1E"));
        assert!(!looks_like_g64_header(b"GCR-9999"));
        assert!(!looks_like_g64_header(b"D64"));
    }

    #[test]
    fn g71_is_detected_but_unsupported() {
        let mut bytes = vec![0u8; 32];
        bytes[..8].copy_from_slice(G71_SIGNATURE);
        bytes[0x0A] = 0xF8;
        bytes[0x0B] = 0x1E;
        let err = decode_g64_bytes(&bytes).unwrap_err();
        assert!(err.to_string().contains("G71"), "got: {err}");
    }

    /// Build a tiny synthetic D64 (header + a couple of files would need the
    /// cbm engine; here we just fill recognizable per-sector bytes), encode
    /// it to G64, decode it back, and assert the data sectors survive.
    #[test]
    fn d64_to_g64_round_trips() {
        // 35-track D64.
        let total_sectors: usize = (1..=35u8).map(|t| sectors_in_track(t) as usize).sum();
        let mut d64 = vec![0u8; total_sectors * CBM_SECTOR];
        // Stamp each sector with a deterministic pattern tied to (track,sector).
        for track in 1..=35u8 {
            for sector in 0..sectors_in_track(track) {
                let off = d64_offset(track, sector);
                for (i, b) in d64[off..off + CBM_SECTOR].iter_mut().enumerate() {
                    *b = (track ^ sector ^ (i as u8)).wrapping_add(track);
                }
            }
        }

        let g64 = encode_g64_from_d64(&d64, 35).expect("encode");
        assert!(looks_like_g64_header(&g64));
        let back = decode_g64_bytes(&g64).expect("decode");
        assert_eq!(back.len(), d64.len(), "decoded D64 size mismatch");
        assert_eq!(back, d64, "round-trip mismatch");
    }
}
