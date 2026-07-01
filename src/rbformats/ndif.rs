//! NDIF (New Disk Image Format) reader — classic Mac OS **Disk Copy 6**
//! read-only disk images (type `rohd` / creator `ddsk`, and the read-write
//! `rdxw` variant).
//!
//! NDIF predates the modern UDIF `.dmg` ([`super::dmg`]) and is laid out
//! differently: the disk blocks live in the file's **data fork** as a series of
//! chunks (zero-fill / raw / ADC-compressed), and the **block map** describing
//! them is a `bcem` resource in the **resource fork**. Reconstructing the raw
//! image therefore needs *both* forks — which every carrier we read (MacBinary,
//! BinHex, AppleDouble) provides.
//!
//! `bcem` layout (big-endian), pinned against real images:
//! - `u16` version, then a `Str63` volume name,
//! - `u32` total 512-byte sectors at offset 68,
//! - `u32` chunk count at offset 124,
//! - then `count` × 12-byte chunk entries from offset 128:
//!   `w0 = (firstSector << 8) | type`, `w1 = dataForkOffset`, `w2 = length`.
//!   `type` `0xFF` ends the table, `0x00` (length 0) is a zero-fill run, and any
//!   other entry is raw when its length equals the span it covers, else ADC.

use anyhow::{bail, Context, Result};

use super::dmg::decompress_adc;

/// Resource-fork tag for the NDIF block map.
const BCEM: [u8; 4] = *b"bcem";

/// Offsets within the `bcem` resource.
const OFF_TOTAL_SECTORS: usize = 68;
const OFF_CHUNK_COUNT: usize = 124;
const OFF_CHUNKS: usize = 128;
const CHUNK_LEN: usize = 12;

/// Chunk type codes (the low byte of the first word).
const TYPE_ZERO: u8 = 0x00;
const TYPE_END: u8 = 0xff;

/// Cap on the reconstructed size (4 GiB) — classic NDIF volumes are far
/// smaller; this just bounds a corrupt header.
const MAX_IMAGE_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// True for a Finder type code that denotes an NDIF disk image.
pub fn is_ndif_type(type_code: &[u8; 4]) -> bool {
    matches!(type_code, b"rohd" | b"rdxw" | b"rwhd" | b"rkhd")
}

/// If `resource_fork` contains a `bcem` resource, return its bytes — i.e. this
/// is (probably) an NDIF image.
pub fn extract_bcem(resource_fork: &[u8]) -> Option<Vec<u8>> {
    resource_by_type(resource_fork, &BCEM)
}

/// Reconstruct the raw disk image from an NDIF data fork and its `bcem` block
/// map. Returns the flat image bytes (boot blocks at sector 0).
pub fn reconstruct(data_fork: &[u8], bcem: &[u8]) -> Result<Vec<u8>> {
    let total_sectors = read_u32(bcem, OFF_TOTAL_SECTORS)? as u64;
    let count = read_u32(bcem, OFF_CHUNK_COUNT)? as usize;
    let image_len = total_sectors
        .checked_mul(512)
        .filter(|&n| n <= MAX_IMAGE_BYTES)
        .with_context(|| format!("NDIF: implausible image size ({total_sectors} sectors)"))?
        as usize;

    // Parse the chunk table up front so each chunk knows where the next begins.
    let mut chunks = Vec::with_capacity(count);
    for i in 0..count {
        let base = OFF_CHUNKS + i * CHUNK_LEN;
        let w0 = read_u32(bcem, base)?;
        let w1 = read_u32(bcem, base + 4)? as usize;
        let w2 = read_u32(bcem, base + 8)? as usize;
        chunks.push((w0 >> 8, (w0 & 0xff) as u8, w1, w2)); // (sector, type, off, len)
    }

    let mut out = vec![0u8; image_len];
    for (i, &(sector, ty, off, len)) in chunks.iter().enumerate() {
        if ty == TYPE_END {
            break;
        }
        let next = chunks
            .get(i + 1)
            .map(|c| c.0)
            .unwrap_or(total_sectors as u32);
        let span = (next.saturating_sub(sector) as usize) * 512;
        let o = sector as usize * 512;
        if o + span > out.len() {
            bail!("NDIF: chunk {i} (sector {sector}) runs past the image end");
        }
        if ty == TYPE_ZERO || len == 0 {
            continue; // already zero-filled
        }
        let src = data_fork.get(off..off + len).with_context(|| {
            format!(
                "NDIF: chunk {i} data range {off}..{} out of bounds",
                off + len
            )
        })?;
        if len == span {
            out[o..o + span].copy_from_slice(src); // raw / uncompressed
        } else {
            let dec = decompress_adc(src, span)
                .with_context(|| format!("NDIF: ADC decompress failed for chunk {i}"))?;
            let n = dec.len().min(span);
            out[o..o + n].copy_from_slice(&dec[..n]);
        }
    }
    Ok(out)
}

/// Read a big-endian u32 at `off`, erroring if it runs past the slice.
fn read_u32(b: &[u8], off: usize) -> Result<u32> {
    let s = b
        .get(off..off + 4)
        .with_context(|| format!("NDIF: truncated bcem at offset {off}"))?;
    Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
}

/// Minimal classic-Mac resource-fork parser: return the data bytes of the first
/// resource of type `want`, or `None`. Layout: a 16-byte header (data offset,
/// map offset, data len, map len), a resource-data area where each resource is
/// a 4-byte length prefix + bytes, and a resource map holding a type list and
/// per-type reference lists. See Inside Macintosh: More Macintosh Toolbox.
fn resource_by_type(fork: &[u8], want: &[u8; 4]) -> Option<Vec<u8>> {
    let be16 = |b: &[u8], o: usize| -> Option<usize> {
        b.get(o..o + 2)
            .map(|s| u16::from_be_bytes([s[0], s[1]]) as usize)
    };
    let be32 = |b: &[u8], o: usize| -> Option<usize> {
        b.get(o..o + 4)
            .map(|s| u32::from_be_bytes([s[0], s[1], s[2], s[3]]) as usize)
    };

    let data_off = be32(fork, 0)?;
    let map_off = be32(fork, 4)?;
    // Map: type-list offset (from map start) at +24, then the type list.
    let type_list_off = map_off + be16(fork, map_off + 24)?;
    let num_types = be16(fork, type_list_off)?.wrapping_add(1) & 0xffff; // count - 1
    for t in 0..num_types {
        let entry = type_list_off + 2 + t * 8;
        let tag = fork.get(entry..entry + 4)?;
        if tag != want {
            continue;
        }
        let ref_list_off = type_list_off + be16(fork, entry + 6)?;
        // First reference: its 3-byte data offset is at ref entry +5.
        let data_at = fork.get(ref_list_off + 5..ref_list_off + 8)?;
        let res_off = data_off
            + ((data_at[0] as usize) << 16 | (data_at[1] as usize) << 8 | data_at[2] as usize);
        let len = be32(fork, res_off)?;
        return fork.get(res_off + 4..res_off + 4 + len).map(<[u8]>::to_vec);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal resource fork holding one `bcem` resource, then parse it
    /// back — exercises the resource-map walk.
    #[test]
    fn extract_bcem_round_trips() {
        let payload = b"the-block-map".to_vec();
        // Resource data: 4-byte length prefix + payload.
        let mut data = (payload.len() as u32).to_be_bytes().to_vec();
        data.extend_from_slice(&payload);
        // Map: 16-byte header copy + 4 (next) + 2 (attrs) + 2 (typeListOff) +
        // 2 (nameListOff) = 26 bytes, then the type list.
        let mut map = vec![0u8; 28];
        map[24..26].copy_from_slice(&28u16.to_be_bytes()); // type list at map+28
        map[26..28].copy_from_slice(&0u16.to_be_bytes());
        // Type list: count-1 (=0), then one type entry (tag, refs-1, refListOff).
        map.extend_from_slice(&0u16.to_be_bytes());
        map.extend_from_slice(&BCEM);
        map.extend_from_slice(&0u16.to_be_bytes()); // 1 ref
        let ref_list_off = 28 + 2 + 8; // within the map
        map.extend_from_slice(&((ref_list_off - 28) as u16).to_be_bytes());
        // Ref entry: id(2) + nameOff(2) + attr(1) + dataOff(3) + reserved(4).
        map.extend_from_slice(&128u16.to_be_bytes());
        map.extend_from_slice(&0xffffu16.to_be_bytes());
        map.push(0);
        map.extend_from_slice(&[0, 0, 0]); // data offset 0
        map.extend_from_slice(&0u32.to_be_bytes());

        let data_off = 16usize;
        let map_off = data_off + data.len();
        let mut fork = Vec::new();
        fork.extend_from_slice(&(data_off as u32).to_be_bytes());
        fork.extend_from_slice(&(map_off as u32).to_be_bytes());
        fork.extend_from_slice(&(data.len() as u32).to_be_bytes());
        fork.extend_from_slice(&(map.len() as u32).to_be_bytes());
        fork.extend_from_slice(&data);
        fork.extend_from_slice(&map);

        assert_eq!(extract_bcem(&fork), Some(payload));
        assert_eq!(resource_by_type(&fork, b"xxxx"), None);
    }

    /// Reconstruct an image from raw + zero + end chunks (the ADC path is the
    /// already-tested `decompress_adc`).
    #[test]
    fn reconstruct_raw_and_zero_chunks() {
        // 4-sector image: sectors 0-1 raw "AB...", sector 2 zero, then end.
        let total = 4u32;
        let mut bcem = vec![0u8; OFF_CHUNKS];
        bcem[OFF_TOTAL_SECTORS..OFF_TOTAL_SECTORS + 4].copy_from_slice(&total.to_be_bytes());
        let entries: &[(u32, u8, usize, usize)] = &[
            (0, 1, 0, 1024),      // raw: sectors 0..2 (1024 bytes) from data[0..1024]
            (2, TYPE_ZERO, 0, 0), // zero: sector 2
            (3, TYPE_END, 0, 0),  // end (sector 3 left zero by total)
        ];
        bcem[OFF_CHUNK_COUNT..OFF_CHUNK_COUNT + 4]
            .copy_from_slice(&(entries.len() as u32).to_be_bytes());
        for (sector, ty, off, len) in entries {
            let w0 = (sector << 8) | (*ty as u32);
            bcem.extend_from_slice(&w0.to_be_bytes());
            bcem.extend_from_slice(&(*off as u32).to_be_bytes());
            bcem.extend_from_slice(&(*len as u32).to_be_bytes());
        }
        let data: Vec<u8> = (0..1024u32).map(|i| (i % 251) as u8).collect();

        let img = reconstruct(&data, &bcem).unwrap();
        assert_eq!(img.len(), total as usize * 512);
        assert_eq!(&img[0..1024], &data[..]); // raw chunk landed
        assert!(img[1024..2048].iter().all(|&b| b == 0)); // zero sector
    }
}
