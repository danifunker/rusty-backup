//! NDIF (New Disk Image Format) reader — classic Mac OS **Disk Copy 6**
//! read-only disk images (type `rohd` / creator `ddsk`, and the read-write
//! `rdxw` variant).
//!
//! NDIF predates the modern UDIF `.dmg` ([`crate::dmg`]) and is laid out
//! differently: the disk blocks live in the file's **data fork** as a series of
//! chunks (zero-fill / raw / ADC-compressed), and the **block map** describing
//! them is a `bcem` resource in the **resource fork**. Reconstructing the raw
//! image therefore needs *both* forks — which every carrier we read (MacBinary,
//! BinHex, AppleDouble) provides.
//!
//! `bcem` layout (big-endian), pinned against real images:
//! - `u16` version, `u16` driver, then a `Str63` volume name,
//! - `u32` total 512-byte sectors at offset 68, `u32` max sectors per chunk at 72,
//! - `u32` data offset at 76 (added to every chunk offset), `u32` CRC at 80,
//! - `u32` chunk count at offset 124,
//! - then `count` × 12-byte chunk entries from offset 128:
//!   `w0 = (firstSector << 8) | type`, `w1 = dataForkOffset`, `w2 = length`.
//!
//! The CRC at 80 covers the rebuilt image: a table CRC that shifts right yet uses the
//! *unreflected* polynomial `0x04C11DB7`, seed `0xFFFFFFFF`, no final XOR, low 28 bits
//! kept (Aaru's `NdifCrc28`; it matches all 15 ShrinkWrap samples of issue #100). A rebuild
//! checks it, which is how a damaged data fork is told apart from an unknown codec.
//!
//! Chunk `type` (the low byte of `w0`) selects the per-chunk codec:
//! `0x00` NOCOPY (zero-fill), `0x02` COPY (raw), `0x83` ADC, `0x81` RLE,
//! `0x82` LZH, `0x80` KenCode, `0xF0` StuffIt, `0xFF` end-of-table. Zero / raw /
//! ADC are decoded; the other compressed codecs are recognized and reported
//! with a clear "not yet supported" error (rather than silently misdecoded) —
//! they await real sample images to implement and verify against. Real
//! Disk Copy 6 output overwhelmingly uses zero + raw + ADC.

use anyhow::{bail, Context, Result};

use crate::dmg::decompress_adc;

/// Resource-fork tag for the NDIF block map.
const BCEM: [u8; 4] = *b"bcem";

/// Offsets within the `bcem` resource.
const OFF_TOTAL_SECTORS: usize = 68;
const OFF_DATA_OFFSET: usize = 76;
const OFF_CRC: usize = 80;
const OFF_CHUNK_COUNT: usize = 124;
const OFF_CHUNKS: usize = 128;
const CHUNK_LEN: usize = 12;

/// Chunk type codes (the low byte of the first word).
const TYPE_ZERO: u8 = 0x00; // NOCOPY — zero-fill run
const TYPE_RAW: u8 = 0x02; // COPY — uncompressed
const TYPE_KENCODE: u8 = 0x80; // KenCode (undocumented)
const TYPE_RLE: u8 = 0x81; // Apple RLE
const TYPE_LZH: u8 = 0x82; // LZH
const TYPE_ADC: u8 = 0x83; // Apple Data Compression
const TYPE_STUFFIT: u8 = 0xf0; // StuffIt
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
    let mut out = std::io::Cursor::new(Vec::new());
    let len = reconstruct_to(
        &mut |off, len| {
            data_fork
                .get(off..off + len)
                .map(<[u8]>::to_vec)
                .with_context(|| {
                    format!("NDIF: chunk data range {off}..{} out of bounds", off + len)
                })
        },
        bcem,
        &mut out,
    )?;
    let mut v = out.into_inner();
    v.resize(len as usize, 0);
    Ok(v)
}

/// [`reconstruct`] chunk by chunk into `out`: `read(off, len)` fetches stored data-fork bytes,
/// zero chunks are skipped (so a file `out` stays sparse). Returns the image length.
pub fn reconstruct_to<W: std::io::Write + std::io::Seek>(
    read: &mut dyn FnMut(usize, usize) -> Result<Vec<u8>>,
    bcem: &[u8],
    out: &mut W,
) -> Result<u64> {
    use std::io::SeekFrom;
    let total_sectors = read_u32(bcem, OFF_TOTAL_SECTORS)? as u64;
    let data_offset = read_u32(bcem, OFF_DATA_OFFSET)? as usize;
    let stored_crc = read_u32(bcem, OFF_CRC)? & CRC28_MASK;
    let count = read_u32(bcem, OFF_CHUNK_COUNT)? as usize;
    let image_len = total_sectors
        .checked_mul(512)
        .filter(|&n| n <= MAX_IMAGE_BYTES)
        .with_context(|| format!("NDIF: implausible image size ({total_sectors} sectors)"))?;

    // Parse the chunk table up front so each chunk knows where the next begins.
    let mut chunks = Vec::with_capacity(count);
    for i in 0..count {
        let base = OFF_CHUNKS + i * CHUNK_LEN;
        let w0 = read_u32(bcem, base)?;
        let w1 = read_u32(bcem, base + 4)? as usize;
        let w2 = read_u32(bcem, base + 8)? as usize;
        let off = w1.saturating_add(data_offset);
        chunks.push((w0 >> 8, (w0 & 0xff) as u8, off, w2)); // (sector, type, off, len)
    }

    let mut crc = Crc28::new();
    for (i, &(sector, ty, off, len)) in chunks.iter().enumerate() {
        if ty == TYPE_END {
            break;
        }
        let next = chunks
            .get(i + 1)
            .map(|c| c.0)
            .unwrap_or(total_sectors as u32);
        let span = (next.saturating_sub(sector) as usize) * 512;
        let o = u64::from(sector) * 512;
        if o + span as u64 > image_len {
            bail!("NDIF: chunk {i} (sector {sector}) runs past the image end");
        }
        crc.zeros_to(o);
        if ty == TYPE_ZERO || len == 0 {
            continue; // left as a hole / zero-fill
        }
        let src = read(off, len).with_context(|| format!("NDIF: chunk {i}"))?;
        let piece: Vec<u8> = match ty {
            // Uncompressed: the stored length equals the span it covers.
            TYPE_RAW => src[..len.min(span)].to_vec(),
            TYPE_ADC => {
                let mut dec = decompress_adc(&src, span).with_context(|| {
                    format!(
                        "NDIF chunk {i} (sector {sector}, type 0x83 ADC) does not decode; the \
                         image's data fork looks damaged (bytes Disk Copy did not write there)"
                    )
                })?;
                dec.truncate(span);
                dec
            }
            // Recognized-but-unimplemented codecs: report clearly instead of
            // silently misrouting the bytes to the ADC decoder. These await a
            // real sample image that uses them (Disk Copy 6 almost always emits
            // zero/raw/ADC; KenCode is undocumented).
            TYPE_RLE => bail!(
                "NDIF chunk {i} uses Apple RLE compression (type 0x81), which is not yet \
                 supported"
            ),
            TYPE_LZH => {
                bail!("NDIF chunk {i} uses LZH compression (type 0x82), which is not yet supported")
            }
            TYPE_KENCODE => bail!(
                "NDIF chunk {i} uses KenCode compression (type 0x80), which is undocumented and \
                 not supported"
            ),
            TYPE_STUFFIT => bail!(
                "NDIF chunk {i} uses StuffIt compression (type 0xF0), which is not yet supported"
            ),
            // Unknown type: a verbatim copy only when the stored bytes fill the span exactly.
            _ if len == span => src,
            _ => bail!("NDIF chunk {i} has unknown type 0x{ty:02X} (length {len}, span {span})"),
        };
        out.seek(SeekFrom::Start(o))?;
        out.write_all(&piece)?;
        crc.update(&piece);
    }
    crc.zeros_to(image_len);
    if let Some(got) = crc.finish().filter(|&c| stored_crc != 0 && c != stored_crc) {
        log::warn!(
            "NDIF: rebuilt image CRC {got:07X} does not match the stored {stored_crc:07X}; the \
             image data is damaged"
        );
    }
    Ok(image_len)
}

/// The 28 bits of a rebuilt image's CRC that `bcem` stores.
const CRC28_MASK: u32 = 0x0fff_ffff;

/// Running NDIF image CRC over the bytes written so far, holes counted as zeros.
struct Crc28 {
    table: [u32; 256],
    crc: u32,
    pos: u64,
    in_order: bool,
}

impl Crc28 {
    fn new() -> Self {
        let mut table = [0u32; 256];
        for (i, t) in table.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 {
                    (c >> 1) ^ 0x04c1_1db7
                } else {
                    c >> 1
                };
            }
            *t = c;
        }
        Crc28 {
            table,
            crc: 0xffff_ffff,
            pos: 0,
            in_order: true,
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.crc = (self.crc >> 8) ^ self.table[((self.crc ^ b as u32) & 0xff) as usize];
        }
        self.pos += bytes.len() as u64;
    }

    /// Feed zeros up to byte `end` of the image (a hole, or a short raw chunk's tail).
    fn zeros_to(&mut self, end: u64) {
        self.in_order &= end >= self.pos;
        let zeros = [0u8; 4096];
        while self.pos < end {
            let n = (end - self.pos).min(zeros.len() as u64) as usize;
            self.update(&zeros[..n]);
        }
    }

    /// `None` when chunks came out of image order, so the CRC is not the image's.
    fn finish(&self) -> Option<u32> {
        self.in_order.then_some(self.crc & CRC28_MASK)
    }
}

/// CRC the stored `bcem` header expects for `image`.
#[cfg(test)]
fn image_crc28(image: &[u8]) -> u32 {
    let mut c = Crc28::new();
    c.update(image);
    c.crc & CRC28_MASK
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
            (0, TYPE_RAW, 0, 1024), // raw: sectors 0..2 (1024 bytes) from data[0..1024]
            (2, TYPE_ZERO, 0, 0),   // zero: sector 2
            (3, TYPE_END, 0, 0),    // end (sector 3 left zero by total)
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

    /// A chunk using an unimplemented codec (LZH) reports a clear error naming
    /// the codec, instead of silently misrouting the bytes to the ADC decoder.
    #[test]
    fn reconstruct_lzh_chunk_reports_clear_error() {
        let total = 2u32;
        let mut bcem = vec![0u8; OFF_CHUNKS];
        bcem[OFF_TOTAL_SECTORS..OFF_TOTAL_SECTORS + 4].copy_from_slice(&total.to_be_bytes());
        let entries: &[(u32, u8, usize, usize)] = &[
            (0, TYPE_LZH, 0, 4), // an LZH-compressed chunk spanning sectors 0..2
            (2, TYPE_END, 0, 0),
        ];
        bcem[OFF_CHUNK_COUNT..OFF_CHUNK_COUNT + 4]
            .copy_from_slice(&(entries.len() as u32).to_be_bytes());
        for (sector, ty, off, len) in entries {
            let w0 = (sector << 8) | (*ty as u32);
            bcem.extend_from_slice(&w0.to_be_bytes());
            bcem.extend_from_slice(&(*off as u32).to_be_bytes());
            bcem.extend_from_slice(&(*len as u32).to_be_bytes());
        }
        let data = vec![0u8; 16];
        let err = reconstruct(&data, &bcem).unwrap_err().to_string();
        assert!(err.contains("LZH"), "error should name the codec: {err}");
    }

    /// Build a `bcem` map for `total` sectors from `(sector, type, offset, length)` entries.
    fn bcem_for(total: u32, entries: &[(u32, u8, usize, usize)], crc: u32) -> Vec<u8> {
        let mut bcem = vec![0u8; OFF_CHUNKS];
        bcem[OFF_TOTAL_SECTORS..OFF_TOTAL_SECTORS + 4].copy_from_slice(&total.to_be_bytes());
        bcem[OFF_CRC..OFF_CRC + 4].copy_from_slice(&crc.to_be_bytes());
        bcem[OFF_CHUNK_COUNT..OFF_CHUNK_COUNT + 4]
            .copy_from_slice(&(entries.len() as u32).to_be_bytes());
        for (sector, ty, off, len) in entries {
            let w0 = (sector << 8) | (*ty as u32);
            bcem.extend_from_slice(&w0.to_be_bytes());
            bcem.extend_from_slice(&(*off as u32).to_be_bytes());
            bcem.extend_from_slice(&(*len as u32).to_be_bytes());
        }
        bcem
    }

    /// Pins the odd CRC (right-shifting with the unreflected polynomial) Disk Copy uses.
    #[test]
    fn crc28_check_value() {
        assert_eq!(image_crc28(b"123456789"), 0x03b0_d416);
    }

    /// An ADC chunk whose bytes are not ADC (issue #99: a damaged data fork) names the
    /// chunk and the likely cause instead of a bare decoder error.
    #[test]
    fn undecodable_adc_chunk_reports_damage() {
        // 0x48 opens a long match at output 0, as the damaged #99 chunks do.
        let data = vec![0x48, 0xe7, 0x01, 0x08, 0x49, 0xee, 0xff, 0xf0];
        let bcem = bcem_for(2, &[(0, TYPE_ADC, 0, data.len()), (2, TYPE_END, 0, 0)], 0);
        let err = format!("{:#}", reconstruct(&data, &bcem).unwrap_err());
        assert!(err.contains("chunk 0") && err.contains("damaged"), "{err}");
    }

    /// The data offset at 76 shifts every chunk, and a matching CRC rebuilds as before.
    #[test]
    fn data_offset_and_crc_are_honoured() {
        let payload: Vec<u8> = (0..512u32).map(|i| (i % 253) as u8).collect();
        let mut img = payload.clone();
        img.resize(1024, 0);
        let mut bcem = bcem_for(
            2,
            &[
                (0, TYPE_RAW, 0, 512),
                (1, TYPE_ZERO, 0, 0),
                (2, TYPE_END, 0, 0),
            ],
            image_crc28(&img),
        );
        bcem[OFF_DATA_OFFSET..OFF_DATA_OFFSET + 4].copy_from_slice(&7u32.to_be_bytes());
        let mut data = vec![0xAA; 7];
        data.extend_from_slice(&payload);
        assert_eq!(reconstruct(&data, &bcem).unwrap(), img);
    }
}
