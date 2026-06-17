//! Salto Alto emulator "cooked" disk image (`.dsk`).
//!
//! Salto (Jürgen Buchmüller's Xerox Alto I/II simulator) stores a Diablo-31
//! pack as a flat array of fixed 267-word sector records, in ascending
//! virtual-disk-address order, with no file-level header:
//!
//! ```text
//! record = [pageno: 1 word][header: 2 words][label: 8 words][data: 256 words]
//!        = 267 words = 534 bytes
//! image  = DRIVE_PAGES (203*2*12 = 4872) records = 2,601,648 bytes
//! ```
//!
//! Word **byte order varies between images** and is auto-detected. Salto's
//! native cooked format is little-endian (its `drive.c` reads each word as
//! `byte0 + 256*byte1`), which is how the disks distributed with Salto
//! (`games`, `bcpl`, `st76boot`, ...) are stored; some images (e.g.
//! `tdisk8.dsk`) are big-endian instead. We detect the order from the `pageno`
//! word of record 1 (which must equal its VDA) and normalize to a big-endian
//! [`Disk`] either way, so the rest of the stack (BFS, PDI, browse) sees one
//! convention. `write` emits Salto-native **little-endian** so a produced image
//! loads in the emulator. The leading `pageno` word is redundant with the
//! record's position; we validate it on read and regenerate it on write.
//!
//! Aside from the `pageno` prefix and the missing 512-byte container header,
//! this is exactly the per-sector content of a PDI image (`headerBytes = 4`):
//! a label-inclusive sector image. So a Salto pack converts to/from our
//! [`Disk`] (and hence PDI, BFS browse, write/resize) with no transcoding.
//!
//! Note: most distributed Salto disks are `.dsk.Z` (Unix `compress`/LZW); this
//! reader takes the already-decompressed image bytes. (`uncompress`/`gzip -d`
//! produces the raw `.dsk` these functions consume.)
//!
//! Source: Salto `include/drive.h` (geometry) and `src/drive.c` (`sector_t`,
//! the cooked-image layout).

use super::super::filesystem::FilesystemError;
use super::{be16, put_be16, Disk, FsFamily, Geometry, Sector};

/// Words per sector record: 1 pageno + 2 header + 8 label + 256 data.
const RECORD_WORDS: usize = 1 + 2 + 8 + 256;
/// Bytes per sector record (534).
pub const RECORD_BYTES: usize = RECORD_WORDS * 2;

const PAGENO_BYTES: usize = 2;
const HEADER_BYTES: usize = 4;
const LABEL_BYTES: usize = 16;
const DATA_BYTES: usize = 512;

/// Diablo-31 geometry, the only shape Salto images use (`DRIVE_CYLINDERS = 203`).
const N_CYLINDERS: u16 = 203;
const N_HEADS: u16 = 2;
const N_SECTORS: u16 = 12;
/// Sectors in a Salto pack (203 * 2 * 12 = 4872).
pub const SALTO_SECTORS: usize = N_CYLINDERS as usize * N_HEADS as usize * N_SECTORS as usize;
/// Total bytes of a single-drive Salto image (4872 * 534 = 2,601,648).
pub const IMAGE_BYTES: usize = SALTO_SECTORS * RECORD_BYTES;

/// Heuristic: does `bytes` look like a Salto cooked `.dsk` image? Salto images
/// carry no magic, so this keys on the exact single-drive Diablo-31 size. (PDI
/// and CopyDisk are distinguished by their leading magic before this is tried.)
pub fn is_salto_image(bytes: &[u8]) -> bool {
    bytes.len() == IMAGE_BYTES
}

fn diablo31_geometry() -> Geometry {
    Geometry {
        family: FsFamily::Diablo,
        disk_model: 31,
        n_disks: 1,
        n_cylinders: N_CYLINDERS,
        n_heads: N_HEADS,
        n_sectors: N_SECTORS,
        label_bytes: LABEL_BYTES as u16,
        data_bytes: DATA_BYTES as u16,
    }
}

/// Swap the bytes of every 16-bit word in `buf` in place (little-endian image
/// word -> our big-endian [`Disk`] word). `buf.len()` must be even.
fn swap_words(buf: &mut [u8]) {
    for w in buf.chunks_exact_mut(2) {
        w.swap(0, 1);
    }
}

/// Detect whether a Salto image stores words little-endian, from the `pageno`
/// of record 1 (its VDA, = 1). Big-endian reads `1`; little-endian reads
/// `0x0100`. Falls back to records 2/3 if record 1's pageno is blank.
fn detect_little_endian(bytes: &[u8]) -> Result<bool, FilesystemError> {
    for vda in [1usize, 2, 3] {
        match be16(bytes, vda * RECORD_BYTES) {
            v if v as usize == vda => return Ok(false), // big-endian
            v if (v.swap_bytes() as usize) == vda => return Ok(true), // little-endian
            0 => continue,                              // blank pageno; try next
            _ => break,
        }
    }
    Err(FilesystemError::Parse(
        "Salto .dsk: page-number prefixes are not in VDA order in either byte order".into(),
    ))
}

/// Parse a Salto cooked `.dsk` image into an in-memory [`Disk`], normalizing to
/// big-endian words regardless of the image's stored byte order.
pub fn read(bytes: &[u8]) -> Result<Disk, FilesystemError> {
    if bytes.len() != IMAGE_BYTES {
        return Err(FilesystemError::Parse(format!(
            "Salto .dsk: file is {} bytes, expected {IMAGE_BYTES} ({SALTO_SECTORS} * {RECORD_BYTES})",
            bytes.len()
        )));
    }
    let little = detect_little_endian(bytes)?;

    let mut sectors = Vec::with_capacity(SALTO_SECTORS);
    for vda in 0..SALTO_SECTORS {
        let rec = vda * RECORD_BYTES;
        let label_off = rec + PAGENO_BYTES + HEADER_BYTES;
        let data_off = label_off + LABEL_BYTES;
        let mut label = bytes[label_off..label_off + LABEL_BYTES].to_vec();
        let mut data = bytes[data_off..data_off + DATA_BYTES].to_vec();
        if little {
            swap_words(&mut label);
            swap_words(&mut data);
        }
        sectors.push(Sector { label, data });
    }

    Ok(Disk {
        geometry: diablo31_geometry(),
        sectors,
    })
}

/// Serialize a [`Disk`] to a Salto cooked `.dsk` image. The disk must be a
/// single-drive Diablo-31 pack (the shape Salto loads). The 2-word sector
/// header is reconstructed from the record's VDA — word 0 is zero and word 1 is
/// the packed Diablo disk address ([`Geometry::da_from_vda`]) — matching how
/// Salto's own disks store it and what the emulator reads back.
pub fn write(disk: &Disk) -> Result<Vec<u8>, FilesystemError> {
    let g = &disk.geometry;
    if g.family != FsFamily::Diablo
        || g.total_sectors() != SALTO_SECTORS
        || g.label_bytes as usize != LABEL_BYTES
        || g.data_bytes as usize != DATA_BYTES
    {
        return Err(FilesystemError::Unsupported(format!(
            "Salto .dsk export requires a single-drive Diablo-31 pack ({SALTO_SECTORS} sectors, 16-byte labels, 512-byte data)"
        )));
    }

    let mut out = vec![0u8; IMAGE_BYTES];
    for (vda, sector) in disk.sectors.iter().enumerate() {
        let rec = vda * RECORD_BYTES;
        put_be16(&mut out, rec, vda as u16); // pageno
                                             // sector header: word 0 = 0, word 1 = packed disk address
        put_be16(&mut out, rec + PAGENO_BYTES + 2, g.da_from_vda(vda));
        let label_off = rec + PAGENO_BYTES + HEADER_BYTES;
        let data_off = label_off + LABEL_BYTES;
        out[label_off..label_off + LABEL_BYTES].copy_from_slice(&sector.label);
        out[data_off..data_off + DATA_BYTES].copy_from_slice(&sector.data);
        // Emit Salto-native little-endian: byte-swap every word of the record.
        swap_words(&mut out[rec..rec + RECORD_BYTES]);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a Salto-shaped disk where each label/data carries a recognizable
    /// pattern, then round-trip it through write -> read.
    fn sample_disk() -> Disk {
        let geometry = diablo31_geometry();
        let sectors = (0..SALTO_SECTORS)
            .map(|i| {
                let mut s = Sector::zeroed(LABEL_BYTES, DATA_BYTES);
                put_be16(&mut s.label, 0, (i & 0xffff) as u16);
                put_be16(&mut s.data, 0, (0x1000u16).wrapping_add(i as u16));
                s
            })
            .collect();
        Disk { geometry, sectors }
    }

    #[test]
    fn round_trip_preserves_labels_and_data() {
        let disk = sample_disk();
        let bytes = write(&disk).expect("write");
        assert_eq!(bytes.len(), IMAGE_BYTES);
        assert!(is_salto_image(&bytes));
        // write() emits Salto-native little-endian.
        assert!(detect_little_endian(&bytes).expect("detect"));

        let back = read(&bytes).expect("read");
        assert_eq!(back.geometry.total_sectors(), SALTO_SECTORS);
        assert_eq!(back.geometry.disk_model, 31);
        for vda in 0..SALTO_SECTORS {
            assert_eq!(
                back.sectors[vda].label, disk.sectors[vda].label,
                "label {vda}"
            );
            assert_eq!(back.sectors[vda].data, disk.sectors[vda].data, "data {vda}");
        }
    }

    #[test]
    fn pageno_prefix_is_the_vda_little_endian() {
        let bytes = write(&sample_disk()).expect("write");
        // record 7's pageno word, stored little-endian, decodes to 7.
        assert_eq!(be16(&bytes, 7 * RECORD_BYTES).swap_bytes(), 7);
    }

    #[test]
    fn reads_big_endian_image_too() {
        // Some images (e.g. tdisk8.dsk) store words big-endian. Build one and
        // confirm the reader normalizes it identically.
        let disk = sample_disk();
        let mut be = vec![0u8; IMAGE_BYTES];
        for (vda, s) in disk.sectors.iter().enumerate() {
            let rec = vda * RECORD_BYTES;
            put_be16(&mut be, rec, vda as u16); // pageno, big-endian
            let label_off = rec + PAGENO_BYTES + HEADER_BYTES;
            let data_off = label_off + LABEL_BYTES;
            be[label_off..label_off + LABEL_BYTES].copy_from_slice(&s.label);
            be[data_off..data_off + DATA_BYTES].copy_from_slice(&s.data);
        }
        assert!(!detect_little_endian(&be).expect("detect"));
        let back = read(&be).expect("read");
        for vda in 0..SALTO_SECTORS {
            assert_eq!(
                back.sectors[vda].label, disk.sectors[vda].label,
                "label {vda}"
            );
            assert_eq!(back.sectors[vda].data, disk.sectors[vda].data, "data {vda}");
        }
    }

    #[test]
    fn rejects_wrong_size() {
        let mut bytes = write(&sample_disk()).expect("write");
        bytes.truncate(bytes.len() - RECORD_BYTES);
        assert!(read(&bytes).is_err());
        assert!(!is_salto_image(&bytes));
    }
}
