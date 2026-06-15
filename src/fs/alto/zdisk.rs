//! Dwarf "Draco" 6085 / Daybreak rigid-disk image (`.zdisk` / `.zdelta`).
//!
//! [Dwarf](https://github.com/devhawala/dwarf) is Hans-Walter Latz's Java
//! emulator for the Xerox Mesa machines; its Draco half emulates a 6085
//! workstation running a **Pilot**-based OS (ViewPoint, XDE). Draco stores the
//! emulated rigid disk in this format, which — unlike the Guam/Duchess `.dsk` —
//! is **label-inclusive**, so it carries the out-of-band sector labels the Pilot
//! filesystem lives in. That makes the three disks shipped with Dwarf
//! (ViewPoint 2.0, XDE 5.0) the first real Pilot/Cedar volumes we can validate
//! [`super::pilot`] against.
//!
//! On-disk layout (from Dwarf `iop6085/HDisk.java`, the `DiskFile` class), a
//! raw **zlib (deflate)** stream of big-endian 16-bit words:
//!
//! ```text
//! header (6 words):  sig1=0xDAAD, #heads, #cylinders, #sectors(dbl-word), sig2=0x5CC5
//! then one record per stored sector, ascending by linear index:
//!   record = [linear sector index: dbl-word][label: 10 words][data: 256 words]
//! ```
//!
//! `#sectors = #cylinders * #heads * 16` (sectors-per-track is fixed at 16). The
//! 2-word hardware header (cyl/head/sector) is not stored — it is derivable from
//! the linear index. No ECC is stored (the controller's concern), matching our
//! PDI decision. A full image carries every sector; a sibling `<name>.zdelta`
//! (same framing) carries only the sectors changed since, and is overlaid on
//! load. We import to a [`Disk`] with `family = Pilot` (6085 disks are Pilot),
//! 20-byte labels and 512-byte data — exactly what [`super::pilot::read_volume`]
//! consumes.
//!
//! **Label byte order.** On the 6085/Daybreak the IOP moves sector labels over
//! its "swapped block" DMA path (`IOPMain.mc` commands 5/6), so the label words
//! are stored **byte-swapped relative to the data words** (the data is plain
//! big-endian — its physical-volume-root seal `0xA28A` = `121212`B reads
//! directly). We verified this against the shipped disks: a label's `fileID`
//! byte-swapped equals the `pvID` embedded big-endian in the data, and label
//! word 7 (`File.Type`) byte-swaps to `6` = `tFreePage` on the (mostly free)
//! pack. So on import we byte-swap each label word to normalize to the
//! big-endian convention the rest of the stack (and our own writer) uses, and
//! [`write`] swaps them back. Data words are left untouched.

use std::io::Read;

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;

use super::super::filesystem::FilesystemError;
use super::{be16, put_be16, Disk, FsFamily, Geometry, Sector};

const SIG1: u16 = 0xDAAD;
const SIG2: u16 = 0x5CC5;
/// Fixed sectors-per-track for the 6085 rigid disk.
const N_SECTORS: u16 = 16;
const LABEL_BYTES: usize = 20; // 10 words
const DATA_BYTES: usize = 512; // 256 words
/// `[dbl-word index][label][data]` = 4 + 20 + 512.
const RECORD_BYTES: usize = 4 + LABEL_BYTES + DATA_BYTES;
const HEADER_BYTES: usize = 12; // 6 words

/// Byte-swap every 16-bit word of `buf` in place (`buf.len()` must be even).
/// Used to normalize the 6085's byte-swapped sector labels to big-endian.
fn swap_words(buf: &mut [u8]) {
    for w in buf.chunks_exact_mut(2) {
        w.swap(0, 1);
    }
}

/// Inflate a raw-zlib stream fully into memory.
fn inflate(bytes: &[u8]) -> Result<Vec<u8>, FilesystemError> {
    let mut out = Vec::new();
    ZlibDecoder::new(bytes)
        .read_to_end(&mut out)
        .map_err(|e| FilesystemError::Parse(format!("zdisk: inflate failed: {e}")))?;
    Ok(out)
}

/// Inflate just the first `n` bytes of a raw-zlib stream (for cheap detection).
fn inflate_prefix(bytes: &[u8], n: usize) -> Option<Vec<u8>> {
    let mut d = ZlibDecoder::new(bytes);
    let mut buf = vec![0u8; n];
    let mut filled = 0;
    while filled < n {
        match d.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(k) => filled += k,
            Err(_) => return None,
        }
    }
    buf.truncate(filled);
    Some(buf)
}

/// Heuristic: does `bytes` look like a Dwarf `.zdisk`/`.zdelta`? Keys on the
/// zlib stream inflating to the `0xDAAD` signature word. (CopyDisk and PDI are
/// distinguished by their leading magic before this is tried; a Salto `.dsk` is
/// not zlib-compressed.)
pub fn is_zdisk(bytes: &[u8]) -> bool {
    inflate_prefix(bytes, 2).is_some_and(|p| p.len() == 2 && be16(&p, 0) == SIG1)
}

/// Parse and validate the 6-word file header, returning `(heads, cyls, sects)`.
fn read_header(d: &[u8]) -> Result<(u16, u16, usize), FilesystemError> {
    if d.len() < HEADER_BYTES {
        return Err(FilesystemError::Parse(
            "zdisk: stream shorter than header".into(),
        ));
    }
    let sig1 = be16(d, 0);
    let heads = be16(d, 2);
    let cyls = be16(d, 4);
    let sects = ((be16(d, 6) as usize) << 16) | be16(d, 8) as usize;
    let sig2 = be16(d, 10);
    if sig1 != SIG1 || sig2 != SIG2 {
        return Err(FilesystemError::Parse(format!(
            "zdisk: bad signatures {sig1:#06x}/{sig2:#06x} (want {SIG1:#06x}/{SIG2:#06x})"
        )));
    }
    let expected = cyls as usize * heads as usize * N_SECTORS as usize;
    if sects != expected || cyls == 0 || heads == 0 {
        return Err(FilesystemError::Parse(format!(
            "zdisk: sectorCount {sects} != {cyls}*{heads}*{N_SECTORS} = {expected}"
        )));
    }
    Ok((heads, cyls, sects))
}

fn geometry(heads: u16, cyls: u16) -> Geometry {
    Geometry {
        family: FsFamily::Pilot,
        disk_model: 0,
        n_disks: 1,
        n_cylinders: cyls,
        n_heads: heads,
        n_sectors: N_SECTORS,
        label_bytes: LABEL_BYTES as u16,
        data_bytes: DATA_BYTES as u16,
    }
}

/// Apply the sector records following the header in an inflated stream `d` onto
/// `sectors` (indexed by linear sector number). Used for both the base image and
/// a delta overlay (identical framing).
fn apply_records(d: &[u8], sectors: &mut [Sector]) -> Result<usize, FilesystemError> {
    let mut off = HEADER_BYTES;
    let mut count = 0;
    // A trailing partial record (fewer than 4 header bytes) is treated as EOF,
    // matching Dwarf's reader (EOF while reading the next sector intro is OK).
    while off + 4 <= d.len() {
        let idx = ((be16(d, off) as usize) << 16) | be16(d, off + 2) as usize;
        off += 4;
        if idx >= sectors.len() {
            return Err(FilesystemError::Parse(format!(
                "zdisk: sector index {idx} out of range (0..{})",
                sectors.len()
            )));
        }
        if off + LABEL_BYTES + DATA_BYTES > d.len() {
            return Err(FilesystemError::Parse(
                "zdisk: truncated sector record".into(),
            ));
        }
        sectors[idx]
            .label
            .copy_from_slice(&d[off..off + LABEL_BYTES]);
        swap_words(&mut sectors[idx].label); // 6085 stores labels byte-swapped
        off += LABEL_BYTES;
        sectors[idx].data.copy_from_slice(&d[off..off + DATA_BYTES]);
        off += DATA_BYTES;
        count += 1;
    }
    Ok(count)
}

/// Parse a complete `.zdisk` image into an in-memory [`Disk`] (`family = Pilot`).
pub fn read(bytes: &[u8]) -> Result<Disk, FilesystemError> {
    let d = inflate(bytes)?;
    let (heads, cyls, sects) = read_header(&d)?;
    let mut sectors = vec![Sector::zeroed(LABEL_BYTES, DATA_BYTES); sects];
    apply_records(&d, &mut sectors)?;
    Ok(Disk {
        geometry: geometry(heads, cyls),
        sectors,
    })
}

/// Overlay a `.zdelta` (sibling delta file) onto an already-read disk. The delta
/// must share the base's geometry. Returns the number of sectors replaced.
pub fn apply_delta(disk: &mut Disk, delta_bytes: &[u8]) -> Result<usize, FilesystemError> {
    let d = inflate(delta_bytes)?;
    let (heads, cyls, sects) = read_header(&d)?;
    if cyls != disk.geometry.n_cylinders
        || heads != disk.geometry.n_heads
        || sects != disk.sectors.len()
    {
        return Err(FilesystemError::Parse(
            "zdisk: delta geometry does not match base disk".into(),
        ));
    }
    apply_records(&d, &mut disk.sectors)
}

/// Serialize a [`Disk`] to a full `.zdisk` image (every sector, zlib-deflated).
/// Requires a single-drive Pilot-shaped pack (16 sectors/track, 20-byte labels,
/// 512-byte data) — the shape Dwarf's Draco loads.
pub fn write(disk: &Disk) -> Result<Vec<u8>, FilesystemError> {
    let g = &disk.geometry;
    if g.n_sectors != N_SECTORS
        || g.n_disks != 1
        || g.label_bytes as usize != LABEL_BYTES
        || g.data_bytes as usize != DATA_BYTES
    {
        return Err(FilesystemError::Unsupported(
            "zdisk export requires a single-drive 6085 Pilot pack (16 sectors/track, 20-byte labels, 512-byte data)".into(),
        ));
    }
    let mut raw = vec![0u8; HEADER_BYTES + disk.sectors.len() * RECORD_BYTES];
    put_be16(&mut raw, 0, SIG1);
    put_be16(&mut raw, 2, g.n_heads);
    put_be16(&mut raw, 4, g.n_cylinders);
    put_be16(&mut raw, 6, (disk.sectors.len() >> 16) as u16);
    put_be16(&mut raw, 8, (disk.sectors.len() & 0xffff) as u16);
    put_be16(&mut raw, 10, SIG2);
    let mut off = HEADER_BYTES;
    for (idx, s) in disk.sectors.iter().enumerate() {
        put_be16(&mut raw, off, (idx >> 16) as u16);
        put_be16(&mut raw, off + 2, (idx & 0xffff) as u16);
        off += 4;
        raw[off..off + LABEL_BYTES].copy_from_slice(&s.label);
        swap_words(&mut raw[off..off + LABEL_BYTES]); // back to 6085 byte order
        off += LABEL_BYTES;
        raw[off..off + DATA_BYTES].copy_from_slice(&s.data);
        off += DATA_BYTES;
    }

    let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
    use std::io::Write;
    enc.write_all(&raw)
        .and_then(|_| enc.finish())
        .map_err(|e| FilesystemError::Parse(format!("zdisk: deflate failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_disk(cyls: u16, heads: u16) -> Disk {
        let geometry = geometry(heads, cyls);
        let total = geometry.total_sectors();
        let sectors = (0..total)
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
        let disk = sample_disk(40, 2);
        let bytes = write(&disk).expect("write");
        assert!(is_zdisk(&bytes));

        let back = read(&bytes).expect("read");
        assert_eq!(back.geometry.family, FsFamily::Pilot);
        assert_eq!(back.geometry.n_cylinders, 40);
        assert_eq!(back.geometry.n_heads, 2);
        assert_eq!(back.geometry.total_sectors(), disk.geometry.total_sectors());
        for vda in 0..disk.geometry.total_sectors() {
            assert_eq!(
                back.sectors[vda].label, disk.sectors[vda].label,
                "label {vda}"
            );
            assert_eq!(back.sectors[vda].data, disk.sectors[vda].data, "data {vda}");
        }
    }

    #[test]
    fn delta_overlays_changed_sectors() {
        let mut disk = sample_disk(40, 2);
        // Build a delta that changes sector 5 only.
        let mut delta_raw = vec![0u8; HEADER_BYTES + RECORD_BYTES];
        put_be16(&mut delta_raw, 0, SIG1);
        put_be16(&mut delta_raw, 2, 2); // heads
        put_be16(&mut delta_raw, 4, 40); // cyls
        put_be16(&mut delta_raw, 6, (disk.sectors.len() >> 16) as u16);
        put_be16(&mut delta_raw, 8, (disk.sectors.len() & 0xffff) as u16);
        put_be16(&mut delta_raw, 10, SIG2);
        put_be16(&mut delta_raw, HEADER_BYTES + 2, 5); // index 5
        put_be16(&mut delta_raw, HEADER_BYTES + 4, 0xBEEF); // new label word 0 (6085 order)
        let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
        use std::io::Write;
        enc.write_all(&delta_raw).unwrap();
        let delta = enc.finish().unwrap();

        let replaced = apply_delta(&mut disk, &delta).expect("delta");
        assert_eq!(replaced, 1);
        // import byte-swaps the label, so the 6085-order 0xBEEF normalizes to 0xEFBE
        assert_eq!(be16(&disk.sectors[5].label, 0), 0xEFBE);
        // sector 6 untouched
        assert_eq!(be16(&disk.sectors[6].label, 0), 6);
    }

    #[test]
    fn open_pack_routes_zdisk() {
        let bytes = write(&sample_disk(40, 2)).expect("write");
        let disk = crate::fs::alto::open_pack(&bytes).expect("open_pack");
        assert_eq!(disk.geometry.family, FsFamily::Pilot);
        assert_eq!(disk.geometry.n_cylinders, 40);
    }

    #[test]
    fn rejects_bad_signature() {
        let mut raw = vec![0u8; HEADER_BYTES];
        put_be16(&mut raw, 0, 0x1234);
        let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
        use std::io::Write;
        enc.write_all(&raw).unwrap();
        let bytes = enc.finish().unwrap();
        assert!(read(&bytes).is_err());
        assert!(!is_zdisk(&bytes));
    }
}
