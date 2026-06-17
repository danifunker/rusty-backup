//! Trident (TFS) disk pack image — the ContrAlto2 / Bitsavers / dorado layout.
//!
//! TFS is the *same logical filesystem* as Alto BFS, on Trident T-80 / T-300
//! hardware instead of Diablo: 2048-byte (1024-word) data pages, a 10-word
//! label, and a 2-word disk address. The directory (SysDir), leader pages, file
//! pointers, and free-page bitmap above the sector layer are identical to BFS —
//! only the page size, [label layout](super::LabelCodec), and geometry differ.
//!
//! Pack-image layout (from the dorado emulator `dorado/src/disk.c`), per sector
//! in cylinder/head/sector order, words stored **little-endian**, no file
//! header:
//!
//! ```text
//! sector = [dummy: 1 word][header: 2 words][label: 10 words][data: 1024 words]
//!        = 1037 words = 2074 bytes
//! ```
//!
//! The leading dummy word and the 2-word hardware header are pack-image framing
//! (the header is the physical disk address, derivable from the sector index);
//! the filesystem object is `label + data`. We normalize the little-endian words
//! to the big-endian convention of [`Disk`] on read (and back on write), exactly
//! as [`super::salto`] does for its `.dsk`.
//!
//! Geometry: **T-80** = 815 × 5 × 9 (~75 MB), **T-300** = 815 × 19 × 9
//! (~285 MB) (`TfsInit.bcpl`, dorado `disk.c`). Real packs are written with a
//! physical sector interleave, so [`read`] places each sector at the VDA in its
//! own header rather than by file position. The T-300 split into ≤3 file systems
//! (`fsNumber`, ≤383 tracks each) is not modeled — we browse the first.
//!
//! Validated against the real **Spruce print-server T-300** pack shipped with
//! ContrAlto2 (`spruce-server-t300.zip`).
//!
//! This module also reads the sibling **ContrAlto2 Diablo `.dsk`** ([`read_diablo`]):
//! the same `[dummy][header][label][data]` framing but Diablo geometry (8-word
//! label, 512-byte data → 534 B/sector, the same size as a Salto `.dsk`).
//! ContrAlto's first word is a zero dummy and the disk address is in the 2-word
//! header, whereas Salto's first word is the page number — so a Salto read is
//! tried first and this is the fallback.

use super::super::filesystem::FilesystemError;
use super::{Disk, FsFamily, Geometry, Sector};

const N_SECTORS: u16 = 9;
const HEADER_WORDS: usize = 2;
const LABEL_WORDS: usize = 10;
const DATA_WORDS: usize = 1024;
const DUMMY_WORDS: usize = 1;
const SECTOR_WORDS: usize = DUMMY_WORDS + HEADER_WORDS + LABEL_WORDS + DATA_WORDS;
/// Bytes per pack-image sector (2074).
pub const SECTOR_BYTES: usize = SECTOR_WORDS * 2;

const LABEL_BYTES: usize = LABEL_WORDS * 2; // 20
const DATA_BYTES: usize = DATA_WORDS * 2; // 2048

const T80_CYLINDERS: u16 = 815;
const T80_HEADS: u16 = 5;
const T300_CYLINDERS: u16 = 815;
const T300_HEADS: u16 = 19;

/// Sectors in a T-80 pack (815 × 5 × 9).
pub const T80_SECTORS: usize = T80_CYLINDERS as usize * T80_HEADS as usize * N_SECTORS as usize;
/// Sectors in a T-300 pack (815 × 19 × 9).
pub const T300_SECTORS: usize = T300_CYLINDERS as usize * T300_HEADS as usize * N_SECTORS as usize;
/// Byte size of a T-80 pack image.
pub const T80_BYTES: usize = T80_SECTORS * SECTOR_BYTES;
/// Byte size of a T-300 pack image.
pub const T300_BYTES: usize = T300_SECTORS * SECTOR_BYTES;

// --- ContrAlto2 Diablo pack (same framing, Diablo geometry) ---
const DIABLO_LABEL_BYTES: usize = 16; // 8 words
const DIABLO_DATA_BYTES: usize = 512; // 256 words
/// `[dummy 1w][header 2w][label 8w][data 256w]` = 534 bytes.
const DIABLO_SECTOR_BYTES: usize = 2 + 4 + DIABLO_LABEL_BYTES + DIABLO_DATA_BYTES;
const DIABLO31_CYLINDERS: u16 = 203;
const DIABLO31_HEADS: u16 = 2;
const DIABLO31_SECTORS_PT: u16 = 12;
/// Sectors in a Diablo-31 pack (203 × 2 × 12 = 4872).
pub const DIABLO31_SECTORS: usize =
    DIABLO31_CYLINDERS as usize * DIABLO31_HEADS as usize * DIABLO31_SECTORS_PT as usize;
/// Byte size of a ContrAlto2 Diablo-31 pack (== a Salto `.dsk`; disambiguated by
/// content — see [`read_diablo`]).
pub const DIABLO_IMAGE_BYTES: usize = DIABLO31_SECTORS * DIABLO_SECTOR_BYTES;

/// Trident geometry for the given model (80 or 300).
pub fn geometry(model: u16) -> Geometry {
    let (n_cylinders, n_heads) = if model == 300 {
        (T300_CYLINDERS, T300_HEADS)
    } else {
        (T80_CYLINDERS, T80_HEADS)
    };
    Geometry {
        family: FsFamily::Trident,
        disk_model: model,
        n_disks: 1,
        n_cylinders,
        n_heads,
        n_sectors: N_SECTORS,
        label_bytes: LABEL_BYTES as u16,
        data_bytes: DATA_BYTES as u16,
    }
}

/// Heuristic: does `bytes` look like a Trident pack image? Keys on the exact
/// T-80 / T-300 size (no magic; `open_pack` validates further).
pub fn is_trident_image(bytes: &[u8]) -> bool {
    bytes.len() == T80_BYTES || bytes.len() == T300_BYTES
}

fn swap_words(buf: &mut [u8]) {
    for w in buf.chunks_exact_mut(2) {
        w.swap(0, 1);
    }
}

/// Parse a Trident pack image into an in-memory [`Disk`] (`family = Trident`),
/// normalizing the little-endian pack words to big-endian.
///
/// Real Trident packs are written with a physical **sector interleave**, so a
/// sector's position in the file is *not* its virtual disk address. Each sector
/// carries its true disk address in its 2-word **header**, so we place every
/// sector at the VDA decoded from its own header ([`Geometry::trident_vda_from_dh`])
/// rather than by file order. (Images our own writer produces have header ==
/// file position, so this de-skews to the identity.) Sectors whose header
/// doesn't resolve to an in-range VDA (unformatted/spare) are dropped; gaps stay
/// zeroed.
pub fn read(bytes: &[u8]) -> Result<Disk, FilesystemError> {
    let (model, sectors_n) = match bytes.len() {
        T80_BYTES => (80u16, T80_SECTORS),
        T300_BYTES => (300u16, T300_SECTORS),
        n => {
            return Err(FilesystemError::Parse(format!(
            "Trident pack: file is {n} bytes, expected {T80_BYTES} (T-80) or {T300_BYTES} (T-300)"
        )))
        }
    };
    let geom = geometry(model);
    let header_off = DUMMY_WORDS * 2; // 2 — skip the dummy word
    let label_off = (DUMMY_WORDS + HEADER_WORDS) * 2; // 6
    let data_off = label_off + LABEL_BYTES; // 26
    let mut sectors = vec![Sector::zeroed(LABEL_BYTES, DATA_BYTES); sectors_n];
    for i in 0..sectors_n {
        let rec = i * SECTOR_BYTES;
        // The 2-word header (little-endian) is the sector's own disk address.
        let h0 = (bytes[rec + header_off] as u16) | ((bytes[rec + header_off + 1] as u16) << 8);
        let h1 = (bytes[rec + header_off + 2] as u16) | ((bytes[rec + header_off + 3] as u16) << 8);
        let vda = geom.trident_vda_from_dh(h0, h1);
        if vda >= sectors_n {
            continue;
        }
        let mut label = bytes[rec + label_off..rec + label_off + LABEL_BYTES].to_vec();
        let mut data = bytes[rec + data_off..rec + data_off + DATA_BYTES].to_vec();
        swap_words(&mut label);
        swap_words(&mut data);
        sectors[vda] = Sector { label, data };
    }
    Ok(Disk {
        geometry: geom,
        sectors,
    })
}

/// Serialize a [`Disk`] (single-drive Trident pack) to a pack image. The 2-word
/// hardware header is reconstructed from each sector's VDA (the disk header);
/// words are emitted little-endian.
pub fn write(disk: &Disk) -> Result<Vec<u8>, FilesystemError> {
    let g = &disk.geometry;
    if g.family != FsFamily::Trident
        || g.label_bytes as usize != LABEL_BYTES
        || g.data_bytes as usize != DATA_BYTES
        || (g.total_sectors() != T80_SECTORS && g.total_sectors() != T300_SECTORS)
    {
        return Err(FilesystemError::Unsupported(
            "Trident pack export requires a single-drive T-80 or T-300 geometry (9 sectors/track, 20-byte labels, 2048-byte data)".into(),
        ));
    }
    let label_off = (DUMMY_WORDS + HEADER_WORDS) * 2;
    let data_off = label_off + LABEL_BYTES;
    let mut out = vec![0u8; disk.sectors.len() * SECTOR_BYTES];
    for (vda, s) in disk.sectors.iter().enumerate() {
        let rec = vda * SECTOR_BYTES;
        // header (words 1-2 after the dummy) = the 2-word disk header (DH).
        let [hw0, hw1] = g.trident_dh_from_vda(vda);
        super::put_be16(&mut out, rec + 2, hw0);
        super::put_be16(&mut out, rec + 4, hw1);
        out[rec + label_off..rec + label_off + LABEL_BYTES].copy_from_slice(&s.label);
        out[rec + data_off..rec + data_off + DATA_BYTES].copy_from_slice(&s.data);
        // Emit little-endian: swap every word of the whole record.
        swap_words(&mut out[rec..rec + SECTOR_BYTES]);
    }
    Ok(out)
}

fn diablo31_geometry() -> Geometry {
    Geometry {
        family: FsFamily::Diablo,
        disk_model: 31,
        n_disks: 1,
        n_cylinders: DIABLO31_CYLINDERS,
        n_heads: DIABLO31_HEADS,
        n_sectors: DIABLO31_SECTORS_PT,
        label_bytes: DIABLO_LABEL_BYTES as u16,
        data_bytes: DIABLO_DATA_BYTES as u16,
    }
}

/// Could this be a ContrAlto2 Diablo `.dsk`? (Same size as a Salto `.dsk`; the
/// caller should try Salto first and fall back to [`read_diablo`].)
pub fn is_diablo_image(bytes: &[u8]) -> bool {
    bytes.len() == DIABLO_IMAGE_BYTES
}

/// Read a ContrAlto2 / Bitsavers **Diablo** pack into a [`Disk`]. Per sector:
/// `[dummy 1w][header 2w][label 8w][data 256w]`, little-endian. The header's
/// second word is the packed Diablo disk address; each sector is placed at the
/// VDA from [`Geometry::vda_from_da`] (handling the physical sector interleave),
/// and the words are normalized to big-endian. Diablo-31 only.
pub fn read_diablo(bytes: &[u8]) -> Result<Disk, FilesystemError> {
    if bytes.len() != DIABLO_IMAGE_BYTES {
        return Err(FilesystemError::Parse(format!(
            "ContrAlto Diablo pack: file is {} bytes, expected {DIABLO_IMAGE_BYTES}",
            bytes.len()
        )));
    }
    let geom = diablo31_geometry();
    let label_off = 6; // 2 dummy + 4 header
    let data_off = label_off + DIABLO_LABEL_BYTES; // 22
    let mut sectors = vec![Sector::zeroed(DIABLO_LABEL_BYTES, DIABLO_DATA_BYTES); DIABLO31_SECTORS];
    for i in 0..DIABLO31_SECTORS {
        let rec = i * DIABLO_SECTOR_BYTES;
        // header word 1 (little-endian) = the packed Diablo disk address.
        let da = (bytes[rec + 4] as u16) | ((bytes[rec + 5] as u16) << 8);
        let vda = geom.vda_from_da(da);
        if vda >= DIABLO31_SECTORS {
            continue;
        }
        let mut label = bytes[rec + label_off..rec + label_off + DIABLO_LABEL_BYTES].to_vec();
        let mut data = bytes[rec + data_off..rec + data_off + DIABLO_DATA_BYTES].to_vec();
        swap_words(&mut label);
        swap_words(&mut data);
        sectors[vda] = Sector { label, data };
    }
    Ok(Disk {
        geometry: geom,
        sectors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::alto::{put_be16, LabelCodec};

    #[test]
    fn label_codec_trident_round_trips() {
        // Encode chain links + fields via the Trident codec, decode them back.
        let g = geometry(80);
        let codec = LabelCodec::Trident;
        let label = codec.make_label(&g, Some(100), Some(50), 1234, 7, 1, 0xABCD, 0x1234);
        assert_eq!(codec.next(&g, &label), Some(100));
        assert_eq!(codec.prev(&g, &label), Some(50));
        assert_eq!(codec.num_chars(&label), 1234);
        assert_eq!(codec.page_number(&label), 7);
        assert_eq!(codec.file_id(&label), (1, 0xABCD, 0x1234));
        // EOF links decode to None.
        let eof = codec.make_label(&g, None, None, 0, 0, 0xffff, 0xffff, 0xffff);
        assert_eq!(codec.next(&g, &eof), None);
        assert_eq!(codec.prev(&g, &eof), None);
    }

    #[test]
    fn dh_vda_round_trips() {
        let g = geometry(300);
        for &vda in &[0usize, 1, 8, 9, 100, 5000, T300_SECTORS - 1] {
            let [w0, w1] = g.trident_dh_from_vda(vda);
            assert_eq!(g.trident_vda_from_dh(w0, w1), vda, "vda {vda}");
        }
    }

    #[test]
    fn diablo_pack_reads_back_a_blank_volume() {
        use crate::fs::alto::bfs::Bfs;
        use crate::fs::alto::write;
        // Format a blank Diablo-31 volume, serialize it into the ContrAlto Diablo
        // pack framing (dummy + DA header + label + data, little-endian, placed by
        // disk address), and confirm read_diablo reconstructs a walkable BFS.
        let disk = write::create_blank(diablo31_geometry()).expect("blank");
        let mut bytes = vec![0u8; DIABLO_IMAGE_BYTES];
        for (vda, s) in disk.sectors.iter().enumerate() {
            let rec = vda * DIABLO_SECTOR_BYTES;
            // dummy word 0; header word 0 = 0, word 1 = packed Diablo DA.
            put_be16(&mut bytes, rec + 4, disk.geometry.da_from_vda(vda));
            bytes[rec + 6..rec + 6 + DIABLO_LABEL_BYTES].copy_from_slice(&s.label);
            bytes[rec + 22..rec + 22 + DIABLO_DATA_BYTES].copy_from_slice(&s.data);
            swap_words(&mut bytes[rec..rec + DIABLO_SECTOR_BYTES]); // -> little-endian
        }
        assert!(is_diablo_image(&bytes));
        let back = read_diablo(&bytes).expect("read_diablo");
        assert_eq!(back.geometry.family, FsFamily::Diablo);
        let files = Bfs::new(&back).list_files().expect("list");
        assert!(files
            .iter()
            .any(|f| f.name.trim_end_matches('.').eq_ignore_ascii_case("SysDir")));
    }

    fn sample_pack(model: u16) -> Disk {
        let g = geometry(model);
        let total = g.total_sectors();
        let sectors = (0..total)
            .map(|i| {
                let mut s = Sector::zeroed(LABEL_BYTES, DATA_BYTES);
                put_be16(&mut s.label, 0, (i & 0xffff) as u16);
                put_be16(&mut s.data, 0, (0x4000u16).wrapping_add(i as u16));
                s
            })
            .collect();
        Disk {
            geometry: g,
            sectors,
        }
    }

    #[test]
    fn pack_round_trips_t80() {
        let disk = sample_pack(80);
        let bytes = write(&disk).expect("write");
        assert_eq!(bytes.len(), T80_BYTES);
        assert!(is_trident_image(&bytes));

        let back = read(&bytes).expect("read");
        assert_eq!(back.geometry.disk_model, 80);
        assert_eq!(back.geometry.total_sectors(), T80_SECTORS);
        // Spot-check a scatter of sectors (full compare is large but cheap).
        for &vda in &[0usize, 1, 9, 1000, T80_SECTORS - 1] {
            assert_eq!(
                back.sectors[vda].label, disk.sectors[vda].label,
                "label {vda}"
            );
            assert_eq!(back.sectors[vda].data, disk.sectors[vda].data, "data {vda}");
        }
    }

    #[test]
    fn rejects_wrong_size() {
        let mut bytes = write(&sample_pack(80)).expect("write");
        bytes.truncate(bytes.len() - SECTOR_BYTES);
        assert!(!is_trident_image(&bytes));
        assert!(read(&bytes).is_err());
    }
}
