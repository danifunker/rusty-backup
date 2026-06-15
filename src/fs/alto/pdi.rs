//! PARC Disk Image (PDI) container — the flat, self-describing, label-inclusive
//! image specified in `~/docs/PARC_DISK_IMAGE_SPEC.md`.
//!
//! Layout: a fixed 512-byte header (`PARCDISK` magic + geometry + field sizes +
//! flags) followed by one record per sector, in ascending virtual-disk-address
//! order, each record `[header?][label][data]`. The central invariant is
//!
//! ```text
//! recordOffset(VDA) = 512 + VDA * recordBytes
//! ```
//!
//! This implementation writes the canonical form (`headerBytes = 0`); the
//! per-sector hardware header is derivable from the record position and is not
//! stored.

use super::super::filesystem::FilesystemError;
use super::{be16, put_be16, Disk, FsFamily, Geometry, Sector};

pub const MAGIC: &[u8; 8] = b"PARCDISK";
pub const HEADER_SIZE: usize = 512;
pub const FORMAT_VERSION: u16 = 1;

const FLAG_LABELS_PRESENT: u16 = 0x0001;
const FLAG_HEADERS_PRESENT: u16 = 0x0002;

/// Parsed PDI header.
#[derive(Debug, Clone)]
pub struct PdiHeader {
    pub geometry: Geometry,
    pub header_bytes: u16,
    pub flags: u16,
    pub fs_number: u16,
}

impl PdiHeader {
    pub fn record_bytes(&self) -> usize {
        self.header_bytes as usize
            + self.geometry.label_bytes as usize
            + self.geometry.data_bytes as usize
    }
}

/// Serialize a [`Disk`] to canonical PDI bytes (`headerBytes = 0`).
pub fn write(disk: &Disk) -> Vec<u8> {
    let g = &disk.geometry;
    let label_bytes = g.label_bytes as usize;
    let data_bytes = g.data_bytes as usize;
    let record = label_bytes + data_bytes;
    let total = g.total_sectors();

    let mut out = vec![0u8; HEADER_SIZE + total * record];
    out[0..8].copy_from_slice(MAGIC);
    put_be16(&mut out, 8, FORMAT_VERSION);
    put_be16(&mut out, 10, g.family.code());
    put_be16(&mut out, 12, g.disk_model);
    put_be16(&mut out, 14, g.n_disks);
    put_be16(&mut out, 16, g.n_cylinders);
    put_be16(&mut out, 18, g.n_heads);
    put_be16(&mut out, 20, g.n_sectors);
    put_be16(&mut out, 22, 0); // headerBytes (canonical: none stored)
    put_be16(&mut out, 24, g.label_bytes);
    put_be16(&mut out, 26, g.data_bytes);
    put_be16(&mut out, 28, FLAG_LABELS_PRESENT);
    put_be16(&mut out, 30, 0); // fsNumber
                               // bytes 32.. (volumeLabel, reserved) left zero

    for (vda, sector) in disk.sectors.iter().enumerate() {
        let off = HEADER_SIZE + vda * record;
        out[off..off + label_bytes].copy_from_slice(&sector.label);
        out[off + label_bytes..off + record].copy_from_slice(&sector.data);
    }
    out
}

/// Parse and validate a PDI header (the first 512 bytes).
pub fn read_header(bytes: &[u8]) -> Result<PdiHeader, FilesystemError> {
    if bytes.len() < HEADER_SIZE {
        return Err(FilesystemError::Parse(
            "PDI: file smaller than header".into(),
        ));
    }
    if &bytes[0..8] != MAGIC {
        return Err(FilesystemError::Parse("PDI: bad magic".into()));
    }
    let version = be16(bytes, 8);
    if version != FORMAT_VERSION {
        return Err(FilesystemError::Unsupported(format!(
            "PDI: unsupported formatVersion {version}"
        )));
    }
    let family = FsFamily::from_code(be16(bytes, 10))
        .ok_or_else(|| FilesystemError::Parse("PDI: unknown fsFamily".into()))?;
    let header_bytes = be16(bytes, 22);
    let flags = be16(bytes, 28);
    if flags & FLAG_LABELS_PRESENT == 0 {
        return Err(FilesystemError::Unsupported(
            "PDI: labels-present flag is required in v1".into(),
        ));
    }
    if (header_bytes == 4) != (flags & FLAG_HEADERS_PRESENT != 0) {
        return Err(FilesystemError::Parse(
            "PDI: headerBytes and headers-present flag disagree".into(),
        ));
    }
    if header_bytes != 0 && header_bytes != 4 {
        return Err(FilesystemError::Parse(format!(
            "PDI: headerBytes must be 0 or 4, got {header_bytes}"
        )));
    }

    let geometry = Geometry {
        family,
        disk_model: be16(bytes, 12),
        n_disks: be16(bytes, 14),
        n_cylinders: be16(bytes, 16),
        n_heads: be16(bytes, 18),
        n_sectors: be16(bytes, 20),
        label_bytes: be16(bytes, 24),
        data_bytes: be16(bytes, 26),
    };
    if geometry.label_bytes == 0 || geometry.data_bytes == 0 || geometry.total_sectors() == 0 {
        return Err(FilesystemError::Parse("PDI: degenerate geometry".into()));
    }

    Ok(PdiHeader {
        geometry,
        header_bytes,
        flags,
        fs_number: be16(bytes, 30),
    })
}

/// Parse a complete PDI image into an in-memory [`Disk`].
pub fn read(bytes: &[u8]) -> Result<Disk, FilesystemError> {
    let header = read_header(bytes)?;
    let g = header.geometry.clone();
    let total = g.total_sectors();
    let record = header.record_bytes();
    let expected = HEADER_SIZE + total * record;
    if bytes.len() != expected {
        return Err(FilesystemError::Parse(format!(
            "PDI: file is {} bytes, expected {expected} (512 + {total} * {record})",
            bytes.len()
        )));
    }

    let hb = header.header_bytes as usize;
    let label_bytes = g.label_bytes as usize;
    let data_bytes = g.data_bytes as usize;

    let mut sectors = Vec::with_capacity(total);
    for vda in 0..total {
        let off = HEADER_SIZE + vda * record + hb;
        let label = bytes[off..off + label_bytes].to_vec();
        let data = bytes[off + label_bytes..off + label_bytes + data_bytes].to_vec();
        sectors.push(Sector { label, data });
    }

    Ok(Disk {
        geometry: g,
        sectors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_disk() -> Disk {
        let geometry = Geometry {
            family: FsFamily::Diablo,
            disk_model: 31,
            n_disks: 1,
            n_cylinders: 2,
            n_heads: 2,
            n_sectors: 3,
            label_bytes: 16,
            data_bytes: 512,
        };
        let total = geometry.total_sectors();
        let sectors = (0..total)
            .map(|i| {
                let mut s = Sector::zeroed(16, 512);
                put_be16(&mut s.label, 0, i as u16);
                put_be16(&mut s.data, 0, (0x1000 + i) as u16);
                s
            })
            .collect();
        Disk { geometry, sectors }
    }

    #[test]
    fn round_trip_preserves_everything() {
        let disk = sample_disk();
        let bytes = write(&disk);
        assert_eq!(&bytes[0..8], MAGIC);
        assert_eq!(bytes.len(), HEADER_SIZE + 12 * (16 + 512));

        let back = read(&bytes).expect("read");
        assert_eq!(back.geometry.total_sectors(), disk.geometry.total_sectors());
        assert_eq!(back.geometry.n_cylinders, 2);
        assert_eq!(back.geometry.disk_model, 31);
        for vda in 0..disk.geometry.total_sectors() {
            assert_eq!(back.sectors[vda].label, disk.sectors[vda].label);
            assert_eq!(back.sectors[vda].data, disk.sectors[vda].data);
        }
    }

    #[test]
    fn record_offset_invariant() {
        let disk = sample_disk();
        let bytes = write(&disk);
        let record = 16 + 512;
        // sector 5's label word 0 must equal 5 at offset 512 + 5*record
        let off = HEADER_SIZE + 5 * record;
        assert_eq!(be16(&bytes, off), 5);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = write(&sample_disk());
        bytes[0] = b'X';
        assert!(read(&bytes).is_err());
    }

    #[test]
    fn rejects_truncated() {
        let mut bytes = write(&sample_disk());
        bytes.truncate(bytes.len() - 1);
        assert!(read(&bytes).is_err());
    }
}
