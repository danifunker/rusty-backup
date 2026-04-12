//! Apple II 5.25" floppy sector interleave conversion.
//!
//! Apple II 5.25" floppy disk images come in two sector orderings:
//!
//! - **ProDOS order** (`.po`): sectors arranged for ProDOS block addressing.
//!   This is the native layout used by ProDOS and is what Rusty Backup expects.
//! - **DOS order** (`.do`, sometimes `.dsk`): sectors arranged for Apple DOS 3.3
//!   block addressing.  The same physical sectors appear in a different logical
//!   sequence.
//!
//! These are standard Apple II community names for the two sector interleave
//! schemes on 5.25" floppies.  The ordering is independent of which filesystem
//! is on the disk — a DOS-order image could contain a ProDOS filesystem and
//! vice versa.  A `.dsk` file can use either ordering; we probe the content to
//! disambiguate.
//!
//! Both orderings store 35 tracks × 16 sectors × 256 bytes = 143,360 bytes.
//! This module converts DOS-order images into ProDOS order so that any
//! filesystem reader (ProDOS, DOS 3.3, etc.) can work with the data.

use std::io::{self, Read, Seek, SeekFrom};

/// Size of a 5.25" floppy: 35 tracks × 16 sectors × 256 bytes.
pub const FLOPPY_140K: u64 = 143_360;

/// Number of tracks on a standard 5.25" floppy.
const NUM_TRACKS: usize = 35;

/// Sectors per track.
const SECTORS_PER_TRACK: usize = 16;

/// Bytes per physical sector.
const SECTOR_SIZE: usize = 256;

/// Maps a physical sector index (in DOS 3.3 order) to the corresponding
/// ProDOS physical sector index.  When reading a `.do` file, the bytes at
/// `track * 16 * 256 + dos_sector * 256` correspond to physical sector
/// `dos_sector`.  To produce ProDOS-order output, we need to rearrange
/// sectors within each track.
///
/// ProDOS block N is composed of two physical sectors from the same track:
///   - Low half (bytes 0..256):   sector `PRODOS_BLOCK_INTERLEAVE[N*2 % 16]`
///   - High half (bytes 256..512): sector `PRODOS_BLOCK_INTERLEAVE[N*2+1 % 16]`
///
/// But it's simpler to express as: for each of the 16 sectors in a track,
/// DOS 3.3 sector S maps to ProDOS sector INTERLEAVE[S].
///
/// The interleave table maps DOS 3.3 sector number → ProDOS sector number.
const DOS_TO_PRODOS_SECTOR: [usize; 16] = [0, 13, 11, 9, 7, 5, 3, 1, 14, 12, 10, 8, 6, 4, 2, 15];

/// Check if a 512-byte buffer looks like a ProDOS volume directory key block.
///
/// This checks the same magic as `validate_volume_key_block` in the ProDOS
/// filesystem module: prev_block == 0, storage_type nibble == 0xF,
/// name_length >= 1, entry_length == 39, entries_per_block == 13.
pub fn has_prodos_volume_header(buf: &[u8]) -> bool {
    buf.len() >= 43
        && buf[0] == 0
        && buf[1] == 0 // prev_block == 0
        && (buf[4] >> 4) == 0xF // storage_type nibble = volume dir header
        && (buf[4] & 0xF) >= 1 // name_length valid
        && buf[35] == 39 // entry_length
        && buf[36] == 13 // entries_per_block
}

/// A `Read + Seek` adapter that presents a DOS 3.3-order 140K floppy image
/// as ProDOS-order data.
///
/// Since 140K fits easily in memory, we decode the entire image at open time
/// into a flat `Vec<u8>` in ProDOS block order.
pub struct DosOrderReader {
    /// Decoded data in ProDOS block order (280 blocks × 512 bytes = 143,360 bytes).
    data: Vec<u8>,
    position: u64,
}

impl DosOrderReader {
    /// Create a new reader by interleaving the given DOS 3.3-order data.
    ///
    /// `raw` must be exactly 143,360 bytes.
    pub fn new(raw: Vec<u8>) -> io::Result<Self> {
        if raw.len() as u64 != FLOPPY_140K {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "DOS 3.3 image must be {} bytes, got {}",
                    FLOPPY_140K,
                    raw.len()
                ),
            ));
        }

        // Build a ProDOS-order image by remapping sectors.
        //
        // In the DOS 3.3-order file, data is laid out as:
        //   track 0 sector 0 (256 bytes), track 0 sector 1, ..., track 0 sector 15,
        //   track 1 sector 0, ...
        //
        // A ProDOS-order file has the same layout but sector numbering follows
        // the ProDOS physical interleave.  To convert, we rearrange sectors
        // within each track: ProDOS sector P gets the data from DOS 3.3 sector
        // D where DOS_TO_PRODOS_SECTOR[D] == P.
        //
        // Build inverse map: prodos_sector → dos_sector
        let mut prodos_to_dos = [0usize; 16];
        for (dos_sec, &prodos_sec) in DOS_TO_PRODOS_SECTOR.iter().enumerate() {
            prodos_to_dos[prodos_sec] = dos_sec;
        }

        let mut data = vec![0u8; FLOPPY_140K as usize];
        for track in 0..NUM_TRACKS {
            for prodos_sec in 0..SECTORS_PER_TRACK {
                let dos_sec = prodos_to_dos[prodos_sec];
                let src_offset = (track * SECTORS_PER_TRACK + dos_sec) * SECTOR_SIZE;
                let dst_offset = (track * SECTORS_PER_TRACK + prodos_sec) * SECTOR_SIZE;
                data[dst_offset..dst_offset + SECTOR_SIZE]
                    .copy_from_slice(&raw[src_offset..src_offset + SECTOR_SIZE]);
            }
        }

        Ok(Self { data, position: 0 })
    }

    /// Total size in bytes.
    pub fn len(&self) -> u64 {
        self.data.len() as u64
    }
}

impl Read for DosOrderReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.data.len() as u64 - self.position;
        if remaining == 0 {
            return Ok(0);
        }
        let to_read = (buf.len() as u64).min(remaining) as usize;
        let pos = self.position as usize;
        buf[..to_read].copy_from_slice(&self.data[pos..pos + to_read]);
        self.position += to_read as u64;
        Ok(to_read)
    }
}

impl Seek for DosOrderReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::Current(delta) => self.position as i64 + delta,
            SeekFrom::End(delta) => self.data.len() as i64 + delta,
        };
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }
        self.position = (new_pos as u64).min(self.data.len() as u64);
        Ok(self.position)
    }
}

/// Try to determine whether a 140K raw image is in ProDOS order or DOS 3.3
/// order by checking for a ProDOS volume directory header.
///
/// ProDOS block 2 is the volume directory key block.  In a ProDOS-order image,
/// this is at byte offset 1024.  In a DOS 3.3-order image, the same logical
/// block maps to different physical sectors, so the header would appear at a
/// different offset.
///
/// Returns `true` if the image appears to be DOS 3.3-order (i.e., ProDOS magic
/// is NOT found at offset 1024 in the raw file, but IS found after
/// interleaving).
pub fn is_dos_order(raw: &[u8]) -> bool {
    if raw.len() as u64 != FLOPPY_140K {
        return false;
    }

    // Check if ProDOS magic is already at block 2 in raw order
    if has_prodos_volume_header(&raw[1024..]) {
        // Already ProDOS order
        return false;
    }

    // Try interleaving and check again
    // Block 2 = track 0, ProDOS sectors 4 and 5
    // In DOS 3.3 order, we need to find which DOS 3.3 sectors map to ProDOS sectors 4 and 5
    let mut prodos_to_dos = [0usize; 16];
    for (dos_sec, &prodos_sec) in DOS_TO_PRODOS_SECTOR.iter().enumerate() {
        prodos_to_dos[prodos_sec] = dos_sec;
    }

    // ProDOS block 2 = physical sectors 4 and 5 (in ProDOS numbering)
    let dos_sec_lo = prodos_to_dos[4];
    let dos_sec_hi = prodos_to_dos[5];

    let mut block2 = [0u8; 512];
    let lo_offset = dos_sec_lo * SECTOR_SIZE;
    let hi_offset = dos_sec_hi * SECTOR_SIZE;
    block2[..256].copy_from_slice(&raw[lo_offset..lo_offset + 256]);
    block2[256..].copy_from_slice(&raw[hi_offset..hi_offset + 256]);

    has_prodos_volume_header(&block2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interleave_table_is_permutation() {
        let mut seen = [false; 16];
        for &s in &DOS_TO_PRODOS_SECTOR {
            assert!(s < 16);
            assert!(!seen[s], "duplicate in interleave table");
            seen[s] = true;
        }
    }

    #[test]
    fn test_dos33_reader_round_trip() {
        // Create a "DOS 3.3 order" image where each sector has a unique byte
        let mut raw = vec![0u8; FLOPPY_140K as usize];
        for track in 0..NUM_TRACKS {
            for sector in 0..SECTORS_PER_TRACK {
                let offset = (track * SECTORS_PER_TRACK + sector) * SECTOR_SIZE;
                let tag = ((track * SECTORS_PER_TRACK + sector) & 0xFF) as u8;
                raw[offset..offset + SECTOR_SIZE].fill(tag);
            }
        }

        let reader = DosOrderReader::new(raw.clone()).unwrap();
        assert_eq!(reader.len(), FLOPPY_140K);

        // After interleaving, track 0 ProDOS sector P should contain data from
        // DOS 3.3 sector prodos_to_dos[P]
        let mut prodos_to_dos = [0usize; 16];
        for (d, &p) in DOS_TO_PRODOS_SECTOR.iter().enumerate() {
            prodos_to_dos[p] = d;
        }

        for prodos_sec in 0..SECTORS_PER_TRACK {
            let dos_sec = prodos_to_dos[prodos_sec];
            let offset = prodos_sec * SECTOR_SIZE;
            let expected_tag = (dos_sec & 0xFF) as u8;
            assert_eq!(
                reader.data[offset], expected_tag,
                "track 0 ProDOS sector {prodos_sec} should map to DOS 3.3 sector {dos_sec}"
            );
        }
    }

    #[test]
    fn test_dos33_reader_read_seek() {
        let raw = vec![0xAA; FLOPPY_140K as usize];
        let mut reader = DosOrderReader::new(raw).unwrap();

        // Read a few bytes
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf).unwrap();
        // Seek to end
        let end = reader.seek(SeekFrom::End(0)).unwrap();
        assert_eq!(end, FLOPPY_140K);
        // Read at end should return 0
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
        // Seek back to start
        reader.seek(SeekFrom::Start(0)).unwrap();
        reader.read_exact(&mut buf).unwrap();
    }

    #[test]
    fn test_is_dos_order_prodos_image() {
        // Build a ProDOS-order image with valid volume header at block 2
        let mut raw = vec![0u8; FLOPPY_140K as usize];
        // Block 2 starts at byte 1024
        raw[1024] = 0; // prev_block low
        raw[1025] = 0; // prev_block high
        raw[1028] = 0xF5; // storage_type=0xF, name_length=5
        raw[1059] = 39; // entry_length
        raw[1060] = 13; // entries_per_block
        assert!(!is_dos_order(&raw), "ProDOS-order should return false");
    }

    #[test]
    fn test_wrong_size_rejected() {
        assert!(DosOrderReader::new(vec![0; 1000]).is_err());
        assert!(!is_dos_order(&[0; 1000]));
    }
}
