//! QDOS Microdrive (.mdv) — Sinclair QL cartridge image format.
//!
//! **Scope: detect-only scaffold.** This module is the engine-level
//! counterpart of the QXL.WIN HDD path in [`super::qdos`]: where that
//! parser walks a hard-disk volume, this one recognises the
//! distinctly-shaped microdrive cartridge container.
//!
//! ## On-disk layout
//!
//! A QL Microdrive cartridge holds 255 fixed-size sectors, each
//! **686 bytes** on tape (174,930 bytes total for a complete image —
//! the size that every MiSTer-distributed `.mdv` we have anchors for
//! happens to be). Each sector is a tightly-packed sequence:
//!
//! ```text
//!  0..0x0A  10-byte preamble (zeros)
//!  0x0A..0x0D  3-byte sync (0xFF 0xFF 0xFF)
//!  0x0D       1-byte sector header flag (typically 0x00)
//!  0x0E..0x18 10-byte cartridge name, ASCII, space-padded
//!  0x18..0x1C 4-byte cartridge random-ID / serial
//!  ... per-sector data block follows, then trailing gap
//! ```
//!
//! The cartridge name + serial in sector 0 is the strongest detection
//! signal: it's at a fixed offset, ASCII, and 174,930 bytes is an
//! exact constant.
//!
//! ## What this module ships
//!
//! 1. `detect_qdos_mdv_cartridge` recognises the structural shape.
//! 2. [`QdosMdvFilesystem::open`] surfaces the cartridge name as the
//!    volume label so `inspect` reports it.
//! 3. Directory walking + file reading return
//!    [`FilesystemError::Unsupported`] with a clear "park" message —
//!    real microdrive directory traversal needs the full
//!    QDOS Reference Manual ch.12 walker (parked in OPEN-WORK §7).
//!
//! Real-world cross-validation: the two anchored fixtures
//! `anchor_mister_GamesCart.mdv.zst` ("MD" cartridge) and
//! `anchor_mister_crazy.mdv.zst` ("Test" cartridge) both decompress to
//! 174,930 bytes and have the cart name visible at byte 0x0E — this
//! scaffold reads either cleanly.

use std::io::{Read, Seek, SeekFrom};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

/// A complete QDOS microdrive cartridge image is exactly 255 × 686 bytes.
pub const MDV_SECTOR_BYTES: usize = 686;
pub const MDV_SECTOR_COUNT: usize = 255;
pub const MDV_CART_BYTES: usize = MDV_SECTOR_BYTES * MDV_SECTOR_COUNT;

/// Byte offset of the cartridge name inside sector 0.
pub const CART_NAME_OFFSET: usize = 0x0E;
/// Cartridge name is 10 bytes ASCII (space-padded).
pub const CART_NAME_LEN: usize = 10;

/// True if `buf` (sector 0, ≥ 32 bytes) looks like a QDOS microdrive
/// cartridge header. The strongest signal: 10-byte preamble of zeros +
/// 3-byte 0xFF sync + ASCII cartridge name at byte 0x0E.
pub fn looks_like_mdv_sector_zero(buf: &[u8]) -> bool {
    if buf.len() < CART_NAME_OFFSET + CART_NAME_LEN {
        return false;
    }
    // Preamble: bytes 0..0x0A all zero (allow a single noise byte).
    let preamble_nonzero = buf[..0x0A].iter().filter(|b| **b != 0).count();
    if preamble_nonzero > 1 {
        return false;
    }
    // Sync: at least one of bytes 0x0A..0x0D is 0xFF.
    if !buf[0x0A..0x0D].contains(&0xFF) {
        return false;
    }
    // Cart name: bytes 0x0E..0x18 must be all printable ASCII or space.
    let name = &buf[CART_NAME_OFFSET..CART_NAME_OFFSET + CART_NAME_LEN];
    name.iter().all(|b| (0x20..=0x7E).contains(b) || *b == 0x00)
        && name.iter().any(|b| (0x21..=0x7E).contains(b))
}

/// Recognise a complete `.mdv` cartridge image given the file size and
/// sector 0's first ~32 bytes. Both checks must hold.
pub fn detect_qdos_mdv_cartridge(file_len: u64, sector_zero_head: &[u8]) -> bool {
    file_len as usize == MDV_CART_BYTES && looks_like_mdv_sector_zero(sector_zero_head)
}

/// Extract the trimmed cartridge name from sector 0. Returns `None` if
/// the name area is all whitespace / zeros.
pub fn read_cartridge_name(sector_zero: &[u8]) -> Option<String> {
    if sector_zero.len() < CART_NAME_OFFSET + CART_NAME_LEN {
        return None;
    }
    let name = &sector_zero[CART_NAME_OFFSET..CART_NAME_OFFSET + CART_NAME_LEN];
    let trimmed = String::from_utf8_lossy(name)
        .trim_end_matches(['\0', ' '])
        .to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

pub struct QdosMdvFilesystem<R: Read + Seek + Send> {
    // Kept for future read/write extension; trait methods that need
    // the reader will land alongside the directory-walk implementation.
    #[allow(dead_code)]
    reader: R,
    #[allow(dead_code)]
    partition_offset: u64,
    cart_name: Option<String>,
}

impl<R: Read + Seek + Send> QdosMdvFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        reader.seek(SeekFrom::Start(partition_offset))?;
        let mut sec0 = vec![0u8; MDV_SECTOR_BYTES];
        reader.read_exact(&mut sec0)?;
        if !looks_like_mdv_sector_zero(&sec0) {
            return Err(FilesystemError::InvalidData(
                "not a recognisable QDOS microdrive sector 0".into(),
            ));
        }
        let cart_name = read_cartridge_name(&sec0);
        Ok(Self {
            reader,
            partition_offset,
            cart_name,
        })
    }
}

impl<R: Read + Seek + Send> Filesystem for QdosMdvFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, _entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        // Real directory walk needs the QDOS Reference Manual ch.12
        // sector-chain implementation. Park with a clear message so
        // callers don't silently treat the volume as empty.
        Err(FilesystemError::Unsupported(
            "QDOS microdrive directory walk not implemented — \
             see src/fs/qdos_mdv.rs and OPEN-WORK §7"
                .into(),
        ))
    }

    fn read_file(
        &mut self,
        _entry: &FileEntry,
        _max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "QDOS microdrive file read not implemented".into(),
        ))
    }

    fn fs_type(&self) -> &str {
        "QDOS Microdrive"
    }

    fn volume_label(&self) -> Option<&str> {
        self.cart_name.as_deref()
    }

    fn total_size(&self) -> u64 {
        MDV_CART_BYTES as u64
    }

    fn used_size(&self) -> u64 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a minimal sector 0 that passes `looks_like_mdv_sector_zero`
    /// with the given cartridge name (must be ≤ 10 chars ASCII).
    fn synth_sector_zero(name: &str) -> Vec<u8> {
        let mut buf = vec![0u8; MDV_SECTOR_BYTES];
        buf[0x0A] = 0xFF;
        buf[0x0B] = 0xFF;
        buf[0x0C] = 0xFF;
        let name_bytes = name.as_bytes();
        for (i, b) in name_bytes.iter().take(CART_NAME_LEN).enumerate() {
            buf[CART_NAME_OFFSET + i] = *b;
        }
        for i in name_bytes.len()..CART_NAME_LEN {
            buf[CART_NAME_OFFSET + i] = b' ';
        }
        buf
    }

    fn synth_cart(name: &str) -> Vec<u8> {
        let mut cart = vec![0u8; MDV_CART_BYTES];
        cart[..MDV_SECTOR_BYTES].copy_from_slice(&synth_sector_zero(name));
        cart
    }

    #[test]
    fn looks_like_mdv_accepts_real_anchor_shape() {
        let sec = synth_sector_zero("Test");
        assert!(looks_like_mdv_sector_zero(&sec));
    }

    #[test]
    fn looks_like_mdv_rejects_random_bytes() {
        let buf = vec![0xCDu8; 64];
        assert!(!looks_like_mdv_sector_zero(&buf));
    }

    #[test]
    fn looks_like_mdv_rejects_zero_filled_sector_zero() {
        let buf = vec![0u8; 64];
        // Zeros have no sync FF and no cart name.
        assert!(!looks_like_mdv_sector_zero(&buf));
    }

    #[test]
    fn detect_qdos_mdv_cartridge_requires_exact_size_and_shape() {
        let cart = synth_cart("MD");
        assert!(detect_qdos_mdv_cartridge(
            cart.len() as u64,
            &cart[..MDV_SECTOR_BYTES]
        ));
        // Wrong size → no.
        assert!(!detect_qdos_mdv_cartridge(
            cart.len() as u64 - 1,
            &cart[..MDV_SECTOR_BYTES]
        ));
        // Right size, garbage sector 0 → no.
        let bogus = vec![0xCDu8; MDV_CART_BYTES];
        assert!(!detect_qdos_mdv_cartridge(
            bogus.len() as u64,
            &bogus[..MDV_SECTOR_BYTES]
        ));
    }

    #[test]
    fn read_cartridge_name_trims_trailing_spaces() {
        let sec = synth_sector_zero("MD");
        assert_eq!(read_cartridge_name(&sec).as_deref(), Some("MD"));
        // All-space name is treated as unset.
        let mut empty = synth_sector_zero("");
        for b in empty.iter_mut().skip(CART_NAME_OFFSET).take(CART_NAME_LEN) {
            *b = b' ';
        }
        assert!(read_cartridge_name(&empty).is_none());
    }

    #[test]
    fn open_surfaces_cart_name_and_unsupported_on_listing() {
        let cart = synth_cart("Test");
        let cur = Cursor::new(cart);
        let mut fs = QdosMdvFilesystem::open(cur, 0).unwrap();
        assert_eq!(fs.fs_type(), "QDOS Microdrive");
        assert_eq!(fs.volume_label(), Some("Test"));
        let root = fs.root().unwrap();
        let err = fs.list_directory(&root).unwrap_err();
        assert!(matches!(err, FilesystemError::Unsupported(_)));
    }
}
