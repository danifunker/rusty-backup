//! ANDOS — the Soviet BK0011M / Elektronika BK alternative disk OS
//! ("Альтернативная Дисковая ОС"). Used by the MiSTer **BK0011M** core
//! on both floppy `.dsk` and `.vhd` HDD images.
//!
//! **Scope: detect-only scaffold.** ANDOS is niche even by retro-
//! computing standards; the on-disk format documentation is sparse and
//! lives almost entirely in Russian-language emulator sources. This
//! module ships the minimum needed to:
//!
//! 1. Recognize an ANDOS volume (boot-block signature probe).
//! 2. Surface "ANDOS" through `fs_type()` so `inspect` reports the
//!    filesystem instead of "Unknown".
//! 3. Return an empty root directory (extract path returns
//!    `Unsupported`).
//!
//! Real read + write support is parked behind both better spec coverage
//! and a real test fixture from the BK-emulator community.
//!
//! Reference: MAME `bk*.cpp`, BKBTL emulator source. See OPEN-WORK §1
//! for the user-side verification path.

use std::io::{Read, Seek, SeekFrom};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

/// ANDOS boot-block contains the ASCII bytes `ANDOS` at one of two
/// candidate offsets across the floppy and HDD variants. Both are
/// probed.
const ANDOS_SIGNATURE: &[u8; 5] = b"ANDOS";

/// Probe a buffer for the ANDOS boot-block signature at any of the
/// likely byte offsets. Returns the offset of the first match.
pub fn detect_andos_signature(buf: &[u8]) -> Option<usize> {
    // BK BIOS leaves the signature in one of these well-known slots.
    for cand in &[0x1F8usize, 0x1B0, 0xE0, 0x00] {
        if buf.len() >= cand + 5 && &buf[*cand..*cand + 5] == ANDOS_SIGNATURE {
            return Some(*cand);
        }
    }
    None
}

pub struct AndosFilesystem<R: Read + Seek + Send> {
    // Kept for future read/write extension; trait methods that need the
    // reader will land alongside the directory-walk implementation.
    #[allow(dead_code)]
    reader: R,
    #[allow(dead_code)]
    partition_offset: u64,
    pub signature_offset: usize,
}

impl<R: Read + Seek + Send> AndosFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        reader.seek(SeekFrom::Start(partition_offset))?;
        let mut buf = [0u8; 512];
        reader.read_exact(&mut buf)?;
        let signature_offset = detect_andos_signature(&buf).ok_or_else(|| {
            FilesystemError::InvalidData("ANDOS signature not found at known offsets".into())
        })?;
        Ok(Self {
            reader,
            partition_offset,
            signature_offset,
        })
    }
}

impl<R: Read + Seek + Send> Filesystem for AndosFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, _entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        // Real directory walk is parked; surface a clear error so callers
        // don't silently treat the volume as empty.
        Err(FilesystemError::Unsupported(
            "ANDOS directory walk not implemented — \
             see src/fs/andos.rs for the scope note"
                .into(),
        ))
    }

    fn read_file(
        &mut self,
        _entry: &FileEntry,
        _max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "ANDOS file read not implemented".into(),
        ))
    }

    fn fs_type(&self) -> &str {
        "ANDOS (BK0011M)"
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn total_size(&self) -> u64 {
        0
    }

    fn used_size(&self) -> u64 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn detect_recognises_signature_at_known_offsets() {
        let mut buf = vec![0u8; 512];
        buf[0x1F8..0x1F8 + 5].copy_from_slice(ANDOS_SIGNATURE);
        assert_eq!(detect_andos_signature(&buf), Some(0x1F8));

        let mut buf2 = vec![0u8; 512];
        buf2[0..5].copy_from_slice(ANDOS_SIGNATURE);
        assert_eq!(detect_andos_signature(&buf2), Some(0));
    }

    #[test]
    fn detect_returns_none_on_random_bytes() {
        let buf = vec![0xCDu8; 512];
        assert!(detect_andos_signature(&buf).is_none());
    }

    #[test]
    fn open_detects_andos_and_surfaces_fs_type() {
        let mut buf = vec![0u8; 512];
        buf[0x1F8..0x1F8 + 5].copy_from_slice(ANDOS_SIGNATURE);
        let cur = Cursor::new(buf);
        let fs = AndosFilesystem::open(cur, 0).unwrap();
        assert_eq!(fs.fs_type(), "ANDOS (BK0011M)");
        assert_eq!(fs.signature_offset, 0x1F8);
    }

    #[test]
    fn list_directory_returns_unsupported_rather_than_empty() {
        let mut buf = vec![0u8; 512];
        buf[0x1F8..0x1F8 + 5].copy_from_slice(ANDOS_SIGNATURE);
        let cur = Cursor::new(buf);
        let mut fs = AndosFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let err = fs.list_directory(&root).unwrap_err();
        assert!(matches!(err, FilesystemError::Unsupported(_)));
    }
}
