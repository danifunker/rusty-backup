//! Boot areas that ride alongside an optical disc's main filesystem.
//!
//! Several discs in the wild carry a second, non-ISO structure in the space ISO 9660
//! leaves free before sector 16:
//!
//! - **HP LIF** (HP-UX install DVDs, NetBSD/hppa CDs): a LIF volume at byte 0 whose
//!   directory names the PA-RISC boot programs (`ISL`, `HPUX`, `CDBOOT`, ...). Read
//!   with [`crate::fs::lif`].
//! - **PReP boot partitions** (AIX CHRP diagnostics CDs, OS/2 Warp for PowerPC): an
//!   MBR at byte 0 whose type-`0x41` entry holds the PowerPC boot image. It has no
//!   filesystem, so it is offered as one file, the partition's bytes.
//!
//! [`detect`] finds them and [`open`] presents each as an `opticaldiscs` filesystem,
//! so the Optical tab, Commander and `rb-cli optical` list them as extra
//! selectable filesystems after the disc's own. Only plain 2048-byte-sector ISO
//! images are probed: the offsets are byte offsets into the image file.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use opticaldiscs::browse::entry::FileEntry;
use opticaldiscs::browse::filesystem::{Filesystem, FilesystemError};

/// Which structure a boot area is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootAreaKind {
    Lif,
    Prep,
}

/// One file in a boot area: a named byte range of the image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootFile {
    pub name: String,
    pub offset: u64,
    pub len: u64,
    pub modified: Option<String>,
}

/// A boot area found on a disc image, with the files it exposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootArea {
    pub kind: BootAreaKind,
    /// Picker label, e.g. `LIF boot area (ISL10)`.
    pub label: String,
    pub files: Vec<BootFile>,
}

/// Every boot area on the plain ISO image at `path`; empty for other formats or none found.
pub fn detect(path: &Path) -> Vec<BootArea> {
    let is_plain_iso = matches!(
        opticaldiscs::formats::DiscFormat::from_path(path),
        Some(opticaldiscs::formats::DiscFormat::Iso)
    );
    if !is_plain_iso {
        return Vec::new();
    }
    let Ok(mut f) = File::open(path) else {
        return Vec::new();
    };
    detect_in(&mut f)
}

/// [`detect`] over an already-open image.
pub fn detect_in<R: Read + Seek>(r: &mut R) -> Vec<BootArea> {
    let mut areas = Vec::new();
    if let Some(vol) = crate::fs::lif::parse_lif(r, 0) {
        let label = if vol.label.is_empty() {
            "LIF boot area".to_string()
        } else {
            format!("LIF boot area ({})", vol.label)
        };
        areas.push(BootArea {
            kind: BootAreaKind::Lif,
            label,
            files: vol
                .files
                .into_iter()
                .map(|f| BootFile {
                    name: f.name,
                    offset: f.start_byte,
                    len: f.len_bytes,
                    modified: f.modified,
                })
                .collect(),
        });
    }
    let mut mbr = [0u8; 512];
    if r.seek(SeekFrom::Start(0)).is_ok()
        && r.read_exact(&mut mbr).is_ok()
        && mbr[510..512] == [0x55, 0xAA]
    {
        let mut files: Vec<BootFile> = Vec::new();
        for slot in mbr[0x1BE..0x1FE].chunks_exact(16) {
            let start = u32::from_le_bytes([slot[8], slot[9], slot[10], slot[11]]) as u64 * 512;
            let len = u32::from_le_bytes([slot[12], slot[13], slot[14], slot[15]]) as u64 * 512;
            // AIX writes the same PReP entry twice; one file per distinct extent.
            if slot[4] == 0x41 && len > 0 && !files.iter().any(|f| f.offset == start) {
                files.push(BootFile {
                    name: format!("prep-boot-{}.img", files.len() + 1),
                    offset: start,
                    len,
                    modified: None,
                });
            }
        }
        if !files.is_empty() {
            areas.push(BootArea {
                kind: BootAreaKind::Prep,
                label: "PReP boot partition".to_string(),
                files,
            });
        }
    }
    areas
}

/// Open `area` of the image at `path` as a flat, read-only `opticaldiscs` filesystem.
pub fn open(path: &Path, area: &BootArea) -> Result<Box<dyn Filesystem>, FilesystemError> {
    Ok(Box::new(BootAreaFs {
        file: File::open(path)?,
        area: area.clone(),
    }))
}

struct BootAreaFs {
    file: File,
    area: BootArea,
}

impl BootAreaFs {
    fn file_for(&self, entry: &FileEntry) -> Result<&BootFile, FilesystemError> {
        self.area
            .files
            .get(entry.location as usize)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))
    }
}

impl Filesystem for BootAreaFs {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::root(0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        Ok(self
            .area
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| {
                FileEntry::new_file(f.name.clone(), format!("/{}", f.name), f.len, i as u64)
            })
            .collect())
    }

    fn read_file(&mut self, entry: &FileEntry) -> Result<Vec<u8>, FilesystemError> {
        let len = self.file_for(entry)?.len as usize;
        self.read_file_range(entry, 0, len)
    }

    fn read_file_range(
        &mut self,
        entry: &FileEntry,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let f = self.file_for(entry)?.clone();
        let start = offset.min(f.len);
        let n = (length as u64).min(f.len - start) as usize;
        let mut out = vec![0u8; n];
        self.file.seek(SeekFrom::Start(f.offset + start))?;
        self.file.read_exact(&mut out)?;
        Ok(out)
    }

    fn read_resource_fork(
        &mut self,
        _entry: &FileEntry,
    ) -> Result<Option<Vec<u8>>, FilesystemError> {
        Ok(None)
    }

    fn read_resource_fork_range(
        &mut self,
        _entry: &FileEntry,
        _offset: u64,
        _length: usize,
    ) -> Result<Option<Vec<u8>>, FilesystemError> {
        Ok(None)
    }

    fn volume_name(&self) -> Option<&str> {
        Some(self.area.label.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn prep_entries_dedupe_and_read_back() {
        let mut img = vec![0u8; 8 * 512];
        img[510] = 0x55;
        img[511] = 0xAA;
        for slot in 0..2 {
            let e = &mut img[0x1BE + slot * 16..0x1BE + slot * 16 + 16];
            e[4] = 0x41;
            e[8..12].copy_from_slice(&2u32.to_le_bytes());
            e[12..16].copy_from_slice(&3u32.to_le_bytes());
        }
        img[2 * 512..5 * 512].fill(0x7E);
        let areas = detect_in(&mut Cursor::new(&img));
        assert_eq!(areas.len(), 1);
        assert_eq!(areas[0].kind, BootAreaKind::Prep);
        assert_eq!(areas[0].files.len(), 1, "duplicate PReP entry collapses");
        assert_eq!(
            (areas[0].files[0].offset, areas[0].files[0].len),
            (1024, 1536)
        );
    }

    #[test]
    fn plain_data_has_no_boot_area() {
        assert!(detect_in(&mut Cursor::new(vec![0u8; 4096])).is_empty());
    }
}
