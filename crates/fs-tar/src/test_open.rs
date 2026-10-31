//! Test-only stand-in for rusty-backup's dispatcher: opens the FAT / ext / UFS / HFS
//! volumes these tests build, by their superblock magic.

use std::io::{Read, Seek, SeekFrom, Write};

use disk_core::filesystem::{EditableFilesystem, Filesystem, FilesystemError};

enum Kind {
    Fat,
    Ext,
    Ufs,
    Hfs,
}

fn detect<R: Read + Seek>(r: &mut R, offset: u64) -> Kind {
    let mut buf = vec![0u8; 16 * 1024];
    r.seek(SeekFrom::Start(offset)).unwrap();
    let n = r.read(&mut buf).unwrap_or(0);
    buf.truncate(n);
    r.seek(SeekFrom::Start(0)).unwrap();
    let at = |o: usize, len: usize| buf.get(o..o + len);
    if at(1024, 2) == Some(b"BD") {
        Kind::Hfs
    } else if at(1024 + 56, 2) == Some(&[0x53, 0xEF][..]) {
        Kind::Ext
    } else if let Some(m) = at(8192 + 1372, 4) {
        if fs_unix::ufs::is_ufs_magic(m.try_into().unwrap(), true) {
            Kind::Ufs
        } else {
            Kind::Fat
        }
    } else {
        Kind::Fat
    }
}

/// Open a test image read-only; the type byte and length are ignored.
pub fn open<R: Read + Seek + Send + 'static>(
    mut r: R,
    offset: u64,
    _type_byte: u8,
    _len: Option<u64>,
) -> Result<Box<dyn Filesystem>, FilesystemError> {
    Ok(match detect(&mut r, offset) {
        Kind::Fat => Box::new(fs_pc::fat::FatFilesystem::open(r, offset)?),
        Kind::Ext => Box::new(fs_unix::ext::ExtFilesystem::open(r, offset)?),
        Kind::Ufs => Box::new(fs_unix::ufs::UfsFilesystem::open(r, offset)?),
        Kind::Hfs => Box::new(fs_mac::hfs::HfsFilesystem::open(r, offset)?),
    })
}

/// Open a test image for editing; the type byte is ignored.
pub fn open_editable<R: Read + Write + Seek + Send + 'static>(
    mut r: R,
    offset: u64,
    _type_byte: u8,
    len: Option<u64>,
) -> Result<Box<dyn EditableFilesystem>, FilesystemError> {
    Ok(match detect(&mut r, offset) {
        Kind::Fat => Box::new(fs_pc::fat::FatFilesystem::open(r, offset)?),
        Kind::Ext => Box::new(fs_unix::ext::ExtFilesystem::open(r, offset)?),
        Kind::Ufs => Box::new(fs_unix::ufs::UfsFilesystem::open(r, offset)?),
        Kind::Hfs => Box::new(fs_mac::hfs::HfsFilesystem::open_for_edit(r, offset, len)?),
    })
}
