//! AppImage: an ELF runtime stub with a filesystem appended to it.
//!
//! A type-2 AppImage is a small ELF executable whose payload — a SquashFS
//! holding the whole application — starts immediately after the ELF image ends
//! and runs to the end of the file. Running it mounts that SquashFS; there is
//! no archive step and no compression wrapper around it. So an AppImage is a
//! disk image with a prefix, and the useful thing to do with one is browse or
//! edit the filesystem in place.
//!
//! Because the payload is the **tail**, it may grow: a rebuilt SquashFS that
//! came out larger just makes the AppImage longer, and the ELF stub in front of
//! it is untouched (it locates its payload by the same arithmetic used here,
//! not by a stored length). That is what makes an AppImage editable where a
//! SquashFS inside an ISO 9660 is not.
//!
//! Type **1** AppImages wrap an ISO 9660 rather than a SquashFS and are not
//! handled here; they are recognised so the caller can say so instead of
//! reporting a corrupt ELF.

use std::io::{Read, Seek, SeekFrom};

/// `AI` plus the format generation, at `e_ident[8..11]` — the padding bytes an
/// ELF leaves free. This is how the AppImage tooling itself identifies one.
const APPIMAGE_MAGIC: [u8; 2] = [b'A', b'I'];

/// Which generation of AppImage a file is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppImageKind {
    /// Type 1: an ISO 9660 payload. Not supported for browsing here.
    Iso9660,
    /// Type 2: a SquashFS payload — the shape everything modern uses.
    SquashFs,
}

/// Read an AppImage's kind from its ELF identification bytes.
///
/// `None` for anything that is not an AppImage, including a plain ELF
/// executable (which has zeroes in the padding these bytes live in).
pub fn detect_kind<R: Read + Seek>(reader: &mut R) -> Option<AppImageKind> {
    let mut ident = [0u8; 16];
    reader.seek(SeekFrom::Start(0)).ok()?;
    reader.read_exact(&mut ident).ok()?;
    if ident[0..4] != [0x7f, b'E', b'L', b'F'] || ident[8..10] != APPIMAGE_MAGIC {
        return None;
    }
    match ident[10] {
        1 => Some(AppImageKind::Iso9660),
        2 => Some(AppImageKind::SquashFs),
        _ => None,
    }
}

/// Where the appended payload begins: the end of the ELF image.
///
/// This is `e_shoff + e_shentsize * e_shnum`, the same computation the AppImage
/// runtime performs to find its own payload — the section-header table is the
/// last thing in the file before the appended data. Handles both ELF32 and
/// ELF64, little- and big-endian, because AppImages exist for armhf and (rarely)
/// big-endian targets.
pub fn payload_offset<R: Read + Seek>(reader: &mut R) -> Option<u64> {
    let mut header = [0u8; 64];
    reader.seek(SeekFrom::Start(0)).ok()?;
    reader.read_exact(&mut header).ok()?;
    if header[0..4] != [0x7f, b'E', b'L', b'F'] {
        return None;
    }
    let is_64 = match header[4] {
        1 => false,
        2 => true,
        _ => return None,
    };
    let big_endian = match header[5] {
        1 => false,
        2 => true,
        _ => return None,
    };
    let u16_at = |off: usize| -> u64 {
        let b = [header[off], header[off + 1]];
        if big_endian {
            u16::from_be_bytes(b) as u64
        } else {
            u16::from_le_bytes(b) as u64
        }
    };
    let (shoff, shentsize, shnum) = if is_64 {
        let mut b = [0u8; 8];
        b.copy_from_slice(&header[0x28..0x30]);
        let shoff = if big_endian {
            u64::from_be_bytes(b)
        } else {
            u64::from_le_bytes(b)
        };
        (shoff, u16_at(0x3A), u16_at(0x3C))
    } else {
        let b = [header[0x20], header[0x21], header[0x22], header[0x23]];
        let shoff = if big_endian {
            u32::from_be_bytes(b) as u64
        } else {
            u32::from_le_bytes(b) as u64
        };
        (shoff, u16_at(0x2E), u16_at(0x30))
    };
    shoff.checked_add(shentsize.checked_mul(shnum)?)
}

/// The offset of an AppImage's SquashFS payload, verified by actually finding a
/// SquashFS superblock there.
///
/// The arithmetic in [`payload_offset`] is what the AppImage runtime uses, but
/// it is arithmetic on a header a file is free to lie about, and this is about
/// to be handed to a filesystem driver. Confirming the superblock parses turns
/// "the header says the payload is here" into "there is a filesystem here".
pub fn squashfs_payload_offset<R: Read + Seek>(reader: &mut R) -> Option<u64> {
    if detect_kind(reader)? != AppImageKind::SquashFs {
        return None;
    }
    let offset = payload_offset(reader)?;
    crate::fs::squashfs::SquashfsFilesystem::detect(reader, offset).then_some(offset)
}

/// Whether `path` holds a type-2 AppImage whose payload we can open.
///
/// Checked by content, not by extension: an AppImage is routinely named without
/// one (they are meant to be run, and `chmod +x` is the only ceremony), so the
/// name is not something to rely on.
pub fn is_squashfs_appimage(path: &std::path::Path) -> bool {
    let Ok(mut f) = std::fs::File::open(path) else {
        return false;
    };
    squashfs_payload_offset(&mut f).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A minimal ELF64 header whose section table ends at `payload_at`, with
    /// `payload` appended there. `kind` goes in the AppImage magic byte.
    fn fake_appimage(kind: u8, payload: &[u8]) -> Vec<u8> {
        const PAYLOAD_AT: u64 = 512;
        let mut v = vec![0u8; PAYLOAD_AT as usize];
        v[0..4].copy_from_slice(&[0x7f, b'E', b'L', b'F']);
        v[4] = 2; // ELF64
        v[5] = 1; // little-endian
        v[8] = b'A';
        v[9] = b'I';
        v[10] = kind;
        // Section table: 2 entries of 64 bytes ending exactly at PAYLOAD_AT.
        let shentsize = 64u64;
        let shnum = 2u64;
        let shoff = PAYLOAD_AT - shentsize * shnum;
        v[0x28..0x30].copy_from_slice(&shoff.to_le_bytes());
        v[0x3A..0x3C].copy_from_slice(&(shentsize as u16).to_le_bytes());
        v[0x3C..0x3E].copy_from_slice(&(shnum as u16).to_le_bytes());
        v.extend_from_slice(payload);
        v
    }

    fn squashfs_payload() -> Vec<u8> {
        use crate::fs::squashfs_write::{write_squashfs, BuildNode, BuildOptions};
        let tree = BuildNode::dir(
            "",
            0o755,
            vec![BuildNode::file(
                "hello",
                0o644,
                b"from an appimage\n".to_vec(),
            )],
        );
        let mut cur = Cursor::new(Vec::new());
        write_squashfs(&mut cur, &tree, &BuildOptions::default()).expect("build");
        cur.into_inner()
    }

    #[test]
    fn finds_the_payload_of_a_type2_appimage() {
        let img = fake_appimage(2, &squashfs_payload());
        let mut cur = Cursor::new(img);
        assert_eq!(detect_kind(&mut cur), Some(AppImageKind::SquashFs));
        assert_eq!(payload_offset(&mut cur), Some(512));
        assert_eq!(squashfs_payload_offset(&mut cur), Some(512));
    }

    /// Type 1 wraps an ISO, not a SquashFS. Recognising it is what lets the
    /// caller say so instead of reporting a corrupt file.
    #[test]
    fn a_type1_appimage_is_recognised_but_not_opened() {
        let img = fake_appimage(1, &squashfs_payload());
        let mut cur = Cursor::new(img);
        assert_eq!(detect_kind(&mut cur), Some(AppImageKind::Iso9660));
        assert_eq!(squashfs_payload_offset(&mut cur), None);
    }

    /// A plain ELF has zeroes where the AppImage magic would be, so an ordinary
    /// executable is never mistaken for one.
    #[test]
    fn a_plain_elf_is_not_an_appimage() {
        let mut img = fake_appimage(2, &squashfs_payload());
        img[8] = 0;
        img[9] = 0;
        img[10] = 0;
        let mut cur = Cursor::new(img);
        assert_eq!(detect_kind(&mut cur), None);
        assert_eq!(squashfs_payload_offset(&mut cur), None);
    }

    /// Editing the payload must leave the ELF stub alone, and must be free to
    /// make the file longer.
    ///
    /// Both halves were wrong in the first cut. The payload window needs no
    /// re-encoding on commit, so it was `RwCommit::None` — which the resolver
    /// read as "the handle is the raw file", so SquashFS atomically replaced
    /// the whole AppImage with just its payload and the stub was gone. Nothing
    /// complained: the result was a perfectly valid SquashFS that was no longer
    /// an executable.
    #[test]
    fn editing_the_payload_leaves_the_stub_alone_and_may_grow_it() {
        use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
        use crate::fs::squashfs_edit::{SizeBudget, SquashfsEditor};
        use crate::rbformats::payload_slice::PayloadSlice;

        let original = fake_appimage(2, &squashfs_payload());
        let stub_len = 512usize;
        let stub_before = original[..stub_len].to_vec();

        let mut cur = Cursor::new(original.clone());
        let offset = squashfs_payload_offset(&mut cur).expect("payload");
        // A tail window, no declared capacity and no whole-file path — the two
        // properties that make this an AppImage rather than a bare image.
        let mut ed = SquashfsEditor::open_within(
            PayloadSlice::tail(cur, offset),
            0,
            None,
            SizeBudget::Fit,
            None,
        )
        .expect("open the payload for edit");
        let root = ed.root().expect("root");
        // Incompressible, so the payload genuinely has to get bigger — a
        // compressible blob would vanish into the existing 4 KiB pad and prove
        // nothing about growth.
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        let big: Vec<u8> = (0..200_000)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                (state >> 33) as u8
            })
            .collect();
        ed.create_file(
            &root,
            "added",
            &mut Cursor::new(big.clone()),
            big.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create");
        ed.sync_metadata().expect("sync");

        let after = ed
            .into_backing()
            .expect("in-place commit keeps the handle")
            .into_inner()
            .into_inner();
        assert_eq!(
            &after[..stub_len],
            &stub_before[..],
            "the ELF stub was modified - the AppImage would no longer run"
        );
        assert!(
            after.len() > original.len(),
            "the payload could not grow: {} -> {}",
            original.len(),
            after.len()
        );
        // And the payload is still a filesystem holding the edit.
        let mut fs = crate::fs::squashfs::SquashfsFilesystem::open(Cursor::new(after), offset)
            .expect("reopen payload");
        let root = fs.root().unwrap();
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.contains(&"added".to_string()), "got: {names:?}");
        assert!(names.contains(&"hello".to_string()), "original file lost");
    }

    /// A header claiming a payload where there is no filesystem is refused:
    /// the arithmetic is only a hint until the superblock confirms it.
    #[test]
    fn a_payload_offset_with_no_filesystem_is_refused() {
        let img = fake_appimage(2, b"not a squashfs at all");
        let mut cur = Cursor::new(img);
        assert_eq!(
            payload_offset(&mut cur),
            Some(512),
            "arithmetic still works"
        );
        assert_eq!(
            squashfs_payload_offset(&mut cur),
            None,
            "but no filesystem is there, so it must not be offered"
        );
    }
}
