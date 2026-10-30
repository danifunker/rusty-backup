//! QNX 2 filesystem (QNX 2.x, and the Unisys ICON's System Software and QDOS): read and edit.
//!
//! The on-disk reader, writer and checker and the `Filesystem` / `EditableFilesystem` adapter
//! all live in the MIT-licensed [`qnx2fs`] crate (its `adapter` module, feature `disk-core`),
//! shared with icon-emu; fixes go there. On-disk facts and how the writes were checked:
//! `docs/PLAN-qnx.md`.

pub use qnx2fs::adapter::*;
pub use qnx2fs::{decode_date, fold_date, looks_like_qnx2, looks_like_rs_text, rs_to_lf, RS};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::entry::FileEntry;
    use crate::fs::filesystem::{EditableFilesystem, Filesystem};
    use std::io::Cursor;

    const ROOT_AT: usize = 68;
    const ENTRY: usize = 48;

    /// A 16-block volume: root at block 3 with `HELLO` (block 5), dir `sub` (block 4), a deleted
    /// `GONE` (block 6) and a bitmap file at block 2; the root's header names its own entry.
    fn sample() -> Cursor<Vec<u8>> {
        let mut img = vec![0u8; 16 * 512];
        let entry = |img: &mut Vec<u8>, at: usize, status: u8, name: &[u8], first: u32, attr| {
            img[at] = status;
            img[at + 1..at + 5].copy_from_slice(&first.to_le_bytes());
            img[at + 26] = attr;
            img[at + 24] = 0x21;
            img[at + 25] = 0x01;
            img[at + 31..at + 31 + name.len()].copy_from_slice(name);
        };
        let hdr = |img: &mut Vec<u8>, block: usize, size: u32| {
            let at = (block - 1) * 512;
            img[at + 8..at + 12].copy_from_slice(&size.to_le_bytes());
        };
        entry(&mut img, ROOT_AT, 0x40, b"", 3, 0x25);
        let dir = 2 * 512 + 16;
        img[dir..dir + 4].copy_from_slice(&[1, 0, 1, 0]);
        entry(&mut img, dir + 4, 0x40, b"HELLO", 5, 0x01);
        entry(&mut img, dir + 4 + ENTRY, 0x40, b"sub", 4, 0x25);
        entry(&mut img, dir + 4 + 2 * ENTRY, 0, b"GONE", 6, 0x11);
        entry(&mut img, dir + 4 + 3 * ENTRY, 0x40, b"bitmap", 2, 0x01);
        hdr(&mut img, 3, 4 + 4 * ENTRY as u32);
        hdr(&mut img, 4, 4);
        hdr(&mut img, 5, 6);
        img[4 * 512 + 16..4 * 512 + 22].copy_from_slice(b"hello\n");
        hdr(&mut img, 2, 2);
        // Blocks 1-5 in use; 6 (the deleted file) free.
        img[512 + 16] = 0x1F;
        hdr(&mut img, 6, 3);
        img[5 * 512 + 16..5 * 512 + 19].copy_from_slice(b"old");
        Cursor::new(img)
    }

    #[test]
    fn cp_between_qnx2_volumes_keeps_raw_attributes() {
        // Bits a Unix mode cannot hold: write-only owner, group r--e-, other a+m, a directory's b.
        let mut img = sample().into_inner();
        let dir = 2 * 512 + 16 + 4;
        img[dir + 26] = 0x12;
        img[dir + 24] = 0x09;
        img[dir + 25] = 0x14;
        img[dir + 15] = 7;
        img[dir + 16] = 3;
        img[dir + 27..dir + 31].copy_from_slice(&[0x45, 0x2C, 0x10, 0x0E]);
        img[dir + ENTRY + 26] = 0x2D;
        let mut src = Qnx2Filesystem::open(Cursor::new(img), 0).unwrap();
        let mut dst = Qnx2Filesystem::open(sample(), 0).unwrap();
        let (sroot, droot) = (src.root().unwrap(), dst.root().unwrap());
        let listed = src.list_directory(&sroot).unwrap();
        let dsub = dst
            .list_directory(&droot)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "sub")
            .unwrap();
        let opts = crate::fs::copy::CopyOptions {
            recursive: true,
            ..Default::default()
        };
        let mut stats = Default::default();
        for name in ["HELLO", "sub"] {
            let e = listed.iter().find(|e| e.name == name).unwrap();
            crate::fs::copy::copy_into(
                &mut src,
                e,
                &mut dst,
                &dsub,
                name,
                &opts,
                &mut stats,
                &|_| {},
            )
            .unwrap();
        }
        dst.sync_metadata().unwrap();
        let copied = dst.list_directory(&dsub).unwrap();
        for name in ["HELLO", "sub"] {
            let a = listed.iter().find(|e| e.name == name).unwrap();
            let b = copied.iter().find(|e| e.name == name).unwrap();
            assert_eq!(a.qnx2_attrs, b.qnx2_attrs, "{name}");
            // A written entry is flagged modified, as QNX flags it.
            let letters = |e: &FileEntry| e.native_attrs.clone().unwrap().replace("; modified", "");
            assert_eq!(letters(a), letters(b), "{name}");
        }
        assert_eq!(stats.dropped_perms, 0);
        assert!(fsck_qnx2(&mut dst).unwrap().is_clean());
    }
}
