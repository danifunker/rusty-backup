//! Moved to the `disk-core` crate; re-exported so `crate::fs::raw_name` paths keep working.

pub use disk_core::raw_name::*;

#[cfg(test)]
mod tests {
    use super::*;

    /// Through a real UFS: the dirent holds the NeXT byte itself, and listing gives the name back.
    #[test]
    fn a_ufs_dirent_stores_the_raw_byte() {
        use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
        let img =
            crate::fs::ufs_format::create_blank_ufs1(&crate::fs::ufs_format::Ufs1FormatParams {
                size_bytes: 8 * 1024 * 1024,
                cg_layout: crate::fs::ufs::CgLayout::Bsd43,
                endian: crate::fs::ufs::UfsEndian::Big,
                ..Default::default()
            })
            .unwrap();
        let mut cur = std::io::Cursor::new(img);
        let mut fs = crate::fs::ufs::UfsFilesystem::open(&mut cur, 0).unwrap();
        let root = fs.root().unwrap();
        let name = decode(b"Steroidgrundger\xf6st.lookMol");
        let opts = CreateFileOptions::default();
        fs.create_file(&root, &name, &mut &b"C27"[..], 3, &opts)
            .unwrap();
        fs.sync_metadata().unwrap();
        let listed = fs.list_directory(&root).unwrap();
        assert!(listed.iter().any(|e| e.name == name), "{listed:?}");

        drop(fs);
        let img = cur.into_inner();
        let raw = b"Steroidgrundger\xf6st.lookMol";
        assert!(img.windows(raw.len()).any(|w| w == raw), "raw 0xF6 on disk");
        let utf8 = name.as_bytes();
        assert!(
            !img.windows(utf8.len()).any(|w| w == utf8),
            "no placeholder bytes on disk"
        );
    }

    #[test]
    fn display_spells_out_the_raw_bytes() {
        assert_eq!(display(&decode(b"Eject\xbcE.tiff")), "Eject\\xBCE.tiff");
        assert_eq!(display("plain"), "plain");
    }
}
