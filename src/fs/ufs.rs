//! Moved to the `fs-unix` crate; re-exported so `crate::fs::ufs` paths keep working.

pub use fs_unix::ufs::*;

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    fn load_fixture(name: &str) -> Vec<u8> {
        use std::io::Read;
        let path = format!("tests/fixtures/{name}");
        let compressed = std::fs::read(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        let mut out = Vec::new();
        crate::rbformats::zstd_compat::decoder(Cursor::new(compressed))
            .and_then(|mut d| d.read_to_end(&mut out))
            .unwrap_or_else(|e| panic!("decompress {path}: {e}"));
        out
    }

    /// Regression: the mod.rs edit dispatch must route an auto-detected
    /// UFS volume (partition type 0x00) to the editable UFS driver rather
    /// than falling through to `Unsupported`. The read path reached UFS but
    /// the edit path did not until the `"ufs"` arm was wired into
    /// `open_editable_filesystem`; this test guards that wiring.
    #[test]
    fn open_editable_filesystem_routes_auto_detected_ufs() {
        // `fs` is a `Box<dyn EditableFilesystem>`, so its (and its supertrait
        // `Filesystem`'s) methods resolve through the trait object without the
        // traits in scope; only the named `CreateFileOptions` type needs importing.
        use disk_core::filesystem::CreateFileOptions;
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = crate::fs::open_editable_filesystem(Cursor::new(img), 0, 0x00, None)
            .expect("edit dispatch must route auto-detected UFS to the editable driver");
        let root = fs.root().expect("root");
        let payload = b"routed through open_editable_filesystem".to_vec();
        let mut src = payload.as_slice();
        let created = fs
            .create_file(
                &root,
                "wired.txt",
                &mut src,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("dispatched editable UFS can create a file");
        assert_eq!(created.size, payload.len() as u64);
        assert!(
            fs.list_directory(&root)
                .expect("list")
                .iter()
                .any(|e| e.name == "wired.txt"),
            "new file visible through the dispatched editable filesystem"
        );
    }
}
