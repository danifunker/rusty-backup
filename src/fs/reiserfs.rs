//! Moved to the `fs-unix` crate; re-exported so `crate::fs::reiserfs` paths keep working.

pub use fs_unix::reiserfs::*;

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

    #[test]
    fn probe_0x83_routes_reiserfs() {
        // Confirm the magic-based dispatch in src/fs/mod.rs surfaces
        // ReiserFS through probe_0x83_fs_type — the public entry point the
        // partition-table label column uses to swap "Linux" for the real
        // filesystem family.
        let buf = load_fixture("test_reiserfs_v3_6.img.zst");
        let mut cursor = Cursor::new(buf);
        assert_eq!(
            crate::fs::probe_0x83_fs_type(&mut cursor, 0),
            Some("ReiserFS")
        );
    }
}
