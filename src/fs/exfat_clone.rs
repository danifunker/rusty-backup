//! Moved to the `fs-pc` crate; re-exported so `crate::fs::exfat_clone` paths keep working.

pub use fs_pc::exfat_clone::*;

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read};

    /// Backup preflight seam: an exFAT volume is detected as an exFAT clone shape.
    #[test]
    fn an_exfat_volume_is_an_exfat_clone_shape() {
        let compressed = std::fs::read("tests/fixtures/test_exfat.img.zst").expect("fixture");
        let mut img = Vec::new();
        crate::rbformats::zstd_compat::decoder(Cursor::new(compressed))
            .and_then(|mut d| d.read_to_end(&mut img))
            .expect("decompress");
        assert_eq!(
            crate::fs::detect_defrag_clone_shape(&mut Cursor::new(img), 0, None).unwrap(),
            crate::fs::DefragCloneShape::Exfat
        );
    }
}
