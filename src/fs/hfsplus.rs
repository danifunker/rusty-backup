//! Moved to the `fs-mac` crate; re-exported so `crate::fs::hfsplus` paths keep working.

pub use fs_mac::hfsplus::*;

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{BigEndian, ByteOrder};

    const HFSPLUS_VOLUME_JOURNALED_BIT: u32 = 0x2000;
    const HFSPLUS_VOLUME_UNMOUNTED_BIT: u32 = 0x100;

    fn make_editable_hfsplus_image() -> Vec<u8> {
        create_blank_hfsplus(4 * 1024 * 1024, 4096, "T", false)
    }

    #[test]
    fn test_can_defrag_clone_accepts_clean_volume() {
        let img = make_editable_hfsplus_image();
        let mut cursor = std::io::Cursor::new(img);
        crate::fs::can_defrag_clone_hfsplus(&mut cursor, 0)
            .expect("plain unjournaled HFS+ must be accepted");
    }

    #[test]
    fn test_can_defrag_clone_accepts_clean_journaled_volume() {
        // Journaled bit set + unmounted bit set — clean. The on-disk
        // journal is empty, so the catalog is authoritative.
        let mut img = make_editable_hfsplus_image();
        let attr_off = 1024 + 4;
        let mut attrs = BigEndian::read_u32(&img[attr_off..attr_off + 4]);
        attrs |= HFSPLUS_VOLUME_JOURNALED_BIT | HFSPLUS_VOLUME_UNMOUNTED_BIT;
        BigEndian::write_u32(&mut img[attr_off..attr_off + 4], attrs);
        let mut cursor = std::io::Cursor::new(img);
        crate::fs::can_defrag_clone_hfsplus(&mut cursor, 0)
            .expect("cleanly-unmounted journaled volume must be accepted");
    }

    #[test]
    fn test_can_defrag_clone_refuses_dirty_journaled_volume() {
        // Journaled bit set, unmounted bit *clear* — dirty.
        let mut img = make_editable_hfsplus_image();
        let attr_off = 1024 + 4;
        let mut attrs = BigEndian::read_u32(&img[attr_off..attr_off + 4]);
        attrs |= HFSPLUS_VOLUME_JOURNALED_BIT;
        attrs &= !HFSPLUS_VOLUME_UNMOUNTED_BIT;
        BigEndian::write_u32(&mut img[attr_off..attr_off + 4], attrs);
        let mut cursor = std::io::Cursor::new(img);
        let err = crate::fs::can_defrag_clone_hfsplus(&mut cursor, 0)
            .expect_err("dirty journaled volume must be refused");
        assert!(
            err.contains("dirty") || err.contains("journal"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_can_defrag_clone_refuses_non_hfsplus() {
        // 1 KiB of zeros at offset 0 → no VH at offset 1024. Refused.
        let img = vec![0u8; 4096];
        let mut cursor = std::io::Cursor::new(img);
        let err = crate::fs::can_defrag_clone_hfsplus(&mut cursor, 0)
            .expect_err("non-HFS+ partition must be refused");
        assert!(err.contains("not an HFS"), "unexpected error: {err}");
    }
}
