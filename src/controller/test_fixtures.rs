//! Fixture builders for shell tests (the TUI's), so a view's tests reach the model through here.

/// A blank FAT volume of `size` bytes.
pub fn blank_fat(size: u64, label: Option<&str>) -> Vec<u8> {
    fs_pc::fat::create_blank_fat(size, label).expect("blank FAT")
}

/// A blank ext4 volume of `size` bytes.
pub fn blank_ext4(size: u64, label: &str) -> Vec<u8> {
    fs_unix::ext_format::create_blank_ext4(size, label).expect("blank ext4")
}

/// Whether `path` starts with a NeXT disk label.
pub fn has_next_label(path: &std::path::Path) -> bool {
    let mut f = std::fs::File::open(path).expect("open");
    part_tables::next::detect(&mut f).is_some()
}
