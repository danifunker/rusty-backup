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

const MIB: usize = 1024 * 1024;

/// A 16 MiB MBR disk image: a 4 MiB FAT16 partition at LBA 2048, the rest free.
pub fn mbr_with_gap(dir: &std::path::Path) -> std::path::PathBuf {
    let vol = blank_fat(4 * MIB as u64, Some("GAP"));
    let mut bytes = vec![0u8; 16 * MIB];
    bytes[2048 * 512..2048 * 512 + vol.len()].copy_from_slice(&vol);
    let e = 0x1BE;
    bytes[e + 4] = 0x06;
    bytes[e + 8..e + 12].copy_from_slice(&2048u32.to_le_bytes());
    bytes[e + 12..e + 16].copy_from_slice(&((vol.len() / 512) as u32).to_le_bytes());
    bytes[510] = 0x55;
    bytes[511] = 0xAA;
    let path = dir.join("gap.img");
    std::fs::write(&path, bytes).expect("write gap.img");
    path
}

/// A 64 MiB IRIX disk: an SGI volume header and one EFS partition.
pub fn sgi_efs_disk(dir: &std::path::Path) -> std::path::PathBuf {
    use crate::partition::sgi_hdd_builder::{build_sgi_efs_hdd, SgiHddOptions};
    let (img, _) = build_sgi_efs_hdd(&SgiHddOptions::new(64 << 20, "tui")).expect("SGI disk");
    let path = dir.join("irix.img");
    std::fs::write(&path, img).expect("write irix.img");
    path
}

/// A blank Diablo 31 Alto pack saved as a PDI.
pub fn alto_pack(dir: &std::path::Path) -> std::path::PathBuf {
    let geometry = crate::model::alto_resize::AltoTarget::Diablo31.geometry();
    let disk = fs_xerox::alto::write::create_blank(geometry).expect("blank Alto pack");
    let path = dir.join("pack.pdi");
    std::fs::write(&path, fs_xerox::alto::pdi::write(&disk)).expect("write pack.pdi");
    path
}

/// A flat 4 MiB classic-HFS volume (a BasiliskII `.hfv`).
pub fn hfs_volume(dir: &std::path::Path) -> std::path::PathBuf {
    let bytes = fs_mac::hfs::create_blank_hfs(4 * MIB as u64, 4096, "Work").expect("blank HFS");
    let path = dir.join("vol.hfv");
    std::fs::write(&path, bytes).expect("write vol.hfv");
    path
}

/// [`hfs_volume`] holding `README`, a TEXT / ttxt file with Mac dates.
pub fn hfs_volume_with_text(dir: &std::path::Path) -> std::path::PathBuf {
    let path = hfs_volume(dir);
    let session = crate::model::browse_session::BrowseSession {
        source_path: Some(path.clone()),
        ..Default::default()
    };
    let (mut fs, commit) = session.open_editable().expect("editable HFS");
    let root = fs.as_filesystem_mut().root().expect("root");
    let opts = disk_core::filesystem::CreateFileOptions {
        os_type: Some(*b"TEXT"),
        os_creator: Some(*b"ttxt"),
        ..Default::default()
    };
    let mut data: &[u8] = b"hello";
    fs.create_file(&root, "README", &mut data, 5, &opts)
        .expect("create README");
    fs.sync_metadata().expect("sync");
    drop(fs);
    commit.commit().expect("commit");
    path
}

/// A StuffIt archive at `dir/disk.sit` whose one entry is a 1 MiB raw HFS disk.
pub fn sit_holding_hfs_disk(dir: &std::path::Path) -> std::path::PathBuf {
    use arc_mac::stuffit::{build_archive, StuffItInput, WriteMethod};
    let disk = fs_mac::hfs::create_blank_hfs(1024 * 1024, 512, "Disk").expect("blank HFS");
    let entry = StuffItInput {
        name: "Disk.image".into(),
        type_code: *b"rohd",
        creator_code: *b"ddsk",
        finder_flags: 0,
        create_date: 0,
        mod_date: 0,
        data_fork: disk,
        resource_fork: Vec::new(),
    };
    let path = dir.join("disk.sit");
    let bytes = build_archive(&[entry], WriteMethod::Store).expect("StuffIt archive");
    std::fs::write(&path, bytes).expect("write disk.sit");
    path
}

/// A SquashFS image at `dir/vol.sqfs` holding `a.txt`.
pub fn squashfs_volume(dir: &std::path::Path) -> std::path::PathBuf {
    use fs_squashfs::squashfs_write::{write_squashfs, BuildNode, BuildOptions};
    let root = BuildNode::dir(
        "",
        0o755,
        vec![BuildNode::file("a.txt", 0o644, b"hi".to_vec())],
    );
    let path = dir.join("vol.sqfs");
    let mut out = std::fs::File::create(&path).expect("create vol.sqfs");
    write_squashfs(&mut out, &root, &BuildOptions::default()).expect("SquashFS image");
    path
}

/// A blank 1.2 MB X68000 XDF floppy.
pub fn blank_xdf(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("disk.xdf");
    std::fs::write(&path, vec![0xE5u8; 1_261_568]).expect("write disk.xdf");
    path
}
