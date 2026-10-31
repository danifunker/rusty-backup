//! Tests of app behaviour (dispatch, detection, builders) over code that moved into the
//! `crates/` workspace; each module is named after the crate module it exercises.

mod exfat_clone {
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

mod hfsplus {
    use byteorder::{BigEndian, ByteOrder};
    use fs_mac::hfsplus::*;

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

mod hfv {
    use crate::partition::PartitionTable;
    use fs_mac::hfv::*;
    use std::io::Cursor;

    #[test]
    fn a_blank_hfv_detects_as_an_hfs_superfloppy() {
        let img = build_blank_hfv(
            16 * 1024 * 1024,
            suggest_block_size(16 * 1024 * 1024),
            "BlankHFV",
        )
        .unwrap();
        // Re-detects as a partition-less HFS superfloppy.
        let mut cur = Cursor::new(img);
        match PartitionTable::detect(&mut cur).unwrap() {
            PartitionTable::None { fs_hint, .. } => assert_eq!(fs_hint, "HFS"),
            other => panic!("expected HFS superfloppy, got {other:?}"),
        }
    }
}

mod id_names {
    use disk_core::id_names::*;

    /// End to end over a real filesystem: build a SquashFS that carries its own
    /// `/etc/passwd` and `/etc/group`, then resolve names out of it. Exercises
    /// `read_file_at_path` walking `root -> etc -> passwd` through the actual
    /// `Filesystem` trait, not just the parser.
    #[test]
    fn resolves_names_from_a_real_image() {
        use fs_squashfs::squashfs::SquashfsFilesystem;
        use fs_squashfs::squashfs_write::{write_squashfs, BuildNode, BuildOptions};
        use std::io::Cursor;

        let passwd = b"root:x:0:0:root:/root:/bin/bash\n\
                       alice:x:1000:1000:Alice:/home/alice:/bin/bash\n"
            .to_vec();
        let group = b"root:x:0:\nstaff:x:50:alice\n".to_vec();
        let tree = BuildNode::dir(
            "/",
            0o755,
            vec![
                BuildNode::dir(
                    "etc",
                    0o755,
                    vec![
                        BuildNode::file("passwd", 0o644, passwd),
                        BuildNode::file("group", 0o644, group),
                    ],
                ),
                BuildNode::dir("home", 0o755, vec![]),
            ],
        );
        let mut cur = Cursor::new(Vec::new());
        write_squashfs(&mut cur, &tree, &BuildOptions::default()).expect("build image");
        cur.set_position(0);
        let mut fs = SquashfsFilesystem::open(cur, 0).expect("open");

        let map = IdNameMap::from_filesystem(&mut fs);
        assert!(!map.is_empty(), "should have parsed the account files");
        assert_eq!(map.user(0), Some("root"));
        assert_eq!(map.user(1000), Some("alice"));
        assert_eq!(map.group(50), Some("staff"));
        assert_eq!(map.user(1234), None, "unknown uid stays unresolved");
        assert_eq!(map.format_owner(1000, 50), "alice:staff (1000:50)");
    }

    /// An image with no `/etc/passwd` (a data volume, a non-Linux FS) must
    /// resolve to an empty map, never an error.
    #[test]
    fn image_without_etc_yields_empty_map() {
        use fs_squashfs::squashfs::SquashfsFilesystem;
        use fs_squashfs::squashfs_write::{write_squashfs, BuildNode, BuildOptions};
        use std::io::Cursor;

        let tree = BuildNode::dir(
            "/",
            0o755,
            vec![BuildNode::file("data.bin", 0o644, vec![0u8; 16])],
        );
        let mut cur = Cursor::new(Vec::new());
        write_squashfs(&mut cur, &tree, &BuildOptions::default()).expect("build");
        cur.set_position(0);
        let mut fs = SquashfsFilesystem::open(cur, 0).expect("open");
        assert!(IdNameMap::from_filesystem(&mut fs).is_empty());
    }
}

mod lif {
    use byteorder::{BigEndian, ByteOrder};
    use disk_core::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
    use fs_8bit::lif::*;
    use std::io::Cursor;

    fn hpux_lif() -> Vec<u8> {
        let mut img = vec![0u8; 256 * 256];
        BigEndian::write_u16(&mut img[0..2], 0x8000);
        img[2..8].copy_from_slice(b"ISL10 ");
        BigEndian::write_u32(&mut img[8..12], 16);
        BigEndian::write_u16(&mut img[12..14], 0x1000);
        BigEndian::write_u32(&mut img[16..20], 8);
        BigEndian::write_u16(&mut img[20..22], 1);
        BigEndian::write_u32(&mut img[24..28], 1);
        BigEndian::write_u32(&mut img[28..32], 1);
        BigEndian::write_u32(&mut img[32..36], 128);
        BigEndian::write_u32(&mut img[240..244], 32 * 256);
        BigEndian::write_u32(&mut img[244..248], 2048);
        BigEndian::write_u32(&mut img[248..252], 0x100);
        // Something below the files that must survive, like an LVM record.
        img[4 * 256..4 * 256 + 8].copy_from_slice(b"LVMREC01");
        let d = 16 * 256;
        img[d..d + 10].copy_from_slice(b"ISL       ");
        BigEndian::write_u16(&mut img[d + 10..d + 12], 0xCE00);
        BigEndian::write_u32(&mut img[d + 12..d + 16], 32);
        BigEndian::write_u32(&mut img[d + 16..d + 20], 4);
        BigEndian::write_u16(&mut img[d + 26..d + 28], 0x8001);
        BigEndian::write_i16(&mut img[d + 42..d + 44], -1);
        img[32 * 256..36 * 256].fill(0x11);
        img
    }

    fn put(fs: &mut LifFilesystem<Cursor<Vec<u8>>>, name: &str, data: &[u8]) {
        let root = fs.root().unwrap();
        let opts = CreateFileOptions::default();
        fs.create_file(&root, name, &mut &data[..], data.len() as u64, &opts)
            .unwrap();
        fs.sync_metadata().unwrap();
    }

    fn replace(fs: &mut LifFilesystem<Cursor<Vec<u8>>>, name: &str, data: &[u8]) {
        use crate::fs::replace::{create_or_replace, OnConflict, ReplacePolicy};
        let root = fs.root().unwrap();
        let policy = ReplacePolicy {
            on_conflict: OnConflict::Replace,
            ..Default::default()
        };
        let opts = CreateFileOptions::default();
        create_or_replace(
            fs,
            &root,
            name,
            &mut &data[..],
            data.len() as u64,
            &opts,
            policy,
        )
        .unwrap();
        fs.sync_metadata().unwrap();
    }

    /// The app's staged replace keeps a file's type, implementation word and IPL pointer.
    #[test]
    fn staged_replace_keeps_type_impl_word_and_ipl() {
        let mut img = hpux_lif();
        BigEndian::write_u32(&mut img[16 * 256 + 28..], 0xFFF0_0000);
        let mut fs = LifFilesystem::open_editable(Cursor::new(img), 0, None).unwrap();
        put(&mut fs, "LABEL", b"x");
        let mut img = fs.into_inner().into_inner();
        let dir = BigEndian::read_u32(&img[8..12]) as usize * 256;
        let label = parse_lif(&mut Cursor::new(&img), 0)
            .unwrap()
            .files
            .iter()
            .find(|f| f.name == "LABEL")
            .unwrap()
            .slot;
        BigEndian::write_u16(&mut img[dir + label * 32 + 10..], 0xA271);
        let mut fs = LifFilesystem::open_editable(Cursor::new(img), 0, None).unwrap();
        replace(&mut fs, "label", b"y");
        replace(&mut fs, "isl", &[0x44; 5000]);
        let img = fs.into_inner().into_inner();
        let vol = parse_lif(&mut Cursor::new(&img), 0).unwrap();
        let names: Vec<_> = vol.files.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["ISL", "LABEL"], "no temporaries left behind");
        let get = |n: &str| vol.files.iter().find(|f| f.name == n).unwrap().clone();
        assert_eq!(get("LABEL").file_type, 0xA271);
        let isl = get("ISL");
        assert_eq!(isl.file_type, 0xCE00);
        assert_eq!(
            BigEndian::read_u32(&img[dir + isl.slot * 32 + 28..]),
            0xFFF0_0000
        );
        assert_eq!(BigEndian::read_u32(&img[240..]) as u64, isl.start_byte);
        assert_eq!(BigEndian::read_u32(&img[244..]), 6144);
    }
}

mod name_legalize {
    use disk_core::name_legalize::*;

    /// Names seen on real classic-Mac and DOS volumes, pushed through the real validators.
    #[test]
    fn vintage_names_become_legal_on_every_real_destination() {
        use disk_core::filesystem::Filesystem;
        use std::io::Cursor;
        let corpus = [
            "Acquire/Export",
            "Schwarz/Weiss",
            "MacDRUMS Instruments/Tracks",
            "Icon\r",
            "Read Me!",
            "R\u{e9}sum\u{e9}\u{2122} \u{192}",
            "\u{F8FF} Menu Items",
            "a:b",
            "CON",
            "lpt1.txt",
            "..",
            "   ",
            "trailing.",
            "*?<>|",
            "1st file",
            "file.tar.gz",
            "Anti-Aliased PICT",
            "An extraordinarily long name that exceeds every limit.html",
        ];
        let fat = fs_pc::fat::FatFilesystem::open(
            Cursor::new(fs_pc::fat::create_blank_fat(737280, Some("T")).unwrap()),
            0,
        )
        .unwrap();
        let hfs = fs_mac::hfs::HfsFilesystem::open(
            Cursor::new(fs_mac::hfs::create_blank_hfs(8 << 20, 4096, "T").unwrap()),
            0,
        )
        .unwrap();
        let prodos = fs_apple2::prodos::ProDosFilesystem::open(
            Cursor::new(fs_apple2::prodos::create_blank_prodos(143_360, "T").unwrap()),
            0,
        )
        .unwrap();
        let mfs = fs_mac::mfs::MfsFilesystem::open(
            Cursor::new(fs_mac::mfs::create_blank_mfs(409_600, "T").unwrap()),
            0,
        )
        .unwrap();
        type Check<'a> = Box<NameValidator<'a>>;
        let validators: Vec<(&str, Check<'_>)> = vec![
            ("FAT", Box::new(|n: &str| fat.validate_name(n))),
            ("HFS", Box::new(|n: &str| hfs.validate_name(n))),
            ("ProDOS", Box::new(|n: &str| prodos.validate_name(n))),
            ("MFS", Box::new(|n: &str| mfs.validate_name(n))),
            (
                "Atari DOS",
                Box::new(|n: &str| fs_8bit::atari_dos::encode_name(n).map(|_| ())),
            ),
            (
                "CBM",
                Box::new(|n: &str| fs_8bit::cbm::encode_petscii_name(n).map(|_| ())),
            ),
            ("HPFS", Box::new(fs_hpfs::hpfs::validate_hpfs_name)),
            (
                "ext",
                Box::new(|n: &str| validate_posix_name(n, 255, "ext")),
            ),
        ];
        for (fs, v) in &validators {
            for name in corpus {
                let out = legalize_name(v.as_ref(), name)
                    .unwrap_or_else(|e| panic!("{fs}: no legal name for {name:?}: {e}"));
                assert!(v(&out).is_ok(), "{fs}: {name:?} -> {out:?} still invalid");
            }
        }
        // HFS keeps slashes (legal there); FAT does not.
        let hfs_v = |n: &str| hfs.validate_name(n);
        assert_eq!(
            legalize_name(&hfs_v, "Acquire/Export").unwrap(),
            "Acquire/Export"
        );
        let fat_v = |n: &str| fat.validate_name(n);
        assert_eq!(
            legalize_name(&fat_v, "Acquire/Export").unwrap(),
            "Acquire_Export"
        );
        assert_eq!(legalize_name(&fat_v, "Icon\r").unwrap(), "Icon_");
        assert_eq!(legalize_name(&fat_v, "CON").unwrap(), "CON_");
        let prodos_v = |n: &str| prodos.validate_name(n);
        assert_eq!(legalize_name(&prodos_v, "Read Me!").unwrap(), "Read.Me.");
    }
}

mod raw_name {
    use disk_core::raw_name::*;

    /// Through a real UFS: the dirent holds the NeXT byte itself, and listing gives the name back.
    #[test]
    fn a_ufs_dirent_stores_the_raw_byte() {
        use disk_core::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
        let img = fs_unix::ufs_format::create_blank_ufs1(&fs_unix::ufs_format::Ufs1FormatParams {
            size_bytes: 8 * 1024 * 1024,
            cg_layout: fs_unix::ufs::CgLayout::Bsd43,
            endian: fs_unix::ufs::UfsEndian::Big,
            ..Default::default()
        })
        .unwrap();
        let mut cur = std::io::Cursor::new(img);
        let mut fs = fs_unix::ufs::UfsFilesystem::open(&mut cur, 0).unwrap();
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

mod reiserfs {
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

mod squashfs {
    use std::io::{Seek, SeekFrom};

    /// The routing layer must recognise a SquashFS partition on its own —
    /// callers go through `open_filesystem`, never the concrete type.
    #[test]
    fn dispatch_routes_a_real_image_to_squashfs() {
        let path = std::env::var("RB_SQUASHFS_ISO").unwrap_or_else(|_| {
            "/Users/dani/Downloads/ubuntu-12.04-desktop-powerpc.iso".to_string()
        });
        let Ok(mut file) = std::fs::File::open(&path) else {
            eprintln!("{path} not present — skipping");
            return;
        };
        let Some(offset) = find_squashfs_offset(&mut file) else {
            return;
        };

        // Type byte 0 is the auto-detect path; 0x83 is how an appliance image
        // stamps a Linux root partition.
        for type_byte in [0x00u8, 0x83u8] {
            let f = std::fs::File::open(&path).expect("reopen");
            let mut fs = crate::fs::open_filesystem(f, offset, type_byte, None)
                .unwrap_or_else(|e| panic!("dispatch failed for type 0x{type_byte:02X}: {e}"));
            assert_eq!(fs.fs_type(), "SquashFS");
            let root = fs.root().expect("root");
            assert!(!fs.list_directory(&root).expect("list").is_empty());
        }

        // And the 0x83 probe should name it for the partition table view.
        let mut f = std::fs::File::open(&path).expect("reopen");
        assert_eq!(
            crate::fs::probe_0x83_fs_type(&mut f, offset),
            Some("SquashFS")
        );
    }

    /// Scan 2048-byte boundaries for a SquashFS superblock.
    fn find_squashfs_offset(file: &mut std::fs::File) -> Option<u64> {
        use std::io::Read as _;
        let mut pos = 0u64;
        let mut buf = vec![0u8; 1 << 22];
        loop {
            file.seek(SeekFrom::Start(pos)).ok()?;
            let n = file.read(&mut buf).ok()?;
            if n == 0 {
                return None;
            }
            let mut i = 0;
            while i + 4 <= n {
                if buf[i..i + 4] == 0x7371_7368u32.to_le_bytes() {
                    return Some(pos + i as u64);
                }
                i += 2048;
            }
            pos += n as u64;
        }
    }
}

mod ufs {
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
