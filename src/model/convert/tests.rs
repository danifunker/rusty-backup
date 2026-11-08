use std::io::{Cursor, Seek, SeekFrom, Write};
use std::path::Path;

use super::run::{convert_one, Hooks, Level};
use super::*;

fn hooks_into(lines: &std::cell::RefCell<Vec<String>>) -> impl Fn(Level, &str) + '_ {
    move |_, m| lines.borrow_mut().push(m.to_string())
}

fn run(src: &Path, dest: &Path, opts: &Options) -> anyhow::Result<run::Converted> {
    let lines = std::cell::RefCell::new(Vec::new());
    let log = hooks_into(&lines);
    let hooks = Hooks {
        progress: &|_, _| {},
        cancelled: &|| false,
        log: &log,
    };
    let found = classify(src)?;
    convert_one(&found, dest, opts, &hooks)
}

/// A FAT12 1.44 MB floppy holding `files` (name, length).
fn floppy(dir: &Path, name: &str, files: &[(&str, usize)]) -> std::path::PathBuf {
    let path = dir.join(name);
    let img = fs_pc::fat::create_dos_floppy(1_474_560, Some("SRC")).unwrap();
    std::fs::write(&path, img).unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut fs = crate::fs::open_editable_filesystem(file, 0, 0, None).unwrap();
    let root = fs.as_filesystem_mut().root().unwrap();
    for (n, len) in files {
        let data = vec![0x5Au8; *len];
        fs.create_file(
            &root,
            n,
            &mut Cursor::new(data),
            *len as u64,
            &Default::default(),
        )
        .unwrap();
    }
    fs.sync_metadata().unwrap();
    path
}

/// An MBR disk of `disk_mib` with one FAT16 partition of `part_mib` at 1 MiB holding one file.
fn mbr_disk(dir: &Path, disk_mib: u64, part_mib: u64) -> std::path::PathBuf {
    let path = dir.join("disk.img");
    let mut file = std::fs::File::create(&path).unwrap();
    file.set_len(disk_mib << 20).unwrap();
    let vol = fs_pc::fat::create_blank_fat(part_mib << 20, Some("HD")).unwrap();
    file.seek(SeekFrom::Start(1 << 20)).unwrap();
    file.write_all(&vol).unwrap();
    let mut mbr = [0u8; 512];
    let entry = &mut mbr[446..462];
    entry[4] = 0x06;
    entry[8..12].copy_from_slice(&2048u32.to_le_bytes());
    entry[12..16].copy_from_slice(&((part_mib << 20) as u32 / 512).to_le_bytes());
    mbr[510] = 0x55;
    mbr[511] = 0xAA;
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&mbr).unwrap();
    drop(file);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut fs = crate::fs::open_editable_filesystem(file, 1 << 20, 0x06, None).unwrap();
    let root = fs.as_filesystem_mut().root().unwrap();
    let data = vec![7u8; 300_000];
    fs.create_file(
        &root,
        "DATA.BIN",
        &mut Cursor::new(data),
        300_000,
        &Default::default(),
    )
    .unwrap();
    fs.sync_metadata().unwrap();
    path
}

#[test]
fn kinds_come_from_the_contents_first() {
    let dir = tempfile::tempdir().unwrap();
    let f = floppy(dir.path(), "game.vhd", &[]);
    let found = classify(&f).unwrap();
    assert_eq!(found.kind, ImageKind::Floppy);
    assert_eq!(found.by_extension, Some(ImageKind::HardDisk));
    assert!(found.mismatch_note().unwrap().contains("floppy"));

    let iso = dir.path().join("cd.bin");
    let mut bytes = vec![0u8; 0x9000];
    bytes[0x8001..0x8006].copy_from_slice(b"CD001");
    std::fs::write(&iso, bytes).unwrap();
    let found = classify(&iso).unwrap();
    assert_eq!(
        (found.kind, found.disc),
        (ImageKind::Disc, Some(DiscSource::Iso))
    );

    let hd = mbr_disk(dir.path(), 8, 6);
    assert_eq!(classify(&hd).unwrap().kind, ImageKind::HardDisk);
}

#[test]
fn sizing_never_grows_or_passes_a_cap() {
    let mib = 1u64 << 20;
    assert_eq!(Sizing::Keep.size_for(100 * mib, 10 * mib, None), 100 * mib);
    assert_eq!(
        Sizing::Minimum.size_for(100 * mib, 10 * mib, None),
        10 * mib
    );
    let plus = |n| Sizing::Extra(Extra::Mib(n));
    assert_eq!(plus(5).size_for(100 * mib, 10 * mib, None), 15 * mib);
    assert_eq!(
        plus(500).size_for(100 * mib, 10 * mib, None),
        100 * mib,
        "capped at original"
    );
    assert_eq!(
        plus(50).size_for(100 * mib, 10 * mib, Some(30 * mib)),
        30 * mib,
        "fs cap"
    );
    let pct = Sizing::Extra(Extra::Percent(25));
    assert_eq!(pct.size_for(100 * mib, 20 * mib, None), 25 * mib);
    assert_eq!(
        Sizing::parse("minimum+10%").unwrap(),
        Sizing::Extra(Extra::Percent(10))
    );
    assert_eq!(Sizing::parse("min+64MB").unwrap(), plus(64));
    assert_eq!(Sizing::parse("keep").unwrap(), Sizing::Keep);
    assert!(Sizing::parse("tiny").is_err());
    assert_eq!(
        FloppySize::parse("720K").unwrap(),
        FloppySize::Refit(737_280)
    );
    assert!(FloppySize::parse("999k").is_err());
}

#[test]
fn scan_mirrors_subfolders_and_skips_sidecars() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir(root.join("sub")).unwrap();
    for f in [
        "a.img",
        "game.cue",
        "game.bin",
        ".hidden",
        "notes.txt",
        "sub/b.img",
    ] {
        std::fs::write(root.join(f), b"x").unwrap();
    }
    let names = |v: Vec<std::path::PathBuf>| -> Vec<String> {
        v.iter()
            .map(|p| p.strip_prefix(root).unwrap().display().to_string())
            .collect()
    };
    assert_eq!(names(scan(root, false).unwrap()), ["a.img", "game.cue"]);
    assert_eq!(
        names(scan(root, true).unwrap()),
        ["a.img", "game.cue", "sub/b.img"]
    );
    let out = output_path(&root.join("sub/b.img"), root, Path::new("/out"), "vhd");
    assert_eq!(out, Path::new("/out/sub/b.vhd"));
}

#[test]
fn formats_say_why_a_source_does_not_fit() {
    let dir = tempfile::tempdir().unwrap();
    let f = classify(&floppy(dir.path(), "f.img", &[])).unwrap();
    assert!(Format::Woz.fits(&f, f.size).unwrap_err().contains("140K"));
    assert!(Format::Dc42.fits(&f, f.size).is_ok());
    assert!(Format::Vhd
        .fits(&f, f.size)
        .unwrap_err()
        .contains("not a format"));
    if cfg!(not(feature = "chd")) {
        assert_eq!(Format::ChdCd.unavailable(), Some(NO_CHD));
    }
}

#[test]
fn a_floppy_refits_to_720k_with_its_files_and_verifies() {
    let dir = tempfile::tempdir().unwrap();
    let src = floppy(
        dir.path(),
        "disk.img",
        &[("README.TXT", 2000), ("GAME.EXE", 300_000)],
    );
    let dest = dir.path().join("out/disk.img");
    let opts = Options {
        floppy_size: FloppySize::Refit(737_280),
        verify: true,
        ..Options::default()
    };
    let done = run(&src, &dest, &opts).unwrap();
    assert!(done.reshaped);
    assert!(done.verified.unwrap().contains("2 file(s)"));
    let out = std::fs::read(&dest).unwrap();
    assert_eq!(out.len(), 737_280);
    assert_eq!((out[21], out[24]), (0xF9, 9), "a real 720K BPB");

    let too_big = floppy(dir.path(), "full.img", &[("BIG.DAT", 900_000)]);
    let err = run(&too_big, &dir.path().join("out/full.img"), &opts).unwrap_err();
    assert!(
        format!("{err:#}").contains("won't fit on 720 KB"),
        "{err:#}"
    );
}

#[test]
fn a_floppy_changes_container_and_verifies_its_sectors() {
    let dir = tempfile::tempdir().unwrap();
    let src = floppy(dir.path(), "disk.img", &[("A.TXT", 10)]);
    let dest = dir.path().join("disk.d88");
    let opts = Options {
        floppy: (Format::D88, "d88".into()),
        verify: true,
        ..Options::default()
    };
    let done = run(&src, &dest, &opts).unwrap();
    assert!(!done.reshaped);
    assert!(done.verified.unwrap().starts_with("Verified: all"));
}

#[test]
fn a_hard_disk_shrinks_to_its_minimum_and_verifies() {
    let dir = tempfile::tempdir().unwrap();
    let src = mbr_disk(dir.path(), 40, 32);
    let dest = dir.path().join("small.img");
    let opts = Options {
        hard_disk: (Format::Raw, "img".into()),
        sizing: Sizing::Minimum,
        verify: true,
        ..Options::default()
    };
    let done = run(&src, &dest, &opts).unwrap();
    assert!(done.reshaped);
    assert!(done.verified.unwrap().contains("1 file(s)"));
    let len = std::fs::metadata(&dest).unwrap().len();
    assert!(len < 20 << 20, "shrunk: {len}");

    let qcow = dir.path().join("small.qcow2");
    let opts = Options {
        hard_disk: (Format::Qcow2, "qcow2".into()),
        sizing: Sizing::Extra(Extra::Percent(50)),
        verify: true,
        ..Options::default()
    };
    assert!(run(&src, &qcow, &opts).unwrap().verified.is_some());
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains("resizing"))
        .collect();
    assert!(leftovers.is_empty(), "the scratch VHD was removed");

    let same = dir.path().join("same.vhd");
    let opts = Options {
        verify: true,
        ..Options::default()
    };
    assert!(run(&src, &same, &opts)
        .unwrap()
        .verified
        .unwrap()
        .starts_with("Verified: all"));
}
