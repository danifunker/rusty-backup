//! End-to-end tests for `rb-cli build`, the command-line form of the Build Disk tab: each
//! build is checked by fsck on every partition it formatted.

use std::path::PathBuf;
use std::process::Command;

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

fn run_raw(args: &[&str]) -> (bool, String) {
    let out = Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

fn run(args: &[&str]) -> String {
    let (ok, text) = run_raw(args);
    assert!(ok, "command {args:?} failed:\n{text}");
    text
}

fn fsck_clean(target: &str) {
    let out = run(&["fsck", target]);
    assert!(!out.to_lowercase().contains("error:"), "{target}: {out}");
}

#[test]
fn an_mbr_disk_formats_each_partition() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("pc.img");
    let s = img.to_str().unwrap();
    run(&[
        "build",
        s,
        "--size",
        "200M",
        "--table",
        "mbr",
        "--partition",
        "60M",
        "--partition",
        "rest",
        "--format",
        "1=fat:DOS",
        "--format",
        "2=ext4:linux",
    ]);
    let info = run(&["inspect", s]);
    assert!(info.contains("Partition table: MBR"), "{info}");
    fsck_clean(&format!("{s}@1"));
    fsck_clean(&format!("{s}@2"));
    let ls = run(&["ls", &format!("{s}@1")]);
    assert!(!ls.contains("error"), "{ls}");
}

#[test]
fn an_rdb_disk_formats_pfs3_and_sfs() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("amiga.img");
    let s = img.to_str().unwrap();
    run(&[
        "build",
        s,
        "--size",
        "120M",
        "--table",
        "rdb",
        "--partition",
        "40M",
        "--partition",
        "rest",
        "--format",
        "1=pfs3:Work",
        "--format",
        "2=sfs:Data",
    ]);
    let info = run(&["inspect", s]);
    assert!(info.contains("PFS"), "{info}");
    assert!(info.contains("SFS"), "{info}");
    fsck_clean(&format!("{s}@1"));
    fsck_clean(&format!("{s}@2"));
}

#[test]
fn a_bare_hpfs_volume_builds_without_a_table() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("os2.img");
    let s = img.to_str().unwrap();
    run(&[
        "build", s, "--size", "64M", "--fs", "hpfs", "--label", "OS2",
    ]);
    assert_eq!(std::fs::metadata(&img).unwrap().len(), 64 << 20);
    fsck_clean(s);
}

#[test]
fn refusals_name_the_reason() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("x.img");
    let s = img.to_str().unwrap();
    let (ok, out) = run_raw(&["build", s, "--size", "64M", "--fs", "prodos"]);
    assert!(!ok && out.contains("ProDOS"), "{out}");
    std::fs::write(&img, b"keep").unwrap();
    let (ok, out) = run_raw(&["build", s, "--size", "8M", "--fs", "fat"]);
    assert!(!ok && out.contains("--force"), "{out}");
    let (ok, out) = run_raw(&[
        "build", s, "--force", "--size", "64M", "--table", "mbr", "--format", "2=fat",
    ]);
    assert!(!ok && out.contains("only 1"), "{out}");
}

/// The target's extension picks the container; each one opens through Inspect.
#[test]
fn containers_follow_the_extension_and_open() {
    let dir = tempfile::tempdir().unwrap();
    for (name, magic, at_end) in [
        ("disk.chd", &b"MComprHD"[..], false),
        ("disk.vhd", &b"conectix"[..], true),
        ("disk.qcow2", &b"QFI\xfb"[..], false),
    ] {
        let img = dir.path().join(name);
        let s = img.to_str().unwrap();
        run(&[
            "build",
            s,
            "--size",
            "32M",
            "--table",
            "mbr",
            "--format",
            "1=fat:CONT",
        ]);
        let bytes = std::fs::read(&img).unwrap();
        let found = if at_end {
            bytes[bytes.len() - 512..].starts_with(magic)
        } else {
            bytes.starts_with(magic)
        };
        assert!(found, "{name} has no {magic:?} header");
        let info = run(&["inspect", s]);
        assert!(info.contains("MBR"), "{name}: {info}");
        assert!(
            !dir.path().join(format!("{name}.rbtmp.img")).exists(),
            "the temp raw is gone"
        );
    }
}

#[test]
fn a_floppy_container_is_refused_for_a_disk_it_cannot_hold() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("big.woz");
    let (ok, out) = run_raw(&[
        "build",
        img.to_str().unwrap(),
        "--size",
        "16M",
        "--fs",
        "prodos",
    ]);
    assert!(!ok && out.contains("WOZ"), "{out}");
    let dc = dir.path().join("mac.dc42");
    run(&[
        "build",
        dc.to_str().unwrap(),
        "--size",
        "800K",
        "--fs",
        "hfs",
        "--label",
        "Mac",
    ]);
    let info = run(&["inspect", dc.to_str().unwrap()]);
    assert!(info.contains("HFS"), "{info}");
}

#[test]
fn bootable_sets_the_mbr_active_flag() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("boot.img");
    let s = img.to_str().unwrap();
    run(&[
        "build",
        s,
        "--size",
        "64M",
        "--table",
        "mbr",
        "--partition",
        "20M",
        "--partition",
        "rest",
        "--format",
        "1=fat",
        "--bootable",
        "1",
    ]);
    let b = std::fs::read(&img).unwrap();
    assert_eq!(b[0x1BE], 0x80, "partition 1 active");
    assert_eq!(b[0x1CE], 0x00, "partition 2 not");
}

/// An RDB build embeds the handler (an FSHD block) and flags the boot partition.
#[test]
fn an_rdb_build_embeds_its_handler_and_boot_flag() {
    let dir = tempfile::tempdir().unwrap();
    let handler = dir.path().join("PFS3");
    // The shortest AmigaDOS load file: HUNK_HEADER and nothing a strap would run here.
    let mut load = vec![0x00, 0x00, 0x03, 0xF3];
    load.extend_from_slice(&[0u8; 60]);
    std::fs::write(&handler, &load).unwrap();
    let img = dir.path().join("amiga.img");
    let s = img.to_str().unwrap();
    let spec = format!("PFS\\3={}", handler.display());
    run(&[
        "build",
        s,
        "--size",
        "80M",
        "--table",
        "rdb",
        "--partition",
        "rest",
        "--format",
        "1=pfs3:Work",
        "--bootable",
        "1",
        "--filesystem",
        spec.as_str(),
    ]);
    let b = std::fs::read(&img).unwrap();
    let head = &b[..256 * 512];
    assert!(head.windows(4).any(|w| w == b"FSHD"), "no FileSystemHeader");
    let part = head.chunks(512).find(|blk| &blk[0..4] == b"PART").unwrap();
    assert_eq!(
        u32::from_be_bytes(part[20..24].try_into().unwrap()) & 1,
        1,
        "bootable"
    );
    fsck_clean(&format!("{s}@1"));
    let (ok, out) = run_raw(&[
        "build",
        s,
        "--force",
        "--size",
        "80M",
        "--table",
        "rdb",
        "--filesystem",
        format!("PFS\\3={}", img.display()).as_str(),
    ]);
    assert!(!ok && out.contains("AmigaDOS load file"), "{out}");
}

/// The presets make the disks `new hd x68k` / `new hd sgi-efs` make.
#[test]
fn presets_match_the_new_hd_builders() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();
    run(&["build", &p("a.hds"), "--size", "40M", "--preset", "x68k"]);
    run(&["new", "hd", "x68k", &p("b.hds"), "--size", "40M"]);
    run(&[
        "build",
        &p("a.img"),
        "--size",
        "64M",
        "--preset",
        "sgi-efs",
        "--label",
        "irix",
    ]);
    run(&[
        "new",
        "hd",
        "sgi-efs",
        &p("b.img"),
        "--size",
        "64M",
        "--name",
        "irix",
    ]);
    for (a, b) in [("a.hds", "b.hds"), ("a.img", "b.img")] {
        let la = std::fs::metadata(p(a)).unwrap().len();
        assert_eq!(la, std::fs::metadata(p(b)).unwrap().len(), "{a}");
        let ia = run(&["inspect", &p(a)]).replace(&p(a), "X");
        let ib = run(&["inspect", &p(b)]).replace(&p(b), "X");
        assert_eq!(ia, ib, "{a} vs {b}");
        fsck_clean(&format!("{}@1", p(a)));
    }
    let (ok, out) = run_raw(&["build", &p("c.hds"), "--size", "1500K", "--preset", "x68k"]);
    assert!(!ok && out.contains("MiB"), "{out}");
}

/// A bigger SGI disk from a small one: the volume header rides over, resized, and the EFS root
/// is copied into the grown slot.
#[test]
fn an_sgi_donor_lays_out_a_bigger_disk() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();
    run(&[
        "new",
        "hd",
        "sgi-efs",
        &p("small.img"),
        "--size",
        "64M",
        "--name",
        "irix",
    ]);
    std::fs::write(p("motd"), b"welcome\n").unwrap();
    run(&["put", &format!("{}@1", p("small.img")), &p("motd"), "/motd"]);
    let before = run(&["inspect", &p("small.img")]);
    run(&[
        "build",
        &p("big.img"),
        "--size",
        "128M",
        "--from",
        &p("small.img"),
        "--partition",
        "rest",
    ]);
    let after = run(&["inspect", &p("big.img")]);
    assert!(after.contains("SGI"), "{after}");
    // VOLUME (slot 10) spans the new disk in whole cylinders; the EFS slot grew to the rest.
    assert!(after.contains("262080"), "{after}");
    assert_eq!(std::fs::metadata(p("big.img")).unwrap().len(), 128 << 20);
    let count = |s: &str| s.matches("EFS").count();
    assert_eq!(count(&before), count(&after), "{before}\n---\n{after}");
    let ls = run(&["ls", &format!("{}@1", p("big.img"))]);
    assert!(ls.contains("motd"), "{ls}");
}

/// An MBR donor: partition 1 is copied, partition 2 formatted afresh at a new size.
#[test]
fn an_mbr_donor_copies_one_partition_and_formats_another() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();
    run(&[
        "build",
        &p("a.img"),
        "--size",
        "64M",
        "--table",
        "mbr",
        "--partition",
        "20M",
        "--partition",
        "rest",
        "--format",
        "1=fat:KEEP",
        "--format",
        "2=fat:OLD",
    ]);
    std::fs::write(p("host"), b"m900\n").unwrap();
    run(&["put", &format!("{}@1", p("a.img")), &p("host"), "/HOST.TXT"]);
    run(&[
        "build",
        &p("b.img"),
        "--size",
        "200M",
        "--from",
        &p("a.img"),
        "--partition",
        "20M",
        "--partition",
        "rest",
        "--format",
        "2=ext4:NEW",
    ]);
    assert!(run(&["ls", &format!("{}@1", p("b.img"))]).contains("HOST.TXT"));
    fsck_clean(&format!("{}@2", p("b.img")));
    let info = run(&["inspect", &p("b.img")]);
    assert!(
        info.contains("Linux"),
        "the type follows the new filesystem: {info}"
    );
}

/// A copied filesystem grows to its bigger row: a file too big for the donor's volume fits.
#[test]
fn a_copied_filesystem_grows_to_its_row() {
    // AFFS stops at the 25 bitmap pages its root block holds, about 49 MiB.
    for (table, fs, disk, file) in [
        ("mbr", "fat", "128M", 64),
        ("mbr", "ext4", "128M", 64),
        ("mbr", "ntfs", "128M", 64),
        ("rdb", "affs", "48M", 36),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();
        run(&[
            "build",
            &p("a.img"),
            "--size",
            "32M",
            "--table",
            table,
            "--partition",
            "rest",
            "--format",
            &format!("1={fs}:KEEP"),
        ]);
        std::fs::write(p("host"), b"m900\n").unwrap();
        run(&["put", &format!("{}@1", p("a.img")), &p("host"), "/HOST.TXT"]);
        run(&[
            "build",
            &p("b.img"),
            "--size",
            disk,
            "--from",
            &p("a.img"),
            "--partition",
            "rest",
        ]);
        let vol = format!("{}@1", p("b.img"));
        assert!(run(&["ls", &vol]).contains("HOST.TXT"), "{fs}");
        fsck_clean(&vol);
        std::fs::write(p("big"), vec![0x5A; file * 1024 * 1024]).unwrap();
        run(&["put", &vol, &p("big"), "/BIG.BIN"]);
        fsck_clean(&vol);
    }
}

/// exFAT's FAT is fixed at format, so a copy grows only as far as it reaches, and warns.
#[test]
fn a_copied_exfat_stays_clean_and_warns_when_its_fat_is_full() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();
    run(&[
        "build",
        &p("a.img"),
        "--size",
        "32M",
        "--table",
        "mbr",
        "--partition",
        "rest",
        "--format",
        "1=exfat:KEEP",
    ]);
    assert!(run(&["ls", &format!("{}@1", p("a.img"))]).contains("0x07"));
    let out = run(&[
        "build",
        &p("b.img"),
        "--size",
        "128M",
        "--from",
        &p("a.img"),
        "--partition",
        "rest",
    ]);
    assert!(out.contains("cannot grow"), "{out}");
    fsck_clean(&format!("{}@1", p("b.img")));
}

/// HFS can't change its block size in place, so a copy past its limit stops with the reason.
#[test]
fn a_copied_hfs_past_its_block_limit_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();
    run(&[
        "build",
        &p("a.img"),
        "--size",
        "32M",
        "--table",
        "apm",
        "--partition",
        "rest",
        "--format",
        "1=hfs:KEEP",
    ]);
    let (ok, text) = run_raw(&[
        "build",
        &p("b.img"),
        "--size",
        "128M",
        "--from",
        &p("a.img"),
        "--partition",
        "rest",
    ]);
    assert!(!ok && text.contains("HFS caps at"), "{text}");
}
