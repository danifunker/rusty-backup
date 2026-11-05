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
