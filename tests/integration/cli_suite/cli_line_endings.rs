//! `rb-cli import --line-endings`: text files lose their CRLFs, a binary file keeps its bytes.

use std::path::PathBuf;
use std::process::Command;

fn cli() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

fn rb(args: &[&str]) -> String {
    let out = Command::new(cli()).args(args).output().expect("run rb-cli");
    assert!(
        out.status.success(),
        "rb-cli {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn import_line_endings_unix_converts_text_and_leaves_binaries() {
    let tmp = tempfile::tempdir().unwrap();
    let img = tmp.path().join("disk.img");
    std::fs::write(
        &img,
        fs_pc::fat::create_blank_fat(8 * 1024 * 1024, Some("LE")).unwrap(),
    )
    .unwrap();
    let src = tmp.path().join("src");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("readme.txt"), b"one\r\ntwo\r\n").unwrap();
    let bin = b"\x00\r\n\xff\r\n".repeat(50);
    std::fs::write(src.join("data.bin"), &bin).unwrap();

    let img_s = img.to_str().unwrap();
    let log = rb(&[
        "import",
        img_s,
        src.to_str().unwrap(),
        "--line-endings",
        "unix",
    ]);
    assert!(
        log.contains("1 text file(s) had their line endings converted"),
        "{log}"
    );

    let out = tmp.path().join("out");
    std::fs::create_dir(&out).unwrap();
    for name in ["readme.txt", "data.bin"] {
        rb(&[
            "get",
            img_s,
            &format!("/{name}"),
            out.join(name).to_str().unwrap(),
        ]);
    }
    assert_eq!(
        std::fs::read(out.join("readme.txt")).unwrap(),
        b"one\ntwo\n"
    );
    assert_eq!(std::fs::read(out.join("data.bin")).unwrap(), bin);

    // An unknown mode is refused by the argument parser.
    let bad = Command::new(cli())
        .args([
            "import",
            img_s,
            src.to_str().unwrap(),
            "--line-endings",
            "vms",
        ])
        .output()
        .unwrap();
    assert!(!bad.status.success());
}
