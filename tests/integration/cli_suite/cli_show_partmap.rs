//! `rb-cli show partmap` against a wrapped image.
//!
//! The verb opened its argument with a plain `File::open`, so a container's own
//! bytes reached the APM parser: a `.zip` reported `bad DDR signature: 0x504B`
//! ("PK"), and a `.chd` failed the same way — while `inspect` on the identical
//! file worked, because it goes through `source_reader`. Real Mac disks arrive
//! wrapped far more often than raw.

use std::path::PathBuf;
use std::process::Command;

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

fn run(args: &[&str]) -> String {
    let out = Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    if !out.status.success() {
        panic!(
            "command {args:?} failed: status={:?}\nstdout:\n{stdout}\nstderr:\n{}",
            out.status,
            String::from_utf8_lossy(&out.stderr),
        );
    }
    stdout
}

#[test]
#[cfg(feature = "chd")]
fn show_partmap_reads_through_a_container() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("apm.img");
    let outdir = dir.path().join("out");
    let img_s = img.to_str().unwrap();
    let outdir_s = outdir.to_str().unwrap();

    run(&[
        "new",
        "hd",
        "apm",
        img_s,
        "--size",
        "8M",
        "--partition",
        "rest:Apple_HFS:Test",
    ]);
    let raw = run(&["show", "partmap", img_s]);
    assert!(raw.contains("Apple_HFS"), "raw image partmap: {raw}");

    // Same disk, wrapped. A CHD starts with its own magic, so this only parses
    // if the verb peels the container first.
    run(&["convert", img_s, outdir_s, "--format", "chd"]);
    let chd = outdir.join("apm.chd");
    assert!(chd.is_file(), "convert produced no {}", chd.display());
    let wrapped = run(&["show", "partmap", chd.to_str().unwrap()]);

    assert!(
        wrapped.contains("Apple_HFS") && wrapped.contains("Apple_partition_map"),
        "partmap through a CHD lost the map: {wrapped}",
    );
    assert_eq!(
        raw, wrapped,
        "the same disk must decode identically raw and wrapped",
    );
}
