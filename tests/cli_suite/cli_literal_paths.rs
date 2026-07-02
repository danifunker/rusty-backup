//! End-to-end tests for the `--literal` flag on the in-image path verbs.
//!
//! Names containing glob metacharacters (`[ ] { } * ?`) are legal on classic
//! HFS but are otherwise mis-parsed as glob patterns. `--literal` addresses
//! such a path verbatim. These drive the `rb-cli` binary against a scratch HFS
//! image, the same path scripted consumers (e.g. MacAtrium's harvest) hit.

use std::path::PathBuf;
use std::process::Command;

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

/// Run rb-cli, panicking with full output on a non-zero exit.
fn run(args: &[&str]) -> std::process::Output {
    let out = Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli");
    if !out.status.success() {
        panic!(
            "command {args:?} failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
    }
    out
}

/// Run rb-cli without asserting success — for the negative cases.
fn try_run(args: &[&str]) -> std::process::Output {
    Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli")
}

fn stdout_of(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn literal_addresses_paths_with_glob_metacharacters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();

    // A directory whose own name contains `[` and `]`, holding one file for
    // each remaining metacharacter: `{ } * ?`.
    let parent = "/Stuff ][ 1";
    let names = ["file[a].txt", "cur{b}.txt", "star*.txt", "quest?.txt"];

    run(&["new", "--fs", "hfs", img_s, "--size", "4M", "--name", "Lit"]);

    // mkdir / put are always-literal, so they create the metachar names
    // verbatim (and accept --literal for scripting symmetry).
    run(&["mkdir", "--literal", img_s, parent]);
    let host = dir.path().join("payload.bin");
    std::fs::write(&host, b"payload-bytes").unwrap();
    for name in names {
        run(&[
            "put",
            img_s,
            host.to_str().unwrap(),
            &format!("{parent}/{name}"),
        ]);
    }

    // 1. `ls --literal <dir>` lists the directory's contents verbatim.
    let ls = run(&["ls", "--literal", img_s, parent]);
    let ls_text = stdout_of(&ls);
    for name in names {
        assert!(
            ls_text.contains(name),
            "ls --literal should list {name:?}:\n{ls_text}"
        );
    }

    // 2. Without --literal the same path is a broken glob (`][` is an unclosed
    //    character class) and the command fails — this is exactly what the flag
    //    rescues.
    let bad = try_run(&["ls", img_s, parent]);
    assert!(
        !bad.status.success(),
        "unflagged ls of a `][` path should fail"
    );
    assert!(
        String::from_utf8_lossy(&bad.stderr).contains("glob"),
        "error should mention the glob parse failure"
    );

    // 3. `get --literal` extracts an exact file whose name has `{ }`.
    let got = dir.path().join("got.bin");
    run(&[
        "get",
        "--literal",
        img_s,
        &format!("{parent}/cur{{b}}.txt"),
        got.to_str().unwrap(),
    ]);
    assert_eq!(std::fs::read(&got).unwrap(), b"payload-bytes");

    // 4. `get-binhex --literal` is accepted (it is always literal) and extracts
    //    the `[ ]`-named file the harvest pipeline cares about.
    let hqx = dir.path().join("out.hqx");
    run(&[
        "get-binhex",
        "--literal",
        img_s,
        &format!("{parent}/file[a].txt"),
        hqx.to_str().unwrap(),
    ]);
    assert!(hqx.exists() && std::fs::metadata(&hqx).unwrap().len() > 0);

    // 5. `rm --literal` deletes exactly the `*`-named file, leaving the rest.
    run(&["rm", "--literal", img_s, &format!("{parent}/star*.txt")]);
    let after = stdout_of(&run(&["ls", "--literal", img_s, parent]));
    assert!(
        !after.contains("star*.txt"),
        "star*.txt should be gone:\n{after}"
    );
    assert!(
        after.contains("quest?.txt"),
        "quest?.txt should remain:\n{after}"
    );
    assert!(
        after.contains("file[a].txt"),
        "file[a].txt should remain:\n{after}"
    );

    // 6. Default glob behaviour is unchanged — a `/**` walk still expands.
    let walk = stdout_of(&run(&["ls", img_s, "/**"]));
    assert!(
        walk.contains(parent),
        "default glob walk should still list {parent:?}:\n{walk}"
    );
}

#[test]
fn literal_conflicts_with_exclude() {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join("disk.dsk");
    let img_s = img.to_str().unwrap();
    run(&["new", "--fs", "hfs", img_s, "--size", "4M", "--name", "Lit"]);

    // --literal addresses one exact path; --exclude only makes sense for a
    // glob walk, so the two are mutually exclusive.
    let out = try_run(&["ls", "--literal", "--exclude", "/x", img_s, "/"]);
    assert!(
        !out.status.success(),
        "--literal with --exclude should be rejected"
    );
}
