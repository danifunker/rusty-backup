//! Snapshot tests for `rb-cli --help` output, catching accidental
//! grammar drift. The asserts intentionally check *structure* (verb
//! names, flag names) rather than the full text — keeping the trip-wire
//! tight without churning on cosmetic copy edits.
//!
//! When a flag is renamed or a verb is added/removed, the corresponding
//! assertion fires and forces an explicit update. That's the point.

use std::path::PathBuf;
use std::process::Command;

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

fn help(args: &[&str]) -> String {
    let mut full = args.to_vec();
    full.push("--help");
    let out = Command::new(cli_bin())
        .args(&full)
        .output()
        .expect("spawn rb-cli --help");
    assert!(
        out.status.success(),
        "{:?} --help failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn top_level_verbs_present() {
    let h = help(&[]);
    // The complete Phase A verb list. Any rename / add / remove fires.
    for verb in &[
        "new",
        "ls",
        "put",
        "get",
        "cp",
        "rm",
        "mkdir",
        "fsck",
        "shrink",
        "inspect",
        "show",
        "optical",
        "resize",
        "expand",
        "convert",
        "terminal",
        "batch-template",
        "completions",
        "install-completions",
    ] {
        assert!(
            h.contains(verb),
            "top-level help missing verb {verb:?}:\n{h}"
        );
    }
}

#[test]
fn global_flags_present() {
    let h = help(&[]);
    for flag in &[
        "--log-level",
        "--quiet",
        "--progress",
        "--color",
        "--log-file",
    ] {
        assert!(h.contains(flag), "top-level help missing flag {flag:?}");
    }
}

#[test]
fn show_subcommands_present() {
    let h = help(&["show"]);
    for sub in &["partmap", "fs-info", "chd-info", "devices"] {
        assert!(h.contains(sub), "`show` help missing subcommand {sub:?}");
    }
}

#[test]
fn put_modes_present() {
    let h = help(&["put"]);
    for flag in &[
        "--zero",
        "--dst",
        "--boot",
        "--type",
        "--creator",
        "--force",
    ] {
        assert!(h.contains(flag), "`put` help missing flag {flag:?}");
    }
}

#[test]
fn fsck_modes_present() {
    let h = help(&["fsck"]);
    for flag in &["--checkonly", "--repair", "--prompt-timeout"] {
        assert!(h.contains(flag), "`fsck` help missing flag {flag:?}");
    }
}

#[test]
fn new_fs_choice_present() {
    let h = help(&["new"]);
    assert!(h.contains("--fs"), "`new` missing --fs");
    assert!(h.contains("hfs"), "`new` missing hfs in --fs values");
}
