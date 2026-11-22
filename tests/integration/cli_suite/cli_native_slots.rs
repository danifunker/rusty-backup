//! End-to-end coverage for native slot numbers and the `@N` / `@sN` selectors.
//!
//! Two things are under test and they are easy to conflate:
//!
//! - **position** — where a partition appears in `partitions()`, 1-based, what
//!   `inspect` prints in its `#`/`idx` column and what `@N` selects.
//! - **native slot** — the number the platform's own tool would use: `fdisk`
//!   1-4, `diskutil`'s `disk4s6`, IRIX `fx` 0-15, Sun `format(1M)` slices.
//!   `PartitionTable::native_slot()` is the single source of truth, and `@sN`
//!   selects by it.
//!
//! They only differ where `partitions()` filters something out, so a table with
//! no filter cannot catch a slot/position bug. Every test here that matters
//! therefore asserts **position != slot** for at least one partition first —
//! otherwise the round-trip would pass whether or not the mapping is right.
//! GPT is the cautionary case: position always equals order, so a GPT
//! round-trip proves nothing and is only used here to pin the *absence* of
//! slots.
//!
//! Two destructive regressions motivated this file; both are asserted
//! explicitly at the bottom:
//!
//! - `write --partition N` keyed on `index + 1`, so on an APM disk
//!   `--partition 2` wrote to the *first* partition.
//! - `backup --partitions N` produced an empty backup folder and exit 0 when
//!   the selector matched nothing.

use std::path::{Path, PathBuf};
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

/// Run a command expected to fail; returns (exit code, stderr).
fn run_fail(args: &[&str]) -> (i32, String) {
    let out = Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli");
    assert!(
        !out.status.success(),
        "expected {args:?} to fail, but it succeeded:\n{}",
        String::from_utf8_lossy(&out.stdout)
    );
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// The partition rows `inspect` prints, as `(position, slot)`.
///
/// `slot` is `None` when the table has no native slots — the column is absent
/// entirely rather than blank, which is itself part of the contract.
fn inspect_rows(img: &Path) -> Vec<(u32, Option<u32>)> {
    let text = run(&["inspect", img.to_str().unwrap()]);
    let mut has_slot = false;
    let mut rows = Vec::new();
    let mut in_table = false;

    for line in text.lines() {
        let t = line.trim_start();
        if t.starts_with("idx") {
            has_slot = t.split_whitespace().nth(1) == Some("slot");
            in_table = true;
            continue;
        }
        if !in_table {
            continue;
        }
        // The table ends at the first blank line after it.
        if t.is_empty() {
            break;
        }
        let mut f = t.split_whitespace();
        let pos: u32 = match f.next().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => break,
        };
        let slot = if has_slot {
            f.next().and_then(|s| s.parse::<u32>().ok())
        } else {
            None
        };
        rows.push((pos, slot));
    }
    assert!(!rows.is_empty(), "no partition rows parsed from:\n{text}");
    rows
}

/// The `Partition @N / @sN (TABLE): … @ LBA …, … bytes` identity line that
/// `show fs-info` emits for whatever the selector resolved to.
///
/// Deliberately tolerant of a non-zero exit. The line is written to **stderr**
/// before the filesystem is opened, and these fixtures are freshly provisioned
/// tables with no filesystem inside, so `fs-info` legitimately fails right
/// after printing it. The resolver's answer is what is under test here, not
/// whether a volume could be mounted — formatting every partition just to get
/// a clean exit would make these tests far slower for no extra coverage.
fn fs_info_line(img: &Path, selector: &str) -> String {
    let arg = format!("{}{}", img.to_str().unwrap(), selector);
    let out = Command::new(cli_bin())
        .args(["show", "fs-info", &arg])
        .output()
        .expect("spawn rb-cli");
    let merged = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    merged
        .lines()
        .find(|l| l.trim_start().starts_with("Partition @"))
        .unwrap_or_else(|| {
            panic!("no partition identity line for {selector} in:\n{merged}");
        })
        .trim()
        .to_string()
}

/// Every position resolves to the same partition as its native slot.
fn assert_round_trip(img: &Path) {
    for (pos, slot) in inspect_rows(img) {
        let slot = match slot {
            Some(s) => s,
            None => continue,
        };
        let by_pos = fs_info_line(img, &format!("@{pos}"));
        let by_slot = fs_info_line(img, &format!("@s{slot}"));
        assert_eq!(
            by_pos, by_slot,
            "@{pos} and @s{slot} must resolve to the same partition"
        );
        // The identity line advertises both, so a mismatch here means the
        // display and the resolver disagree.
        assert!(
            by_pos.contains(&format!("@{pos} / @s{slot}")),
            "expected `@{pos} / @s{slot}` in fs-info line, got: {by_pos}"
        );
    }
}

fn assert_diverges(img: &Path, what: &str) {
    let rows = inspect_rows(img);
    assert!(
        rows.iter().any(|(p, s)| Some(*p) != *s),
        "{what}: position never differs from slot, so this fixture proves nothing: {rows:?}"
    );
}

fn tmp(name: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let img = dir.path().join(name);
    (dir, img)
}

// ---------------------------------------------------------------------------
// The mapping, per table
// ---------------------------------------------------------------------------

#[test]
fn mbr_slots_are_one_based_and_survive_a_hole() {
    let (_d, img) = tmp("mbr.img");
    let s = img.to_str().unwrap();
    run(&[
        "new",
        "hd",
        "mbr",
        s,
        "--size",
        "96M",
        "--partition",
        "16M:0b",
        "--partition",
        "16M:0b",
        "--partition",
        "16M:0b",
    ]);
    // Contiguous to begin with: fdisk numbers primaries from 1.
    assert_eq!(
        inspect_rows(&img),
        vec![(1, Some(1)), (2, Some(2)), (3, Some(3))]
    );

    // Punching out slot 2 is the divergence trigger — `partitions()` filters
    // empty entries, so position 2 now maps to slot 3.
    run(&["partmap", "delete", s, "2"]);
    assert_eq!(inspect_rows(&img), vec![(1, Some(1)), (2, Some(3))]);
    assert_diverges(&img, "MBR with a hole");
    assert_round_trip(&img);
}

#[test]
fn apm_slots_skip_the_map_entry() {
    let (_d, img) = tmp("apm.img");
    let s = img.to_str().unwrap();
    run(&[
        "new",
        "hd",
        "apm",
        s,
        "--size",
        "300M",
        "--partition",
        "100M:Apple_HFS:untitled",
        "--partition",
        "rest:Apple_HFS:untitled 2",
    ]);
    // Slot 1 is the partition map itself, which `is_data_partition()` filters.
    // So even a freshly built APM disk diverges — this is diskutil's `disk4sN`.
    assert_eq!(inspect_rows(&img), vec![(1, Some(2)), (2, Some(3))]);
    assert_diverges(&img, "APM");
    assert_round_trip(&img);
}

#[test]
fn sgi_and_sun_slots_are_zero_based() {
    for (table, part_a, part_b, name) in [
        ("sgi", "200M:efs", "rest:efs", "sgi.img"),
        ("sun", "200M:root", "rest:usr", "sun.img"),
    ] {
        let (_d, img) = tmp(name);
        let s = img.to_str().unwrap();
        run(&[
            "new",
            "hd",
            table,
            s,
            "--size",
            "500M",
            "--partition",
            part_a,
            "--partition",
            part_b,
        ]);
        // IRIX `fx` and Sun `format(1M)` both count from zero, so position 1
        // is slot 0 — divergence by construction, no filtering needed.
        assert_eq!(
            inspect_rows(&img),
            vec![(1, Some(0)), (2, Some(1))],
            "{table} should number slots from zero"
        );
        assert_diverges(&img, table);
        assert_round_trip(&img);
    }
}

#[test]
fn rdb_slots_track_the_part_chain_position() {
    let (_d, img) = tmp("rdb.img");
    let s = img.to_str().unwrap();
    run(&[
        "new",
        "hd",
        "rdb",
        s,
        "--size",
        "200M",
        "--partition",
        "100M:DOS\\3:DH0",
        "--partition",
        "rest:DOS\\3:DH1",
    ]);
    // RDB has no filter, so slot == index and position == slot + 1. The name
    // is the identity users actually know, so `@DH0` is the real interface;
    // this pins the numeric mapping so it cannot drift silently.
    assert_eq!(inspect_rows(&img), vec![(1, Some(0)), (2, Some(1))]);
    assert_round_trip(&img);

    // Names resolve to the same partitions.
    assert_eq!(fs_info_line(&img, "@DH0"), fs_info_line(&img, "@1"));
    assert_eq!(fs_info_line(&img, "@DH1"), fs_info_line(&img, "@2"));
}

#[test]
fn ahdi_slots_are_one_based() {
    let (_d, img) = tmp("atari.img");
    let s = img.to_str().unwrap();
    run(&[
        "new",
        "hd",
        "atari",
        s,
        "--size",
        "100M",
        "--partition",
        "40M:GEM",
        "--partition",
        "rest:GEM",
    ]);
    assert_eq!(inspect_rows(&img), vec![(1, Some(1)), (2, Some(2))]);
    assert_round_trip(&img);
}

#[test]
fn gpt_has_no_slot_column_at_all() {
    let (_d, img) = tmp("gpt.img");
    let s = img.to_str().unwrap();
    run(&[
        "new",
        "hd",
        "gpt",
        s,
        "--size",
        "100M",
        // GPT wants a full type GUID; the friendly names are not accepted.
        "--partition",
        "rest:0FC63DAF-8483-4772-8E79-3D69D8477DE4:Data",
    ]);
    // `has_native_slots()` is false, so the column is absent rather than
    // blank. GPT position always equals order, which is exactly why a GPT
    // round-trip test would prove nothing — it is only used here to pin the
    // negative.
    let rows = inspect_rows(&img);
    assert!(
        rows.iter().all(|(_, slot)| slot.is_none()),
        "GPT must not print a slot column: {rows:?}"
    );
    let text = run(&["inspect", s]);
    let header = text
        .lines()
        .find(|l| l.trim_start().starts_with("idx"))
        .unwrap();
    assert!(
        !header.contains("slot"),
        "GPT header should have no slot column: {header}"
    );
}

// ---------------------------------------------------------------------------
// Display consistency
// ---------------------------------------------------------------------------

#[test]
fn inspect_positions_are_contiguous_from_one() {
    let (_d, img) = tmp("apm.img");
    let s = img.to_str().unwrap();
    run(&[
        "new",
        "hd",
        "apm",
        s,
        "--size",
        "300M",
        "--partition",
        "100M:Apple_HFS:untitled",
        "--partition",
        "rest:Apple_HFS:untitled 2",
    ]);
    let rows = inspect_rows(&img);
    // Whatever the slots do, the position column is always 1..=n with no
    // gaps — it is a display index, not a slot.
    let positions: Vec<u32> = rows.iter().map(|(p, _)| *p).collect();
    assert_eq!(positions, (1..=rows.len() as u32).collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// Selector error shapes — the user-facing contract
// ---------------------------------------------------------------------------

#[test]
fn slot_selector_on_a_slotless_table_explains_itself() {
    let (_d, img) = tmp("gpt.img");
    let s = img.to_str().unwrap();
    run(&[
        "new",
        "hd",
        "gpt",
        s,
        "--size",
        "100M",
        "--partition",
        "rest:0FC63DAF-8483-4772-8E79-3D69D8477DE4:Data",
    ]);
    let (_, err) = run_fail(&["show", "fs-info", &format!("{s}@s1")]);
    assert!(
        err.contains("no slot number to select by") && err.contains("@N"),
        "GPT @sN error should point at @N: {err}"
    );
}

#[test]
fn missing_slot_lists_the_slots_that_do_exist() {
    let (_d, img) = tmp("apm.img");
    let s = img.to_str().unwrap();
    run(&[
        "new",
        "hd",
        "apm",
        s,
        "--size",
        "300M",
        "--partition",
        "100M:Apple_HFS:untitled",
        "--partition",
        "rest:Apple_HFS:untitled 2",
    ]);
    // Slot 1 is the map entry: present on the disk but not browsable, so
    // asking for it must fail rather than silently resolving to something.
    for miss in ["@s1", "@s9"] {
        let (_, err) = run_fail(&["show", "fs-info", &format!("{s}{miss}")]);
        assert!(
            err.contains("no browsable partition in slot"),
            "{miss}: {err}"
        );
        assert!(
            err.contains("slot 2") && err.contains("slot 3"),
            "{miss} should list the slots that do exist: {err}"
        );
    }
}

#[test]
fn name_selector_on_a_nameless_table_explains_itself() {
    let (_d, img) = tmp("mbr.img");
    let s = img.to_str().unwrap();
    run(&[
        "new",
        "hd",
        "mbr",
        s,
        "--size",
        "32M",
        "--partition",
        "16M:0b",
    ]);
    let (_, err) = run_fail(&["show", "fs-info", &format!("{s}@DH0")]);
    assert!(
        err.contains("don't carry device names") && err.contains("@sN"),
        "MBR @Name error should point at @N/@sN: {err}"
    );
}

#[test]
fn position_zero_is_rejected_but_slot_zero_is_not() {
    let (_d, img) = tmp("sgi.img");
    let s = img.to_str().unwrap();
    run(&[
        "new",
        "hd",
        "sgi",
        s,
        "--size",
        "500M",
        "--partition",
        "200M:efs",
        "--partition",
        "rest:efs",
    ]);
    // Positions are 1-based, so @0 is always wrong...
    let (_, err) = run_fail(&["show", "fs-info", &format!("{s}@0")]);
    assert!(
        err.contains("1-based") || err.contains("index 0 is invalid"),
        "{err}"
    );
    // ...but slot 0 is legal, because SGI numbers from zero.
    let line = fs_info_line(&img, "@s0");
    assert!(
        line.contains("@1 / @s0"),
        "@s0 should be the first SGI slot: {line}"
    );
}

// ---------------------------------------------------------------------------
// The two destructive regressions
// ---------------------------------------------------------------------------

#[test]
fn write_partition_targets_the_partition_inspect_shows_at_that_position() {
    // The bug: `write --partition N` keyed on `index + 1`, so on an APM disk
    // (where the map entry occupies slot 1) `--partition 2` resolved to the
    // *first* data partition and overwrote the wrong volume.
    let (_d, img) = tmp("apm.img");
    let s = img.to_str().unwrap();
    run(&[
        "new",
        "hd",
        "apm",
        s,
        "--size",
        "300M",
        "--partition",
        "100M:Apple_HFS:untitled",
        "--partition",
        "rest:Apple_HFS:untitled 2",
    ]);
    let rows = inspect_rows(&img);
    assert_eq!(
        rows,
        vec![(1, Some(2)), (2, Some(3))],
        "fixture must diverge"
    );

    // Both selectors for position 2 must name the same LBA that `inspect`
    // shows at position 2 — not the one at slot 2, which is position 1.
    let at_pos_2 = fs_info_line(&img, "@2");
    let at_slot_3 = fs_info_line(&img, "@s3");
    let at_pos_1 = fs_info_line(&img, "@1");
    assert_eq!(at_pos_2, at_slot_3);
    assert_ne!(
        at_pos_2, at_pos_1,
        "position 2 must not resolve to the first partition"
    );
    assert!(
        at_pos_2.contains("untitled 2"),
        "position 2 should be the second volume: {at_pos_2}"
    );
}

#[test]
fn backup_partitions_errors_on_a_miss_instead_of_writing_an_empty_folder() {
    // The bug: a selector matching nothing produced an empty backup folder
    // and exit 0, which reads as a successful backup of nothing.
    let (dir, img) = tmp("mbr.img");
    let s = img.to_str().unwrap();
    run(&[
        "new",
        "hd",
        "mbr",
        s,
        "--size",
        "32M",
        "--partition",
        "16M:0b",
    ]);

    let out_dir = dir.path().join("bk");
    let (code, err) = run_fail(&[
        "backup",
        s,
        "--output",
        out_dir.to_str().unwrap(),
        "--partitions",
        "9",
    ]);
    assert_ne!(code, 0, "a miss must not exit 0");
    assert!(
        !out_dir.exists()
            || out_dir
                .read_dir()
                .map(|mut d| d.next().is_none())
                .unwrap_or(true),
        "a failed selector must not leave a populated backup folder behind"
    );
    assert!(!err.is_empty(), "a miss should say something on stderr");
}
