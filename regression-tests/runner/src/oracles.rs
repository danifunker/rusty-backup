//! `oracles` — detect which third-party tools this machine actually has, and
//! write that as a gitignored overlay.
//!
//! The split matters. `data/oracles.toml` says what an oracle *is* and what it
//! proves: portable knowledge that belongs to everyone who clones the repo.
//! Whether *this* box has the tool, and where it lives, is neither portable nor
//! publishable — that is how an absolute `D:/ROMs/...` ended up in a tracked
//! file. So availability lives in `data/oracles.local.toml`, gitignored,
//! generated rather than hand-maintained.
//!
//! Two verbs, mirroring the corpus:
//!
//! - `--detect` probes the host and rewrites the overlay.
//! - `--export` prints it, to seed another machine or paste into a handoff.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::exec;
use crate::registry::Registry;

/// What a probe concluded about one tool on this host.
pub struct Detected {
    pub oracle: String,
    pub status: &'static str,
    pub resolved: Option<String>,
}

/// The executable to look for, given an oracle's display `tool` string.
///
/// `tool` is prose for the report — `mount -t affs`, `MiSTer Minimig core` —
/// so the first whitespace-separated word is the closest thing to a program
/// name. An oracle that needs something else sets `program` in the registry.
fn program_for(tool: &str, program: Option<&str>) -> String {
    program
        .map(|p| p.to_string())
        .unwrap_or_else(|| tool.split_whitespace().next().unwrap_or(tool).to_string())
}

/// Resolve a `path_hint`, which may name the program itself or the directory
/// holding it. `chdman`'s hint is `C:/Tools/chdman`, a directory — testing it
/// as a file reported the tool absent on a box that has it.
fn resolve_hint(hint: &str, program: &str) -> Option<String> {
    let p = Path::new(hint);
    if p.is_file() {
        return Some(hint.to_string());
    }
    if p.is_dir() {
        let exts: &[&str] = if cfg!(windows) {
            &["", ".exe", ".cmd", ".bat"]
        } else {
            &[""]
        };
        for ext in exts {
            let cand = p.join(format!("{}{}", program, ext));
            if cand.is_file() {
                return Some(cand.display().to_string());
            }
        }
    }
    None
}

/// Directories to search beyond `PATH`, for tools that install as an app
/// rather than a command — every emulator here is a GUI program that never
/// lands on `PATH`.
///
/// `RB_EMULATOR_DIRS` is checked first and takes the platform separator, so a
/// machine keeping its emulators somewhere unusual needs no code change and no
/// hand-edited TOML.
fn search_roots(platform: &str) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(v) = std::env::var_os("RB_EMULATOR_DIRS") {
        roots.extend(std::env::split_paths(&v));
    }
    let home = std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from);
    match platform {
        "windows" => {
            for v in ["ProgramFiles", "ProgramFiles(x86)", "LOCALAPPDATA"] {
                if let Some(p) = std::env::var_os(v) {
                    roots.push(PathBuf::from(p));
                }
            }
            roots.push(PathBuf::from("C:/Tools"));
            roots.push(PathBuf::from("C:/Emulators"));
        }
        "macos" => {
            roots.push(PathBuf::from("/Applications"));
            roots.push(PathBuf::from("/opt/homebrew/bin"));
            roots.push(PathBuf::from("/usr/local/bin"));
            if let Some(h) = &home {
                roots.push(h.join("Applications"));
            }
        }
        _ => {
            roots.push(PathBuf::from("/usr/bin"));
            roots.push(PathBuf::from("/usr/local/bin"));
            roots.push(PathBuf::from("/opt"));
            if let Some(h) = &home {
                roots.push(h.join(".local/bin"));
            }
        }
    }
    roots
}

/// Look for `program` under each root: directly, one level down (the usual
/// `Program Files/WinUAE/winuae64.exe` shape), and inside a macOS `.app`.
fn find_under_roots(program: &str, roots: &[PathBuf]) -> Option<String> {
    let exts: &[&str] = if cfg!(windows) {
        &["", ".exe", ".cmd", ".bat"]
    } else {
        &[""]
    };
    for root in roots {
        for ext in exts {
            let direct = root.join(format!("{}{}", program, ext));
            if direct.is_file() {
                return Some(direct.display().to_string());
            }
        }
        let entries = match fs::read_dir(root) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for e in entries.flatten() {
            let d = e.path();
            if !d.is_dir() {
                continue;
            }
            for ext in exts {
                let cand = d.join(format!("{}{}", program, ext));
                if cand.is_file() {
                    return Some(cand.display().to_string());
                }
            }
            // macOS bundle: Iris.app/Contents/MacOS/Iris
            let bundled = d.join("Contents").join("MacOS").join(program);
            if bundled.is_file() {
                return Some(bundled.display().to_string());
            }
        }
    }
    None
}

/// Probe every oracle the registry declares against this machine.
///
/// Kinds that are not a program on PATH are reported as such rather than
/// guessed at: an emulator or a MiSTer core is not something `which` can find,
/// and claiming "absent" for them would read as a missing install rather than
/// a different kind of oracle.
pub fn detect(reg: &Registry, path_hints: &BTreeMap<String, String>, platform: &str) -> Vec<Detected> {
    let mut out = Vec::new();
    for o in &reg.oracles {
        // A `mount` oracle is the kernel's opinion, so it only means anything
        // on a kernel that has the driver. Probing the program name here found
        // Cygwin's `mount` on Windows and called AFFS "verified", which is a
        // confidently wrong answer — the worst kind for a tool whose whole job
        // is telling you what is true.
        if o.kind == "mount" && !matches!(platform, "linux" | "wsl" | "mister-hps") {
            out.push(Detected {
                oracle: o.id.clone(),
                status: "absent",
                resolved: None,
            });
            continue;
        }
        // Hardware and round-trip oracles are not programs at all.
        if matches!(o.kind.as_str(), "hardware" | "roundtrip") {
            out.push(Detected {
                oracle: o.id.clone(),
                status: "manual",
                resolved: None,
            });
            continue;
        }
        // An emulator IS findable — it just installs as an app, not a command.
        // Finding the binary does not make the oracle runnable, though: it
        // still needs a configured guest. `installed` says exactly that, which
        // `verified` would overclaim and `manual` would hide.
        if o.kind == "emulator" {
            let prog = program_for(&o.tool, o.program.as_deref());
            let found = path_hints
                .get(&o.id)
                .and_then(|h| resolve_hint(h, &prog))
                .or_else(|| exec::tool_available(&prog).then(|| prog.clone()))
                .or_else(|| find_under_roots(&prog, &search_roots(platform)));
            out.push(match found {
                Some(p) => Detected {
                    oracle: o.id.clone(),
                    status: "installed",
                    resolved: Some(p),
                },
                None => Detected {
                    oracle: o.id.clone(),
                    status: "manual",
                    resolved: None,
                },
            });
            continue;
        }
        // An explicit hint wins: chdman and qemu-img are installed here but not
        // on PATH, which is exactly the case a hint exists for.
        let prog = program_for(&o.tool, o.program.as_deref());
        if let Some(hint) = path_hints.get(&o.id) {
            if let Some(found) = resolve_hint(hint, &prog) {
                out.push(Detected {
                    oracle: o.id.clone(),
                    status: "verified",
                    resolved: Some(found),
                });
                continue;
            }
        }
        if exec::tool_available(&prog) {
            out.push(Detected {
                oracle: o.id.clone(),
                status: "verified",
                resolved: Some(prog),
            });
        } else {
            out.push(Detected {
                oracle: o.id.clone(),
                status: "absent",
                resolved: None,
            });
        }
    }
    out
}

/// Render the overlay. `verified_on` is stamped by the caller, not here, so
/// this stays deterministic and testable.
pub fn render(found: &[Detected], platform: &str, today: &str) -> String {
    let mut s = String::new();
    s.push_str(
        "# Per-host oracle availability. GITIGNORED - generated, do not hand-edit.\n\
         #\n\
         # Regenerate:  rb-regress oracles --detect\n\
         # Share:       rb-regress oracles --export\n\
         #\n\
         # What an oracle IS lives in the tracked data/oracles.toml. Whether\n\
         # this machine has it lives here, because an absolute path in a public\n\
         # repo is how D:/ROMs ended up committed.\n\
         #\n\
         # `manual` means the oracle is not a program on PATH at all - an\n\
         # emulator or a real MiSTer core. Not the same as absent.\n\n",
    );
    for d in found {
        // Absent is the default the tracked registry already implies; writing
        // it would double the file for no information.
        if d.status == "absent" {
            continue;
        }
        s.push_str("[[availability]]\n");
        s.push_str(&format!("oracle = {:?}\n", d.oracle));
        s.push_str(&format!("platform = {:?}\n", platform));
        s.push_str(&format!("status = {:?}\n", d.status));
        if let Some(r) = &d.resolved {
            s.push_str(&format!("path_hint = {:?}\n", r));
        }
        s.push_str(&format!("verified_on = {:?}\n\n", today));
    }
    s
}

/// Existing hints from the tracked registry for this platform, so a detect run
/// does not throw away a hand-recorded path that is still correct.
pub fn hints_for(reg: &Registry, platform: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    for a in &reg.availability {
        if a.platform == platform {
            if let Some(h) = &a.path_hint {
                m.insert(a.oracle.clone(), h.clone());
            }
        }
    }
    m
}

pub fn overlay_path(regression_dir: &Path) -> PathBuf {
    regression_dir.join("data").join("oracles.local.toml")
}

/// Record one oracle's path by hand, for a tool `--detect` cannot find.
///
/// Rewrites just that oracle's row in the overlay and leaves the rest alone, so
/// providing a path never costs you the detected ones. The path is checked
/// here: a hint that points at nothing is the kind of thing you discover three
/// weeks later when a verify run quietly skips.
pub fn set_hint(
    existing: &str,
    oracle: &str,
    path: &str,
    platform: &str,
    today: &str,
    status: &str,
) -> Result<String, String> {
    if !Path::new(path).exists() {
        return Err(format!("{path}: no such file or directory"));
    }
    let mut kept = String::new();
    let mut skipping = false;
    for block in existing.split("[[availability]]") {
        if block.contains(&format!("oracle = {:?}", oracle)) {
            skipping = true;
            continue;
        }
        if !skipping && kept.is_empty() {
            kept.push_str(block);
        } else {
            kept.push_str("[[availability]]");
            kept.push_str(block);
        }
        skipping = false;
    }
    if kept.is_empty() {
        kept.push_str(existing);
    }
    if !kept.ends_with('\n') {
        kept.push('\n');
    }
    kept.push_str(&format!(
        "[[availability]]
oracle = {:?}
platform = {:?}
status = {:?}
path_hint = {:?}
verified_on = {:?}

",
        oracle, platform, status, path, today
    ));
    Ok(kept)
}

pub fn write(regression_dir: &Path, body: &str) -> Result<PathBuf, String> {
    let p = overlay_path(regression_dir);
    fs::write(&p, body).map_err(|e| format!("{}: {}", p.display(), e))?;
    Ok(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_display_string_yields_its_first_word() {
        assert_eq!(program_for("mount -t affs", None), "mount");
        assert_eq!(program_for("qemu-img", None), "qemu-img");
        // An explicit program wins, for tools whose display name is prose.
        assert_eq!(program_for("MiSTer Minimig core", Some("none")), "none");
    }

    #[test]
    fn a_hint_may_be_the_directory_holding_the_program() {
        let dir = std::env::temp_dir().join("rb-oracle-hint-test");
        let _ = fs::create_dir_all(&dir);
        let exe = dir.join(if cfg!(windows) { "toolx.exe" } else { "toolx" });
        let _ = fs::write(&exe, b"x");
        assert_eq!(
            resolve_hint(&dir.display().to_string(), "toolx"),
            Some(exe.display().to_string()),
            "a directory hint must find the program inside it"
        );
        assert_eq!(resolve_hint(&dir.display().to_string(), "absent"), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn absent_rows_are_not_written() {
        let found = vec![
            Detected {
                oracle: "here".into(),
                status: "verified",
                resolved: Some("chdman".into()),
            },
            Detected {
                oracle: "gone".into(),
                status: "absent",
                resolved: None,
            },
        ];
        let out = render(&found, "windows", "2026-08-10");
        assert!(out.contains("\"here\""));
        // The tracked registry already implies absent; writing it doubles the
        // file for no information.
        assert!(!out.contains("\"gone\""));
        assert!(out.contains("path_hint = \"chdman\""));
    }

    #[test]
    fn setting_a_hint_replaces_only_that_oracle() {
        let start = concat!(
            "[[availability]]
oracle = \"keepme\"
platform = \"windows\"
",
            "status = \"verified\"

",
            "[[availability]]
oracle = \"fs-uae\"
platform = \"windows\"
",
            "status = \"manual\"

"
        );
        let here = std::env::current_exe().unwrap();
        let out = set_hint(
            start,
            "fs-uae",
            &here.display().to_string(),
            "windows",
            "2026-08-10",
            "installed",
        )
        .expect("path exists");
        assert!(out.contains("\"keepme\""), "other rows must survive");
        assert_eq!(out.matches("oracle = \"fs-uae\"").count(), 1, "no duplicate row");
        assert!(out.contains("status = \"installed\""));
    }

    #[test]
    fn setting_a_hint_to_nothing_is_refused() {
        // A hint pointing at nothing is discovered three weeks later, as a
        // quiet skip. Fail at the point the mistake is made.
        let e = set_hint("", "fs-uae", "/definitely/not/here", "windows", "2026-08-10", "installed");
        assert!(e.is_err());
    }

    #[test]
    fn manual_oracles_are_recorded_but_carry_no_path() {
        let found = vec![Detected {
            oracle: "fs-uae".into(),
            status: "manual",
            resolved: None,
        }];
        let out = render(&found, "windows", "2026-08-10");
        assert!(out.contains("status = \"manual\""));
        assert!(!out.contains("path_hint"));
    }
}

/// Ask the user where an emulator lives, for the ones `--detect` could not
/// find.
///
/// Only emulators, and only on a terminal. Those two limits are the whole
/// design:
///
/// - **Only emulators**, because they are the case where the tool *is*
///   installed and we merely cannot see it. A `package` oracle that came back
///   absent is genuinely not installed, and prompting for a path to software
///   you do not have is noise — 18 questions to reach the 5 that matter.
/// - **Only on a terminal**, because the harness runs in CI and over ssh. A
///   prompt with no one to answer it is a hang, and a hang in a regression run
///   looks exactly like a test that never finishes.
///
/// A blank answer skips. A path that does not exist is refused and re-asked,
/// rather than written and discovered later as a silent skip.
pub fn prompt_for_missing(
    found: &mut [Detected],
    reg: &Registry,
    input: &mut impl std::io::BufRead,
    out: &mut impl std::io::Write,
) -> usize {
    let emulators: std::collections::BTreeSet<&str> = reg
        .oracles
        .iter()
        .filter(|o| o.kind == "emulator")
        .map(|o| o.id.as_str())
        .collect();

    let mut filled = 0;
    for d in found.iter_mut() {
        if d.status != "manual" || !emulators.contains(d.oracle.as_str()) {
            continue;
        }
        let prog = reg
            .oracles
            .iter()
            .find(|o| o.id == d.oracle)
            .map(|o| program_for(&o.tool, o.program.as_deref()))
            .unwrap_or_else(|| d.oracle.clone());
        loop {
            let _ = writeln!(
                out,
                "\n{} not found (looked for `{}` on PATH and the usual install roots).",
                d.oracle, prog
            );
            let _ = write!(out, "  path to it, or Enter to skip: ");
            let _ = out.flush();
            let mut line = String::new();
            if input.read_line(&mut line).unwrap_or(0) == 0 {
                return filled; // EOF: stop asking rather than spin.
            }
            let ans = line.trim();
            if ans.is_empty() {
                break;
            }
            if Path::new(ans).exists() {
                d.status = "installed";
                d.resolved = Some(ans.to_string());
                filled += 1;
                break;
            }
            let _ = writeln!(out, "  {ans}: no such file or directory");
        }
    }
    filled
}

#[cfg(test)]
mod prompt_tests {
    use super::*;
    use crate::registry::{Oracle, Registry};

    fn reg_with(kind: &str, id: &str) -> Registry {
        let mut r = Registry::default();
        r.oracles.push(Oracle {
            id: id.into(),
            tool: "Thing".into(),
            kind: kind.into(),
            program: Some("thing".into()),
            notes: None,
            core: None,
        });
        r
    }

    #[test]
    fn a_blank_answer_skips() {
        let reg = reg_with("emulator", "fs-uae");
        let mut found = vec![Detected {
            oracle: "fs-uae".into(),
            status: "manual",
            resolved: None,
        }];
        let mut input = std::io::Cursor::new(b"\n".to_vec());
        let mut out = Vec::new();
        assert_eq!(prompt_for_missing(&mut found, &reg, &mut input, &mut out), 0);
        assert_eq!(found[0].status, "manual");
    }

    #[test]
    fn a_real_path_is_recorded_as_installed() {
        let reg = reg_with("emulator", "fs-uae");
        let mut found = vec![Detected {
            oracle: "fs-uae".into(),
            status: "manual",
            resolved: None,
        }];
        let here = std::env::current_exe().unwrap();
        let mut input = std::io::Cursor::new(format!("{}\n", here.display()).into_bytes());
        let mut out = Vec::new();
        assert_eq!(prompt_for_missing(&mut found, &reg, &mut input, &mut out), 1);
        assert_eq!(found[0].status, "installed");
    }

    #[test]
    fn a_bad_path_is_refused_then_reasked() {
        let reg = reg_with("emulator", "fs-uae");
        let mut found = vec![Detected {
            oracle: "fs-uae".into(),
            status: "manual",
            resolved: None,
        }];
        // Wrong once, then blank. The wrong one must not be written.
        let mut input = std::io::Cursor::new(b"/nope/nowhere\n\n".to_vec());
        let mut out = Vec::new();
        assert_eq!(prompt_for_missing(&mut found, &reg, &mut input, &mut out), 0);
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("no such file"), "must say why it was refused");
        assert_eq!(found[0].status, "manual");
    }

    #[test]
    fn package_oracles_are_never_asked_about() {
        // Absent means not installed. Asking would be 18 questions to reach
        // the 5 that matter.
        let reg = reg_with("package", "cpmtools");
        let mut found = vec![Detected {
            oracle: "cpmtools".into(),
            status: "manual",
            resolved: None,
        }];
        let mut input = std::io::Cursor::new(b"".to_vec());
        let mut out = Vec::new();
        assert_eq!(prompt_for_missing(&mut found, &reg, &mut input, &mut out), 0);
        assert!(out.is_empty(), "no prompt should have been printed");
    }
}

/// What a MiSTer scan found on the board.
pub struct MisterScan {
    /// `.rbf` base names present, date suffix stripped.
    pub cores: Vec<String>,
    /// Oracles matched to a core that is actually installed.
    pub matched: Vec<(String, String)>,
    /// Oracles whose core the board does not have.
    pub missing: Vec<(String, String)>,
}

/// Strip MiSTer's `_YYYYMMDD` build-date suffix from an `.rbf` filename.
///
/// Cores are redistributed as `X68000_20260603.rbf` and the date moves every
/// time the board is updated, so the bare name is the only stable identifier.
pub fn core_base_name(filename: &str) -> String {
    let stem = filename.strip_suffix(".rbf").unwrap_or(filename);
    match stem.rsplit_once('_') {
        Some((head, tail)) if tail.len() == 8 && tail.chars().all(|c| c.is_ascii_digit()) => {
            head.to_string()
        }
        _ => stem.to_string(),
    }
}

/// Match the board's cores against the oracles that name one.
pub fn match_cores(reg: &Registry, present: &[String]) -> MisterScan {
    let have: std::collections::BTreeSet<&str> = present.iter().map(|s| s.as_str()).collect();
    let mut matched = Vec::new();
    let mut missing = Vec::new();
    for o in &reg.oracles {
        if let Some(core) = &o.core {
            if have.contains(core.as_str()) {
                matched.push((o.id.clone(), core.clone()));
            } else {
                missing.push((o.id.clone(), core.clone()));
            }
        }
    }
    MisterScan {
        cores: present.to_vec(),
        matched,
        missing,
    }
}

/// Render scan results as overlay rows. A core that is present is `verified`:
/// unlike an emulator, there is no guest to configure — the core IS the guest.
/// Running the check is still manual, which the oracle's `kind` already says.
pub fn render_mister(scan: &MisterScan, platform: &str, today: &str) -> String {
    let mut s = String::new();
    s.push_str(
        "# MiSTer cores found by `rb-regress oracles --scan`. GITIGNORED.\n\
         # A core present on the board is recorded verified; whether a given\n\
         # check has been RUN is a separate question the verify tree answers.\n\n",
    );
    for (oracle, core) in &scan.matched {
        s.push_str("[[availability]]\n");
        s.push_str(&format!("oracle = {:?}\n", oracle));
        s.push_str(&format!("platform = {:?}\n", platform));
        s.push_str("status = \"verified\"\n");
        s.push_str(&format!("path_hint = {:?}\n", core));
        s.push_str(&format!("verified_on = {:?}\n\n", today));
    }
    s
}

#[cfg(test)]
mod mister_tests {
    use super::*;
    use crate::registry::{Oracle, Registry};

    fn reg() -> Registry {
        let mut r = Registry::default();
        for (id, core) in [("mister-core-amiga", "Minimig"), ("mister-core-ti99", "Ti994a")] {
            r.oracles.push(Oracle {
                id: id.into(),
                tool: "core".into(),
                kind: "hardware".into(),
                program: None,
                notes: None,
                core: Some(core.into()),
            });
        }
        r
    }

    #[test]
    fn the_build_date_suffix_is_stripped() {
        assert_eq!(core_base_name("X68000_20260603.rbf"), "X68000");
        assert_eq!(core_base_name("ZX-Spectrum_20250930.rbf"), "ZX-Spectrum");
        // A name with an underscore but no date must survive intact.
        assert_eq!(core_base_name("Apple-II.rbf"), "Apple-II");
        assert_eq!(core_base_name("My_Core.rbf"), "My_Core");
    }

    #[test]
    fn present_and_absent_cores_are_told_apart() {
        let present = vec!["Minimig".to_string(), "X68000".to_string()];
        let scan = match_cores(&reg(), &present);
        assert_eq!(scan.matched.len(), 1);
        assert_eq!(scan.matched[0].0, "mister-core-amiga");
        // Ti994a is not on this imaginary board, and must be reported missing
        // rather than silently dropped.
        assert_eq!(scan.missing.len(), 1);
        assert_eq!(scan.missing[0].1, "Ti994a");
    }

    #[test]
    fn only_matched_cores_reach_the_overlay() {
        let scan = match_cores(&reg(), &["Minimig".to_string()]);
        let out = render_mister(&scan, "mister-core", "2026-08-12");
        assert!(out.contains("mister-core-amiga"));
        assert!(!out.contains("mister-core-ti99"));
    }
}
