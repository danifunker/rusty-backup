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
        if matches!(o.kind.as_str(), "emulator" | "hardware" | "roundtrip") {
            out.push(Detected {
                oracle: o.id.clone(),
                status: "manual",
                resolved: None,
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
