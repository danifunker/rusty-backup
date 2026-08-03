//! Build provenance from git.
//!
//! `rb-cli` does not bake its commit into `--version` (that would mean editing
//! `build.rs`, which is engine code and out of scope for the suite). So the
//! harness derives it from the working tree instead.
//!
//! That substitution is only sound because of two rules the runner enforces
//! together:
//!
//! 1. **The tree must be clean** (`--require-clean`). A dirty tree means the
//!    source no longer matches any commit, so no sha describes what was tested.
//! 2. **rb-cli is built at run time from that tree.** This is the important
//!    half. If the harness merely *found* an `rb-cli.exe` lying around, the sha
//!    would describe the checkout rather than the binary — and on a host where
//!    someone had copied a stale binary in, the label would be confidently
//!    wrong. Building it as part of the run makes the sha true by construction.
//!
//! Once the sha is baked into `--version` upstream, this becomes a
//! cross-check rather than the source of truth.

use std::path::Path;
use std::process::Command;

fn git(repo: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Full commit sha of HEAD.
pub fn head_sha(repo: &Path) -> Option<String> {
    git(repo, &["rev-parse", "HEAD"])
}

/// Short sha, for display.
pub fn short_sha(repo: &Path) -> Option<String> {
    git(repo, &["rev-parse", "--short", "HEAD"])
}

pub fn branch(repo: &Path) -> Option<String> {
    git(repo, &["rev-parse", "--abbrev-ref", "HEAD"])
}

/// Files that make the tree dirty.
///
/// Uses `status --porcelain`, which includes untracked files — deliberately.
/// An untracked file that should have been ignored is exactly the kind of
/// transient artefact that makes a "clean" claim meaningless, so it should
/// block a run until it is either committed or added to `.gitignore`.
pub fn dirty_files(repo: &Path) -> Option<Vec<String>> {
    let out = git(repo, &["status", "--porcelain"])?;
    Some(
        out.lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect(),
    )
}

pub fn is_clean(repo: &Path) -> bool {
    dirty_files(repo).map(|v| v.is_empty()).unwrap_or(false)
}

/// Human-readable build identity, e.g. `0.1.0+g184c764` or
/// `0.1.0+g184c764.dirty`.
pub fn build_label(repo: &Path, version: &str) -> String {
    match short_sha(repo) {
        Some(sha) => {
            let suffix = if is_clean(repo) { "" } else { ".dirty" };
            format!("{}+g{}{}", version.trim(), sha, suffix)
        }
        None => version.trim().to_string(),
    }
}
