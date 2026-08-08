//! `produce` — build every artifact rb-cli can write, on whatever OS is running.
//!
//! The producer is the thing under test. rb-cli on Windows and rb-cli on macOS
//! are different builds taking different code paths, so every host produces the
//! whole set; only verification is OS-specific. See RUNBOOK.md § The run/verify
//! split for why an earlier design that produced on one host and shipped the
//! result was testing one build and calling it coverage.
//!
//! Each recipe runs **twice**. Diffing the two outputs discovers that build's
//! volatile byte ranges empirically — FAT/NTFS/ProDOS come out byte-identical,
//! HFS/ext/HFS+ differ in the handful of bytes holding an embedded creation
//! timestamp. Recording the ranges here, rather than maintaining a per-format
//! table of which bytes to ignore, is what lets `parity` compare across hosts
//! without either hand-waving a whole-file sha or hard-coding offsets that go
//! stale the moment a formatter changes.
//!
//! The two runs are **two passes over the whole recipe set**, separated by at
//! least [`MIN_GAP`], not two back-to-back runs of one recipe. That is not
//! tidiness — measured 2026-08-07, a back-to-back pair reports HFS as
//! byte-deterministic and a pair three seconds apart reports six differing
//! bytes. Timestamps have one-second granularity, so a fast recipe produces
//! both copies inside one tick and the volatility hides. A run that misses a
//! volatile range does not fail; it makes `parity` report a false divergence
//! on the next host, which is worse.
//!
//! Discovery still only ever finds a **lower bound**. Two samples seconds apart
//! differ in the low byte of a seconds field and not the high ones, so a pair
//! of hosts producing days apart can diverge in bytes this never saw vary.
//! `parity` handles that by reporting divergences adjacent to a known volatile
//! range separately rather than calling them findings.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::exec;

const PRODUCE_TIMEOUT: Duration = Duration::from_secs(300);

/// Minimum separation between the two produce passes.
///
/// Was three seconds, which cleared the one-second granularity HFS/ext use and
/// the two-second DOS timestamp. ProDOS then proved that insufficient: it
/// stamps the volume directory header to the **minute**, so both passes landed
/// in the same minute, `produce` recorded it as byte-deterministic, and the
/// first three-OS `parity` run reported a one-byte cross-OS divergence that was
/// nothing but the clock. A false finding is the expensive kind.
///
/// Sixty-five seconds guarantees a minute boundary is crossed. Once the minute
/// byte is discovered, the ±8-byte adjacency rule in `parity` covers the hour
/// and date bytes beside it, so the whole field is handled by finding one byte
/// of it. Coarser fields than a minute — an hour-granularity stamp — remain out
/// of reach by this method and would need declaring via `expect_divergence`.
///
/// The cost is one sleep per produce run, not per recipe.
const MIN_GAP: Duration = Duration::from_secs(65);

#[derive(Debug, Clone, Deserialize)]
pub struct Recipe {
    /// Matches a `[[format]]` id in `formats.toml`.
    pub id: String,
    /// Extension for the produced artifact, without the dot.
    pub ext: String,
    /// Commands run before `args`, in the same scratch directory.
    #[serde(default)]
    pub pre: Vec<Vec<String>>,
    pub args: Vec<String>,
    /// Byte ranges this format is *expected* to differ in across producer
    /// OSes, with a reason. See [`ExpectedDivergence`].
    #[serde(default)]
    pub expect_divergence: Vec<ExpectedDivergence>,
    /// Where the artifact actually lands, when the verb does not write to
    /// `{out}` directly. `convert` takes a destination *folder* and names the
    /// file after its input, so those recipes point here instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub produces: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RecipeFile {
    #[serde(default)]
    recipe: Vec<Recipe>,
}

/// A range a format is allowed to differ in between producer OSes, declared
/// up front with a reason.
///
/// Distinct from a volatile range, which `produce` discovers by building twice
/// on one machine. This is the opposite: a difference that will *never* show up
/// that way, because it is stable on any single host and varies only by which
/// host built it — VHD's Creator Host OS field being the case that prompted it
/// (R-019). Declaring it keeps `parity` honest in both directions: the
/// divergence stops being reported as a finding, and it stays visible and
/// attributed rather than being silently swallowed by a widened mask.
///
/// Give exactly one of `at` (absolute) or `from_end` (offset back from EOF).
/// `from_end` exists because container footers are anchored to the end of the
/// file, so an absolute offset would be wrong the moment a recipe's size
/// changes.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExpectedDivergence {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_end: Option<u64>,
    pub len: u64,
    pub reason: String,
}

/// An `ExpectedDivergence` resolved against a real artifact, so `meta.json`
/// stays self-contained and `parity` never needs the recipe file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpectedRange {
    pub start: u64,
    pub end: u64,
    pub reason: String,
}

/// A byte range that differed between two runs of the same recipe on the same
/// machine, so it cannot carry information about cross-OS divergence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolatileRange {
    pub start: u64,
    /// Exclusive.
    pub end: u64,
}

/// Written beside every artifact. Self-contained on purpose: a verifier reading
/// an artifact tree off the NAS months later must not need the registry, the
/// recipe file, or the run that made it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactMeta {
    pub format: String,
    pub file: String,
    pub producer_os: String,
    pub producer_host: String,
    pub rb_version: String,
    pub git_sha: String,
    pub argv: Vec<String>,
    pub sha256: String,
    pub size: u64,
    /// Empty means the recipe is byte-deterministic on this build.
    pub volatile: Vec<VolatileRange>,
    /// Declared cross-OS differences, resolved against this artifact's size.
    #[serde(default)]
    pub expected: Vec<ExpectedRange>,
    /// Set when the two runs produced different *sizes*, which makes range
    /// discovery meaningless — the artifact is kept but parity must skip it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nondeterministic: Option<String>,
}

#[derive(Debug)]
pub enum Outcome {
    Produced {
        volatile_bytes: u64,
        size: u64,
    },
    /// rb-cli exited non-zero, timed out, or wrote no file.
    Failed {
        step: String,
        detail: String,
    },
    /// Two runs, two different sizes. Kept, but excluded from parity.
    SizeUnstable {
        a: u64,
        b: u64,
    },
}

pub struct Report {
    pub outcomes: Vec<(String, Outcome)>,
    /// `builder` in formats.toml with no recipe here.
    pub missing_recipes: Vec<String>,
    /// Recipe here whose id is not a known format.
    pub unknown_formats: Vec<String>,
}

pub fn load_recipes(path: &Path) -> Result<Vec<Recipe>, String> {
    let text = fs::read_to_string(path).map_err(|e| format!("{}: {}", path.display(), e))?;
    let parsed: RecipeFile =
        toml::from_str(&text).map_err(|e| format!("{}: {}", path.display(), e))?;
    Ok(parsed.recipe)
}

/// Byte ranges where `a` and `b` differ. Adjacent differing bytes coalesce into
/// one range so a timestamp reads as one entry rather than eight.
pub fn diff_ranges(a: &[u8], b: &[u8]) -> Vec<VolatileRange> {
    let mut out = Vec::new();
    let mut open: Option<u64> = None;
    for i in 0..a.len().min(b.len()) {
        if a[i] != b[i] {
            if open.is_none() {
                open = Some(i as u64);
            }
        } else if let Some(start) = open.take() {
            out.push(VolatileRange {
                start,
                end: i as u64,
            });
        }
    }
    if let Some(start) = open {
        out.push(VolatileRange {
            start,
            end: a.len().min(b.len()) as u64,
        });
    }
    out
}

/// Turn declared ranges into absolute ones for an artifact of `size` bytes.
/// A range that would fall outside the file is dropped rather than clamped —
/// a mis-declared offset should stop masking anything, not mask the wrong
/// bytes.
fn resolve_expected(decls: &[ExpectedDivergence], size: u64) -> Vec<ExpectedRange> {
    let mut out = Vec::new();
    for d in decls {
        let start = match (d.at, d.from_end) {
            (Some(a), None) => a,
            (None, Some(f)) => match size.checked_sub(f) {
                Some(v) => v,
                None => continue,
            },
            _ => continue,
        };
        let end = start.saturating_add(d.len);
        if end <= size {
            out.push(ExpectedRange {
                start,
                end,
                reason: d.reason.clone(),
            });
        }
    }
    out
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{:02x}", b)).collect()
}

/// Substitute `{out}` / `{work}` and normalise separators — the recipes are
/// written with forward slashes and must work on Windows too.
fn expand(args: &[String], out: &Path, work: &Path) -> Vec<String> {
    args.iter()
        .map(|a| {
            a.replace("{out}", &out.display().to_string())
                .replace("{work}", &work.display().to_string())
        })
        .collect()
}

/// Run one recipe once into `work`, returning the artifact bytes.
fn produce_once(
    rb_cli: &Path,
    recipe: &Recipe,
    work: &Path,
) -> Result<(Vec<u8>, Vec<String>), (String, String)> {
    fs::create_dir_all(work).map_err(|e| ("scratch".to_string(), e.to_string()))?;
    let out = work.join(format!("image.{}", recipe.ext));

    for (n, pre) in recipe.pre.iter().enumerate() {
        let argv = expand(pre, &out, work);
        let r = exec::run(rb_cli, &argv, work, PRODUCE_TIMEOUT)
            .map_err(|e| (format!("pre[{}]", n), e.to_string()))?;
        if r.timed_out {
            return Err((format!("pre[{}]", n), "timed out".to_string()));
        }
        if r.exit_code != Some(0) {
            return Err((
                format!("pre[{}]", n),
                format!("exit {:?}: {}", r.exit_code, first_line(&r.stderr)),
            ));
        }
    }

    let argv = expand(&recipe.args, &out, work);
    let r = exec::run(rb_cli, &argv, work, PRODUCE_TIMEOUT)
        .map_err(|e| ("args".to_string(), e.to_string()))?;
    if r.timed_out {
        return Err(("args".to_string(), "timed out".to_string()));
    }
    if r.exit_code != Some(0) {
        return Err((
            "args".to_string(),
            format!("exit {:?}: {}", r.exit_code, first_line(&r.stderr)),
        ));
    }
    let artifact = match &recipe.produces {
        Some(p) => std::path::PathBuf::from(expand(std::slice::from_ref(p), &out, work).remove(0)),
        None => out.clone(),
    };
    // An exit code of 0 with no file is its own failure, and one worth naming
    // separately: it means rb-cli reported success for work it did not do.
    let bytes = fs::read(&artifact).map_err(|e| {
        (
            "args".to_string(),
            format!("no artifact at {}: {}", artifact.display(), e),
        )
    })?;
    Ok((bytes, argv))
}

#[allow(clippy::too_many_arguments)]
pub fn produce(
    rb_cli: &Path,
    recipes: &[Recipe],
    known_formats: &BTreeMap<String, String>,
    builders: &BTreeMap<String, String>,
    out_root: &Path,
    scratch_root: &Path,
    host: &str,
    rb_version: &str,
    git_sha: &str,
    filter: Option<&str>,
) -> Result<Report, String> {
    let platform = exec::platform_token();
    fs::create_dir_all(out_root).map_err(|e| format!("{}: {}", out_root.display(), e))?;

    let mut outcomes = Vec::new();
    let have: Vec<&Recipe> = recipes
        .iter()
        .filter(|r| filter.map(|f| r.id.contains(f)).unwrap_or(true))
        .collect();

    // Pass A. Each artifact lands in its final home immediately, so pass B can
    // diff against it from disk and only one artifact is ever held in memory.
    let pass_a_started = Instant::now();
    let mut first: Vec<(&Recipe, Vec<u8>, Vec<String>)> = Vec::new();
    for recipe in &have {
        let work = scratch_root.join(format!("{}-a", recipe.id));
        let _ = fs::remove_dir_all(&work);
        match produce_once(rb_cli, recipe, &work) {
            Ok((bytes, argv)) => first.push((recipe, bytes, argv)),
            Err((step, detail)) => {
                outcomes.push((recipe.id.clone(), Outcome::Failed { step, detail }))
            }
        }
        let _ = fs::remove_dir_all(&work);
    }

    // The whole point of two passes: cross a clock tick before pass B, or a
    // one-second-granularity timestamp reads as deterministic.
    let elapsed = pass_a_started.elapsed();
    if elapsed < MIN_GAP {
        std::thread::sleep(MIN_GAP - elapsed);
    }

    for (recipe, bytes_a, argv) in first {
        let dir = out_root.join(&recipe.id);
        let work_b = scratch_root.join(format!("{}-b", recipe.id));
        let _ = fs::remove_dir_all(&work_b);

        let bytes_b = match produce_once(rb_cli, recipe, &work_b) {
            Ok((v, _)) => v,
            Err((step, detail)) => {
                outcomes.push((recipe.id.clone(), Outcome::Failed { step, detail }));
                let _ = fs::remove_dir_all(&work_b);
                continue;
            }
        };

        let unstable = bytes_a.len() != bytes_b.len();
        let volatile = if unstable {
            Vec::new()
        } else {
            diff_ranges(&bytes_a, &bytes_b)
        };
        let volatile_bytes: u64 = volatile.iter().map(|r| r.end - r.start).sum();

        let file = format!("image.{}", recipe.ext);
        let meta = ArtifactMeta {
            format: recipe.id.clone(),
            file: file.clone(),
            producer_os: platform.to_string(),
            producer_host: host.to_string(),
            rb_version: rb_version.to_string(),
            git_sha: git_sha.to_string(),
            argv,
            sha256: sha256_hex(&bytes_a),
            size: bytes_a.len() as u64,
            volatile,
            expected: resolve_expected(&recipe.expect_divergence, bytes_a.len() as u64),
            nondeterministic: if unstable {
                Some(format!(
                    "two runs produced {} and {} bytes",
                    bytes_a.len(),
                    bytes_b.len()
                ))
            } else {
                None
            },
        };

        fs::create_dir_all(&dir).map_err(|e| format!("{}: {}", dir.display(), e))?;
        fs::write(dir.join(&file), &bytes_a).map_err(|e| format!("{}: {}", dir.display(), e))?;
        let meta_json = serde_json::to_string_pretty(&meta).map_err(|e| e.to_string())?;
        fs::write(dir.join("meta.json"), meta_json).map_err(|e| e.to_string())?;

        let _ = fs::remove_dir_all(&work_b);

        outcomes.push((
            recipe.id.clone(),
            if unstable {
                Outcome::SizeUnstable {
                    a: bytes_a.len() as u64,
                    b: bytes_b.len() as u64,
                }
            } else {
                Outcome::Produced {
                    volatile_bytes,
                    size: meta.size,
                }
            },
        ));
    }

    let have_ids: Vec<&str> = recipes.iter().map(|r| r.id.as_str()).collect();
    let missing_recipes = builders
        .keys()
        .filter(|id| !have_ids.contains(&id.as_str()))
        .cloned()
        .collect();
    let unknown_formats = recipes
        .iter()
        .filter(|r| !known_formats.contains_key(&r.id))
        .map(|r| r.id.clone())
        .collect();

    Ok(Report {
        outcomes,
        missing_recipes,
        unknown_formats,
    })
}

fn first_line(s: &str) -> String {
    s.lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("")
        .trim()
        .chars()
        .take(160)
        .collect()
}

pub fn render(report: &Report, out_root: &Path) -> String {
    let mut s = String::new();
    let mut produced = 0;
    let mut failed = 0;
    let mut unstable = 0;

    for (id, outcome) in &report.outcomes {
        match outcome {
            Outcome::Produced {
                volatile_bytes,
                size,
            } => {
                produced += 1;
                let note = if *volatile_bytes == 0 {
                    "deterministic".to_string()
                } else {
                    format!("{} volatile byte(s)", volatile_bytes)
                };
                s.push_str(&format!("ok    {:<24} {:>10} B  {}\n", id, size, note));
            }
            Outcome::SizeUnstable { a, b } => {
                unstable += 1;
                s.push_str(&format!("warn  {:<24} size unstable: {} vs {}\n", id, a, b));
            }
            Outcome::Failed { step, detail } => {
                failed += 1;
                s.push_str(&format!("FAIL  {:<24} {}: {}\n", id, step, detail));
            }
        }
    }

    s.push('\n');
    s.push_str(&format!(
        "produced {}, unstable {}, failed {}\nartifacts: {}\n",
        produced,
        unstable,
        failed,
        out_root.display()
    ));

    // A recipe gap is not a failure, but it is the reason a format is absent
    // from the tree, and silence there reads as coverage.
    if !report.missing_recipes.is_empty() {
        s.push_str(&format!(
            "\n{} format(s) have a builder in formats.toml but no recipe in produce.toml:\n",
            report.missing_recipes.len()
        ));
        for id in &report.missing_recipes {
            s.push_str(&format!("  {}\n", id));
        }
    }
    if !report.unknown_formats.is_empty() {
        s.push_str("\nrecipe id(s) not present in formats.toml:\n");
        for id in &report.unknown_formats {
            s.push_str(&format!("  {}\n", id));
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_ranges_coalesces_adjacent_bytes() {
        let a = [0u8, 1, 2, 3, 4, 5];
        let b = [0u8, 9, 9, 3, 4, 9];
        let r = diff_ranges(&a, &b);
        assert_eq!(r.len(), 2);
        assert_eq!((r[0].start, r[0].end), (1, 3));
        assert_eq!((r[1].start, r[1].end), (5, 6));
    }

    #[test]
    fn from_end_resolves_against_the_artifact_size() {
        // A VHD footer is the last 512 bytes; Creator Host OS sits at +0x24.
        let d = ExpectedDivergence {
            at: None,
            from_end: Some(476),
            len: 4,
            reason: "r".into(),
        };
        let r = resolve_expected(&[d], 2_097_664);
        assert_eq!((r[0].start, r[0].end), (0x0020_0024, 0x0020_0028));
    }

    #[test]
    fn a_range_past_the_end_is_dropped_not_clamped() {
        let d = ExpectedDivergence {
            at: Some(90),
            from_end: None,
            len: 20,
            reason: "r".into(),
        };
        assert!(resolve_expected(&[d], 100).is_empty());
    }

    #[test]
    fn identical_inputs_have_no_volatile_ranges() {
        let a = [7u8; 32];
        assert!(diff_ranges(&a, &a).is_empty());
    }
}
