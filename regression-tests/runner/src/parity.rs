//! `parity` — cross-OS comparison of produced artifacts, needing no oracle.
//!
//! Producing everywhere gives this for free, and it catches the class of bug a
//! cross-platform regression exists to find: same format, same arguments, three
//! OSes, and the bytes should match. Where they don't, the OSes have caught
//! each other with no third-party tool involved.
//!
//! It cannot be a naked sha compare, because several builders embed a creation
//! timestamp. `produce` discovers those ranges empirically by building each
//! artifact twice on one machine; this masks the **union** of both sides'
//! ranges before comparing. A difference outside that union is a genuine
//! cross-OS divergence, and the mask is self-calibrating rather than a
//! per-format table that goes stale when a formatter changes.
//!
//! Discovery finds only a lower bound — see the `produce` module header. Two
//! samples seconds apart move the low byte of a seconds field and not the high
//! ones, so a byte within [`ADJACENCY`] of an observed range is reported as
//! *adjacent* rather than counted as a divergence. That keeps a timestamp from
//! reading as a cross-OS bug without silently widening the mask: the adjacent
//! count is printed, so a suspiciously large one is visible.

use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::produce::{ArtifactMeta, VolatileRange};

#[derive(Debug, Clone, Serialize)]
pub struct Divergence {
    pub offset: u64,
    pub left: u8,
    pub right: u8,
}

/// How far outside a known volatile range a divergence may sit and still be
/// read as the rest of the same timestamp field. `produce` only ever observes
/// the bytes that changed across a few seconds — the high bytes of a 32- or
/// 64-bit field never move in that window but do move across hosts producing
/// days apart. Eight bytes covers the widest such field.
const ADJACENCY: u64 = 8;

#[derive(Debug, Clone, Serialize)]
pub enum Verdict {
    /// Bytes agree once volatile ranges are masked.
    Match {
        masked_bytes: u64,
        /// Differed, but within [`ADJACENCY`] of a volatile range — almost
        /// certainly the untravelled half of a timestamp. Reported, not
        /// counted as a divergence, so the distinction stays visible.
        adjacent_bytes: u64,
    },
    /// Same length, differing bytes outside the volatile mask.
    Differ {
        diverged_bytes: u64,
        adjacent_bytes: u64,
        first: Vec<Divergence>,
    },
    /// Different lengths — a divergence too, and a louder one.
    SizeDiffer { left: u64, right: u64 },
    /// One or both sides were marked non-deterministic by `produce`.
    Skipped { reason: String },
}

#[derive(Debug, Serialize)]
pub struct Comparison {
    pub format: String,
    pub left_os: String,
    pub right_os: String,
    pub verdict: Verdict,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub comparisons: Vec<Comparison>,
    /// Formats present on exactly one producer OS — nothing to compare against.
    pub single_producer: BTreeMap<String, String>,
    pub producer_oses: Vec<String>,
}

struct Loaded {
    meta: ArtifactMeta,
    bytes: Vec<u8>,
}

/// True when `offset` falls inside any range.
fn masked(ranges: &[VolatileRange], offset: u64) -> bool {
    ranges.iter().any(|r| offset >= r.start && offset < r.end)
}

/// True when `offset` sits within [`ADJACENCY`] of any range without being in
/// one.
fn adjacent(ranges: &[VolatileRange], offset: u64) -> bool {
    ranges.iter().any(|r| {
        offset >= r.start.saturating_sub(ADJACENCY) && offset < r.end.saturating_add(ADJACENCY)
    })
}

fn load_os_tree(dir: &Path) -> BTreeMap<String, Loaded> {
    let mut out = BTreeMap::new();
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return out,
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if !p.is_dir() {
            continue;
        }
        let meta_path = p.join("meta.json");
        let text = match fs::read_to_string(&meta_path) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let meta: ArtifactMeta = match serde_json::from_str(&text) {
            Ok(m) => m,
            Err(_) => continue,
        };
        let bytes = match fs::read(p.join(&meta.file)) {
            Ok(b) => b,
            Err(_) => continue,
        };
        out.insert(meta.format.clone(), Loaded { meta, bytes });
    }
    out
}

fn compare(left: &Loaded, right: &Loaded) -> Verdict {
    if let Some(reason) = left.meta.nondeterministic.as_ref() {
        return Verdict::Skipped {
            reason: format!("{} is non-deterministic: {}", left.meta.producer_os, reason),
        };
    }
    if let Some(reason) = right.meta.nondeterministic.as_ref() {
        return Verdict::Skipped {
            reason: format!(
                "{} is non-deterministic: {}",
                right.meta.producer_os, reason
            ),
        };
    }
    if left.bytes.len() != right.bytes.len() {
        return Verdict::SizeDiffer {
            left: left.bytes.len() as u64,
            right: right.bytes.len() as u64,
        };
    }

    let mut diverged = 0u64;
    let mut masked_bytes = 0u64;
    let mut adjacent_bytes = 0u64;
    let mut first = Vec::new();
    for i in 0..left.bytes.len() {
        if left.bytes[i] == right.bytes[i] {
            continue;
        }
        let off = i as u64;
        if masked(&left.meta.volatile, off) || masked(&right.meta.volatile, off) {
            masked_bytes += 1;
            continue;
        }
        if adjacent(&left.meta.volatile, off) || adjacent(&right.meta.volatile, off) {
            adjacent_bytes += 1;
            continue;
        }
        diverged += 1;
        // A whole-file divergence list is noise; the first handful is what
        // anyone actually reads before opening a hex editor.
        if first.len() < 16 {
            first.push(Divergence {
                offset: off,
                left: left.bytes[i],
                right: right.bytes[i],
            });
        }
    }

    if diverged == 0 {
        Verdict::Match {
            masked_bytes,
            adjacent_bytes,
        }
    } else {
        Verdict::Differ {
            diverged_bytes: diverged,
            adjacent_bytes,
            first,
        }
    }
}

/// `artifacts_root` holds one directory per producer OS.
pub fn parity(artifacts_root: &Path) -> Result<Report, String> {
    let mut os_dirs: Vec<(String, PathBuf)> = Vec::new();
    let entries =
        fs::read_dir(artifacts_root).map_err(|e| format!("{}: {}", artifacts_root.display(), e))?;
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                os_dirs.push((name.to_string(), p.clone()));
            }
        }
    }
    os_dirs.sort_by(|a, b| a.0.cmp(&b.0));

    let trees: Vec<(String, BTreeMap<String, Loaded>)> = os_dirs
        .iter()
        .map(|(name, dir)| (name.clone(), load_os_tree(dir)))
        .collect();

    let mut all_formats: BTreeSet<String> = BTreeSet::new();
    for (_, tree) in &trees {
        all_formats.extend(tree.keys().cloned());
    }

    let mut comparisons = Vec::new();
    let mut single_producer = BTreeMap::new();

    for format in &all_formats {
        let present: Vec<&(String, BTreeMap<String, Loaded>)> = trees
            .iter()
            .filter(|(_, t)| t.contains_key(format))
            .collect();
        if present.len() < 2 {
            if let Some((os, _)) = present.first() {
                single_producer.insert(format.clone(), os.clone());
            }
            continue;
        }
        // Every unordered pair, so three OSes give three comparisons and a
        // single odd one out is visible rather than averaged away.
        for i in 0..present.len() {
            for j in (i + 1)..present.len() {
                let (lo, lt) = present[i];
                let (ro, rt) = present[j];
                comparisons.push(Comparison {
                    format: format.clone(),
                    left_os: lo.clone(),
                    right_os: ro.clone(),
                    verdict: compare(&lt[format], &rt[format]),
                });
            }
        }
    }

    Ok(Report {
        comparisons,
        single_producer,
        producer_oses: trees.into_iter().map(|(n, _)| n).collect(),
    })
}

pub fn render(report: &Report) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "producer OSes: {}\n\n",
        if report.producer_oses.is_empty() {
            "(none)".to_string()
        } else {
            report.producer_oses.join(", ")
        }
    ));

    let (mut ok, mut bad, mut skipped) = (0, 0, 0);
    for c in &report.comparisons {
        match &c.verdict {
            Verdict::Match {
                masked_bytes,
                adjacent_bytes,
            } => {
                ok += 1;
                let note = match (*masked_bytes, *adjacent_bytes) {
                    (0, 0) => "identical".to_string(),
                    (m, 0) => format!("identical outside {} masked byte(s)", m),
                    (m, a) => format!("identical outside {} masked + {} adjacent byte(s)", m, a),
                };
                s.push_str(&format!(
                    "ok    {:<24} {} vs {}  {}\n",
                    c.format, c.left_os, c.right_os, note
                ));
            }
            Verdict::Differ {
                diverged_bytes,
                adjacent_bytes: _,
                first,
            } => {
                bad += 1;
                s.push_str(&format!(
                    "DIFF  {:<24} {} vs {}  {} byte(s) outside the mask\n",
                    c.format, c.left_os, c.right_os, diverged_bytes
                ));
                for d in first {
                    s.push_str(&format!(
                        "        @{:#010x}  {:02x} != {:02x}\n",
                        d.offset, d.left, d.right
                    ));
                }
            }
            Verdict::SizeDiffer { left, right } => {
                bad += 1;
                s.push_str(&format!(
                    "DIFF  {:<24} {} vs {}  size {} != {}\n",
                    c.format, c.left_os, c.right_os, left, right
                ));
            }
            Verdict::Skipped { reason } => {
                skipped += 1;
                s.push_str(&format!(
                    "skip  {:<24} {} vs {}  {}\n",
                    c.format, c.left_os, c.right_os, reason
                ));
            }
        }
    }

    s.push('\n');
    s.push_str(&format!(
        "match {}, differ {}, skipped {}\n",
        ok, bad, skipped
    ));

    if !report.single_producer.is_empty() {
        s.push_str(&format!(
            "\n{} format(s) produced on one OS only — nothing to compare against yet:\n",
            report.single_producer.len()
        ));
        for (format, os) in &report.single_producer {
            s.push_str(&format!("  {:<24} {}\n", format, os));
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loaded(os: &str, bytes: &[u8], volatile: Vec<VolatileRange>) -> Loaded {
        Loaded {
            meta: ArtifactMeta {
                format: "fs.test".into(),
                file: "image.img".into(),
                producer_os: os.into(),
                producer_host: "h".into(),
                rb_version: "v".into(),
                git_sha: "s".into(),
                argv: vec![],
                sha256: String::new(),
                size: bytes.len() as u64,
                volatile,
                nondeterministic: None,
            },
            bytes: bytes.to_vec(),
        }
    }

    #[test]
    fn timestamp_bytes_are_masked_but_real_divergence_is_not() {
        let mut left = vec![0u8; 64];
        let mut right = vec![0u8; 64];
        // Byte 1 is a known-volatile timestamp byte; byte 40 is well clear of
        // it and of the adjacency window, so it is a genuine divergence.
        left[1] = 2;
        right[1] = 9;
        left[40] = 3;
        right[40] = 8;
        let a = loaded("windows", &left, vec![VolatileRange { start: 1, end: 2 }]);
        let b = loaded("macos", &right, vec![]);
        match compare(&a, &b) {
            Verdict::Differ {
                diverged_bytes,
                first,
                ..
            } => {
                assert_eq!(diverged_bytes, 1);
                assert_eq!(first[0].offset, 40);
            }
            other => panic!("expected Differ, got {:?}", other),
        }
    }

    #[test]
    fn a_byte_beside_a_volatile_range_is_adjacent_not_a_finding() {
        // The case this exists for: `produce` saw the low byte of a timestamp
        // move across three seconds and never the high bytes, but two hosts
        // producing days apart differ in those too. Reported, not a divergence.
        let mut left = vec![0u8; 64];
        let mut right = vec![0u8; 64];
        left[4] = 1;
        right[4] = 2; // observed volatile
        left[7] = 1;
        right[7] = 9; // three bytes further into the same field
        let a = loaded("windows", &left, vec![VolatileRange { start: 4, end: 5 }]);
        let b = loaded("macos", &right, vec![]);
        match compare(&a, &b) {
            Verdict::Match {
                masked_bytes,
                adjacent_bytes,
            } => {
                assert_eq!(masked_bytes, 1);
                assert_eq!(adjacent_bytes, 1);
            }
            other => panic!("expected Match, got {:?}", other),
        }
    }

    #[test]
    fn mask_is_the_union_of_both_sides() {
        // Volatile on the right only — still masked.
        let a = loaded("windows", &[1, 2], vec![]);
        let b = loaded("macos", &[1, 9], vec![VolatileRange { start: 1, end: 2 }]);
        match compare(&a, &b) {
            Verdict::Match { masked_bytes, .. } => assert_eq!(masked_bytes, 1),
            other => panic!("expected Match, got {:?}", other),
        }
    }

    #[test]
    fn nondeterministic_side_is_skipped_not_failed() {
        let mut a = loaded("windows", &[1, 2], vec![]);
        a.meta.nondeterministic = Some("sizes differed".into());
        let b = loaded("macos", &[1, 2], vec![]);
        assert!(matches!(compare(&a, &b), Verdict::Skipped { .. }));
    }
}
