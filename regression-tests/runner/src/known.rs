//! Known failures — the bug list, applied to verdicts.
//!
//! A case listed here still runs. Only the reading of its result changes, so a
//! fixed bug announces itself as `XPASS` instead of quietly staying green while
//! the entry rots. See `data/known-failures.toml` for the reasoning.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct Entry {
    id: String,
    finding: String,
    #[serde(default)]
    note: Option<String>,
    /// Platforms the finding applies to, e.g. `["windows"]`. Empty means all.
    ///
    /// Without this a platform-specific finding is a false XPASS everywhere
    /// else, and XPASS is supposed to mean "fixed, remove the entry" — not
    /// "never applied here". R-025 (`squashfs put` cannot replace the image on
    /// Windows) produced exactly that on the first macOS run.
    #[serde(default)]
    platforms: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct File_ {
    #[serde(default)]
    known: Vec<Entry>,
}

#[derive(Debug, Default)]
pub struct KnownFailures {
    by_case: BTreeMap<String, (String, Option<String>)>,
    /// Entries filtered out because they do not apply to this platform, as
    /// (id, platforms, finding). Kept so a run can say so rather than
    /// behaving differently here for no visible reason, and so `validate`
    /// still checks them — a stale entry scoped to another platform is just
    /// as stale.
    elsewhere: Vec<(String, String, String)>,
}

impl KnownFailures {
    /// Absent file is not an error — a fresh checkout has no known failures,
    /// and that state should mean "everything must pass", not a hard stop.
    pub fn load(path: &Path) -> Result<KnownFailures, String> {
        if !path.is_file() {
            return Ok(KnownFailures::default());
        }
        let text = fs::read_to_string(path).map_err(|e| format!("{}: {}", path.display(), e))?;
        let parsed: File_ =
            toml::from_str(&text).map_err(|e| format!("{}: {}", path.display(), e))?;
        let here = crate::exec::platform_token();
        let mut by_case = BTreeMap::new();
        let mut elsewhere = Vec::new();
        for e in parsed.known {
            if !e.platforms.is_empty() && !e.platforms.iter().any(|p| p == here) {
                elsewhere.push((e.id, e.platforms.join(","), e.finding));
                continue;
            }
            by_case.insert(e.id, (e.finding, e.note));
        }
        Ok(KnownFailures {
            by_case,
            elsewhere,
        })
    }

    /// Entries scoped to other platforms, so a run can report what it is not
    /// applying rather than leaving the difference invisible.
    pub fn not_applicable_here(&self) -> &[(String, String, String)] {
        &self.elsewhere
    }

    /// Every entry in the file, applicable here or not, as (id, finding).
    /// `validate` uses this: correctness of the list is not platform-specific.
    pub fn all_entries(&self) -> Vec<(&str, &str)> {
        let mut v: Vec<(&str, &str)> = self
            .by_case
            .iter()
            .map(|(id, (f, _))| (id.as_str(), f.as_str()))
            .collect();
        v.extend(
            self.elsewhere
                .iter()
                .map(|(id, _, f)| (id.as_str(), f.as_str())),
        );
        v
    }

    /// `Some(finding)` when this case is expected to fail.
    pub fn finding_for(&self, case_id: &str) -> Option<&str> {
        self.by_case.get(case_id).map(|(f, _)| f.as_str())
    }

    pub fn note_for(&self, case_id: &str) -> Option<&str> {
        self.by_case.get(case_id).and_then(|(_, n)| n.as_deref())
    }

    pub fn len(&self) -> usize {
        self.by_case.len()
    }
}
