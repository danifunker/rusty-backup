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
}

#[derive(Debug, Deserialize, Default)]
struct File_ {
    #[serde(default)]
    known: Vec<Entry>,
}

#[derive(Debug, Default)]
pub struct KnownFailures {
    by_case: BTreeMap<String, (String, Option<String>)>,
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
        let mut by_case = BTreeMap::new();
        for e in parsed.known {
            by_case.insert(e.id, (e.finding, e.note));
        }
        Ok(KnownFailures { by_case })
    }

    /// `Some(finding)` when this case is expected to fail.
    pub fn finding_for(&self, case_id: &str) -> Option<&str> {
        self.by_case.get(case_id).map(|(f, _)| f.as_str())
    }

    pub fn note_for(&self, case_id: &str) -> Option<&str> {
        self.by_case.get(case_id).and_then(|(_, n)| n.as_deref())
    }

    pub fn ids(&self) -> impl Iterator<Item = &str> {
        self.by_case.keys().map(|s| s.as_str())
    }

    pub fn len(&self) -> usize {
        self.by_case.len()
    }
}
