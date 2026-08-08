//! `fixtures` — what we have, and therefore what we can run.
//!
//! Test selection is driven by the corpus, not the other way round. Before a
//! run, this takes inventory: which catalogued fixtures are present on this
//! host, whether their bytes match the catalogue, and which cases that lets us
//! execute.
//!
//! Two rules shape it.
//!
//! **Presence is not enough.** A truncated or half-copied fixture resolves
//! fine and then produces a FAIL that reads like an engine defect. That is the
//! most expensive wrong answer this suite can give, so every fixture with a
//! recorded sha256 is verified against it.
//!
//! **Blocked cases get written out, never silently dropped.** A case skipped
//! for a missing fixture and a case that does not exist look identical in a
//! summary, and the difference is the whole point: one is a sourcing problem
//! with a known shopping list, the other is a coverage hole nobody has noticed.

use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::fixtures::Catalog;
use crate::manifest;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FixtureState {
    /// Present, and the bytes match the catalogue's sha256.
    Verified,
    /// Present, but the catalogue records no hash to check it against.
    Unverified,
    /// Present and the bytes do NOT match. Worse than missing: it will produce
    /// confident, wrong results.
    Corrupt,
    /// Catalogued, not on this host.
    Missing,
}

impl FixtureState {
    /// Can a case that needs this fixture actually run?
    pub fn usable(self) -> bool {
        matches!(self, FixtureState::Verified | FixtureState::Unverified)
    }
}

#[derive(Debug, Serialize)]
pub struct FixtureReport {
    pub id: String,
    pub state: FixtureState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Cases that need it. Empty means a catalogued fixture nothing uses.
    pub cases: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Inventory {
    pub fixtures: Vec<FixtureReport>,
    /// Cases with no fixture requirement — always runnable.
    pub unfixtured_cases: usize,
    /// Cases whose fixture is present and usable.
    pub runnable_cases: Vec<String>,
    /// Cases blocked, and by which fixture.
    pub blocked_cases: Vec<(String, String)>,
    /// Named by a case but absent from the catalogue entirely — a manifest bug,
    /// not a sourcing problem, and reported separately for that reason.
    pub uncatalogued: Vec<(String, String)>,
}

/// Copy the catalogued corpus from `source` into `dest`, so a run reads from
/// local disk instead of over SMB.
///
/// A run that hits the NAS once per fixture is slow and fragile — a dropped
/// share mid-run turns into a wall of skip-fixture that looks like a corpus
/// problem. Copy once, verify once, then run as many times as you like.
///
/// Skips a file that is already present with the right sha256, so re-running
/// is cheap and only fetches what changed.
pub fn sync(catalog: &Catalog, source: &Path, dest: &Path) -> String {
    use std::fs;
    let mut copied = 0usize;
    let mut skipped = 0usize;
    let mut failed = Vec::new();
    let mut bytes = 0u64;

    // The catalogue lives BESIDE the corpus, not in it, so a sync that copies
    // only fixtures leaves the destination with no map. Everything then reads
    // as "not in the catalogue at all" — a manifest bug — when the truth is
    // "not fetched yet". Copy the map first so the two states stay
    // distinguishable.
    for name in ["fixture-map.tsv", "fixture-map.repo.tsv"] {
        if let (Some(sp), Some(dp)) = (source.parent(), dest.parent()) {
            let (a, b) = (sp.join(name), dp.join(name));
            if a.is_file() {
                let _ = fs::create_dir_all(dp);
                if let Err(e) = fs::copy(&a, &b) {
                    failed.push(format!("{}: {}", name, e));
                }
            }
        }
    }

    for row in catalog.rows() {
        // `repo:` rows already live in the checkout; nothing to copy.
        if row.relpath.starts_with("repo:") {
            skipped += 1;
            continue;
        }
        let src = source.join(&row.relpath);
        let dst = dest.join(&row.relpath);
        if !src.exists() {
            failed.push(format!("{}: not at {}", row.id, src.display()));
            continue;
        }

        // A fixture stored inside its own directory is stored that way because
        // it is a SET: a CloneCD dump is .ccd + .img + .sub, a cue is nothing
        // without its .bin. Only the entry point is catalogued, so copying
        // catalogued files alone lands a directory that looks present and
        // fails to open. Bring the whole directory.
        //
        // Found by moving the corpus off the share: three optical cases passed
        // against the NAS and failed against a local copy, because the share
        // still held siblings the sync had never fetched.
        if Path::new(&row.relpath).parent().is_some_and(|p| !p.as_os_str().is_empty()) {
            match copy_dir(src.parent().unwrap(), dst.parent().unwrap()) {
                Ok((n, b)) => {
                    if n == 0 {
                        skipped += 1;
                    } else {
                        copied += n;
                        bytes += b;
                    }
                }
                Err(e) => failed.push(format!("{}: {}", row.id, e)),
            }
            continue;
        }
        // Already here and the right size? Trust it; the full verify pass
        // happens in `take` and would catch a bad one anyway.
        if let (Ok(a), Ok(b)) = (fs::metadata(&src), fs::metadata(&dst)) {
            if a.len() == b.len() {
                skipped += 1;
                continue;
            }
        }
        if let Some(parent) = dst.parent() {
            let _ = fs::create_dir_all(parent);
        }
        match fs::copy(&src, &dst) {
            Ok(n) => {
                copied += 1;
                bytes += n;
            }
            Err(e) => failed.push(format!("{}: {}", row.id, e)),
        }
    }

    let mut s = format!(
        "sync: {} copied ({:.1} MB), {} already current, {} failed
",
        copied,
        bytes as f64 / 1_048_576.0,
        skipped,
        failed.len()
    );
    for f in &failed {
        s.push_str(&format!("  FAILED {}
", f));
    }
    s
}

/// Copy every file in `src` into `dst`, skipping same-size ones. Returns the
/// count and bytes actually written. Not recursive: fixture sets are flat.
fn copy_dir(src: &Path, dst: &Path) -> Result<(usize, u64), String> {
    use std::fs;
    fs::create_dir_all(dst).map_err(|e| format!("{}: {}", dst.display(), e))?;
    let mut n = 0usize;
    let mut bytes = 0u64;
    let entries = fs::read_dir(src).map_err(|e| format!("{}: {}", src.display(), e))?;
    for e in entries.flatten() {
        if !e.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        let from = e.path();
        let to = dst.join(e.file_name());
        if let (Ok(a), Ok(b)) = (fs::metadata(&from), fs::metadata(&to)) {
            if a.len() == b.len() {
                continue;
            }
        }
        let w = fs::copy(&from, &to).map_err(|err| format!("{}: {}", from.display(), err))?;
        n += 1;
        bytes += w;
    }
    Ok((n, bytes))
}

pub fn take(cases_dir: &Path, catalog: &Catalog) -> Inventory {
    let (manifests, _) = manifest::load_dir(cases_dir);

    // case id -> fixture id
    let mut needs: BTreeMap<String, String> = BTreeMap::new();
    let mut unfixtured = 0usize;
    for (_, m) in &manifests {
        for c in &m.cases {
            match &c.fixture {
                Some(f) => {
                    needs.insert(c.id.clone(), f.clone());
                }
                None => unfixtured += 1,
            }
        }
    }

    // fixture id -> cases needing it
    let mut by_fixture: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (case, fixture) in &needs {
        by_fixture
            .entry(fixture.clone())
            .or_default()
            .push(case.clone());
    }

    let catalogued: BTreeSet<String> = catalog.rows().map(|r| r.id.clone()).collect();

    let mut fixtures = Vec::new();
    for row in catalog.rows() {
        let (state, detail) = match catalog.resolve(&row.id) {
            Err(e) => (FixtureState::Missing, Some(e)),
            Ok(_) => match catalog.verify(&row.id) {
                Ok(true) => (FixtureState::Verified, None),
                Ok(false) => (FixtureState::Unverified, None),
                Err(e) => (FixtureState::Corrupt, Some(e)),
            },
        };
        fixtures.push(FixtureReport {
            id: row.id.clone(),
            state,
            detail,
            cases: by_fixture.get(&row.id).cloned().unwrap_or_default(),
        });
    }

    let usable: BTreeSet<&str> = fixtures
        .iter()
        .filter(|f| f.state.usable())
        .map(|f| f.id.as_str())
        .collect();

    let mut runnable = Vec::new();
    let mut blocked = Vec::new();
    let mut uncatalogued = Vec::new();
    for (case, fixture) in &needs {
        if !catalogued.contains(fixture) {
            uncatalogued.push((case.clone(), fixture.clone()));
        } else if usable.contains(fixture.as_str()) {
            runnable.push(case.clone());
        } else {
            blocked.push((case.clone(), fixture.clone()));
        }
    }

    Inventory {
        fixtures,
        unfixtured_cases: unfixtured,
        runnable_cases: runnable,
        blocked_cases: blocked,
        uncatalogued,
    }
}

pub fn render(inv: &Inventory, verbose: bool) -> String {
    let mut s = String::new();
    let count = |st: FixtureState| inv.fixtures.iter().filter(|f| f.state == st).count();

    let verified = count(FixtureState::Verified);
    let unverified = count(FixtureState::Unverified);
    let corrupt = count(FixtureState::Corrupt);
    let missing = count(FixtureState::Missing);
    let total = inv.fixtures.len();

    s.push_str(&format!(
        "fixtures: {} catalogued - {} verified, {} unverified, {} missing, {} CORRUPT\n",
        total, verified, unverified, missing, corrupt
    ));

    // Corrupt first and always: it is the only state that produces confidently
    // wrong results rather than an honest skip.
    if corrupt > 0 {
        s.push_str("\nCORRUPT - present but the bytes do not match the catalogue:\n");
        for f in inv.fixtures.iter().filter(|f| f.state == FixtureState::Corrupt) {
            s.push_str(&format!(
                "  {}\n      {}\n",
                f.id,
                f.detail.as_deref().unwrap_or("")
            ));
        }
    }

    let total_cases = inv.unfixtured_cases + inv.runnable_cases.len() + inv.blocked_cases.len();
    s.push_str(&format!(
        "\ncases: {} total - {} need no fixture, {} runnable, {} blocked\n",
        total_cases,
        inv.unfixtured_cases,
        inv.runnable_cases.len(),
        inv.blocked_cases.len()
    ));

    // The shopping list: what to fetch, and what it would buy.
    if !inv.blocked_cases.is_empty() {
        let mut by_fixture: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (case, fixture) in &inv.blocked_cases {
            by_fixture
                .entry(fixture.as_str())
                .or_default()
                .push(case.as_str());
        }
        s.push_str(&format!(
            "\nblocked on {} missing fixture(s) - fetch these to unblock:\n",
            by_fixture.len()
        ));
        let mut ranked: Vec<_> = by_fixture.into_iter().collect();
        ranked.sort_by_key(|(_, cs)| std::cmp::Reverse(cs.len()));
        for (fixture, cases) in ranked {
            s.push_str(&format!(
                "  {:<40} unblocks {} case(s)\n",
                fixture,
                cases.len()
            ));
            if verbose {
                for c in cases {
                    s.push_str(&format!("      {}\n", c));
                }
            }
        }
    }

    // A case naming a fixture the catalogue has never heard of is a typo in a
    // manifest, not a sourcing problem. Different fix, so different section.
    if !inv.uncatalogued.is_empty() {
        s.push_str("\ncase(s) naming a fixture that is not in the catalogue at all:\n");
        for (case, fixture) in &inv.uncatalogued {
            s.push_str(&format!("  {:<40} wants '{}'\n", case, fixture));
        }
    }

    // Fixtures nobody uses are the other half of the same question: the corpus
    // costs money and copying time, and an unused fixture is either a coverage
    // gap or dead weight.
    let unused: Vec<&FixtureReport> = inv
        .fixtures
        .iter()
        .filter(|f| f.cases.is_empty() && f.state.usable())
        .collect();
    if !unused.is_empty() {
        s.push_str(&format!(
            "\n{} present fixture(s) that no case uses - each is a coverage gap or dead weight:\n",
            unused.len()
        ));
        if verbose {
            for f in &unused {
                s.push_str(&format!("  {}\n", f.id));
            }
        } else {
            for f in unused.iter().take(10) {
                s.push_str(&format!("  {}\n", f.id));
            }
            if unused.len() > 10 {
                s.push_str(&format!("  ... and {} more (--verbose)\n", unused.len() - 10));
            }
        }
    }

    s
}
