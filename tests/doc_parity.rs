//! Documentation-to-source parity.
//!
//! R-001, R-002 and R-018 were all the same failure: a hand-kept list in a
//! markdown file drifted from the code or the CI config it described, and all
//! three were found by a human reading both halves. Nothing in the regression
//! suite could catch them — the suite runs the binary, and these are claims
//! *about* the binary. These tests read both halves instead.
//!
//! Scope is deliberately narrow: only pairs where one side is machine-readable
//! and the drift has actually happened. A test that has to guess at prose is
//! worse than no test, because it gets muted.

use std::fs;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

/// The lines of the markdown table that follows `heading`, up to the next
/// heading of any level.
fn table_rows_under(doc: &str, heading: &str) -> Vec<String> {
    let start = doc
        .find(heading)
        .unwrap_or_else(|| panic!("heading {heading:?} not found — was it renamed?"));
    let rest = &doc[start + heading.len()..];
    let end = rest.find("\n#").unwrap_or(rest.len());
    rest[..end]
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with('|'))
        .map(str::to_string)
        .collect()
}

/// The first cell of a markdown table row.
fn first_cell(row: &str) -> String {
    row.trim_matches('|')
        .split('|')
        .next()
        .unwrap_or("")
        .trim()
        .to_string()
}

/// R-001: every `PartitionTable` variant needs a row in the README's
/// partition-table table. AHDI and X68000 were both missing when this was
/// found by hand; DSD went missing later, which is the drift repeating.
#[test]
fn readme_documents_every_partition_table_scheme() {
    let readme = read("README.md");
    let cells: Vec<String> = table_rows_under(&readme, "### Partition tables")
        .iter()
        .map(|r| first_cell(r).to_uppercase())
        .collect();

    let missing: Vec<&str> = rusty_backup::partition::PartitionTable::ALL_TYPE_NAMES
        .iter()
        .copied()
        .filter(|name| {
            let want = name.to_uppercase();
            !cells.iter().any(|c| c.contains(&want))
        })
        .collect();

    assert!(
        missing.is_empty(),
        "README.md 'Partition tables' has no row for {missing:?}. \
         Every PartitionTable variant is a scheme a user can open, so it needs one. \
         Rows found: {cells:?}"
    );
}

/// R-002: the stale capability table in `src/fs/README.md` was deleted in
/// favour of pointing at the live dispatch, because a hand-kept table two
/// levels below the code it describes cannot keep up with forty drivers.
/// This stops one growing back.
#[test]
fn fs_readme_has_no_hand_kept_capability_table() {
    let doc = read("src/fs/README.md");
    assert!(
        !doc.contains("(planned)"),
        "src/fs/README.md claims a filesystem is 'planned'. That claim outlived \
         the ext driver by years (R-002). Capabilities belong in the top-level \
         README's Filesystems table; routing belongs in fs/mod.rs."
    );
    assert!(
        !doc.contains("| Browsing |"),
        "a per-filesystem capability table has grown back in src/fs/README.md. \
         It went stale last time (R-002) — see the note in that file."
    );
}

/// R-018: CONTRIBUTING.md's vintage-build verification command must be exactly
/// what CI's Windows vintage leg runs. It drifted by one feature
/// (`windows-legacy`), and the resulting error named two call sites, so it read
/// as an engine bug rather than a stale doc.
#[test]
fn contributing_vintage_features_match_ci() {
    let contributing = read("CONTRIBUTING.md");
    let workflow = read(".github/workflows/release.yml");

    let features = contributing
        .lines()
        .find_map(|l| {
            let l = l.trim();
            l.strip_prefix("--no-default-features --features ")
                .map(|rest| rest.trim_end_matches('\\').trim().to_string())
        })
        .expect("CONTRIBUTING.md no longer has a '--no-default-features --features ...' line");

    assert!(
        workflow.contains(&features),
        "CONTRIBUTING.md documents the vintage feature list as `{features}`, which \
         appears nowhere in .github/workflows/release.yml. The two must stay together \
         (R-018) — a doc-only feature list is one nobody has ever built."
    );
}
