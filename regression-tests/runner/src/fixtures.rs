//! Fixture catalogue: logical ID to a real file on this host.
//!
//! Case manifests only ever name fixtures by logical ID. The ID-to-path
//! mapping lives outside the repository (FIXTURES.md), so neither fixture
//! files nor their paths are ever committed.
//!
//! An ID with no file behind it is **not an error**. It resolves to
//! `skip-fixture`, gets written to the run's shopping list, and the run
//! carries on. The corpus is expected to be incomplete for a long time.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// One catalogue row. The trailing columns are unread today and kept anyway:
/// this struct is the only in-code record of the TSV's shape.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct FixtureRow {
    pub id: String,
    pub relpath: String,
    pub bytes: u64,
    pub sha256: String,
    pub origin: String,
    pub redistributable: String,
    pub notes: String,
}

#[derive(Debug, Default)]
pub struct Catalog {
    root: Option<PathBuf>,
    /// Repository root, so catalogue rows may point at the small fixtures
    /// already committed under `tests/fixtures/` via a `repo:` prefix.
    repo_root: Option<PathBuf>,
    rows: BTreeMap<String, FixtureRow>,
    /// Problems loading the catalogue, surfaced in the report rather than
    /// killing the run.
    pub warnings: Vec<String>,
}

impl Catalog {
    /// Load `fixture-map.tsv` sitting beside the corpus directory.
    ///
    /// `root` is the corpus directory itself (`.../rb-fixtures/fixtures`);
    /// the catalogue is expected one level up (`.../rb-fixtures/`).
    ///
    /// A row whose `relpath` starts with `repo:` resolves against the
    /// repository instead of the NAS corpus — that is how the ~4 MB of
    /// fixtures already committed under `tests/fixtures/` get reused rather
    /// than duplicated onto the NAS. Those rows work on any clone with no
    /// network at all.
    pub fn load(root: Option<&Path>, repo_root: Option<&Path>) -> Catalog {
        let mut cat = Catalog {
            repo_root: repo_root.map(|p| p.to_path_buf()),
            ..Default::default()
        };

        // The repo-backed half of the catalogue is usable even with no NAS
        // reachable, so load it before bailing out on a missing corpus.
        if let Some(rr) = repo_root {
            let repo_map = rr.join("regression-tests").join("fixture-map.repo.tsv");
            if repo_map.is_file() {
                cat.ingest(&repo_map);
            }
        }

        let root = match root {
            Some(r) => r,
            None => {
                cat.warnings.push(
                    "no fixture root configured (--fixture-root / RB_FIXTURE_ROOT / local.toml); \
                     every fixture-backed case will report skip-fixture"
                        .to_string(),
                );
                return cat;
            }
        };
        cat.root = Some(root.to_path_buf());

        if !root.is_dir() {
            cat.warnings.push(format!(
                "fixture root {} is not reachable; every fixture-backed case will report skip-fixture",
                root.display()
            ));
            return cat;
        }

        let map_path = root
            .parent()
            .map(|p| p.join("fixture-map.tsv"))
            .unwrap_or_else(|| root.join("fixture-map.tsv"));

        if !map_path.is_file() {
            cat.warnings.push(format!(
                "no fixture catalogue at {}; NAS-backed cases will report skip-fixture",
                map_path.display()
            ));
            return cat;
        }
        cat.ingest(&map_path);

        cat
    }

    /// Read one TSV catalogue into the row table. Later files win on a
    /// duplicate ID, so the NAS catalogue can override a repo row when a
    /// bigger or better fixture becomes available.
    fn ingest(&mut self, map_path: &Path) {
        let text = match fs::read_to_string(map_path) {
            Ok(t) => t,
            Err(e) => {
                self.warnings
                    .push(format!("cannot read {}: {}", map_path.display(), e));
                return;
            }
        };

        for (lineno, line) in text.lines().enumerate() {
            let line = line.trim_end_matches('\r');
            if line.trim().is_empty() || line.starts_with('#') {
                continue;
            }
            let cols: Vec<&str> = line.split('\t').collect();
            // Header row.
            if lineno == 0 && cols.first().map(|c| c.trim() == "id").unwrap_or(false) {
                continue;
            }
            if cols.len() < 2 {
                self.warnings.push(format!(
                    "{}:{}: expected at least id and relpath columns",
                    map_path.display(),
                    lineno + 1
                ));
                continue;
            }
            let get = |i: usize| cols.get(i).map(|s| s.trim().to_string()).unwrap_or_default();
            let row = FixtureRow {
                id: get(0),
                relpath: get(1),
                bytes: get(2).parse().unwrap_or(0),
                sha256: get(3),
                origin: get(5),
                redistributable: get(6),
                notes: get(8),
            };
            if row.id.is_empty() {
                continue;
            }
            self.rows.insert(row.id.clone(), row);
        }
    }

    /// Resolve an ID to a path that actually exists on this host.
    ///
    /// Returns `Err` with a human-readable reason suitable for the shopping
    /// list — "not in the catalogue" and "catalogued but the file is missing
    /// here" are different problems and the report should say which.
    pub fn resolve(&self, id: &str) -> Result<PathBuf, String> {
        let row = self
            .rows
            .get(id)
            .ok_or_else(|| format!("'{}' is not in the fixture catalogue", id))?;

        // `repo:` rows point into the checkout, so they resolve with no NAS
        // and no configured fixture root at all.
        let path = if let Some(rel) = row.relpath.strip_prefix("repo:") {
            let repo = self
                .repo_root
                .as_ref()
                .ok_or_else(|| format!("'{}' is a repo: fixture but the repo root is unknown", id))?;
            repo.join(rel)
        } else {
            let root = self.root.as_ref().ok_or_else(|| {
                format!(
                    "'{}' needs the NAS corpus but no fixture root is configured",
                    id
                )
            })?;
            root.join(&row.relpath)
        };

        if !path.exists() {
            return Err(format!(
                "'{}' is catalogued as {} but that file is not present on this host",
                id, row.relpath
            ));
        }
        Ok(path)
    }

    /// Resolve an ID and hand back something `rb-cli` can actually open.
    ///
    /// The corpus is stored zstd-compressed — that is what keeps it small
    /// enough to live in the repo and to copy to every test machine — but
    /// rb-cli does not open `.zst` in place. Compressed fixtures are expanded
    /// once per run into `cache_dir` and reused by every case that names them,
    /// so a hundred cases sharing one fixture pay the decompression once.
    pub fn materialise(&self, id: &str, cache_dir: &Path) -> Result<PathBuf, String> {
        let stored = self.resolve(id)?;

        let is_zst = stored
            .extension()
            .map(|e| e.eq_ignore_ascii_case("zst"))
            .unwrap_or(false);
        if !is_zst {
            return Ok(stored);
        }

        let stem = stored
            .file_stem()
            .ok_or_else(|| format!("'{}' has no filename stem", id))?;
        let out = cache_dir.join(sanitise(id)).join(stem);

        if out.exists() {
            return Ok(out);
        }
        if let Some(parent) = out.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("cannot create fixture cache for '{}': {}", id, e))?;
        }

        // Decompress to a temporary sibling and rename, so an interrupted run
        // can never leave a truncated fixture behind for the next one to use.
        let tmp = out.with_extension("partial");
        let src = fs::File::open(&stored)
            .map_err(|e| format!("cannot open '{}' ({}): {}", id, stored.display(), e))?;
        let mut dst = fs::File::create(&tmp)
            .map_err(|e| format!("cannot write fixture cache for '{}': {}", id, e))?;
        let mut decoder = zstd::stream::Decoder::new(src)
            .map_err(|e| format!("'{}' is not valid zstd: {}", id, e))?;
        std::io::copy(&mut decoder, &mut dst)
            .map_err(|e| format!("cannot decompress '{}': {}", id, e))?;
        drop(dst);
        fs::rename(&tmp, &out)
            .map_err(|e| format!("cannot finalise fixture cache for '{}': {}", id, e))?;

        Ok(out)
    }

    /// Every catalogued row, for the fixture inventory.
    pub fn rows(&self) -> impl Iterator<Item = &FixtureRow> {
        self.rows.values()
    }

    /// Verify a fixture's bytes against the sha256 the catalogue records.
    ///
    /// Presence is not enough. A truncated or half-copied fixture resolves
    /// fine and then produces a FAIL that looks like an engine defect — the
    /// most expensive kind of wrong answer this suite can give. Rows with no
    /// recorded hash return `Ok(false)`, meaning "present but unverified",
    /// which is reported separately from "verified".
    pub fn verify(&self, id: &str) -> Result<bool, String> {
        let row = self
            .rows
            .get(id)
            .ok_or_else(|| format!("'{}' is not in the catalogue", id))?;
        let path = self.resolve(id)?;
        if row.sha256.is_empty() {
            return Ok(false);
        }
        let bytes = fs::read(&path).map_err(|e| format!("{}: {}", path.display(), e))?;
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(&bytes);
        let got: String = h.finalize().iter().map(|b| format!("{:02x}", b)).collect();
        if got.eq_ignore_ascii_case(&row.sha256) {
            Ok(true)
        } else {
            Err(format!(
                "checksum mismatch: catalogue {}, file {}",
                &row.sha256[..row.sha256.len().min(16)],
                &got[..16]
            ))
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Unused, but `len` without it is its own clippy lint.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn root(&self) -> Option<&Path> {
        self.root.as_deref()
    }
}

/// Fixture IDs are dotted and safe, but never trust one into a path.
fn sanitise(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

