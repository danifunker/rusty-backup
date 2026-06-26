//! Parsing + diffing of the per-partition file manifest cb-dos emits
//! (`manifest-N.json`, Phase 7f/7g; see `crusty-backup/src/cbmanifest.c`).
//!
//! The daemon uses this for **incremental change detection** (Phase 7h, §5b/§5d):
//! when a networked `PUT` arrives for a `NAME` that already has a `NAME.cbk`, the
//! new manifests are diffed against the prior backup's to report which partitions
//! are unchanged and whether the boot chain (`system` block) changed. This is the
//! cheap-gate / bootability-flag half of incremental; the streaming-skip
//! optimization builds on it.
//!
//! Change detection is **rsync-tier best-effort** (size + mtime + attribs +
//! start cluster, per file), exactly as §5b describes — the streamed per-member
//! CRCs remain the integrity guarantee. Swap/page files flagged `volatile` (§6b)
//! are excluded from the change counter, so a churning swap file never makes a
//! backup look "changed."

use serde::Deserialize;
use std::collections::HashMap;

/// One partition's manifest document. Every field defaults so a partial or
/// placeholder manifest (`{"manifest_version":1,"error":"..."}`) still parses.
#[derive(Debug, Deserialize, Default)]
pub struct PartitionManifest {
    #[serde(default)]
    pub filesystem: Option<String>,
    #[serde(default)]
    pub system: Option<SystemBlock>,
    #[serde(default)]
    pub files: Vec<FileEntry>,
}

/// The `system` boot-fingerprint block (§5d): MBR boot-code CRC, the partition's
/// reserved/boot-sectors CRC, and the DOS system files with content hashes.
#[derive(Debug, Deserialize, PartialEq, Eq, Clone, Default)]
pub struct SystemBlock {
    #[serde(default)]
    pub mbr_boot_code_crc: String,
    #[serde(default)]
    pub boot_sectors_crc: String,
    #[serde(default)]
    pub sysfiles: Vec<SysFile>,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone, Default)]
pub struct SysFile {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub mtime: String,
    #[serde(default)]
    pub attr: u32,
    #[serde(default)]
    pub first_cluster: u64,
    #[serde(default)]
    pub contiguous: bool,
    #[serde(default)]
    pub hash: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct FileEntry {
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub mtime: String,
    #[serde(default)]
    pub attr: u32,
    #[serde(default)]
    pub start_cluster: u64,
    #[serde(default)]
    pub dir: bool,
    /// Swap/page file (§6) — excluded from the change counter (§6b).
    #[serde(rename = "volatile", default)]
    pub is_volatile: bool,
}

impl PartitionManifest {
    pub fn parse(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }

    /// A manifest carrying neither a `system` block nor any files — e.g. the
    /// `{"error":"manifest unavailable"}` placeholder cb-dos ships when it can't
    /// read the directory tree. Not comparable.
    pub fn is_placeholder(&self) -> bool {
        self.system.is_none() && self.files.is_empty()
    }
}

/// Result of diffing one partition's new manifest against the prior backup's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionDiff {
    /// The `system` block (boot code / boot sectors / sysfile hashes) differs —
    /// a bootability change worth surfacing (§5d).
    pub bootability_changed: bool,
    /// Count of non-volatile files added, removed, or modified (size / mtime /
    /// attribs / start cluster / dir-ness).
    pub files_changed: usize,
    /// False when either side was a placeholder (nothing meaningful to compare).
    pub comparable: bool,
}

impl PartitionDiff {
    /// Comparable and nothing changed (boot chain identical, no file diffs).
    pub fn is_unchanged(&self) -> bool {
        self.comparable && !self.bootability_changed && self.files_changed == 0
    }
}

/// Diff a new partition manifest against the prior backup's (§5b). Non-volatile
/// files are keyed by path → (size, mtime, attribs, start cluster, dir); the
/// `system` blocks are compared whole for the bootability flag.
pub fn diff_partition(prior: &PartitionManifest, new: &PartitionManifest) -> PartitionDiff {
    if prior.is_placeholder() || new.is_placeholder() {
        return PartitionDiff {
            bootability_changed: false,
            files_changed: 0,
            comparable: false,
        };
    }

    let bootability_changed = prior.system != new.system;

    // Key every non-volatile file by its identifying tuple. Swap files (§6b) are
    // excluded so their per-boot churn never registers as a change.
    type Key = (u64, String, u32, u64, bool);
    let index = |files: &[FileEntry]| -> HashMap<String, Key> {
        files
            .iter()
            .filter(|f| !f.is_volatile)
            .map(|f| {
                (
                    f.path.clone(),
                    (f.size, f.mtime.clone(), f.attr, f.start_cluster, f.dir),
                )
            })
            .collect()
    };
    let prior_idx = index(&prior.files);
    let new_idx = index(&new.files);

    let mut files_changed = 0usize;
    for (path, k) in &new_idx {
        match prior_idx.get(path) {
            Some(pk) if pk == k => {} // unchanged
            _ => files_changed += 1,  // added or modified
        }
    }
    for path in prior_idx.keys() {
        if !new_idx.contains_key(path) {
            files_changed += 1; // removed
        }
    }

    PartitionDiff {
        bootability_changed,
        files_changed,
        comparable: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"{
      "manifest_version": 1,
      "filesystem": "fat16",
      "system": {
        "mbr_boot_code_crc": "aed2dfe7",
        "boot_sectors_crc": "4b0f3e71",
        "sysfiles": [
          {"name": "IO.SYS", "size": 8192, "mtime": "2026-06-25T16:24:30", "attr": 32, "first_cluster": 2, "contiguous": true, "hash": "beac2c52"}
        ]
      },
      "files": [
        {"path": "\\IO.SYS", "size": 8192, "mtime": "2026-06-25T16:24:30", "attr": 32, "start_cluster": 2},
        {"path": "\\WIN386.SWP", "size": 524288, "mtime": "2026-06-25T16:24:30", "attr": 32, "start_cluster": 2326, "volatile": true, "content": "zeroed"},
        {"path": "\\REPORT.TXT", "size": 4096, "mtime": "2026-06-25T16:24:30", "attr": 32, "start_cluster": 40}
      ]
    }"#;

    #[test]
    fn identical_manifest_is_unchanged() {
        let a = PartitionManifest::parse(BASE.as_bytes()).unwrap();
        let b = PartitionManifest::parse(BASE.as_bytes()).unwrap();
        let d = diff_partition(&a, &b);
        assert!(d.is_unchanged(), "{d:?}");
    }

    #[test]
    fn a_changed_data_file_counts_but_swap_churn_does_not() {
        let prior = PartitionManifest::parse(BASE.as_bytes()).unwrap();
        // REPORT.TXT grows; the volatile WIN386.SWP "moves" + changes mtime.
        let modified = BASE
            .replace(
                r#""path": "\\REPORT.TXT", "size": 4096"#,
                r#""path": "\\REPORT.TXT", "size": 9000"#,
            )
            .replace(r#""start_cluster": 2326"#, r#""start_cluster": 9999"#);
        let new = PartitionManifest::parse(modified.as_bytes()).unwrap();
        let d = diff_partition(&prior, &new);
        assert!(!d.bootability_changed);
        assert_eq!(d.files_changed, 1, "only REPORT.TXT counts, not swap churn");
        assert!(!d.is_unchanged());
    }

    #[test]
    fn a_changed_sysfile_hash_flags_bootability() {
        let prior = PartitionManifest::parse(BASE.as_bytes()).unwrap();
        let reflashed = BASE.replace(r#""hash": "beac2c52""#, r#""hash": "deadbeef""#);
        let new = PartitionManifest::parse(reflashed.as_bytes()).unwrap();
        let d = diff_partition(&prior, &new);
        assert!(d.bootability_changed, "sysfile content hash changed");
    }

    #[test]
    fn placeholder_manifest_is_not_comparable() {
        let placeholder =
            PartitionManifest::parse(br#"{"manifest_version":1,"error":"x"}"#).unwrap();
        let real = PartitionManifest::parse(BASE.as_bytes()).unwrap();
        assert!(!diff_partition(&placeholder, &real).comparable);
        assert!(placeholder.is_placeholder());
    }
}
