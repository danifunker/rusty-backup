//! `IMG@N` partition-selector parser.
//!
//! `IMG` is a path to an image file (or eventually a block device);
//! the optional `@N` suffix names a 1-based partition index inside that
//! image. The `@` character is unambiguous because it is illegal in
//! every filesystem name we support, and is safe-by-default in every
//! shell that matters (bash, zsh, fish, sh, dash, PowerShell, cmd) when
//! used mid-token.
//!
//! Real-world disk-image filenames very rarely contain `@`. The
//! `--partition N` flag stays available as a fallback for anyone whose
//! filename does.

use anyhow::{bail, Result};
use std::path::PathBuf;

/// Parsed `IMG[@N]` argument.
///
/// `path` is the image / device path; `partition` is the 1-based index
/// supplied via `@N`, or `None` when the user didn't pass one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageRef {
    pub path: PathBuf,
    pub partition: Option<u32>,
}

impl ImageRef {
    /// Parse a string of the form `"path"` or `"path@N"`.
    ///
    /// Errors when:
    /// - the string is empty;
    /// - more than one `@` is present (likely a typo / a filename
    ///   containing `@` — use `--partition` as the fallback);
    /// - the `@N` suffix is not a positive integer.
    pub fn parse(s: &str) -> Result<Self> {
        if s.is_empty() {
            bail!("empty image reference");
        }

        // Count `@` to detect ambiguity. Most paths have zero; a single one
        // means "path@N"; two or more means we won't try to disambiguate.
        let at_positions: Vec<usize> = s.match_indices('@').map(|(i, _)| i).collect();

        match at_positions.len() {
            0 => Ok(Self {
                path: PathBuf::from(s),
                partition: None,
            }),
            1 => {
                let at = at_positions[0];
                let (path_str, partition_str) = s.split_at(at);
                // partition_str includes the leading '@'.
                let partition_str = &partition_str[1..];
                if path_str.is_empty() {
                    bail!("image reference missing path component before '@'");
                }
                if partition_str.is_empty() {
                    bail!("image reference missing partition index after '@'");
                }
                let n: u32 = partition_str.parse().map_err(|_| {
                    anyhow::anyhow!(
                        "invalid partition index after '@' in {s:?} (expected 1-based integer)"
                    )
                })?;
                if n == 0 {
                    bail!("partition index 0 is invalid (use 1-based indexing)");
                }
                Ok(Self {
                    path: PathBuf::from(path_str),
                    partition: Some(n),
                })
            }
            _ => bail!(
                "image reference {s:?} contains multiple '@' characters; \
                 if the filename itself contains '@', use --partition N instead"
            ),
        }
    }
}

impl std::str::FromStr for ImageRef {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_path_only() {
        let r = ImageRef::parse("disk.hda").unwrap();
        assert_eq!(r.path, PathBuf::from("disk.hda"));
        assert!(r.partition.is_none());
    }

    #[test]
    fn parses_partition_suffix() {
        let r = ImageRef::parse("disk.hda@2").unwrap();
        assert_eq!(r.path, PathBuf::from("disk.hda"));
        assert_eq!(r.partition, Some(2));
    }

    #[test]
    fn accepts_complex_paths() {
        let r = ImageRef::parse("/path/to/disk.hda@1").unwrap();
        assert_eq!(r.path, PathBuf::from("/path/to/disk.hda"));
        assert_eq!(r.partition, Some(1));

        let r = ImageRef::parse("../images/foo.img").unwrap();
        assert_eq!(r.path, PathBuf::from("../images/foo.img"));
        assert!(r.partition.is_none());
    }

    #[test]
    fn rejects_zero_partition() {
        assert!(ImageRef::parse("disk.hda@0").is_err());
    }

    #[test]
    fn rejects_non_numeric_suffix() {
        assert!(ImageRef::parse("disk.hda@abc").is_err());
        assert!(ImageRef::parse("disk.hda@1.5").is_err());
    }

    #[test]
    fn rejects_empty_components() {
        assert!(ImageRef::parse("").is_err());
        assert!(ImageRef::parse("@2").is_err());
        assert!(ImageRef::parse("disk.hda@").is_err());
    }

    #[test]
    fn rejects_multiple_at_symbols() {
        // Two `@` is ambiguous; user should use --partition instead.
        assert!(ImageRef::parse("disk@a@2").is_err());
    }

    #[test]
    fn fromstr_works_for_clap() {
        let r: ImageRef = "disk.hda@3".parse().unwrap();
        assert_eq!(r.partition, Some(3));
    }

    #[test]
    fn windows_style_path_without_partition() {
        // `:` and `\` mid-path don't trip the parser.
        let r = ImageRef::parse(r"C:\Users\foo\disk.hda").unwrap();
        assert_eq!(r.path, PathBuf::from(r"C:\Users\foo\disk.hda"));
        assert!(r.partition.is_none());
    }
}
