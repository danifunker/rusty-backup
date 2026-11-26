//! `IMG@N` partition-selector parser.
//!
//! `IMG` is a path to an image file (or a block device); the optional suffix
//! names one partition inside it. The `@` character is unambiguous because it
//! is illegal in every filesystem name we support, and is safe-by-default in
//! every shell that matters (bash, zsh, fish, sh, dash, PowerShell, cmd) when
//! used mid-token.
//!
//! Three suffix forms, in precedence order:
//!
//! | form | meaning | example |
//! |------|---------|---------|
//! | `@N` | 1-based position in the list `inspect` prints | `disk.img@2` |
//! | `@sN` | the partition table's own slot, as the platform names it | `disk.img@s6` |
//! | `@NAME` | AmigaDOS device name (RDB only) | `amiga.hdf@DH0` |
//!
//! `@N` is portable across every table type; `@sN` is stable across changes to
//! which partitions we consider browsable. See `docs/partition-selectors.md`.
//!
//! Real-world disk-image filenames very rarely contain `@`. The
//! `--partition N` flag stays available as a fallback for anyone whose
//! filename does.

use anyhow::{bail, Result};
use std::path::PathBuf;

/// Which partition an `IMG@…` suffix names. Serialized over the wire as-is.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PartSelector {
    /// `@N` — 1-based position in the `#` column `inspect` prints.
    Position(u32),
    /// `@sN` — the table's own slot, spelled as the platform spells it.
    Slot(u32),
    /// `@DH0` — AmigaDOS device name, case-insensitive. RDB only.
    Name(String),
}

impl PartSelector {
    /// How the user typed it, for error messages that echo the input back.
    pub fn display(&self) -> String {
        match self {
            PartSelector::Position(n) => format!("@{n}"),
            PartSelector::Slot(n) => format!("@s{n}"),
            PartSelector::Name(s) => format!("@{s}"),
        }
    }
}

impl From<u32> for PartSelector {
    /// A bare `--partition N` flag is the positional form.
    fn from(n: u32) -> Self {
        PartSelector::Position(n)
    }
}

impl std::fmt::Display for PartSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.display())
    }
}

/// Parsed `IMG[@…]` argument.
///
/// `path` is the image / device path; `partition` is the selector supplied
/// after `@`, or `None` when the user didn't pass one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageRef {
    pub path: PathBuf,
    pub partition: Option<PartSelector>,
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
                Ok(Self {
                    path: PathBuf::from(path_str),
                    partition: Some(parse_selector(partition_str, s)?),
                })
            }
            _ => bail!(
                "image reference {s:?} contains multiple '@' characters; \
                 if the filename itself contains '@', use --partition N instead"
            ),
        }
    }
}

/// Classify the text after `@`. All-digits is a position, `s` + digits a slot,
/// anything else a device name. `whole` is the original argument, for errors.
fn parse_selector(suffix: &str, whole: &str) -> Result<PartSelector> {
    if suffix.chars().all(|c| c.is_ascii_digit()) {
        let n: u32 = suffix.parse().map_err(|_| {
            anyhow::anyhow!("invalid partition index after '@' in {whole:?} (expected an integer)")
        })?;
        if n == 0 {
            bail!("partition index 0 is invalid (use 1-based indexing, or @s0 for slot 0)");
        }
        return Ok(PartSelector::Position(n));
    }

    // Slot 0 is legal here: SGI numbers its partitions from zero.
    if let Some(rest) = suffix
        .strip_prefix('s')
        .or_else(|| suffix.strip_prefix('S'))
    {
        if !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()) {
            let n: u32 = rest
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid slot number after '@s' in {whole:?}"))?;
            return Ok(PartSelector::Slot(n));
        }
    }

    Ok(PartSelector::Name(suffix.to_string()))
}

impl std::str::FromStr for ImageRef {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

/// Detect an `@N` suffix on a verb that takes a *whole disk* rather than an
/// `IMG@N` reference (`inspect`, `partmap`).
///
/// Those take a plain path, so a trailing `@2` is read as part of the filename
/// and the open fails with `No such file or directory` - which describes a
/// missing file, not a misapplied selector, and reads as "`@N` is broken" to
/// anyone who has just been told `@N` is how you choose a partition. Returns
/// `Some((real_path, index))` only when stripping the suffix names a file that
/// exists, so a filename that genuinely ends in `@2` is left alone.
pub fn stray_selector(path: &std::path::Path) -> Option<(PathBuf, String)> {
    let s = path.to_str()?;
    let at = s.rfind('@')?;
    let (prefix, suffix) = s.split_at(at);
    if prefix.is_empty() {
        return None;
    }
    // Numeric forms only: a name-shaped suffix is likelier to be a filename.
    let body = &suffix[1..];
    let numeric = body.chars().all(|c| c.is_ascii_digit()) && body.parse::<u32>().ok()? > 0;
    let slotted = body
        .strip_prefix('s')
        .is_some_and(|r| !r.is_empty() && r.chars().all(|c| c.is_ascii_digit()));
    if !numeric && !slotted {
        return None;
    }
    let real = PathBuf::from(prefix);
    real.exists().then_some((real, body.to_string()))
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
        assert_eq!(r.partition, Some(PartSelector::Position(2)));
    }

    #[test]
    fn accepts_complex_paths() {
        let r = ImageRef::parse("/path/to/disk.hda@1").unwrap();
        assert_eq!(r.path, PathBuf::from("/path/to/disk.hda"));
        assert_eq!(r.partition, Some(PartSelector::Position(1)));

        let r = ImageRef::parse("../images/foo.img").unwrap();
        assert_eq!(r.path, PathBuf::from("../images/foo.img"));
        assert!(r.partition.is_none());
    }

    #[test]
    fn parses_the_slot_form() {
        // `@sN` is the table's own numbering, so slot 0 is legal here even
        // though `@0` is not — SGI volume headers number from zero.
        let r = ImageRef::parse("disk.hda@s6").unwrap();
        assert_eq!(r.path, PathBuf::from("disk.hda"));
        assert_eq!(r.partition, Some(PartSelector::Slot(6)));
        assert_eq!(
            ImageRef::parse("irix.img@s0").unwrap().partition,
            Some(PartSelector::Slot(0)),
        );
        assert_eq!(
            ImageRef::parse("disk.hda@S6").unwrap().partition,
            Some(PartSelector::Slot(6)),
        );
    }

    #[test]
    fn parses_the_amiga_device_name_form() {
        let r = ImageRef::parse("amiga.hdf@DH0").unwrap();
        assert_eq!(r.path, PathBuf::from("amiga.hdf"));
        assert_eq!(r.partition, Some(PartSelector::Name("DH0".into())));
        // A name that merely starts with `s` is still a name.
        assert_eq!(
            ImageRef::parse("amiga.hdf@System").unwrap().partition,
            Some(PartSelector::Name("System".into())),
        );
    }

    #[test]
    fn rejects_zero_partition() {
        assert!(ImageRef::parse("disk.hda@0").is_err());
    }

    #[test]
    fn non_numeric_suffixes_are_device_names_now() {
        // These used to be errors; they are RDB device names today and fail
        // later, at resolution, where we can say which names the disk has.
        assert_eq!(
            ImageRef::parse("disk.hda@abc").unwrap().partition,
            Some(PartSelector::Name("abc".into())),
        );
        assert_eq!(
            ImageRef::parse("disk.hda@1.5").unwrap().partition,
            Some(PartSelector::Name("1.5".into())),
        );
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
        assert_eq!(r.partition, Some(PartSelector::Position(3)));
    }

    /// The hint fires only when stripping `@N` names something real, so a
    /// missing file still reports as missing and a filename that genuinely
    /// ends in `@2` is left alone.
    #[test]
    fn stray_selector_only_fires_when_the_stripped_path_exists() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::write(&img, b"x").unwrap();

        let with_suffix = dir.path().join("disk.img@2");
        assert_eq!(
            stray_selector(&with_suffix),
            Some((img.clone(), "2".to_string())),
            "a selector on an existing image should be recognised"
        );
        assert_eq!(
            stray_selector(&dir.path().join("disk.img@s6")),
            Some((img.clone(), "s6".to_string())),
            "the slot form lands on whole-disk verbs the same way"
        );
        // A name-shaped suffix is left alone: far likelier to be a filename.
        assert!(stray_selector(&dir.path().join("disk.img@DH0")).is_none());

        // Nothing to strip to: a plain missing file, report it as such.
        assert!(stray_selector(&dir.path().join("absent.img@2")).is_none());
        // No suffix at all.
        assert!(stray_selector(&img).is_none());
        // `@0` is not a valid selector, so it is just part of a name.
        let zero = dir.path().join("disk.img@0");
        assert!(stray_selector(&zero).is_none());

        // A file whose name really ends in `@2`: opening it must not be
        // hijacked by the hint.
        let literal = dir.path().join("weird@2");
        std::fs::write(&literal, b"x").unwrap();
        assert!(
            stray_selector(&literal).is_none(),
            "`weird` does not exist, so `weird@2` is a filename"
        );
    }

    #[test]
    fn windows_style_path_without_partition() {
        // `:` and `\` mid-path don't trip the parser.
        let r = ImageRef::parse(r"C:\Users\foo\disk.hda").unwrap();
        assert_eq!(r.path, PathBuf::from(r"C:\Users\foo\disk.hda"));
        assert!(r.partition.is_none());
    }
}
