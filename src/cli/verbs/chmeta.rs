//! `rb-cli chmeta IMG@N PATH [--type TYPE] [--creator CREATOR]` — change
//! the type and/or creator code on an existing file. HFS / HFS+ /
//! ProDOS (`--type` only).

use anyhow::{anyhow, bail, Result};
use clap::Args;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::resolve::resolve_partition_rw;

#[derive(Debug, Args)]
pub struct ChmetaArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,
    /// Absolute Mac path of the file to update.
    pub path: String,
    /// New 4-character type code.
    #[arg(long = "type")]
    pub type_code: Option<String>,
    /// New 4-character creator code (HFS / HFS+ only).
    #[arg(long)]
    pub creator: Option<String>,

    /// DOS attribute bits (FAT / exFAT). Comma-separated flags, each optionally
    /// prefixed `+` to set or `-` to clear: `readonly`, `hidden`, `system`,
    /// `archive`. Without a prefix the listed set becomes the whole set, so
    /// `--attrs readonly,hidden` clears anything else.
    ///
    /// `--attrs +readonly` marks a file read-only and leaves the rest alone;
    /// `--attrs -hidden` just unhides it.
    ///
    /// `allow_hyphen_values` so a leading `-` reads as "clear this" rather than
    /// as the start of another flag.
    #[arg(long, allow_hyphen_values = true)]
    pub attrs: Option<String>,
}

/// Parse the `--attrs` grammar into `(set_mask, clear_mask, absolute)`.
///
/// `absolute` means no entry carried a `+`/`-`, so the list is the complete
/// desired set rather than a delta. Mixing the two forms is rejected: it reads
/// as though it should work and there is no sensible answer for what
/// `readonly,-hidden` means.
fn parse_attrs(spec: &str) -> Result<(u16, u16, bool)> {
    let mut set = 0u16;
    let mut clear = 0u16;
    let mut saw_delta = false;
    let mut saw_absolute = false;
    for raw in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (op, name) = match raw.as_bytes()[0] {
            b'+' => (Some(true), &raw[1..]),
            b'-' => (Some(false), &raw[1..]),
            _ => (None, raw),
        };
        let bit = match name.to_ascii_lowercase().as_str() {
            "readonly" | "ro" | "r" => 0x01u16,
            "hidden" | "h" => 0x02,
            "system" | "sys" | "s" => 0x04,
            "archive" | "arch" | "a" => 0x20,
            other => {
                bail!("unknown attribute {other:?}; expected readonly, hidden, system or archive")
            }
        };
        match op {
            Some(true) => {
                saw_delta = true;
                set |= bit;
            }
            Some(false) => {
                saw_delta = true;
                clear |= bit;
            }
            None => {
                saw_absolute = true;
                set |= bit;
            }
        }
    }
    if saw_delta && saw_absolute {
        bail!("--attrs: mix of absolute and +/- forms; use one or the other");
    }
    Ok((set, clear, saw_absolute))
}

pub fn run(args: ChmetaArgs) -> Result<()> {
    if args.type_code.is_none() && args.creator.is_none() && args.attrs.is_none() {
        bail!("chmeta: pass at least one of --type, --creator or --attrs");
    }
    let (file, ctx, commit) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = ctx
        .open_editable(file)
        .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    let entry = super::ls::resolve_path(fs.as_filesystem_mut(), &args.path)?;

    if let Some(spec) = args.attrs.as_deref() {
        let (set, clear, absolute) = parse_attrs(spec)?;
        let current = entry.dos_attributes.unwrap_or(0);
        let next = if absolute {
            set
        } else {
            (current | set) & !clear
        };
        let (parent, _) = super::ls::resolve_parent(fs.as_filesystem_mut(), &args.path)?;
        fs.set_dos_attributes(&parent, &entry, next)
            .map_err(|e| anyhow!("set_dos_attributes: {e}"))?;
        log_stderr(format!(
            "{}: attributes {} -> {}",
            args.path,
            describe_attrs(current),
            describe_attrs(next),
        ));
    }
    if args.type_code.is_none() && args.creator.is_none() {
        fs.sync_metadata()
            .map_err(|e| anyhow!("sync_metadata: {e}"))?;
        drop(fs);
        commit.commit()?;
        return Ok(());
    }
    // Default the un-overridden half to the file's current code (display form;
    // chmeta's `set_type_creator` API is text-based).
    let entry_type = entry.type_code_display();
    let entry_creator = entry.creator_code_display();
    let new_type = args
        .type_code
        .as_deref()
        .or(entry_type.as_deref())
        .unwrap_or("BINA");
    let new_creator = args
        .creator
        .as_deref()
        .or(entry_creator.as_deref())
        .unwrap_or("????");
    fs.set_type_creator(&entry, new_type, new_creator)
        .map_err(|e| anyhow!("set_type_creator: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    log_stderr(format!(
        "{}: type={new_type} creator={new_creator}",
        args.path
    ));
    Ok(())
}

/// Render the attribute bits the way the flags spell them.
fn describe_attrs(a: u16) -> String {
    let mut parts = Vec::new();
    for (bit, name) in [
        (0x01u16, "readonly"),
        (0x02, "hidden"),
        (0x04, "system"),
        (0x20, "archive"),
    ] {
        if a & bit != 0 {
            parts.push(name);
        }
    }
    if parts.is_empty() {
        "none".to_string()
    } else {
        parts.join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attrs_grammar_covers_absolute_and_delta_forms() {
        // Absolute: the list is the whole set.
        let (set, clear, absolute) = parse_attrs("readonly,hidden").unwrap();
        assert_eq!((set, clear, absolute), (0x03, 0x00, true));

        // Delta: only what is named changes.
        let (set, clear, absolute) = parse_attrs("+readonly,-hidden").unwrap();
        assert_eq!((set, clear, absolute), (0x01, 0x02, false));

        // Short forms, since these get typed a lot.
        assert_eq!(parse_attrs("r,h,s,a").unwrap().0, 0x27);

        // Mixing the forms has no sensible meaning, so it is refused rather
        // than guessed at.
        assert!(parse_attrs("readonly,-hidden").is_err());
        assert!(parse_attrs("bogus").is_err());
    }
}
