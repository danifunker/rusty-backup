//! `rb-cli chmod IMG[@N] PATH MODE` and `rb-cli chown IMG[@N] PATH OWNER` —
//! change POSIX permissions / ownership on an entry inside an image.
//!
//! The scriptable half of the GUI's file-metadata editor, which stages the
//! same two `EditableFilesystem` hooks. Supported wherever the filesystem
//! stores POSIX metadata: ext, EFS, UFS, Minix, JFS, XFS, SquashFS, and
//! HFS+ (where the bits live in the catalog record's `HFSPlusBSDInfo` —
//! that is how OS X does Unix permissions).

use anyhow::{anyhow, bail, Result};
use clap::Args;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::resolve::resolve_partition_rw;
use crate::fs::unix_common::inode::unix_mode_string;

#[derive(Debug, Args)]
pub struct ChmodArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,
    /// Absolute path of the entry inside the filesystem.
    pub path: String,
    /// New permission bits in octal, with or without a leading `0`
    /// (`755`, `0644`, `4755` for setuid). The file-type bits are kept.
    pub mode: String,
}

#[derive(Debug, Args)]
pub struct ChownArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,
    /// Absolute path of the entry inside the filesystem.
    pub path: String,
    /// New owner as `UID`, `UID:GID`, or `:GID` to change the group alone.
    pub owner: String,
}

/// Parse `755` / `0755` / `04755` as octal permission bits.
fn parse_mode(text: &str) -> Result<u32> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        bail!("chmod: MODE is empty; pass octal permission bits such as 755");
    }
    // Symbolic modes (`u+x`) are deliberately unsupported: partial, easy to
    // get subtly wrong, and the octal form is unambiguous in a script.
    if !trimmed.bytes().all(|b| b.is_ascii_digit()) {
        bail!("chmod: MODE must be octal digits (e.g. 755); symbolic modes like 'u+x' are not supported");
    }
    let mode = u32::from_str_radix(trimmed, 8)
        .map_err(|_| anyhow!("chmod: '{trimmed}' is not a valid octal mode"))?;
    if mode > 0o7777 {
        bail!("chmod: mode {trimmed} is past the 4-digit maximum 7777");
    }
    Ok(mode)
}

/// Parse `UID`, `UID:GID` or `:GID`, resolved against the entry's current
/// ids so an unspecified half is left alone.
fn parse_owner(text: &str, cur_uid: u32, cur_gid: u32) -> Result<(u32, u32)> {
    let trimmed = text.trim();
    let (uid_part, gid_part) = match trimmed.split_once(':') {
        Some((u, g)) => (u.trim(), Some(g.trim())),
        None => (trimmed, None),
    };
    let uid = if uid_part.is_empty() {
        cur_uid
    } else {
        uid_part
            .parse::<u32>()
            .map_err(|_| anyhow!("chown: '{uid_part}' is not a numeric uid"))?
    };
    let gid = match gid_part {
        None | Some("") => cur_gid,
        Some(g) => g
            .parse::<u32>()
            .map_err(|_| anyhow!("chown: '{g}' is not a numeric gid"))?,
    };
    if uid_part.is_empty() && gid_part.is_none() {
        bail!("chown: OWNER is empty; pass UID, UID:GID or :GID");
    }
    Ok((uid, gid))
}

pub fn run_chmod(args: ChmodArgs) -> Result<()> {
    let mode = parse_mode(&args.mode)?;
    let (file, ctx, commit) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = ctx
        .open_editable(file)
        .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    let entry = super::ls::resolve_path(fs.as_filesystem_mut(), &args.path)?;
    fs.set_permissions(&entry, mode)
        .map_err(|e| anyhow!("set_permissions: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;

    // Show the resulting rwx string, not just the octal the user typed —
    // the type bits are the filesystem's, not theirs.
    let type_bits = entry.mode.unwrap_or(if entry.is_directory() {
        0o040_000
    } else {
        0o100_000
    }) & 0o170_000;
    println!(
        "{}: mode {:04o} ({})",
        args.path,
        mode,
        unix_mode_string(type_bits | mode)
    );
    Ok(())
}

pub fn run_chown(args: ChownArgs) -> Result<()> {
    let (file, ctx, commit) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = ctx
        .open_editable(file)
        .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    let entry = super::ls::resolve_path(fs.as_filesystem_mut(), &args.path)?;
    let (uid, gid) = parse_owner(&args.owner, entry.uid.unwrap_or(0), entry.gid.unwrap_or(0))?;
    fs.set_owner(&entry, uid, gid)
        .map_err(|e| anyhow!("set_owner: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;

    println!("{}: owner {uid}:{gid}", args.path);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_octal_modes() {
        assert_eq!(parse_mode("755").unwrap(), 0o755);
        assert_eq!(parse_mode("0644").unwrap(), 0o644);
        assert_eq!(parse_mode("4755").unwrap(), 0o4755);
        assert_eq!(parse_mode(" 600 ").unwrap(), 0o600);
    }

    #[test]
    fn rejects_non_octal_and_oversized_modes() {
        // A decimal-looking 8 or 9 is not octal — catching it matters
        // because `chmod 779` would otherwise be a silent misparse.
        assert!(parse_mode("779").is_err());
        assert!(parse_mode("u+x").is_err());
        assert!(parse_mode("").is_err());
        assert!(parse_mode("77777").is_err());
    }

    #[test]
    fn owner_forms_resolve_against_current_ids() {
        assert_eq!(parse_owner("1000:100", 1, 2).unwrap(), (1000, 100));
        // uid only -> keep the group.
        assert_eq!(parse_owner("1000", 1, 2).unwrap(), (1000, 2));
        // `:gid` -> keep the user.
        assert_eq!(parse_owner(":100", 1, 2).unwrap(), (1, 100));
        // `uid:` -> keep the group.
        assert_eq!(parse_owner("1000:", 1, 2).unwrap(), (1000, 2));
        assert!(parse_owner("", 1, 2).is_err());
        assert!(parse_owner("root", 1, 2).is_err());
    }
}
