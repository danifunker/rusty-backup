//! `rb-cli fsck IMG[@N]` — check a filesystem.
//!
//! Three modes (mutually exclusive):
//! - default: scan + report, then prompt to repair if issues are found
//!   and the filesystem supports it. Non-TTY stdin downgrades to
//!   `--checkonly` semantics.
//! - `--checkonly`: scan + report only. Non-zero exit if issues found.
//! - `--repair`: scan + repair without prompting.
//!
//! Phase A scope: the existing HFS `validate_hfs_integrity` check, which
//! is read-only. Repair / prompt machinery lands when we generalize
//! through the `Filesystem::fsck()` and `EditableFilesystem::repair()`
//! traits — that's Phase B work because it touches every FS. The
//! `--repair` flag here is recognized and currently bails with a
//! "Phase B" message for HFS; the parser shape is fixed so scripts
//! that write `rb-cli fsck ... --checkonly` today won't break later.

use anyhow::{bail, Result};
use clap::Args;

use crate::cli::img_at::ImageRef;

#[derive(Debug, Args)]
pub struct FsckArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Scan only. Never prompt, never repair. Returns non-zero exit if
    /// issues were found. CI / cron friendly.
    #[arg(long, conflicts_with = "repair")]
    pub checkonly: bool,

    /// Auto-repair detected issues without prompting. Requires the
    /// filesystem to be repairable and the image to be writable.
    #[arg(long, conflicts_with = "checkonly")]
    pub repair: bool,

    /// Seconds to wait for an interactive repair confirmation before
    /// resolving to "No" (default 30). Use `0` to wait indefinitely
    /// (only meaningful on a TTY).
    #[arg(long = "prompt-timeout", default_value = "30")]
    pub prompt_timeout: u64,
}

pub fn run(args: FsckArgs) -> Result<()> {
    if args.repair {
        bail!(
            "--repair is not yet wired through the generic filesystem dispatch; \
             coming in Phase B. Use --checkonly today, or the per-FS `api hfs validate` form."
        );
    }
    // Default-mode prompt is also Phase B (depends on the shared fsck
    // dispatch and the repair trait). For now, default mode is checkonly
    // semantics with an explicit nudge in the report.
    let _ = args.prompt_timeout;
    crate::cli::api::hfs::cmd_validate(args.image.path, args.image.partition)
}
