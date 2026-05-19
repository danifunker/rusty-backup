//! `rb-cli shrink INPUT OUTPUT` — re-encode a disk image into a CHD that
//! drops trailing zero padding. Phase A: SGI/IRIX only (matches the
//! existing `api sgi shrink`); Phase D will widen to other layouts that
//! have a well-defined "used floor" (e.g. ProDOS).

use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Debug, Args)]
pub struct ShrinkArgs {
    /// Source image (raw `.img` or `.chd`). Must contain an SGI volume
    /// header at sector 0.
    pub input: PathBuf,

    /// Destination CHD path. Must end in `.chd`, must not already exist,
    /// and must not resolve to the same file as `input`.
    pub output: PathBuf,
}

pub fn run(args: ShrinkArgs) -> Result<()> {
    crate::cli::api::sgi::cmd_shrink(args.input, args.output)
}
