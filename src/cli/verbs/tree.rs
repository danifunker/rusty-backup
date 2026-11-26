//! `rb-cli tree IMG[@N]` — a GNU `tree`-style listing of every volume on a disk, or of one
//! partition: the browser's Tree, for the whole disk at once. The walk is `model::disk_tree`.

use anyhow::{bail, Result};
use clap::Args;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::resolve::FsDispatchOverride;
use crate::model::disk_tree::{trees, TreeOptions};

#[derive(Debug, Args)]
pub struct TreeArgs {
    /// The disk: `path` lists every partition (or a disc's every filesystem) under a heading,
    /// `path@N` just that one. Disk images, containers, backup folders, discs and
    /// `rb://host:port/img` work.
    pub image: ImageRef,

    /// Show each entry's filesystem ID too (CNID on HFS / HFS+, inode on ext, cluster on FAT).
    #[arg(long)]
    pub ids: bool,

    /// Passphrase for an encrypted volume or container (APFS, BitLocker, IMZ).
    #[arg(long)]
    pub password: Option<String>,

    /// The disk image to open inside a `.zip` that holds several.
    #[arg(long = "inside", value_name = "NAME")]
    pub inside: Option<String>,

    #[command(flatten)]
    pub fs_override: FsDispatchOverride,
}

pub fn run(args: TreeArgs) -> Result<()> {
    crate::fs::carve::set_full_scan(args.fs_override.carve_full);
    let opts = TreeOptions {
        ids: args.ids,
        password: args.password.as_deref(),
        fs_type: args.fs_override.fs_type.as_deref(),
        inside: args.inside.as_deref(),
    };
    let found = trees(&args.image.path, args.image.partition.clone(), &opts)?;
    // One volume: the listing alone on stdout, its label on stderr (as `ls` does).
    if let [only] = found.as_slice() {
        log_stderr(&only.label);
        return match &only.tree {
            Ok(text) => {
                out_stdout(text.trim_end());
                Ok(())
            }
            Err(e) => bail!("{e}"),
        };
    }
    let mut listed = 0;
    for (n, v) in found.iter().enumerate() {
        if n > 0 {
            out_stdout("");
        }
        out_stdout(format!("== {} ==", v.label));
        match &v.tree {
            Ok(text) => {
                listed += 1;
                out_stdout(text.trim_end());
            }
            Err(e) => out_stdout(format!("({e})")),
        }
    }
    if listed == 0 {
        bail!("no volume on this disk could be listed");
    }
    Ok(())
}
