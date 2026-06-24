//! `rb-cli cbk pack|unpack` — convert a native backup folder to/from the
//! single-file `.cbk` chunked container (the on-disk artifact specced in
//! `docs/cb_dos_network_and_state.md` §2). `rb-cli restore` reads a `.cbk`
//! directly (it materializes a temp folder first), so these verbs are the
//! producer/inspector side: pack a finished backup into one portable file, or
//! unpack one back to the folder the rest of the toolchain expects.

use anyhow::Result;
use clap::{Args, Subcommand};
use std::path::PathBuf;

use crate::rbformats::cbk;

#[derive(Debug, Args)]
pub struct CbkArgs {
    #[command(subcommand)]
    pub cmd: CbkCmd,
}

#[derive(Debug, Subcommand)]
pub enum CbkCmd {
    /// Pack a native backup folder into a single `.cbk` container.
    Pack {
        /// The backup folder (the directory containing `metadata.json`).
        folder: PathBuf,
        /// Output `.cbk` file.
        out: PathBuf,
    },
    /// Unpack a `.cbk` container back into a native backup folder.
    Unpack {
        /// Input `.cbk` file.
        container: PathBuf,
        /// Output folder (created if absent).
        folder: PathBuf,
    },
}

pub fn run(args: CbkArgs) -> Result<()> {
    match args.cmd {
        CbkCmd::Pack { folder, out } => {
            cbk::pack_folder_to_cbk(&folder, &out)?;
            println!(
                "rb-cli cbk: packed {} -> {}",
                folder.display(),
                out.display()
            );
            Ok(())
        }
        CbkCmd::Unpack { container, folder } => {
            cbk::materialize_cbk_to_folder(&container, &folder)?;
            println!(
                "rb-cli cbk: unpacked {} -> {}",
                container.display(),
                folder.display()
            );
            Ok(())
        }
    }
}
