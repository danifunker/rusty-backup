//! `rb-cli serve` — run the network daemon (Family F, read-only, Phase 0).
//!
//! Lets a remote `rb-cli` browse and read files inside disk images this host
//! holds, via `rb://host:port/img@N` references. See
//! `docs/remote_transfer_plan.md`.

use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

use crate::remote::{serve, ServeConfig};

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Address to bind, `host:port`. Default binds all interfaces on the
    /// rusty-backup port (7341).
    #[arg(long, default_value = "0.0.0.0:7341")]
    pub bind: String,

    /// Root directory images are served from. Every `rb://` path a client
    /// opens is sandboxed under this directory.
    #[arg(long, default_value = ".")]
    pub root: PathBuf,
}

pub fn run(args: ServeArgs) -> Result<()> {
    serve(ServeConfig {
        bind: args.bind,
        root: args.root,
    })
}
