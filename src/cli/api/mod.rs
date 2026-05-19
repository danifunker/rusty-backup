//! Legacy `api` namespace. Kept as a stable but deprecated grammar
//! through the introduction of the flat verbs (Phase A of
//! `docs/cli-todo.md`). Drop in a future release.
//!
//! Inside `api`, grammar is allowed to churn — the flat verbs at the
//! crate-root level are the stable surface. Scripts that already call
//! `api hfs put …` keep working; new scripts should target the flat
//! verbs.

use anyhow::Result;
use clap::Subcommand;

pub mod apm;
pub mod hfs;
pub mod sgi;

#[derive(Subcommand, Debug)]
pub enum ApiGroup {
    /// Classic-HFS image operations (create, browse, edit single-partition .dsk images).
    Hfs {
        #[command(subcommand)]
        verb: hfs::HfsCommand,
    },
    /// SGI/IRIX disk operations.
    Sgi {
        #[command(subcommand)]
        verb: sgi::SgiCommand,
    },
    /// Apple Partition Map (APM) disk operations.
    Apm {
        #[command(subcommand)]
        verb: apm::ApmCommand,
    },
}

pub fn run(group: ApiGroup) -> Result<()> {
    match group {
        ApiGroup::Hfs { verb } => hfs::run(verb),
        ApiGroup::Sgi { verb } => sgi::run(verb),
        ApiGroup::Apm { verb } => apm::run(verb),
    }
}
