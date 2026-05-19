//! Flat verbs — the stable top-level CLI surface.
//!
//! Each verb is a thin handler that parses its clap struct, validates
//! inputs, and dispatches into the engine layer (`src/fs/`, `src/backup/`,
//! `src/restore/`, `src/partition/`, `src/rbformats/`). The handler never
//! touches threads or status structs directly; long-running work goes
//! through `src/model/*_runner.rs` runners shared with the GUI.

pub mod backup;
pub mod batch;
pub mod batch_template;
pub mod completions;
pub mod config;
pub mod convert;
pub mod expand;
pub mod fsck;
pub mod get;
pub mod inspect;
pub mod ls;
pub mod mkdir;
pub mod new;
pub mod optical;
pub mod put;
pub mod resize;
pub mod restore;
pub mod rm;
pub mod show;
pub mod shrink;
pub mod terminal;
pub mod write;
