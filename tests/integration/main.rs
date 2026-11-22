//! Every app integration test, as modules of one test binary.
//!
//! Each file or folder here used to be its own `tests/*.rs` or `tests/<dir>/main.rs` target,
//! and every target statically re-linked the whole `rusty-backup` crate: some 20 release links
//! per CI job, most of the Windows test step. One binary links it once. Run one group with
//! `cargo test --test integration <module>::`. `ui_scenarios` and `doc_parity` stay their own
//! targets (CI's Rust 1.73 job and `scripts/preflight.sh` run them by name).

mod cli_flat;
mod cli_help_snapshot;
mod cli_optical_du;
mod cli_suite;
mod cli_tree;
mod commander_archive;
mod cp_engine;
mod daemon_service;
mod export_selection_paths;
mod filesystem_e2e;
mod floppy_container_roundtrip;
mod fork_export;
mod fs_e2e_suite;
mod remote_filesystem;
mod remote_optical;
mod resize_suite;
mod sgi_efs_v1_e2e;
mod short_source_warning;
mod split_backup_roundtrip;
mod superfloppy_compression;
mod timestamp_preservation;
mod wave2_dispatch_e2e;
