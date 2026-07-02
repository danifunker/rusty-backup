//! Aggregated library-level filesystem end-to-end tests.
//!
//! Each `<core>.rs` beside this file used to be its own `tests/*_e2e.rs` (or
//! small fs) binary that re-linked the whole `rusty-backup` rlib. Cargo
//! auto-discovers `tests/<dir>/main.rs` as one test target, so gathering these
//! as modules links the library once instead of ~11 times.
//!
//! `remote_optical.rs` stays its own binary (it carries a crate-level
//! `#![cfg(all(feature = "remote", feature = "optical"))]` gate).

mod apple_dos_e2e;
mod atarist_e2e;
mod cbm_g64;
mod cbm_g71;
mod cpc_e2e;
mod cpm_e2e;
mod d88_e2e;
mod hfsplus_journal_e2e;
mod macplus_mfs_e2e;
mod os9_lib;
mod pcw_e2e;
