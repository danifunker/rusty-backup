//! Aggregated library-level filesystem end-to-end tests.
//!
//! Each `<core>.rs` beside this file used to be its own `tests/*_e2e.rs` (or small fs) test
//! binary; they are modules of the one `integration` binary now (see `../main.rs`).

mod apple_dos_e2e;
mod atarist_e2e;
mod cbm_fsck;
mod cbm_g64;
mod cbm_g71;
mod cpc_e2e;
mod cpm_e2e;
mod d88_e2e;
mod hfsplus_journal_e2e;
mod macplus_mfs_e2e;
mod os9_lib;
mod pcw_e2e;
mod unix_owner_perms;
