//! Aggregated rb-cli integration tests.
//!
//! Each `<core>.rs` beside this file used to be its own `tests/cli_*.rs` file,
//! i.e. its own test binary that statically re-linked the whole ~150 MB
//! `rusty-backup` rlib. Cargo auto-discovers `tests/<dir>/main.rs` as a single
//! test target, so gathering these as modules links the library once for the
//! whole group instead of ~18 times — a large compile-time and disk win on CI.
//! Each module keeps its own private `cli_bin` / `run` helpers; as separate
//! modules the identical names no longer collide.
//!
//! `tests/cli_flat.rs` and `tests/cli_help_snapshot.rs` stay as their own
//! binaries (the former is large; the latter has distinct snapshot deps).

mod cli_altair;
mod cli_archie;
mod cli_atari800;
mod cli_atarist;
mod cli_bbcmicro;
mod cli_bk0011m;
mod cli_cbm;
mod cli_coco;
mod cli_coco_os9;
mod cli_cp;
mod cli_cpm_floppy;
mod cli_dragon;
mod cli_hfs;
mod cli_literal_paths;
mod cli_macplus_appleii;
mod cli_ql;
mod cli_sgi_hdd;
mod cli_x68000;
