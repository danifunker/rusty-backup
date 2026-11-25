//! Aggregated rb-cli integration tests.
//!
//! Each `<core>.rs` beside this file used to be its own `tests/cli_*.rs` test binary; they are
//! modules of the one `integration` binary now (see `../main.rs`). Each keeps its own private
//! `cli_bin` / `run` helpers; as separate modules the identical names do not collide.

mod cli_altair;
mod cli_alto;
mod cli_archie;
mod cli_atari800;
mod cli_atarist;
mod cli_bbcmicro;
mod cli_bbcmicro_dsd;
mod cli_bk0011m;
mod cli_build;
mod cli_cbm;
mod cli_coco;
mod cli_coco_os9;
mod cli_container_rw;
mod cli_cp;
mod cli_cpm_floppy;
mod cli_cpm_ibm3740;
mod cli_dragon;
mod cli_export;
mod cli_hfs;
mod cli_line_endings;
mod cli_literal_paths;
mod cli_macplus_appleii;
mod cli_native_slots;
mod cli_next_cdrom;
#[cfg(feature = "optical")]
mod cli_optical_extract;
mod cli_provision;
mod cli_put_case;
mod cli_ql;
mod cli_restore_apm_driver;
mod cli_restore_hfs;
mod cli_sgi_hdd;
mod cli_show_partmap;
mod cli_x68000;
mod cli_xfs_log;
