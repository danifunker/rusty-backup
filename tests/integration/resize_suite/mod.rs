//! Aggregated in-place filesystem resize round-trip tests.
//!
//! `ql_resize` (QDOS/QXL.WIN) and `x68000_resize` (Human68k SASI/SCSI) are both
//! `reconstruct_disk_from_backup` + `resize_filesystem_for` round-trips with
//! near-identical structure; both are modules of the `integration` test binary (`../main.rs`).

mod efs_gui_resize;
mod ql_resize;
mod x68000_resize;
