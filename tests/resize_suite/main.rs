//! Aggregated in-place filesystem resize round-trip tests.
//!
//! `ql_resize` (QDOS/QXL.WIN) and `x68000_resize` (Human68k SASI/SCSI) are both
//! `reconstruct_disk_from_backup` + `resize_filesystem_for` round-trips with
//! near-identical structure. Gathering them under one `tests/<dir>/main.rs`
//! target links the `rusty-backup` rlib once instead of twice.

mod ql_resize;
mod x68000_resize;
