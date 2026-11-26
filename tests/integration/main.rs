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
mod vhd_export_from_backup;
mod wave2_dispatch_e2e;

/// Decode a zstd fixture into `out`, seeking over all-zero runs so the file stays sparse.
pub(crate) fn decode_sparse(src: std::fs::File, out: &mut std::fs::File) {
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut dec = zstd::stream::read::Decoder::new(src).unwrap();
    let mut buf = vec![0u8; 1 << 16];
    let mut len = 0u64;
    loop {
        let mut n = 0;
        while n < buf.len() {
            match dec.read(&mut buf[n..]).unwrap() {
                0 => break,
                k => n += k,
            }
        }
        if n == 0 {
            break;
        }
        if buf[..n].iter().all(|&b| b == 0) {
            out.seek(SeekFrom::Current(n as i64)).unwrap();
        } else {
            out.write_all(&buf[..n]).unwrap();
        }
        len += n as u64;
    }
    out.set_len(len).unwrap();
}
