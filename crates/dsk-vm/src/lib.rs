//! Virtual-machine disk images: [`vhd`] (Microsoft VHD), [`vmdk`] / [`vmdk_sparse`] (VMware)
//! and [`qcow2`] (QEMU). [`sparse`] is the allocate-on-demand bookkeeping the sparse writers share.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod qcow2;
pub mod sparse;
pub mod vhd;
pub mod vmdk;
pub mod vmdk_sparse;

#[cfg(test)]
mod qemu_img_test;

use std::io::Write;

use anyhow::{Context, Result};

/// Streaming I/O buffer size.
pub(crate) const CHUNK_SIZE: usize = 256 * 1024;

/// Whether every byte of `data` is zero.
pub(crate) fn is_all_zeros(data: &[u8]) -> bool {
    data.iter().all(|&b| b == 0)
}

/// Write `count` zero bytes to `writer`.
pub(crate) fn write_zeros(writer: &mut impl Write, count: u64) -> Result<()> {
    let zeros = vec![0u8; CHUNK_SIZE];
    let mut remaining = count;
    while remaining > 0 {
        let n = (remaining as usize).min(CHUNK_SIZE);
        writer
            .write_all(&zeros[..n])
            .context("failed to write zeros")?;
        remaining -= n as u64;
    }
    Ok(())
}
