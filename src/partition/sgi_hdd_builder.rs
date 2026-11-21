//! SGI / IRIX disk builder; moved to the MIT `sgi-disk` crate, re-exported here for the app.

use std::io::{Seek, Write};

use anyhow::Result;
pub use sgi_disk::build::*;

/// The name rb-cli and the provisioning runner have always called; same as [`write_sgi_disk`].
pub fn write_sgi_efs_hdd<W: Write + Seek>(
    sink: &mut W,
    opts: &SgiHddOptions,
) -> Result<SgiHddLayout> {
    write_sgi_disk(sink, opts)
}

/// The name the app's tests have always called; same as [`build_sgi_disk`].
pub fn build_sgi_efs_hdd(opts: &SgiHddOptions) -> Result<(Vec<u8>, SgiHddLayout)> {
    build_sgi_disk(opts)
}
