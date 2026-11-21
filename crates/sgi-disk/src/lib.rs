//! SGI / IRIX disks: build an image with a volume header and an EFS, XFS or empty
//! root partition ([`build`]), and open a partition of an existing disk as a
//! `disk-core` filesystem ([`open`]).
//!
//! - [`write_sgi_disk`] / [`build_sgi_disk`]: stream or build a labelled disk; the
//!   options choose EFS or XFS (feature `xfs`), label only, and a data-only or
//!   root-plus-swap layout. Geometry defaults to 16 heads × 63 sectors, which is
//!   what the IRIS emulator's SCSI disk reports.
//! - [`read_volume_header`], [`filesystem_partitions`], [`open_partition`] /
//!   [`open_partition_editable`]: find and open EFS or XFS partitions over any
//!   `Read + Seek (+ Write) + Send` reader.
//!
//! Errors are `anyhow::Error`, as the builder has always returned.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod build;
pub mod open;

pub use build::{
    build_sgi_disk, build_sgi_efs_hdd, build_volume_header, compute_sgi_layout, write_sgi_disk,
    write_sgi_efs_hdd, SgiHddLayout, SgiHddOptions, SgiMedia, SgiPartitionLayout, SgiRootFs,
    CDROM_HEADS, CDROM_SECTORS_PER_TRACK, DEFAULT_HEADS, DEFAULT_SECTORS_PER_TRACK,
    DEFAULT_SWAP_BYTES,
};
pub use open::{
    filesystem_partitions, open_kind, open_kind_editable, open_partition, open_partition_editable,
    partition, read_volume_header, SgiFsKind, SgiPartition,
};
