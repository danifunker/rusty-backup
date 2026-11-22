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
//!   `Read + Seek (+ Write) + Send` reader, refusing one larger than its partition.
//! - [`build_header`]: a volume header from explicit partitions; [`disk_bytes_for_root`]:
//!   the disk size a host tree needs; [`SgiFit`]: round to cylinders up or within a target.
//!
//! Errors are `anyhow::Error`, as the builder has always returned.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod build;
pub mod open;

pub use build::{
    build_header, build_sgi_disk, build_volume_header, compute_sgi_layout, disk_bytes_for_root,
    write_sgi_disk, SgiFit, SgiHddLayout, SgiHddOptions, SgiHeaderSpec, SgiMedia,
    SgiPartitionLayout, SgiRootFs, SgiSlot, CDROM_HEADS, CDROM_SECTORS_PER_TRACK, DEFAULT_HEADS,
    DEFAULT_SECTORS_PER_TRACK, DEFAULT_SWAP_BYTES,
};
pub use open::{
    filesystem_partitions, open_kind, open_kind_editable, open_partition, open_partition_editable,
    partition, read_volume_header, SgiFsKind, SgiPartition,
};
