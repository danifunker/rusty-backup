//! Open the filesystem in an SGI partition: read the volume header with
//! [`part_tables::sgi`], pick EFS or XFS by the partition's type, and hand back a
//! `disk-core` [`Filesystem`] or [`EditableFilesystem`].
//!
//! Everything works over the caller's reader (`R: Read + Seek + Send`, plus
//! `Write` to edit), so a block device, a file or an in-memory image all do. The
//! partition's `first` / `blocks` count 512-byte sectors.
//!
//! The rule that picks the filesystem lives in [`SgiFsKind::of`]: EFS for type 7
//! and for SYSV (5), which IRIX uses for EFS CD-ROMs; XFS for type 10. Every other
//! type (volume header, whole-volume, swap / raw, XFS log, XLV, XVM, ...) holds no
//! filesystem this crate opens.

use std::io::{Read, Seek, SeekFrom, Write};

use anyhow::{anyhow, bail, Context, Result};
use disk_core::filesystem::{EditableFilesystem, Filesystem};
use fs_unix::efs::EfsFilesystem;
use part_tables::sgi::{SgiPartitionType, SgiVolumeHeader, SGI_VOLHDR_MAGIC};

const SECTOR_SIZE: u64 = 512;

/// The filesystem an SGI partition type carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SgiFsKind {
    Efs,
    Xfs,
}

impl SgiFsKind {
    /// EFS for type 7 and SYSV (EFS CD-ROMs), XFS for type 10, `None` otherwise.
    pub fn of(ptype: SgiPartitionType) -> Option<Self> {
        if ptype.is_efs() {
            Some(SgiFsKind::Efs)
        } else if ptype == SgiPartitionType::Xfs {
            Some(SgiFsKind::Xfs)
        } else {
            None
        }
    }
}

/// One partition that holds a filesystem this crate can open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SgiPartition {
    /// Slot in the volume header's partition table (0..16).
    pub index: usize,
    pub kind: SgiFsKind,
    /// Byte offset of the partition from the start of the disk.
    pub offset: u64,
    /// Partition length in bytes.
    pub len: u64,
}

/// Read and validate the 512-byte volume header at sector 0.
pub fn read_volume_header<R: Read + Seek>(reader: &mut R) -> Result<SgiVolumeHeader> {
    reader.seek(SeekFrom::Start(0))?;
    let mut sector = [0u8; SECTOR_SIZE as usize];
    reader.read_exact(&mut sector).context("reading sector 0")?;
    if u32::from_be_bytes([sector[0], sector[1], sector[2], sector[3]]) != SGI_VOLHDR_MAGIC {
        let mut swapped = sector;
        disk_core::sgi::swab16_in_place(&mut swapped);
        if u32::from_be_bytes([swapped[0], swapped[1], swapped[2], swapped[3]]) == SGI_VOLHDR_MAGIC
        {
            bail!(
                "byte-swapped SGI image: swap every 16-bit word (disk_core::sgi) before opening it"
            );
        }
        bail!("no SGI volume header at sector 0");
    }
    Ok(SgiVolumeHeader::parse(&sector)?)
}

/// The partitions in `header` that hold EFS or XFS, in slot order.
pub fn filesystem_partitions(header: &SgiVolumeHeader) -> Vec<SgiPartition> {
    header
        .partitions
        .iter()
        .enumerate()
        .filter(|(_, p)| !p.is_empty())
        .filter_map(|(index, p)| {
            SgiFsKind::of(p.partition_type()).map(|kind| SgiPartition {
                index,
                kind,
                offset: p.first as u64 * SECTOR_SIZE,
                len: p.blocks as u64 * SECTOR_SIZE,
            })
        })
        .collect()
}

/// The filesystem partition in slot `index`, or an error naming what is there instead.
pub fn partition(header: &SgiVolumeHeader, index: usize) -> Result<SgiPartition> {
    let entry = header
        .partitions
        .get(index)
        .ok_or_else(|| anyhow!("no partition slot {index} (an SGI label has 16)"))?;
    if entry.is_empty() {
        bail!("partition slot {index} is empty");
    }
    filesystem_partitions(header)
        .into_iter()
        .find(|p| p.index == index)
        .ok_or_else(|| {
            anyhow!(
                "partition {index} is {}, which holds no EFS or XFS filesystem",
                entry.partition_type().display_name()
            )
        })
}

/// Open the filesystem at `offset` read-only (EFS CD-ROMs, write-protected images).
pub fn open_kind<'a, R: Read + Seek + Send + 'a>(
    kind: SgiFsKind,
    reader: R,
    offset: u64,
) -> Result<Box<dyn Filesystem + 'a>> {
    match kind {
        SgiFsKind::Efs => Ok(Box::new(EfsFilesystem::open(reader, offset)?)),
        SgiFsKind::Xfs => open_xfs(reader, offset),
    }
}

/// Open the filesystem at `offset` for editing.
pub fn open_kind_editable<'a, R: Read + Write + Seek + Send + 'a>(
    kind: SgiFsKind,
    reader: R,
    offset: u64,
) -> Result<Box<dyn EditableFilesystem + 'a>> {
    match kind {
        SgiFsKind::Efs => Ok(Box::new(EfsFilesystem::open(reader, offset)?)),
        SgiFsKind::Xfs => open_xfs_editable(reader, offset),
    }
}

/// Read the volume header from `reader` and open partition `index` read-only.
pub fn open_partition<'a, R: Read + Seek + Send + 'a>(
    mut reader: R,
    index: usize,
) -> Result<Box<dyn Filesystem + 'a>> {
    let p = partition(&read_volume_header(&mut reader)?, index)?;
    open_kind(p.kind, reader, p.offset)
}

/// Read the volume header from `reader` and open partition `index` for editing.
pub fn open_partition_editable<'a, R: Read + Write + Seek + Send + 'a>(
    mut reader: R,
    index: usize,
) -> Result<Box<dyn EditableFilesystem + 'a>> {
    let p = partition(&read_volume_header(&mut reader)?, index)?;
    open_kind_editable(p.kind, reader, p.offset)
}

#[cfg(feature = "xfs")]
fn open_xfs<'a, R: Read + Seek + Send + 'a>(
    reader: R,
    offset: u64,
) -> Result<Box<dyn Filesystem + 'a>> {
    Ok(Box::new(fs_xfs::xfs::XfsFilesystem::open(reader, offset)?))
}

#[cfg(feature = "xfs")]
fn open_xfs_editable<'a, R: Read + Write + Seek + Send + 'a>(
    reader: R,
    offset: u64,
) -> Result<Box<dyn EditableFilesystem + 'a>> {
    Ok(Box::new(fs_xfs::xfs::XfsFilesystem::open(reader, offset)?))
}

#[cfg(not(feature = "xfs"))]
fn open_xfs<'a, R: Read + Seek + Send + 'a>(_: R, _: u64) -> Result<Box<dyn Filesystem + 'a>> {
    bail!("this build of sgi-disk has no XFS support (feature `xfs`)")
}

#[cfg(not(feature = "xfs"))]
fn open_xfs_editable<'a, R: Read + Write + Seek + Send + 'a>(
    _: R,
    _: u64,
) -> Result<Box<dyn EditableFilesystem + 'a>> {
    bail!("this build of sgi-disk has no XFS support (feature `xfs`)")
}
