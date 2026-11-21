//! Open the filesystem in an SGI partition: read the volume header with
//! [`part_tables::sgi`], pick EFS or XFS by the partition's type, and hand back a
//! `disk-core` [`Filesystem`] or [`EditableFilesystem`].
//!
//! Everything works over the caller's reader (`R: Read + Seek + Send`, plus
//! `Write` to edit), so a block device, a file or an in-memory image all do. The
//! partition's `first` / `blocks` count 512-byte sectors.
//!
//! Every opener takes the partition's length and refuses a filesystem that
//! declares more (EFS: `fs_size` and the replica superblock; XFS: `dblocks`
//! blocks), read-only too: a corrupt label or a label-only relabel over an old,
//! larger volume would otherwise be read, or edited, past the partition end.
//!
//! The rule that picks the filesystem lives in [`SgiFsKind::of`]: EFS for type 7
//! and for SYSV (5), which IRIX uses for EFS CD-ROMs; XFS for type 10. Every other
//! type (volume header, whole-volume, swap / raw, XFS log, XLV, XVM, ...) holds no
//! filesystem this crate opens.

use std::io::{Read, Seek, SeekFrom, Write};

use anyhow::{anyhow, bail, Context, Result};
use disk_core::filesystem::{EditableFilesystem, Filesystem};
use fs_unix::efs::{EfsFilesystem, EfsSuperblock};
use part_tables::sgi::{SgiPartitionType, SgiVolumeHeader, SGI_VOLHDR_MAGIC};

const SECTOR_SIZE: u64 = 512;

/// The filesystem an SGI partition type carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SgiFsKind {
    Efs,
    Xfs,
}

impl SgiFsKind {
    /// "EFS" or "XFS".
    pub fn name(self) -> &'static str {
        match self {
            SgiFsKind::Efs => "EFS",
            SgiFsKind::Xfs => "XFS",
        }
    }

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

/// Open the filesystem at `offset` read-only (EFS CD-ROMs, write-protected images); `len` is
/// the partition's length in bytes, and a filesystem that declares more is refused.
pub fn open_kind<'a, R: Read + Seek + Send + 'a>(
    kind: SgiFsKind,
    mut reader: R,
    offset: u64,
    len: u64,
) -> Result<Box<dyn Filesystem + 'a>> {
    let floor = efs_replica_end(kind, &mut reader, offset)?;
    let fs: Box<dyn Filesystem + 'a> = match kind {
        SgiFsKind::Efs => Box::new(EfsFilesystem::open(reader, offset)?),
        SgiFsKind::Xfs => open_xfs(reader, offset)?,
    };
    ensure_fits(kind, fs.total_size().max(floor), len)?;
    Ok(fs)
}

/// Open the filesystem at `offset` for editing; `len` bounds it as in [`open_kind`].
pub fn open_kind_editable<'a, R: Read + Write + Seek + Send + 'a>(
    kind: SgiFsKind,
    mut reader: R,
    offset: u64,
    len: u64,
) -> Result<Box<dyn EditableFilesystem + 'a>> {
    let floor = efs_replica_end(kind, &mut reader, offset)?;
    let fs: Box<dyn EditableFilesystem + 'a> = match kind {
        SgiFsKind::Efs => Box::new(EfsFilesystem::open(reader, offset)?),
        SgiFsKind::Xfs => open_xfs_editable(reader, offset)?,
    };
    ensure_fits(kind, fs.total_size().max(floor), len)?;
    Ok(fs)
}

/// Read the volume header from `reader` and open partition `index` read-only.
pub fn open_partition<'a, R: Read + Seek + Send + 'a>(
    mut reader: R,
    index: usize,
) -> Result<Box<dyn Filesystem + 'a>> {
    let p = partition(&read_volume_header(&mut reader)?, index)?;
    open_kind(p.kind, reader, p.offset, p.len)
}

/// Read the volume header from `reader` and open partition `index` for editing.
pub fn open_partition_editable<'a, R: Read + Write + Seek + Send + 'a>(
    mut reader: R,
    index: usize,
) -> Result<Box<dyn EditableFilesystem + 'a>> {
    let p = partition(&read_volume_header(&mut reader)?, index)?;
    open_kind_editable(p.kind, reader, p.offset, p.len)
}

/// Refuse a filesystem larger than its partition: an edit would write past the partition end.
fn ensure_fits(kind: SgiFsKind, declared: u64, len: u64) -> Result<()> {
    if declared > len {
        bail!(
            "the {} filesystem declares {declared} bytes but its partition holds {len}: the label \
             and the filesystem disagree, so it is not opened",
            kind.name()
        );
    }
    Ok(())
}

/// End of an EFS volume's replica superblock, which `fs_size` does not cover; 0 for XFS.
fn efs_replica_end<R: Read + Seek>(kind: SgiFsKind, reader: &mut R, offset: u64) -> Result<u64> {
    if kind != SgiFsKind::Efs {
        return Ok(0);
    }
    let mut sector = [0u8; SECTOR_SIZE as usize];
    reader.seek(SeekFrom::Start(offset + SECTOR_SIZE))?;
    reader
        .read_exact(&mut sector)
        .context("reading the EFS superblock")?;
    let sb = EfsSuperblock::parse(&sector)?;
    Ok(if sb.replsb == 0 {
        0
    } else {
        (u64::from(sb.replsb) + 1) * SECTOR_SIZE
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::{build_sgi_disk, SgiHddOptions};
    use std::io::Cursor;

    /// A 16 MiB EFS disk whose label is shrunk to 2 cylinders: the volume overruns its slot.
    #[test]
    fn a_filesystem_larger_than_its_partition_is_refused() {
        let (mut img, layout) =
            build_sgi_disk(&SgiHddOptions::new(16 * 1024 * 1024, "BIG")).unwrap();
        let mut vh = SgiVolumeHeader::parse(&img[..512]).unwrap();
        vh.partitions[0].blocks = 2 * layout.cylinder_sectors as u32;
        img[..512].copy_from_slice(&vh.to_bytes());
        let before = img.clone();

        let err = open_partition_editable(Cursor::new(&mut img[..]), 0)
            .err()
            .expect("an overrunning volume is refused")
            .to_string();
        assert!(err.contains("declares"), "{err}");
        assert!(open_partition(Cursor::new(&img[..]), 0).is_err());
        let end = (vh.partitions[0].first as usize + vh.partitions[0].blocks as usize) * 512;
        assert_eq!(
            &img[end..],
            &before[end..],
            "nothing past the partition changed"
        );
        assert_eq!(img, before);

        // The same disk with its true label opens.
        let (good, _) = build_sgi_disk(&SgiHddOptions::new(16 * 1024 * 1024, "BIG")).unwrap();
        assert!(open_partition(Cursor::new(good), 0).is_ok());
    }
}
