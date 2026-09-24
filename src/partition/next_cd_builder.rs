//! Synthesize a NeXTSTEP / OPENSTEP CD-ROM image from scratch: a NeXT `dlV3`
//! disk label counted in **2048-byte** sectors, wrapping one `4.3BSD` UFS
//! partition. This is what `rb-cli optical new next-ufs` writes.
//!
//! A NeXT "ISO" is not ISO 9660 at all. Every NeXTSTEP 3.x CISC disc (m68k +
//! Intel) is a hard-disk layout at CD sector size, and this module reproduces
//! the one both NeXTSTEP 3.3 reference CDs share:
//!
//! - label: `d_secsize` 2048, 32 tracks x 64 sectors, 300 rpm, an 80-sector
//!   (160 KiB) front porch, drive type `removable_rw_scsi`, boot blocks
//!   recorded at sectors 16 / 48, root partition `a`; four checksummed copies
//!   at 512-byte blocks 0 / 15 / 30 / 45, exactly as on a NeXT hard disk,
//! - slot 0: `4.3BSD`, base 0, the rest of the disc, `p_cpg` 2,
//! - the filesystem: the pre-4.4BSD UFS1 of [`crate::fs::ufs_format`] with
//!   8 KiB blocks over **2048-byte fragments**, `fs_fsbtodb` 0 (the device
//!   block is the CD sector), 2 cylinders per group, 1984 inodes per group,
//!   `fs_cgoffset` 64 — see [`Bsd43Geometry::NEXT_CDROM`].
//!
//! Directory chunks stay 1024 bytes even though the device block is 2048:
//! that is NeXT's compile-time `DIRBLKSIZ`, and every one of the 3168
//! directories on the NeXTSTEP 3.3 CISC disc is chunked that way.
//!
//! The disc is a data disc — no boot blocks are written. The RISC discs
//! (HP-PA LIF + Sun label + NeXT label at 8 KiB) are a different shape and not
//! produced here.

use anyhow::{ensure, Context, Result};
use std::io::{Read, Seek, SeekFrom, Write};

use crate::fs::ufs::{CgLayout, UfsEndian};
use crate::fs::ufs_format::{write_blank_ufs1, Bsd43Geometry, Ufs1FormatParams, Ufs1Geometry};
use crate::partition::next::{
    build_label, write_copies, NextLabelSpec, NextPartitionSpec, LABEL_BLOCKS, N_PARTITIONS,
};
use crate::rbformats::payload_slice::PayloadSlice;

/// `d_secsize` on a NeXT CD — the CD-ROM sector.
pub const NEXT_CD_SECTOR: u64 = 2048;
/// `d_front`, in CD sectors: the same 160 KiB a NeXT hard disk reserves.
pub const NEXT_CD_FRONT_PORCH: u64 = 80;
/// `d_ncylinders` both reference CDs record whatever their size.
const NEXT_CD_NCYLINDERS: u32 = 1024;
const NEXT_CD_RPM: u32 = 300;
/// Filesystem block and fragment the reference CDs use.
const NEXT_CD_BSIZE: u64 = 8192;
const NEXT_CD_FSIZE: u64 = NEXT_CD_SECTOR;
/// One inode per two fragments, which lands on the reference CDs' 1984 inodes per group.
pub const NEXT_CD_BYTES_PER_INODE: u64 = 4096;
/// Smallest disc worth building: a few cylinder groups past the porch.
const MIN_DISC_BYTES: u64 = 4 * 1024 * 1024;

/// Inputs for [`write_next_ufs_cd`].
#[derive(Debug, Clone)]
pub struct NextCdOptions {
    /// Requested disc size in bytes, porch included. Rounded up to a whole CD sector.
    pub size_bytes: u64,
    /// `dl_label` / `d_name` — the name NeXTSTEP shows for the disc (23 bytes max).
    pub label: String,
    /// UFS inode density; `None` takes the reference CDs' [`NEXT_CD_BYTES_PER_INODE`].
    pub bytes_per_inode: Option<u64>,
}

impl NextCdOptions {
    pub fn new(size_bytes: u64, label: impl Into<String>) -> Self {
        Self {
            size_bytes,
            label: label.into(),
            bytes_per_inode: None,
        }
    }
}

/// Where everything landed.
#[derive(Debug, Clone)]
pub struct NextCdLayout {
    pub disk_bytes: u64,
    /// Byte offset of the UFS — the end of the front porch.
    pub fs_offset: u64,
    /// Bytes partition `a` covers, in whole CD sectors.
    pub fs_bytes: u64,
    pub geometry: Ufs1Geometry,
}

/// The label a disc of `partition_sectors` CD sectors carries.
fn label_spec(opts: &NextCdOptions, partition_sectors: u64, geo: &Ufs1Geometry) -> NextLabelSpec {
    let mut partitions: Vec<Option<NextPartitionSpec>> = vec![None; N_PARTITIONS];
    partitions[0] = Some(NextPartitionSpec {
        base: 0,
        size: partition_sectors as i32,
        block_size: geo.bsize as u16,
        frag_size: geo.fsize as u16,
        cpg: geo.cpg as u16,
        ..Default::default()
    });
    let per_cylinder = Bsd43Geometry::NEXT_CDROM.ntrak * Bsd43Geometry::NEXT_CDROM.nsect;
    let needed = (NEXT_CD_FRONT_PORCH + partition_sectors).div_ceil(per_cylinder) as u32;
    NextLabelSpec {
        label: opts.label.clone(),
        // Both reference CDs repeat the disc name as the drive name.
        drive_name: opts.label.clone(),
        drive_type: "removable_rw_scsi".to_string(),
        sector_size: NEXT_CD_SECTOR as u32,
        ntracks: Bsd43Geometry::NEXT_CDROM.ntrak as u32,
        nsectors: Bsd43Geometry::NEXT_CDROM.nsect as u32,
        ncylinders: NEXT_CD_NCYLINDERS.max(needed),
        rpm: NEXT_CD_RPM,
        front_porch: NEXT_CD_FRONT_PORCH as u16,
        partitions,
        ..Default::default()
    }
}

/// The UFS parameters for a partition of `fs_bytes`.
fn ufs_params(opts: &NextCdOptions, fs_bytes: u64) -> Ufs1FormatParams {
    Ufs1FormatParams {
        size_bytes: fs_bytes,
        block_size: NEXT_CD_BSIZE,
        frag_size: NEXT_CD_FSIZE,
        bytes_per_inode: opts.bytes_per_inode.unwrap_or(NEXT_CD_BYTES_PER_INODE),
        endian: UfsEndian::Big,
        cg_layout: CgLayout::Bsd43,
        label: None,
        bsd43_geometry: Some(Bsd43Geometry::NEXT_CDROM),
    }
}

/// Plan the disc without writing it: sizes, offsets and the UFS geometry.
pub fn plan_next_ufs_cd(opts: &NextCdOptions) -> Result<NextCdLayout> {
    ensure!(
        opts.size_bytes >= MIN_DISC_BYTES,
        "a NeXT CD needs at least {}; {} was asked for",
        crate::partition::format_size(MIN_DISC_BYTES),
        crate::partition::format_size(opts.size_bytes),
    );
    let disc_sectors = opts.size_bytes.div_ceil(NEXT_CD_SECTOR);
    let partition_sectors = disc_sectors - NEXT_CD_FRONT_PORCH;
    ensure!(
        partition_sectors <= i32::MAX as u64,
        "a NeXT label cannot describe a {}-sector partition",
        partition_sectors,
    );
    let fs_bytes = partition_sectors * NEXT_CD_SECTOR;
    let geometry = crate::fs::ufs_format::plan(&ufs_params(opts, fs_bytes))
        .map_err(|e| anyhow::anyhow!("planning the NeXT CD's UFS: {e}"))?;
    Ok(NextCdLayout {
        disk_bytes: disc_sectors * NEXT_CD_SECTOR,
        fs_offset: NEXT_CD_FRONT_PORCH * NEXT_CD_SECTOR,
        fs_bytes,
        geometry,
    })
}

/// Stream a blank NeXT CD into `sink`, which ends up exactly `disk_bytes` long.
/// Only the label and UFS metadata are written, so a file sink stays sparse.
pub fn write_next_ufs_cd<W: Read + Write + Seek>(
    sink: &mut W,
    opts: &NextCdOptions,
) -> Result<NextCdLayout> {
    let layout = plan_next_ufs_cd(opts)?;
    let partition_sectors = layout.fs_bytes / NEXT_CD_SECTOR;
    let label = build_label(&label_spec(opts, partition_sectors, &layout.geometry));
    write_copies(sink, &label, &LABEL_BLOCKS).context("writing the NeXT disk label")?;

    let mut window = PayloadSlice::bounded(&mut *sink, layout.fs_offset, layout.fs_bytes);
    write_blank_ufs1(&mut window, &ufs_params(opts, layout.fs_bytes))
        .map_err(|e| anyhow::anyhow!("formatting the NeXT CD's UFS: {e}"))?;

    // The metadata ends well short of the disc; one byte at the end fixes its length.
    sink.seek(SeekFrom::Start(layout.disk_bytes - 1))?;
    sink.write_all(&[0])?;
    sink.flush()?;
    Ok(layout)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::{
        CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
    };
    use crate::fs::ufs::UfsFilesystem;
    use crate::fs::ufs_fsck::fsck_ufs;
    use crate::partition::PartitionTable;
    use byteorder::{BigEndian, ByteOrder};
    use std::io::Cursor;

    /// `NeXTTIME.iso`'s partition `a`: 98304 CD sectors behind the 80-sector porch.
    const NEXTTIME_PARTITION_SECTORS: u64 = 98304;

    fn nexttime_sized() -> NextCdOptions {
        NextCdOptions::new(
            (NEXT_CD_FRONT_PORCH + NEXTTIME_PARTITION_SECTORS) * NEXT_CD_SECTOR,
            "NEXTIME_1.0",
        )
    }

    fn build(opts: &NextCdOptions) -> (Vec<u8>, NextCdLayout) {
        let mut cur = Cursor::new(Vec::new());
        let layout = write_next_ufs_cd(&mut cur, opts).expect("build");
        (cur.into_inner(), layout)
    }

    /// Every derived field matches what NeXT's own mastering wrote for the same size.
    #[test]
    fn geometry_matches_the_nexttime_reference_cd() {
        let layout = plan_next_ufs_cd(&nexttime_sized()).unwrap();
        let g = layout.geometry;
        assert_eq!(layout.fs_offset, 163_840);
        assert_eq!(
            (g.bsize, g.fsize, g.frag, g.fsbtodb, g.nspf),
            (8192, 2048, 4, 0, 1)
        );
        assert_eq!((g.sblkno, g.cblkno, g.iblkno, g.dblkno), (8, 12, 16, 140));
        assert_eq!((g.cgoffset, g.cgstagger), (64, 31));
        assert_eq!((g.ntrak, g.nsect, g.spc, g.cpg), (32, 64, 2048, 2));
        assert_eq!((g.fpg, g.ipg, g.ncg, g.ncyl), (4096, 1984, 24, 48));
        assert_eq!((g.size_frags, g.dsize_frags), (98304, 95127));
        assert_eq!((g.csaddr, g.cssize, g.cgsize), (140, 2048, 2048));
        assert_eq!((g.rps, g.maxcontig, g.maxbpg), (5, 20000, 512));
    }

    /// The superblock words NeXTTIME.iso carries, read big-endian off our image.
    #[test]
    fn superblock_matches_the_nexttime_reference_cd() {
        let (img, layout) = build(&nexttime_sized());
        let sb = &img[(layout.fs_offset + 8192) as usize..][..2048];
        let w = |o: usize| BigEndian::read_i32(&sb[o..o + 4]);
        let want: &[(usize, i32)] = &[
            (0x08, 8),
            (0x0C, 12),
            (0x10, 16),
            (0x14, 140),
            (0x18, 64),
            (0x1C, -32),
            (0x24, 98304),
            (0x28, 95127),
            (0x2C, 24),
            (0x30, 8192),
            (0x34, 2048),
            (0x38, 4),
            (0x3C, 10),
            (0x40, 0),
            (0x44, 5),
            (0x48, -8192),
            (0x4C, -2048),
            (0x50, 13),
            (0x54, 11),
            (0x58, 20000),
            (0x5C, 512),
            (0x60, 2),
            (0x64, 0),
            (0x68, 2048),
            (0x6C, -512),
            (0x70, 9),
            (0x74, 2048),
            (0x78, 64),
            (0x7C, 1),
            (0x80, 0),
            (0x98, 140),
            (0x9C, 2048),
            (0xA0, 2048),
            (0xA4, 32),
            (0xA8, 64),
            (0xAC, 2048),
            (0xB0, 48),
            (0xB4, 2),
            (0xB8, 1984),
            (0xBC, 4096),
        ];
        for &(off, v) in want {
            assert_eq!(w(off), v, "superblock word at {off:#x}");
        }
        assert_eq!(BigEndian::read_i32(&sb[1372..1376]), 0x011954);
    }

    /// Label header and slot 0 match the reference disc byte for byte, name aside.
    #[test]
    fn label_matches_the_nexttime_reference_cd() {
        let (img, _) = build(&nexttime_sized());
        for block in LABEL_BLOCKS {
            let at = (block * 512) as usize;
            assert_eq!(&img[at..at + 4], b"dlV3", "copy at block {block}");
            assert_eq!(BigEndian::read_u32(&img[at + 4..at + 8]), block as u32);
        }
        let l = &img[..0x230];
        assert_eq!(&l[0x44..0x55], b"removable_rw_scsi");
        let u32_at = |o: usize| BigEndian::read_u32(&l[o..o + 4]);
        assert_eq!(
            [
                u32_at(0x5C),
                u32_at(0x60),
                u32_at(0x64),
                u32_at(0x68),
                u32_at(0x6C)
            ],
            [2048, 32, 64, 1024, 300]
        );
        assert_eq!(BigEndian::read_u16(&l[0x70..0x72]), 80);
        assert_eq!([u32_at(0x7C), u32_at(0x80)], [16, 48]);
        assert_eq!((l[0xBC], l[0xBD]), (b'a', b'b'));
        // Slot 0 as NeXTTIME.iso records it.
        let reference = "0000000000018000200008007400000210000a01\
                         0000000000000000000000000000000001342e33425344000000";
        let got: String = l[0xBE..0xBE + 46]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(got, reference);
        // Slot 1 carries the unused pattern.
        assert_eq!(&l[0xBE + 46..0xBE + 58], &[0xFF; 12]);
    }

    /// The disc parses as a NeXT label and its UFS takes a tree an fsck calls clean.
    #[test]
    fn built_disc_round_trips_through_the_reader_and_fsck() {
        let opts = NextCdOptions::new(24 * 1024 * 1024, "RoundTrip");
        let (img, layout) = build(&opts);
        assert_eq!(img.len() as u64, layout.disk_bytes);

        let mut cur = Cursor::new(img);
        let table = PartitionTable::detect(&mut cur).expect("detect");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1, "{parts:?}");
        assert_eq!(parts[0].byte_offset(), layout.fs_offset);

        let mut fs = UfsFilesystem::open(cur, layout.fs_offset).expect("open");
        assert_eq!(fs.dirblksiz, 1024, "NeXT CDs chunk directories at 1024");
        let root = fs.root().unwrap();
        let dir = fs
            .create_directory(&root, "NextApps", &CreateDirectoryOptions::default())
            .unwrap();
        // Enough names to spill the directory past its first 1024-byte chunk.
        for i in 0..80 {
            let body = vec![i as u8; 3000];
            fs.create_file(
                &dir,
                &format!("Application-{i:03}.app"),
                &mut Cursor::new(body),
                3000,
                &CreateFileOptions::default(),
            )
            .unwrap();
        }
        fs.sync_metadata().unwrap();
        let report = fsck_ufs(&mut fs).expect("fsck");
        assert!(
            report.errors.is_empty() && report.warnings.is_empty(),
            "{:?} / {:?}",
            report.errors,
            report.warnings
        );
        let listed = fs.list_directory(&dir).unwrap();
        assert_eq!(
            listed.iter().filter(|e| e.name.ends_with(".app")).count(),
            80
        );
    }

    #[test]
    fn a_disc_below_the_floor_is_refused() {
        assert!(plan_next_ufs_cd(&NextCdOptions::new(1024 * 1024, "tiny")).is_err());
    }
}
