//! `rb-cli new` — create blank images, grouped by media class:
//!
//! * `new floppy <fs> IMG`  — a bare single-volume image at floppy geometry
//!   (the fixed-geometry retro filesystems plus small FAT/HFS).
//! * `new volume <fs> IMG`  — a bare single-volume image of arbitrary size
//!   (a "superfloppy": NTFS, ext4, HFS+, EFS, …). No partition table.
//! * `new hd {x68k|sgi-efs} IMG` — a partition-table-wrapped, bootable
//!   hard-disk image.
//!
//! CD-ROM images live under `optical new` (e.g. `optical new sgi-efs`).
//! Multi-partition images go through the `batch` verb.
//!
//! Each class exposes only the filesystems and flags valid for it, so
//! `new floppy --help` no longer lists NTFS/ext4 and `--cpm-preset` only
//! appears where CP/M can be formatted. All three ultimately funnel through
//! the shared [`FsKind`] dispatch in [`format_image`].

use anyhow::{Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use std::path::PathBuf;

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::fs::ntfs_format::{create_ntfs, NtfsFormatParams, NtfsGeometry};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FsKind {
    /// Classic HFS (Mac OS Standard).
    Hfs,
    /// HFS+ / HFSX (Mac OS Extended). A bare single volume, no partition
    /// map. Use --case-sensitive for HFSX and --min-catalog to floor the
    /// catalog B-tree size.
    Hfsplus,
    /// BasiliskII HFV — a flat classic-HFS volume, no partition table.
    /// Same on-disk bytes as `hfs`, but capped at 2047 MB and with the
    /// allocation block size auto-floored so the result is mountable by
    /// BasiliskII / SheepShaver and classic Mac OS.
    Hfv,
    /// FAT12 / FAT16 / FAT32, auto-selected: FAT32 above 2 GiB, otherwise
    /// FAT12 or FAT16 by cluster count (small volumes are FAT12).
    Fat,
    /// IRIX EFS (single cylinder group).
    Efs,
    /// XFS (IRIX 6 / Linux). A v5/CRC volume with 4 KiB blocks, 512-byte
    /// inodes and an internal log; minimum 32 MiB.
    Xfs,
    /// Amiga FFS / OFS (variant selected via --affs-variant).
    Affs,
    /// NTFS (Windows NT / 2000 / XP). Cluster and sector size via
    /// --cluster-size / --sector-size; both auto-selected when unset.
    Ntfs,
    /// HPFS (OS/2 High Performance File System). A bare single volume with
    /// boot/super/spare blocks, free-space bitmaps, a directory band, and an
    /// empty root directory. `--name` sets the volume label.
    Hpfs,
    /// ext2 (Linux). A plain rev-1 ext2 volume (128-byte inodes, no journal or
    /// checksums); block size auto-selected (1 KiB below 8 MiB, else 4 KiB).
    #[value(alias = "ext2")]
    Ext,
    /// ext3 (Linux). ext2 plus an empty jbd2 journal; 4 KiB blocks, minimum
    /// ~8 MiB (must hold the 4 MiB journal).
    Ext3,
    /// ext4 (Linux). Extents + metadata_csum (crc32c) + a jbd2 journal; 256-byte
    /// inodes, 4 KiB blocks, minimum ~8 MiB.
    Ext4,
    /// ProDOS (Apple II / IIgs). A bare volume with boot blocks, a 4-block
    /// volume directory, and a bitmap; size rounds to 512-byte blocks, from a
    /// 8 KiB minimum up to a ~32 MiB (65535-block) maximum. The name is
    /// uppercased and must follow ProDOS rules: up to 15 of A-Z / 0-9 / '.',
    /// with a leading letter.
    Prodos,
    /// Atari DOS 2.0S (Atari 8-bit). A single-density 90 KB floppy (720 × 128 B
    /// sectors) with boot / VTOC / directory reserved. Fixed geometry, so
    /// `--size` and `--name` are ignored (enhanced density is read-only).
    Atari,
    /// Apple DOS 3.3 (Apple II). A 140 KB 5.25" floppy (35 × 16 × 256, DOS
    /// order) with a VTOC + empty catalog and only track 17 reserved (a
    /// non-bootable data disk). Fixed geometry, so `--size` / `--name` are
    /// ignored.
    #[value(alias = "appledos", alias = "dos33")]
    AppleDos,
    /// CP/M (Amstrad / PCW, Einstein, SV-328, Altair, MultiComp, ZX +3). The
    /// geometry comes from the disk-parameter block chosen with `--cpm-preset`
    /// (required); `--size` / `--name` are ignored. A blank CP/M disk is just
    /// its reserved tracks + data area filled with 0xE5.
    Cpm,
    /// OS-9 / NitrOS-9 RBF (Tandy CoCo, Dragon). A standard 35-track floppy
    /// (630 × 256-byte sectors): identification sector, allocation bitmap, and
    /// an empty root directory. Fixed geometry, so `--size` is ignored; `--name`
    /// is sanitized to the OS-9 name set (A-Z a-z 0-9 . _ $).
    #[value(alias = "nitros9", alias = "rbf")]
    Os9,
    /// Minix V1 (Minix, early Linux). Classic 30-char-name filesystem (magic
    /// 0x138F), 1 KiB blocks, geometry matching `mkfs.minix -1`; up to 64 MiB.
    /// `--name` is ignored (Minix has no volume label).
    #[value(alias = "minix1")]
    Minix,
    /// Minix V2 (magic 0x2478): like V1 with 32-bit zone pointers and triple
    /// indirection, matching `mkfs.minix -2`.
    Minix2,
    /// Minix V3 (magic 0x4D5A): 60-char names, matching `mkfs.minix -3`.
    Minix3,
    /// UCSD p-System (UCSD Pascal — Apple II/III, PC). A flat volume with a
    /// zeroed bootstrap and an empty directory. `--name` sets the volume label
    /// (uppercased, ≤ 7 chars). Defaults to a 140 KB Apple II floppy.
    #[value(alias = "pascal", alias = "psystem")]
    Ucsd,
    /// TR-DOS (ZX Spectrum Beta Disk). A flat volume with an empty catalogue.
    /// `--name` sets the 8-char disk label. Geometry follows `--size`: the
    /// smallest of 160 KB (40-track SS), 320 KB (80-track SS), or 640 KB
    /// (80-track DS, the default and maximum) that covers it.
    #[value(alias = "beta", alias = "betadisk", alias = "zx")]
    Trdos,
    /// TI-99/4A disk. An empty VIB + FDIR (flat V9T9 `.dsk`). `--name` sets the
    /// 10-char disk name. Geometry follows `--size`: the smallest of 90 KB
    /// (SSSD), 180 KB (DSSD), or 360 KB (DSDD, the default and maximum).
    #[value(alias = "ti99_4a", alias = "ti994a")]
    Ti99,
    /// Oric Jasmin (Oric-1 / Atmos / Telestrat). A flat 256-byte-sector image
    /// with an empty directory. Single-sided (178432 bytes / 697 blocks);
    /// `--name` sets the 8-char volume name. `--size` is ignored.
    #[value(alias = "jasmin")]
    Oric,
    /// MFS (Macintosh File System — Mac 128K / 512K / Plus). A flat, pre-HFS
    /// 400 KB / 800 KB floppy volume: zeroed boot blocks, an MDB, an all-free
    /// allocation map, and an empty directory. `--name` sets the volume label
    /// (Mac Roman, up to 27 chars). Geometry follows `--size` (which defaults
    /// to 800 KB; pass `--size 400K` for the classic single-sided floppy).
    #[value(alias = "macintosh")]
    Mfs,
    /// Acorn ADFS E-format (Archimedes / RISC OS). An 800 KB double-sided
    /// floppy: 1024-byte sectors, single-zone new-map Free Space Map, and an
    /// empty Hugo root directory. Fixed geometry, so `--size` is ignored;
    /// `--name` sets the 10-char disc name (space-padded).
    #[value(alias = "acorn")]
    Adfs,
}

/// The three media classes `new` creates. CD-ROM images are not here —
/// they live under `optical new`.
#[derive(Debug, Subcommand)]
pub enum NewCommand {
    /// Blank floppy-geometry single volume (bare, no partition table):
    /// FAT / HFS and the fixed-geometry retro filesystems.
    Floppy(FloppyArgs),

    /// Blank bare single volume of arbitrary size (a "superfloppy"): the
    /// larger filesystems (NTFS, ext, HFS+, EFS, AFFS, …). No partition table.
    Volume(VolumeArgs),

    /// Partition-table-wrapped, self-bootable hard-disk image.
    Hd {
        #[command(subcommand)]
        cmd: HdCommand,
    },
}

/// Bootable hard-disk targets — each wraps a platform partition table around
/// a formatted root, so the "filesystem" here is really the platform. Reuses
/// the standalone builders (formerly `new-x68k-hdd` / `new-sgi-hdd`).
#[derive(Debug, Subcommand)]
pub enum HdCommand {
    /// Sharp X68000 HDD (SASI / SCSI): X68K partition table + IPL stub + a
    /// blank or donor-cloned Human68k partition.
    X68k(super::new_x68k_hdd::NewX68kHddArgs),

    /// dvh-wrapped IRIX HDD: an SGI volume header + partition table wrapping a
    /// formatted EFS root partition, mountable by IRIX 5.3-6.5. Pass
    /// `--from-dir` to fill the root filesystem from a host folder in the
    /// same step; otherwise it comes out blank for `import` / `put`.
    #[command(name = "sgi-efs")]
    SgiEfs(super::new_sgi_hdd::NewSgiHddArgs),

    /// MBR (DOS / PC) disk with partitions you size and type yourself. The
    /// partitions come out empty — fill them with `write --partition N` or
    /// format them with `reformat`.
    Mbr(super::new_partitioned_hd::PartitionedHdArgs),

    /// GPT (UEFI, modern macOS / Linux) disk with partitions you size and
    /// type yourself.
    Gpt(super::new_partitioned_hd::PartitionedHdArgs),

    /// APM (Apple Partition Map) disk for classic Mac OS / PowerPC, with
    /// partitions you size and type yourself.
    Apm(super::new_partitioned_hd::PartitionedHdArgs),

    /// SGI volume header (IRIX) with partitions you size and type yourself.
    /// Cylinder-aligned from `--heads` / `--sectors`. Unlike `sgi-efs` the
    /// partitions come out empty rather than EFS-formatted.
    Sgi(super::new_partitioned_hd::SgiHdArgs),

    /// Sharp X68000 SCSI/SASI table with partitions you size yourself.
    /// Unlike `x68k` this writes only the table -- no IPL stub, no Human68k
    /// formatting -- so it is a data disk, not a bootable one.
    #[command(name = "x68k-table")]
    X68kTable(super::new_partitioned_hd::PartitionedHdArgs),
}

/// Filesystems offered under `new floppy`. Converts to the master [`FsKind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FloppyFs {
    /// FAT12 / FAT16 (auto by cluster count at floppy sizes).
    Fat,
    /// Classic HFS (Mac OS Standard) — a 400/800 KB Mac floppy.
    Hfs,
    /// Atari DOS 2.0S (Atari 8-bit) — 90 KB single-density.
    Atari,
    /// Apple DOS 3.3 (Apple II) — 140 KB 5.25".
    #[value(alias = "appledos", alias = "dos33")]
    AppleDos,
    /// CP/M (Amstrad / PCW, Einstein, SV-328, Altair, MultiComp, ZX +3) —
    /// geometry from `--cpm-preset`.
    Cpm,
    /// OS-9 / NitrOS-9 RBF (Tandy CoCo, Dragon) — 35-track floppy.
    #[value(alias = "nitros9", alias = "rbf")]
    Os9,
    /// UCSD p-System (UCSD Pascal) — defaults to a 140 KB Apple II floppy.
    #[value(alias = "pascal", alias = "psystem")]
    Ucsd,
    /// TR-DOS (ZX Spectrum Beta Disk) — 160/320/640 KB.
    #[value(alias = "beta", alias = "betadisk", alias = "zx")]
    Trdos,
    /// TI-99/4A disk (flat V9T9 `.dsk`) — 90/180/360 KB.
    #[value(alias = "ti99_4a", alias = "ti994a")]
    Ti99,
    /// Oric Jasmin (Oric-1 / Atmos / Telestrat) — 178 KB single-sided.
    #[value(alias = "jasmin")]
    Oric,
    /// MFS (Macintosh File System) — 400/800 KB pre-HFS floppy.
    #[value(alias = "macintosh")]
    Mfs,
    /// Acorn ADFS E-format (Archimedes / RISC OS) — 800 KB.
    #[value(alias = "acorn")]
    Adfs,
    /// Minix V1 — classic 30-char-name filesystem, `mkfs.minix -1` geometry.
    #[value(alias = "minix1")]
    Minix,
}

impl FloppyFs {
    fn to_fs_kind(self) -> FsKind {
        match self {
            FloppyFs::Fat => FsKind::Fat,
            FloppyFs::Hfs => FsKind::Hfs,
            FloppyFs::Atari => FsKind::Atari,
            FloppyFs::AppleDos => FsKind::AppleDos,
            FloppyFs::Cpm => FsKind::Cpm,
            FloppyFs::Os9 => FsKind::Os9,
            FloppyFs::Ucsd => FsKind::Ucsd,
            FloppyFs::Trdos => FsKind::Trdos,
            FloppyFs::Ti99 => FsKind::Ti99,
            FloppyFs::Oric => FsKind::Oric,
            FloppyFs::Mfs => FsKind::Mfs,
            FloppyFs::Adfs => FsKind::Adfs,
            FloppyFs::Minix => FsKind::Minix,
        }
    }
}

/// Filesystems offered under `new volume`. Converts to the master [`FsKind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum VolumeFs {
    /// Classic HFS (Mac OS Standard).
    Hfs,
    /// HFS+ / HFSX (Mac OS Extended). Use --case-sensitive for HFSX.
    Hfsplus,
    /// BasiliskII HFV — a flat classic-HFS volume, capped at 2047 MB.
    Hfv,
    /// FAT12 / FAT16 / FAT32, auto-selected by size.
    Fat,
    /// NTFS (Windows NT / 2000 / XP). Tune with --cluster-size / --sector-size.
    Ntfs,
    /// HPFS (OS/2). A bare volume with an empty root directory.
    Hpfs,
    /// ext2 (Linux) — plain rev-1, no journal or checksums.
    #[value(alias = "ext2")]
    Ext,
    /// ext3 (Linux) — ext2 plus an empty jbd2 journal.
    Ext3,
    /// ext4 (Linux) — extents + metadata_csum (crc32c) + a jbd2 journal.
    Ext4,
    /// Amiga FFS / OFS (variant via --affs-variant).
    Affs,
    /// ProDOS (Apple II / IIgs) — up to ~32 MiB.
    Prodos,
    /// IRIX EFS (single cylinder group) — a bare EFS superfloppy.
    Efs,
    /// XFS (IRIX 6 / Linux) — a bare v5/CRC volume, minimum 32 MiB.
    Xfs,
    /// Minix V2 — 32-bit zone pointers, `mkfs.minix -2`.
    Minix2,
    /// Minix V3 — 60-char names, `mkfs.minix -3`.
    Minix3,
}

impl VolumeFs {
    fn to_fs_kind(self) -> FsKind {
        match self {
            VolumeFs::Hfs => FsKind::Hfs,
            VolumeFs::Hfsplus => FsKind::Hfsplus,
            VolumeFs::Hfv => FsKind::Hfv,
            VolumeFs::Fat => FsKind::Fat,
            VolumeFs::Ntfs => FsKind::Ntfs,
            VolumeFs::Hpfs => FsKind::Hpfs,
            VolumeFs::Ext => FsKind::Ext,
            VolumeFs::Ext3 => FsKind::Ext3,
            VolumeFs::Ext4 => FsKind::Ext4,
            VolumeFs::Affs => FsKind::Affs,
            VolumeFs::Prodos => FsKind::Prodos,
            VolumeFs::Efs => FsKind::Efs,
            VolumeFs::Xfs => FsKind::Xfs,
            VolumeFs::Minix2 => FsKind::Minix2,
            VolumeFs::Minix3 => FsKind::Minix3,
        }
    }
}

/// `new floppy <fs> IMG` — a bare floppy-geometry volume.
#[derive(Debug, Args)]
pub struct FloppyArgs {
    /// Filesystem to format (see the per-value help above).
    #[arg(value_enum)]
    pub fs: FloppyFs,

    /// Image file to create. Overwritten if it already exists.
    pub image: PathBuf,

    /// Volume size (bytes or `K`/`M`/`G` suffixes). Ignored by the
    /// fixed-geometry filesystems. Defaults to 800K.
    #[arg(long, default_value = "800K")]
    pub size: String,

    /// Volume label/name. Defaults to `rusty-backup`.
    #[arg(long, default_value = "rusty-backup")]
    pub name: String,

    /// HFS allocation block size in bytes (multiple of 512). Auto when unset.
    #[arg(long = "block-size")]
    pub block_size: Option<u32>,

    /// HFS Catalog B-tree initial size in bytes. Auto when unset.
    #[arg(long = "catalog-size")]
    pub catalog_size: Option<String>,

    /// HFS Extents-overflow B-tree initial size in bytes. Auto when unset.
    #[arg(long = "extents-size")]
    pub extents_size: Option<String>,

    /// CP/M disk-parameter-block preset (required with `cpm`). One of:
    /// amstrad_data, amstrad_sys, amstrad_pcw, einstein, svi328_cpm,
    /// altair_8in, altair_cf, multicomp, zxplus3.
    #[arg(long = "cpm-preset")]
    pub cpm_preset: Option<String>,

    /// FAT only: format FAT32 regardless of size. Without this the type comes
    /// from the capacity and only reaches FAT32 above 2 GiB, which cannot
    /// express an EFI System Partition - FAT32, and usually 100-512 MiB.
    #[arg(long = "fat32")]
    pub fat32: bool,
}

impl FloppyArgs {
    fn into_new_args(self) -> NewArgs {
        NewArgs {
            image: self.image,
            fs: self.fs.to_fs_kind(),
            size: self.size,
            name: self.name,
            block_size: self.block_size,
            catalog_size: self.catalog_size,
            extents_size: self.extents_size,
            case_sensitive: false,
            min_catalog: None,
            affs_variant: 1,
            fat32: self.fat32,
            cpm_preset: self.cpm_preset,
            inodes: None,
            bytes_per_inode: None,
            cluster_size: None,
            sector_size: None,
        }
    }
}

/// `new volume <fs> IMG` — a bare single volume of arbitrary size.
#[derive(Debug, Args)]
pub struct VolumeArgs {
    /// Filesystem to format (see the per-value help above).
    #[arg(value_enum)]
    pub fs: VolumeFs,

    /// Image file to create. Overwritten if it already exists.
    pub image: PathBuf,

    /// Volume size (bytes or `K`/`M`/`G` suffixes). Defaults to 800K.
    #[arg(long, default_value = "800K")]
    pub size: String,

    /// Volume label/name. Defaults to `rusty-backup`.
    #[arg(long, default_value = "rusty-backup")]
    pub name: String,

    /// HFS/HFS+ allocation block size in bytes (multiple of 512). Auto when unset.
    #[arg(long = "block-size")]
    pub block_size: Option<u32>,

    /// HFS Catalog B-tree initial size in bytes. Auto when unset.
    #[arg(long = "catalog-size")]
    pub catalog_size: Option<String>,

    /// HFS Extents-overflow B-tree initial size in bytes. Auto when unset.
    #[arg(long = "extents-size")]
    pub extents_size: Option<String>,

    /// HFS+ only: format a case-sensitive (HFSX) volume.
    #[arg(long = "case-sensitive")]
    pub case_sensitive: bool,

    /// HFS+ only: minimum catalog B-tree size in bytes (a floor).
    #[arg(long = "min-catalog")]
    pub min_catalog: Option<String>,

    /// FAT only: format FAT32 regardless of size. Without this the type is
    /// picked from the capacity and only reaches FAT32 above 2 GiB, which
    /// cannot express an EFI System Partition - FAT32, and usually
    /// 100-512 MiB.
    #[arg(long = "fat32")]
    pub fat32: bool,

    /// AFFS variant byte (0=OFS, 1=FFS, 2=OFS+intl, 3=FFS+intl,
    /// 4=OFS+dircache, 5=FFS+dircache). Defaults to 1 (FFS).
    #[arg(long = "affs-variant", default_value = "1")]
    pub affs_variant: u8,

    /// EFS only: approximate total inode count. Mutually exclusive with
    /// `--bytes-per-inode`.
    #[arg(long, conflicts_with = "bytes_per_inode")]
    pub inodes: Option<u64>,

    /// EFS only: inode density in bytes per inode (smaller = more inodes).
    #[arg(long)]
    pub bytes_per_inode: Option<u64>,

    /// NTFS only: cluster (allocation unit) size, e.g. `4K`, `64K`. Auto when unset.
    #[arg(long = "cluster-size")]
    pub cluster_size: Option<String>,

    /// NTFS only: bytes per sector — 512, 1024, 2048 or 4096. Defaults to 512.
    #[arg(long = "sector-size")]
    pub sector_size: Option<u32>,
}

impl VolumeArgs {
    fn into_new_args(self) -> NewArgs {
        NewArgs {
            image: self.image,
            fs: self.fs.to_fs_kind(),
            size: self.size,
            name: self.name,
            block_size: self.block_size,
            catalog_size: self.catalog_size,
            extents_size: self.extents_size,
            case_sensitive: self.case_sensitive,
            min_catalog: self.min_catalog,
            affs_variant: self.affs_variant,
            fat32: self.fat32,
            cpm_preset: None,
            inodes: self.inodes,
            bytes_per_inode: self.bytes_per_inode,
            cluster_size: self.cluster_size,
            sector_size: self.sector_size,
        }
    }
}

/// Dispatch a parsed `new` subcommand to its formatter.
pub fn run(cmd: NewCommand) -> Result<()> {
    match cmd {
        NewCommand::Floppy(args) => format_image(args.into_new_args()),
        NewCommand::Volume(args) => format_image(args.into_new_args()),
        NewCommand::Hd { cmd } => match cmd {
            HdCommand::X68k(args) => super::new_x68k_hdd::run(args),
            HdCommand::SgiEfs(args) => super::new_sgi_hdd::run(args),
            HdCommand::Mbr(args) => super::new_partitioned_hd::run(
                super::new_partitioned_hd::PartitionedHdCommand::Mbr(args),
            ),
            HdCommand::Gpt(args) => super::new_partitioned_hd::run(
                super::new_partitioned_hd::PartitionedHdCommand::Gpt(args),
            ),
            HdCommand::Apm(args) => super::new_partitioned_hd::run(
                super::new_partitioned_hd::PartitionedHdCommand::Apm(args),
            ),
            HdCommand::Sgi(args) => super::new_partitioned_hd::run(
                super::new_partitioned_hd::PartitionedHdCommand::Sgi(args),
            ),
            HdCommand::X68kTable(args) => super::new_partitioned_hd::run(
                super::new_partitioned_hd::PartitionedHdCommand::X68k(args),
            ),
        },
    }
}

/// Internal plumbing struct: the union of every `new` flag, built from a
/// class-specific arg struct and dispatched by [`format_image`]. Still derives
/// `Args` so its field attributes stay valid, but it is never wired to a
/// top-level command — the class subcommands construct it directly.
#[derive(Debug, Args)]
pub struct NewArgs {
    /// Image file to create. Overwritten if it already exists.
    pub image: PathBuf,

    /// Filesystem to format. One of: hfs, hfsplus, hfv, fat, efs, affs, ntfs,
    /// ext (alias ext2), ext3, ext4, prodos, atari, apple-dos (alias appledos /
    /// dos33), cpm, os9 (alias nitros9 / rbf), minix (alias minix1), minix2,
    /// minix3, ucsd (alias pascal / psystem), trdos (alias beta / betadisk / zx),
    /// ti99 (alias ti99_4a / ti994a), mfs (alias macintosh), adfs (alias acorn).
    #[arg(long, value_enum)]
    pub fs: FsKind,

    /// Volume size, accepting plain bytes or `K`/`KiB`/`M`/`MiB`/`G`/`GiB`
    /// suffixes (e.g. `800K`, `5M`). Defaults to 800K (an 800 KiB floppy).
    #[arg(long, default_value = "800K")]
    pub size: String,

    /// Volume label/name. Defaults to `rusty-backup`. HFS: up to 27 Mac
    /// Roman bytes. FAT: up to 11 chars (uppercased; non-ASCII → `_`).
    /// EFS: 6-byte fname/fpack. AFFS: up to 30 bytes.
    #[arg(long, default_value = "rusty-backup")]
    pub name: String,

    /// HFS allocation block size in bytes. Must be a non-zero multiple of
    /// 512. When unset, the smallest size that keeps `total_blocks <=
    /// 65535` is chosen automatically. Ignored for other filesystems.
    #[arg(long = "block-size")]
    pub block_size: Option<u32>,

    /// HFS Catalog B-tree initial size in bytes (rounded up to a whole
    /// allocation block). When unset, scales with volume size like
    /// hformat (~0.5%, clump-aligned, 24-block floor). Ignored for other
    /// filesystems.
    #[arg(long = "catalog-size")]
    pub catalog_size: Option<String>,

    /// HFS Extents-overflow B-tree initial size in bytes (rounded up to a
    /// whole allocation block). When unset, ~half the catalog size.
    /// Ignored for other filesystems.
    #[arg(long = "extents-size")]
    pub extents_size: Option<String>,

    /// HFS+ only: format a case-sensitive (HFSX) volume instead of the
    /// default case-insensitive HFS+. Ignored for other filesystems.
    #[arg(long = "case-sensitive")]
    pub case_sensitive: bool,

    /// HFS+ only: minimum catalog B-tree size in bytes (a floor, rounded up
    /// to whole 4096-byte nodes). Set this *small* to make the catalog easy
    /// to outgrow and exercise the fork grow-on-full path. Ignored for other
    /// filesystems.
    #[arg(long = "min-catalog")]
    pub min_catalog: Option<String>,

    /// FAT only: format FAT32 regardless of size. Without this the type is
    /// picked from the capacity and only reaches FAT32 above 2 GiB, which
    /// cannot express an EFI System Partition - FAT32, and usually
    /// 100-512 MiB.
    #[arg(long = "fat32")]
    pub fat32: bool,

    /// AFFS variant byte (0=OFS, 1=FFS, 2=OFS+intl, 3=FFS+intl,
    /// 4=OFS+dircache, 5=FFS+dircache). Defaults to 1 (FFS).
    #[arg(long = "affs-variant", default_value = "1")]
    pub affs_variant: u8,

    /// CP/M disk-parameter-block preset (required with `--fs cpm`). One of:
    /// amstrad_data, amstrad_sys, amstrad_pcw, einstein, svi328_cpm,
    /// altair_8in, altair_cf, multicomp, zxplus3.
    #[arg(long = "cpm-preset")]
    pub cpm_preset: Option<String>,

    /// EFS only: approximate total inode count. The formatter scales its
    /// cylinder groups to hit roughly this many inodes. Mutually exclusive with
    /// `--bytes-per-inode`; default density is ~1 inode/4 KiB.
    #[arg(long, conflicts_with = "bytes_per_inode")]
    pub inodes: Option<u64>,

    /// EFS only: inode density in bytes per inode (smaller = more inodes),
    /// floored at one inode per 512-byte block. Mutually exclusive with
    /// `--inodes`.
    #[arg(long)]
    pub bytes_per_inode: Option<u64>,

    /// NTFS only: cluster (allocation unit) size, e.g. `4K`, `64K`, or a plain
    /// byte count. A power of two from 512 to 2 MiB and at least the sector
    /// size. When unset, chosen automatically from the volume size (the classic
    /// mkntfs default-by-size table). Ignored for other filesystems.
    #[arg(long = "cluster-size")]
    pub cluster_size: Option<String>,

    /// NTFS only: bytes per sector — 512, 1024, 2048 or 4096. Defaults to 512.
    /// Ignored for other filesystems.
    #[arg(long = "sector-size")]
    pub sector_size: Option<u32>,
}

/// Shared formatter: validate cross-flag/fs constraints, then dispatch on
/// [`FsKind`]. Fed by the class subcommands via `into_new_args`.
fn format_image(args: NewArgs) -> Result<()> {
    if (args.inodes.is_some() || args.bytes_per_inode.is_some()) && args.fs != FsKind::Efs {
        anyhow::bail!("--inodes / --bytes-per-inode are only valid with --fs efs");
    }
    let cluster_bytes = args
        .cluster_size
        .as_deref()
        .map(|s| parse_size(s).context("parsing --cluster-size"))
        .transpose()?
        .map(|v| v.min(u32::MAX as u64) as u32);
    if (cluster_bytes.is_some() || args.sector_size.is_some()) && args.fs != FsKind::Ntfs {
        anyhow::bail!("--cluster-size / --sector-size are only valid with --fs ntfs");
    }
    if (args.case_sensitive || args.min_catalog.is_some()) && args.fs != FsKind::Hfsplus {
        anyhow::bail!("--case-sensitive / --min-catalog are only valid with --fs hfsplus");
    }
    if args.cpm_preset.is_some() && args.fs != FsKind::Cpm {
        anyhow::bail!("--cpm-preset is only valid with --fs cpm");
    }
    match args.fs {
        FsKind::Hfs => {
            let catalog_bytes = args
                .catalog_size
                .as_deref()
                .map(|s| parse_size(s).context("parsing --catalog-size"))
                .transpose()?
                .map(|v| v.min(u32::MAX as u64) as u32);
            let extents_bytes = args
                .extents_size
                .as_deref()
                .map(|s| parse_size(s).context("parsing --extents-size"))
                .transpose()?
                .map(|v| v.min(u32::MAX as u64) as u32);
            crate::cli::api::hfs::cmd_new_sized(
                args.image,
                &args.size,
                &args.name,
                args.block_size,
                catalog_bytes,
                extents_bytes,
            )
        }
        FsKind::Hfsplus => {
            let min_catalog = args
                .min_catalog
                .as_deref()
                .map(|s| parse_size(s).context("parsing --min-catalog"))
                .transpose()?
                .map(|v| v.min(u32::MAX as u64) as u32);
            crate::cli::api::hfs::cmd_new_hfsplus_sized(
                args.image,
                &args.size,
                &args.name,
                args.block_size,
                args.case_sensitive,
                min_catalog,
            )
        }
        FsKind::Hfv => {
            // A flat HFV is just a blank classic-HFS volume with the BasiliskII
            // limits enforced: <= 2047 MB and a block size that keeps
            // total_blocks <= 65535. build_blank_hfv validates the cap.
            let explicit_bs = args.block_size;
            format_and_write(&args.image, &args.size, &args.name, move |size, name| {
                let bs = explicit_bs.unwrap_or_else(|| crate::fs::hfv::suggest_block_size(size));
                Ok(crate::fs::hfv::build_blank_hfv(size, bs, name)?)
            })
        }
        FsKind::Fat => {
            let fat32 = args.fat32;
            format_and_write(&args.image, &args.size, &args.name, move |size, name| {
                if fat32 {
                    crate::fs::fat::create_blank_fat32(size, Some(name))
                } else {
                    crate::fs::fat::create_blank_fat(size, Some(name))
                }
            })
        }
        FsKind::Efs => write_blank_efs_image(
            &args.image,
            &args.size,
            &args.name,
            crate::fs::efs::resolve_bytes_per_inode(
                parse_size(&args.size).context("parsing --size")?,
                args.inodes,
                args.bytes_per_inode,
            ),
        ),
        FsKind::Xfs => write_blank_xfs_image(&args.image, &args.size, &args.name),
        FsKind::Affs => {
            let variant = args.affs_variant;
            format_and_write(&args.image, &args.size, &args.name, |size, name| {
                crate::fs::affs::create_blank_affs(size, variant, name)
            })
        }
        FsKind::Ntfs => {
            let raw = parse_size(&args.size).context("parsing --size")?;
            let sector = args.sector_size.unwrap_or(512);
            // Auto cluster follows the mkntfs default-by-size table (512-byte
            // sectors); when an explicit sector size is given the cluster is
            // floored to it so the geometry stays valid.
            let geometry = match cluster_bytes {
                Some(c) => NtfsGeometry::with_cluster_size(c, sector)?,
                None => {
                    let auto = NtfsGeometry::for_volume_size(raw).cluster_size() as u32;
                    NtfsGeometry::with_cluster_size(auto.max(sector), sector)?
                }
            };
            let cluster = geometry.cluster_size();
            let total_size = raw / cluster * cluster;
            if total_size == 0 {
                anyhow::bail!("--size {raw} is smaller than one {cluster}-byte NTFS cluster");
            }
            if total_size != raw {
                log_stderr(format!(
                    "Note: NTFS size rounded down to {total_size} bytes (multiple of the {cluster}-byte cluster)"
                ));
            }
            // ~1 MFT record per 256 KiB, with a 128-record floor for headroom.
            let mft_records_hint = (total_size / (256 * 1024)).clamp(128, 1 << 20);
            write_blank_ntfs_image(
                &args.image,
                total_size,
                geometry,
                mft_records_hint,
                &args.name,
            )
        }
        FsKind::Hpfs => format_and_write(&args.image, &args.size, &args.name, |size, name| {
            Ok(crate::fs::hpfs::create_blank_hpfs(size, name)?)
        }),
        FsKind::Ext => write_blank_ext_image(&args.image, &args.size, &args.name, "ext2"),
        FsKind::Ext3 => write_blank_ext_image(&args.image, &args.size, &args.name, "ext3"),
        FsKind::Ext4 => write_blank_ext_image(&args.image, &args.size, &args.name, "ext4"),
        FsKind::Prodos => format_and_write(&args.image, &args.size, &args.name, |size, name| {
            crate::fs::prodos::create_blank_prodos(size, name)
        }),
        FsKind::Atari => format_and_write(&args.image, &args.size, &args.name, |_size, _name| {
            Ok(crate::fs::atari_dos::create_blank_atari_sd())
        }),
        FsKind::AppleDos => {
            format_and_write(&args.image, &args.size, &args.name, |_size, _name| {
                Ok(crate::fs::apple_dos::create_blank_apple_dos())
            })
        }
        FsKind::Os9 => {
            let vname = sanitize_os9_volume_name(&args.name);
            format_and_write(&args.image, &args.size, &vname, |_size, name| {
                Ok(crate::fs::os9::create_blank_os9(name)?)
            })
        }
        FsKind::Cpm => {
            let preset_name = args.cpm_preset.as_deref().ok_or_else(|| {
                anyhow::anyhow!("--fs cpm requires --cpm-preset <name> (e.g. amstrad_data)")
            })?;
            let dpb = *crate::fs::cpm_diskdefs::preset_by_name(preset_name).ok_or_else(|| {
                let valid: Vec<&str> = crate::fs::cpm_diskdefs::ALL_PRESETS
                    .iter()
                    .map(|d| d.name)
                    .collect();
                anyhow::anyhow!(
                    "unknown CP/M preset '{preset_name}'; valid presets: {}",
                    valid.join(", ")
                )
            })?;
            format_and_write(&args.image, &args.size, &args.name, move |_size, _name| {
                Ok(crate::fs::cpm::create_blank(dpb))
            })
        }
        FsKind::Minix => {
            write_blank_minix_image(&args.image, &args.size, crate::fs::minix::MinixVersion::V1)
        }
        FsKind::Minix2 => {
            write_blank_minix_image(&args.image, &args.size, crate::fs::minix::MinixVersion::V2)
        }
        FsKind::Minix3 => {
            write_blank_minix_image(&args.image, &args.size, crate::fs::minix::MinixVersion::V3)
        }
        FsKind::Ucsd => write_blank_ucsd_image(&args.image, &args.size, &args.name),
        FsKind::Trdos => write_blank_trdos_image(&args.image, &args.size, &args.name),
        FsKind::Ti99 => write_blank_ti99_image(&args.image, &args.size, &args.name),
        FsKind::Oric => format_and_write(&args.image, &args.size, &args.name, |_size, name| {
            Ok(crate::fs::oric::create_blank_oric(false, name))
        }),
        FsKind::Mfs => format_and_write(&args.image, &args.size, &args.name, |size, name| {
            crate::fs::mfs::create_blank_mfs(size, name).map_err(|e| anyhow::anyhow!("{e}"))
        }),
        FsKind::Adfs => format_and_write(&args.image, &args.size, &args.name, |_size, name| {
            Ok(crate::fs::adfs::create_blank_adfs(name))
        }),
    }
}

fn write_blank_ucsd_image(image: &std::path::Path, size_str: &str, name: &str) -> Result<()> {
    let size = parse_size(size_str).context("parsing --size")?;
    let img = crate::fs::ucsd::create_blank_ucsd(size, name)
        .map_err(|e| anyhow::anyhow!("formatting UCSD p-System: {e}"))?;
    std::fs::write(image, &img).with_context(|| format!("writing {}", image.display()))?;
    log_stderr(format!(
        "wrote {} ({} bytes, UCSD p-System volume)",
        image.display(),
        img.len()
    ));
    Ok(())
}

fn write_blank_trdos_image(image: &std::path::Path, size_str: &str, name: &str) -> Result<()> {
    let size = parse_size(size_str).context("parsing --size")?;
    let img = crate::fs::trdos::create_blank_trdos(size, name)
        .map_err(|e| anyhow::anyhow!("formatting TR-DOS: {e}"))?;
    std::fs::write(image, &img).with_context(|| format!("writing {}", image.display()))?;
    log_stderr(format!(
        "wrote {} ({} bytes, TR-DOS volume)",
        image.display(),
        img.len()
    ));
    Ok(())
}

fn write_blank_ti99_image(image: &std::path::Path, size_str: &str, name: &str) -> Result<()> {
    let size = parse_size(size_str).context("parsing --size")?;
    let img = crate::fs::ti99::create_blank_ti99(size, name)
        .map_err(|e| anyhow::anyhow!("formatting TI-99: {e}"))?;
    std::fs::write(image, &img).with_context(|| format!("writing {}", image.display()))?;
    log_stderr(format!(
        "wrote {} ({} bytes, TI-99 volume)",
        image.display(),
        img.len()
    ));
    Ok(())
}

fn write_blank_minix_image(
    image: &std::path::Path,
    size_str: &str,
    version: crate::fs::minix::MinixVersion,
) -> Result<()> {
    let size = parse_size(size_str).context("parsing --size")?;
    let img = crate::fs::minix::create_blank_minix(size, version)
        .map_err(|e| anyhow::anyhow!("formatting Minix: {e}"))?;
    std::fs::write(image, &img).with_context(|| format!("writing {}", image.display()))?;
    log_stderr(format!(
        "wrote {} ({} bytes, Minix {:?})",
        image.display(),
        img.len(),
        version
    ));
    Ok(())
}

fn format_and_write(
    image: &std::path::Path,
    size_str: &str,
    name: &str,
    formatter: impl FnOnce(u64, &str) -> anyhow::Result<Vec<u8>>,
) -> Result<()> {
    let size = parse_size(size_str).context("parsing --size")?;
    let bytes = formatter(size, name)?;
    std::fs::write(image, &bytes).with_context(|| format!("writing {}", image.display()))?;
    log_stderr(format!(
        "wrote {} ({} bytes, volume {:?})",
        image.display(),
        bytes.len(),
        name
    ));
    Ok(())
}

/// Coerce a free-form `--name` into a valid OS-9 volume name: keep only
/// `A-Z a-z 0-9 . _ $`, cap at 29 chars, and guarantee a leading letter (the
/// generic default `rusty-backup` becomes `rustybackup`; a digit-leading or
/// empty result is prefixed with `OS9`).
fn sanitize_os9_volume_name(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '$'))
        .take(29)
        .collect();
    match cleaned.chars().next() {
        Some(c) if c.is_ascii_alphabetic() || c == '.' => cleaned,
        _ => format!("OS9{cleaned}").chars().take(29).collect(),
    }
}

/// Format a bare NTFS superfloppy straight into the target file. `create_ntfs`
/// seeks and writes only the populated regions (boot, MFT, metadata, backup
/// boot at the last sector), so the bulk of the volume stays sparse rather than
/// being materialized in RAM.
fn write_blank_ntfs_image(
    image: &std::path::Path,
    total_size: u64,
    geometry: NtfsGeometry,
    mft_records_hint: u64,
    name: &str,
) -> Result<()> {
    let mut file =
        std::fs::File::create(image).with_context(|| format!("creating {}", image.display()))?;
    create_ntfs(
        &mut file,
        &NtfsFormatParams {
            total_size,
            geometry,
            mft_records_hint,
            label: Some(name.to_string()),
        },
    )
    .with_context(|| format!("formatting NTFS into {}", image.display()))?;
    // Ensure the file is exactly the requested size even though the trailing
    // clusters are sparse.
    file.set_len(total_size)
        .with_context(|| format!("sizing {}", image.display()))?;
    log_stderr(format!(
        "wrote {} ({total_size} bytes, NTFS cluster={} sector={}, volume {:?})",
        image.display(),
        geometry.cluster_size(),
        geometry.bytes_per_sector,
        name
    ));
    Ok(())
}

/// Format a bare ext2/ext3 superfloppy by streaming only its populated metadata
/// regions to the output file (the rest stays sparse). Returns the exact
/// formatted length, which may be slightly under `--size` when a trailing runt
/// block group is dropped.
fn write_blank_ext_image(
    image: &std::path::Path,
    size_str: &str,
    name: &str,
    kind: &str,
) -> Result<()> {
    let size = parse_size(size_str).context("parsing --size")?;
    let mut file =
        std::fs::File::create(image).with_context(|| format!("creating {}", image.display()))?;
    let disk_bytes = match kind {
        "ext3" => crate::fs::ext_format::write_blank_ext3(&mut file, 0, size, name),
        "ext4" => crate::fs::ext_format::write_blank_ext4(&mut file, 0, size, name),
        _ => crate::fs::ext_format::write_blank_ext2(&mut file, 0, size, name),
    }
    .with_context(|| format!("formatting {kind} into {}", image.display()))?;
    // Extend to the exact formatted length; the trailing region stays sparse.
    file.set_len(disk_bytes)
        .with_context(|| format!("sizing {}", image.display()))?;
    log_stderr(format!(
        "wrote {} ({disk_bytes} bytes, {kind}, volume {:?})",
        image.display(),
        name
    ));
    Ok(())
}

/// Format a bare XFS superfloppy by streaming only its metadata regions to the
/// output file; the log body and the free space past them stay sparse. The
/// formatted length can be a touch under `--size` when the trailing partial
/// allocation group is dropped.
fn write_blank_xfs_image(image: &std::path::Path, size_str: &str, name: &str) -> Result<()> {
    let size = parse_size(size_str).context("parsing --size")?;
    let mut file =
        std::fs::File::create(image).with_context(|| format!("creating {}", image.display()))?;
    let disk_bytes = crate::fs::xfs::format::write_blank_xfs(&mut file, 0, size, name)
        .with_context(|| format!("formatting XFS into {}", image.display()))?;
    file.set_len(disk_bytes)
        .with_context(|| format!("sizing {}", image.display()))?;
    log_stderr(format!(
        "wrote {} ({disk_bytes} bytes, XFS v5, volume {:?})",
        image.display(),
        name
    ));
    Ok(())
}

/// Format a bare EFS superfloppy by streaming only its non-zero regions to the
/// output file (the rest stays sparse), so large volumes never materialize the
/// whole image in memory. `bytes_per_inode` sets the inode density.
fn write_blank_efs_image(
    image: &std::path::Path,
    size_str: &str,
    name: &str,
    bytes_per_inode: u64,
) -> Result<()> {
    let size = parse_size(size_str).context("parsing --size")?;
    if size < 32 * 1024 {
        anyhow::bail!("EFS volume must be at least 32 KiB, got {size}");
    }
    let total_blocks_u64 = size / 512;
    if total_blocks_u64 > u32::MAX as u64 {
        anyhow::bail!("EFS volume size {size} exceeds u32 block range");
    }
    let total_blocks = total_blocks_u64 as u32;
    let disk_bytes = total_blocks as u64 * 512;

    let mut file =
        std::fs::File::create(image).with_context(|| format!("creating {}", image.display()))?;
    crate::fs::efs::write_blank_efs(&mut file, 0, total_blocks, name, bytes_per_inode)
        .with_context(|| format!("writing {}", image.display()))?;
    // Ensure the file is exactly the requested size even though the trailing
    // blocks past the replica superblock are sparse.
    file.set_len(disk_bytes)
        .with_context(|| format!("sizing {}", image.display()))?;
    log_stderr(format!(
        "wrote {} ({disk_bytes} bytes, volume {:?})",
        image.display(),
        name
    ));
    Ok(())
}
