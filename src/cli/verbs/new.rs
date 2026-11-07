//! `rb-cli new` — create blank images, grouped by media class:
//!
//! * `new floppy <fs> IMG`  — a bare single-volume image at floppy geometry
//!   (the fixed-geometry retro filesystems plus small FAT/HFS).
//! * `new volume <fs> IMG`  — a bare single-volume image of arbitrary size
//!   (a "superfloppy": NTFS, ext4, HFS+, EFS, …). No partition table.
//! * `new hd {x68k|sgi-efs|mbr|gpt|apm|sgi|sgi-dklabel|rdb|sun|next|
//!   solaris-x86|atari|x68k-table} IMG` — a
//!   partition-table-wrapped, bootable
//!   hard-disk image.
//!
//! CD-ROM images live under `optical new` (e.g. `optical new sgi-efs`).
//! Multi-partition images go through the `batch` verb.
//!
//! Each class exposes only the filesystems and flags valid for it, so
//! `new floppy --help` no longer lists NTFS/ext4 and `--cpm-preset` only
//! appears where CP/M can be formatted. `floppy` and `volume` both format
//! through [`crate::model::format`], the dispatch Build Disk uses too.

use anyhow::{Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use std::path::{Path, PathBuf};

use crate::model::format::{FormatOptions, FsKind};
pub use crate::model::format::{DEFAULT_VOLUME_NAME, PRODOS_DEFAULT_VOLUME_NAME};

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;

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
    Sgi(super::new_partitioned_hd::CylinderHdArgs),

    /// SGI disk label (IRIS 2000 / 3000) -- the pre-IRIX scheme an IRIS 3130
    /// boots from, with eight slots you size and give a role (root / swap /
    /// boot / slice). Cylinder-aligned from `--heads` / `--sectors`; the
    /// slots come out empty, so format them with `new volume efs-v1` and
    /// `write --partition N`. IRIX 3.x needs a `swap` slot to boot.
    #[command(name = "sgi-dklabel")]
    SgiDklabel(super::new_partitioned_hd::CylinderHdArgs),

    /// Amiga Rigid Disk Block (RDB) with partitions you size and type
    /// yourself. Cylinder-aligned from `--heads` / `--sectors`; types are
    /// AmigaDOS DosType tags (`DOS\\3`, `PFS\\3`, `SFS\\0`, ...).
    Rdb(super::new_partitioned_hd::CylinderHdArgs),

    /// Sun disk label (SMI VTOC) for SPARC Solaris / SunOS, with slices you
    /// size and tag yourself. Cylinder-aligned from `--heads` / `--sectors`;
    /// slice 2 is reserved for the whole-disk "backup" alias. Fill a slice
    /// with a volume from `new volume ufs-solaris`; Solaris rejects a 4.4BSD `ufs`.
    Sun(super::new_partitioned_hd::CylinderHdArgs),

    /// NeXT disk label (NeXTSTEP / OPENSTEP, black m68k hardware and
    /// NeXTSTEP/Intel), with the partitions you name. Four checksummed copies
    /// at 512-byte blocks 0/15/30/45; partitions are counted in the label's
    /// own 1024-byte sectors, so `--sectors` is in those units. Fill a
    /// partition with a volume from `new volume ufs-43bsd` -- the 4.4BSD
    /// `new volume ufs` parses but NeXTSTEP cannot read its directories.
    Next(super::new_partitioned_hd::CylinderHdArgs),

    /// Solaris x86: an MBR whose one Solaris partition holds a 16-slice VTOC
    /// in its second sector, with the slice tags you name. Cylinder-aligned;
    /// slices 2 / 8 / 9 are the label's own backup, boot and alternates.
    #[command(name = "solaris-x86")]
    SolarisX86(super::new_partitioned_hd::CylinderHdArgs),

    /// Sharp X68000 SCSI/SASI table with partitions you size yourself.
    /// Unlike `x68k` this writes only the table -- no IPL stub, no Human68k
    /// formatting -- so it is a data disk, not a bootable one.
    #[command(name = "x68k-table")]
    X68kTable(super::new_partitioned_hd::PartitionedHdArgs),

    /// Atari ST / TT / Falcon AHDI disk with partitions you size and type
    /// yourself. Types are the 3-character tags GEM / BGM / RAW; a GEM
    /// partition over 16 MiB is promoted to BGM, which is what TOS needs.
    Atari(super::new_partitioned_hd::PartitionedHdArgs),
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
    pub fn to_fs_kind(self) -> FsKind {
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
    /// BasiliskII HFV — a flat classic-HFS volume, capped at 4063 MB.
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
    /// Amiga PFS3 (`PFS\3`) — a bare volume.
    Pfs3,
    /// Amiga SFS (`SFS\0`) — a bare volume, minimum 64 blocks (32 KiB).
    Sfs,
    /// ProDOS (Apple II / IIgs) — up to ~32 MiB.
    Prodos,
    /// IRIX EFS (single cylinder group) — a bare EFS superfloppy.
    Efs,
    /// SGI EFS v1 (IRIS 2000 / 3000) — a bare superfloppy, native byte order.
    #[value(name = "efs-v1")]
    EfsV1,
    /// XFS (IRIX 6 / Linux) — a bare v5/CRC volume, minimum 32 MiB.
    Xfs,
    /// Minix V2 — 32-bit zone pointers, `mkfs.minix -2`.
    Minix2,
    /// Minix V3 — 60-char names, `mkfs.minix -3`.
    Minix3,
    /// BFS (BeOS / Haiku) — a bare volume, minimum 72 MiB. `--big-endian`
    /// writes the BeOS/PPC byte order.
    #[value(alias = "befs", alias = "beos")]
    Bfs,
    /// BeOS OFS (the pre-BFS Hobbit BeBox filesystem) — big-endian, 512-byte
    /// sectors.
    #[value(alias = "beos-ofs")]
    Ofs,
    /// UFS1 / FFS (4.4BSD: FreeBSD, NetBSD, OpenBSD). `--big-endian` writes
    /// the SPARC / m68k byte order. For Solaris use `ufs-solaris`.
    #[value(alias = "ufs1", alias = "ffs")]
    Ufs,
    /// UFS1 in the pre-4.4BSD (NeXTSTEP / OPENSTEP) on-disk format, which is
    /// what a `new hd next` partition needs. Big-endian by default.
    #[value(name = "ufs-43bsd", alias = "ufs43", alias = "nextstep")]
    Ufs43,
    /// UFS1 as Solaris / SunOS 5 writes it, which is what a `new hd sun`
    /// slice needs. Big-endian by default.
    #[value(name = "ufs-solaris", alias = "solaris")]
    UfsSun,
}

impl VolumeFs {
    pub fn to_fs_kind(self) -> FsKind {
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
            VolumeFs::Pfs3 => FsKind::Pfs3,
            VolumeFs::Sfs => FsKind::Sfs,
            VolumeFs::Prodos => FsKind::Prodos,
            VolumeFs::Efs => FsKind::Efs,
            VolumeFs::EfsV1 => FsKind::EfsV1,
            VolumeFs::Xfs => FsKind::Xfs,
            VolumeFs::Minix2 => FsKind::Minix2,
            VolumeFs::Minix3 => FsKind::Minix3,
            VolumeFs::Bfs => FsKind::Bfs,
            VolumeFs::Ofs => FsKind::Ofs,
            VolumeFs::Ufs => FsKind::Ufs,
            VolumeFs::Ufs43 => FsKind::Ufs43,
            VolumeFs::UfsSun => FsKind::UfsSun,
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
    #[arg(long, default_value = DEFAULT_VOLUME_NAME)]
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
    fn format(self) -> Result<()> {
        let opts = FormatOptions {
            block_size: self.block_size,
            catalog_bytes: parse_opt_size(self.catalog_size.as_deref(), "--catalog-size")?,
            extents_bytes: parse_opt_size(self.extents_size.as_deref(), "--extents-size")?,
            fat32: self.fat32,
            cpm_preset: self.cpm_preset,
            ..FormatOptions::default()
        };
        format_cli(
            self.image.as_path(),
            self.size.as_str(),
            self.name.as_str(),
            self.fs.to_fs_kind(),
            opts,
        )
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

    /// Volume label/name. Defaults to `rusty-backup`. Ignored by the
    /// filesystems with no label field, among them 4.3BSD UFS.
    #[arg(long, default_value = DEFAULT_VOLUME_NAME)]
    pub name: String,

    /// HFS/HFS+ allocation block size in bytes (multiple of 512). On `--fs bfs`
    /// this is the BFS block size (power of two, 1024..=8192); on `--fs ufs`
    /// it is `fs_bsize`, and the fragment is an eighth of it. Auto when unset.
    #[arg(long = "block-size")]
    pub block_size: Option<u32>,

    /// BFS and UFS: write the big-endian byte order (BeOS/PPC; SPARC / m68k /
    /// MIPS) instead of the little-endian default. Implied by `ufs-43bsd`
    /// and `ufs-solaris`, the byte order of every NeXT and SPARC disk.
    #[arg(long = "big-endian")]
    pub big_endian: bool,

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

    /// EFS and UFS: inode density in bytes per inode (smaller = more inodes).
    /// UFS defaults to one inode per 4 fragments, as `newfs -i` does.
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
    fn format(self) -> Result<()> {
        let opts = FormatOptions {
            block_size: self.block_size,
            big_endian: self.big_endian,
            catalog_bytes: parse_opt_size(self.catalog_size.as_deref(), "--catalog-size")?,
            extents_bytes: parse_opt_size(self.extents_size.as_deref(), "--extents-size")?,
            case_sensitive: self.case_sensitive,
            min_catalog: parse_opt_size(self.min_catalog.as_deref(), "--min-catalog")?,
            fat32: self.fat32,
            affs_variant: self.affs_variant,
            inodes: self.inodes,
            bytes_per_inode: self.bytes_per_inode,
            cluster_bytes: parse_opt_size(self.cluster_size.as_deref(), "--cluster-size")?,
            sector_size: self.sector_size,
            ..FormatOptions::default()
        };
        format_cli(
            self.image.as_path(),
            self.size.as_str(),
            self.name.as_str(),
            self.fs.to_fs_kind(),
            opts,
        )
    }
}

/// Dispatch a parsed `new` subcommand to its formatter.
pub fn run(cmd: NewCommand) -> Result<()> {
    match cmd {
        NewCommand::Floppy(args) => args.format(),
        NewCommand::Volume(args) => args.format(),
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
            HdCommand::SgiDklabel(args) => super::new_partitioned_hd::run(
                super::new_partitioned_hd::PartitionedHdCommand::SgiDklabel(args),
            ),
            HdCommand::Rdb(args) => super::new_partitioned_hd::run(
                super::new_partitioned_hd::PartitionedHdCommand::Rdb(args),
            ),
            HdCommand::Sun(args) => super::new_partitioned_hd::run(
                super::new_partitioned_hd::PartitionedHdCommand::Sun(args),
            ),
            HdCommand::Next(args) => super::new_partitioned_hd::run(
                super::new_partitioned_hd::PartitionedHdCommand::Next(args),
            ),
            HdCommand::SolarisX86(args) => super::new_partitioned_hd::run(
                super::new_partitioned_hd::PartitionedHdCommand::SolarisX86(args),
            ),
            HdCommand::X68kTable(args) => super::new_partitioned_hd::run(
                super::new_partitioned_hd::PartitionedHdCommand::X68k(args),
            ),
            HdCommand::Atari(args) => super::new_partitioned_hd::run(
                super::new_partitioned_hd::PartitionedHdCommand::Atari(args),
            ),
        },
    }
}

/// A size flag in bytes, clamped to `u32` as the formatters take it.
fn parse_opt_size(s: Option<&str>, flag: &str) -> Result<Option<u32>> {
    s.map(|s| parse_size(s).with_context(|| format!("parsing {flag}")))
        .transpose()
        .map(|v| v.map(|v| v.min(u32::MAX as u64) as u32))
}

/// Format through the model and report it the way every `new` verb does.
fn format_cli(
    image: &Path,
    size: &str,
    name: &str,
    kind: FsKind,
    mut opts: FormatOptions,
) -> Result<()> {
    let size = parse_size(size).context("parsing --size")?;
    // Only the untouched default is swapped (ProDOS forbids its `-`); a typed name still errors.
    opts.label = if name == DEFAULT_VOLUME_NAME {
        kind.default_label().to_string()
    } else {
        name.to_string()
    };
    let done = crate::model::format::format_to_file(image, size, kind, &opts)?;
    for note in &done.notes {
        log_stderr(format!("Note: {note}"));
    }
    log_stderr(format!(
        "wrote {} ({} bytes, {})",
        image.display(),
        done.len,
        done.summary
    ));
    Ok(())
}
