//! Format a blank filesystem: the one dispatch every "make a new volume" path goes through.
//!
//! `rb-cli new floppy / volume`, Build Disk's formatted rows and the TUI wizard all name a
//! [`FsKind`] plus [`FormatOptions`] and call [`format_to_file`] or [`format_into`]. The
//! per-filesystem limits the wizard checks before it offers anything live in [`FsKind::caps`].
//!
//! Every formatter writes from position 0 of the writer it is handed and assumes the bytes it
//! does not write already read as zero (a fresh or sparse file). The streaming ones
//! ([`FsCaps::streams`]) write only their metadata; the rest build the volume in memory and
//! write its non-zero 1 MiB runs. Formatting at an offset inside a larger disk goes through
//! [`Window`].

use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};

/// The on-disk NTFS version a format writes (1.2 for Windows NT 3.5 to 4.0, 3.1 for XP and later).
pub use fs_pc::ntfs_format::NtfsVersion;

/// The default volume label every `new` path shares.
pub const DEFAULT_VOLUME_NAME: &str = "rusty-backup";

/// ProDOS forbids `-`, so it gets a default of its own that keeps the same name.
pub const PRODOS_DEFAULT_VOLUME_NAME: &str = "RUSTY.BACKUP";

/// The largest volume built in memory: only true floppy formats may (CONTRIBUTING.md § Streaming rule).
pub const IN_RAM_LIMIT: u64 = 3 * 1024 * 1024;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;
const TIB: u64 = 1024 * GIB;

/// Every filesystem the engine can format blank.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FsKind {
    Hfs,
    Hfsplus,
    Hfv,
    Fat,
    Efs,
    EfsV1,
    Xfs,
    Affs,
    Pfs3,
    Sfs,
    Ntfs,
    Hpfs,
    Ext,
    Ext3,
    Ext4,
    Prodos,
    Atari,
    AppleDos,
    Cpm,
    Os9,
    Minix,
    Minix2,
    Minix3,
    Ucsd,
    Trdos,
    Ti99,
    Oric,
    Bfs,
    Ofs,
    Mfs,
    Adfs,
    Ufs,
    Ufs43,
    UfsSun,
    Exfat,
    Cbm,
    Dfs,
    Rsdos,
    DragonDos,
}

/// A per-filesystem option a front-end may show; [`FsCaps::options`] lists the ones that apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsOption {
    BlockSize,
    BigEndian,
    BtreeSizes,
    CaseSensitive,
    MinCatalog,
    AffsVariant,
    CpmPreset,
    /// A fixed geometry from [`geometries`] (CBM drive, DFS / RS-DOS / DragonDOS tracks).
    Geometry,
    Inodes,
    BytesPerInode,
    ClusterSize,
    SectorSize,
    /// NTFS 1.2 or 3.1.
    NtfsVersion,
}

/// What a filesystem's formatter can do, for the wizard and the size checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsCaps {
    /// Smallest size the formatter accepts, in bytes.
    pub min_size: u64,
    /// Largest size it accepts (its on-disk limit, or the one we offer).
    pub max_size: u64,
    /// The geometry is fixed and the size is ignored (an Atari or DOS 3.3 floppy).
    pub fixed_size: bool,
    /// Writes only its metadata through `Write + Seek`; otherwise it builds the volume in RAM.
    pub streams: bool,
    /// Longest label in bytes; 0 when the filesystem has no label.
    pub label_max: usize,
    /// The options that change its output.
    pub options: &'static [FsOption],
    /// Offered by `new floppy` (floppy-sized) as well as, or instead of, `new volume`.
    pub floppy: bool,
    /// Offered by `new volume`.
    pub volume: bool,
}

impl FsCaps {
    /// The largest size Build Disk offers: the on-disk limit, capped at [`IN_RAM_LIMIT`] when
    /// the formatter builds in memory.
    pub fn max_buildable(&self) -> u64 {
        if self.streams {
            self.max_size
        } else {
            self.max_size.min(IN_RAM_LIMIT.max(self.min_size))
        }
    }
}

impl FsKind {
    pub const ALL: [FsKind; 39] = [
        FsKind::Hfs,
        FsKind::Hfsplus,
        FsKind::Hfv,
        FsKind::Fat,
        FsKind::Efs,
        FsKind::EfsV1,
        FsKind::Xfs,
        FsKind::Affs,
        FsKind::Pfs3,
        FsKind::Sfs,
        FsKind::Ntfs,
        FsKind::Hpfs,
        FsKind::Ext,
        FsKind::Ext3,
        FsKind::Ext4,
        FsKind::Prodos,
        FsKind::Atari,
        FsKind::AppleDos,
        FsKind::Cpm,
        FsKind::Os9,
        FsKind::Minix,
        FsKind::Minix2,
        FsKind::Minix3,
        FsKind::Ucsd,
        FsKind::Trdos,
        FsKind::Ti99,
        FsKind::Oric,
        FsKind::Bfs,
        FsKind::Ofs,
        FsKind::Mfs,
        FsKind::Adfs,
        FsKind::Ufs,
        FsKind::Ufs43,
        FsKind::UfsSun,
        FsKind::Exfat,
        FsKind::Cbm,
        FsKind::Dfs,
        FsKind::Rsdos,
        FsKind::DragonDos,
    ];

    /// The stable id, which is also the CLI's value name.
    pub fn id(self) -> &'static str {
        match self {
            FsKind::Hfs => "hfs",
            FsKind::Hfsplus => "hfsplus",
            FsKind::Hfv => "hfv",
            FsKind::Fat => "fat",
            FsKind::Efs => "efs",
            FsKind::EfsV1 => "efs-v1",
            FsKind::Xfs => "xfs",
            FsKind::Affs => "affs",
            FsKind::Pfs3 => "pfs3",
            FsKind::Sfs => "sfs",
            FsKind::Ntfs => "ntfs",
            FsKind::Hpfs => "hpfs",
            FsKind::Ext => "ext",
            FsKind::Ext3 => "ext3",
            FsKind::Ext4 => "ext4",
            FsKind::Prodos => "prodos",
            FsKind::Atari => "atari",
            FsKind::AppleDos => "apple-dos",
            FsKind::Cpm => "cpm",
            FsKind::Os9 => "os9",
            FsKind::Minix => "minix",
            FsKind::Minix2 => "minix2",
            FsKind::Minix3 => "minix3",
            FsKind::Ucsd => "ucsd",
            FsKind::Trdos => "trdos",
            FsKind::Ti99 => "ti99",
            FsKind::Oric => "oric",
            FsKind::Bfs => "bfs",
            FsKind::Ofs => "ofs",
            FsKind::Mfs => "mfs",
            FsKind::Adfs => "adfs",
            FsKind::Ufs => "ufs",
            FsKind::Ufs43 => "ufs-43bsd",
            FsKind::UfsSun => "ufs-solaris",
            FsKind::Exfat => "exfat",
            FsKind::Cbm => "cbm",
            FsKind::Dfs => "dfs",
            FsKind::Rsdos => "rsdos",
            FsKind::DragonDos => "dragondos",
        }
    }

    /// The kind whose [`FsKind::id`] is `id`.
    pub fn from_id(id: &str) -> Option<FsKind> {
        FsKind::ALL.iter().copied().find(|k| k.id() == id)
    }

    /// The name a front-end shows.
    pub fn display_name(self) -> &'static str {
        match self {
            FsKind::Hfs => "HFS (Mac OS Standard)",
            FsKind::Hfsplus => "HFS+ (Mac OS Extended)",
            FsKind::Hfv => "HFV (BasiliskII flat HFS)",
            FsKind::Fat => "FAT12 / FAT16 / FAT32",
            FsKind::Efs => "EFS (IRIX)",
            FsKind::EfsV1 => "EFS v1 (IRIS 2000 / 3000)",
            FsKind::Xfs => "XFS",
            FsKind::Affs => "Amiga FFS / OFS",
            FsKind::Pfs3 => "Amiga PFS3",
            FsKind::Sfs => "Amiga SFS",
            FsKind::Ntfs => "NTFS",
            FsKind::Hpfs => "HPFS (OS/2)",
            FsKind::Ext => "ext2",
            FsKind::Ext3 => "ext3",
            FsKind::Ext4 => "ext4",
            FsKind::Prodos => "ProDOS",
            FsKind::Atari => "Atari DOS 2.0S",
            FsKind::AppleDos => "Apple DOS 3.3",
            FsKind::Cpm => "CP/M",
            FsKind::Os9 => "OS-9 RBF",
            FsKind::Minix => "Minix V1",
            FsKind::Minix2 => "Minix V2",
            FsKind::Minix3 => "Minix V3",
            FsKind::Ucsd => "UCSD p-System",
            FsKind::Trdos => "TR-DOS",
            FsKind::Ti99 => "TI-99/4A",
            FsKind::Oric => "Oric Jasmin",
            FsKind::Bfs => "BFS (BeOS / Haiku)",
            FsKind::Ofs => "BeOS OFS",
            FsKind::Mfs => "MFS (Macintosh File System)",
            FsKind::Adfs => "Acorn ADFS E",
            FsKind::Ufs => "UFS1 (4.4BSD)",
            FsKind::Ufs43 => "UFS1 (4.3BSD / NeXTSTEP)",
            FsKind::UfsSun => "UFS1 (Solaris)",
            FsKind::Exfat => "exFAT",
            FsKind::Cbm => "Commodore DOS (1541 / 1571 / 1581 / 8050)",
            FsKind::Dfs => "Acorn DFS (BBC Micro)",
            FsKind::Rsdos => "RS-DOS (TRS-80 Color Computer)",
            FsKind::DragonDos => "DragonDOS (Dragon 32 / 64)",
        }
    }

    /// The label used when the caller gives none.
    pub fn default_label(self) -> &'static str {
        match self {
            FsKind::Prodos => PRODOS_DEFAULT_VOLUME_NAME,
            _ => DEFAULT_VOLUME_NAME,
        }
    }

    /// The formatter's limits and options.
    pub fn caps(self) -> FsCaps {
        use FsOption::*;
        let c = |min: u64, max: u64, streams: bool, label_max: usize| FsCaps {
            min_size: min,
            max_size: max,
            fixed_size: false,
            streams,
            label_max,
            options: &[],
            floppy: false,
            volume: true,
        };
        let fixed = |size: u64, label_max: usize| FsCaps {
            min_size: size,
            max_size: size,
            fixed_size: true,
            streams: false,
            label_max,
            options: &[],
            floppy: true,
            volume: false,
        };
        match self {
            FsKind::Hfs => FsCaps {
                options: &[BlockSize, BtreeSizes],
                floppy: true,
                ..c(400 * KIB, fs_mac::hfs::HFS_MAX_VOLUME_BYTES, true, 27)
            },
            FsKind::Hfsplus => FsCaps {
                options: &[BlockSize, CaseSensitive, MinCatalog],
                ..c(MIB, 2 * TIB, true, 255)
            },
            FsKind::Hfv => FsCaps {
                options: &[BlockSize],
                ..c(400 * KIB, fs_mac::hfv::HFV_MAX_BYTES, true, 27)
            },
            FsKind::Fat => FsCaps {
                floppy: true,
                ..c(64 * KIB, 2 * TIB, true, 11)
            },
            FsKind::Efs => FsCaps {
                options: &[Inodes, BytesPerInode],
                ..c(32 * KIB, 8 * GIB, true, 6)
            },
            FsKind::EfsV1 => c(MIB, 2 * GIB, true, 6),
            FsKind::Xfs => c(32 * MIB, 16 * TIB, true, 12),
            FsKind::Affs => FsCaps {
                options: &[AffsVariant],
                ..c(32 * KIB, 2 * TIB, true, 30)
            },
            FsKind::Pfs3 => c(MIB, 2 * TIB, true, 30),
            FsKind::Sfs => c(32 * KIB, 2 * TIB, true, 30),
            FsKind::Ntfs => FsCaps {
                options: &[NtfsVersion, ClusterSize, SectorSize],
                ..c(2 * MIB, 16 * TIB, true, 32)
            },
            FsKind::Hpfs => c(MIB, 64 * GIB, true, 11),
            FsKind::Ext => c(256 * KIB, 16 * TIB, true, 16),
            FsKind::Ext3 | FsKind::Ext4 => c(8 * MIB, 16 * TIB, true, 16),
            FsKind::Prodos => c(8 * KIB, 32 * MIB, true, 15),
            FsKind::Atari => fixed(92_160, 0),
            FsKind::AppleDos => fixed(143_360, 0),
            FsKind::Cpm => FsCaps {
                options: &[CpmPreset],
                min_size: 0,
                max_size: 0,
                streams: true,
                ..fixed(0, 0)
            },
            FsKind::Os9 => fixed(161_280, 29),
            FsKind::Minix => FsCaps {
                floppy: true,
                volume: false,
                ..c(64 * KIB, 65_535 * KIB, true, 0)
            },
            FsKind::Minix2 | FsKind::Minix3 => c(64 * KIB, 4 * GIB, true, 0),
            FsKind::Ucsd => FsCaps {
                floppy: true,
                volume: false,
                ..c(4 * KIB, 65_535 * 512, true, 7)
            },
            FsKind::Trdos => FsCaps {
                floppy: true,
                volume: false,
                ..c(KIB, 640 * KIB, false, 8)
            },
            FsKind::Ti99 => FsCaps {
                floppy: true,
                volume: false,
                ..c(KIB, 360 * KIB, false, 10)
            },
            FsKind::Oric => fixed(178_432, 8),
            FsKind::Bfs => FsCaps {
                options: &[BlockSize, BigEndian],
                ..c(72 * MIB, 2 * TIB, true, 31)
            },
            FsKind::Ofs => c(64 * KIB, 2 * TIB, true, 31),
            FsKind::Mfs => FsCaps {
                floppy: true,
                volume: false,
                ..c(400 * KIB, 800 * KIB, false, 27)
            },
            FsKind::Adfs => fixed(819_200, 10),
            FsKind::Ufs => FsCaps {
                options: &[BlockSize, BigEndian, BytesPerInode],
                ..c(MIB, TIB, true, 32)
            },
            FsKind::Ufs43 | FsKind::UfsSun => FsCaps {
                options: &[BlockSize, BigEndian, BytesPerInode],
                ..c(MIB, TIB, true, 0)
            },
            FsKind::Exfat => c(MIB, 16 * TIB, true, 11),
            FsKind::Cbm => FsCaps {
                options: &[Geometry],
                min_size: 0,
                max_size: 0,
                ..fixed(0, 16)
            },
            FsKind::Dfs => FsCaps {
                options: &[Geometry],
                min_size: 0,
                max_size: 0,
                ..fixed(0, 12)
            },
            FsKind::Rsdos | FsKind::DragonDos => FsCaps {
                options: &[Geometry],
                min_size: 0,
                max_size: 0,
                ..fixed(0, 0)
            },
        }
    }
}

/// The options a format takes beyond the kind and size; [`Default`] is every formatter's default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatOptions {
    pub label: String,
    /// HFS / HFS+ allocation block, BFS block, or UFS `fs_bsize`.
    pub block_size: Option<u32>,
    /// BFS and UFS byte order (4.3BSD and Solaris UFS are always big-endian).
    pub big_endian: bool,
    /// HFS catalog and extents B-tree sizes in bytes.
    pub catalog_bytes: Option<u32>,
    pub extents_bytes: Option<u32>,
    /// HFS+: format HFSX.
    pub case_sensitive: bool,
    /// HFS+: catalog B-tree floor in bytes.
    pub min_catalog: Option<u32>,
    /// FAT: FAT32 whatever the size.
    pub fat32: bool,
    /// AFFS DosType variant byte (0..=5).
    pub affs_variant: u8,
    /// CP/M disk-parameter-block preset name.
    pub cpm_preset: Option<String>,
    /// A geometry id from [`geometries`]; `None` is the kind's first.
    pub geometry: Option<String>,
    /// EFS approximate inode count.
    pub inodes: Option<u64>,
    /// EFS and UFS inode density.
    pub bytes_per_inode: Option<u64>,
    /// NTFS cluster and sector sizes.
    pub cluster_bytes: Option<u32>,
    pub sector_size: Option<u32>,
    /// NTFS on-disk version; `None` is 3.1.
    pub ntfs_version: Option<NtfsVersion>,
}

impl Default for FormatOptions {
    fn default() -> Self {
        FormatOptions {
            label: DEFAULT_VOLUME_NAME.to_string(),
            block_size: None,
            big_endian: false,
            catalog_bytes: None,
            extents_bytes: None,
            case_sensitive: false,
            min_catalog: None,
            fat32: false,
            affs_variant: 1,
            cpm_preset: None,
            geometry: None,
            inodes: None,
            bytes_per_inode: None,
            cluster_bytes: None,
            sector_size: None,
            ntfs_version: None,
        }
    }
}

/// What a format produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Formatted {
    /// The volume's length; can be under the requested size when the fs rounds down.
    pub len: u64,
    /// One line describing the volume, for a log.
    pub summary: String,
    /// Notes worth showing (a size that was rounded, for example).
    pub notes: Vec<String>,
}

/// Refuse options that do not apply to `kind`, before anything is written.
pub fn check_options(kind: FsKind, opts: &FormatOptions) -> Result<()> {
    let has = |o: FsOption| kind.caps().options.contains(&o);
    if opts.inodes.is_some() && !has(FsOption::Inodes) {
        bail!("the inode count option only applies to EFS");
    }
    if opts.bytes_per_inode.is_some() && !has(FsOption::BytesPerInode) {
        bail!("the bytes-per-inode option only applies to EFS and UFS");
    }
    if opts.inodes.is_some() && opts.bytes_per_inode.is_some() {
        bail!("give an inode count or a bytes-per-inode density, not both");
    }
    if (opts.cluster_bytes.is_some() || opts.sector_size.is_some()) && kind != FsKind::Ntfs {
        bail!("the cluster and sector size options only apply to NTFS");
    }
    if opts.ntfs_version.is_some() && kind != FsKind::Ntfs {
        bail!("the NTFS version option only applies to NTFS");
    }
    if opts.ntfs_version == Some(NtfsVersion::V1_2) && opts.sector_size.is_some_and(|s| s != 512) {
        bail!("NTFS 1.2 (Windows NT 3.5 to 4.0) uses 512-byte sectors only");
    }
    if (opts.case_sensitive || opts.min_catalog.is_some()) && kind != FsKind::Hfsplus {
        bail!("the case-sensitive and minimum-catalog options only apply to HFS+");
    }
    if opts.cpm_preset.is_some() && kind != FsKind::Cpm {
        bail!("a CP/M preset only applies to CP/M");
    }
    if kind == FsKind::Cpm {
        cpm_dpb(opts)?;
    }
    if opts.geometry.is_some() && !has(FsOption::Geometry) {
        bail!("a geometry choice only applies to the fixed-geometry floppy formats");
    }
    if has(FsOption::Geometry) {
        geometry_id(kind, opts)?;
    }
    Ok(())
}

/// The fixed geometries a kind with [`FsOption::Geometry`] offers: `(id, label)`, default first.
pub fn geometries(kind: FsKind) -> &'static [(&'static str, &'static str)] {
    match kind {
        FsKind::Cbm => &[
            ("d64", "1541, 35 tracks (D64, 170 KB)"),
            ("d64-40", "1541, 40 tracks (D64, 192 KB)"),
            ("d71", "1571 (D71, 340 KB)"),
            ("d81", "1581 (D81, 800 KB)"),
            ("d80", "8050 (D80, 520 KB)"),
            ("d82", "8250 (D82, 1 MB)"),
        ],
        FsKind::Dfs => &[
            ("40", "40 tracks, single-sided (100 KB)"),
            ("80", "80 tracks, single-sided (200 KB)"),
        ],
        FsKind::Rsdos => &[
            ("35", "35 tracks (157 KB)"),
            ("40", "40 tracks (180 KB)"),
            ("80", "80 tracks (360 KB)"),
        ],
        FsKind::DragonDos => &[
            ("40ss", "40 tracks, single-sided (180 KB)"),
            ("40ds", "40 tracks, double-sided (360 KB)"),
            ("80ss", "80 tracks, single-sided (360 KB)"),
            ("80ds", "80 tracks, double-sided (720 KB)"),
        ],
        _ => &[],
    }
}

/// The geometry id `opts` names for `kind`, or its default.
fn geometry_id(kind: FsKind, opts: &FormatOptions) -> Result<&'static str> {
    let all = geometries(kind);
    match opts.geometry.as_deref() {
        None => Ok(all[0].0),
        Some(want) => all.iter().map(|g| g.0).find(|g| *g == want).ok_or_else(|| {
            let ids: Vec<&str> = all.iter().map(|g| g.0).collect();
            anyhow!(
                "unknown {} geometry '{want}'; one of: {}",
                kind.display_name(),
                ids.join(", ")
            )
        }),
    }
}

/// Format `kind` into a new file at `path`, which ends up exactly the volume's length.
pub fn format_to_file(
    path: &Path,
    size: u64,
    kind: FsKind,
    opts: &FormatOptions,
) -> Result<Formatted> {
    check_options(kind, opts)?;
    let mut file =
        std::fs::File::create(path).with_context(|| format!("creating {}", path.display()))?;
    // UFS sizes the file first, as `newfs` sees a disk of that length.
    if matches!(kind, FsKind::Ufs | FsKind::Ufs43 | FsKind::UfsSun) {
        file.set_len(size)
            .with_context(|| format!("sizing {}", path.display()))?;
    }
    let done = match format_into(&mut file, size, kind, opts) {
        Ok(d) => d,
        Err(e) => {
            drop(file);
            let _ = std::fs::remove_file(path);
            return Err(e)
                .with_context(|| format!("formatting {} into {}", kind.id(), path.display()));
        }
    };
    file.set_len(done.len)
        .with_context(|| format!("sizing {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("syncing {}", path.display()))?;
    Ok(done)
}

/// Format `kind` from position 0 of `w`, whose unwritten bytes must already read as zero.
pub fn format_into<W: Write + Seek>(
    w: &mut W,
    size: u64,
    kind: FsKind,
    opts: &FormatOptions,
) -> Result<Formatted> {
    let mut made = format_volume(w, size, kind, opts)?;
    if let Some(facts) = crate::model::fs_limits::VolumeFacts::of_kind(kind, size, opts) {
        let caveats = crate::model::fs_limits::caveats(&facts, size);
        made.notes.extend(caveats.into_iter().map(|c| c.text));
    }
    Ok(made)
}

fn format_volume<W: Write + Seek>(
    w: &mut W,
    size: u64,
    kind: FsKind,
    opts: &FormatOptions,
) -> Result<Formatted> {
    check_options(kind, opts)?;
    let label = opts.label.as_str();
    let done = |len: u64, what: String| Formatted {
        len,
        summary: what,
        notes: Vec::new(),
    };
    match kind {
        FsKind::Hfs => {
            if size < 1024 {
                bail!("size {size} is too small for an HFS volume");
            }
            let bs = hfs_block_size(opts.block_size, size)?;
            let (cat, ext) = fs_mac::hfs::default_btree_sizes(size, bs);
            let cat = opts.catalog_bytes.unwrap_or(cat);
            let ext = opts.extents_bytes.unwrap_or(ext);
            let len = fs_mac::hfs::write_blank_hfs_sized_into(w, 0, size, bs, label, ext, cat)
                .map_err(|e| anyhow!("failed to format HFS volume: {e}"))?;
            Ok(done(len, format!("HFS, block size {bs}, volume {label:?}")))
        }
        FsKind::Hfsplus => {
            let bs = match opts.block_size {
                Some(bs) if !bs.is_power_of_two() || !(512..=4096).contains(&bs) => {
                    bail!("HFS+ block size must be a power of two in [512, 4096] (got {bs})")
                }
                Some(bs) => bs,
                None => 4096,
            };
            if size < u64::from(bs) * 64 {
                bail!(
                    "size {size} is too small for an HFS+ volume (need at least {} bytes for 64 \
                     allocation blocks of {bs})",
                    u64::from(bs) * 64
                );
            }
            let len = fs_mac::hfsplus::write_blank_hfsplus_sized_into(
                w,
                0,
                size,
                bs,
                label,
                opts.case_sensitive,
                opts.min_catalog.unwrap_or(0),
                0,
            )?;
            let what = if opts.case_sensitive { "HFSX" } else { "HFS+" };
            Ok(done(
                len,
                format!("{what}, block size {bs}, volume {label:?}"),
            ))
        }
        FsKind::Hfv => {
            fs_mac::hfv::validate_hfv_target(size, fs_mac::hfv::HfvVolumeKind::Hfs)?;
            let bs = opts
                .block_size
                .unwrap_or_else(|| fs_mac::hfv::suggest_block_size(size));
            let len = fs_mac::hfs::write_blank_hfs_into(w, 0, size, bs, label)?;
            Ok(done(len, format!("HFV, block size {bs}, volume {label:?}")))
        }
        FsKind::Fat => {
            let layout = fs_pc::fat::compute_fat_blank_layout_forced(size, 512, opts.fat32)?;
            fs_pc::fat::write_blank_fat_metadata_to_sink(w, &layout, Some(label))?;
            Ok(done(
                layout.image_size(),
                format!("{:?}, volume {label:?}", layout.fat_type),
            ))
        }
        FsKind::Efs => {
            if size < 32 * KIB {
                bail!("EFS volume must be at least 32 KiB, got {size}");
            }
            let blocks = u32::try_from(size / 512)
                .map_err(|_| anyhow!("EFS volume size {size} exceeds u32 block range"))?;
            let bpi =
                fs_unix::efs::resolve_bytes_per_inode(size, opts.inodes, opts.bytes_per_inode);
            fs_unix::efs::write_blank_efs(w, 0, blocks, label, bpi)?;
            Ok(done(
                u64::from(blocks) * 512,
                format!("EFS, volume {label:?}"),
            ))
        }
        FsKind::EfsV1 => {
            let len = fs_unix::efs_v1::write_blank_efs_v1_into(w, 0, size, label)?;
            Ok(done(len, format!("EFS v1, volume {label:?}")))
        }
        FsKind::Xfs => {
            let len = fs_xfs::xfs::format::write_blank_xfs(w, 0, size, label)?;
            Ok(done(len, format!("XFS v5, volume {label:?}")))
        }
        FsKind::Affs => {
            let len = fs_amiga::affs::write_blank_affs_into(w, 0, size, opts.affs_variant, label)?;
            Ok(done(
                len,
                format!("AFFS DOS\\{}, volume {label:?}", opts.affs_variant),
            ))
        }
        FsKind::Pfs3 => {
            let len =
                fs_amiga::pfs3::write_blank_pfs3_into(w, 0, blocks_512(size, "PFS3")?, label)?;
            Ok(done(len, format!("PFS3, volume {label:?}")))
        }
        FsKind::Sfs => {
            let len = fs_amiga::sfs::write_blank_sfs_into(w, 0, blocks_512(size, "SFS")?, label)?;
            Ok(done(len, format!("SFS, volume {label:?}")))
        }
        FsKind::Ntfs => format_ntfs(w, size, opts),
        FsKind::Hpfs => {
            let len = fs_hpfs::hpfs::write_blank_hpfs_into(w, 0, size, label)?;
            Ok(done(len, format!("HPFS, volume {label:?}")))
        }
        FsKind::Ext => {
            let len = fs_unix::ext_format::write_blank_ext2(w, 0, size, label)?;
            Ok(done(len, format!("ext2, volume {label:?}")))
        }
        FsKind::Ext3 => {
            let len = fs_unix::ext_format::write_blank_ext3(w, 0, size, label)?;
            Ok(done(len, format!("ext3, volume {label:?}")))
        }
        FsKind::Ext4 => {
            let len = fs_unix::ext_format::write_blank_ext4(w, 0, size, label)?;
            Ok(done(len, format!("ext4, volume {label:?}")))
        }
        FsKind::Prodos => {
            let len = fs_apple2::prodos::write_blank_prodos_into(w, 0, size, label)?;
            Ok(done(len, format!("ProDOS, volume {label:?}")))
        }
        FsKind::Atari => in_ram(w, "Atari DOS 2.0S", "", || {
            Ok(fs_8bit::atari_dos::create_blank_atari_sd())
        }),
        FsKind::AppleDos => in_ram(w, "Apple DOS 3.3", "", || {
            Ok(fs_apple2::apple_dos::create_blank_apple_dos())
        }),
        FsKind::Os9 => {
            let name = sanitize_os9_volume_name(label);
            in_ram(w, "OS-9", name.as_str(), || {
                Ok(fs_8bit::os9::create_blank_os9(name.as_str())?)
            })
        }
        FsKind::Cpm => {
            let dpb = cpm_dpb(opts)?;
            let len = fs_cpm::cpm::write_blank_into(w, 0, dpb)?;
            Ok(done(len, "CP/M".to_string()))
        }
        FsKind::Minix => minix(w, size, fs_unix::minix::MinixVersion::V1),
        FsKind::Minix2 => minix(w, size, fs_unix::minix::MinixVersion::V2),
        FsKind::Minix3 => minix(w, size, fs_unix::minix::MinixVersion::V3),
        FsKind::Ucsd => {
            let len = fs_apple2::ucsd::write_blank_ucsd_into(w, 0, size, label)
                .map_err(|e| anyhow!("formatting UCSD p-System: {e}"))?;
            Ok(done(len, format!("UCSD p-System, volume {label:?}")))
        }
        FsKind::Trdos => in_ram(w, "TR-DOS", label, || {
            fs_8bit::trdos::create_blank_trdos(size, label)
                .map_err(|e| anyhow!("formatting TR-DOS: {e}"))
        }),
        FsKind::Ti99 => in_ram(w, "TI-99", label, || {
            fs_8bit::ti99::create_blank_ti99(size, label)
                .map_err(|e| anyhow!("formatting TI-99: {e}"))
        }),
        FsKind::Oric => in_ram(w, "Oric Jasmin", label, || {
            Ok(fs_8bit::oric::create_blank_oric(false, label))
        }),
        FsKind::Bfs => {
            let endian = if opts.big_endian {
                fs_unix::bfs::BfsEndian::Big
            } else {
                fs_unix::bfs::BfsEndian::Little
            };
            let bs = opts.block_size.unwrap_or(1024);
            let len = fs_unix::bfs_write::write_blank_bfs_into(w, 0, size, bs, label, endian)?;
            Ok(done(len, format!("BFS, block size {bs}, volume {label:?}")))
        }
        FsKind::Ofs => {
            let len = fs_unix::ofs_write::write_blank_ofs_into(w, 0, size, label)?;
            Ok(done(len, format!("BeOS OFS, volume {label:?}")))
        }
        FsKind::Mfs => in_ram(w, "MFS", label, || {
            fs_mac::mfs::create_blank_mfs(size, label).map_err(|e| anyhow!("{e}"))
        }),
        FsKind::Adfs => in_ram(w, "ADFS E", label, || {
            Ok(fs_adfs::adfs::create_blank_adfs(label))
        }),
        FsKind::Exfat => {
            // The cluster sizes Windows picks: 4 KiB to 256 MiB, 32 KiB to 32 GiB, 128 KiB above.
            let cluster = if size <= 256 * MIB {
                4 * KIB
            } else if size <= 32 * GIB {
                32 * KIB
            } else {
                128 * KIB
            };
            let template = fs_pc::exfat::ExfatFormatTemplate {
                bytes_per_sector: 512,
                sectors_per_cluster: cluster / 512,
                label: (!label.is_empty()).then(|| label.to_string()),
            };
            let len = size / 512 * 512;
            fs_pc::exfat::create_blank_exfat(w, &template, len)?;
            Ok(done(
                len,
                format!("exFAT, cluster {cluster}, volume {label:?}"),
            ))
        }
        FsKind::Cbm => {
            use fs_8bit::cbm::CbmVariant as V;
            let variant = match geometry_id(kind, opts)? {
                "d64-40" => V::D64_40,
                "d71" => V::D71,
                "d81" => V::D81,
                "d80" => V::D80,
                "d82" => V::D82,
                _ => V::D64,
            };
            in_ram(w, "Commodore DOS", label, || {
                Ok(fs_8bit::cbm::create_blank(variant, label, "RB")?)
            })
        }
        FsKind::Dfs => {
            let sectors = if geometry_id(kind, opts)? == "80" {
                800
            } else {
                400
            };
            in_ram(w, "Acorn DFS", label, || {
                Ok(fs_8bit::dfs::create_blank_dfs(sectors, label))
            })
        }
        FsKind::Rsdos => {
            let tracks: u64 = geometry_id(kind, opts)?.parse().unwrap_or(35);
            in_ram(w, "RS-DOS", "", || Ok(fs_8bit::rsdos::create_blank(tracks)))
        }
        FsKind::DragonDos => {
            let g = geometry_id(kind, opts)?;
            let tracks = if g.starts_with("80") { 80 } else { 40 };
            let sides = if g.ends_with("ds") { 2 } else { 1 };
            in_ram(w, "DragonDOS", "", || {
                Ok(fs_8bit::dragondos::create_blank(tracks, sides))
            })
        }
        FsKind::Ufs => format_ufs(w, size, opts, fs_unix::ufs::CgLayout::Modern, false),
        FsKind::Ufs43 => format_ufs(w, size, opts, fs_unix::ufs::CgLayout::Bsd43, false),
        FsKind::UfsSun => format_ufs(w, size, opts, fs_unix::ufs::CgLayout::Modern, true),
    }
}

/// A writer that sees only `len` bytes of `inner` starting at `base`, so a formatter that writes
/// from position 0 lands inside a partition.
pub struct Window<'a, W> {
    inner: &'a mut W,
    base: u64,
    len: u64,
    pos: u64,
}

impl<'a, W: Write + Seek> Window<'a, W> {
    pub fn new(inner: &'a mut W, base: u64, len: u64) -> Self {
        Window {
            inner,
            base,
            len,
            pos: 0,
        }
    }
}

impl<W: Write + Seek> Write for Window<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let room = self.len.saturating_sub(self.pos);
        if room == 0 && !buf.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "write past the end of the partition",
            ));
        }
        let n = (buf.len() as u64).min(room) as usize;
        self.inner.seek(SeekFrom::Start(self.base + self.pos))?;
        let wrote = self.inner.write(&buf[..n])?;
        self.pos += wrote as u64;
        Ok(wrote)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<W: std::io::Read + Seek> std::io::Read for Window<'_, W> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = (buf.len() as u64).min(self.len.saturating_sub(self.pos)) as usize;
        self.inner.seek(SeekFrom::Start(self.base + self.pos))?;
        let got = self.inner.read(&mut buf[..n])?;
        self.pos += got as u64;
        Ok(got)
    }
}

impl<W: Seek> Seek for Window<'_, W> {
    fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
        let next = match to {
            SeekFrom::Start(n) => Some(n),
            SeekFrom::End(d) => self.len.checked_add_signed(d),
            SeekFrom::Current(d) => self.pos.checked_add_signed(d),
        };
        match next {
            Some(n) => {
                self.pos = n;
                Ok(n)
            }
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek before the start of the partition",
            )),
        }
    }
}

/// Write an in-memory volume's non-zero 1 MiB runs; the zero runs are left to the target.
fn in_ram<W: Write + Seek>(
    w: &mut W,
    what: &str,
    label: &str,
    build: impl FnOnce() -> Result<Vec<u8>>,
) -> Result<Formatted> {
    const RUN: usize = 1 << 20;
    let bytes = build()?;
    for (k, chunk) in bytes.chunks(RUN).enumerate() {
        if chunk.iter().all(|&b| b == 0) {
            continue;
        }
        w.seek(SeekFrom::Start((k * RUN) as u64))?;
        w.write_all(chunk)?;
    }
    let summary = if label.is_empty() {
        what.to_string()
    } else {
        format!("{what}, volume {label:?}")
    };
    Ok(Formatted {
        len: bytes.len() as u64,
        summary,
        notes: Vec::new(),
    })
}

fn minix<W: Write + Seek>(
    w: &mut W,
    size: u64,
    version: fs_unix::minix::MinixVersion,
) -> Result<Formatted> {
    let len = fs_unix::minix::write_blank_minix_into(w, 0, size, version)
        .map_err(|e| anyhow!("formatting Minix: {e}"))?;
    Ok(Formatted {
        len,
        summary: format!("Minix {version:?}"),
        notes: Vec::new(),
    })
}

fn hfs_block_size(explicit: Option<u32>, size: u64) -> Result<u32> {
    match explicit {
        Some(bs) if bs == 0 || bs % 512 != 0 => {
            bail!("block size must be a non-zero multiple of 512 (got {bs})")
        }
        Some(bs) => Ok(bs),
        None => Ok(fs_mac::hfs::pick_block_size(size)),
    }
}

fn cpm_dpb(opts: &FormatOptions) -> Result<fs_cpm::cpm_diskdefs::Dpb> {
    let name = opts
        .cpm_preset
        .as_deref()
        .ok_or_else(|| anyhow!("CP/M needs a disk-parameter preset (e.g. amstrad_data)"))?;
    fs_cpm::cpm_diskdefs::preset_by_name(name)
        .copied()
        .ok_or_else(|| {
            let valid: Vec<&str> = fs_cpm::cpm_diskdefs::ALL_PRESETS
                .iter()
                .map(|d| d.name)
                .collect();
            anyhow!(
                "unknown CP/M preset '{name}'; valid presets: {}",
                valid.join(", ")
            )
        })
}

/// 512-byte block count for the Amiga builders, refusing what their `u32` cannot hold.
fn blocks_512(size: u64, fs: &str) -> Result<u32> {
    let blocks = size / 512;
    if blocks == 0 {
        bail!("{size} bytes is smaller than one 512-byte {fs} block");
    }
    u32::try_from(blocks).map_err(|_| anyhow!("{size} bytes exceeds the {fs} 2 TiB block limit"))
}

/// Coerce a label into the OS-9 name set, with a leading letter and at most 29 chars.
pub fn sanitize_os9_volume_name(name: &str) -> String {
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

fn format_ntfs<W: Write + Seek>(w: &mut W, raw: u64, opts: &FormatOptions) -> Result<Formatted> {
    use fs_pc::ntfs_format::{create_ntfs_version, NtfsFormatParams, NtfsGeometry};
    let version = opts.ntfs_version.unwrap_or_default();
    let sector = opts.sector_size.unwrap_or(512);
    // The auto cluster follows mkntfs's default-by-size table, floored to the sector size.
    let geometry = match opts.cluster_bytes {
        Some(c) => NtfsGeometry::with_cluster_size(c, sector)?,
        None => {
            let auto = NtfsGeometry::for_volume_size(raw).cluster_size() as u32;
            NtfsGeometry::with_cluster_size(auto.max(sector), sector)?
        }
    };
    let cluster = geometry.cluster_size();
    let total = raw / cluster * cluster;
    if total == 0 {
        bail!("{raw} bytes is smaller than one {cluster}-byte NTFS cluster");
    }
    let mut notes = Vec::new();
    if total != raw {
        notes.push(format!(
            "NTFS size rounded down to {total} bytes (multiple of the {cluster}-byte cluster)"
        ));
    }
    // An explicit cluster is the user's call; only the sector size is a hard NTFS 1.2 rule.
    if let Some(problem) = version.geometry_problem(&geometry) {
        notes.push(problem);
    }
    // About one MFT record per 256 KiB, with a 128-record floor for headroom.
    let mft_records_hint = (total / (256 * KIB)).clamp(128, 1 << 20);
    create_ntfs_version(
        w,
        &NtfsFormatParams {
            total_size: total,
            geometry,
            mft_records_hint,
            label: Some(opts.label.clone()),
        },
        version,
    )?;
    Ok(Formatted {
        len: total,
        summary: format!(
            "NTFS {} cluster={cluster} sector={}, volume {:?}",
            version.label(),
            geometry.bytes_per_sector,
            opts.label
        ),
        notes,
    })
}

fn format_ufs<W: Write + Seek>(
    w: &mut W,
    size: u64,
    opts: &FormatOptions,
    cg_layout: fs_unix::ufs::CgLayout,
    sun: bool,
) -> Result<Formatted> {
    use fs_unix::ufs::{CgLayout, UfsEndian};
    use fs_unix::ufs_format::{write_blank_ufs1, Ufs1FormatParams};
    let block_size = u64::from(opts.block_size.unwrap_or(8192));
    // Every NeXT and SPARC disk is big-endian, so those two layouts always are.
    let big_endian = opts.big_endian || cg_layout == CgLayout::Bsd43 || sun;
    let params = Ufs1FormatParams {
        size_bytes: size,
        block_size,
        // FFS puts eight fragments in a block, and the reader's MAXFRAG gate matches.
        frag_size: block_size / 8,
        bytes_per_inode: opts.bytes_per_inode.unwrap_or(0),
        endian: if big_endian {
            UfsEndian::Big
        } else {
            UfsEndian::Little
        },
        cg_layout,
        // 4.3BSD keeps `fs_fsmnt` where 4.4BSD has `fs_volname`, and Solaris has no label.
        label: (cg_layout == CgLayout::Modern && !sun).then(|| opts.label.clone()),
        bsd43_geometry: None,
        sun,
    };
    let geo = write_blank_ufs1(w, &params)?;
    let what = if sun {
        "Solaris"
    } else if cg_layout == CgLayout::Bsd43 {
        "4.3BSD"
    } else {
        "4.4BSD"
    };
    Ok(Formatted {
        len: size,
        summary: format!(
            "UFS1 {what} bsize={} fsize={} {} cylinder group(s), {} inodes",
            geo.bsize,
            geo.fsize,
            geo.ncg,
            geo.ncg * geo.ipg
        ),
        notes: Vec::new(),
    })
}

#[cfg(test)]
mod tests;
