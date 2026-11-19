//! Easy mode: a ready-made disk for one system. Each target is one partition across the whole
//! disk with the table and filesystem that system reads, so the user picks a system and a size
//! and nothing else. A target builds as an ordinary partitioned disk through [`DiskBuilder`], so
//! the era's alignment, the APM driver and the size checks are the advanced wizard's own.

use super::{FsChoice, FsKind, NtfsVersion, PresetKind, TableKind};
use crate::model::disk_builder::{suggested_type, DiskBuilder, RowFormat};

const GIB: u64 = 1024 * 1024 * 1024;

/// What an easy target makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EasyDisk {
    pub table: TableKind,
    pub fs: FsChoice,
    /// The volume name it starts with.
    pub label: &'static str,
    /// The largest disk the system can use whole, and why.
    pub max: Option<(u64, &'static str)>,
    /// The image file name the Save dialog proposes.
    pub file_name: &'static str,
}

impl PresetKind {
    /// The one-partition disk this target makes; `None` for the fixed-shape presets.
    pub fn easy(self) -> Option<EasyDisk> {
        let disk = |table, fs: FsChoice, label, max, file_name| {
            Some(EasyDisk {
                table,
                fs,
                label,
                max,
                file_name,
            })
        };
        let fat32 = FsChoice {
            fat32: true,
            ..FsKind::Fat.into()
        };
        let ntfs = |v| FsChoice {
            ntfs: Some(v),
            ..FsKind::Ntfs.into()
        };
        match self {
            PresetKind::MacHfs => disk(
                TableKind::Apm,
                FsKind::Hfs.into(),
                "Macintosh HD",
                None,
                "macintosh.img",
            ),
            PresetKind::MacHfsPlus => disk(
                TableKind::Apm,
                FsKind::Hfsplus.into(),
                "Macintosh HD",
                None,
                "macintosh.img",
            ),
            PresetKind::Dos => disk(
                TableKind::Mbr,
                FsKind::Fat.into(),
                "DOS",
                Some((
                    2 * GIB,
                    "FAT16, the largest FAT MS-DOS reads, stops at 2 GiB",
                )),
                "dos.img",
            ),
            PresetKind::Win9x => disk(TableKind::Mbr, fat32, "WIN98", None, "win98.img"),
            PresetKind::WinNt => disk(
                TableKind::Mbr,
                ntfs(NtfsVersion::V1_2),
                "WINNT",
                None,
                "winnt.img",
            ),
            PresetKind::WinXp => disk(
                TableKind::Mbr,
                ntfs(NtfsVersion::V3_1),
                "WINXP",
                None,
                "winxp.img",
            ),
            PresetKind::Amiga => disk(
                TableKind::Rdb,
                FsKind::Affs.into(),
                "Workbench",
                None,
                "amiga.hdf",
            ),
            PresetKind::Linux => disk(
                TableKind::Mbr,
                FsKind::Ext.into(),
                "linux",
                None,
                "linux.img",
            ),
            PresetKind::X68k | PresetKind::SgiEfs => None,
        }
    }
}

impl EasyDisk {
    /// The builder for a `size`-byte disk: one row, formatted, at the type the table expects.
    pub fn builder(&self, size: u64, label: Option<&str>) -> DiskBuilder {
        let mut b = DiskBuilder::new(self.table, size);
        let mut f = RowFormat::new(self.fs.kind);
        self.fs.apply_to(&mut f);
        let max = self.fs.kind.caps().label_max;
        let name = label.unwrap_or(self.label);
        if max == 0 || name.len() <= max {
            f.opts.label = name.to_string();
        }
        // FFS with international names: what Kickstart 2.0 and later format a hard disk with.
        if self.fs.kind == FsKind::Affs {
            f.opts.affs_variant = 3;
        }
        let row = &mut b.rows[0];
        if let Some(t) = suggested_type(self.table, self.fs.kind, size, self.fs.fat32) {
            row.type_text = t;
        }
        row.format = Some(f);
        b
    }

    /// Why this target cannot be `size` bytes, before the builder's own checks.
    pub fn refuses(&self, size: u64) -> Option<String> {
        let (max, why) = self.max?;
        (size > max).then(|| {
            format!(
                "{why}; make the disk {} or smaller.",
                disk_core::display::format_size(max)
            )
        })
    }
}
