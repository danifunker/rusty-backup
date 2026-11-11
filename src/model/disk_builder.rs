//! Working state for "build a disk": a table type, a disk size, and an ordered
//! list of partitions to lay down on it.
//!
//! The counterpart to [`crate::model::partition_editor::PartitionEditor`],
//! which diffs edits against a table that already exists. Here there is nothing
//! to diff — the user names sizes in order and
//! [`crate::partition::provision::place`] does the arithmetic, exactly as it
//! does for `rb-cli new hd`. Rows therefore carry a size *string* (`"20M"`,
//! `"rest"`), not a start LBA: the layout is derived, never hand-entered.
//!
//! [`DiskBuilder::validate`] is the single gate before applying — it plans the
//! layout, checks every assigned source fits its partition, and leaves
//! `errors` / `status` for the view to render.

#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::OptionIsNoneOr as _;
use std::path::PathBuf;

use anyhow::Result;

use crate::model::format::{self, FormatOptions, FsKind};
use crate::partition::parse_size;
use crate::partition::provision::{self, Geometry, PartSpec, Placed};
use crate::partition::type_catalog::{self, TableKind};
use disk_core::display::format_size;

/// One partition being defined, before it is placed.
#[derive(Debug, Clone, Default)]
pub struct BuilderRow {
    /// `"20M"`, `"1G"`, a plain byte count, or `"rest"` for the remainder.
    pub size_text: String,
    /// Type value in the table's own vocabulary; blank means the default.
    pub type_text: String,
    /// GPT / APM / X68000 entry name. Ignored by MBR and SGI.
    pub name: String,
    /// Image poured into this partition once the table is written.
    pub source: Option<PathBuf>,
    /// Decoded size of `source`, cached by the caller so validation can flag
    /// an image too big for the partition without re-opening it every frame.
    pub source_size: u64,
    /// Format this partition blank; ignored when `source` is set.
    pub format: Option<RowFormat>,
    /// Boot from this partition, where the table has a flag for it.
    pub bootable: bool,
    /// Copy the donor's partition at this position into the row (its bytes, verbatim).
    pub copy_from: Option<usize>,
    /// The donor partition this row was laid out from, kept when its content changes.
    pub donor_part: Option<usize>,
}

/// A disk whose layout a build starts from: its table and partitions, and for the label
/// schemes the head region that rides verbatim (resized by `partition::restore_patch`).
#[derive(Debug, Clone)]
pub struct Donor {
    pub path: PathBuf,
    /// The table type as `PartitionTable::type_name` spells it.
    pub table_type: String,
    pub kind: TableKind,
    pub disk_size: u64,
    /// The bytes before the first partition; written back patched for a label scheme.
    pub head: Vec<u8>,
    pub parts: Vec<DonorPart>,
}

/// One partition of a donor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DonorPart {
    /// `PartitionInfo::index`, which the label patchers key on.
    pub index: usize,
    pub start_byte: u64,
    pub size: u64,
    /// The type in the table's own vocabulary, and its display name.
    pub type_text: String,
    pub type_name: String,
    pub bootable: bool,
}

impl Donor {
    /// Read `path`'s table (any container the engine opens).
    pub fn read(path: &std::path::Path) -> Result<Donor> {
        use std::io::{Read, Seek, SeekFrom};
        let mut r = crate::model::source_reader::open_read(path)?;
        let table = crate::partition::PartitionTable::detect(&mut r)
            .map_err(|e| anyhow::anyhow!("no partition table found: {e}"))?;
        let kind = type_catalog::kind_of(&table);
        if !provision::WRITABLE_TABLES.contains(&kind) {
            anyhow::bail!("a {} table cannot be built from yet", table.type_name());
        }
        let infos: Vec<_> = table
            .partitions()
            .into_iter()
            .filter(|p| !p.is_extended_container && p.size_bytes > 0)
            .collect();
        if infos.is_empty() {
            anyhow::bail!("the donor has no partitions");
        }
        if crate::partition::partitions_overlap(infos.as_slice()) {
            anyhow::bail!("the donor's partitions overlap; it describes several layouts at once");
        }
        if kind == TableKind::Mbr && infos.iter().any(|p| p.is_logical) {
            anyhow::bail!("the donor has logical partitions, which Build Disk cannot write yet");
        }
        let disk_size = r.seek(SeekFrom::End(0))?;
        let head_len = infos
            .iter()
            .map(|p| p.byte_offset())
            .min()
            .unwrap_or(512)
            .max(512);
        let mut head = vec![0u8; head_len as usize];
        r.seek(SeekFrom::Start(0))?;
        r.read_exact(&mut head)?;
        let parts = infos
            .iter()
            .map(|p| DonorPart {
                index: p.index,
                start_byte: p.byte_offset(),
                size: p.size_bytes,
                type_text: match kind {
                    TableKind::Mbr => format!("{:02X}", p.partition_type_byte),
                    _ => p
                        .partition_type_string
                        .clone()
                        .unwrap_or_else(|| p.type_name.clone()),
                },
                type_name: p.type_name.clone(),
                bootable: p.bootable,
            })
            .collect();
        Ok(Donor {
            path: path.to_path_buf(),
            table_type: table.type_name().to_string(),
            kind,
            disk_size,
            head,
            parts,
        })
    }

    /// The table is a label the patchers rewrite in the verbatim head (SGI, Sun, NeXT, RDB, ...).
    pub fn keeps_head(&self) -> bool {
        crate::partition::restore_patch::can_patch(self.table_type.as_str())
    }
}

/// A filesystem to format and its options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowFormat {
    pub kind: FsKind,
    pub opts: FormatOptions,
}

impl RowFormat {
    /// `kind` with its default options (CP/M gets its first preset).
    pub fn new(kind: FsKind) -> Self {
        // The CLI's default name is longer than some labels allow; those start as UNTITLED.
        let max = kind.caps().label_max;
        let label = match kind.default_label() {
            l if max == 0 || l.len() <= max => l.to_string(),
            _ => "UNTITLED".chars().take(max).collect(),
        };
        let mut opts = FormatOptions {
            label,
            ..FormatOptions::default()
        };
        if kind == FsKind::Cpm {
            opts.cpm_preset = fs_cpm::cpm_diskdefs::ALL_PRESETS
                .first()
                .map(|d| d.name.to_string());
        }
        RowFormat { kind, opts }
    }

    /// The filesystem and, when it has one, its label: `FAT12 / FAT16 / FAT32 "DOS"`.
    pub fn describe(&self) -> String {
        // The NTFS version decides which Windows can mount it, so the review names it.
        let name = match self.kind {
            FsKind::Ntfs => format!(
                "NTFS {}",
                self.opts.ntfs_version.unwrap_or_default().label()
            ),
            _ => self.kind.display_name().to_string(),
        };
        if self.kind.caps().label_max == 0 {
            name
        } else {
            format!("{name} {:?}", self.opts.label)
        }
    }

    /// The options and label are ones this filesystem takes.
    pub fn check(&self) -> Result<(), String> {
        format::check_options(self.kind, &self.opts).map_err(|e| format!("{e:#}"))?;
        let max = self.kind.caps().label_max;
        if max > 0 && self.opts.label.len() > max {
            return Err(format!(
                "the {} label is at most {max} bytes",
                self.kind.display_name()
            ));
        }
        if self.opts.affs_variant > 7 {
            return Err("the AFFS variant is 0 to 7".to_string());
        }
        Ok(())
    }

    /// A volume of `size` bytes is one this filesystem can be formatted at.
    pub fn fits(&self, size: u64) -> Result<(), String> {
        let caps = self.kind.caps();
        let name = self.kind.display_name();
        if caps.fixed_size {
            return if caps.min_size > size {
                Err(format!(
                    "{name} needs {}, more than {}",
                    format_size(caps.min_size),
                    format_size(size)
                ))
            } else {
                Ok(())
            };
        }
        if size < caps.min_size {
            return Err(format!(
                "{name} needs at least {}, not {}",
                format_size(caps.min_size),
                format_size(size)
            ));
        }
        if size > caps.max_buildable() {
            return Err(format!(
                "{name} can be at most {}, not {}",
                format_size(caps.max_buildable()),
                format_size(size)
            ));
        }
        Ok(())
    }
}

/// The partition type a row formatted with `fs` should carry on a `kind` table, when the table
/// has one for it. `size` picks FAT's type byte.
pub fn suggested_type(kind: TableKind, fs: FsKind, size: u64) -> Option<String> {
    use FsKind as F;
    let t = match (kind, fs) {
        (TableKind::Mbr, F::Fat) => {
            let fat = fs_pc::fat::compute_fat_blank_layout(size.max(64 * 1024)).ok()?;
            match fat.fat_type {
                fs_pc::fat::FatType::Fat12 => "01",
                fs_pc::fat::FatType::Fat16 if size < 32 * 1024 * 1024 => "04",
                fs_pc::fat::FatType::Fat16 => "06",
                fs_pc::fat::FatType::Fat32 => "0C",
            }
        }
        (TableKind::Mbr, F::Ntfs | F::Hpfs | F::Exfat) => "07",
        (TableKind::Mbr, F::Ext | F::Ext3 | F::Ext4 | F::Xfs) => "83",
        (TableKind::Mbr, F::Hfs | F::Hfsplus | F::Hfv) => "AF",
        (TableKind::Mbr, F::Bfs) => "EB",
        (TableKind::Mbr, F::Ufs) => "A5",
        (TableKind::Gpt, F::Fat | F::Ntfs | F::Exfat) => "EBD0A0A2-B9E5-4433-87C0-68B6B72699C7",
        (TableKind::Gpt, F::Ext | F::Ext3 | F::Ext4 | F::Xfs) => {
            "0FC63DAF-8483-4772-8E79-3D69D8477DE4"
        }
        (TableKind::Gpt, F::Hfs | F::Hfsplus | F::Hfv) => "48465300-0000-11AA-AA11-00306543ECAC",
        (TableKind::Gpt, F::Ufs) => "516E7CB4-6ECF-11D6-8FF8-00022D09712B",
        (TableKind::Apm, F::Hfs | F::Hfsplus | F::Hfv) => "Apple_HFS",
        (TableKind::Apm, F::Prodos) => "Apple_ProDOS",
        (TableKind::Apm, F::Mfs) => "Apple_MFS",
        (TableKind::Apm, F::Ufs) => "Apple_UFS",
        (TableKind::Apm, F::Bfs) => "Be_BFS",
        (TableKind::Sgi, F::Xfs) => "XFS",
        (TableKind::Sgi, F::Efs) => "EFS",
        (TableKind::Rdb, F::Pfs3) => "PFS\\3",
        (TableKind::Rdb, F::Sfs) => "SFS\\0",
        (TableKind::Rdb, F::Affs) => "DOS\\3",
        (TableKind::Next, F::Ufs43) => "4.3BSD",
        (TableKind::Bsd, F::Ufs) => "4.2BSD",
        (TableKind::Atari, F::Fat) if size > 16 * 1024 * 1024 => "BGM",
        (TableKind::Atari, F::Fat) => "GEM",
        _ => return None,
    };
    Some(t.to_string())
}

impl BuilderRow {
    /// A row sized `size_text` at `kind`'s default type.
    pub fn new(kind: TableKind, size_text: &str) -> Self {
        Self {
            size_text: size_text.to_string(),
            type_text: provision::default_type(kind).to_string(),
            ..Default::default()
        }
    }

    pub fn is_rest(&self) -> bool {
        self.size_text.trim().eq_ignore_ascii_case("rest")
    }
}

/// Mutable state for the Build Disk modal.
#[derive(Debug, Clone)]
pub struct DiskBuilder {
    pub kind: TableKind,
    /// The disk this layout started from, when it did.
    pub donor: Option<Donor>,
    /// Size of the disk being built, in bytes.
    pub disk_size: u64,
    /// Alignment for partition starts; the same grammar as `--align`
    /// (`"1M"`, `"63s"`).
    pub align_text: String,
    /// Only consulted for SGI, whose partitions are cylinder-aligned.
    pub geometry: Geometry,
    pub rows: Vec<BuilderRow>,
    pub errors: Vec<String>,
    pub status: Option<String>,
}

impl Default for DiskBuilder {
    fn default() -> Self {
        Self {
            kind: TableKind::Mbr,
            donor: None,
            disk_size: 0,
            // Blank means "the table's own default" — 1 MiB, or a cylinder on SGI.
            align_text: String::new(),
            geometry: Geometry::default(),
            rows: Vec::new(),
            errors: Vec::new(),
            status: None,
        }
    }
}

impl DiskBuilder {
    /// A builder seeded with one partition spanning the whole disk.
    pub fn new(kind: TableKind, disk_size: u64) -> Self {
        Self {
            kind,
            disk_size,
            rows: vec![BuilderRow::new(kind, "rest")],
            ..Default::default()
        }
    }

    /// A builder laid out like `donor`: one row per donor partition at its own size, each
    /// copying the donor's bytes.
    pub fn from_donor(donor: Donor, disk_size: u64) -> Self {
        // A table with one boot flag keeps the first, as a donor's own BIOS would.
        let one_only = provision::bootable_rule(donor.kind) == Some(true);
        let first = donor.parts.iter().position(|p| p.bootable);
        let rows = donor
            .parts
            .iter()
            .enumerate()
            .map(|(i, p)| BuilderRow {
                size_text: p.size.to_string(),
                type_text: p.type_text.clone(),
                bootable: p.bootable && (!one_only || first == Some(i)),
                copy_from: Some(i),
                donor_part: Some(i),
                ..Default::default()
            })
            .collect();
        Self {
            kind: donor.kind,
            disk_size,
            rows,
            donor: Some(donor),
            ..Default::default()
        }
    }

    /// The donor keeps its head and its slots: rows cannot be added, removed or moved.
    pub fn fixed_slots(&self) -> bool {
        self.donor.as_ref().is_some_and(|d| d.keeps_head())
    }

    /// The donor's head patched for this layout, and where each row lands, for a label donor.
    pub fn patched_head(&self) -> Result<Option<(Vec<u8>, Vec<Placed>)>> {
        let Some(d) = self.donor.as_ref().filter(|d| d.keeps_head()) else {
            return Ok(None);
        };
        if self.rows.len() != d.parts.len() {
            anyhow::bail!("the donor's layout has {} slots", d.parts.len());
        }
        let (unit, tail) =
            crate::partition::restore_patch::layout_limits(d.table_type.as_str(), &d.head);
        let fixed: u64 = self
            .rows
            .iter()
            .filter(|r| !r.is_rest())
            .map(|r| parse_size(&r.size_text).unwrap_or(0).div_ceil(unit) * unit)
            .sum();
        let floor = d.parts.iter().map(|p| p.start_byte).min().unwrap_or(0);
        let rest = self.disk_size.saturating_sub(fixed + floor + tail) / unit * unit;
        let mut overrides = Vec::new();
        for (i, (row, part)) in self.rows.iter().zip(d.parts.iter()).enumerate() {
            let size = if row.is_rest() {
                rest
            } else {
                parse_size(&row.size_text)
                    .map_err(|e| anyhow::anyhow!("partition {}: {e:#}", i + 1))?
            };
            overrides.push(part_tables::PartitionSizeOverride::size_only(
                part.index,
                part.start_byte / 512,
                part.size,
                size,
            ));
        }
        let patched = crate::partition::restore_patch::patch_head_for_restore(
            d.table_type.as_str(),
            d.head.as_slice(),
            overrides.as_slice(),
            self.disk_size,
            &mut |_| {},
        )?;
        let placed = patched
            .overrides
            .iter()
            .zip(d.parts.iter())
            .zip(self.rows.iter())
            .map(|((ov, part), row)| Placed {
                start_lba: ov.effective_start_lba(),
                size_bytes: ov.export_size,
                type_text: part.type_text.clone(),
                name: String::new(),
                bootable: row.bootable,
            })
            .collect();
        Ok(Some((patched.head, placed)))
    }

    /// The donor bytes each row copies: `(row, donor start byte, length)`, the length capped at
    /// the row's planned size.
    pub fn copies(&self, placed: &[Placed]) -> Vec<(usize, u64, u64)> {
        let Some(d) = self.donor.as_ref() else {
            return Vec::new();
        };
        self.rows
            .iter()
            .zip(placed.iter())
            .enumerate()
            .filter(|(_, (r, _))| r.source.is_none() && r.format.is_none())
            .filter_map(|(i, (r, p))| {
                let part = d.parts.get(r.copy_from?)?;
                Some((i, part.start_byte, part.size.min(p.size_bytes)))
            })
            .collect()
    }

    /// Switch table type, re-stamping the default partition type on any row
    /// still carrying the previous default. A type the user typed is left
    /// alone even when it makes no sense on the new table — the validation
    /// pass reports that rather than silently discarding their input.
    pub fn set_kind(&mut self, kind: TableKind) {
        if kind == self.kind {
            return;
        }
        let was_default = provision::default_type(self.kind);
        let now_default = provision::default_type(kind);
        for row in &mut self.rows {
            if row.type_text.trim().is_empty() || row.type_text.trim() == was_default {
                row.type_text = now_default.to_string();
            }
        }
        self.kind = kind;
        self.errors.clear();
        self.status = None;
    }

    /// How many more partitions this table can hold.
    pub fn remaining_slots(&self) -> Option<usize> {
        provision::slot_limit(self.kind).map(|l| l.saturating_sub(self.rows.len()))
    }

    pub fn can_add_row(&self) -> bool {
        self.remaining_slots().is_none_or(|n| n > 0)
    }

    /// Add a row claiming the rest of the disk. When a `rest` row already
    /// exists the new row is a fixed 100 MiB inserted *before* it, so the
    /// catch-all stays last and the layout keeps planning.
    pub fn add_row(&mut self) {
        match self.rows.iter().position(|r| r.is_rest()) {
            Some(at) => self.rows.insert(at, BuilderRow::new(self.kind, "100M")),
            None => self.rows.push(BuilderRow::new(self.kind, "rest")),
        }
        self.errors.clear();
        self.status = None;
    }

    pub fn remove_row(&mut self, index: usize) {
        if index < self.rows.len() {
            self.rows.remove(index);
            self.errors.clear();
            self.status = None;
        }
    }

    /// Move a row one place earlier (`-1`) or later (`+1`) in disk order.
    pub fn move_row(&mut self, index: usize, delta: isize) {
        let target = index as isize + delta;
        if target < 0 || target as usize >= self.rows.len() || index >= self.rows.len() {
            return;
        }
        self.rows.swap(index, target as usize);
        self.errors.clear();
        self.status = None;
    }

    /// Alignment in bytes, falling back to the table's default when the field
    /// is blank or malformed.
    pub fn align_bytes(&self) -> u64 {
        if self.align_text.trim().is_empty() {
            return provision::default_align(self.kind, self.geometry);
        }
        provision::parse_align(&self.align_text)
            .unwrap_or_else(|_| provision::default_align(self.kind, self.geometry))
    }

    /// The rows as specs, or the first parse error.
    pub fn specs(&self) -> Result<Vec<PartSpec>> {
        self.rows
            .iter()
            .enumerate()
            .map(|(i, row)| {
                let size = if row.is_rest() {
                    None
                } else {
                    Some(
                        parse_size(&row.size_text)
                            .map_err(|e| anyhow::anyhow!("partition {}: {e:#}", i + 1))?,
                    )
                };
                Ok(PartSpec {
                    size,
                    type_text: Some(row.type_text.clone()),
                    name: Some(row.name.clone()),
                })
            })
            .collect()
    }

    /// Place the rows on the disk. The layout bar calls this every frame, so
    /// it must stay cheap and must not mutate state.
    pub fn plan(&self) -> Result<Vec<Placed>> {
        if self.rows.is_empty() {
            anyhow::bail!("no partitions defined");
        }
        if self.disk_size == 0 {
            anyhow::bail!("target size is unknown");
        }
        if let Some((_, placed)) = self.patched_head()? {
            return Ok(placed);
        }
        if let Err(e) = provision::parse_align(&self.align_text) {
            if !self.align_text.trim().is_empty() {
                return Err(e);
            }
        }
        let mut placed = provision::place(
            &self.specs()?,
            self.kind,
            self.disk_size,
            self.align_bytes(),
            self.geometry,
        )?;
        if provision::bootable_rule(self.kind).is_some() {
            for (p, row) in placed.iter_mut().zip(self.rows.iter()) {
                p.bootable = row.bootable;
            }
        }
        Ok(placed)
    }

    /// Source image per row, in the shape
    /// [`crate::model::provision_runner::ProvisionRequest`] wants.
    pub fn sources(&self) -> Vec<Option<PathBuf>> {
        self.rows.iter().map(|r| r.source.clone()).collect()
    }

    /// The filesystem to format per row, in the shape the build request wants.
    pub fn formats(&self) -> Vec<Option<RowFormat>> {
        self.rows
            .iter()
            .map(|r| {
                if r.source.is_some() {
                    None
                } else {
                    r.format.clone()
                }
            })
            .collect()
    }

    /// Plan the layout and check every assigned source fits. Populates
    /// `errors` and `status`; returns the plan when it is clean.
    pub fn validate(&mut self) -> Option<Vec<Placed>> {
        self.errors.clear();
        self.status = None;

        let placed = match self.plan() {
            Ok(p) => p,
            Err(e) => {
                self.errors.push(format!("{e:#}"));
                return None;
            }
        };

        let boots = placed.iter().filter(|p| p.bootable).count();
        if provision::bootable_rule(self.kind) == Some(true) && boots > 1 {
            self.errors.push(format!(
                "Only one {} partition can be marked bootable; {boots} are",
                self.kind.label()
            ));
        }
        for (i, (row, p)) in self.rows.iter().zip(placed.iter()).enumerate() {
            if row.source.is_some() && row.source_size > p.size_bytes {
                self.errors.push(format!(
                    "Partition {}: the source is {} but the partition is only {}",
                    i + 1,
                    format_size(row.source_size),
                    format_size(p.size_bytes),
                ));
            }
            let donor_part = row
                .copy_from
                .and_then(|k| self.donor.as_ref().and_then(|d| d.parts.get(k)));
            if let (None, None, Some(part)) = (&row.source, &row.format, donor_part) {
                if part.size > p.size_bytes {
                    self.errors.push(format!(
                        "Partition {}: the donor's {} is {}, more than the {} it would copy into",
                        i + 1,
                        part.type_name,
                        format_size(part.size),
                        format_size(p.size_bytes),
                    ));
                }
            }
            if let (None, Some(f)) = (&row.source, &row.format) {
                if let Err(e) = f.fits(p.size_bytes).and_then(|()| f.check()) {
                    self.errors.push(format!("Partition {}: {e}", i + 1));
                }
            }
            if !row.type_text.trim().is_empty()
                && !type_catalog::choices(self.kind).is_empty()
                && type_catalog::describe(self.kind, &row.type_text).is_none()
            {
                self.errors.push(format!(
                    "Warning: partition {} has type '{}', which is not a known {} type",
                    i + 1,
                    row.type_text.trim(),
                    self.kind.label(),
                ));
            }
        }

        if self.errors.iter().all(|e| e.starts_with("Warning:")) {
            let filled = self.rows.iter().filter(|r| r.source.is_some()).count();
            let formatted = self.formats().iter().filter(|f| f.is_some()).count();
            self.status = Some(format!(
                "Ready: {} partition(s), {} with a source image, {} formatted.",
                placed.len(),
                filled,
                formatted,
            ));
            Some(placed)
        } else {
            None
        }
    }

    /// True when [`validate`](Self::validate) left nothing but warnings.
    pub fn is_applicable(&self) -> bool {
        self.status.is_some() && self.errors.iter().all(|e| e.starts_with("Warning:"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn builder() -> DiskBuilder {
        DiskBuilder::new(TableKind::Mbr, 512 * MIB)
    }

    #[test]
    fn a_fresh_builder_plans_one_whole_disk_partition() {
        let b = builder();
        let placed = b.plan().expect("plans");
        assert_eq!(placed.len(), 1);
        assert_eq!(placed[0].start_lba, 2048);
        // The whole disk past the 1 MiB alignment.
        assert_eq!(placed[0].size_bytes, 511 * MIB);
    }

    #[test]
    fn size_text_accepts_the_cli_grammar() {
        let mut b = builder();
        b.rows[0].size_text = "64M".to_string();
        b.add_row();
        let placed = b.plan().expect("plans");
        assert_eq!(placed[0].size_bytes, 64 * MIB);
        // The appended row claimed `rest`.
        assert_eq!(placed[1].start_lba, 2048 + 64 * 2048);
        assert!(placed[1].size_bytes > 400 * MIB);
    }

    #[test]
    fn a_new_row_lands_before_the_rest_row_so_the_plan_still_fits() {
        let mut b = builder();
        assert!(b.rows[0].is_rest());
        b.add_row();
        assert!(!b.rows[0].is_rest(), "the fixed row goes first");
        assert!(b.rows[1].is_rest(), "the catch-all stays last");
        let placed = b.plan().expect("plans");
        assert_eq!(placed[0].size_bytes, 100 * MIB);
        assert!(placed[1].size_bytes > 400 * MIB);
    }

    #[test]
    fn switching_table_type_restamps_untouched_default_types() {
        let mut b = builder();
        assert_eq!(b.rows[0].type_text, "83");
        b.add_row();
        b.rows[1].type_text = "0C".to_string();

        b.set_kind(TableKind::Apm);
        assert_eq!(
            b.rows[0].type_text, "Apple_HFS",
            "default follows the table"
        );
        assert_eq!(b.rows[1].type_text, "0C", "a typed value is left alone");
    }

    #[test]
    fn slot_limits_gate_the_add_button() {
        let mut b = builder();
        for _ in 0..3 {
            b.add_row();
        }
        assert_eq!(b.rows.len(), 4);
        assert!(!b.can_add_row(), "MBR holds 4");

        b.set_kind(TableKind::Gpt);
        assert!(b.can_add_row(), "GPT is unbounded in practice");
    }

    #[test]
    fn rows_reorder_within_bounds_only() {
        let mut b = builder();
        b.rows[0].size_text = "10M".to_string();
        b.add_row();
        b.rows[1].size_text = "20M".to_string();

        b.move_row(0, 1);
        assert_eq!(b.rows[0].size_text, "20M");
        // Out-of-range moves are no-ops rather than panics.
        b.move_row(0, -1);
        b.move_row(1, 1);
        assert_eq!(b.rows[0].size_text, "20M");
    }

    #[test]
    fn validate_refuses_a_source_bigger_than_its_partition() {
        let mut b = builder();
        b.rows[0].size_text = "16M".to_string();
        b.rows[0].source = Some(PathBuf::from("/tmp/whatever.img"));
        b.rows[0].source_size = 64 * MIB;

        assert!(b.validate().is_none());
        assert!(!b.is_applicable());
        assert!(
            b.errors.iter().any(|e| e.contains("only 16.0 MiB")),
            "{:?}",
            b.errors,
        );
    }

    #[test]
    fn validate_passes_with_a_source_that_fits() {
        let mut b = builder();
        b.rows[0].size_text = "64M".to_string();
        b.rows[0].source = Some(PathBuf::from("/tmp/whatever.img"));
        b.rows[0].source_size = 16 * MIB;

        let placed = b.validate().expect("valid");
        assert_eq!(placed.len(), 1);
        assert!(b.is_applicable());
        assert!(b.status.as_deref().unwrap().contains("1 with a source"));
    }

    #[test]
    fn an_unknown_type_warns_without_blocking_apply() {
        let mut b = builder();
        b.rows[0].type_text = "F9".to_string();
        assert!(b.validate().is_some(), "warnings do not block");
        assert!(b.is_applicable());
        assert!(b.errors[0].starts_with("Warning:"), "{:?}", b.errors);
    }

    #[test]
    fn a_bad_size_is_an_error_not_a_panic() {
        let mut b = builder();
        b.rows[0].size_text = "twenty".to_string();
        assert!(b.validate().is_none());
        assert!(b.errors[0].contains("partition 1"), "{:?}", b.errors);
    }

    #[test]
    fn sgi_defaults_to_cylinder_alignment() {
        let b = DiskBuilder::new(TableKind::Sgi, 1024 * MIB);
        let align = b.align_bytes();
        assert_eq!(align, b.geometry.cylinder_bytes());
        let placed = b.plan().expect("plans");
        // Past the 2 MiB volume header, on a cylinder boundary.
        assert!(placed[0].start_byte() >= 2 * MIB);
        assert_eq!(placed[0].start_byte() % align, 0);
    }
}
