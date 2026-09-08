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

use crate::partition::provision::{self, Geometry, PartSpec, Placed};
use crate::partition::type_catalog::{self, TableKind};
use crate::partition::{format_size, parse_size};

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
        if let Err(e) = provision::parse_align(&self.align_text) {
            if !self.align_text.trim().is_empty() {
                return Err(e);
            }
        }
        provision::place(
            &self.specs()?,
            self.kind,
            self.disk_size,
            self.align_bytes(),
            self.geometry,
        )
    }

    /// Source image per row, in the shape
    /// [`crate::model::provision_runner::ProvisionRequest`] wants.
    pub fn sources(&self) -> Vec<Option<PathBuf>> {
        self.rows.iter().map(|r| r.source.clone()).collect()
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

        for (i, (row, p)) in self.rows.iter().zip(placed.iter()).enumerate() {
            if row.source.is_some() && row.source_size > p.size_bytes {
                self.errors.push(format!(
                    "Partition {}: the source is {} but the partition is only {}",
                    i + 1,
                    format_size(row.source_size),
                    format_size(p.size_bytes),
                ));
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
            self.status = Some(format!(
                "Ready: {} partition(s), {} with a source image.",
                placed.len(),
                filled,
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
