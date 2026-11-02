//! The targets: each mode's device or image, the full-disk sizing form, the single-partition
//! target scan, the new-disk table choice and Build Disk's target and plan.

use std::path::{Path, PathBuf};

use super::source::finished;
use super::{
    purpose, AlignmentChoice, DiskBuilder, ExpandPlan, LayoutFit, NewDiskChoice, NewTableType,
    Picked, PlanSegment, RestoreController, SizeRow, Target, TargetPartition, TargetSlot,
};
use crate::controller::{Effect, PickSpec};
use crate::device::{DeviceIdentity, DiskDevice};
use crate::model::job::{Job, Snapshot};
use crate::model::source_scan::ScanFailure;
use crate::partition::PartitionInfo;

/// A target scan's result: the partitions and table name, or why it failed.
type TargetScan = Result<(Vec<PartitionInfo>, String), ScanFailure>;

pub(super) struct TargetState {
    pub full: Target,
    pub single: Target,
    pub new_disk: Target,
    pub build: Target,
    pub alignment: AlignmentChoice,
    pub custom_alignment: u32,
    pub expand: ExpandPlan,
    /// The single-partition target's partitions, the device they were read from, the pick.
    pub sp_parts: Vec<PartitionInfo>,
    pub sp_identity: Option<DeviceIdentity>,
    pub sp_part: Option<usize>,
    pub sp_scan: Option<(DeviceIdentity, Job<TargetScan>)>,
    pub sp_scan_error: Option<String>,
    pub new_disk_choice: NewDiskChoice,
    /// An image target's size, in the `rb-cli new hd --size` grammar.
    pub build_size_text: String,
    /// The partition plan, while Build Disk's editor is open.
    pub builder: Option<DiskBuilder>,
    /// The plan row a picked source image goes to, then its decoded-size probe.
    pub build_source_row: Option<usize>,
    pub build_source: Option<(usize, Job<(PathBuf, u64)>)>,
}

impl Default for TargetState {
    fn default() -> Self {
        TargetState {
            full: Target::new(true),
            single: Target::new(true),
            new_disk: Target::new(true),
            build: Target::new(false),
            alignment: AlignmentChoice::Original,
            custom_alignment: 2048,
            expand: ExpandPlan::default(),
            sp_parts: Vec::new(),
            sp_identity: None,
            sp_part: None,
            sp_scan: None,
            sp_scan_error: None,
            new_disk_choice: NewDiskChoice {
                table: NewTableType::Mbr,
                alignment: AlignmentChoice::Modern1MB,
                custom_alignment: 2048,
                bootable: false,
            },
            build_size_text: "512M".to_string(),
            builder: None,
            build_source_row: None,
            build_source: None,
        }
    }
}

impl TargetState {
    pub fn target(&self, slot: TargetSlot) -> &Target {
        match slot {
            TargetSlot::FullDisk => &self.full,
            TargetSlot::SinglePartition => &self.single,
            TargetSlot::NewDisk => &self.new_disk,
            TargetSlot::BuildDisk => &self.build,
        }
    }

    pub fn target_mut(&mut self, slot: TargetSlot) -> &mut Target {
        match slot {
            TargetSlot::FullDisk => &mut self.full,
            TargetSlot::SinglePartition => &mut self.single,
            TargetSlot::NewDisk => &mut self.new_disk,
            TargetSlot::BuildDisk => &mut self.build,
        }
    }

    pub fn scanning(&self) -> bool {
        self.sp_scan.is_some()
    }

    fn forget_scan(&mut self) {
        self.sp_parts.clear();
        self.sp_part = None;
        self.sp_identity = None;
        self.sp_scan_error = None;
    }
}

impl RestoreController {
    // --- devices ------------------------------------------------------------------------

    /// The device list row a mode's target device was picked from.
    pub fn device_row(&self, slot: TargetSlot) -> Option<usize> {
        self.targets.target(slot).device.as_ref().map(|p| p.row)
    }

    /// A mode's target device.
    pub fn device(&self, slot: TargetSlot) -> Option<&DiskDevice> {
        self.targets.target(slot).device.as_ref().map(|p| &p.device)
    }

    pub fn target_is_device(&self, slot: TargetSlot) -> bool {
        self.targets.target(slot).is_device
    }

    pub fn target_image(&self, slot: TargetSlot) -> Option<&Path> {
        self.targets.target(slot).image.as_deref()
    }

    /// "Refresh Devices" replaced the list: another disk (or none) may now sit at a target's row.
    pub fn reconcile_devices(&mut self, devices: &[DiskDevice]) {
        if self.is_running() {
            return;
        }
        for slot in [
            TargetSlot::FullDisk,
            TargetSlot::SinglePartition,
            TargetSlot::NewDisk,
            TargetSlot::BuildDisk,
        ] {
            let t = self.targets.target_mut(slot);
            let Some(picked) = &t.device else {
                continue;
            };
            let now = devices.get(picked.row);
            if now.map(DiskDevice::identity) == Some(picked.device.identity()) {
                continue;
            }
            let row = picked.row;
            t.device = now.map(|d| Picked {
                row,
                device: d.clone(),
            });
        }
        // The old disk's scan must not pick a partition on the new one.
        let now = self
            .targets
            .single
            .device
            .as_ref()
            .map(|p| p.device.identity());
        if !self.targets.sp_parts.is_empty() && now != self.targets.sp_identity {
            self.targets.forget_scan();
            self.targets.sp_scan_error =
                Some("The target device changed since it was scanned; scan it again.".into());
        }
    }

    pub(super) fn select_device(&mut self, slot: TargetSlot, row: usize, device: DiskDevice) {
        if slot == TargetSlot::SinglePartition {
            self.targets.forget_scan();
        }
        self.targets.target_mut(slot).device = Some(Picked { row, device });
    }

    pub(super) fn clear_device(&mut self, slot: TargetSlot) {
        if slot == TargetSlot::SinglePartition {
            self.targets.forget_scan();
        }
        self.targets.target_mut(slot).device = None;
    }

    pub(super) fn set_target_is_device(&mut self, slot: TargetSlot, on: bool) {
        self.targets.target_mut(slot).is_device = on;
        // A device's size is fixed, so trailing free space no longer applies.
        if slot == TargetSlot::FullDisk && on {
            self.targets.expand.enabled = false;
        }
    }

    // --- the full-disk form -------------------------------------------------------------

    /// The full-disk target is complete, or what is missing.
    pub(super) fn full_disk_target_ready(&self) -> Result<(), String> {
        if self.remote_active() {
            return match self.remote_target_label() {
                Some(_) => Ok(()),
                None => Err("Choose a remote target.".into()),
            };
        }
        let t = &self.targets.full;
        if t.is_device && t.device.is_none() {
            return Err("Choose a target device.".into());
        }
        if !t.is_device && t.image.is_none() {
            return Err("Enter a target image path.".into());
        }
        Ok(())
    }

    /// The full-disk target's size: the device, or the source plus the trailing free space.
    pub fn target_size(&self) -> u64 {
        let t = &self.targets.full;
        if t.is_device {
            t.device.as_ref().map(|p| p.device.size_bytes).unwrap_or(0)
        } else {
            self.source_size()
                .unwrap_or(0)
                .saturating_add(self.targets.expand.free_bytes(false))
        }
    }

    pub fn alignment(&self) -> (AlignmentChoice, u32) {
        (self.targets.alignment, self.targets.custom_alignment)
    }

    /// The "Original" alignment radio's label.
    pub fn original_alignment_label(&self) -> String {
        self.loaded()
            .map(crate::model::restore_plan::original_alignment_label)
            .unwrap_or_else(|| "Original".to_string())
    }

    /// The per-partition size rows of the loaded backup.
    pub fn size_rows(&self) -> &[SizeRow] {
        self.loaded().map(|l| l.rows.as_slice()).unwrap_or(&[])
    }

    /// The sum of the fixed sizes ("Total: x / y available").
    pub fn fixed_total(&self) -> u64 {
        crate::model::restore_plan::fixed_total(self.size_rows())
    }

    /// The engine's layout against the target; `None` when there is nothing to project.
    pub fn layout_fit(&self) -> Option<LayoutFit> {
        let meta = self.src.metadata()?;
        let target = self.target_size();
        if target == 0 {
            return None;
        }
        let is_device = self.targets.full.is_device;
        Some(crate::model::restore_plan::layout_fit(
            meta,
            &self.resolved_alignment(),
            self.partition_sizes(is_device).as_slice(),
            target,
        ))
    }

    pub(super) fn resolved_alignment(&self) -> crate::restore::RestoreAlignment {
        crate::model::restore_plan::resolve_alignment(
            self.targets.alignment,
            self.targets.custom_alignment,
        )
    }

    pub(super) fn partition_sizes(
        &self,
        target_is_device: bool,
    ) -> Vec<crate::restore::RestorePartitionSize> {
        crate::model::restore_plan::partition_sizes(
            self.size_rows(),
            self.targets.expand.extends_last(target_is_device),
        )
    }

    /// The trailing-free-space section shows: an image target and a loaded backup.
    pub fn expand_shown(&self) -> bool {
        !self.targets.full.is_device && self.source_size().is_some()
    }

    pub fn expand(&self) -> ExpandPlan {
        self.targets.expand
    }

    /// The largest free space offered, in MiB.
    pub fn expand_cap_mib(&self) -> u32 {
        ExpandPlan::cap_mib(self.source_size().unwrap_or(0))
    }

    /// The bytes the free space adds to an image target.
    pub fn expand_free_bytes(&self) -> u64 {
        self.targets.expand.free_bytes(self.targets.full.is_device)
    }

    /// The backup's top-level partitions, for the before / after preview bars.
    pub fn plan_segments(&self) -> Vec<PlanSegment> {
        self.loaded()
            .map(crate::model::restore_plan::plan_segments)
            .unwrap_or_default()
    }

    // --- the single-partition target ----------------------------------------------------

    pub(super) fn scan_target(&mut self) {
        self.targets.forget_scan();
        let Some(device) = self
            .targets
            .single
            .device
            .as_ref()
            .map(|p| p.device.clone())
        else {
            return;
        };
        self.info(format!(
            "Scanning target partitions on {}...",
            device.path.display()
        ));
        let path = device.path.clone();
        let job = self.spawn("Scanning the target", move |ctx| {
            Ok(crate::model::source_scan::read_device_table(
                path.as_path(),
                ctx,
            ))
        });
        self.targets.sp_scan = Some((device.identity(), job));
    }

    pub(super) fn poll_scan(&mut self) {
        let Some(done) = self.targets.sp_scan.as_ref().and_then(|(_, j)| finished(j)) else {
            return;
        };
        let Some((identity, _)) = self.targets.sp_scan.take() else {
            return;
        };
        match done {
            Ok(Ok((partitions, type_name))) => {
                self.info(format!(
                    "Found {} partition(s) on target ({})",
                    partitions.len(),
                    type_name,
                ));
                self.targets.sp_parts = partitions;
                self.targets.sp_identity = Some(identity);
            }
            Ok(Err(f)) => {
                if let Some(msg) = f.log_error {
                    self.error(msg);
                }
                self.targets.sp_scan_error = Some(f.load_error);
            }
            Err(e) => {
                self.error(format!("Target scan failed: {e}"));
                self.targets.sp_scan_error = Some(e);
            }
        }
    }

    /// The target scan in progress.
    pub fn target_scan(&self) -> Option<Snapshot> {
        self.targets.sp_scan.as_ref().map(|(_, j)| j.snapshot())
    }

    pub fn target_scan_error(&self) -> Option<&str> {
        self.targets.sp_scan_error.as_deref()
    }

    /// The bytes a single-partition or new-disk restore writes, once known.
    pub fn partition_source_size(&self) -> Option<u64> {
        self.partition_source().and_then(|s| s.data_size())
    }

    /// The scanned target's partitions, with whether the source fits each.
    pub fn target_partitions(&self) -> Vec<TargetPartition> {
        let need = self.partition_source_size();
        self.targets
            .sp_parts
            .iter()
            .enumerate()
            .filter(|(_, p)| !p.is_extended_container)
            .map(|(pos, p)| TargetPartition {
                index: p.index,
                position: pos + 1,
                type_name: p.type_name.clone(),
                size_bytes: p.size_bytes,
                fits: need.map(|n| p.size_bytes >= n),
            })
            .collect()
    }

    pub fn target_partition(&self) -> Option<usize> {
        self.targets.sp_part
    }

    // --- new disk -----------------------------------------------------------------------

    pub fn new_disk(&self) -> NewDiskChoice {
        self.targets.new_disk_choice
    }

    /// Where the new disk's partition starts.
    pub fn new_disk_start_lba(&self) -> u64 {
        let c = &self.targets.new_disk_choice;
        crate::model::restore_plan::new_disk_start_lba(
            c.table,
            crate::model::restore_plan::new_disk_alignment_sectors(c.alignment, c.custom_alignment),
        )
    }

    // --- build disk ---------------------------------------------------------------------

    pub fn build_size_text(&self) -> &str {
        self.targets.build_size_text.as_str()
    }

    /// The typed image size, or why it does not parse.
    pub fn build_size(&self) -> Result<u64, String> {
        crate::partition::parse_size(self.targets.build_size_text.as_str())
            .map_err(|e| format!("{e:#}"))
    }

    /// Build Disk's target as `(path, size)`, once complete.
    pub fn build_target(&self) -> Option<(PathBuf, u64)> {
        let t = &self.targets.build;
        if t.is_device {
            let p = t.device.as_ref()?;
            Some((p.device.path.clone(), p.device.size_bytes))
        } else {
            let path = t.image.clone()?;
            let size = self.build_size().ok()?;
            (size > 0).then_some((path, size))
        }
    }

    /// Build Disk's partition window content, while it is open.
    pub fn build_view(&self) -> Option<crate::controller::partition_edit::BuildView> {
        self.targets
            .builder
            .as_ref()
            .map(crate::controller::partition_edit::build_view)
    }

    /// A row's source image is being measured.
    pub fn build_source_probing(&self) -> Option<Snapshot> {
        self.targets
            .build_source
            .as_ref()
            .map(|(_, j)| j.snapshot())
    }

    /// An edit in Build Disk's partition window; a source pick asks for the file first.
    pub(super) fn build_edit(
        &mut self,
        action: crate::controller::partition_edit::BuildAction,
    ) -> Vec<Effect> {
        use crate::controller::partition_edit::BuildAction;
        let Some(builder) = self.targets.builder.as_mut() else {
            return Vec::new();
        };
        if let BuildAction::PickRowSource(row) = action {
            if row >= builder.rows.len() {
                return Vec::new();
            }
            self.targets.build_source_row = Some(row);
            let mut filters = vec![(
                "Disk images".to_string(),
                Self::ext(crate::model::file_types::DISK_IMAGE_EXTS),
            )];
            filters.push(("All Files".to_string(), vec!["*".to_string()]));
            return vec![Effect::PickFile(PickSpec {
                purpose: purpose::BUILD_SOURCE,
                title: format!("Source image for partition {}", row + 1),
                filters,
                save: false,
                file_name: String::new(),
            })];
        }
        crate::controller::partition_edit::apply_build(builder, action);
        Vec::new()
    }

    /// A row's source image was picked: measure its decoded size off the UI thread.
    pub(super) fn build_source_picked(&mut self, path: PathBuf) {
        let Some(row) = self.targets.build_source_row.take() else {
            return;
        };
        let job = self.spawn("Measuring the source image...", move |_| {
            let size = crate::model::source_reader::decoded_image_size(path.as_path()).unwrap_or(0);
            Ok((path, size))
        });
        self.targets.build_source = Some((row, job));
    }

    pub(super) fn poll_build_source(&mut self) {
        let Some(done) = self
            .targets
            .build_source
            .as_ref()
            .and_then(|(_, j)| finished(j))
        else {
            return;
        };
        let Some((row, _)) = self.targets.build_source.take() else {
            return;
        };
        if let (Ok((path, size)), Some(builder)) = (done, self.targets.builder.as_mut()) {
            crate::controller::partition_edit::set_row_source(builder, row, path, size);
        }
    }

    pub fn builder_open(&self) -> bool {
        self.targets.builder.is_some()
    }

    pub(super) fn build_define(&mut self) {
        let Some((_, size)) = self.build_target() else {
            return;
        };
        let kind = self
            .targets
            .builder
            .as_ref()
            .map(|b| b.kind)
            .unwrap_or(crate::partition::type_catalog::TableKind::Mbr);
        self.targets.builder = Some(DiskBuilder::new(kind, size));
    }
}
