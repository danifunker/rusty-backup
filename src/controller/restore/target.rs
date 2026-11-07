//! The targets: each mode's device or image, the full-disk sizing form, the single-partition
//! target scan and the new-disk table choice.

use std::path::Path;

use super::source::finished;
use super::{
    AlignmentChoice, Caveat, CaveatLevel, ExpandPlan, ExpandPreview, LayoutFit, NewDiskChoice,
    NewTableType, Picked, PlanSegment, RestoreController, RestoreMode, RowCaveat, SizeRow, Target,
    TargetPartition, TargetSlot,
};
use crate::device::{DeviceIdentity, DiskDevice};
use crate::model::job::{Job, Snapshot};
use crate::model::source_scan::ScanFailure;
use crate::partition::PartitionInfo;
use disk_core::display::format_size;

/// A target scan's result: the partitions and table name, or why it failed.
type TargetScan = Result<(Vec<PartitionInfo>, String), ScanFailure>;

pub(super) struct TargetState {
    pub full: Target,
    pub single: Target,
    pub new_disk: Target,
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
    /// Put the bundled Mac driver on a disk whose backup lost its own.
    pub install_mac_driver: bool,
}

impl Default for TargetState {
    fn default() -> Self {
        TargetState {
            full: Target::new(true),
            single: Target::new(true),
            new_disk: Target::new(true),
            alignment: AlignmentChoice::Original,
            custom_alignment: 2048,
            expand: ExpandPlan::default(),
            sp_parts: Vec::new(),
            sp_identity: None,
            sp_part: None,
            sp_scan: None,
            sp_scan_error: None,
            install_mac_driver: true,
            new_disk_choice: NewDiskChoice {
                table: NewTableType::Mbr,
                alignment: AlignmentChoice::Modern1MB,
                custom_alignment: 2048,
                bootable: false,
            },
        }
    }
}

impl TargetState {
    pub fn target(&self, slot: TargetSlot) -> &Target {
        match slot {
            TargetSlot::FullDisk => &self.full,
            TargetSlot::SinglePartition => &self.single,
            TargetSlot::NewDisk => &self.new_disk,
        }
    }

    pub fn target_mut(&mut self, slot: TargetSlot) -> &mut Target {
        match slot {
            TargetSlot::FullDisk => &mut self.full,
            TargetSlot::SinglePartition => &mut self.single,
            TargetSlot::NewDisk => &mut self.new_disk,
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
            let moved = t.device.as_ref().map(|p| p.device.display_name());
            if let Some(name) = moved {
                self.warn(format!(
                    "The restore target is now {name}: the list changed."
                ));
            }
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

    /// "Target: x | Source was: y" under a full-disk target, once both are known.
    pub fn target_readout(&self) -> Option<Caveat> {
        let source = self.source_size()?;
        let target = self.target_size();
        if target == 0 {
            return None;
        }
        let small = target < source;
        Some(Caveat {
            level: if small {
                CaveatLevel::Warning
            } else {
                CaveatLevel::Info
            },
            text: format!(
                "Target: {} | Source was: {}{}",
                format_size(target),
                format_size(source),
                if small { " (target is smaller!)" } else { "" },
            ),
        })
    }

    /// The size total and the engine's layout check, under the size rows; Blocked means it fails.
    pub fn size_notes(&self) -> Vec<Caveat> {
        let target = self.target_size();
        if target == 0 {
            return Vec::new();
        }
        let note = |level, text| Caveat { level, text };
        let total = self.fixed_total();
        let level = if total <= target {
            CaveatLevel::Info
        } else {
            CaveatLevel::Blocked
        };
        let mut notes = vec![note(
            level,
            format!(
                "Total: {} / {} available",
                format_size(total),
                format_size(target),
            ),
        )];
        // Sizes adding up is not the layout fitting: Original alignment keeps every start.
        match self.layout_fit() {
            Some(LayoutFit::Refused { message, original }) => {
                notes.push(note(
                    CaveatLevel::Blocked,
                    format!("This layout will not fit: {message}"),
                ));
                if original {
                    notes.push(note(
                        CaveatLevel::Warning,
                        "Original alignment keeps every partition at its \
                         original position, so shrinking only leaves gaps. \
                         Choose 1 MB boundaries or Custom to repack them \
                         from the start of the disk."
                            .to_string(),
                    ));
                }
            }
            Some(LayoutFit::Overrun { end, target }) => notes.push(note(
                CaveatLevel::Blocked,
                format!(
                    "Layout ends at {} but the target holds {}",
                    format_size(end),
                    format_size(target),
                ),
            )),
            Some(LayoutFit::Fits) | None => {}
        }
        notes
    }

    /// The warning under a target that is the system disk.
    pub fn system_disk_warning(&self, slot: TargetSlot) -> Option<&'static str> {
        if !self.device(slot).is_some_and(|d| d.is_system) {
            return None;
        }
        Some(match slot {
            TargetSlot::SinglePartition => "WARNING: This is a system disk!",
            TargetSlot::FullDisk | TargetSlot::NewDisk => {
                "WARNING: This is a system disk! Restoring will destroy your OS."
            }
        })
    }

    /// The backup lost its Mac driver, so the form offers the bundled one.
    pub fn mac_driver_missing(&self) -> bool {
        self.mode == RestoreMode::FullDisk && self.loaded().is_some_and(|l| l.mac_driver_missing)
    }

    pub fn install_mac_driver(&self) -> bool {
        self.targets.install_mac_driver
    }

    /// What each volume asks of the machine reading it, at the sizes chosen; worst first per row.
    pub fn size_caveats(&self) -> Vec<RowCaveat> {
        use crate::model::restore_plan::{plan_caveats, volume_caveats};
        match self.mode {
            RestoreMode::FullDisk => self.loaded().map_or_else(Vec::new, |loaded| {
                let is_device = self.targets.full.is_device;
                plan_caveats(
                    loaded,
                    &self.resolved_alignment(),
                    self.partition_sizes(is_device).as_slice(),
                    self.target_size(),
                    self.targets.install_mac_driver,
                )
            }),
            RestoreMode::SinglePartition | RestoreMode::NewDisk => {
                let facts = if self.src.has_image() {
                    self.src.image_facts
                } else {
                    let index = self.src.source_partition;
                    self.size_rows()
                        .iter()
                        .find(|r| Some(r.index) == index)
                        .and_then(|r| r.facts)
                };
                match self.partition_target_size() {
                    Some(size) => volume_caveats(facts, size),
                    None => Vec::new(),
                }
            }
            RestoreMode::WriteImage => Vec::new(),
        }
    }

    /// The partition a single-partition or new-disk restore fills, once its target is known.
    fn partition_target_size(&self) -> Option<u64> {
        if self.mode == RestoreMode::SinglePartition {
            let index = self.targets.sp_part?;
            let parts = &self.targets.sp_parts;
            return parts
                .iter()
                .find(|p| p.index == index)
                .map(|p| p.size_bytes);
        }
        let source = self.partition_source()?;
        let t = &self.targets.new_disk;
        let device_size = t.device.as_ref().map(|p| p.device.size_bytes);
        let path = t
            .device
            .as_ref()
            .map(|p| p.device.path.clone())
            .or(t.image.clone())?;
        let config = crate::model::restore_plan::new_disk_config(
            &source,
            path.as_path(),
            t.is_device,
            device_size.filter(|_| t.is_device),
            &self.targets.new_disk_choice,
        );
        let disk = config.new_disk.as_ref().map(|d| d.disk_size_bytes)?;
        Some(disk.saturating_sub(config.target_offset_bytes))
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

    /// The padded target's size change, in words.
    pub fn expand_size_line(&self) -> String {
        let source = self.source_size().unwrap_or(0);
        let total = source.saturating_add(self.expand_free_bytes());
        format!(
            "(target size {} -> {})",
            format_size(source),
            format_size(total),
        )
    }

    /// The before and after bars of the padded target.
    pub fn expand_preview(&self) -> ExpandPreview {
        let current = self.plan_segments();
        let source = self.source_size().unwrap_or(0);
        let added = self.expand_free_bytes();
        let mut after = current.clone();
        let mut free_tail = added;
        if added > 0 && self.targets.expand.extend_last {
            if let Some(last) = after.last_mut() {
                last.size_bytes = last.size_bytes.saturating_add(added);
                free_tail = 0;
            }
        }
        let total = source.saturating_add(added).max(1);
        ExpandPreview {
            current,
            after,
            free_tail,
            added,
            scale: (source as f64 / total as f64) as f32,
        }
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
}
