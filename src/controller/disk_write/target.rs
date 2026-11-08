//! The target: a device row (its partitions read in a job) or an image file.

use std::path::PathBuf;

use super::{finished, DiskWriteController, TargetPartition};
use crate::device::{DeviceIdentity, DiskDevice};
use crate::model::disk_write_plan::WriteTarget;
use crate::model::job::{Job, Snapshot};
use crate::partition::PartitionInfo;
use disk_core::display::format_size;

/// A device picked from the list: its row and what was there.
#[derive(Clone)]
struct Picked {
    row: usize,
    device: DiskDevice,
}

#[derive(Default)]
pub(super) struct TargetState {
    device: Option<Picked>,
    image: Option<PathBuf>,
    /// Partitions on the device, once read.
    partitions: Vec<PartitionInfo>,
    scan: Option<Job<Vec<PartitionInfo>>>,
    /// The device `partitions` was (or is being) read from.
    scanned: Option<DeviceIdentity>,
    pub partition: Option<usize>,
}

impl TargetState {
    pub fn scanning(&self) -> bool {
        self.scan.is_some()
    }

    pub fn cancel_scan(&mut self) {
        if let Some(job) = self.scan.take() {
            job.cancel();
        }
    }
}

impl DiskWriteController {
    pub(super) fn select_device(&mut self, row: usize, device: DiskDevice) {
        self.target.image = None;
        let identity = device.identity();
        self.target.device = Some(Picked { row, device });
        if self.target.scanned.as_ref() != Some(&identity) {
            self.scan_device();
        }
    }

    pub(super) fn set_target_image(&mut self, path: PathBuf) {
        self.target.cancel_scan();
        self.target.device = None;
        self.target.scanned = None;
        self.target.partitions.clear();
        self.target.partition = None;
        self.target.image = Some(path);
    }

    /// Read the device's partitions off the UI thread; the list fills in when it lands.
    fn scan_device(&mut self) {
        let Some(picked) = &self.target.device else {
            return;
        };
        let path = picked.device.path.clone();
        let identity = picked.device.identity();
        self.target.cancel_scan();
        self.target.partitions.clear();
        self.target.partition = None;
        self.target.scanned = Some(identity);
        self.target.scan = Some(
            self.spawn("Reading the target's partition table...", move |_| {
                Ok(crate::model::disk_write_plan::read_target_partitions(
                    path.as_path(),
                ))
            }),
        );
    }

    pub(super) fn poll_scan(&mut self) {
        let Some(done) = self.target.scan.as_ref().and_then(finished) else {
            return;
        };
        self.target.scan = None;
        self.target.partitions = done.unwrap_or_default();
    }

    /// A refreshed device list: the target follows its own disk, and never another one.
    pub fn reconcile_devices(&mut self, devices: &[DiskDevice]) {
        if self.run.running() {
            return;
        }
        let Some(picked) = &self.target.device else {
            return;
        };
        let identity = picked.device.identity();
        if devices.get(picked.row).map(DiskDevice::identity) == Some(identity.clone()) {
            return;
        }
        match devices.iter().position(|d| d.identity() == identity) {
            Some(row) => self.select_device(row, devices[row].clone()),
            None => {
                self.target.cancel_scan();
                self.target.device = None;
                self.target.scanned = None;
                self.target.partitions.clear();
                self.target.partition = None;
            }
        }
    }

    /// The device list row of the target device.
    pub fn target_row(&self) -> Option<usize> {
        self.target.device.as_ref().map(|p| p.row)
    }

    pub fn target_device(&self) -> Option<&DiskDevice> {
        self.target.device.as_ref().map(|p| &p.device)
    }

    pub fn target_image(&self) -> Option<&std::path::Path> {
        self.target.image.as_deref()
    }

    /// The device's name as the write prompt wants it typed.
    pub fn device_name(&self) -> Option<String> {
        self.target_device().map(DiskDevice::display_name)
    }

    /// Reading the device's partitions.
    pub fn target_scan(&self) -> Option<Snapshot> {
        self.target.scan.as_ref().map(|j| j.snapshot())
    }

    /// The partitions a write may go into (empty: only the whole disk).
    pub fn target_partitions(&self) -> Vec<TargetPartition> {
        self.target
            .partitions
            .iter()
            .enumerate()
            .map(|(pos, p)| TargetPartition {
                index: p.index,
                position: pos + 1,
                type_name: p.type_name.clone(),
                label: format!(
                    "Partition {} - {} ({}) at LBA {}",
                    p.index + 1,
                    p.type_name,
                    format_size(p.size_bytes),
                    p.start_lba,
                ),
                size_bytes: p.size_bytes,
            })
            .collect()
    }

    pub(super) fn cycle_target_partition(&mut self) {
        let indices: Vec<usize> = self.target.partitions.iter().map(|p| p.index).collect();
        self.target.partition = next_region(indices.as_slice(), self.target.partition);
    }

    pub fn target_partition(&self) -> Option<usize> {
        self.target.partition
    }

    /// "Source is X but the partition holds Y" for the chosen partition.
    pub fn partition_warning(&self) -> Option<String> {
        let src = self.source.as_ref()?;
        let part = self.chosen_partition()?;
        crate::model::disk_write_plan::partition_fit_warning(src.size_bytes, part)
    }

    pub(super) fn chosen_partition(&self) -> Option<&PartitionInfo> {
        let idx = self.target.partition?;
        self.target.partitions.iter().find(|p| p.index == idx)
    }

    /// The target as the plan wants it; an image file's length is read now.
    pub(super) fn write_target(&self) -> Option<WriteTarget> {
        if let Some(p) = &self.target.device {
            return Some(WriteTarget {
                path: p.device.path.clone(),
                size_bytes: p.device.size_bytes,
                is_device: true,
            });
        }
        let path = self.target.image.clone()?;
        let size_bytes = std::fs::metadata(path.as_path()).map_or(0, |m| m.len());
        Some(WriteTarget {
            path,
            size_bytes,
            is_device: false,
        })
    }
}

/// Whole disk (`None`) -> each partition -> whole disk; a vanished choice falls back to the disk.
pub(super) fn next_region(parts: &[usize], current: Option<usize>) -> Option<usize> {
    match current {
        None => parts.first().copied(),
        Some(cur) => match parts.iter().position(|&p| p == cur) {
            Some(i) if i + 1 < parts.len() => Some(parts[i + 1]),
            _ => None,
        },
    }
}
