//! Export Disk Image (whole disk, per partition, flat HFV) and the remote image's "Back Up Image".

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::{purpose, InspectController, LayoutSegment, LogSink, SegmentKind};
use crate::controller::{Effect, PickSpec};
use crate::model::export_runner::{self, ExportStatus, PartitionExportConfig, PerPartitionInputs};
use crate::model::job::{Job, JobCtx, Status};
use crate::rbformats::chd_options::ChdOptions;
use crate::rbformats::export::ExportFormat;

/// One format the Export window offers: its label and what it is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExportFormatChoice {
    pub format: ExportFormat,
    pub label: &'static str,
    pub hint: &'static str,
}

const fn fmt(format: ExportFormat, label: &'static str, hint: &'static str) -> ExportFormatChoice {
    ExportFormatChoice {
        format,
        label,
        hint,
    }
}

/// The Export window's formats, in the order every shell lists them (CHD last).
pub const EXPORT_FORMATS: &[ExportFormatChoice] = &[
    fmt(ExportFormat::Vhd, "VHD", ""),
    fmt(
        ExportFormat::VhdDynamic,
        "VHD (Dynamic)",
        "Sparse VHD - all-zero blocks are omitted. Same .vhd extension; readable by Hyper-V, \
         qemu-img, Disk Management.",
    ),
    fmt(
        ExportFormat::Qcow2,
        "QCOW2",
        "QCOW2 v3 - sparse, uncompressed. The container UTM uses for classic-Mac PPC guests; \
         opens in QEMU, virt-manager.",
    ),
    fmt(
        ExportFormat::VmdkFlat,
        "VMDK (Flat)",
        "monolithicFlat VMDK - emits <name>.vmdk descriptor + <name>-flat.vmdk raw extent. \
         Opens in VMware Workstation/Fusion, VirtualBox, qemu-img.",
    ),
    fmt(
        ExportFormat::VmdkSparse,
        "VMDK (Sparse)",
        "monolithicSparse VMDK - single self-contained .vmdk; zero grains omitted. Opens in \
         VMware, VirtualBox, qemu-img.",
    ),
    fmt(ExportFormat::Raw, "Raw (.img)", ""),
    fmt(ExportFormat::TwoMg, "2MG (.2mg)", ""),
    fmt(
        ExportFormat::Woz,
        "WOZ (.woz)",
        "Apple II WOZ 2.0 (floppy only: 140K / 400K / 800K sources)",
    ),
    fmt(
        ExportFormat::Moof,
        "MOOF (.moof)",
        "Applesauce Macintosh 3.5\" GCR bitstream (floppy only: 400K / 800K). Loadable by \
         Snow / MAME / Applesauce.",
    ),
    fmt(
        ExportFormat::Dc42,
        "DiskCopy 4.2 (.dsk)",
        "Mac / Apple IIgs DiskCopy 4.2 (floppy only: 400K / 720K / 800K / 1440K)",
    ),
    fmt(
        ExportFormat::Hfv,
        "HFV (BasiliskII)",
        "Flat classic-HFS volume for BasiliskII / SheepShaver (no partition table, \
         classic-HFS-only, max 4063 MB). Exported per-partition: one .hfv per HFS partition, \
         cloned and re-floored to the chosen size.",
    ),
    fmt(
        ExportFormat::Chd,
        "CHD (Hard Disk)",
        "MAME hard-disk CHD (512-byte unit). DVD / CD CHD and BIN/CUE extraction live on the \
         Optical tab.",
    ),
];

/// Why CHD is not offered for this source.
pub const CHD_REFUSAL: &str = "CHD export needs a raw image or device source - backup folders \
                               and Clonezilla images aren't supported.";

/// Why a whole-disk-only format cannot be exported per partition, or `None` when it can.
pub fn per_partition_refusal(format: ExportFormat) -> Option<&'static str> {
    Some(match format {
        ExportFormat::Chd => {
            "Per-partition export is not supported for CHD - each partition would be a \
             headless slice with no MBR/GPT, which no emulator can consume. Use whole-disk \
             export instead."
        }
        ExportFormat::VhdDynamic => {
            "Per-partition export is not supported for dynamic VHD - the sparse layout wraps \
             a whole disk and buys nothing on a single mostly-used partition. Use fixed VHD \
             for per-partition export."
        }
        ExportFormat::Qcow2 => {
            "Per-partition export is not supported for QCOW2 - the sparse layout wraps a \
             whole-disk geometry that QEMU/UTM expect to find an MBR/GPT/APM at sector 0."
        }
        ExportFormat::VmdkFlat => {
            "Per-partition export is not supported for VMDK flat - the descriptor wraps a \
             whole-disk geometry that VMware/qemu-img/VirtualBox expect to find a partition \
             table at sector 0 of."
        }
        ExportFormat::VmdkSparse => {
            "Per-partition export is not supported for VMDK sparse - the grain directory \
             wraps a whole-disk geometry that VMware/qemu-img/VirtualBox expect to find a \
             partition table at sector 0 of."
        }
        _ => return None,
    })
}

/// The Export window's opening line.
pub const EXPORT_INTRO: &str = "Export partitions as disk image files.";
pub const HFV_WHOLE_REFUSAL: &str = "HFV is a single classic-HFS volume with no partition table, \
    so it is exported per-partition (one .hfv per HFS partition).";
pub const SIZES_HINT: &str = "Minimum is the trim point: the imaged size of a compacted backup \
    partition, or the last used byte of a live volume. The exported volume is resized in place \
    to match. Anything smaller needs a Backup with shrink-to-minimum, which clones the volume.";
pub const SECTOR_BY_SECTOR_WARNING: &str = "Warning: the source is a sector-by-sector backup. \
    Re-exporting with new partition sizes drops the byte-for-byte preservation of free space and \
    unrecognized filesystem regions - only smart-compact areas of recognized partitions carry \
    over.";
pub const PHYSICAL_REFUSAL: &str = "Physical Disk Export currently supports raw image file \
    sources only. Backup folders, devices, and Clonezilla images aren't wired in yet.";

pub const PFS3_SHRINK_TITLE: &str = "PFS3 shrink: volume will be rebuilt";
/// The PFS3 rebuild's explanation, one line each.
pub const PFS3_SHRINK_LINES: &[&str] = &[
    "PFS3's allocator scatters blocks throughout the volume, so in-place trim can't free up \
     space. The export pipeline will instead walk the source and rebuild a packed copy at the new \
     size - limitations to be aware of:",
    "  - PFS3 trashcan (deldir) contents are NOT preserved.",
    "  - File data, directory tree, protection bits, comments, dates, softlinks, and hardlinks \
     (both directions) ARE preserved.",
    "  - Operation is slower than a verbatim copy: every file is read from the source and \
     re-written to the destination.",
    "  - Uses a tempfile for the rebuilt image; ensure your temp directory has enough free space \
     for the target partition size.",
];

pub fn pfs3_affected(names: &[String]) -> String {
    format!("Affected partitions: {}", names.join(", "))
}

/// A capped row's hover: the largest size its volume can grow to in place.
pub fn max_size_hint(max_size: u64) -> String {
    let max_mib = (max_size / (1024 * 1024)).max(1);
    format!(
        "HFS volumes are capped at 65535 allocation blocks. For this volume's block size, the \
         maximum is {max_mib} MiB. Growing further requires reformatting."
    )
}

/// The CHD estimate line, from [`ExportNotes::chd_estimate`].
pub fn chd_estimate_line((total, scratch, output): (u64, u64, u64)) -> String {
    const MIB: u64 = 1024 * 1024;
    format!(
        "Estimated peak new disk usage: {} MiB (scratch {} MiB + output {} MiB). Available space \
         is checked when you pick a destination.",
        total / MIB,
        scratch / MIB,
        output / MIB
    )
}

/// The caption over the After bar.
pub fn after_caption(layout: &LayoutBar) -> String {
    let total = |s: &[LayoutSegment]| s.iter().map(|x| x.size_bytes).sum::<u64>();
    format!(
        "After  ({} -> {}):",
        super::format_size(total(&layout.current)),
        super::format_size(total(&layout.after))
    )
}

/// The Export window's choices; the window edits them in place.
#[derive(Debug, Clone)]
pub struct ExportSettings {
    pub whole_disk: bool,
    pub format: ExportFormat,
    pub configs: Vec<PartitionExportConfig>,
}

impl Default for ExportSettings {
    fn default() -> Self {
        ExportSettings {
            whole_disk: true,
            format: ExportFormat::Vhd,
            configs: Vec::new(),
        }
    }
}

impl ExportSettings {
    /// Formats that wrap a whole-disk geometry an emulator expects a table at sector 0 of.
    pub fn whole_disk_only(&self) -> bool {
        matches!(
            self.format,
            ExportFormat::Chd
                | ExportFormat::VhdDynamic
                | ExportFormat::Qcow2
                | ExportFormat::VmdkFlat
                | ExportFormat::VmdkSparse
        )
    }

    /// A flat HFV is one classic-HFS volume, so it is exported per partition.
    pub fn per_partition_only(&self) -> bool {
        self.format == ExportFormat::Hfv
    }

    /// Whole disk or per partition; a format that allows only one keeps it.
    pub fn set_whole_disk(&mut self, on: bool) {
        if (on && self.per_partition_only()) || (!on && self.whole_disk_only()) {
            return;
        }
        self.whole_disk = on;
    }

    fn normalize(&mut self, chd_allowed: bool) {
        if self.format == ExportFormat::Chd && !chd_allowed {
            self.format = ExportFormat::Vhd;
        }
        if self.whole_disk_only() {
            self.whole_disk = true;
        }
        if self.per_partition_only() {
            self.whole_disk = false;
        }
    }

    /// Some partition is exported at a size other than its own.
    fn has_resize(&self, partitions: &[crate::partition::PartitionInfo]) -> bool {
        self.configs.iter().any(|cfg| {
            partitions
                .iter()
                .find(|p| p.index == cfg.index)
                .is_some_and(|p| p.size_bytes != cfg.effective_size())
        })
    }
}

/// The Export window's Current / After bars.
pub struct LayoutBar {
    pub current: Vec<LayoutSegment>,
    pub after: Vec<LayoutSegment>,
}

/// Warnings and estimates the Export window shows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExportNotes {
    /// Type names of PFS3 partitions being shrunk (their volume is rebuilt, trashcan lost).
    pub pfs3_shrink: Vec<String>,
    /// Whole-disk CHD only: how the export runs.
    pub chd_workflow: Option<&'static str>,
    /// Whole-disk CHD only: estimated peak usage (total, scratch, output) in bytes.
    pub chd_estimate: Option<(u64, u64, u64)>,
    /// Resizing a sector-by-sector backup drops its byte-for-byte free space.
    pub sector_by_sector_warning: bool,
}

impl ExportNotes {
    /// The notes under the Export form, one line each, as every shell shows them.
    pub fn lines(&self) -> Vec<String> {
        let mut out = Vec::new();
        if !self.pfs3_shrink.is_empty() {
            out.push(PFS3_SHRINK_TITLE.to_string());
            out.push(pfs3_affected(&self.pfs3_shrink));
            out.extend(PFS3_SHRINK_LINES.iter().map(|l| l.to_string()));
        }
        if let Some(workflow) = self.chd_workflow {
            out.push(workflow.to_string());
            if let Some(estimate) = self.chd_estimate {
                out.push(chd_estimate_line(estimate));
            }
            if self.sector_by_sector_warning {
                out.push(SECTOR_BY_SECTOR_WARNING.to_string());
            }
        }
        out
    }
}

#[derive(Default)]
pub(super) struct ExportState {
    open: bool,
    settings: ExportSettings,
    chd_options: Option<ChdOptions>,
    job: Option<Job<()>>,
    remote: Option<Job<()>>,
}

impl ExportState {
    pub fn close(&mut self) {
        self.open = false;
    }

    pub fn running(&self) -> Option<&'static str> {
        if self.job.is_some() {
            Some("Inspect: Export")
        } else if self.remote.is_some() {
            Some("Inspect: Back Up Image")
        } else {
            None
        }
    }
}

/// Starts an export runner on the job's thread.
type ExportStart = Box<dyn FnOnce() -> anyhow::Result<Arc<Mutex<ExportStatus>>> + Send>;

fn finished(job: &Job<()>) -> Option<Result<(), String>> {
    match job.snapshot().status {
        Status::Running => None,
        Status::Done => Some(Ok(())),
        Status::Failed(e) => Some(Err(e)),
        Status::Cancelled => Some(Err("cancelled".to_string())),
    }
}

impl InspectController {
    pub fn export_running(&self) -> bool {
        self.export.job.is_some()
    }

    /// The running export, for its progress bar.
    pub fn export_job(&self) -> Option<crate::model::job::Snapshot> {
        self.export.job.as_ref().map(|j| j.snapshot())
    }

    pub fn remote_backup_running(&self) -> bool {
        self.export.remote.is_some()
    }

    /// Export needs partitions, a local source, and no export already running.
    pub fn can_export(&self) -> bool {
        !self.found.partitions.is_empty() && !self.export_running() && !self.is_remote()
    }

    pub fn export_open(&self) -> bool {
        self.export.open
    }

    /// CHD (Hard Disk) needs a raw image or device; backups and Clonezilla images cannot feed it.
    pub fn chd_export_allowed(&self) -> bool {
        self.backup_folder().is_none() && self.found.clonezilla.is_none()
    }

    /// The formats the window lists, each with whether this source can take it.
    pub fn export_formats(&self) -> Vec<(ExportFormatChoice, bool)> {
        let chd = self.chd_export_allowed();
        EXPORT_FORMATS
            .iter()
            .map(|c| (*c, c.format != ExportFormat::Chd || chd))
            .collect()
    }

    /// Physical Disk Export takes a raw image file only.
    pub fn can_export_physical(&self) -> bool {
        self.image_path().is_some()
    }

    /// The window's choices as the last edit left them.
    pub fn export_settings(&self) -> &ExportSettings {
        &self.export.settings
    }

    pub fn export_settings_mut(&mut self) -> &mut ExportSettings {
        let allowed = self.chd_export_allowed();
        self.export.settings.normalize(allowed);
        &mut self.export.settings
    }

    pub(super) fn open_export(&mut self) {
        self.export.settings.configs = export_runner::build_partition_configs(
            self.found.partitions.as_slice(),
            self.found.backup.as_ref(),
            &self.found.min_sizes,
            &self.found.defrag_min_sizes,
            self.image_path().map(|p| p.to_path_buf()).as_ref(),
        );
        self.export.open = true;
    }

    pub fn export_layout(&self) -> LayoutBar {
        let parts = self
            .found
            .partitions
            .iter()
            .filter(|p| !p.is_extended_container && !p.is_logical);
        let seg = |color_index, p: &crate::partition::PartitionInfo, size: u64| LayoutSegment {
            label: format!("Partition {}", p.index + 1),
            fs: p.type_name.clone(),
            size_bytes: size,
            kind: SegmentKind::Partition { color_index },
            partition: Some(p.index),
        };
        let current = parts
            .clone()
            .enumerate()
            .map(|(i, p)| seg(i, p, p.size_bytes))
            .collect();
        let after = parts
            .enumerate()
            .map(|(i, p)| {
                let size = match self
                    .export
                    .settings
                    .configs
                    .iter()
                    .find(|c| c.index == p.index)
                {
                    // FillRemaining reports 0; keep it visible at its original size.
                    Some(c) if c.effective_size() != 0 => c.effective_size(),
                    _ => p.size_bytes,
                };
                seg(i, p, size)
            })
            .collect();
        LayoutBar { current, after }
    }

    pub fn export_notes(&self) -> ExportNotes {
        let s = &self.export.settings;
        let pfs3_shrink = s
            .configs
            .iter()
            .filter(|cfg| cfg.effective_size() < cfg.original_size)
            .filter(|cfg| {
                self.found
                    .partitions
                    .iter()
                    .find(|p| p.index == cfg.index)
                    .and_then(|p| p.partition_type_string.as_deref())
                    .is_some_and(crate::fs::is_amiga_pfs3_type)
            })
            .map(|cfg| cfg.type_name.clone())
            .collect();
        let mut notes = ExportNotes {
            pfs3_shrink,
            ..Default::default()
        };
        if s.whole_disk && s.format == ExportFormat::Chd && self.found.table.is_some() {
            let chd_source = self.chd_source_size().is_some();
            let has_resize = s.has_resize(self.found.partitions.as_slice());
            notes.chd_workflow = Some(match (chd_source, has_resize) {
                (true, true) => {
                    "Workflow: decompress source CHD to a temp file -> apply resize plan -> \
                     recompress to destination CHD."
                }
                (true, false) => {
                    "Workflow: stream source CHD -> recompress to destination CHD (no scratch file)."
                }
                (false, true) => {
                    "Workflow: build resized image in a sparse temp file -> compress to \
                     destination CHD."
                }
                (false, false) => {
                    "Workflow: stream source -> compress to destination CHD (no scratch file)."
                }
            });
            notes.chd_estimate = self.chd_estimate();
            notes.sector_by_sector_warning =
                has_resize && self.found.chd_backup.as_ref().is_some_and(|c| c.1);
        }
        notes
    }

    /// The source CHD's file size, when the image is a CHD.
    fn chd_source_size(&self) -> Option<u64> {
        self.image_path()
            .filter(|&p| crate::model::source_reader::is_chd_path(p))
            .and_then(|p| std::fs::metadata(p).ok())
            .map(|m| m.len())
    }

    #[cfg(feature = "chd")]
    fn chd_estimate(&self) -> Option<(u64, u64, u64)> {
        let logical = self
            .found
            .partitions
            .iter()
            .map(|p| p.start_lba.saturating_mul(512).saturating_add(p.size_bytes))
            .max()
            .unwrap_or(0);
        let est = crate::backup::single_file_chd::estimate_export_disk_usage(
            logical,
            self.chd_source_size(),
            self.found.partitions.as_slice(),
            self.export
                .settings
                .has_resize(self.found.partitions.as_slice()),
        );
        Some((est.required_total(), est.scratch_bytes, est.output_bytes))
    }

    #[cfg(not(feature = "chd"))]
    fn chd_estimate(&self) -> Option<(u64, u64, u64)> {
        None
    }

    pub(super) fn set_export_size(
        &mut self,
        index: usize,
        mode: crate::model::size_mode::SizeMode,
        custom_mib: u32,
    ) {
        let configs = &mut self.export_settings_mut().configs;
        let Some(cfg) = configs.iter_mut().find(|c| c.index == index) else {
            return;
        };
        let was_custom = cfg.choice == crate::model::size_mode::SizeMode::Custom;
        cfg.set_choice(mode);
        // Switching into Custom starts at the minimum; only an edit inside Custom sets the size.
        if was_custom && mode == crate::model::size_mode::SizeMode::Custom && custom_mib > 0 {
            cfg.custom_size_mib = custom_mib;
        }
    }

    /// The window's Export button: pick where it goes.
    pub(super) fn start_export(&mut self, chd: ChdOptions) -> Vec<Effect> {
        self.export.open = false;
        self.export.chd_options = Some(chd);
        let s = &self.export.settings;
        let format = s.format;
        if format == ExportFormat::Hfv {
            if self.disk_path().is_none() {
                self.error("HFV export needs a raw image or device source.");
                return Vec::new();
            }
            return vec![Effect::PickFolder(Self::picker(
                purpose::EXPORT_HFV,
                "Export HFV files to",
                false,
                String::new(),
            ))];
        }
        if s.whole_disk {
            let (label, exts) = format.dialog_filter();
            let spec = PickSpec {
                purpose: purpose::EXPORT_DISK,
                title: "Export disk image".into(),
                filters: vec![(
                    label.to_string(),
                    exts.iter().map(|e| e.to_string()).collect(),
                )],
                save: true,
                file_name: format.default_filename("disk"),
            };
            return vec![Effect::PickFile(spec)];
        }
        vec![Effect::PickFolder(Self::picker(
            purpose::EXPORT_FOLDER,
            "Export partitions to",
            false,
            String::new(),
        ))]
    }

    pub(super) fn cancel_export(&mut self) {
        if let Some(job) = &self.export.job {
            job.cancel();
            self.warn("Export cancellation requested...");
        }
    }

    pub(super) fn export_picked(&mut self, purpose: &'static str, dest: Option<PathBuf>) {
        let Some(dest) = dest else {
            return;
        };
        if self.export.job.is_some() {
            return;
        }
        let s = self.export.settings.clone();
        let format = s.format;
        let size_map = export_runner::build_size_map(s.configs.as_slice());
        let overrides = export_runner::build_size_overrides(
            s.configs.as_slice(),
            self.found.partitions.as_slice(),
        );
        let total: u64 = size_map.values().sum();
        let partitions = self.found.partitions.clone();
        let start: ExportStart = match purpose {
            super::purpose::EXPORT_HFV => {
                let Some(source) = self.disk_path() else {
                    return;
                };
                self.info(format!(
                    "Exporting classic-HFS partition(s) as flat HFV file(s) to {}...",
                    dest.display()
                ));
                Box::new(move || {
                    Ok(export_runner::start_per_partition_hfv(
                        source, partitions, size_map, dest, total,
                    ))
                })
            }
            super::purpose::EXPORT_FOLDER => {
                self.info(format!(
                    "Exporting per-partition {} files to {}...",
                    format.description(),
                    dest.display()
                ));
                let inputs = PerPartitionInputs {
                    format,
                    source_folder: self.backup_folder().map(|p| p.to_path_buf()),
                    source_image: self.disk_path(),
                    meta: self.found.backup.clone(),
                    partitions,
                    cz_image: self.found.clonezilla.clone(),
                    size_map,
                    dest_folder: dest,
                    total_bytes: total,
                };
                Box::new(move || Ok(export_runner::start_per_partition(inputs)))
            }
            _ => match self.whole_disk_start(format, dest, overrides, total) {
                Some(start) => start,
                None => return,
            },
        };
        let sink = Arc::clone(&self.log);
        self.export.job = Some(self.spawn("Exporting", move |job| {
            let status = start()?;
            wait_export(status, &sink, job)
        }));
    }

    fn whole_disk_start(
        &mut self,
        format: ExportFormat,
        dest: PathBuf,
        overrides: Vec<part_tables::PartitionSizeOverride>,
        total: u64,
    ) -> Option<ExportStart> {
        // A heads-up before a CHD encode fills the disk; writes still fail cleanly if it does.
        if let Some((required, _, _)) = self
            .chd_estimate()
            .filter(|_| format == ExportFormat::Chd && self.found.table.is_some())
        {
            let dir = dest.parent().unwrap_or(Path::new("."));
            if let Some(avail) = crate::os::available_space(dir) {
                if required > avail {
                    let msg = format!(
                        "Estimated peak new disk usage ({} MiB) exceeds available space at {} \
                         ({} MiB). Free up space or pick a different destination, then try again.",
                        required / (1024 * 1024),
                        dir.display(),
                        avail / (1024 * 1024),
                    );
                    self.error(msg.as_str());
                    self.found.error = Some(msg);
                    return None;
                }
            }
        }
        let desc = format.description();
        if let Some(cz) = self.found.clonezilla.clone() {
            let Some(folder) = self.backup_folder().map(|p| p.to_path_buf()) else {
                self.error("No backup folder for Clonezilla export");
                return None;
            };
            self.info(format!(
                "Exporting Clonezilla image as whole-disk {} to {}...",
                desc,
                dest.display()
            ));
            return Some(Box::new(move || {
                Ok(export_runner::start_clonezilla_whole_disk(
                    format, cz, folder, dest, overrides, total,
                ))
            }));
        }
        let Some(source) = self
            .backup_folder()
            .map(|p| p.to_path_buf())
            .or_else(|| self.disk_path())
        else {
            self.error("No source available for export");
            return None;
        };
        self.info(format!(
            "Exporting whole-disk {} to {}...",
            desc,
            dest.display()
        ));
        if format == ExportFormat::Chd {
            return self.chd_start(source, dest, total);
        }
        let meta = self.found.backup.clone();
        Some(Box::new(move || {
            Ok(export_runner::start_native_whole_disk(
                format, source, meta, overrides, dest, total,
            ))
        }))
    }

    #[cfg(feature = "chd")]
    fn chd_start(&mut self, source: PathBuf, dest: PathBuf, total: u64) -> Option<ExportStart> {
        let opts = self.export.chd_options.clone();
        let table = self.found.table.clone();
        let partitions = self.found.partitions.clone();
        let alignment = self
            .found
            .alignment
            .as_ref()
            .map(|a| a.alignment_sectors)
            .unwrap_or(0);
        let has_resize = self.export.settings.has_resize(partitions.as_slice());
        let targets: Vec<(usize, u64)> = self
            .export
            .settings
            .configs
            .iter()
            .filter_map(|cfg| {
                let new = cfg.effective_size();
                partitions
                    .iter()
                    .find(|p| p.index == cfg.index && p.size_bytes != new)
                    .map(|_| (cfg.index, new))
            })
            .collect();
        let sink = Arc::clone(&self.log);
        Some(Box::new(move || {
            let context = table.and_then(|t| {
                chd_partition_context(
                    t,
                    partitions,
                    alignment,
                    targets,
                    has_resize,
                    source.as_path(),
                    &sink,
                )
            });
            Ok(export_runner::start_native_whole_disk_chd(
                ExportFormat::Chd,
                source,
                dest,
                opts,
                false,
                total,
                context,
            ))
        }))
    }

    #[cfg(not(feature = "chd"))]
    fn chd_start(&mut self, _: PathBuf, _: PathBuf, _: u64) -> Option<ExportStart> {
        self.error("CHD support is not built in.");
        None
    }

    // --- Back Up Image (remote) ---------------------------------------------------------

    pub(super) fn ask_remote_backup(&mut self) -> Vec<Effect> {
        if !self.is_remote() || self.export.remote.is_some() || self.found.partitions.is_empty() {
            return Vec::new();
        }
        vec![Effect::PickFolder(Self::picker(
            purpose::REMOTE_BACKUP,
            "Back up the remote image to",
            false,
            String::new(),
        ))]
    }

    #[cfg(feature = "remote")]
    pub(super) fn remote_backup_picked(&mut self, dest: Option<PathBuf>) {
        let (Some(dest), Some(super::Source::Remote { conn })) = (dest, &self.source) else {
            return;
        };
        if self.export.remote.is_some() {
            return;
        }
        let (conn, rpath) = (Arc::clone(&conn.0), conn.1.clone());
        let stem = Path::new(rpath.as_str())
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("remote-image");
        let backup_name = format!("{stem}-backup");
        let addr = crate::model::worker::lock_status(&*conn).addr().to_string();
        let path_part = if rpath.starts_with('/') {
            rpath.clone()
        } else {
            format!("/{rpath}")
        };
        let display = format!("rb://{addr}{path_part}");
        // Zstd per partition is the only remote output: CHD needs whole-disk staging the tier lacks.
        let config = crate::backup::BackupConfig {
            source_path: PathBuf::from(rpath.as_str()),
            destination_dir: dest.clone(),
            backup_name: backup_name.clone(),
            compression: crate::backup::CompressionType::Zstd,
            checksum: crate::backup::ChecksumType::Sha256,
            split_size_mib: None,
            sector_by_sector: false,
            partition_filter: None,
            chd_options: None,
            size_policy: None,
            partition_target_sizes: None,
            shrink_to_minimum: false,
            precomputed_minimum_sizes: None,
            defrag_partition_indices: None,
            defrag_fat: false,
            // The agent excludes swap itself (cb-dos CRUSTYBK zeros it).
            keep_swap: false,
        };
        self.info(format!(
            "Backing up remote image {display} -> {}",
            dest.join(backup_name.as_str()).display()
        ));
        let source = crate::backup::BackupSource::Remote {
            conn,
            path: rpath,
            display,
            is_device: false,
        };
        let sink = Arc::clone(&self.log);
        self.export.remote = Some(self.spawn("Backing up the remote image", move |job| {
            let _wake = crate::os::wakelock::acquire("Rusty Backup: remote backup");
            let progress = Arc::new(Mutex::new(crate::backup::BackupProgress::new()));
            let mut err = None;
            crate::model::worker::run_mirrored(
                job,
                &progress,
                |p, ctx| {
                    for msg in p.log_messages.drain(..) {
                        let level = match msg.level {
                            crate::backup::LogLevel::Error => {
                                crate::controller::log::LogLevel::Error
                            }
                            crate::backup::LogLevel::Warning => {
                                crate::controller::log::LogLevel::Warning
                            }
                            crate::backup::LogLevel::Info => crate::controller::log::LogLevel::Info,
                        };
                        super::push_log(&sink, level, msg.message);
                    }
                    ctx.set_bytes(p.current_bytes, p.total_bytes);
                    if ctx.is_cancelled() {
                        p.cancel_requested = true;
                    }
                },
                || {
                    if let Err(e) =
                        crate::backup::run_backup_from(source, config, Arc::clone(&progress))
                    {
                        err = Some(format!("{e:#}"));
                    }
                },
            );
            match err.or_else(|| crate::model::worker::lock_status(&*progress).error.take()) {
                Some(e) => Err(anyhow::anyhow!(e)),
                None => Ok(()),
            }
        }));
    }

    #[cfg(not(feature = "remote"))]
    pub(super) fn remote_backup_picked(&mut self, _: Option<PathBuf>) {}

    pub(super) fn poll_export(&mut self) {
        if let Some(done) = self.export.job.as_ref().and_then(finished) {
            self.export.job = None;
            match done {
                Ok(()) => self.info("Export completed successfully."),
                Err(e) => self.error(format!("Export failed: {e}")),
            }
        }
        if let Some(done) = self.export.remote.as_ref().and_then(finished) {
            self.export.remote = None;
            match done {
                Ok(()) => self.info("Remote backup completed successfully."),
                Err(e) => self.error(format!("Remote backup failed: {e}")),
            }
        }
    }
}

/// Follow an export runner to its end, moving its log lines to the panel and its cancel in.
fn wait_export(
    status: Arc<Mutex<ExportStatus>>,
    sink: &LogSink,
    job: &JobCtx,
) -> anyhow::Result<()> {
    let mut err = None;
    crate::model::worker::wait_spawned(job, &status, |s, ctx| {
        super::tools::drain(sink, &mut s.log_messages);
        if s.total_bytes > 0 {
            ctx.set_bytes(s.current_bytes, s.total_bytes);
        }
        if ctx.is_cancelled() {
            s.cancel_requested = true;
        }
        if s.finished {
            err = s.error.take();
        }
        s.finished
    });
    match err {
        Some(e) => Err(anyhow::anyhow!(e)),
        None => Ok(()),
    }
}

/// The partition context that lets a CHD export resize; `None` streams the disk as it is.
#[cfg(feature = "chd")]
fn chd_partition_context(
    table: crate::partition::PartitionTable,
    partitions: Vec<crate::partition::PartitionInfo>,
    alignment_sectors: u64,
    resize_targets: Vec<(usize, u64)>,
    has_resize: bool,
    source: &Path,
    sink: &LogSink,
) -> Option<export_runner::NativeWholeDiskChdPartitionContext> {
    use std::io::Read;
    let log =
        |level: crate::controller::log::LogLevel, msg: String| super::push_log(sink, level, msg);
    if !crate::backup::single_file_chd::is_supported(&table) {
        if has_resize {
            log(
                crate::controller::log::LogLevel::Warning,
                format!(
                    "{} sources cannot be resized on export; the disk is exported as-is",
                    table.type_name()
                ),
            );
        }
        return None;
    }
    let mut sector0 = [0u8; 512];
    let read = if crate::model::source_reader::is_chd_path(source) {
        crate::rbformats::chd::ChdReader::open(source)
            .map_err(|e| format!("Couldn't read CHD source for partition-table sector: {e}"))
            .and_then(|mut r| {
                r.read_exact(&mut sector0)
                    .map_err(|e| format!("Couldn't read sector 0 from source: {e}"))
            })
    } else {
        std::fs::File::open(source)
            .map_err(|e| format!("Couldn't open source to read partition-table sector: {e}"))
            .and_then(|mut f| {
                f.read_exact(&mut sector0)
                    .map_err(|e| format!("Couldn't read sector 0 from source: {e}"))
            })
    };
    if let Err(e) = read {
        log(
            crate::controller::log::LogLevel::Error,
            format!("{e}; falling back to streaming export."),
        );
        return None;
    }
    Some(export_runner::NativeWholeDiskChdPartitionContext {
        partition_table: table,
        partitions,
        source_partition_table_bytes: sector0,
        alignment_sectors,
        resize_targets,
    })
}
