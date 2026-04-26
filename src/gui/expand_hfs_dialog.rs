//! "Expand HFS Volume…" dialog for the Inspect tab.
//!
//! Step 7 of `docs/hfs_expand_block_size.md`. The user picks a target volume
//! size + allocation block size; a worker thread captures the source HFS
//! volume, builds a fresh blank HFS image of the chosen size, copies every
//! file/dir/metadata across, and wraps the result in a new APM disk image
//! at the user-chosen output path.
//!
//! Source disk is opened read-only; the output is always a brand-new file.

use std::fs::File;
use std::io::{BufReader, Cursor};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::fs::hfs::{create_blank_hfs_sized, HfsFilesystem};
use rusty_backup::fs::hfs_clone::{
    clone_hfs_volume, emit_apm_disk_with_hfs, CloneReport, EmitReport,
};

use super::progress::LogPanel;

/// Allocation block sizes the user can pick. Each maps to a `2 GB`-class
/// ceiling defined by the 65535-block u16 in HFS.
const BLOCK_SIZE_CHOICES: &[u32] = &[4096, 8192, 16384, 32768, 65536];

/// Maximum HFS volume size for a given allocation block size.
fn max_volume_for_block_size(bs: u32) -> u64 {
    65535u64 * bs as u64
}

/// Suggest the smallest block size whose ceiling can hold `target_bytes`.
/// Falls back to the largest available size if nothing fits comfortably.
fn suggest_block_size(target_bytes: u64) -> u32 {
    for &bs in BLOCK_SIZE_CHOICES {
        if max_volume_for_block_size(bs) >= target_bytes {
            return bs;
        }
    }
    *BLOCK_SIZE_CHOICES.last().unwrap()
}

/// Background-thread status for one expand run.
struct ExpandStatus {
    finished: bool,
    error: Option<String>,
    log_messages: Vec<String>,
    current_step: String,
    clone_report: Option<CloneReport>,
    emit_report: Option<EmitReport>,
}

/// Source-volume summary the dialog displays + uses to validate inputs.
#[derive(Debug, Clone)]
pub struct ExpandSource {
    /// Path to the source disk file (image or device path).
    pub source_path: PathBuf,
    /// Byte offset of the source HFS partition on the disk.
    pub partition_offset: u64,
    /// Source partition size in bytes (block_count × 512).
    pub partition_size: u64,
    /// Source allocation block size (drAlBlkSiz), informational.
    pub source_block_size: u32,
    /// Source catalog file size (bytes). Used to size the target catalog so
    /// it can hold every record from the source without running out of nodes.
    pub source_catalog_size: u32,
    /// Source extents-overflow file size (bytes). Used the same way for the
    /// target's extents-overflow B-tree.
    pub source_extents_size: u32,
    /// Source allocated bytes (used = (total_blocks - free_blocks) × block_size).
    pub used_bytes: u64,
    /// Source file count, informational.
    pub file_count: u64,
    /// Source directory count, informational.
    pub dir_count: u64,
    /// Source volume name (carried into the new volume).
    pub volume_name: String,
}

pub struct ExpandHfsDialog {
    pub open: bool,
    source: ExpandSource,
    /// Target size in MiB.
    target_size_mib: u32,
    /// Target allocation block size (one of `BLOCK_SIZE_CHOICES`).
    target_block_size: u32,
    /// User-picked output file path.
    output_path: Option<PathBuf>,
    /// Background work status; `Some` while running and after completion.
    status: Option<Arc<Mutex<ExpandStatus>>>,
    /// Set once the user has acknowledged a finished run.
    completed: bool,
}

impl ExpandHfsDialog {
    pub fn new(source: ExpandSource) -> Self {
        // Default target = current partition size, but never below the source's
        // used space. Round up to nearest MiB.
        let suggested_bytes = source.partition_size.max(source.used_bytes);
        let suggested_mib =
            (suggested_bytes.div_ceil(1024 * 1024)).clamp(1, u32::MAX as u64) as u32;
        let suggested_bs = suggest_block_size(suggested_bytes);
        Self {
            open: true,
            source,
            target_size_mib: suggested_mib,
            target_block_size: suggested_bs,
            output_path: None,
            status: None,
            completed: false,
        }
    }

    /// Render the dialog. Returns `false` once the user closes it for good.
    pub fn show(&mut self, ctx: &egui::Context, log: &mut LogPanel) -> bool {
        if !self.open {
            return false;
        }
        // Snapshot status state so the closure doesn't have to lock twice.
        let status_snapshot: Option<(bool, String, Vec<String>, Option<String>)> =
            self.status.as_ref().and_then(|s| {
                s.lock().ok().map(|g| {
                    (
                        g.finished,
                        g.current_step.clone(),
                        g.log_messages.clone(),
                        g.error.clone(),
                    )
                })
            });

        let mut start_clicked = false;
        let mut close_clicked = false;
        let mut pick_output_clicked = false;

        let mut window_open = self.open;
        egui::Window::new("Expand HFS Volume…")
            .open(&mut window_open)
            .resizable(true)
            .default_width(520.0)
            .show(ctx, |ui| {
                ui.label(egui::RichText::new("Source").strong());
                ui.label(format!("Volume: {}", self.source.volume_name));
                ui.label(format!(
                    "Size: {} MiB ({} bytes)",
                    self.source.partition_size / (1024 * 1024),
                    self.source.partition_size
                ));
                ui.label(format!(
                    "Allocation block size: {} KiB",
                    self.source.source_block_size / 1024
                ));
                ui.label(format!(
                    "Used: {} MiB across {} file(s) in {} folder(s)",
                    self.source.used_bytes / (1024 * 1024),
                    self.source.file_count,
                    self.source.dir_count
                ));

                ui.separator();
                ui.label(egui::RichText::new("Target").strong());

                let busy = status_snapshot
                    .as_ref()
                    .map(|(finished, _, _, _)| !*finished)
                    .unwrap_or(false);

                ui.add_enabled_ui(!busy && !self.completed, |ui| {
                    ui.horizontal(|ui| {
                        ui.label("Allocation block size:");
                        egui::ComboBox::new("expand_hfs_bs", "")
                            .selected_text(format!(
                                "{} KiB (max {} MiB)",
                                self.target_block_size / 1024,
                                max_volume_for_block_size(self.target_block_size) / (1024 * 1024)
                            ))
                            .show_ui(ui, |ui| {
                                for &bs in BLOCK_SIZE_CHOICES {
                                    ui.selectable_value(
                                        &mut self.target_block_size,
                                        bs,
                                        format!(
                                            "{} KiB (max {} MiB)",
                                            bs / 1024,
                                            max_volume_for_block_size(bs) / (1024 * 1024)
                                        ),
                                    );
                                }
                            });
                    });

                    let max_mib_for_bs =
                        (max_volume_for_block_size(self.target_block_size) / (1024 * 1024)) as u32;
                    let min_mib = (self.source.used_bytes.div_ceil(1024 * 1024)).max(1) as u32;
                    if self.target_size_mib > max_mib_for_bs {
                        self.target_size_mib = max_mib_for_bs;
                    }
                    if self.target_size_mib < min_mib {
                        self.target_size_mib = min_mib;
                    }

                    ui.horizontal(|ui| {
                        ui.label("Target size (MiB):");
                        ui.add(egui::Slider::new(
                            &mut self.target_size_mib,
                            min_mib..=max_mib_for_bs,
                        ));
                    });

                    ui.horizontal(|ui| {
                        ui.label("Output file:");
                        let path_text = self
                            .output_path
                            .as_ref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|| "(not chosen)".to_string());
                        ui.label(path_text);
                        if ui.button("Save As…").clicked() {
                            pick_output_clicked = true;
                        }
                    });
                });

                ui.separator();
                if let Some((finished, step, msgs, error)) = &status_snapshot {
                    if !*finished {
                        ui.horizontal(|ui| {
                            ui.spinner();
                            ui.label(step);
                        });
                    } else if let Some(err) = error {
                        ui.colored_label(
                            egui::Color32::from_rgb(255, 100, 100),
                            format!("Failed: {err}"),
                        );
                    } else {
                        ui.colored_label(
                            egui::Color32::from_rgb(100, 200, 100),
                            "Expand complete.",
                        );
                    }
                    if !msgs.is_empty() {
                        egui::ScrollArea::vertical()
                            .max_height(140.0)
                            .id_salt("expand_hfs_log")
                            .show(ui, |ui| {
                                for m in msgs {
                                    ui.label(m);
                                }
                            });
                    }
                }

                ui.horizontal(|ui| {
                    let can_start = !busy && !self.completed && self.output_path.is_some();
                    if ui
                        .add_enabled(can_start, egui::Button::new("Expand"))
                        .clicked()
                    {
                        start_clicked = true;
                    }
                    let close_label = if self.completed { "Close" } else { "Cancel" };
                    if ui
                        .add_enabled(!busy, egui::Button::new(close_label))
                        .clicked()
                    {
                        close_clicked = true;
                    }
                });
            });
        self.open = window_open;

        if pick_output_clicked {
            let default_name = format!(
                "{}-expanded.hda",
                sanitize_filename(&self.source.volume_name)
            );
            if let Some(path) = super::file_dialog()
                .set_file_name(&default_name)
                .add_filter("Disk image", &["hda", "img", "dsk"])
                .save_file()
            {
                self.output_path = Some(path);
            }
        }
        if start_clicked {
            if let Some(out) = self.output_path.clone() {
                self.spawn_worker(out, log);
            }
        }
        if close_clicked {
            self.open = false;
        }

        // Promote a finished run into the "completed" state so the Expand
        // button stays disabled and the close button reads "Close".
        if let Some((finished, _, _, _)) = &status_snapshot {
            if *finished {
                self.completed = true;
            }
        }

        self.open
    }

    fn spawn_worker(&mut self, output: PathBuf, log: &mut LogPanel) {
        let status = Arc::new(Mutex::new(ExpandStatus {
            finished: false,
            error: None,
            log_messages: Vec::new(),
            current_step: "Starting…".to_string(),
            clone_report: None,
            emit_report: None,
        }));
        self.status = Some(Arc::clone(&status));

        let source = self.source.clone();
        let target_size = self.target_size_mib as u64 * 1024 * 1024;
        let target_bs = self.target_block_size;

        log.info(format!(
            "Expanding HFS volume '{}' to {} MiB at {} KiB blocks → {}",
            source.volume_name,
            self.target_size_mib,
            target_bs / 1024,
            output.display()
        ));

        std::thread::spawn(move || {
            let result = run_expand(source, target_size, target_bs, output, &status);
            let mut s = status.lock().expect("expand status lock");
            s.finished = true;
            match result {
                Ok((clone, emit)) => {
                    s.clone_report = Some(clone);
                    s.emit_report = Some(emit);
                    s.current_step = "Done.".into();
                }
                Err(e) => {
                    s.error = Some(e.to_string());
                    s.current_step = "Failed.".into();
                }
            }
        });
    }
}

/// Strip filesystem-unfriendly characters from a volume name for use as a
/// default output filename.
fn sanitize_filename(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ' ') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim().to_string();
    if trimmed.is_empty() {
        "Expanded".to_string()
    } else {
        trimmed
    }
}

/// Worker body for one expand run. Returns the clone+emit reports on success.
fn run_expand(
    source: ExpandSource,
    target_size_bytes: u64,
    target_block_size: u32,
    output_path: PathBuf,
    status: &Arc<Mutex<ExpandStatus>>,
) -> anyhow::Result<(CloneReport, EmitReport)> {
    let push = |s: &str| {
        if let Ok(mut g) = status.lock() {
            g.log_messages.push(s.to_string());
        }
    };
    let step = |s: &str| {
        if let Ok(mut g) = status.lock() {
            g.current_step = s.to_string();
        }
    };

    step("Building blank target volume…");
    // Size the target B-trees from the source's actual on-disk file sizes,
    // with 50% headroom on the catalog (split-based insertion can leave
    // some nodes underfull). The blank-volume builder picks a node size
    // that lets the header-node bitmap address every node, so there's no
    // hard cap on B-tree size beyond what the partition can hold.
    // Cap requested B-tree file sizes at 1 MiB — that's the largest classic
    // HFS B-tree (with node_size=512 and only the header-node bitmap) we can
    // build without map nodes. Quadra ROMs reject larger node sizes with
    // -127. The source's catalog typically has substantial free space, so
    // sizing the new catalog above its used portion is plenty.
    let catalog_min = source
        .source_catalog_size
        .saturating_mul(3)
        .saturating_div(2)
        .min(rusty_backup::fs::hfs::HFS_MAX_BTREE_FILE_SIZE);
    let extents_min = source
        .source_extents_size
        .min(rusty_backup::fs::hfs::HFS_MAX_BTREE_FILE_SIZE);
    let mut target_buf = create_blank_hfs_sized(
        target_size_bytes,
        target_block_size,
        &source.volume_name,
        extents_min,
        catalog_min,
    )
    .map_err(|e| anyhow::anyhow!("create_blank_hfs failed: {e}"))?;
    push(&format!(
        "Target B-trees sized: catalog ≥ {} KiB (source {} KiB), extents-overflow ≥ {} KiB (source {} KiB)",
        catalog_min / 1024,
        source.source_catalog_size / 1024,
        extents_min / 1024,
        source.source_extents_size / 1024
    ));
    push(&format!(
        "Allocated {} MiB target image at {} KiB blocks",
        target_buf.len() / (1024 * 1024),
        target_block_size / 1024
    ));

    step("Reading source catalog…");
    let source_file =
        File::open(&source.source_path).map_err(|e| anyhow::anyhow!("open source: {e}"))?;
    let source_buffered = BufReader::new(
        source_file
            .try_clone()
            .map_err(|e| anyhow::anyhow!("clone source: {e}"))?,
    );
    let source_disk_size = source_file
        .metadata()
        .map(|m| m.len())
        .map_err(|e| anyhow::anyhow!("source metadata: {e}"))?;

    let mut source_fs = HfsFilesystem::open(source_buffered, source.partition_offset)
        .map_err(|e| anyhow::anyhow!("open source HFS: {e}"))?;

    // Capture which fsck issue codes the source already exhibits — pre-
    // existing quirks (e.g. system files with embedded null bytes in their
    // catalog names) faithfully copied to the target shouldn't fail the
    // post-clone verification.
    let source_issue_codes: std::collections::HashSet<String> = source_fs
        .fsck()
        .map(|r| r.errors.iter().map(|e| e.code.clone()).collect())
        .unwrap_or_default();
    if !source_issue_codes.is_empty() {
        push(&format!(
            "Source has pre-existing fsck issue codes: {:?} (will be ignored on target)",
            source_issue_codes
        ));
    }

    step("Cloning files…");
    let target_cursor = Cursor::new(&mut target_buf);
    let mut target_fs = HfsFilesystem::open(target_cursor, 0)
        .map_err(|e| anyhow::anyhow!("open target HFS: {e}"))?;
    let clone_report = clone_hfs_volume(&mut source_fs, &mut target_fs)
        .map_err(|e| anyhow::anyhow!("clone failed: {e}"))?;
    push(&format!(
        "Copied {} file(s) and {} directory(ies); {} bytes data + {} bytes resource fork",
        clone_report.files_copied,
        clone_report.dirs_copied,
        clone_report.data_bytes_copied,
        clone_report.rsrc_bytes_copied
    ));

    step("Verifying target with fsck…");
    {
        // Re-open target read-only to fsck the cloned-and-synced bytes.
        let mut verify_fs = HfsFilesystem::open(Cursor::new(&target_buf), 0)
            .map_err(|e| anyhow::anyhow!("reopen target for fsck: {e}"))?;
        let result = verify_fs
            .fsck()
            .map_err(|e| anyhow::anyhow!("fsck error: {e}"))?;
        let (new_issues, inherited): (Vec<_>, Vec<_>) = result
            .errors
            .iter()
            .partition(|i| !source_issue_codes.contains(&i.code));
        for issue in &inherited {
            push(&format!(
                "fsck (inherited from source): [{}] {}",
                issue.code, issue.message
            ));
        }
        for issue in &new_issues {
            push(&format!("fsck: [{}] {}", issue.code, issue.message));
        }
        if !new_issues.is_empty() {
            let codes: Vec<String> = new_issues.iter().map(|e| e.code.clone()).collect();
            anyhow::bail!(
                "target fsck reported {} new error(s) not present in source: {:?}",
                codes.len(),
                codes
            );
        }
    }

    step("Writing new APM disk image…");
    let output_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&output_path)
        .map_err(|e| anyhow::anyhow!("create output: {e}"))?;
    let mut source_for_apm =
        File::open(&source.source_path).map_err(|e| anyhow::anyhow!("reopen source: {e}"))?;
    let emit_report = {
        // emit_apm_disk_with_hfs needs Read+Write+Seek on the writer.
        let mut writer = output_file;
        emit_apm_disk_with_hfs(
            &mut source_for_apm,
            source_disk_size,
            &target_buf,
            &mut writer,
        )
        .map_err(|e| anyhow::anyhow!("emit APM: {e}"))?
    };
    push(&format!(
        "Wrote {} MiB output: {} driver(s) preserved, HFS at block {} ({} blocks)",
        emit_report.total_bytes / (1024 * 1024),
        emit_report.drivers_copied,
        emit_report.hfs_start_block,
        emit_report.hfs_block_count
    ));

    Ok((clone_report, emit_report))
}

/// Quickly summarise a source HFS volume given a path + partition byte
/// offset. Used by the inspect tab to populate the dialog before showing it.
pub fn summarize_source(
    source_path: &std::path::Path,
    partition_offset: u64,
    partition_size: u64,
) -> anyhow::Result<ExpandSource> {
    let file = File::open(source_path)
        .map_err(|e| anyhow::anyhow!("open {}: {e}", source_path.display()))?;
    let summary = HfsFilesystem::open(BufReader::new(file), partition_offset)
        .map_err(|e| anyhow::anyhow!("open HFS: {e}"))?
        .volume_summary();
    Ok(ExpandSource {
        source_path: source_path.to_path_buf(),
        partition_offset,
        partition_size,
        source_block_size: summary.block_size,
        source_catalog_size: summary.catalog_file_size,
        source_extents_size: summary.extents_file_size,
        used_bytes: summary.used_bytes,
        file_count: summary.file_count as u64,
        dir_count: summary.folder_count as u64,
        volume_name: summary.volume_name,
    })
}
