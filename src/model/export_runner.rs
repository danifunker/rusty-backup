//! Export configuration + background-thread orchestration.
//!
//! Owns the data structures for the "Export Disk Image" popup
//! ([`PartitionExportConfig`], [`SizeMode`]) and the shared status
//! struct that the GUI polls while a worker thread runs ([`ExportStatus`]).
//!
//! Each `start_*` function takes the prepared inputs, spawns a worker thread
//! that drives the appropriate `rbformats::export` API, and returns nothing —
//! the caller stores the [`Arc<Mutex<ExportStatus>>`] handed to the function
//! and polls it from the GUI thread.
//!
//! Extracted from `gui/inspect_tab.rs` per §5 of `docs/codecleanup.md`.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::backup::metadata::BackupMetadata;
use crate::clonezilla::metadata::ClonezillaImage;
use crate::fs::fat::resize_fat_in_place;
use crate::fs::hfs_max_growable_size;
use crate::model::size_mode::SizeMode;
use crate::partition::{self, PartitionInfo, PartitionSizeOverride};
use crate::rbformats::chd_options::{ChdOptions, ChdProfile};
use crate::rbformats::export::{
    export_clonezilla_disk, export_clonezilla_partition, export_partition, export_whole_disk,
    export_whole_disk_bincue, export_whole_disk_chd, export_whole_disk_chd_cd, ExportFormat,
};
use crate::rbformats::vhd::build_vhd_footer;

/// Shared status for a background export operation. Worker threads write
/// progress / log lines / errors here; the GUI thread polls and renders.
pub struct ExportStatus {
    pub finished: bool,
    pub error: Option<String>,
    pub log_messages: Vec<String>,
    pub current_bytes: u64,
    pub total_bytes: u64,
    pub cancel_requested: bool,
}

/// Per-partition export configuration.
#[derive(Debug, Clone)]
pub struct PartitionExportConfig {
    pub index: usize,
    pub type_name: String,
    pub original_size: u64,
    pub minimum_size: u64,
    /// Upper bound (bytes) on this partition's export size. For classic HFS
    /// this reflects the 65535-block / Volume Bitmap ceiling, so the user
    /// can't request a resize that would corrupt the MDB. `None` means no
    /// filesystem-specific cap.
    pub max_size: Option<u64>,
    pub choice: SizeMode,
    pub custom_size_mib: u32,
}

impl PartitionExportConfig {
    pub fn effective_size(&self) -> u64 {
        self.choice
            .effective_size(self.original_size, self.minimum_size, self.custom_size_mib)
    }
}

fn new_status(total_bytes: u64) -> Arc<Mutex<ExportStatus>> {
    Arc::new(Mutex::new(ExportStatus {
        finished: false,
        error: None,
        log_messages: Vec::new(),
        current_bytes: 0,
        total_bytes,
        cancel_requested: false,
    }))
}

/// Build the per-partition config list. For classic HFS partitions, probes
/// the source image to determine the volume's u16/VBM-derived size ceiling.
pub fn build_partition_configs(
    partitions: &[PartitionInfo],
    backup_metadata: Option<&BackupMetadata>,
    partition_min_sizes: &HashMap<usize, u64>,
    image_file_path: Option<&PathBuf>,
) -> Vec<PartitionExportConfig> {
    let mut source_reader: Option<BufReader<File>> = image_file_path
        .and_then(|p| File::open(p).ok())
        .map(BufReader::new);

    let mut configs = Vec::new();
    for part in partitions {
        if part.is_extended_container {
            continue;
        }
        let min_size = backup_metadata
            .and_then(|m| {
                m.partitions
                    .iter()
                    .find(|pm| pm.index == part.index)
                    .map(|pm| pm.imaged_size_bytes)
            })
            .filter(|&sz| sz > 0)
            .or_else(|| partition_min_sizes.get(&part.index).copied())
            .unwrap_or(part.size_bytes);

        let max_size = if let Some(reader) = source_reader.as_mut() {
            let is_hfs_candidate = part
                .partition_type_string
                .as_deref()
                .map(|s| s.eq_ignore_ascii_case("Apple_HFS"))
                .unwrap_or(false);
            if is_hfs_candidate {
                hfs_max_growable_size(reader, part.start_lba * 512)
            } else {
                None
            }
        } else {
            None
        };

        configs.push(PartitionExportConfig {
            index: part.index,
            type_name: part.type_name.clone(),
            original_size: part.size_bytes,
            minimum_size: min_size,
            max_size,
            choice: SizeMode::Original,
            custom_size_mib: (part.size_bytes / (1024 * 1024)) as u32,
        });
    }
    configs
}

/// Build whole-disk size overrides from the per-partition configs, looking
/// up each partition's `start_lba` in `partitions`.
pub fn build_size_overrides(
    configs: &[PartitionExportConfig],
    partitions: &[PartitionInfo],
) -> Vec<PartitionSizeOverride> {
    configs
        .iter()
        .map(|cfg| {
            let start_lba = partitions
                .iter()
                .find(|p| p.index == cfg.index)
                .map(|p| p.start_lba)
                .unwrap_or(0);
            PartitionSizeOverride::size_only(
                cfg.index,
                start_lba,
                cfg.original_size,
                cfg.effective_size(),
            )
        })
        .collect()
}

/// Map of partition index to chosen export size, derived from the configs.
pub fn build_size_map(configs: &[PartitionExportConfig]) -> HashMap<usize, u64> {
    configs
        .iter()
        .map(|cfg| (cfg.index, cfg.effective_size()))
        .collect()
}

/// Spawn a Clonezilla whole-disk export. Returns the status the GUI should poll.
pub fn start_clonezilla_whole_disk(
    format: ExportFormat,
    cz_image: ClonezillaImage,
    source: PathBuf,
    dest: PathBuf,
    overrides: Vec<PartitionSizeOverride>,
    total_bytes: u64,
) -> Arc<Mutex<ExportStatus>> {
    let status = new_status(total_bytes);
    let status_thread = Arc::clone(&status);

    std::thread::spawn(move || {
        let _wake = crate::os::wakelock::acquire("Rusty Backup: Clonezilla disk export");
        let status2 = Arc::clone(&status_thread);
        let status3 = Arc::clone(&status_thread);
        let result = export_clonezilla_disk(
            format,
            &cz_image,
            &source,
            &dest,
            &overrides,
            move |bytes| {
                if let Ok(mut s) = status2.lock() {
                    s.current_bytes = bytes;
                }
            },
            move || status3.lock().map(|s| s.cancel_requested).unwrap_or(false),
            |msg| {
                if let Ok(mut s) = status_thread.lock() {
                    s.log_messages.push(msg.to_string());
                }
            },
        );
        if let Ok(mut s) = status_thread.lock() {
            s.finished = true;
            if let Err(e) = result {
                s.error = Some(format!("{e:#}"));
            }
        }
    });

    status
}

/// Spawn a native (rusty-backup or raw image) whole-disk export.
#[allow(clippy::too_many_arguments)]
pub fn start_native_whole_disk(
    format: ExportFormat,
    source: PathBuf,
    meta: Option<BackupMetadata>,
    overrides: Vec<PartitionSizeOverride>,
    dest: PathBuf,
    total_bytes: u64,
) -> Arc<Mutex<ExportStatus>> {
    let status = new_status(total_bytes);
    let status_thread = Arc::clone(&status);

    std::thread::spawn(move || {
        let _wake = crate::os::wakelock::acquire("Rusty Backup: whole-disk export");
        let status2 = Arc::clone(&status_thread);
        let status3 = Arc::clone(&status_thread);
        let result = export_whole_disk(
            format,
            &source,
            meta.as_ref(),
            None,
            &overrides,
            &dest,
            move |bytes| {
                if let Ok(mut s) = status2.lock() {
                    s.current_bytes = bytes;
                }
            },
            move || status3.lock().map(|s| s.cancel_requested).unwrap_or(false),
            |msg| {
                if let Ok(mut s) = status_thread.lock() {
                    s.log_messages.push(msg.to_string());
                }
            },
        );
        if let Ok(mut s) = status_thread.lock() {
            s.finished = true;
            if let Err(e) = result {
                s.error = Some(format!("{e:#}"));
            }
        }
    });

    status
}

/// Spawn a CHD whole-disk export for raw images / devices. CHD doesn't
/// support backup-folder reconstruction or per-partition splitting (those
/// produce headless partition slices that no emulator can consume), so the
/// caller is responsible for gating availability before reaching here.
///
/// `format` must be one of `Chd`, `ChdDvd`, `ChdCd`, or `BinCue`. For the
/// first three, `chd_options` selects the user's codec/hunk-size choice
/// (None = profile defaults). For `BinCue`, `bincue_multi_bin` toggles
/// single- vs. multi-bin output.
/// Optional partition context that lets `start_native_whole_disk_chd`
/// route through `single_file_chd::run_export` (resize-capable) instead
/// of the legacy raw-stream `export_whole_disk_chd`. Only meaningful for
/// `Chd` / `ChdDvd` formats; the optical paths ignore it.
pub struct NativeWholeDiskChdPartitionContext {
    pub partition_table: crate::partition::PartitionTable,
    pub partitions: Vec<PartitionInfo>,
    pub source_partition_table_bytes: [u8; 512],
    pub alignment_sectors: u64,
    /// `(partition_index, new_size_bytes)` per partition. Empty (or all
    /// no-op) routes to the no-scratch fast path inside `run_export`.
    pub resize_targets: Vec<(usize, u64)>,
}

pub fn start_native_whole_disk_chd(
    format: ExportFormat,
    source: PathBuf,
    dest: PathBuf,
    chd_options: Option<ChdOptions>,
    bincue_multi_bin: bool,
    total_bytes: u64,
    partition_context: Option<NativeWholeDiskChdPartitionContext>,
) -> Arc<Mutex<ExportStatus>> {
    let status = new_status(total_bytes);
    let status_thread = Arc::clone(&status);

    std::thread::spawn(move || {
        let _wake = crate::os::wakelock::acquire("Rusty Backup: CHD/BinCue export");
        let status2 = Arc::clone(&status_thread);
        let status3 = Arc::clone(&status_thread);
        let status_log = Arc::clone(&status_thread);
        let progress_cb = move |bytes| {
            if let Ok(mut s) = status2.lock() {
                s.current_bytes = bytes;
            }
        };
        let cancel_cb = move || status3.lock().map(|s| s.cancel_requested).unwrap_or(false);
        let log_cb = |msg: &str| {
            if let Ok(mut s) = status_log.lock() {
                s.log_messages.push(msg.to_string());
            }
        };

        let result = match format {
            ExportFormat::Chd | ExportFormat::ChdDvd if partition_context.is_some() => {
                let ctx = partition_context.expect("checked just above");
                let p_cb = progress_cb;
                let c_cb = cancel_cb;
                let l_cb = log_cb;
                let mut p_cb_dyn = p_cb;
                let c_cb_dyn = c_cb;
                let mut l_cb_dyn = l_cb;
                crate::backup::single_file_chd::run_export(
                    crate::backup::single_file_chd::SingleFileChdExportInputs {
                        source_path: &source,
                        partition_table: &ctx.partition_table,
                        partitions: &ctx.partitions,
                        source_partition_table_bytes: &ctx.source_partition_table_bytes,
                        alignment_sectors: ctx.alignment_sectors,
                        dest_path: &dest,
                        chd_options,
                        is_dvd: matches!(format, ExportFormat::ChdDvd),
                        resize_targets: if ctx.resize_targets.is_empty() {
                            None
                        } else {
                            Some(ctx.resize_targets.as_slice())
                        },
                    },
                    &mut p_cb_dyn,
                    &c_cb_dyn,
                    &mut l_cb_dyn,
                )
            }
            ExportFormat::Chd => export_whole_disk_chd(
                &source,
                None,
                None,
                &[],
                &dest,
                ChdProfile::Hd,
                chd_options,
                progress_cb,
                cancel_cb,
                log_cb,
            ),
            ExportFormat::ChdDvd => export_whole_disk_chd(
                &source,
                None,
                None,
                &[],
                &dest,
                ChdProfile::Dvd,
                chd_options,
                progress_cb,
                cancel_cb,
                log_cb,
            ),
            ExportFormat::ChdCd => {
                export_whole_disk_chd_cd(&source, &dest, chd_options, cancel_cb, log_cb)
            }
            ExportFormat::BinCue => {
                export_whole_disk_bincue(&source, &dest, bincue_multi_bin, cancel_cb, log_cb)
            }
            other => Err(anyhow::anyhow!(
                "start_native_whole_disk_chd called with non-CHD format {:?}",
                other
            )),
        };

        if let Ok(mut s) = status_thread.lock() {
            s.finished = true;
            if let Err(e) = result {
                s.error = Some(format!("{e:#}"));
            }
        }
    });

    status
}

/// Inputs for [`start_per_partition`]. Bundled to keep the call site readable.
pub struct PerPartitionInputs {
    pub format: ExportFormat,
    pub source_folder: Option<PathBuf>,
    pub source_image: Option<PathBuf>,
    pub meta: Option<BackupMetadata>,
    pub partitions: Vec<PartitionInfo>,
    pub cz_image: Option<ClonezillaImage>,
    pub size_map: HashMap<usize, u64>,
    pub dest_folder: PathBuf,
    pub total_bytes: u64,
}

/// Spawn a per-partition export across all sources (Clonezilla, native
/// backup, or raw image / device).
pub fn start_per_partition(inputs: PerPartitionInputs) -> Arc<Mutex<ExportStatus>> {
    let PerPartitionInputs {
        format,
        source_folder,
        source_image,
        meta,
        partitions,
        cz_image,
        size_map,
        dest_folder,
        total_bytes,
    } = inputs;
    let ext = format.extension();

    let status = new_status(total_bytes);
    let status_thread = Arc::clone(&status);

    std::thread::spawn(move || {
        let _wake = crate::os::wakelock::acquire("Rusty Backup: per-partition export");
        let result = run_per_partition(
            format,
            ext,
            source_folder,
            source_image,
            meta,
            partitions,
            cz_image,
            size_map,
            dest_folder,
            &status_thread,
        );

        if let Ok(mut s) = status_thread.lock() {
            s.finished = true;
            if let Err(e) = result {
                s.error = Some(format!("{e:#}"));
            }
        }
    });

    status
}

#[allow(clippy::too_many_arguments)]
fn run_per_partition(
    format: ExportFormat,
    ext: &str,
    source_folder: Option<PathBuf>,
    source_image: Option<PathBuf>,
    meta: Option<BackupMetadata>,
    partitions: Vec<PartitionInfo>,
    cz_image: Option<ClonezillaImage>,
    size_map: HashMap<usize, u64>,
    dest_folder: PathBuf,
    status: &Arc<Mutex<ExportStatus>>,
) -> anyhow::Result<()> {
    let mut overall_written: u64 = 0;

    if let (Some(cz), Some(_folder)) = (&cz_image, &source_folder) {
        for cz_part in &cz.partitions {
            if status.lock().map(|s| s.cancel_requested).unwrap_or(false) {
                anyhow::bail!("export cancelled");
            }
            if cz_part.is_extended || cz_part.partclone_files.is_empty() {
                continue;
            }

            let export_size = size_map
                .get(&cz_part.index)
                .copied()
                .unwrap_or(cz_part.size_bytes());
            let dest_path = dest_folder.join(format!("partition-{}.{}", cz_part.index, ext));

            if let Ok(mut s) = status.lock() {
                s.log_messages.push(format!(
                    "Exporting partition-{} ({}) to {}",
                    cz_part.index,
                    partition::format_size(export_size),
                    dest_path.display()
                ));
            }

            let base_written = overall_written;
            let status_progress = Arc::clone(status);
            let status_cancel = Arc::clone(status);
            let status_log = Arc::clone(status);
            export_clonezilla_partition(
                format,
                &cz_part.partclone_files,
                &dest_path,
                Some(export_size),
                move |bytes| {
                    if let Ok(mut s) = status_progress.lock() {
                        s.current_bytes = base_written + bytes;
                    }
                },
                move || {
                    status_cancel
                        .lock()
                        .map(|s| s.cancel_requested)
                        .unwrap_or(false)
                },
                |msg| {
                    if let Ok(mut s) = status_log.lock() {
                        s.log_messages.push(msg.to_string());
                    }
                },
            )?;

            overall_written += export_size;
        }
    } else if let (Some(folder), Some(meta)) = (&source_folder, &meta) {
        for pm in &meta.partitions {
            if status.lock().map(|s| s.cancel_requested).unwrap_or(false) {
                anyhow::bail!("export cancelled");
            }
            if pm.compressed_files.is_empty() {
                continue;
            }

            let export_size = size_map
                .get(&pm.index)
                .copied()
                .unwrap_or(pm.original_size_bytes);
            let data_file = &pm.compressed_files[0];
            let data_path = folder.join(data_file);
            let dest_path = dest_folder.join(format!("partition-{}.{}", pm.index, ext));

            if let Ok(mut s) = status.lock() {
                s.log_messages.push(format!(
                    "Exporting partition-{} ({}) to {}",
                    pm.index,
                    partition::format_size(export_size),
                    dest_path.display()
                ));
            }

            let base_written = overall_written;
            let status_progress = Arc::clone(status);
            let status_cancel = Arc::clone(status);
            let status_log = Arc::clone(status);
            export_partition(
                format,
                &data_path,
                &meta.compression_type,
                &dest_path,
                Some(export_size),
                move |bytes| {
                    if let Ok(mut s) = status_progress.lock() {
                        s.current_bytes = base_written + bytes;
                    }
                },
                move || {
                    status_cancel
                        .lock()
                        .map(|s| s.cancel_requested)
                        .unwrap_or(false)
                },
                |msg| {
                    if let Ok(mut s) = status_log.lock() {
                        s.log_messages.push(msg.to_string());
                    }
                },
            )?;

            if export_size != pm.original_size_bytes {
                let new_sectors = (export_size / 512) as u32;
                let mut rw = OpenOptions::new().read(true).write(true).open(&dest_path)?;
                let status_log = Arc::clone(status);
                resize_fat_in_place(&mut rw, 0, new_sectors, &mut |msg| {
                    if let Ok(mut s) = status_log.lock() {
                        s.log_messages.push(msg.to_string());
                    }
                })?;
            }

            overall_written += export_size;
        }
    } else if let Some(image_path) = &source_image {
        if format.is_floppy_only() {
            anyhow::bail!(
                "{} per-partition export from a raw disk image is not supported — use Whole Disk export for floppy-only formats",
                format.description()
            );
        }
        for part in &partitions {
            if status.lock().map(|s| s.cancel_requested).unwrap_or(false) {
                anyhow::bail!("export cancelled");
            }
            if part.is_extended_container {
                continue;
            }

            let export_size = size_map
                .get(&part.index)
                .copied()
                .unwrap_or(part.size_bytes);
            let dest_path = dest_folder.join(format!("partition-{}.{}", part.index, ext));
            let offset = part.start_lba * 512;

            if let Ok(mut s) = status.lock() {
                s.log_messages.push(format!(
                    "Exporting partition-{} ({}) to {}",
                    part.index,
                    partition::format_size(export_size),
                    dest_path.display()
                ));
            }

            let read_limit = export_size.min(part.size_bytes);
            let file = File::open(image_path)?;
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(offset))?;
            let mut limited = reader.take(read_limit);

            let mut writer = BufWriter::new(File::create(&dest_path)?);

            if format == ExportFormat::TwoMg {
                let hdr = crate::rbformats::twomg::build_twomg_header(export_size);
                writer.write_all(&hdr)?;
            }

            let mut buf = vec![0u8; 256 * 1024];
            let mut total: u64 = 0;
            let base_written = overall_written;
            loop {
                if status.lock().map(|s| s.cancel_requested).unwrap_or(false) {
                    anyhow::bail!("export cancelled");
                }
                let n = limited.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                writer.write_all(&buf[..n])?;
                total += n as u64;
                if let Ok(mut s) = status.lock() {
                    s.current_bytes = base_written + total;
                }
            }

            if total < export_size {
                let pad = export_size - total;
                let zeros = vec![0u8; 256 * 1024];
                let mut remaining = pad;
                while remaining > 0 {
                    let n = (remaining as usize).min(zeros.len());
                    writer.write_all(&zeros[..n])?;
                    remaining -= n as u64;
                }
                total = export_size;
            }
            writer.flush()?;

            if export_size != part.size_bytes {
                let new_sectors = (export_size / 512) as u32;
                let fs_offset = if format == ExportFormat::TwoMg { 64 } else { 0 };
                let status_log = Arc::clone(status);
                resize_fat_in_place(writer.get_mut(), fs_offset, new_sectors, &mut |msg| {
                    if let Ok(mut s) = status_log.lock() {
                        s.log_messages.push(msg.to_string());
                    }
                })?;
                let end = if format == ExportFormat::TwoMg {
                    64 + total
                } else {
                    total
                };
                writer.seek(SeekFrom::Start(end))?;
            }

            if format == ExportFormat::Vhd {
                let footer = build_vhd_footer(total);
                writer.write_all(&footer)?;
            }
            writer.flush()?;

            overall_written += export_size;
        }
    } else {
        anyhow::bail!("no source available for export");
    }

    Ok(())
}
