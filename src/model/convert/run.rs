//! Convert one file: route a disc, decode and refit a floppy, or size and export a hard disk.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};

use super::{Classified, DiscSource, FloppySize, Format, ImageKind, Options, Sizing};
use crate::partition::{PartitionInfo, PartitionTable};
use crate::rbformats::export::{export_whole_disk, ExportFormat};
use part_tables::PartitionSizeOverride;

/// How a log line reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Info,
    Warning,
}

/// What a conversion reports through while it runs.
pub struct Hooks<'a> {
    /// Bytes done of the file's total.
    pub progress: &'a dyn Fn(u64, u64),
    pub cancelled: &'a dyn Fn() -> bool,
    pub log: &'a dyn Fn(Level, &str),
}

/// How one conversion ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Converted {
    /// The data was resized or refitted, so verify compares files rather than bytes.
    pub reshaped: bool,
    /// The verify step's line, when it ran.
    pub verified: Option<String>,
}

/// Convert `src` to `dest` under `opts`, then verify it if asked.
pub fn convert_one(
    src: &Classified,
    dest: &Path,
    opts: &Options,
    hooks: &Hooks<'_>,
) -> Result<Converted> {
    if let Some(why) = opts.refusal(src) {
        bail!("{why}");
    }
    if same_file(src.path.as_path(), dest) {
        bail!("the output would replace the source");
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let (format, _) = opts.target(src.kind);
    let mut done = match src.kind {
        ImageKind::Disc => {
            convert_disc(src, format, dest, opts, hooks)?;
            Converted {
                reshaped: false,
                verified: None,
            }
        }
        ImageKind::Floppy => convert_floppy(src, format, dest, opts, hooks)?,
        ImageKind::HardDisk => convert_hard_disk(src, format, dest, opts, hooks)?,
    };
    if opts.verify {
        (hooks.log)(Level::Info, "  Verifying...");
        let line = super::verify::verify(src, format, dest, done.reshaped, hooks)
            .map_err(|e| anyhow!("verify failed: {e}"))?;
        done.verified = Some(line);
    }
    Ok(done)
}

fn same_file(a: &Path, b: &Path) -> bool {
    a == b
        || match (a.canonicalize(), b.canonicalize()) {
            (Ok(a), Ok(b)) => a == b,
            _ => false,
        }
}

// ---- Discs -------------------------------------------------------------------------------

#[cfg_attr(not(feature = "chd"), allow(unused_variables))]
fn convert_disc(
    src: &Classified,
    format: Format,
    dest: &Path,
    opts: &Options,
    hooks: &Hooks<'_>,
) -> Result<()> {
    let path = src.path.as_path();
    let log = |m: &str| (hooks.log)(Level::Info, m);
    let total = src.size;
    let progress = |b: u64| (hooks.progress)(b, total);
    let disc = src
        .disc
        .ok_or_else(|| anyhow!("the disc's format could not be read"))?;
    match (disc, format) {
        #[cfg(feature = "chd")]
        (DiscSource::Iso | DiscSource::Cue, Format::ChdCd) => {
            crate::rbformats::export::export_whole_disk_chd_cd(
                path,
                dest,
                opts.chd.cd.clone(),
                hooks.cancelled,
                log,
            )
        }
        #[cfg(feature = "chd")]
        (DiscSource::Iso, Format::ChdDvd) => crate::rbformats::export::export_whole_disk_chd(
            path,
            None,
            None,
            &[],
            dest,
            crate::rbformats::chd_options::ChdProfile::Dvd,
            opts.chd.dvd.clone(),
            progress,
            hooks.cancelled,
            log,
        ),
        #[cfg(feature = "chd")]
        (DiscSource::CdChd, Format::BinCue) => crate::rbformats::export::export_whole_disk_bincue(
            path,
            dest,
            opts.multi_bin,
            hooks.cancelled,
            log,
        ),
        (DiscSource::DvdChd, Format::Iso) => export_whole_disk(
            ExportFormat::Raw,
            path,
            None,
            None,
            &[],
            dest,
            progress,
            hooks.cancelled,
            log,
        ),
        #[cfg(feature = "optical")]
        (DiscSource::Iso, Format::BinCue)
        | (DiscSource::Cue, Format::Iso)
        | (DiscSource::CdChd, Format::Iso) => {
            optical_convert(disc, format, path, dest, opts, hooks)
        }
        _ => bail!("{} to {} is not available", disc.label(), format.label()),
    }
}

#[cfg(feature = "optical")]
fn optical_convert(
    disc: DiscSource,
    format: Format,
    path: &Path,
    dest: &Path,
    opts: &Options,
    hooks: &Hooks<'_>,
) -> Result<()> {
    use crate::model::optical_ops::{run_conversion, OutputFormat};
    use opticaldiscs::formats::DiscFormat;
    use std::sync::{Arc, Mutex};

    let source = match disc {
        DiscSource::Iso => DiscFormat::Iso,
        DiscSource::Cue => DiscFormat::BinCue,
        DiscSource::CdChd | DiscSource::DvdChd => DiscFormat::Chd,
    };
    let output = if format == Format::Iso {
        OutputFormat::Iso
    } else {
        OutputFormat::BinCue
    };
    let progress = Arc::new(Mutex::new(crate::optical::convert::ConvertProgress::new()));
    let watch = Arc::clone(&progress);
    let (input, dest_owned, chd) = (path.to_path_buf(), dest.to_path_buf(), opts.chd.cd.clone());
    let worker = std::thread::spawn(move || {
        run_conversion(&input, &dest_owned, Some(source), output, chd, watch)
    });
    while !worker.is_finished() {
        {
            let mut p = crate::model::worker::lock_status(&*progress);
            if (hooks.cancelled)() {
                p.cancel_requested = true;
            }
            (hooks.progress)(p.current_bytes, p.total_bytes);
            for m in p.log_messages.drain(..) {
                (hooks.log)(Level::Info, m.message.as_str());
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    worker
        .join()
        .map_err(|_| anyhow!("the disc conversion stopped unexpectedly"))?
}

// ---- Floppies ----------------------------------------------------------------------------

fn convert_floppy(
    src: &Classified,
    format: Format,
    dest: &Path,
    opts: &Options,
    hooks: &Hooks<'_>,
) -> Result<Converted> {
    let mut flat = read_floppy(src.path.as_path())?;
    let mut reshaped = false;
    if let FloppySize::Refit(bytes) = opts.floppy_size {
        if bytes != flat.len() as u64 {
            let log = |m: &str| (hooks.log)(Level::Info, m);
            let done = super::refit::refit(flat.as_slice(), bytes, &log).map_err(|e| anyhow!(e))?;
            (hooks.log)(
                Level::Info,
                &format!(
                    "  Refitted to {}: {} file(s), {} copied",
                    super::refit::size_label(bytes),
                    done.files,
                    super::classify::size_text(done.bytes)
                ),
            );
            flat = done.image;
            reshaped = true;
        }
    }
    write_floppy(flat.as_slice(), format, dest, hooks)?;
    Ok(Converted {
        reshaped,
        verified: None,
    })
}

/// A floppy's plain sectors, its container peeled.
pub fn read_floppy(path: &Path) -> Result<Vec<u8>> {
    let (mut reader, len) =
        super::open_disk(path).with_context(|| format!("opening {}", path.display()))?;
    if len > super::classify::FLOPPY_MAX {
        bail!("{} is too large for a floppy", path.display());
    }
    reader.seek(SeekFrom::Start(0))?;
    let mut flat = Vec::with_capacity(len as usize);
    reader.read_to_end(&mut flat)?;
    Ok(flat)
}

fn write_floppy(flat: &[u8], format: Format, dest: &Path, hooks: &Hooks<'_>) -> Result<()> {
    let total = flat.len() as u64;
    match format {
        Format::Raw => {
            std::fs::write(dest, flat).with_context(|| format!("writing {}", dest.display()))
        }
        Format::Xdf | Format::Hdm | Format::Dim | Format::D88 => {
            let kind = format
                .export_format()
                .and_then(ExportFormat::to_floppy_container_kind)
                .ok_or_else(|| anyhow!("{} is not a floppy container", format.label()))?;
            let encoded = crate::rbformats::containers::encode_floppy_container(flat, kind)?;
            std::fs::write(dest, encoded).with_context(|| format!("writing {}", dest.display()))
        }
        _ => {
            let export = format
                .export_format()
                .ok_or_else(|| anyhow!("{} is not a floppy format", format.label()))?;
            let dir = tempfile::tempdir()?;
            let flat_path = dir.path().join("floppy.img");
            std::fs::write(&flat_path, flat)?;
            export_whole_disk(
                export,
                &flat_path,
                None,
                None,
                &[],
                dest,
                |b| (hooks.progress)(b, total),
                hooks.cancelled,
                |m| (hooks.log)(Level::Info, m),
            )
        }
    }
}

// ---- Hard disks --------------------------------------------------------------------------

fn convert_hard_disk(
    src: &Classified,
    format: Format,
    dest: &Path,
    opts: &Options,
    hooks: &Hooks<'_>,
) -> Result<Converted> {
    // A gzipped disk (.hdz, .gz) is unpacked to a scratch file first, streaming.
    let (path, _tmp) = if crate::model::source_reader::is_gzip_wrapped_path(src.path.as_path()) {
        (hooks.log)(Level::Info, "  Unpacking...");
        let (p, t) = crate::model::image_prep::prepare_disk_image_path(src.path.as_path(), true)?;
        (p, t)
    } else {
        (src.path.clone(), None)
    };
    let plan = if opts.sizing.resizes() {
        plan_sizes(path.as_path(), opts.sizing, opts.measure_slow, hooks)?
    } else {
        None
    };
    let export = format
        .export_format()
        .ok_or_else(|| anyhow!("{} is not a hard disk format", format.label()))?;
    let reshaped = plan.as_ref().is_some_and(|p| !p.targets.is_empty());
    let total = src.size;
    let progress = |b: u64| (hooks.progress)(b, total);
    let log = |m: &str| (hooks.log)(Level::Info, m);
    if export == ExportFormat::Chd {
        return chd_hard_disk(path.as_path(), dest, plan, opts, hooks).map(|reshaped| Converted {
            reshaped,
            verified: None,
        });
    }
    let overrides = plan.map(|p| p.overrides).unwrap_or_default();
    if !reshaped {
        export_whole_disk(
            export,
            path.as_path(),
            None,
            None,
            &[],
            dest,
            progress,
            hooks.cancelled,
            log,
        )?;
        return Ok(Converted {
            reshaped,
            verified: None,
        });
    }
    // The VHD writer is the one that rebuilds every table at new sizes; the others take its output.
    let scratch = match export {
        ExportFormat::Vhd => dest.to_path_buf(),
        ExportFormat::Raw => dest.to_path_buf(),
        _ => scratch_beside(dest),
    };
    let result = (|| -> Result<()> {
        export_whole_disk(
            ExportFormat::Vhd,
            path.as_path(),
            None,
            None,
            overrides.as_slice(),
            scratch.as_path(),
            progress,
            hooks.cancelled,
            log,
        )?;
        match export {
            ExportFormat::Vhd => Ok(()),
            ExportFormat::Raw => {
                let file = std::fs::OpenOptions::new().write(true).open(dest)?;
                let len = file.metadata()?.len();
                file.set_len(len.saturating_sub(512))?;
                Ok(())
            }
            _ => export_whole_disk(
                export,
                scratch.as_path(),
                None,
                None,
                &[],
                dest,
                |b| (hooks.progress)(b, total),
                hooks.cancelled,
                |m| (hooks.log)(Level::Info, m),
            ),
        }
    })();
    if scratch.as_path() != dest {
        let _ = std::fs::remove_file(&scratch);
    }
    result?;
    Ok(Converted {
        reshaped,
        verified: None,
    })
}

/// A hidden scratch file beside `dest`, so it lands on the output's disk.
fn scratch_beside(dest: &Path) -> PathBuf {
    let stem = dest.file_stem().and_then(|s| s.to_str()).unwrap_or("disk");
    dest.with_file_name(format!(".{stem}.resizing.vhd"))
}

#[cfg(feature = "chd")]
fn chd_hard_disk(
    path: &Path,
    dest: &Path,
    plan: Option<SizePlan>,
    opts: &Options,
    hooks: &Hooks<'_>,
) -> Result<bool> {
    use crate::backup::single_file_chd as sfc;
    let mut progress = |b: u64| (hooks.progress)(b, 0);
    let mut log = |m: &str| (hooks.log)(Level::Info, m);
    if let Some(plan) = plan.filter(|p| !p.targets.is_empty()) {
        if sfc::is_supported(&plan.table) {
            let mut sector0 = [0u8; 512];
            let (mut reader, _) = super::open_disk(path)?;
            reader.read_exact(&mut sector0)?;
            sfc::run_export(
                sfc::SingleFileChdExportInputs {
                    source_path: path,
                    partition_table: &plan.table,
                    partitions: plan.partitions.as_slice(),
                    source_partition_table_bytes: &sector0,
                    alignment_sectors: plan.alignment_sectors,
                    dest_path: dest,
                    chd_options: opts.chd.hd.clone(),
                    is_dvd: false,
                    resize_targets: Some(plan.targets.as_slice()),
                },
                &mut progress,
                hooks.cancelled,
                &mut log,
            )?;
            return Ok(true);
        }
        (hooks.log)(
            Level::Warning,
            &format!(
                "  A {} disk can't be resized into a CHD; it keeps its size.",
                plan.table.type_name()
            ),
        );
    }
    crate::rbformats::export::export_whole_disk_chd(
        path,
        None,
        None,
        &[],
        dest,
        crate::rbformats::chd_options::ChdProfile::Hd,
        opts.chd.hd.clone(),
        progress,
        hooks.cancelled,
        log,
    )?;
    Ok(false)
}

#[cfg(not(feature = "chd"))]
fn chd_hard_disk(
    _path: &Path,
    _dest: &Path,
    _plan: Option<SizePlan>,
    _opts: &Options,
    _hooks: &Hooks<'_>,
) -> Result<bool> {
    bail!("This copy was built without CHD support.")
}

/// A hard disk's new partition sizes.
pub struct SizePlan {
    pub table: PartitionTable,
    pub partitions: Vec<PartitionInfo>,
    pub overrides: Vec<PartitionSizeOverride>,
    /// `(index, new size)` for each partition that changes.
    pub targets: Vec<(usize, u64)>,
    pub alignment_sectors: u64,
}

/// Measure each partition and size it under `sizing`; `None` when the disk has no table.
pub fn plan_sizes(
    path: &Path,
    sizing: Sizing,
    measure_slow: bool,
    hooks: &Hooks<'_>,
) -> Result<Option<SizePlan>> {
    let info = |m: String| (hooks.log)(Level::Info, m.as_str());
    let warn = |m: String| (hooks.log)(Level::Warning, m.as_str());
    let (mut reader, _) = super::open_disk(path)?;
    let table = PartitionTable::detect(&mut reader).map_err(|e| anyhow!("{e}"))?;
    if matches!(table, PartitionTable::None { .. }) {
        warn("  No partition table, so the disk keeps its size.".to_string());
        return Ok(None);
    }
    let partitions = table.partitions();
    let mut in_place: HashMap<usize, u64> = HashMap::new();
    let mut defrag: HashMap<usize, u64> = HashMap::new();
    for p in partitions.iter().filter(|p| !p.is_extended_container) {
        if (hooks.cancelled)() {
            bail!("cancelled");
        }
        let gate = p.gate_type_byte();
        let type_string = p.partition_type_string.as_deref();
        let name = crate::fs::fs_name_for(gate, type_string);
        if crate::fs::is_expensive_minimum(gate, type_string) && !measure_slow {
            info(format!(
                "  Partition {} ({name}) keeps its size: measuring it is slow, and \
                 \"Measure slow filesystems too\" is off.",
                p.index
            ));
            continue;
        }
        let (part_reader, _) = super::open_disk(path)?;
        let result = crate::fs::partition_minimum_size_cancellable(
            part_reader,
            p.start_lba * 512,
            gate,
            type_string,
            p.size_bytes,
            true,
            None,
            &|_| {},
            hooks.cancelled,
        );
        if let crate::fs::MinimumResult::Computed {
            in_place: ip,
            defragmented,
            ..
        } = result
        {
            if let Some(v) = ip {
                in_place.insert(p.index, v);
            }
            if let Some(v) = defragmented {
                defrag.insert(p.index, v);
            }
        }
    }
    let image = PathBuf::from(path);
    let configs = crate::model::export_runner::build_partition_configs(
        partitions.as_slice(),
        None,
        &in_place,
        &defrag,
        Some(&image),
    );
    let mut overrides = Vec::new();
    let mut targets = Vec::new();
    for cfg in &configs {
        let Some(p) = partitions.iter().find(|p| p.index == cfg.index) else {
            continue;
        };
        let measured = in_place.contains_key(&cfg.index) || defrag.contains_key(&cfg.index);
        let new = if measured {
            sizing.size_for(cfg.original_size, cfg.minimum_size, cfg.max_size)
        } else {
            cfg.original_size
        };
        if measured && new == cfg.original_size {
            info(format!(
                "  Partition {} ({}) can't get smaller; it keeps its size.",
                cfg.index, cfg.type_name
            ));
        } else if new != cfg.original_size {
            info(format!(
                "  Partition {} ({}): {} -> {}",
                cfg.index,
                cfg.type_name,
                super::classify::size_text(cfg.original_size),
                super::classify::size_text(new)
            ));
            targets.push((cfg.index, new));
        }
        overrides.push(PartitionSizeOverride::size_only(
            cfg.index,
            p.start_lba,
            cfg.original_size,
            new,
        ));
    }
    let alignment_sectors = crate::partition::alignment::detect_alignment(&table).alignment_sectors;
    Ok(Some(SizePlan {
        table,
        partitions,
        overrides,
        targets,
        alignment_sectors,
    }))
}
