//! Read a backup source's partition table and the cheap per-partition minimum sizes.
//!
//! The Backup tab runs this as a [`crate::model::job::Job`] so opening a device
//! (which may prompt for administrator credentials), detecting the table and
//! sizing each partition no longer freeze the window
//! (`docs/long_operations_inventory.md`, item 1). Expensive filesystems come back
//! [`MinimumResult::Deferred`], as before, for "Calc min" to compute later.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use crate::fs::{self, MinimumResult};
use crate::model::job::JobCtx;
use crate::partition::{PartitionInfo, PartitionTable};
use crate::rbformats::ReadSeek;

/// One partition's minimum-size probe.
pub struct ProbeResult {
    pub index: usize,
    pub size_bytes: u64,
    pub type_byte: u8,
    pub type_string: Option<String>,
    pub result: MinimumResult,
}

/// What a scan found.
pub struct SourceScan {
    pub partitions: Vec<PartitionInfo>,
    /// e.g. "MBR", "APM (big-endian)", "No partition table (superfloppy)".
    pub table_desc: String,
    /// Lines for the log, in order, before the per-partition results; `true` marks a warning.
    pub notes: Vec<(bool, String)>,
    /// Cheap minimum sizes, in partition order; extended containers are skipped.
    pub probes: Vec<ProbeResult>,
    /// The open source, kept so workers can clone it instead of reopening (and re-elevating).
    pub source_file: Option<File>,
}

/// Why a scan stopped: `load_error` is shown under the source, `log_error` goes to the log.
#[derive(Debug)]
pub struct ScanFailure {
    pub load_error: String,
    pub log_error: Option<String>,
}

impl std::fmt::Display for ScanFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.load_error)
    }
}

impl std::error::Error for ScanFailure {}

fn failure(load_error: String, log_error: Option<String>) -> anyhow::Error {
    ScanFailure {
        load_error,
        log_error,
    }
    .into()
}

/// The "Load partition info" scan: open the source (elevating a device), detect the table, size each partition.
pub fn scan_source(path: &Path, is_device: bool, ctx: &JobCtx) -> anyhow::Result<SourceScan> {
    let file: crate::os::SourceHandle = if is_device {
        ctx.set_phase("Opening device (may ask for administrator credentials)...");
        match crate::os::open_source_for_reading(path) {
            Ok(elevated) => elevated.into_parts().0,
            Err(e) => {
                let msg = format!("Cannot access device: {e}");
                return Err(failure(msg.clone(), Some(msg)));
            }
        }
    } else {
        match File::open(path) {
            Ok(f) => f.into(),
            Err(e) => {
                let msg = format!("Cannot open source: {e}");
                return Err(failure(msg.clone(), Some(msg)));
            }
        }
    };
    let source_handle = file.try_clone().ok();
    // A device cannot answer seek(End); carry the length the OS reports instead.
    let mut reader: Box<dyn ReadSeek> = if is_device {
        Box::new(crate::os::known_len_source(file, path))
    } else {
        Box::new(BufReader::new(file))
    };
    ctx.set_phase("Reading the partition table...");
    let table = PartitionTable::detect(&mut reader).map_err(|e| {
        failure(
            format!("Could not read partition table: {e}"),
            Some(format!("Partition scan failed: {e}")),
        )
    })?;
    let table_desc = if matches!(table, PartitionTable::None { .. }) {
        "No partition table (superfloppy)".to_string()
    } else {
        match table.byte_order_name() {
            Some(order) => format!("{} ({order})", table.type_name()),
            None => table.type_name().to_string(),
        }
    };
    let partitions = table.partitions();
    let owned = path.to_path_buf();
    let handle = source_handle.as_ref();
    let probes = probe_all(&partitions, ctx, |_| {
        let clone = handle?.try_clone().ok()?;
        // AFFS / Atari / DFS / CBM size themselves via seek(End), which a device refuses.
        Some(if is_device {
            Box::new(crate::os::known_len_source(clone, &owned)) as Box<dyn ReadSeek>
        } else {
            Box::new(BufReader::new(clone))
        })
    })?;
    let source_file = source_handle
        .and_then(|f| f.try_clone().ok())
        .and_then(|h| h.into_file().ok());
    Ok(SourceScan {
        partitions,
        table_desc,
        notes: Vec::new(),
        probes,
        source_file,
    })
}

/// Read just the partition table of a device (elevating to open it), for picking a restore target.
pub fn read_device_table(
    path: &Path,
    ctx: &JobCtx,
) -> Result<(Vec<PartitionInfo>, String), ScanFailure> {
    ctx.set_phase("Opening device (may ask for administrator credentials)...");
    let file = crate::os::open_source_for_reading(path)
        .map_err(|e| ScanFailure {
            load_error: format!("Cannot access device: {e}"),
            log_error: Some(format!("Cannot access target device: {e}")),
        })?
        .into_parts()
        .0;
    ctx.set_phase("Reading the partition table...");
    // A device cannot answer seek(End); carry the length the OS reports instead.
    let mut reader = crate::os::known_len_source(file, path);
    let table = PartitionTable::detect(&mut reader).map_err(|e| ScanFailure {
        load_error: format!("Could not read partition table: {e}"),
        log_error: Some(format!("Target scan failed: {e}")),
    })?;
    Ok((table.partitions(), table.type_name().to_string()))
}

/// The automatic preview of an image file: detect its container, the table inside, and size each partition.
pub fn scan_image(path: &Path, ctx: &JobCtx) -> anyhow::Result<SourceScan> {
    let file = File::open(path).map_err(|e| failure(format!("Cannot open source: {e}"), None))?;
    let mut notes = Vec::new();
    ctx.set_phase("Detecting the image format...");
    let reopen = |p: &Path| File::open(p).map_err(anyhow::Error::from);
    let detect_raw =
        |f: File| PartitionTable::detect(&mut BufReader::new(f)).map_err(anyhow::Error::from);
    let detected = match crate::rbformats::detect_image_format_with_path(file, Some(path)) {
        Ok(format) => {
            notes.push((false, format!("Detected format: {}", format.description())));
            match reopen(path) {
                Ok(f2) => match crate::rbformats::wrap_image_reader(f2, format) {
                    Ok((mut reader, _size)) => {
                        PartitionTable::detect(&mut reader).map_err(anyhow::Error::from)
                    }
                    Err(e) => {
                        notes.push((true, format!("Format wrap failed: {e}")));
                        reopen(path).and_then(detect_raw)
                    }
                },
                Err(e) => Err(e),
            }
        }
        Err(_) => reopen(path).and_then(detect_raw),
    };
    ctx.set_phase("Reading the partition table...");
    let table =
        detected.map_err(|e| failure(format!("Could not read partition table: {e}"), None))?;
    let table_desc = if matches!(table, PartitionTable::None { .. }) {
        "No partition table (superfloppy)".to_string()
    } else {
        table.type_name().to_string()
    };
    let partitions = table.partitions();
    // Each probe gets its own handle; DMG / VHDX wrapping is skipped, as raw tables sit where expected.
    let owned: PathBuf = path.to_path_buf();
    let probes = probe_all(&partitions, ctx, |_| {
        let f = File::open(&owned).ok()?;
        Some(Box::new(BufReader::new(f)) as Box<dyn ReadSeek>)
    })?;
    Ok(SourceScan {
        partitions,
        table_desc,
        notes,
        probes,
        source_file: File::open(path).ok(),
    })
}

fn probe_all(
    partitions: &[PartitionInfo],
    ctx: &JobCtx,
    open: impl Fn(usize) -> Option<Box<dyn ReadSeek>>,
) -> anyhow::Result<Vec<ProbeResult>> {
    let sized: Vec<&PartitionInfo> = partitions
        .iter()
        .filter(|p| !p.is_extended_container)
        .collect();
    let mut probes = Vec::new();
    for (n, p) in sized.iter().enumerate() {
        ctx.check_cancel()?;
        ctx.set_phase(format!(
            "Sizing partition {} ({} of {})...",
            p.index,
            n + 1,
            sized.len()
        ));
        ctx.set_progress(crate::model::job::Progress::Determinate {
            done: n as u64,
            total: sized.len() as u64,
            unit: crate::model::job::Unit::Items,
        });
        let Some(reader) = open(p.index) else {
            continue;
        };
        let result = fs::partition_minimum_size(
            reader,
            p.start_lba * 512,
            p.partition_type_byte,
            p.partition_type_string.as_deref(),
            p.size_bytes,
            false,
            None,
            &|_| {},
        );
        probes.push(ProbeResult {
            index: p.index,
            size_bytes: p.size_bytes,
            type_byte: p.partition_type_byte,
            type_string: p.partition_type_string.clone(),
            result,
        });
    }
    Ok(probes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::job::{Job, NoNotify, Status};
    use std::io::{Cursor, Read};
    use std::sync::Arc;

    fn fixture_file(name: &str) -> tempfile::NamedTempFile {
        let compressed = std::fs::read(format!("tests/fixtures/{name}")).unwrap();
        let mut raw = Vec::new();
        crate::rbformats::zstd_compat::decoder(Cursor::new(compressed))
            .and_then(|mut d| d.read_to_end(&mut raw))
            .unwrap();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), raw).unwrap();
        tmp
    }

    fn run(
        body: impl FnOnce(&JobCtx) -> anyhow::Result<SourceScan> + Send + 'static,
    ) -> (Status, Option<SourceScan>) {
        let job = Job::spawn("scan", Arc::new(NoNotify), body);
        let status = job.wait().status;
        (status, job.take_result())
    }

    #[test]
    fn a_fat16_image_scans_with_its_minimum_size_off_the_ui_thread() {
        let img = fixture_file("test_fat16.img.zst");
        for preview in [true, false] {
            let path = img.path().to_path_buf();
            let (status, scan) = run(move |ctx| {
                if preview {
                    scan_image(&path, ctx)
                } else {
                    scan_source(&path, false, ctx)
                }
            });
            assert_eq!(status, Status::Done);
            let scan = scan.unwrap();
            assert_eq!(scan.partitions.len(), 1, "preview={preview}");
            assert_eq!(scan.probes.len(), 1);
            assert!(matches!(
                scan.probes[0].result,
                MinimumResult::Computed {
                    in_place: Some(_),
                    ..
                }
            ));
            assert!(scan.source_file.is_some());
        }
    }

    #[test]
    fn a_missing_source_fails_with_the_message_shown_under_it() {
        let job = Job::spawn("scan", Arc::new(NoNotify), |ctx| {
            let missing = Path::new("/nonexistent/disk.img");
            let failures = [
                scan_source(missing, false, ctx).err(),
                scan_image(missing, ctx).err(),
            ];
            Ok(failures
                .into_iter()
                .map(|e| {
                    let f = e.unwrap().downcast::<ScanFailure>().unwrap();
                    (f.load_error, f.log_error.is_some())
                })
                .collect::<Vec<_>>())
        });
        assert_eq!(job.wait().status, Status::Done);
        let got = job.take_result().unwrap();
        assert!(got[0].0.starts_with("Cannot open source:"));
        assert!(got[0].1, "the Load partition info scan also logs it");
        assert!(got[1].0.starts_with("Cannot open source:"));
        assert!(!got[1].1, "the automatic preview only shows it");
    }
}
