//! Bulk-convert worker: convert a list of disk images to a chosen output
//! format, dropping results into an output folder.
//!
//! Shared by the GUI's Bulk Convert dialog (`gui/bulk_convert_dialog.rs`) and
//! `rb-cli convert`. Both surfaces give the runner a fully-resolved list of
//! source paths; the GUI's review-phase checklist is a UI concern that lives
//! in the dialog.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::model::status::{BulkConvertLogLevel as LogLevel, BulkConvertStatus};
use crate::rbformats::chd_options::{ChdOptions, ChdProfile};
use crate::rbformats::export::{
    export_whole_disk, export_whole_disk_bincue, export_whole_disk_chd, export_whole_disk_chd_cd,
    ExportFormat,
};

/// One entry surfaced by `scan_source_folder`.
pub struct ScannedFile {
    pub path: PathBuf,
    pub size: u64,
    /// UI-only field used by the GUI's review checklist; the CLI ignores it.
    pub selected: bool,
}

pub fn fmt_bytes(n: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    if n >= GB {
        format!("{:.2} GB", n as f64 / GB as f64)
    } else if n >= MB {
        format!("{:.1} MB", n as f64 / MB as f64)
    } else if n >= KB {
        format!("{:.1} KB", n as f64 / KB as f64)
    } else {
        format!("{} B", n)
    }
}

/// Scan a folder for files the chosen output format can consume.
///
/// Subdirectories and dotfiles are skipped. Format filter:
/// - **CD CHD**: only `.iso` and `.cue` (`.bin` is referenced by its cue).
/// - **BIN/CUE**: only `.chd`.
/// - **HD/DVD CHD**: skip `.cue` and `.bin` (those are CD content).
/// - **All others**: no filtering beyond dotfiles.
pub fn scan_source_folder(
    source: &Path,
    format: ExportFormat,
) -> std::io::Result<Vec<ScannedFile>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(s) => s,
            None => continue,
        };
        if name.starts_with('.') {
            continue;
        }
        let ext_lower = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|s| s.to_ascii_lowercase());
        let keep = match format {
            ExportFormat::ChdCd => matches!(ext_lower.as_deref(), Some("iso" | "cue")),
            ExportFormat::BinCue => matches!(ext_lower.as_deref(), Some("chd")),
            ExportFormat::Chd | ExportFormat::ChdDvd => {
                !matches!(ext_lower.as_deref(), Some("cue" | "bin"))
            }
            _ => true,
        };
        if !keep {
            continue;
        }
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        out.push(ScannedFile {
            path,
            size,
            selected: true,
        });
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

/// Spawn a worker thread that converts the supplied files into `output`.
/// Returns the shared status handle.
pub fn start_bulk_convert(
    files: Vec<PathBuf>,
    output: PathBuf,
    format: ExportFormat,
    extension: String,
    chd_options: Option<ChdOptions>,
    bincue_multi_bin: bool,
) -> Arc<Mutex<BulkConvertStatus>> {
    let status = Arc::new(Mutex::new(BulkConvertStatus {
        finished: false,
        cancel_requested: false,
        current_index: 0,
        total_files: files.len(),
        current_file: String::new(),
        current_bytes: 0,
        current_total_bytes: 0,
        succeeded: 0,
        failed: 0,
        log_messages: Vec::new(),
    }));

    let status_thread = Arc::clone(&status);
    std::thread::spawn(move || {
        let _wake = crate::os::wakelock::acquire("Rusty Backup: bulk convert");
        let status_panic = Arc::clone(&status_thread);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_bulk_convert(
                files,
                output,
                format,
                extension,
                chd_options,
                bincue_multi_bin,
                status_thread,
            );
        }));
        if let Err(payload) = result {
            let msg = if let Some(s) = payload.downcast_ref::<&'static str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "(unknown panic payload)".to_string()
            };
            if let Ok(mut s) = status_panic.lock() {
                s.log_messages.push((
                    LogLevel::Error,
                    format!("Bulk convert worker panicked: {msg}"),
                ));
                s.finished = true;
            }
        }
    });

    status
}

fn run_bulk_convert(
    files: Vec<PathBuf>,
    output: PathBuf,
    format: ExportFormat,
    extension: String,
    chd_options: Option<ChdOptions>,
    bincue_multi_bin: bool,
    status: Arc<Mutex<BulkConvertStatus>>,
) {
    let push_log = |level: LogLevel, msg: String| {
        if let Ok(mut s) = status.lock() {
            s.log_messages.push((level, msg));
        }
    };

    push_log(
        LogLevel::Info,
        format!(
            "Bulk convert: {} file(s), output -> {}",
            files.len(),
            output.display(),
        ),
    );

    if let Err(e) = std::fs::create_dir_all(&output) {
        push_log(
            LogLevel::Error,
            format!("Failed to create output folder {}: {}", output.display(), e),
        );
        if let Ok(mut s) = status.lock() {
            s.finished = true;
        }
        return;
    }

    let mut skipped: Vec<(PathBuf, String)> = Vec::new();

    for (idx, file) in files.iter().enumerate() {
        if status.lock().map(|s| s.cancel_requested).unwrap_or(false) {
            push_log(LogLevel::Warn, "Cancelled by user.".to_string());
            break;
        }

        let stem = file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("disk")
            .to_string();
        let dest = output.join(format!("{stem}.{extension}"));

        let source_size = std::fs::metadata(file).map(|m| m.len()).unwrap_or(0);
        if let Ok(mut s) = status.lock() {
            s.current_index = idx + 1;
            s.current_file = file
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            s.current_bytes = 0;
            s.current_total_bytes = source_size;
        }

        push_log(
            LogLevel::Info,
            format!(
                "[{}/{}] Converting {} ({}) -> {}",
                idx + 1,
                files.len(),
                file.display(),
                fmt_bytes(source_size),
                dest.display(),
            ),
        );

        let status_progress = Arc::clone(&status);
        let status_cancel = Arc::clone(&status);
        let status_log = Arc::clone(&status);

        let started = Instant::now();
        let log_for_progress = Arc::clone(&status);
        let mut last_logged_bytes: u64 = 0;
        const LOG_EVERY_BYTES: u64 = 64 * 1024 * 1024;

        let progress_cb = move |bytes: u64| {
            if let Ok(mut s) = status_progress.lock() {
                s.current_bytes = bytes;
            }
            if bytes >= last_logged_bytes + LOG_EVERY_BYTES {
                last_logged_bytes = bytes;
                let elapsed = started.elapsed().as_secs_f64().max(0.001);
                let mbs = (bytes as f64 / 1_048_576.0) / elapsed;
                if let Ok(mut s) = log_for_progress.lock() {
                    s.log_messages.push((
                        LogLevel::Info,
                        format!("    … {} written ({:.1} MB/s)", fmt_bytes(bytes), mbs),
                    ));
                }
            }
        };
        let cancel_cb = move || {
            status_cancel
                .lock()
                .map(|s| s.cancel_requested)
                .unwrap_or(false)
        };
        let log_cb = move |msg: &str| {
            if let Ok(mut s) = status_log.lock() {
                s.log_messages.push((LogLevel::Info, msg.to_string()));
            }
        };

        let result = match format {
            ExportFormat::Chd => export_whole_disk_chd(
                file,
                None,
                None,
                &[],
                &dest,
                ChdProfile::Hd,
                chd_options.clone(),
                progress_cb,
                cancel_cb,
                log_cb,
            ),
            ExportFormat::ChdDvd => export_whole_disk_chd(
                file,
                None,
                None,
                &[],
                &dest,
                ChdProfile::Dvd,
                chd_options.clone(),
                progress_cb,
                cancel_cb,
                log_cb,
            ),
            ExportFormat::ChdCd => {
                let _ = progress_cb;
                export_whole_disk_chd_cd(file, &dest, chd_options.clone(), cancel_cb, log_cb)
            }
            ExportFormat::BinCue => {
                let _ = progress_cb;
                export_whole_disk_bincue(file, &dest, bincue_multi_bin, cancel_cb, log_cb)
            }
            _ => export_whole_disk(
                format,
                file,
                None,
                None,
                &[],
                &dest,
                progress_cb,
                cancel_cb,
                log_cb,
            ),
        };
        match result {
            Ok(()) => {
                if let Ok(mut s) = status.lock() {
                    s.succeeded += 1;
                }
                let elapsed = started.elapsed().as_secs_f64();
                push_log(
                    LogLevel::Info,
                    format!("  OK: {} (in {:.1}s)", dest.display(), elapsed),
                );
            }
            Err(e) => {
                if let Ok(mut s) = status.lock() {
                    s.failed += 1;
                }
                let _ = std::fs::remove_file(&dest);
                let reason = format!("{e:#}");
                push_log(
                    LogLevel::Warn,
                    format!("  Skipped {} - {}", file.display(), reason),
                );
                skipped.push((file.clone(), reason));
            }
        }
    }

    let (succeeded, failed) = if let Ok(s) = status.lock() {
        (s.succeeded, s.failed)
    } else {
        (0, 0)
    };
    push_log(
        LogLevel::Info,
        format!(
            "Bulk convert finished: {} succeeded, {} skipped/failed.",
            succeeded, failed,
        ),
    );
    if !skipped.is_empty() {
        push_log(
            LogLevel::Warn,
            format!("{} file(s) were skipped:", skipped.len()),
        );
        for (path, reason) in &skipped {
            push_log(
                LogLevel::Warn,
                format!("  - {}: {}", path.display(), reason),
            );
        }
    }

    if let Ok(mut s) = status.lock() {
        s.finished = true;
    }
}
