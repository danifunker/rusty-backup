//! Archives tab — browse and extract classic Macintosh archives (StuffIt
//! `.sit` / `.sea`, BinHex-wrapped `.sit.hqx`, classic + StuffIt 5).
//!
//! Read-only: pick an archive, see its entries, extract all to a folder in a
//! chosen fork-preserving container (BinHex / MacBinary / AppleDouble / raw).
//! All decode logic lives in `rusty_backup::macarchive`; this tab is just the
//! egui surface around it.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::macarchive::extract::{self, ExtractStats, ForkFormat};
use rusty_backup::macarchive::stuffit::StuffItArchive;

use super::progress::LogPanel;

/// A successfully loaded archive: the decoded bytes plus parsed directory.
struct Loaded {
    bytes: Arc<Vec<u8>>,
    archive: Arc<StuffItArchive>,
    file_count: usize,
}

/// Background extraction status, shared with the worker thread.
#[derive(Default)]
struct ExtractStatus {
    done: usize,
    total: usize,
    current: String,
    finished: bool,
    result: Option<Result<ExtractStats, String>>,
    log_lines: Vec<String>,
}

pub struct ArchivesTab {
    archive_path: Option<PathBuf>,
    prev_path: Option<PathBuf>,
    loaded: Option<Loaded>,
    load_error: Option<String>,
    format: ForkFormat,
    output_dir: Option<PathBuf>,
    status: Option<Arc<Mutex<ExtractStatus>>>,
}

impl Default for ArchivesTab {
    fn default() -> Self {
        Self {
            archive_path: None,
            prev_path: None,
            loaded: None,
            load_error: None,
            format: ForkFormat::BinHex,
            output_dir: None,
            status: None,
        }
    }
}

impl ArchivesTab {
    fn load(&mut self) {
        self.loaded = None;
        self.load_error = None;
        let Some(path) = self.archive_path.clone() else {
            return;
        };
        match extract::open(&path) {
            Ok((bytes, archive)) => {
                let file_count = archive.entries.iter().filter(|e| !e.is_dir).count();
                self.loaded = Some(Loaded {
                    bytes: Arc::new(bytes),
                    archive: Arc::new(archive),
                    file_count,
                });
            }
            Err(e) => self.load_error = Some(format!("{e}")),
        }
    }

    fn start_extract(&mut self, log: &mut LogPanel) {
        let (Some(loaded), Some(dest)) = (self.loaded.as_ref(), self.output_dir.clone()) else {
            return;
        };
        let bytes = loaded.bytes.clone();
        let archive = loaded.archive.clone();
        let format = self.format;
        let status = Arc::new(Mutex::new(ExtractStatus {
            total: loaded.file_count,
            ..Default::default()
        }));
        self.status = Some(status.clone());
        log.info(format!(
            "Extracting archive to {} as {}",
            dest.display(),
            format.label()
        ));

        std::thread::spawn(move || {
            let result = {
                let progress_status = status.clone();
                let log_status = status.clone();
                extract::extract_all(
                    &bytes,
                    &archive,
                    &dest,
                    format,
                    |done, total, name| {
                        if let Ok(mut s) = progress_status.lock() {
                            s.done = done;
                            s.total = total;
                            s.current = name.to_string();
                        }
                    },
                    |line| {
                        if let Ok(mut s) = log_status.lock() {
                            s.log_lines.push(line);
                        }
                    },
                )
            };
            if let Ok(mut s) = status.lock() {
                s.finished = true;
                s.result = Some(result.map_err(|e| format!("{e}")));
            }
        });
    }

    /// Poll the worker; move completion into the log. Returns true while a job
    /// is still running.
    fn poll(&mut self, log: &mut LogPanel) -> bool {
        let mut running = false;
        let mut clear = false;
        if let Some(status) = self.status.as_ref() {
            if let Ok(s) = status.lock() {
                if s.finished {
                    for line in &s.log_lines {
                        log.warn(line.clone());
                    }
                    match &s.result {
                        Some(Ok(stats)) => log.info(format!(
                            "Extraction complete: {} files{}",
                            stats.files,
                            if stats.skipped > 0 {
                                format!(", {} skipped", stats.skipped)
                            } else {
                                String::new()
                            }
                        )),
                        Some(Err(e)) => log.error(format!("Extraction failed: {e}")),
                        None => {}
                    }
                    clear = true;
                } else {
                    running = true;
                }
            }
        }
        if clear {
            self.status = None;
        }
        running
    }

    pub fn show(&mut self, ui: &mut egui::Ui, log: &mut LogPanel) {
        let running = self.poll(log);

        ui.heading("Mac Archives");
        ui.label(
            "Browse and extract classic Mac archives: StuffIt (.sit / .sea, \
             classic + StuffIt 5) and BinHex (.sit.hqx).",
        );
        ui.separator();

        // ── Source picker ────────────────────────────────────────────────
        ui.horizontal(|ui| {
            ui.label("Archive:");
            let text = self
                .archive_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "(none selected)".to_string());
            ui.label(text);
            if ui
                .add_enabled(!running, egui::Button::new("Browse..."))
                .clicked()
            {
                if let Some(path) = super::file_dialog()
                    .add_filter(
                        "Mac Archives",
                        rusty_backup::model::file_types::MAC_ARCHIVE_EXTS,
                    )
                    .add_filter("All Files", &["*"])
                    .pick_file()
                {
                    self.archive_path = Some(path);
                }
            }
        });

        // Reload on path change.
        if self.archive_path != self.prev_path {
            self.prev_path = self.archive_path.clone();
            self.load();
        }

        if let Some(err) = &self.load_error {
            ui.colored_label(
                egui::Color32::from_rgb(200, 80, 80),
                format!("Error: {err}"),
            );
        }

        // ── Entry list ───────────────────────────────────────────────────
        if let Some(loaded) = &self.loaded {
            ui.separator();
            ui.label(format!("{} files in archive", loaded.file_count));

            egui::ScrollArea::vertical()
                .max_height(320.0)
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    egui::Grid::new("archive_entries")
                        .striped(true)
                        .num_columns(4)
                        .spacing([12.0, 2.0])
                        .show(ui, |ui| {
                            ui.strong("Name");
                            ui.strong("Type/Creator");
                            ui.strong("Size");
                            ui.strong("Method");
                            ui.end_row();

                            for e in &loaded.archive.entries {
                                if e.is_dir {
                                    ui.label(format!("[ {} ]", e.display_path()));
                                    ui.label("");
                                    ui.label("");
                                    ui.label("");
                                    ui.end_row();
                                    continue;
                                }
                                ui.label(e.display_path());
                                let tc = String::from_utf8_lossy(&e.type_code);
                                let cc = String::from_utf8_lossy(&e.creator_code);
                                ui.label(format!("{tc} / {cc}"));
                                let (size, method) = e
                                    .data
                                    .as_ref()
                                    .filter(|f| f.uncompressed_len > 0)
                                    .or(e.rsrc.as_ref())
                                    .map(|f| (f.uncompressed_len, f.method_name()))
                                    .unwrap_or((0, "None"));
                                ui.label(human_size(size as u64));
                                ui.label(method);
                                ui.end_row();
                            }
                        });
                });

            // ── Extraction controls ──────────────────────────────────────
            ui.separator();
            ui.horizontal(|ui| {
                ui.label("Extract as:");
                egui::ComboBox::from_id_salt("archive_fork_format")
                    .selected_text(self.format.label())
                    .show_ui(ui, |ui| {
                        for f in ForkFormat::ALL {
                            ui.selectable_value(&mut self.format, f, f.label());
                        }
                    });

                if ui
                    .add_enabled(!running, egui::Button::new("Extract All..."))
                    .clicked()
                {
                    if let Some(dir) = super::file_dialog().pick_folder() {
                        self.output_dir = Some(dir);
                        self.start_extract(log);
                    }
                }
            });
        }

        // ── Progress ─────────────────────────────────────────────────────
        if running {
            if let Some(status) = self.status.as_ref() {
                if let Ok(s) = status.lock() {
                    let frac = if s.total > 0 {
                        s.done as f32 / s.total as f32
                    } else {
                        0.0
                    };
                    ui.add(
                        egui::ProgressBar::new(frac)
                            .text(format!("{} / {}  {}", s.done, s.total, s.current)),
                    );
                }
            }
            ui.ctx().request_repaint();
        }
    }
}

fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}
