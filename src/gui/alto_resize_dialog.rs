//! "Resize Alto Disk…" dialog for the Inspect tab.
//!
//! Re-lays an Alto BFS volume onto a different disk geometry (Diablo 31 or 44 —
//! grow or shrink) and writes the result as a new PARC Disk Image. The source
//! is opened read-only; the output is always a brand-new file. Alto packs are
//! small, so the clone runs synchronously (no worker thread needed).

use std::path::PathBuf;

use rusty_backup::fs::alto::bfs::Bfs;
use rusty_backup::fs::alto::{open_pack, pdi, write, FsFamily, Geometry};

use super::progress::LogPanel;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Target {
    Diablo31,
    Diablo44,
}

impl Target {
    fn pages(self) -> usize {
        match self {
            Target::Diablo31 => 203 * 2 * 12,
            Target::Diablo44 => 406 * 2 * 12,
        }
    }

    fn geometry(self) -> Geometry {
        let (model, cylinders) = match self {
            Target::Diablo31 => (31, 203),
            Target::Diablo44 => (44, 406),
        };
        Geometry {
            family: FsFamily::Diablo,
            disk_model: model,
            n_disks: 1,
            n_cylinders: cylinders,
            n_heads: 2,
            n_sectors: 12,
            label_bytes: 16,
            data_bytes: 512,
        }
    }
}

pub struct AltoResizeDialog {
    source_path: PathBuf,
    current_summary: String,
    used_pages: usize,
    target: Target,
    /// Result of the last attempt: `Ok(message)` or `Err(message)`.
    result: Option<Result<String, String>>,
    closed: bool,
}

impl AltoResizeDialog {
    /// Open the dialog for a source Alto pack, reading its current geometry and
    /// used-page count up front.
    pub fn new(source_path: PathBuf) -> Result<Self, String> {
        let bytes = std::fs::read(&source_path).map_err(|e| e.to_string())?;
        let disk = open_pack(&bytes).map_err(|e| format!("{e}"))?;
        let g = &disk.geometry;
        let total = g.total_sectors();
        let free = Bfs::new(&disk)
            .disk_descriptor()
            .map(|d| d.free_pages as usize)
            .unwrap_or(0);
        let used = total.saturating_sub(free);
        let current_summary = format!(
            "Current: Diablo {} - {}x{}x{}x{} = {} pages ({} used)",
            g.disk_model, g.n_disks, g.n_cylinders, g.n_heads, g.n_sectors, total, used
        );
        // Default to the geometry the source is NOT, so the button does something.
        let target = if g.disk_model == 31 {
            Target::Diablo44
        } else {
            Target::Diablo31
        };
        Ok(AltoResizeDialog {
            source_path,
            current_summary,
            used_pages: used,
            target,
            result: None,
            closed: false,
        })
    }

    /// Returns `true` while the dialog should stay open.
    pub fn show(&mut self, ctx: &egui::Context, log: &mut LogPanel) -> bool {
        let mut window_open = true;
        egui::Window::new("Resize Alto Disk")
            .collapsible(false)
            .resizable(false)
            .open(&mut window_open)
            .show(ctx, |ui| {
                ui.label(format!("Source: {}", self.source_path.display()));
                ui.label(&self.current_summary);
                ui.separator();
                ui.label("Target geometry:");
                ui.radio_value(
                    &mut self.target,
                    Target::Diablo31,
                    "Diablo 31 - 203x2x12 (4872 pages, ~2.5 MB)",
                );
                ui.radio_value(
                    &mut self.target,
                    Target::Diablo44,
                    "Diablo 44 - 406x2x12 (9744 pages, ~5 MB)",
                );

                let fits = self.used_pages <= self.target.pages();
                if !fits {
                    ui.colored_label(
                        egui::Color32::from_rgb(200, 90, 70),
                        format!(
                            "Warning: {} used pages may not fit the target's {} pages.",
                            self.used_pages,
                            self.target.pages()
                        ),
                    );
                }

                ui.separator();
                match &self.result {
                    Some(Ok(msg)) => {
                        ui.colored_label(egui::Color32::from_rgb(90, 170, 90), msg.clone());
                    }
                    Some(Err(e)) => {
                        ui.colored_label(
                            egui::Color32::from_rgb(200, 90, 70),
                            format!("Error: {e}"),
                        );
                    }
                    None => {}
                }

                ui.horizontal(|ui| {
                    if ui.button("Resize and Save As...").clicked() {
                        self.do_resize(log);
                    }
                    if ui.button("Close").clicked() {
                        self.closed = true;
                    }
                });
            });
        window_open && !self.closed
    }

    fn do_resize(&mut self, log: &mut LogPanel) {
        let Some(out) = rfd::FileDialog::new()
            .add_filter("PARC Disk Image", &["pdi"])
            .set_file_name("resized.pdi")
            .save_file()
        else {
            return;
        };
        let result = (|| -> Result<String, String> {
            let bytes = std::fs::read(&self.source_path).map_err(|e| e.to_string())?;
            let src = open_pack(&bytes).map_err(|e| format!("{e}"))?;
            let resized =
                write::clone_to(&src, self.target.geometry()).map_err(|e| format!("{e}"))?;
            std::fs::write(&out, pdi::write(&resized)).map_err(|e| e.to_string())?;
            Ok(format!(
                "Saved {} ({} pages).",
                out.display(),
                resized.geometry.total_sectors()
            ))
        })();
        match &result {
            Ok(msg) => log.info(format!("Alto resize: {msg}")),
            Err(e) => log.error(format!("Alto resize failed: {e}")),
        }
        self.result = Some(result);
    }
}
