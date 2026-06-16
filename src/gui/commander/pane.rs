//! One Commander pane: a source bar (open + partition picker), a path line, and
//! a flat single-directory listing grid with sortable columns, multi-selection,
//! and `..` / double-click navigation.
//!
//! All listing state lives in the [`DirListing`] model
//! (`rusty_backup::model::dir_listing`); the pane is the thin egui renderer over
//! it. Opening a source is delegated to
//! [`rusty_backup::model::commander_source`] (partition probe + `BrowseSession`)
//! and runs off-thread via [`BrowseSession::spawn_open`], polled each frame.
//!
//! Read-only for now: copy / delete / staging arrive with the middle-column
//! engine in a later milestone (see `docs/commander_mode.md` §5, §7).
//!
//! [`BrowseSession::spawn_open`]: rusty_backup::model::browse_session::BrowseSession::spawn_open

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use eframe::egui;

use rusty_backup::model::browse_session::BrowseOpenStatus;
use rusty_backup::model::commander_source;
use rusty_backup::model::dir_listing::{type_tag, DirListing, Row, SortColumn};
use rusty_backup::partition::{format_size, PartitionInfo};

use super::Side;

const ROW_H: f32 = 20.0;

pub(crate) struct CommanderPane {
    side: Side,
    /// Loaded source path (image / container). `None` until the user opens one.
    source: Option<PathBuf>,
    /// Partitions parsed from the source; drives the partition dropdown.
    partitions: Vec<PartitionInfo>,
    /// Index into `partitions` currently being browsed.
    selected_part: Option<usize>,
    /// The directory-listing model this pane renders.
    listing: DirListing,
    /// In-flight async open (spinner) from `BrowseSession::spawn_open`.
    pending_open: Option<Arc<Mutex<BrowseOpenStatus>>>,
    /// Phase text shown next to the spinner while `pending_open` is live.
    open_phase: String,
    /// Volume metadata captured on open, for the source-bar readout.
    volume_label: String,
    fs_type: String,
    total_size: u64,
    used_size: u64,
    /// Last open / navigation error, shown in the pane body.
    error: Option<String>,
}

impl CommanderPane {
    pub(crate) fn new(side: Side) -> Self {
        Self {
            side,
            source: None,
            partitions: Vec::new(),
            selected_part: None,
            listing: DirListing::new(),
            pending_open: None,
            open_phase: String::new(),
            volume_label: String::new(),
            fs_type: String::new(),
            total_size: 0,
            used_size: 0,
            error: None,
        }
    }

    /// Render the pane. Returns a status line when the user did something worth
    /// surfacing in the overlay's bottom bar.
    pub(crate) fn show(&mut self, ui: &mut egui::Ui) -> Option<String> {
        let mut status = self.poll_open(ui.ctx());

        if let Some(s) = self.source_bar(ui) {
            status = Some(s);
        }
        self.path_line(ui);
        ui.separator();

        if self.pending_open.is_some() {
            ui.add_space(20.0);
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                ui.spinner();
                ui.label(if self.open_phase.is_empty() {
                    "Opening...".to_string()
                } else {
                    self.open_phase.clone()
                });
            });
        } else if let Some(err) = &self.error {
            ui.add_space(12.0);
            ui.colored_label(egui::Color32::from_rgb(220, 120, 120), err);
        } else if self.listing.is_loaded() {
            self.render_header(ui);
            if let Some(s) = self.render_rows(ui) {
                status = Some(s);
            }
        } else {
            ui.centered_and_justified(|ui| {
                ui.weak("Open a disk image or container to browse it here.");
            });
        }

        status
    }

    // --- source bar --------------------------------------------------------

    fn source_bar(&mut self, ui: &mut egui::Ui) -> Option<String> {
        let mut status = None;
        ui.horizontal_wrapped(|ui| {
            if ui.button("Open...").clicked() {
                if let Some(path) = super::super::file_dialog()
                    .add_filter(
                        "Disk Images",
                        rusty_backup::model::file_types::DISK_IMAGE_EXTS,
                    )
                    .add_filter("All Files", &["*"])
                    .pick_file()
                {
                    status = Some(self.load_source(path));
                }
            }

            // Partition dropdown (populated after a source is opened).
            let current = self
                .selected_part
                .and_then(|i| self.partitions.get(i))
                .map(partition_label)
                .unwrap_or_else(|| "(no partitions)".to_string());
            let mut chosen = self.selected_part;
            egui::ComboBox::from_id_salt(("commander_part", self.side.idx()))
                .selected_text(current)
                .show_ui(ui, |ui| {
                    for (i, p) in self.partitions.iter().enumerate() {
                        ui.selectable_value(&mut chosen, Some(i), partition_label(p));
                    }
                });
            if chosen != self.selected_part {
                if let Some(i) = chosen {
                    status = Some(self.open_partition(i));
                }
            }

            // Right-aligned volume label + free space.
            if self.listing.is_loaded() {
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let free = self.total_size.saturating_sub(self.used_size);
                    ui.label(format!("free: {}", format_size(free)));
                    ui.separator();
                    let label = if self.volume_label.is_empty() {
                        self.fs_type.clone()
                    } else {
                        format!("{} ({})", self.volume_label, self.fs_type)
                    };
                    ui.strong(label);
                });
            }
        });
        status
    }

    fn path_line(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            let prefix = match self.selected_part.and_then(|i| self.partitions.get(i)) {
                Some(_) => format!("@{}", self.selected_part.map(|i| i + 1).unwrap_or(0)),
                None => "-".to_string(),
            };
            ui.monospace(prefix);
            let path = self.listing.cwd_path();
            ui.monospace(if path.is_empty() { "/" } else { path });
        });
    }

    // --- opening -----------------------------------------------------------

    /// Probe a freshly-picked file and start browsing its first real partition.
    fn load_source(&mut self, path: PathBuf) -> String {
        self.source = Some(path.clone());
        self.listing = DirListing::new();
        self.pending_open = None;
        self.error = None;
        self.selected_part = None;
        self.volume_label.clear();
        self.fs_type.clear();

        match commander_source::probe_partitions(&path) {
            Ok(parts) => {
                self.partitions = parts;
                // Auto-open the first non-extended-container partition.
                let first = self
                    .partitions
                    .iter()
                    .position(|p| !p.is_extended_container);
                match first {
                    Some(i) => self.open_partition(i),
                    None => format!(
                        "[{}] {} has no browsable partitions.",
                        self.side.label(),
                        path.display()
                    ),
                }
            }
            Err(e) => {
                self.partitions.clear();
                self.error = Some(format!("Could not read partitions: {e:#}"));
                format!("[{}] failed to open {}", self.side.label(), path.display())
            }
        }
    }

    /// Begin an async open of partition `idx`.
    fn open_partition(&mut self, idx: usize) -> String {
        let Some(path) = self.source.clone() else {
            return String::new();
        };
        let Some(part) = self.partitions.get(idx) else {
            return String::new();
        };
        self.selected_part = Some(idx);
        self.error = None;
        self.listing = DirListing::new();
        let session = commander_source::session_for(&path, part);
        self.pending_open = Some(session.spawn_open());
        self.open_phase = "Opening...".to_string();
        format!(
            "[{}] opening {} ...",
            self.side.label(),
            partition_label(part)
        )
    }

    /// Poll an in-flight open; on completion, hand the filesystem + root listing
    /// to the model or record the error. Returns a status line on completion.
    fn poll_open(&mut self, ctx: &egui::Context) -> Option<String> {
        let arc = self.pending_open.clone()?;
        ctx.request_repaint(); // keep polling until the worker finishes
        let mut guard = arc.lock().ok()?;
        if !guard.finished {
            self.open_phase = guard.phase.clone();
            return None;
        }
        // Finished — detach the pending handle either way.
        self.pending_open = None;

        if let Some(err) = guard.error.take() {
            self.error = Some(err);
            return Some(format!("[{}] open failed.", self.side.label()));
        }

        let fs = guard.fs.take();
        let root = guard.root.take();
        let entries = guard.root_entries.take().unwrap_or_default();
        self.volume_label = guard.volume_label.clone();
        self.fs_type = guard.fs_type.clone();
        self.total_size = guard.total_size;
        self.used_size = guard.used_size;
        drop(guard);

        match (fs, root) {
            (Some(fs), Some(root)) => {
                self.listing.load_root(fs, root, entries, false);
                Some(format!(
                    "[{}] opened {} ({} item(s)).",
                    self.side.label(),
                    if self.volume_label.is_empty() {
                        self.fs_type.clone()
                    } else {
                        self.volume_label.clone()
                    },
                    self.listing.entries().len()
                ))
            }
            _ => {
                self.error = Some("Filesystem opened but no root directory was returned.".into());
                None
            }
        }
    }

    // --- listing grid ------------------------------------------------------

    fn render_header(&mut self, ui: &mut egui::Ui) {
        let (rect, resp) = ui.allocate_exact_size(
            egui::vec2(ui.available_width(), ROW_H),
            egui::Sense::click(),
        );
        let c = cols(rect);
        let mid = rect.center().y;
        let font = egui::FontId::proportional(13.0);
        let color = ui.visuals().strong_text_color();
        let active = self.listing.sort_column();
        let desc = self.listing.is_descending();
        let caret = |col: SortColumn| -> &'static str {
            if col != active {
                ""
            } else if desc {
                " v"
            } else {
                " ^"
            }
        };
        let pt = ui.painter();
        pt.text(
            egui::pos2(c.name_l, mid),
            egui::Align2::LEFT_CENTER,
            format!("Name{}", caret(SortColumn::Name)),
            font.clone(),
            color,
        );
        pt.text(
            egui::pos2(c.size_r, mid),
            egui::Align2::RIGHT_CENTER,
            format!("Size{}", caret(SortColumn::Size)),
            font.clone(),
            color,
        );
        pt.text(
            egui::pos2(c.mod_l, mid),
            egui::Align2::LEFT_CENTER,
            format!("Modified{}", caret(SortColumn::Modified)),
            font.clone(),
            color,
        );
        pt.text(
            egui::pos2(c.type_l, mid),
            egui::Align2::LEFT_CENTER,
            format!("Type{}", caret(SortColumn::Type)),
            font,
            color,
        );
        pt.line_segment(
            [
                egui::pos2(rect.left(), rect.bottom()),
                egui::pos2(rect.right(), rect.bottom()),
            ],
            egui::Stroke::new(1.0, ui.visuals().window_stroke.color),
        );

        if resp.clicked() {
            if let Some(pos) = resp.interact_pointer_pos() {
                let clicked = if pos.x < c.size_l {
                    SortColumn::Name
                } else if pos.x < c.mod_l {
                    SortColumn::Size
                } else if pos.x < c.type_l {
                    SortColumn::Modified
                } else {
                    SortColumn::Type
                };
                self.listing.resort(clicked);
            }
        }
    }

    fn render_rows(&mut self, ui: &mut egui::Ui) -> Option<String> {
        let rows = build_display_rows(&self.listing);

        let mut to_enter: Option<String> = None;
        let mut to_up = false;
        let mut click: Option<(String, bool, bool)> = None;
        let mut bg_deselect = false;

        egui::ScrollArea::vertical()
            .id_salt(("commander_rows", self.side.idx()))
            .auto_shrink([false, false])
            .show(ui, |ui| {
                let mods = ui.input(|i| i.modifiers);
                for row in &rows {
                    let (rect, resp) = ui.allocate_exact_size(
                        egui::vec2(ui.available_width(), ROW_H),
                        egui::Sense::click(),
                    );
                    let selected = !row.is_parent && self.listing.is_selected(&row.name);
                    if selected {
                        ui.painter().rect_filled(
                            rect,
                            egui::CornerRadius::ZERO,
                            ui.visuals().selection.bg_fill,
                        );
                    } else if resp.hovered() {
                        ui.painter().rect_filled(
                            rect,
                            egui::CornerRadius::ZERO,
                            ui.visuals().widgets.hovered.bg_fill,
                        );
                    }
                    paint_row(ui, rect, row);

                    if resp.double_clicked() {
                        if row.is_parent {
                            to_up = true;
                        } else if row.is_dir {
                            to_enter = Some(row.name.clone());
                        }
                    } else if resp.clicked() {
                        if row.is_parent {
                            to_up = true;
                        } else {
                            click = Some((row.name.clone(), mods.command, mods.shift));
                        }
                    }
                }

                // Click empty space to clear the selection.
                let remaining = ui.available_size();
                if remaining.y > 4.0 {
                    let (_r, bgr) = ui.allocate_exact_size(remaining, egui::Sense::click());
                    if bgr.clicked() {
                        bg_deselect = true;
                    }
                }
            });

        let mut status = None;
        if to_up {
            self.listing.up();
        }
        if let Some(name) = to_enter {
            if let Err(e) = self.listing.enter(&name) {
                status = Some(format!("[{}] cannot open '{name}': {e}", self.side.label()));
            }
        }
        if bg_deselect {
            self.listing.clear_selection();
        }
        if let Some((name, command, shift)) = click {
            if shift {
                self.listing.shift_click(&name);
            } else if command {
                self.listing.ctrl_click(&name);
            } else {
                self.listing.click(&name);
            }
        }
        status
    }
}

/// Owned per-frame row snapshot, so the row loop can mutate the listing freely
/// after rendering without holding a borrow of it.
struct DisplayRow {
    name: String,
    is_dir: bool,
    is_parent: bool,
    size: u64,
    modified: String,
    type_tag: String,
}

fn build_display_rows(listing: &DirListing) -> Vec<DisplayRow> {
    listing
        .current_rows()
        .into_iter()
        .map(|r| match r {
            Row::Parent => DisplayRow {
                name: "..".to_string(),
                is_dir: true,
                is_parent: true,
                size: 0,
                modified: String::new(),
                type_tag: String::new(),
            },
            Row::Entry(e) => DisplayRow {
                name: e.name.clone(),
                is_dir: e.is_directory(),
                is_parent: false,
                size: e.size,
                modified: e.modified.clone().unwrap_or_default(),
                type_tag: type_tag(e),
            },
        })
        .collect()
}

/// Short label for a partition in the dropdown: `1: FAT16 (510.0 MiB)`.
fn partition_label(p: &PartitionInfo) -> String {
    format!(
        "{}: {} ({})",
        p.index + 1,
        p.type_name,
        format_size(p.size_bytes)
    )
}

// --- column geometry + painting (adapted from the layout mock) -------------

struct Cols {
    name_l: f32,
    name_r: f32,
    size_l: f32,
    size_r: f32,
    mod_l: f32,
    type_l: f32,
}

fn cols(rect: egui::Rect) -> Cols {
    let pad = 6.0;
    let gap = 10.0;
    let type_w = 56.0;
    let mod_w = 134.0;
    let size_w = 80.0;
    let name_l = rect.left() + pad;
    let name_w = (rect.width() - type_w - mod_w - size_w - 4.0 * gap).max(60.0);
    let name_r = name_l + name_w;
    let size_l = name_r + gap;
    let size_r = size_l + size_w;
    let mod_l = size_r + gap;
    let type_l = mod_l + mod_w + gap;
    Cols {
        name_l,
        name_r,
        size_l,
        size_r,
        mod_l,
        type_l,
    }
}

fn paint_row(ui: &egui::Ui, rect: egui::Rect, row: &DisplayRow) {
    let c = cols(rect);
    let mid = rect.center().y;
    let font = egui::FontId::proportional(13.0);
    let base = ui.visuals().text_color();
    let color = if row.is_parent {
        ui.visuals().weak_text_color()
    } else if row.is_dir {
        egui::Color32::from_rgb(120, 160, 255)
    } else {
        base
    };

    let display_name = if row.is_dir && !row.is_parent {
        format!("{}/", row.name)
    } else {
        row.name.clone()
    };

    let name_cell = egui::Rect::from_min_max(
        egui::pos2(c.name_l, rect.top()),
        egui::pos2(c.name_r, rect.bottom()),
    );
    ui.painter_at(name_cell).text(
        egui::pos2(c.name_l, mid),
        egui::Align2::LEFT_CENTER,
        display_name,
        font.clone(),
        color,
    );

    if !row.is_dir {
        ui.painter().text(
            egui::pos2(c.size_r, mid),
            egui::Align2::RIGHT_CENTER,
            format_size(row.size),
            font.clone(),
            color,
        );
    }
    ui.painter().text(
        egui::pos2(c.mod_l, mid),
        egui::Align2::LEFT_CENTER,
        row.modified.clone(),
        font.clone(),
        ui.visuals().weak_text_color(),
    );
    ui.painter().text(
        egui::pos2(c.type_l, mid),
        egui::Align2::LEFT_CENTER,
        row.type_tag.clone(),
        font,
        ui.visuals().weak_text_color(),
    );
}
