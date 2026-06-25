//! Archives tab — browse and extract classic Macintosh archives.
//!
//! Detection is magic-driven via `macarchive::detect::detect_mac_archive`,
//! so the picker accepts any file the user points at: classic StuffIt
//! (`SIT!`), StuffIt 5, SEA, BinHex 4.0 (loose `.hqx` carrying a single
//! Mac file *or* BinHex-wrapped SIT / SEA). Filename extension is a
//! picker hint only, never load-gating.
//!
//! Read-only: pick an archive, see its entries, extract all to a folder
//! in a chosen fork-preserving container (BinHex / MacBinary /
//! AppleDouble / raw). All decode + the loose-HQX synthesis live in
//! `rusty_backup::macarchive::extract`; this tab is just the egui
//! surface around it.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::macarchive::detect::{detect_mountable_image, MountableImageKind};
use rusty_backup::macarchive::extract::{self, ExtractStats, ForkFormat};
use rusty_backup::macarchive::stuffit::{self, StuffItArchive};

use super::progress::LogPanel;

/// A successfully loaded archive: the decoded bytes plus parsed directory.
struct Loaded {
    bytes: Arc<Vec<u8>>,
    archive: Arc<StuffItArchive>,
    file_count: usize,
    /// Set when the archive has exactly one non-folder entry whose data
    /// fork decompresses to a payload [`detect_mountable_image`] recognizes
    /// (DiskCopy 4.2 / raw HFS / raw HFS+). Drives the "Mount in new
    /// Inspect tab" button in `show()` (Workflow D.2 auto-unwrap hint).
    mountable: Option<MountablePayload>,
    /// Per-entry "include in extract" checkbox state (Workflow D.3),
    /// indexed parallel to `archive.entries`. All-true on fresh load;
    /// the user unchecks entries they don't want before clicking
    /// "Extract Selected...". Folder markers are always extracted (the
    /// directory rebuild leans on them) so their checkbox is unused but
    /// kept for index alignment.
    selected: Vec<bool>,
}

/// Cached single-entry payload ready to hand off to the Inspect tab.
struct MountablePayload {
    kind: MountableImageKind,
    /// Decompressed data-fork bytes of the single entry. Owned because
    /// the user might never click the Mount button; we re-decompress
    /// from the source archive bytes lazily would also work, but the
    /// archive-load cost is already paid once.
    decoded: Vec<u8>,
    /// Suggested filename for the host-side temp file. Drives both the
    /// tempfile name and the title rendered in the Inspect tab.
    name: String,
}

/// Hand-off the Archives tab signals to the main update loop to open
/// in the Inspect tab. Drained by `RustyBackupApp::update`.
pub struct PendingInspectOpen {
    pub path: PathBuf,
    pub guard: tempfile::TempDir,
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
    /// Queued by the "Mount in new Inspect tab" button; drained each
    /// frame by `RustyBackupApp::update` via [`take_pending_inspect_open`].
    pending_inspect_open: Option<PendingInspectOpen>,
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
            pending_inspect_open: None,
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
                let mountable = sniff_mountable_payload(&bytes, &archive);
                let selected = vec![true; archive.entries.len()];
                self.loaded = Some(Loaded {
                    bytes: Arc::new(bytes),
                    archive: Arc::new(archive),
                    file_count,
                    mountable,
                    selected,
                });
            }
            Err(e) => self.load_error = Some(format!("{e}")),
        }
    }

    /// Drained by the main update loop. When `Some`, the host should
    /// route the path into the Inspect tab (e.g. via
    /// `InspectTab::load_image_with_tempdir`) and switch the active tab
    /// to Inspect.
    pub fn take_pending_inspect_open(&mut self) -> Option<PendingInspectOpen> {
        self.pending_inspect_open.take()
    }

    /// Load `path` as the archive source (the next `show` reloads on the path
    /// change). Used when the Inspect tab redirects a picked Mac-archive file
    /// here instead of trying to parse it as a disk image.
    pub fn open_path(&mut self, path: PathBuf) {
        self.archive_path = Some(path);
    }

    /// Spawn the background extractor. When `selected_only` is true,
    /// only entries whose checkbox is ticked are extracted via
    /// [`extract::extract_filtered`]; otherwise [`extract::extract_all`]
    /// pulls everything (Workflow D.3).
    fn start_extract(&mut self, log: &mut LogPanel, selected_only: bool) {
        let (Some(loaded), Some(dest)) = (self.loaded.as_ref(), self.output_dir.clone()) else {
            return;
        };
        let bytes = loaded.bytes.clone();
        let archive = loaded.archive.clone();
        let selected = loaded.selected.clone();
        let format = self.format;

        // Total = the file count we're actually about to write so the
        // progress bar tracks the right denominator.
        let total = if selected_only {
            archive
                .entries
                .iter()
                .enumerate()
                .filter(|(i, e)| !e.is_dir && selected.get(*i).copied().unwrap_or(true))
                .count()
        } else {
            loaded.file_count
        };
        let status = Arc::new(Mutex::new(ExtractStatus {
            total,
            ..Default::default()
        }));
        self.status = Some(status.clone());
        log.info(format!(
            "Extracting {} archive entries to {} as {}",
            if selected_only {
                format!("selected ({total})")
            } else {
                "all".into()
            },
            dest.display(),
            format.label(),
        ));

        std::thread::spawn(move || {
            let result = {
                let progress_status = status.clone();
                let log_status = status.clone();
                let progress = move |done: usize, total: usize, name: &str| {
                    if let Ok(mut s) = progress_status.lock() {
                        s.done = done;
                        s.total = total;
                        s.current = name.to_string();
                    }
                };
                let log_cb = move |line: String| {
                    if let Ok(mut s) = log_status.lock() {
                        s.log_lines.push(line);
                    }
                };
                if selected_only {
                    let keep = move |i: usize| selected.get(i).copied().unwrap_or(true);
                    extract::extract_filtered(
                        &bytes, &archive, &dest, format, keep, progress, log_cb,
                    )
                } else {
                    extract::extract_all(&bytes, &archive, &dest, format, progress, log_cb)
                }
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
            "Browse and extract classic Mac archives. Detection is magic-driven, \
             so the picker accepts any file: StuffIt (.sit / .sea, classic + StuffIt \
             5) and BinHex 4.0 (loose .hqx or .sit.hqx / .sea.hqx wrappers).",
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
                        &rusty_backup::model::file_types::archives_picker_exts(),
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

        // Lift the mount-button hint out of the `&self.loaded` borrow
        // so the mount-row closure below can mutate `self` (to queue
        // `pending_inspect_open`) without tangling with the
        // extraction-row closure's own `&mut self` calls.
        let mount_hint: Option<(MountableImageKind, String)> = self
            .loaded
            .as_ref()
            .and_then(|l| l.mountable.as_ref())
            .map(|m| (m.kind, m.name.clone()));

        // ── Entry list ───────────────────────────────────────────────────
        // `file_count` lifted out so the grid closure can take
        // `self.loaded.as_mut()` (needed for the per-entry checkbox
        // state) without tangling with the outer borrow.
        let file_count_hint = self.loaded.as_ref().map(|l| l.file_count);
        let has_loaded = self.loaded.is_some();
        if let Some(fc) = file_count_hint {
            ui.separator();
            ui.label(format!("{fc} files in archive"));

            egui::ScrollArea::vertical()
                .max_height(320.0)
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    let Some(loaded) = self.loaded.as_mut() else {
                        return;
                    };
                    egui::Grid::new("archive_entries")
                        .striped(true)
                        .num_columns(5)
                        .spacing([12.0, 2.0])
                        .show(ui, |ui| {
                            ui.strong("Extract");
                            ui.strong("Name");
                            ui.strong("Type/Creator");
                            ui.strong("Size");
                            ui.strong("Method");
                            ui.end_row();

                            // Iterate by index so the checkbox can mutate
                            // `loaded.selected[i]` without a borrow on the
                            // entries slice itself.
                            for i in 0..loaded.archive.entries.len() {
                                let e = &loaded.archive.entries[i];
                                if e.is_dir {
                                    // Folder markers are always extracted
                                    // (the rebuilt tree needs them), so no
                                    // checkbox column.
                                    ui.label("");
                                    ui.label(format!("[ {} ]", e.display_path()));
                                    ui.label("");
                                    ui.label("");
                                    ui.label("");
                                    ui.end_row();
                                    continue;
                                }
                                // Per-entry checkbox.
                                let checked = loaded.selected.get(i).copied().unwrap_or(true);
                                let mut new_checked = checked;
                                ui.checkbox(&mut new_checked, "");
                                if new_checked != checked {
                                    if let Some(slot) = loaded.selected.get_mut(i) {
                                        *slot = new_checked;
                                    }
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
        }

        if has_loaded {
            // ── Extraction controls ──────────────────────────────────────
            // Pre-compute the selected-file count so the closure below
            // doesn't have to re-borrow `self.loaded`.
            let selected_file_count = self
                .loaded
                .as_ref()
                .map(|l| {
                    l.archive
                        .entries
                        .iter()
                        .enumerate()
                        .filter(|(i, e)| !e.is_dir && l.selected.get(*i).copied().unwrap_or(true))
                        .count()
                })
                .unwrap_or(0);

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
                        self.start_extract(log, false);
                    }
                }

                let extract_selected_label = format!("Extract Selected... ({selected_file_count})");
                if ui
                    .add_enabled(
                        !running && selected_file_count > 0,
                        egui::Button::new(extract_selected_label),
                    )
                    .on_hover_text(
                        "Extract only the entries with their checkbox ticked. \
                         Folder markers are always extracted so the rebuilt \
                         tree stays rooted.",
                    )
                    .clicked()
                {
                    if let Some(dir) = super::file_dialog().pick_folder() {
                        self.output_dir = Some(dir);
                        self.start_extract(log, true);
                    }
                }

                // Workflow D.2 auto-unwrap hint button is rendered
                // below so the closure here doesn't have to borrow
                // `loaded.mountable` (which conflicts with the
                // `start_extract` &mut self call above).
            });

            // Workflow D.2 auto-unwrap: the archive's single entry
            // decompressed to a payload the Inspect tab can mount.
            // `mount_hint` was lifted out of the `&self.loaded` borrow
            // up-front so the click handler can mutate `self` without
            // borrow-checker conflicts.
            if let Some((kind, name)) = mount_hint {
                ui.horizontal(|ui| {
                    let label = format!("Mount in new Inspect tab ({})", kind.label());
                    let tooltip = format!(
                        "The archive's contents are themselves a {} image. \
                         Open the decoded payload in a new Inspect tab \
                         instead of extracting it as a file.",
                        kind.label(),
                    );
                    if ui
                        .add_enabled(!running, egui::Button::new(label))
                        .on_hover_text(tooltip)
                        .clicked()
                    {
                        // Re-borrow the decoded bytes through self —
                        // safe here because the extraction-row closure
                        // above has already returned.
                        if let Some(decoded) = self
                            .loaded
                            .as_ref()
                            .and_then(|l| l.mountable.as_ref())
                            .map(|m| m.decoded.clone())
                        {
                            match write_payload_to_tempdir(&name, &decoded) {
                                Ok(open) => {
                                    log.info(format!(
                                        "Routing {} payload to Inspect tab: {}",
                                        kind.label(),
                                        open.path.display()
                                    ));
                                    self.pending_inspect_open = Some(open);
                                }
                                Err(e) => {
                                    log.error(format!(
                                        "Failed to materialize archive payload: {e}"
                                    ));
                                }
                            }
                        }
                    }
                });
            }
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

/// Inspect a freshly-loaded archive for the Workflow D.2 auto-unwrap
/// case: exactly one non-folder entry, whose data fork decompresses to
/// a payload [`detect_mountable_image`] recognizes. Folders and
/// resource forks don't count — a disk image lives in the data fork.
/// Returns `None` for multi-file archives, decompression failures, or
/// payloads that don't sniff as DC42 / raw HFS / raw HFS+.
fn sniff_mountable_payload(bytes: &[u8], archive: &StuffItArchive) -> Option<MountablePayload> {
    let files: Vec<&_> = archive.entries.iter().filter(|e| !e.is_dir).collect();
    if files.len() != 1 {
        return None;
    }
    let entry = files[0];
    let fork = entry.data.as_ref()?;
    if fork.uncompressed_len == 0 {
        return None;
    }
    let decoded = stuffit::decompress_fork(bytes, fork).ok()?;
    let kind = detect_mountable_image(&decoded)?;
    Some(MountablePayload {
        kind,
        decoded,
        name: entry.name.clone(),
    })
}

/// Write `bytes` to a fresh tempfile under a new [`tempfile::TempDir`]
/// so the file outlives the click handler's frame. The directory guard
/// rides on the returned [`PendingInspectOpen`] and is moved onto
/// `InspectTab::amiga_tempdir` (which already exists for transparent
/// `.adz` decompression) so the temp survives until the user opens a
/// different image.
fn write_payload_to_tempdir(name: &str, bytes: &[u8]) -> std::io::Result<PendingInspectOpen> {
    let dir = tempfile::Builder::new()
        .prefix("rusty-archive-mount-")
        .tempdir()?;
    let safe_name = if name.is_empty() {
        "payload.bin".to_string()
    } else {
        name.replace(['/', '\\'], "_")
    };
    let path = dir.path().join(safe_name);
    std::fs::write(&path, bytes)?;
    Ok(PendingInspectOpen { path, guard: dir })
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
