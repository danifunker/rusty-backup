//! Remote image **picker** for the Inspect tab: connect to an `rb-cli serve`
//! daemon and browse its host filesystem (folders + disk images) inline in the
//! Inspect main area, built on the testable [`RemoteBrowser`] core's host-browse
//! mode.
//!
//! Picking an image (double-click a teal entry) emits an `inspect_request`
//! carrying the shared connection + remote path; the Inspect tab opens it over
//! the **block tier** (a `RemoteBlockReader`) and runs its full pipeline
//! (partition table, per-partition probes) against the remote image with **no
//! download** — the daemon keeps the image open and serves raw byte ranges. Once
//! emitted, the panel collapses (yields the main area to the partition view).

use std::sync::{Arc, Mutex};

use eframe::egui;

use rusty_backup::model::dir_listing::{type_tag, DirListing, Row};
use rusty_backup::model::remote_browser::{BrowseMode, BrowseTarget, RemoteBrowser};
use rusty_backup::partition::format_size;
use rusty_backup::remote::RemoteConnection;

/// Folder rows — a blue distinct from plain files.
const FOLDER_COLOR: egui::Color32 = egui::Color32::from_rgb(120, 170, 255);
/// A file that looks like a disk image we can open (teal).
const IMAGE_COLOR: egui::Color32 = egui::Color32::from_rgb(110, 210, 190);

/// Whether a filename's extension is one of the disk-image containers the engine
/// can open (so the host picker tints it as openable).
fn looks_like_disk_image(name: &str) -> bool {
    name.rsplit_once('.')
        .map(|(_, ext)| {
            rusty_backup::model::file_types::DISK_IMAGE_EXTS
                .contains(&ext.to_ascii_lowercase().as_str())
        })
        .unwrap_or(false)
}

/// Worker-thread handoff for a blocking transition (connect / open / close).
/// Both `RemoteBrowser` and `BrowseTarget` are `Send`, so they move back here.
#[derive(Default)]
struct RemoteTransition {
    done: bool,
    addr: String,
    /// The browser, returned so the UI thread re-installs it. `Some` after any
    /// op on an existing browser (success *or* failure), and after a successful
    /// fresh connect. `None` only when a fresh connect failed.
    browser: Option<RemoteBrowser>,
    result: Option<Result<BrowseTarget, String>>,
}

/// State of the modal "Connect to remote..." prompt.
struct ConnectDialog {
    host: String,
    error: Option<String>,
}

/// What the grid reports the user did this frame (at most one action).
enum GridAction {
    None,
    Up,
    Enter(String),
    OpenImage(String),
}

/// Inline remote browse panel for the Inspect tab.
#[derive(Default)]
pub struct RemoteBrowsePanel {
    /// The live browser session (connection + mode). `None` when disconnected,
    /// or transiently while a transition runs on the worker thread.
    browser: Option<RemoteBrowser>,
    /// Daemon address (display / reconnect seed).
    addr: String,
    /// Display cache of the current mode (held across an in-flight transition).
    mode: Option<BrowseMode>,
    /// The listing grid (host picker, or inside an image).
    listing: DirListing,
    /// In-flight transition (connect / open image / close image).
    pending: Option<Arc<Mutex<RemoteTransition>>>,
    /// The connect prompt, if open.
    connect_dialog: Option<ConnectDialog>,
    /// Spinner phase text while a transition runs.
    phase: String,
    /// Last error, shown in the panel body.
    error: Option<String>,
    /// Status lines accumulated this frame, drained by the caller for the log.
    log: Vec<String>,
    /// Set when the user picks an image to inspect: `(shared connection, remote
    /// path)`. The Inspect tab drains it, opens a `RemoteBlockReader` on the
    /// connection, and runs its full pipeline (partition table, browse) over the
    /// wire. The panel collapses (yields the main area) once emitted.
    inspect_request: Option<(Arc<Mutex<RemoteConnection>>, String)>,
}

impl RemoteBrowsePanel {
    /// Start the connect flow (pops the host:port prompt). If already connected,
    /// re-pops it so the user can switch daemons.
    pub fn open(&mut self) {
        self.connect_dialog = Some(ConnectDialog {
            host: self.addr.clone(),
            error: None,
        });
    }

    /// True when a remote session is live or being established — the Inspect tab
    /// renders this panel's browse area instead of the normal inspection.
    pub fn is_active(&self) -> bool {
        self.pending.is_some() || self.browser.is_some() || self.error.is_some()
    }

    /// Disconnect: drop the browser (the last Arc to the shared connection goes
    /// with it; the daemon reaps its handles) and reset.
    pub fn disconnect(&mut self) {
        let was = !self.addr.is_empty();
        self.browser = None;
        self.mode = None;
        self.pending = None;
        self.connect_dialog = None;
        self.listing = DirListing::new();
        self.error = None;
        self.phase.clear();
        if was {
            self.log.push(format!("disconnected from {}", self.addr));
        }
        self.addr.clear();
    }

    /// Drain the status lines accumulated this frame.
    pub fn take_log(&mut self) -> Vec<String> {
        std::mem::take(&mut self.log)
    }

    /// Drain a pending "inspect this remote image" request — `(shared
    /// connection, remote path)`. The caller opens it over the block tier.
    pub fn take_inspect_request(&mut self) -> Option<(Arc<Mutex<RemoteConnection>>, String)> {
        self.inspect_request.take()
    }

    /// Render the source-bar controls (Disconnect, while picking). Call inside
    /// the Inspect source-bar row, after the source dropdown.
    pub fn source_bar_controls(&mut self, ui: &mut egui::Ui) {
        // Only meaningful while the picker is connected (before an image is
        // handed off to Inspect, at which point the panel collapses).
        if self.browser.is_none() && self.pending.is_none() {
            return;
        }
        if !self.addr.is_empty() {
            let enabled = self.pending.is_none();
            if ui
                .add_enabled(
                    enabled,
                    egui::Button::new(format!("Disconnect from {}", self.addr)),
                )
                .on_hover_text("Close the connection to the remote daemon")
                .clicked()
            {
                self.disconnect();
            }
        }
    }

    /// Render the panel (connect modal + browse area). Returns `true` when a
    /// remote session is active, so the Inspect tab skips its normal inspection.
    pub fn show(&mut self, ui: &mut egui::Ui) -> bool {
        if let Some(s) = self.poll(ui.ctx()) {
            self.log.push(s);
        }
        if let Some(s) = self.render_connect_dialog(ui.ctx()) {
            self.log.push(s);
        }
        if !self.is_active() {
            return false;
        }

        ui.separator();
        let mut action = GridAction::None;
        if self.pending.is_some() {
            ui.add_space(16.0);
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                ui.spinner();
                ui.label(if self.phase.is_empty() {
                    "Working...".to_string()
                } else {
                    self.phase.clone()
                });
            });
        } else if let Some(e) = &self.error {
            ui.add_space(8.0);
            ui.colored_label(egui::Color32::from_rgb(220, 120, 120), e);
        } else if self.listing.is_loaded() {
            let hint = if matches!(self.mode, Some(BrowseMode::Host)) {
                "Double-click a folder to open it, or a disk image to inspect it."
            } else {
                "Browsing the remote image (read-only). Double-click a folder to open it."
            };
            ui.label(egui::RichText::new(hint).weak().small());
            action = self.render_grid(ui);
        }

        // Apply the grid action (mutate the listing / spawn an open).
        match action {
            GridAction::None => {}
            GridAction::Up => self.listing.up(),
            GridAction::Enter(name) => {
                if let Err(e) = self.listing.enter(&name) {
                    self.error = Some(format!("{e}"));
                }
            }
            GridAction::OpenImage(path) => {
                // Hand the image to Inspect to open over the block tier (full
                // partition view). The connection rides along in the request, so
                // the panel can collapse (yield the main area) — the daemon keeps
                // the image open via the block reader on that connection.
                if let Some(b) = &self.browser {
                    self.inspect_request = Some((b.connection(), path.clone()));
                    self.log.push(format!("inspecting remote image {path}"));
                    self.collapse();
                }
            }
        }
        true
    }

    /// Collapse the panel UI after handing an image off to Inspect: the
    /// connection now lives in `inspect_request`, so reset the browse state
    /// without a "disconnected" log.
    fn collapse(&mut self) {
        self.browser = None;
        self.mode = None;
        self.pending = None;
        self.connect_dialog = None;
        self.listing = DirListing::new();
        self.error = None;
        self.phase.clear();
        self.addr.clear();
    }

    /// Render the listing grid. Read-only over `self`; returns one action.
    fn render_grid(&self, ui: &mut egui::Ui) -> GridAction {
        let host_mode = matches!(self.mode, Some(BrowseMode::Host));
        let mut action = GridAction::None;
        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                egui::Grid::new("remote_inspect_grid")
                    .num_columns(3)
                    .striped(true)
                    .spacing([12.0, 2.0])
                    .show(ui, |ui| {
                        ui.strong("Name");
                        ui.strong("Size");
                        ui.strong("Type");
                        ui.end_row();

                        for row in self.listing.current_rows() {
                            match row {
                                Row::Parent => {
                                    if ui.selectable_label(false, "..").double_clicked() {
                                        action = GridAction::Up;
                                    }
                                    ui.label("");
                                    ui.label("dir");
                                    ui.end_row();
                                }
                                Row::Entry(e) => {
                                    let is_img =
                                        !e.is_directory() && looks_like_disk_image(&e.name);
                                    let color = if e.is_directory() {
                                        FOLDER_COLOR
                                    } else if host_mode && is_img {
                                        IMAGE_COLOR
                                    } else {
                                        ui.visuals().text_color()
                                    };
                                    let mut resp = ui.selectable_label(
                                        false,
                                        egui::RichText::new(&e.name).color(color),
                                    );
                                    if host_mode && is_img {
                                        resp =
                                            resp.on_hover_text("Double-click to open this image");
                                    }
                                    if resp.double_clicked() {
                                        if e.is_directory() {
                                            action = GridAction::Enter(e.name.clone());
                                        } else if host_mode && is_img {
                                            action = GridAction::OpenImage(e.path.clone());
                                        }
                                    }
                                    if e.is_directory() {
                                        ui.label("");
                                    } else {
                                        ui.label(format_size(e.size));
                                    }
                                    ui.label(type_tag(e));
                                    ui.end_row();
                                }
                            }
                        }
                    });
            });
        action
    }

    /// The modal "Connect to remote..." prompt (host:port only).
    fn render_connect_dialog(&mut self, ctx: &egui::Context) -> Option<String> {
        self.connect_dialog.as_ref()?;
        let mut do_connect = false;
        let mut cancel = false;
        {
            let d = self.connect_dialog.as_mut().unwrap();
            egui::Window::new("Connect to remote")
                .collapsible(false)
                .resizable(false)
                .order(egui::Order::Foreground)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label("Host (rb-cli serve daemon):");
                    let resp = ui.text_edit_singleline(&mut d.host);
                    ui.label(
                        egui::RichText::new("e.g. 192.168.1.42:7341  (port 7341 if omitted)")
                            .weak()
                            .small(),
                    );
                    if let Some(e) = &d.error {
                        ui.colored_label(egui::Color32::from_rgb(220, 120, 120), e);
                    }
                    let valid = !d.host.trim().is_empty();
                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        if ui
                            .add_enabled(valid, egui::Button::new("Connect"))
                            .clicked()
                        {
                            do_connect = true;
                        }
                        if ui.button("Cancel").clicked() {
                            cancel = true;
                        }
                    });
                    if valid && resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                        do_connect = true;
                    }
                });
        }

        if cancel {
            self.connect_dialog = None;
            return None;
        }
        if do_connect {
            let host_raw = self
                .connect_dialog
                .as_ref()
                .unwrap()
                .host
                .trim()
                .to_string();
            let addr = if host_raw.contains(':') {
                host_raw
            } else {
                format!(
                    "{host_raw}:{}",
                    rusty_backup::remote::protocol::DEFAULT_PORT
                )
            };
            self.connect_dialog = None;
            return Some(self.spawn_connect(addr));
        }
        None
    }

    /// Connect to `addr` and browse the host FS at `/`, on a worker thread.
    fn spawn_connect(&mut self, addr: String) -> String {
        let status = Arc::new(Mutex::new(RemoteTransition {
            addr: addr.clone(),
            ..Default::default()
        }));
        self.pending = Some(status.clone());
        self.browser = None;
        self.error = None;
        self.phase = format!("Connecting to {addr}...");
        let msg = format!("connecting to {addr}...");
        std::thread::spawn(move || {
            let outcome = RemoteBrowser::connect(&addr, "/");
            if let Ok(mut s) = status.lock() {
                match outcome {
                    Ok((browser, target)) => {
                        s.browser = Some(browser);
                        s.result = Some(Ok(target));
                    }
                    Err(e) => s.result = Some(Err(format!("{e:#}"))),
                }
                s.done = true;
            }
        });
        msg
    }

    /// Poll an in-flight transition; on completion re-install the browser and
    /// swap the new target into the listing.
    fn poll(&mut self, ctx: &egui::Context) -> Option<String> {
        let done = match self.pending.as_ref() {
            Some(s) => s.lock().ok().map(|g| g.done).unwrap_or(false),
            None => return None,
        };
        if !done {
            ctx.request_repaint();
            return None;
        }
        let (addr, browser, result) = {
            let arc = self.pending.take()?;
            let mut g = arc.lock().ok()?;
            (g.addr.clone(), g.browser.take(), g.result.take()?)
        };
        self.phase.clear();
        let had_browser = browser.is_some();
        if let Some(b) = browser {
            self.browser = Some(b);
        }
        match result {
            Ok(target) => {
                let mode = target.mode.clone();
                self.addr = addr.clone();
                self.mode = Some(mode.clone());
                self.error = None;
                self.listing
                    .load_root(target.fs, target.root, target.entries, false);
                match mode {
                    BrowseMode::Host => Some(format!("connected to {addr} (browsing host)")),
                    BrowseMode::Image { path, .. } => Some(format!("opened rb://{addr}{path}")),
                }
            }
            Err(e) => {
                if had_browser {
                    // Connection survived a failed open/close — keep the listing.
                    Some(format!("remote operation failed: {e}"))
                } else {
                    // A fresh connect failed — nothing to show but the error.
                    self.error = Some(e.clone());
                    self.mode = None;
                    self.addr.clear();
                    Some(format!("remote connect failed: {e}"))
                }
            }
        }
    }
}
