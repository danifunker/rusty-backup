//! A reusable, self-contained remote file-browser **window** (egui).
//!
//! Wraps the testable [`RemoteBrowser`] core (`model::remote_browser`) in a
//! floating window any tab can pop: connect to an `rb-cli serve` daemon, browse
//! its host filesystem, open a disk image found there — and **switch between
//! images on the same connection without reconnecting** — then browse the files
//! inside it. The listing renders through the shared [`DirListing`] grid model.
//!
//! It is its *own* browser: Inspect's `BrowseView` rebuilds a filesystem from a
//! local path and can't accept the daemon's `Box<dyn Filesystem>`, so the whole
//! browse experience lives in this window. The blocking transitions (connect /
//! open image / close image) run on a worker thread; per-directory navigation
//! within an already-open filesystem is a quick synchronous `list_dir`.

use std::sync::{Arc, Mutex};

use eframe::egui;

use rusty_backup::model::dir_listing::{type_tag, DirListing, Row};
use rusty_backup::model::remote_browser::{BrowseMode, BrowseTarget, RemoteBrowser};
use rusty_backup::partition::format_size;

/// Folder rows — a blue distinct from plain files.
const FOLDER_COLOR: egui::Color32 = egui::Color32::from_rgb(120, 170, 255);
/// A file in a host listing that looks like a disk image we can open (teal).
const IMAGE_COLOR: egui::Color32 = egui::Color32::from_rgb(110, 210, 190);
/// The "Close Image" affordance — amber, reads as "step back out".
const CLOSE_IMAGE_COLOR: egui::Color32 = egui::Color32::from_rgb(230, 170, 90);

/// Whether a filename's extension is one of the disk-image containers the engine
/// can open (so the host browser can tint it as openable).
fn looks_like_disk_image(name: &str) -> bool {
    name.rsplit_once('.')
        .map(|(_, ext)| {
            rusty_backup::model::file_types::DISK_IMAGE_EXTS
                .contains(&ext.to_ascii_lowercase().as_str())
        })
        .unwrap_or(false)
}

/// Worker-thread handoff for a blocking remote transition. Both `RemoteBrowser`
/// and `BrowseTarget` are `Send`, so they move back to the UI thread here.
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

/// Display cache (addr + mode) — held across an in-flight transition so the
/// source bar's label stays stable while the browser is on the worker thread.
struct RemoteConn {
    addr: String,
    mode: BrowseMode,
}

/// State of the modal "Connect to remote..." prompt.
struct ConnectDialog {
    host: String,
    error: Option<String>,
}

/// What the listing grid reports the user did this frame (at most one action).
enum GridAction {
    None,
    Up,
    Enter(String),
    OpenImage(String),
}

/// A self-contained remote file-browser window, poppable from any tab.
#[derive(Default)]
pub struct RemoteBrowserWindow {
    /// Whether the window is shown at all.
    open: bool,
    /// The live browser session (connection + mode). `None` until connected, or
    /// transiently while a transition runs on the worker thread.
    browser: Option<RemoteBrowser>,
    /// Display cache (addr + mode).
    remote: Option<RemoteConn>,
    /// The listing grid model (host browse, or inside an image).
    listing: DirListing,
    /// In-flight transition (connect / open image / close image).
    pending: Option<Arc<Mutex<RemoteTransition>>>,
    /// The connect prompt, if open.
    connect_dialog: Option<ConnectDialog>,
    /// Spinner phase text while a transition runs.
    phase: String,
    /// Last error, shown in the window body.
    error: Option<String>,
}

impl RemoteBrowserWindow {
    /// Open the window. If not already connected (and not mid-connect), pops the
    /// connect prompt seeded with the last-used host.
    pub fn open(&mut self) {
        self.open = true;
        if self.browser.is_none() && self.pending.is_none() {
            self.connect_dialog = Some(ConnectDialog {
                host: self
                    .remote
                    .as_ref()
                    .map(|r| r.addr.clone())
                    .unwrap_or_default(),
                error: None,
            });
        }
    }

    /// Render the window (no-op when closed). Returns an optional status line for
    /// the caller's log.
    pub fn show(&mut self, ctx: &egui::Context) -> Option<String> {
        if !self.open {
            return None;
        }
        let mut status = self.poll(ctx);
        if let Some(s) = self.render_connect_dialog(ctx) {
            status = Some(s);
        }

        let mut keep_open = true;
        let mut action = GridAction::None;
        let mut do_close_image = false;
        let mut do_reconnect = false;
        egui::Window::new("Remote Browser")
            .open(&mut keep_open)
            .resizable(true)
            .default_width(560.0)
            .default_height(420.0)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    let label = match &self.remote {
                        Some(r) => match &r.mode {
                            BrowseMode::Host => format!("Remote {} (host)", r.addr),
                            BrowseMode::Image { path, partition } => {
                                let base = path.rsplit('/').find(|s| !s.is_empty()).unwrap_or(path);
                                let part = partition.map(|n| format!("@{n}")).unwrap_or_default();
                                format!("Remote {}: {base}{part}", r.addr)
                            }
                        },
                        None => "Not connected".to_string(),
                    };
                    ui.strong(label);
                    if ui
                        .button("Connect...")
                        .on_hover_text("Connect to a remote rb-cli serve daemon")
                        .clicked()
                    {
                        do_reconnect = true;
                    }
                    // When inside an image, "Close Image" steps back to the host
                    // file browser on the SAME connection (no reconnect).
                    if matches!(
                        &self.remote,
                        Some(RemoteConn {
                            mode: BrowseMode::Image { .. },
                            ..
                        })
                    ) {
                        let btn = egui::Button::new(
                            egui::RichText::new("Close Image").color(CLOSE_IMAGE_COLOR),
                        );
                        if ui
                            .add(btn)
                            .on_hover_text("Back to browsing the remote host filesystem")
                            .clicked()
                        {
                            do_close_image = true;
                        }
                    }
                });
                ui.horizontal(|ui| {
                    let p = self.listing.cwd_path();
                    ui.monospace(if p.is_empty() { "/" } else { p });
                });
                ui.separator();

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
                    action = self.render_grid(ui);
                } else {
                    ui.centered_and_justified(|ui| {
                        ui.weak("Connect to a remote daemon to browse it here.");
                    });
                }
            });

        if !keep_open {
            return Some(self.close());
        }
        if do_reconnect {
            self.connect_dialog = Some(ConnectDialog {
                host: self
                    .remote
                    .as_ref()
                    .map(|r| r.addr.clone())
                    .unwrap_or_default(),
                error: None,
            });
        }
        if do_close_image {
            status = Some(self.spawn_close_image());
        }

        // Apply the grid action (mutates the listing or spawns an open).
        match action {
            GridAction::None => {}
            GridAction::Up => {
                self.listing.up();
            }
            GridAction::Enter(name) => {
                if let Err(e) = self.listing.enter(&name) {
                    self.error = Some(format!("{e}"));
                }
            }
            GridAction::OpenImage(path) => {
                let opened_from = self.listing.cwd_path().to_string();
                status = Some(self.spawn_open_image(path, None, opened_from));
            }
        }

        status
    }

    /// Render the listing grid. Read-only over `self` — it returns at most one
    /// `GridAction`; the caller applies the mutation afterwards.
    fn render_grid(&self, ui: &mut egui::Ui) -> GridAction {
        let host_mode = matches!(
            self.remote,
            Some(RemoteConn {
                mode: BrowseMode::Host,
                ..
            })
        );
        let mut action = GridAction::None;
        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                egui::Grid::new("remote_browser_grid")
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

    /// The modal "Connect to remote..." prompt (host:port only — you browse the
    /// daemon and open an image you find, rather than naming it up front).
    fn render_connect_dialog(&mut self, ctx: &egui::Context) -> Option<String> {
        self.connect_dialog.as_ref()?;
        let mut do_connect = false;
        let mut cancel = false;
        {
            let d = self.connect_dialog.as_mut().unwrap();
            egui::Window::new("Connect to remote")
                .collapsible(false)
                .resizable(false)
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

    /// Connect to `addr` and browse the host FS at `/`, on a worker thread. A
    /// fresh connection (replaces any prior browser).
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

    /// Open an image found in the host browser as a new handle on the **same**
    /// connection (no reconnect), on a worker thread.
    fn spawn_open_image(
        &mut self,
        path: String,
        partition: Option<u32>,
        opened_from: String,
    ) -> String {
        let Some(mut browser) = self.browser.take() else {
            return "not connected to a remote.".to_string();
        };
        let status = Arc::new(Mutex::new(RemoteTransition {
            addr: browser.addr().to_string(),
            ..Default::default()
        }));
        self.pending = Some(status.clone());
        self.error = None;
        self.phase = format!("Opening {path}...");
        let msg = format!("opening {path}...");
        std::thread::spawn(move || {
            let result = browser
                .open_image(&path, partition, &opened_from)
                .map_err(|e| format!("{e:#}"));
            if let Ok(mut s) = status.lock() {
                s.browser = Some(browser);
                s.result = Some(result);
                s.done = true;
            }
        });
        msg
    }

    /// Step back out of an open image to the host file browser (same
    /// connection), on a worker thread.
    fn spawn_close_image(&mut self) -> String {
        let Some(mut browser) = self.browser.take() else {
            return "not connected to a remote.".to_string();
        };
        let status = Arc::new(Mutex::new(RemoteTransition {
            addr: browser.addr().to_string(),
            ..Default::default()
        }));
        self.pending = Some(status.clone());
        self.error = None;
        self.phase = "Returning to host browser...".to_string();
        let msg = "closing image...".to_string();
        std::thread::spawn(move || {
            let result = browser.close_image().map_err(|e| format!("{e:#}"));
            if let Ok(mut s) = status.lock() {
                s.browser = Some(browser);
                s.result = Some(result);
                s.done = true;
            }
        });
        msg
    }

    /// Poll an in-flight transition; when done, re-install the browser and swap
    /// the new target into the listing.
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
                self.error = None;
                self.remote = Some(RemoteConn {
                    addr: addr.clone(),
                    mode: mode.clone(),
                });
                self.listing
                    .load_root(target.fs, target.root, target.entries, false);
                match mode {
                    BrowseMode::Host => Some(format!("connected to {addr} (browsing host)")),
                    BrowseMode::Image { path, .. } => Some(format!("opened rb://{addr}{path}")),
                }
            }
            Err(e) => {
                if had_browser {
                    // The connection survived a failed open / close — keep the
                    // current listing, just report it.
                    Some(format!("remote operation failed: {e}"))
                } else {
                    // A fresh connect failed — nothing to show but the error.
                    self.error = Some(e.clone());
                    self.remote = None;
                    Some(format!("remote connect failed: {e}"))
                }
            }
        }
    }

    /// Close the window and disconnect: dropping the browser drops the last Arc
    /// to the shared connection (the daemon reaps its handles).
    fn close(&mut self) -> String {
        self.open = false;
        self.browser = None;
        self.remote = None;
        self.pending = None;
        self.connect_dialog = None;
        self.listing = DirListing::new();
        self.error = None;
        self.phase.clear();
        "closed remote browser".to_string()
    }
}
