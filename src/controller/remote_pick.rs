//! The Inspect tab's remote image picker: connect to an `rb-cli serve` daemon and pick an image.
//!
//! [`RemotePickController`] asks for the daemon's address (`Effect::AskText` with the browser
//! pane's address validator), connects in a job, lists the daemon's host folders (each folder
//! read in a job), and hands a picked disk image to the host as a [`RemoteImage`] (the shared
//! connection plus the daemon-side path), which Inspect opens over the block tier. It can also
//! browse again on the connection of an image already open. Shell:
//! `src/gui/remote_browser.rs`. See docs/phase2_commander_plan.md § "Slice 15".

use std::sync::Arc;

use crate::controller::browser::pane::validate_remote_addr;
use crate::controller::{job_outcome, Controller, Effect, TextSpec};
use crate::model::dir_listing::{type_tag, DirListing, EnteredDir, Row};
use crate::model::job::{Job, Notify, Snapshot};
use crate::model::remote_browser::{BrowseMode, BrowseTarget, RemoteBrowser};

/// A remote image: the shared connection and the daemon-side path.
pub type RemoteImage = crate::model::inspect_runner::RemoteImage;

/// The `purpose` of each prompt this controller asks for.
pub mod purpose {
    pub const CONNECT: &str = "remote_pick.connect";
}

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const DISCONNECT: &str = "remote_pick.disconnect";
    pub const LIST: &str = "remote_pick.list";
}

/// User intents.
pub enum RemotePickAction {
    /// Ask for a daemon address, then connect.
    Connect,
    Answered {
        purpose: &'static str,
        text: Option<String>,
    },
    /// Browse the daemon again on the connection of an image already open (no reconnect).
    BrowseOn(RemoteImage),
    /// Drop the connection and the listing.
    Disconnect,
    /// The `..` row.
    Up,
    /// Open the folder with this name.
    Enter(String),
    /// Hand the image at this daemon path to the host.
    OpenImage(String),
}

/// One row of the listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PickRow {
    /// The `..` row.
    pub parent: bool,
    pub name: String,
    /// The daemon-side path, for `OpenImage`.
    pub path: String,
    pub is_dir: bool,
    /// A disk image the engine can open (only offered while browsing the host).
    pub openable: bool,
    /// Empty for a folder.
    pub size: String,
    pub type_tag: String,
}

/// A connect / reopen job's result: the address, the browser (if it survived) and the listing.
struct Transition {
    addr: String,
    browser: Option<RemoteBrowser>,
    result: Result<BrowseTarget, String>,
}

/// Whether a file name's extension is a disk-image container the engine can open.
fn looks_like_disk_image(name: &str) -> bool {
    name.rsplit_once('.')
        .map(|(_, ext)| {
            crate::model::file_types::DISK_IMAGE_EXTS.contains(&ext.to_ascii_lowercase().as_str())
        })
        .unwrap_or(false)
}

pub struct RemotePickController {
    notify: Arc<dyn Notify>,
    browser: Option<RemoteBrowser>,
    addr: String,
    mode: Option<BrowseMode>,
    listing: DirListing,
    entering: Option<Job<EnteredDir>>,
    pending: Option<Job<Transition>>,
    asking: bool,
    error: Option<String>,
    log: Vec<String>,
    request: Option<RemoteImage>,
}

impl RemotePickController {
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        RemotePickController {
            notify,
            browser: None,
            addr: String::new(),
            mode: None,
            listing: DirListing::new(),
            entering: None,
            pending: None,
            asking: false,
            error: None,
            log: Vec::new(),
            request: None,
        }
    }

    /// Status lines since the last call ("connected to ...").
    pub fn take_log(&mut self) -> Vec<String> {
        std::mem::take(&mut self.log)
    }

    /// A picked image for the host to open, handed over once.
    pub fn take_request(&mut self) -> Option<RemoteImage> {
        self.request.take()
    }

    /// The picker owns the main area: connected, connecting, or showing a failed connect.
    pub fn is_active(&self) -> bool {
        self.pending.is_some() || self.browser.is_some() || self.error.is_some()
    }

    /// "Disconnect from <addr>" applies: connected or connecting, with an address known.
    pub fn can_disconnect(&self) -> bool {
        (self.browser.is_some() || self.pending.is_some()) && !self.addr.is_empty()
    }

    /// Disconnect waits for a connect or reopen to land.
    pub fn transition_running(&self) -> bool {
        self.pending.is_some()
    }

    pub fn addr(&self) -> &str {
        self.addr.as_str()
    }

    /// The connect, reopen or folder read in flight.
    pub fn working(&self) -> Option<Snapshot> {
        match (&self.entering, &self.pending) {
            (Some(job), _) => Some(job.snapshot()),
            (None, Some(job)) => Some(job.snapshot()),
            (None, None) => None,
        }
    }

    /// Why the last connect or folder read failed.
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// The listing is ready to show.
    pub fn is_loaded(&self) -> bool {
        self.listing.is_loaded()
    }

    /// The line above the listing.
    pub fn hint(&self) -> &'static str {
        if self.browsing_host() {
            "Double-click a folder to open it, or a disk image to inspect it."
        } else {
            "Browsing the remote image (read-only). Double-click a folder to open it."
        }
    }

    fn browsing_host(&self) -> bool {
        matches!(self.mode, Some(BrowseMode::Host))
    }

    pub fn rows(&self) -> Vec<PickRow> {
        let host = self.browsing_host();
        self.listing
            .current_rows()
            .into_iter()
            .map(|row| match row {
                Row::Parent => PickRow {
                    parent: true,
                    name: "..".to_string(),
                    path: String::new(),
                    is_dir: true,
                    openable: false,
                    size: String::new(),
                    type_tag: "dir".to_string(),
                },
                Row::Entry(e) => PickRow {
                    parent: false,
                    name: e.name.clone(),
                    path: e.path.clone(),
                    is_dir: e.is_directory(),
                    openable: host && !e.is_directory() && looks_like_disk_image(e.name.as_str()),
                    size: if e.is_directory() {
                        String::new()
                    } else {
                        disk_core::display::format_size(e.size)
                    },
                    type_tag: type_tag(e),
                },
            })
            .collect()
    }

    fn connect_spec(&self) -> TextSpec {
        TextSpec {
            purpose: purpose::CONNECT,
            title: "Connect to remote".to_string(),
            prompt: "Host (rb-cli serve daemon), e.g. 192.168.1.42:7341 (port 7341 if omitted):"
                .to_string(),
            initial: self.addr.clone(),
            ok_label: "Connect".to_string(),
            unchanged: String::new(),
            taken: Vec::new(),
            fold_case: false,
            forbidden: String::new(),
            error: None,
            validator: validate_remote_addr,
        }
    }

    fn answered(&mut self, text: Option<String>) -> Vec<Effect> {
        if !std::mem::take(&mut self.asking) {
            return Vec::new();
        }
        let Some(text) = text else {
            return Vec::new();
        };
        let typed = text.trim().to_string();
        let spec = self.connect_spec();
        if let Err(why) = spec.validate(typed.as_str()) {
            self.asking = true;
            let mut again = spec;
            again.initial = typed;
            again.error = Some(if why.is_empty() {
                "Enter the daemon's host.".to_string()
            } else {
                why
            });
            return vec![Effect::AskText(again)];
        }
        let addr = if typed.contains(':') {
            typed
        } else {
            format!("{typed}:{}", crate::remote::protocol::DEFAULT_PORT)
        };
        self.start_connect(addr);
        Vec::new()
    }

    fn start_connect(&mut self, addr: String) {
        self.browser = None;
        self.error = None;
        self.log.push(format!("connecting to {addr}..."));
        let to = addr.clone();
        self.pending = Some(Job::spawn(
            format!("Connecting to {addr}..."),
            Arc::clone(&self.notify),
            move |ctx| {
                ctx.set_can_cancel(false);
                Ok(match RemoteBrowser::connect(to.as_str(), "/") {
                    Ok((browser, target)) => Transition {
                        addr: to,
                        browser: Some(browser),
                        result: Ok(target),
                    },
                    Err(e) => Transition {
                        addr: to,
                        browser: None,
                        result: Err(format!("{e:#}")),
                    },
                })
            },
        ));
    }

    fn browse_on(&mut self, conn: RemoteImage) {
        let (conn, _) = conn;
        self.error = None;
        self.pending = Some(Job::spawn(
            "Reopening remote browser...",
            Arc::clone(&self.notify),
            move |ctx| {
                ctx.set_can_cancel(false);
                let addr = crate::model::worker::lock_status(&*conn).addr().to_string();
                Ok(match RemoteBrowser::from_connection(conn, "/") {
                    Ok((browser, target)) => Transition {
                        addr,
                        browser: Some(browser),
                        result: Ok(target),
                    },
                    Err(e) => Transition {
                        addr,
                        browser: None,
                        result: Err(format!("{e:#}")),
                    },
                })
            },
        ));
    }

    fn disconnect(&mut self) {
        if self.pending.is_some() {
            return;
        }
        let was = !self.addr.is_empty();
        let addr = std::mem::take(&mut self.addr);
        self.reset();
        if was {
            self.log.push(format!("disconnected from {addr}"));
        }
    }

    /// Drop the browse state; the connection lives on only in a handed-over request.
    fn reset(&mut self) {
        self.browser = None;
        self.mode = None;
        self.pending = None;
        self.asking = false;
        self.listing = DirListing::new();
        self.entering = None;
        self.error = None;
        self.addr.clear();
    }

    fn enter(&mut self, name: &str) {
        if self.entering.is_some() || self.pending.is_some() {
            return;
        }
        match self.listing.begin_enter(name) {
            Ok(pending) => {
                self.entering = Some(Job::spawn(
                    format!("Opening {name}..."),
                    Arc::clone(&self.notify),
                    move |job| {
                        job.set_can_cancel(false);
                        Ok(pending.run())
                    },
                ))
            }
            Err(e) => self.error = Some(format!("{e}")),
        }
    }

    fn open_image(&mut self, path: String) {
        if let Some(b) = &self.browser {
            self.request = Some((b.connection(), path.clone()));
            self.log.push(format!("inspecting remote image {path}"));
            self.reset();
        }
    }

    fn poll(&mut self) {
        if let Some(done) = self.entering.as_ref().and_then(job_outcome) {
            self.entering = None;
            match done {
                Ok(entered) => {
                    if let Err(e) = self.listing.finish_enter(entered) {
                        self.error = Some(format!("{e}"));
                    }
                }
                Err(_) => self.error = Some("Listing the folder failed".to_string()),
            }
        }
        let Some(done) = self.pending.as_ref().and_then(job_outcome) else {
            return;
        };
        self.pending = None;
        let t = match done {
            Ok(t) => t,
            Err(e) => Transition {
                addr: self.addr.clone(),
                browser: None,
                result: Err(e),
            },
        };
        let had_browser = t.browser.is_some();
        if let Some(b) = t.browser {
            self.browser = Some(b);
        }
        match t.result {
            Ok(target) => {
                let mode = target.mode.clone();
                self.addr = t.addr.clone();
                self.mode = Some(mode.clone());
                self.error = None;
                self.entering = None;
                self.listing
                    .load_root(target.fs, target.root, target.entries, false);
                self.log.push(match mode {
                    BrowseMode::Host => format!("connected to {} (browsing host)", t.addr),
                    BrowseMode::Image { path, .. } => format!("opened rb://{}{path}", t.addr),
                });
            }
            Err(e) if had_browser => self.log.push(format!("remote operation failed: {e}")),
            Err(e) => {
                self.error = Some(e.clone());
                self.mode = None;
                self.addr.clear();
                self.log.push(format!("remote connect failed: {e}"));
            }
        }
    }

    /// Block until the running connect or folder read ends; for a headless shell or a test.
    pub fn wait_idle(&mut self) {
        while self.pending.is_some() || self.entering.is_some() {
            self.poll();
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
}

impl Controller for RemotePickController {
    type Action = RemotePickAction;

    fn apply(&mut self, action: RemotePickAction) -> Vec<Effect> {
        match action {
            RemotePickAction::Connect => {
                if self.pending.is_none() {
                    self.asking = true;
                    return vec![Effect::AskText(self.connect_spec())];
                }
            }
            RemotePickAction::Answered { purpose, text } => {
                if purpose == purpose::CONNECT {
                    return self.answered(text);
                }
            }
            RemotePickAction::BrowseOn(conn) => self.browse_on(conn),
            RemotePickAction::Disconnect => self.disconnect(),
            RemotePickAction::Up => {
                if self.entering.is_none() {
                    self.listing.up();
                }
            }
            RemotePickAction::Enter(name) => self.enter(name.as_str()),
            RemotePickAction::OpenImage(path) => self.open_image(path),
        }
        Vec::new()
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.poll();
        Vec::new()
    }
}
