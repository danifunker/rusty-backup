//! A pane browsing an `rb-cli serve` daemon: connect, the host listing, and images opened on it.
//!
//! One [`RemoteBrowser`] connection holds every image the user opens, so switching images
//! never reconnects. Connect, open and close are jobs; the browser moves into the job and
//! comes back with the new listing, so the pane holds it only between jobs. Applying to a
//! remote image goes through the same preflight / review / write flow as a local one (see
//! `edits.rs`), and the image is reopened afterwards because the daemon's browse handle
//! predates the write. Without the `remote` feature the actions only report that.

use super::{BrowserPaneAction, BrowserPaneController};
use crate::controller::browser::pane::purpose;
use crate::controller::{Effect, TextSpec};
use crate::model::commander_ops::StageSource;
use crate::model::job::Snapshot;

#[cfg(feature = "remote")]
use super::VolumeInfo;
#[cfg(feature = "remote")]
use crate::cli::img_at::PartSelector;
#[cfg(feature = "remote")]
use crate::controller::browser::rows::remote_parent_dir;
#[cfg(feature = "remote")]
use crate::model::edit_queue::StagedEdit;
#[cfg(feature = "remote")]
use crate::model::job::{Job, JobCtx, Resource, Status};
#[cfg(feature = "remote")]
use crate::model::remote_browser::{BrowseMode, BrowseTarget, RemoteBrowser};
#[cfg(feature = "remote")]
use std::path::PathBuf;

#[cfg(not(feature = "remote"))]
const NOT_BUILT: &str = "remote browsing is not built in.";

/// The rule for a daemon address: `host` or `host:port`, no spaces or slashes.
pub fn validate_remote_addr(_spec: &TextSpec, text: &str) -> Result<(), String> {
    let t = text.trim();
    if t.is_empty() {
        return Err(String::new());
    }
    if t.contains(|c: char| c.is_whitespace() || c == '/') {
        return Err("Enter a host name or address, optionally with :port.".into());
    }
    let (host, port) = match t.rsplit_once(':') {
        Some((h, p)) => (h, Some(p)),
        None => (t, None),
    };
    if host.is_empty() {
        return Err("Enter the host before the port.".into());
    }
    match port.map(str::parse::<u16>) {
        Some(Ok(0)) | Some(Err(_)) => Err("The port must be a number from 1 to 65535.".into()),
        _ => Ok(()),
    }
}

fn msg(text: impl Into<String>) -> Vec<Effect> {
    vec![Effect::Message(text.into())]
}

/// What a connect, open or close job hands back: the browser, and the new listing or why not.
#[cfg(feature = "remote")]
struct Transition {
    browser: RemoteBrowser,
    result: Result<BrowseTarget, String>,
}

/// A remote image the pane is browsing, for the apply jobs.
#[cfg(feature = "remote")]
#[derive(Clone)]
pub(super) struct RemoteImage {
    pub addr: String,
    pub path: String,
    pub partition: Option<PartSelector>,
}

#[cfg(feature = "remote")]
impl RemoteImage {
    /// The key apply jobs hold while they write.
    pub(super) fn resource(&self) -> Resource {
        Resource::Image(PathBuf::from(format!("rb://{}{}", self.addr, self.path)))
    }
}

/// The remote half of a pane's state.
#[derive(Default)]
pub(super) struct RemoteState {
    /// The connect prompt is up.
    #[cfg(feature = "remote")]
    asking: bool,
    /// The daemon session; `None` while a job holds it.
    #[cfg(feature = "remote")]
    browser: Option<RemoteBrowser>,
    /// The daemon address and what the pane shows; kept while a job runs.
    #[cfg(feature = "remote")]
    shown: Option<(String, BrowseMode)>,
    #[cfg(feature = "remote")]
    job: Option<Job<Transition>>,
    /// Edits still staged and the folder to return to once a post-apply reopen lands.
    #[cfg(feature = "remote")]
    restore: Option<(Vec<StagedEdit>, String)>,
}

impl BrowserPaneController {
    // --- read accessors ---------------------------------------------------

    /// True while the pane browses a daemon (its host filesystem or an image on it).
    pub fn is_remote(&self) -> bool {
        #[cfg(feature = "remote")]
        return self.remote.shown.is_some();
        #[cfg(not(feature = "remote"))]
        false
    }

    /// True while the pane lists the daemon's host filesystem, where a file opens as an image.
    pub fn is_remote_host(&self) -> bool {
        #[cfg(feature = "remote")]
        return matches!(self.remote.shown, Some((_, BrowseMode::Host)));
        #[cfg(not(feature = "remote"))]
        false
    }

    /// True while the pane browses a disk image on a daemon.
    pub fn is_remote_image(&self) -> bool {
        #[cfg(feature = "remote")]
        return matches!(self.remote.shown, Some((_, BrowseMode::Image { .. })));
        #[cfg(not(feature = "remote"))]
        false
    }

    /// The daemon's `host:port`, while connected.
    pub fn remote_addr(&self) -> Option<String> {
        #[cfg(feature = "remote")]
        return self.remote.shown.as_ref().map(|(a, _)| a.clone());
        #[cfg(not(feature = "remote"))]
        None
    }

    /// The source bar's text for a remote pane: the daemon and the image's base name.
    pub fn remote_label(&self) -> Option<String> {
        #[cfg(feature = "remote")]
        {
            let (addr, mode) = self.remote.shown.as_ref()?;
            Some(match mode {
                BrowseMode::Host => format!("Remote {addr} (host)"),
                BrowseMode::Image { path, partition } => {
                    let base = path.rsplit('/').find(|s| !s.is_empty()).unwrap_or(path);
                    let part = partition.as_ref().map(|n| n.to_string());
                    format!("Remote {addr}: {base}{}", part.unwrap_or_default())
                }
            })
        }
        #[cfg(not(feature = "remote"))]
        None
    }

    /// The connect, open or close in progress.
    pub(super) fn remote_working(&self) -> Option<Snapshot> {
        #[cfg(feature = "remote")]
        return self.remote.job.as_ref().map(|j| j.snapshot());
        #[cfg(not(feature = "remote"))]
        None
    }

    /// How a worker reopens the remote image on a fresh connection.
    pub(super) fn remote_stage_source(&self) -> Option<StageSource> {
        #[cfg(feature = "remote")]
        return self.remote_image().map(|r| StageSource::Remote {
            addr: r.addr,
            path: r.path,
            partition: r.partition,
        });
        #[cfg(not(feature = "remote"))]
        None
    }

    #[cfg(feature = "remote")]
    pub(super) fn remote_image(&self) -> Option<RemoteImage> {
        match &self.remote.shown {
            Some((addr, BrowseMode::Image { path, partition })) => Some(RemoteImage {
                addr: addr.clone(),
                path: path.clone(),
                partition: partition.clone(),
            }),
            _ => None,
        }
    }

    // --- actions ----------------------------------------------------------

    /// Handles a remote action, or hands it back.
    pub(super) fn apply_remote(
        &mut self,
        action: BrowserPaneAction,
    ) -> Result<Vec<Effect>, BrowserPaneAction> {
        use BrowserPaneAction as A;
        let fx = match action {
            #[cfg(feature = "remote")]
            A::Connect => self.ask_connect(),
            #[cfg(feature = "remote")]
            A::Answered { purpose, text } if purpose == purpose::CONNECT => {
                self.connect_answered(text)
            }
            #[cfg(not(feature = "remote"))]
            A::Connect => msg(NOT_BUILT),
            #[cfg(not(feature = "remote"))]
            A::Answered { purpose, .. } if purpose == purpose::CONNECT => Vec::new(),
            A::OpenRemoteImage(name) => self.open_remote_image(&name),
            A::CloseRemoteImage => self.close_remote_image(),
            A::Disconnect if self.is_remote() => self.apply_source(A::CloseSource),
            A::Disconnect => Vec::new(),
            other => return Err(other),
        };
        Ok(fx)
    }

    /// Drop the connection and anything in flight on it.
    pub(super) fn drop_remote(&mut self) {
        // Dropping the browser drops the last handle on the connection; the daemon reaps its handles.
        self.remote = RemoteState::default();
    }

    #[cfg(feature = "remote")]
    fn connect_spec(&self, initial: String, error: Option<String>) -> TextSpec {
        TextSpec {
            purpose: purpose::CONNECT,
            title: "Connect to remote".into(),
            prompt:
                "Host of an rb-cli serve daemon, e.g. 192.168.1.42:7341 (port 7341 if omitted):"
                    .into(),
            initial,
            ok_label: "Connect".into(),
            unchanged: String::new(),
            taken: Vec::new(),
            fold_case: false,
            forbidden: String::new(),
            error,
            validator: validate_remote_addr,
        }
    }

    #[cfg(feature = "remote")]
    fn ask_connect(&mut self) -> Vec<Effect> {
        if !self.queue.is_empty() {
            return msg("apply or discard staged edits before connecting to a remote.");
        }
        if self.is_busy() {
            return msg("wait for the current operation to finish.");
        }
        self.remote.asking = true;
        let initial = self.remote_addr().unwrap_or_default();
        vec![Effect::AskText(self.connect_spec(initial, None))]
    }

    #[cfg(feature = "remote")]
    fn connect_answered(&mut self, text: Option<String>) -> Vec<Effect> {
        if !std::mem::take(&mut self.remote.asking) {
            return Vec::new();
        }
        let Some(text) = text else {
            return Vec::new();
        };
        let addr = text.trim().to_string();
        let spec = self.connect_spec(addr.clone(), None);
        if let Err(why) = spec.validate(&addr) {
            let why = if why.is_empty() {
                "Enter the daemon's host.".to_string()
            } else {
                why
            };
            self.remote.asking = true;
            return vec![Effect::AskText(self.connect_spec(addr, Some(why)))];
        }
        self.start_connect(addr)
    }

    #[cfg(feature = "remote")]
    fn start_connect(&mut self, typed: String) -> Vec<Effect> {
        let addr = crate::remote::protocol::RemoteRef::parse(&format!("rb://{typed}"))
            .map(|r| r.addr())
            .unwrap_or(typed);
        // A new connection replaces the old one and any local source.
        self.reset_source();
        let resource = Resource::Folder(PathBuf::from(format!("rb://{addr}/")));
        let to = addr.clone();
        let spawned = self.spawn_remote(format!("Connecting to {addr}"), resource, move |ctx| {
            ctx.set_phase(format!("Connecting to {to}..."));
            let (browser, target) = RemoteBrowser::connect(&to, "/")?;
            Ok(Transition {
                browser,
                result: Ok(target),
            })
        });
        match spawned {
            Ok(()) => msg(format!("connecting to {addr}...")),
            Err(busy) => msg(format!("cannot connect: {busy}")),
        }
    }

    #[cfg(not(feature = "remote"))]
    pub(super) fn open_remote_image(&mut self, _name: &str) -> Vec<Effect> {
        msg(NOT_BUILT)
    }

    /// Open a file of the remote host listing as an image on the same connection.
    #[cfg(feature = "remote")]
    pub(super) fn open_remote_image(&mut self, name: &str) -> Vec<Effect> {
        if !self.is_remote_host() {
            return msg("open an image from a remote host listing.");
        }
        let path = self.listing.entries().iter().find(|e| e.name == name);
        let Some(path) = path.map(|e| e.path.clone()) else {
            return Vec::new();
        };
        let from = self.listing.cwd_path().to_string();
        self.start_open(path, None, from)
    }

    #[cfg(not(feature = "remote"))]
    fn close_remote_image(&mut self) -> Vec<Effect> {
        msg(NOT_BUILT)
    }

    /// Back to the host folder the image was opened from, on the same connection.
    #[cfg(feature = "remote")]
    fn close_remote_image(&mut self) -> Vec<Effect> {
        if !self.is_remote_image() {
            return Vec::new();
        }
        if !self.queue.is_empty() {
            return msg("apply or discard staged edits before closing the image.");
        }
        let Some(browser) = self.take_browser() else {
            return msg("wait for the current operation to finish.");
        };
        let resource = Resource::Folder(PathBuf::from(format!("rb://{}/", browser.addr())));
        let spawned = self.spawn_browser_job(
            "Returning to the host".into(),
            resource,
            browser,
            |browser| browser.close_image(),
        );
        match spawned {
            Ok(()) => msg("closing image..."),
            Err(e) => msg(e),
        }
    }

    #[cfg(not(feature = "remote"))]
    pub(super) fn reopen_remote_image(&mut self) -> Vec<Effect> {
        Vec::new()
    }

    /// Reopen the image after a write, keeping what is still staged and the current folder.
    #[cfg(feature = "remote")]
    pub(super) fn reopen_remote_image(&mut self) -> Vec<Effect> {
        let Some(image) = self.remote_image() else {
            return Vec::new();
        };
        let keep: Vec<StagedEdit> = self.queue.drain().collect();
        let cwd = self.listing.cwd_path().to_string();
        let from = remote_parent_dir(&image.path);
        let fx = self.start_open(image.path, image.partition, from);
        if self.remote.job.is_some() {
            self.remote.restore = Some((keep, cwd));
        } else {
            for e in keep {
                self.queue.push(e);
            }
        }
        fx
    }

    #[cfg(feature = "remote")]
    fn start_open(
        &mut self,
        path: String,
        partition: Option<PartSelector>,
        from: String,
    ) -> Vec<Effect> {
        let Some(browser) = self.take_browser() else {
            return msg("wait for the current operation to finish.");
        };
        let resource = Resource::Image(PathBuf::from(format!("rb://{}{path}", browser.addr())));
        let base = path.rsplit('/').find(|s| !s.is_empty()).unwrap_or(&path);
        let label = format!("Opening {base}");
        let p = path.clone();
        let spawned = self.spawn_browser_job(label, resource, browser, move |browser| {
            browser.open_image(&p, partition, &from)
        });
        match spawned {
            Ok(()) => msg(format!("opening {path}...")),
            Err(e) => msg(e),
        }
    }

    /// The browser, unless a job holds it.
    #[cfg(feature = "remote")]
    fn take_browser(&mut self) -> Option<RemoteBrowser> {
        if self.remote.job.is_some() {
            return None;
        }
        self.remote.browser.take()
    }

    /// Run `step` on the browser as a job; the browser comes back with its result either way.
    #[cfg(feature = "remote")]
    fn spawn_browser_job(
        &mut self,
        label: String,
        resource: Resource,
        browser: RemoteBrowser,
        step: impl FnOnce(&mut RemoteBrowser) -> anyhow::Result<BrowseTarget> + Send + 'static,
    ) -> Result<(), String> {
        let holder = self.job_services().0.holder(&resource);
        if let Some(holder) = holder {
            self.remote.browser = Some(browser);
            return Err(format!(
                "{} is in use by {holder}.",
                resource.path().display()
            ));
        }
        let phase = format!("{label}...");
        let spawned = self.spawn_remote(label, resource, move |ctx| {
            ctx.set_phase(phase);
            let mut browser = browser;
            let result = step(&mut browser).map_err(|e| format!("{e:#}"));
            Ok(Transition { browser, result })
        });
        if let Err(busy) = &spawned {
            // The refused job took the browser with it.
            self.reset_source();
            self.set_source_error(format!("Remote connection dropped: {busy}"));
        }
        spawned
    }

    #[cfg(feature = "remote")]
    fn spawn_remote(
        &mut self,
        label: String,
        resource: Resource,
        body: impl FnOnce(&JobCtx) -> anyhow::Result<Transition> + Send + 'static,
    ) -> Result<(), String> {
        let (jobs, notify) = self.job_services();
        let spawned = jobs.spawn(label, vec![resource], notify, move |ctx| {
            ctx.set_can_cancel(false);
            body(ctx)
        });
        match spawned {
            Ok(job) => {
                self.remote.job = Some(job);
                Ok(())
            }
            Err(busy) => Err(busy.to_string()),
        }
    }

    // --- polling ----------------------------------------------------------

    #[cfg(not(feature = "remote"))]
    pub(super) fn poll_remote(&mut self) -> Vec<Effect> {
        Vec::new()
    }

    /// Installs a finished connect, open or close; called from `tick`.
    #[cfg(feature = "remote")]
    pub(super) fn poll_remote(&mut self) -> Vec<Effect> {
        let finished = self.remote.job.as_ref().is_some_and(|j| !j.is_running());
        if !finished {
            return Vec::new();
        }
        let Some(job) = self.remote.job.take() else {
            return Vec::new();
        };
        let restore = self.remote.restore.take();
        match (job.snapshot().status, job.take_result()) {
            (Status::Done, Some(t)) => {
                let addr = t.browser.addr().to_string();
                self.remote.browser = Some(t.browser);
                match t.result {
                    Ok(target) => self.show_remote_target(addr, target, restore),
                    Err(e) => {
                        // The connection survived; keep the listing and what was staged.
                        for edit in restore.into_iter().flat_map(|(edits, _)| edits) {
                            self.queue.push(edit);
                        }
                        msg(format!("remote operation failed: {e}"))
                    }
                }
            }
            (status, _) => {
                let why = match status {
                    Status::Failed(e) => e,
                    _ => "cancelled".to_string(),
                };
                let connected = self.remote.shown.is_some();
                self.reset_source();
                if connected {
                    self.set_source_error(format!("Lost the remote connection: {why}"));
                    msg("lost the remote connection.")
                } else {
                    self.set_source_error(why.clone());
                    msg(format!("remote connect failed: {why}"))
                }
            }
        }
    }

    #[cfg(feature = "remote")]
    fn show_remote_target(
        &mut self,
        addr: String,
        target: BrowseTarget,
        restore: Option<(Vec<StagedEdit>, String)>,
    ) -> Vec<Effect> {
        let volume = VolumeInfo {
            label: target.volume_label.clone(),
            fs_type: target.fs_type.clone(),
            total_size: target.total_size,
            used_size: target.used_size,
        };
        let mode = target.mode.clone();
        self.show_remote_listing(target.fs, target.root, target.entries, volume);
        self.remote.shown = Some((addr.clone(), mode.clone()));
        if let Some((edits, cwd)) = restore {
            for e in edits {
                self.queue.push(e);
            }
            if let Err(e) = self.listing.navigate_to(&cwd) {
                log::warn!("could not return to {cwd} after reopening: {e}");
            }
        }
        match mode {
            BrowseMode::Host => msg(format!("connected to {addr} (browsing host)")),
            BrowseMode::Image { path, .. } => msg(format!("opened rb://{addr}{path}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::Controller;

    type A = BrowserPaneAction;

    fn spec() -> TextSpec {
        TextSpec {
            purpose: purpose::CONNECT,
            title: String::new(),
            prompt: String::new(),
            initial: String::new(),
            ok_label: String::new(),
            unchanged: String::new(),
            taken: Vec::new(),
            fold_case: false,
            forbidden: String::new(),
            error: None,
            validator: validate_remote_addr,
        }
    }

    #[test]
    fn the_address_validator_takes_host_and_host_port() {
        let s = spec();
        assert_eq!(s.validate(""), Err(String::new()));
        assert_eq!(s.validate("   "), Err(String::new()));
        assert!(s.validate("192.168.1.42").is_ok());
        assert!(s.validate(" mister.local:7341 ").is_ok());
        assert!(s.validate("host:0").is_err());
        assert!(s.validate("host:99999").is_err());
        assert!(s.validate("host:port").is_err());
        assert!(s.validate(":7341").is_err());
        assert!(s.validate("rb://host/").is_err());
        assert!(s.validate("a host").is_err());
    }

    #[test]
    fn remote_actions_without_a_connection_do_nothing_harmful() {
        let mut vm = BrowserPaneController::new();
        assert!(!vm.is_remote() && vm.remote_addr().is_none() && vm.remote_label().is_none());
        assert!(vm.apply(A::Disconnect).is_empty());
        assert!(vm.apply(A::CloseRemoteImage).is_empty() || !cfg!(feature = "remote"));
        let fx = vm.apply(A::OpenRemoteImage("disk.img".into()));
        assert_eq!(fx.len(), 1, "{fx:?}");
        // An answer nobody asked for is ignored.
        let fx = vm.apply(A::Answered {
            purpose: purpose::CONNECT,
            text: Some("host".into()),
        });
        assert!(fx.is_empty(), "{fx:?}");
        assert!(!vm.is_opening());
    }

    #[cfg(not(feature = "remote"))]
    #[test]
    fn connect_says_it_is_not_built_in() {
        let mut vm = BrowserPaneController::new();
        assert_eq!(vm.apply(A::Connect), msg(NOT_BUILT));
    }
}
