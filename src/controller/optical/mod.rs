//! The Optical tab: rip a physical drive (local or on a remote daemon) or convert a disc image,
//! with the disc browser and the CD-DA player beside it.
//!
//! [`OpticalController`] owns the source (drive list, remote daemons, the image file), the
//! detected disc, the output form, and every job: the drive scan, the detect, Eject, CHD Info, the
//! remote connect and the rip / conversion. It hosts [`browse::DiscBrowseController`] and
//! [`audio::AudioPlayerController`]. The operation bodies are `model::optical_ops`. Shells: the
//! egui tab (`src/gui/optical_tab.rs`) and the TUI's rip screen. See
//! docs/phase2_commander_plan.md § "Slice 16".

pub mod audio;
pub mod browse;
#[cfg(feature = "remote")]
mod remote;
mod run;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use opticaldiscs::detect::DiscImageInfo;

use crate::controller::log::LogLevel;
use crate::controller::{job_outcome, Controller, Effect, PickSpec};
use crate::model::job::{Job, Notify, Snapshot};
use crate::model::optical_devices::RipDevice;
use crate::model::optical_ops;

pub use crate::controller::backup::ChdChoice;
pub use crate::model::job::NoNotify;
pub use crate::model::optical_ops::OutputFormat;
pub use crate::rbformats::chd_options::{ChdOptions, ChdProfile};
pub use audio::{AudioAction, PlayState};
pub use browse::{DiscBrowseAction, DiscBrowseController};

/// The `purpose` of each picker this controller asks for (the browser's are `browse::purpose`).
pub mod purpose {
    pub const IMAGE: &str = "optical.image";
    pub const OUTPUT: &str = "optical.output";
}

/// Stable widget ids.
pub mod ids {
    pub const SOURCE_DRIVE: &str = "optical.source_drive";
    pub const SOURCE_IMAGE: &str = "optical.source_image";
    pub const ADD_REMOTE: &str = "optical.add_remote";
    pub const DRIVE: &str = "optical.drive";
    pub const REFRESH: &str = "optical.refresh";
    pub const EJECT: &str = "optical.eject";
    pub const BROWSE_IMAGE: &str = "optical.browse_image";
    pub const BROWSE_CONTENTS: &str = "optical.browse_contents";
    pub const CLOSE_DISC: &str = "optical.close_disc";
    pub const CHD_INFO: &str = "optical.chd_info";
    pub const OUTPUT_FORMAT: &str = "optical.output_format";
    pub const OUTPUT: &str = "optical.output";
    pub const EJECT_AFTER: &str = "optical.eject_after";
    pub const START: &str = "optical.start";
    pub const CANCEL: &str = "optical.cancel";
    pub const REMOTE_ADDRESS: &str = "optical.remote_address";
    pub const REMOTE_CONNECT: &str = "optical.remote_connect";
    pub const REMOTE_CANCEL: &str = "optical.remote_cancel";
}

/// Where the disc comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceMode {
    PhysicalDrive,
    ImageFile,
}

/// What Start does: a drive is ripped, an image converted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Task {
    Rip,
    Convert,
}

/// The detected disc, as the info line shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscSummary {
    pub format: String,
    pub filesystem: String,
    pub volume: Option<String>,
    /// A drive's capacity and sector count.
    pub size: Option<String>,
    pub game: Option<String>,
}

/// The shared progress bar's numbers while a rip or conversion runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpticalBar {
    pub operation: String,
    pub current_bytes: u64,
    pub total_bytes: u64,
}

/// Something only the shell can do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostRequest {
    /// An image was opened: put it at the top of the Optical recent list.
    RememberRecent(PathBuf),
}

/// User intents.
#[derive(Debug, Clone)]
pub enum OpticalAction {
    /// Scan the drives at start-up; logs how many were found, if any.
    FindDrives,
    /// "Refresh": re-scan the local drives and every connected daemon's.
    RefreshDrives,
    SetSourceMode(SourceMode),
    SelectDrive(Option<usize>),
    Eject,
    /// "Browse...": asks for a disc image.
    PickImage,
    /// An image picked, dropped, chosen from the recent list or handed over by Inspect.
    OpenImage(PathBuf),
    /// "Add remote daemon...": opens the connect dialog.
    OpenAddRemote,
    CloseAddRemote,
    /// Connect to the daemon at this `host:port`.
    ConnectRemote(String),
    BrowseContents,
    CloseDisc,
    ChdInfo,
    CloseChdInfo,
    SetOutputFormat(OutputFormat),
    /// The output's "Browse...": asks where to write.
    PickOutput,
    SetEjectAfter(bool),
    /// Start the rip or conversion, with the CHD control's choice when CHD is the output.
    Start(Option<ChdChoice>),
    Cancel,
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    Browse(DiscBrowseAction),
    Audio(AudioAction),
}

/// Why a drive scan runs: start-up and Refresh log what they found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DriveScan {
    Startup,
    Refresh,
    Quiet,
}

pub struct OpticalController {
    notify: Arc<dyn Notify>,
    log: Vec<(LogLevel, String)>,
    requests: Vec<HostRequest>,
    mode: SourceMode,
    drives: Vec<RipDevice>,
    drives_job: Option<(Job<Vec<RipDevice>>, DriveScan)>,
    drive: Option<usize>,
    image: Option<PathBuf>,
    disc: Option<DiscImageInfo>,
    /// The detect; `true` reopens the browser once it lands.
    detecting: Option<(Job<Result<DiscImageInfo, String>>, bool)>,
    disc_error: Option<String>,
    ejecting: Option<Job<(String, Result<(), String>)>>,
    chd_info: Option<String>,
    chd_info_job: Option<Job<Result<String, String>>>,
    format: OutputFormat,
    output: Option<PathBuf>,
    eject_after: bool,
    run: run::RunState,
    #[cfg(feature = "remote")]
    remote: remote::RemoteState,
    browse: DiscBrowseController,
    #[cfg(feature = "audio")]
    audio: audio::AudioPlayerController,
}

impl OpticalController {
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        OpticalController {
            log: Vec::new(),
            requests: Vec::new(),
            mode: SourceMode::ImageFile,
            drives: Vec::new(),
            drives_job: None,
            drive: None,
            image: None,
            disc: None,
            detecting: None,
            disc_error: None,
            ejecting: None,
            chd_info: None,
            chd_info_job: None,
            format: OutputFormat::Iso,
            output: None,
            eject_after: false,
            run: run::RunState::default(),
            #[cfg(feature = "remote")]
            remote: remote::RemoteState::load(),
            browse: DiscBrowseController::new(Arc::clone(&notify)),
            #[cfg(feature = "audio")]
            audio: audio::AudioPlayerController::new(Arc::clone(&notify)),
            notify,
        }
    }

    /// Lines for the log panel since the last call.
    pub fn take_log(&mut self) -> Vec<(LogLevel, String)> {
        std::mem::take(&mut self.log)
    }

    pub fn take_requests(&mut self) -> Vec<HostRequest> {
        std::mem::take(&mut self.requests)
    }

    fn info(&mut self, msg: impl Into<String>) {
        self.log.push((LogLevel::Info, msg.into()));
    }

    fn warn(&mut self, msg: impl Into<String>) {
        self.log.push((LogLevel::Warning, msg.into()));
    }

    fn error(&mut self, msg: impl Into<String>) {
        self.log.push((LogLevel::Error, msg.into()));
    }

    pub fn source_mode(&self) -> SourceMode {
        self.mode
    }

    pub fn task(&self) -> Task {
        match self.mode {
            SourceMode::PhysicalDrive => Task::Rip,
            SourceMode::ImageFile => Task::Convert,
        }
    }

    /// The drive picker's labels.
    pub fn drive_labels(&self) -> Vec<String> {
        self.drives.iter().map(|d| d.picker_label()).collect()
    }

    pub fn drive_count(&self) -> usize {
        self.drives.len()
    }

    pub fn selected_drive(&self) -> Option<usize> {
        self.drive
    }

    /// A drive's name and device path, for a listing that shows both.
    pub fn drive_names(&self) -> Vec<(String, String)> {
        self.drives
            .iter()
            .map(|d| (d.display_name.clone(), d.device_path.clone()))
            .collect()
    }

    /// A drive scan runs.
    pub fn scanning_drives(&self) -> bool {
        self.drives_job.is_some()
    }

    fn selected_device(&self) -> Option<&RipDevice> {
        self.drive.and_then(|i| self.drives.get(i))
    }

    /// The selected local drive's device path, when it has a disc to eject.
    fn ejectable(&self) -> Option<String> {
        self.selected_device()
            .filter(|d| !d.is_remote() && !d.device_path.is_empty())
            .map(|d| d.device_path.clone())
    }

    pub fn can_eject(&self) -> bool {
        self.ejecting.is_none() && self.ejectable().is_some()
    }

    pub fn image(&self) -> Option<&Path> {
        self.image.as_deref()
    }

    /// A remote daemon is connected: physical-drive mode is offered without elevation.
    pub fn has_remote_daemons(&self) -> bool {
        #[cfg(feature = "remote")]
        {
            self.remote.has_daemons()
        }
        #[cfg(not(feature = "remote"))]
        {
            false
        }
    }

    pub fn disc(&self) -> Option<DiscSummary> {
        let info = self.disc.as_ref()?;
        Some(DiscSummary {
            format: info.format.display_name().to_string(),
            filesystem: info.filesystem.display_name().to_string(),
            volume: info.volume_label.clone(),
            size: info.media_size_bytes.map(|bytes| {
                format!(
                    "{} ({} sectors)",
                    crate::model::optical_browse::format_size(bytes),
                    bytes / 2048
                )
            }),
            game: info.game.as_ref().map(crate::optical::format_game_identity),
        })
    }

    pub fn disc_error(&self) -> Option<&str> {
        self.disc_error.as_deref()
    }

    /// The detect runs.
    pub fn detecting(&self) -> Option<Snapshot> {
        self.detecting.as_ref().map(|(j, _)| j.snapshot())
    }

    /// The image is a CHD: CHD Info is offered.
    pub fn can_chd_info(&self) -> bool {
        self.image.as_ref().is_some_and(|p| {
            p.extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| e.eq_ignore_ascii_case("chd"))
        })
    }

    /// The CHD Info window's text, while it is open.
    pub fn chd_info(&self) -> Option<&str> {
        self.chd_info.as_deref()
    }

    pub fn output_format(&self) -> OutputFormat {
        self.format
    }

    /// The output formats and whether each is offered (a conversion never targets its own format).
    pub fn format_choices(&self) -> Vec<(OutputFormat, bool)> {
        let source = self.disc.as_ref().map(|i| i.format);
        OutputFormat::ALL
            .iter()
            .map(|&f| (f, self.task() == Task::Rip || f.converts_from(source)))
            .collect()
    }

    pub fn output(&self) -> Option<&Path> {
        self.output.as_deref()
    }

    pub fn set_output_path(&mut self, path: PathBuf) {
        self.output = Some(path);
    }

    pub fn eject_after(&self) -> bool {
        self.eject_after
    }

    /// A source to rip or convert: the selected drive, or the image.
    pub fn has_valid_source(&self) -> bool {
        match self.mode {
            SourceMode::PhysicalDrive => self.selected_device().is_some(),
            SourceMode::ImageFile => self.image.is_some(),
        }
    }

    pub fn can_start(&self) -> bool {
        !self.is_running() && self.output.is_some() && self.has_valid_source()
    }

    pub fn browse(&self) -> &DiscBrowseController {
        &self.browse
    }

    #[cfg(feature = "audio")]
    pub fn audio(&self) -> &audio::AudioPlayerController {
        &self.audio
    }

    /// The path the detector and the browser open; a remote drive has none (it is rip-only).
    fn browsable(&self) -> Option<PathBuf> {
        match self.mode {
            SourceMode::ImageFile => self.image.clone(),
            SourceMode::PhysicalDrive => self
                .selected_device()
                .filter(|d| !d.is_remote())
                .map(|d| PathBuf::from(&d.device_path)),
        }
    }

    fn scan_drives(&mut self, why: DriveScan) {
        if self.drives_job.is_some() {
            return;
        }
        #[cfg(feature = "remote")]
        let remotes = self.remote.daemons();
        let job = Job::spawn(
            "Looking for optical drives...",
            Arc::clone(&self.notify),
            move |ctx| {
                ctx.set_can_cancel(false);
                #[cfg(feature = "remote")]
                let found = crate::model::optical_devices::list_rip_devices(remotes.as_slice());
                #[cfg(not(feature = "remote"))]
                let found = crate::model::optical_devices::list_local_rip_devices();
                Ok(found)
            },
        );
        self.drives_job = Some((job, why));
    }

    /// The source changed: close the browser, detect the new disc, reload the player.
    fn source_changed(&mut self) {
        let was_browsing = self.browse.is_active();
        self.browse.close();
        self.detect(was_browsing);
        #[cfg(feature = "audio")]
        match self.browsable() {
            Some(path) => self.audio.set_disc(path.as_path()),
            None => self.audio.clear(),
        }
    }

    /// Detect the disc in a job (a drive can take seconds); `reopen` reopens the browser after.
    fn detect(&mut self, reopen: bool) {
        self.disc = None;
        self.disc_error = None;
        self.detecting = None;
        let Some(path) = self.browsable() else {
            return;
        };
        let physical = self.mode == SourceMode::PhysicalDrive;
        let job = Job::spawn(
            "Detecting the disc...",
            Arc::clone(&self.notify),
            move |ctx| {
                ctx.set_can_cancel(false);
                Ok(optical_ops::detect(path.as_path(), physical))
            },
        );
        self.detecting = Some((job, reopen));
    }

    fn open_browse(&mut self) {
        if let Some(path) = self.browsable() {
            let physical = self.mode == SourceMode::PhysicalDrive;
            self.browse
                .open(crate::model::optical_browse::DiscSource { path, physical });
        }
    }

    fn close_disc(&mut self) {
        self.browse.close();
        self.disc = None;
        self.disc_error = None;
        self.detecting = None;
        self.image = None;
        self.drive = None;
        #[cfg(feature = "audio")]
        self.audio.clear();
    }

    fn open_image(&mut self, path: PathBuf) {
        self.mode = SourceMode::ImageFile;
        self.requests
            .push(HostRequest::RememberRecent(path.clone()));
        let changed = self.image.as_ref() != Some(&path);
        self.image = Some(path);
        if changed {
            self.source_changed();
        }
    }

    fn set_mode(&mut self, mode: SourceMode) {
        if mode == self.mode {
            return;
        }
        self.mode = mode;
        self.source_changed();
    }

    fn format_ok(&self, f: OutputFormat) -> bool {
        self.format_choices()
            .iter()
            .any(|&(g, enabled)| g == f && enabled)
    }

    fn eject(&mut self) {
        let Some(path) = self.ejectable() else {
            return;
        };
        if self.ejecting.is_some() {
            return;
        }
        self.ejecting = Some(Job::spawn(
            "Ejecting the disc...",
            Arc::clone(&self.notify),
            move |ctx| {
                ctx.set_can_cancel(false);
                let done = optical_ops::eject(Path::new(path.as_str()));
                Ok((path, done))
            },
        ));
    }

    fn start_chd_info(&mut self) {
        let Some(path) = self.image.clone().filter(|_| self.can_chd_info()) else {
            return;
        };
        if self.chd_info_job.is_some() {
            return;
        }
        self.chd_info_job = Some(Job::spawn(
            "Reading the CHD header...",
            Arc::clone(&self.notify),
            move |ctx| {
                ctx.set_can_cancel(false);
                Ok(optical_ops::chd_info(path.as_path()))
            },
        ));
    }

    fn pick_image(&self) -> Vec<Effect> {
        vec![Effect::PickFile(PickSpec {
            purpose: purpose::IMAGE,
            title: "Open Disc Image".to_string(),
            filters: vec![
                (
                    "Disc Images".to_string(),
                    crate::model::file_types::OPTICAL_EXTS
                        .iter()
                        .map(|e| e.to_string())
                        .collect(),
                ),
                ("All Files".to_string(), vec!["*".to_string()]),
            ],
            save: false,
            file_name: String::new(),
        })]
    }

    /// The name the output picker suggests.
    pub fn default_output_name(&self) -> String {
        optical_ops::default_output_name(
            self.image.as_deref(),
            self.disc.as_ref().and_then(|i| i.volume_label.as_deref()),
            self.format,
        )
    }

    fn pick_output(&self) -> Vec<Effect> {
        vec![Effect::PickFile(PickSpec {
            purpose: purpose::OUTPUT,
            title: "Save Output As".to_string(),
            filters: vec![(
                "Output".to_string(),
                vec![self.format.extension().to_string()],
            )],
            save: true,
            file_name: self.default_output_name(),
        })]
    }

    fn poll(&mut self) {
        if let Some(found) = self.drives_job.as_ref().and_then(|(j, _)| job_outcome(j)) {
            let why = self.drives_job.take().map_or(DriveScan::Quiet, |(_, w)| w);
            if let Ok(devices) = found {
                self.drives = devices;
                let n = self.drives.len();
                match why {
                    DriveScan::Startup if n > 0 => self.info(format!("Found {n} optical drive(s)")),
                    DriveScan::Refresh => {
                        self.info(format!("Refreshed: {n} optical drive(s) found"))
                    }
                    _ => {}
                }
            }
        }
        if let Some(done) = self.detecting.as_ref().and_then(|(j, _)| job_outcome(j)) {
            let reopen = self.detecting.take().is_some_and(|(_, r)| r);
            match done.and_then(|r| r) {
                Ok(info) => {
                    self.info(format!(
                        "Detected disc: {} / {} {}",
                        info.format.display_name(),
                        info.filesystem.display_name(),
                        info.volume_label.as_deref().unwrap_or(""),
                    ));
                    if let Some(g) = &info.game {
                        let line =
                            format!("Game disc: {}", crate::optical::format_game_identity(g));
                        self.info(line);
                    }
                    self.disc = Some(info);
                    if reopen {
                        self.open_browse();
                    }
                }
                Err(e) => {
                    let msg = format!("Failed to detect disc info: {e}");
                    self.warn(msg.as_str());
                    self.disc_error = Some(msg);
                }
            }
        }
        if let Some(done) = self.ejecting.as_ref().and_then(job_outcome) {
            self.ejecting = None;
            match done {
                Ok((path, Ok(()))) => {
                    self.info(format!("Ejected {path}"));
                    // The disc is gone: drop what points at it, then re-scan so the drive shows empty.
                    self.close_disc();
                    self.scan_drives(DriveScan::Quiet);
                }
                Ok((_, Err(e))) | Err(e) => self.warn(format!("Eject failed: {e}")),
            }
        }
        if let Some(done) = self.chd_info_job.as_ref().and_then(job_outcome) {
            self.chd_info_job = None;
            match done.and_then(|r| r) {
                Ok(text) => self.chd_info = Some(text),
                Err(e) => self.error(format!("CHD Info failed: {e}")),
            }
        }
        #[cfg(feature = "remote")]
        self.poll_remote();
        self.poll_run();
        self.browse.tick();
        #[cfg(feature = "audio")]
        self.audio.tick();
    }

    /// The last Custom CD codec / hunk choice from config.json, laid over `base`.
    pub fn saved_chd_custom(base: ChdOptions) -> ChdOptions {
        run::saved_chd_custom(base)
    }

    /// Block until every job ends (a playing track excepted); for a headless shell or a test.
    pub fn wait_idle(&mut self) {
        loop {
            self.poll();
            #[cfg(feature = "remote")]
            let remote_busy = self.remote.connecting();
            #[cfg(not(feature = "remote"))]
            let remote_busy = false;
            if self.drives_job.is_none()
                && self.detecting.is_none()
                && self.ejecting.is_none()
                && self.chd_info_job.is_none()
                && !self.run.running()
                && !remote_busy
            {
                self.browse.wait_idle();
                #[cfg(feature = "audio")]
                self.audio.wait_loaded();
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
}

impl Controller for OpticalController {
    type Action = OpticalAction;

    fn apply(&mut self, action: OpticalAction) -> Vec<Effect> {
        let running = self.is_running();
        match action {
            OpticalAction::FindDrives => self.scan_drives(DriveScan::Startup),
            OpticalAction::RefreshDrives => self.scan_drives(DriveScan::Refresh),
            OpticalAction::SetSourceMode(mode) if !running => self.set_mode(mode),
            OpticalAction::SelectDrive(idx) if !running => {
                let idx = idx.filter(|&i| i < self.drives.len());
                if idx != self.drive {
                    self.drive = idx;
                    if self.mode == SourceMode::PhysicalDrive {
                        self.source_changed();
                    }
                }
            }
            OpticalAction::Eject if !running => self.eject(),
            OpticalAction::PickImage if !running => return self.pick_image(),
            OpticalAction::OpenImage(path) => self.open_image(path),
            OpticalAction::OpenAddRemote => {
                #[cfg(feature = "remote")]
                self.remote.open_dialog();
            }
            OpticalAction::CloseAddRemote => {
                #[cfg(feature = "remote")]
                self.remote.close_dialog();
            }
            OpticalAction::ConnectRemote(addr) => {
                #[cfg(feature = "remote")]
                self.remote.connect(addr.as_str(), &self.notify);
                #[cfg(not(feature = "remote"))]
                {
                    let _ = addr;
                    self.error("Remote daemons need the `remote` feature");
                }
            }
            OpticalAction::BrowseContents => {
                if self.disc.is_some() && !self.browse.is_active() {
                    self.open_browse();
                }
            }
            OpticalAction::CloseDisc => self.close_disc(),
            OpticalAction::ChdInfo => self.start_chd_info(),
            OpticalAction::CloseChdInfo => self.chd_info = None,
            OpticalAction::SetOutputFormat(f) if !running => {
                if self.format_ok(f) {
                    self.format = f;
                }
            }
            OpticalAction::PickOutput if !running => return self.pick_output(),
            OpticalAction::SetEjectAfter(on) if !running => self.eject_after = on,
            OpticalAction::Start(chd) => self.start(chd),
            OpticalAction::Cancel => self.cancel(),
            OpticalAction::Picked { purpose, path } => match (purpose, path) {
                (purpose::IMAGE, Some(path)) if !running => self.open_image(path),
                (purpose::OUTPUT, Some(path)) if !running => self.output = Some(path),
                (p, path) if p.starts_with("optical_browse.") => {
                    return self
                        .browse
                        .apply(DiscBrowseAction::Picked { purpose: p, path })
                }
                _ => {}
            },
            OpticalAction::Browse(a) => return self.browse.apply(a),
            OpticalAction::Audio(a) => {
                #[cfg(feature = "audio")]
                self.audio.apply(a);
                #[cfg(not(feature = "audio"))]
                let _ = a;
            }
            // Refused while a rip or conversion runs.
            OpticalAction::SetSourceMode(_)
            | OpticalAction::SelectDrive(_)
            | OpticalAction::Eject
            | OpticalAction::PickImage
            | OpticalAction::SetOutputFormat(_)
            | OpticalAction::PickOutput
            | OpticalAction::SetEjectAfter(_) => {}
        }
        Vec::new()
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.poll();
        Vec::new()
    }
}
