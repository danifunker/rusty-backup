//! The Optical tab: rip a physical drive (local or on a remote daemon) or convert a disc image,
//! with the disc browser and the CD-DA player beside it.
//!
//! [`OpticalController`] owns the source (drive list, remote daemons, the image file), the
//! detected disc, the output form, and every job: the drive scan, the detect, Eject, CHD Info, the
//! remote connect and the rip / conversion. It hosts [`audio::AudioPlayerController`]; the disc
//! browser is the shared browser pane ([`disc_browser`], `PaneProfile::optical`), which the shell
//! opens and closes on [`HostRequest`]s. The operation bodies are `model::optical_ops`. Shells:
//! the egui tab (`src/gui/optical_tab.rs`) and the TUI's rip screen. See
//! docs/phase2_commander_plan.md § "Slice 16" and § "Slice 18".

pub mod audio;
#[cfg(feature = "remote")]
mod remote;
mod run;

#[cfg(all(test, feature = "native-zstd"))]
mod identity_tests;
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
pub use crate::controller::browser::DiscSource;
pub use crate::model::job::NoNotify;
pub use crate::model::optical_ops::OutputFormat;
pub use crate::rbformats::chd_options::{ChdOptions, ChdProfile};
pub use audio::{AudioAction, PlayState, Readout};

/// The Optical tab's disc browser: the browser pane, one pane, under `PaneProfile::optical`.
pub fn disc_browser(
    services: crate::controller::browser::PaneServices,
    keymap: crate::controller::keys::KeyMap,
) -> crate::controller::commander::CommanderController {
    use crate::controller::commander::{CommanderAction, CommanderController, ResourceForkMode};
    let profile = crate::controller::browser::PaneProfile::optical();
    let mut ctl = CommanderController::single(services, keymap, profile);
    // Exports keep resource forks as AppleDouble, as the disc browser always did.
    ctl.apply(CommanderAction::SetForkMode(ResourceForkMode::AppleDouble));
    ctl
}

/// The `purpose` of each picker this controller asks for.
pub mod purpose {
    pub const IMAGE: &str = "optical.image";
    pub const OUTPUT: &str = "optical.output";
    /// The question Start asks before it replaces files that exist.
    pub const OVERWRITE: &str = "optical.overwrite";
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
    pub const IMAGE: &str = "optical.image";
    pub const IMAGE_RECENT: &str = "optical.image_recent";
    pub const WRITE_TARGET: &str = "optical.write_target";
    pub const PICK_OUTPUT: &str = "optical.pick_output";
    pub const CHD_MODE: &str = "optical.chd_mode";
    pub const CHD_HUNK: &str = "optical.chd_hunk";
    pub const CHD_CODEC: &str = "optical.chd_codec";
    pub const CONVERT: &str = "optical.convert";
}

/// What every shell calls each control and outcome (D8); a shell adds only layout (a `:`, brackets).
pub mod labels {
    use super::ids;
    pub const CONVERT: &str = "Convert...";
    /// Id to label, for drivers that find by label.
    pub const TABLE: &[(&str, &str)] = &[(ids::CONVERT, CONVERT)];
}

/// Tooltips every shell shows on the same controls.
pub mod hints {
    pub const CONVERT: &str = "Change this image's format (CHD, BIN/CUE, ISO) in the Convert tab.";
    pub const CONVERT_MOVED: &str = "Disc images are converted in the Convert tab.";
    pub const PHYSICAL_DRIVE: &str =
        "Click \"Show Physical Devices\" (top bar) for a local drive, \
         or \"Add remote daemon...\" for a networked one.";
    pub const EJECT: &str = "Select a local drive with a disc in it to eject.";
    pub const EJECT_AFTER: &str = "Ejects the source drive on its own machine - the \
         remote daemon's drive for a remote source.";
    pub const AUDIO_TRACKS: &str = "Open the CD-DA player";
}

/// What CHD output and CHD Info say in a build without the `chd` feature.
pub const CHD_NOT_BUILT: &str = "CHD needs the `chd` feature; this binary was built without it.";

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
    /// Show the disc browser (`disc_browser`) on this disc.
    OpenBrowser(DiscSource),
    /// The disc changed or closed: close the disc browser.
    CloseBrowser,
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
    /// The user closed the disc browser.
    BrowserClosed,
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
    /// The answer to Start's question about replacing files.
    Confirmed {
        purpose: &'static str,
        yes: bool,
    },
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
    /// The shell shows the disc browser.
    browsing: bool,
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
            browsing: false,
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

    /// The disc image the Convert tab takes over; a drive is ripped here instead.
    pub fn convert_source(&self) -> Option<PathBuf> {
        match self.mode {
            SourceMode::ImageFile => self.image.clone(),
            SourceMode::PhysicalDrive => None,
        }
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

    /// The connect dialog is open; never without `remote`.
    #[cfg(not(feature = "remote"))]
    pub fn add_remote_open(&self) -> bool {
        false
    }

    #[cfg(not(feature = "remote"))]
    pub fn add_remote_error(&self) -> Option<&str> {
        None
    }

    #[cfg(not(feature = "remote"))]
    pub fn remote_connecting(&self) -> Option<Snapshot> {
        None
    }

    #[cfg(not(feature = "remote"))]
    pub fn recent_daemons(&self) -> &[String] {
        &[]
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

    /// The image is a CHD: CHD Info is offered (a build without `chd` cannot read one).
    pub fn can_chd_info(&self) -> bool {
        cfg!(feature = "chd") && self.chd_info_job.is_none() && self.is_chd_image()
    }

    /// The image file is a `.chd`.
    pub fn is_chd_image(&self) -> bool {
        self.image.as_ref().is_some_and(|p| {
            p.extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| e.eq_ignore_ascii_case("chd"))
        })
    }

    /// CHD Info is reading the header.
    pub fn chd_info_running(&self) -> bool {
        self.chd_info_job.is_some()
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
            .map(|&f| {
                let built = f != OutputFormat::Chd || cfg!(feature = "chd");
                (
                    f,
                    built && (self.task() == Task::Rip || f.converts_from(source)),
                )
            })
            .collect()
    }

    /// Why `format` is not offered, for a shell that names it beside the choice.
    pub fn format_refusal(&self, format: OutputFormat) -> Option<String> {
        if self.format_ok(format) {
            return None;
        }
        Some(if format == OutputFormat::Chd && !cfg!(feature = "chd") {
            CHD_NOT_BUILT.to_string()
        } else {
            format!(
                "The image is already {}; choose another output format.",
                format.label()
            )
        })
    }

    pub fn output(&self) -> Option<&Path> {
        self.output.as_deref()
    }

    pub fn set_output_path(&mut self, path: PathBuf) {
        self.output = Some(path);
    }

    /// A typed output field was emptied.
    pub fn clear_output(&mut self) {
        self.output = None;
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
        self.start_refusal().is_none()
    }

    /// What the output form says the task is.
    pub fn task_label(&self) -> &'static str {
        match self.task() {
            Task::Rip => "Rip to disk",
            Task::Convert => "Convert format",
        }
    }

    /// "Start Rip" or "Start Convert".
    pub fn start_label(&self) -> &'static str {
        match self.task() {
            Task::Rip => "Start Rip",
            Task::Convert => "Start Convert",
        }
    }

    /// Why Start is refused, or `None` when it can run.
    pub fn start_refusal(&self) -> Option<String> {
        if self.is_running() {
            return Some("A rip or conversion is running.".to_string());
        }
        if !self.has_valid_source() {
            return Some(match self.mode {
                SourceMode::PhysicalDrive => "Select a drive.".to_string(),
                SourceMode::ImageFile => "Choose a disc image.".to_string(),
            });
        }
        // A conversion picks its route by the detected format.
        if self.task() == Task::Convert && self.disc.is_none() {
            return Some(match self.detecting {
                Some(_) => "Detecting the disc...".to_string(),
                None => "The image was not recognised as a disc.".to_string(),
            });
        }
        if let Some(why) = self.format_refusal(self.format) {
            return Some(why);
        }
        let Some(output) = self.output.as_deref() else {
            return Some("Choose an output file.".to_string());
        };
        optical_ops::output_refusal(output, self.format, self.source_files().as_slice())
    }

    /// The image files a conversion reads; a rip reads a drive and has none.
    fn source_files(&self) -> Vec<PathBuf> {
        match (self.task(), self.image.as_deref()) {
            (Task::Convert, Some(image)) => {
                optical_ops::source_files(image, self.disc.as_ref().map(|i| i.format))
            }
            _ => Vec::new(),
        }
    }

    /// Whether this build has remote daemons (`Add remote daemon`).
    pub const fn remote_available() -> bool {
        cfg!(feature = "remote")
    }

    /// The disc browser is open (`HostRequest::OpenBrowser` was the last word on it).
    pub fn is_browsing(&self) -> bool {
        self.browsing
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

    /// A scan's drives: the selection stays on its own drive, and never takes the one now in its row.
    fn land_drives(&mut self, devices: Vec<RipDevice>) {
        let picked = self.selected_device().map(RipDevice::picker_label);
        self.drives = devices;
        let Some(label) = picked else {
            return;
        };
        let at = |i: usize, d: &[RipDevice]| d.get(i).map(RipDevice::picker_label);
        if self
            .drive
            .and_then(|i| at(i, self.drives.as_slice()))
            .as_ref()
            == Some(&label)
        {
            return;
        }
        self.drive = self.drives.iter().position(|d| d.picker_label() == label);
        if self.drive.is_none() {
            self.warn(format!(
                "{label} is no longer listed, so it is no longer the source."
            ));
            if self.mode == SourceMode::PhysicalDrive {
                self.source_changed();
            }
        }
    }

    /// The source changed: close the browser, detect the new disc, reload the player.
    fn source_changed(&mut self) {
        let was_browsing = self.browsing;
        self.close_browser();
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
            self.browsing = true;
            let src = DiscSource { path, physical };
            self.requests.push(HostRequest::OpenBrowser(src));
        }
    }

    fn close_browser(&mut self) {
        if self.browsing {
            self.browsing = false;
            self.requests.push(HostRequest::CloseBrowser);
        }
    }

    fn close_disc(&mut self) {
        self.close_browser();
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

    /// The output keeps following the format: a CHD must not land in the `.cue` chosen for BIN/CUE.
    fn set_format(&mut self, f: OutputFormat) {
        let old = self.format.extension();
        self.format = f;
        let named_for_old = |p: &PathBuf| {
            let ext = p.extension().and_then(|e| e.to_str());
            ext.is_some_and(|e| e.eq_ignore_ascii_case(old))
        };
        if let Some(path) = self.output.as_mut().filter(|p| named_for_old(p)) {
            path.set_extension(f.extension());
        }
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
        if !cfg!(feature = "chd") && self.is_chd_image() {
            self.warn(CHD_NOT_BUILT);
            return;
        }
        let Some(path) = self.image.clone().filter(|_| self.can_chd_info()) else {
            return;
        };
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
                self.land_drives(devices);
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
                    // The disc is gone: drop what points at it, unless the source moved on meanwhile.
                    let on_it = self.mode == SourceMode::PhysicalDrive
                        && self.selected_device().map(|d| d.device_path.as_str())
                            == Some(path.as_str());
                    if on_it {
                        self.close_disc();
                    }
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
                #[cfg(not(feature = "remote"))]
                self.warn(crate::controller::backup::REMOTE_NOT_BUILT);
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
                if self.disc.is_some() && !self.browsing {
                    self.open_browse();
                }
            }
            OpticalAction::BrowserClosed => self.browsing = false,
            OpticalAction::CloseDisc => self.close_disc(),
            OpticalAction::ChdInfo => self.start_chd_info(),
            OpticalAction::CloseChdInfo => self.chd_info = None,
            OpticalAction::SetOutputFormat(f) if !running => {
                if self.format_ok(f) {
                    self.set_format(f);
                }
            }
            OpticalAction::PickOutput if !running => return self.pick_output(),
            OpticalAction::SetEjectAfter(on) if !running => self.eject_after = on,
            OpticalAction::Start(chd) => return self.start(chd),
            OpticalAction::Cancel => self.cancel(),
            OpticalAction::Picked { purpose, path } => match (purpose, path) {
                (purpose::IMAGE, Some(path)) if !running => self.open_image(path),
                (purpose::OUTPUT, Some(path)) if !running => self.output = Some(path),
                _ => {}
            },
            OpticalAction::Confirmed { purpose, yes } => {
                if purpose == purpose::OVERWRITE {
                    self.answered(yes);
                }
            }
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
