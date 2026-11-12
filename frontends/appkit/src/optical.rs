//! The Optical screen over `controller::optical`; the Win32 shell's `optical.rs` is the reference.
//!
//! Slice 6: the source (a drive, or an image with its recent list), the detected disc and its
//! tools (Browse Contents, Close Disc, CHD Info, Audio Tracks), the rip's output form, and Start /
//! Cancel. An image converts in the Convert screen, so image mode offers Convert... instead. The
//! disc browser is a `BrowseView` in disc mode, shown in the screen's place while it is open, and
//! the CD-DA player is its own window. Add remote daemon comes with slice 7b, Convert with 7c.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use objc2::rc::Retained;
use objc2::{sel, MainThreadMarker};
use objc2_app_kit::{NSButton, NSProgressIndicator, NSView, NSWindow};
use rusty_backup::controller::app::chd::ChdOptionsControl;
use rusty_backup::controller::app::{
    busy_elsewhere_notice, physical_devices_available, Notify, RecentHandle, RecentMode,
};
use rusty_backup::controller::backup::format_size;
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::optical::audio::ids as audio_ids;
use rusty_backup::controller::optical::{
    hints, ids, labels, AudioAction, ChdChoice, ChdProfile, HostRequest, OpticalAction,
    OpticalController, OutputFormat, SourceMode, Task,
};
use rusty_backup::controller::{Controller, Effect};

use crate::browse::BrowseView;
use crate::chd::{ChdControl, ChdIds, ChdKey};
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::report::{self, ReportWindow};
use crate::sync::SyncGuard;
use crate::ui::{self, ns, CloseDelegate};

/// CHD Info's Close, which the app routes back here by its id.
pub const CHD_CLOSE: &str = "optical.chd_info.close";

fn radio(mtm: MainThreadMarker, title: &str, id: &str) -> Retained<NSButton> {
    // SAFETY: the form points the button's action at its own target when it is added.
    let b = unsafe {
        NSButton::radioButtonWithTitle_target_action(&ns(title), None, Some(sel!(fire:)), mtm)
    };
    ui::set_id(&b, id);
    b
}

fn progress_of<K: Copy + PartialEq + 'static>(
    form: &Form<K>,
    k: K,
) -> Retained<NSProgressIndicator> {
    form.v(k)
        .downcast::<NSProgressIndicator>()
        .expect("a progress bar")
}

/// One audio track as the player shows it.
struct TrackShown {
    number: u32,
    idle: String,
    /// The playing track's row; `Some(None)` while it prepares.
    now: Option<Option<String>>,
}

struct AudioShown {
    tracks: Vec<TrackShown>,
    heading: Option<(String, Option<f64>)>,
    error: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum AK {
    Heading,
    Bar,
    Error,
    Play(u32),
    Track(u32),
    Close,
}

/// The CD-DA player window: a Play / Stop button per audio track and the playing one's line.
struct AudioWindow {
    window: Retained<NSWindow>,
    _delegate: Retained<CloseDelegate>,
    form: Form<AK>,
    tracks: Vec<u32>,
    playing: RefCell<Vec<u32>>,
    send: Rc<dyn Fn(OpticalAction)>,
}

impl AudioWindow {
    fn open(
        mtm: MainThreadMarker,
        tracks: &[(u32, String)],
        send: Rc<dyn Fn(OpticalAction)>,
    ) -> Rc<AudioWindow> {
        Rc::new_cyclic(|me: &Weak<AudioWindow>| {
            let close = send.clone();
            let (window, delegate) = ui::window(
                mtm,
                "Audio Tracks",
                560.0,
                420.0,
                Box::new(move || close(OpticalAction::Audio(AudioAction::Close))),
            );
            let resize = me.clone();
            delegate.on_resize(Box::new(move || {
                if let Some(w) = resize.upgrade() {
                    w.layout();
                }
            }));
            let body = window.contentView().expect("the window's content view");
            let fire = me.clone();
            let form = Form::new(
                mtm,
                body,
                Rc::new(move |k| {
                    if let Some(w) = fire.upgrade() {
                        w.fired(k);
                    }
                }),
            );
            form.label(AK::Heading, "");
            form.progress(AK::Bar);
            form.label(AK::Error, "");
            for (n, idle) in tracks {
                form.button(AK::Play(*n), "Play", &format!("{}.{n}", audio_ids::PLAY));
                form.label(AK::Track(*n), idle);
            }
            form.button(AK::Close, "Close", audio_ids::CLOSE);
            ui::present(&window);
            AudioWindow {
                window,
                _delegate: delegate,
                form,
                tracks: tracks.iter().map(|t| t.0).collect(),
                playing: RefCell::default(),
                send,
            }
        })
    }

    fn show(&self, s: &AudioShown) {
        let f = &self.form;
        let (heading, fraction) = s.heading.clone().unzip();
        f.line(AK::Heading, heading.as_deref(), None);
        let pct = fraction
            .flatten()
            .map(|x| (x.clamp(0.0, 1.0) * 100.0) as u32);
        ui::set_progress(&progress_of(f, AK::Bar), heading.is_some(), pct);
        f.line(AK::Error, s.error.as_deref(), Some(Tone::Error));
        let mut playing = Vec::new();
        for t in &s.tracks {
            let on = t.now.is_some();
            if on {
                playing.push(t.number);
            }
            f.set_title(AK::Play(t.number), if on { "Stop" } else { "Play" });
            let text = t.now.clone().flatten().unwrap_or_else(|| t.idle.clone());
            f.set_text(AK::Track(t.number), &text);
        }
        *self.playing.borrow_mut() = playing;
        self.layout();
    }

    fn layout(&self) {
        let Some(body) = self.window.contentView() else {
            return;
        };
        let line = |k: AK| Node::leaf(k, Size::Stretch(1), Size::Fixed(layout::ROW - 4));
        let mut col = vec![
            line(AK::Heading),
            line(AK::Error),
            Node::leaf(AK::Bar, Size::Stretch(1), Size::Fixed(layout::ROW - 8)),
        ];
        for pair in self.tracks.chunks(2) {
            let mut row = Vec::new();
            for n in pair {
                row.push(Node::button(AK::Play(*n), 70));
                row.push(Node::leaf(
                    AK::Track(*n),
                    Size::Stretch(1),
                    Size::Fixed(layout::ROW),
                ));
            }
            col.push(Node::row(row));
        }
        col.push(Node::Space(Size::Stretch(1)));
        col.push(Node::row(vec![
            Node::Space(Size::Stretch(1)),
            Node::button(AK::Close, 90),
        ]));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        self.form.place_shown(tree, ui::area(&body));
    }

    fn fired(&self, k: AK) {
        let audio = match k {
            AK::Close => AudioAction::Close,
            AK::Play(n) if self.playing.borrow().contains(&n) => AudioAction::Stop,
            AK::Play(n) => AudioAction::Play(n),
            _ => return,
        };
        let send = self.send.clone();
        // The window may close with what this sends: act once the handler returns.
        crate::app::later(Box::new(move || send(OpticalAction::Audio(audio))));
    }
}

impl Drop for AudioWindow {
    fn drop(&mut self) {
        self.window.setDelegate(None);
        self.window.orderOut(None);
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    BusyNote,
    DriveMode,
    ImageMode,
    DriveCaption,
    Drive,
    Refresh,
    Eject,
    AddRemote,
    DriveNote,
    ImageCaption,
    Image,
    RecentItem(usize),
    OpenImage,
    Job,
    JobProgress,
    Disc,
    BrowseContents,
    CloseDisc,
    ChdInfo,
    Audio,
    NowPlaying,
    Task,
    FormatCaption,
    Format(usize),
    Chd(ChdKey),
    OutputCaption,
    Output,
    PickOutput,
    EjectAfter,
    Convert,
    ConvertNote,
    Target,
    Outcome,
    Run,
    Progress,
    Start,
    Cancel,
    Why,
}

pub struct OpticalScreen {
    mtm: MainThreadMarker,
    ctrl: RefCell<OpticalController>,
    host: EffectHost,
    recent: RecentHandle,
    recent_files: RefCell<Vec<String>>,
    busy: Cell<Option<&'static str>>,
    sync: SyncGuard,
    me: Weak<OpticalScreen>,
    body: Retained<NSView>,
    form: Form<K>,
    chd: ChdControl<K>,
    /// The disc browser, which stands in the screen's place while it is open.
    pub browse: Rc<BrowseView>,
    chd_window: RefCell<Option<ReportWindow>>,
    audio_window: RefCell<Option<Rc<AudioWindow>>>,
}

impl OpticalScreen {
    pub fn build(
        mtm: MainThreadMarker,
        body: Retained<NSView>,
        notify: Arc<dyn Notify>,
        recent: RecentHandle,
        host: EffectHost,
    ) -> Rc<OpticalScreen> {
        let browse = BrowseView::build(mtm, notify.clone(), host.clone(), true);
        let screen = Rc::new_cyclic(|me: &Weak<OpticalScreen>| {
            let fire = me.clone();
            let form = Form::new(
                mtm,
                body.clone(),
                Rc::new(move |k| {
                    if let Some(s) = fire.upgrade() {
                        s.fired(k);
                    }
                }),
            );
            let f = &form;
            f.wrap(K::BusyNote, "");
            f.add(
                K::DriveMode,
                &radio(mtm, "Physical drive", ids::SOURCE_DRIVE),
            );
            f.add(K::ImageMode, &radio(mtm, "Image file", ids::SOURCE_IMAGE));
            f.label(K::DriveCaption, "Drive:");
            f.popup(K::Drive, &[], ids::DRIVE);
            f.button(K::Refresh, "Refresh", ids::REFRESH);
            f.button(K::Eject, "Eject", ids::EJECT);
            let remote = ui::later(
                mtm,
                "Add remote daemon...",
                ids::ADD_REMOTE,
                "Arrives in slice 7b (remote windows).",
            );
            f.add(K::AddRemote, &remote);
            remote.setEnabled(false);
            f.wrap(K::DriveNote, "");
            f.label(K::ImageCaption, "Image:");
            f.pulldown(K::Image, ids::IMAGE);
            f.button(K::OpenImage, "Open File...", ids::BROWSE_IMAGE);
            f.label(K::Job, "");
            f.progress(K::JobProgress);
            f.wrap(K::Disc, "");
            f.button(K::BrowseContents, "Browse Contents", ids::BROWSE_CONTENTS);
            f.button(K::CloseDisc, "Close Disc", ids::CLOSE_DISC);
            f.button(K::ChdInfo, "CHD Info", ids::CHD_INFO);
            f.button(K::Audio, "Audio Tracks", audio_ids::OPEN);
            f.label(K::NowPlaying, "");
            f.label(K::Task, "");
            f.label(K::FormatCaption, "Output format:");
            for (i, x) in OutputFormat::ALL.iter().enumerate() {
                let id = format!("{}.{}", ids::OUTPUT_FORMAT, x.extension());
                f.add(K::Format(i), &radio(mtm, x.label(), &id));
            }
            let chd = ChdControl::build(
                f,
                K::Chd,
                ChdIds {
                    mode: ids::CHD_MODE,
                    hunk: ids::CHD_HUNK,
                    codec: ids::CHD_CODEC,
                },
                ChdOptionsControl::new(ChdProfile::Cd),
                ChdProfile::Cd,
            );
            f.label(K::OutputCaption, "Output:");
            f.label(K::Output, "");
            f.button(K::PickOutput, "Browse...", ids::PICK_OUTPUT);
            f.check(K::EjectAfter, "Eject after ripping", ids::EJECT_AFTER);
            let convert = ui::later(
                mtm,
                labels::CONVERT,
                ids::CONVERT,
                "Arrives in slice 7c (the Convert screen).",
            );
            f.add(K::Convert, &convert);
            convert.setEnabled(false);
            f.wrap(K::ConvertNote, hints::CONVERT_MOVED);
            f.label(K::Target, "");
            f.wrap(K::Outcome, "");
            f.label(K::Run, "");
            f.progress(K::Progress);
            f.button(K::Start, "Start", ids::START);
            f.button(K::Cancel, "Cancel", ids::CANCEL);
            f.label(K::Why, "");
            OpticalScreen {
                mtm,
                ctrl: RefCell::new(OpticalController::new(notify)),
                host,
                recent_files: RefCell::new(recent.list(RecentMode::Optical)),
                recent,
                busy: Cell::new(None),
                sync: SyncGuard::default(),
                me: me.clone(),
                body,
                form,
                chd,
                browse,
                chd_window: RefCell::default(),
                audio_window: RefCell::default(),
            }
        });
        screen.apply(OpticalAction::FindDrives);
        screen
    }

    /// Settings cleared the recent files: the picker forgets its copy too.
    pub fn forget_recents(&self) {
        self.recent_files.borrow_mut().clear();
    }

    pub fn set_busy(&self, busy: Option<&'static str>) {
        self.busy.set(busy);
    }

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    /// Open `path` as the image source (Inspect's hand-off, or the File menu).
    pub fn open_image(&self, path: PathBuf) {
        self.apply(OpticalAction::OpenImage(path));
    }

    pub fn apply(&self, action: OpticalAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        self.browse.tick();
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// The disc browser opened or closed; a close the user made tells the controller.
    pub fn browser_changed(&self) {
        if !self.browse.is_active() && self.ctrl.borrow().is_browsing() {
            self.apply(OpticalAction::BrowserClosed);
        }
    }

    /// A track's position moves with no job to wake the shell, so a playing track keeps ticking.
    pub fn playing(&self) -> bool {
        self.ctrl.borrow().audio().now_playing().is_some()
    }

    /// A control's action by its controller id: the menu bar's, and CHD Info's Close.
    pub fn command(&self, id: &str) {
        let action = match id {
            ids::BROWSE_IMAGE => OpticalAction::PickImage,
            CHD_CLOSE => OpticalAction::CloseChdInfo,
            _ => return,
        };
        self.apply(action);
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(s) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => OpticalAction::Picked { purpose, path },
                Reply::Confirmed { purpose, yes } => OpticalAction::Confirmed { purpose, yes },
                // Optical asks for neither text nor a choice and has no screen to leave.
                Reply::Answered { .. } | Reply::Chose { .. } | Reply::CloseScreen => return,
            };
            s.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let (requests, log) = {
            let mut ctrl = self.ctrl.borrow_mut();
            (ctrl.take_requests(), ctrl.take_log())
        };
        for request in requests {
            match request {
                HostRequest::RememberRecent(path) => {
                    let list = self
                        .recent
                        .remember(RecentMode::Optical, &path.display().to_string());
                    *self.recent_files.borrow_mut() = list;
                }
                HostRequest::OpenBrowser(src) => self.browse.open_disc(src),
                HostRequest::CloseBrowser => self.browse.close(),
            }
        }
        crate::app::append_log(log);
        self.show_windows();
        // Off screen nothing needs it, and a job wakes this many times a second.
        if !self.body.isHiddenOrHasHiddenAncestor() {
            self.refresh();
        }
    }

    fn send(&self) -> Rc<dyn Fn(OpticalAction)> {
        let me = self.me.clone();
        Rc::new(move |a| {
            if let Some(s) = me.upgrade() {
                s.apply(a);
            }
        })
    }

    /// CHD Info and the player follow the controller: open while it says so.
    fn show_windows(&self) {
        let info = self.ctrl.borrow().chd_info().map(str::to_string);
        match info {
            None => {
                self.chd_window.replace(None);
            }
            Some(text) => {
                if self.chd_window.borrow().is_none() {
                    let send = self.send();
                    let w = ReportWindow::open(
                        self.mtm,
                        "CHD Info",
                        None,
                        None,
                        CHD_CLOSE,
                        Box::new(move || send(OpticalAction::CloseChdInfo)),
                    );
                    *self.chd_window.borrow_mut() = Some(w);
                }
                if let Some(w) = &*self.chd_window.borrow() {
                    w.show(&report::Shown {
                        headline: String::new(),
                        stats: String::new(),
                        lines: text.lines().map(str::to_string).collect(),
                        toggle: None,
                        button: false,
                    });
                }
            }
        }
        let Some(shown) = self.read_audio() else {
            self.audio_window.replace(None);
            return;
        };
        let numbers: Vec<u32> = shown.tracks.iter().map(|t| t.number).collect();
        let held = self
            .audio_window
            .borrow()
            .clone()
            .filter(|w| w.tracks == numbers);
        let w = held.unwrap_or_else(|| {
            self.audio_window.replace(None);
            let list: Vec<(u32, String)> = shown
                .tracks
                .iter()
                .map(|t| (t.number, t.idle.clone()))
                .collect();
            let w = AudioWindow::open(self.mtm, &list, self.send());
            *self.audio_window.borrow_mut() = Some(w.clone());
            w
        });
        w.show(&shown);
    }

    fn read_audio(&self) -> Option<AudioShown> {
        let ctrl = self.ctrl.borrow();
        let player = ctrl.audio();
        if !player.window_open() {
            return None;
        }
        let now = player.readout();
        let tracks = player.audio_tracks().map(|t| TrackShown {
            number: t.number,
            idle: format!("Track {} ({})", t.number, t.duration_mmss()),
            now: now
                .as_ref()
                .filter(|n| n.track == t.number)
                .map(|n| n.row.clone()),
        });
        Some(AudioShown {
            tracks: tracks.collect(),
            heading: now.as_ref().map(|n| (n.heading.clone(), n.fraction)),
            error: player.error().map(|e| format!("Playback failed: {e}")),
        })
    }

    /// Start's whole rule: asked again at the click, not only when the button was last drawn.
    fn may_start(&self) -> bool {
        let ctrl = self.ctrl.borrow();
        let chd_ok = ctrl.output_format() != OutputFormat::Chd || self.chd.is_valid();
        ctrl.can_start() && chd_ok && self.busy.get().is_none()
    }

    fn start(&self) -> Option<OpticalAction> {
        if !self.may_start() {
            return None;
        }
        let chd = (self.ctrl.borrow().output_format() == OutputFormat::Chd).then(|| {
            let (options, custom) = self.chd.choice();
            ChdChoice { options, custom }
        });
        Some(OpticalAction::Start(chd))
    }

    pub fn refresh(&self) {
        let can_start = self.may_start();
        self.sync.run(|| self.show(can_start));
        self.relayout();
    }

    fn show(&self, can_start: bool) {
        let f = &self.form;
        let ctrl = self.ctrl.borrow();
        let busy = self.busy.get();
        let notice = busy.map(busy_elsewhere_notice);
        f.line(K::BusyNote, notice.as_deref(), Some(Tone::Warning));
        let image_mode = ctrl.source_mode() == SourceMode::ImageFile;
        f.set_check(K::DriveMode, !image_mode);
        f.set_check(K::ImageMode, image_mode);
        for k in [
            K::DriveCaption,
            K::Drive,
            K::Refresh,
            K::Eject,
            K::AddRemote,
        ] {
            f.show(k, !image_mode);
        }
        let mut drives = vec![if ctrl.scanning_drives() {
            "Scanning drives...".to_string()
        } else {
            "Select a drive...".to_string()
        }];
        drives.extend(ctrl.drive_labels());
        f.set_items(K::Drive, &drives);
        f.select(K::Drive, ctrl.selected_drive().map_or(0, |i| i + 1));
        f.enable(K::Eject, ctrl.can_eject());
        let no_drive = !physical_devices_available() && ctrl.drive_count() == 0;
        f.line(
            K::DriveNote,
            (!image_mode && no_drive).then_some(hints::PHYSICAL_DRIVE),
            None,
        );
        for k in [K::ImageCaption, K::Image, K::OpenImage] {
            f.show(k, image_mode);
        }
        let image = ctrl.image().map(|p| p.display().to_string());
        let recent = self.recent_files.borrow();
        let mut items: Vec<(String, Option<K>, bool)> = recent
            .iter()
            .enumerate()
            .map(|(i, p)| (p.clone(), Some(K::RecentItem(i)), true))
            .collect();
        if items.is_empty() {
            items.push(("No recent images".to_string(), None, false));
        }
        f.set_pulldown(
            K::Image,
            image.as_deref().unwrap_or("No file selected"),
            &items,
        );

        let job = ctrl.detecting();
        let (text, pct) = job
            .as_ref()
            .map(crate::inspect::job_line)
            .unwrap_or_default();
        f.set_text(K::Job, &text);
        f.show(K::Job, job.is_some());
        ui::set_progress(&progress_of(f, K::JobProgress), job.is_some(), pct);

        let disc = ctrl.disc();
        let text = match (&disc, ctrl.disc_error()) {
            (Some(d), _) => {
                let mut lines = vec![format!(
                    "Format: {}   Filesystem: {}",
                    d.format, d.filesystem
                )];
                lines.extend(d.volume.as_ref().map(|v| format!("Volume: {v}")));
                lines.extend(d.size.as_ref().map(|s| format!("Size: {s}")));
                lines.extend(d.game.as_ref().map(|g| format!("Game: {g}")));
                Some((lines.join("\n"), None))
            }
            (None, Some(e)) => Some((e.to_string(), Some(Tone::Warning))),
            (None, None) => None,
        };
        let (text, tone) = text.unzip();
        f.line(K::Disc, text.as_deref(), tone.flatten());
        let has_disc = disc.is_some();
        f.show(K::BrowseContents, has_disc);
        f.show(K::CloseDisc, has_disc);
        f.enable(K::BrowseContents, !ctrl.is_browsing());
        f.show(K::ChdInfo, has_disc && ctrl.is_chd_image());
        f.enable(K::ChdInfo, ctrl.can_chd_info() && !ctrl.chd_info_running());
        let player = ctrl.audio();
        let tracks = player.audio_tracks().count();
        f.show(K::Audio, has_disc && player.has_audio());
        f.set_title(K::Audio, &format!("Audio Tracks ({tracks})"));
        let now = player.readout().map(|r| r.line);
        f.line(K::NowPlaying, now.as_deref(), None);

        let rip = ctrl.task() == Task::Rip;
        f.set_text(K::Task, ctrl.task_label());
        f.show(K::Task, rip);
        let choices = ctrl.format_choices();
        for (i, format) in OutputFormat::ALL.iter().enumerate() {
            f.show(K::Format(i), rip);
            let ok = choices.iter().any(|(x, on)| x == format && *on);
            f.enable(K::Format(i), ok);
            f.set_check(K::Format(i), *format == ctrl.output_format());
        }
        f.show(K::FormatCaption, rip);
        self.chd
            .set_visible(f, rip && ctrl.output_format() == OutputFormat::Chd);
        for k in [K::OutputCaption, K::Output, K::PickOutput, K::EjectAfter] {
            f.show(k, rip);
        }
        let out = ctrl.output().map(|p| p.display().to_string());
        f.set_text(
            K::Output,
            out.as_deref().unwrap_or("No output path selected"),
        );
        f.set_check(K::EjectAfter, ctrl.eject_after());
        f.show(K::Convert, !rip);
        f.show(K::ConvertNote, !rip);

        let running = ctrl.is_running();
        let locked = running || busy.is_some();
        for k in [
            K::DriveMode,
            K::ImageMode,
            K::Drive,
            K::Refresh,
            K::Image,
            K::OpenImage,
            K::PickOutput,
            K::EjectAfter,
        ] {
            f.enable(k, !locked);
        }
        let writes = ctrl.write_target().map(|t| format!("Writes: {t}"));
        f.line(K::Target, writes.as_deref().filter(|_| rip), None);
        f.show(K::Start, rip && !running);
        f.enable(K::Start, can_start);
        f.set_title(K::Start, ctrl.start_label());
        f.show(K::Cancel, running);
        f.enable(K::Cancel, !ctrl.cancelling());
        let bar = ctrl.progress();
        let run = bar.as_ref().map(|b| {
            format!(
                "{}: {} of {}",
                b.operation,
                format_size(b.current_bytes),
                format_size(b.total_bytes)
            )
        });
        let pct = bar.as_ref().and_then(|b| {
            (b.total_bytes > 0)
                .then(|| (b.current_bytes.saturating_mul(100) / b.total_bytes).min(100) as u32)
        });
        f.set_text(K::Run, run.as_deref().unwrap_or(""));
        f.show(K::Run, run.is_some());
        ui::set_progress(&progress_of(f, K::Progress), run.is_some(), pct);
        let chd_error = (ctrl.output_format() == OutputFormat::Chd && !self.chd.is_valid())
            .then(|| "Check the CHD options.".to_string());
        let why = ctrl.start_refusal().or(chd_error).or(notice);
        let show_why = rip && !running && !can_start;
        f.line(K::Why, why.as_deref().filter(|_| show_why), None);
        let outcome = match ctrl.last_result() {
            Some(Ok(())) => Some(("Finished.".to_string(), None)),
            Some(Err(e)) => Some((e.clone(), Some(Tone::Error))),
            None => None,
        };
        let (text, tone) = outcome.filter(|_| run.is_none() && !show_why).unzip();
        f.line(K::Outcome, text.as_deref(), tone.flatten());
    }

    pub fn relayout(&self) {
        let f = &self.form;
        let row_h = Size::Fixed(layout::ROW);
        let fixed = |k: K, w: i32| Node::leaf(k, Size::Fixed(w), row_h);
        let pop = |k: K, w: Size| Node::leaf(k, w, Size::Fixed(layout::ROW + 2));
        let stretch = |k: K| Node::leaf(k, Size::Stretch(1), row_h);
        let lines = |k: K| {
            let n = f.text(k).lines().count().max(1) as i32;
            Node::leaf(k, Size::Stretch(1), Size::Fixed((layout::ROW - 6) * n + 4))
        };
        let shown = |k: K| f.shown(k);
        let cap = 110;
        let mut col = Vec::new();
        if shown(K::BusyNote) {
            col.push(lines(K::BusyNote));
        }
        col.push(Node::row(vec![
            fixed(K::DriveMode, 150),
            fixed(K::ImageMode, 150),
            Node::Space(Size::Stretch(1)),
        ]));
        if shown(K::Drive) {
            col.push(Node::row(vec![
                fixed(K::DriveCaption, cap),
                pop(K::Drive, Size::Stretch(1)),
                Node::button(K::Refresh, 85),
                Node::button(K::Eject, 75),
                Node::button(K::AddRemote, 170),
            ]));
        }
        if shown(K::DriveNote) {
            col.push(lines(K::DriveNote));
        }
        if shown(K::Image) {
            col.push(Node::row(vec![
                fixed(K::ImageCaption, cap),
                pop(K::Image, Size::Stretch(1)),
                Node::button(K::OpenImage, 115),
            ]));
        }
        if shown(K::Job) {
            col.push(Node::row(vec![
                stretch(K::Job),
                Node::leaf(
                    K::JobProgress,
                    Size::Fixed(180),
                    Size::Fixed(layout::ROW - 4),
                ),
            ]));
        }
        if shown(K::Disc) {
            col.push(lines(K::Disc));
        }
        if shown(K::BrowseContents) {
            let mut row = vec![
                Node::button(K::BrowseContents, 145),
                Node::button(K::CloseDisc, 105),
            ];
            for (k, w) in [(K::ChdInfo, 95), (K::Audio, 150)] {
                if shown(k) {
                    row.push(Node::button(k, w));
                }
            }
            row.push(Node::Space(Size::Stretch(1)));
            col.push(Node::row(row));
        }
        if shown(K::NowPlaying) {
            col.push(stretch(K::NowPlaying));
        }
        if shown(K::Task) {
            col.push(stretch(K::Task));
            let mut row = vec![fixed(K::FormatCaption, cap)];
            row.extend((0..OutputFormat::ALL.len()).map(|i| fixed(K::Format(i), 115)));
            row.push(Node::Space(Size::Stretch(1)));
            col.push(Node::row(row));
            col.extend(self.chd.rows());
            col.push(Node::row(vec![
                fixed(K::OutputCaption, cap),
                stretch(K::Output),
                Node::button(K::PickOutput, 90),
            ]));
            col.push(Node::row(vec![stretch(K::EjectAfter)]));
        }
        if shown(K::Convert) {
            col.push(Node::row(vec![
                Node::button(K::Convert, 115),
                Node::Space(Size::Stretch(1)),
            ]));
            col.push(lines(K::ConvertNote));
        }
        col.push(Node::Space(Size::Stretch(1)));
        if shown(K::Target) {
            col.push(stretch(K::Target));
        }
        let mut foot = Vec::new();
        for (k, w) in [(K::Start, 125), (K::Cancel, 100)] {
            if shown(k) {
                foot.push(Node::button(k, w));
            }
        }
        if shown(K::Run) {
            foot.push(Node::leaf(
                K::Progress,
                Size::Fixed(200),
                Size::Fixed(layout::ROW - 4),
            ));
            foot.push(stretch(K::Run));
        } else if shown(K::Why) {
            foot.push(stretch(K::Why));
        } else {
            foot.push(stretch(K::Outcome));
        }
        col.push(Node::row(foot));
        f.place_page(Node::inset(Node::column(col), layout::MARGIN));
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let f = &self.form;
        let action = match k {
            K::Chd(c) => {
                if self.chd.handle(f, c) {
                    self.refresh();
                }
                None
            }
            K::Drive => {
                let i = f.picked(k).and_then(|i| i.checked_sub(1));
                Some(OpticalAction::SelectDrive(i))
            }
            K::Format(i) => OutputFormat::ALL
                .get(i)
                .map(|x| OpticalAction::SetOutputFormat(*x)),
            K::DriveMode => Some(OpticalAction::SetSourceMode(SourceMode::PhysicalDrive)),
            K::ImageMode => Some(OpticalAction::SetSourceMode(SourceMode::ImageFile)),
            K::Refresh => Some(OpticalAction::RefreshDrives),
            K::Eject => Some(OpticalAction::Eject),
            K::AddRemote => Some(OpticalAction::OpenAddRemote),
            K::RecentItem(i) => self
                .recent_files
                .borrow()
                .get(i)
                .map(|p| OpticalAction::OpenImage(PathBuf::from(p))),
            K::OpenImage => Some(OpticalAction::PickImage),
            K::BrowseContents => Some(OpticalAction::BrowseContents),
            K::CloseDisc => Some(OpticalAction::CloseDisc),
            K::ChdInfo => Some(OpticalAction::ChdInfo),
            K::Audio => Some(OpticalAction::Audio(AudioAction::Open)),
            K::PickOutput => Some(OpticalAction::PickOutput),
            K::EjectAfter => Some(OpticalAction::SetEjectAfter(f.checked(k))),
            K::Start => self.start(),
            K::Cancel => Some(OpticalAction::Cancel),
            _ => None,
        };
        if let Some(a) = action {
            let me = self.me.clone();
            crate::app::later(Box::new(move || {
                if let Some(s) = me.upgrade() {
                    s.apply(a);
                }
            }));
        }
    }
}
