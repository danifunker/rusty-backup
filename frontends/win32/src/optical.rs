//! The Optical screen over `controller::optical`; GTK's `optical/` is the reference.
//!
//! The source (a drive, or an image with its recent list), the detected disc and its tools
//! (Browse Contents, Close Disc, CHD Info, Audio Tracks), the rip's output form, and Start /
//! Cancel. An image converts in the Convert tab, so image mode offers Convert... instead. The disc
//! browser is a `BrowseView` in disc mode on its own page, shown in the screen's place. Remote
//! drives arrive with slice 7b.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

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
use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, RECT};
use windows::Win32::UI::Controls::{BCN_DROPDOWN, BS_SPLITBUTTON, NMHDR};
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, GetAncestor, GetWindowRect, ShowWindow, BN_CLICKED, BS_AUTORADIOBUTTON,
    CBN_SELCHANGE, ES_AUTOVSCROLL, ES_MULTILINE, ES_READONLY, GA_ROOT, SW_SHOW, WINDOW_EX_STYLE,
    WINDOW_STYLE, WS_EX_CLIENTEDGE, WS_GROUP, WS_TABSTOP, WS_VSCROLL,
};

use crate::browse::BrowseView;
use crate::build_disk::{checked, set_check, set_line};
use crate::chd::{ChdControl, ChdIds};
use crate::combo::Combo;
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;

fn radio(parent: HWND, text: &str, first: bool, id: &str) -> HWND {
    let style = (BS_AUTORADIOBUTTON as u32) | WS_TABSTOP.0 | if first { WS_GROUP.0 } else { 0 };
    ui::child(
        parent,
        w!("BUTTON"),
        text,
        WINDOW_STYLE(style),
        WINDOW_EX_STYLE(0),
        id,
    )
}

fn progress(parent: HWND) -> HWND {
    ui::child(
        parent,
        w!("msctls_progress32"),
        "",
        WINDOW_STYLE(0),
        WINDOW_EX_STYLE(0),
        "",
    )
}

/// A read-only text window (CHD Info); closing it tells the screen.
struct TextWindow {
    window: HWND,
    text: HWND,
    close: HWND,
    on_close: Box<dyn Fn()>,
}

impl TextWindow {
    fn open(owner: HWND, title: &str, body: &str, on_close: Box<dyn Fn()>) -> Rc<TextWindow> {
        let window = panel::dialog(owner, title, 620, 480);
        let style = ES_MULTILINE as u32 | ES_READONLY as u32 | ES_AUTOVSCROLL as u32;
        let text = ui::child(
            window,
            w!("EDIT"),
            &body.replace('\n', "\r\n"),
            WINDOW_STYLE(style | WS_VSCROLL.0 | WS_TABSTOP.0),
            WS_EX_CLIENTEDGE,
            "",
        );
        // SAFETY: a plain query of the window we just made.
        let dpi = unsafe { windows::Win32::UI::HiDpi::GetDpiForWindow(window) }.max(96);
        ui::set_font(text, ui::mono_font_at(dpi));
        let close = ui::button(window, "Close", "window.close");
        let w = Rc::new(TextWindow {
            window,
            text,
            close,
            on_close,
        });
        let weak: Weak<dyn Handler> = Rc::downgrade(&w) as Weak<dyn Handler>;
        panel::register(window, weak);
        w.resized();
        // SAFETY: showing the window we created.
        unsafe {
            let _ = ShowWindow(window, SW_SHOW);
        }
        w
    }
}

impl Drop for TextWindow {
    fn drop(&mut self) {
        panel::unregister(self.window);
        // SAFETY: our own window, closed with its controller state.
        let _ = unsafe { DestroyWindow(self.window) };
    }
}

impl Handler for TextWindow {
    fn command(&self, from: HWND, code: u32) {
        if from == self.close && code == BN_CLICKED {
            (self.on_close)();
        }
    }

    fn notify(&self, _hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        None
    }

    fn resized(&self) {
        let (area, dpi) = panel::client(self.window);
        let tree = Node::inset(
            Node::column(vec![
                Node::leaf(self.text, Size::Stretch(1), Size::Stretch(1)),
                Node::row(vec![
                    Node::Space(Size::Stretch(1)),
                    Node::button(self.close, 90),
                ]),
            ]),
            layout::MARGIN,
        );
        for (h, r) in layout::layout(&tree, area, dpi) {
            ui::move_to(h, r);
        }
    }

    fn close(&self) {
        (self.on_close)();
    }
}

/// The CD-DA player window: a Play / Stop button per audio track and the playing one's line.
struct AudioWindow {
    window: HWND,
    heading: HWND,
    bar: HWND,
    error: HWND,
    rows: Vec<(u32, HWND, HWND)>,
    close: HWND,
    send: Rc<dyn Fn(OpticalAction)>,
}

impl AudioWindow {
    fn open(owner: HWND, tracks: &[(u32, String)], send: Rc<dyn Fn(OpticalAction)>) -> Rc<Self> {
        let window = panel::dialog(owner, "Audio Tracks", 560, 420);
        let rows = tracks
            .iter()
            .map(|(n, idle)| {
                let id = format!("{}.{n}", audio_ids::PLAY);
                (
                    *n,
                    ui::button(window, "Play", &id),
                    ui::cell_label(window, idle),
                )
            })
            .collect();
        let w = Rc::new(AudioWindow {
            window,
            heading: ui::label(window, ""),
            bar: progress(window),
            error: ui::label(window, ""),
            rows,
            close: ui::button(window, "Close", audio_ids::CLOSE),
            send,
        });
        let weak: Weak<dyn Handler> = Rc::downgrade(&w) as Weak<dyn Handler>;
        panel::register(window, weak);
        w.resized();
        // SAFETY: showing the window we created.
        unsafe {
            let _ = ShowWindow(window, SW_SHOW);
        }
        w
    }

    fn tracks(&self) -> Vec<u32> {
        self.rows.iter().map(|r| r.0).collect()
    }

    fn show(&self, s: &AudioShown) {
        let (heading, fraction) = s.heading.clone().unzip();
        set_line(self.heading, heading.as_deref(), None);
        let pct = fraction
            .flatten()
            .map(|f| (f.clamp(0.0, 1.0) * 100.0) as u32);
        crate::inspect::set_progress(self.bar, heading.is_some(), pct);
        set_line(self.error, s.error.as_deref(), Some(Tone::Error));
        for ((n, button, label), t) in self.rows.iter().zip(&s.tracks) {
            debug_assert_eq!(*n, t.number);
            let playing = t.now.is_some();
            ui::set_text(*button, if playing { "Stop" } else { "Play" });
            let text = t.now.clone().flatten().unwrap_or_else(|| t.idle.clone());
            ui::set_text(*label, &text);
        }
        self.resized();
    }
}

impl Drop for AudioWindow {
    fn drop(&mut self) {
        panel::unregister(self.window);
        // SAFETY: our own window; the player stopped with it.
        let _ = unsafe { DestroyWindow(self.window) };
    }
}

impl Handler for AudioWindow {
    fn command(&self, from: HWND, code: u32) {
        if code != BN_CLICKED {
            return;
        }
        if from == self.close {
            (self.send)(OpticalAction::Audio(AudioAction::Close));
            return;
        }
        if let Some((n, b, _)) = self.rows.iter().find(|r| r.1 == from) {
            let audio = match ui::text(*b).as_str() {
                "Stop" => AudioAction::Stop,
                _ => AudioAction::Play(*n),
            };
            (self.send)(OpticalAction::Audio(audio));
        }
    }

    fn notify(&self, _hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        None
    }

    fn resized(&self) {
        let (area, dpi) = panel::client(self.window);
        let line = |h: HWND| Node::leaf(h, Size::Stretch(1), Size::Fixed(layout::ROW - 8));
        let mut col = Vec::new();
        for h in [self.heading, self.error] {
            if ui::shown(h) {
                col.push(line(h));
            }
        }
        if ui::shown(self.bar) {
            col.push(Node::leaf(
                self.bar,
                Size::Stretch(1),
                Size::Fixed(layout::ROW - 10),
            ));
        }
        for chunk in self.rows.chunks(2) {
            let mut row = Vec::new();
            for (_, b, l) in chunk {
                row.push(Node::button(*b, 60));
                row.push(Node::leaf(*l, Size::Stretch(1), Size::Fixed(layout::ROW)));
            }
            col.push(Node::row(row));
        }
        col.push(Node::Space(Size::Stretch(1)));
        col.push(Node::row(vec![
            Node::Space(Size::Stretch(1)),
            Node::button(self.close, 90),
        ]));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in layout::layout(&tree, area, dpi) {
            if ui::shown(h) {
                ui::move_to(h, r);
            }
        }
    }

    fn close(&self) {
        (self.send)(OpticalAction::Audio(AudioAction::Close));
    }
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

pub struct OpticalScreen {
    ctrl: RefCell<OpticalController>,
    host: EffectHost,
    recent: RecentHandle,
    recent_files: RefCell<Vec<String>>,
    busy: Cell<Option<&'static str>>,
    sync: SyncGuard,
    me: Weak<OpticalScreen>,
    page: HWND,
    browse_page: HWND,
    browse: Rc<BrowseView>,
    convert_asked: RefCell<Option<PathBuf>>,
    busy_note: HWND,
    drive_mode: HWND,
    image_mode: HWND,
    drive_caption: HWND,
    drive: Combo,
    refresh: HWND,
    eject: HWND,
    drive_note: HWND,
    image_caption: HWND,
    image: HWND,
    open_image: HWND,
    job: HWND,
    job_progress: HWND,
    disc: HWND,
    browse_contents: HWND,
    close_disc: HWND,
    chd_info: HWND,
    audio: HWND,
    now_playing: HWND,
    task: HWND,
    format_caption: HWND,
    formats: Vec<(OutputFormat, HWND)>,
    chd: ChdControl,
    output_caption: HWND,
    output: HWND,
    pick_output: HWND,
    eject_after: HWND,
    convert: HWND,
    convert_note: HWND,
    target: HWND,
    outcome: HWND,
    run: HWND,
    progress: HWND,
    start: HWND,
    cancel: HWND,
    why: HWND,
    chd_window: RefCell<Option<Rc<TextWindow>>>,
    audio_window: RefCell<Option<Rc<AudioWindow>>>,
    shape: RefCell<Vec<usize>>,
}

impl OpticalScreen {
    pub fn build(
        page: HWND,
        browse_page: HWND,
        notify: Arc<dyn Notify>,
        recent: RecentHandle,
        host: EffectHost,
    ) -> Rc<OpticalScreen> {
        let split = WINDOW_STYLE(BS_SPLITBUTTON as u32 | WS_TABSTOP.0);
        let image = ui::child(
            page,
            w!("BUTTON"),
            "",
            split,
            WINDOW_EX_STYLE(0),
            ids::IMAGE,
        );
        let chd = ChdControl::build(
            page,
            ChdIds {
                mode: ids::CHD_MODE,
                hunk: ids::CHD_HUNK,
                codec: ids::CHD_CODEC,
            },
            ChdOptionsControl::new(ChdProfile::Cd),
            ChdProfile::Cd,
        );
        let formats = OutputFormat::ALL
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let id = format!("{}.{}", ids::OUTPUT_FORMAT, f.extension());
                (*f, radio(page, f.label(), i == 0, &id))
            })
            .collect();
        let browse = BrowseView::build(browse_page, notify.clone(), host.clone(), true);
        let screen = Rc::new_cyclic(|me: &Weak<OpticalScreen>| OpticalScreen {
            ctrl: RefCell::new(OpticalController::new(notify)),
            host,
            recent_files: RefCell::new(recent.list(RecentMode::Optical)),
            recent,
            busy: Cell::new(None),
            sync: SyncGuard::default(),
            me: me.clone(),
            page,
            browse_page,
            browse,
            convert_asked: RefCell::default(),
            busy_note: ui::label(page, ""),
            drive_mode: radio(page, "Physical drive", true, ids::SOURCE_DRIVE),
            image_mode: radio(page, "Image file", false, ids::SOURCE_IMAGE),
            drive_caption: ui::cell_label(page, "Drive:"),
            drive: Combo::build(page, ids::DRIVE, &[]),
            refresh: ui::button(page, "Refresh", ids::REFRESH),
            eject: ui::button(page, "Eject", ids::EJECT),
            drive_note: ui::label(page, ""),
            image_caption: ui::cell_label(page, "Image:"),
            image,
            open_image: ui::button(page, "Open File...", ids::BROWSE_IMAGE),
            job: ui::cell_label(page, ""),
            job_progress: progress(page),
            disc: ui::label(page, ""),
            browse_contents: ui::button(page, "Browse Contents", ids::BROWSE_CONTENTS),
            close_disc: ui::button(page, "Close Disc", ids::CLOSE_DISC),
            chd_info: ui::button(page, "CHD Info", ids::CHD_INFO),
            audio: ui::button(page, "Audio Tracks", audio_ids::OPEN),
            now_playing: ui::cell_label(page, ""),
            task: ui::cell_label(page, ""),
            format_caption: ui::cell_label(page, "Output format:"),
            formats,
            chd,
            output_caption: ui::cell_label(page, "Output:"),
            output: ui::path_label(page, ""),
            pick_output: ui::button(page, "Browse...", ids::PICK_OUTPUT),
            eject_after: ui::checkbox(page, "Eject after ripping", ids::EJECT_AFTER),
            convert: ui::button(page, labels::CONVERT, ids::CONVERT),
            convert_note: ui::label(page, hints::CONVERT_MOVED),
            target: ui::path_label(page, ""),
            outcome: ui::label(page, ""),
            run: ui::cell_label(page, ""),
            progress: progress(page),
            start: ui::button(page, "Start", ids::START),
            cancel: ui::button(page, "Cancel", ids::CANCEL),
            why: ui::cell_label(page, ""),
            chd_window: RefCell::default(),
            audio_window: RefCell::default(),
            shape: RefCell::default(),
        });
        let weak: Weak<dyn Handler> = screen.me.clone();
        panel::register(page, weak);
        let me = screen.me.clone();
        screen.browse.on_change(Rc::new(move || {
            if let Some(s) = me.upgrade() {
                s.browser_changed();
            }
        }));
        screen.apply(OpticalAction::FindDrives);
        screen
    }

    pub fn set_busy(&self, busy: Option<&'static str>) {
        self.busy.set(busy);
    }

    /// Settings cleared the recent files: the picker forgets its copy too.
    pub fn forget_recents(&self) {
        self.recent_files.borrow_mut().clear();
    }

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    /// The disc browser is open, in the screen's place.
    pub fn browsing(&self) -> bool {
        self.browse.is_active()
    }

    /// Open `path` as the image source (Inspect's hand-off).
    pub fn open_image(&self, path: PathBuf) {
        self.apply(OpticalAction::OpenImage(path));
    }

    /// The disc image "Convert..." asked to hand to the Convert tab, once.
    pub fn take_convert(&self) -> Option<PathBuf> {
        self.convert_asked.borrow_mut().take()
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
    fn browser_changed(&self) {
        let open = self.browse.is_active();
        if !open && self.ctrl.borrow().is_browsing() {
            self.apply(OpticalAction::BrowserClosed);
        }
        self.show_page();
    }

    /// Show the disc browser while it is open, the screen otherwise; only while the tab is up.
    fn show_page(&self) {
        let here = ui::shown(self.page) || ui::shown(self.browse_page);
        let browsing = self.browse.is_active();
        ui::set_visible(self.page, here && !browsing);
        ui::set_visible(self.browse_page, here && browsing);
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
        if ui::shown(self.page) {
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

    fn owner(&self) -> HWND {
        // SAFETY: a plain query of our own window.
        unsafe { GetAncestor(self.page, GA_ROOT) }
    }

    /// CHD Info and the player follow the controller: open while it says so.
    fn show_windows(&self) {
        let info = self.ctrl.borrow().chd_info().map(str::to_string);
        match info {
            None => {
                self.chd_window.replace(None);
            }
            Some(text) if self.chd_window.borrow().is_none() => {
                let send = self.send();
                let w = TextWindow::open(
                    self.owner(),
                    "CHD Info",
                    &text,
                    Box::new(move || send(OpticalAction::CloseChdInfo)),
                );
                *self.chd_window.borrow_mut() = Some(w);
            }
            Some(_) => {}
        }
        let shown = self.read_audio();
        let Some(shown) = shown else {
            self.audio_window.replace(None);
            return;
        };
        let numbers: Vec<u32> = shown.tracks.iter().map(|t| t.number).collect();
        let held = self
            .audio_window
            .borrow()
            .clone()
            .filter(|w| w.tracks() == numbers);
        let w = held.unwrap_or_else(|| {
            self.audio_window.replace(None);
            let list: Vec<(u32, String)> = shown
                .tracks
                .iter()
                .map(|t| (t.number, t.idle.clone()))
                .collect();
            let w = AudioWindow::open(self.owner(), &list, self.send());
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

    /// A track's position moves with no job to wake the shell, so a playing track keeps ticking.
    pub fn playing(&self) -> bool {
        self.ctrl.borrow().audio().now_playing().is_some()
    }

    /// Start's whole rule: asked again at the click, not only when the button was last drawn.
    fn may_start(&self) -> bool {
        let ctrl = self.ctrl.borrow();
        let chd_ok = ctrl.output_format() != OutputFormat::Chd || self.chd.is_valid();
        ctrl.can_start() && chd_ok && self.busy.get().is_none()
    }

    fn start(&self) {
        if self.may_start() {
            let chd = (self.ctrl.borrow().output_format() == OutputFormat::Chd).then(|| {
                let (options, custom) = self.chd.choice();
                ChdChoice { options, custom }
            });
            self.apply(OpticalAction::Start(chd));
        }
    }

    /// The image button's menu: recent images, then Open File.
    fn image_menu(&self) {
        let recent = self.recent_files.borrow().clone();
        let mut entries: Vec<(String, bool)> = recent.iter().map(|p| (p.clone(), true)).collect();
        if !recent.is_empty() {
            entries.push((String::new(), false));
        }
        entries.push(("Open File...".into(), true));
        let mut r = RECT::default();
        // SAFETY: a plain query of our own window.
        let picked = unsafe {
            let _ = GetWindowRect(self.image, &mut r);
            crate::menu::choose(self.owner(), &entries, r.left, r.bottom)
        };
        match picked {
            Some(i) if i < recent.len() => {
                self.apply(OpticalAction::OpenImage(PathBuf::from(&recent[i])))
            }
            Some(i) if i == entries.len() - 1 => self.apply(OpticalAction::PickImage),
            _ => {}
        }
    }

    pub fn refresh(&self) {
        self.show_page();
        self.sync.run(|| self.show());
        if *self.shape.borrow() != self.shape_now() {
            self.layout();
        }
    }

    fn show(&self) {
        let can_start = self.may_start();
        let ctrl = self.ctrl.borrow();
        let busy = self.busy.get();
        let notice = busy.map(busy_elsewhere_notice);
        set_line(self.busy_note, notice.as_deref(), Some(Tone::Warning));
        let image_mode = ctrl.source_mode() == SourceMode::ImageFile;
        set_check(self.drive_mode, !image_mode);
        set_check(self.image_mode, image_mode);
        for h in [self.drive_caption, self.drive.h, self.refresh, self.eject] {
            ui::set_visible(h, !image_mode);
        }
        let mut drives = vec![if ctrl.scanning_drives() {
            "Scanning drives...".to_string()
        } else {
            "Select a drive...".to_string()
        }];
        drives.extend(ctrl.drive_labels());
        self.drive.set_items(&drives);
        self.drive
            .select(ctrl.selected_drive().map_or(0, |i| i + 1));
        ui::set_enabled(self.eject, ctrl.can_eject());
        let no_drive = !physical_devices_available() && ctrl.drive_count() == 0;
        set_line(
            self.drive_note,
            (!image_mode && no_drive).then_some(hints::PHYSICAL_DRIVE),
            None,
        );
        for h in [self.image_caption, self.image, self.open_image] {
            ui::set_visible(h, image_mode);
        }
        let image = ctrl.image().map(|p| p.display().to_string());
        ui::set_text(self.image, image.as_deref().unwrap_or("No file selected"));

        let job = ctrl.detecting();
        let (line, pct) = job
            .as_ref()
            .map(crate::inspect::job_line)
            .unwrap_or_default();
        ui::set_text(self.job, &line);
        ui::set_visible(self.job, job.is_some());
        crate::inspect::set_progress(self.job_progress, job.is_some(), pct);

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
                Some((lines.join("\r\n"), None))
            }
            (None, Some(e)) => Some((e.to_string(), Some(Tone::Warning))),
            (None, None) => None,
        };
        let (text, tone) = text.unzip();
        set_line(self.disc, text.as_deref(), tone.flatten());
        let has_disc = disc.is_some();
        for h in [self.browse_contents, self.close_disc] {
            ui::set_visible(h, has_disc);
        }
        ui::set_enabled(self.browse_contents, !ctrl.is_browsing());
        ui::set_visible(self.chd_info, has_disc && ctrl.is_chd_image());
        ui::set_enabled(
            self.chd_info,
            ctrl.can_chd_info() && !ctrl.chd_info_running(),
        );
        let player = ctrl.audio();
        let tracks = player.audio_tracks().count();
        ui::set_visible(self.audio, has_disc && player.has_audio());
        ui::set_text(self.audio, &format!("Audio Tracks ({tracks})"));
        let now = player.readout().map(|r| r.line);
        set_line(self.now_playing, now.as_deref(), None);

        let rip = ctrl.task() == Task::Rip;
        ui::set_text(self.task, ctrl.task_label());
        ui::set_visible(self.task, rip);
        let choices = ctrl.format_choices();
        for (format, h) in &self.formats {
            ui::set_visible(*h, rip);
            let ok = choices.iter().any(|(f, on)| f == format && *on);
            ui::set_enabled(*h, ok);
            set_check(*h, *format == ctrl.output_format());
        }
        ui::set_visible(self.format_caption, rip);
        self.chd
            .set_visible(rip && ctrl.output_format() == OutputFormat::Chd);
        for h in [
            self.output_caption,
            self.output,
            self.pick_output,
            self.eject_after,
        ] {
            ui::set_visible(h, rip);
        }
        let out = ctrl.output().map(|p| p.display().to_string());
        ui::set_text(
            self.output,
            out.as_deref().unwrap_or("No output path selected"),
        );
        set_check(self.eject_after, ctrl.eject_after());
        ui::set_visible(self.convert, !rip);
        ui::set_visible(self.convert_note, !rip);
        ui::set_enabled(self.convert, ctrl.convert_source().is_some());

        let running = ctrl.is_running();
        let locked = running || busy.is_some();
        for h in [
            self.drive_mode,
            self.image_mode,
            self.drive.h,
            self.refresh,
            self.image,
            self.open_image,
            self.pick_output,
            self.eject_after,
        ] {
            ui::set_enabled(h, !locked);
        }
        let writes = ctrl.write_target().map(|t| format!("Writes: {t}"));
        set_line(self.target, writes.as_deref().filter(|_| rip), None);
        ui::set_visible(self.start, rip && !running);
        ui::set_enabled(self.start, can_start);
        ui::set_text(self.start, ctrl.start_label());
        ui::set_visible(self.cancel, running);
        ui::set_enabled(self.cancel, !ctrl.cancelling());
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
        ui::set_text(self.run, run.as_deref().unwrap_or(""));
        ui::set_visible(self.run, run.is_some());
        crate::inspect::set_progress(
            self.progress,
            run.is_some(),
            pct.or(Some(0)).filter(|_| run.is_some()),
        );
        let chd_error = (ctrl.output_format() == OutputFormat::Chd && !self.chd.is_valid())
            .then(|| "Check the CHD options.".to_string());
        let why = ctrl.start_refusal().or(chd_error).or(notice);
        let show_why = rip && !running && !can_start;
        set_line(self.why, why.as_deref().filter(|_| show_why), None);
        let outcome = match ctrl.last_result() {
            Some(Ok(())) => Some(("Finished.".to_string(), None)),
            Some(Err(e)) => Some((e.clone(), Some(Tone::Error))),
            None => None,
        };
        let (text, tone) = outcome.filter(|_| run.is_none() && !show_why).unzip();
        set_line(self.outcome, text.as_deref(), tone.flatten());
    }

    fn shape_now(&self) -> Vec<usize> {
        let mut v: Vec<usize> = [
            self.busy_note,
            self.drive.h,
            self.drive_note,
            self.image,
            self.job,
            self.disc,
            self.browse_contents,
            self.chd_info,
            self.audio,
            self.now_playing,
            self.task,
            self.convert,
            self.target,
            self.outcome,
            self.run,
            self.start,
            self.cancel,
            self.why,
        ]
        .iter()
        .map(|h| usize::from(ui::shown(*h)))
        .collect();
        v.extend(self.chd.combos().iter().map(|h| usize::from(ui::shown(*h))));
        v.push(ui::text(self.disc).lines().count());
        v
    }

    fn layout(&self) {
        *self.shape.borrow_mut() = self.shape_now();
        let (area, dpi) = panel::client(self.page);
        let row_h = Size::Fixed(layout::ROW);
        let fixed = |h: HWND, w: i32| Node::leaf(h, Size::Fixed(w), row_h);
        let stretch = |h: HWND| Node::leaf(h, Size::Stretch(1), row_h);
        let lines = |h: HWND| {
            let n = ui::text(h).lines().count().max(1) as i32;
            Node::leaf(h, Size::Stretch(1), Size::Fixed((layout::ROW - 8) * n))
        };
        let shown = |h: HWND| ui::shown(h);
        let cap = 110;
        let mut col = Vec::new();
        if shown(self.busy_note) {
            col.push(lines(self.busy_note));
        }
        col.push(Node::row(vec![
            fixed(self.drive_mode, 150),
            fixed(self.image_mode, 150),
            Node::Space(Size::Stretch(1)),
        ]));
        if shown(self.drive.h) {
            col.push(Node::row(vec![
                fixed(self.drive_caption, cap),
                stretch(self.drive.h),
                Node::button(self.refresh, 80),
                Node::button(self.eject, 70),
            ]));
        }
        if shown(self.drive_note) {
            col.push(lines(self.drive_note));
        }
        if shown(self.image) {
            col.push(Node::row(vec![
                fixed(self.image_caption, cap),
                stretch(self.image),
                Node::button(self.open_image, 110),
            ]));
        }
        if shown(self.job) {
            col.push(Node::row(vec![
                stretch(self.job),
                Node::leaf(
                    self.job_progress,
                    Size::Fixed(180),
                    Size::Fixed(layout::ROW - 6),
                ),
            ]));
        }
        if shown(self.disc) {
            col.push(lines(self.disc));
        }
        if shown(self.browse_contents) {
            let mut row = vec![
                Node::button(self.browse_contents, 140),
                Node::button(self.close_disc, 100),
            ];
            for (h, w) in [(self.chd_info, 90), (self.audio, 140)] {
                if shown(h) {
                    row.push(Node::button(h, w));
                }
            }
            row.push(Node::Space(Size::Stretch(1)));
            col.push(Node::row(row));
        }
        if shown(self.now_playing) {
            col.push(stretch(self.now_playing));
        }
        if shown(self.task) {
            col.push(stretch(self.task));
            let mut row = vec![fixed(self.format_caption, cap)];
            row.extend(self.formats.iter().map(|(_, h)| fixed(*h, 110)));
            row.push(Node::Space(Size::Stretch(1)));
            col.push(Node::row(row));
            col.extend(self.chd.rows());
            col.push(Node::row(vec![
                fixed(self.output_caption, cap),
                stretch(self.output),
                Node::button(self.pick_output, 85),
            ]));
            col.push(Node::row(vec![stretch(self.eject_after)]));
        }
        if shown(self.convert) {
            col.push(Node::row(vec![
                Node::button(self.convert, 110),
                Node::Space(Size::Stretch(1)),
            ]));
            col.push(lines(self.convert_note));
        }
        col.push(Node::Space(Size::Stretch(1)));
        if shown(self.target) {
            col.push(stretch(self.target));
        }
        let mut foot = Vec::new();
        for (h, w) in [(self.start, 120), (self.cancel, 100)] {
            if shown(h) {
                foot.push(Node::button(h, w));
            }
        }
        if shown(self.run) {
            foot.push(Node::leaf(
                self.progress,
                Size::Fixed(200),
                Size::Fixed(layout::ROW - 6),
            ));
            foot.push(stretch(self.run));
        } else if shown(self.why) {
            foot.push(stretch(self.why));
        } else {
            foot.push(stretch(self.outcome));
        }
        col.push(Node::row(foot));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in layout::layout(&tree, area, dpi) {
            if ui::shown(h) {
                ui::move_to(h, r);
            }
        }
    }
}

impl Handler for OpticalScreen {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        if self.chd.command(from, code) {
            self.refresh();
            return;
        }
        if code == CBN_SELCHANGE && from == self.drive.h {
            let i = self.drive.picked().and_then(|i| i.checked_sub(1));
            self.apply(OpticalAction::SelectDrive(i));
            return;
        }
        if code != BN_CLICKED {
            return;
        }
        if let Some((f, _)) = self.formats.iter().find(|(_, h)| *h == from) {
            self.apply(OpticalAction::SetOutputFormat(*f));
            return;
        }
        let action = if from == self.drive_mode {
            OpticalAction::SetSourceMode(SourceMode::PhysicalDrive)
        } else if from == self.image_mode {
            OpticalAction::SetSourceMode(SourceMode::ImageFile)
        } else if from == self.refresh {
            OpticalAction::RefreshDrives
        } else if from == self.eject {
            OpticalAction::Eject
        } else if from == self.image {
            self.image_menu();
            return;
        } else if from == self.open_image {
            OpticalAction::PickImage
        } else if from == self.browse_contents {
            OpticalAction::BrowseContents
        } else if from == self.close_disc {
            OpticalAction::CloseDisc
        } else if from == self.chd_info {
            OpticalAction::ChdInfo
        } else if from == self.audio {
            OpticalAction::Audio(AudioAction::Open)
        } else if from == self.pick_output {
            OpticalAction::PickOutput
        } else if from == self.eject_after {
            OpticalAction::SetEjectAfter(checked(from))
        } else if from == self.convert {
            *self.convert_asked.borrow_mut() = self.ctrl.borrow().convert_source();
            return;
        } else if from == self.start {
            self.start();
            return;
        } else if from == self.cancel {
            OpticalAction::Cancel
        } else {
            return;
        };
        self.apply(action);
    }

    fn notify(&self, hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        if hdr.hwndFrom == self.image && hdr.code == BCN_DROPDOWN {
            self.image_menu();
            return Some(LRESULT(0));
        }
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {}
}
