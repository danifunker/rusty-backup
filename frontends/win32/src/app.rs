//! The application window: top bar, tabs, update banner, status line and log, drawn from `AppController`.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use rusty_backup::controller::app::{
    about, app_version, elevation_button_hover, elevation_button_label, elevation_confirm,
    elevation_gate, ids, AppAction, AppController, AppTab, ElevationGate,
};
use rusty_backup::controller::log::{capture, LogLevel};
use rusty_backup::controller::{Controller, Effect};
use windows::core::{w, PCWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, RECT, WPARAM};
use windows::Win32::System::LibraryLoader::GetModuleHandleW;
use windows::Win32::UI::Controls::{
    EM_GETFIRSTVISIBLELINE, EM_LINESCROLL, EM_REPLACESEL, EM_SCROLLCARET, EM_SETLIMITTEXT,
    EM_SETSEL,
};
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::WindowsAndMessaging::{
    CreateWindowExW, DefWindowProcW, DestroyWindow, EnumChildWindows, GetClientRect, LoadCursorW,
    LoadIconW, PostQuitMessage, RegisterClassExW, SendMessageW, SetWindowPos, BM_GETCHECK,
    BM_SETCHECK, BN_CLICKED, CS_HREDRAW, CS_VREDRAW, CW_USEDEFAULT, ES_AUTOVSCROLL, ES_MULTILINE,
    ES_READONLY, HMENU, IDC_ARROW, MINMAXINFO, SWP_NOACTIVATE, SWP_NOZORDER, WINDOW_STYLE,
    WM_CLOSE, WM_COMMAND, WM_CTLCOLORBTN, WM_CTLCOLORDLG, WM_CTLCOLOREDIT, WM_CTLCOLORLISTBOX,
    WM_CTLCOLORSTATIC, WM_DESTROY, WM_DPICHANGED, WM_DROPFILES, WM_ERASEBKGND, WM_GETMINMAXINFO,
    WM_NOTIFY, WM_SETTINGCHANGE, WM_SIZE, WNDCLASSEXW, WS_CLIPCHILDREN, WS_EX_CLIENTEDGE,
    WS_EX_CONTROLPARENT, WS_OVERLAPPEDWINDOW, WS_TABSTOP, WS_VSCROLL,
};

use crate::backup::BackupScreen;
use crate::build_disk::BuildScreen;
use crate::commander::CommanderScreen;
use crate::effects::{note, EffectHost, Reply, ReplyFn};
use crate::inspect::InspectScreen;
use crate::layout::{self, Node, Rect, Size};
use crate::restore::RestoreScreen;
use crate::sync::SyncGuard;
use crate::toolbar;

/// The screen switcher's command ids, in `AppTab::ALL` order, and Commander's.
const SCREEN_CMD: i32 = 100;
const COMMANDER_CMD: i32 = 200;
use crate::ui;
use crate::wake::{Win32Wake, WM_APP_JOB};

/// What each control is, for layout and for `WM_COMMAND`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Part {
    Commander,
    CloseBackup,
    Refresh,
    Elevate,
    Settings,
    About,
    BannerText,
    ViewRelease,
    Dismiss,
    /// The screen switcher (D9).
    Screens,
    Status,
    LogCopy,
    LogClear,
    LogAuto,
    Log,
}

pub struct App {
    ctrl: RefCell<AppController>,
    wake: Arc<Win32Wake>,
    host: EffectHost,
    window: HWND,
    parts: Vec<(Part, HWND)>,
    /// One page per tab, shown inside the tab control; the screens not built yet hold a note.
    pages: Vec<(AppTab, HWND)>,
    /// The Inspect tab's browse view, shown in its page's place while it is open.
    browse_page: HWND,
    inspect: Rc<InspectScreen>,
    backup: Rc<BackupScreen>,
    restore: Rc<RestoreScreen>,
    build: Rc<BuildScreen>,
    /// Commander, while it is open, on its own page over the whole window.
    commander: RefCell<Option<(Rc<CommanderScreen>, HWND)>>,
    /// What was showing when Commander opened, put back when it closes.
    covered: RefCell<Vec<(HWND, bool)>>,
    log_shown: Cell<usize>,
    sync: SyncGuard,
}

thread_local! {
    static APP: RefCell<Option<Rc<App>>> = const { RefCell::new(None) };
}

fn app() -> Option<Rc<App>> {
    APP.with(|a| a.borrow().clone())
}

/// Answers to the app controller's own questions.
fn reply() -> ReplyFn {
    Rc::new(|reply| {
        let Some(app) = app() else { return };
        match reply {
            Reply::Confirmed {
                purpose: "elevate",
                yes,
            } => app.apply(AppAction::ConfirmElevation(yes)),
            Reply::Picked { .. }
            | Reply::Confirmed { .. }
            | Reply::Answered { .. }
            | Reply::Chose { .. }
            | Reply::CloseScreen => {}
        }
    })
}

/// Create the main window and show it.
pub fn start() {
    let wake = Arc::new(Win32Wake::default());
    capture::set_wake(wake.clone());
    // SAFETY: a plain query; the window opens at 1200 x 800 DIPs on the primary monitor.
    let dpi = unsafe { windows::Win32::UI::HiDpi::GetDpiForSystem() }.max(96);
    // SAFETY: plain window-class registration and creation on the UI thread.
    let window = unsafe {
        let inst = GetModuleHandleW(None).expect("module handle");
        let class = w!("RustyBackupWin32");
        let wc = WNDCLASSEXW {
            cbSize: std::mem::size_of::<WNDCLASSEXW>() as u32,
            style: CS_HREDRAW | CS_VREDRAW,
            lpfnWndProc: Some(wndproc),
            hInstance: inst.into(),
            hIcon: LoadIconW(Some(inst.into()), PCWSTR(1 as _)).unwrap_or_default(),
            hCursor: LoadCursorW(None, IDC_ARROW).unwrap_or_default(),
            hbrBackground: windows::Win32::Graphics::Gdi::HBRUSH(
                (windows::Win32::Graphics::Gdi::COLOR_BTNFACE.0 + 1) as _,
            ),
            lpszClassName: class,
            ..Default::default()
        };
        RegisterClassExW(&wc);
        CreateWindowExW(
            WS_EX_CONTROLPARENT,
            class,
            w!("Rusty Backup (Win32 preview)"),
            WS_OVERLAPPEDWINDOW | WS_CLIPCHILDREN,
            CW_USEDEFAULT,
            CW_USEDEFAULT,
            layout::scale(1200, dpi),
            layout::scale(800, dpi),
            None,
            Some(HMENU(std::ptr::null_mut())),
            Some(inst.into()),
            None,
        )
        .expect("the main window")
    };
    wake.set_window(window);
    crate::theme::title_bar(window);
    // SAFETY: our own window takes files dropped from Explorer.
    unsafe { windows::Win32::UI::Shell::DragAcceptFiles(window, true) };
    let app = App::build(window, wake);
    APP.with(|a| *a.borrow_mut() = Some(app.clone()));
    app.relayout();
    // SAFETY: showing the window we created.
    unsafe {
        let _ = windows::Win32::UI::WindowsAndMessaging::ShowWindow(
            window,
            windows::Win32::UI::WindowsAndMessaging::SW_SHOWDEFAULT,
        );
    }
    app.tick();
}

/// Run the controllers now, as a wake does; a screen asks after handing something to the app.
pub fn tick() {
    if let Some(app) = app() {
        app.tick();
    }
}

/// Add a screen's drained log lines to the log panel.
pub fn append_log(lines: Vec<(LogLevel, String)>) {
    if lines.is_empty() {
        return;
    }
    if let Some(app) = app() {
        app.ctrl.borrow_mut().log.extend_lines(lines);
        let auto = app.ctrl.borrow().log_auto_scroll();
        app.sync_log(auto);
    }
}

/// Commander's controller left its screen: close it once the action in hand is done.
fn close_commander() {
    if let Some(app) = app() {
        app.close_commander();
    }
}

/// The main window, for the message loop's dialog navigation.
pub fn window() -> Option<HWND> {
    app().map(|a| a.window)
}

impl App {
    fn build(window: HWND, wake: Arc<Win32Wake>) -> Rc<App> {
        let mut ctrl = AppController::new(wake.clone());
        ctrl.load_devices();
        let gate = elevation_gate();

        let mut parts = Vec::new();
        let mut add = |p: Part, h: HWND| {
            parts.push((p, h));
            h
        };
        let screens: Vec<(i32, &str, &str)> = AppTab::ALL
            .iter()
            .enumerate()
            .map(|(i, t)| (SCREEN_CMD + i as i32, t.label(), t.icon()))
            .collect();
        add(
            Part::Screens,
            toolbar::build(window, "app.screens", &screens, true),
        );
        let commander = add(
            Part::Commander,
            toolbar::build(
                window,
                ids::COMMANDER,
                &[(COMMANDER_CMD, "Commander", "commander")],
                false,
            ),
        );
        // Settings arrives in slice 7b; until then its button is shown but off.
        let _ = commander;
        add(
            Part::CloseBackup,
            ui::button(window, "Close Backup", ids::CLOSE_BACKUP),
        );
        let refresh = add(
            Part::Refresh,
            ui::button(window, "Refresh Devices", ids::REFRESH_DEVICES),
        );
        ui::set_visible(
            refresh,
            matches!(gate, ElevationGate::NotApplicable | ElevationGate::Elevated),
        );
        let elevate = add(
            Part::Elevate,
            ui::button(window, elevation_button_label(), ids::ELEVATE),
        );
        ui::set_visible(elevate, gate == ElevationGate::NeedsElevation);
        let _ = elevation_button_hover();
        let settings = add(
            Part::Settings,
            ui::button(window, "Settings...", ids::SETTINGS),
        );
        ui::set_enabled(settings, false);
        add(
            Part::About,
            ui::button(window, "About Rusty Backup", ids::ABOUT),
        );
        add(Part::BannerText, ui::label(window, ""));
        add(
            Part::ViewRelease,
            ui::button(window, "View Release", ids::UPDATE_VIEW),
        );
        add(
            Part::Dismiss,
            ui::button(window, "Dismiss update notification", ids::UPDATE_DISMISS),
        );
        let status = add(Part::Status, ui::label(window, ""));
        add(Part::LogCopy, ui::button(window, "Copy Log", ids::LOG_COPY));
        add(
            Part::LogClear,
            ui::button(window, "Clear Log", ids::LOG_CLEAR),
        );
        let auto = add(
            Part::LogAuto,
            ui::checkbox(window, "Auto-scroll", ids::LOG_AUTO_SCROLL),
        );
        let style = ES_MULTILINE as u32
            | ES_READONLY as u32
            | ES_AUTOVSCROLL as u32
            | WS_VSCROLL.0
            | WS_TABSTOP.0;
        let log = add(
            Part::Log,
            ui::child(
                window,
                w!("EDIT"),
                "",
                WINDOW_STYLE(style),
                WS_EX_CLIENTEDGE,
                ids::LOG_LIST,
            ),
        );
        // SAFETY: plain control messages.
        unsafe {
            SendMessageW(log, EM_SETLIMITTEXT, Some(WPARAM(0)), Some(LPARAM(0)));
            let on = if ctrl.log_auto_scroll() { 1 } else { 0 };
            SendMessageW(auto, BM_SETCHECK, Some(WPARAM(on)), Some(LPARAM(0)));
        }
        let host = EffectHost::new(window, status);
        let mut inspect = None;
        let mut backup = None;
        let mut restore = None;
        let mut build = None;
        let browse_page = crate::panel::page(window);
        ui::set_visible(browse_page, false);
        let pages = AppTab::ALL
            .iter()
            .map(|t| {
                let page = match t {
                    AppTab::Inspect => {
                        let page = crate::panel::page(window);
                        let screen = InspectScreen::build(
                            window,
                            page,
                            browse_page,
                            ctrl.notify(),
                            ctrl.recent_handle(),
                            host.clone(),
                        );
                        inspect = Some(screen);
                        page
                    }
                    AppTab::Backup => {
                        let page = crate::panel::page(window);
                        backup = Some(BackupScreen::build(
                            page,
                            ctrl.notify(),
                            ctrl.recent_handle(),
                            host.clone(),
                        ));
                        page
                    }
                    AppTab::Restore => {
                        let page = crate::panel::page(window);
                        restore = Some(RestoreScreen::build(
                            page,
                            ctrl.notify(),
                            ctrl.recent_handle(),
                            host.clone(),
                        ));
                        page
                    }
                    AppTab::Build => {
                        let page = crate::panel::page(window);
                        build = Some(BuildScreen::build(page, ctrl.notify(), host.clone()));
                        page
                    }
                    AppTab::Convert | AppTab::Optical | AppTab::Archives => {
                        let text = format!(
                            "The {} screen arrives in a later slice of the Win32 shell.",
                            t.label()
                        );
                        ui::label(window, &text)
                    }
                };
                ui::set_visible(page, false);
                (*t, page)
            })
            .collect();
        let inspect = inspect.expect("the Inspect page");
        let backup = backup.expect("the Backup page");
        let restore = restore.expect("the Restore page");
        let build = build.expect("the Build Disk page");
        ctrl.start_update_check();
        Rc::new(App {
            ctrl: RefCell::new(ctrl),
            wake,
            host,
            window,
            parts,
            pages,
            browse_page,
            inspect,
            backup,
            restore,
            build,
            commander: RefCell::default(),
            covered: RefCell::default(),
            log_shown: Cell::new(0),
            sync: SyncGuard::default(),
        })
    }

    fn part(&self, p: Part) -> HWND {
        self.parts
            .iter()
            .find(|(q, _)| *q == p)
            .map(|(_, h)| *h)
            .expect("every part is built")
    }

    fn apply(&self, action: AppAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    fn tick(&self) {
        self.wake.ticked();
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &reply());
        if self.ctrl.borrow_mut().take_elevation_prompt() {
            self.host
                .run(vec![Effect::Confirm(elevation_confirm())], &reply());
        }
        let devices = self.ctrl.borrow().devices().to_vec();
        self.inspect.set_devices(&devices);
        self.inspect.tick();
        self.backup.set_devices(&devices);
        self.restore.set_devices(&devices);
        self.build.set_devices(&devices);
        self.lock_tabs();
        self.backup.tick();
        self.restore.tick();
        self.build.tick();
        if let Some(path) = self.build.take_open_inspect() {
            self.apply(AppAction::SelectTab(AppTab::Inspect));
            self.inspect.open_image(path);
        }
        let commander = self.commander.borrow().as_ref().map(|(c, _)| c.clone());
        if let Some(c) = commander {
            c.tick();
        }
        if self.inspect.take_backup_closed() {
            self.ctrl.borrow_mut().close_shared_backup();
        }
        if self.inspect.take_convert().is_some() {
            let line = "The Convert tab arrives in slice 7c of the Win32 shell.";
            append_log(vec![(LogLevel::Warning, line.into())]);
        }
        self.refresh();
    }

    /// One disk job at a time: each tab hears who else runs.
    fn lock_tabs(&self) {
        let inspect = self.inspect.running_job();
        let backup = self.backup.is_running().then_some("Backup");
        let restore = self.restore.is_running().then_some("Restore");
        let build = self.build.is_running().then_some("Build Disk");
        self.backup.set_busy(inspect.or(restore).or(build));
        self.restore.set_busy(inspect.or(backup).or(build));
        self.build.set_busy(inspect.or(backup).or(restore));
    }

    /// Open Commander over the whole window; the app's own controls hide until it closes.
    fn open_commander(&self) {
        if self.commander.borrow().is_some() {
            return;
        }
        let mut covered: Vec<(HWND, bool)> = self
            .parts
            .iter()
            .map(|(_, h)| (*h, ui_visible(*h)))
            .collect();
        covered.extend(self.pages.iter().map(|(_, h)| (*h, ui_visible(*h))));
        covered.push((self.browse_page, ui_visible(self.browse_page)));
        for (h, _) in &covered {
            ui::set_visible(*h, false);
        }
        *self.covered.borrow_mut() = covered;
        let page = crate::panel::page(self.window);
        let notify = self.ctrl.borrow().notify();
        let jobs = rusty_backup::controller::commander::JobRegistry::new();
        let on_close: Rc<dyn Fn()> = Rc::new(close_commander);
        let screen = CommanderScreen::build(page, &jobs, &notify, self.host.clone(), on_close);
        *self.commander.borrow_mut() = Some((screen, page));
        self.relayout();
        ui::set_visible(page, true);
    }

    fn close_commander(&self) {
        let Some((_, page)) = self.commander.borrow_mut().take() else {
            return;
        };
        crate::panel::unregister(page);
        ui::set_visible(page, false);
        // The page goes once the action in hand is done; Commander may still be on the stack.
        // SAFETY: posting to our own page, which no longer has a handler.
        unsafe {
            let _ = windows::Win32::UI::WindowsAndMessaging::PostMessageW(
                Some(page),
                WM_CLOSE,
                WPARAM(0),
                LPARAM(0),
            );
        }
        for (h, on) in self.covered.borrow_mut().drain(..) {
            ui::set_visible(h, on);
        }
        self.refresh();
        self.relayout();
    }

    /// Files dropped from Explorer: Commander opens the first, a browse view in Edit Mode stages them.
    fn dropped(&self, paths: Vec<std::path::PathBuf>, at: windows::Win32::Foundation::POINT) {
        let commander = self.commander.borrow().as_ref().map(|(c, _)| c.clone());
        match commander {
            Some(c) => c.drop_paths(paths, at),
            None => {
                if !self.inspect.drop_paths(paths) {
                    let line = "Drop files on Commander, or on a browsed volume in Edit Mode.";
                    append_log(vec![(LogLevel::Info, line.into())]);
                }
            }
        }
    }

    /// Read the controller first and release it, then set controls inside the guard.
    fn refresh(&self) {
        if self.commander.borrow().is_some() {
            return;
        }
        let (tab, shared, banner, auto) = {
            let c = self.ctrl.borrow();
            (
                c.active_tab(),
                c.shared_backup().is_some(),
                c.update_banner_text(),
                c.log_auto_scroll(),
            )
        };
        let banner_was = ui_visible(self.part(Part::BannerText));
        self.sync.run(|| {
            ui::set_visible(self.part(Part::CloseBackup), shared);
            for p in [Part::BannerText, Part::ViewRelease, Part::Dismiss] {
                ui::set_visible(self.part(p), banner.is_some());
            }
            if let Some(text) = &banner {
                ui::set_text(self.part(Part::BannerText), &format!("Update: {text}"));
            }
            let want = AppTab::ALL.iter().position(|t| *t == tab).unwrap_or(0);
            toolbar::check(self.part(Part::Screens), SCREEN_CMD + want as i32);
            // SAFETY: a plain button message.
            unsafe {
                let on = if auto { 1 } else { 0 };
                SendMessageW(
                    self.part(Part::LogAuto),
                    BM_SETCHECK,
                    Some(WPARAM(on)),
                    None,
                );
            }
            let browsing = self.inspect.browsing();
            for (t, page) in &self.pages {
                ui::set_visible(*page, *t == tab && !(browsing && *t == AppTab::Inspect));
            }
            ui::set_visible(self.browse_page, browsing && tab == AppTab::Inspect);
        });
        if banner_was != banner.is_some() {
            self.relayout();
        }
        self.sync_log(auto);
    }

    /// Append the log lines not shown yet; a cleared log starts over.
    fn sync_log(&self, auto: bool) {
        let log = self.part(Part::Log);
        let text = {
            let c = self.ctrl.borrow();
            let entries = c.log.entries();
            if entries.len() < self.log_shown.get() {
                ui::set_text(log, "");
                self.log_shown.set(0);
            }
            let mut text = String::new();
            for e in &entries[self.log_shown.get()..] {
                text.push_str(&ui::plain(&e.line()));
                text.push_str("\r\n");
            }
            self.log_shown.set(entries.len());
            text
        };
        if text.is_empty() {
            return;
        }
        let w = ui::wide(&text);
        // SAFETY: plain edit-control messages; `w` outlives EM_REPLACESEL.
        unsafe {
            let top = SendMessageW(log, EM_GETFIRSTVISIBLELINE, None, None).0;
            SendMessageW(
                log,
                EM_SETSEL,
                Some(WPARAM(usize::MAX >> 1)),
                Some(LPARAM(isize::MAX)),
            );
            SendMessageW(
                log,
                EM_REPLACESEL,
                Some(WPARAM(0)),
                Some(LPARAM(w.as_ptr() as isize)),
            );
            if auto {
                SendMessageW(log, EM_SCROLLCARET, None, None);
            } else {
                let now = SendMessageW(log, EM_GETFIRSTVISIBLELINE, None, None).0;
                SendMessageW(log, EM_LINESCROLL, Some(WPARAM(0)), Some(LPARAM(top - now)));
            }
        }
    }

    /// Place every control for the client area at the window's DPI.
    fn relayout(&self) {
        let mut rc = RECT::default();
        // SAFETY: plain queries of our own window.
        let dpi = unsafe {
            let _ = GetClientRect(self.window, &mut rc);
            GetDpiForWindow(self.window).max(96)
        };
        if let Some((_, page)) = &*self.commander.borrow() {
            let all = Rect {
                x: 0,
                y: 0,
                w: rc.right,
                h: rc.bottom,
            };
            ui::move_to(*page, all);
            return;
        }
        let banner = ui_visible(self.part(Part::BannerText));
        let p = |x: Part| self.part(x);
        let bar = toolbar::height(p(Part::Screens), dpi);
        let mut rows = vec![
            Node::Row {
                children: vec![
                    Node::leaf(p(Part::Screens), Size::Stretch(1), Size::Fixed(bar)),
                    Node::leaf(p(Part::Commander), Size::Fixed(100), Size::Fixed(bar)),
                ],
                gap: layout::GAP,
            },
            Node::Row {
                children: vec![
                    Node::Space(Size::Stretch(1)),
                    Node::button(p(Part::CloseBackup), 100),
                    Node::button(p(Part::Refresh), 110),
                    Node::button(p(Part::Elevate), 190),
                    Node::button(p(Part::Settings), 85),
                    Node::button(p(Part::About), 130),
                ],
                gap: layout::GAP,
            },
        ];
        if banner {
            rows.push(Node::row(vec![
                Node::leaf(
                    p(Part::BannerText),
                    Size::Stretch(1),
                    Size::Fixed(layout::ROW - 8),
                ),
                Node::button(p(Part::ViewRelease), 100),
                Node::button(p(Part::Dismiss), 200),
            ]));
        }
        // Every page takes the same place; the first stands for them in the tree.
        let page = self.pages[0].1;
        rows.push(Node::leaf(page, Size::Stretch(1), Size::Stretch(3)));
        rows.push(Node::leaf(
            p(Part::Status),
            Size::Stretch(1),
            Size::Fixed(layout::ROW - 8),
        ));
        rows.push(Node::row(vec![
            Node::button(p(Part::LogCopy), 85),
            Node::button(p(Part::LogClear), 85),
            Node::button(p(Part::LogAuto), 100),
            Node::Space(Size::Stretch(1)),
        ]));
        rows.push(Node::leaf(p(Part::Log), Size::Stretch(1), Size::Stretch(1)));
        let tree = Node::inset(Node::column(rows), layout::MARGIN);
        let area = Rect {
            x: 0,
            y: 0,
            w: rc.right,
            h: rc.bottom,
        };
        let placed = layout::layout(&tree, area, dpi);
        let hidden: Vec<HWND> = [Part::Refresh, Part::Elevate, Part::CloseBackup]
            .iter()
            .map(|x| p(*x))
            .filter(|h| !ui_visible(*h))
            .collect();
        for (h, r) in &placed {
            if hidden.contains(h) || *h == page {
                continue;
            }
            ui::move_to(*h, *r);
        }
        if let Some((_, r)) = placed.iter().find(|(h, _)| *h == page) {
            for (_, h) in &self.pages {
                ui::move_to(*h, *r);
            }
            ui::move_to(self.browse_page, *r);
        }
    }

    fn command(&self, from: HWND, id: u32, code: u32) {
        if self.sync.active() || code != BN_CLICKED {
            return;
        }
        if from == self.part(Part::Screens) {
            let i = (id as i32 - SCREEN_CMD) as usize;
            if let Some(tab) = AppTab::ALL.get(i) {
                self.apply(AppAction::SelectTab(*tab));
            }
            return;
        }
        let Some(part) = self.parts.iter().find(|(_, h)| *h == from).map(|(p, _)| *p) else {
            return;
        };
        match part {
            Part::CloseBackup => {
                self.ctrl.borrow_mut().close_shared_backup();
                self.refresh();
            }
            Part::Refresh => self.apply(AppAction::RefreshDevices),
            Part::Elevate => self.apply(AppAction::RequestElevation),
            Part::About => note(
                self.window,
                "About Rusty Backup",
                &format!(
                    "{} {}\r\n\r\n{}\r\n\r\n{}\r\n{}",
                    about::NAME,
                    app_version(),
                    about::SUMMARY,
                    about::DEVELOPER,
                    about::WEBSITE
                ),
            ),
            Part::ViewRelease => self.apply(AppAction::ViewRelease),
            Part::Dismiss => self.apply(AppAction::DismissUpdate),
            Part::LogCopy => self.apply(AppAction::CopyLog),
            Part::LogClear => self.apply(AppAction::ClearLog),
            Part::LogAuto => {
                // SAFETY: a plain query of our checkbox.
                let on = unsafe { SendMessageW(from, BM_GETCHECK, None, None).0 } == 1;
                self.apply(AppAction::SetLogAutoScroll(on));
            }
            Part::Commander => self.open_commander(),
            Part::Settings | Part::BannerText | Part::Screens | Part::Status | Part::Log => {}
        }
    }

    /// The system theme may have changed (`WM_SETTINGCHANGE` "ImmersiveColorSet"): follow it.
    fn theme_changed(&self, lp: LPARAM) -> bool {
        // SAFETY: the setting's name, a NUL-terminated string the system owns for this message.
        let name = unsafe {
            (lp.0 != 0)
                .then(|| PCWSTR(lp.0 as *const u16).to_string().ok())
                .flatten()
        };
        if name.as_deref() != Some("ImmersiveColorSet") || !crate::theme::adopt() {
            return false;
        }
        unsafe extern "system" fn each(h: HWND, _: LPARAM) -> windows::core::BOOL {
            crate::theme::restyle(h);
            true.into()
        }
        // SAFETY: re-styling every window of this thread: the main window and its dialogs.
        unsafe {
            let _ = windows::Win32::UI::WindowsAndMessaging::EnumThreadWindows(
                windows::Win32::System::Threading::GetCurrentThreadId(),
                Some(each),
                LPARAM(0),
            );
        }
        let dpi = unsafe { GetDpiForWindow(self.window) }.max(96);
        let names = AppTab::ALL.iter().map(|t| t.icon());
        toolbar::set_icons(self.part(Part::Screens), names, dpi);
        toolbar::set_icons(self.part(Part::Commander), ["commander"].into_iter(), dpi);
        self.inspect.theme_changed();
        if let Some((c, _)) = &*self.commander.borrow() {
            c.set_icons();
        }
        true
    }

    fn dpi_changed(&self, dpi: u32, suggested: &RECT) {
        let font = ui::font_at(dpi);
        // SAFETY: the suggested rectangle comes with WM_DPICHANGED; children get the new font.
        unsafe {
            let _ = SetWindowPos(
                self.window,
                None,
                suggested.left,
                suggested.top,
                suggested.right - suggested.left,
                suggested.bottom - suggested.top,
                SWP_NOZORDER | SWP_NOACTIVATE,
            );
            let _ = EnumChildWindows(Some(self.window), Some(refont), LPARAM(font.0 as isize));
        }
        let names = AppTab::ALL.iter().map(|t| t.icon());
        toolbar::set_icons(self.part(Part::Screens), names, dpi);
        toolbar::set_icons(self.part(Part::Commander), ["commander"].into_iter(), dpi);
        self.inspect.theme_changed();
        if let Some((c, _)) = &*self.commander.borrow() {
            c.set_icons();
        }
        self.relayout();
    }
}

fn ui_visible(h: HWND) -> bool {
    ui::shown(h)
}

unsafe extern "system" fn refont(h: HWND, font: LPARAM) -> windows::core::BOOL {
    ui::set_font(h, windows::Win32::Graphics::Gdi::HFONT(font.0 as _));
    true.into()
}

unsafe extern "system" fn wndproc(hwnd: HWND, msg: u32, wp: WPARAM, lp: LPARAM) -> LRESULT {
    let Some(app) = app() else {
        // SAFETY: before the app exists (WM_CREATE and friends) the default handling is right.
        return unsafe { DefWindowProcW(hwnd, msg, wp, lp) };
    };
    match msg {
        WM_APP_JOB => app.tick(),
        WM_COMMAND => app.command(
            HWND(lp.0 as _),
            (wp.0 & 0xffff) as u32,
            ((wp.0 >> 16) & 0xffff) as u32,
        ),
        WM_SIZE => app.relayout(),
        WM_ERASEBKGND => {
            let mut rc = RECT::default();
            // SAFETY: painting our own window's background with the mode's brush.
            unsafe {
                let _ = GetClientRect(hwnd, &mut rc);
                windows::Win32::Graphics::Gdi::FillRect(
                    windows::Win32::Graphics::Gdi::HDC(wp.0 as _),
                    &rc,
                    crate::theme::window_brush(),
                );
            }
            return LRESULT(1);
        }
        WM_SETTINGCHANGE => {
            if app.theme_changed(lp) {
                return LRESULT(0);
            }
            // SAFETY: other setting changes get the default handling.
            return unsafe { DefWindowProcW(hwnd, msg, wp, lp) };
        }
        WM_NOTIFY => {
            // SAFETY: WM_NOTIFY's lParam is an NMHDR.
            let hdr = unsafe { &*(lp.0 as *const windows::Win32::UI::Controls::NMHDR) };
            if hdr.code == windows::Win32::UI::Controls::NM_CUSTOMDRAW {
                return toolbar::custom_draw(lp);
            }
            // SAFETY: the default handling.
            return unsafe { DefWindowProcW(hwnd, msg, wp, lp) };
        }
        WM_CTLCOLORSTATIC | WM_CTLCOLOREDIT | WM_CTLCOLORBTN | WM_CTLCOLORDLG
        | WM_CTLCOLORLISTBOX => {
            return crate::theme::ctl_color(msg, wp, lp, false).unwrap_or_default();
        }
        WM_DPICHANGED => {
            // SAFETY: WM_DPICHANGED's lParam is the suggested window rectangle.
            let r = unsafe { *(lp.0 as *const RECT) };
            app.dpi_changed((wp.0 & 0xffff) as u32, &r);
        }
        WM_GETMINMAXINFO => {
            // SAFETY: WM_GETMINMAXINFO's lParam is a MINMAXINFO we may fill.
            let info = unsafe { &mut *(lp.0 as *mut MINMAXINFO) };
            let dpi = unsafe { GetDpiForWindow(hwnd) }.max(96);
            info.ptMinTrackSize.x = layout::scale(700, dpi);
            info.ptMinTrackSize.y = layout::scale(460, dpi);
        }
        WM_CLOSE => {
            let commander = app.commander.borrow().as_ref().map(|(c, _)| c.clone());
            if let Some(c) = commander.filter(|c| c.has_staged_edits()) {
                c.hold_quit();
                return LRESULT(0);
            }
            // SAFETY: closing our own window.
            let _ = unsafe { DestroyWindow(hwnd) };
        }
        WM_DROPFILES => {
            let drop = windows::Win32::UI::Shell::HDROP(wp.0 as _);
            let mut paths = Vec::new();
            // SAFETY: the drop handle Windows gave us, read and released here.
            unsafe {
                let n = windows::Win32::UI::Shell::DragQueryFileW(drop, u32::MAX, None);
                for i in 0..n {
                    let len = windows::Win32::UI::Shell::DragQueryFileW(drop, i, None) as usize;
                    let mut buf = vec![0u16; len + 1];
                    windows::Win32::UI::Shell::DragQueryFileW(drop, i, Some(&mut buf));
                    paths.push(std::path::PathBuf::from(String::from_utf16_lossy(
                        &buf[..len],
                    )));
                }
            }
            // The drop point, in screen pixels, says which of Commander's panes it landed on.
            let mut at = windows::Win32::Foundation::POINT::default();
            // SAFETY: the same drop handle, released once read.
            unsafe {
                let _ = windows::Win32::UI::Shell::DragQueryPoint(drop, &mut at);
                let _ = windows::Win32::Graphics::Gdi::ClientToScreen(hwnd, &mut at);
                windows::Win32::UI::Shell::DragFinish(drop);
            }
            app.dropped(paths, at);
        }
        WM_DESTROY => {
            APP.with(|a| a.borrow_mut().take());
            // SAFETY: ends the message loop.
            unsafe { PostQuitMessage(0) };
        }
        // SAFETY: everything else gets the default handling.
        _ => return unsafe { DefWindowProcW(hwnd, msg, wp, lp) },
    }
    LRESULT(0)
}
