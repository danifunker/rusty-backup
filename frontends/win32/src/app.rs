//! The application window: top bar, tabs, update banner, status line and log, drawn from `AppController`.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use rusty_backup::controller::app::{
    about, app_version, elevation_button_hover, elevation_button_label, elevation_confirm,
    elevation_gate, ids, AppAction, AppController, AppTab, ElevationGate,
};
use rusty_backup::controller::log::capture;
use rusty_backup::controller::{Controller, Effect};
use windows::core::{w, PCWSTR, PWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, RECT, WPARAM};
use windows::Win32::System::LibraryLoader::GetModuleHandleW;
use windows::Win32::UI::Controls::{
    EM_GETFIRSTVISIBLELINE, EM_LINESCROLL, EM_REPLACESEL, EM_SCROLLCARET, EM_SETLIMITTEXT,
    EM_SETSEL, NMHDR, TCIF_TEXT, TCITEMW, TCM_ADJUSTRECT, TCM_GETCURSEL, TCM_INSERTITEMW,
    TCM_SETCURSEL, TCN_SELCHANGE, TCS_FOCUSNEVER,
};
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::WindowsAndMessaging::{
    CreateWindowExW, DefWindowProcW, DestroyWindow, EnumChildWindows, GetClientRect, LoadCursorW,
    LoadIconW, PostQuitMessage, RegisterClassExW, SendMessageW, SetWindowPos, BM_GETCHECK,
    BM_SETCHECK, BN_CLICKED, CS_HREDRAW, CS_VREDRAW, CW_USEDEFAULT, ES_AUTOVSCROLL, ES_MULTILINE,
    ES_READONLY, HMENU, IDC_ARROW, MINMAXINFO, SWP_NOACTIVATE, SWP_NOZORDER, WINDOW_EX_STYLE,
    WINDOW_STYLE, WM_CLOSE, WM_COMMAND, WM_DESTROY, WM_DPICHANGED, WM_GETMINMAXINFO, WM_NOTIFY,
    WM_SIZE, WNDCLASSEXW, WS_CLIPCHILDREN, WS_EX_CLIENTEDGE, WS_EX_CONTROLPARENT,
    WS_OVERLAPPEDWINDOW, WS_TABSTOP, WS_VSCROLL,
};

use crate::effects::{note, EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Rect, Size};
use crate::sync::SyncGuard;
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
    Tabs,
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
    /// One page per tab, shown inside the tab control; the screens fill them in later slices.
    pages: Vec<(AppTab, HWND)>,
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
        let commander = add(
            Part::Commander,
            ui::button(window, "Commander Mode", ids::COMMANDER),
        );
        // Commander arrives in slice 3 and Settings in 7b; until then their buttons are shown but off.
        ui::set_enabled(commander, false);
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
        let tabs = add(
            Part::Tabs,
            ui::child(
                window,
                w!("SysTabControl32"),
                "",
                WINDOW_STYLE(TCS_FOCUSNEVER | WS_TABSTOP.0),
                WINDOW_EX_STYLE(0),
                "app.tabs",
            ),
        );
        for (i, tab) in AppTab::ALL.iter().enumerate() {
            let mut text = ui::wide(tab.label());
            let item = TCITEMW {
                mask: TCIF_TEXT,
                pszText: PWSTR(text.as_mut_ptr()),
                ..Default::default()
            };
            // SAFETY: `text` outlives the call; the control copies it.
            unsafe {
                SendMessageW(
                    tabs,
                    TCM_INSERTITEMW,
                    Some(WPARAM(i)),
                    Some(LPARAM(&item as *const _ as isize)),
                );
            }
        }
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
        let pages = AppTab::ALL
            .iter()
            .map(|t| {
                let text = format!(
                    "The {} screen arrives in a later slice of the Win32 shell.",
                    t.label()
                );
                let page = ui::label(window, &text);
                ui::set_visible(page, false);
                (*t, page)
            })
            .collect();
        let host = EffectHost::new(window, status);
        ctrl.start_update_check();
        Rc::new(App {
            ctrl: RefCell::new(ctrl),
            wake,
            host,
            window,
            parts,
            pages,
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
        self.refresh();
    }

    /// Read the controller first and release it, then set controls inside the guard.
    fn refresh(&self) {
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
            let tabs = self.part(Part::Tabs);
            let want = AppTab::ALL.iter().position(|t| *t == tab).unwrap_or(0);
            // SAFETY: plain control messages; TCM_SETCURSEL sends no TCN_SELCHANGE.
            unsafe {
                if SendMessageW(tabs, TCM_GETCURSEL, None, None).0 as usize != want {
                    SendMessageW(tabs, TCM_SETCURSEL, Some(WPARAM(want)), None);
                }
                let on = if auto { 1 } else { 0 };
                SendMessageW(
                    self.part(Part::LogAuto),
                    BM_SETCHECK,
                    Some(WPARAM(on)),
                    None,
                );
            }
            for (t, page) in &self.pages {
                ui::set_visible(*page, *t == tab);
            }
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
        let banner = ui_visible(self.part(Part::BannerText));
        let p = |x: Part| self.part(x);
        let mut rows = vec![Node::Row {
            children: vec![
                Node::button(p(Part::Commander), 120),
                Node::Space(Size::Stretch(1)),
                Node::button(p(Part::CloseBackup), 100),
                Node::button(p(Part::Refresh), 110),
                Node::button(p(Part::Elevate), 190),
                Node::button(p(Part::Settings), 85),
                Node::button(p(Part::About), 130),
            ],
            gap: layout::GAP,
        }];
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
        rows.push(Node::leaf(
            p(Part::Tabs),
            Size::Stretch(1),
            Size::Stretch(3),
        ));
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
            if hidden.contains(h) {
                continue;
            }
            move_to(*h, *r);
        }
        if let Some((_, r)) = placed.iter().find(|(h, _)| *h == p(Part::Tabs)) {
            let mut inner = RECT {
                left: r.x,
                top: r.y,
                right: r.x + r.w,
                bottom: r.y + r.h,
            };
            // SAFETY: TCM_ADJUSTRECT fills the rectangle we pass.
            unsafe {
                SendMessageW(
                    p(Part::Tabs),
                    TCM_ADJUSTRECT,
                    Some(WPARAM(0)),
                    Some(LPARAM(&mut inner as *mut _ as isize)),
                );
            }
            let m = layout::scale(layout::MARGIN, dpi);
            let page = Rect {
                x: inner.left + m,
                y: inner.top + m,
                w: (inner.right - inner.left - 2 * m).max(0),
                h: (inner.bottom - inner.top - 2 * m).max(0),
            };
            for (_, h) in &self.pages {
                move_to(*h, page);
                // SAFETY: pages sit above the tab control in z-order, so they draw over it.
                let _ = unsafe {
                    SetWindowPos(
                        *h,
                        Some(windows::Win32::UI::WindowsAndMessaging::HWND_TOP),
                        0,
                        0,
                        0,
                        0,
                        windows::Win32::UI::WindowsAndMessaging::SWP_NOMOVE
                            | windows::Win32::UI::WindowsAndMessaging::SWP_NOSIZE
                            | SWP_NOACTIVATE,
                    )
                };
            }
        }
    }

    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() || code != BN_CLICKED {
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
            Part::Commander
            | Part::Settings
            | Part::BannerText
            | Part::Tabs
            | Part::Status
            | Part::Log => {}
        }
    }

    fn tab_changed(&self) {
        if self.sync.active() {
            return;
        }
        // SAFETY: a plain query of our tab control.
        let i = unsafe { SendMessageW(self.part(Part::Tabs), TCM_GETCURSEL, None, None).0 };
        if let Some(tab) = usize::try_from(i).ok().and_then(|i| AppTab::ALL.get(i)) {
            self.apply(AppAction::SelectTab(*tab));
        }
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
        self.relayout();
    }
}

fn ui_visible(h: HWND) -> bool {
    ui::shown(h)
}

fn move_to(h: HWND, r: Rect) {
    // SAFETY: moving our own child control.
    let _ = unsafe { SetWindowPos(h, None, r.x, r.y, r.w, r.h, SWP_NOZORDER | SWP_NOACTIVATE) };
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
        WM_COMMAND => app.command(HWND(lp.0 as _), ((wp.0 >> 16) & 0xffff) as u32),
        WM_NOTIFY => {
            // SAFETY: WM_NOTIFY's lParam is an NMHDR.
            let hdr = unsafe { &*(lp.0 as *const NMHDR) };
            if hdr.hwndFrom == app.part(Part::Tabs) && hdr.code == TCN_SELCHANGE {
                app.tab_changed();
            }
        }
        WM_SIZE => app.relayout(),
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
            // SAFETY: closing our own window.
            let _ = unsafe { DestroyWindow(hwnd) };
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
