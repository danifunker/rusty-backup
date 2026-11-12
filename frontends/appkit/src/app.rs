//! The application window: the screen toolbar, top bar, update banner, a page per screen, the
//! status line and the log, drawn from `AppController`.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use objc2::rc::Retained;
use objc2::runtime::{NSObject, NSObjectProtocol, ProtocolObject};
use objc2::{define_class, msg_send, sel, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSApplication, NSApplicationActivationPolicy, NSApplicationDelegate, NSBackingStoreType,
    NSButton, NSColor, NSControlStateValueOff, NSControlStateValueOn, NSFont, NSScrollView,
    NSTextField, NSTextView, NSToolbar, NSView, NSWindow, NSWindowDelegate, NSWindowStyleMask,
    NSWindowToolbarStyle,
};
use objc2_foundation::{NSNotification, NSPoint, NSRange, NSRect, NSSize};
use rusty_backup::controller::app::{
    about, app_version, elevation_button_hover, elevation_button_label, elevation_confirm,
    elevation_gate, ids, AppAction, AppController, AppTab, ElevationGate, InstallState,
};
use rusty_backup::controller::log::{capture, LogLevel};
use rusty_backup::controller::{Controller, Effect};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::sync::SyncGuard;
use crate::ui::{self, ns};
use crate::wake::MacWake;

/// What each top-level view is, for layout.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Part {
    CloseBackup,
    Refresh,
    Elevate,
    Settings,
    About,
    BannerText,
    Install,
    Restart,
    Retry,
    ViewRelease,
    Dismiss,
    /// Every page takes this place; the one for the screen on show is visible.
    Page,
    Status,
    LogCopy,
    LogClear,
    LogAuto,
    Log,
}

/// A screen's page: a scroll view over a flipped body, so a short window scrolls, never clips.
struct Page {
    scroll: Retained<NSScrollView>,
    body: Retained<NSView>,
    /// What the page lays out; a screen builds its own from slice 1 on.
    note: Retained<NSTextField>,
}

impl Page {
    fn new(mtm: MainThreadMarker, tab: AppTab) -> Page {
        let scroll = NSScrollView::initWithFrame(mtm.alloc(), NSRect::ZERO);
        scroll.setHasVerticalScroller(true);
        scroll.setAutohidesScrollers(true);
        scroll.setDrawsBackground(false);
        let body = ui::flipped(mtm);
        scroll.setDocumentView(Some(&body));
        let note = ui::label(
            mtm,
            &format!("{} arrives in slice {}.", tab.label(), slice(tab)),
        );
        body.addSubview(&note);
        Page { scroll, body, note }
    }

    /// Lay the body out at the scroll view's width; taller than the view, it scrolls.
    fn relayout(&self) {
        let tree = Node::inset(Node::field(0usize), layout::MARGIN);
        let visible = self.scroll.contentSize();
        let h = (visible.height as i32).max(tree.min_height());
        let w = visible.width as i32;
        let size = NSSize::new(f64::from(w), f64::from(h));
        if self.body.frame().size != size {
            self.body.setFrameSize(size);
        }
        let area = layout::Rect { x: 0, y: 0, w, h };
        for (_, r) in layout::layout(&tree, area, 96) {
            ui::move_to(&self.note, r);
        }
    }
}

/// Which slice brings each screen (docs/appkit_shell_plan.md).
fn slice(tab: AppTab) -> u32 {
    match tab {
        AppTab::Inspect => 1,
        AppTab::Backup | AppTab::Restore => 4,
        AppTab::Build => 5,
        AppTab::Optical | AppTab::Archives => 6,
        AppTab::Convert => 7,
    }
}

pub struct App {
    ctrl: RefCell<AppController>,
    wake: Arc<MacWake>,
    host: EffectHost,
    window: Retained<NSWindow>,
    content: Retained<NSView>,
    toolbar: Retained<NSToolbar>,
    parts: Vec<(Part, Retained<NSView>)>,
    banner: Retained<NSTextField>,
    log_auto: Retained<NSButton>,
    log: Retained<NSTextView>,
    pages: Vec<(AppTab, Page)>,
    log_shown: Cell<usize>,
    banner_shape: RefCell<Vec<bool>>,
    sync: SyncGuard,
}

thread_local! {
    static APP: RefCell<Option<Rc<App>>> = const { RefCell::new(None) };
    static KEEP: RefCell<Vec<Retained<NSObject>>> = const { RefCell::new(Vec::new()) };
    static DELEGATE: RefCell<Option<Retained<Delegate>>> = const { RefCell::new(None) };
}

fn app() -> Option<Rc<App>> {
    APP.with(|a| a.borrow().clone())
}

/// Hold an object for the app's lifetime (delegates, which AppKit keeps weakly).
fn keep(o: Retained<NSObject>) {
    KEEP.with(|k| k.borrow_mut().push(o));
}

define_class!(
    /// The application's and the main window's delegate.
    #[unsafe(super(NSObject))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBAppDelegate"]
    struct Delegate;

    unsafe impl NSObjectProtocol for Delegate {}

    unsafe impl NSApplicationDelegate for Delegate {
        #[unsafe(method(applicationDidFinishLaunching:))]
        fn did_finish_launching(&self, _note: &NSNotification) {
            start(self.mtm());
        }

        #[unsafe(method(applicationShouldTerminateAfterLastWindowClosed:))]
        fn last_window_closed(&self, _app: &NSApplication) -> bool {
            true
        }
    }

    unsafe impl NSWindowDelegate for Delegate {
        #[unsafe(method(windowDidResize:))]
        fn did_resize(&self, _note: &NSNotification) {
            if let Some(app) = app() {
                app.relayout();
            }
        }
    }
);

/// Run the application until it quits.
pub fn run() {
    let mtm = MainThreadMarker::new().expect("main runs on the main thread");
    let ns_app = NSApplication::sharedApplication(mtm);
    ns_app.setActivationPolicy(NSApplicationActivationPolicy::Regular);
    let delegate: Retained<Delegate> = {
        let this = mtm.alloc::<Delegate>().set_ivars(());
        // SAFETY: NSObject's plain init.
        unsafe { msg_send![super(this), init] }
    };
    ns_app.setDelegate(Some(ProtocolObject::from_ref(&*delegate)));
    DELEGATE.with(|d| *d.borrow_mut() = Some(delegate));
    crate::menu::build(mtm);
    ns_app.run();
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

fn start(mtm: MainThreadMarker) {
    let wake = Arc::new(MacWake::default());
    capture::set_wake(wake.clone());
    let style = NSWindowStyleMask::Titled
        | NSWindowStyleMask::Closable
        | NSWindowStyleMask::Miniaturizable
        | NSWindowStyleMask::Resizable;
    let frame = NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(1200.0, 800.0));
    // SAFETY: the designated initialiser; the window is kept by `App`.
    let window = unsafe {
        NSWindow::initWithContentRect_styleMask_backing_defer(
            mtm.alloc(),
            frame,
            style,
            NSBackingStoreType::Buffered,
            false,
        )
    };
    // SAFETY: `App` owns the window, so AppKit must not release it on close.
    unsafe { window.setReleasedWhenClosed(false) };
    window.setTitle(&ns("Rusty Backup (AppKit preview)"));
    window.setContentMinSize(NSSize::new(700.0, 460.0));
    let (toolbar, toolbar_delegate) = crate::toolbar::build(mtm);
    keep(Retained::into_super(toolbar_delegate));
    // macOS 11's tall toolbar shows icons with labels; 10.13 to 10.15 lack the call.
    if window.respondsToSelector(sel!(setToolbarStyle:)) {
        window.setToolbarStyle(NSWindowToolbarStyle::Expanded);
    }
    window.setToolbar(Some(&toolbar));
    let content = ui::flipped(mtm);
    window.setContentView(Some(&content));
    let app = App::build(mtm, window, content, toolbar, wake);
    DELEGATE.with(|d| {
        if let Some(delegate) = &*d.borrow() {
            app.window
                .setDelegate(Some(ProtocolObject::from_ref(&**delegate)));
        }
    });
    APP.with(|a| *a.borrow_mut() = Some(app.clone()));
    app.window.center();
    app.window.makeKeyAndOrderFront(None);
    NSApplication::sharedApplication(mtm).activate();
    app.relayout();
    app.tick();
}

/// Run the controllers now, as a wake does.
pub fn tick() {
    if let Some(app) = app() {
        app.tick();
    }
}

/// An action from a control, menu item or toolbar item, by its controller id.
pub fn command(id: &str) {
    if let Some(app) = app() {
        app.command(id);
    }
}

/// Add a screen's drained log lines to the log panel.
#[allow(dead_code)] // the screens drain their logs from slice 1 on
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

impl App {
    fn build(
        mtm: MainThreadMarker,
        window: Retained<NSWindow>,
        content: Retained<NSView>,
        toolbar: Retained<NSToolbar>,
        wake: Arc<MacWake>,
    ) -> Rc<App> {
        let mut ctrl = AppController::new(wake.clone());
        ctrl.load_devices();
        let gate = elevation_gate();
        let mut parts: Vec<(Part, Retained<NSView>)> = Vec::new();
        let mut add = |p: Part, v: Retained<NSView>| {
            content.addSubview(&v);
            parts.push((p, v.clone()));
            v
        };
        let into = |b: Retained<NSButton>| -> Retained<NSView> {
            Retained::into_super(Retained::into_super(b))
        };
        let close = add(
            Part::CloseBackup,
            into(ui::button(mtm, "Close Backup", ids::CLOSE_BACKUP)),
        );
        close.setHidden(true);
        let refresh = add(
            Part::Refresh,
            into(ui::button(mtm, "Refresh Devices", ids::REFRESH_DEVICES)),
        );
        refresh.setHidden(!matches!(
            gate,
            ElevationGate::NotApplicable | ElevationGate::Elevated
        ));
        let elevate = ui::button(mtm, elevation_button_label(), ids::ELEVATE);
        elevate.setToolTip(Some(&ns(elevation_button_hover())));
        let elevate = add(Part::Elevate, into(elevate));
        elevate.setHidden(gate != ElevationGate::NeedsElevation);
        // Settings arrives in slice 7b; until then it shows but is off.
        let settings = ui::button(mtm, "Settings...", ids::SETTINGS);
        settings.setEnabled(false);
        add(Part::Settings, into(settings));
        add(
            Part::About,
            into(ui::button(mtm, "About Rusty Backup", ids::ABOUT)),
        );
        let banner = ui::label(mtm, "");
        add(
            Part::BannerText,
            Retained::into_super(Retained::into_super(banner.clone())),
        );
        add(
            Part::Install,
            into(ui::button(
                mtm,
                "Download & Install Update",
                ids::UPDATE_INSTALL,
            )),
        );
        add(
            Part::Restart,
            into(ui::button(mtm, "Restart Now", ids::UPDATE_RESTART)),
        );
        add(
            Part::Retry,
            into(ui::button(mtm, "Retry", ids::UPDATE_RETRY)),
        );
        add(
            Part::ViewRelease,
            into(ui::button(mtm, "View Release", ids::UPDATE_VIEW)),
        );
        add(
            Part::Dismiss,
            into(ui::button(
                mtm,
                "Dismiss update notification",
                ids::UPDATE_DISMISS,
            )),
        );
        let status = ui::label(mtm, "");
        add(
            Part::Status,
            Retained::into_super(Retained::into_super(status.clone())),
        );
        add(
            Part::LogCopy,
            into(ui::button(mtm, "Copy Log", ids::LOG_COPY)),
        );
        add(
            Part::LogClear,
            into(ui::button(mtm, "Clear Log", ids::LOG_CLEAR)),
        );
        let log_auto = ui::checkbox(mtm, "Auto-scroll", ids::LOG_AUTO_SCROLL);
        add(Part::LogAuto, into(log_auto.clone()));
        let (log_scroll, log) = log_view(mtm);
        add(Part::Log, Retained::into_super(log_scroll.clone()));
        let page_place = ui::flipped(mtm);
        add(Part::Page, page_place);
        let pages: Vec<(AppTab, Page)> = AppTab::ALL
            .iter()
            .map(|t| {
                let page = Page::new(mtm, *t);
                content.addSubview(&page.scroll);
                page.scroll.setHidden(true);
                (*t, page)
            })
            .collect();
        for (p, v) in &parts {
            if matches!(
                p,
                Part::BannerText
                    | Part::Install
                    | Part::Restart
                    | Part::Retry
                    | Part::ViewRelease
                    | Part::Dismiss
            ) {
                v.setHidden(true);
            }
        }
        let host = EffectHost::new(mtm, status);
        ctrl.start_update_check();
        Rc::new(App {
            ctrl: RefCell::new(ctrl),
            wake,
            host,
            window,
            content,
            toolbar,
            parts,
            banner,
            log_auto,
            log,
            pages,
            log_shown: Cell::new(0),
            banner_shape: RefCell::default(),
            sync: SyncGuard::default(),
        })
    }

    fn part(&self, p: Part) -> &NSView {
        self.parts
            .iter()
            .find(|(q, _)| *q == p)
            .map(|(_, v)| &**v)
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

    fn command(&self, id: &str) {
        if self.sync.active() {
            return;
        }
        if let Some(tab) = crate::toolbar::tab_of(id) {
            self.apply(AppAction::SelectTab(tab));
            return;
        }
        match id {
            ids::CLOSE_BACKUP => {
                self.ctrl.borrow_mut().close_shared_backup();
                append_log(vec![(LogLevel::Info, "Backup closed".to_string())]);
                self.refresh();
            }
            ids::REFRESH_DEVICES => self.apply(AppAction::RefreshDevices),
            ids::ELEVATE => self.apply(AppAction::RequestElevation),
            ids::ABOUT => self.about(),
            ids::UPDATE_VIEW => self.apply(AppAction::ViewRelease),
            ids::UPDATE_INSTALL => self.apply(AppAction::InstallUpdate),
            ids::UPDATE_RESTART => self.apply(AppAction::RestartApp),
            ids::UPDATE_RETRY => self.apply(AppAction::RetryUpdate),
            ids::UPDATE_CHECK => self.apply(AppAction::CheckForUpdates),
            ids::UPDATE_DISMISS => self.apply(AppAction::DismissUpdate),
            ids::LOG_COPY => self.apply(AppAction::CopyLog),
            ids::LOG_CLEAR => self.apply(AppAction::ClearLog),
            ids::LOG_AUTO_SCROLL => {
                let on = self.log_auto.state() == NSControlStateValueOn;
                self.apply(AppAction::SetLogAutoScroll(on));
            }
            crate::menu::WEBSITE => self
                .host
                .run(vec![Effect::OpenUrl(about::WEBSITE.to_string())], &reply()),
            _ => {}
        }
    }

    /// The controller's About text as a note; `adw::AboutDialog` and Win32's note say the same.
    fn about(&self) {
        crate::effects::note(
            format!("About {}", about::NAME),
            format!(
                "{} {}\n\n{}\n\n{}\n{}",
                about::NAME,
                app_version(),
                about::SUMMARY,
                about::DEVELOPER,
                about::WEBSITE
            ),
        );
    }

    /// Read the controller first and release it, then set views inside the guard.
    fn refresh(&self) {
        let (tab, shared, banner, auto) = {
            let c = self.ctrl.borrow();
            (
                c.active_tab(),
                c.shared_backup().is_some(),
                banner_of(&c),
                c.log_auto_scroll(),
            )
        };
        let banner_was = ui::shown(self.part(Part::BannerText));
        let shared_was = ui::shown(self.part(Part::CloseBackup));
        self.sync.run(|| {
            ui::set_visible(self.part(Part::CloseBackup), shared);
            let shows = |p: Part| banner.as_ref().is_some_and(|b| b.buttons.contains(&p));
            ui::set_visible(self.part(Part::BannerText), banner.is_some());
            for p in [
                Part::Install,
                Part::Restart,
                Part::Retry,
                Part::ViewRelease,
                Part::Dismiss,
            ] {
                ui::set_visible(self.part(p), shows(p));
            }
            if let Some(b) = &banner {
                ui::set_text(&self.banner, &b.text);
            }
            crate::toolbar::select(&self.toolbar, tab);
            let state = if auto {
                NSControlStateValueOn
            } else {
                NSControlStateValueOff
            };
            if self.log_auto.state() != state {
                self.log_auto.setState(state);
            }
            for (t, page) in &self.pages {
                ui::set_visible(&page.scroll, *t == tab);
            }
        });
        if banner_was != banner.is_some() || shared_was != shared || self.banner_buttons_moved() {
            self.relayout();
        }
        self.sync_log(auto);
    }

    /// Append the log lines not shown yet; a cleared log starts over.
    fn sync_log(&self, auto: bool) {
        let text = {
            let c = self.ctrl.borrow();
            let entries = c.log.entries();
            if entries.len() < self.log_shown.get() {
                self.log.setString(&ns(""));
                self.log_shown.set(0);
            }
            let mut text = String::new();
            for e in &entries[self.log_shown.get()..] {
                text.push_str(&ui::plain(&e.line()));
                text.push('\n');
            }
            self.log_shown.set(entries.len());
            text
        };
        if text.is_empty() {
            return;
        }
        // SAFETY: the text view's own storage, changed on the main thread.
        unsafe {
            if let Some(storage) = self.log.textStorage() {
                storage.mutableString().appendString(&ns(&text));
            }
        }
        // Appended text has no attributes of its own: give it the log's colour and font.
        self.log.setTextColor(Some(&NSColor::textColor()));
        self.log.setFont(Some(&log_font()));
        if auto {
            let end = self.log.string().length();
            self.log.scrollRangeToVisible(NSRange::new(end, 0));
        }
    }

    /// Which banner buttons show changed since the last layout, so the row must be laid out again.
    fn banner_buttons_moved(&self) -> bool {
        let now: Vec<bool> = [Part::Install, Part::Restart, Part::Retry, Part::ViewRelease]
            .iter()
            .map(|p| ui::shown(self.part(*p)))
            .collect();
        self.banner_shape.replace(now.clone()) != now
    }

    /// Place every view for the window's content area.
    fn relayout(&self) {
        let p = |x: Part| x;
        let rows = vec![
            Node::Row {
                children: vec![
                    Node::Space(Size::Stretch(1)),
                    Node::button(p(Part::CloseBackup), 120),
                    Node::button(p(Part::Refresh), 140),
                    Node::button(p(Part::Elevate), 220),
                    Node::button(p(Part::Settings), 100),
                    Node::button(p(Part::About), 160),
                ],
                gap: layout::GAP,
            },
            Node::row(vec![
                Node::leaf(
                    p(Part::BannerText),
                    Size::Stretch(1),
                    Size::Fixed(layout::ROW - 4),
                ),
                Node::button(p(Part::Install), 210),
                Node::button(p(Part::Restart), 120),
                Node::button(p(Part::Retry), 80),
                Node::button(p(Part::ViewRelease), 120),
                Node::button(p(Part::Dismiss), 230),
            ]),
            Node::leaf(p(Part::Page), Size::Stretch(1), Size::Stretch(3)),
            Node::leaf(
                p(Part::Status),
                Size::Stretch(1),
                Size::Fixed(layout::ROW - 4),
            ),
            Node::row(vec![
                Node::button(p(Part::LogCopy), 100),
                Node::button(p(Part::LogClear), 100),
                Node::button(p(Part::LogAuto), 110),
                Node::Space(Size::Stretch(1)),
            ]),
            Node::leaf(p(Part::Log), Size::Stretch(1), Size::Stretch(1)),
        ];
        let tree = Node::inset(Node::column(rows), layout::MARGIN);
        // Hidden views take no room (the Win32 shell's R-095 lesson); the page place always shows.
        let keep = |x: &Part| *x == Part::Page || ui::shown(self.part(*x));
        let Some(tree) = tree.prune(&keep) else {
            return;
        };
        let placed = layout::layout(&tree, ui::area(&self.content), 96);
        for (part, r) in &placed {
            if *part == Part::Page {
                for (_, page) in &self.pages {
                    ui::move_to(&page.scroll, *r);
                    page.relayout();
                }
            }
            ui::move_to(self.part(*part), *r);
        }
    }
}

/// What the update banner says, and which of its buttons show.
struct Banner {
    text: String,
    buttons: Vec<Part>,
}

/// The banner from the app controller: the announced release, then the in-app install's state.
fn banner_of(c: &AppController) -> Option<Banner> {
    let announced = c.update_banner_text();
    let state = c.install_state();
    if announced.is_none() && state.is_none() {
        return None;
    }
    let (text, buttons) = match state {
        None if c.can_install_update() => (
            format!("Update: {}", announced.unwrap_or_default()),
            vec![Part::Install, Part::ViewRelease, Part::Dismiss],
        ),
        None => (
            format!("Update: {}", announced.unwrap_or_default()),
            vec![Part::ViewRelease, Part::Dismiss],
        ),
        Some(InstallState::Downloading(f)) => (
            match f {
                Some(f) => format!("Downloading update... {:.0}%", f * 100.0),
                None => "Downloading update...".to_string(),
            },
            Vec::new(),
        ),
        Some(InstallState::Ready) => ("Update installed.".to_string(), vec![Part::Restart]),
        Some(InstallState::Manual(why)) => (
            format!("Update: {why}"),
            vec![Part::ViewRelease, Part::Dismiss],
        ),
        Some(InstallState::Failed(e)) => (
            format!("Update failed: {e}"),
            vec![Part::Retry, Part::ViewRelease],
        ),
    };
    Some(Banner { text, buttons })
}

fn log_font() -> Retained<NSFont> {
    NSFont::userFixedPitchFontOfSize(11.0).unwrap_or_else(|| NSFont::systemFontOfSize(11.0))
}

/// The log: a read-only text view in a scroll view (built by hand: the helper needs 10.14).
fn log_view(mtm: MainThreadMarker) -> (Retained<NSScrollView>, Retained<NSTextView>) {
    let frame = NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(400.0, 120.0));
    let scroll = NSScrollView::initWithFrame(mtm.alloc(), frame);
    scroll.setHasVerticalScroller(true);
    scroll.setBorderType(objc2_app_kit::NSBorderType::BezelBorder);
    let text = NSTextView::initWithFrame(mtm.alloc(), frame);
    text.setEditable(false);
    text.setSelectable(true);
    text.setVerticallyResizable(true);
    text.setHorizontallyResizable(false);
    text.setAutoresizingMask(objc2_app_kit::NSAutoresizingMaskOptions::ViewWidthSizable);
    text.setMaxSize(NSSize::new(f64::MAX, f64::MAX));
    if let Some(container) = unsafe { text.textContainer() } {
        container.setWidthTracksTextView(true);
    }
    text.setFont(Some(&log_font()));
    ui::set_id(&text, ids::LOG_LIST);
    scroll.setDocumentView(Some(&text));
    (scroll, text)
}
