//! The application window: the screen toolbar, top bar, update banner, a page per screen, the
//! status line and the log, drawn from `AppController`.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use dispatch2::{DispatchQueue, DispatchTime};
use objc2::rc::Retained;
use objc2::runtime::{NSObject, NSObjectProtocol, ProtocolObject};
use objc2::{define_class, msg_send, sel, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSApplication, NSApplicationActivationPolicy, NSApplicationDelegate, NSBackingStoreType,
    NSButton, NSColor, NSControlStateValueOff, NSControlStateValueOn, NSFont, NSScrollView,
    NSTextField, NSTextView, NSToolbar, NSView, NSWindow, NSWindowDelegate, NSWindowStyleMask,
    NSWindowToolbarStyle,
};
use objc2_foundation::{NSArray, NSNotification, NSPoint, NSRange, NSRect, NSSize, NSURL};
use rusty_backup::controller::app::{
    about, app_version, elevation_button_hover, elevation_button_label, elevation_confirm,
    elevation_gate, ids, AppAction, AppController, AppTab, ElevationGate, InstallState,
};
use rusty_backup::controller::log::{capture, LogLevel};
use rusty_backup::controller::{Controller, Effect};

use crate::archives::ArchivesScreen;
use crate::backup::BackupScreen;
use crate::build_disk::BuildScreen;
use crate::commander::CommanderScreen;
use crate::convert::ConvertScreen;
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::inspect::InspectScreen;
use crate::layout::{self, Node, Size};
use crate::optical::OpticalScreen;
use crate::restore::RestoreScreen;
use crate::settings::SettingsWindow;
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
}

impl Page {
    fn new(mtm: MainThreadMarker) -> Page {
        let scroll = NSScrollView::initWithFrame(mtm.alloc(), NSRect::ZERO);
        scroll.setHasVerticalScroller(true);
        scroll.setAutohidesScrollers(true);
        scroll.setDrawsBackground(false);
        let body = ui::flipped(mtm);
        scroll.setDocumentView(Some(&body));
        Page { scroll, body }
    }
}

/// Lay `tree` out in a page's body at its scroll view's width: taller than the view, it scrolls.
pub fn place_page<T: Copy>(body: &NSView, tree: &Node<T>, set: &dyn Fn(T, layout::Rect)) {
    let Some(scroll) = body.enclosingScrollView() else {
        return;
    };
    let visible = scroll.contentSize();
    let h = (visible.height as i32).max(tree.min_height());
    let w = visible.width as i32;
    let size = NSSize::new(f64::from(w), f64::from(h));
    if body.frame().size != size {
        body.setFrameSize(size);
    }
    let area = layout::Rect { x: 0, y: 0, w, h };
    for (id, r) in layout::layout(tree, area, 96) {
        set(id, r);
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
    inspect: Rc<InspectScreen>,
    backup: Rc<BackupScreen>,
    restore: Rc<RestoreScreen>,
    build: Rc<BuildScreen>,
    archives: Rc<ArchivesScreen>,
    optical: Rc<OpticalScreen>,
    convert: Rc<ConvertScreen>,
    settings: RefCell<Option<Rc<SettingsWindow>>>,
    /// A tick is queued for the playing track's position.
    player_tick: Cell<bool>,
    /// Commander, while it is open, over the whole window.
    commander: RefCell<Option<Rc<CommanderScreen>>>,
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

        #[unsafe(method(application:openURLs:))]
        fn open_urls(&self, _app: &NSApplication, urls: &NSArray<NSURL>) {
            let paths: Vec<std::path::PathBuf> = urls
                .iter()
                .filter_map(|u| u.path())
                .map(|p| std::path::PathBuf::from(p.to_string()))
                .collect();
            open_later(paths);
        }

        #[unsafe(method(applicationShouldTerminateAfterLastWindowClosed:))]
        fn last_window_closed(&self, _app: &NSApplication) -> bool {
            true
        }
    }

    unsafe impl NSWindowDelegate for Delegate {
        #[unsafe(method(windowShouldClose:))]
        fn should_close(&self, _sender: &NSWindow) -> bool {
            // Commander's staged edits would be lost: its controller asks, and may quit after.
            let held = app().and_then(|a| a.commander.borrow().clone());
            match held.filter(|c| c.has_staged_edits()) {
                Some(c) => {
                    c.hold_quit();
                    false
                }
                None => true,
            }
        }

        #[unsafe(method(windowDidResize:))]
        fn did_resize(&self, _note: &NSNotification) {
            if let Some(app) = app() {
                app.relayout();
            }
        }
    }
);

thread_local! {
    /// Files asked for before the app was built: opened once it is.
    static PENDING: RefCell<Vec<std::path::PathBuf>> = const { RefCell::new(Vec::new()) };
}

/// Open `paths` in Inspect now, or once the app is built.
fn open_later(paths: Vec<std::path::PathBuf>) {
    match app() {
        Some(app) => {
            for p in paths {
                app.open_file(p);
            }
        }
        None => PENDING.with(|q| q.borrow_mut().extend(paths)),
    }
}

/// `RUSTY_BACKUP_APPEARANCE=light|dark` pins the look, so a screenshot tour can shoot both.
fn force_appearance(ns_app: &NSApplication) {
    use objc2_app_kit::{NSAppearance, NSAppearanceNameAqua, NSAppearanceNameDarkAqua};
    let want = std::env::var("RUSTY_BACKUP_APPEARANCE").unwrap_or_default();
    // SAFETY: AppKit's own appearance name constants.
    let name = unsafe {
        match want.as_str() {
            "dark" => NSAppearanceNameDarkAqua,
            "light" => NSAppearanceNameAqua,
            _ => return,
        }
    };
    ns_app.setAppearance(NSAppearance::appearanceNamed(name).as_deref());
}

/// Run the application until it quits.
pub fn run() {
    let mtm = MainThreadMarker::new().expect("main runs on the main thread");
    let ns_app = NSApplication::sharedApplication(mtm);
    ns_app.setActivationPolicy(NSApplicationActivationPolicy::Regular);
    force_appearance(&ns_app);
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
    let drop = ui::drop_view(
        mtm,
        Rc::new(|paths, at| {
            if let Some(app) = app() {
                app.dropped(paths, at);
            }
        }),
    );
    window.setContentView(Some(&drop));
    let content: Retained<NSView> = Retained::into_super(drop);
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
    // A file named on the command line, then any Finder asked for while launching.
    let named = std::env::args_os().nth(1).map(std::path::PathBuf::from);
    let mut pending: Vec<std::path::PathBuf> = named.into_iter().collect();
    pending.extend(PENDING.with(|q| q.take()));
    open_later(pending);
}

thread_local! {
    static LATER: RefCell<std::collections::VecDeque<Box<dyn FnOnce()>>> = RefCell::default();
}

/// Run `f` from a main-queue block of its own, once the handler on the stack has returned.
pub fn later(f: Box<dyn FnOnce()>) {
    LATER.with(|l| l.borrow_mut().push_back(f));
    dispatch2::DispatchQueue::main().exec_async(|| {
        let next = LATER.with(|l| l.borrow_mut().pop_front());
        if let Some(f) = next {
            f();
        }
    });
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
        let settings = ui::button(mtm, "Settings...", ids::SETTINGS);
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
                let page = Page::new(mtm);
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
        let inspect_body = pages
            .iter()
            .find(|(t, _)| *t == AppTab::Inspect)
            .map(|(_, p)| p.body.clone())
            .expect("the Inspect page");
        let inspect = InspectScreen::build(
            mtm,
            inspect_body,
            ctrl.notify(),
            ctrl.recent_handle(),
            host.clone(),
        );
        let body_of = |tab: AppTab| {
            pages
                .iter()
                .find(|(t, _)| *t == tab)
                .map(|(_, p)| p.body.clone())
                .expect("a page per screen")
        };
        let backup = BackupScreen::build(
            mtm,
            body_of(AppTab::Backup),
            ctrl.notify(),
            ctrl.recent_handle(),
            host.clone(),
        );
        let restore = RestoreScreen::build(
            mtm,
            body_of(AppTab::Restore),
            ctrl.notify(),
            ctrl.recent_handle(),
            host.clone(),
        );
        let build = BuildScreen::build(mtm, body_of(AppTab::Build), ctrl.notify(), host.clone());
        let archives = ArchivesScreen::build(
            mtm,
            body_of(AppTab::Archives),
            ctrl.notify(),
            ctrl.recent_handle(),
            host.clone(),
        );
        let optical = OpticalScreen::build(
            mtm,
            body_of(AppTab::Optical),
            ctrl.notify(),
            ctrl.recent_handle(),
            host.clone(),
        );
        let convert =
            ConvertScreen::build(mtm, body_of(AppTab::Convert), ctrl.notify(), host.clone());
        content.addSubview(&inspect.browse.page);
        inspect.browse.page.setHidden(true);
        content.addSubview(&optical.browse.page);
        optical.browse.page.setHidden(true);
        content.addSubview(&inspect.remote.page);
        inspect.remote.page.setHidden(true);
        inspect.on_page(Rc::new(|| {
            if let Some(app) = app() {
                app.refresh();
                app.relayout();
            }
        }));
        optical.browse.on_change(Rc::new(|| {
            if let Some(app) = app() {
                app.optical.browser_changed();
                app.refresh();
                app.relayout();
            }
        }));
        inspect.browse.on_change(Rc::new(|| {
            if let Some(app) = app() {
                app.refresh();
                app.relayout();
            }
        }));
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
            inspect,
            backup,
            restore,
            build,
            archives,
            optical,
            convert,
            player_tick: Cell::new(false),
            settings: RefCell::default(),
            commander: RefCell::default(),
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
        let devices = self.ctrl.borrow().devices().to_vec();
        self.inspect.set_devices(&devices);
        self.inspect.tick();
        self.backup.set_devices(&devices);
        self.restore.set_devices(&devices);
        self.build.set_devices(&devices);
        self.lock_screens();
        self.backup.tick();
        self.restore.tick();
        self.build.tick();
        self.archives.tick();
        self.optical.tick();
        self.convert.tick();
        self.share_backup();
        for path in [self.inspect.take_convert(), self.optical.take_convert()]
            .into_iter()
            .flatten()
        {
            self.apply(AppAction::SelectTab(AppTab::Convert));
            self.convert.open(path);
        }
        if let Some(path) = self.inspect.take_optical() {
            self.apply(AppAction::SelectTab(AppTab::Optical));
            self.optical.open_image(path);
        }
        if let Some(path) = self.inspect.take_archive() {
            self.apply(AppAction::SelectTab(AppTab::Archives));
            self.archives.open_path(path);
        }
        if let Some(path) = self.archives.take_mount() {
            self.apply(AppAction::SelectTab(AppTab::Inspect));
            self.inspect.open_image(path);
        }
        if let Some(path) = self.build.take_open_inspect() {
            self.apply(AppAction::SelectTab(AppTab::Inspect));
            self.inspect.open_image(path);
        }
        let commander = self.commander.borrow().clone();
        if let Some(c) = commander {
            c.tick();
        }
        self.refresh();
        self.player_timer();
    }

    /// Files dropped from Finder: Commander takes them by pane, else the screen on show routes them.
    fn dropped(&self, paths: Vec<std::path::PathBuf>, at: NSPoint) {
        let commander = self.commander.borrow().clone();
        match (commander, MainThreadMarker::new()) {
            (Some(c), Some(mtm)) => c.drop_paths(mtm, paths, at),
            _ => self.drop_on_tab(paths),
        }
    }

    /// A drop on the window: Inspect's open volume takes it in Edit Mode, else the route says.
    fn drop_on_tab(&self, paths: Vec<std::path::PathBuf>) {
        use rusty_backup::controller::app::{DropRoute, DropTargets};
        let Some(first) = paths.first().cloned() else {
            return;
        };
        let tab = self.ctrl.borrow().active_tab();
        if tab == AppTab::Inspect && self.inspect.drop_paths(paths) {
            return;
        }
        let (inspect_remote, inspect_loaded) = self.inspect.drop_state();
        let targets = DropTargets {
            inspect_remote,
            inspect_loaded,
            optical_browsing: self.optical.browse.is_active(),
        };
        let route = self.ctrl.borrow_mut().route_drop(first, targets);
        let tab = match route {
            DropRoute::Inspect(path) => {
                self.inspect.open_image(path);
                AppTab::Inspect
            }
            DropRoute::Optical(path) => {
                self.optical.open_image(path);
                AppTab::Optical
            }
            DropRoute::Archives(path) => {
                self.archives.open_path(path);
                AppTab::Archives
            }
            DropRoute::Restore(path) => {
                self.restore
                    .apply(rusty_backup::controller::restore::RestoreAction::OpenDropped(path));
                AppTab::Restore
            }
            DropRoute::Backup(path) => {
                self.backup
                    .apply(rusty_backup::controller::backup::BackupAction::OpenImage {
                        path,
                        rename: false,
                    });
                AppTab::Backup
            }
            DropRoute::Convert(path) => {
                self.convert.open(path);
                AppTab::Convert
            }
            DropRoute::Ignore => return,
        };
        self.apply(AppAction::SelectTab(tab));
    }

    /// A file Finder (or the command line) asks this app to open: Inspect opens it, as on Windows.
    fn open_file(&self, path: std::path::PathBuf) {
        if !path.exists() {
            return;
        }
        self.apply(AppAction::SelectTab(AppTab::Inspect));
        self.inspect.open_image(path);
    }

    /// Settings, one window at a time; Clear Recent Files reaches every screen's picker.
    fn open_settings(&self) {
        let open = self.settings.borrow().as_ref().is_some_and(|w| w.is_open());
        if open {
            return;
        }
        let Some(mtm) = MainThreadMarker::new() else {
            return;
        };
        let recent = self.ctrl.borrow().recent_handle();
        let window = SettingsWindow::open(
            mtm,
            recent,
            Box::new(|| {
                if let Some(app) = app() {
                    app.forget_recents();
                }
            }),
        );
        *self.settings.borrow_mut() = Some(window);
    }

    fn forget_recents(&self) {
        self.backup.forget_recents();
        self.restore.forget_recents();
        self.optical.forget_recents();
        self.archives.forget_recents();
    }

    /// A playing track moves with no job to wake the shell, so a timer ticks while it plays.
    fn player_timer(&self) {
        if !self.optical.playing() || self.player_tick.replace(true) {
            return;
        }
        let when = DispatchTime::try_from(std::time::Duration::from_millis(250))
            .unwrap_or(DispatchTime::NOW);
        let queued = DispatchQueue::main().after(when, || {
            if let Some(app) = app() {
                app.player_tick.set(false);
                app.tick();
            }
        });
        if queued.is_err() {
            self.player_tick.set(false);
        }
    }

    /// One disk job at a time: each screen hears who else runs.
    fn lock_screens(&self) {
        let running = [
            ("Inspect", self.inspect.running_job()),
            ("Backup", self.backup.is_running().then_some("Backup")),
            ("Restore", self.restore.is_running().then_some("Restore")),
            (
                "Build Disk",
                self.build.is_running().then_some("Build Disk"),
            ),
            ("Optical", self.optical.is_running().then_some("Optical")),
            ("Convert", self.convert.is_running().then_some("Convert")),
        ];
        let elsewhere = |me: &str| {
            running
                .iter()
                .find(|(t, r)| *t != me && r.is_some())
                .and_then(|(_, r)| *r)
        };
        self.backup.set_busy(elsewhere("Backup"));
        self.restore.set_busy(elsewhere("Restore"));
        self.build.set_busy(elsewhere("Build Disk"));
        self.archives.set_busy(elsewhere("Archives"));
        self.optical.set_busy(elsewhere("Optical"));
        self.convert.set_busy(elsewhere("Convert"));
    }

    /// Restore and Inspect share a backup folder: the screen on show publishes or adopts it.
    fn share_backup(&self) {
        use rusty_backup::controller::app::ShareStep;
        if self.inspect.take_backup_closed() {
            self.ctrl.borrow_mut().close_shared_backup();
            self.restore.clear_backup();
        }
        let tab = self.ctrl.borrow().active_tab();
        let (loaded, empty) = match tab {
            AppTab::Restore => (self.restore.loaded_backup(), self.restore.is_empty()),
            AppTab::Inspect => (self.inspect.loaded_backup(), self.inspect.is_empty()),
            AppTab::Backup
            | AppTab::Build
            | AppTab::Optical
            | AppTab::Archives
            | AppTab::Convert => return,
        };
        let step = self.ctrl.borrow_mut().share_backup(loaded, empty);
        match (step, tab) {
            (ShareStep::Publish(folder), AppTab::Restore)
            | (ShareStep::Adopt(folder), AppTab::Inspect) => self.inspect.load_backup(&folder),
            (ShareStep::Publish(folder), _) | (ShareStep::Adopt(folder), _) => {
                self.restore.load_backup(&folder)
            }
            (ShareStep::Nothing, _) => {}
        }
    }

    /// Open Commander over the whole window; the toolbar and the screens hide until it closes.
    fn open_commander(&self) {
        if self.commander.borrow().is_some() {
            return;
        }
        let Some(mtm) = MainThreadMarker::new() else {
            return;
        };
        let notify = self.ctrl.borrow().notify();
        let jobs = rusty_backup::controller::commander::JobRegistry::new();
        let on_close: Rc<dyn Fn()> = Rc::new(|| {
            // The screen may still be on the stack: drop it once the action in hand is done.
            later(Box::new(|| {
                if let Some(app) = app() {
                    app.close_commander();
                }
            }));
        });
        let screen = CommanderScreen::build(mtm, &jobs, &notify, self.host.clone(), on_close);
        self.content.addSubview(&screen.page);
        *self.commander.borrow_mut() = Some(screen);
        self.toolbar.setVisible(false);
        self.relayout();
    }

    fn close_commander(&self) {
        let Some(screen) = self.commander.borrow_mut().take() else {
            return;
        };
        screen.page.removeFromSuperview();
        self.toolbar.setVisible(true);
        self.refresh();
        self.relayout();
    }

    fn command(&self, id: &str) {
        if self.sync.active() {
            return;
        }
        if let Some(tab) = crate::toolbar::tab_of(id) {
            self.apply(AppAction::SelectTab(tab));
            return;
        }
        if id == ids::COMMANDER {
            self.open_commander();
            return;
        }
        if id.starts_with("optical.") {
            if self.ctrl.borrow().active_tab() != AppTab::Optical {
                self.apply(AppAction::SelectTab(AppTab::Optical));
            }
            self.optical.command(id);
            self.refresh();
            return;
        }
        // Inspect's own, and its remote session's buttons.
        if id.starts_with("inspect.") || id.starts_with("remote_pick.") {
            // A menu item can reach Inspect from another screen: show it first.
            if self.ctrl.borrow().active_tab() != AppTab::Inspect {
                self.apply(AppAction::SelectTab(AppTab::Inspect));
            }
            self.inspect.command(id);
            self.refresh();
            return;
        }
        match id {
            ids::CLOSE_BACKUP => {
                // It closes on both screens.
                self.ctrl.borrow_mut().close_shared_backup();
                self.restore.clear_backup();
                self.inspect.clear_backup();
                append_log(vec![(LogLevel::Info, "Backup closed".to_string())]);
                self.refresh();
            }
            ids::REFRESH_DEVICES => self.apply(AppAction::RefreshDevices),
            ids::ELEVATE => self.apply(AppAction::RequestElevation),
            ids::ABOUT => self.about(),
            ids::SETTINGS => self.open_settings(),
            ids::UPDATE_VIEW => self.apply(AppAction::ViewRelease),
            ids::UPDATE_INSTALL => self.apply(AppAction::InstallUpdate),
            ids::UPDATE_RESTART => self.apply(AppAction::RestartApp),
            ids::UPDATE_RETRY => self.apply(AppAction::RetryUpdate),
            ids::UPDATE_CHECK => self.apply(AppAction::CheckForUpdates),
            ids::UPDATE_DISMISS => self.apply(AppAction::DismissUpdate),
            ids::LOG_COPY => self.apply(AppAction::CopyLog),
            ids::LOG_CLEAR => self.apply(AppAction::ClearLog),
            ids::LOG_AUTO_SCROLL => {
                // The tick box has flipped already; the menu item asks for the same flip.
                let on = !self.ctrl.borrow().log_auto_scroll();
                self.apply(AppAction::SetLogAutoScroll(on));
            }
            crate::menu::ISSUES => self
                .host
                .run(vec![Effect::OpenUrl(about::ISSUES.to_string())], &reply()),
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
            // A browse view stands in its page's place while it is open.
            let browsing = self.inspect.browse.is_active();
            let picking = !browsing && self.inspect.remote.is_active();
            let disc = self.optical.browse.is_active();
            let covered = self.commander.borrow().is_some();
            for (t, page) in &self.pages {
                let inspect = (browsing || picking) && *t == AppTab::Inspect;
                let over = inspect || (disc && *t == AppTab::Optical);
                ui::set_visible(&page.scroll, *t == tab && !over && !covered);
            }
            let browse = browsing && tab == AppTab::Inspect && !covered;
            ui::set_visible(&self.inspect.browse.page, browse);
            let pick = picking && tab == AppTab::Inspect && !covered;
            ui::set_visible(&self.inspect.remote.page, pick);
            let disc = disc && tab == AppTab::Optical && !covered;
            ui::set_visible(&self.optical.browse.page, disc);
        });
        if banner_was != banner.is_some() || shared_was != shared || self.banner_buttons_moved() {
            self.relayout();
        }
        self.sync_log(auto);
        let shared = self.ctrl.borrow().shared_backup().is_some();
        let commander = self.commander.borrow().is_some();
        crate::menu::sync(&|id| match id {
            ids::COMMANDER => Some(!commander),
            ids::CLOSE_BACKUP => Some(shared),
            ids::LOG_CLEAR | ids::LOG_AUTO_SCROLL => Some(true),
            _ if crate::toolbar::tab_of(id).is_some() => Some(true),
            _ => None,
        });
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

    /// After Commander: show the parts refresh does not manage, then let refresh decide the rest.
    fn restore_parts(&self) {
        for (p, v) in &self.parts {
            let managed = matches!(
                p,
                Part::CloseBackup
                    | Part::Refresh
                    | Part::Elevate
                    | Part::BannerText
                    | Part::Install
                    | Part::Restart
                    | Part::Retry
                    | Part::ViewRelease
                    | Part::Dismiss
            );
            if !managed && v.isHidden() {
                v.setHidden(false);
            }
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
        if let Some(c) = &*self.commander.borrow() {
            // Commander covers the window; what is under it hides until it closes.
            for (_, v) in &self.parts {
                v.setHidden(true);
            }
            for (_, page) in &self.pages {
                page.scroll.setHidden(true);
            }
            self.inspect.browse.page.setHidden(true);
            self.optical.browse.page.setHidden(true);
            self.inspect.remote.page.setHidden(true);
            ui::move_to(&c.page, ui::area(&self.content));
            c.layout();
            return;
        }
        self.restore_parts();
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
                }
                self.inspect.relayout();
                self.backup.relayout();
                self.restore.relayout();
                self.build.relayout();
                self.archives.relayout();
                self.optical.relayout();
                self.convert.relayout();
                ui::move_to(&self.inspect.browse.page, *r);
                self.inspect.browse.layout();
                ui::move_to(&self.optical.browse.page, *r);
                self.optical.browse.layout();
                ui::move_to(&self.inspect.remote.page, *r);
                self.inspect.remote.layout();
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

pub fn log_font() -> Retained<NSFont> {
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
