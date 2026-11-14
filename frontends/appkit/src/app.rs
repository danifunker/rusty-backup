//! The application: `NSApplication` and its delegate, the menu bar, and the shared main window
//! (frontends/mac-screens/src/main_window.rs) on the AppKit host.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use objc2::rc::Retained;
use objc2::runtime::{NSObject, NSObjectProtocol, ProtocolObject};
use objc2::{define_class, msg_send, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{NSApplication, NSApplicationActivationPolicy, NSApplicationDelegate, NSFont};
use objc2_foundation::{NSArray, NSNotification, NSURL};
use rusty_backup::controller::log::{capture, LogLevel};

use crate::form::AppKitHost;
use crate::screens::main_window::MainWindow;
use crate::wake::MacWake;

thread_local! {
    static APP: RefCell<Option<Rc<MainWindow>>> = const { RefCell::new(None) };
    static DELEGATE: RefCell<Option<Retained<Delegate>>> = const { RefCell::new(None) };
    /// Files asked for before the window was built: opened once it is.
    static PENDING: RefCell<Vec<std::path::PathBuf>> = const { RefCell::new(Vec::new()) };
}

fn app() -> Option<Rc<MainWindow>> {
    APP.with(|a| a.borrow().clone())
}

define_class!(
    /// The application's delegate.
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
);

/// Open `paths` in Inspect now, or once the window is built.
fn open_later(paths: Vec<std::path::PathBuf>) {
    match app() {
        Some(app) => app.open_files(paths),
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

/// The app's own icon, so a build run outside its bundle (the flows, the tour) shows it in
/// alerts and the Dock instead of a folder.
fn set_icon(app: &NSApplication) {
    use objc2::AllocAnyThread;
    let bytes = include_bytes!("../../../assets/icons/icon-256.png");
    let data = objc2_foundation::NSData::with_bytes(bytes);
    if let Some(image) =
        objc2_app_kit::NSImage::initWithData(objc2_app_kit::NSImage::alloc(), &data)
    {
        // SAFETY: setting the shared application's icon on the main thread.
        unsafe { app.setApplicationIconImage(Some(&image)) };
    }
}

/// Run the application until it quits.
pub fn run() {
    let mtm = MainThreadMarker::new().expect("main runs on the main thread");
    let ns_app = NSApplication::sharedApplication(mtm);
    ns_app.setActivationPolicy(NSApplicationActivationPolicy::Regular);
    force_appearance(&ns_app);
    set_icon(&ns_app);
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

fn start(mtm: MainThreadMarker) {
    let wake = Arc::new(MacWake::default());
    capture::set_wake(wake.clone());
    let ticked = wake.clone();
    let app = MainWindow::open(
        AppKitHost::get(mtm),
        "Rusty Backup (AppKit preview)",
        wake,
        Box::new(move || ticked.ticked()),
    );
    APP.with(|a| *a.borrow_mut() = Some(app));
    NSApplication::sharedApplication(mtm).activate();
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
pub fn append_log(lines: Vec<(LogLevel, String)>) {
    if let Some(app) = app() {
        app.append_log(lines);
    }
}

pub fn log_font() -> Retained<NSFont> {
    NSFont::userFixedPitchFontOfSize(11.0).unwrap_or_else(|| NSFont::systemFontOfSize(11.0))
}
