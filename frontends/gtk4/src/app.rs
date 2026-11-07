//! The application window: tabs, top bar and log panel, drawn from `AppController`.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use adw::prelude::*;
use gtk::glib;
use rusty_backup::controller::app::{
    app_version, busy_elsewhere, elevation_button_hover, elevation_button_label, elevation_confirm,
    elevation_gate, ids, AppAction, AppController, AppTab, ElevationGate, ShareStep,
};
use rusty_backup::controller::inspect::{InspectAction, InspectSource};
use rusty_backup::controller::keys::{Key, Mods};
use rusty_backup::controller::log::{capture, LogLevel};
use rusty_backup::controller::{Controller, Effect};

// Through `super`: scripts/check-boundaries.sh takes a crate path to these names for the engine's.
use super::archives::ArchivesScreen;
use super::backup::BackupScreen;
use super::build_disk::BuildDiskScreen;
use super::optical::OpticalScreen;
use super::restore::RestoreScreen;
use crate::commander::CommanderScreen;
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::inspect::InspectScreen;
use crate::keys::{key_of, mods_of};
use crate::settings::SettingsWindow;
use crate::sync::SyncGuard;
use crate::wake::GtkWake;
use crate::widgets::plain;

const TABS: [(AppTab, &str, &str, &str); 6] = [
    (AppTab::Backup, "backup", "Backup", "document-save-symbolic"),
    (
        AppTab::Restore,
        "restore",
        "Restore",
        "document-revert-symbolic",
    ),
    (
        AppTab::Build,
        "build",
        "Build Disk",
        "drive-harddisk-symbolic",
    ),
    (
        AppTab::Inspect,
        "inspect",
        "Inspect",
        "system-search-symbolic",
    ),
    (
        AppTab::Optical,
        "optical",
        "Optical",
        "media-optical-symbolic",
    ),
    (
        AppTab::Archives,
        "archives",
        "Archives",
        "package-x-generic-symbolic",
    ),
];

pub struct App {
    ctrl: RefCell<AppController>,
    host: EffectHost,
    inspect: Rc<InspectScreen>,
    backup: Rc<BackupScreen>,
    restore: Rc<RestoreScreen>,
    build: Rc<BuildDiskScreen>,
    optical: Rc<OpticalScreen>,
    archives: Rc<ArchivesScreen>,
    close_backup: gtk::Button,
    settings: RefCell<Option<Rc<SettingsWindow>>>,
    banner: gtk::Revealer,
    banner_text: gtk::Label,
    /// The tabs, or Commander Mode, which takes the whole window while open.
    screens: gtk::Stack,
    commander: RefCell<Option<Rc<CommanderScreen>>>,
    stack: adw::ViewStack,
    log_view: gtk::TextView,
    log_shown: Cell<usize>,
    auto_scroll: gtk::CheckButton,
    sync: SyncGuard,
}

thread_local! {
    static APP: RefCell<Option<Rc<App>>> = const { RefCell::new(None) };
}

/// Drain the controllers; `GtkWake` calls this on the main loop.
pub fn tick() {
    if let Some(app) = APP.with(|a| a.borrow().clone()) {
        app.tick();
    }
}

/// Settings cleared the recent files: every tab's picker forgets its copy.
fn forget_recents() {
    if let Some(app) = APP.with(|a| a.borrow().clone()) {
        app.forget_recents();
    }
}

/// Leave Commander Mode for the tabs, once its controller has said so.
fn close_commander() {
    if let Some(app) = APP.with(|a| a.borrow().clone()) {
        app.close_commander();
    }
}

/// Add a controller's drained log lines to the log panel.
pub fn append_log(lines: Vec<(LogLevel, String)>) {
    if lines.is_empty() {
        return;
    }
    if let Some(app) = APP.with(|a| a.borrow().clone()) {
        app.ctrl.borrow_mut().log.extend_lines(lines);
        app.sync_log(&app.ctrl.borrow());
    }
}

/// Show the window, creating it on first use, and open `image` in Inspect if one was named.
pub fn present(application: &adw::Application, image: Option<PathBuf>) {
    let existing = APP.with(|a| a.borrow().clone());
    let app = existing.unwrap_or_else(|| {
        let app = App::build(application);
        APP.with(|a| *a.borrow_mut() = Some(app.clone()));
        app
    });
    app.host_window().present();
    tick();
    if let Some(path) = image {
        app.apply(AppAction::SelectTab(AppTab::Inspect));
        app.inspect.open_file(path);
    }
}

/// Build Disk's "Open in Inspect" on the image it just wrote.
pub fn open_in_inspect(path: PathBuf) {
    if let Some(app) = APP.with(|a| a.borrow().clone()) {
        app.apply(AppAction::SelectTab(AppTab::Inspect));
        app.inspect.open_file(path);
    }
}

/// Archives' Mount: a decoded payload, which its guard keeps alive, in Inspect.
pub fn load_in_inspect(source: InspectSource) {
    if let Some(app) = APP.with(|a| a.borrow().clone()) {
        app.apply(AppAction::SelectTab(AppTab::Inspect));
        app.inspect.apply(InspectAction::Load(source));
    }
}

/// An optical disc Inspect opened goes to the Optical tab.
pub fn open_in_optical(path: PathBuf) {
    if let Some(app) = APP.with(|a| a.borrow().clone()) {
        app.apply(AppAction::SelectTab(AppTab::Optical));
        app.optical.open_image(path);
    }
}

/// A Mac archive Inspect opened goes to the Archives tab.
pub fn open_in_archives(path: PathBuf) {
    if let Some(app) = APP.with(|a| a.borrow().clone()) {
        app.apply(AppAction::SelectTab(AppTab::Archives));
        app.archives.open_path(path);
    }
}

/// Answers to the app controller's own questions.
fn reply() -> ReplyFn {
    Rc::new(|reply| {
        let Some(app) = APP.with(|a| a.borrow().clone()) else {
            return;
        };
        if let Reply::Confirmed {
            purpose: "elevate",
            yes,
        } = reply
        {
            app.apply(AppAction::ConfirmElevation(yes));
        }
    })
}

impl App {
    fn host_window(&self) -> gtk::Window {
        self.log_view
            .root()
            .and_then(|r| r.downcast::<gtk::Window>().ok())
            .expect("the log view is in the window")
    }

    fn build(application: &adw::Application) -> Rc<App> {
        let wake = Arc::new(GtkWake::default());
        capture::set_wake(wake.clone());
        let mut ctrl = AppController::new(wake);
        ctrl.load_devices();
        crate::theme::install_css();

        let stack = adw::ViewStack::new();

        let switcher = adw::ViewSwitcher::builder()
            .stack(&stack)
            .policy(adw::ViewSwitcherPolicy::Wide)
            .build();
        let header = adw::HeaderBar::builder().title_widget(&switcher).build();

        let refresh = gtk::Button::from_icon_name("view-refresh-symbolic");
        refresh.set_tooltip_text(Some("Refresh devices"));
        refresh.set_widget_name(ids::REFRESH_DEVICES);
        let gate = elevation_gate();
        refresh.set_visible(matches!(
            gate,
            ElevationGate::NotApplicable | ElevationGate::Elevated
        ));
        header.pack_end(&refresh);

        let elevate = gtk::Button::with_label(elevation_button_label());
        elevate.set_tooltip_text(Some(elevation_button_hover()));
        elevate.set_widget_name(ids::ELEVATE);
        elevate.set_visible(gate == ElevationGate::NeedsElevation);
        header.pack_end(&elevate);

        let commander = gtk::Button::with_label("Commander Mode");
        commander.set_widget_name(ids::COMMANDER);
        header.pack_start(&commander);

        let close_backup = gtk::Button::with_label("Close Backup");
        close_backup.set_widget_name(ids::CLOSE_BACKUP);
        close_backup.set_visible(false);
        header.pack_end(&close_backup);

        let settings = gtk::Button::with_label("Settings...");
        settings.set_widget_name(ids::SETTINGS);
        let bulk = gtk::Button::with_label("Bulk Convert...");
        bulk.set_widget_name(ids::BULK_CONVERT);
        bulk.set_tooltip_text(Some(
            "Convert every disk image in a folder to one chosen format, \
             using the same parameters for every file.",
        ));
        crate::widgets::coming(&bulk, "Bulk Convert");
        let version = gtk::Label::new(Some(&format!("Version {}", app_version())));
        version.add_css_class("dim-label");
        let menu_box = gtk::Box::new(gtk::Orientation::Vertical, 2);
        for b in [&settings, &bulk] {
            b.add_css_class("flat");
            if let Some(label) = b.child().and_then(|c| c.downcast::<gtk::Label>().ok()) {
                label.set_xalign(0.0);
            }
            menu_box.append(b);
        }
        menu_box.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
        menu_box.append(&version);
        let popover = gtk::Popover::builder().child(&menu_box).build();
        let menu = gtk::MenuButton::builder()
            .icon_name("open-menu-symbolic")
            .popover(&popover)
            .tooltip_text("Menu")
            .build();
        menu.set_widget_name(ids::MENU);
        header.pack_end(&menu);

        let banner_text = gtk::Label::builder().xalign(0.0).hexpand(true).build();
        let banner_title = gtk::Label::new(Some("Update"));
        banner_title.add_css_class("warning");
        banner_title.add_css_class("heading");
        let view_release = gtk::Button::with_label("View Release");
        view_release.set_widget_name(ids::UPDATE_VIEW);
        let dismiss = gtk::Button::with_label("Dismiss update notification");
        dismiss.set_widget_name(ids::UPDATE_DISMISS);
        let banner_box = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        banner_box.set_margin_start(8);
        banner_box.set_margin_end(8);
        banner_box.set_margin_top(4);
        banner_box.set_margin_bottom(4);
        banner_box.append(&banner_title);
        banner_box.append(&banner_text);
        banner_box.append(&view_release);
        banner_box.append(&dismiss);
        let banner = gtk::Revealer::builder().child(&banner_box).build();

        let log_view = gtk::TextView::builder()
            .editable(false)
            .cursor_visible(false)
            .monospace(true)
            .wrap_mode(gtk::WrapMode::WordChar)
            .build();
        let log_scroll = gtk::ScrolledWindow::builder()
            .child(&log_view)
            .vexpand(true)
            .build();
        log_view.set_widget_name(ids::LOG_LIST);
        let copy = gtk::Button::with_label("Copy Log");
        copy.set_widget_name(ids::LOG_COPY);
        let clear = gtk::Button::with_label("Clear Log");
        clear.set_widget_name(ids::LOG_CLEAR);
        let auto_scroll = gtk::CheckButton::with_label("Auto-scroll");
        auto_scroll.set_widget_name(ids::LOG_AUTO_SCROLL);
        auto_scroll.set_active(ctrl.log_auto_scroll());
        let log_bar = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        log_bar.set_margin_start(6);
        log_bar.set_margin_end(6);
        log_bar.append(&copy);
        log_bar.append(&clear);
        log_bar.append(&auto_scroll);
        let log_box = gtk::Box::new(gtk::Orientation::Vertical, 4);
        log_box.append(&log_bar);
        log_box.append(&log_scroll);

        let split = gtk::Paned::builder()
            .orientation(gtk::Orientation::Vertical)
            .start_child(&stack)
            .end_child(&log_box)
            .resize_end_child(false)
            .shrink_end_child(false)
            .position(600)
            .build();

        let view = adw::ToolbarView::new();
        view.add_top_bar(&header);
        view.add_top_bar(&banner);
        view.set_content(Some(&split));
        let screens = gtk::Stack::new();
        screens.add_named(&view, Some("tabs"));
        let toasts = adw::ToastOverlay::new();
        toasts.set_child(Some(&screens));

        let window = adw::ApplicationWindow::builder()
            .application(application)
            .title("Rusty Backup")
            .default_width(1200)
            .default_height(800)
            .content(&toasts)
            .build();
        window.set_size_request(700, 460);

        let host = EffectHost::new(window, toasts);
        let inspect = InspectScreen::build(ctrl.notify(), ctrl.recent_handle(), host.clone());
        let backup = BackupScreen::build(ctrl.notify(), ctrl.recent_handle(), host.clone());
        let restore = RestoreScreen::build(ctrl.notify(), ctrl.recent_handle(), host.clone());
        let build = BuildDiskScreen::build(ctrl.notify(), host.clone());
        let optical = OpticalScreen::build(ctrl.notify(), ctrl.recent_handle(), host.clone());
        let archives = ArchivesScreen::build(ctrl.notify(), ctrl.recent_handle(), host.clone());
        for (tab, name, title, icon) in TABS {
            match tab {
                AppTab::Inspect => {
                    stack.add_titled_with_icon(inspect.root(), Some(name), title, icon);
                }
                AppTab::Backup => {
                    stack.add_titled_with_icon(backup.root(), Some(name), title, icon);
                }
                AppTab::Restore => {
                    stack.add_titled_with_icon(restore.root(), Some(name), title, icon);
                }
                AppTab::Build => {
                    stack.add_titled_with_icon(build.root(), Some(name), title, icon);
                }
                AppTab::Optical => {
                    stack.add_titled_with_icon(optical.root(), Some(name), title, icon);
                }
                AppTab::Archives => {
                    stack.add_titled_with_icon(archives.root(), Some(name), title, icon);
                }
            }
        }

        let app = Rc::new(App {
            ctrl: RefCell::new(ctrl),
            host,
            inspect,
            backup,
            restore,
            build,
            optical,
            archives,
            close_backup: close_backup.clone(),
            settings: RefCell::default(),
            banner,
            banner_text,
            screens,
            commander: RefCell::default(),
            stack: stack.clone(),
            log_view,
            log_shown: Cell::new(0),
            auto_scroll: auto_scroll.clone(),
            sync: SyncGuard::default(),
        });

        app.optical.find_drives();
        app.ctrl.borrow_mut().start_update_check();

        let send = |action: fn() -> AppAction| {
            let app = app.clone();
            move |_: &gtk::Button| app.apply(action())
        };
        let a = app.clone();
        commander.connect_clicked(move |_| a.open_commander());
        let a = app.clone();
        close_backup.connect_clicked(move |_| a.close_backup());
        refresh.connect_clicked(send(|| AppAction::RefreshDevices));
        elevate.connect_clicked(send(|| AppAction::RequestElevation));
        copy.connect_clicked(send(|| AppAction::CopyLog));
        view_release.connect_clicked(send(|| AppAction::ViewRelease));
        dismiss.connect_clicked(send(|| AppAction::DismissUpdate));
        let a = app.clone();
        let p = popover.clone();
        settings.connect_clicked(move |_| {
            p.popdown();
            a.open_settings();
        });
        clear.connect_clicked(send(|| AppAction::ClearLog));
        let a = app.clone();
        auto_scroll.connect_toggled(move |b| {
            if !a.sync.active() {
                a.apply(AppAction::SetLogAutoScroll(b.is_active()));
            }
        });
        let a = app.clone();
        stack.connect_visible_child_name_notify(move |s| {
            if a.sync.active() {
                return;
            }
            let name = s.visible_child_name();
            let tab = TABS.iter().find(|t| Some(t.1) == name.as_deref());
            if let Some(t) = tab {
                a.apply(AppAction::SelectTab(t.0));
            }
        });
        // On the window: a closing dialog leaves nothing focused, and keys must still work then.
        let keys = gtk::EventControllerKey::new();
        keys.set_propagation_phase(gtk::PropagationPhase::Capture);
        let a = app.clone();
        keys.connect_key_pressed(move |_, key, _, state| {
            let used = key_of(key).is_some_and(|k| a.key(k, mods_of(state)));
            match used {
                true => glib::Propagation::Stop,
                false => glib::Propagation::Proceed,
            }
        });
        app.host_window().add_controller(keys);
        let a = app.clone();
        app.host_window().connect_close_request(move |_| {
            let screen = a.commander.borrow().clone();
            match screen.is_some_and(|s| s.hold_quit()) {
                true => glib::Propagation::Stop,
                false => glib::Propagation::Proceed,
            }
        });
        app
    }

    /// A shortcut goes to the screen on top; a dialog over the window keeps its keys.
    fn key(&self, key: Key, mods: Mods) -> bool {
        if self.host.dialog_open() {
            return false;
        }
        let commander = self.commander.borrow().clone();
        match commander {
            Some(screen) => screen.key(key, mods),
            None => {
                let tab = self.ctrl.borrow().active_tab();
                match tab {
                    AppTab::Inspect => self.inspect.key(key, mods),
                    AppTab::Optical => self.optical.key(key, mods),
                    AppTab::Backup | AppTab::Restore | AppTab::Build | AppTab::Archives => false,
                }
            }
        }
    }

    fn open_commander(&self) {
        if self.commander.borrow().is_some() {
            return;
        }
        let ctrl = self.ctrl.borrow();
        let on_close: Rc<dyn Fn()> = Rc::new(close_commander);
        let screen =
            CommanderScreen::build(ctrl.jobs(), &ctrl.notify(), self.host.clone(), on_close);
        drop(ctrl);
        self.screens.add_named(screen.root(), Some("commander"));
        self.screens.set_visible_child_name("commander");
        screen.opened();
        *self.commander.borrow_mut() = Some(screen);
    }

    fn close_commander(&self) {
        let Some(screen) = self.commander.borrow_mut().take() else {
            return;
        };
        self.screens.set_visible_child_name("tabs");
        self.screens.remove(screen.root());
    }

    fn apply(&self, action: AppAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    fn tick(&self) {
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
        let refreshing = self.ctrl.borrow().devices_refreshing().is_some();
        self.backup.set_devices(&devices, refreshing);
        self.restore.set_devices(&devices, refreshing);
        self.build.set_devices(&devices, refreshing);
        self.inspect.set_devices(devices);
        self.inspect.tick();
        self.backup.tick();
        self.restore.tick();
        self.build.tick();
        self.optical.tick();
        self.archives.tick();
        self.share_backup();
        self.lock_tabs();
        let commander = self.commander.borrow().clone();
        if let Some(screen) = commander {
            screen.tick();
        }
        self.refresh();
    }

    /// Restore and Inspect share a backup folder: the tab on screen publishes or adopts it.
    fn share_backup(&self) {
        let tab = self.ctrl.borrow().active_tab();
        if self.inspect.take_backup_closed() {
            self.ctrl.borrow_mut().close_shared_backup();
            self.restore.clear_backup();
        }
        let (loaded, empty) = match tab {
            AppTab::Restore => (self.restore.loaded_backup(), self.restore.is_empty()),
            AppTab::Inspect => (self.inspect.loaded_backup(), self.inspect.is_empty()),
            AppTab::Backup | AppTab::Build | AppTab::Optical | AppTab::Archives => return,
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

    /// The top bar's Close Backup: it closes on both tabs.
    fn open_settings(&self) {
        let open = self.settings.borrow().as_ref().is_some_and(|w| w.is_open());
        if open {
            return;
        }
        let recent = self.ctrl.borrow().recent_handle();
        let window = SettingsWindow::open(recent, Box::new(forget_recents));
        self.host.present(window.dialog());
        *self.settings.borrow_mut() = Some(window);
    }

    fn forget_recents(&self) {
        self.inspect.forget_recents();
        self.backup.forget_recents();
        self.restore.forget_recents();
        self.optical.forget_recents();
        self.archives.forget_recents();
    }

    fn close_backup(&self) {
        self.ctrl.borrow_mut().close_shared_backup();
        self.restore.clear_backup();
        self.inspect.clear_backup();
        append_log(vec![(LogLevel::Info, "Backup closed".to_string())]);
        self.refresh();
    }

    /// One disk job at a time: each tab hears who else runs.
    fn lock_tabs(&self) {
        let on = |running: bool, name| running.then_some(name);
        let running = [
            (AppTab::Backup, on(self.backup.is_running(), "Backup")),
            (AppTab::Restore, on(self.restore.is_running(), "Restore")),
            (AppTab::Build, on(self.build.is_running(), "Build Disk")),
            (AppTab::Optical, on(self.optical.is_running(), "Optical")),
            (AppTab::Inspect, self.inspect.running_job()),
        ];
        self.backup
            .set_busy(busy_elsewhere(AppTab::Backup, &running, false));
        self.restore
            .set_busy(busy_elsewhere(AppTab::Restore, &running, false));
        self.build
            .set_busy(busy_elsewhere(AppTab::Build, &running, false));
        self.optical
            .set_busy(busy_elsewhere(AppTab::Optical, &running, false));
    }

    /// Read the controller first and release it: setting a widget can fire its change signal.
    fn refresh(&self) {
        let tab = self.ctrl.borrow().active_tab();
        let shared = self.ctrl.borrow().shared_backup().is_some();
        self.close_backup.set_visible(shared);
        let banner = self.ctrl.borrow().update_banner_text();
        self.banner.set_reveal_child(banner.is_some());
        if let Some(text) = banner {
            crate::widgets::set_text(&self.banner_text, &text);
        }
        self.sync.run(|| {
            if let Some((_, name, ..)) = TABS.iter().find(|t| t.0 == tab) {
                if self.stack.visible_child_name().as_deref() != Some(*name) {
                    self.stack.set_visible_child_name(name);
                }
            }
        });
        self.sync_log(&self.ctrl.borrow());
    }

    fn sync_log(&self, ctrl: &AppController) {
        let entries = ctrl.log.entries();
        let buffer = self.log_view.buffer();
        if entries.len() < self.log_shown.get() {
            buffer.set_text("");
            self.log_shown.set(0);
        }
        if entries.len() == self.log_shown.get() {
            return;
        }
        let mut end = buffer.end_iter();
        for entry in &entries[self.log_shown.get()..] {
            buffer.insert(&mut end, &plain(&entry.line()));
            buffer.insert(&mut end, "\n");
        }
        self.log_shown.set(entries.len());
        if self.auto_scroll.is_active() {
            // A mark that stays: GTK scrolls once the new lines are measured, after this returns.
            let mark = buffer
                .mark("log-end")
                .unwrap_or_else(|| buffer.create_mark(Some("log-end"), &buffer.end_iter(), false));
            buffer.move_mark(&mark, &buffer.end_iter());
            self.log_view.scroll_to_mark(&mark, 0.0, false, 0.0, 0.0);
        }
    }
}
