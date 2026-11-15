//! The application window: the screen toolbar, top bar, update banner, a page per screen, the
//! status line and the log, drawn from `AppController`. Commander covers it while it is open.
//!
//! The shell supplies the toolbar's and menu bar's native parts through the [`Host`]; both, and
//! every control here, act by controller id through [`MainWindow::command`].

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::{
    about, app_version, elevation_button_hover, elevation_button_label, elevation_confirm,
    elevation_gate, ids, AppAction, AppController, AppTab, ElevationGate, Notify,
};
// The in-app update needs the network, which only some builds have; without it, no banner.
#[cfg(feature = "update")]
use rusty_backup::controller::app::InstallState;
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::{Controller, Effect};

use super::archives::ArchivesScreen;
use super::backup::BackupScreen;
use super::build_disk::BuildScreen;
use super::commander::CommanderScreen;
use super::convert::ConvertScreen;
use super::effects::{EffectHost, Reply, ReplyFn};
use super::form::Form;
use super::host::{Host, Surface, ToolItem};
use super::inspect::InspectScreen;
use super::layout::{self, Node, Size};
#[cfg(feature = "optical")]
use super::optical::OpticalScreen;
use super::restore::RestoreScreen;
use super::settings::SettingsWindow;
use super::sync::SyncGuard;

/// The menu bar's Help items, which act here like any control.
pub const WEBSITE: &str = "app.website";
pub const ISSUES: &str = "app.issues";

/// The toolbar item id of a screen; `command` maps it back.
pub fn screen_id(tab: AppTab) -> String {
    format!("app.screen.{}", tab.icon())
}

pub fn tab_of(id: &str) -> Option<AppTab> {
    AppTab::ALL.iter().copied().find(|t| screen_id(*t) == id)
}

/// The screens this build has: Optical needs optical media support.
pub fn tabs() -> Vec<AppTab> {
    AppTab::ALL
        .iter()
        .copied()
        .filter(|t| cfg!(feature = "optical") || *t != AppTab::Optical)
        .collect()
}

/// What each top-level control is, for layout.
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
    /// A screen's scrolling page; the one for the screen on show is visible.
    Page(AppTab),
    /// The pages that stand in a screen's place: Inspect's browse view and remote picker, and
    /// Optical's disc browser.
    InspectBrowse,
    InspectRemote,
    OpticalBrowse,
    Commander,
    Status,
    LogCopy,
    LogClear,
    LogAuto,
    Log,
}

/// The controller id a control sends.
fn part_id(p: Part) -> Option<&'static str> {
    Some(match p {
        Part::CloseBackup => ids::CLOSE_BACKUP,
        Part::Refresh => ids::REFRESH_DEVICES,
        Part::Elevate => ids::ELEVATE,
        Part::Settings => ids::SETTINGS,
        Part::About => ids::ABOUT,
        Part::Install => ids::UPDATE_INSTALL,
        Part::Restart => ids::UPDATE_RESTART,
        Part::Retry => ids::UPDATE_RETRY,
        Part::ViewRelease => ids::UPDATE_VIEW,
        Part::Dismiss => ids::UPDATE_DISMISS,
        Part::LogCopy => ids::LOG_COPY,
        Part::LogClear => ids::LOG_CLEAR,
        Part::LogAuto => ids::LOG_AUTO_SCROLL,
        _ => return None,
    })
}

pub struct MainWindow {
    ui: Rc<dyn Host>,
    window: Surface,
    form: Form<Part>,
    ctrl: RefCell<AppController>,
    /// Called once a wake's tick has run, so the next wake queues again.
    ticked: Box<dyn Fn()>,
    host: EffectHost,
    me: Weak<MainWindow>,
    inspect: Rc<InspectScreen>,
    backup: Rc<BackupScreen>,
    restore: Rc<RestoreScreen>,
    build: Rc<BuildScreen>,
    archives: Rc<ArchivesScreen>,
    #[cfg(feature = "optical")]
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

/// Answers to the app controller's own questions.
fn reply(me: Weak<MainWindow>) -> ReplyFn {
    Rc::new(move |reply| {
        let app = match me.upgrade() {
            Some(a) => a,
            None => return,
        };
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

impl MainWindow {
    /// The window, built and shown; `wake` reaches the controllers' jobs, `ticked` re-arms it.
    pub fn open(
        ui: Rc<dyn Host>,
        title: &str,
        wake: Arc<dyn Notify>,
        ticked: Box<dyn Fn()>,
    ) -> Rc<MainWindow> {
        let mut ctrl = AppController::new(wake);
        ctrl.load_devices();
        let gate = elevation_gate();
        let app = Rc::new_cyclic(|me: &Weak<MainWindow>| {
            let close_me = me.clone();
            let window = ui.window(
                title,
                1200,
                800,
                Box::new(move || {
                    if let Some(app) = close_me.upgrade() {
                        app.close_asked();
                    }
                }),
            );
            ui.set_min_size(window, 700, 460);
            let resize_me = me.clone();
            ui.on_resize(
                window,
                Box::new(move || {
                    if let Some(app) = resize_me.upgrade() {
                        app.relayout();
                    }
                }),
            );
            let fire = me.clone();
            let form = Form::with_host(
                ui.clone(),
                window,
                Rc::new(move |p| {
                    if let Some(app) = fire.upgrade() {
                        if let Some(id) = part_id(p) {
                            app.command(id);
                        }
                    }
                }),
            );
            let f = &form;
            f.button(Part::CloseBackup, "Close Backup", ids::CLOSE_BACKUP);
            f.show(Part::CloseBackup, false);
            f.button(Part::Refresh, "Refresh Devices", ids::REFRESH_DEVICES);
            f.show(
                Part::Refresh,
                matches!(gate, ElevationGate::NotApplicable | ElevationGate::Elevated),
            );
            f.button(Part::Elevate, elevation_button_label(), ids::ELEVATE);
            f.tooltip(Part::Elevate, elevation_button_hover());
            f.show(Part::Elevate, gate == ElevationGate::NeedsElevation);
            f.button(Part::Settings, "Settings...", ids::SETTINGS);
            f.button(Part::About, "About Rusty Backup", ids::ABOUT);
            f.label(Part::BannerText, "");
            f.button(
                Part::Install,
                "Download & Install Update",
                ids::UPDATE_INSTALL,
            );
            f.button(Part::Restart, "Restart Now", ids::UPDATE_RESTART);
            f.button(Part::Retry, "Retry", ids::UPDATE_RETRY);
            f.button(Part::ViewRelease, "View Release", ids::UPDATE_VIEW);
            f.button(
                Part::Dismiss,
                "Dismiss update notification",
                ids::UPDATE_DISMISS,
            );
            for p in [
                Part::BannerText,
                Part::Install,
                Part::Restart,
                Part::Retry,
                Part::ViewRelease,
                Part::Dismiss,
            ] {
                f.show(p, false);
            }
            f.label(Part::Status, "");
            ui.status_to(window, f.ctl(Part::Status));
            f.button(Part::LogCopy, "Copy Log", ids::LOG_COPY);
            f.button(Part::LogClear, "Clear Log", ids::LOG_CLEAR);
            f.check(Part::LogAuto, "Auto-scroll", ids::LOG_AUTO_SCROLL);
            f.text_view(Part::Log, ids::LOG_LIST);
            let page = |tab: AppTab| {
                let s = ui.page(window, f.custom(Part::Page(tab)));
                f.show(Part::Page(tab), false);
                s
            };
            let pane = |p: Part| {
                let s = ui.pane(window, f.custom(p));
                f.show(p, false);
                s
            };
            let host = EffectHost::new(ui.clone());
            let notify = ctrl.notify();
            let inspect_page = page(AppTab::Inspect);
            let inspect = InspectScreen::build(
                ui.clone(),
                inspect_page,
                pane(Part::InspectBrowse),
                pane(Part::InspectRemote),
                notify.clone(),
                ctrl.recent_handle(),
                host.clone(),
            );
            let backup = BackupScreen::build(
                ui.clone(),
                page(AppTab::Backup),
                notify.clone(),
                ctrl.recent_handle(),
                host.clone(),
            );
            let restore = RestoreScreen::build(
                ui.clone(),
                page(AppTab::Restore),
                notify.clone(),
                ctrl.recent_handle(),
                host.clone(),
            );
            let build = BuildScreen::build(
                ui.clone(),
                page(AppTab::Build),
                notify.clone(),
                host.clone(),
            );
            let archives = ArchivesScreen::build(
                ui.clone(),
                page(AppTab::Archives),
                notify.clone(),
                ctrl.recent_handle(),
                host.clone(),
            );
            #[cfg(feature = "optical")]
            let optical = OpticalScreen::build(
                ui.clone(),
                page(AppTab::Optical),
                pane(Part::OpticalBrowse),
                notify.clone(),
                ctrl.recent_handle(),
                host.clone(),
            );
            let convert =
                ConvertScreen::build(ui.clone(), page(AppTab::Convert), notify, host.clone());
            let items: Vec<ToolItem> = tabs()
                .iter()
                .map(|t| ToolItem {
                    id: screen_id(*t),
                    label: t.label().to_string(),
                    icon: t.icon().to_string(),
                })
                .collect();
            ui.toolbar(window, &items, ids::COMMANDER);
            let drop_me = me.clone();
            ui.on_drop(
                window,
                Rc::new(move |paths, x, y| {
                    if let Some(app) = drop_me.upgrade() {
                        app.dropped(paths, x, y);
                    }
                }),
            );
            MainWindow {
                ui: ui.clone(),
                window,
                form,
                ctrl: RefCell::new(ctrl),
                ticked,
                host,
                me: me.clone(),
                inspect,
                backup,
                restore,
                build,
                archives,
                #[cfg(feature = "optical")]
                optical,
                convert,
                settings: RefCell::default(),
                player_tick: Cell::new(false),
                commander: RefCell::default(),
                log_shown: Cell::new(0),
                banner_shape: RefCell::default(),
                sync: SyncGuard::default(),
            }
        });
        let me = app.me.clone();
        app.inspect.on_page(Rc::new(move || {
            if let Some(app) = me.upgrade() {
                app.refresh();
                app.relayout();
            }
        }));
        #[cfg(feature = "optical")]
        {
            let me = app.me.clone();
            app.optical.browse.on_change(Rc::new(move || {
                if let Some(app) = me.upgrade() {
                    app.optical.browser_changed();
                    app.refresh();
                    app.relayout();
                }
            }));
        }
        let me = app.me.clone();
        app.inspect.browse.on_change(Rc::new(move || {
            if let Some(app) = me.upgrade() {
                app.refresh();
                app.relayout();
            }
        }));
        #[cfg(feature = "update")]
        app.ctrl.borrow_mut().start_update_check();
        app.ui.present(app.window);
        app.relayout();
        app.tick();
        app
    }

    fn apply(&self, action: AppAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    /// Run the controllers, as a wake asks.
    pub fn tick(&self) {
        (self.ticked)();
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// Add lines to the log panel.
    pub fn append_log(&self, lines: Vec<(LogLevel, String)>) {
        if lines.is_empty() {
            return;
        }
        self.ctrl.borrow_mut().log.extend_lines(lines);
        let auto = self.ctrl.borrow().log_auto_scroll();
        self.sync_log(auto);
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &reply(self.me.clone()));
        if self.ctrl.borrow_mut().take_elevation_prompt() {
            self.host.run(
                vec![Effect::Confirm(elevation_confirm())],
                &reply(self.me.clone()),
            );
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
        #[cfg(feature = "optical")]
        self.optical.tick();
        self.convert.tick();
        self.share_backup();
        #[cfg(feature = "optical")]
        let converts = vec![self.inspect.take_convert(), self.optical.take_convert()];
        #[cfg(not(feature = "optical"))]
        let converts = vec![self.inspect.take_convert()];
        for path in converts.into_iter().flatten() {
            self.apply(AppAction::SelectTab(AppTab::Convert));
            self.convert.open(path);
        }
        #[cfg(feature = "optical")]
        if let Some(path) = self.inspect.take_optical() {
            self.apply(AppAction::SelectTab(AppTab::Optical));
            self.optical.open_image(path);
        }
        #[cfg(not(feature = "optical"))]
        let _ = self.inspect.take_optical();
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

    /// The window asked to close: Commander's staged edits would be lost, so its controller asks.
    fn close_asked(&self) {
        let held = self.commander.borrow().clone();
        match held.filter(|c| c.has_staged_edits()) {
            Some(c) => c.hold_quit(),
            None => self.ui.quit(),
        }
    }

    /// Files dropped on the window at `(x, y)`: Commander takes them by pane, else the screen on
    /// show routes them.
    fn dropped(&self, paths: Vec<PathBuf>, x: f64, y: f64) {
        let commander = self.commander.borrow().clone();
        match commander {
            Some(c) => {
                let under = [
                    rusty_backup::controller::commander::Side::Left,
                    rusty_backup::controller::commander::Side::Right,
                ]
                .into_iter()
                .find(|side| self.ui.hit(c.page(), c.list_ctl(*side), x, y));
                c.drop_paths(paths, under);
            }
            None => self.drop_on_tab(paths),
        }
    }

    /// A drop on the window: Inspect's open volume takes it in Edit Mode, else the route says.
    fn drop_on_tab(&self, paths: Vec<PathBuf>) {
        use rusty_backup::controller::app::{DropRoute, DropTargets};
        let first = match paths.first().cloned() {
            Some(p) => p,
            None => return,
        };
        let tab = self.ctrl.borrow().active_tab();
        if tab == AppTab::Inspect && self.inspect.drop_paths(paths) {
            return;
        }
        let (inspect_remote, inspect_loaded) = self.inspect.drop_state();
        let targets = DropTargets {
            inspect_remote,
            inspect_loaded,
            optical_browsing: self.optical_browsing(),
        };
        let route = self.ctrl.borrow_mut().route_drop(first, targets);
        let tab = match route {
            DropRoute::Inspect(path) => {
                self.inspect.open_image(path);
                AppTab::Inspect
            }
            #[cfg(feature = "optical")]
            DropRoute::Optical(path) => {
                self.optical.open_image(path);
                AppTab::Optical
            }
            #[cfg(not(feature = "optical"))]
            DropRoute::Optical(_) => return,
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

    fn optical_browsing(&self) -> bool {
        #[cfg(feature = "optical")]
        return self.optical.browse.is_active();
        #[cfg(not(feature = "optical"))]
        return false;
    }

    /// Files the desktop (or the command line) asks this app to open: Inspect opens them.
    pub fn open_files(&self, paths: Vec<PathBuf>) {
        for path in paths {
            if !path.exists() {
                continue;
            }
            self.apply(AppAction::SelectTab(AppTab::Inspect));
            self.inspect.open_image(path);
        }
    }

    /// Settings, one window at a time; Clear Recent Files reaches every screen's picker.
    fn open_settings(&self) {
        let open = self.settings.borrow().as_ref().is_some_and(|w| w.is_open());
        if open {
            return;
        }
        let recent = self.ctrl.borrow().recent_handle();
        let me = self.me.clone();
        let window = SettingsWindow::open(
            self.ui.clone(),
            recent,
            Box::new(move || {
                if let Some(app) = me.upgrade() {
                    app.forget_recents();
                }
            }),
        );
        *self.settings.borrow_mut() = Some(window);
    }

    fn forget_recents(&self) {
        self.backup.forget_recents();
        self.restore.forget_recents();
        #[cfg(feature = "optical")]
        self.optical.forget_recents();
        self.archives.forget_recents();
    }

    /// A playing track moves with no job to wake the shell, so a timer ticks while it plays.
    fn player_timer(&self) {
        #[cfg(feature = "optical")]
        {
            if !self.optical.playing() || self.player_tick.replace(true) {
                return;
            }
            let me = self.me.clone();
            self.ui.after(
                250,
                Box::new(move || {
                    if let Some(app) = me.upgrade() {
                        app.player_tick.set(false);
                        app.tick();
                    }
                }),
            );
        }
    }

    /// One disk job at a time: each screen hears who else runs.
    fn lock_screens(&self) {
        #[cfg(feature = "optical")]
        let optical = self.optical.is_running().then_some("Optical");
        #[cfg(not(feature = "optical"))]
        let optical: Option<&'static str> = None;
        let running = [
            ("Inspect", self.inspect.running_job()),
            ("Backup", self.backup.is_running().then_some("Backup")),
            ("Restore", self.restore.is_running().then_some("Restore")),
            (
                "Build Disk",
                self.build.is_running().then_some("Build Disk"),
            ),
            ("Optical", optical),
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
        #[cfg(feature = "optical")]
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
        let notify = self.ctrl.borrow().notify();
        let jobs = rusty_backup::controller::commander::JobRegistry::new();
        let (me, later) = (self.me.clone(), self.ui.clone());
        let on_close: Rc<dyn Fn()> = Rc::new(move || {
            // The screen may still be on the stack: drop it once the action in hand is done.
            let me = me.clone();
            later.later(Box::new(move || {
                if let Some(app) = me.upgrade() {
                    app.close_commander();
                }
            }));
        });
        let c = self.form.custom(Part::Commander);
        let page = self.ui.pane(self.window, c);
        let screen = CommanderScreen::build(
            self.ui.clone(),
            page,
            &jobs,
            &notify,
            self.host.clone(),
            on_close,
        );
        *self.commander.borrow_mut() = Some(screen);
        self.ui.show_toolbar(self.window, false);
        self.relayout();
    }

    fn close_commander(&self) {
        let screen = match self.commander.borrow_mut().take() {
            Some(s) => s,
            None => return,
        };
        drop(screen);
        self.form.remove(|p| *p == Part::Commander);
        self.ui.show_toolbar(self.window, true);
        self.refresh();
        self.relayout();
    }

    /// An action from a control, menu item or toolbar item, by its controller id.
    pub fn command(&self, id: &str) {
        if self.sync.active() {
            return;
        }
        if let Some(tab) = tab_of(id) {
            self.apply(AppAction::SelectTab(tab));
            return;
        }
        if id == ids::COMMANDER {
            self.open_commander();
            return;
        }
        #[cfg(feature = "optical")]
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
                self.append_log(vec![(LogLevel::Info, "Backup closed".to_string())]);
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
            ISSUES => self.host.run(
                vec![Effect::OpenUrl(about::ISSUES.to_string())],
                &reply(self.me.clone()),
            ),
            WEBSITE => self.host.run(
                vec![Effect::OpenUrl(about::WEBSITE.to_string())],
                &reply(self.me.clone()),
            ),
            _ => {}
        }
    }

    /// The controller's About text as a note.
    fn about(&self) {
        let title = format!("About {}", about::NAME);
        let text = format!(
            "{} {}\n\n{}\n\n{}\n{}",
            about::NAME,
            app_version(),
            about::SUMMARY,
            about::DEVELOPER,
            about::WEBSITE
        );
        // Opened from a turn of its own, like every other question.
        let ui = self.ui.clone();
        self.ui.later(Box::new(move || ui.note(&title, &text)));
    }

    /// Read the controller first and release it, then set views inside the guard.
    fn refresh(&self) {
        let f = &self.form;
        let (tab, shared, banner, auto) = {
            let c = self.ctrl.borrow();
            (
                c.active_tab(),
                c.shared_backup().is_some(),
                banner_of(&c),
                c.log_auto_scroll(),
            )
        };
        let banner_was = f.shown(Part::BannerText);
        let shared_was = f.shown(Part::CloseBackup);
        let covered = self.commander.borrow().is_some();
        self.sync.run(|| {
            f.show(Part::CloseBackup, shared && !covered);
            let shows = |p: Part| banner.as_ref().is_some_and(|b| b.buttons.contains(&p));
            f.show(Part::BannerText, banner.is_some() && !covered);
            for p in [
                Part::Install,
                Part::Restart,
                Part::Retry,
                Part::ViewRelease,
                Part::Dismiss,
            ] {
                f.show(p, shows(p) && !covered);
            }
            if let Some(b) = &banner {
                f.set_text(Part::BannerText, &b.text);
            }
            self.ui.select_tool(self.window, &screen_id(tab));
            f.set_check(Part::LogAuto, auto);
            // A browse view stands in its page's place while it is open.
            let browsing = self.inspect.browse.is_active();
            let picking = !browsing && self.inspect.remote.is_active();
            let disc = self.optical_browsing();
            for t in tabs() {
                let inspect = (browsing || picking) && t == AppTab::Inspect;
                let over = inspect || (disc && t == AppTab::Optical);
                f.show(Part::Page(t), t == tab && !over && !covered);
            }
            f.show(
                Part::InspectBrowse,
                browsing && tab == AppTab::Inspect && !covered,
            );
            f.show(
                Part::InspectRemote,
                picking && tab == AppTab::Inspect && !covered,
            );
            if cfg!(feature = "optical") {
                f.show(
                    Part::OpticalBrowse,
                    disc && tab == AppTab::Optical && !covered,
                );
            }
        });
        if banner_was != banner.is_some() || shared_was != shared || self.banner_buttons_moved() {
            self.relayout();
        }
        self.sync_log(auto);
        let commander = covered;
        self.ui.sync_menu(&|id| match id {
            ids::COMMANDER => Some(!commander),
            ids::CLOSE_BACKUP => Some(shared),
            ids::LOG_CLEAR | ids::LOG_AUTO_SCROLL => Some(true),
            _ if tab_of(id).is_some() => Some(true),
            _ => None,
        });
    }

    /// Append the log lines not shown yet; a cleared log starts over.
    fn sync_log(&self, auto: bool) {
        let text = {
            let c = self.ctrl.borrow();
            let entries = c.log.entries();
            if entries.len() < self.log_shown.get() {
                self.form.set_text_view(Part::Log, "");
                self.log_shown.set(0);
            }
            let mut text = String::new();
            for e in &entries[self.log_shown.get()..] {
                text.push_str(&e.line());
                text.push('\n');
            }
            self.log_shown.set(entries.len());
            text
        };
        if !text.is_empty() {
            self.form.append_text_view(Part::Log, &text, auto);
        }
    }

    /// Which banner buttons show changed since the last layout, so the row must be laid out again.
    fn banner_buttons_moved(&self) -> bool {
        let now: Vec<bool> = [Part::Install, Part::Restart, Part::Retry, Part::ViewRelease]
            .iter()
            .map(|p| self.form.shown(*p))
            .collect();
        self.banner_shape.replace(now.clone()) != now
    }

    /// Place every view for the window's content area.
    fn relayout(&self) {
        let f = &self.form;
        let area = self.ui.area(self.window);
        if let Some(c) = &*self.commander.borrow() {
            // Commander covers the window; what is under it hides until it closes.
            let all = [
                Part::Refresh,
                Part::Elevate,
                Part::Settings,
                Part::About,
                Part::Status,
                Part::LogCopy,
                Part::LogClear,
                Part::LogAuto,
                Part::Log,
                Part::InspectBrowse,
                Part::InspectRemote,
            ];
            for p in all {
                f.show(p, false);
            }
            for t in tabs() {
                f.show(Part::Page(t), false);
            }
            if cfg!(feature = "optical") {
                f.show(Part::OpticalBrowse, false);
            }
            f.place(
                &Node::leaf(Part::Commander, Size::Stretch(1), Size::Stretch(1)),
                area,
            );
            c.layout();
            return;
        }
        // After Commander: show the parts refresh does not manage.
        for p in [
            Part::Settings,
            Part::About,
            Part::Status,
            Part::LogCopy,
            Part::LogClear,
            Part::LogAuto,
            Part::Log,
        ] {
            f.show(p, true);
        }
        let gate = elevation_gate();
        f.show(
            Part::Refresh,
            matches!(gate, ElevationGate::NotApplicable | ElevationGate::Elevated),
        );
        f.show(Part::Elevate, gate == ElevationGate::NeedsElevation);
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
            Node::leaf(
                p(Part::Page(AppTab::Inspect)),
                Size::Stretch(1),
                Size::Stretch(3),
            ),
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
        let keep = |x: &Part| matches!(x, Part::Page(_)) || f.shown(*x);
        let tree = match tree.prune(&keep) {
            Some(t) => t,
            None => return,
        };
        let placed = layout::layout(&tree, area, 96);
        for (part, r) in &placed {
            if let Part::Page(_) = part {
                // Every page and every view that stands in a page's place shares the page place.
                let mut over = vec![Part::InspectBrowse, Part::InspectRemote];
                if cfg!(feature = "optical") {
                    over.push(Part::OpticalBrowse);
                }
                let places: Vec<Node<Part>> = tabs()
                    .into_iter()
                    .map(Part::Page)
                    .chain(over)
                    .map(|k| Node::leaf(k, Size::Stretch(1), Size::Stretch(1)))
                    .collect();
                for n in &places {
                    f.place(n, *r);
                }
                self.inspect.relayout();
                self.backup.relayout();
                self.restore.relayout();
                self.build.relayout();
                self.archives.relayout();
                self.convert.relayout();
                self.inspect.browse.layout();
                self.inspect.remote.layout();
                #[cfg(feature = "optical")]
                {
                    self.optical.relayout();
                    self.optical.browse.layout();
                }
                continue;
            }
            f.place(&Node::leaf(*part, Size::Stretch(1), Size::Stretch(1)), *r);
        }
    }
}

/// What the update banner says, and which of its buttons show.
struct Banner {
    text: String,
    buttons: Vec<Part>,
}

/// Without the in-app update there is nothing to announce.
#[cfg(not(feature = "update"))]
fn banner_of(_c: &AppController) -> Option<Banner> {
    None
}

/// The banner from the app controller: the announced release, then the in-app install's state.
#[cfg(feature = "update")]
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
            format!("Update: {}", why),
            vec![Part::ViewRelease, Part::Dismiss],
        ),
        Some(InstallState::Failed(e)) => (
            format!("Update failed: {}", e),
            vec![Part::Retry, Part::ViewRelease],
        ),
    };
    Some(Banner { text, buttons })
}
