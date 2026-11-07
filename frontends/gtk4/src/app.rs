//! The application window: tabs, top bar and log panel, drawn from `AppController`.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use adw::prelude::*;
use rusty_backup::controller::app::{
    elevation_button_hover, elevation_button_label, elevation_confirm, elevation_gate, ids,
    AppAction, AppController, AppTab, ElevationGate,
};
use rusty_backup::controller::log::capture;
use rusty_backup::controller::{Controller, Effect};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::sync::SyncGuard;
use crate::wake::GtkWake;

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

/// Show the window, creating it on first use.
pub fn present(application: &adw::Application, _image: Option<PathBuf>) {
    if let Some(app) = APP.with(|a| a.borrow().clone()) {
        app.host_window().present();
        return;
    }
    let app = App::build(application);
    app.host_window().present();
    APP.with(|a| *a.borrow_mut() = Some(app));
    tick();
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

        let stack = adw::ViewStack::new();
        for (_, name, title, icon) in TABS {
            let page = adw::StatusPage::builder().title(title).build();
            stack.add_titled_with_icon(&page, Some(name), title, icon);
        }

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

        let toasts = adw::ToastOverlay::new();
        toasts.set_child(Some(&split));
        let view = adw::ToolbarView::new();
        view.add_top_bar(&header);
        view.set_content(Some(&toasts));

        let window = adw::ApplicationWindow::builder()
            .application(application)
            .title("Rusty Backup")
            .default_width(1200)
            .default_height(800)
            .content(&view)
            .build();
        window.set_size_request(700, 460);

        let app = Rc::new(App {
            ctrl: RefCell::new(ctrl),
            host: EffectHost::new(window, toasts),
            stack: stack.clone(),
            log_view,
            log_shown: Cell::new(0),
            auto_scroll: auto_scroll.clone(),
            sync: SyncGuard::default(),
        });

        let send = |action: fn() -> AppAction| {
            let app = app.clone();
            move |_: &gtk::Button| app.apply(action())
        };
        refresh.connect_clicked(send(|| AppAction::RefreshDevices));
        elevate.connect_clicked(send(|| AppAction::RequestElevation));
        copy.connect_clicked(send(|| AppAction::CopyLog));
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
        app
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
        self.refresh();
    }

    /// Read the controller first and release it: setting a widget can fire its change signal.
    fn refresh(&self) {
        let tab = self.ctrl.borrow().active_tab();
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
            buffer.insert(&mut end, &entry.line());
            buffer.insert(&mut end, "\n");
        }
        self.log_shown.set(entries.len());
        if self.auto_scroll.is_active() {
            let mark = buffer.create_mark(None, &buffer.end_iter(), false);
            self.log_view.scroll_mark_onscreen(&mark);
            buffer.delete_mark(&mark);
        }
    }
}
