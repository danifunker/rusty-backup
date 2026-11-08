//! The Settings window over `controller::settings`; `src/gui/settings_dialog.rs` is the reference.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use adw::prelude::*;
use rusty_backup::controller::app::RecentHandle;
use rusty_backup::controller::settings::{ids, SettingsAction, SettingsController};
use rusty_backup::controller::Controller as _;

use crate::browser::dialogs::{page, window, wrapped};
use crate::form::{button, group, set_entry, set_line, set_switch, switch};
use crate::sync::SyncGuard;
use crate::volume::button_row;

const RECENT_NOTE: &str = "Forget the recently-opened file lists shown in the Inspect, \
    Restore, Optical, Archives, and Commander source pickers.";

struct Shown {
    open: bool,
    update_check: bool,
    repo_url: String,
    clear_label: String,
    can_clear: bool,
    status: Option<String>,
}

pub struct SettingsWindow {
    ctrl: RefCell<SettingsController>,
    sync: SyncGuard,
    dialog: adw::Dialog,
    update_check: adw::SwitchRow,
    repo_url: adw::EntryRow,
    clear: gtk::Button,
    status: gtk::Label,
    /// Called once after Clear Recent Files, so the tabs drop their in-memory lists.
    on_cleared: Box<dyn Fn()>,
}

impl SettingsWindow {
    /// The window over the app's config store, loaded and ready to present.
    pub fn open(recent: RecentHandle, on_cleared: Box<dyn Fn()>) -> Rc<SettingsWindow> {
        let mut ctrl = SettingsController::with_store(Box::new(recent));
        ctrl.apply(SettingsAction::Open);

        let update_check = switch("Check for updates at startup", ids::UPDATE_CHECK);
        let repo_url = adw::EntryRow::builder()
            .title("GitHub repository URL (e.g., https://github.com/owner/repo)")
            .build();
        repo_url.set_widget_name(ids::REPO_URL);
        let updates = group("Update Check");
        updates.add(&update_check);
        updates.add(&repo_url);

        let clear = button("Clear Recent Files", ids::CLEAR_RECENT);
        clear.set_halign(gtk::Align::Start);
        let recents = group("Recent Files");
        recents.set_description(Some(RECENT_NOTE));
        recents.add(&clear);

        let status = crate::form::line();
        let save = button("Save", ids::SAVE);
        save.add_css_class("suggested-action");
        let cancel = button("Cancel", ids::CANCEL);

        let body = page();
        body.set_spacing(12);
        body.append(&updates);
        body.append(&recents);
        body.append(&status);
        body.append(&button_row(&[&cancel, &save]));
        if let Some(path) = SettingsController::config_file() {
            let where_ = wrapped(&format!("Settings file: {}", path.display()));
            where_.add_css_class("dim-label");
            body.append(&where_);
        }
        let dialog = window("Settings", 520, 460, &body);

        let me = Rc::new(SettingsWindow {
            ctrl: RefCell::new(ctrl),
            sync: SyncGuard::default(),
            dialog: dialog.clone(),
            update_check: update_check.clone(),
            repo_url: repo_url.clone(),
            clear: clear.clone(),
            status,
            on_cleared,
        });

        let send = |me: &Rc<SettingsWindow>| {
            let weak: Weak<SettingsWindow> = Rc::downgrade(me);
            move |action: SettingsAction| {
                if let Some(me) = weak.upgrade() {
                    if !me.sync.active() {
                        me.apply(action);
                    }
                }
            }
        };
        let s = send(&me);
        update_check
            .connect_active_notify(move |row| s(SettingsAction::SetUpdateCheck(row.is_active())));
        let s = send(&me);
        repo_url.connect_changed(move |row| s(SettingsAction::SetRepoUrl(row.text().to_string())));
        let s = send(&me);
        clear.connect_clicked(move |_| s(SettingsAction::ClearRecent));
        let s = send(&me);
        save.connect_clicked(move |_| s(SettingsAction::Save));
        let d = dialog.clone();
        cancel.connect_clicked(move |_| {
            d.close();
        });
        let weak = Rc::downgrade(&me);
        dialog.connect_closed(move |_| {
            if let Some(me) = weak.upgrade() {
                me.ctrl.borrow_mut().apply(SettingsAction::Close);
            }
        });
        me.refresh();
        me
    }

    pub fn dialog(&self) -> &adw::Dialog {
        &self.dialog
    }

    pub fn is_open(&self) -> bool {
        self.ctrl.borrow().is_open()
    }

    fn apply(&self, action: SettingsAction) {
        self.ctrl.borrow_mut().apply(action);
        if self.ctrl.borrow_mut().take_cleared_recents() {
            (self.on_cleared)();
        }
        self.refresh();
    }

    fn read(&self) -> Shown {
        let ctrl = self.ctrl.borrow();
        Shown {
            open: ctrl.is_open(),
            update_check: ctrl.update_check(),
            repo_url: ctrl.repo_url().to_string(),
            clear_label: ctrl.clear_recent_label(),
            can_clear: ctrl.can_clear_recent(),
            status: ctrl.status().map(str::to_string),
        }
    }

    fn refresh(&self) {
        let s = self.read();
        if !s.open {
            return;
        }
        self.sync.run(|| {
            set_switch(&self.update_check, s.update_check);
            set_entry(&self.repo_url, &s.repo_url);
            if self.clear.label().as_deref() != Some(s.clear_label.as_str()) {
                self.clear.set_label(&s.clear_label);
            }
            self.clear.set_sensitive(s.can_clear);
            let error = s.status.as_deref().is_some_and(|m| m.starts_with("Error"));
            self.status
                .remove_css_class(if error { "success" } else { "error" });
            self.status
                .add_css_class(if error { "error" } else { "success" });
            set_line(&self.status, s.status.as_deref());
        });
    }
}
