//! The Settings window over `controller::settings`; the Win32 shell's `settings.rs` is the
//! reference. The update check and Clear Recent Files; Save writes config.json, Cancel closes
//! without saving. File associations are the Windows shell's (a Mac app declares its document
//! types in Info.plist), so this window leaves them out.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::NSWindow;
use rusty_backup::controller::app::RecentHandle;
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::settings::{ids, SettingsAction, SettingsController};
use rusty_backup::controller::Controller as _;

use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::sync::SyncGuard;
use crate::ui::{self, CloseDelegate};

const RECENT_NOTE: &str = "Forget the recently-opened file lists shown in the Inspect, \
    Restore, Optical, Archives, and Commander source pickers.";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    Updates,
    UpdateCheck,
    RepoCaption,
    RepoUrl,
    RecentHead,
    RecentNote,
    Clear,
    Status,
    File,
    Save,
    Cancel,
}

pub struct SettingsWindow {
    ctrl: RefCell<SettingsController>,
    sync: SyncGuard,
    window: Retained<NSWindow>,
    _delegate: Retained<CloseDelegate>,
    form: Form<K>,
    /// Called once after Clear Recent Files, so the screens drop their in-memory lists.
    on_cleared: Box<dyn Fn()>,
}

impl SettingsWindow {
    /// The window over the app's config store, loaded and shown.
    pub fn open(
        mtm: MainThreadMarker,
        recent: RecentHandle,
        on_cleared: Box<dyn Fn()>,
    ) -> Rc<SettingsWindow> {
        let mut ctrl = SettingsController::with_store(Box::new(recent));
        ctrl.apply(SettingsAction::Open);
        let me = Rc::new_cyclic(|me: &Weak<SettingsWindow>| {
            let close = me.clone();
            let (window, delegate) = ui::window(
                mtm,
                "Settings",
                560.0,
                360.0,
                Box::new(move || {
                    let close = close.clone();
                    crate::app::later(Box::new(move || {
                        if let Some(w) = close.upgrade() {
                            w.apply(SettingsAction::Close);
                        }
                    }));
                }),
            );
            let body = window.contentView().expect("the window's content view");
            let fire = me.clone();
            let form = Form::new(
                mtm,
                body,
                Rc::new(move |k| {
                    if let Some(w) = fire.upgrade() {
                        w.fired(k);
                    }
                }),
            );
            let f = &form;
            let file = SettingsController::config_file()
                .map(|p| format!("Settings file: {}", p.display()))
                .unwrap_or_default();
            f.label(K::Updates, "Update Check");
            f.check(
                K::UpdateCheck,
                "Check for updates at startup",
                ids::UPDATE_CHECK,
            );
            f.label(K::RepoCaption, "GitHub repository URL:");
            f.field(K::RepoUrl, ids::REPO_URL, "GitHub repository URL", false);
            f.label(K::RecentHead, "Recent Files");
            f.wrap(K::RecentNote, RECENT_NOTE);
            f.button(K::Clear, "Clear Recent Files", ids::CLEAR_RECENT);
            f.label(K::Status, "");
            f.label(K::File, &file);
            f.button(K::Save, "Save", ids::SAVE);
            f.button(K::Cancel, "Cancel", ids::CANCEL);
            ui::present(&window);
            SettingsWindow {
                ctrl: RefCell::new(ctrl),
                sync: SyncGuard::default(),
                window,
                _delegate: delegate,
                form,
                on_cleared,
            }
        });
        me.refresh();
        me
    }

    pub fn is_open(&self) -> bool {
        self.ctrl.borrow().is_open()
    }

    fn apply(&self, action: SettingsAction) {
        self.ctrl.borrow_mut().apply(action);
        if self.ctrl.borrow_mut().take_cleared_recents() {
            (self.on_cleared)();
        }
        if !self.ctrl.borrow().is_open() {
            self.window.orderOut(None);
            return;
        }
        self.refresh();
    }

    fn refresh(&self) {
        let f = &self.form;
        {
            let c = self.ctrl.borrow();
            self.sync.run(|| {
                f.set_check(K::UpdateCheck, c.update_check());
                f.set_text(K::RepoUrl, c.repo_url());
                f.set_title(K::Clear, &c.clear_recent_label());
                f.enable(K::Clear, c.can_clear_recent());
                let error = c.status().is_some_and(|m| m.starts_with("Error"));
                let tone = if error { Tone::Error } else { Tone::Success };
                f.line(K::Status, c.status(), Some(tone));
            });
        }
        self.layout();
    }

    fn layout(&self) {
        let Some(body) = self.window.contentView() else {
            return;
        };
        let row_h = Size::Fixed(layout::ROW);
        let line =
            |k: K, n: i32| Node::leaf(k, Size::Stretch(1), Size::Fixed((layout::ROW - 6) * n + 4));
        let tree = Node::inset(
            Node::column(vec![
                line(K::Updates, 1),
                Node::leaf(K::UpdateCheck, Size::Stretch(1), row_h),
                Node::row(vec![
                    Node::leaf(K::RepoCaption, Size::Fixed(170), row_h),
                    Node::field(K::RepoUrl),
                ]),
                Node::Space(Size::Fixed(8)),
                line(K::RecentHead, 1),
                line(K::RecentNote, 2),
                Node::row(vec![
                    Node::button(K::Clear, 200),
                    Node::Space(Size::Stretch(1)),
                ]),
                line(K::Status, 1),
                Node::Space(Size::Stretch(1)),
                line(K::File, 1),
                Node::row(vec![
                    Node::Space(Size::Stretch(1)),
                    Node::button(K::Cancel, 90),
                    Node::button(K::Save, 90),
                ]),
            ]),
            layout::MARGIN,
        );
        self.form.place_shown(tree, ui::area(&body));
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let f = &self.form;
        let action = match k {
            K::RepoUrl => SettingsAction::SetRepoUrl(f.text(k)),
            K::UpdateCheck => SettingsAction::SetUpdateCheck(f.checked(k)),
            K::Clear => SettingsAction::ClearRecent,
            K::Save => SettingsAction::Save,
            K::Cancel => SettingsAction::Close,
            _ => return,
        };
        self.apply(action);
    }
}

impl Drop for SettingsWindow {
    fn drop(&mut self) {
        self.window.setDelegate(None);
        self.window.orderOut(None);
    }
}
