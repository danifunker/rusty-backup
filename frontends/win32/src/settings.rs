//! The Settings window over `controller::settings`; `src/gui/settings_dialog.rs` is the reference.
//!
//! The update check, the Windows file associations, and Clear Recent Files. Save writes
//! config.json (and registers or removes the associations); Cancel closes without saving.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use rusty_backup::controller::app::RecentHandle;
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::settings::{ids, SettingsAction, SettingsController};
use rusty_backup::controller::Controller as _;
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT};
use windows::Win32::UI::Controls::NMHDR;
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, ShowWindow, BN_CLICKED, EN_CHANGE, SW_SHOW,
};

use crate::build_disk::{checked, set_check, set_field, set_line};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;

const RECENT_NOTE: &str = "Forget the recently-opened file lists shown in the Inspect, \
    Restore, Optical, Archives, and Commander source pickers.";
const ASSOCIATIONS_NOTE: &str = "Adds Rusty Backup to the Windows \"Open with\" list for disk \
    image files (.img, .vhd, .chd, .adf, ...).";

pub struct SettingsWindow {
    ctrl: RefCell<SettingsController>,
    sync: SyncGuard,
    me: Weak<SettingsWindow>,
    window: HWND,
    updates: HWND,
    update_check: HWND,
    repo_caption: HWND,
    repo_url: HWND,
    associations_head: HWND,
    associations: HWND,
    associations_note: HWND,
    recent_head: HWND,
    recent_note: HWND,
    clear: HWND,
    status: HWND,
    file: HWND,
    save: HWND,
    cancel: HWND,
    /// Called once after Clear Recent Files, so the tabs drop their in-memory lists.
    on_cleared: Box<dyn Fn()>,
}

impl SettingsWindow {
    /// The window over the app's config store, loaded and shown.
    pub fn open(owner: HWND, recent: RecentHandle, on_cleared: Box<dyn Fn()>) -> Rc<Self> {
        let mut ctrl = SettingsController::with_store(Box::new(recent));
        ctrl.apply(SettingsAction::Open);
        let window = panel::dialog(owner, "Settings", 560, 440);
        let file = SettingsController::config_file()
            .map(|p| format!("Settings file: {}", p.display()))
            .unwrap_or_default();
        let me = Rc::new_cyclic(|me: &Weak<SettingsWindow>| SettingsWindow {
            ctrl: RefCell::new(ctrl),
            sync: SyncGuard::default(),
            me: me.clone(),
            window,
            updates: ui::label(window, "Update Check"),
            update_check: ui::checkbox(window, "Check for updates at startup", ids::UPDATE_CHECK),
            repo_caption: ui::cell_label(window, "GitHub repository URL:"),
            repo_url: ui::edit(window, ids::REPO_URL),
            associations_head: ui::label(window, "File Associations"),
            associations: ui::checkbox(
                window,
                "Associate disk image files with Rusty Backup",
                ids::FILE_ASSOCIATIONS,
            ),
            associations_note: ui::label(window, ASSOCIATIONS_NOTE),
            recent_head: ui::label(window, "Recent Files"),
            recent_note: ui::label(window, RECENT_NOTE),
            clear: ui::button(window, "Clear Recent Files", ids::CLEAR_RECENT),
            status: ui::label(window, ""),
            file: ui::path_label(window, &file),
            save: ui::button(window, "Save", ids::SAVE),
            cancel: ui::button(window, "Cancel", ids::CANCEL),
            on_cleared,
        });
        let weak: Weak<dyn Handler> = me.me.clone();
        panel::register(window, weak);
        me.refresh();
        me.layout();
        // SAFETY: showing the window we created.
        unsafe {
            let _ = ShowWindow(window, SW_SHOW);
        }
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
            panel::unregister(self.window);
            // SAFETY: the controller closed; the window goes with it.
            let _ = unsafe { DestroyWindow(self.window) };
            return;
        }
        self.refresh();
    }

    fn refresh(&self) {
        let c = self.ctrl.borrow();
        self.sync.run(|| {
            set_check(self.update_check, c.update_check());
            set_field(self.repo_url, c.repo_url());
            set_check(self.associations, c.file_associations());
            ui::set_text(self.clear, &c.clear_recent_label());
            ui::set_enabled(self.clear, c.can_clear_recent());
            let error = c.status().is_some_and(|m| m.starts_with("Error"));
            let tone = if error { Tone::Error } else { Tone::Success };
            set_line(self.status, c.status(), Some(tone));
        });
        drop(c);
        self.layout();
    }

    fn layout(&self) {
        let (area, dpi) = panel::client(self.window);
        let row_h = Size::Fixed(layout::ROW);
        let line =
            |h: HWND, n: i32| Node::leaf(h, Size::Stretch(1), Size::Fixed((layout::ROW - 8) * n));
        let mut col = vec![
            line(self.updates, 1),
            Node::leaf(self.update_check, Size::Stretch(1), row_h),
            Node::row(vec![
                Node::leaf(self.repo_caption, Size::Fixed(170), row_h),
                Node::field(self.repo_url),
            ]),
            Node::Space(Size::Fixed(8)),
            line(self.associations_head, 1),
            Node::leaf(self.associations, Size::Stretch(1), row_h),
            line(self.associations_note, 2),
            Node::Space(Size::Fixed(8)),
            line(self.recent_head, 1),
            line(self.recent_note, 2),
            Node::row(vec![
                Node::button(self.clear, 200),
                Node::Space(Size::Stretch(1)),
            ]),
        ];
        if ui::shown(self.status) {
            col.push(line(self.status, 1));
        }
        col.push(Node::Space(Size::Stretch(1)));
        col.push(line(self.file, 1));
        col.push(Node::row(vec![
            Node::Space(Size::Stretch(1)),
            Node::button(self.cancel, 90),
            Node::button(self.save, 90),
        ]));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in layout::layout(&tree, area, dpi) {
            if ui::shown(h) {
                ui::move_to(h, r);
            }
        }
    }
}

impl Handler for SettingsWindow {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        let action = match code {
            EN_CHANGE if from == self.repo_url => SettingsAction::SetRepoUrl(ui::text(from)),
            BN_CLICKED if from == self.update_check => {
                SettingsAction::SetUpdateCheck(checked(from))
            }
            BN_CLICKED if from == self.associations => {
                SettingsAction::SetFileAssociations(checked(from))
            }
            BN_CLICKED if from == self.clear => SettingsAction::ClearRecent,
            BN_CLICKED if from == self.save => SettingsAction::Save,
            BN_CLICKED if from == self.cancel => SettingsAction::Close,
            _ => return,
        };
        self.apply(action);
    }

    fn notify(&self, _hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {
        self.apply(SettingsAction::Close);
    }
}
