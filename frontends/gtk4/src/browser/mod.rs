//! The file browser: [`pane::PaneView`] over a `CommanderController`, and the Inspect browse view.
//!
//! `src/gui/browse_view.rs` is the reference for [`BrowseView`]. The pane, the detail panel and
//! the dialogs read a controller and send actions, so they know nothing of Inspect; Commander
//! reuses them. The metadata editors are slice 7 and stay greyed out.

mod bars;
mod detail;
pub mod dialogs;
mod info;
pub mod pane;
mod tree;

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use gtk::glib;
use rusty_backup::controller::app::{CommanderRecent, Notify, Snapshot, UserConfig};
use rusty_backup::controller::browser::ids;
use rusty_backup::controller::commander::{
    BrowserPaneAction as P, CommanderAction as A, CommanderController, DisplayRow, ExportFormat,
    JobRegistry, ModeAction, PaneProfile, PaneServices, RowLabel, RowMenuEntry, Side, FOLLOW_DELAY,
};
use rusty_backup::controller::inspect::{BrowseEdit, BrowseOpen, InspectAction, InspectSource};
use rusty_backup::controller::keys::{Command, Key, KeyMap, Mods};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::optical::{disc_browser, DiscSource};
use rusty_backup::controller::{Controller, Effect};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::progress::JobList;
use crate::sync::SyncGuard;
use crate::widgets::{keep_focus, plain, set_text, typing};

pub use bars::view_popover;
use bars::{DiscHeader, DiscShown, EditBar, EditShown, Header, HeaderShown, MarkBar, MarkShown};
use detail::{Detail, DetailShown};
use dialogs::{present_editor, present_journal, Dialogs, DialogsShown, KeyFn};
pub use info::{FileInfo, InfoShown};
use pane::{PaneHost, PaneShown, PaneView};

/// Sends an action to the screen's controller; a change a sync made is dropped here.
pub type SendFn = Rc<dyn Fn(A)>;

/// The controller of the Inspect browse view, or of the Optical tab's disc browser (`disc`).
fn new_controller(notify: &Arc<dyn Notify>, disc: bool) -> CommanderController {
    let services = PaneServices {
        jobs: JobRegistry::new(),
        notify: Arc::clone(notify),
        recent: Box::new(CommanderRecent(UserConfig)),
    };
    let (keymap, problems) = KeyMap::load(KeyMap::user_file().as_deref());
    let mut ctl = match disc {
        true => disc_browser(services, keymap),
        false => CommanderController::single(services, keymap, PaneProfile::inspect()),
    };
    ctl.report_keymap_problems(problems);
    ctl
}

/// The browse view: the single pane with Inspect's edit modes around it.
pub struct BrowseView {
    ctrl: RefCell<CommanderController>,
    notify: Arc<dyn Notify>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<BrowseView>,
    active: Cell<bool>,
    /// The Optical tab's disc browser: its own header, and only Extract beside the listing.
    disc: bool,
    root: gtk::Box,
    header: Header,
    disc_header: DiscHeader,
    edit: EditBar,
    marks: MarkBar,
    banner: adw::Banner,
    jobs: JobList,
    pane: Rc<PaneView>,
    detail: Rc<Detail>,
    status: gtk::Label,
    dialogs: Rc<Dialogs>,
    label_prefix: RefCell<String>,
    host_jobs: RefCell<Vec<Snapshot>>,
    generation: Cell<u64>,
    follow_timer: Cell<bool>,
    /// The user asked for the File Info window, beyond what the side panel shows.
    info_asked: Cell<bool>,
    on_change: RefCell<Option<Rc<dyn Fn()>>>,
}

/// Everything a refresh shows, read while the controller is borrowed and shown after it is not.
struct Shown {
    header: HeaderShown,
    disc_header: Option<DiscShown>,
    edit: EditShown,
    marks: MarkShown,
    jobs: Vec<(Snapshot, bool)>,
    pane: PaneShown,
    detail: DetailShown,
    dialogs: DialogsShown,
    status: String,
    menu_up: bool,
    follow: bool,
}

/// The pane's window onto its host: it holds no strong reference, so the two can be built apart.
struct Host(Weak<BrowseView>);

impl PaneHost for Host {
    fn send(&self, action: A) {
        if let Some(view) = self.0.upgrade() {
            view.send(action);
        }
    }

    fn label(&self, side: Side, row: &DisplayRow) -> RowLabel {
        let view = self.0.upgrade().expect("the pane's host");
        let ctl = view.ctrl.borrow();
        ctl.pane(side).row_label(row, false)
    }

    fn menu(&self, side: Side, row: &DisplayRow) -> Vec<RowMenuEntry> {
        let view = self.0.upgrade().expect("the pane's host");
        let ctl = view.ctrl.borrow();
        ctl.row_menu(side, row)
    }

    fn click(&self, side: Side, row: &DisplayRow, ctrl: bool, shift: bool) -> Option<P> {
        let view = self.0.upgrade()?;
        let ctl = view.ctrl.borrow();
        ctl.pane(side).row_click(row, ctrl, shift)
    }

    fn context(&self, side: Side, row: &DisplayRow) -> Option<P> {
        let view = self.0.upgrade()?;
        let ctl = view.ctrl.borrow();
        ctl.pane(side).row_context(row)
    }

    fn key(&self, _: Side, key: Key, mods: Mods) -> bool {
        self.0.upgrade().is_some_and(|v| v.key(key, mods))
    }
}

impl BrowseView {
    pub fn build(notify: Arc<dyn Notify>, host: EffectHost) -> Rc<BrowseView> {
        Self::make(notify, host, false)
    }

    /// The Optical tab's disc browser: the same view over `PaneProfile::optical`.
    pub fn disc(notify: Arc<dyn Notify>, host: EffectHost) -> Rc<BrowseView> {
        Self::make(notify, host, true)
    }

    fn make(notify: Arc<dyn Notify>, host: EffectHost, disc: bool) -> Rc<BrowseView> {
        Rc::new_cyclic(|me: &Weak<BrowseView>| {
            let target = me.clone();
            let send: SendFn = Rc::new(move |action| {
                if let Some(view) = target.upgrade() {
                    view.send(action);
                }
            });
            let target = me.clone();
            let items = Rc::new(move || {
                let view = target.upgrade();
                view.map(|v| v.ctrl.borrow().view_menu())
                    .unwrap_or_default()
            });
            let target = me.clone();
            let extract_all = Rc::new(move || {
                if let Some(view) = target.upgrade() {
                    view.extract("/".to_string());
                }
            });
            let header = Header::build(&send, items.clone(), extract_all);
            let disc_header = DiscHeader::build(&send, items);
            header.root().set_visible(!disc);
            disc_header.root().set_visible(disc);
            let target = me.clone();
            let add_files = Rc::new(move |done: Rc<dyn Fn(Vec<PathBuf>)>| {
                if let Some(view) = target.upgrade() {
                    view.host
                        .pick_files("Select files to add", move |paths| done(paths));
                }
            });
            let edit = EditBar::build(&send, add_files);
            let target = me.clone();
            let export = Rc::new(move |format: ExportFormat| {
                if let Some(view) = target.upgrade() {
                    view.export_marked(format);
                }
            });
            let marks = MarkBar::build(&send, export);

            let banner = adw::Banner::new("");
            banner.set_use_markup(false);
            banner.set_button_label(Some("OK"));
            banner.connect_button_clicked(|b| b.set_revealed(false));

            let target = me.clone();
            let jobs = JobList::new(
                ids::CANCEL_JOB,
                Rc::new(move || {
                    if let Some(view) = target.upgrade() {
                        view.cancel_job();
                    }
                }),
            );
            jobs.root().set_margin_start(8);
            jobs.root().set_margin_end(8);

            let pane = PaneView::build(Side::Left, Rc::new(Host(me.clone())));
            let detail = Detail::build(&send);
            detail.set_plain(disc);
            let split = gtk::Paned::builder()
                .orientation(gtk::Orientation::Horizontal)
                .start_child(pane.root())
                .end_child(detail.root())
                .shrink_start_child(false)
                .shrink_end_child(false)
                .position(640)
                .vexpand(true)
                .build();
            let status = gtk::Label::builder()
                .xalign(0.0)
                .ellipsize(gtk::pango::EllipsizeMode::End)
                .margin_start(8)
                .margin_end(8)
                .build();
            status.add_css_class("dim-label");

            let root = gtk::Box::new(gtk::Orientation::Vertical, 4);
            root.append(header.root());
            root.append(disc_header.root());
            root.append(&banner);
            root.append(jobs.root());
            root.append(edit.root());
            root.append(&split);
            // One fixed row, so marking a row never moves the listing under the pointer.
            let foot = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            foot.set_height_request(38);
            status.set_hexpand(true);
            foot.append(&status);
            foot.append(marks.root());
            root.append(&foot);

            keep_focus(&root);
            pane.watch_drags(&root);
            let target = me.clone();
            let keys: KeyFn = Rc::new(move |key, mods| {
                let view = target.upgrade();
                view.is_some_and(|v| v.key(key, mods))
            });
            let dialogs = Dialogs::new(host.clone(), send, &[Side::Left], keys);
            dialogs.set_plain_tree(disc);
            BrowseView {
                ctrl: RefCell::new(new_controller(&notify, disc)),
                notify,
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                active: Cell::new(false),
                disc,
                root,
                header,
                disc_header,
                edit,
                marks,
                banner,
                jobs,
                pane,
                detail,
                status,
                dialogs,
                label_prefix: RefCell::default(),
                host_jobs: RefCell::default(),
                generation: Cell::new(0),
                follow_timer: Cell::new(false),
                info_asked: Cell::new(false),
                on_change: RefCell::default(),
            }
        })
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn is_active(&self) -> bool {
        self.active.get()
    }

    /// Closing or switching the source would lose work.
    pub fn has_unsaved_edits(&self) -> bool {
        self.active.get() && self.ctrl.borrow().has_unsaved_edits()
    }

    /// Called when the view opens or closes, for the screen that shows it.
    pub fn on_change(&self, f: Rc<dyn Fn()>) {
        *self.on_change.borrow_mut() = Some(f);
    }

    fn changed(&self) {
        let f = self.on_change.borrow().clone();
        if let Some(f) = f {
            f();
        }
    }

    /// Jobs the host screen runs for this view, shown here since the view covers that screen.
    pub fn set_host_jobs(&self, jobs: Vec<Snapshot>) {
        *self.host_jobs.borrow_mut() = jobs;
    }

    /// Open the partition the Inspect controller chose, with its edit flow and header extras.
    pub fn open_request(&self, req: BrowseOpen) {
        self.close();
        let BrowseOpen {
            session,
            edit,
            label_prefix,
            backup_folder,
        } = req;
        let label = session
            .source_path
            .as_ref()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "partition".into());
        let fx = {
            let mut ctl = self.ctrl.borrow_mut();
            let fx = ctl.open_inspect(session, &label);
            match edit {
                BrowseEdit::ReadOnly => {}
                BrowseEdit::InPlace => ctl.inspect_edit_in_place(),
                BrowseEdit::Archive(ctx) => ctl.inspect_edit_archive(ctx),
            }
            if let Some(folder) = backup_folder {
                ctl.inspect_backup_folder(folder);
            }
            fx
        };
        *self.label_prefix.borrow_mut() = label_prefix.unwrap_or_default();
        self.active.set(true);
        self.after(fx);
        self.changed();
        self.pane.focus();
    }

    /// Browse a disc image or the disc in a drive.
    pub fn open_disc(&self, src: DiscSource) {
        self.close();
        let fx = self.ctrl.borrow_mut().open_disc(src);
        self.active.set(true);
        self.after(fx);
        self.changed();
        self.pane.focus();
    }

    /// The streamed backup's seekable cache is ready; the listing stays where it is.
    pub fn upgrade_seekable(&self, path: PathBuf) {
        if self.active.get() {
            self.apply(A::Mode(ModeAction::UpgradeSeekable(path)));
        }
    }

    pub fn close(&self) {
        if !self.active.replace(false) {
            return;
        }
        let fresh = new_controller(&self.notify, self.disc);
        // The old pane's readers go before its CHD diff or archive temp is removed.
        self.ctrl.replace(fresh).close_inspect();
        self.label_prefix.borrow_mut().clear();
        self.dialogs.clear();
        self.banner.set_revealed(false);
        self.generation.set(0);
        self.refresh();
        self.changed();
    }

    /// A finished "Open as Disk Image" copy, as the load the Inspect controller takes.
    pub fn take_nested_load(&self) -> Option<InspectAction> {
        let (path, guard) = self.ctrl.borrow_mut().take_nested_open()?;
        // A temp copy out of the browsed volume: kept out of the recents.
        Some(InspectAction::Load(InspectSource::Image {
            path,
            temp: Some(guard),
        }))
    }

    fn send(&self, action: A) {
        if !self.sync.active() {
            self.apply(action);
        }
    }

    pub fn apply(&self, action: A) {
        match action {
            A::OpenDetail(..) => self.info_asked.set(true),
            A::CloseDetail => self.info_asked.set(false),
            _ => {}
        }
        let fx = self.ctrl.borrow_mut().apply(action);
        self.after(fx);
    }

    pub fn tick(&self) {
        if self.active.get() {
            let fx = self.ctrl.borrow_mut().tick();
            self.after(fx);
        }
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(view) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => A::Picked { purpose, path },
                Reply::Confirmed { purpose, yes } => A::Confirmed { purpose, yes },
                Reply::Answered { purpose, text } => A::Answered { purpose, text },
                Reply::Chose { purpose, choice } => A::Chose { purpose, choice },
                Reply::CloseScreen => A::Mode(ModeAction::RequestClose),
            };
            view.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        if self.ctrl.borrow_mut().take_close_request() {
            self.close();
            return;
        }
        self.take_results();
        // A double-click or F3 needs no window: the side panel already shows it.
        if !self.info_asked.get() && self.ctrl.borrow().detail().is_some() {
            self.ctrl.borrow_mut().apply(A::CloseDetail);
        }
        self.refresh();
    }

    /// What jobs ended with: notices, a tree listing, a journal, a file for the editor.
    fn take_results(&self) {
        let (tree, journal, doc, notice, error, archive, generation) = {
            let mut ctl = self.ctrl.borrow_mut();
            let pane = ctl.host_pane();
            let tree = pane.take_tree_text();
            let journal = pane.take_journal();
            let archive = pane.take_archive_notice();
            let doc = ctl.take_text_doc();
            let notice = ctl.take_mode_notice();
            let error = ctl.take_mode_error();
            (
                tree,
                journal,
                doc,
                notice,
                error,
                archive,
                ctl.volume_generation(),
            )
        };
        if let Some(tree) = tree {
            self.dialogs.set_tree(tree);
        }
        if let Some(journal) = journal {
            present_journal(&journal, &self.host);
        }
        if let Some(doc) = doc {
            let me = self.me.clone();
            let save = Rc::new(move |name, folder, text, shape| {
                let view = me.upgrade().ok_or_else(String::new)?;
                view.save_text(name, folder, text, shape)
            });
            present_editor(doc, &self.host, save);
        }
        for line in [notice, archive, error].into_iter().flatten() {
            self.banner.set_title(&plain(&line));
            self.banner.set_revealed(true);
            crate::app::append_log(vec![(LogLevel::Info, line)]);
        }
        self.generation.set(generation);
    }

    /// Staged like any edit; a refusal (an unencodable character) keeps the window and the text.
    fn save_text(
        &self,
        name: String,
        folder: String,
        text: String,
        shape: rusty_backup::controller::commander::TextShape,
    ) -> Result<String, String> {
        let before = self.ctrl.borrow().pane(Side::Left).queue.len();
        let staged = format!("Staged the new {name}; Apply Edits writes it.");
        let save = P::SaveText {
            name,
            folder,
            text,
            shape,
        };
        self.apply(A::Pane(Side::Left, save));
        let ctl = self.ctrl.borrow();
        match ctl.pane(Side::Left).queue.len() > before {
            true => Ok(staged),
            false => Err(ctl.status().to_string()),
        }
    }

    fn extract(&self, path: String) {
        let options = self.detail.extract_options();
        self.apply(A::Extract {
            from: Side::Left,
            path,
            options,
            dest: None,
        });
    }

    fn export_marked(&self, format: ExportFormat) {
        let options = self.detail.extract_options();
        self.apply(A::SetForkMode(options.fork_mode));
        self.apply(A::Export {
            from: Side::Left,
            format: Some(format),
        });
    }

    fn cancel_job(&self) {
        let copying = self.ctrl.borrow().copy_job().is_some();
        self.apply(match copying {
            true => A::CancelCopy,
            false => A::Pane(Side::Left, P::CancelOpen),
        });
    }

    /// Run a key through the key map; the Extract command asks for what the panel's options say.
    pub fn key(&self, key: Key, mods: Mods) -> bool {
        if !self.active.get() || typing(&self.root) {
            return false;
        }
        let command = {
            let ctl = self.ctrl.borrow();
            if ctl.modal_open() || !ctl.binds(key, mods) {
                return false;
            }
            ctl.command_for(key, mods)
        };
        match command {
            Some(Command::Extract) => {
                let cursor = {
                    let ctl = self.ctrl.borrow();
                    ctl.side_detail(Side::Left).map(|d| d.entry.path.clone())
                };
                if let Some(path) = cursor {
                    self.extract(path);
                }
            }
            Some(Command::PageUp | Command::PageDown) => {
                self.apply(A::SetPageRows(Side::Left, self.pane.page_rows()));
                self.apply(A::Key(key, mods));
            }
            _ => self.apply(A::Key(key, mods)),
        }
        true
    }

    fn read(&self) -> Shown {
        let prefix = self.label_prefix.borrow();
        let ctl = self.ctrl.borrow();
        let pane = ctl.pane(Side::Left);
        let mut jobs: Vec<(Snapshot, bool)> = Vec::new();
        jobs.extend(pane.opening().map(|s| (s, true)));
        jobs.extend(ctl.copy_job().map(|(_, s)| (s, true)));
        jobs.extend(ctl.mode_working().map(|s| (s, false)));
        jobs.extend(pane.checking().map(|s| (s, false)));
        jobs.extend(pane.applying().map(|s| (s, false)));
        jobs.extend(pane.tool_working().map(|s| (s, false)));
        jobs.extend(pane.archive_working().map(|s| (s, false)));
        jobs.extend(pane.wrapper_opening().map(|(_, s)| (s, false)));
        let host_jobs = self.host_jobs.borrow();
        jobs.extend(host_jobs.iter().map(|s| (s.clone(), false)));
        let caps = ctl.browse_caps();
        Shown {
            header: Header::read(&ctl, &prefix),
            disc_header: self.disc.then(|| DiscHeader::read(&ctl)),
            edit: EditBar::read(&ctl),
            marks: MarkBar::read(&ctl),
            jobs,
            pane: self.pane.read(&ctl),
            detail: self.detail.read(&ctl, caps),
            dialogs: self.dialogs.read(&ctl),
            status: ctl.status().to_string(),
            menu_up: ctl.view_menu_shown(),
            follow: ctl.follow_pending(),
        }
    }

    /// Read the controller first and release it: setting a widget can fire its change signal.
    fn refresh(&self) {
        let shown = self.read();
        self.sniff();
        let follow = shown.follow;
        self.sync.run(|| self.show(shown));
        if follow {
            self.wake_for_follow();
        }
    }

    /// Ask whether the file under the cursor is a Mac archive, once.
    fn sniff(&self) {
        if self.disc {
            return;
        }
        let path = {
            let ctl = self.ctrl.borrow();
            let d = ctl.side_detail(Side::Left);
            let pane = ctl.pane(Side::Left);
            d.filter(|d| pane.wants_sniff(&d.entry))
                .map(|d| d.entry.path.clone())
        };
        if let Some(path) = path {
            let fx = self
                .ctrl
                .borrow_mut()
                .apply(A::Pane(Side::Left, P::SniffArchive(path)));
            self.host.run(fx, &self.reply());
        }
    }

    /// The cursor's preview starts after a rest with no job to wake the shell, so a one-shot does.
    fn wake_for_follow(&self) {
        if self.follow_timer.replace(true) {
            return;
        }
        let me = self.me.clone();
        glib::timeout_add_local_once(FOLLOW_DELAY, move || {
            if let Some(view) = me.upgrade() {
                view.follow_timer.set(false);
                view.tick();
            }
        });
    }

    fn show(&self, s: Shown) {
        self.header.show(s.header, s.menu_up && !self.disc);
        if let Some(disc) = s.disc_header {
            self.disc_header.show(disc, s.menu_up);
        }
        self.edit.show(s.edit);
        self.marks.show(s.marks);
        self.jobs.show(&s.jobs);
        self.pane.show(s.pane);
        self.detail.show(s.detail);
        self.dialogs.show(s.dialogs);
        set_text(&self.status, &s.status);
    }
}
