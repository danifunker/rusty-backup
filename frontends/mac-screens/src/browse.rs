//! The Inspect browse view over a single-pane `CommanderController`; the Win32 shell's
//! `browse.rs` and `browse_bars.rs` are the reference.
//!
//! The volume line and the header's tools, the edit bar while Edit Mode is on, the running job
//! with Cancel, notices, the folder tree and the listing ([`super::pane_view`]), the side panel
//! over a text or hex preview, the status line and the mark bar, the View and row menus, and the
//! windows the controller describes ([`super::model_window`]). Keys go through the controller's
//! key map. Every control's action comes to [`BrowseView::fired`] by its key.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::{CommanderRecent, Notify, Snapshot, UserConfig};
use rusty_backup::controller::browser::{ids, TextDoc, CLOSE_LABEL};
use rusty_backup::controller::commander::{
    editor_window, extract_fork_modes, journal_window, tree_window, ArchiveUse,
    BrowserPaneAction as P, CommanderAction as A, CommanderController, EditBar, ExportBar,
    ExportFormat, ExtractOptions, HeaderTools, JobRegistry, MarkBar, ModeAction, PaneProfile,
    PaneServices, PaneWindow, PanelActions, PanelPreview, ProdosExportMode, ResourceForkMode, Side,
    SidePanel, Tone, TreeText, VolumeLine, WindowKind, FOLLOW_DELAY,
};
use rusty_backup::controller::inspect::{
    ids as inspect_ids, BrowseEdit, BrowseOpen, InspectAction, InspectSource,
};
use rusty_backup::controller::keys::{Command, Key, KeyMap, Mods};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::{picks, Controller, Effect};

use super::effects::{EffectHost, Reply, ReplyFn};
use super::form::Form;
use super::host::{Host, Surface};
use super::layout::{self, Node, Size};
use super::model_window::{Event, ModelWindow};
use super::pane_view::{PaneEvent, PaneShown, PaneView};
use super::popup::{track, MenuItem};
use super::sync::SyncGuard;

/// The side panel's width and the folder tree's, in points.
const DETAIL_W: i32 = 330;
const TREE_W: i32 = 200;

/// The Mac archive formats the side panel exports to, with their menu labels.
const MAC_FORMATS: [(ExportFormat, &str); 4] = [
    (ExportFormat::BinHex, "BinHex (.hqx)"),
    (ExportFormat::StuffIt, "StuffIt (.sit)"),
    (ExportFormat::StuffItHqx, "StuffIt over BinHex (.sit.hqx)"),
    (ExportFormat::MacArchive, "MAR (.mar)"),
];

/// Every control of the view, as its tag and its place in the layout.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum C {
    Title,
    Partition,
    Volume,
    Boot,
    EditMode,
    ExtractAll,
    ExportVolume,
    VolumeFormat,
    Check,
    Journal,
    ChdInfo,
    Tree,
    FullScan,
    ShowDeleted,
    View,
    Close,
    EditNote,
    EditTarget,
    AddFile,
    Import,
    NewFolder,
    Rename,
    Delete,
    Bless,
    BootBlocks,
    Lock,
    Unlock,
    Apply,
    Discard,
    Edits,
    Free,
    After,
    Job,
    Progress,
    Cancel,
    Notice,
    NoticeOk,
    Message,
    TreeView,
    List,
    Name,
    Facts,
    Note,
    Budget,
    Result,
    ForkCaption,
    Fork,
    Extract,
    Export,
    Nested,
    ProdosCaption,
    Prodos,
    RsToLf,
    SaveArchive,
    BrowseArchive,
    EditText,
    Metadata,
    MacArchive,
    Preview,
    Status,
    MarkCount,
    MarkClear,
}

/// The pane's controller: Inspect's browse profile.
/// The pane's controller: Inspect's browse profile, or the Optical screen's disc browser.
fn new_controller(notify: &Arc<dyn Notify>, disc: bool) -> CommanderController {
    let services = PaneServices {
        jobs: JobRegistry::new(),
        notify: Arc::clone(notify),
        recent: Box::new(CommanderRecent(UserConfig)),
    };
    let (keymap, problems) = KeyMap::load(KeyMap::user_file().as_deref());
    #[cfg(feature = "optical")]
    let mut ctl = match disc {
        true => rusty_backup::controller::optical::disc_browser(services, keymap),
        false => CommanderController::single(services, keymap, PaneProfile::inspect()),
    };
    // Without optical media there is no Optical screen, so no disc browser either.
    #[cfg(not(feature = "optical"))]
    let mut ctl = {
        let _ = disc;
        CommanderController::single(services, keymap, PaneProfile::inspect())
    };
    ctl.report_keymap_problems(problems);
    ctl
}

/// Everything a refresh shows, read while the controller is borrowed.
struct Shown {
    pane: PaneShown,
    loaded: bool,
    volume: VolumeLine,
    message: String,
    export_volume: bool,
    export: ExportBar,
    job: Option<(Snapshot, bool)>,
    panel_key: Option<(u64, bool)>,
    panel: Option<SidePanel>,
    result: Option<String>,
    actions: Option<PanelActions>,
    status: String,
    follow: bool,
    tools: HeaderTools,
    edit: EditBar,
    marks: MarkBar,
    windows: Vec<PaneWindow>,
    view_menu: bool,
}

pub struct BrowseView {
    ui: Rc<dyn Host>,
    ctrl: RefCell<CommanderController>,
    notify: Arc<dyn Notify>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<BrowseView>,
    active: Cell<bool>,
    /// The Optical screen's disc browser rather than Inspect's.
    disc: bool,
    /// The view's own page, which the Inspect page gives way to while it is open.
    page: Surface,
    form: Form<C>,
    pane: PaneView,
    windows: RefCell<Vec<Rc<ModelWindow>>>,
    /// The Check report's "Show debug messages", which only this view keeps.
    debug: Cell<bool>,
    /// The listing Tree handed over, shown until its window closes.
    tree_text: RefCell<Option<TreeText>>,
    journal: RefCell<Option<PaneWindow>>,
    /// The file Edit as text opened, and why its last Save was refused.
    editor: RefCell<Option<(TextDoc, Option<String>)>>,
    /// The user asked for the File Info window, beyond what the side panel shows.
    info_asked: Cell<bool>,
    panel_key: Cell<Option<(u64, bool)>>,
    label_prefix: RefCell<String>,
    follow_set: Cell<bool>,
    edits_shown: Cell<bool>,
    on_change: RefCell<Option<Rc<dyn Fn()>>>,
    /// The partition switcher's indices, in its order, and who hears a pick.
    partitions: RefCell<Vec<usize>>,
    on_partition: RefCell<Option<Rc<dyn Fn(usize)>>>,
}

impl BrowseView {
    /// The view on `page`, a surface its parent places.
    pub fn build(
        ui: Rc<dyn Host>,
        page: Surface,
        notify: Arc<dyn Notify>,
        host: EffectHost,
        disc: bool,
    ) -> Rc<BrowseView> {
        Rc::new_cyclic(|me: &Weak<BrowseView>| {
            let fire_me = me.clone();
            let form = Form::with_host(
                ui.clone(),
                page,
                Rc::new(move |c| {
                    if let Some(v) = fire_me.upgrade() {
                        v.fired(c);
                    }
                }),
            );
            let f = &form;
            f.label(C::Title, "Filesystem Browser");
            f.popup(C::Partition, &[], inspect_ids::BROWSE_PARTITION);
            f.show(C::Partition, false);
            f.label(C::Volume, "");
            f.label(C::Boot, "");
            f.button(C::EditMode, "Edit Mode", ids::EDIT_MODE);
            f.button(C::ExtractAll, "Extract All...", ids::EXTRACT_ALL);
            f.button(C::ExportVolume, "Export Volume...", ids::EXPORT_VOLUME);
            f.popup(C::VolumeFormat, &[], ids::EXPORT_VOLUME_FORMAT);
            f.button(C::Check, "Check", ids::CHECK);
            f.button(C::Journal, "Journal", ids::JOURNAL);
            f.button(C::ChdInfo, "CHD Info", ids::CHD_INFO);
            f.button(C::Tree, "Tree", ids::TREE);
            f.check(C::FullScan, "Full scan", ids::FULL_SCAN);
            f.check(C::ShowDeleted, "Show deleted", ids::SHOW_DELETED);
            f.button(C::View, "View...", ids::VIEW_MENU);
            f.button(C::Close, CLOSE_LABEL, ids::CLOSE);
            f.label(C::EditNote, "");
            f.set_tone(C::EditNote, Some(Tone::Warning));
            f.label(C::EditTarget, "");
            f.button(C::AddFile, "Add File...", ids::ADD_FILE);
            f.button(C::Import, "Import Archive...", ids::IMPORT_ARCHIVE);
            f.button(C::NewFolder, "New Folder...", ids::NEW_FOLDER);
            f.button(C::Rename, "Rename...", ids::RENAME);
            f.button(C::Delete, "Delete", ids::DELETE);
            f.button(C::Bless, "Bless Folder", ids::BLESS);
            f.button(C::BootBlocks, "Boot Blocks...", ids::BOOT_BLOCKS);
            f.button(C::Lock, "Lock", ids::PRODOS_LOCK);
            f.button(C::Unlock, "Unlock", ids::PRODOS_UNLOCK);
            f.button(C::Apply, "Apply Edits", ids::APPLY);
            f.button(C::Discard, "Discard", ids::DISCARD);
            f.button(C::Edits, "Edits", ids::EDITS);
            f.label(C::Free, "");
            f.label(C::After, "");
            f.label(C::Job, "");
            f.progress(C::Progress);
            f.button(C::Cancel, "Cancel", ids::CANCEL_JOB);
            // The notice row shows only while a notice is up (show_notice).
            f.label(C::Notice, "");
            f.show(C::Notice, false);
            f.button(C::NoticeOk, "OK", "browse.notice_ok");
            f.show(C::NoticeOk, false);
            f.label(C::Message, "");
            let pane = PaneView::build(
                f,
                C::List,
                C::TreeView,
                Side::Left,
                ids::LIST,
                ids::FOLDER_TREE,
            );
            f.label(C::Name, "");
            f.label(C::Facts, "");
            f.label(C::Note, "");
            f.label(C::Budget, "");
            f.label(C::Result, "");
            f.label(C::ForkCaption, "Resource forks:");
            let modes: Vec<String> = extract_fork_modes()
                .iter()
                .map(|m| m.label().to_string())
                .collect();
            f.popup(C::Fork, &modes, "");
            let start = extract_fork_modes()
                .iter()
                .position(|m| *m == ResourceForkMode::AppleDouble)
                .unwrap_or(0);
            f.select(C::Fork, start);
            f.button(C::Extract, "Extract...", ids::EXTRACT);
            f.button(C::Export, "Export to .tgz...", ids::EXPORT_ENTRY);
            f.button(C::Nested, "Open as Disk Image", ids::OPEN_NESTED);
            f.label(C::ProdosCaption, "Export name:");
            let prodos: Vec<String> = ProdosExportMode::ALL
                .iter()
                .map(|m| m.label().to_string())
                .collect();
            f.popup(C::Prodos, &prodos, "");
            f.check(C::RsToLf, "Text: RS to LF", "");
            f.button(C::SaveArchive, "Save as Mac archive...", ids::SAVE_ARCHIVE);
            f.button(C::BrowseArchive, "Browse archive...", ids::BROWSE_ARCHIVE);
            f.button(C::EditText, "Edit as text...", ids::EDIT_TEXT);
            f.button(C::Metadata, "Edit Metadata...", ids::EDIT_METADATA);
            f.button(
                C::MacArchive,
                "Export as Mac archive...",
                "browse.export_mac",
            );
            f.text_view(C::Preview, ids::DETAIL);
            f.label(C::Status, "");
            f.label(C::MarkCount, "");
            f.button(C::MarkClear, "Clear", ids::CLEAR_MARKS);
            let events_me = me.clone();
            pane.on_event(Rc::new(move |e| {
                if let Some(v) = events_me.upgrade() {
                    v.pane_event(e);
                }
            }));
            BrowseView {
                ui: ui.clone(),
                ctrl: RefCell::new(new_controller(&notify, disc)),
                disc,
                notify,
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                active: Cell::new(false),
                page,
                form,
                pane,
                windows: RefCell::default(),
                debug: Cell::new(false),
                tree_text: RefCell::default(),
                journal: RefCell::default(),
                editor: RefCell::default(),
                info_asked: Cell::new(false),
                panel_key: Cell::new(None),
                label_prefix: RefCell::default(),
                follow_set: Cell::new(false),
                edits_shown: Cell::new(false),
                on_change: RefCell::default(),
                partitions: RefCell::default(),
                on_partition: RefCell::default(),
            }
        })
    }

    fn shown(&self, c: C) -> bool {
        self.form.shown(c)
    }

    fn show_if(&self, c: C, on: bool) {
        self.form.show(c, on);
    }

    fn enable(&self, c: C, on: bool) {
        self.form.enable(c, on);
    }

    fn checked(&self, c: C) -> bool {
        self.form.checked(c)
    }

    fn set_check(&self, c: C, on: bool) {
        self.form.set_check(c, on);
    }

    fn set_title(&self, c: C, t: &str) {
        self.form.set_title(c, t);
    }

    pub fn is_active(&self) -> bool {
        self.active.get()
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

    /// Files dropped from Finder, staged into the browsed volume while it is in Edit Mode.
    pub fn drop_paths(&self, paths: Vec<PathBuf>) -> bool {
        if !self.active.get() || !self.ctrl.borrow().edit_mode() {
            return false;
        }
        self.apply(A::Pane(Side::Left, P::AddPicked(paths)));
        true
    }

    pub fn has_unsaved_edits(&self) -> bool {
        self.active.get() && self.ctrl.borrow().has_unsaved_edits()
    }

    /// Open the Optical screen's disc in the disc browser.
    #[cfg(feature = "optical")]
    pub fn open_disc(&self, src: rusty_backup::controller::optical::DiscSource) {
        self.close();
        let fx = self.ctrl.borrow_mut().open_disc(src);
        self.active.set(true);
        self.after(fx);
        self.changed();
        self.pane.focus();
    }

    /// Open the partition the Inspect controller chose, with its edit flow and label prefix.
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

    /// The streamed backup's seekable cache is ready; the listing stays where it is.
    pub fn upgrade_seekable(&self, path: PathBuf) {
        if self.active.get() {
            self.apply(A::Mode(ModeAction::UpgradeSeekable(path)));
        }
    }

    /// The owner's partitions for the header's switcher (hidden with fewer than two).
    pub fn set_partitions(&self, choices: &[(usize, String)], current: Option<usize>) {
        let show = choices.len() > 1;
        if show {
            let labels: Vec<String> = choices.iter().map(|(_, l)| l.clone()).collect();
            if self.form.items(C::Partition) != labels {
                self.form.set_items(C::Partition, &labels);
            }
            let at = choices.iter().position(|(i, _)| Some(*i) == current);
            if let Some(at) = at {
                self.form.select(C::Partition, at);
            }
            self.partitions.replace(choices.iter().map(|(i, _)| *i).collect());
        }
        if self.form.shown(C::Partition) != show {
            self.form.show(C::Partition, show);
            self.layout();
        }
    }

    /// Who hears the switcher's pick (the owner switches, asking first about staged edits).
    pub fn on_partition(&self, f: Rc<dyn Fn(usize)>) {
        self.on_partition.replace(Some(f));
    }

    pub fn close(&self) {
        if !self.active.replace(false) {
            return;
        }
        let fresh = new_controller(&self.notify, self.disc);
        // The old pane's readers go before its CHD diff or archive temp is removed.
        self.ctrl.replace(fresh).close_inspect();
        self.label_prefix.borrow_mut().clear();
        self.tree_text.replace(None);
        self.journal.replace(None);
        self.editor.replace(None);
        self.windows.borrow_mut().clear();
        self.show_notice(None);
        self.refresh();
        self.changed();
    }

    /// A finished "Open as Disk Image" copy, as the load the Inspect controller takes.
    pub fn take_nested_load(&self) -> Option<InspectAction> {
        let (path, guard) = self.ctrl.borrow_mut().take_nested_open()?;
        Some(InspectAction::Load(InspectSource::Image {
            path,
            temp: Some(guard),
        }))
    }

    fn apply(&self, action: A) {
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
        // The side panel already shows File Info, so the window a double-click asks for is not opened.
        if !self.info_asked.get() && self.ctrl.borrow().detail().is_some() {
            self.ctrl.borrow_mut().apply(A::CloseDetail);
        }
        self.refresh();
    }

    /// What jobs ended with: notices, a tree listing, a journal, a file for the text editor.
    fn take_results(&self) {
        let lines: Vec<String> = {
            let mut ctl = self.ctrl.borrow_mut();
            let pane = ctl.host_pane();
            if let Some(tree) = pane.take_tree_text() {
                *self.tree_text.borrow_mut() = Some(tree);
            }
            if let Some(j) = pane.take_journal() {
                *self.journal.borrow_mut() = Some(journal_window(&j));
            }
            let archive = pane.take_archive_notice();
            if let Some(doc) = ctl.take_text_doc() {
                *self.editor.borrow_mut() = Some((doc, None));
            }
            let notice = ctl.take_mode_notice();
            let error = ctl.take_mode_error();
            [notice, archive, error].into_iter().flatten().collect()
        };
        if let Some(last) = lines.last() {
            self.show_notice(Some(last));
        }
        self.ui
            .log(lines.into_iter().map(|l| (LogLevel::Info, l)).collect());
    }

    fn show_notice(&self, text: Option<&str>) {
        let text = text.filter(|t| !t.trim().is_empty());
        self.form.set_text(C::Notice, text.unwrap_or(""));
        self.show_if(C::Notice, text.is_some());
        self.show_if(C::NoticeOk, text.is_some());
        self.layout();
    }

    fn extract_options(&self) -> ExtractOptions {
        let i = self.form.picked(C::Fork).map_or(-1, |i| i as isize);
        let fork_mode = usize::try_from(i)
            .ok()
            .and_then(|i| extract_fork_modes().get(i).copied())
            .unwrap_or(ResourceForkMode::AppleDouble);
        let mode = usize::try_from(self.form.picked(C::Prodos).map_or(-1, |i| i as isize))
            .ok()
            .and_then(|i| ProdosExportMode::ALL.get(i).copied());
        ExtractOptions {
            fork_mode,
            prodos_suffix: mode != Some(ProdosExportMode::Plain),
            rs_to_lf: self.checked(C::RsToLf),
        }
    }

    fn extract(&self, path: String) {
        let options = self.extract_options();
        self.apply(A::Extract {
            from: Side::Left,
            path,
            options,
            dest: None,
        });
    }

    fn cursor_path(&self) -> Option<String> {
        let ctl = self.ctrl.borrow();
        ctl.side_detail(Side::Left).map(|d| d.entry.path.clone())
    }

    /// Run a key through the key map; true when it was bound.
    fn key(&self, key: Key, mods: Mods) -> bool {
        if !self.active.get() {
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
                if let Some(path) = self.cursor_path() {
                    self.extract(path);
                }
            }
            Some(Command::ExportVolume) => self.apply(A::ExportEntry {
                from: Side::Left,
                path: "/".to_string(),
                format: None,
            }),
            _ => self.apply(A::Key(key, mods)),
        }
        true
    }

    fn read(&self) -> Shown {
        let prefix = self.label_prefix.borrow();
        let ctl = self.ctrl.borrow();
        let pane = ctl.pane(Side::Left);
        let message = match (pane.opening(), pane.source_error()) {
            (Some(_), _) => "Reading the volume...".to_string(),
            (None, Some(err)) => err.to_string(),
            (None, None) if pane.is_opening() => "Opening...".to_string(),
            (None, None) => String::new(),
        };
        let mut jobs: Vec<(Snapshot, bool)> = Vec::new();
        jobs.extend(pane.opening().map(|s| (s, true)));
        jobs.extend(ctl.copy_job().map(|(_, s)| (s, true)));
        jobs.extend(ctl.mode_working().map(|s| (s, false)));
        jobs.extend(pane.checking().map(|s| (s, false)));
        jobs.extend(pane.applying().map(|s| (s, false)));
        jobs.extend(pane.tool_working().map(|s| (s, false)));
        jobs.extend(pane.archive_working().map(|s| (s, false)));
        let caps = ctl.browse_caps();
        let detail = ctl.side_detail(Side::Left);
        let panel_key = detail.map(|d| (d.serial, d.loading));
        Shown {
            loaded: pane.listing.is_loaded(),
            volume: ctl.volume_line(Side::Left, &prefix),
            message,
            export_volume: !ctl.edit_mode() && ctl.copy_job().is_none(),
            export: ctl.export_bar(Side::Left),
            job: jobs.into_iter().next(),
            panel: detail
                .filter(|_| self.panel_key.get() != panel_key)
                .map(|d| d.side_panel(caps)),
            panel_key,
            result: detail.and_then(|d| d.result.clone()),
            actions: ctl.panel_actions(Side::Left, false),
            status: ctl.status().to_string(),
            follow: ctl.follow_pending(),
            tools: ctl.header_tools(Side::Left),
            edit: ctl.edit_bar(Side::Left),
            marks: ctl.mark_bar(Side::Left),
            pane: self.pane.read(&ctl),
            windows: {
                let mut w = ctl.pane_windows(Side::Left, self.debug.get());
                if let Some(t) = &*self.tree_text.borrow() {
                    w.push(tree_window(t, false));
                }
                w.extend(self.journal.borrow().clone());
                if let Some((doc, err)) = &*self.editor.borrow() {
                    w.push(editor_window(doc, err.as_deref()));
                }
                w
            },
            view_menu: ctl.view_menu_shown(),
        }
    }

    /// Read the controller first and release it, then set the views inside the guard.
    fn refresh(&self) {
        let shown = self.read();
        let follow = shown.follow;
        let view_menu = shown.view_menu && self.active.get();
        self.sync.run(|| self.show(shown));
        self.layout();
        if view_menu {
            // The key map asked for the View menu: it opens once this refresh is done.
            let me = self.me.clone();
            self.ui.later(Box::new(move || {
                if let Some(v) = me.upgrade() {
                    v.view_menu();
                }
            }));
        }
        if follow && !self.follow_set.replace(true) {
            // The preview follows the cursor once it has rested: a tick after the delay.
            let me = self.me.clone();
            let ms = FOLLOW_DELAY.as_millis() as u64;
            self.ui.after(
                ms,
                Box::new(move || {
                    if let Some(v) = me.upgrade() {
                        v.tick();
                    }
                }),
            );
        } else if !follow {
            self.follow_set.set(false);
        }
    }

    /// Text wraps at the pane's width; a hex dump keeps its columns and scrolls sideways.
    fn wrap_preview(&self, wrap: bool) {
        self.ui
            .wrap_text_view(self.page, self.form.ctl(C::Preview), wrap);
    }

    /// The export bar: its list refilled only when the choices change, its button's target.
    fn show_export(&self, bar: &ExportBar) {
        let labels: Vec<String> = bar.choices.iter().map(|c| c.label.clone()).collect();
        self.form.set_items(C::VolumeFormat, &labels);
        self.form.select(C::VolumeFormat, bar.pick);
        self.set_title(C::ExportVolume, &bar.button);
        self.enable(C::ExportVolume, bar.enabled);
        self.enable(C::VolumeFormat, bar.enabled);
    }

    fn show(&self, s: Shown) {
        let v = &s.volume;
        let parts = [&v.fs, &v.usage, &v.label, &v.blessed];
        let line: Vec<&str> = parts
            .iter()
            .map(|p| p.as_str())
            .filter(|p| !p.is_empty())
            .collect();
        self.form.set_text(C::Volume, &line.join("    "));
        self.show_if(C::ExtractAll, s.export_volume && s.loaded);
        self.show_if(C::ExportVolume, s.export_volume && s.loaded);
        self.show_if(C::VolumeFormat, s.export_volume && s.loaded);
        self.show_export(&s.export);
        let (job_text, pct) = s
            .job
            .as_ref()
            .map(|(j, _)| super::jobs::job_line(j))
            .unwrap_or_default();
        self.form.set_text(C::Job, &job_text);
        self.show_if(C::Job, s.job.is_some());
        self.form.set_progress(C::Progress, s.job.is_some(), pct);
        self.show_if(C::Cancel, s.job.as_ref().is_some_and(|(_, c)| *c));
        self.form.set_text(C::Message, &s.message);
        self.show_if(C::Message, !s.message.is_empty());
        self.pane.show(s.pane);
        self.show_panel(s.panel_key, s.panel, s.result, s.actions);
        self.form.set_text(C::Status, &s.status);
        self.show_tools(&s.tools);
        self.show_edit(&s.edit);
        self.show_marks(&s.marks);
        self.show_windows(s.windows);
    }

    fn show_tools(&self, t: &HeaderTools) {
        self.show_if(C::EditMode, t.caps.edit_toggle);
        self.enable(C::EditMode, t.edit_enabled);
        self.set_title(C::EditMode, t.edit_label);
        self.show_if(C::Check, t.check);
        self.show_if(C::Journal, t.caps.journal);
        self.show_if(C::ChdInfo, t.chd);
        self.show_if(C::Tree, t.loaded);
        self.show_if(C::FullScan, t.caps.carve);
        self.set_check(C::FullScan, t.full_scan);
        self.show_if(C::ShowDeleted, t.caps.qnx2);
        self.set_check(C::ShowDeleted, t.show_deleted);
        let (text, warn) = match t.boot {
            Some(true) => ("Boot blocks: present", false),
            Some(false) => ("Boot blocks: absent", true),
            None => ("", false),
        };
        self.form.set_text(C::Boot, text);
        self.form
            .set_tone(C::Boot, if warn { Some(Tone::Warning) } else { None });
        self.show_if(C::Boot, t.boot.is_some());
    }

    fn show_edit(&self, e: &EditBar) {
        const BAR: [C; 16] = [
            C::EditNote,
            C::EditTarget,
            C::AddFile,
            C::Import,
            C::NewFolder,
            C::Rename,
            C::Delete,
            C::Bless,
            C::BootBlocks,
            C::Lock,
            C::Unlock,
            C::Apply,
            C::Discard,
            C::Edits,
            C::Free,
            C::After,
        ];
        for c in BAR {
            self.show_if(c, e.on);
        }
        if !e.on {
            return;
        }
        self.form.set_text(C::EditNote, e.copy_note.unwrap_or(""));
        self.show_if(C::EditNote, e.copy_note.is_some());
        self.form.set_text(C::EditTarget, &e.target);
        self.show_if(C::NewFolder, e.caps.new_folder);
        self.enable(C::Rename, e.deletable);
        self.enable(C::Delete, e.deletable);
        self.show_if(C::Bless, e.caps.hfs);
        self.enable(C::Bless, e.folder);
        self.show_if(C::BootBlocks, e.caps.hfs);
        self.show_if(C::Lock, e.caps.prodos);
        self.show_if(C::Unlock, e.caps.prodos);
        self.enable(C::Lock, e.deletable);
        self.enable(C::Unlock, e.deletable);
        let any = e.staged > 0;
        for c in [C::Apply, C::Discard, C::Edits] {
            self.show_if(c, any);
        }
        self.set_title(C::Apply, &e.apply_label);
        self.set_title(C::Edits, &e.edits_label);
        self.enable(C::Apply, e.apply_enabled);
        self.enable(C::Discard, e.discard_enabled);
        self.edits_shown.set(e.edits_shown);
        self.form.set_text(C::Free, &e.free);
        let (after, tone) = e.after.clone().unzip();
        self.form.set_text(C::After, after.as_deref().unwrap_or(""));
        self.form.set_tone(C::After, tone);
    }

    fn show_marks(&self, m: &MarkBar) {
        for c in [C::MarkCount, C::MarkClear] {
            self.show_if(c, m.count > 0);
        }
        self.form.set_text(C::MarkCount, &m.label);
    }

    /// Open, keep or close the controller's windows: one per kind, built again when it changes.
    fn show_windows(&self, wanted: Vec<PaneWindow>) {
        let mut open = self.windows.borrow_mut();
        open.retain(|w| wanted.iter().any(|p| p.kind == w.kind));
        for p in wanted {
            let at = open.iter().position(|w| w.kind == p.kind);
            if let Some(w) = at.map(|i| &open[i]).filter(|w| w.key == p.key) {
                w.update(&p);
                continue;
            }
            let frame = at.map(|i| open.remove(i).frame());
            let me = self.me.clone();
            let kind = p.kind;
            let ui = self.ui.clone();
            let on_event: Rc<dyn Fn(Event)> = Rc::new(move |e| {
                if let Some(view) = me.upgrade() {
                    // The window's own handler is still on the stack: act once it returns.
                    let weak = Rc::downgrade(&view);
                    ui.later(Box::new(move || {
                        if let Some(view) = weak.upgrade() {
                            view.window_event(kind, e);
                        }
                    }));
                }
            });
            open.push(ModelWindow::open(
                self.ui.clone(),
                &p,
                frame.flatten(),
                on_event,
            ));
        }
    }

    fn window_event(&self, kind: WindowKind, e: Event) {
        match e {
            // A Save is staged like any edit; a refusal keeps the window and the text.
            Event::Send(actions) if kind == WindowKind::Editor => {
                let before = self.ctrl.borrow().pane(Side::Left).queue.len();
                for a in actions {
                    self.apply(a);
                }
                let why = {
                    let ctl = self.ctrl.borrow();
                    let staged = ctl.pane(Side::Left).queue.len() > before;
                    (!staged).then(|| ctl.status().to_string())
                };
                match why {
                    None => drop(self.editor.borrow_mut().take()),
                    Some(why) => {
                        if let Some((_, err)) = &mut *self.editor.borrow_mut() {
                            *err = Some(why);
                        }
                    }
                }
                self.refresh();
            }
            Event::Send(actions) => {
                for a in actions {
                    self.apply(a);
                }
            }
            Event::Copy(text) => self.ui.copy_text(&text),
            Event::Toggle(on) => {
                self.debug.set(on);
                self.refresh();
            }
            Event::Closed => {
                match kind {
                    WindowKind::Tree => drop(self.tree_text.borrow_mut().take()),
                    WindowKind::Journal => drop(self.journal.borrow_mut().take()),
                    WindowKind::Editor => drop(self.editor.borrow_mut().take()),
                    _ => {}
                }
                self.refresh();
            }
        }
    }

    /// The View menu under its button: listing styles, quick view, the log and help.
    fn view_menu(&self) {
        let items: Vec<MenuItem> = self
            .ctrl
            .borrow()
            .view_menu()
            .into_iter()
            .map(MenuItem::view)
            .collect();
        if self.ctrl.borrow().view_menu_shown() {
            self.apply(A::ShowViewMenu(false));
        }
        let c = self.form.ctl(C::View);
        let (_, h) = self.ui.control_size(self.page, c);
        if let Some(a) = track(&*self.ui, self.page, c, items, 0.0, h + 2.0) {
            self.apply(a);
        }
    }

    /// What the user did in the listing, as actions on the controller.
    fn pane_event(&self, event: PaneEvent) {
        if self.sync.active() {
            return;
        }
        let pane = |a| A::Pane(Side::Left, a);
        match event {
            PaneEvent::Click { row, ctrl, shift } => {
                let click = self
                    .ctrl
                    .borrow()
                    .pane(Side::Left)
                    .row_click(&row, ctrl, shift);
                self.apply(A::Focus(Side::Left));
                if let Some(action) = click {
                    self.apply(pane(action));
                }
            }
            PaneEvent::Mark(id) => self.apply(pane(P::Mark(id))),
            PaneEvent::Toggle(id) => self.apply(pane(P::ToggleExpand(id))),
            PaneEvent::Activate => {
                let id = {
                    let ctl = self.ctrl.borrow();
                    let p = ctl.pane(Side::Left);
                    p.cursor_index().map(|i| p.rows()[i].node_id.clone())
                };
                if let Some(id) = id {
                    self.apply(pane(P::Activate(id)));
                }
            }
            PaneEvent::Key(key, mods) => {
                if matches!(
                    self.ctrl.borrow().command_for(key, mods),
                    Some(Command::PageUp | Command::PageDown)
                ) {
                    self.apply(A::SetPageRows(Side::Left, self.pane.page_rows()));
                }
                self.key(key, mods);
            }
            PaneEvent::Sort(col) => self.apply(pane(P::SortBy(col))),
            PaneEvent::Menu(i, x, y) => {
                if let Some(row) = self.pane.row_at(i) {
                    self.row_menu(&row, x, y);
                }
            }
            PaneEvent::Folder(a) => {
                self.apply(A::Focus(Side::Left));
                self.apply(pane(a));
            }
            PaneEvent::Focused => {}
        }
    }

    /// The right-click menu of `row` at `at` in the list.
    fn row_menu(&self, row: &rusty_backup::controller::commander::DisplayRow, x: f64, y: f64) {
        let context = self.ctrl.borrow().pane(Side::Left).row_context(row);
        self.apply(A::Focus(Side::Left));
        if let Some(a) = context {
            self.apply(A::Pane(Side::Left, a));
        }
        let items: Vec<MenuItem> = self
            .ctrl
            .borrow()
            .row_menu(Side::Left, row)
            .into_iter()
            .map(MenuItem::row)
            .collect();
        if items.is_empty() {
            return;
        }
        if let Some(a) = track(&*self.ui, self.page, self.pane.list_ctl(), items, x, y) {
            self.apply(a);
        }
    }

    fn show_panel(
        &self,
        key: Option<(u64, bool)>,
        panel: Option<SidePanel>,
        result: Option<String>,
        actions: Option<PanelActions>,
    ) {
        if key.is_none() {
            for c in [C::Name, C::Facts, C::Note, C::Budget] {
                self.form.set_text(c, "");
            }
            self.form.set_text_view(C::Preview, "");
        }
        self.panel_key.set(key);
        if let Some(p) = panel {
            self.form.set_text(C::Name, &p.name);
            self.form.set_text(C::Facts, &p.facts.join("\n"));
            self.form.set_text(C::Note, p.note.as_deref().unwrap_or(""));
            self.form.set_text(C::Budget, &p.budget.join("\n"));
            let hex = matches!(p.preview, PanelPreview::Hex(_));
            self.wrap_preview(!hex);
            self.form.set_text_view(C::Preview, p.preview.text());
        }
        self.form
            .set_text(C::Result, result.as_deref().unwrap_or(""));
        for c in [C::Note, C::Budget, C::Result] {
            let on = !self.form.text(c).is_empty();
            self.show_if(c, on);
        }
        let a = actions.unwrap_or_default();
        let on = a.extractable;
        self.show_if(C::ProdosCaption, on && a.prodos);
        self.show_if(C::Prodos, on && a.prodos);
        self.show_if(C::RsToLf, on && a.qnx);
        self.show_if(C::SaveArchive, on && a.archive);
        self.show_if(C::BrowseArchive, on && a.archive);
        self.show_if(C::EditText, on && a.text);
        self.enable(C::EditText, a.edit_mode);
        self.show_if(C::Metadata, on && a.metadata);
        self.show_if(C::MacArchive, on && a.mac);
        let (extract, export) = match a.folder {
            true => ("Extract Folder...", "Export Folder..."),
            false => ("Extract File...", "Export File..."),
        };
        self.set_title(C::Extract, extract);
        self.set_title(C::Export, export);
        self.show_if(C::Extract, a.extractable);
        self.show_if(C::Export, a.extractable && a.export);
        self.enable(C::Extract, !a.copying);
        self.enable(C::Export, !a.copying);
        self.show_if(C::Nested, a.extractable && a.nested);
        self.show_if(C::ForkCaption, a.extractable && a.forks);
        self.show_if(C::Fork, a.extractable && a.forks);
    }

    /// Pick host files and stage them: the flow suite answers through `controller::picks`.
    fn add_files(&self) {
        if let Some(answer) = picks::scripted("add_files") {
            let paths: Vec<PathBuf> = answer.into_iter().collect();
            if !paths.is_empty() {
                self.apply(A::Pane(Side::Left, P::AddPicked(paths)));
            }
            return;
        }
        let paths = self.ui.pick_files("Select files to add");
        if !paths.is_empty() {
            self.apply(A::Pane(Side::Left, P::AddPicked(paths)));
        }
    }

    /// A control's action, by the control.
    fn fired(&self, c: C) {
        if self.sync.active() {
            return;
        }
        let pane = |a| A::Pane(Side::Left, a);
        let on_path = |f: &dyn Fn(String) -> A| {
            if let Some(path) = self.cursor_path() {
                self.apply(f(path));
            }
        };
        match c {
            C::EditMode => self.apply(A::Mode(ModeAction::ToggleEditMode)),
            C::Check => self.apply(pane(P::Fsck)),
            C::Journal => self.apply(pane(P::ShowJournal)),
            C::ChdInfo => self.apply(A::Mode(ModeAction::ChdInfo)),
            C::Tree => self.apply(pane(P::ListTree { ids: false })),
            C::View => self.view_menu(),
            C::FullScan => self.apply(A::Mode(ModeAction::CarveFullScan(self.checked(c)))),
            C::ShowDeleted => self.apply(A::Mode(ModeAction::ShowDeleted(self.checked(c)))),
            C::Close => self.apply(A::Mode(ModeAction::RequestClose)),
            C::Partition => {
                let pick = self.form.picked(c);
                let index = pick.and_then(|p| self.partitions.borrow().get(p).copied());
                let f = self.on_partition.borrow().clone();
                if let (Some(index), Some(f)) = (index, f) {
                    f(index);
                }
            }
            C::AddFile => self.add_files(),
            C::Import => self.apply(pane(P::ImportTar)),
            C::NewFolder => self.apply(pane(P::RequestNewFolder)),
            C::Rename => self.apply(pane(P::RenameAtCursor)),
            C::Delete => self.apply(pane(P::Delete)),
            C::Bless => self.apply(pane(P::Bless)),
            C::BootBlocks => self.apply(pane(P::BootBlocks)),
            C::Lock => self.apply(pane(P::SetProdosLock(true))),
            C::Unlock => self.apply(pane(P::SetProdosLock(false))),
            C::Apply => self.apply(A::Mode(ModeAction::Apply)),
            C::Discard => self.apply(pane(P::Discard)),
            C::Edits => self.apply(pane(P::ShowEdits(!self.edits_shown.get()))),
            C::Cancel => {
                let copying = self.ctrl.borrow().copy_job().is_some();
                self.apply(match copying {
                    true => A::CancelCopy,
                    false => pane(P::CancelOpen),
                });
            }
            C::NoticeOk => self.show_notice(None),
            C::ExtractAll => self.extract("/".to_string()),
            C::ExportVolume => {
                let mode = self.extract_options().fork_mode;
                self.apply(A::SetForkMode(mode));
                self.apply(A::ExportFromBar(Side::Left));
            }
            C::VolumeFormat => {
                let at = self.form.picked(C::VolumeFormat).map_or(-1, |i| i as isize);
                if let Ok(choice) = usize::try_from(at) {
                    self.apply(A::PickExport {
                        from: Side::Left,
                        choice,
                    });
                }
            }
            C::Extract => {
                if let Some(path) = self.cursor_path() {
                    self.extract(path);
                }
            }
            C::Export => {
                let mode = self.extract_options().fork_mode;
                self.apply(A::SetForkMode(mode));
                on_path(&|path| A::ExportEntry {
                    from: Side::Left,
                    path,
                    format: None,
                });
            }
            C::Nested => on_path(&|path| A::Mode(ModeAction::OpenNested(path))),
            C::Fork => {
                let mode = self.extract_options().fork_mode;
                self.apply(A::SetForkMode(mode));
            }
            C::SaveArchive => on_path(&|path| {
                pane(P::ReadArchive {
                    path,
                    purpose: ArchiveUse::Extract,
                })
            }),
            C::BrowseArchive => on_path(&|path| {
                pane(P::ReadArchive {
                    path,
                    purpose: ArchiveUse::Browse,
                })
            }),
            C::EditText => self.apply(pane(P::EditText)),
            C::Metadata => self.apply(pane(P::EditMetadata)),
            C::MacArchive => self.mac_menu(),
            C::MarkClear => self.apply(pane(P::ClearSelection)),
            _ => {}
        }
    }

    /// Offer the Mac archive formats under their button; the one picked exports the cursor's entry.
    fn mac_menu(&self) {
        let Some(path) = self.cursor_path() else {
            return;
        };
        let items = MAC_FORMATS
            .iter()
            .map(|(f, label)| MenuItem::Item {
                label: label.to_string(),
                enabled: true,
                checked: false,
                action: A::ExportEntry {
                    from: Side::Left,
                    path: path.clone(),
                    format: Some(*f),
                },
            })
            .collect();
        let c = self.form.ctl(C::MacArchive);
        let (_, h) = self.ui.control_size(self.page, c);
        if let Some(export) = track(&*self.ui, self.page, c, items, 0.0, h + 2.0) {
            let mode = self.extract_options().fork_mode;
            self.apply(A::SetForkMode(mode));
            self.apply(export);
        }
    }

    /// Lay the page out at its size; hidden views take no room.
    pub fn layout(&self) {
        let line = |c: C| Node::leaf(c, Size::Stretch(1), Size::Fixed(layout::ROW - 4));
        let button = |c: C, w: i32| Node::button(c, w);
        let fixed = |c: C, h: i32| Node::leaf(c, Size::Fixed(DETAIL_W), Size::Fixed(h));
        let mut col = vec![Node::row(vec![
            Node::leaf(C::Title, Size::Fixed(130), Size::Fixed(layout::ROW - 4)),
            Node::leaf(C::Partition, Size::Fixed(260), Size::Fixed(layout::ROW + 2)),
            line(C::Volume),
            Node::leaf(C::Boot, Size::Fixed(150), Size::Fixed(layout::ROW - 4)),
        ])];
        col.push(Node::row(vec![
            button(C::EditMode, 120),
            button(C::ExtractAll, 110),
            button(C::ExportVolume, 150),
            Node::leaf(
                C::VolumeFormat,
                Size::Fixed(230),
                Size::Fixed(layout::ROW + 2),
            ),
            button(C::Check, 70),
            button(C::Journal, 80),
            button(C::ChdInfo, 80),
            button(C::Tree, 60),
            Node::Space(Size::Stretch(1)),
            Node::leaf(C::FullScan, Size::Fixed(100), Size::Fixed(layout::ROW)),
            Node::leaf(C::ShowDeleted, Size::Fixed(120), Size::Fixed(layout::ROW)),
            button(C::View, 80),
            button(C::Close, 110),
        ]));
        col.push(line(C::EditNote));
        col.push(Node::row(vec![
            Node::leaf(
                C::EditTarget,
                Size::Fixed(150),
                Size::Fixed(layout::ROW - 4),
            ),
            button(C::AddFile, 95),
            button(C::Import, 130),
            button(C::NewFolder, 115),
            button(C::Rename, 90),
            button(C::Delete, 70),
            button(C::Bless, 105),
            button(C::BootBlocks, 115),
            button(C::Lock, 60),
            button(C::Unlock, 70),
            Node::Space(Size::Stretch(1)),
        ]));
        col.push(Node::row(vec![
            button(C::Apply, 140),
            button(C::Discard, 80),
            button(C::Edits, 100),
            Node::leaf(C::Free, Size::Fixed(160), Size::Fixed(layout::ROW - 4)),
            Node::leaf(C::After, Size::Fixed(160), Size::Fixed(layout::ROW - 4)),
            Node::Space(Size::Stretch(1)),
        ]));
        col.push(Node::row(vec![
            line(C::Job),
            Node::leaf(C::Progress, Size::Fixed(160), Size::Fixed(layout::ROW - 4)),
            button(C::Cancel, 80),
        ]));
        col.push(Node::row(vec![line(C::Notice), button(C::NoticeOk, 60)]));
        col.push(line(C::Message));
        // A line of the label font is 16 points; about 46 characters fit across the panel.
        let lines = |c: C| -> i32 {
            let text = self.form.text(c);
            let n: usize = text
                .lines()
                .map(|l| l.chars().count().div_ceil(46).max(1))
                .sum();
            16 * n.max(1) as i32
        };
        let mut side = vec![
            fixed(C::Name, layout::ROW - 4),
            fixed(C::Facts, lines(C::Facts)),
        ];
        for c in [C::Note, C::Budget, C::Result] {
            side.push(fixed(c, lines(c)));
        }
        side.push(Node::row(vec![
            Node::leaf(
                C::ForkCaption,
                Size::Fixed(110),
                Size::Fixed(layout::ROW - 4),
            ),
            Node::leaf(
                C::Fork,
                Size::Fixed(DETAIL_W - 110 - layout::GAP),
                Size::Fixed(layout::ROW + 2),
            ),
        ]));
        side.push(Node::row(vec![
            button(C::Extract, 140),
            button(C::Export, DETAIL_W - 140 - layout::GAP),
        ]));
        side.push(button(C::Nested, 160));
        side.push(Node::row(vec![
            Node::leaf(
                C::ProdosCaption,
                Size::Fixed(110),
                Size::Fixed(layout::ROW - 4),
            ),
            Node::leaf(
                C::Prodos,
                Size::Fixed(DETAIL_W - 110 - layout::GAP),
                Size::Fixed(layout::ROW + 2),
            ),
        ]));
        side.push(Node::leaf(
            C::RsToLf,
            Size::Fixed(DETAIL_W),
            Size::Fixed(layout::ROW),
        ));
        let half = (DETAIL_W - layout::GAP) / 2;
        side.push(Node::row(vec![
            button(C::SaveArchive, half),
            button(C::BrowseArchive, half),
        ]));
        side.push(Node::row(vec![
            button(C::EditText, half),
            button(C::Metadata, half),
        ]));
        side.push(button(C::MacArchive, half));
        side.push(Node::leaf(
            C::Preview,
            Size::Fixed(DETAIL_W),
            Size::Stretch(1),
        ));
        col.push(Node::row(vec![
            Node::leaf(C::TreeView, Size::Fixed(TREE_W), Size::Stretch(1)),
            Node::leaf(C::List, Size::Stretch(1), Size::Stretch(1)),
            Node::column(side),
        ]));
        col.push(Node::row(vec![
            Node::leaf(C::Status, Size::Stretch(1), Size::Fixed(layout::ROW - 4)),
            Node::leaf(C::MarkCount, Size::Fixed(100), Size::Fixed(layout::ROW - 4)),
            button(C::MarkClear, 70),
        ]));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        let keep = |c: &C| match c {
            C::Note | C::Budget | C::Result | C::Facts => !self.form.text(*c).is_empty(),
            C::Preview | C::Name => true,
            _ => self.shown(*c),
        };
        let Some(tree) = tree.prune(&keep) else {
            return;
        };
        self.form.place(&tree, self.ui.area(self.page));
    }
}
