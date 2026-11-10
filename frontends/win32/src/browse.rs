//! The Inspect browse view over a single-pane `CommanderController`; GTK's `browser/mod.rs` is
//! the reference.
//!
//! The volume line and the header's tools ([`crate::browse_bars`]), the edit bar, the running
//! job with Cancel, notices, the folder tree, the listing (a virtual list view with tick boxes
//! for marks), the side panel over a text or hex preview, the status line and the mark bar, the
//! View and row menus, and the windows the controller describes ([`crate::window`]). Keys go
//! through the controller's key map. Still to come: File Info's window and metadata form, the
//! text editor, Mac archive and tar import windows, the size budget, checksums and passwords.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::{CommanderRecent, Notify, Snapshot, UserConfig};
use rusty_backup::controller::browser::{ids, TextDoc};
use rusty_backup::controller::commander::{
    editor_window, extract_fork_modes, journal_window, tree_window, BrowserPaneAction as P,
    CommanderAction as A, CommanderController, DisplayRow, EditBar, ExportFormat, ExtractOptions,
    HeaderTools, JobRegistry, MarkBar, ModeAction, PaneProfile, PaneServices, PaneWindow,
    PanelActions, ResourceForkMode, Side, SidePanel, TreeText, VolumeLine, WindowKind,
    FOLLOW_DELAY,
};
use rusty_backup::controller::inspect::{BrowseEdit, BrowseOpen, InspectAction, InspectSource};
use rusty_backup::controller::keys::{Command, Key, KeyMap, Mods};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::{Controller, Effect};
use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::Foundation::{POINT, RECT};
use windows::Win32::UI::Controls::NMHDR;
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::WindowsAndMessaging::{
    GetAncestor, GetCursorPos, GetWindowRect, KillTimer, PostMessageW, SendMessageW, SetTimer,
    CBN_SELCHANGE, CBS_DROPDOWNLIST, CB_ADDSTRING, CB_GETCURSEL, CB_SETCURSEL, ES_AUTOVSCROLL,
    ES_MULTILINE, ES_READONLY, GA_ROOT, WINDOW_EX_STYLE, WINDOW_STYLE, WM_COMMAND,
    WS_EX_CLIENTEDGE, WS_HSCROLL, WS_TABSTOP, WS_VSCROLL,
};

use crate::browse_bars::{BarCmd, EditRow, MarkRow, PanelCmd, PanelTools, Tools, MAC_FORMATS};
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::menu::{track, MenuItem};
use crate::pane_view::{key_of, mods, PaneEvent, PaneShown, PaneView, DEFERRED};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;
use crate::window::{Event, ModelWindow};

/// The side panel's width and the folder tree's, in DIPs.
const DETAIL_W: i32 = 330;
const TREE_W: i32 = 200;

/// Another: pop up the View menu the key map asked for.
const VIEW_MENU: u32 = 0x7F01;

fn new_controller(notify: &Arc<dyn Notify>) -> CommanderController {
    let services = PaneServices {
        jobs: JobRegistry::new(),
        notify: Arc::clone(notify),
        recent: Box::new(CommanderRecent(UserConfig)),
    };
    let (keymap, problems) = KeyMap::load(KeyMap::user_file().as_deref());
    let mut ctl = CommanderController::single(services, keymap, PaneProfile::inspect());
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
    ctrl: RefCell<CommanderController>,
    notify: Arc<dyn Notify>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<BrowseView>,
    active: Cell<bool>,
    page: HWND,
    title: HWND,
    volume: HWND,
    extract_all: HWND,
    export_volume: HWND,
    close: HWND,
    job: HWND,
    progress: HWND,
    cancel: HWND,
    notice: HWND,
    notice_ok: HWND,
    message: HWND,
    pane: PaneView,
    name: HWND,
    facts: HWND,
    note: HWND,
    budget: HWND,
    result: HWND,
    fork_caption: HWND,
    fork: HWND,
    extract: HWND,
    export: HWND,
    nested: HWND,
    preview: HWND,
    status: HWND,
    tools: Tools,
    panel_tools: PanelTools,
    edit_row: EditRow,
    mark_row: MarkRow,
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
    follow_timer: Cell<usize>,
    on_change: RefCell<Option<Rc<dyn Fn()>>>,
    /// Which optional lines the last layout gave room to.
    shape: Cell<u64>,
    /// How many lines of facts the side panel shows, which the layout gives room to.
    facts_rows: Cell<i32>,
}

thread_local! {
    /// The view the follow timer wakes; a timer callback has no context of its own.
    static FOLLOW: RefCell<Weak<BrowseView>> = const { RefCell::new(Weak::new()) };
}

unsafe extern "system" fn follow_fired(_: HWND, _: u32, id: usize, _: u32) {
    // SAFETY: the timer is ours and one-shot.
    let _ = unsafe { KillTimer(None, id) };
    if let Some(view) = FOLLOW.with(|f| f.borrow().upgrade()) {
        view.follow_timer.set(0);
        view.tick();
    }
}

fn multiline(parent: HWND, id: &str) -> HWND {
    let style = ES_MULTILINE as u32
        | ES_READONLY as u32
        | ES_AUTOVSCROLL as u32
        | WS_VSCROLL.0
        | WS_HSCROLL.0
        | WS_TABSTOP.0;
    ui::child(
        parent,
        w!("EDIT"),
        "",
        WINDOW_STYLE(style),
        WS_EX_CLIENTEDGE,
        id,
    )
}

impl BrowseView {
    pub fn build(page: HWND, notify: Arc<dyn Notify>, host: EffectHost) -> Rc<BrowseView> {
        let title = ui::label(page, "Filesystem Browser");
        let volume = ui::label(page, "");
        let extract_all = ui::button(page, "Extract All...", ids::EXTRACT);
        let export_volume = ui::button(page, "Export to .tar.gz...", ids::EXPORT_VOLUME);
        let close = ui::button(page, "Close", ids::CLOSE);
        let job = ui::label(page, "");
        let progress = ui::child(
            page,
            w!("msctls_progress32"),
            "",
            WINDOW_STYLE(0),
            WINDOW_EX_STYLE(0),
            "",
        );
        let cancel = ui::button(page, "Cancel", ids::CANCEL_JOB);
        let notice = ui::label(page, "");
        let notice_ok = ui::button(page, "OK", "");
        let message = ui::label(page, "");
        let pane = PaneView::build(page, Side::Left, ids::LIST, ids::FOLDER_TREE, false);
        let name = ui::label(page, "");
        let facts = ui::label(page, "");
        let note = ui::label(page, "");
        let budget = ui::label(page, "");
        let result = ui::label(page, "");
        let fork_caption = ui::cell_label(page, "Resource forks:");
        let combo = WINDOW_STYLE((CBS_DROPDOWNLIST as u32) | WS_TABSTOP.0 | WS_VSCROLL.0);
        let fork = ui::child(page, w!("COMBOBOX"), "", combo, WINDOW_EX_STYLE(0), "");
        let modes = extract_fork_modes();
        for m in &modes {
            let t = ui::wide(m.label());
            // SAFETY: the combo copies the string.
            unsafe {
                SendMessageW(fork, CB_ADDSTRING, None, Some(LPARAM(t.as_ptr() as isize)));
            }
        }
        let start = modes
            .iter()
            .position(|m| *m == ResourceForkMode::AppleDouble)
            .unwrap_or(0);
        // SAFETY: a plain combo message.
        unsafe {
            SendMessageW(fork, CB_SETCURSEL, Some(WPARAM(start)), None);
        }
        let extract = ui::button(page, "Extract...", ids::EXTRACT);
        let export = ui::button(page, "Export to .tgz...", ids::EXPORT_ENTRY);
        let nested = ui::button(page, "Open as Disk Image", ids::OPEN_NESTED);
        let preview = multiline(page, ids::DETAIL);
        // SAFETY: a plain query of our own page.
        let dpi = unsafe { GetDpiForWindow(page) }.max(96);
        ui::set_font(preview, ui::mono_font_at(dpi));
        let status = ui::cell_label(page, "");
        let tools = Tools::build(page);
        let panel_tools = PanelTools::build(page);
        let edit_row = EditRow::build(page);
        let mark_row = MarkRow::build(page);
        let view = Rc::new_cyclic(|me: &Weak<BrowseView>| BrowseView {
            ctrl: RefCell::new(new_controller(&notify)),
            notify,
            host,
            sync: SyncGuard::default(),
            me: me.clone(),
            active: Cell::new(false),
            page,
            title,
            volume,
            extract_all,
            export_volume,
            close,
            job,
            progress,
            cancel,
            notice,
            notice_ok,
            message,
            pane,
            name,
            facts,
            note,
            budget,
            result,
            fork_caption,
            fork,
            extract,
            export,
            nested,
            preview,
            status,
            tools,
            panel_tools,
            edit_row,
            mark_row,
            windows: RefCell::default(),
            debug: Cell::new(false),
            tree_text: RefCell::default(),
            journal: RefCell::default(),
            editor: RefCell::default(),
            info_asked: Cell::new(false),
            panel_key: Cell::new(None),
            label_prefix: RefCell::default(),
            follow_timer: Cell::new(0),
            on_change: RefCell::default(),
            shape: Cell::new(u64::MAX),
            facts_rows: Cell::new(1),
        });
        FOLLOW.with(|f| *f.borrow_mut() = view.me.clone());
        view.set_icons();
        let weak: Weak<dyn Handler> = view.me.clone();
        panel::register(page, weak);
        ui::set_visible(notice, false);
        ui::set_visible(notice_ok, false);
        view.refresh();
        view
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

    /// Files dropped from Explorer: staged into the open volume while editing.
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

    pub fn close(&self) {
        if !self.active.replace(false) {
            return;
        }
        let fresh = new_controller(&self.notify);
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
        crate::app::append_log(lines.into_iter().map(|l| (LogLevel::Info, l)).collect());
    }

    fn show_notice(&self, text: Option<&str>) {
        ui::set_text(self.notice, text.unwrap_or(""));
        ui::set_visible(self.notice, text.is_some());
        ui::set_visible(self.notice_ok, text.is_some());
        if self.shape.get() != self.shape_now() {
            self.layout();
        }
    }

    fn extract_options(&self) -> ExtractOptions {
        // SAFETY: a plain combo query.
        let i = unsafe { SendMessageW(self.fork, CB_GETCURSEL, None, None).0 };
        let fork_mode = usize::try_from(i)
            .ok()
            .and_then(|i| extract_fork_modes().get(i).copied())
            .unwrap_or(ResourceForkMode::AppleDouble);
        let (prodos_suffix, rs_to_lf) = self.panel_tools.options();
        ExtractOptions {
            fork_mode,
            prodos_suffix,
            rs_to_lf,
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

    /// Read the controller first and release it, then set the controls inside the guard.
    fn refresh(&self) {
        let shown = self.read();
        let follow = shown.follow;
        let view_menu = shown.view_menu && self.active.get();
        self.sync.run(|| self.show(shown));
        if view_menu {
            // SAFETY: posting to our own page; the menu opens once this refresh is done.
            unsafe {
                let _ = PostMessageW(
                    Some(self.page),
                    WM_COMMAND,
                    WPARAM((VIEW_MENU as usize) << 16),
                    LPARAM(self.tools.view.0 as isize),
                );
            }
        }
        if self.shape.get() != self.shape_now() {
            self.layout();
        }
        if follow && self.follow_timer.get() == 0 {
            let ms = FOLLOW_DELAY.as_millis() as u32;
            // SAFETY: a one-shot thread timer; its callback finds the view through FOLLOW.
            let id = unsafe { SetTimer(None, 0, ms, Some(follow_fired)) };
            self.follow_timer.set(id);
        }
    }

    fn show(&self, s: Shown) {
        let v = &s.volume;
        let parts = [&v.fs, &v.usage, &v.label, &v.blessed];
        let line: Vec<&str> = parts
            .iter()
            .map(|p| p.as_str())
            .filter(|p| !p.is_empty())
            .collect();
        ui::set_text(self.volume, &line.join("    "));
        ui::set_visible(self.extract_all, s.export_volume && s.loaded);
        ui::set_visible(self.export_volume, s.export_volume && s.loaded);
        let (job_text, pct) = s
            .job
            .as_ref()
            .map(|(j, _)| crate::inspect::job_line(j))
            .unwrap_or_default();
        ui::set_text(self.job, &job_text);
        ui::set_visible(self.job, s.job.is_some());
        crate::inspect::set_progress(self.progress, s.job.is_some(), pct);
        ui::set_visible(self.cancel, s.job.as_ref().is_some_and(|(_, c)| *c));
        ui::set_text(self.message, &s.message);
        ui::set_visible(self.message, !s.message.is_empty());
        self.pane.show(s.pane);
        self.show_panel(s.panel_key, s.panel, s.result, s.actions);
        ui::set_text(self.status, &s.status);
        self.tools.show(&s.tools);
        self.edit_row.show(&s.edit);
        self.mark_row.show(&s.marks);
        self.show_windows(s.windows);
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
            let rect = at.map(|i| open.remove(i).rect());
            let owner = self.owner();
            let me = self.me.clone();
            let kind = p.kind;
            let on_event: Rc<dyn Fn(Event)> = Rc::new(move |e| {
                if let Some(view) = me.upgrade() {
                    view.window_event(kind, e);
                }
            });
            let w = ModelWindow::open(owner, &p, rect, on_event);
            let me = self.me.clone();
            let keys: panel::KeyHook = Rc::new(move |vk| {
                let view = me.upgrade();
                let key = key_of(vk).filter(|k| matches!(k, Key::F(_)));
                matches!((view, key), (Some(v), Some(k)) if v.key(k, mods()))
            });
            panel::set_keys(w.hwnd(), keys);
            open.push(w);
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
            Event::Copy(text) => self.host.copy(&text),
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

    /// The main window, which owns the browse view's windows and menus.
    fn owner(&self) -> HWND {
        // SAFETY: a plain query of our own page.
        unsafe { GetAncestor(self.page, GA_ROOT) }
    }

    fn bar(&self, cmd: BarCmd) {
        match cmd {
            BarCmd::Send(actions) => {
                for a in actions {
                    self.apply(a);
                }
            }
            BarCmd::AddFiles => {
                let paths = self.host.pick_files("Select files to add");
                if !paths.is_empty() {
                    self.apply(A::Pane(Side::Left, P::AddPicked(paths)));
                }
            }
            BarCmd::ExportMarked(format) => {
                let mode = self.extract_options().fork_mode;
                self.apply(A::SetForkMode(mode));
                self.apply(A::Export {
                    from: Side::Left,
                    format: Some(format),
                });
            }
            BarCmd::ViewMenu => self.view_menu(),
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
        let mut r = RECT::default();
        // SAFETY: a plain query of our own button.
        let _ = unsafe { GetWindowRect(self.tools.view, &mut r) };
        if let Some(a) = track(self.owner(), items, r.left, r.bottom) {
            self.apply(a);
        }
    }

    /// What the user did in the listing, as actions on the controller.
    fn pane_event(&self, event: PaneEvent) {
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
            PaneEvent::Menu(row) => self.row_menu(&row),
            PaneEvent::Drag | PaneEvent::Focused => {}
        }
    }

    /// The right-click menu of `row` at the pointer.
    fn row_menu(&self, row: &DisplayRow) {
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
        let mut at = POINT::default();
        // SAFETY: a plain query of the pointer.
        let _ = unsafe { GetCursorPos(&mut at) };
        if let Some(a) = track(self.owner(), items, at.x, at.y) {
            self.apply(a);
        }
    }

    /// Give the list and the folder tree the row icons for the page's DPI and the theme.
    pub fn set_icons(&self) {
        self.pane.set_icons();
    }

    fn show_panel(
        &self,
        key: Option<(u64, bool)>,
        panel: Option<SidePanel>,
        result: Option<String>,
        actions: Option<PanelActions>,
    ) {
        if key.is_none() {
            for h in [self.name, self.facts, self.note, self.budget, self.preview] {
                ui::set_text(h, "");
            }
        }
        self.panel_key.set(key);
        if let Some(p) = panel {
            ui::set_text(self.name, &p.name);
            ui::set_text(self.facts, &p.facts.join("\r\n"));
            if self.facts_rows.replace(p.facts.len() as i32) != p.facts.len() as i32 {
                self.shape.set(u64::MAX);
            }
            ui::set_text(self.note, p.note.as_deref().unwrap_or(""));
            ui::set_text(self.budget, &p.budget.join("\r\n"));
            ui::set_text(self.preview, &p.preview.text().replace('\n', "\r\n"));
        }
        ui::set_text(self.result, result.as_deref().unwrap_or(""));
        for h in [self.note, self.budget, self.result] {
            let on = !ui::text(h).is_empty();
            if ui::shown(h) != on {
                ui::set_visible(h, on);
                self.shape.set(u64::MAX);
            }
        }
        let a = actions.unwrap_or_default();
        self.panel_tools.show(&a);
        let (extract, export) = match a.folder {
            true => ("Extract Folder...", "Export Folder to .tgz..."),
            false => ("Extract File...", "Export File to .tgz..."),
        };
        ui::set_text(self.extract, extract);
        ui::set_text(self.export, export);
        ui::set_visible(self.extract, a.extractable);
        ui::set_visible(self.export, a.extractable && a.export);
        ui::set_enabled(self.extract, !a.copying);
        ui::set_enabled(self.export, !a.copying);
        ui::set_visible(self.nested, a.extractable && a.nested);
        ui::set_visible(self.fork_caption, a.extractable && a.forks);
        ui::set_visible(self.fork, a.extractable && a.forks);
    }

    /// Which optional controls are up, so a change lays the page out again.
    fn shape_now(&self) -> u64 {
        let mut all = vec![
            self.job,
            self.cancel,
            self.notice,
            self.message,
            self.pane.tree,
            self.extract,
            self.export,
            self.nested,
            self.fork,
            self.extract_all,
        ];
        all.extend(self.tools.all());
        all.extend(self.panel_tools.all());
        all.extend(self.edit_row.all());
        all.extend(self.mark_row.all().into_iter().take(1));
        all.iter()
            .enumerate()
            .map(|(i, h)| u64::from(ui::shown(*h)) << i)
            .sum()
    }

    fn layout(&self) {
        self.shape.set(self.shape_now());
        let (area, dpi) = panel::client(self.page);
        let line = |h: HWND| Node::leaf(h, Size::Stretch(1), Size::Fixed(layout::ROW - 8));
        let fixed = |h: HWND, n: i32| Node::leaf(h, Size::Fixed(DETAIL_W), Size::Fixed(n));
        let mut head = vec![
            Node::leaf(self.title, Size::Fixed(130), Size::Fixed(layout::ROW - 8)),
            line(self.volume),
        ];
        if ui::shown(self.tools.boot) {
            head.push(Node::leaf(
                self.tools.boot,
                Size::Fixed(150),
                Size::Fixed(layout::ROW - 8),
            ));
        }
        let mut col = vec![Node::row(head)];
        let mut tools = Vec::new();
        if ui::shown(self.tools.edit) {
            tools.push(Node::button(self.tools.edit, 100));
        }
        if ui::shown(self.extract_all) {
            tools.push(Node::button(self.extract_all, 100));
            tools.push(Node::button(self.export_volume, 140));
        }
        let (before, after) = self.tools.nodes();
        tools.extend(before);
        tools.push(Node::Space(Size::Stretch(1)));
        tools.extend(after);
        tools.push(Node::button(self.close, 70));
        col.push(Node::row(tools));
        col.extend(self.edit_row.nodes());
        if ui::shown(self.job) {
            let mut r = vec![
                line(self.job),
                Node::leaf(
                    self.progress,
                    Size::Fixed(160),
                    Size::Fixed(layout::ROW - 6),
                ),
            ];
            if ui::shown(self.cancel) {
                r.push(Node::button(self.cancel, 80));
            }
            col.push(Node::row(r));
        }
        if ui::shown(self.notice) {
            col.push(Node::row(vec![
                line(self.notice),
                Node::button(self.notice_ok, 60),
            ]));
        }
        if ui::shown(self.message) {
            col.push(line(self.message));
        }
        // A line of the message font is 16 DIPs; about 46 characters fit across the panel.
        let lines = |h: HWND| -> i32 {
            let text = ui::text(h);
            let n: usize = text
                .lines()
                .map(|l| l.chars().count().div_ceil(46).max(1))
                .sum();
            16 * n as i32
        };
        let mut side = vec![
            fixed(self.name, layout::ROW - 8),
            fixed(self.facts, 16 * self.facts_rows.get().max(1)),
        ];
        for h in [self.note, self.budget, self.result] {
            if !ui::text(h).is_empty() {
                side.push(fixed(h, lines(h)));
            }
        }
        if ui::shown(self.fork) {
            side.push(Node::row(vec![
                Node::leaf(
                    self.fork_caption,
                    Size::Fixed(110),
                    Size::Fixed(layout::ROW),
                ),
                Node::leaf(
                    self.fork,
                    Size::Fixed(DETAIL_W - 110 - layout::GAP),
                    Size::Fixed(layout::ROW),
                ),
            ]));
        }
        let mut buttons = Vec::new();
        if ui::shown(self.extract) {
            buttons.push(Node::button(self.extract, 130));
        }
        if ui::shown(self.export) {
            buttons.push(Node::button(self.export, DETAIL_W - 130 - layout::GAP));
        }
        if !buttons.is_empty() {
            side.push(Node::row(buttons));
        }
        if ui::shown(self.nested) {
            side.push(Node::button(self.nested, 150));
        }
        side.extend(self.panel_tools.nodes(DETAIL_W));
        side.push(Node::leaf(
            self.preview,
            Size::Fixed(DETAIL_W),
            Size::Stretch(1),
        ));
        let mut body = Vec::new();
        if ui::shown(self.pane.tree) {
            body.push(Node::leaf(
                self.pane.tree,
                Size::Fixed(TREE_W),
                Size::Stretch(1),
            ));
        }
        body.push(Node::leaf(
            self.pane.list,
            Size::Stretch(1),
            Size::Stretch(1),
        ));
        body.push(Node::column(side));
        col.push(Node::row(body));
        let mut foot = vec![Node::leaf(
            self.status,
            Size::Stretch(1),
            Size::Fixed(layout::ROW),
        )];
        foot.extend(self.mark_row.nodes());
        col.push(Node::row(foot));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in layout::layout(&tree, area, dpi) {
            if ui::shown(h) {
                ui::move_to(h, r);
            }
        }
    }
}

impl Handler for BrowseView {
    fn command(&self, from: HWND, code: u32) {
        if code == DEFERRED && from == self.pane.tree {
            if let Some(action) = self.pane.take_deferred() {
                self.apply(A::Focus(Side::Left));
                self.apply(A::Pane(Side::Left, action));
            }
            return;
        }
        if code == VIEW_MENU {
            self.view_menu();
            return;
        }
        if self.sync.active() {
            return;
        }
        let bar = self
            .tools
            .command(from, code)
            .or_else(|| self.edit_row.command(from, code))
            .or_else(|| self.mark_row.command(from, code));
        if let Some(cmd) = bar {
            self.bar(cmd);
            return;
        }
        if let Some(cmd) = self.panel_tools.command(from, code) {
            let Some(path) = self.cursor_path() else {
                return;
            };
            match cmd {
                PanelCmd::OnPath(f) => {
                    for a in f(path) {
                        self.apply(a);
                    }
                }
                PanelCmd::MacMenu(button) => {
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
                    let mut r = RECT::default();
                    // SAFETY: a plain query of our own button.
                    let _ = unsafe { GetWindowRect(button, &mut r) };
                    let Some(export) = track(self.owner(), items, r.left, r.bottom) else {
                        return;
                    };
                    let mode = self.extract_options().fork_mode;
                    self.apply(A::SetForkMode(mode));
                    self.apply(export);
                }
            }
            return;
        }
        if from == self.fork {
            if code == CBN_SELCHANGE {
                let mode = self.extract_options().fork_mode;
                self.apply(A::SetForkMode(mode));
            }
            return;
        }
        if code != windows::Win32::UI::WindowsAndMessaging::BN_CLICKED {
            return;
        }
        if from == self.close {
            self.apply(A::Mode(ModeAction::RequestClose));
        } else if from == self.cancel {
            let copying = self.ctrl.borrow().copy_job().is_some();
            self.apply(match copying {
                true => A::CancelCopy,
                false => A::Pane(Side::Left, P::CancelOpen),
            });
        } else if from == self.notice_ok {
            self.show_notice(None);
        } else if from == self.extract_all {
            self.extract("/".to_string());
        } else if from == self.export_volume {
            self.apply(A::ExportEntry {
                from: Side::Left,
                path: "/".to_string(),
                format: Some(ExportFormat::TarGz),
            });
        } else if from == self.extract {
            if let Some(path) = self.cursor_path() {
                self.extract(path);
            }
        } else if from == self.export {
            if let Some(path) = self.cursor_path() {
                let mode = self.extract_options().fork_mode;
                self.apply(A::SetForkMode(mode));
                self.apply(A::ExportEntry {
                    from: Side::Left,
                    path,
                    format: Some(ExportFormat::TarGz),
                });
            }
        } else if from == self.nested {
            if let Some(path) = self.cursor_path() {
                self.apply(A::Mode(ModeAction::OpenNested(path)));
            }
        }
    }

    fn notify(&self, hdr: &NMHDR, lp: LPARAM) -> Option<LRESULT> {
        let (answer, event) = self.pane.notify(hdr, lp)?;
        if let Some(event) = event {
            self.pane_event(event);
        }
        answer
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {}
}
