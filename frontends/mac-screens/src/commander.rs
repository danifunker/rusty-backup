//! Commander Mode: two browser panes that take over the window, on one `CommanderController`.
//!
//! The Win32 shell's `commander.rs` is the reference. Each side is a [`SideView`]: its source,
//! volume and path rows over the same [`PaneView`] Inspect's browse view uses; between them is the
//! copy column. Every window comes from the controller (`side_windows` for each side,
//! `screen_windows`) through [`ModelWindow`]. Every control reports to
//! [`CommanderScreen::fired`] by its [`Leaf`]: the side and the control, or the screen's own.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::{CommanderRecent, Notify, Snapshot, UserConfig};
use rusty_backup::controller::commander::{
    ids, BrowserPaneAction as P, CommanderAction as A, CommanderController, CopyColumn, DropPlan,
    ExportFormat, JobRegistry, PaneServices, PaneWindow, ResourceForkMode, Side, SideBars,
    FOLLOW_DELAY,
};
use rusty_backup::controller::keys::{Command, Key, KeyMap, Mods};
use rusty_backup::controller::{Controller, Effect};

use super::effects::{EffectHost, Reply, ReplyFn};
use super::form::Form;
use super::host::{Host, Surface};
use super::layout::{self, Node, Size};
use super::model_window::{Event, ModelWindow};
use super::pane_view::{PaneEvent, PaneShown, PaneView};
use super::popup::{track, MenuItem};
use super::sync::SyncGuard;

const SIDES: [Side; 2] = [Side::Left, Side::Right];

/// The copy column's width, in points.
const COLUMN_W: i32 = 150;

fn idx(side: Side) -> usize {
    match side {
        Side::Left => 0,
        Side::Right => 1,
    }
}

fn new_controller(jobs: &JobRegistry, notify: &Arc<dyn Notify>) -> CommanderController {
    let services = || PaneServices {
        jobs: jobs.clone(),
        notify: Arc::clone(notify),
        recent: Box::new(CommanderRecent(UserConfig)),
    };
    let (keymap, problems) = KeyMap::load(KeyMap::user_file().as_deref());
    let mut ctl = CommanderController::new(services(), services(), keymap);
    ctl.report_keymap_problems(problems);
    ctl
}

/// A side's controls.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum S {
    Source,
    OpenFile,
    OpenFolder,
    OpenBackup,
    Connect,
    Close,
    CloseImage,
    Partition,
    NoParts,
    Title,
    Free,
    ReadOnly,
    NewFolder,
    Apply,
    Discard,
    Edits,
    Job,
    Progress,
    CancelOpen,
    UnlockNote,
    Password,
    Unlock,
    QuickHint,
    Quick,
    Tree,
    List,
}

/// The screen's own controls.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum T {
    Title,
    View,
    Log,
    Close,
    CopyJob,
    CopyProgress,
    CancelCopy,
    Ltr,
    Rtl,
    Delete,
    Compare,
    KeepDates,
    FormatCaption,
    Format,
    ForkCaption,
    Fork,
    Status,
}

/// A control on the page: a side's, a side's path crumb or recent source, or the screen's.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Leaf {
    Side(usize, S),
    Crumb(usize, usize),
    Recent(usize, usize),
    Top(T),
}

/// One side: its bars, its job line, the unlock row and the listing (or the other side's quick view).
struct SideView {
    side: Side,
    pane: PaneView,
    /// Each crumb's label and target folder; the crumbs' buttons are rebuilt with the path.
    crumbs: RefCell<Vec<(String, Option<String>)>>,
    bars: RefCell<Option<SideBars>>,
    edits_open: Cell<bool>,
}

/// What one side shows this refresh.
struct SideShown {
    bars: SideBars,
    job: Option<(Snapshot, bool)>,
    pane: PaneShown,
}

impl SideView {
    fn build(f: &Form<Leaf>, side: Side) -> SideView {
        let names = match side {
            Side::Left => ids::LEFT,
            Side::Right => ids::RIGHT,
        };
        let i = idx(side);
        let l = |s: S| Leaf::Side(i, s);
        f.pulldown(l(S::Source), names.source);
        f.button(l(S::OpenFile), "File...", names.open_file);
        f.button(l(S::OpenFolder), "Folder...", names.open_folder);
        f.button(l(S::OpenBackup), "Backup...", names.open_backup);
        f.button(l(S::Connect), "Remote...", names.connect);
        f.button(l(S::Close), "Close", names.close);
        f.button(l(S::CloseImage), "Close image", names.close_image);
        f.popup(l(S::Partition), &[], names.partition);
        f.label(l(S::NoParts), "");
        f.label(l(S::Title), "");
        f.label(l(S::Free), "");
        f.label(l(S::ReadOnly), "Read-only");
        f.button(l(S::NewFolder), "New Folder...", names.new_folder);
        f.button(l(S::Apply), "Apply", names.apply);
        f.button(l(S::Discard), "Discard", names.discard);
        f.button(l(S::Edits), "Edits", names.edits);
        f.label(l(S::Job), "");
        f.progress(l(S::Progress));
        f.button(l(S::CancelOpen), "Cancel", names.cancel_open);
        f.wrap(l(S::UnlockNote), "");
        f.field(l(S::Password), "", "Password", true);
        f.button(l(S::Unlock), "Unlock", names.unlock);
        f.label(l(S::QuickHint), "");
        f.text_view(l(S::Quick), "");
        let pane = PaneView::build(f, l(S::List), l(S::Tree), side, names.list, "");
        f.show(l(S::Quick), false);
        f.show(l(S::QuickHint), false);
        SideView {
            side,
            pane,
            crumbs: RefCell::default(),
            bars: RefCell::default(),
            edits_open: Cell::new(false),
        }
    }

    fn l(&self, s: S) -> Leaf {
        Leaf::Side(idx(self.side), s)
    }

    fn read(&self, ctl: &CommanderController) -> SideShown {
        let pane = ctl.pane(self.side);
        let mut jobs: Vec<(Snapshot, bool)> = Vec::new();
        jobs.extend(pane.opening().map(|s| (s, true)));
        jobs.extend(pane.wrapper_opening().map(|(_, s)| (s, false)));
        jobs.extend(pane.checking().map(|s| (s, false)));
        jobs.extend(pane.host_writing().map(|s| (s, false)));
        jobs.extend(pane.applying().map(|s| (s, false)));
        jobs.extend(pane.tool_working().map(|s| (s, false)));
        jobs.extend(pane.archive_working().map(|s| (s, false)));
        SideShown {
            bars: ctl.side_bars(self.side),
            job: jobs.into_iter().next(),
            pane: self.pane.read(ctl),
        }
    }

    /// Show `s`; true when the bars changed, so the screen lays out again.
    fn show(&self, f: &Form<Leaf>, s: SideShown) -> bool {
        let (line, pct) = s
            .job
            .as_ref()
            .map(|(j, _)| super::jobs::job_line(j))
            .unwrap_or_default();
        f.set_text(self.l(S::Job), &line);
        f.show(self.l(S::Job), s.job.is_some());
        f.set_progress(self.l(S::Progress), s.job.is_some(), pct);
        f.show(
            self.l(S::CancelOpen),
            s.job.as_ref().is_some_and(|(_, c)| *c),
        );
        self.pane.show(s.pane);
        if self.bars.borrow().as_ref() == Some(&s.bars) {
            return false;
        }
        let b = s.bars;
        self.show_source(f, &b.source, &b.recent);
        f.show(self.l(S::CloseImage), b.close_image.is_some());
        f.enable(self.l(S::CloseImage), b.close_image.unwrap_or(false));
        f.show(self.l(S::Close), b.can_close);
        for x in [S::Apply, S::Discard] {
            f.show(self.l(x), b.stages);
            f.enable(self.l(x), b.apply_enabled);
        }
        f.set_title(self.l(S::Apply), &b.apply_label);
        f.show(self.l(S::Edits), b.stages && b.staged > 0);
        f.set_title(self.l(S::Edits), &b.edits_label);
        self.edits_open.set(b.edits_open);
        f.set_items(self.l(S::Partition), &b.parts);
        if let Some(i) = b.part {
            f.select(self.l(S::Partition), i);
        }
        // Shown whenever the source has partitions, so one that fails to open can be left.
        f.show(self.l(S::Partition), !b.parts.is_empty());
        f.set_text(self.l(S::NoParts), b.no_parts);
        f.show(
            self.l(S::NoParts),
            b.loaded && b.parts.is_empty() && !b.no_parts.is_empty(),
        );
        f.show(self.l(S::NewFolder), b.new_folder);
        f.set_text(self.l(S::Title), &b.title);
        f.show(self.l(S::Title), b.loaded);
        f.set_text(self.l(S::Free), b.free.as_deref().unwrap_or(""));
        f.show(self.l(S::Free), b.loaded && b.free.is_some());
        f.show(self.l(S::ReadOnly), b.read_only);
        self.show_crumbs(f, &b.prefix, &b.crumbs);
        let locked = b.locked;
        for x in [S::UnlockNote, S::Password, S::Unlock] {
            f.show(self.l(x), locked.is_some());
        }
        if let Some(apfs) = locked {
            let note = match apfs {
                true => "This APFS volume is encrypted with FileVault. Enter the volume password or personal recovery key.",
                false => "This source is password-protected.",
            };
            f.set_text(self.l(S::UnlockNote), note);
        }
        *self.bars.borrow_mut() = Some(b);
        true
    }

    /// The source pull-down: its title is the source; its menu the recent sources and the ways to open one.
    fn show_source(&self, f: &Form<Leaf>, title: &str, recent: &[String]) {
        let title = if title.is_empty() {
            "Open image, backup, or folder..."
        } else {
            title
        };
        let i = idx(self.side);
        let mut items: Vec<(String, Option<Leaf>, bool)> = recent
            .iter()
            .take(8)
            .enumerate()
            .map(|(n, path)| (path.clone(), Some(Leaf::Recent(i, n)), true))
            .collect();
        for (label, s) in [
            ("Open Image File...", S::OpenFile),
            ("Open Host Folder...", S::OpenFolder),
            ("Open Backup Folder...", S::OpenBackup),
            ("Connect to Remote...", S::Connect),
        ] {
            items.push((label.to_string(), Some(self.l(s)), true));
        }
        f.set_pulldown(self.l(S::Source), title, &items);
    }

    /// The path's crumbs as buttons; the last, where the pane is, cannot be pressed.
    fn show_crumbs(&self, f: &Form<Leaf>, prefix: &str, crumbs: &[(String, Option<String>)]) {
        let i = idx(self.side);
        f.remove(|k| matches!(k, Leaf::Crumb(s, _) if *s == i));
        let mut targets = Vec::new();
        let mut add = |label: &str, target: Option<String>, link: bool| {
            let k = Leaf::Crumb(i, targets.len());
            f.button(k, label, "");
            f.enable(k, link);
            targets.push((label.to_string(), target.filter(|_| link)));
        };
        if !prefix.is_empty() {
            add(prefix, None, false);
        }
        if crumbs.is_empty() {
            add("/", None, false);
        }
        let last = crumbs.len().saturating_sub(1);
        for (n, (label, t)) in crumbs.iter().enumerate() {
            let link = t.is_some() && n < last;
            add(label, t.clone(), link);
        }
        *self.crumbs.borrow_mut() = targets;
    }

    /// The crumbs' row: each as wide as its label.
    fn crumb_row(&self, f: &Form<Leaf>) -> Node<Leaf> {
        let i = idx(self.side);
        let _ = f;
        let mut row: Vec<Node<Leaf>> = self
            .crumbs
            .borrow()
            .iter()
            .enumerate()
            .map(|(n, (label, _))| {
                let w = (label.chars().count() as i32 * 7 + 16).max(24);
                Node::button(Leaf::Crumb(i, n), w)
            })
            .collect();
        row.push(Node::Space(Size::Stretch(1)));
        Node::Row {
            children: row,
            gap: 2,
        }
    }

    /// The side's rows: bars, path, job, then the listing, the unlock row or the quick view.
    fn nodes(&self, f: &Form<Leaf>) -> Vec<Node<Leaf>> {
        let l = |s: S| self.l(s);
        // A pane is about half the window: the bars take four short rows, not two long ones.
        let first = vec![
            Node::leaf(l(S::Source), Size::Stretch(1), Size::Fixed(layout::ROW + 2)),
            Node::button(l(S::Close), 65),
        ];
        let opens = vec![
            // Sized to their labels: at 800 px wide a pane is about 310 px.
            Node::button(l(S::OpenFile), 60),
            Node::button(l(S::OpenFolder), 72),
            Node::button(l(S::OpenBackup), 76),
            Node::button(l(S::Connect), 78),
            Node::button(l(S::CloseImage), 95),
            Node::Space(Size::Stretch(1)),
        ];
        let mut second = vec![
            Node::leaf(
                l(S::Partition),
                Size::Fixed(170),
                Size::Fixed(layout::ROW + 2),
            ),
            Node::leaf(
                l(S::NoParts),
                Size::Fixed(110),
                Size::Fixed(layout::ROW - 4),
            ),
            Node::leaf(l(S::Title), Size::Stretch(1), Size::Fixed(layout::ROW - 4)),
            Node::leaf(l(S::Free), Size::Fixed(110), Size::Fixed(layout::ROW - 4)),
            Node::leaf(
                l(S::ReadOnly),
                Size::Fixed(75),
                Size::Fixed(layout::ROW - 4),
            ),
        ];
        if !f.shown(l(S::Title)) {
            second.push(Node::Space(Size::Stretch(1)));
        }
        second.push(Node::button(l(S::NewFolder), 105));
        let edits = vec![
            Node::Space(Size::Stretch(1)),
            Node::button(l(S::Apply), 90),
            Node::button(l(S::Discard), 75),
            Node::button(l(S::Edits), 85),
        ];
        let mut rows = vec![
            Node::row(first),
            Node::row(opens),
            Node::row(second),
            Node::row(edits),
            self.crumb_row(f),
            Node::row(vec![
                Node::leaf(l(S::Job), Size::Stretch(1), Size::Fixed(layout::ROW - 4)),
                Node::leaf(
                    l(S::Progress),
                    Size::Fixed(120),
                    Size::Fixed(layout::ROW - 4),
                ),
                Node::button(l(S::CancelOpen), 75),
            ]),
        ];
        if f.shown(l(S::Unlock)) {
            rows.push(Node::leaf(
                l(S::UnlockNote),
                Size::Stretch(1),
                Size::Fixed(2 * (layout::ROW - 4)),
            ));
            rows.push(Node::row(vec![
                Node::leaf(l(S::Password), Size::Stretch(1), Size::Fixed(layout::ROW)),
                Node::button(l(S::Unlock), 75),
            ]));
            rows.push(Node::Space(Size::Stretch(1)));
        } else if f.shown(l(S::Quick)) {
            rows.push(Node::leaf(
                l(S::QuickHint),
                Size::Stretch(1),
                Size::Fixed(layout::ROW - 4),
            ));
            rows.push(Node::leaf(l(S::Quick), Size::Stretch(1), Size::Stretch(1)));
        } else {
            rows.push(Node::row(vec![
                Node::leaf(l(S::Tree), Size::Fixed(160), Size::Stretch(1)),
                Node::leaf(l(S::List), Size::Stretch(1), Size::Stretch(1)),
            ]));
        }
        rows
    }

    /// A side control's action, as the actions it sends.
    fn fired(&self, f: &Form<Leaf>, s: S) -> Option<Vec<A>> {
        let pane = |a| A::Pane(self.side, a);
        let a = match s {
            S::Partition => pane(P::SelectPartition(f.picked(self.l(S::Partition))?)),
            S::OpenFile => pane(P::PickImage),
            S::OpenFolder => pane(P::PickHostFolder),
            S::OpenBackup => pane(P::PickBackupFolder),
            S::Connect => pane(P::Connect),
            S::Close => pane(P::CloseSource),
            S::CloseImage => pane(P::CloseRemoteImage),
            S::NewFolder => pane(P::RequestNewFolder),
            S::Apply => pane(P::Apply),
            S::Discard => pane(P::Discard),
            S::Edits => pane(P::ShowEdits(!self.edits_open.get())),
            S::CancelOpen => pane(P::CancelOpen),
            S::Unlock => {
                let secret = f.text(self.l(S::Password));
                if secret.is_empty() {
                    return None;
                }
                f.set_text(self.l(S::Password), "");
                pane(P::SubmitPassword(secret))
            }
            _ => return None,
        };
        Some(vec![A::Focus(self.side), a])
    }

    /// A recent source picked from the source menu.
    fn recent(&self, n: usize) -> Option<Vec<A>> {
        let path = self.bars.borrow().as_ref()?.recent.get(n)?.clone();
        Some(vec![
            A::Focus(self.side),
            A::Pane(self.side, P::OpenPath(PathBuf::from(path))),
        ])
    }

    /// A path crumb pressed.
    fn crumb(&self, n: usize) -> Option<Vec<A>> {
        let target = self.crumbs.borrow().get(n)?.1.clone()?;
        Some(vec![
            A::Focus(self.side),
            A::Pane(self.side, P::NavigateTo(target)),
        ])
    }
}

/// Everything a refresh shows, read while the controller is borrowed.
struct Shown {
    status: String,
    log_lines: usize,
    copying: Option<Snapshot>,
    sides: Vec<SideShown>,
    column: CopyColumn,
    windows: Vec<PaneWindow>,
    active: Side,
    /// The side the quick view covers, its text and its hint.
    quick: Option<(Side, String, String)>,
    menu_up: bool,
    follow: bool,
}

pub struct CommanderScreen {
    ui: Rc<dyn Host>,
    ctrl: RefCell<CommanderController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<CommanderScreen>,
    page: Surface,
    form: Form<Leaf>,
    sides: [SideView; 2],
    windows: RefCell<Vec<(Option<Side>, Rc<ModelWindow>)>>,
    debug: [Cell<bool>; 2],
    active: Cell<Side>,
    /// `CloseScreen` arrived; the app drops the screen once the action in hand is done.
    closed: Cell<bool>,
    on_close: Rc<dyn Fn()>,
    follow_set: Cell<bool>,
}

impl CommanderScreen {
    /// The screen on `page`; `on_close` runs when the controller leaves it, and drops it.
    pub fn build(
        ui: Rc<dyn Host>,
        page: Surface,
        jobs: &JobRegistry,
        notify: &Arc<dyn Notify>,
        host: EffectHost,
        on_close: Rc<dyn Fn()>,
    ) -> Rc<CommanderScreen> {
        let screen = Rc::new_cyclic(|me: &Weak<CommanderScreen>| {
            let fire_me = me.clone();
            let form = Form::with_host(
                ui.clone(),
                page,
                Rc::new(move |l| {
                    if let Some(s) = fire_me.upgrade() {
                        s.fired(l);
                    }
                }),
            );
            let f = &form;
            let t = Leaf::Top;
            f.label(t(T::Title), "Commander Mode");
            f.button(t(T::View), "View...", ids::VIEW_MENU);
            f.button(t(T::Log), "Log (0)", ids::LOG);
            f.button(t(T::Close), "Close", ids::CLOSE);
            f.label(t(T::CopyJob), "");
            f.progress(t(T::CopyProgress));
            f.button(t(T::CancelCopy), "Cancel", ids::CANCEL_COPY);
            f.button(t(T::Ltr), "Copy ->", ids::COPY_LEFT_TO_RIGHT);
            f.button(t(T::Rtl), "<- Copy", ids::COPY_RIGHT_TO_LEFT);
            f.button(t(T::Delete), "Delete", ids::DELETE);
            f.button(t(T::Compare), "Compare", ids::COMPARE);
            f.enable(t(T::Compare), false);
            f.check(t(T::KeepDates), "Keep original dates", ids::KEEP_DATES);
            f.label(t(T::FormatCaption), "Export as:");
            let formats: Vec<String> = ExportFormat::ALL
                .iter()
                .map(|x| x.label().to_string())
                .collect();
            f.popup(t(T::Format), &formats, ids::EXPORT_FORMAT);
            f.label(t(T::ForkCaption), "Forks as:");
            let forks: Vec<String> = ResourceForkMode::ALL
                .iter()
                .map(|m| m.label().to_string())
                .collect();
            f.popup(t(T::Fork), &forks, ids::FORK_MODE);
            f.label(t(T::Status), "");
            let sides = [
                SideView::build(f, Side::Left),
                SideView::build(f, Side::Right),
            ];
            CommanderScreen {
                ui: ui.clone(),
                ctrl: RefCell::new(new_controller(jobs, notify)),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                page,
                form,
                sides,
                windows: RefCell::default(),
                debug: [Cell::new(false), Cell::new(false)],
                active: Cell::new(Side::Left),
                closed: Cell::new(false),
                on_close,
                follow_set: Cell::new(false),
            }
        });
        for side in SIDES {
            let me = screen.me.clone();
            screen.sides[idx(side)].pane.on_event(Rc::new(move |e| {
                if let Some(s) = me.upgrade() {
                    s.pane_event(side, e);
                }
            }));
        }
        screen.refresh();
        screen.sides[0].pane.focus();
        screen
    }

    /// The page the screen is on.
    pub fn page(&self) -> Surface {
        self.page
    }

    /// The list control of `side`, for a shell that hit-tests a drop against it.
    pub fn list_ctl(&self, side: Side) -> super::host::Ctl {
        self.sides[idx(side)].pane.list_ctl()
    }

    /// Files dropped from the desktop: `under` is the pane the drop landed on, else ask which.
    pub fn drop_paths(&self, paths: Vec<PathBuf>, under: Option<Side>) {
        let first = match paths.first() {
            Some(p) => p,
            None => return,
        };
        let side = match under {
            Some(side) => side,
            None => {
                let what = match paths.len() {
                    1 => format!(
                        "\"{}\"",
                        first.file_name().map_or_else(
                            || first.display().to_string(),
                            |n| n.to_string_lossy().into_owned()
                        )
                    ),
                    n => format!("these {} items", n),
                };
                let message = format!("Which pane should take {}?", what);
                let buttons = ["Left pane".to_string(), "Right pane".to_string()];
                match self.ui.ask("Dropped files", &message, &buttons) {
                    Some(0) => Side::Left,
                    Some(1) => Side::Right,
                    _ => return,
                }
            }
        };
        let plan = self.ctrl.borrow().drop_plan(side, &paths);
        let actions = match plan {
            None => return,
            Some(DropPlan::Do(actions)) => actions,
            Some(DropPlan::Ask {
                title,
                question,
                choices,
            }) => {
                let labels: Vec<String> = choices.iter().map(|(l, _)| l.clone()).collect();
                let i = match self.ui.ask(&title, &question, &labels) {
                    Some(i) => i,
                    None => return,
                };
                choices
                    .into_iter()
                    .nth(i)
                    .map(|(_, a)| a)
                    .unwrap_or_default()
            }
        };
        for a in actions {
            self.apply(a);
        }
    }

    /// Staged edits would be lost if the window closed now.
    pub fn has_staged_edits(&self) -> bool {
        self.ctrl.borrow().has_staged_edits()
    }

    /// The window is closing with edits staged: ask the controller, which may quit after.
    pub fn hold_quit(&self) {
        self.apply(A::RequestQuit);
    }

    fn apply(&self, action: A) {
        let fx = self.ctrl.borrow_mut().apply(action);
        self.after(fx);
    }

    pub fn tick(&self) {
        let fx = self.ctrl.borrow_mut().tick();
        self.after(fx);
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let screen = match me.upgrade() {
                Some(s) => s,
                None => return,
            };
            let action = match reply {
                Reply::Picked { purpose, path } => A::Picked { purpose, path },
                Reply::Confirmed { purpose, yes } => A::Confirmed { purpose, yes },
                Reply::Answered { purpose, text } => A::Answered { purpose, text },
                Reply::Chose { purpose, choice } => A::Chose { purpose, choice },
                Reply::CloseScreen => {
                    screen.closed.set(true);
                    screen.after(Vec::new());
                    return;
                }
            };
            screen.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        if self.closed.get() {
            self.windows.borrow_mut().clear();
            (self.on_close)();
            return;
        }
        self.refresh();
    }

    /// Run a key through the key map; true when it meant something here.
    fn key(&self, key: Key, mods: Mods) -> bool {
        let (command, side) = {
            let ctl = self.ctrl.borrow();
            if ctl.modal_open() || !ctl.binds(key, mods) {
                return false;
            }
            (ctl.command_for(key, mods), ctl.active())
        };
        if matches!(command, Some(Command::PageUp | Command::PageDown)) {
            let rows = self.sides[idx(side)].pane.page_rows();
            self.apply(A::SetPageRows(side, rows));
        }
        self.apply(A::Key(key, mods));
        true
    }

    fn read(&self) -> Shown {
        let ctl = self.ctrl.borrow();
        let active = ctl.active();
        let quick = ctl.quick_view().then(|| {
            let side = active.other();
            let text = ctl
                .quick_view_detail()
                .map(|d| d.info_text(ctl.browse_caps()))
                .unwrap_or_default();
            let hint = ctl.key_hint(Command::QuickView);
            (side, text, format!("Quick view ({} to close)", hint))
        });
        let mut windows = Vec::new();
        for side in SIDES {
            windows.extend(ctl.side_windows(side, self.debug[idx(side)].get()));
        }
        windows.extend(ctl.screen_windows());
        Shown {
            status: ctl.status().to_string(),
            log_lines: ctl.log().len(),
            copying: ctl.copy_job().map(|(_, s)| s),
            sides: self.sides.iter().map(|s| s.read(&ctl)).collect(),
            column: ctl.copy_column(),
            windows,
            active,
            quick,
            menu_up: ctl.view_menu_shown(),
            follow: ctl.follow_pending(),
        }
    }

    /// Read the controller first and release it, then set the views inside the guard.
    fn refresh(&self) {
        let shown = self.read();
        let follow = shown.follow;
        let menu_up = shown.menu_up;
        self.sync.run(|| {
            self.show(shown);
        });
        self.layout();
        if menu_up {
            let me = self.me.clone();
            self.ui.later(Box::new(move || {
                if let Some(s) = me.upgrade() {
                    s.view_menu();
                }
            }));
        }
        if follow && !self.follow_set.replace(true) {
            let me = self.me.clone();
            let ms = FOLLOW_DELAY.as_millis() as u64;
            self.ui.after(
                ms,
                Box::new(move || {
                    if let Some(s) = me.upgrade() {
                        s.tick();
                    }
                }),
            );
        } else if !follow {
            self.follow_set.set(false);
        }
    }

    fn show(&self, s: Shown) {
        let f = &self.form;
        let t = Leaf::Top;
        f.set_text(t(T::Status), &s.status);
        f.set_title(t(T::Log), &format!("Log ({})", s.log_lines));
        let (line, pct) = s
            .copying
            .as_ref()
            .map(super::jobs::job_line)
            .unwrap_or_default();
        f.set_text(t(T::CopyJob), &line);
        for x in [T::CopyJob, T::CancelCopy] {
            f.show(t(x), s.copying.is_some());
        }
        f.set_progress(t(T::CopyProgress), s.copying.is_some(), pct);
        for (view, side) in self.sides.iter().zip(s.sides) {
            view.show(f, side);
        }
        self.show_column(&s.column);
        self.show_quick(s.quick);
        self.show_windows(s.windows);
        if self.active.replace(s.active) != s.active {
            self.sides[idx(s.active)].pane.focus();
        }
    }

    fn show_column(&self, c: &CopyColumn) {
        let f = &self.form;
        let t = Leaf::Top;
        f.enable(t(T::Ltr), c.copy_left);
        f.enable(t(T::Rtl), c.copy_right);
        f.enable(t(T::Delete), c.delete);
        f.set_check(t(T::KeepDates), c.keep_dates);
        f.select(t(T::Format), c.format);
        f.select(t(T::Fork), c.fork);
        f.enable(t(T::Fork), c.loose);
    }

    fn show_quick(&self, quick: Option<(Side, String, String)>) {
        let f = &self.form;
        let covered = quick.as_ref().map(|q| q.0);
        for view in &self.sides {
            let on = covered == Some(view.side);
            f.show(view.l(S::Quick), on);
            f.show(view.l(S::QuickHint), on);
            f.show(view.l(S::List), !on);
            if on {
                f.show(view.l(S::Tree), false);
            }
        }
        if let Some((side, text, hint)) = quick {
            let view = &self.sides[idx(side)];
            f.set_text_view(view.l(S::Quick), &text);
            f.set_text(view.l(S::QuickHint), &hint);
        }
    }

    /// Open, update or close the controller's windows: one per side and kind.
    fn show_windows(&self, wanted: Vec<PaneWindow>) {
        let mut open = self.windows.borrow_mut();
        open.retain(|(side, w)| wanted.iter().any(|p| (p.side, p.kind) == (*side, w.kind)));
        for p in wanted {
            let at = open
                .iter()
                .position(|(side, w)| (*side, w.kind) == (p.side, p.kind));
            if let Some((_, w)) = at.map(|i| &open[i]).filter(|(_, w)| w.key == p.key) {
                w.update(&p);
                continue;
            }
            let frame = at.and_then(|i| open.remove(i).1.frame());
            let me = self.me.clone();
            let side = p.side;
            let ui = self.ui.clone();
            let on_event: Rc<dyn Fn(Event)> = Rc::new(move |e| {
                let me = me.clone();
                ui.later(Box::new(move || {
                    if let Some(screen) = me.upgrade() {
                        screen.window_event(side, e);
                    }
                }));
            });
            open.push((
                p.side,
                ModelWindow::open(self.ui.clone(), &p, frame, on_event),
            ));
        }
    }

    fn window_event(&self, side: Option<Side>, e: Event) {
        match e {
            Event::Send(actions) => {
                for a in actions {
                    self.apply(a);
                }
            }
            Event::Copy(text) => self.ui.copy_text(&text),
            Event::Toggle(on) => {
                if let Some(side) = side {
                    self.debug[idx(side)].set(on);
                }
                self.refresh();
            }
            Event::Closed => self.refresh(),
        }
    }

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
        let c = self.form.ctl(Leaf::Top(T::View));
        let (_, h) = self.ui.control_size(self.page, c);
        if let Some(a) = track(&*self.ui, self.page, c, items, 0.0, h + 2.0) {
            self.apply(a);
        }
    }

    /// What the user did in `side`'s listing, as actions on the controller.
    fn pane_event(&self, side: Side, event: PaneEvent) {
        if self.sync.active() {
            return;
        }
        let pane = |a| A::Pane(side, a);
        match event {
            PaneEvent::Click { row, ctrl, shift } => {
                let click = self.ctrl.borrow().pane(side).row_click(&row, ctrl, shift);
                self.apply(A::Focus(side));
                if let Some(action) = click {
                    self.apply(pane(action));
                }
            }
            PaneEvent::Mark(id) => self.apply(pane(P::Mark(id))),
            PaneEvent::Toggle(id) => self.apply(pane(P::ToggleExpand(id))),
            PaneEvent::Activate => {
                let id = {
                    let ctl = self.ctrl.borrow();
                    let p = ctl.pane(side);
                    p.cursor_index().map(|i| p.rows()[i].node_id.clone())
                };
                self.apply(A::Focus(side));
                if let Some(id) = id {
                    self.apply(pane(P::Activate(id)));
                }
            }
            PaneEvent::Key(key, mods) => {
                if self.ctrl.borrow().active() != side {
                    self.apply(A::Focus(side));
                }
                self.key(key, mods);
            }
            PaneEvent::Sort(col) => self.apply(pane(P::SortBy(col))),
            PaneEvent::Menu(i, x, y) => {
                let row = match self.sides[idx(side)].pane.row_at(i) {
                    Some(r) => r,
                    None => return,
                };
                self.apply(A::Focus(side));
                let context = {
                    let ctl = self.ctrl.borrow();
                    let p = ctl.pane(side);
                    p.applying()
                        .is_none()
                        .then(|| p.row_context(&row))
                        .flatten()
                };
                if let Some(a) = context {
                    self.apply(pane(a));
                }
                let items: Vec<MenuItem> = self
                    .ctrl
                    .borrow()
                    .row_menu(side, &row)
                    .into_iter()
                    .map(MenuItem::row)
                    .collect();
                if items.is_empty() {
                    return;
                }
                let list = self.sides[idx(side)].pane.list_ctl();
                if let Some(a) = track(&*self.ui, self.page, list, items, x, y) {
                    self.apply(a);
                }
            }
            PaneEvent::Folder(a) => {
                self.apply(A::Focus(side));
                self.apply(pane(a));
            }
            PaneEvent::Focused => {
                if self.ctrl.borrow().active() != side {
                    self.apply(A::Focus(side));
                }
            }
        }
    }

    /// A control's action, by its leaf.
    fn fired(&self, l: Leaf) {
        if self.sync.active() {
            return;
        }
        let actions = match l {
            Leaf::Recent(i, n) => self.sides.get(i).and_then(|s| s.recent(n)),
            Leaf::Crumb(i, n) => self.sides.get(i).and_then(|s| s.crumb(n)),
            Leaf::Side(i, s) => self.sides.get(i).and_then(|v| v.fired(&self.form, s)),
            Leaf::Top(t) => self.top_fired(t),
        };
        for a in actions.unwrap_or_default() {
            self.apply(a);
        }
    }

    fn top_fired(&self, t: T) -> Option<Vec<A>> {
        let f = &self.form;
        Some(vec![match t {
            T::View => {
                self.view_menu();
                return None;
            }
            T::Log => A::ShowLog(!self.ctrl.borrow().log_shown()),
            T::Close => A::RequestClose,
            T::CancelCopy => A::CancelCopy,
            T::Ltr => A::Copy(Side::Left),
            T::Rtl => A::Copy(Side::Right),
            T::Delete => A::Delete,
            T::KeepDates => A::SetKeepDates(f.checked(Leaf::Top(T::KeepDates))),
            T::Format => {
                let i = f.picked(Leaf::Top(T::Format))?;
                A::SetExportFormat(*ExportFormat::ALL.get(i)?)
            }
            T::Fork => {
                let i = f.picked(Leaf::Top(T::Fork))?;
                A::SetForkMode(*ResourceForkMode::ALL.get(i)?)
            }
            _ => return None,
        }])
    }

    pub fn layout(&self) {
        let f = &self.form;
        let w = COLUMN_W;
        let top = Leaf::Top;
        let line = |t: T| Node::leaf(top(t), Size::Fixed(w), Size::Fixed(layout::ROW - 4));
        let column = Node::column(vec![
            Node::Space(Size::Fixed(3 * layout::ROW)),
            Node::button(top(T::Ltr), w),
            Node::button(top(T::Rtl), w),
            Node::button(top(T::Delete), w),
            Node::button(top(T::Compare), w),
            Node::leaf(top(T::KeepDates), Size::Fixed(w), Size::Fixed(layout::ROW)),
            line(T::FormatCaption),
            Node::leaf(top(T::Format), Size::Fixed(w), Size::Fixed(layout::ROW + 2)),
            line(T::ForkCaption),
            Node::leaf(top(T::Fork), Size::Fixed(w), Size::Fixed(layout::ROW + 2)),
            Node::Space(Size::Stretch(1)),
        ]);
        let col = vec![
            Node::row(vec![
                Node::leaf(
                    top(T::Title),
                    Size::Fixed(150),
                    Size::Fixed(layout::ROW - 4),
                ),
                Node::button(top(T::View), 80),
                Node::Space(Size::Stretch(1)),
                Node::button(top(T::Log), 90),
                Node::button(top(T::Close), 75),
            ]),
            Node::row(vec![
                Node::leaf(
                    top(T::CopyJob),
                    Size::Stretch(1),
                    Size::Fixed(layout::ROW - 4),
                ),
                Node::leaf(
                    top(T::CopyProgress),
                    Size::Fixed(160),
                    Size::Fixed(layout::ROW - 4),
                ),
                Node::button(top(T::CancelCopy), 75),
            ]),
            Node::row(vec![
                Node::column(self.sides[0].nodes(f)),
                column,
                Node::column(self.sides[1].nodes(f)),
            ]),
            Node::leaf(
                top(T::Status),
                Size::Stretch(1),
                Size::Fixed(layout::ROW - 4),
            ),
        ];
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        let keep = |l: &Leaf| match l {
            Leaf::Side(i, S::Job) => f.shown(Leaf::Side(*i, S::Progress)),
            Leaf::Recent(..) => false,
            _ => f.shown(*l),
        };
        let tree = match tree.prune(&keep) {
            Some(t) => t,
            None => return,
        };
        f.place(&tree, self.ui.area(self.page));
    }
}
