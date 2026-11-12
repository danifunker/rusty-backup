//! Commander Mode: two browser panes that take over the window, on one `CommanderController`.
//!
//! The Win32 shell's `commander.rs` is the reference. Each side is a [`SideView`]: its source,
//! volume and path rows (`side_bars`) over the same [`PaneView`] Inspect's browse view uses;
//! between them is the copy column (`copy_column`). Every window comes from the controller
//! (`side_windows` for each side, `screen_windows`) through [`ModelWindow`]. Every control's action
//! comes to [`CommanderScreen::fired`] by its tag: the side, then the control.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use objc2::rc::Retained;
use objc2::runtime::AnyObject;
use objc2::{MainThreadMarker, Message};
use objc2_app_kit::{
    NSAccessibility, NSButton, NSColor, NSControl, NSControlStateValueOff, NSControlStateValueOn,
    NSPopUpButton, NSProgressIndicator, NSScrollView, NSSecureTextField, NSTextField, NSTextView,
    NSView,
};
use objc2_foundation::{NSPoint, NSRect, NSSize};
use rusty_backup::controller::app::{CommanderRecent, Notify, Snapshot, UserConfig};
use rusty_backup::controller::commander::{
    ids, BrowserPaneAction as P, CommanderAction as A, CommanderController, CopyColumn, DropPlan,
    ExportFormat, JobRegistry, PaneServices, PaneWindow, ResourceForkMode, Side, SideBars,
    FOLLOW_DELAY,
};
use rusty_backup::controller::keys::{Command, Key, KeyMap, Mods};
use rusty_backup::controller::{Controller, Effect};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::model_window::{Event, ModelWindow};
use crate::pane_view::{PaneEvent, PaneShown, PaneView};
use crate::popup::{track, MenuItem};
use crate::sync::SyncGuard;
use crate::ui::{self, ns, Callback};

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

/// A side's controls, as tags and places in the layout.
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
    Path,
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

const SIDE_ALL: [S; 27] = [
    S::Source,
    S::OpenFile,
    S::OpenFolder,
    S::OpenBackup,
    S::Connect,
    S::Close,
    S::CloseImage,
    S::Partition,
    S::NoParts,
    S::Title,
    S::Free,
    S::ReadOnly,
    S::NewFolder,
    S::Apply,
    S::Discard,
    S::Edits,
    S::Path,
    S::Job,
    S::Progress,
    S::CancelOpen,
    S::UnlockNote,
    S::Password,
    S::Unlock,
    S::QuickHint,
    S::Quick,
    S::Tree,
    S::List,
];

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

const TOP_ALL: [T; 17] = [
    T::Title,
    T::View,
    T::Log,
    T::Close,
    T::CopyJob,
    T::CopyProgress,
    T::CancelCopy,
    T::Ltr,
    T::Rtl,
    T::Delete,
    T::Compare,
    T::KeepDates,
    T::FormatCaption,
    T::Format,
    T::ForkCaption,
    T::Fork,
    T::Status,
];

/// A layout leaf: a side's control or the screen's.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Leaf {
    Side(usize, S),
    Top(T),
}

/// Tags: a side's control is 1000 * (side + 1) + its place; the screen's is its place; a path
/// crumb is 10000 * (side + 1) + its place.
fn side_tag(side: usize, s: S) -> isize {
    1000 * (side as isize + 1) + SIDE_ALL.iter().position(|x| *x == s).unwrap_or(0) as isize
}

fn top_tag(t: T) -> isize {
    TOP_ALL.iter().position(|x| *x == t).unwrap_or(0) as isize
}

/// One side: its bars, its job line, the unlock row and the listing (or the other side's quick view).
struct SideView {
    side: Side,
    views: Vec<(S, Retained<NSView>)>,
    crumbs: RefCell<Vec<(Retained<NSButton>, Option<String>)>>,
    path: Retained<NSView>,
    quick: Retained<NSTextView>,
    progress: Retained<NSProgressIndicator>,
    pane: PaneView,
    bars: RefCell<Option<SideBars>>,
    edits_open: Cell<bool>,
}

/// What one side shows this refresh.
struct SideShown {
    bars: SideBars,
    job: Option<(Snapshot, bool)>,
    pane: PaneShown,
}

fn as_view<T: AsRef<NSView>>(v: &T) -> Retained<NSView> {
    v.as_ref().retain()
}

impl SideView {
    fn build(mtm: MainThreadMarker, page: &NSView, target: &Callback, side: Side) -> SideView {
        let names = match side {
            Side::Left => ids::LEFT,
            Side::Right => ids::RIGHT,
        };
        let i = idx(side);
        let mut views: Vec<(S, Retained<NSView>)> = Vec::new();
        let mut add = |s: S, v: Retained<NSView>| {
            if let Some(c) = v.downcast_ref::<NSControl>() {
                c.setTag(side_tag(i, s));
                if c.downcast_ref::<NSTextField>().is_none()
                    || c.downcast_ref::<NSSecureTextField>().is_some()
                {
                    ui::hook(c, target);
                }
            }
            page.addSubview(&v);
            views.push((s, v));
        };
        let button = |title: &str, id: &str| as_view(&ui::button(mtm, title, id));
        let label = |text: &str| as_view(&ui::label(mtm, text));
        let source = NSPopUpButton::initWithFrame_pullsDown(mtm.alloc(), NSRect::ZERO, true);
        ui::set_id(&source, names.source);
        add(S::Source, as_view(&source));
        add(S::OpenFile, button("File...", names.open_file));
        add(S::OpenFolder, button("Folder...", names.open_folder));
        add(S::OpenBackup, button("Backup...", names.open_backup));
        add(S::Connect, button("Remote...", names.connect));
        add(S::Close, button("Close", names.close));
        add(S::CloseImage, button("Close image", names.close_image));
        let partition = NSPopUpButton::initWithFrame_pullsDown(mtm.alloc(), NSRect::ZERO, false);
        ui::set_id(&partition, names.partition);
        add(S::Partition, as_view(&partition));
        add(S::NoParts, label(""));
        add(S::Title, label(""));
        add(S::Free, label(""));
        add(S::ReadOnly, label("Read-only"));
        add(S::NewFolder, button("New Folder...", names.new_folder));
        add(S::Apply, button("Apply", names.apply));
        add(S::Discard, button("Discard", names.discard));
        add(S::Edits, button("Edits", names.edits));
        let path = ui::flipped(mtm);
        add(S::Path, path.clone());
        add(S::Job, label(""));
        let progress = ui::progress(mtm);
        add(S::Progress, as_view(&progress));
        add(S::CancelOpen, button("Cancel", names.cancel_open));
        let note = ui::label(mtm, "");
        note.setLineBreakMode(objc2_app_kit::NSLineBreakMode::ByWordWrapping);
        add(S::UnlockNote, as_view(&note));
        let frame = NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(200.0, 22.0));
        let password = NSSecureTextField::initWithFrame(mtm.alloc(), frame);
        password.setAccessibilityLabel(Some(&ns("Password")));
        add(S::Password, as_view(&password));
        add(S::Unlock, button("Unlock", names.unlock));
        add(S::QuickHint, label(""));
        let scroll = NSScrollView::initWithFrame(mtm.alloc(), frame);
        scroll.setHasVerticalScroller(true);
        scroll.setBorderType(objc2_app_kit::NSBorderType::BezelBorder);
        let quick = NSTextView::initWithFrame(mtm.alloc(), frame);
        quick.setEditable(false);
        quick.setFont(Some(&crate::app::log_font()));
        quick.setTextColor(Some(&NSColor::textColor()));
        scroll.setDocumentView(Some(&quick));
        add(S::Quick, as_view(&scroll));
        let pane = PaneView::build(mtm, side, names.list, "");
        add(S::Tree, as_view(&pane.tree_scroll));
        add(S::List, as_view(&pane.list_scroll));
        let view = SideView {
            side,
            views,
            crumbs: RefCell::default(),
            path,
            quick,
            progress,
            pane,
            bars: RefCell::default(),
            edits_open: Cell::new(false),
        };
        view.show_if(S::Quick, false);
        view.show_if(S::QuickHint, false);
        view
    }

    fn v(&self, s: S) -> &NSView {
        self.views
            .iter()
            .find(|(x, _)| *x == s)
            .map(|(_, v)| &**v)
            .expect("every control is built")
    }

    fn text(&self, s: S) -> &NSTextField {
        self.v(s).downcast_ref::<NSTextField>().expect("a label")
    }

    fn button(&self, s: S) -> &NSButton {
        self.v(s).downcast_ref::<NSButton>().expect("a button")
    }

    fn popup(&self, s: S) -> &NSPopUpButton {
        self.v(s).downcast_ref::<NSPopUpButton>().expect("a pop-up")
    }

    fn show_if(&self, s: S, on: bool) {
        ui::set_visible(self.v(s), on);
    }

    fn shown(&self, s: S) -> bool {
        ui::shown(self.v(s))
    }

    fn set_title(&self, s: S, t: &str) {
        let b = self.button(s);
        if b.title().to_string() != t {
            b.setTitle(&ns(t));
        }
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
    fn show(&self, mtm: MainThreadMarker, target: &Callback, s: SideShown) -> bool {
        let (line, pct) = s
            .job
            .as_ref()
            .map(|(j, _)| crate::inspect::job_line(j))
            .unwrap_or_default();
        ui::set_text(self.text(S::Job), &line);
        self.show_if(S::Job, s.job.is_some());
        ui::set_progress(&self.progress, s.job.is_some(), pct);
        self.show_if(S::CancelOpen, s.job.as_ref().is_some_and(|(_, c)| *c));
        self.pane.show(s.pane);
        if self.bars.borrow().as_ref() == Some(&s.bars) {
            return false;
        }
        let b = s.bars;
        self.show_source(mtm, target, &b.source, &b.recent);
        self.show_if(S::CloseImage, b.close_image.is_some());
        ui::set_enabled(self.button(S::CloseImage), b.close_image.unwrap_or(false));
        self.show_if(S::Close, b.can_close);
        for x in [S::Apply, S::Discard] {
            self.show_if(x, b.stages);
            ui::set_enabled(self.button(x), b.apply_enabled);
        }
        self.set_title(S::Apply, &b.apply_label);
        self.show_if(S::Edits, b.stages && b.staged > 0);
        self.set_title(S::Edits, &b.edits_label);
        self.edits_open.set(b.edits_open);
        let p = self.popup(S::Partition);
        p.removeAllItems();
        for part in &b.parts {
            p.addItemWithTitle(&ns(&ui::plain(part)));
        }
        if let Some(i) = b.part {
            p.selectItemAtIndex(i as isize);
        }
        // Shown whenever the source has partitions, so one that fails to open can be left.
        self.show_if(S::Partition, !b.parts.is_empty());
        ui::set_text(self.text(S::NoParts), b.no_parts);
        self.show_if(
            S::NoParts,
            b.loaded && b.parts.is_empty() && !b.no_parts.is_empty(),
        );
        self.show_if(S::NewFolder, b.new_folder);
        ui::set_text(self.text(S::Title), &b.title);
        self.show_if(S::Title, b.loaded);
        ui::set_text(self.text(S::Free), b.free.as_deref().unwrap_or(""));
        self.show_if(S::Free, b.loaded && b.free.is_some());
        self.show_if(S::ReadOnly, b.read_only);
        self.show_crumbs(mtm, target, &b.prefix, &b.crumbs);
        let locked = b.locked;
        for x in [S::UnlockNote, S::Password, S::Unlock] {
            self.show_if(x, locked.is_some());
        }
        if let Some(apfs) = locked {
            let note = match apfs {
                true => "This APFS volume is encrypted with FileVault. Enter the volume password or personal recovery key.",
                false => "This source is password-protected.",
            };
            ui::set_text(self.text(S::UnlockNote), note);
        }
        *self.bars.borrow_mut() = Some(b);
        true
    }

    /// The source pull-down: its title is the source; its menu the recent sources and the ways to open one.
    fn show_source(
        &self,
        mtm: MainThreadMarker,
        target: &Callback,
        title: &str,
        recent: &[String],
    ) {
        let p = self.popup(S::Source);
        p.removeAllItems();
        let title = if title.is_empty() {
            "Open image, backup, or folder..."
        } else {
            title
        };
        p.addItemWithTitle(&ns(&ui::plain(title)));
        let i = idx(self.side);
        let add = |label: &str, tag: isize| {
            p.addItemWithTitle(&ns(&ui::plain(label)));
            if let Some(item) = p.lastItem() {
                item.setTag(tag);
                // SAFETY: the target is the screen's, kept as long as the controls.
                unsafe {
                    item.setTarget(Some(target));
                    item.setAction(Some(objc2::sel!(fire:)));
                }
            }
        };
        for (n, path) in recent.iter().take(8).enumerate() {
            add(path, 20000 * (i as isize + 1) + n as isize);
        }
        add("Open Image File...", side_tag(i, S::OpenFile));
        add("Open Host Folder...", side_tag(i, S::OpenFolder));
        add("Open Backup Folder...", side_tag(i, S::OpenBackup));
        add("Connect to Remote...", side_tag(i, S::Connect));
        let _ = mtm;
    }

    /// The path's crumbs as borderless buttons; the last, where the pane is, cannot be pressed.
    fn show_crumbs(
        &self,
        mtm: MainThreadMarker,
        target: &Callback,
        prefix: &str,
        crumbs: &[(String, Option<String>)],
    ) {
        for (b, _) in self.crumbs.borrow_mut().drain(..) {
            b.removeFromSuperview();
        }
        let mut built = Vec::new();
        let i = idx(self.side);
        let mut x = 0.0;
        let mut add = |label: &str, target_path: Option<String>, link: bool| {
            let n = built.len();
            let b = ui::button(mtm, label, "");
            b.setBordered(false);
            if link {
                b.setContentTintColor(Some(&NSColor::linkColor()));
                b.setTag(10000 * (i as isize + 1) + n as isize);
                ui::hook(&b, target);
            } else {
                b.setEnabled(target_path.is_some() && link);
            }
            b.sizeToFit();
            let w = b.frame().size.width;
            b.setFrame(NSRect::new(NSPoint::new(x, 0.0), NSSize::new(w, 22.0)));
            x += w + 2.0;
            self.path.addSubview(&b);
            built.push((b, target_path));
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
        *self.crumbs.borrow_mut() = built;
    }

    /// The side's rows: bars, path, job, then the listing, the unlock row or the quick view.
    fn nodes(&self) -> Vec<Node<Leaf>> {
        let i = idx(self.side);
        let l = |s: S| Leaf::Side(i, s);
        let first = vec![
            Node::leaf(l(S::Source), Size::Stretch(1), Size::Fixed(layout::ROW + 2)),
            Node::button(l(S::OpenFile), 70),
            Node::button(l(S::OpenFolder), 80),
            Node::button(l(S::OpenBackup), 80),
            Node::button(l(S::Connect), 80),
            Node::button(l(S::CloseImage), 95),
            Node::button(l(S::Close), 65),
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
        if !self.shown(S::Title) {
            second.push(Node::Space(Size::Stretch(1)));
        }
        second.extend([
            Node::button(l(S::NewFolder), 105),
            Node::button(l(S::Apply), 90),
            Node::button(l(S::Discard), 75),
            Node::button(l(S::Edits), 85),
        ]);
        let mut rows = vec![
            Node::row(first),
            Node::row(second),
            Node::leaf(l(S::Path), Size::Stretch(1), Size::Fixed(layout::ROW)),
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
        if self.shown(S::Unlock) {
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
        } else if self.shown(S::Quick) {
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
    fn fired(&self, s: S, sender: &AnyObject) -> Option<Vec<A>> {
        let pane = |a| A::Pane(self.side, a);
        let a = match s {
            S::Partition => {
                let i = self.popup(S::Partition).indexOfSelectedItem();
                pane(P::SelectPartition(usize::try_from(i).ok()?))
            }
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
            S::Unlock | S::Password => {
                let field = self.v(S::Password).downcast_ref::<NSTextField>()?;
                let secret = field.stringValue().to_string();
                if secret.is_empty() {
                    return None;
                }
                field.setStringValue(&ns(""));
                pane(P::SubmitPassword(secret))
            }
            _ => return None,
        };
        let _ = sender;
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
    mtm: MainThreadMarker,
    ctrl: RefCell<CommanderController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<CommanderScreen>,
    pub page: Retained<NSView>,
    target: Retained<Callback>,
    top: Vec<(T, Retained<NSView>)>,
    copy_progress: Retained<NSProgressIndicator>,
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
    /// The screen; `on_close` runs when the controller leaves it, and drops it.
    pub fn build(
        mtm: MainThreadMarker,
        jobs: &JobRegistry,
        notify: &Arc<dyn Notify>,
        host: EffectHost,
        on_close: Rc<dyn Fn()>,
    ) -> Rc<CommanderScreen> {
        let screen = Rc::new_cyclic(|me: &Weak<CommanderScreen>| {
            let fire_me = me.clone();
            let target = ui::callback(
                mtm,
                Box::new(move |sender: &AnyObject| {
                    let tag = sender
                        .downcast_ref::<NSControl>()
                        .map(|c| c.tag())
                        .or_else(|| {
                            sender
                                .downcast_ref::<objc2_app_kit::NSMenuItem>()
                                .map(|m| m.tag())
                        });
                    if let (Some(s), Some(tag)) = (fire_me.upgrade(), tag) {
                        s.fired(tag, sender);
                    }
                }),
            );
            let page = ui::flipped(mtm);
            let mut top: Vec<(T, Retained<NSView>)> = Vec::new();
            let mut add = |t: T, v: Retained<NSView>| {
                if let Some(c) = v.downcast_ref::<NSControl>() {
                    c.setTag(top_tag(t));
                    if c.downcast_ref::<NSTextField>().is_none() {
                        ui::hook(c, &target);
                    }
                }
                page.addSubview(&v);
                top.push((t, v));
            };
            let button = |title: &str, id: &str| as_view(&ui::button(mtm, title, id));
            let label = |text: &str| as_view(&ui::label(mtm, text));
            add(T::Title, label("Commander Mode"));
            add(T::View, button("View...", ids::VIEW_MENU));
            add(T::Log, button("Log (0)", ids::LOG));
            add(T::Close, button("Close", ids::CLOSE));
            add(T::CopyJob, label(""));
            let copy_progress = ui::progress(mtm);
            add(T::CopyProgress, as_view(&copy_progress));
            add(T::CancelCopy, button("Cancel", ids::CANCEL_COPY));
            add(T::Ltr, button("Copy ->", ids::COPY_LEFT_TO_RIGHT));
            add(T::Rtl, button("<- Copy", ids::COPY_RIGHT_TO_LEFT));
            add(T::Delete, button("Delete", ids::DELETE));
            let compare = ui::button(mtm, "Compare", ids::COMPARE);
            compare.setEnabled(false);
            add(T::Compare, as_view(&compare));
            add(
                T::KeepDates,
                as_view(&ui::checkbox(mtm, "Keep original dates", ids::KEEP_DATES)),
            );
            add(T::FormatCaption, label("Export as:"));
            let format = NSPopUpButton::initWithFrame_pullsDown(mtm.alloc(), NSRect::ZERO, false);
            for f in ExportFormat::ALL {
                format.addItemWithTitle(&ns(f.label()));
            }
            ui::set_id(&format, ids::EXPORT_FORMAT);
            add(T::Format, as_view(&format));
            add(T::ForkCaption, label("Forks as:"));
            let fork = NSPopUpButton::initWithFrame_pullsDown(mtm.alloc(), NSRect::ZERO, false);
            for m in ResourceForkMode::ALL {
                fork.addItemWithTitle(&ns(m.label()));
            }
            ui::set_id(&fork, ids::FORK_MODE);
            add(T::Fork, as_view(&fork));
            let status = ui::label(mtm, "");
            ui::set_id(&status, ids::STATUS);
            add(T::Status, as_view(&status));
            let sides = [
                SideView::build(mtm, &page, &target, Side::Left),
                SideView::build(mtm, &page, &target, Side::Right),
            ];
            CommanderScreen {
                mtm,
                ctrl: RefCell::new(new_controller(jobs, notify)),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                page,
                target,
                top,
                copy_progress,
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
            screen.sides[idx(side)].pane.on_event(
                mtm,
                Rc::new(move |e| {
                    if let Some(s) = me.upgrade() {
                        s.pane_event(side, e);
                    }
                }),
            );
        }
        screen.refresh();
        screen.sides[0].pane.focus();
        screen
    }

    fn t(&self, t: T) -> &NSView {
        self.top
            .iter()
            .find(|(x, _)| *x == t)
            .map(|(_, v)| &**v)
            .expect("every control is built")
    }

    fn t_button(&self, t: T) -> &NSButton {
        self.t(t).downcast_ref::<NSButton>().expect("a button")
    }

    fn t_popup(&self, t: T) -> &NSPopUpButton {
        self.t(t).downcast_ref::<NSPopUpButton>().expect("a pop-up")
    }

    /// Staged edits would be lost if the window closed now.
    /// Files dropped from Finder at window point `at`: the pane under it takes them, else ask.
    pub fn drop_paths(&self, mtm: MainThreadMarker, paths: Vec<PathBuf>, at: NSPoint) {
        let Some(first) = paths.first() else {
            return;
        };
        let under = [Side::Left, Side::Right].into_iter().find(|side| {
            let scroll = &self.sides[idx(*side)].pane.list_scroll;
            let p = scroll.convertPoint_fromView(at, None);
            let b = scroll.bounds();
            p.x >= 0.0 && p.y >= 0.0 && p.x <= b.size.width && p.y <= b.size.height
        });
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
                    n => format!("these {n} items"),
                };
                let message = format!("Which pane should take {what}?");
                let buttons = ["Left pane".to_string(), "Right pane".to_string()];
                match crate::effects::ask(mtm, "Dropped files", &message, &buttons) {
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
                let Some(i) = crate::effects::ask(mtm, &title, &question, &labels) else {
                    return;
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
            let Some(screen) = me.upgrade() else {
                return;
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
            (side, text, format!("Quick view ({hint} to close)"))
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
            crate::app::later(Box::new(move || {
                if let Some(s) = me.upgrade() {
                    s.view_menu();
                }
            }));
        }
        if follow && !self.follow_set.replace(true) {
            let when = dispatch2::DispatchTime::try_from(FOLLOW_DELAY)
                .unwrap_or(dispatch2::DispatchTime::NOW);
            let _ = dispatch2::DispatchQueue::main().after(when, crate::app::tick);
        } else if !follow {
            self.follow_set.set(false);
        }
    }

    fn show(&self, s: Shown) {
        let status = self
            .t(T::Status)
            .downcast_ref::<NSTextField>()
            .expect("a label");
        ui::set_text(status, &s.status);
        let log = self.t_button(T::Log);
        let title = format!("Log ({})", s.log_lines);
        if log.title().to_string() != title {
            log.setTitle(&ns(&title));
        }
        let (line, pct) = s
            .copying
            .as_ref()
            .map(crate::inspect::job_line)
            .unwrap_or_default();
        let job = self
            .t(T::CopyJob)
            .downcast_ref::<NSTextField>()
            .expect("a label");
        ui::set_text(job, &line);
        for t in [T::CopyJob, T::CancelCopy] {
            ui::set_visible(self.t(t), s.copying.is_some());
        }
        ui::set_progress(&self.copy_progress, s.copying.is_some(), pct);
        for (view, side) in self.sides.iter().zip(s.sides) {
            view.show(self.mtm, &self.target, side);
        }
        self.show_column(&s.column);
        self.show_quick(s.quick);
        self.show_windows(s.windows);
        if self.active.replace(s.active) != s.active {
            self.sides[idx(s.active)].pane.focus();
        }
    }

    fn show_column(&self, c: &CopyColumn) {
        ui::set_enabled(self.t_button(T::Ltr), c.copy_left);
        ui::set_enabled(self.t_button(T::Rtl), c.copy_right);
        ui::set_enabled(self.t_button(T::Delete), c.delete);
        let keep = self.t_button(T::KeepDates);
        let want = if c.keep_dates {
            NSControlStateValueOn
        } else {
            NSControlStateValueOff
        };
        if keep.state() != want {
            keep.setState(want);
        }
        for (t, i) in [(T::Format, c.format), (T::Fork, c.fork)] {
            let p = self.t_popup(t);
            if p.indexOfSelectedItem() != i as isize {
                p.selectItemAtIndex(i as isize);
            }
        }
        ui::set_enabled(self.t_popup(T::Fork), c.loose);
    }

    fn show_quick(&self, quick: Option<(Side, String, String)>) {
        let covered = quick.as_ref().map(|q| q.0);
        for view in &self.sides {
            let on = covered == Some(view.side);
            view.show_if(S::Quick, on);
            view.show_if(S::QuickHint, on);
            view.show_if(S::List, !on);
            if on {
                view.show_if(S::Tree, false);
            }
        }
        if let Some((side, text, hint)) = quick {
            let view = &self.sides[idx(side)];
            view.quick.setString(&ns(&ui::plain(&text)));
            view.quick.setTextColor(Some(&NSColor::textColor()));
            ui::set_text(view.text(S::QuickHint), &hint);
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
            let frame = at.map(|i| open.remove(i).1.frame());
            let me = self.me.clone();
            let side = p.side;
            let on_event: Rc<dyn Fn(Event)> = Rc::new(move |e| {
                let me = me.clone();
                crate::app::later(Box::new(move || {
                    if let Some(screen) = me.upgrade() {
                        screen.window_event(side, e);
                    }
                }));
            });
            open.push((p.side, ModelWindow::open(self.mtm, &p, frame, on_event)));
        }
    }

    fn window_event(&self, side: Option<Side>, e: Event) {
        match e {
            Event::Send(actions) => {
                for a in actions {
                    self.apply(a);
                }
            }
            Event::Copy(text) => crate::effects::copy_text(&text),
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
        let b = self.t(T::View);
        let at = NSPoint::new(0.0, b.frame().size.height + 2.0);
        if let Some(a) = track(self.mtm, b, items, at) {
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
            PaneEvent::Menu(i, at) => {
                let Some(row) = self.sides[idx(side)].pane.row_at(i) else {
                    return;
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
                let list = self.sides[idx(side)].pane.list_view().retain();
                if let Some(a) = track(self.mtm, &list, items, at) {
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

    /// A control's action, by its tag.
    fn fired(&self, tag: isize, sender: &AnyObject) {
        if self.sync.active() {
            return;
        }
        let actions = if tag >= 20000 {
            let side = (tag / 20000 - 1) as usize;
            self.sides
                .get(side)
                .and_then(|s| s.recent((tag % 20000) as usize))
        } else if tag >= 10000 {
            let side = (tag / 10000 - 1) as usize;
            self.sides
                .get(side)
                .and_then(|s| s.crumb((tag % 10000) as usize))
        } else if tag >= 1000 {
            let side = (tag / 1000 - 1) as usize;
            let s = SIDE_ALL.get((tag % 1000) as usize).copied();
            s.and_then(|s| self.sides.get(side).and_then(|v| v.fired(s, sender)))
        } else {
            let t = TOP_ALL.get(tag as usize).copied();
            t.and_then(|t| self.top_fired(t))
        };
        for a in actions.unwrap_or_default() {
            self.apply(a);
        }
    }

    fn top_fired(&self, t: T) -> Option<Vec<A>> {
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
            T::KeepDates => {
                A::SetKeepDates(self.t_button(T::KeepDates).state() == NSControlStateValueOn)
            }
            T::Format => {
                let i = usize::try_from(self.t_popup(T::Format).indexOfSelectedItem()).ok()?;
                A::SetExportFormat(*ExportFormat::ALL.get(i)?)
            }
            T::Fork => {
                let i = usize::try_from(self.t_popup(T::Fork).indexOfSelectedItem()).ok()?;
                A::SetForkMode(*ResourceForkMode::ALL.get(i)?)
            }
            _ => return None,
        }])
    }

    pub fn layout(&self) {
        let w = COLUMN_W;
        let top = |t: T| Leaf::Top(t);
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
                Node::column(self.sides[0].nodes()),
                column,
                Node::column(self.sides[1].nodes()),
            ]),
            Node::leaf(
                top(T::Status),
                Size::Stretch(1),
                Size::Fixed(layout::ROW - 4),
            ),
        ];
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        let keep = |l: &Leaf| match l {
            Leaf::Top(T::CopyJob) => ui::shown(self.t(T::CopyJob)),
            Leaf::Top(T::CopyProgress) => ui::shown(&self.copy_progress),
            Leaf::Top(t) => ui::shown(self.t(*t)),
            Leaf::Side(i, S::Job) => ui::shown(&self.sides[*i].progress),
            Leaf::Side(i, s) => self.sides[*i].shown(*s),
        };
        let Some(tree) = tree.prune(&keep) else {
            return;
        };
        for (l, r) in layout::layout(&tree, ui::area(&self.page), 96) {
            let v = match l {
                Leaf::Top(t) => self.t(t),
                Leaf::Side(i, s) => self.sides[i].v(s),
            };
            ui::move_to(v, r);
        }
    }
}
