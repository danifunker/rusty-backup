//! Commander Mode: two browser panes that take over the window, on one `CommanderController`.
//!
//! `src/gui/commander/` is the reference. Each side is a [`pane::CommanderPane`] around the same
//! [`PaneView`](crate::browser::pane::PaneView) Inspect's browse view uses, and every window comes
//! from [`Dialogs`] with a set of pane windows per side. A drag between the panes is the view's own
//! pointer tracking, not a GDK drag: it needs no second process, and every backend delivers it.

mod column;
mod pane;
mod windows;

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use gtk::{glib, graphene};
use rusty_backup::controller::app::{CommanderRecent, Notify, Snapshot, UserConfig};
use rusty_backup::controller::commander::{
    ids, BrowserPaneAction as P, CommanderAction as A, CommanderController, DisplayRow,
    JobRegistry, PaneServices, RowLabel, RowMenuEntry, Side, FOLLOW_DELAY,
};
use rusty_backup::controller::keys::{Command, Key, KeyMap, Mods};
use rusty_backup::controller::{Controller, Effect};

use crate::browser::dialogs::{Dialogs, DialogsShown, KeyFn};
use crate::browser::pane::PaneHost;
use crate::browser::{view_popover, FileInfo, SendFn};
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::progress::JobList;
use crate::sync::SyncGuard;
use crate::widgets::{keep_focus, set_text, typing};

use column::{Column, ColumnShown};
use pane::{CommanderPane, SideShown};
use windows::{Windows, WindowsShown};

const SIDES: [Side; 2] = [Side::Left, Side::Right];

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

pub struct CommanderScreen {
    ctrl: RefCell<CommanderController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<CommanderScreen>,
    root: adw::ToolbarView,
    body: gtk::Box,
    view_menu: gtk::MenuButton,
    log: gtk::Button,
    copying: JobList,
    panes: [Rc<CommanderPane>; 2],
    sides: [gtk::Stack; 2],
    quick: [Rc<FileInfo>; 2],
    quick_hint: [gtk::Label; 2],
    column: Column,
    status: gtk::Label,
    dialogs: Rc<Dialogs>,
    windows: Windows,
    /// The active pane last refresh, so the keyboard follows a switch.
    active: Cell<Side>,
    /// `CloseScreen` arrived; the app drops the screen once the action in hand is done.
    closed: Cell<bool>,
    on_close: Rc<dyn Fn()>,
    /// The close-window question is up, so another close does not ask it twice.
    quit_asked: Cell<bool>,
    follow_timer: Cell<bool>,
}

/// Everything a refresh shows, read while the controller is borrowed and shown after it is not.
struct Shown {
    status: String,
    log_lines: usize,
    copying: Vec<(Snapshot, bool)>,
    sides: Vec<SideShown>,
    column: ColumnShown,
    dialogs: DialogsShown,
    windows: WindowsShown,
    active: Side,
    /// The side the quick view covers, its text and what closes it.
    quick: Option<(Side, Option<crate::browser::InfoShown>, String)>,
    menu_up: bool,
    follow: bool,
}

/// The panes' window onto the screen: no strong reference, so the two can be built apart.
struct Host(Weak<CommanderScreen>);

impl PaneHost for Host {
    fn send(&self, action: A) {
        if let Some(screen) = self.0.upgrade() {
            screen.send(action);
        }
    }

    fn label(&self, side: Side, row: &DisplayRow) -> RowLabel {
        let screen = self.0.upgrade().expect("the pane's screen");
        let ctl = screen.ctrl.borrow();
        ctl.pane(side).row_label(row, false)
    }

    fn menu(&self, side: Side, row: &DisplayRow) -> Vec<RowMenuEntry> {
        let screen = self.0.upgrade().expect("the pane's screen");
        let ctl = screen.ctrl.borrow();
        ctl.row_menu(side, row)
    }

    fn click(&self, side: Side, row: &DisplayRow, ctrl: bool, shift: bool) -> Option<P> {
        let screen = self.0.upgrade()?;
        let ctl = screen.ctrl.borrow();
        ctl.pane(side).row_click(row, ctrl, shift)
    }

    fn context(&self, side: Side, row: &DisplayRow) -> Option<P> {
        let screen = self.0.upgrade()?;
        let ctl = screen.ctrl.borrow();
        let pane = ctl.pane(side);
        pane.applying().is_none().then(|| pane.row_context(row))?
    }

    fn key(&self, _: Side, key: Key, mods: Mods) -> bool {
        self.0.upgrade().is_some_and(|s| s.key(key, mods))
    }

    fn dragged(&self, side: Side, from: &gtk::Widget, at: (f64, f64), end: bool) {
        if let Some(screen) = self.0.upgrade() {
            screen.dragged(side, from, at, end);
        }
    }
}

impl CommanderScreen {
    /// `on_close` runs when the controller leaves the screen, which then drops it.
    pub fn build(
        jobs: &JobRegistry,
        notify: &Arc<dyn Notify>,
        host: EffectHost,
        on_close: Rc<dyn Fn()>,
    ) -> Rc<CommanderScreen> {
        let ctl = new_controller(jobs, notify);
        Rc::new_cyclic(|me: &Weak<CommanderScreen>| {
            let target = me.clone();
            let send: SendFn = Rc::new(move |action| {
                if let Some(screen) = target.upgrade() {
                    screen.send(action);
                }
            });

            let view_menu = gtk::MenuButton::builder().label("View").build();
            view_menu.set_widget_name(ids::VIEW_MENU);
            let (target, to_menu) = (me.clone(), send.clone());
            view_menu.set_create_popup_func(move |b| {
                let items = target.upgrade().map(|s: Rc<CommanderScreen>| {
                    let ctl = s.ctrl.borrow();
                    ctl.view_menu()
                });
                let items = items.unwrap_or_default();
                b.set_popover(Some(&view_popover(&items, &to_menu)));
            });
            let close = gtk::Button::with_label("Close");
            close.set_widget_name(ids::CLOSE);
            let to_screen = send.clone();
            close.connect_clicked(move |_| to_screen(A::RequestClose));
            let log = gtk::Button::with_label("Log (0)");
            log.set_widget_name(ids::LOG);
            log.set_tooltip_text(Some("Operations completed this Commander session"));
            let target = me.clone();
            log.connect_clicked(move |_| {
                if let Some(screen) = target.upgrade() {
                    let shown = screen.ctrl.borrow().log_shown();
                    screen.send(A::ShowLog(!shown));
                }
            });
            let title = gtk::Label::new(Some("Commander Mode"));
            title.add_css_class("heading");
            let header = adw::HeaderBar::builder().title_widget(&title).build();
            header.pack_start(&view_menu);
            header.pack_end(&close);
            header.pack_end(&log);

            let to_screen = send.clone();
            let copying = JobList::new(ids::CANCEL_COPY, Rc::new(move || to_screen(A::CancelCopy)));
            copying.root().set_margin_start(8);
            copying.root().set_margin_end(8);

            let pane_for = |side| {
                let host = Rc::new(Host(me.clone()));
                CommanderPane::build(side, &send, host)
            };
            let panes = [pane_for(Side::Left), pane_for(Side::Right)];
            let quick = [FileInfo::build(&send), FileInfo::build(&send)];
            let hint = || gtk::Label::builder().xalign(0.0).margin_start(8).build();
            let quick_hint = [hint(), hint()];
            let side_stack = |i: usize| {
                let page = gtk::Box::new(gtk::Orientation::Vertical, 4);
                page.append(&quick_hint[i]);
                page.append(quick[i].root());
                let stack = gtk::Stack::new();
                stack.set_hexpand(true);
                stack.set_vexpand(true);
                stack.add_named(panes[i].root(), Some("pane"));
                stack.add_named(&page, Some("quick"));
                stack
            };
            let sides = [side_stack(0), side_stack(1)];
            // Clipped so a pane's own width never moves the column: the two halves stay equal.
            let clip = |stack: &gtk::Stack| {
                gtk::ScrolledWindow::builder()
                    .child(stack)
                    .hscrollbar_policy(gtk::PolicyType::External)
                    .vscrollbar_policy(gtk::PolicyType::Never)
                    .min_content_width(240)
                    .hexpand(true)
                    .vexpand(true)
                    .build()
            };

            let column = Column::build(&send);
            let split = gtk::CenterBox::builder().vexpand(true).build();
            split.set_start_widget(Some(&clip(&sides[0])));
            split.set_center_widget(Some(column.root()));
            split.set_end_widget(Some(&clip(&sides[1])));

            let status = gtk::Label::builder()
                .xalign(0.0)
                .ellipsize(gtk::pango::EllipsizeMode::End)
                .margin_start(8)
                .margin_end(8)
                .margin_bottom(4)
                .build();
            status.set_widget_name(ids::STATUS);

            let body = gtk::Box::new(gtk::Orientation::Vertical, 4);
            body.append(copying.root());
            body.append(&split);
            body.append(&status);
            keep_focus(&body);
            panes[0].view().watch_drags(&body);
            panes[1].view().watch_drags(&body);

            let root = adw::ToolbarView::new();
            root.add_top_bar(&header);
            root.set_content(Some(&body));

            let target = me.clone();
            let key: KeyFn = Rc::new(move |key, mods| {
                let screen = target.upgrade();
                screen.is_some_and(|s| s.key(key, mods))
            });
            CommanderScreen {
                ctrl: RefCell::new(ctl),
                dialogs: Dialogs::new(host.clone(), send.clone(), &SIDES, key),
                windows: Windows::new(host.clone(), send),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                root,
                body,
                view_menu,
                log,
                copying,
                panes,
                sides,
                quick,
                quick_hint,
                column,
                status,
                active: Cell::new(Side::Left),
                closed: Cell::new(false),
                on_close,
                quit_asked: Cell::new(false),
                follow_timer: Cell::new(false),
            }
        })
    }

    pub fn root(&self) -> &adw::ToolbarView {
        &self.root
    }

    /// Show the first state and hand the keyboard to the active pane.
    pub fn opened(&self) {
        self.refresh();
        self.panes[self.active.get().idx()].focus();
    }

    /// The window is closing: `true` holds it while the controller asks about staged edits.
    pub fn hold_quit(&self) -> bool {
        if !self.ctrl.borrow().has_staged_edits() {
            return false;
        }
        if !self.quit_asked.replace(true) {
            self.apply(A::RequestQuit);
        }
        true
    }

    fn send(&self, action: A) {
        if !self.sync.active() {
            self.apply(action);
        }
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
            if let Some(screen) = me.upgrade() {
                screen.answered(reply);
            }
        })
    }

    fn answered(&self, reply: Reply) {
        let action = match reply {
            Reply::Picked { purpose, path } => A::Picked { purpose, path },
            Reply::Confirmed { purpose, yes } => {
                self.quit_asked.set(false);
                A::Confirmed { purpose, yes }
            }
            Reply::Answered { purpose, text } => A::Answered { purpose, text },
            Reply::Chose { purpose, choice } => A::Chose { purpose, choice },
            Reply::CloseScreen => {
                self.closed.set(true);
                return;
            }
        };
        self.apply(action);
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        if self.closed.get() {
            (self.on_close)();
            return;
        }
        self.refresh();
    }

    /// Run a key through the key map; true when it meant something here.
    pub fn key(&self, key: Key, mods: Mods) -> bool {
        if typing(&self.root) {
            return false;
        }
        let (command, side) = {
            let ctl = self.ctrl.borrow();
            if ctl.modal_open() || !ctl.binds(key, mods) {
                return false;
            }
            (ctl.command_for(key, mods), ctl.active())
        };
        if matches!(command, Some(Command::PageUp | Command::PageDown)) {
            let rows = self.panes[side.idx()].page_rows();
            self.apply(A::SetPageRows(side, rows));
        }
        self.apply(A::Key(key, mods));
        true
    }

    /// Follow a dragged row: `end` copies it if it was let go over a pane that can take it.
    fn dragged(&self, side: Side, from: &gtk::Widget, at: (f64, f64), end: bool) {
        let target = self.panes[side.other().idx()].root();
        let point = graphene::Point::new(at.0 as f32, at.1 as f32);
        let inside = from.compute_point(target, &point).is_some_and(|p| {
            let (x, y) = (f64::from(p.x()), f64::from(p.y()));
            target.contains(x, y)
        });
        let over = inside && self.ctrl.borrow().can_copy(side);
        match (end, over) {
            (false, true) => target.add_css_class("rb-drop"),
            (false, false) | (true, _) => target.remove_css_class("rb-drop"),
        }
        let cursor = (!end).then_some(if over { "copy" } else { "no-drop" });
        self.body.set_cursor_from_name(cursor);
        if end && over {
            self.send(A::Copy(side));
        }
    }

    fn read(&self) -> Shown {
        let ctl = self.ctrl.borrow();
        let active = ctl.active();
        let quick = ctl.quick_view().then(|| {
            let side = active.other();
            let known = self.quick[side.idx()].key();
            let info = ctl
                .quick_view_detail()
                .map(|d| FileInfo::read(&ctl, d, known));
            let hint = ctl.key_hint(Command::QuickView);
            (side, info, format!("Quick view ({hint} to close)"))
        });
        Shown {
            status: ctl.status().to_string(),
            log_lines: ctl.log().len(),
            copying: ctl.copy_job().map(|(_, s)| (s, true)).into_iter().collect(),
            sides: self.panes.iter().map(|p| p.read(&ctl)).collect(),
            column: Column::read(&ctl),
            dialogs: self.dialogs.read(&ctl),
            windows: Windows::read(&ctl),
            active,
            quick,
            menu_up: ctl.view_menu_shown(),
            follow: ctl.follow_pending(),
        }
    }

    /// Read the controller first and release it: setting a widget can fire its change signal.
    fn refresh(&self) {
        let shown = self.read();
        let follow = shown.follow;
        self.sync.run(|| self.show(shown));
        if follow {
            self.wake_for_follow();
        }
    }

    /// The cursor's preview starts after a rest with no job to wake the shell, so a one-shot does.
    fn wake_for_follow(&self) {
        if self.follow_timer.replace(true) {
            return;
        }
        let me = self.me.clone();
        glib::timeout_add_local_once(FOLLOW_DELAY, move || {
            if let Some(screen) = me.upgrade() {
                screen.follow_timer.set(false);
                screen.tick();
            }
        });
    }

    fn show(&self, s: Shown) {
        set_text(&self.status, &s.status);
        let log = format!("Log ({})", s.log_lines);
        if self.log.label().as_deref() != Some(log.as_str()) {
            self.log.set_label(&log);
        }
        if s.menu_up && !self.view_menu.is_active() {
            self.view_menu.popup();
        }
        self.copying.show(&s.copying);
        for (pane, side) in self.panes.iter().zip(s.sides) {
            pane.show(side);
        }
        self.column.show(s.column);
        self.show_quick(s.quick);
        self.dialogs.show(s.dialogs);
        self.windows.show(s.windows);
        if self.active.replace(s.active) != s.active {
            self.panes[s.active.idx()].focus();
        }
    }

    fn show_quick(&self, quick: Option<(Side, Option<crate::browser::InfoShown>, String)>) {
        let covered = quick.as_ref().map(|q| q.0);
        for side in SIDES {
            let page = match covered == Some(side) {
                true => "quick",
                false => "pane",
            };
            let stack = &self.sides[side.idx()];
            if stack.visible_child_name().as_deref() != Some(page) {
                stack.set_visible_child_name(page);
            }
        }
        let Some((side, info, hint)) = quick else {
            return;
        };
        set_text(&self.quick_hint[side.idx()], &hint);
        match info {
            Some(info) => self.quick[side.idx()].show(info),
            None => self.quick[side.idx()].clear(),
        }
    }
}
