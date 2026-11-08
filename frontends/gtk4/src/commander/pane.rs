//! One Commander side: the source bar, the volume bar, the path and the shared [`PaneView`].

use std::cell::RefCell;
use std::rc::Rc;

use adw::prelude::*;
use gtk::glib;
use rusty_backup::controller::app::Snapshot;
use rusty_backup::controller::commander::{
    format_size, ids, BrowserPaneAction as P, CommanderAction as A, CommanderController, Side,
};

use crate::browser::pane::{PaneHost, PaneShown, PaneView};
use crate::browser::SendFn;
use crate::progress::JobList;
use crate::source::{Pick, PickerSpec, SourcePicker};
use crate::widgets::{plain, set_text};

/// The bars around the listing, read while the controller is borrowed.
#[derive(Clone, PartialEq)]
struct Bars {
    source: String,
    recent: Vec<String>,
    close_image: Option<bool>,
    can_close: bool,
    stages: bool,
    staged: usize,
    busy: bool,
    edits_open: bool,
    loaded: bool,
    host: bool,
    parts: Vec<String>,
    part: Option<usize>,
    no_parts: &'static str,
    new_folder: bool,
    title: String,
    free: Option<String>,
    read_only: bool,
    prefix: String,
    crumbs: Vec<(String, Option<String>)>,
    locked: Option<bool>,
    active: bool,
}

pub struct SideShown {
    bars: Bars,
    jobs: Vec<(Snapshot, bool)>,
    pane: PaneShown,
}

pub struct CommanderPane {
    side: Side,
    send: SendFn,
    root: gtk::Box,
    /// The source and volume rows, for the screen's size groups.
    rows: [gtk::ScrolledWindow; 2],
    source: Rc<SourcePicker>,
    close_image: gtk::Button,
    close: gtk::Button,
    apply: gtk::Button,
    discard: gtk::Button,
    edits: gtk::Button,
    part: gtk::DropDown,
    part_labels: gtk::StringList,
    new_folder: gtk::Button,
    title: gtk::Label,
    free: gtk::Label,
    read_only: gtk::Label,
    prefix: gtk::Label,
    crumbs: gtk::Box,
    jobs: JobList,
    unlock: gtk::Box,
    unlock_note: gtk::Label,
    password_label: gtk::Label,
    password: gtk::PasswordEntry,
    view: Rc<PaneView>,
    shown: RefCell<Option<Bars>>,
}

fn id_set(side: Side) -> ids::PaneIds {
    match side {
        Side::Left => ids::LEFT,
        Side::Right => ids::RIGHT,
    }
}

fn pick_action(pick: Pick) -> Option<P> {
    match pick {
        Pick::Recent(path) => Some(P::OpenPath(path)),
        Pick::OpenFile => Some(P::PickImage),
        Pick::OpenFolder => Some(P::PickHostFolder),
        Pick::OpenBackup => Some(P::PickBackupFolder),
        Pick::Connect => Some(P::Connect),
        // A pane offers no devices.
        Pick::Device(_) => None,
    }
}

fn pane_button(label: &str, id: &str, side: Side, send: &SendFn, action: P) -> gtk::Button {
    let b = gtk::Button::with_label(label);
    b.set_widget_name(id);
    let send = send.clone();
    b.connect_clicked(move |_| send(A::Pane(side, action.clone())));
    b
}

/// A partition dropdown keeps a long label from widening its pane.
fn short_labels() -> gtk::SignalListItemFactory {
    let factory = gtk::SignalListItemFactory::new();
    factory.connect_setup(|_, obj| {
        let item: &gtk::ListItem = obj.downcast_ref().expect("a list item");
        let label = gtk::Label::builder()
            .xalign(0.0)
            .max_width_chars(30)
            .ellipsize(gtk::pango::EllipsizeMode::End)
            .build();
        item.set_child(Some(&label));
    });
    factory.connect_bind(|_, obj| {
        let item: &gtk::ListItem = obj.downcast_ref().expect("a list item");
        let line = item.item().and_downcast::<gtk::StringObject>();
        let label = item.child().and_downcast::<gtk::Label>();
        if let (Some(line), Some(label)) = (line, label) {
            label.set_label(&line.string());
        }
    });
    factory
}

impl CommanderPane {
    pub fn build(side: Side, send: &SendFn, host: Rc<dyn PaneHost>) -> Rc<CommanderPane> {
        let names = id_set(side);
        let source = SourcePicker::build(PickerSpec {
            source: names.source,
            recent: names.recent,
            device: "",
            open_file: Some(names.open_file),
            open_folder: Some(names.open_folder),
            open_backup: Some(names.open_backup),
            connect: Some(names.connect),
            connect_live: true,
            no_devices: None,
            width_chars: 16,
            caption: None,
        });
        let to_pane = send.clone();
        source.connect_pick(move |pick| {
            if let Some(action) = pick_action(pick) {
                to_pane(A::Pane(side, action));
            }
        });
        let close_image = pane_button(
            "Close Image",
            names.close_image,
            side,
            send,
            P::CloseRemoteImage,
        );
        close_image.add_css_class("warning");
        close_image.set_tooltip_text(Some("Back to browsing the remote host filesystem"));
        let close = pane_button("Close", names.close, side, send, P::CloseSource);
        close.set_tooltip_text(Some("Unload this pane / disconnect"));
        let apply = pane_button("Apply", names.apply, side, send, P::Apply);
        apply.add_css_class("suggested-action");
        let discard = pane_button("Discard", names.discard, side, send, P::Discard);
        let edits = gtk::Button::with_label("Edits");
        edits.set_widget_name(names.edits);
        edits.set_tooltip_text(Some("Review the pending staged edits"));
        let to_pane = send.clone();
        edits.connect_clicked(move |b| {
            let open = b.has_css_class("rb-open");
            to_pane(A::Pane(side, P::ShowEdits(!open)));
        });
        let source_bar = gtk::Box::new(gtk::Orientation::Horizontal, 4);
        source_bar.append(source.root());
        for b in [&close_image, &close, &apply, &discard, &edits] {
            source_bar.append(b);
        }

        let part_labels = gtk::StringList::new(&[]);
        let part = gtk::DropDown::builder()
            .model(&part_labels)
            .factory(&short_labels())
            .build();
        part.set_widget_name(names.partition);
        let to_pane = send.clone();
        part.connect_selected_notify(move |d| {
            if d.selected() != gtk::INVALID_LIST_POSITION {
                to_pane(A::Pane(side, P::SelectPartition(d.selected() as usize)));
            }
        });
        let new_folder = pane_button(
            "New Folder",
            names.new_folder,
            side,
            send,
            P::RequestNewFolder,
        );
        new_folder.set_tooltip_text(Some("Create a folder in the current directory"));
        // On a narrow pane the volume name gives way, not the partition picker.
        part.set_size_request(150, -1);
        let title = gtk::Label::builder()
            .ellipsize(gtk::pango::EllipsizeMode::End)
            .max_width_chars(18)
            .build();
        title.add_css_class("heading");
        let free = gtk::Label::new(None);
        let read_only = gtk::Label::new(Some("Read-only"));
        read_only.add_css_class("warning");
        read_only.set_tooltip_text(Some(
            "This source can be browsed and copied out of, but not written to - copies into \
             this pane are disabled.",
        ));
        let volume_bar = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        volume_bar.append(&part);
        volume_bar.append(&new_folder);
        volume_bar.append(&title);
        volume_bar.append(&free);
        volume_bar.append(&read_only);

        let prefix = gtk::Label::new(None);
        prefix.add_css_class("monospace");
        let crumbs = gtk::Box::new(gtk::Orientation::Horizontal, 0);
        let path = gtk::Box::new(gtk::Orientation::Horizontal, 0);
        path.append(&prefix);
        path.append(&crumbs);
        let path = gtk::ScrolledWindow::builder()
            .child(&path)
            .hscrollbar_policy(gtk::PolicyType::External)
            .vscrollbar_policy(gtk::PolicyType::Never)
            .propagate_natural_height(true)
            .build();
        // The deepest folder is the part worth seeing.
        path.hadjustment().connect_changed(|a| {
            let a = a.clone();
            glib::idle_add_local_once(move || a.set_value(a.upper()));
        });

        let to_pane = send.clone();
        let jobs = JobList::new(
            names.cancel_open,
            Rc::new(move || to_pane(A::Pane(side, P::CancelOpen))),
        );

        let unlock_note = gtk::Label::builder()
            .wrap(true)
            .justify(gtk::Justification::Center)
            .build();
        let password_label = gtk::Label::new(None);
        let password = gtk::PasswordEntry::builder()
            .show_peek_icon(true)
            .hexpand(true)
            .build();
        let go = gtk::Button::with_label("Unlock");
        go.set_widget_name(names.unlock);
        let (to_pane, field) = (send.clone(), password.clone());
        let submit = move || {
            let secret = field.text().to_string();
            field.set_text("");
            to_pane(A::Pane(side, P::SubmitPassword(secret)));
        };
        let on_enter = submit.clone();
        password.connect_activate(move |_| on_enter());
        go.connect_clicked(move |_| submit());
        let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        row.append(&password_label);
        row.append(&password);
        row.append(&go);
        let unlock = gtk::Box::new(gtk::Orientation::Vertical, 8);
        unlock.set_margin_start(16);
        unlock.set_margin_end(16);
        unlock.set_margin_top(24);
        unlock.append(&unlock_note);
        unlock.append(&row);

        let view = PaneView::build(side, host);
        view.set_list_id(names.list);
        view.root().set_vexpand(true);

        // The bars above the listing; the screen keeps both sides' the same height.
        // Fixed rows, clipped rather than wrapped, so a row is as tall on both sides.
        let rows = [clip(&source_bar), clip(&volume_bar)];
        let header = gtk::Box::new(gtk::Orientation::Vertical, 4);
        header.append(&rows[0]);
        header.append(&rows[1]);
        header.append(&path);
        let root = gtk::Box::new(gtk::Orientation::Vertical, 4);
        root.add_css_class("rb-pane");
        root.append(&header);
        root.append(jobs.root());
        root.append(&unlock);
        root.append(view.root());
        Rc::new(CommanderPane {
            side,
            send: send.clone(),
            root,
            rows,
            source,
            close_image,
            close,
            apply,
            discard,
            edits,
            part,
            part_labels,
            new_folder,
            title,
            free,
            read_only,
            prefix,
            crumbs,
            jobs,
            unlock,
            unlock_note,
            password_label,
            password,
            view,
            shown: RefCell::default(),
        })
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    /// The source and volume rows; the screen keeps each one height on both sides.
    pub fn rows(&self) -> &[gtk::ScrolledWindow; 2] {
        &self.rows
    }

    pub fn view(&self) -> &PaneView {
        &self.view
    }

    pub fn page_rows(&self) -> i32 {
        self.view.page_rows()
    }

    /// Give the pane the keyboard: its password field while locked, else the listing.
    pub fn focus(&self) {
        if self.unlock.is_visible() {
            self.password.grab_focus();
        } else {
            self.view.focus();
        }
    }

    pub fn read(&self, ctl: &CommanderController) -> SideShown {
        let pane = ctl.pane(self.side);
        let v = pane.volume();
        let host = pane.listing.is_host();
        let loaded = pane.listing.is_loaded();
        let free = v.total_size.saturating_sub(v.used_size);
        let mut jobs: Vec<(Snapshot, bool)> = Vec::new();
        jobs.extend(pane.opening().map(|s| (s, true)));
        jobs.extend(pane.wrapper_opening().map(|(_, s)| (s, false)));
        jobs.extend(pane.checking().map(|s| (s, false)));
        jobs.extend(pane.host_writing().map(|s| (s, false)));
        jobs.extend(pane.applying().map(|s| (s, false)));
        jobs.extend(pane.tool_working().map(|s| (s, false)));
        jobs.extend(pane.archive_working().map(|s| (s, false)));
        let bars = Bars {
            source: pane.source_label(),
            recent: pane.recent().to_vec(),
            close_image: pane.is_remote_image().then(|| pane.queue.is_empty()),
            can_close: pane.source_path().is_some() || loaded,
            stages: pane.stages_edits(),
            staged: pane.queue.len(),
            busy: pane.is_busy(),
            edits_open: pane.edits_shown(),
            loaded,
            host,
            parts: match host {
                true => Vec::new(),
                false => pane.partition_labels(),
            },
            part: pane.selected_partition(),
            no_parts: pane.no_partitions_label(),
            new_folder: pane.can_new_folder(),
            title: pane.volume_title(),
            free: (!host).then(|| format!("free: {}", format_size(free))),
            read_only: !host && pane.is_read_only(),
            prefix: pane.path_prefix(),
            crumbs: pane.path_crumbs(),
            locked: pane.needs_password().then(|| v.fs_type == "APFS"),
            active: ctl.active() == self.side,
        };
        SideShown {
            bars,
            jobs,
            pane: self.view.read(ctl),
        }
    }

    pub fn show(&self, s: SideShown) {
        self.jobs.show(&s.jobs);
        self.view.show(s.pane);
        if self.shown.borrow().as_ref() == Some(&s.bars) {
            return;
        }
        let b = s.bars;
        self.source.show(&b.source, &b.recent, &[]);
        shown(&self.close_image, b.close_image.is_some());
        self.close_image
            .set_sensitive(b.close_image.unwrap_or(false));
        shown(&self.close, b.can_close);
        for w in [&self.apply, &self.discard] {
            shown(w, b.stages);
            w.set_sensitive(b.staged > 0 && !b.busy);
        }
        shown(&self.edits, b.stages && b.staged > 0);
        self.apply.set_label(&format!("Apply ({})", b.staged));
        self.edits.set_label(&format!("Edits ({})", b.staged));
        match b.edits_open {
            true => self.edits.add_css_class("rb-open"),
            false => self.edits.remove_css_class("rb-open"),
        }
        // The row stays, empty, so the listing starts where the other side's does.
        for w in [&self.title, &self.free] {
            w.set_visible(b.loaded);
        }
        self.show_parts(&b);
        shown(&self.new_folder, b.new_folder);
        set_text(&self.title, &b.title);
        set_text(&self.free, b.free.as_deref().unwrap_or(""));
        shown(&self.free, b.loaded && b.free.is_some());
        shown(&self.read_only, b.read_only);
        set_text(&self.prefix, &b.prefix);
        self.show_crumbs(&b);
        self.show_lock(b.locked);
        match b.active {
            true => self.root.add_css_class("rb-pane-active"),
            false => self.root.remove_css_class("rb-pane-active"),
        }
        *self.shown.borrow_mut() = Some(b);
    }

    fn show_parts(&self, b: &Bars) {
        let same = {
            let held = self.shown.borrow();
            held.as_ref().is_some_and(|old| old.parts == b.parts)
        };
        if !same {
            let empty = [b.no_parts.to_string()];
            let lines = match b.parts.is_empty() {
                true => &empty[..],
                false => &b.parts[..],
            };
            let labels: Vec<_> = lines.iter().map(|l| plain(l)).collect();
            let labels: Vec<&str> = labels.iter().map(|l| l.as_ref()).collect();
            self.part_labels
                .splice(0, self.part_labels.n_items(), &labels);
        }
        self.part.set_sensitive(!b.parts.is_empty());
        let at = b.part.map_or(0, |p| p as u32);
        if self.part.selected() != at {
            self.part.set_selected(at);
        }
        shown(&self.part, b.loaded && !b.host);
    }

    fn show_crumbs(&self, b: &Bars) {
        let same = {
            let held = self.shown.borrow();
            held.as_ref().is_some_and(|old| old.crumbs == b.crumbs)
        };
        if same {
            return;
        }
        while let Some(child) = self.crumbs.first_child() {
            self.crumbs.remove(&child);
        }
        if b.crumbs.is_empty() {
            self.crumbs.append(&gtk::Label::new(Some("/")));
        }
        let last = b.crumbs.len().saturating_sub(1);
        for (i, (label, target)) in b.crumbs.iter().enumerate() {
            let widget: gtk::Widget = match target.clone().filter(|_| i < last) {
                Some(target) => {
                    let link = gtk::Button::with_label(&plain(label));
                    link.add_css_class("flat");
                    link.add_css_class("rb-crumb");
                    link.set_focus_on_click(false);
                    let (send, side) = (self.send.clone(), self.side);
                    link.connect_clicked(move |_| {
                        send(A::Pane(side, P::NavigateTo(target.clone())));
                    });
                    link.upcast()
                }
                None => gtk::Label::new(Some(&plain(label))).upcast(),
            };
            widget.add_css_class("monospace");
            self.crumbs.append(&widget);
        }
    }

    fn show_lock(&self, locked: Option<bool>) {
        self.unlock.set_visible(locked.is_some());
        self.view.root().set_visible(locked.is_none());
        let Some(apfs) = locked else {
            return;
        };
        let (note, label) = match apfs {
            true => (
                "This APFS volume is encrypted with FileVault.\nEnter the volume password or \
                 personal recovery key.",
                "Passphrase:",
            ),
            false => ("This source is password-protected.", "Password:"),
        };
        set_text(&self.unlock_note, note);
        set_text(&self.password_label, label);
    }
}

fn shown(widget: &impl IsA<gtk::Widget>, on: bool) {
    widget.set_visible(on);
}

/// A row that clips instead of widening the pane.
fn clip(row: &gtk::Box) -> gtk::ScrolledWindow {
    gtk::ScrolledWindow::builder()
        .child(row)
        .hscrollbar_policy(gtk::PolicyType::External)
        .vscrollbar_policy(gtk::PolicyType::Never)
        .propagate_natural_height(true)
        .build()
}
