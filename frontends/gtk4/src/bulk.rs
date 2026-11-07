//! Bulk Convert over `controller::bulk_convert`; `src/gui/bulk_convert_dialog.rs` is the reference.
//!
//! The controller outlives its window: a conversion keeps running after Start closes the form, and
//! the strip under the header shows it from any tab.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use gtk::{gio, glib};
use rusty_backup::controller::app::chd::{ChdOptionsControl, ChdProfile};
use rusty_backup::controller::app::{Notify, Snapshot};
use rusty_backup::controller::bulk_convert::{
    chd_profile, conflict_heading, fmt_bytes, folder_label, format_label, format_note, ids,
    BulkConvertAction, BulkConvertController, BulkPhase, ConflictChoice, ExportFormat,
    CONFLICT_HELP, CONFLICT_TITLE, FORMATS, INTRO, MULTI_BIN_LABEL, MULTI_BIN_NOTE, REVIEW_NOTE,
    SAME_FOLDERS_WARNING, SCAN_HINT,
};
use rusty_backup::controller::{Controller, Effect};

use crate::browser::dialogs::{page, wrapped};
use crate::chd::{ChdControl, ChdIds};
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::{button, line, set_line};
use crate::progress::JobList;
use crate::sync::SyncGuard;
use crate::volume::{button_row, window};
use crate::widgets::{plain, set_text};

const PROFILES: [ChdProfile; 3] = [ChdProfile::Hd, ChdProfile::Dvd, ChdProfile::Cd];

/// One file in the Review list.
struct Row {
    index: usize,
    name: String,
    size: u64,
}

struct Shown {
    open: bool,
    phase: BulkPhase,
    source: String,
    output: String,
    format: ExportFormat,
    multi_bin: bool,
    same: bool,
    scan_error: Option<String>,
    can_scan: bool,
    scan: Option<Snapshot>,
    summary: String,
    extension: String,
    recommended: &'static str,
    files: Vec<(String, u64)>,
    selected: Vec<bool>,
    can_start: bool,
    start_label: String,
    conflicts: Option<Vec<String>>,
    strip: Option<(f32, String)>,
}

/// The files in the store and their ticks.
type Listed = (Vec<(String, u64)>, Vec<bool>);

/// The form's widgets, built when it opens.
struct Form {
    dialog: adw::Dialog,
    stack: gtk::Stack,
    source: gtk::Label,
    output: gtk::Label,
    format: gtk::DropDown,
    note: gtk::Label,
    chd: Vec<Rc<ChdControl>>,
    multi_bin: gtk::CheckButton,
    same: gtk::Label,
    scan_error: gtk::Label,
    scan: gtk::Button,
    jobs: JobList,
    summary: gtk::Label,
    extension: gtk::Entry,
    reset: gtk::Button,
    recommended: gtk::Label,
    store: gio::ListStore,
    start: gtk::Button,
    /// The rows in the store, so a tick only rebinds its own row.
    rows: RefCell<Listed>,
}

pub struct BulkScreen {
    ctrl: RefCell<BulkConvertController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<BulkScreen>,
    form: RefCell<Option<Rc<Form>>>,
    conflict: RefCell<Option<adw::AlertDialog>>,
    strip: gtk::Revealer,
    strip_bar: gtk::ProgressBar,
}

impl BulkScreen {
    pub fn build(notify: Arc<dyn Notify>, host: EffectHost) -> Rc<BulkScreen> {
        let strip_bar = gtk::ProgressBar::builder()
            .show_text(true)
            .hexpand(true)
            .valign(gtk::Align::Center)
            .build();
        let cancel = button("Cancel Bulk", ids::CANCEL);
        let row = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        row.set_margin_start(8);
        row.set_margin_end(8);
        row.set_margin_top(2);
        row.set_margin_bottom(2);
        row.append(&strip_bar);
        row.append(&cancel);
        let strip = gtk::Revealer::builder().child(&row).build();
        let screen = Rc::new_cyclic(|me| BulkScreen {
            ctrl: RefCell::new(BulkConvertController::new(notify)),
            host,
            sync: SyncGuard::default(),
            me: me.clone(),
            form: RefCell::default(),
            conflict: RefCell::default(),
            strip,
            strip_bar,
        });
        let me = screen.me.clone();
        cancel.connect_clicked(move |_| {
            if let Some(s) = me.upgrade() {
                s.apply(BulkConvertAction::Cancel);
            }
        });
        screen
    }

    /// The progress strip, for the window's top bars.
    pub fn strip(&self) -> &gtk::Revealer {
        &self.strip
    }

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    /// Show a fresh form; a conversion still running keeps going.
    pub fn open(&self) {
        if self.form.borrow().is_some() || self.is_running() {
            return;
        }
        self.apply(BulkConvertAction::Open);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    fn apply(&self, action: BulkConvertAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let lines = self.ctrl.borrow_mut().take_log();
        crate::app::append_log(lines);
        self.refresh();
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(s) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => BulkConvertAction::Picked { purpose, path },
                // Bulk Convert asks only for its two folders.
                Reply::Confirmed { .. }
                | Reply::Answered { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => return,
            };
            s.apply(action);
        })
    }

    /// The action closure the form's handlers call; it drops actions while a sync runs.
    fn sender(&self) -> Rc<dyn Fn(BulkConvertAction)> {
        let me = self.me.clone();
        Rc::new(move |action| {
            if let Some(s) = me.upgrade().filter(|s| !s.sync.active()) {
                s.apply(action);
            }
        })
    }

    fn read(&self) -> Shown {
        let ctrl = self.ctrl.borrow();
        let files = ctrl.files();
        Shown {
            open: ctrl.is_open(),
            phase: ctrl.phase(),
            source: folder_label(ctrl.source()),
            output: folder_label(ctrl.output()),
            format: ctrl.format(),
            multi_bin: ctrl.multi_bin(),
            same: ctrl.same_folders(),
            scan_error: ctrl.scan_error().map(str::to_string),
            can_scan: ctrl.can_scan(),
            scan: ctrl.scan_snapshot(),
            summary: ctrl.review_summary(),
            extension: ctrl.extension().to_string(),
            recommended: ctrl.recommended_extension(),
            files: files.iter().map(|f| (file_name(&f.path), f.size)).collect(),
            selected: files.iter().map(|f| f.selected).collect(),
            can_start: ctrl.can_start(),
            start_label: ctrl.start_label(),
            conflicts: ctrl
                .conflicts()
                .map(|c| c.iter().map(|c| file_name(&c.dest)).collect()),
            strip: ctrl.strip(),
        }
    }

    fn refresh(&self) {
        let s = self.read();
        self.strip.set_reveal_child(s.strip.is_some());
        if let Some((fraction, text)) = &s.strip {
            self.strip_bar.set_fraction(f64::from(*fraction));
            self.strip_bar.set_text(Some(&plain(text)));
        }
        if !s.open {
            if let Some(form) = self.form.borrow_mut().take() {
                form.dialog.force_close();
            }
            return;
        }
        if self.form.borrow().is_none() {
            let form = self.build_form();
            self.host.present(&form.dialog);
            *self.form.borrow_mut() = Some(form);
        }
        let Some(form) = self.form.borrow().clone() else {
            return;
        };
        self.sync.run(|| self.show_form(&form, &s));
        self.ask_conflicts(s.conflicts);
    }

    fn show_form(&self, form: &Form, s: &Shown) {
        let review = s.phase == BulkPhase::Review;
        let name = if review { "review" } else { "setup" };
        if form.stack.visible_child_name().as_deref() != Some(name) {
            form.stack.set_visible_child_name(name);
        }
        let title = if review {
            "Bulk Convert - Review"
        } else {
            "Bulk Convert"
        };
        if form.dialog.title() != title {
            form.dialog.set_title(title);
        }
        set_text(&form.source, &s.source);
        set_text(&form.output, &s.output);
        let index = FORMATS.iter().position(|f| *f == s.format).unwrap_or(0) as u32;
        if form.format.selected() != index {
            form.format.set_selected(index);
        }
        set_line(&form.note, format_note(s.format));
        let profile = chd_profile(s.format);
        for (control, p) in form.chd.iter().zip(PROFILES) {
            control.root().set_visible(profile == Some(p));
        }
        form.multi_bin.set_visible(s.format == ExportFormat::BinCue);
        if form.multi_bin.is_active() != s.multi_bin {
            form.multi_bin.set_active(s.multi_bin);
        }
        set_line(&form.same, s.same.then_some(SAME_FOLDERS_WARNING));
        set_line(&form.scan_error, s.scan_error.as_deref());
        form.scan.set_sensitive(s.can_scan);
        let jobs: Vec<(Snapshot, bool)> = s.scan.iter().map(|j| (j.clone(), false)).collect();
        form.jobs.show(&jobs);

        set_text(&form.summary, &s.summary);
        if !form.extension.has_focus() && form.extension.text() != s.extension {
            form.extension.set_text(&s.extension);
        }
        form.extension.set_placeholder_text(Some(s.recommended));
        form.reset.set_sensitive(s.extension != s.recommended);
        form.reset
            .set_tooltip_text(Some(&format!("Reset to recommended: .{}", s.recommended)));
        set_text(
            &form.recommended,
            &format!("(recommended: .{})", s.recommended),
        );
        self.show_rows(form, s);
        let chd_ok = form
            .chd
            .iter()
            .zip(PROFILES)
            .all(|(c, p)| profile != Some(p) || c.is_valid());
        form.start.set_sensitive(s.can_start && chd_ok);
        if form.start.label().as_deref() != Some(s.start_label.as_str()) {
            form.start.set_label(&s.start_label);
        }
    }

    /// A new list replaces the rows; a changed tick replaces only its own row.
    fn show_rows(&self, form: &Form, s: &Shown) {
        let mut rows = form.rows.borrow_mut();
        if rows.0 != s.files {
            let boxed: Vec<glib::BoxedAnyObject> = s
                .files
                .iter()
                .enumerate()
                .map(|(index, (name, size))| {
                    glib::BoxedAnyObject::new(Row {
                        index,
                        name: name.clone(),
                        size: *size,
                    })
                })
                .collect();
            *rows = (s.files.clone(), s.selected.clone());
            drop(rows);
            form.store.splice(0, form.store.n_items(), &boxed);
            return;
        }
        let changed: Vec<u32> = (0..s.selected.len())
            .filter(|i| rows.1.get(*i) != s.selected.get(*i))
            .map(|i| i as u32)
            .collect();
        rows.1 = s.selected.clone();
        drop(rows);
        // A row reported changed with its own object keeps its cell: a new object rebinds it.
        for i in changed {
            let row = (s.files.get(i as usize)).map(|(name, size)| Row {
                index: i as usize,
                name: name.clone(),
                size: *size,
            });
            if let Some(row) = row {
                form.store.splice(i, 1, &[glib::BoxedAnyObject::new(row)]);
            }
        }
    }

    /// The "already exist" question, asked once per Start that found some.
    fn ask_conflicts(&self, conflicts: Option<Vec<String>>) {
        let Some(names) = conflicts else {
            if let Some(d) = self.conflict.borrow_mut().take() {
                d.force_close();
            }
            return;
        };
        if self.conflict.borrow().is_some() {
            return;
        }
        let list: Vec<String> = names.iter().map(|n| format!("- {n}")).collect();
        let body = format!(
            "{}\n\n{}\n\n{}",
            conflict_heading(names.len()),
            list.join("\n"),
            CONFLICT_HELP
        );
        let dialog = adw::AlertDialog::new(Some(CONFLICT_TITLE), Some(&plain(&body)));
        dialog.add_response("back", "Back");
        dialog.add_response("skip", "Skip Conflicting");
        dialog.add_response("overwrite", "Overwrite");
        dialog.set_response_appearance("overwrite", adw::ResponseAppearance::Destructive);
        dialog.set_close_response("back");
        dialog.set_default_response(Some("back"));
        let me = self.me.clone();
        dialog.connect_response(None, move |_, id| {
            let Some(s) = me.upgrade() else {
                return;
            };
            let choice = match id {
                "skip" => ConflictChoice::SkipConflicting,
                "overwrite" => ConflictChoice::Overwrite,
                _ => ConflictChoice::Back,
            };
            s.conflict.borrow_mut().take();
            s.apply(BulkConvertAction::ResolveConflicts(choice));
        });
        let parent = self.form.borrow().as_ref().map(|f| f.dialog.clone());
        if let Some(parent) = parent {
            dialog.present(Some(&parent));
        }
        *self.conflict.borrow_mut() = Some(dialog);
    }

    fn chd_choice(&self) -> Option<rusty_backup::controller::app::chd::ChdOptions> {
        let profile = chd_profile(self.ctrl.borrow().format())?;
        let form = self.form.borrow().clone()?;
        let at = PROFILES.iter().position(|p| *p == profile)?;
        form.chd.get(at).map(|c| c.choice().0)
    }

    fn build_form(&self) -> Rc<Form> {
        let send = self.sender();
        let me = self.me.clone();
        let changed: Rc<dyn Fn()> = Rc::new(move || {
            if let Some(s) = me.upgrade() {
                s.refresh();
            }
        });

        // Setup.
        let setup = page();
        setup.set_spacing(8);
        setup.append(&wrapped(INTRO));
        let source = folder_row(&setup, "Source folder:");
        let pick_source = button("Choose Source Folder...", ids::SOURCE);
        pick_source.set_halign(gtk::Align::Start);
        setup.append(&pick_source);
        let output = folder_row(&setup, "Output folder:");
        let pick_output = button("Choose Output Folder...", ids::OUTPUT);
        pick_output.set_halign(gtk::Align::Start);
        setup.append(&pick_output);
        let labels: Vec<&str> = FORMATS.iter().map(|f| format_label(*f)).collect();
        let format = gtk::DropDown::from_strings(&labels);
        format.set_widget_name(ids::FORMAT);
        let format_row = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        let format_title = gtk::Label::new(Some("Output format:"));
        format_title.add_css_class("heading");
        format_row.append(&format_title);
        format_row.append(&format);
        setup.append(&format_row);
        let note = line();
        note.add_css_class("dim-label");
        setup.append(&note);
        let chd: Vec<Rc<ChdControl>> = PROFILES
            .iter()
            .map(|p| {
                ChdControl::build(
                    "CHD",
                    "",
                    ChdIds {
                        mode: ids::CHD_MODE,
                        hunk: ids::CHD_HUNK,
                        codec: ids::CHD_CODEC,
                    },
                    ChdOptionsControl::new(*p),
                    *p,
                    changed.clone(),
                )
            })
            .collect();
        for c in &chd {
            setup.append(c.root());
        }
        let multi_bin = gtk::CheckButton::with_label(MULTI_BIN_LABEL);
        multi_bin.set_tooltip_text(Some(MULTI_BIN_NOTE));
        multi_bin.set_widget_name(ids::MULTI_BIN);
        setup.append(&multi_bin);
        let same = line();
        same.add_css_class("warning");
        setup.append(&same);
        let scan_error = line();
        scan_error.add_css_class("error");
        setup.append(&scan_error);
        let jobs = JobList::new(ids::SCAN, Rc::new(|| {}));
        setup.append(jobs.root());
        let scan = button("Scan...", ids::SCAN);
        scan.set_tooltip_text(Some(SCAN_HINT));
        scan.add_css_class("suggested-action");
        let close_setup = button("Cancel", ids::CLOSE);
        setup.append(&button_row(&[&close_setup, &scan]));

        // Review.
        let review = page();
        review.set_spacing(6);
        let summary = wrapped("");
        review.append(&summary);
        review.append(&wrapped(REVIEW_NOTE));
        let ext_row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        let ext_title = gtk::Label::new(Some("Output extension: ."));
        ext_title.add_css_class("heading");
        let extension = gtk::Entry::builder().width_chars(8).build();
        extension.set_widget_name(ids::EXTENSION);
        let reset = button("Reset", ids::RESET_EXTENSION);
        let recommended = gtk::Label::new(None);
        recommended.add_css_class("dim-label");
        ext_row.append(&ext_title);
        ext_row.append(&extension);
        ext_row.append(&reset);
        ext_row.append(&recommended);
        review.append(&ext_row);
        let all = button("Select All", ids::SELECT_ALL);
        let none = button("Select None", ids::SELECT_NONE);
        let picks = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        picks.append(&all);
        picks.append(&none);
        review.append(&picks);
        let store = gio::ListStore::new::<glib::BoxedAnyObject>();
        let list = self.file_list(&store);
        review.append(&list);
        let back = button("Back", ids::BACK);
        let start = button("Start Conversion", ids::START);
        start.add_css_class("suggested-action");
        let close_review = button("Cancel", ids::CLOSE);
        review.append(&button_row(&[&back, &close_review, &start]));

        let stack = gtk::Stack::new();
        stack.add_named(&setup, Some("setup"));
        stack.add_named(&review, Some("review"));
        let s = send.clone();
        let dialog = window(
            "Bulk Convert",
            640,
            620,
            &stack,
            Rc::new(move || s(BulkConvertAction::Close)),
        );

        let s = send.clone();
        pick_source.connect_clicked(move |_| s(BulkConvertAction::PickSource));
        let s = send.clone();
        pick_output.connect_clicked(move |_| s(BulkConvertAction::PickOutput));
        let s = send.clone();
        format.connect_selected_notify(move |d| {
            if let Some(f) = FORMATS.get(d.selected() as usize) {
                s(BulkConvertAction::SetFormat(*f));
            }
        });
        let s = send.clone();
        multi_bin.connect_toggled(move |b| s(BulkConvertAction::SetMultiBin(b.is_active())));
        let s = send.clone();
        scan.connect_clicked(move |_| s(BulkConvertAction::Scan));
        for b in [&close_setup, &close_review] {
            let s = send.clone();
            b.connect_clicked(move |_| s(BulkConvertAction::Close));
        }
        let s = send.clone();
        extension.connect_changed(move |e| s(BulkConvertAction::SetExtension(e.text().into())));
        let s = send.clone();
        reset.connect_clicked(move |_| s(BulkConvertAction::ResetExtension));
        let s = send.clone();
        all.connect_clicked(move |_| s(BulkConvertAction::SelectAll));
        let s = send.clone();
        none.connect_clicked(move |_| s(BulkConvertAction::SelectNone));
        let s = send.clone();
        back.connect_clicked(move |_| s(BulkConvertAction::Back));
        let me = self.me.clone();
        start.connect_clicked(move |_| {
            let Some(screen) = me.upgrade().filter(|s| !s.sync.active()) else {
                return;
            };
            // Asked again at the click: the button only knows what the last refresh knew.
            if !screen.ctrl.borrow().can_start() {
                return;
            }
            let chd = screen.chd_choice();
            screen.apply(BulkConvertAction::Start(chd));
        });

        Rc::new(Form {
            dialog,
            stack,
            source,
            output,
            format,
            note,
            chd,
            multi_bin,
            same,
            scan_error,
            scan,
            jobs,
            summary,
            extension,
            reset,
            recommended,
            store,
            start,
            rows: RefCell::default(),
        })
    }

    fn file_list(&self, store: &gio::ListStore) -> gtk::ScrolledWindow {
        let view = gtk::ColumnView::new(Some(gtk::NoSelection::new(Some(store.clone()))));
        view.set_widget_name(ids::FILES);
        view.set_show_row_separators(true);
        view.append_column(&self.tick_column());
        view.append_column(&text_column("File", 0.0, true, |r| r.name.clone()));
        view.append_column(&text_column("Size", 1.0, false, |r| fmt_bytes(r.size)));
        gtk::ScrolledWindow::builder()
            .child(&view)
            .vexpand(true)
            .min_content_height(240)
            .build()
    }

    fn tick_column(&self) -> gtk::ColumnViewColumn {
        let factory = gtk::SignalListItemFactory::new();
        let send = self.sender();
        factory.connect_setup(move |_, obj| {
            let item = list_item(obj);
            let tick = gtk::CheckButton::new();
            tick.set_halign(gtk::Align::Center);
            let weak = item.downgrade();
            let send = send.clone();
            tick.connect_toggled(move |b| {
                let row = weak.upgrade().and_then(|i| row_of(&i));
                if let Some(row) = row {
                    let index = row.borrow::<Row>().index;
                    send(BulkConvertAction::SetSelected(index, b.is_active()));
                }
            });
            item.set_child(Some(&tick));
        });
        let me = self.me.clone();
        factory.connect_bind(move |_, obj| {
            let item = list_item(obj);
            let (Some(row), Some(tick), Some(screen)) = (
                row_of(item),
                item.child().and_downcast::<gtk::CheckButton>(),
                me.upgrade(),
            ) else {
                return;
            };
            let index = row.borrow::<Row>().index;
            let on = screen
                .form
                .borrow()
                .as_ref()
                .and_then(|f| f.rows.borrow().1.get(index).copied())
                .unwrap_or(false);
            // A recycled cell takes this row's state with the toggle muted.
            screen.sync.run(|| tick.set_active(on));
        });
        gtk::ColumnViewColumn::new(None, Some(factory))
    }
}

fn file_name(path: &std::path::Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn folder_row(page: &gtk::Box, title: &str) -> gtk::Label {
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 8);
    let head = gtk::Label::new(Some(title));
    head.add_css_class("heading");
    let value = gtk::Label::builder()
        .xalign(0.0)
        .hexpand(true)
        .ellipsize(gtk::pango::EllipsizeMode::Middle)
        .build();
    row.append(&head);
    row.append(&value);
    page.append(&row);
    value
}

fn list_item(obj: &glib::Object) -> &gtk::ListItem {
    obj.downcast_ref().expect("a column factory's item")
}

fn row_of(item: &gtk::ListItem) -> Option<glib::BoxedAnyObject> {
    item.item().and_downcast::<glib::BoxedAnyObject>()
}

fn text_column(
    title: &str,
    xalign: f32,
    expand: bool,
    cell: fn(&Row) -> String,
) -> gtk::ColumnViewColumn {
    let factory = gtk::SignalListItemFactory::new();
    factory.connect_setup(move |_, obj| {
        let label = gtk::Label::builder()
            .xalign(xalign)
            .ellipsize(gtk::pango::EllipsizeMode::Middle)
            .build();
        list_item(obj).set_child(Some(&label));
    });
    factory.connect_bind(move |_, obj| {
        let item = list_item(obj);
        let (Some(row), Some(label)) = (row_of(item), item.child().and_downcast::<gtk::Label>())
        else {
            return;
        };
        set_text(&label, &cell(&row.borrow::<Row>()));
    });
    let column = gtk::ColumnViewColumn::new(Some(title), Some(factory));
    column.set_resizable(true);
    column.set_expand(expand);
    column
}
