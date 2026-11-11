//! The windows the pane and the screen ask for, each up exactly while the controller says so.
//!
//! A [`Slot`] holds one dialog and the key of what it shows: a new key rebuilds it, no key closes
//! it, and a user closing it sends the slot's dismiss action. These are the same windows Commander
//! opens, so nothing here assumes the Inspect browse view beyond the pane it reads.

use std::cell::{Cell, RefCell};
use std::path::Path;
use std::rc::Rc;

use adw::prelude::*;
use gtk::glib;
use rusty_backup::controller::app::Snapshot;
use rusty_backup::controller::browser::metadata::known_type_creators;
use rusty_backup::controller::browser::{ids, TextDoc};
use rusty_backup::controller::browser::{MetadataFields, MetadataForm};
use rusty_backup::controller::commander::{
    format_size, import_prompt, journal_status, journal_summary, journal_txn_blocks,
    journal_txn_rows, ApplyPreflight, ApplyReport, ArchiveSave, ArchiveViewer,
    BrowserPaneAction as P, BudgetAction, BudgetForm, BudgetMode, ChecksumRow,
    CommanderAction as A, CommanderController, ForkFormat, HashAlgo, ImportChoice, JournalDetail,
    JournalTone, LineEnding, MacArchiveKind, ModeAction, OnConflict, Side, SkippedReport,
    TarChoice, TarPreview, TextShape, ToolReport, TreeText, UnsavedChoice,
    MAX_CONSECUTIVE_FAILURES,
};
use rusty_backup::controller::keys::{Key, Mods};

use super::info::{FileInfo, InfoShown};
use super::SendFn;
use crate::effects::EffectHost;
use crate::keys::{key_of, mods_of};
use crate::progress::JobList;
use crate::widgets::plain;

/// Runs a key through the screen's key map; true when it meant something.
pub type KeyFn = Rc<dyn Fn(Key, Mods) -> bool>;

/// Past this a tree listing offers Save before it fills a text view.
const TREE_INLINE_LIMIT: usize = 1024 * 1024;

fn pane(side: Side, action: P) -> A {
    A::Pane(side, action)
}

/// A dialog the controller holds open.
#[derive(Default)]
pub struct Slot {
    shown: Rc<RefCell<Option<(String, adw::Dialog)>>>,
    /// Runs when the user closes it, for what the view itself holds.
    on_close: RefCell<Option<Rc<dyn Fn()>>>,
}

impl Slot {
    /// Show what `want` keys, built by `build` when it changed; `None` closes it.
    pub fn sync<T: Clone + 'static>(
        &self,
        host: &EffectHost,
        send: &Rc<dyn Fn(T)>,
        want: Option<String>,
        dismiss: Option<T>,
        build: impl FnOnce() -> adw::Dialog,
    ) {
        let same = {
            let shown = self.shown.borrow();
            shown.as_ref().map(|(k, _)| k) == want.as_ref()
        };
        if same {
            return;
        }
        let old = self.shown.borrow_mut().take();
        if let Some((_, dialog)) = old {
            dialog.force_close();
        }
        let Some(key) = want else {
            return;
        };
        let dialog = build();
        let held = self.shown.clone();
        let send = send.clone();
        let on_close = self.on_close.borrow().clone();
        dialog.connect_closed(move |d| {
            let mine = held.borrow().as_ref().is_some_and(|(_, s)| s == d);
            if mine {
                held.borrow_mut().take();
                if let Some(f) = &on_close {
                    f();
                }
                if let Some(action) = dismiss.clone() {
                    send(action);
                }
            }
        });
        *self.shown.borrow_mut() = Some((key, dialog.clone()));
        host.present(&dialog);
    }
}

/// A dialog with a title bar around `body`.
pub fn window(title: &str, width: i32, height: i32, body: &impl IsA<gtk::Widget>) -> adw::Dialog {
    let bar = adw::HeaderBar::new();
    let view = adw::ToolbarView::new();
    view.add_top_bar(&bar);
    view.set_content(Some(body));
    adw::Dialog::builder()
        .title(plain(title).as_ref())
        .content_width(width)
        .content_height(height)
        .child(&view)
        .build()
}

pub fn page() -> gtk::Box {
    let b = gtk::Box::new(gtk::Orientation::Vertical, 6);
    b.set_margin_start(12);
    b.set_margin_end(12);
    b.set_margin_top(8);
    b.set_margin_bottom(12);
    b
}

pub fn wrapped(text: &str) -> gtk::Label {
    gtk::Label::builder()
        .label(plain(text).as_ref())
        .xalign(0.0)
        .wrap(true)
        .wrap_mode(gtk::pango::WrapMode::WordChar)
        .build()
}

fn toned(text: &str, class: &str) -> gtk::Label {
    let l = wrapped(text);
    l.add_css_class(class);
    l
}

pub fn buttons(send: &SendFn, list: &[(&str, A)]) -> gtk::Box {
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    row.set_halign(gtk::Align::End);
    for (label, action) in list {
        let b = gtk::Button::with_label(label);
        let (send, action) = (send.clone(), action.clone());
        b.connect_clicked(move |_| send(action.clone()));
        row.append(&b);
    }
    row
}

/// Read-only monospace text with scroll bars.
pub fn text_view(text: &str) -> gtk::ScrolledWindow {
    let view = gtk::TextView::builder()
        .editable(false)
        .monospace(true)
        .cursor_visible(false)
        .wrap_mode(gtk::WrapMode::None)
        .build();
    view.buffer().set_text(&plain(text));
    gtk::ScrolledWindow::builder()
        .child(&view)
        .vexpand(true)
        .min_content_height(120)
        .build()
}

/// A list of paths, or anything else that can run long, in a short scroller.
pub fn listing(lines: &[String]) -> gtk::ScrolledWindow {
    let scroll = text_view(&lines.join("\n"));
    scroll.set_max_content_height(200);
    scroll.set_propagate_natural_height(true);
    scroll.set_vexpand(false);
    scroll
}

/// One pane's windows, read while the controller is borrowed.
pub struct PaneDialogsShown {
    review: Option<ApplyPreflight>,
    report: Option<(ApplyReport, usize)>,
    edits: Option<(Vec<String>, bool)>,
    tar: Option<String>,
    tar_preview: Option<(TarPreview, String)>,
    import: Option<(String, MacArchiveKind)>,
    save: Option<ArchiveSave>,
    viewer: Option<ArchiveViewer>,
    errors: Vec<(String, String)>,
    fs_type: String,
    tool: Option<(ToolReport, bool, bool)>,
    metadata: Option<MetadataForm>,
}

/// What the windows show this refresh, read while the controller is borrowed.
pub struct DialogsShown {
    panes: Vec<PaneDialogsShown>,
    unsaved: Option<(usize, Vec<String>)>,
    budget: Option<BudgetForm>,
    chd: Option<String>,
    log: Option<Vec<String>>,
    help: Option<(Vec<(&'static str, String)>, String)>,
    checksums: Option<ChecksumsShown>,
    conflict: Option<Vec<String>>,
    skipped: Option<SkippedReport>,
    password: Option<bool>,
    detail: Option<(u64, String)>,
    info: Option<InfoShown>,
}

struct ChecksumsShown {
    title: String,
    error: Option<String>,
    rows: Vec<ChecksumRow>,
    running: Option<Snapshot>,
}

/// The windows a pane opens; a two-pane screen has one set per side.
struct PaneDialogs {
    side: Side,
    /// Names the pane in a two-pane screen's titles.
    tag: String,
    host: EffectHost,
    send: SendFn,
    review: Slot,
    report: Slot,
    edits: Slot,
    tar: Slot,
    tar_preview: Slot,
    import: Slot,
    save: Slot,
    viewer: Slot,
    viewer_extract: RefCell<Option<gtk::Button>>,
    errors: Slot,
    tool: Slot,
    show_debug: Rc<Cell<bool>>,
    metadata: Slot,
    metadata_error: RefCell<Option<gtk::Label>>,
}

pub struct Dialogs {
    host: EffectHost,
    send: SendFn,
    panes: Vec<Rc<PaneDialogs>>,
    key: KeyFn,
    unsaved: Slot,
    tree: Slot,
    tree_large: Slot,
    tree_text: Rc<RefCell<Option<TreeText>>>,
    tree_force: Rc<Cell<bool>>,
    plain_tree: Cell<bool>,
    budget: Slot,
    budget_live: RefCell<Option<(gtk::Label, gtk::Label, gtk::Button)>>,
    chd: Slot,
    log: Slot,
    help: Slot,
    checksums: Slot,
    checksum_jobs: RefCell<Option<JobList>>,
    conflict: Slot,
    skipped: Slot,
    password: Slot,
    detail: Slot,
    info: RefCell<Option<Rc<FileInfo>>>,
}

impl PaneDialogs {
    fn new(side: Side, tag: String, host: &EffectHost, send: &SendFn) -> Rc<PaneDialogs> {
        Rc::new(PaneDialogs {
            side,
            tag,
            host: host.clone(),
            send: send.clone(),
            review: Slot::default(),
            report: Slot::default(),
            edits: Slot::default(),
            tar: Slot::default(),
            tar_preview: Slot::default(),
            import: Slot::default(),
            save: Slot::default(),
            viewer: Slot::default(),
            viewer_extract: RefCell::default(),
            errors: Slot::default(),
            tool: Slot::default(),
            show_debug: Rc::default(),
            metadata: Slot::default(),
            metadata_error: RefCell::default(),
        })
    }

    fn read(ctl: &CommanderController, side: Side) -> PaneDialogsShown {
        let pane = ctl.pane(side);
        PaneDialogsShown {
            review: pane.apply_review().cloned(),
            report: pane.apply_report().cloned().map(|r| (r, pane.queue.len())),
            edits: pane
                .edits_shown()
                .then(|| (pane.edit_lines(), pane.is_busy())),
            tar: pane.pending_tar(),
            tar_preview: pane
                .tar_preview()
                .cloned()
                .map(|p| (p, pane.volume().fs_type.clone())),
            import: pane.pending_import(),
            save: pane.archive_save().cloned(),
            viewer: pane.archive_viewer().cloned(),
            errors: pane.staging_errors().to_vec(),
            fs_type: pane.volume().fs_type.clone(),
            tool: pane.tool_report().cloned().map(|r| {
                let repairable = !ctl.has_archive_flow();
                (r, repairable, ctl.edit_mode())
            }),
            metadata: pane.metadata_form().cloned(),
        }
    }

    fn show(self: &Rc<Self>, s: PaneDialogsShown) {
        let (side, host, send) = (self.side, &self.host, &self.send);

        let want = s.metadata.as_ref().map(|f| f.entry.path.clone());
        let built = RefCell::new(None);
        self.metadata.sync(
            host,
            send,
            want,
            Some(pane(side, P::CancelMetadata)),
            || {
                let form = s.metadata.clone().expect("a form to show");
                let (dialog, error) = metadata_dialog(self, &form);
                *built.borrow_mut() = Some(error);
                dialog
            },
        );
        if let Some(error) = built.into_inner() {
            *self.metadata_error.borrow_mut() = Some(error);
        }
        if let Some(label) = &*self.metadata_error.borrow() {
            let error = s.metadata.as_ref().and_then(|f| f.error.as_deref());
            label.set_visible(error.is_some());
            label.set_text(&plain(error.unwrap_or("")));
        }

        let want = s.review.as_ref().map(|_| "review".to_string());
        self.review
            .sync(host, send, want, Some(pane(side, P::DismissReview)), || {
                let p = s.review.clone().expect("a review to show");
                review_dialog(self, &p)
            });

        let want = s
            .report
            .as_ref()
            .map(|(r, n)| format!("{}/{}/{n}/{}", r.applied, r.total, r.failed.len()));
        self.report
            .sync(host, send, want, Some(pane(side, P::DismissReport)), || {
                let (r, still) = s.report.clone().expect("a report to show");
                report_dialog(self, &r, still)
            });

        let want = s.edits.as_ref().map(|(l, busy)| format!("{l:?}{busy}"));
        self.edits.sync(
            host,
            send,
            want,
            Some(pane(side, P::ShowEdits(false))),
            || {
                let (lines, busy) = s.edits.clone().unwrap_or_default();
                edits_dialog(self, &lines, busy)
            },
        );

        self.tar.sync(
            host,
            send,
            s.tar.clone(),
            Some(pane(side, P::ChooseTar(TarChoice::Cancel))),
            || {
                let name = s.tar.clone().unwrap_or_default();
                let body = page();
                body.append(&wrapped(&format!("\"{name}\" is a tar archive.")));
                body.append(&wrapped(
                    "Import its contents into the current folder, or add the archive itself as \
                     a file?",
                ));
                let choose = |c| pane(side, P::ChooseTar(c));
                body.append(&buttons(
                    send,
                    &[
                        ("Import contents", choose(TarChoice::Import)),
                        ("Add as file", choose(TarChoice::AsFile)),
                        ("Cancel", choose(TarChoice::Cancel)),
                    ],
                ));
                window("Tar archive", 460, -1, &body)
            },
        );

        let want = s.tar_preview.as_ref().map(|_| "preview".to_string());
        self.tar_preview.sync(
            host,
            send,
            want,
            Some(pane(side, P::ConfirmTarImport(false))),
            || {
                let (p, fs) = s.tar_preview.clone().expect("a preview to show");
                let body = page();
                body.append(&toned(
                    &format!("This [{fs}] volume can't store everything in the archive:"),
                    "warning",
                ));
                for w in &p.warnings {
                    body.append(&wrapped(&format!("- {w}")));
                }
                if p.sidecars > 0 {
                    let line = format!("- {} macOS ._ sidecar(s) will be skipped.", p.sidecars);
                    body.append(&wrapped(&line));
                }
                body.append(&wrapped(&format!(
                    "{} file(s) and {} dir(s) will be imported. Existing names are skipped.",
                    p.files, p.dirs
                )));
                body.append(&buttons(
                    send,
                    &[
                        ("Continue", pane(side, P::ConfirmTarImport(true))),
                        ("Cancel", pane(side, P::ConfirmTarImport(false))),
                    ],
                ));
                window(
                    "Import archive - some content can't be kept",
                    520,
                    -1,
                    &body,
                )
            },
        );

        let want = s.import.as_ref().map(|(n, k)| format!("{n}{}", k.label()));
        self.import.sync(
            host,
            send,
            want,
            Some(pane(side, P::ChooseImport(ImportChoice::Cancel))),
            || {
                let (name, kind) = s.import.clone().expect("an import to show");
                import_dialog(self, &name, kind)
            },
        );

        let want = s.save.as_ref().map(|p| p.entry_name.clone());
        self.save.sync(
            host,
            send,
            want,
            Some(pane(side, P::CancelSaveArchive)),
            || {
                let p = s.save.clone().expect("an archive to save");
                archive_save_dialog(self, &p)
            },
        );

        let want = s.viewer.as_ref().map(|w| w.entry_name.clone());
        let mut extract = None;
        self.viewer
            .sync(host, send, want, Some(pane(side, P::CloseArchive)), || {
                let w = s.viewer.clone().expect("an archive to view");
                let (dialog, button) = archive_viewer_dialog(self, &w);
                extract = Some(button);
                dialog
            });
        if let Some(button) = extract {
            *self.viewer_extract.borrow_mut() = Some(button);
        }
        if let (Some(w), Some(button)) = (&s.viewer, &*self.viewer_extract.borrow()) {
            let n = w.marked_files();
            button.set_label(&format!("Extract Selected... ({n})"));
            button.set_sensitive(n > 0);
        }

        let want = (!s.errors.is_empty()).then(|| format!("{:?}", s.errors));
        self.errors.sync(
            host,
            send,
            want,
            Some(pane(side, P::DismissStagingErrors)),
            || errors_dialog(self, &s.errors, &s.fs_type),
        );

        let debug = self.show_debug.clone();
        let want = s
            .tool
            .as_ref()
            .map(|(r, repairable, edit)| format!("{r:?}{repairable}{edit}{}", debug.get()));
        self.tool.sync(
            host,
            send,
            want,
            Some(pane(side, P::DismissToolReport)),
            || {
                let (r, repairable, edit) = s.tool.clone().expect("a report to show");
                tool_dialog(&r, repairable, edit, self)
            },
        );
    }
}

impl Dialogs {
    /// One set of pane windows per side in `sides`; `key` runs a key press through the key map.
    pub fn new(host: EffectHost, send: SendFn, sides: &[Side], key: KeyFn) -> Rc<Dialogs> {
        let tree_text: Rc<RefCell<Option<TreeText>>> = Rc::default();
        let drop_tree = |held: &Rc<RefCell<Option<TreeText>>>| {
            let held = held.clone();
            let f: Rc<dyn Fn()> = Rc::new(move || *held.borrow_mut() = None);
            RefCell::new(Some(f))
        };
        let slot_dropping_tree = || Slot {
            on_close: drop_tree(&tree_text),
            ..Slot::default()
        };
        let tag = |s: &Side| match sides.len() {
            1 => String::new(),
            _ => format!(" ({})", s.label()),
        };
        let panes = sides
            .iter()
            .map(|s| PaneDialogs::new(*s, tag(s), &host, &send))
            .collect();
        Rc::new(Dialogs {
            host,
            send,
            panes,
            key,
            unsaved: Slot::default(),
            tree: slot_dropping_tree(),
            tree_large: slot_dropping_tree(),
            tree_text: tree_text.clone(),
            tree_force: Rc::default(),
            plain_tree: Cell::default(),
            budget: Slot::default(),
            budget_live: RefCell::default(),
            chd: Slot::default(),
            log: Slot::default(),
            help: Slot::default(),
            checksums: Slot::default(),
            checksum_jobs: RefCell::default(),
            conflict: Slot::default(),
            skipped: Slot::default(),
            password: Slot::default(),
            detail: Slot::default(),
            info: RefCell::default(),
        })
    }

    pub fn read(&self, ctl: &CommanderController) -> DialogsShown {
        let pane = ctl.pane(Side::Left);
        let checksums = ctl.checksums().map(|c| ChecksumsShown {
            title: c.title.clone(),
            error: c.error.clone(),
            rows: c.rows.clone(),
            running: c.running(),
        });
        let known = self.info.borrow().as_ref().and_then(|live| live.key());
        DialogsShown {
            panes: self
                .panes
                .iter()
                .map(|p| PaneDialogs::read(ctl, p.side))
                .collect(),
            unsaved: ctl
                .unsaved_shown()
                .then(|| (pane.queue.len(), pane.staged_summary())),
            budget: ctl.size_budget().copied(),
            chd: ctl.chd_info().map(str::to_string),
            log: ctl.log_shown().then(|| ctl.log().to_vec()),
            help: ctl
                .help_shown()
                .then(|| (ctl.key_help(), ctl.key_help_footer())),
            checksums,
            conflict: ctl.host_conflict().map(<[String]>::to_vec),
            skipped: ctl.skipped_report().cloned(),
            // A two-pane screen asks inside the pane, beside the other pane's work.
            password: (pane.needs_password() && ctl.is_single())
                .then(|| pane.volume().fs_type == "APFS"),
            detail: ctl
                .detail()
                .map(|d| (d.serial, format!("File Info: {}", d.entry.name))),
            info: ctl.detail().map(|d| FileInfo::read(ctl, d, known)),
        }
    }

    /// The tree window leaves out Show IDs.
    pub fn set_plain_tree(&self, on: bool) {
        self.plain_tree.set(on);
    }

    /// Keep a tree listing the pane handed over until its window closes.
    pub fn set_tree(&self, tree: TreeText) {
        self.tree_force.set(false);
        *self.tree_text.borrow_mut() = Some(tree);
    }

    pub fn clear(&self) {
        *self.tree_text.borrow_mut() = None;
    }

    pub fn show(self: &Rc<Self>, s: DialogsShown) {
        let (host, send) = (&self.host, &self.send);
        for (pane, shown) in self.panes.iter().zip(s.panes) {
            pane.show(shown);
        }

        let want = s.unsaved.as_ref().map(|(n, l)| format!("{n}{l:?}"));
        self.unsaved.sync(
            host,
            send,
            want,
            Some(A::Mode(ModeAction::Unsaved(UnsavedChoice::Cancel))),
            || {
                let (n, lines) = s.unsaved.clone().unwrap_or_default();
                let body = page();
                body.append(&wrapped(&format!("You have {n} unsaved edit(s).")));
                for line in &lines {
                    body.append(&wrapped(line));
                }
                let answer = |c| A::Mode(ModeAction::Unsaved(c));
                body.append(&buttons(
                    send,
                    &[
                        ("Discard Edits", answer(UnsavedChoice::Discard)),
                        ("Apply Edits", answer(UnsavedChoice::Apply)),
                        ("Cancel", answer(UnsavedChoice::Cancel)),
                    ],
                ));
                window("Unsaved Changes", 420, -1, &body)
            },
        );

        self.show_tree();

        self.show_budget(s.budget.as_ref());

        self.chd.sync(
            host,
            send,
            s.chd.clone(),
            Some(A::Mode(ModeAction::CloseChdInfo)),
            || {
                let body = page();
                body.append(&text_view(s.chd.as_deref().unwrap_or("")));
                window("CHD Info", 560, 420, &body)
            },
        );

        let want = s.log.as_ref().map(|l| format!("{l:?}"));
        self.log
            .sync(host, send, want, Some(A::ShowLog(false)), || {
                log_dialog(s.log.as_deref().unwrap_or(&[]), send)
            });

        let want = s.help.as_ref().map(|_| "help".to_string());
        self.help
            .sync(host, send, want, Some(A::ShowHelp(false)), || {
                let (keys, footer) = s.help.clone().unwrap_or_default();
                let grid = gtk::Grid::builder()
                    .row_spacing(2)
                    .column_spacing(16)
                    .build();
                for (i, (label, chord)) in keys.iter().enumerate() {
                    let name = gtk::Label::builder().label(*label).xalign(0.0).build();
                    let keys = gtk::Label::builder().label(chord).xalign(0.0).build();
                    keys.add_css_class("monospace");
                    grid.attach(&name, 0, i as i32, 1, 1);
                    grid.attach(&keys, 1, i as i32, 1, 1);
                }
                let body = page();
                let scroll = gtk::ScrolledWindow::builder()
                    .child(&grid)
                    .vexpand(true)
                    .build();
                body.append(&scroll);
                body.append(&wrapped(&footer));
                let dialog = window("Keyboard shortcuts", 420, 480, &body);
                dialog.add_controller(forward_keys(&self.key));
                dialog
            });

        self.show_checksums(s.checksums.as_ref());

        let want = s.conflict.as_ref().map(|n| format!("{n:?}"));
        self.conflict
            .sync(host, send, want, Some(A::HostConflictCancel), || {
                let names = s.conflict.clone().unwrap_or_default();
                let body = page();
                body.append(&wrapped(&format!(
                    "{} item(s) already exist in the destination folder:",
                    names.len()
                )));
                body.append(&listing(&names));
                body.append(&buttons(
                    send,
                    &[
                        ("Overwrite", A::HostConflict(OnConflict::Replace)),
                        ("Skip existing", A::HostConflict(OnConflict::Skip)),
                        ("Cancel", A::HostConflictCancel),
                    ],
                ));
                window("Files already exist", 480, -1, &body)
            });

        let want = s.skipped.as_ref().map(|r| r.summary.clone());
        self.skipped
            .sync(host, send, want, Some(A::SkippedReportOk), || {
                let r = s.skipped.clone().expect("a report to show");
                let body = page();
                body.append(&wrapped(&r.summary));
                let lines: Vec<String> = r.skipped.iter().map(|s| format!("Skipped {s}")).collect();
                if !lines.is_empty() {
                    body.append(&listing(&lines));
                }
                body.append(&buttons(send, &[("OK", A::SkippedReportOk)]));
                window(&r.title, 520, -1, &body)
            });

        let want = s.password.map(|apfs| apfs.to_string());
        self.password.sync(host, send, want, None, || {
            password_dialog(s.password.unwrap_or(false), send)
        });

        self.show_detail(s.detail, s.info);
    }

    /// File Info: the key is the entry; what a job reads later lands in the open window.
    fn show_detail(&self, detail: Option<(u64, String)>, info: Option<InfoShown>) {
        let (host, send) = (&self.host, &self.send);
        let title = detail.as_ref().map(|d| d.1.clone()).unwrap_or_default();
        let want = detail.map(|d| d.0.to_string());
        if want.is_none() {
            *self.info.borrow_mut() = None;
        }
        let mut fresh = None;
        self.detail
            .sync(host, send, want, Some(A::CloseDetail), || {
                let live = FileInfo::build(send);
                let dialog = window(&title, 620, 560, live.root());
                fresh = Some(live);
                dialog
            });
        if let Some(live) = fresh {
            *self.info.borrow_mut() = Some(live);
        }
        if let (Some(live), Some(info)) = (&*self.info.borrow(), info) {
            live.show(info);
        }
    }

    fn show_checksums(&self, c: Option<&ChecksumsShown>) {
        let (host, send) = (&self.host, &self.send);
        // The running job's numbers stay out of the key, or every wake would rebuild the window.
        let want = c.map(|c| {
            format!(
                "{}{:?}{}{}",
                c.title,
                c.error,
                c.rows.len(),
                c.running.is_some()
            )
        });
        let mut jobs = None;
        self.checksums
            .sync(host, send, want, Some(A::CloseChecksums), || {
                let c = c.expect("checksums to show");
                let body = page();
                if let Some(err) = &c.error {
                    body.append(&toned(err, "error"));
                } else if c.running.is_some() {
                    let list = JobList::new(ids::CANCEL_JOB, Rc::new(|| {}));
                    body.append(list.root());
                    jobs = Some(list);
                } else {
                    body.append(&buttons(send, &[("Copy all", A::CopyChecksums)]));
                    body.append(&checksum_rows(&c.rows, send));
                }
                window(&c.title, 600, 420, &body)
            });
        if jobs.is_some() {
            *self.checksum_jobs.borrow_mut() = jobs;
        }
        let held = self.checksum_jobs.borrow();
        if let (Some(list), Some(snap)) = (&*held, c.and_then(|c| c.running.clone())) {
            list.show(&[(snap, false)]);
        }
    }

    fn show_tree(self: &Rc<Self>) {
        let (host, send) = (&self.host, &self.send);
        let held = self.tree_text.borrow().clone();
        let large = held
            .as_ref()
            .is_some_and(|t| t.text.len() > TREE_INLINE_LIMIT && !self.tree_force.get());
        let want = held
            .as_ref()
            .filter(|_| large)
            .map(|t| format!("large{}", t.text.len()));
        let this = self.clone();
        self.tree_large.sync(host, send, want, None, || {
            let text = held.as_ref().map(|t| t.text.as_str()).unwrap_or("");
            let lines = text.bytes().filter(|b| *b == b'\n').count();
            let body = page();
            body.append(&wrapped(&format!(
                "The generated tree is {:.1} MB (~{lines} entries).",
                text.len() as f64 / (1024.0 * 1024.0)
            )));
            body.append(&wrapped("Save it to a file, or show it here (slow)."));
            let save = gtk::Button::with_label("Save to File...");
            let send = this.send.clone();
            save.connect_clicked(move |_| send(pane(Side::Left, P::SaveTree)));
            let anyway = gtk::Button::with_label("Show in app anyway (slow)");
            let again = this.clone();
            anyway.connect_clicked(move |_| {
                again.tree_force.set(true);
                again.show_tree();
            });
            let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            row.set_halign(gtk::Align::End);
            row.append(&save);
            row.append(&anyway);
            body.append(&row);
            window("Tree view is large", 460, -1, &body)
        });
        let want = held
            .as_ref()
            .filter(|_| !large)
            .map(|t| format!("{}{}", t.ids, t.text.len()));
        let this = self.clone();
        self.tree.sync(host, send, want, None, || {
            tree_dialog(&held.clone().expect("a tree to show"), &this)
        });
    }

    fn show_budget(&self, form: Option<&BudgetForm>) {
        let (host, send) = (&self.host, &self.send);
        let want = form.map(|_| "budget".to_string());
        let mut live = None;
        self.budget.sync(
            host,
            send,
            want,
            Some(A::Mode(ModeAction::Budget(BudgetAction::Cancel))),
            || {
                let form = form.expect("a form to show");
                let (dialog, parts) = budget_dialog(form, send);
                live = Some(parts);
                dialog
            },
        );
        if live.is_some() {
            *self.budget_live.borrow_mut() = live;
        }
        let (Some(form), Some((ceiling, over, start))) = (form, &*self.budget_live.borrow()) else {
            return;
        };
        let text = match form.ceiling() {
            Some(c) => format!("Ceiling: {}", format_size(c)),
            None => "Ceiling: none beyond what the container allows".to_string(),
        };
        ceiling.set_label(&text);
        over.set_visible(form.over_capacity());
        over.set_label(&format!(
            "That is more than the {} available where this image lives.",
            format_size(form.plan().capacity.unwrap_or(0))
        ));
        start.set_sensitive(!form.over_capacity());
    }
}

/// A window's keys go through the screen's key map, so its toggle key closes it too.
fn forward_keys(key: &KeyFn) -> gtk::EventControllerKey {
    let keys = gtk::EventControllerKey::new();
    let key = key.clone();
    keys.connect_key_pressed(move |_, pressed, _, state| {
        let used = key_of(pressed).is_some_and(|k| key(k, mods_of(state)));
        match used {
            true => glib::Propagation::Stop,
            false => glib::Propagation::Proceed,
        }
    });
    keys
}

fn log_dialog(lines: &[String], send: &SendFn) -> adw::Dialog {
    let body = page();
    body.append(&wrapped(&format!(
        "{} operation(s) this session.",
        lines.len()
    )));
    if lines.is_empty() {
        let hint = wrapped(
            "No operations yet. Applied edits, host copies / exports, and immediate host \
             deletes / renames / new folders are recorded here.",
        );
        hint.add_css_class("dim-label");
        hint.set_vexpand(true);
        hint.set_valign(gtk::Align::Start);
        body.append(&hint);
    } else {
        let scroll = text_view(&lines.join("\n"));
        if let Some(view) = scroll.child().and_downcast::<gtk::TextView>() {
            let buffer = view.buffer();
            let end = buffer.create_mark(None, &buffer.end_iter(), false);
            glib::idle_add_local_once(move || view.scroll_mark_onscreen(&end));
        }
        body.append(&scroll);
    }
    let row = buttons(send, &[("Copy all", A::CopyLog), ("Clear", A::ClearLog)]);
    if let Some(copy) = row.first_child() {
        copy.set_sensitive(!lines.is_empty());
    }
    body.append(&row);
    window("Session log", 560, 360, &body)
}

/// One block per file: its name, then each value with a Copy button.
fn checksum_rows(rows: &[ChecksumRow], send: &SendFn) -> gtk::ScrolledWindow {
    let list = gtk::Box::new(gtk::Orientation::Vertical, 8);
    for (file, row) in rows.iter().enumerate() {
        let name = wrapped(&format!("{}  ({})", row.name, format_size(row.size)));
        name.add_css_class("heading");
        list.append(&name);
        if let Some(err) = &row.error {
            list.append(&toned(&format!("failed: {err}"), "error"));
        }
        let Some(values) = &row.values else {
            continue;
        };
        let grid = gtk::Grid::builder()
            .row_spacing(2)
            .column_spacing(12)
            .build();
        for (i, (algo, value)) in HashAlgo::ALL.iter().zip(values).enumerate() {
            let shown = gtk::Label::builder()
                .label(value.as_str())
                .xalign(0.0)
                .selectable(true)
                .wrap(true)
                .wrap_mode(gtk::pango::WrapMode::Char)
                .hexpand(true)
                .build();
            shown.add_css_class("monospace");
            let copy = gtk::Button::with_label("Copy");
            copy.add_css_class("flat");
            let (send, algo) = (send.clone(), *algo);
            copy.connect_clicked(move |_| send(A::CopyChecksum { file, algo }));
            let label = gtk::Label::builder()
                .label(algo.label())
                .xalign(0.0)
                .build();
            grid.attach(&label, 0, i as i32, 1, 1);
            grid.attach(&shown, 1, i as i32, 1, 1);
            grid.attach(&copy, 2, i as i32, 1, 1);
        }
        list.append(&grid);
    }
    gtk::ScrolledWindow::builder()
        .child(&list)
        .vexpand(true)
        .min_content_height(160)
        .hscrollbar_policy(gtk::PolicyType::Never)
        .build()
}

fn review_dialog(on: &PaneDialogs, p: &ApplyPreflight) -> adw::Dialog {
    let (side, send) = (on.side, &on.send);
    let body = page();
    if p.shortfall() > 0 {
        body.append(&toned(
            &format!(
                "Not enough space: needs about {}, {} free on {}.",
                format_size(p.bytes_needed),
                format_size(p.bytes_free),
                p.fs_type
            ),
            "error",
        ));
    }
    let choice = Rc::new(Cell::new(OnConflict::Skip));
    if !p.scan.conflicts.is_empty() {
        body.append(&wrapped(&format!(
            "{} item(s) already exist on {}:",
            p.scan.conflicts.len(),
            p.fs_type
        )));
        let names: Vec<String> = p.scan.conflicts.iter().map(|c| c.0.clone()).collect();
        body.append(&listing(&names));
        let skip = gtk::CheckButton::with_label("Skip existing");
        skip.set_active(true);
        let replace = gtk::CheckButton::with_label("Overwrite");
        replace.set_group(Some(&skip));
        let held = choice.clone();
        replace.connect_toggled(move |b| {
            held.set(match b.is_active() {
                true => OnConflict::Replace,
                false => OnConflict::Skip,
            });
        });
        let row = gtk::Box::new(gtk::Orientation::Horizontal, 12);
        row.append(&skip);
        row.append(&replace);
        body.append(&row);
    }
    if !p.scan.blocked_folders.is_empty() {
        body.append(&wrapped(&format!(
            "{} folder(s) can't be created because a file has the same name; their contents \
             will fail:",
            p.scan.blocked_folders.len()
        )));
        body.append(&listing(&p.scan.blocked_folders));
    }
    if !p.scan.missing_folders.is_empty() {
        body.append(&wrapped(&format!(
            "{} destination folder(s) no longer exist; items going into them will fail:",
            p.scan.missing_folders.len()
        )));
        body.append(&listing(&p.scan.missing_folders));
    }
    body.append(&wrapped(
        "Items that fail are listed at the end; everything else is still written.",
    ));
    let label = if p.shortfall() > 0 {
        "Apply anyway"
    } else {
        "Apply"
    };
    let go = gtk::Button::with_label(label);
    let (to_pane, held) = (send.clone(), choice);
    go.connect_clicked(move |_| to_pane(pane(side, P::ApplyReviewed(held.get()))));
    let cancel = gtk::Button::with_label("Cancel");
    let to_pane = send.clone();
    cancel.connect_clicked(move |_| to_pane(pane(side, P::DismissReview)));
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    row.set_halign(gtk::Align::End);
    row.append(&go);
    row.append(&cancel);
    body.append(&row);
    window(&format!("Before applying{}", on.tag), 480, -1, &body)
}

fn report_dialog(on: &PaneDialogs, r: &ApplyReport, still: usize) -> adw::Dialog {
    let (side, send) = (on.side, &on.send);
    let body = page();
    if let Some(fatal) = &r.fatal {
        body.append(&toned(fatal, "error"));
    }
    body.append(&wrapped(&format!(
        "Applied {} of {} edit(s). {still} still staged.",
        r.applied, r.total
    )));
    if !r.failed.is_empty() {
        body.append(&wrapped(&format!("{} item(s) failed:", r.failed.len())));
        let lines: Vec<String> = r
            .failed
            .iter()
            .map(|f| format!("{}: {}", f.label, f.error))
            .collect();
        body.append(&listing(&lines));
    }
    if r.not_tried > 0 {
        body.append(&wrapped(&format!(
            "Stopped after {MAX_CONSECUTIVE_FAILURES} failures in a row; {} edit(s) were not tried.",
            r.not_tried
        )));
    }
    let mut list: Vec<(&str, Vec<A>)> = Vec::new();
    if still > 0 {
        list.push((
            "Retry",
            vec![pane(side, P::DismissReport), pane(side, P::Apply)],
        ));
        let discard = vec![pane(side, P::DismissReport), pane(side, P::Discard)];
        list.push(("Discard remaining", discard));
    }
    list.push(("Close", vec![pane(side, P::DismissReport)]));
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    row.set_halign(gtk::Align::End);
    for (label, actions) in list {
        let b = gtk::Button::with_label(label);
        let send = send.clone();
        b.connect_clicked(move |_| {
            for a in &actions {
                send(a.clone());
            }
        });
        row.append(&b);
    }
    body.append(&row);
    window(&format!("Apply report{}", on.tag), 560, -1, &body)
}

fn edits_dialog(on: &PaneDialogs, lines: &[String], busy: bool) -> adw::Dialog {
    let (side, send) = (on.side, &on.send);
    let body = page();
    body.append(&wrapped(
        "Staged for this pane (applied in order on Apply):",
    ));
    let list = gtk::ListBox::new();
    list.set_selection_mode(gtk::SelectionMode::None);
    for (i, line) in lines.iter().enumerate() {
        let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        let remove = gtk::Button::with_label("X");
        remove.add_css_class("flat");
        remove.set_tooltip_text(Some("Remove this edit from the queue"));
        remove.set_sensitive(!busy);
        let to_pane = send.clone();
        remove.connect_clicked(move |_| to_pane(pane(side, P::Unstage(i))));
        row.append(&remove);
        row.append(&wrapped(&format!("{}. {line}", i + 1)));
        list.append(&row);
    }
    let scroll = gtk::ScrolledWindow::builder()
        .child(&list)
        .vexpand(true)
        .min_content_height(160)
        .build();
    body.append(&scroll);
    window(&format!("Pending edits{}", on.tag), 460, 360, &body)
}

fn import_dialog(on: &PaneDialogs, name: &str, kind: MacArchiveKind) -> adw::Dialog {
    let (side, send) = (on.side, &on.send);
    let prompt = import_prompt(name, kind);
    let body = page();
    body.append(&wrapped(&prompt.intro));
    body.append(&wrapped(&prompt.question));
    let choose = |c| pane(side, P::ChooseImport(c));
    let mut list = vec![(prompt.expand, choose(ImportChoice::Expand))];
    list.extend(
        prompt
            .convert_only
            .map(|l| (l, choose(ImportChoice::ConvertOnly))),
    );
    list.push(("Add as-is", choose(ImportChoice::AsIs)));
    list.push(("Cancel", choose(ImportChoice::Cancel)));
    body.append(&buttons(send, &list));
    window(prompt.title, 520, -1, &body)
}

fn fork_dropdown(on: &PaneDialogs, current: ForkFormat, viewer: bool) -> gtk::DropDown {
    let (side, send) = (on.side, &on.send);
    let labels: Vec<&str> = ForkFormat::ALL.iter().map(|f| f.label()).collect();
    let drop = gtk::DropDown::from_strings(&labels);
    let at = ForkFormat::ALL.iter().position(|f| *f == current);
    drop.set_selected(at.unwrap_or(0) as u32);
    let send = send.clone();
    drop.connect_selected_notify(move |d| {
        if let Some(format) = ForkFormat::ALL.get(d.selected() as usize).copied() {
            send(pane(side, P::ArchiveFormat { viewer, format }));
        }
    });
    drop
}

fn archive_save_dialog(on: &PaneDialogs, p: &ArchiveSave) -> adw::Dialog {
    let (side, send) = (on.side, &on.send);
    let body = page();
    body.append(&wrapped(&format!(
        "{} is a {} archive.",
        p.entry_name,
        p.kind.label()
    )));
    body.append(&wrapped("How would you like to save it?"));
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    row.append(&gtk::Label::new(Some("Decoded fork container:")));
    row.append(&fork_dropdown(on, p.format, false));
    body.append(&row);
    body.append(&buttons(
        send,
        &[
            ("Save as-is", pane(side, P::SaveArchive { raw: true })),
            (
                "Decode and save contents to a folder...",
                pane(side, P::SaveArchive { raw: false }),
            ),
            ("Cancel", pane(side, P::CancelSaveArchive)),
        ],
    ));
    window("Save Mac archive", 520, -1, &body)
}

/// A compact binary size, as the Archives tab shows it.
fn binary_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    match u {
        0 => format!("{bytes} B"),
        _ => format!("{v:.1} {}", UNITS[u]),
    }
}

fn archive_viewer_dialog(on: &PaneDialogs, w: &ArchiveViewer) -> (adw::Dialog, gtk::Button) {
    let (side, send) = (on.side, &on.send);
    let body = page();
    body.append(&wrapped(&format!(
        "{} file(s) in archive (read-only view).",
        w.file_count()
    )));
    let list = gtk::StringList::new(&[]);
    for (i, row) in w.rows.iter().enumerate() {
        let line = match row.is_dir {
            true => format!("D{i}\t[ {} ]", row.path),
            false => format!(
                "F{i}\t{}\t{}\t{}\t{}",
                row.path,
                row.type_creator,
                binary_size(row.size),
                row.method
            ),
        };
        list.append(&plain(&line));
    }
    let marked = Rc::new(RefCell::new(w.marked.clone()));
    let factory = gtk::SignalListItemFactory::new();
    let to_pane = send.clone();
    let ticks = marked.clone();
    factory.connect_setup(move |_, obj| {
        let item: &gtk::ListItem = obj.downcast_ref().expect("a list item");
        let check = gtk::CheckButton::new();
        let label = gtk::Label::builder()
            .xalign(0.0)
            .single_line_mode(true)
            .build();
        let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        row.append(&check);
        row.append(&label);
        item.set_child(Some(&row));
        let weak = item.downgrade();
        let (send, ticks) = (to_pane.clone(), ticks.clone());
        check.connect_toggled(move |c| {
            let index = weak.upgrade().and_then(|i| {
                let line = i.item().and_downcast::<gtk::StringObject>()?;
                line.string().split('\t').next()?[1..].parse::<usize>().ok()
            });
            let Some(index) = index else {
                return;
            };
            let was = ticks.borrow().get(index).copied();
            if was.is_some_and(|m| m != c.is_active()) {
                ticks.borrow_mut()[index] = c.is_active();
                send(pane(side, P::ArchiveMark(index)));
            }
        });
    });
    let ticks = marked;
    factory.connect_bind(move |_, obj| {
        let item: &gtk::ListItem = obj.downcast_ref().expect("a list item");
        let (Some(line), Some(row)) = (
            item.item().and_downcast::<gtk::StringObject>(),
            item.child(),
        ) else {
            return;
        };
        let line = line.string();
        let mut parts = line.split('\t');
        let head = parts.next().unwrap_or("");
        let text: Vec<&str> = parts.collect();
        let check = row.first_child().and_downcast::<gtk::CheckButton>();
        let label = row.last_child().and_downcast::<gtk::Label>();
        if let (Some(check), Some(label)) = (check, label) {
            let index: usize = head[1..].parse().unwrap_or(0);
            check.set_visible(head.starts_with('F'));
            check.set_active(ticks.borrow().get(index).copied().unwrap_or(false));
            label.set_label(&text.join("   "));
        }
    });
    let view = gtk::ListView::new(Some(gtk::NoSelection::new(Some(list))), Some(factory));
    let scroll = gtk::ScrolledWindow::builder()
        .child(&view)
        .vexpand(true)
        .min_content_height(280)
        .build();
    body.append(&scroll);
    let extract = gtk::Button::with_label("Extract Selected...");
    let to_pane = send.clone();
    extract.connect_clicked(move |_| to_pane(pane(side, P::ArchiveExtract { all: false })));
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    row.append(&gtk::Label::new(Some("Extract as:")));
    row.append(&fork_dropdown(on, w.format, true));
    row.append(&buttons(
        send,
        &[(
            "Extract All...",
            pane(side, P::ArchiveExtract { all: true }),
        )],
    ));
    row.append(&extract);
    row.append(&buttons(send, &[("Close", pane(side, P::CloseArchive))]));
    body.append(&row);
    let title = format!("Archive: {} ({})", w.entry_name, w.kind.label());
    (window(&title, 640, 480, &body), extract)
}

fn errors_dialog(on: &PaneDialogs, rows: &[(String, String)], fs: &str) -> adw::Dialog {
    let (side, send) = (on.side, &on.send);
    let body = page();
    body.append(&toned(
        &format!(
            "{} item(s) could not be staged for the [{fs}] filesystem:",
            rows.len()
        ),
        "warning",
    ));
    let lines: Vec<String> = rows
        .iter()
        .map(|(path, reason)| {
            let name = Path::new(path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(path);
            format!("{name} - {reason}\n    {path}")
        })
        .collect();
    body.append(&text_view(&lines.join("\n")));
    body.append(&buttons(
        send,
        &[("OK", pane(side, P::DismissStagingErrors))],
    ));
    window("Could not add some files", 520, 320, &body)
}

fn tool_dialog(
    r: &ToolReport,
    repairable: bool,
    edit: bool,
    this: &Rc<PaneDialogs>,
) -> adw::Dialog {
    let side = this.side;
    let body = page();
    let Some(check) = &r.check else {
        body.append(&text_view(&r.lines.join("\n")));
        return window("Filesystem Check Results", 520, 360, &body);
    };
    if check.clean {
        body.append(&toned("Filesystem is clean.", "success"));
    } else {
        let line = format!("{} error(s) found.", check.errors.len());
        body.append(&toned(&line, "error"));
    }
    body.append(&wrapped(&check.stats_line));
    let debug = this.show_debug.get();
    let mut text = Vec::new();
    if !check.errors.is_empty() {
        text.push("Errors:".to_string());
        text.extend(check.errors.iter().cloned());
    }
    if !check.orphans.is_empty() {
        let total: usize = check.orphans.iter().map(|(_, v)| v.len()).sum();
        let parents = check.orphans.len();
        let tail = if parents == 1 { "y" } else { "ies" };
        text.push(format!(
            "{total} file(s)/folder(s) reference {parents} missing parent director{tail} - not repairable."
        ));
        text.push(
            "These entries exist in the catalog but their parent directory is gone. This \
             typically indicates severe directory corruption."
                .to_string(),
        );
        for (parent, entries) in &check.orphans {
            text.push(format!("Missing parent ID {parent}:"));
            text.extend(entries.iter().map(|e| format!("    {e}")));
        }
    }
    let shown: Vec<&String> = check
        .warnings
        .iter()
        .filter(|(_, is_debug)| !is_debug || debug)
        .map(|(line, _)| line)
        .collect();
    if !shown.is_empty() {
        text.push("Warnings:".to_string());
        text.extend(shown.into_iter().cloned());
    }
    if let Some(repair) = &r.repair {
        text.push("Repair Report:".to_string());
        text.extend(repair.applied.iter().map(|f| format!("  {f}")));
        text.extend(repair.failed.iter().map(|f| format!("  {f}")));
        if repair.unrepairable > 0 {
            text.push(format!(
                "{} error(s) could not be repaired (missing parent directories).",
                repair.unrepairable
            ));
        }
    }
    if !text.is_empty() {
        body.append(&text_view(&text.join("\n")));
    }
    if check.warnings.iter().any(|(_, d)| *d) {
        let toggle = gtk::CheckButton::with_label("Show debug messages");
        toggle.set_active(debug);
        let again = this.clone();
        toggle.connect_toggled(move |b| {
            again.show_debug.set(b.is_active());
            (again.send)(A::Focus(side));
        });
        body.append(&toggle);
    }
    if check.repairable && repairable {
        let repair = gtk::Button::with_label("Repair");
        repair.set_sensitive(edit);
        if !edit {
            repair.set_tooltip_text(Some("Turn on Edit Mode to repair the volume."));
        }
        let send = this.send.clone();
        repair.connect_clicked(move |_| send(pane(side, P::Repair)));
        body.append(&repair);
    }
    window("Filesystem Check Results", 520, 420, &body)
}

fn tree_dialog(tree: &TreeText, this: &Rc<Dialogs>) -> adw::Dialog {
    let body = page();
    let copy = gtk::Button::with_label("Copy to Clipboard");
    let text = tree.text.clone();
    copy.connect_clicked(move |b| b.clipboard().set_text(&plain(&text)));
    let save = gtk::Button::with_label("Save to File...");
    let send = this.send.clone();
    save.connect_clicked(move |_| send(pane(Side::Left, P::SaveTree)));
    let ids = gtk::CheckButton::with_label("Show IDs");
    ids.set_active(tree.ids);
    // A disc's tree has no filesystem ids.
    ids.set_visible(!this.plain_tree.get());
    let send = this.send.clone();
    ids.connect_toggled(move |b| {
        send(pane(Side::Left, P::ListTree { ids: b.is_active() }));
    });
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    row.append(&copy);
    row.append(&save);
    row.append(&ids);
    body.append(&row);
    body.append(&text_view(&tree.text));
    window("Tree View", 640, 500, &body)
}

fn budget_dialog(
    form: &BudgetForm,
    send: &SendFn,
) -> (adw::Dialog, (gtk::Label, gtk::Label, gtk::Button)) {
    let plan = *form.plan();
    let body = page();
    body.append(&wrapped(
        "SquashFS has no free space to report and no in-place write: saving an edit rebuilds \
         the whole image. Choose how large the result may be.",
    ));
    let room = match (plan.capacity, plan.headroom) {
        (Some(cap), Some(head)) => format!("{} ({} unused)", format_size(cap), format_size(head)),
        _ => "unbounded - this file is the filesystem".to_string(),
    };
    let facts = [
        format!("Image occupies   {}", format_size(plan.image_len)),
        format!(
            "File content     {} uncompressed",
            format_size(plan.content_len)
        ),
        format!("Compressed to    {}", plan.describe_ratio()),
        format!("Room available   {room}"),
    ];
    body.append(&wrapped(&facts.join("\n")));

    let budget = |a| A::Mode(ModeAction::Budget(a));
    let fit = gtk::CheckButton::with_label("Fit - accept whatever it becomes");
    let absolute = gtk::CheckButton::with_label("At most (MiB)");
    let grow = gtk::CheckButton::with_label("Grow by at most (MiB)");
    absolute.set_group(Some(&fit));
    grow.set_group(Some(&fit));
    match form.mode() {
        BudgetMode::Fit => fit.set_active(true),
        BudgetMode::Absolute => absolute.set_active(true),
        BudgetMode::Headroom => grow.set_active(true),
    }
    for (radio, mode) in [
        (&fit, BudgetMode::Fit),
        (&absolute, BudgetMode::Absolute),
        (&grow, BudgetMode::Headroom),
    ] {
        let send = send.clone();
        radio.connect_toggled(move |b| {
            if b.is_active() {
                send(budget(BudgetAction::SetMode(mode)));
            }
        });
    }
    let spin = |value: u64, set: fn(u64) -> BudgetAction| {
        let s = gtk::SpinButton::with_range(1.0, 1_000_000.0, 1.0);
        s.set_value(value as f64);
        let send = send.clone();
        s.connect_value_changed(move |s| send(budget(set(s.value() as u64))));
        s
    };
    let at_most = spin(form.absolute_mib(), BudgetAction::SetAbsoluteMib);
    let headroom = spin(form.headroom_mib(), BudgetAction::SetHeadroomMib);
    body.append(&fit);
    for (radio, value) in [(&absolute, &at_most), (&grow, &headroom)] {
        let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        row.append(radio);
        row.append(value);
        body.append(&row);
    }
    let ceiling = wrapped("");
    let over = toned("", "error");
    body.append(&ceiling);
    body.append(&over);
    let start = gtk::Button::with_label("Start editing");
    let to_mode = send.clone();
    start.connect_clicked(move |_| to_mode(budget(BudgetAction::Start)));
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    row.set_halign(gtk::Align::End);
    row.append(&start);
    row.append(&buttons(send, &[("Cancel", budget(BudgetAction::Cancel))]));
    body.append(&row);
    (
        window("SquashFS size budget", 480, -1, &body),
        (ceiling, over, start),
    )
}

fn password_dialog(apfs: bool, send: &SendFn) -> adw::Dialog {
    let body = page();
    if apfs {
        body.append(&wrapped("This APFS volume is encrypted with FileVault."));
        body.append(&wrapped(
            "Enter the volume password or personal recovery key.",
        ));
    } else {
        body.append(&wrapped("This image is password-protected."));
    }
    let entry = gtk::PasswordEntry::builder()
        .show_peek_icon(true)
        .activates_default(true)
        .build();
    body.append(&entry);
    let unlock = gtk::Button::with_label("Unlock");
    let to_mode = send.clone();
    let field = entry.clone();
    unlock.connect_clicked(move |_| {
        to_mode(A::Mode(ModeAction::Password(field.text().to_string())));
    });
    let close = gtk::Button::with_label("Close");
    let to_mode = send.clone();
    close.connect_clicked(move |_| to_mode(A::Mode(ModeAction::RequestClose)));
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    row.set_halign(gtk::Align::End);
    row.append(&close);
    row.append(&unlock);
    body.append(&row);
    let title = if apfs { "Passphrase" } else { "Password" };
    window(title, 420, -1, &body)
}

/// The journal viewer: a window of its own, which the pane hands over once and forgets.
pub fn present_journal(detail: &JournalDetail, host: &EffectHost) {
    let body = page();
    let title = wrapped("Volume journal");
    title.add_css_class("heading");
    body.append(&title);
    for line in journal_summary(detail) {
        body.append(&wrapped(&line));
    }
    for (tone, line) in journal_status(detail) {
        let class = match tone {
            JournalTone::Good => "success",
            JournalTone::Warning => "warning",
            JournalTone::Fault => "error",
        };
        body.append(&toned(&line, class));
    }
    let heading = wrapped("Transactions (newest first):");
    heading.add_css_class("heading");
    body.append(&heading);
    let rows = journal_txn_rows(detail);
    if rows.is_empty() {
        body.append(&wrapped("  (none)"));
    } else {
        let labels: Vec<_> = rows.iter().map(|(_, l)| plain(l)).collect();
        let labels: Vec<&str> = labels.iter().map(|l| l.as_ref()).collect();
        let selection = gtk::SingleSelection::builder()
            .model(&gtk::StringList::new(&labels))
            .autoselect(false)
            .can_unselect(true)
            .build();
        selection.set_selected(gtk::INVALID_LIST_POSITION);
        let factory = gtk::SignalListItemFactory::new();
        factory.connect_setup(|_, obj| {
            let item: &gtk::ListItem = obj.downcast_ref().expect("a list item");
            item.set_child(Some(&gtk::Label::builder().xalign(0.0).build()));
        });
        factory.connect_bind(|_, obj| {
            let item: &gtk::ListItem = obj.downcast_ref().expect("a list item");
            let line = item.item().and_downcast::<gtk::StringObject>();
            let label = item.child().and_downcast::<gtk::Label>();
            if let (Some(line), Some(label)) = (line, label) {
                label.set_label(&line.string());
            }
        });
        let list = gtk::ListView::new(Some(selection.clone()), Some(factory));
        let scroll = gtk::ScrolledWindow::builder()
            .child(&list)
            .min_content_height(160)
            .vexpand(true)
            .build();
        body.append(&scroll);
        let blocks = gtk::TextView::builder()
            .editable(false)
            .monospace(true)
            .cursor_visible(false)
            .build();
        let block_scroll = gtk::ScrolledWindow::builder()
            .child(&blocks)
            .min_content_height(140)
            .vexpand(true)
            .build();
        body.append(&block_scroll);
        let held = detail.clone();
        selection.connect_selected_notify(move |s| {
            let picked = rows.get(s.selected() as usize).map(|(idx, _)| *idx);
            let text = picked.and_then(|idx| journal_txn_blocks(&held, idx));
            let text = text.map(|(title, lines)| format!("{title}\n{}", lines.join("\n")));
            blocks
                .buffer()
                .set_text(&plain(text.as_deref().unwrap_or("")));
        });
    }
    host.present(&window("HFS+ Journal", 640, 520, &body));
}

/// Stage the editor's text; `Err` keeps the window open and says why.
pub type SaveText = Rc<dyn Fn(String, String, String, TextShape) -> Result<String, String>>;

/// The text editor window; conversion is the controller's, the buffer is the widget's.
pub fn present_editor(doc: TextDoc, host: &EffectHost, save: SaveText) {
    let original = doc.shape.ending;
    let shape = Rc::new(Cell::new(doc.shape));
    let body = page();
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 8);
    row.append(&gtk::Label::new(Some(&format!(
        "Encoding: {}",
        doc.shape.encoding.label()
    ))));
    row.append(&gtk::Label::new(Some("Line endings:")));
    let note = toned("", "warning");
    let endings = [
        (LineEnding::Lf, "LF (Unix)"),
        (LineEnding::CrLf, "CRLF (DOS)"),
        (LineEnding::Cr, "CR (classic Mac)"),
    ];
    let mut group: Option<gtk::CheckButton> = None;
    for (ending, label) in endings {
        let radio = gtk::CheckButton::with_label(label);
        radio.set_group(group.as_ref());
        group.get_or_insert_with(|| radio.clone());
        radio.set_active(doc.shape.ending == ending);
        let (held, shown) = (shape.clone(), note.clone());
        radio.connect_toggled(move |b| {
            if !b.is_active() {
                return;
            }
            let mut s = held.get();
            s.ending = ending;
            held.set(s);
            let text = (ending != original)
                .then(|| format!("Will convert {} -> {}", original.label(), ending.label()));
            shown.set_label(text.as_deref().unwrap_or(""));
        });
        row.append(&radio);
    }
    body.append(&row);
    if doc.shape.mixed_endings {
        body.append(&toned(
            "This file mixes line-ending styles; saving makes them all the same.",
            "warning",
        ));
    }
    body.append(&note);
    let view = gtk::TextView::builder().monospace(true).build();
    // Named for screen readers and the flow driver alike; the text view has no caption of its own.
    view.update_property(&[gtk::accessible::Property::Label("Text")]);
    view.buffer().set_text(&plain(&doc.text));
    let scroll = gtk::ScrolledWindow::builder()
        .child(&view)
        .vexpand(true)
        .min_content_height(300)
        .build();
    body.append(&scroll);
    let status = toned("", "error");
    body.append(&status);
    let save_button = gtk::Button::with_label("Save");
    save_button.set_tooltip_text(Some("Stage the new text; Apply Edits writes it."));
    let cancel = gtk::Button::with_label("Cancel");
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    row.append(&save_button);
    row.append(&cancel);
    body.append(&row);
    let dialog = window(&format!("Edit  {}", doc.name), 720, 520, &body);
    let closing = dialog.clone();
    cancel.connect_clicked(move |_| {
        closing.force_close();
    });
    let closing = dialog.clone();
    let (name, folder) = (doc.name, doc.folder);
    save_button.connect_clicked(move |_| {
        let buffer = view.buffer();
        let text = buffer.text(&buffer.start_iter(), &buffer.end_iter(), true);
        match save(name.clone(), folder.clone(), text.to_string(), shape.get()) {
            Ok(_) => {
                closing.force_close();
            }
            Err(why) => status.set_label(&plain(&why)),
        }
    });
    host.present(&dialog);
}

/// Edit Metadata over the pane's form: only the fields this entry carries are offered.
fn metadata_dialog(d: &PaneDialogs, form: &MetadataForm) -> (adw::Dialog, gtk::Label) {
    let e = &form.entry;
    let init = &form.initial;
    let body = page();
    body.append(&wrapped(&format!("Metadata for {}", e.name)));
    let grid = gtk::Grid::builder()
        .row_spacing(6)
        .column_spacing(8)
        .build();
    let mut line = 0;
    let mut field = |label: &str, value: &str, hint: &str| {
        let entry = gtk::Entry::builder()
            .text(plain(value).as_ref())
            .hexpand(true)
            .build();
        entry.set_placeholder_text(Some(hint));
        let name = gtk::Label::builder().label(label).xalign(0.0).build();
        grid.attach(&name, 0, line, 1, 1);
        grid.attach(&entry, 1, line, 1, 1);
        line += 1;
        entry
    };
    let mac = e.is_file() && e.type_code.is_some();
    let type_code = field("Type:", &init.type_code, "4 characters");
    let creator = field("Creator:", &init.creator, "4 characters");
    let dated = e.mac_dates.is_some();
    let modified = field("Modified:", &init.modified, "YYYY-MM-DD HH:MM:SS (UTC)");
    let created = field("Created:", &init.created, "YYYY-MM-DD HH:MM:SS (UTC)");
    let posix = e.mode.is_some();
    let mode = field("Permissions:", &init.mode, "octal, e.g. 644");
    let owner = field("Owner:", &init.owner, "uid:gid");
    for (w, on) in [
        (&type_code, mac),
        (&creator, mac),
        (&modified, dated),
        (&created, dated),
        (&mode, posix),
        (&owner, posix),
    ] {
        w.set_visible(on);
        if let Some(label) = w.prev_sibling() {
            label.set_visible(on);
        }
    }
    body.append(&grid);
    if mac {
        let list = gtk::Box::new(gtk::Orientation::Vertical, 0);
        let pop = gtk::Popover::builder()
            .child(
                &gtk::ScrolledWindow::builder()
                    .child(&list)
                    .min_content_height(320)
                    .min_content_width(380)
                    .build(),
            )
            .build();
        for (label, t, c) in known_type_creators() {
            let b = gtk::Button::with_label(&label);
            b.add_css_class("flat");
            let (ty, cr, p) = (type_code.clone(), creator.clone(), pop.clone());
            let code = |b: [u8; 4]| b.iter().map(|&c| c as char).collect::<String>();
            let (tt, ct) = (code(t), code(c));
            b.connect_clicked(move |_| {
                ty.set_text(&plain(&tt));
                cr.set_text(&plain(&ct));
                p.popdown();
            });
            list.append(&b);
        }
        let pick = gtk::MenuButton::builder()
            .label("Pick a known type...")
            .popover(&pop)
            .build();
        pick.set_halign(gtk::Align::Start);
        body.append(&pick);
    }
    let error = toned("", "error");
    error.set_visible(false);
    body.append(&error);
    let save = gtk::Button::with_label("Stage");
    save.add_css_class("suggested-action");
    let cancel = gtk::Button::with_label("Cancel");
    let (side, send) = (d.side, d.send.clone());
    let read = move || MetadataFields {
        type_code: type_code.text().to_string(),
        creator: creator.text().to_string(),
        modified: modified.text().to_string(),
        created: created.text().to_string(),
        mode: mode.text().to_string(),
        owner: owner.text().to_string(),
    };
    let s = send.clone();
    save.connect_clicked(move |_| s(pane(side, P::SubmitMetadata(Box::new(read())))));
    cancel.connect_clicked(move |_| send(pane(side, P::CancelMetadata)));
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    row.set_halign(gtk::Align::End);
    row.append(&cancel);
    row.append(&save);
    body.append(&row);
    (
        window(&format!("Edit Metadata{}", d.tag), 460, -1, &body),
        error,
    )
}
