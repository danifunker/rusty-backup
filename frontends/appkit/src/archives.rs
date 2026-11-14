//! The Archives screen over `controller::archives`; the Win32 shell's `archives.rs` is the
//! reference.
//!
//! Slice 6: the archive (a pull-down of recent ones beside Open File...), its entries in a list
//! with a tick box each, the extraction format, Extract All / Extract Selected, and Mount for a
//! payload that is itself a disk image, which goes to Inspect. The entries are copied into the
//! list once per archive; a tick there is the only change that comes back.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::{NSProgressIndicator, NSView};
use rusty_backup::controller::app::{busy_elsewhere_notice, Notify, RecentHandle, RecentMode};
use rusty_backup::controller::archives::{
    hints, human_size, ids, mount_label, row_label, ArchiveEntryRow, ArchivesAction,
    ArchivesController, ExtractOutcome, ForkFormat, HostRequest,
};
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::{Controller, Effect};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::sync::SyncGuard;
use crate::table::Table;

const COLUMNS: [(&str, f64, bool); 4] = [
    ("Name", 360.0, false),
    ("Type/Creator", 120.0, false),
    ("Size", 90.0, true),
    ("Method", 120.0, false),
];

fn cells(r: &ArchiveEntryRow) -> Vec<String> {
    match r.is_dir {
        true => vec![row_label(r), String::new(), String::new(), String::new()],
        false => vec![
            row_label(r),
            format!("{} / {}", r.type_code, r.creator),
            human_size(r.size),
            r.method.to_string(),
        ],
    }
}

fn outcome_text(outcome: &ExtractOutcome) -> (String, bool) {
    match outcome {
        Ok((stats, dest)) => {
            let skipped = match stats.skipped {
                0 => String::new(),
                n => format!(", {n} skipped"),
            };
            let line = format!(
                "Extracted {} files to {}{skipped}",
                stats.files,
                dest.display()
            );
            (line, false)
        }
        Err(e) => (format!("Extraction failed: {}", e.message), true),
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    BusyNote,
    Intro,
    SourceCaption,
    Source,
    RecentItem(usize),
    Browse,
    Close,
    Job,
    JobProgress,
    Notes,
    Count,
    List,
    FormatCaption,
    Format,
    ExtractAll,
    ExtractSelected,
    Mount,
    Run,
    RunProgress,
}

pub struct ArchivesScreen {
    ctrl: RefCell<ArchivesController>,
    host: EffectHost,
    recent: RecentHandle,
    recent_files: RefCell<Vec<String>>,
    busy: Cell<Option<&'static str>>,
    sync: SyncGuard,
    me: Weak<ArchivesScreen>,
    body: Retained<NSView>,
    form: Form<K>,
    list: Table,
    /// A mounted payload's temporary folder lives as long as the screen.
    guards: RefCell<Vec<Box<dyn std::any::Any>>>,
    mount_asked: RefCell<Option<PathBuf>>,
    /// The archive the list was filled for, so its rows are copied once.
    listed: RefCell<Option<(PathBuf, usize)>>,
}

fn progress_of(form: &Form<K>, k: K) -> Retained<NSProgressIndicator> {
    form.v(k)
        .downcast::<NSProgressIndicator>()
        .expect("a progress bar")
}

impl ArchivesScreen {
    pub fn build(
        mtm: MainThreadMarker,
        body: Retained<NSView>,
        notify: Arc<dyn Notify>,
        recent: RecentHandle,
        host: EffectHost,
    ) -> Rc<ArchivesScreen> {
        let screen = Rc::new_cyclic(|me: &Weak<ArchivesScreen>| {
            let fire = me.clone();
            let form = Form::new(
                mtm,
                body.clone(),
                Rc::new(move |k| {
                    if let Some(s) = fire.upgrade() {
                        s.fired(k);
                    }
                }),
            );
            let f = &form;
            f.wrap(K::BusyNote, "");
            f.wrap(K::Intro, hints::INTRO);
            f.label(K::SourceCaption, "Archive:");
            f.pulldown(K::Source, ids::SOURCE);
            f.button(K::Browse, "Open File...", ids::BROWSE);
            f.button(K::Close, "Close", ids::CLOSE);
            f.label(K::Job, "");
            f.progress(K::JobProgress);
            f.wrap(K::Notes, "");
            f.label(K::Count, "");
            let list = Table::new(mtm, ids::ENTRIES, &COLUMNS);
            let tick = me.clone();
            list.add_checks(
                mtm,
                Rc::new(move |i, on| {
                    if let Some(s) = tick.upgrade() {
                        s.ticked(i, on);
                    }
                }),
            );
            f.add(K::List, &list.scroll);
            let formats: Vec<String> = ForkFormat::ALL
                .iter()
                .map(|x| x.label().to_string())
                .collect();
            f.label(K::FormatCaption, "Extract as:");
            f.popup(K::Format, &formats, ids::FORMAT);
            f.button(K::ExtractAll, "Extract All...", ids::EXTRACT_ALL);
            f.button(
                K::ExtractSelected,
                "Extract Selected... (0)",
                ids::EXTRACT_SELECTED,
            );
            f.button(K::Mount, "Mount in new Inspect tab", ids::MOUNT);
            f.label(K::Run, "");
            f.progress(K::RunProgress);
            ArchivesScreen {
                ctrl: RefCell::new(ArchivesController::new(notify)),
                host,
                recent_files: RefCell::new(recent.list(RecentMode::Archives)),
                recent,
                busy: Cell::new(None),
                sync: SyncGuard::default(),
                me: me.clone(),
                body,
                form,
                list,
                guards: RefCell::default(),
                mount_asked: RefCell::default(),
                listed: RefCell::default(),
            }
        });
        screen.refresh();
        screen
    }

    /// Settings cleared the recent files: the picker forgets its copy too.
    pub fn forget_recents(&self) {
        self.recent_files.borrow_mut().clear();
    }

    pub fn set_busy(&self, busy: Option<&'static str>) {
        self.busy.set(busy);
    }

    /// Open `path` as the archive (Inspect's hand-off, or a drop).
    pub fn open_path(&self, path: PathBuf) {
        self.apply(ArchivesAction::Open(path));
    }

    /// The payload Mount decoded, for Inspect, once.
    pub fn take_mount(&self) -> Option<PathBuf> {
        self.mount_asked.borrow_mut().take()
    }

    pub fn apply(&self, action: ArchivesAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(s) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => ArchivesAction::Picked { purpose, path },
                // Archives asks only for files and folders.
                Reply::Confirmed { .. }
                | Reply::Answered { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => return,
            };
            s.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let (requests, log) = {
            let mut ctrl = self.ctrl.borrow_mut();
            (ctrl.take_requests(), ctrl.take_log())
        };
        for request in requests {
            match request {
                HostRequest::RememberRecent(path) => {
                    let list = self
                        .recent
                        .remember(RecentMode::Archives, &path.display().to_string());
                    *self.recent_files.borrow_mut() = list;
                }
                HostRequest::OpenInInspect { path, guard } => {
                    self.guards.borrow_mut().push(Box::new(guard));
                    *self.mount_asked.borrow_mut() = Some(path);
                }
            }
        }
        crate::app::append_log(log);
        // Off screen nothing needs it, and a job wakes this many times a second.
        if !self.body.isHiddenOrHasHiddenAncestor() {
            self.refresh();
        }
    }

    fn ticked(&self, i: usize, on: bool) {
        if self.sync.active() || self.ctrl.borrow().is_selected(i) == on {
            return;
        }
        let me = self.me.clone();
        // The list reloads with the new ticks: act once its click handler returns.
        crate::app::later(Box::new(move || {
            if let Some(s) = me.upgrade() {
                s.apply(ArchivesAction::SetSelected(i, on));
            }
        }));
    }

    /// Copy the archive's rows into the list, once per archive.
    fn fill_list(&self) {
        let ctrl = self.ctrl.borrow();
        let key = ctrl.path().map(|p| (p.to_path_buf(), ctrl.rows().len()));
        if *self.listed.borrow() == key {
            return;
        }
        self.list.set_rows(ctrl.rows().iter().map(cells).collect());
        *self.listed.borrow_mut() = key;
    }

    pub fn refresh(&self) {
        self.sync.run(|| {
            self.fill_list();
            self.show();
        });
        self.relayout();
    }

    fn show(&self) {
        let f = &self.form;
        let ctrl = self.ctrl.borrow();
        let notice = self.busy.get().map(busy_elsewhere_notice);
        f.line(K::BusyNote, notice.as_deref(), Some(Tone::Warning));
        let extracting = ctrl.is_extracting();
        let path = ctrl.path().map(|p| p.display().to_string());
        let recent = self.recent_files.borrow();
        let mut items: Vec<(String, Option<K>, bool)> = recent
            .iter()
            .enumerate()
            .map(|(i, p)| (p.clone(), Some(K::RecentItem(i)), true))
            .collect();
        if items.is_empty() {
            items.push(("No recent archives".to_string(), None, false));
        }
        f.set_pulldown(
            K::Source,
            path.as_deref().unwrap_or("No archive selected"),
            &items,
        );
        f.enable(K::Source, !extracting);
        f.enable(K::Browse, !extracting);
        f.show(K::Close, path.is_some());
        f.enable(K::Close, !extracting);
        let job = ctrl.load_snapshot();
        let (text, pct) = job
            .as_ref()
            .map(crate::inspect::job_line)
            .unwrap_or_default();
        f.set_text(K::Job, &text);
        f.show(K::Job, job.is_some());
        crate::ui::set_progress(&progress_of(f, K::JobProgress), job.is_some(), pct);

        let mut notes = Vec::new();
        let mut tone = None;
        if let Some(e) = ctrl.load_error() {
            notes.push(format!("Error: {}", e.message));
            tone = Some(Tone::Error);
        }
        if let Some(outcome) = ctrl.last_extract() {
            let (text, failed) = outcome_text(outcome);
            notes.push(text);
            if failed {
                tone = Some(Tone::Error);
            }
        }
        let notes = notes.join("\n");
        f.line(
            K::Notes,
            Some(notes.as_str()).filter(|n| !n.is_empty()),
            tone,
        );

        let loaded = ctrl.is_loaded();
        f.set_text(K::Count, &ctrl.summary());
        for k in [
            K::Count,
            K::List,
            K::FormatCaption,
            K::Format,
            K::ExtractAll,
            K::ExtractSelected,
        ] {
            f.show(k, loaded);
        }
        let ticks = (0..ctrl.rows().len())
            .map(|i| ctrl.is_selected(i))
            .collect();
        self.list.set_checks(ticks);
        let at = ForkFormat::ALL.iter().position(|x| *x == ctrl.format());
        f.select(K::Format, at.unwrap_or(0));
        f.enable(K::ExtractAll, !extracting);
        let selected = ctrl.selected_file_count();
        f.enable(K::ExtractSelected, !extracting && selected > 0);
        f.set_title(
            K::ExtractSelected,
            &format!("Extract Selected... ({selected})"),
        );
        let mountable = ctrl.mountable();
        f.show(K::Mount, loaded && mountable.is_some());
        f.enable(K::Mount, !extracting);
        if let Some(kind) = mountable {
            f.set_title(K::Mount, &mount_label(kind));
        }
        let run = ctrl.extract_progress();
        let text = run
            .as_ref()
            .map(|(done, total, current)| format!("{done} / {total}  {current}"));
        f.set_text(K::Run, text.as_deref().unwrap_or(""));
        f.show(K::Run, run.is_some());
        let pct = run
            .as_ref()
            .and_then(|(d, t, _)| (*t > 0).then(|| (d * 100 / t).min(100) as u32));
        crate::ui::set_progress(&progress_of(f, K::RunProgress), run.is_some(), pct);
    }

    pub fn relayout(&self) {
        let f = &self.form;
        let row_h = Size::Fixed(layout::ROW);
        let fixed = |k: K, w: i32| Node::leaf(k, Size::Fixed(w), row_h);
        let pop = |k: K, w: Size| Node::leaf(k, w, Size::Fixed(layout::ROW + 2));
        let stretch = |k: K| Node::leaf(k, Size::Stretch(1), row_h);
        let lines = |k: K, n: i32| {
            Node::leaf(
                k,
                Size::Stretch(1),
                Size::Fixed((layout::ROW - 6) * n.max(1) + 4),
            )
        };
        let mut col = Vec::new();
        if f.shown(K::BusyNote) {
            col.push(lines(K::BusyNote, 1));
        }
        col.push(lines(K::Intro, 2));
        let mut source = vec![
            fixed(K::SourceCaption, 70),
            pop(K::Source, Size::Stretch(1)),
            Node::button(K::Browse, 115),
        ];
        if f.shown(K::Close) {
            source.push(Node::button(K::Close, 75));
        }
        col.push(Node::row(source));
        if f.shown(K::Job) {
            col.push(Node::row(vec![
                stretch(K::Job),
                Node::leaf(
                    K::JobProgress,
                    Size::Fixed(180),
                    Size::Fixed(layout::ROW - 4),
                ),
            ]));
        }
        if f.shown(K::Notes) {
            let n = f.text(K::Notes).lines().count() as i32;
            col.push(lines(K::Notes, n));
        }
        if f.shown(K::List) {
            col.push(stretch(K::Count));
            col.push(Node::leaf(K::List, Size::Stretch(1), Size::Stretch(1)));
            let mut row = vec![
                fixed(K::FormatCaption, 80),
                pop(K::Format, Size::Fixed(220)),
                Node::button(K::ExtractAll, 115),
                Node::button(K::ExtractSelected, 175),
            ];
            if f.shown(K::Mount) {
                row.push(Node::button(K::Mount, 260));
            }
            row.push(Node::Space(Size::Stretch(1)));
            col.push(Node::row(row));
        } else {
            col.push(Node::Space(Size::Stretch(1)));
        }
        if f.shown(K::Run) {
            col.push(Node::row(vec![
                Node::leaf(
                    K::RunProgress,
                    Size::Fixed(200),
                    Size::Fixed(layout::ROW - 4),
                ),
                stretch(K::Run),
            ]));
        }
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        f.place_page(tree);
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let action = match k {
            K::Format => self
                .form
                .picked(k)
                .and_then(|i| ForkFormat::ALL.get(i))
                .map(|x| ArchivesAction::SetFormat(*x)),
            K::RecentItem(i) => self
                .recent_files
                .borrow()
                .get(i)
                .map(|p| ArchivesAction::Open(PathBuf::from(p))),
            K::Browse => Some(ArchivesAction::Pick),
            K::Close => Some(ArchivesAction::Close),
            K::ExtractAll => Some(ArchivesAction::ExtractAll),
            K::ExtractSelected => Some(ArchivesAction::ExtractSelected),
            K::Mount => Some(ArchivesAction::Mount),
            _ => None,
        };
        if let Some(a) = action {
            let me = self.me.clone();
            crate::app::later(Box::new(move || {
                if let Some(s) = me.upgrade() {
                    s.apply(a);
                }
            }));
        }
    }
}
