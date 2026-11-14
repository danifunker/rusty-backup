//! The Convert screen over `controller::convert`; the Win32 shell's `convert.rs` is the reference.
//!
//! Slice 7c: a source (one image or a folder of them), the output, a format and extension for
//! each kind the scan found, the CHD options of each CHD profile in use, the hard-disk size and
//! floppy refit choices, the file list with a tick box per file, and Convert / Cancel. Start asks
//! Skip / Overwrite / Back through the controller's own question when outputs exist.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::{NSProgressIndicator, NSView};
use rusty_backup::controller::app::chd::ChdOptionsControl;
use rusty_backup::controller::app::{busy_elsewhere_notice, Notify};
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::convert::{
    floppy_size_choices, formats_for, hints, ids, kind_ids, ChdChoices, ChdProfile, ConvertAction,
    ConvertController, ExtraUnit, FloppySize, Format, ImageKind, Row, SizeChoice, ONLY_FILES,
};
use rusty_backup::controller::{Controller, Effect};

use crate::chd::{ChdControl, ChdIds, ChdKey};
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::sync::SyncGuard;
use crate::table::Table;

const PROFILES: [ChdProfile; 3] = [ChdProfile::Hd, ChdProfile::Cd, ChdProfile::Dvd];
const COLUMNS: [(&str, f64, bool); 4] = [
    ("File", 260.0, false),
    ("Kind", 90.0, false),
    ("Read as", 220.0, false),
    ("Result", 260.0, false),
];

fn profile_name(p: ChdProfile) -> &'static str {
    match p {
        ChdProfile::Hd => "Hard disk",
        ChdProfile::Cd => "CD",
        ChdProfile::Dvd => "DVD",
    }
}

/// A format's list label; one this build cannot write says so.
fn format_label(f: Format) -> String {
    match f.unavailable() {
        Some(_) => format!("{} (not in this version)", f.label()),
        None => f.label().to_string(),
    }
}

fn cells(r: &Row) -> Vec<String> {
    let read_as = match &r.note {
        Some(n) => format!("{} - {n}", r.detail),
        None => r.detail.clone(),
    };
    vec![r.name.clone(), r.kind.clone(), read_as, r.outcome.clone()]
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    BusyNote,
    Intro,
    SourceCaption,
    Source,
    PickFile,
    PickFolder,
    Clear,
    Recursive,
    OutputCaption,
    Output,
    OutputHint,
    PickOutput,
    Scan,
    ScanProgress,
    ScanError,
    KindTitle(usize),
    FormatCaption(usize),
    Format(usize),
    ExtCaption(usize),
    Extension(usize),
    KindHelp(usize),
    ChdTitle(usize),
    ChdHd(ChdKey),
    ChdCd(ChdKey),
    ChdDvd(ChdKey),
    MultiBin,
    SizeCaption,
    Sizing,
    Extra,
    Unit,
    MeasureSlow,
    FloppyCaption,
    FloppySize,
    FloppyHelp,
    Verify,
    Summary,
    All,
    None,
    List,
    Problem,
    Run,
    RunProgress,
    Start,
    Cancel,
    Outcome,
}

pub struct ConvertScreen {
    ctrl: RefCell<ConvertController>,
    host: EffectHost,
    busy: Cell<Option<&'static str>>,
    sync: SyncGuard,
    me: Weak<ConvertScreen>,
    body: Retained<NSView>,
    form: Form<K>,
    chd: Vec<(ChdProfile, ChdControl<K>)>,
    list: Table,
    listed: RefCell<Vec<Row>>,
}

fn progress_of(form: &Form<K>, k: K) -> Retained<NSProgressIndicator> {
    form.v(k)
        .downcast::<NSProgressIndicator>()
        .expect("a progress bar")
}

impl ConvertScreen {
    pub fn build(
        mtm: MainThreadMarker,
        body: Retained<NSView>,
        notify: Arc<dyn Notify>,
        host: EffectHost,
    ) -> Rc<ConvertScreen> {
        let screen = Rc::new_cyclic(|me: &Weak<ConvertScreen>| {
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
            let strings = |v: Vec<&str>| v.into_iter().map(str::to_string).collect::<Vec<_>>();
            f.wrap(K::BusyNote, "");
            f.wrap(K::Intro, hints::INTRO);
            f.label(K::SourceCaption, "Source:");
            f.label(K::Source, "");
            f.button(K::PickFile, "Choose Image...", ids::SOURCE_FILE);
            f.button(K::PickFolder, "Choose Folder...", ids::SOURCE_FOLDER);
            f.button(K::Clear, "Clear", ids::CLEAR);
            f.check(K::Recursive, "Include subfolders", ids::RECURSIVE);
            f.label(K::OutputCaption, "");
            f.label(K::Output, "");
            f.label(K::OutputHint, "");
            f.button(K::PickOutput, "Choose...", ids::OUTPUT);
            f.label(K::Scan, "");
            f.progress(K::ScanProgress);
            f.wrap(K::ScanError, "");
            for (i, kind) in ImageKind::ALL.iter().enumerate() {
                let (format_id, ext_id) = kind_ids(*kind);
                let labels: Vec<String> = formats_for(*kind)
                    .iter()
                    .map(|x| format_label(*x))
                    .collect();
                f.label(K::KindTitle(i), kind.plural());
                f.label(K::FormatCaption(i), "Format:");
                f.popup(K::Format(i), &labels, format_id);
                f.label(K::ExtCaption(i), "Extension:");
                f.field(K::Extension(i), ext_id, "Extension", false);
                f.label(K::KindHelp(i), "");
            }
            let wraps: [fn(ChdKey) -> K; 3] = [K::ChdHd, K::ChdCd, K::ChdDvd];
            let chd = PROFILES
                .iter()
                .zip(wraps)
                .enumerate()
                .map(|(i, (p, wrap))| {
                    f.label(K::ChdTitle(i), &format!("{} CHD options", profile_name(*p)));
                    let control = ChdControl::build(
                        f,
                        wrap,
                        ChdIds {
                            mode: ids::CHD_MODE,
                            hunk: ids::CHD_HUNK,
                            codec: ids::CHD_CODEC,
                        },
                        ChdOptionsControl::new(*p),
                        *p,
                    );
                    (*p, control)
                })
                .collect();
            f.check(K::MultiBin, "One .bin per track", ids::MULTI_BIN);
            f.label(K::SizeCaption, "Hard disk size:");
            f.popup(
                K::Sizing,
                &strings(SizeChoice::ALL.iter().map(|c| c.label()).collect()),
                ids::SIZING,
            );
            f.field(K::Extra, ids::EXTRA_AMOUNT, "Extra space", false);
            f.popup(
                K::Unit,
                &strings(ExtraUnit::ALL.iter().map(|u| u.label()).collect()),
                ids::EXTRA_UNIT,
            );
            f.check(
                K::MeasureSlow,
                "Measure slow filesystems too",
                ids::MEASURE_SLOW,
            );
            f.label(K::FloppyCaption, "Floppy size:");
            let floppy: Vec<String> = floppy_size_choices().iter().map(|x| x.label()).collect();
            f.popup(K::FloppySize, &floppy, ids::FLOPPY_SIZE);
            f.wrap(K::FloppyHelp, "");
            f.check(K::Verify, "Verify after converting", ids::VERIFY);
            f.label(K::Summary, "");
            f.button(K::All, "Select All", ids::SELECT_ALL);
            f.button(K::None, "Select None", ids::SELECT_NONE);
            let list = Table::new(mtm, ids::FILES, &COLUMNS);
            let tick = me.clone();
            list.add_checks(
                mtm,
                Rc::new(move |i, on| {
                    if let Some(s) = tick.upgrade() {
                        s.later(ConvertAction::SetSelected(i, on));
                    }
                }),
            );
            f.add(K::List, &list.scroll);
            f.label(K::Problem, "");
            f.label(K::Run, "");
            f.progress(K::RunProgress);
            f.button(K::Start, "Convert", ids::START);
            f.button(K::Cancel, "Cancel", ids::CANCEL);
            f.wrap(K::Outcome, "");
            ConvertScreen {
                ctrl: RefCell::new(ConvertController::new(notify)),
                host,
                busy: Cell::new(None),
                sync: SyncGuard::default(),
                me: me.clone(),
                body,
                form,
                chd,
                list,
                listed: RefCell::default(),
            }
        });
        screen.refresh();
        screen
    }

    pub fn set_busy(&self, busy: Option<&'static str>) {
        self.busy.set(busy);
    }

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    /// A file or folder another screen hands over.
    pub fn open(&self, path: PathBuf) {
        self.apply(ConvertAction::SetSource(path));
    }

    pub fn apply(&self, action: ConvertAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// Apply `action` once the current handler returns: the controls may be rebuilt by it.
    fn later(&self, action: ConvertAction) {
        let me = self.me.clone();
        crate::app::later(Box::new(move || {
            if let Some(s) = me.upgrade() {
                s.apply(action);
            }
        }));
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(s) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => ConvertAction::Picked { purpose, path },
                Reply::Chose { purpose, choice } => ConvertAction::Chose {
                    purpose,
                    index: choice,
                },
                // Convert asks for files, folders and one choice; no text, no yes/no.
                Reply::Confirmed { .. } | Reply::Answered { .. } | Reply::CloseScreen => return,
            };
            s.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let log = self.ctrl.borrow_mut().take_log();
        crate::app::append_log(log);
        // Off screen nothing needs it, and a job wakes this many times a second.
        if !self.body.isHiddenOrHasHiddenAncestor() {
            self.refresh();
        }
    }

    fn used_profiles(&self) -> Vec<ChdProfile> {
        self.ctrl
            .borrow()
            .chd_profiles()
            .into_iter()
            .map(|(_, p)| p)
            .collect()
    }

    fn chd_choices(&self) -> ChdChoices {
        let mut out = ChdChoices::default();
        let used = self.used_profiles();
        for (p, control) in &self.chd {
            if !used.contains(p) {
                continue;
            }
            let opts = Some(control.choice().0);
            match p {
                ChdProfile::Hd => out.hd = opts,
                ChdProfile::Cd => out.cd = opts,
                ChdProfile::Dvd => out.dvd = opts,
            }
        }
        out
    }

    fn chd_ok(&self) -> bool {
        let used = self.used_profiles();
        self.chd
            .iter()
            .all(|(p, c)| !used.contains(p) || c.is_valid())
    }

    /// Start's whole rule: asked again at the click, not only when the button was last drawn.
    fn may_start(&self) -> bool {
        self.ctrl.borrow().can_start() && self.chd_ok() && self.busy.get().is_none()
    }

    pub fn refresh(&self) {
        let can_start = self.may_start();
        self.sync.run(|| self.show(can_start));
        self.relayout();
    }

    fn show(&self, can_start: bool) {
        let f = &self.form;
        let c = self.ctrl.borrow();
        let notice = self.busy.get().map(busy_elsewhere_notice);
        f.line(K::BusyNote, notice.as_deref(), Some(Tone::Warning));
        f.set_text(K::Source, &ConvertController::path_text(c.source()));
        f.show(K::Recursive, c.source_is_folder());
        f.set_check(K::Recursive, c.recursive());
        let has = c.source().is_some();
        f.show(K::Clear, has);
        for k in [K::OutputCaption, K::Output, K::OutputHint, K::PickOutput] {
            f.show(k, has);
        }
        f.set_text(K::OutputCaption, &format!("{}:", c.output_label()));
        f.set_text(K::Output, &ConvertController::path_text(c.output()));
        f.set_text(K::OutputHint, c.output_hint());
        let scan = c.scan_snapshot();
        let (line, pct) = scan
            .as_ref()
            .map(crate::inspect::job_line)
            .unwrap_or_default();
        f.set_text(K::Scan, &line);
        f.show(K::Scan, scan.is_some());
        crate::ui::set_progress(&progress_of(f, K::ScanProgress), scan.is_some(), pct);
        f.line(K::ScanError, c.scan_error(), Some(Tone::Error));

        let shown = c.shown_kinds();
        for (i, kind) in ImageKind::ALL.iter().enumerate() {
            let on = shown.contains(kind);
            for k in [
                K::KindTitle(i),
                K::FormatCaption(i),
                K::Format(i),
                K::ExtCaption(i),
                K::Extension(i),
                K::KindHelp(i),
            ] {
                f.show(k, on);
            }
            let format = c.format(*kind);
            let at = formats_for(*kind).iter().position(|x| *x == format);
            f.select(K::Format(i), at.unwrap_or(0));
            f.set_text(K::Extension(i), c.extension(*kind));
            let help = match format.unavailable() {
                Some(why) => format!("Not in this version of Rusty Backup: {why}"),
                None => format.hint().to_string(),
            };
            f.set_text(K::KindHelp(i), &help);
        }
        let used: Vec<ChdProfile> = c.chd_profiles().into_iter().map(|(_, p)| p).collect();
        for (i, (p, control)) in self.chd.iter().enumerate() {
            let on = used.contains(p);
            f.show(K::ChdTitle(i), on);
            control.set_visible(f, on);
        }
        f.show(K::MultiBin, c.multi_bin_shown());
        f.set_check(K::MultiBin, c.multi_bin());

        let hd = shown.contains(&ImageKind::HardDisk);
        for k in [K::SizeCaption, K::Sizing, K::MeasureSlow] {
            f.show(k, hd);
        }
        let at = SizeChoice::ALL.iter().position(|s| *s == c.size_choice());
        f.select(K::Sizing, at.unwrap_or(0));
        let extra = hd && c.size_choice() == SizeChoice::Extra;
        f.show(K::Extra, extra);
        f.show(K::Unit, extra);
        f.set_text(K::Extra, &c.extra_amount().to_string());
        let at = ExtraUnit::ALL.iter().position(|u| *u == c.extra_unit());
        f.select(K::Unit, at.unwrap_or(0));
        f.enable(K::MeasureSlow, c.measure_slow_enabled());
        f.set_check(K::MeasureSlow, c.measure_slow());

        let floppy = shown.contains(&ImageKind::Floppy);
        for k in [K::FloppyCaption, K::FloppySize, K::FloppyHelp] {
            f.show(k, floppy);
        }
        let sizes = floppy_size_choices();
        let at = sizes.iter().position(|x| *x == c.floppy_size());
        f.select(K::FloppySize, at.unwrap_or(0));
        let help = match c.floppy_size() {
            FloppySize::Same => hints::FLOPPY_SIZE.to_string(),
            _ => format!("{} {ONLY_FILES}", hints::FLOPPY_SIZE),
        };
        f.set_text(K::FloppyHelp, &help);
        f.show(K::Verify, !shown.is_empty());
        f.set_check(K::Verify, c.verify());

        let rows = c.rows();
        for k in [K::Summary, K::All, K::None, K::List] {
            f.show(k, !rows.is_empty());
        }
        f.set_text(K::Summary, &c.summary());
        if *self.listed.borrow() != rows {
            self.list.set_rows(rows.iter().map(cells).collect());
            self.list
                .set_checks(rows.iter().map(|r| r.selected && !r.skipped).collect());
            *self.listed.borrow_mut() = rows;
        }

        let running = c.is_running();
        f.show(K::Start, !running);
        f.enable(K::Start, can_start);
        f.set_title(K::Start, &c.start_label());
        f.show(K::Cancel, running);
        let problem = c.start_problem().filter(|_| !running);
        f.line(K::Problem, problem.as_deref(), None);
        let run = c.strip();
        f.set_text(K::Run, run.as_ref().map_or("", |(_, t)| t.as_str()));
        f.show(K::Run, run.is_some());
        let pct = run
            .as_ref()
            .map(|(x, _)| (x.clamp(0.0, 1.0) * 100.0) as u32);
        crate::ui::set_progress(&progress_of(f, K::RunProgress), run.is_some(), pct);
        let (text, error) = c.outcome().unzip();
        f.line(
            K::Outcome,
            text.as_deref(),
            error.filter(|e| *e).map(|_| Tone::Error),
        );
        let free = !running && self.busy.get().is_none();
        for k in [
            K::PickFile,
            K::PickFolder,
            K::Clear,
            K::Recursive,
            K::PickOutput,
        ] {
            f.enable(k, free);
        }
    }

    pub fn relayout(&self) {
        let f = &self.form;
        let row_h = Size::Fixed(layout::ROW);
        let fixed = |k: K, w: i32| Node::leaf(k, Size::Fixed(w), row_h);
        let pop = |k: K, w: i32| Node::leaf(k, Size::Fixed(w), Size::Fixed(layout::ROW + 2));
        let stretch = |k: K| Node::leaf(k, Size::Stretch(1), row_h);
        let lines =
            |k: K, n: i32| Node::leaf(k, Size::Stretch(1), Size::Fixed((layout::ROW - 6) * n + 4));
        let cap = 120;
        let mut col = vec![
            lines(K::BusyNote, 1),
            lines(K::Intro, 2),
            Node::row(vec![
                fixed(K::SourceCaption, cap),
                stretch(K::Source),
                Node::button(K::PickFile, 125),
                Node::button(K::PickFolder, 125),
                Node::button(K::Clear, 65),
            ]),
            Node::row(vec![Node::Space(Size::Fixed(cap)), stretch(K::Recursive)]),
            Node::row(vec![
                fixed(K::OutputCaption, cap),
                stretch(K::Output),
                Node::button(K::PickOutput, 90),
            ]),
            Node::row(vec![Node::Space(Size::Fixed(cap)), stretch(K::OutputHint)]),
            Node::row(vec![
                stretch(K::Scan),
                Node::leaf(
                    K::ScanProgress,
                    Size::Fixed(180),
                    Size::Fixed(layout::ROW - 4),
                ),
            ]),
            lines(K::ScanError, 1),
        ];
        for i in 0..ImageKind::ALL.len() {
            col.push(Node::row(vec![
                fixed(K::KindTitle(i), cap),
                fixed(K::FormatCaption(i), 60),
                pop(K::Format(i), 260),
                fixed(K::ExtCaption(i), 75),
                fixed(K::Extension(i), 80),
                stretch(K::KindHelp(i)),
            ]));
        }
        for (i, (_, control)) in self.chd.iter().enumerate() {
            if f.shown(K::ChdTitle(i)) {
                col.push(stretch(K::ChdTitle(i)));
                col.extend(control.rows());
            }
        }
        col.push(stretch(K::MultiBin));
        col.push(Node::row(vec![
            fixed(K::SizeCaption, cap),
            pop(K::Sizing, 220),
            fixed(K::Extra, 70),
            pop(K::Unit, 80),
            stretch(K::MeasureSlow),
        ]));
        col.push(Node::row(vec![
            fixed(K::FloppyCaption, cap),
            pop(K::FloppySize, 220),
            lines(K::FloppyHelp, 2),
        ]));
        col.push(stretch(K::Verify));
        if f.shown(K::List) {
            col.push(Node::row(vec![
                stretch(K::Summary),
                Node::button(K::All, 95),
                Node::button(K::None, 100),
            ]));
            col.push(Node::leaf(K::List, Size::Stretch(1), Size::Stretch(1)));
        } else {
            col.push(Node::Space(Size::Stretch(1)));
        }
        col.push(lines(K::Outcome, 1));
        let mut foot = vec![Node::button(K::Start, 145), Node::button(K::Cancel, 90)];
        if f.shown(K::Run) {
            foot.push(Node::leaf(
                K::RunProgress,
                Size::Fixed(200),
                Size::Fixed(layout::ROW - 4),
            ));
            foot.push(stretch(K::Run));
        } else {
            foot.push(stretch(K::Problem));
        }
        col.push(Node::row(foot));
        f.place_page(Node::inset(Node::column(col), layout::MARGIN));
    }

    fn chd_of(&self, k: K) -> Option<(usize, ChdKey)> {
        match k {
            K::ChdHd(c) => Some((0, c)),
            K::ChdCd(c) => Some((1, c)),
            K::ChdDvd(c) => Some((2, c)),
            _ => None,
        }
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let f = &self.form;
        if let Some((i, c)) = self.chd_of(k) {
            if self.chd[i].1.handle(f, c) {
                self.refresh();
            }
            return;
        }
        let picked = |list_len: usize| f.picked(k).filter(|i| *i < list_len);
        let action = match k {
            K::Format(i) => {
                let Some(kind) = ImageKind::ALL.get(i) else {
                    return;
                };
                let formats = formats_for(*kind);
                match picked(formats.len()) {
                    Some(n) => ConvertAction::SetFormat(*kind, formats[n]),
                    None => return,
                }
            }
            K::Extension(i) => match ImageKind::ALL.get(i) {
                Some(kind) => ConvertAction::SetExtension(*kind, f.text(k)),
                None => return,
            },
            K::Sizing => match picked(SizeChoice::ALL.len()) {
                Some(n) => ConvertAction::SetSizing(SizeChoice::ALL[n]),
                None => return,
            },
            K::Unit => match picked(ExtraUnit::ALL.len()) {
                Some(n) => ConvertAction::SetExtraUnit(ExtraUnit::ALL[n]),
                None => return,
            },
            K::FloppySize => {
                let sizes = floppy_size_choices();
                match picked(sizes.len()) {
                    Some(n) => ConvertAction::SetFloppySize(sizes[n]),
                    None => return,
                }
            }
            K::Extra => match f.text(k).trim().parse() {
                Ok(n) => ConvertAction::SetExtraAmount(n),
                Err(_) => return,
            },
            K::PickFile => ConvertAction::PickSourceFile,
            K::PickFolder => ConvertAction::PickSourceFolder,
            K::Clear => ConvertAction::Clear,
            K::Recursive => ConvertAction::SetRecursive(f.checked(k)),
            K::PickOutput => ConvertAction::PickOutput,
            K::MultiBin => ConvertAction::SetMultiBin(f.checked(k)),
            K::MeasureSlow => ConvertAction::SetMeasureSlow(f.checked(k)),
            K::Verify => ConvertAction::SetVerify(f.checked(k)),
            K::All => ConvertAction::SelectAll,
            K::None => ConvertAction::SelectNone,
            K::Start if self.may_start() => ConvertAction::Start(self.chd_choices()),
            K::Cancel => ConvertAction::Cancel,
            _ => return,
        };
        self.later(action);
    }
}
