//! The Build Disk wizard over `controller::build_disk`; the Win32 shell's `build_disk.rs` is the
//! reference.
//!
//! Slice 5: the steps are the controller's, and the screen shows the current one's controls with
//! Back / Next / Create below them. Kind, Output, Disk, Review and Build live here; the Partitions
//! and Options steps, whose rows are built from the plan, are `build_rows.rs`.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::{NSProgressIndicator, NSView};
use rusty_backup::controller::app::{busy_elsewhere_notice, no_devices_hint, DiskDevice, Notify};
use rusty_backup::controller::build_disk::{
    containers, hints, ids, labels, BuildDiskAction, BuildDiskController, CdLayout, CdOption,
    CdSidecars, CdView, DiskKind, ExportFormat, FsChoice, PresetKind, PresetView, Step,
    CD_AUTO_SIZE,
};
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::partition_edit::BuildAction;
use rusty_backup::controller::restore::device_label;
use rusty_backup::controller::{Controller, Effect};

use crate::build_rows::{line, Options, Rows};
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::partition_bar::Bar;
use crate::sync::SyncGuard;

const OUTPUTS: [&str; 2] = ["Image file", "Physical disk"];
const CONTROLLERS: [&str; 2] = ["SASI (256-byte sectors)", "SCSI (1024-byte sectors)"];

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum K {
    BusyNote,
    StepLine,
    KindCaption,
    Kind,
    KindHint,
    OutCaption,
    OutKind,
    ImageCaption,
    Image,
    PickImage,
    ContainerCaption,
    Container,
    DeviceCaption,
    Device,
    DeviceWarning,
    Donor,
    DonorBlank,
    DonorLine,
    TableCaption,
    Table,
    VolumeCaption,
    Volume,
    SizeCaption,
    Size,
    SizeHint,
    AlignCaption,
    Align,
    HeadsCaption,
    Heads,
    SectorsCaption,
    Sectors,
    PresetCaption,
    Preset,
    X68kScsi,
    X68kPartsCaption,
    X68kParts,
    SystemLine,
    SystemChoose,
    SystemClear,
    SgiCaption,
    SgiLabel,
    CdLayoutCaption,
    CdLayout,
    CdNameCaption,
    CdName,
    CdAuto,
    CdFolder,
    CdChoose,
    CdClear,
    CdOption(usize),
    CdSidecarsCaption,
    CdSidecars,
    RowsCaption,
    Bar,
    RowsError,
    RowHead(usize),
    RowNumber(usize),
    RowSize(usize),
    RowType(usize),
    RowPick(usize),
    RowPickItem(usize, usize),
    RowNote(usize),
    RowName(usize),
    RowBoot(usize),
    RowStart(usize),
    RowContent(usize),
    RowImage(usize),
    RowSource(usize),
    RowClear(usize),
    RowUp(usize),
    RowDown(usize),
    RowRemove(usize),
    AddRow,
    RowsStatus,
    HandlersTitle,
    HandlerLine(usize),
    HandlerRemove(usize),
    HandlerAdd(usize),
    DriverCaption,
    Driver,
    DriverDisk,
    OptTitle(usize),
    OptError(usize),
    OptLabelCaption(usize),
    OptLabel(usize),
    OptCaption(usize, usize),
    Opt(usize, usize),
    Review,
    Warnings,
    Danger,
    ConfirmCaption,
    Confirm,
    Job,
    JobProgress,
    Cancel,
    Outcome,
    Open,
    Another,
    Blocked,
    Back,
    Next,
    Create,
}

pub struct BuildScreen {
    ctrl: RefCell<BuildDiskController>,
    host: EffectHost,
    devices: RefCell<Vec<DiskDevice>>,
    /// Another screen's disk job, which locks Create.
    busy: Cell<Option<&'static str>>,
    sync: SyncGuard,
    me: Weak<BuildScreen>,
    body: Retained<NSView>,
    form: Form<K>,
    bar: Retained<Bar>,
    rows: Rows,
    options: Options,
    formats: Vec<ExportFormat>,
    table_kinds: RefCell<Vec<rusty_backup::controller::build_disk::TableKind>>,
    sidecars: RefCell<Vec<CdSidecars>>,
    open_inspect: RefCell<Option<PathBuf>>,
    notify: Arc<dyn Notify>,
}

fn strings(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

impl BuildScreen {
    pub fn build(
        mtm: MainThreadMarker,
        body: Retained<NSView>,
        notify: Arc<dyn Notify>,
        host: EffectHost,
    ) -> Rc<BuildScreen> {
        let screen = Rc::new_cyclic(|me: &Weak<BuildScreen>| {
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
            let kinds: Vec<&str> = DiskKind::ALL.iter().map(|k| k.label()).collect();
            let formats = containers();
            let names: Vec<&str> = formats.iter().map(|c| c.description()).collect();
            f.wrap(K::BusyNote, "");
            f.label(K::StepLine, "");
            f.label(K::KindCaption, &format!("{}:", labels::KIND));
            f.popup(K::Kind, &strings(&kinds), ids::KIND);
            f.wrap(K::KindHint, "");
            f.label(K::OutCaption, "Write to:");
            f.popup(K::OutKind, &strings(&OUTPUTS), ids::OUTPUT_KIND);
            f.label(K::ImageCaption, "Save as:");
            f.label(K::Image, "");
            f.button(K::PickImage, labels::PICK_IMAGE, ids::PICK_IMAGE);
            f.label(K::ContainerCaption, "Format:");
            f.popup(K::Container, &strings(&names), ids::CONTAINER);
            f.label(K::DeviceCaption, "Device:");
            f.popup(K::Device, &[], ids::OUTPUT_DEVICE);
            f.wrap(K::DeviceWarning, "");
            f.button(K::Donor, "An existing disk's layout...", ids::DONOR);
            f.button(K::DonorBlank, "Blank table", ids::DONOR_BLANK);
            f.wrap(K::DonorLine, "");
            f.label(K::TableCaption, "Table type:");
            f.popup(K::Table, &[], ids::TABLE);
            let fs_labels: Vec<String> = FsChoice::all().iter().map(|k| k.label()).collect();
            f.label(K::VolumeCaption, "Filesystem:");
            f.popup(K::Volume, &fs_labels, ids::VOLUME_FS);
            f.label(K::SizeCaption, "Size:");
            f.field(K::Size, ids::SIZE, "Size", false);
            f.label(K::SizeHint, "");
            f.label(K::AlignCaption, "Alignment:");
            f.field(K::Align, ids::ALIGN, "Alignment", false);
            f.label(K::HeadsCaption, "Heads:");
            f.field(K::Heads, ids::HEADS, "Heads", false);
            f.label(K::SectorsCaption, "Sectors/track:");
            f.field(K::Sectors, ids::SECTORS, "Sectors/track", false);
            let presets: Vec<&str> = PresetKind::ALL.iter().map(|p| p.label()).collect();
            f.label(K::PresetCaption, "Disk:");
            f.popup(K::Preset, &strings(&presets), ids::PRESET);
            f.popup(K::X68kScsi, &strings(&CONTROLLERS), ids::X68K_SCSI);
            f.label(K::X68kPartsCaption, "Human68k partitions:");
            f.field(K::X68kParts, ids::X68K_PARTS, "Human68k partitions", false);
            f.label(K::SystemLine, "");
            f.button(K::SystemChoose, "Choose...", ids::SYSTEM_DISK);
            f.button(K::SystemClear, "Clear", ids::SYSTEM_DISK_CLEAR);
            f.label(K::SgiCaption, "EFS label:");
            f.field(K::SgiLabel, ids::SGI_LABEL, "EFS label", false);
            let layouts: Vec<&str> = CdLayout::ALL.iter().map(|l| l.label()).collect();
            f.label(K::CdLayoutCaption, &format!("{}:", labels::CD_LAYOUT));
            f.popup(K::CdLayout, &strings(&layouts), ids::CD_LAYOUT);
            f.label(K::CdNameCaption, "Volume name:");
            f.field(K::CdName, ids::CD_NAME, "Volume name", false);
            f.check(K::CdAuto, labels::CD_AUTO_SIZE, ids::CD_AUTO_SIZE);
            f.label(K::CdFolder, "");
            f.button(K::CdChoose, labels::CD_FOLDER, ids::CD_FOLDER);
            f.button(K::CdClear, "Clear", ids::CD_FOLDER_CLEAR);
            for (i, o) in CdOption::ALL.iter().enumerate() {
                f.check(K::CdOption(i), o.label(), o.id());
            }
            f.label(K::CdSidecarsCaption, "macOS ._ files:");
            f.popup(K::CdSidecars, &[], ids::CD_SIDECARS);
            let bar = Bar::new(mtm, "build_disk.bar");
            bar.set_options(1.0, true);
            let rows = Rows::build(f, &bar);
            f.wrap(K::Review, "");
            f.wrap(K::Warnings, "");
            f.wrap(K::Danger, "");
            f.label(K::ConfirmCaption, "");
            f.field(K::Confirm, ids::CONFIRM, "Confirm", false);
            f.label(K::Job, "");
            f.progress(K::JobProgress);
            f.button(K::Cancel, "Cancel", ids::CANCEL);
            f.wrap(K::Outcome, "");
            f.button(K::Open, labels::OPEN_INSPECT, ids::OPEN_INSPECT);
            f.button(K::Another, labels::BUILD_ANOTHER, ids::BUILD_ANOTHER);
            f.label(K::Blocked, "");
            f.button(K::Back, "Back", ids::BACK);
            f.button(K::Next, labels::NEXT, ids::NEXT);
            f.button(K::Create, labels::CREATE, ids::CREATE);
            BuildScreen {
                ctrl: RefCell::new(BuildDiskController::new(notify.clone())),
                host,
                devices: RefCell::default(),
                busy: Cell::new(None),
                sync: SyncGuard::default(),
                me: me.clone(),
                body,
                form,
                bar,
                rows,
                options: Options::default(),
                formats,
                table_kinds: RefCell::default(),
                sidecars: RefCell::default(),
                open_inspect: RefCell::default(),
                notify,
            }
        });
        screen.refresh();
        screen
    }

    pub fn set_devices(&self, devices: &[DiskDevice]) {
        self.ctrl.borrow_mut().reconcile_devices(devices);
        *self.devices.borrow_mut() = devices.to_vec();
    }

    pub fn set_busy(&self, busy: Option<&'static str>) {
        self.busy.set(busy);
    }

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    /// The image "Open in Inspect" asked to show, once.
    pub fn take_open_inspect(&self) -> Option<PathBuf> {
        self.open_inspect.borrow_mut().take()
    }

    pub fn apply(&self, action: BuildDiskAction) {
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
                Reply::Picked { purpose, path } => BuildDiskAction::Picked { purpose, path },
                // Which NTFS version a new NTFS target gets; a device is confirmed by typing.
                Reply::Chose { purpose, choice } => BuildDiskAction::Chose { purpose, choice },
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

    pub fn refresh(&self) {
        self.sync.run(|| self.show());
        self.relayout();
    }

    fn show(&self) {
        let f = &self.form;
        let ctrl = self.ctrl.borrow();
        let step = ctrl.step();
        let kind = ctrl.kind();
        let notice = self.busy.get().map(busy_elsewhere_notice);
        line(f, K::BusyNote, notice.as_deref(), Some(Tone::Warning));
        let steps = ctrl.steps();
        let at = steps.iter().position(|s| *s == step).unwrap_or(0);
        let names: Vec<&str> = steps.iter().map(|s| s.label()).collect();
        let text = format!(
            "Step {} of {}: {}   ({})",
            at + 1,
            steps.len(),
            step.label(),
            names.join(" > ")
        );
        f.set_text(K::StepLine, &text);

        let on = step == Step::Kind;
        for k in [K::KindCaption, K::Kind, K::KindHint] {
            f.show(k, on);
        }
        let k = DiskKind::ALL.iter().position(|d| *d == kind).unwrap_or(0);
        f.select(K::Kind, k);
        f.set_text(K::KindHint, kind.hint());

        self.show_output(&ctrl, step == Step::Output);
        self.show_disk(&ctrl, step == Step::Disk);
        self.rows
            .show(f, &self.bar, &ctrl, step == Step::Partitions);
        self.options.show(f, &ctrl, step == Step::Options);
        self.show_review(&ctrl, step == Step::Review);
        self.show_build(&ctrl, step == Step::Build);

        let blocked = ctrl.next_blocked();
        let running = ctrl.is_running();
        let free = self.busy.get().is_none();
        f.show(K::Back, step != Step::Kind && step != Step::Build);
        f.enable(K::Back, !running);
        f.show(K::Next, step != Step::Review && step != Step::Build);
        f.enable(K::Next, blocked.is_none());
        f.show(K::Create, step == Step::Review);
        f.enable(K::Create, blocked.is_none() && free);
        let why = blocked.or_else(|| (step == Step::Review).then_some(notice).flatten());
        line(
            f,
            K::Blocked,
            why.as_deref().filter(|_| step != Step::Build),
            None,
        );
    }

    fn show_output(&self, ctrl: &BuildDiskController, on: bool) {
        let f = &self.form;
        let device = ctrl.output_is_device();
        let cd = ctrl.kind() == DiskKind::Cd;
        f.show(K::OutCaption, on && (!cd || device));
        f.show(K::OutKind, on && (!cd || device));
        f.select(K::OutKind, usize::from(device));
        for k in [K::ImageCaption, K::Image, K::PickImage] {
            f.show(k, on && !device);
        }
        let image = ctrl.image().map(|p| p.display().to_string());
        f.set_text(K::Image, image.as_deref().unwrap_or("(not chosen)"));
        for k in [K::ContainerCaption, K::Container] {
            f.show(k, on && !device && !cd);
        }
        let at = self.formats.iter().position(|c| *c == ctrl.container());
        f.select(K::Container, at.unwrap_or(0));
        for k in [K::DeviceCaption, K::Device] {
            f.show(k, on && device);
        }
        let devices = self.devices.borrow();
        let mut items = vec![if devices.is_empty() {
            no_devices_hint().to_string()
        } else {
            "Select a device...".to_string()
        }];
        items.extend(devices.iter().map(device_label));
        f.set_items(K::Device, &items);
        f.select(K::Device, ctrl.device_row().map_or(0, |r| r + 1));
        line(
            f,
            K::DeviceWarning,
            (on && device).then_some(hints::DEVICE_WARNING),
            Some(Tone::Warning),
        );
    }

    fn show_disk(&self, ctrl: &BuildDiskController, on: bool) {
        let f = &self.form;
        let kind = ctrl.kind();
        let part = on && kind == DiskKind::Partitioned;
        let vol = on && kind == DiskKind::Volume;
        let preset = on && kind == DiskKind::Preset;
        let cd = on && kind == DiskKind::Cd;
        let view = ctrl.table_view();

        f.show(K::Donor, part);
        f.show(K::DonorBlank, part);
        f.enable(K::DonorBlank, ctrl.donor().is_some());
        let donor = if ctrl.donor_reading() {
            Some(("Reading the disk...".to_string(), None))
        } else if let Some(e) = ctrl.donor_error() {
            Some((e.to_string(), Some(Tone::Error)))
        } else {
            ctrl.donor()
                .map(|(p, t)| (format!("From {} ({t})", p.display()), None))
        };
        let (text, tone) = donor.filter(|_| part).unzip();
        line(f, K::DonorLine, text.as_deref(), tone.flatten());

        f.show(K::TableCaption, part);
        f.show(K::Table, part);
        let labels: Vec<String> = view.kinds.iter().map(|k| k.label().to_string()).collect();
        f.set_items(K::Table, &labels);
        let at = view.kinds.iter().position(|k| *k == view.kind);
        f.select(K::Table, at.unwrap_or(0));
        *self.table_kinds.borrow_mut() = view.kinds.to_vec();
        f.enable(K::Table, !view.fixed);
        f.show(K::AlignCaption, part);
        f.show(K::Align, part);
        f.set_text(K::Align, &view.align_text);
        let geo = part && view.geometry.is_some();
        for k in [K::HeadsCaption, K::Heads, K::SectorsCaption, K::Sectors] {
            f.show(k, geo);
        }
        if let Some((heads, sectors)) = view.geometry {
            f.set_text(K::Heads, &heads.to_string());
            f.set_text(K::Sectors, &sectors.to_string());
        }

        f.show(K::VolumeCaption, vol);
        f.show(K::Volume, vol);
        let at = ctrl.volume_fs_choice().index_in(&FsChoice::all());
        f.select(K::Volume, at.unwrap_or(0));

        // The size is set here unless a device fixes it (Output) or the preset sizes itself.
        let sized = (part || vol || cd) && !ctrl.output_is_device();
        let fixed = vol && ctrl.volume_size_range().is_none();
        f.show(K::SizeCaption, sized && !fixed);
        f.show(K::Size, sized && !fixed);
        f.enable(K::Size, !(cd && ctrl.cd_auto_size()));
        f.set_text(K::Size, ctrl.size_text());
        let hint = if fixed {
            Some(hints::FIXED_SIZE.to_string())
        } else if vol {
            ctrl.volume_size_range()
        } else if cd {
            Some(hints::CD_SIZE.to_string())
        } else {
            Some(hints::SIZE.to_string())
        };
        line(f, K::SizeHint, hint.as_deref().filter(|_| sized), None);

        let p = ctrl.preset();
        f.show(K::PresetCaption, preset);
        f.show(K::Preset, preset);
        let at = PresetKind::ALL.iter().position(|k| *k == p.kind);
        f.select(K::Preset, at.unwrap_or(0));
        let x68k = preset && p.kind == PresetKind::X68k;
        for k in [
            K::X68kScsi,
            K::X68kPartsCaption,
            K::X68kParts,
            K::SystemLine,
            K::SystemChoose,
            K::SystemClear,
        ] {
            f.show(k, x68k);
        }
        f.select(K::X68kScsi, usize::from(p.x68k_scsi));
        f.set_text(K::X68kParts, &p.x68k_partitions.to_string());
        let system = p.x68k_system_disk.as_ref().map(|s| s.display().to_string());
        let system = system.as_deref().unwrap_or(hints::NO_SYSTEM_DISK);
        f.set_text(K::SystemLine, &format!("System disk: {system}"));
        let sgi = preset && p.kind == PresetKind::SgiEfs;
        f.show(K::SgiCaption, sgi);
        f.show(K::SgiLabel, sgi);
        f.set_text(K::SgiLabel, &p.sgi_label);

        let c = ctrl.cd();
        for k in [
            K::CdLayoutCaption,
            K::CdLayout,
            K::CdNameCaption,
            K::CdName,
            K::CdAuto,
            K::CdFolder,
            K::CdChoose,
            K::CdClear,
            K::CdSidecarsCaption,
            K::CdSidecars,
        ] {
            f.show(k, cd);
        }
        let at = CdLayout::ALL.iter().position(|l| *l == c.layout);
        f.select(K::CdLayout, at.unwrap_or(0));
        f.set_text(K::CdName, &c.name);
        f.set_check(K::CdAuto, ctrl.cd_auto_size());
        let folder = c.from_dir.as_ref().map(|d| d.display().to_string());
        let folder = folder.as_deref().unwrap_or("(blank disc)");
        f.set_text(K::CdFolder, &format!("Fill from: {folder}"));
        f.enable(K::CdClear, c.from_dir.is_some());
        for row in ctrl.cd_options() {
            if let Some(i) = CdOption::ALL.iter().position(|o| *o == row.option) {
                f.show(K::CdOption(i), cd);
                f.set_check(K::CdOption(i), row.on);
                f.enable(K::CdOption(i), row.unavailable.is_none());
            }
        }
        let choices = CdSidecars::choices(c.layout);
        let names: Vec<String> = choices.iter().map(|s| s.label().to_string()).collect();
        f.set_items(K::CdSidecars, &names);
        let at = choices.iter().position(|s| *s == ctrl.cd_sidecars());
        f.select(K::CdSidecars, at.unwrap_or(0));
        f.enable(K::CdSidecars, ctrl.cd_sidecars_unavailable().is_none());
        *self.sidecars.borrow_mut() = choices.to_vec();
    }

    fn show_review(&self, ctrl: &BuildDiskController, on: bool) {
        let f = &self.form;
        let lines = ctrl.review_lines().join("\n");
        line(f, K::Review, on.then_some(lines.as_str()), None);
        let warnings = ctrl.warnings();
        let tone = warnings
            .iter()
            .any(|w| w.starts_with("Warning:"))
            .then_some(Tone::Warning);
        let text = warnings.join("\n");
        let shown = (on && !warnings.is_empty()).then_some(text.as_str());
        line(f, K::Warnings, shown, tone);
        let want = ctrl.confirm_expected().filter(|_| on);
        let erased = want
            .as_ref()
            .map(|w| format!("Everything on {w} will be erased."));
        line(f, K::Danger, erased.as_deref(), Some(Tone::Error));
        let title = want.as_ref().map(|w| format!("Type `{w}` to confirm:"));
        line(f, K::ConfirmCaption, title.as_deref(), None);
        f.show(K::Confirm, want.is_some());
        f.set_text(K::Confirm, ctrl.confirm_text());
    }

    fn show_build(&self, ctrl: &BuildDiskController, on: bool) {
        let f = &self.form;
        let job = ctrl.progress().filter(|p| p.is_running());
        let (text, pct) = job
            .as_ref()
            .map(crate::inspect::job_line)
            .unwrap_or_default();
        f.set_text(K::Job, &text);
        f.show(K::Job, on && job.is_some());
        let bar = f
            .v(K::JobProgress)
            .downcast::<NSProgressIndicator>()
            .expect("a progress bar");
        crate::ui::set_progress(&bar, on && job.is_some(), pct);
        f.show(K::Cancel, on && job.is_some());
        let outcome = match ctrl.outcome() {
            Some(Ok(())) => Some((labels::DONE.to_string(), None)),
            Some(Err(e)) => Some((format!("Error: {e}"), Some(Tone::Error))),
            None => None,
        };
        let (text, tone) = outcome.filter(|_| on).unzip();
        line(f, K::Outcome, text.as_deref(), tone.flatten());
        let done = on && job.is_none();
        f.show(K::Another, done);
        f.show(K::Open, done && ctrl.built_image().is_some());
    }

    pub fn relayout(&self) {
        let f = &self.form;
        let row_h = Size::Fixed(layout::ROW);
        let lines = |k: K| {
            let n = f.text(k).lines().count().max(1) as i32;
            Node::leaf(k, Size::Stretch(1), Size::Fixed((layout::ROW - 6) * n + 4))
        };
        let fixed = |k: K, w: i32| Node::leaf(k, Size::Fixed(w), row_h);
        let pop = |k: K, w: Size| Node::leaf(k, w, Size::Fixed(layout::ROW + 2));
        let stretch = |k: K| Node::leaf(k, Size::Stretch(1), row_h);
        let shown = |k: K| f.shown(k);
        let cap = 130;
        let pair = |c: K, n: Node<K>| Node::row(vec![fixed(c, cap), n]);
        let mut col = Vec::new();
        if shown(K::BusyNote) {
            col.push(lines(K::BusyNote));
        }
        col.push(Node::row(vec![stretch(K::StepLine)]));
        if shown(K::Kind) {
            col.push(pair(K::KindCaption, pop(K::Kind, Size::Fixed(320))));
            col.push(lines(K::KindHint));
        }
        if shown(K::OutKind) {
            col.push(pair(K::OutCaption, pop(K::OutKind, Size::Fixed(200))));
        }
        if shown(K::Image) {
            col.push(Node::row(vec![
                fixed(K::ImageCaption, cap),
                stretch(K::Image),
                Node::button(K::PickImage, 110),
            ]));
        }
        if shown(K::Container) {
            col.push(pair(
                K::ContainerCaption,
                pop(K::Container, Size::Fixed(300)),
            ));
        }
        if shown(K::Device) {
            col.push(pair(K::DeviceCaption, pop(K::Device, Size::Stretch(1))));
        }
        if shown(K::DeviceWarning) {
            col.push(lines(K::DeviceWarning));
        }
        if shown(K::Donor) {
            col.push(Node::row(vec![
                Node::button(K::Donor, 230),
                Node::button(K::DonorBlank, 110),
                Node::Space(Size::Stretch(1)),
            ]));
        }
        if shown(K::DonorLine) {
            col.push(lines(K::DonorLine));
        }
        if shown(K::Table) {
            col.push(pair(K::TableCaption, pop(K::Table, Size::Fixed(200))));
        }
        if shown(K::Volume) {
            col.push(pair(K::VolumeCaption, pop(K::Volume, Size::Fixed(300))));
        }
        if shown(K::Preset) {
            col.push(pair(K::PresetCaption, pop(K::Preset, Size::Fixed(360))));
        }
        if shown(K::X68kScsi) {
            col.push(Node::row(vec![
                Node::Space(Size::Fixed(cap)),
                pop(K::X68kScsi, Size::Fixed(220)),
                fixed(K::X68kPartsCaption, 150),
                fixed(K::X68kParts, 50),
                Node::Space(Size::Stretch(1)),
            ]));
            col.push(Node::row(vec![
                Node::Space(Size::Fixed(cap)),
                stretch(K::SystemLine),
                Node::button(K::SystemChoose, 90),
                Node::button(K::SystemClear, 70),
            ]));
        }
        if shown(K::SgiLabel) {
            col.push(pair(K::SgiCaption, fixed(K::SgiLabel, 120)));
        }
        if shown(K::CdLayout) {
            col.push(pair(K::CdLayoutCaption, pop(K::CdLayout, Size::Fixed(360))));
            col.push(pair(K::CdNameCaption, fixed(K::CdName, 200)));
            col.push(Node::row(vec![
                Node::Space(Size::Fixed(cap)),
                stretch(K::CdAuto),
            ]));
        }
        if shown(K::Size) {
            col.push(Node::row(vec![
                fixed(K::SizeCaption, cap),
                fixed(K::Size, 120),
                stretch(K::SizeHint),
            ]));
        } else if shown(K::SizeHint) {
            col.push(Node::row(vec![
                Node::Space(Size::Fixed(cap)),
                stretch(K::SizeHint),
            ]));
        }
        if shown(K::Align) {
            let mut row = vec![fixed(K::AlignCaption, cap), fixed(K::Align, 120)];
            if shown(K::Heads) {
                row.extend([
                    fixed(K::HeadsCaption, 60),
                    fixed(K::Heads, 60),
                    fixed(K::SectorsCaption, 110),
                    fixed(K::Sectors, 60),
                ]);
            }
            row.push(Node::Space(Size::Stretch(1)));
            col.push(Node::row(row));
        }
        if shown(K::CdFolder) {
            col.push(Node::row(vec![
                Node::Space(Size::Fixed(cap)),
                stretch(K::CdFolder),
                Node::button(K::CdChoose, 150),
                Node::button(K::CdClear, 70),
            ]));
            let opts: Vec<usize> = (0..CdOption::ALL.len()).collect();
            for two in opts.chunks(2) {
                let mut row = vec![Node::Space(Size::Fixed(cap))];
                row.extend(two.iter().map(|i| stretch(K::CdOption(*i))));
                col.push(Node::row(row));
            }
            col.push(pair(
                K::CdSidecarsCaption,
                pop(K::CdSidecars, Size::Fixed(240)),
            ));
        }
        col.extend(self.rows.nodes(f));
        col.extend(self.options.nodes(f));
        for k in [K::Review, K::Warnings, K::Danger] {
            if shown(k) {
                col.push(lines(k));
            }
        }
        if shown(K::Confirm) {
            col.push(Node::row(vec![
                fixed(K::ConfirmCaption, 260),
                fixed(K::Confirm, 200),
                Node::Space(Size::Stretch(1)),
            ]));
        }
        if shown(K::Job) {
            col.push(Node::row(vec![
                stretch(K::Job),
                Node::leaf(
                    K::JobProgress,
                    Size::Fixed(200),
                    Size::Fixed(layout::ROW - 4),
                ),
                Node::button(K::Cancel, 85),
            ]));
        }
        if shown(K::Outcome) {
            col.push(lines(K::Outcome));
        }
        if shown(K::Another) {
            let mut row = vec![Node::button(K::Another, 130)];
            if shown(K::Open) {
                row.push(Node::button(K::Open, 140));
            }
            row.push(Node::Space(Size::Stretch(1)));
            col.push(Node::row(row));
        }
        col.push(Node::Space(Size::Stretch(1)));
        let mut foot = Vec::new();
        if shown(K::Back) {
            foot.push(Node::button(K::Back, 85));
        }
        foot.push(stretch(K::Blocked));
        for (k, w) in [(K::Next, 95), (K::Create, 125)] {
            if shown(k) {
                foot.push(Node::button(k, w));
            }
        }
        col.push(Node::row(foot));
        f.place_page(Node::inset(Node::column(col), layout::MARGIN));
    }

    fn preset_edit(&self, change: impl FnOnce(&mut PresetView)) -> BuildDiskAction {
        let mut p = self.ctrl.borrow().preset().clone();
        change(&mut p);
        BuildDiskAction::SetPreset(p)
    }

    fn cd_edit(&self, change: impl FnOnce(&mut CdView)) -> BuildDiskAction {
        let mut c = self.ctrl.borrow().cd().clone();
        change(&mut c);
        BuildDiskAction::SetCd(c)
    }

    fn action(&self, k: K) -> Option<BuildDiskAction> {
        let f = &self.form;
        let text = || f.text(k);
        let number = || f.text(k).trim().parse::<u16>().ok();
        let table = |a: BuildAction| Some(BuildDiskAction::Table(a));
        Some(match k {
            K::Kind => BuildDiskAction::SetKind(*DiskKind::ALL.get(f.picked(k)?)?),
            K::OutKind => BuildDiskAction::SetOutputIsDevice(f.picked(k)? == 1),
            K::Container => BuildDiskAction::SetContainer(*self.formats.get(f.picked(k)?)?),
            K::Device => {
                let Some(row) = f.picked(k)?.checked_sub(1) else {
                    return Some(BuildDiskAction::ClearDevice);
                };
                let device = self.devices.borrow().get(row).cloned()?;
                BuildDiskAction::SelectDevice { row, device }
            }
            K::Table => {
                let kind = *self.table_kinds.borrow().get(f.picked(k)?)?;
                return table(BuildAction::SetKind(kind));
            }
            K::Volume => BuildDiskAction::SetVolumeFs(*FsChoice::all().get(f.picked(k)?)?),
            K::Preset => {
                let kind = *PresetKind::ALL.get(f.picked(k)?)?;
                self.preset_edit(|p| p.kind = kind)
            }
            K::X68kScsi => {
                let scsi = f.picked(k)? == 1;
                self.preset_edit(|p| p.x68k_scsi = scsi)
            }
            K::CdLayout => {
                let layout = *CdLayout::ALL.get(f.picked(k)?)?;
                self.cd_edit(|c| c.layout = layout)
            }
            K::CdSidecars => {
                BuildDiskAction::SetCdSidecars(*self.sidecars.borrow().get(f.picked(k)?)?)
            }
            K::Size => BuildDiskAction::SetSize(text()),
            K::Align => return table(BuildAction::SetAlign(text())),
            K::Heads => return table(BuildAction::SetHeads(number()?)),
            K::Sectors => return table(BuildAction::SetSectorsPerTrack(number()?)),
            K::X68kParts => {
                let n = f.text(k).trim().parse::<usize>().ok();
                let n = n.filter(|n| (1..=8).contains(n))?;
                self.preset_edit(|p| p.x68k_partitions = n)
            }
            K::SgiLabel => {
                let t = text();
                self.preset_edit(|p| p.sgi_label = t)
            }
            K::CdName => {
                let t = text();
                self.cd_edit(|c| c.name = t)
            }
            K::Confirm => BuildDiskAction::SetConfirmText(text()),
            K::CdOption(i) => BuildDiskAction::SetCdOption(*CdOption::ALL.get(i)?, f.checked(k)),
            K::PickImage => BuildDiskAction::PickOutputImage,
            K::Donor => BuildDiskAction::PickDonor,
            K::DonorBlank => BuildDiskAction::ClearDonor,
            K::SystemChoose => BuildDiskAction::PickSystemDisk,
            K::SystemClear => self.preset_edit(|p| p.x68k_system_disk = None),
            K::CdAuto => {
                // Sizing to the folder is the size text `auto`; off goes back to a CD-R's size.
                let size = if f.checked(k) { CD_AUTO_SIZE } else { "600M" };
                BuildDiskAction::SetSize(size.to_string())
            }
            K::CdChoose => BuildDiskAction::PickCdFolder,
            K::CdClear => self.cd_edit(|c| c.from_dir = None),
            K::Back => BuildDiskAction::Back,
            K::Next => BuildDiskAction::Next,
            K::Create => {
                // Create asks the controller again at the click.
                if self.ctrl.borrow().next_blocked().is_some() || self.busy.get().is_some() {
                    return None;
                }
                BuildDiskAction::Create
            }
            K::Cancel => BuildDiskAction::Cancel,
            K::Another => BuildDiskAction::BuildAnother,
            K::Open => {
                *self.open_inspect.borrow_mut() =
                    self.ctrl.borrow().built_image().map(PathBuf::from);
                // No action reaches the app loop from here, so wake it to take the request.
                self.notify.wake();
                return None;
            }
            _ => {
                let ctrl = self.ctrl.borrow();
                return self
                    .rows
                    .fired(f, &ctrl, k)
                    .or_else(|| self.options.fired(f, &ctrl, k));
            }
        })
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        if let Some(a) = self.action(k) {
            // Rows may be built again by what this sends: act once the handler returns.
            let me = self.me.clone();
            crate::app::later(Box::new(move || {
                if let Some(s) = me.upgrade() {
                    s.apply(a);
                }
            }));
        }
    }
}
