//! The Build Disk wizard over `controller::build_disk`; GTK's `build_disk/` is the reference.
//!
//! The steps are the controller's: this screen shows the current one's controls and Back / Next /
//! Create below them. Kind, Output, Disk, Review and Build live here; the Partitions and Options
//! steps, whose rows are built from the plan, are `build_rows.rs`.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::{busy_elsewhere_notice, no_devices_hint, DiskDevice, Notify};
use rusty_backup::controller::build_disk::{
    containers, hints, ids, labels, BuildDiskAction, BuildDiskController, CdLayout, CdOption,
    CdSidecars, CdView, DiskKind, ExportFormat, FsChoice, PresetKind, PresetView, Step, CD_AUTO_SIZE,
};
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::partition_edit::BuildAction;
use rusty_backup::controller::restore::device_label;
use rusty_backup::controller::{Controller, Effect};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::UI::Controls::NMHDR;
use windows::Win32::UI::Input::KeyboardAndMouse::GetFocus;
use windows::Win32::UI::WindowsAndMessaging::{
    SendMessageW, BM_GETCHECK, BM_SETCHECK, BN_CLICKED, CBN_SELCHANGE, EN_CHANGE, ES_NUMBER,
    WINDOW_EX_STYLE, WINDOW_STYLE, WS_EX_CLIENTEDGE, WS_TABSTOP,
};

use crate::build_rows::{OptionsStep, RowsStep};
use crate::combo::Combo;
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;

const OUTPUTS: [&str; 2] = ["Image file", "Physical disk"];
const CONTROLLERS: [&str; 2] = ["SASI (256-byte sectors)", "SCSI (1024-byte sectors)"];

pub(crate) fn checked(h: HWND) -> bool {
    // SAFETY: a plain button query.
    unsafe { SendMessageW(h, BM_GETCHECK, None, None).0 == 1 }
}

pub(crate) fn set_check(h: HWND, on: bool) {
    if checked(h) != on {
        // SAFETY: a plain button message; it sends no BN_CLICKED.
        unsafe {
            SendMessageW(h, BM_SETCHECK, Some(WPARAM(on as usize)), None);
        }
    }
}

/// Set a field's text unless the user is typing in it.
pub(crate) fn set_field(h: HWND, text: &str) {
    // SAFETY: a plain focus query.
    if unsafe { GetFocus() } != h && ui::text(h) != text {
        ui::set_text(h, text);
    }
}

pub(crate) fn number(parent: HWND, id: &str) -> HWND {
    let style = WINDOW_STYLE(ES_NUMBER as u32 | WS_TABSTOP.0);
    ui::child(
        parent,
        windows::core::w!("EDIT"),
        "",
        style,
        WS_EX_CLIENTEDGE,
        id,
    )
}

/// One line of text with a tone; hidden when there is none.
pub(crate) fn set_line(h: HWND, text: Option<&str>, tone: Option<Tone>) {
    ui::set_text(h, text.unwrap_or(""));
    ui::set_visible(h, text.is_some());
    crate::theme::set_tone(h, tone);
}

/// The Disk step's controls.
struct DiskStep {
    donor: HWND,
    donor_blank: HWND,
    donor_line: HWND,
    table_caption: HWND,
    table: Combo,
    kinds: RefCell<Vec<rusty_backup::controller::build_disk::TableKind>>,
    volume_caption: HWND,
    volume: Combo,
    size_caption: HWND,
    size: HWND,
    size_hint: HWND,
    align_caption: HWND,
    align: HWND,
    heads_caption: HWND,
    heads: HWND,
    sectors_caption: HWND,
    sectors: HWND,
    preset_caption: HWND,
    preset: Combo,
    x68k_scsi: Combo,
    x68k_parts_caption: HWND,
    x68k_parts: HWND,
    system_line: HWND,
    system_choose: HWND,
    system_clear: HWND,
    sgi_caption: HWND,
    sgi_label: HWND,
    cd_layout_caption: HWND,
    cd_layout: Combo,
    cd_name_caption: HWND,
    cd_name: HWND,
    cd_auto: HWND,
    cd_folder: HWND,
    cd_choose: HWND,
    cd_clear: HWND,
    cd_options: Vec<(CdOption, HWND)>,
    cd_sidecars_caption: HWND,
    cd_sidecars: Combo,
    cd_sidecar_choices: RefCell<Vec<CdSidecars>>,
}

impl DiskStep {
    fn build(page: HWND) -> DiskStep {
        let fs_labels: Vec<String> = FsChoice::all().iter().map(|k| k.label()).collect();
        let fs_names: Vec<&str> = fs_labels.iter().map(String::as_str).collect();
        let presets: Vec<&str> = PresetKind::ALL.iter().map(|p| p.label()).collect();
        let layouts: Vec<&str> = CdLayout::ALL.iter().map(|l| l.label()).collect();
        DiskStep {
            donor: ui::button(page, "An existing disk's layout...", ids::DONOR),
            donor_blank: ui::button(page, "Blank table", ids::DONOR_BLANK),
            donor_line: ui::label(page, ""),
            table_caption: ui::cell_label(page, "Table type:"),
            table: Combo::build(page, ids::TABLE, &[]),
            kinds: RefCell::default(),
            volume_caption: ui::cell_label(page, "Filesystem:"),
            volume: Combo::build(page, ids::VOLUME_FS, &fs_names),
            size_caption: ui::cell_label(page, "Size:"),
            size: ui::edit(page, ids::SIZE),
            size_hint: ui::cell_label(page, ""),
            align_caption: ui::cell_label(page, "Alignment:"),
            align: ui::edit(page, ids::ALIGN),
            heads_caption: ui::cell_label(page, "Heads:"),
            heads: number(page, ids::HEADS),
            sectors_caption: ui::cell_label(page, "Sectors/track:"),
            sectors: number(page, ids::SECTORS),
            preset_caption: ui::cell_label(page, "Disk:"),
            preset: Combo::build(page, ids::PRESET, &presets),
            x68k_scsi: Combo::build(page, ids::X68K_SCSI, &CONTROLLERS),
            x68k_parts_caption: ui::cell_label(page, "Human68k partitions:"),
            x68k_parts: number(page, ids::X68K_PARTS),
            system_line: ui::path_label(page, ""),
            system_choose: ui::button(page, "Choose...", ids::SYSTEM_DISK),
            system_clear: ui::button(page, "Clear", ids::SYSTEM_DISK_CLEAR),
            sgi_caption: ui::cell_label(page, "EFS label:"),
            sgi_label: ui::edit(page, ids::SGI_LABEL),
            cd_layout_caption: ui::cell_label(page, &format!("{}:", labels::CD_LAYOUT)),
            cd_layout: Combo::build(page, ids::CD_LAYOUT, &layouts),
            cd_name_caption: ui::cell_label(page, "Volume name:"),
            cd_name: ui::edit(page, ids::CD_NAME),
            cd_auto: ui::checkbox(page, labels::CD_AUTO_SIZE, ids::CD_AUTO_SIZE),
            cd_folder: ui::path_label(page, ""),
            cd_choose: ui::button(page, labels::CD_FOLDER, ids::CD_FOLDER),
            cd_clear: ui::button(page, "Clear", ids::CD_FOLDER_CLEAR),
            cd_options: CdOption::ALL
                .iter()
                .map(|o| (*o, ui::checkbox(page, o.label(), o.id())))
                .collect(),
            cd_sidecars_caption: ui::cell_label(page, "macOS ._ files:"),
            cd_sidecars: Combo::build(page, ids::CD_SIDECARS, &[]),
            cd_sidecar_choices: RefCell::default(),
        }
    }
}

pub struct BuildScreen {
    ctrl: RefCell<BuildDiskController>,
    host: EffectHost,
    devices: RefCell<Vec<DiskDevice>>,
    busy: Cell<Option<&'static str>>,
    sync: SyncGuard,
    me: Weak<BuildScreen>,
    page: HWND,
    open_inspect: RefCell<Option<PathBuf>>,
    notify: Arc<dyn Notify>,
    busy_note: HWND,
    step_line: HWND,
    kind_caption: HWND,
    kind: Combo,
    kind_hint: HWND,
    out_caption: HWND,
    out_kind: Combo,
    image_caption: HWND,
    image: HWND,
    pick_image: HWND,
    container_caption: HWND,
    container: Combo,
    formats: Vec<ExportFormat>,
    device_caption: HWND,
    device: Combo,
    device_warning: HWND,
    disk: DiskStep,
    rows: RowsStep,
    options: OptionsStep,
    review: HWND,
    warnings: HWND,
    danger: HWND,
    confirm_caption: HWND,
    confirm: HWND,
    job: HWND,
    job_progress: HWND,
    cancel: HWND,
    outcome: HWND,
    open: HWND,
    another: HWND,
    blocked: HWND,
    back: HWND,
    next: HWND,
    create: HWND,
    shape: RefCell<Vec<usize>>,
}

impl BuildScreen {
    pub fn build(page: HWND, notify: Arc<dyn Notify>, host: EffectHost) -> Rc<BuildScreen> {
        let kinds: Vec<&str> = DiskKind::ALL.iter().map(|k| k.label()).collect();
        let formats = containers();
        let names: Vec<&str> = formats.iter().map(|c| c.description()).collect();
        let progress = ui::child(
            page,
            windows::core::w!("msctls_progress32"),
            "",
            WINDOW_STYLE(0),
            WINDOW_EX_STYLE(0),
            "",
        );
        let screen = Rc::new_cyclic(|me: &Weak<BuildScreen>| BuildScreen {
            ctrl: RefCell::new(BuildDiskController::new(notify.clone())),
            host,
            devices: RefCell::default(),
            busy: Cell::new(None),
            sync: SyncGuard::default(),
            me: me.clone(),
            page,
            open_inspect: RefCell::default(),
            notify,
            busy_note: ui::label(page, ""),
            step_line: ui::cell_label(page, ""),
            kind_caption: ui::cell_label(page, &format!("{}:", labels::KIND)),
            kind: Combo::build(page, ids::KIND, &kinds),
            kind_hint: ui::label(page, ""),
            out_caption: ui::cell_label(page, "Write to:"),
            out_kind: Combo::build(page, ids::OUTPUT_KIND, &OUTPUTS),
            image_caption: ui::cell_label(page, "Save as:"),
            image: ui::path_label(page, ""),
            pick_image: ui::button(page, labels::PICK_IMAGE, ids::PICK_IMAGE),
            container_caption: ui::cell_label(page, "Format:"),
            container: Combo::build(page, ids::CONTAINER, &names),
            formats,
            device_caption: ui::cell_label(page, "Device:"),
            device: Combo::build(page, ids::OUTPUT_DEVICE, &[]),
            device_warning: ui::label(page, ""),
            disk: DiskStep::build(page),
            rows: RowsStep::build(page),
            options: OptionsStep::new(page),
            review: ui::label(page, ""),
            warnings: ui::label(page, ""),
            danger: ui::label(page, ""),
            confirm_caption: ui::cell_label(page, ""),
            confirm: ui::edit(page, ids::CONFIRM),
            job: ui::cell_label(page, ""),
            job_progress: progress,
            cancel: ui::button(page, "Cancel", ids::CANCEL),
            outcome: ui::label(page, ""),
            open: ui::button(page, "Open in Inspect", ids::OPEN_INSPECT),
            another: ui::button(page, labels::BUILD_ANOTHER, ids::BUILD_ANOTHER),
            blocked: ui::cell_label(page, ""),
            back: ui::button(page, "Back", ids::BACK),
            next: ui::button(page, labels::NEXT, ids::NEXT),
            create: ui::button(page, labels::CREATE, ids::CREATE),
            shape: RefCell::default(),
        });
        let weak: Weak<dyn Handler> = screen.me.clone();
        panel::register(page, weak);
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
        if ui::shown(self.page) {
            self.refresh();
        }
    }

    pub fn refresh(&self) {
        self.sync.run(|| self.show());
        if *self.shape.borrow() != self.shape_now() {
            self.layout();
        }
    }

    fn show(&self) {
        let ctrl = self.ctrl.borrow();
        let step = ctrl.step();
        let kind = ctrl.kind();
        let notice = self.busy.get().map(busy_elsewhere_notice);
        set_line(self.busy_note, notice.as_deref(), Some(Tone::Warning));
        let steps = ctrl.steps();
        let at = steps.iter().position(|s| *s == step).unwrap_or(0);
        let names: Vec<&str> = steps.iter().map(|s| s.label()).collect();
        let line = format!(
            "Step {} of {}: {}   ({})",
            at + 1,
            steps.len(),
            step.label(),
            names.join(" > ")
        );
        ui::set_text(self.step_line, &line);

        let on = step == Step::Kind;
        for h in [self.kind_caption, self.kind.h, self.kind_hint] {
            ui::set_visible(h, on);
        }
        let k = DiskKind::ALL.iter().position(|d| *d == kind).unwrap_or(0);
        self.kind.select(k);
        ui::set_text(self.kind_hint, kind.hint());

        self.show_output(&ctrl, step == Step::Output);
        self.show_disk(&ctrl, step == Step::Disk);
        self.rows.show(&ctrl, step == Step::Partitions);
        self.options.show(&ctrl, step == Step::Options);
        self.show_review(&ctrl, step == Step::Review);
        self.show_build(&ctrl, step == Step::Build);

        let blocked = ctrl.next_blocked();
        let running = ctrl.is_running();
        let free = self.busy.get().is_none();
        ui::set_visible(self.back, step != Step::Kind && step != Step::Build);
        ui::set_enabled(self.back, !running);
        ui::set_visible(self.next, step != Step::Review && step != Step::Build);
        ui::set_enabled(self.next, blocked.is_none());
        ui::set_visible(self.create, step == Step::Review);
        ui::set_enabled(self.create, blocked.is_none() && free);
        let why = blocked.or_else(|| (step == Step::Review).then_some(notice).flatten());
        set_line(
            self.blocked,
            why.as_deref().filter(|_| step != Step::Build),
            None,
        );
    }

    fn show_output(&self, ctrl: &BuildDiskController, on: bool) {
        let device = ctrl.output_is_device();
        let cd = ctrl.kind() == DiskKind::Cd;
        ui::set_visible(self.out_caption, on && (!cd || device));
        ui::set_visible(self.out_kind.h, on && (!cd || device));
        self.out_kind.select(usize::from(device));
        for h in [self.image_caption, self.image, self.pick_image] {
            ui::set_visible(h, on && !device);
        }
        let image = ctrl.image().map(|p| p.display().to_string());
        ui::set_text(self.image, image.as_deref().unwrap_or("(not chosen)"));
        for h in [self.container_caption, self.container.h] {
            ui::set_visible(h, on && !device && !cd);
        }
        let at = self.formats.iter().position(|c| *c == ctrl.container());
        self.container.select(at.unwrap_or(0));
        for h in [self.device_caption, self.device.h] {
            ui::set_visible(h, on && device);
        }
        let devices = self.devices.borrow();
        let mut items = vec![if devices.is_empty() {
            no_devices_hint().to_string()
        } else {
            "Select a device...".to_string()
        }];
        items.extend(devices.iter().map(device_label));
        self.device.set_items(&items);
        self.device.select(ctrl.device_row().map_or(0, |r| r + 1));
        set_line(
            self.device_warning,
            (on && device).then_some(hints::DEVICE_WARNING),
            Some(Tone::Warning),
        );
    }

    fn show_disk(&self, ctrl: &BuildDiskController, on: bool) {
        let d = &self.disk;
        let kind = ctrl.kind();
        let part = on && kind == DiskKind::Partitioned;
        let vol = on && kind == DiskKind::Volume;
        let preset = on && kind == DiskKind::Preset;
        let cd = on && kind == DiskKind::Cd;
        let view = ctrl.table_view();

        for h in [d.donor, d.donor_blank] {
            ui::set_visible(h, part);
        }
        ui::set_enabled(d.donor_blank, ctrl.donor().is_some());
        let donor = if ctrl.donor_reading() {
            Some(("Reading the disk...".to_string(), None))
        } else if let Some(e) = ctrl.donor_error() {
            Some((e.to_string(), Some(Tone::Error)))
        } else {
            ctrl.donor()
                .map(|(p, t)| (format!("From {} ({t})", p.display()), None))
        };
        let (text, tone) = donor.filter(|_| part).unzip();
        set_line(d.donor_line, text.as_deref(), tone.flatten());

        for h in [d.table_caption, d.table.h] {
            ui::set_visible(h, part);
        }
        let labels: Vec<String> = view.kinds.iter().map(|k| k.label().to_string()).collect();
        d.table.set_items(&labels);
        let at = view.kinds.iter().position(|k| *k == view.kind);
        d.table.select(at.unwrap_or(0));
        *d.kinds.borrow_mut() = view.kinds.to_vec();
        ui::set_enabled(d.table.h, !view.fixed);
        for h in [d.align_caption, d.align] {
            ui::set_visible(h, part);
        }
        set_field(d.align, &view.align_text);
        let geo = part && view.geometry.is_some();
        for h in [d.heads_caption, d.heads, d.sectors_caption, d.sectors] {
            ui::set_visible(h, geo);
        }
        if let Some((heads, sectors)) = view.geometry {
            set_field(d.heads, &heads.to_string());
            set_field(d.sectors, &sectors.to_string());
        }

        for h in [d.volume_caption, d.volume.h] {
            ui::set_visible(h, vol);
        }
        let at = ctrl.volume_fs_choice().index_in(&FsChoice::all());
        d.volume.select(at.unwrap_or(0));

        // The size is set here unless a device fixes it (Output) or the preset sizes itself.
        let sized = (part || vol || cd) && !ctrl.output_is_device();
        let fixed = vol && ctrl.volume_size_range().is_none();
        for h in [d.size_caption, d.size] {
            ui::set_visible(h, sized && !fixed);
        }
        ui::set_enabled(d.size, !(cd && ctrl.cd_auto_size()));
        set_field(d.size, ctrl.size_text());
        let hint = if fixed {
            Some(hints::FIXED_SIZE.to_string())
        } else if vol {
            ctrl.volume_size_range()
        } else if cd {
            Some(hints::CD_SIZE.to_string())
        } else {
            Some(hints::SIZE.to_string())
        };
        set_line(d.size_hint, hint.as_deref().filter(|_| sized), None);

        let p = ctrl.preset();
        for h in [d.preset_caption, d.preset.h] {
            ui::set_visible(h, preset);
        }
        let at = PresetKind::ALL.iter().position(|k| *k == p.kind);
        d.preset.select(at.unwrap_or(0));
        let x68k = preset && p.kind == PresetKind::X68k;
        for h in [
            d.x68k_scsi.h,
            d.x68k_parts_caption,
            d.x68k_parts,
            d.system_line,
            d.system_choose,
            d.system_clear,
        ] {
            ui::set_visible(h, x68k);
        }
        d.x68k_scsi.select(usize::from(p.x68k_scsi));
        set_field(d.x68k_parts, &p.x68k_partitions.to_string());
        let system = p.x68k_system_disk.as_ref().map(|s| s.display().to_string());
        ui::set_text(
            d.system_line,
            &format!(
                "System disk: {}",
                system.as_deref().unwrap_or(hints::NO_SYSTEM_DISK)
            ),
        );
        let sgi = preset && p.kind == PresetKind::SgiEfs;
        for h in [d.sgi_caption, d.sgi_label] {
            ui::set_visible(h, sgi);
        }
        set_field(d.sgi_label, &p.sgi_label);

        let c = ctrl.cd();
        for h in [
            d.cd_layout_caption,
            d.cd_layout.h,
            d.cd_name_caption,
            d.cd_name,
            d.cd_auto,
            d.cd_folder,
            d.cd_choose,
            d.cd_clear,
            d.cd_sidecars_caption,
            d.cd_sidecars.h,
        ] {
            ui::set_visible(h, cd);
        }
        let at = CdLayout::ALL.iter().position(|l| *l == c.layout);
        d.cd_layout.select(at.unwrap_or(0));
        set_field(d.cd_name, &c.name);
        set_check(d.cd_auto, ctrl.cd_auto_size());
        let folder = c.from_dir.as_ref().map(|f| f.display().to_string());
        ui::set_text(
            d.cd_folder,
            &format!("Fill from: {}", folder.as_deref().unwrap_or("(blank disc)")),
        );
        ui::set_enabled(d.cd_clear, c.from_dir.is_some());
        for row in ctrl.cd_options() {
            if let Some((_, h)) = d.cd_options.iter().find(|(o, _)| *o == row.option) {
                ui::set_visible(*h, cd);
                set_check(*h, row.on);
                ui::set_enabled(*h, row.unavailable.is_none());
            }
        }
        let choices = CdSidecars::choices(c.layout);
        let names: Vec<String> = choices.iter().map(|s| s.label().to_string()).collect();
        d.cd_sidecars.set_items(&names);
        let at = choices.iter().position(|s| *s == ctrl.cd_sidecars());
        d.cd_sidecars.select(at.unwrap_or(0));
        ui::set_enabled(d.cd_sidecars.h, ctrl.cd_sidecars_unavailable().is_none());
        *d.cd_sidecar_choices.borrow_mut() = choices.to_vec();
    }

    fn show_review(&self, ctrl: &BuildDiskController, on: bool) {
        let lines = ctrl.review_lines().join("\r\n");
        set_line(self.review, on.then_some(lines.as_str()), None);
        let warnings = ctrl.warnings();
        let tone = warnings
            .iter()
            .any(|w| w.starts_with("Warning:"))
            .then_some(Tone::Warning);
        let text = warnings.join("\r\n");
        let shown = (on && !warnings.is_empty()).then_some(text.as_str());
        set_line(self.warnings, shown, tone);
        let want = ctrl.confirm_expected().filter(|_| on);
        let erased = want
            .as_ref()
            .map(|w| format!("Everything on {w} will be erased."));
        set_line(self.danger, erased.as_deref(), Some(Tone::Error));
        let title = want.as_ref().map(|w| format!("Type `{w}` to confirm:"));
        set_line(self.confirm_caption, title.as_deref(), None);
        ui::set_visible(self.confirm, want.is_some());
        set_field(self.confirm, ctrl.confirm_text());
    }

    fn show_build(&self, ctrl: &BuildDiskController, on: bool) {
        let job = ctrl.progress().filter(|p| p.is_running());
        let (line, pct) = job
            .as_ref()
            .map(crate::inspect::job_line)
            .unwrap_or_default();
        ui::set_text(self.job, &line);
        ui::set_visible(self.job, on && job.is_some());
        crate::inspect::set_progress(self.job_progress, on && job.is_some(), pct);
        ui::set_visible(self.cancel, on && job.is_some());
        let outcome = match ctrl.outcome() {
            Some(Ok(())) => Some((labels::DONE.to_string(), None)),
            Some(Err(e)) => Some((format!("Error: {e}"), Some(Tone::Error))),
            None => None,
        };
        let (text, tone) = outcome.filter(|_| on).unzip();
        set_line(self.outcome, text.as_deref(), tone.flatten());
        let done = on && job.is_none();
        ui::set_visible(self.another, done);
        ui::set_visible(self.open, done && ctrl.built_image().is_some());
    }

    /// What decides the layout: every control's visibility and the rows' and options' shapes.
    fn shape_now(&self) -> Vec<usize> {
        let mut v: Vec<usize> = self
            .controls()
            .iter()
            .map(|h| usize::from(ui::shown(*h)))
            .collect();
        v.extend(self.rows.shape());
        v.extend(self.options.shape());
        for h in [self.review, self.warnings, self.donor_line()] {
            v.push(ui::text(h).lines().count());
        }
        v
    }

    fn donor_line(&self) -> HWND {
        self.disk.donor_line
    }

    fn controls(&self) -> Vec<HWND> {
        let d = &self.disk;
        let mut v = vec![
            self.busy_note,
            self.kind.h,
            self.out_kind.h,
            self.image,
            self.container.h,
            self.device.h,
            self.device_warning,
            d.donor,
            d.donor_line,
            d.table.h,
            d.volume.h,
            d.size,
            d.size_hint,
            d.align,
            d.heads,
            d.preset.h,
            d.x68k_scsi.h,
            d.sgi_label,
            d.cd_layout.h,
            self.review,
            self.warnings,
            self.danger,
            self.confirm,
            self.job,
            self.cancel,
            self.outcome,
            self.another,
            self.open,
            self.blocked,
            self.back,
            self.next,
            self.create,
        ];
        v.extend(d.cd_options.iter().map(|(_, h)| *h));
        v
    }

    fn layout(&self) {
        *self.shape.borrow_mut() = self.shape_now();
        let (area, dpi) = panel::client(self.page);
        let row_h = Size::Fixed(layout::ROW);
        let lines = |h: HWND| {
            let n = ui::text(h).lines().count().max(1) as i32;
            Node::leaf(h, Size::Stretch(1), Size::Fixed((layout::ROW - 8) * n))
        };
        let fixed = |h: HWND, w: i32| Node::leaf(h, Size::Fixed(w), row_h);
        let stretch = |h: HWND| Node::leaf(h, Size::Stretch(1), row_h);
        let shown = |h: HWND| ui::shown(h);
        let cap = 130;
        let pair = |c: HWND, f: Node<HWND>| Node::row(vec![fixed(c, cap), f]);
        let d = &self.disk;
        let mut col = Vec::new();
        if shown(self.busy_note) {
            col.push(lines(self.busy_note));
        }
        col.push(Node::row(vec![stretch(self.step_line)]));
        if shown(self.kind.h) {
            col.push(pair(self.kind_caption, stretch(self.kind.h)));
            col.push(lines(self.kind_hint));
        }
        if shown(self.out_kind.h) {
            col.push(pair(self.out_caption, fixed(self.out_kind.h, 200)));
        }
        if shown(self.image) {
            col.push(Node::row(vec![
                fixed(self.image_caption, cap),
                stretch(self.image),
                Node::button(self.pick_image, 90),
            ]));
        }
        if shown(self.container.h) {
            col.push(pair(self.container_caption, fixed(self.container.h, 300)));
        }
        if shown(self.device.h) {
            col.push(pair(self.device_caption, stretch(self.device.h)));
        }
        if shown(self.device_warning) {
            col.push(lines(self.device_warning));
        }
        if shown(d.donor) {
            col.push(Node::row(vec![
                Node::button(d.donor, 220),
                Node::button(d.donor_blank, 100),
                Node::Space(Size::Stretch(1)),
            ]));
        }
        if shown(d.donor_line) {
            col.push(lines(d.donor_line));
        }
        if shown(d.table.h) {
            col.push(pair(d.table_caption, fixed(d.table.h, 200)));
        }
        if shown(d.volume.h) {
            col.push(pair(d.volume_caption, fixed(d.volume.h, 280)));
        }
        if shown(d.preset.h) {
            col.push(pair(d.preset_caption, fixed(d.preset.h, 360)));
        }
        if shown(d.x68k_scsi.h) {
            col.push(Node::row(vec![
                Node::Space(Size::Fixed(cap)),
                fixed(d.x68k_scsi.h, 220),
                fixed(d.x68k_parts_caption, 150),
                fixed(d.x68k_parts, 50),
                Node::Space(Size::Stretch(1)),
            ]));
            col.push(Node::row(vec![
                Node::Space(Size::Fixed(cap)),
                stretch(d.system_line),
                Node::button(d.system_choose, 85),
                Node::button(d.system_clear, 70),
            ]));
        }
        if shown(d.sgi_label) {
            col.push(pair(d.sgi_caption, fixed(d.sgi_label, 120)));
        }
        if shown(d.cd_layout.h) {
            col.push(pair(d.cd_layout_caption, fixed(d.cd_layout.h, 360)));
            col.push(pair(d.cd_name_caption, fixed(d.cd_name, 200)));
            col.push(Node::row(vec![
                Node::Space(Size::Fixed(cap)),
                stretch(d.cd_auto),
            ]));
        }
        if shown(d.size) {
            col.push(Node::row(vec![
                fixed(d.size_caption, cap),
                fixed(d.size, 120),
                stretch(d.size_hint),
            ]));
        } else if shown(d.size_hint) {
            col.push(Node::row(vec![
                Node::Space(Size::Fixed(cap)),
                stretch(d.size_hint),
            ]));
        }
        if shown(d.align) {
            let mut row = vec![fixed(d.align_caption, cap), fixed(d.align, 120)];
            if shown(d.heads) {
                row.extend([
                    fixed(d.heads_caption, 60),
                    fixed(d.heads, 60),
                    fixed(d.sectors_caption, 110),
                    fixed(d.sectors, 60),
                ]);
            }
            row.push(Node::Space(Size::Stretch(1)));
            col.push(Node::row(row));
        }
        if shown(d.cd_folder) {
            col.push(Node::row(vec![
                Node::Space(Size::Fixed(cap)),
                stretch(d.cd_folder),
                Node::button(d.cd_choose, 130),
                Node::button(d.cd_clear, 70),
            ]));
            for pair_of in d.cd_options.chunks(2) {
                let mut row = vec![Node::Space(Size::Fixed(cap))];
                row.extend(pair_of.iter().map(|(_, h)| stretch(*h)));
                col.push(Node::row(row));
            }
            col.push(pair(d.cd_sidecars_caption, fixed(d.cd_sidecars.h, 220)));
        }
        col.extend(self.rows.nodes());
        col.extend(self.options.nodes());
        for h in [self.review, self.warnings, self.danger] {
            if shown(h) {
                col.push(lines(h));
            }
        }
        if shown(self.confirm) {
            col.push(Node::row(vec![
                fixed(self.confirm_caption, 260),
                fixed(self.confirm, 200),
                Node::Space(Size::Stretch(1)),
            ]));
        }
        if shown(self.job) {
            col.push(Node::row(vec![
                stretch(self.job),
                Node::leaf(
                    self.job_progress,
                    Size::Fixed(200),
                    Size::Fixed(layout::ROW - 6),
                ),
                Node::button(self.cancel, 80),
            ]));
        }
        if shown(self.outcome) {
            col.push(lines(self.outcome));
        }
        if shown(self.another) {
            let mut row = vec![Node::button(self.another, 120)];
            if shown(self.open) {
                row.push(Node::button(self.open, 130));
            }
            row.push(Node::Space(Size::Stretch(1)));
            col.push(Node::row(row));
        }
        col.push(Node::Space(Size::Stretch(1)));
        let mut foot = Vec::new();
        if shown(self.back) {
            foot.push(Node::button(self.back, 80));
        }
        foot.push(stretch(self.blocked));
        for (h, w) in [(self.next, 90), (self.create, 120)] {
            if shown(h) {
                foot.push(Node::button(h, w));
            }
        }
        col.push(Node::row(foot));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in layout::layout(&tree, panel::scroll_area(self.page, &tree, area, dpi), dpi) {
            if ui::shown(h) {
                ui::move_to(h, r);
            }
        }
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

    fn selected(&self, from: HWND) -> Option<BuildDiskAction> {
        let d = &self.disk;
        if from == self.kind.h {
            return Some(BuildDiskAction::SetKind(
                *DiskKind::ALL.get(self.kind.picked()?)?,
            ));
        }
        if from == self.out_kind.h {
            return Some(BuildDiskAction::SetOutputIsDevice(
                self.out_kind.picked()? == 1,
            ));
        }
        if from == self.container.h {
            return Some(BuildDiskAction::SetContainer(
                *self.formats.get(self.container.picked()?)?,
            ));
        }
        if from == self.device.h {
            let Some(row) = self.device.picked()?.checked_sub(1) else {
                return Some(BuildDiskAction::ClearDevice);
            };
            let device = self.devices.borrow().get(row).cloned()?;
            return Some(BuildDiskAction::SelectDevice { row, device });
        }
        if from == d.table.h {
            let kind = *d.kinds.borrow().get(d.table.picked()?)?;
            return Some(BuildDiskAction::Table(BuildAction::SetKind(kind)));
        }
        if from == d.volume.h {
            return Some(BuildDiskAction::SetVolumeFs(
                *FsChoice::all().get(d.volume.picked()?)?,
            ));
        }
        if from == d.preset.h {
            let kind = *PresetKind::ALL.get(d.preset.picked()?)?;
            return Some(self.preset_edit(|p| p.kind = kind));
        }
        if from == d.x68k_scsi.h {
            let scsi = d.x68k_scsi.picked()? == 1;
            return Some(self.preset_edit(|p| p.x68k_scsi = scsi));
        }
        if from == d.cd_layout.h {
            let layout = *CdLayout::ALL.get(d.cd_layout.picked()?)?;
            return Some(self.cd_edit(|c| c.layout = layout));
        }
        if from == d.cd_sidecars.h {
            let s = *d.cd_sidecar_choices.borrow().get(d.cd_sidecars.picked()?)?;
            return Some(BuildDiskAction::SetCdSidecars(s));
        }
        None
    }

    fn changed(&self, from: HWND) -> Option<BuildDiskAction> {
        let d = &self.disk;
        let text = ui::text(from);
        if from == d.size {
            return Some(BuildDiskAction::SetSize(text));
        }
        if from == d.align {
            return Some(BuildDiskAction::Table(BuildAction::SetAlign(text)));
        }
        if from == d.heads {
            let n = text.trim().parse().ok()?;
            return Some(BuildDiskAction::Table(BuildAction::SetHeads(n)));
        }
        if from == d.sectors {
            let n = text.trim().parse().ok()?;
            return Some(BuildDiskAction::Table(BuildAction::SetSectorsPerTrack(n)));
        }
        if from == d.x68k_parts {
            let n: usize = text.trim().parse().ok().filter(|n| (1..=8).contains(n))?;
            return Some(self.preset_edit(|p| p.x68k_partitions = n));
        }
        if from == d.sgi_label {
            return Some(self.preset_edit(|p| p.sgi_label = text));
        }
        if from == d.cd_name {
            return Some(self.cd_edit(|c| c.name = text));
        }
        if from == self.confirm {
            return Some(BuildDiskAction::SetConfirmText(text));
        }
        None
    }

    fn clicked(&self, from: HWND) -> Option<BuildDiskAction> {
        let d = &self.disk;
        if let Some((o, _)) = d.cd_options.iter().find(|(_, h)| *h == from) {
            return Some(BuildDiskAction::SetCdOption(*o, checked(from)));
        }
        let action = if from == self.pick_image {
            BuildDiskAction::PickOutputImage
        } else if from == d.donor {
            BuildDiskAction::PickDonor
        } else if from == d.donor_blank {
            BuildDiskAction::ClearDonor
        } else if from == d.system_choose {
            BuildDiskAction::PickSystemDisk
        } else if from == d.system_clear {
            self.preset_edit(|p| p.x68k_system_disk = None)
        } else if from == d.cd_auto {
            let auto = checked(from);
            // Sizing to the folder is the size text `auto`; off goes back to a CD-R's size.
            let text = if auto { CD_AUTO_SIZE } else { "600M" };
            BuildDiskAction::SetSize(text.to_string())
        } else if from == d.cd_choose {
            BuildDiskAction::PickCdFolder
        } else if from == d.cd_clear {
            self.cd_edit(|c| c.from_dir = None)
        } else if from == self.back {
            BuildDiskAction::Back
        } else if from == self.next {
            BuildDiskAction::Next
        } else if from == self.create {
            // Create asks the controller again at the click.
            if self.ctrl.borrow().next_blocked().is_some() || self.busy.get().is_some() {
                return None;
            }
            BuildDiskAction::Create
        } else if from == self.cancel {
            BuildDiskAction::Cancel
        } else if from == self.another {
            BuildDiskAction::BuildAnother
        } else if from == self.open {
            *self.open_inspect.borrow_mut() = self.ctrl.borrow().built_image().map(PathBuf::from);
            // No action reaches the app loop from here, so wake it to take the request.
            self.notify.wake();
            return None;
        } else {
            return None;
        };
        Some(action)
    }
}

impl Handler for BuildScreen {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        let action = {
            let ctrl = self.ctrl.borrow();
            self.rows
                .command(&ctrl, from, code)
                .or_else(|| self.options.command(&ctrl, from, code))
        };
        let action = action.or_else(|| match code {
            CBN_SELCHANGE => self.selected(from),
            EN_CHANGE => self.changed(from),
            BN_CLICKED => self.clicked(from),
            _ => None,
        });
        if let Some(a) = action {
            self.apply(a);
        }
    }

    fn notify(&self, _hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {}
}
