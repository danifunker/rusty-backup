//! The Restore screen over `controller::restore::RestoreController`; GTK's `restore/` is the reference.
//!
//! Slice 4: the four modes, the backup folder (a split button listing recent folders) or a
//! partition image, each mode's target (a device, an image file or a scanned partition), the new
//! disk's table, the full-disk sizes with alignment and trailing free space, the caveats, and
//! Restore / Cancel with the progress line and the outcome. Start only ever sends `Start`: the
//! write begins on the controller's own `Confirm`. Write Image File opens the disk writer
//! window. Still to come: remote targets (slice 7b).

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::{
    busy_elsewhere_notice, no_devices_hint, DiskDevice, Notify, RecentHandle, RecentMode, Snapshot,
};
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::inspect::{LayoutSegment, SegmentKind};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::restore::{
    device_label, format_size, hints, ids, labels, AlignmentChoice, CaveatLevel, ExpandPreview,
    HostRequest, JobRegistry, NewDiskChoice, NewTableType, PlanSegment, RestoreAction,
    RestoreController, RestoreMode, RestoreServices, SizeMode, SizeRow, SourcePartition,
    TargetPartition, TargetSlot, ALIGN_SECTORS_MAX, MODES,
};
use rusty_backup::controller::{Controller, Effect};
use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, RECT, WPARAM};
use windows::Win32::UI::Controls::{BCN_DROPDOWN, BS_SPLITBUTTON, NMHDR};
use windows::Win32::UI::Input::KeyboardAndMouse::GetFocus;
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, GetAncestor, GetWindowRect, SendMessageW, BM_GETCHECK, BM_SETCHECK, BN_CLICKED,
    CBN_SELCHANGE, EN_CHANGE, ES_NUMBER, GA_ROOT, WINDOW_EX_STYLE, WINDOW_STYLE, WS_EX_CLIENTEDGE,
    WS_TABSTOP,
};

use crate::combo::Combo;
use crate::disk_write::DiskWriteWindow;
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::partition_bar;
use crate::sync::SyncGuard;
use crate::ui;

const KINDS: [&str; 2] = ["Write to device", "Save as image file"];
const TABLES: [(NewTableType, &str); 3] = [
    (NewTableType::Mbr, "MBR"),
    (NewTableType::Gpt, "GPT"),
    (NewTableType::Apm, "APM"),
];
const NEW_ALIGNMENTS: [(AlignmentChoice, &str); 3] = [
    (AlignmentChoice::Modern1MB, "1 MB (LBA 2048)"),
    (AlignmentChoice::Original, "DOS (LBA 63)"),
    (AlignmentChoice::Custom, "Custom"),
];
const ALIGNMENTS: [AlignmentChoice; 3] = [
    AlignmentChoice::Original,
    AlignmentChoice::Modern1MB,
    AlignmentChoice::Custom,
];
const MIB: u64 = 1024 * 1024;

/// The target a mode writes; Write Image File has its own window.
fn slot_of(mode: RestoreMode) -> Option<TargetSlot> {
    match mode {
        RestoreMode::FullDisk => Some(TargetSlot::FullDisk),
        RestoreMode::SinglePartition => Some(TargetSlot::SinglePartition),
        RestoreMode::NewDisk => Some(TargetSlot::NewDisk),
        RestoreMode::WriteImage => None,
    }
}

/// The strongest tone among controller notes.
fn tone_of(levels: impl Iterator<Item = CaveatLevel>) -> Option<Tone> {
    let rank = |l: CaveatLevel| match l {
        CaveatLevel::Info => 0,
        CaveatLevel::Warning => 1,
        CaveatLevel::Blocked => 2,
    };
    match levels.map(rank).max() {
        Some(2) => Some(Tone::Error),
        Some(1) => Some(Tone::Warning),
        _ => None,
    }
}

fn segments(list: &[PlanSegment]) -> Vec<LayoutSegment> {
    let each = list
        .iter()
        .enumerate()
        .map(|(color_index, s)| LayoutSegment {
            label: s.label.clone(),
            fs: s.fs.clone(),
            size_bytes: s.size_bytes,
            kind: SegmentKind::Partition { color_index },
            partition: None,
        });
    each.collect()
}

fn checked(h: HWND) -> bool {
    // SAFETY: a plain button query.
    unsafe { SendMessageW(h, BM_GETCHECK, None, None).0 == 1 }
}

fn set_check(h: HWND, on: bool) {
    if checked(h) != on {
        // SAFETY: a plain button message; it sends no BN_CLICKED.
        unsafe {
            SendMessageW(h, BM_SETCHECK, Some(WPARAM(on as usize)), None);
        }
    }
}

fn number(parent: HWND, id: &str) -> HWND {
    let style = WINDOW_STYLE(ES_NUMBER as u32 | WS_TABSTOP.0);
    ui::child(parent, w!("EDIT"), "", style, WS_EX_CLIENTEDGE, id)
}

/// One partition's row in the full-disk size table.
struct SizeCells {
    index: usize,
    number: HWND,
    kind: HWND,
    original: HWND,
    minimum: HWND,
    mode: Combo,
    choices: Vec<SizeMode>,
    size: HWND,
}

impl SizeCells {
    fn build(page: HWND, pos: usize, row: &SizeRow, choices: Vec<SizeMode>) -> SizeCells {
        let index = row.index;
        let names: Vec<&str> = choices.iter().map(|m| m.label()).collect();
        SizeCells {
            index,
            number: ui::cell_label(page, &(pos + 1).to_string()),
            kind: ui::cell_label(page, &ui::plain(&row.type_name)),
            original: ui::cell_label(page, &format_size(row.original_size)),
            minimum: ui::cell_label(page, &format_size(row.minimum_size)),
            mode: Combo::build(page, &format!("{}.{index}", ids::ROW_MODE), &names),
            choices,
            size: number(page, &format!("{}.{index}", ids::ROW_SIZE)),
        }
    }

    fn all(&self) -> [HWND; 6] {
        [
            self.number,
            self.kind,
            self.original,
            self.minimum,
            self.mode.h,
            self.size,
        ]
    }
}

/// Everything a refresh shows, read while the controller is borrowed.
struct Shown {
    mode: RestoreMode,
    job: Option<Snapshot>,
    folder: Option<String>,
    image: Option<String>,
    summary: Option<(String, String)>,
    native: bool,
    sources: Vec<SourcePartition>,
    source: Option<usize>,
    load_error: Option<String>,
    write_path: Option<String>,
    write_line: Option<String>,
    is_device: bool,
    device_row: Option<usize>,
    has_device: bool,
    target_image: Option<String>,
    warning: Option<&'static str>,
    readout: Option<(Option<Tone>, String)>,
    scanning: bool,
    parts: Vec<TargetPartition>,
    part: Option<usize>,
    scan_error: Option<String>,
    new_disk: NewDiskChoice,
    start_lba: u64,
    has_backup: bool,
    alignment: (AlignmentChoice, u32),
    original_label: String,
    rows: Vec<(SizeRow, Vec<SizeMode>)>,
    notes: Option<(Option<Tone>, String)>,
    expand_shown: bool,
    expand: rusty_backup::controller::restore::ExpandPlan,
    expand_line: String,
    preview: ExpandPreview,
    mac_driver: Option<bool>,
    caveats: Option<(Option<Tone>, String)>,
    write_target: Option<String>,
    running: bool,
    cancelling: bool,
    can_start: bool,
    why: Option<String>,
    outcome: Option<Result<(), String>>,
    run: Option<String>,
    pct: Option<u32>,
}

pub struct RestoreScreen {
    ctrl: RefCell<RestoreController>,
    host: EffectHost,
    /// Write Image File's window, kept while open so a running write keeps its controller.
    writer: RefCell<Option<Rc<DiskWriteWindow>>>,
    notify: Arc<dyn Notify>,
    recent: RecentHandle,
    recent_files: RefCell<Vec<String>>,
    devices: RefCell<Vec<DiskDevice>>,
    /// Another tab's disk job, which locks this one.
    busy: Cell<Option<&'static str>>,
    sync: SyncGuard,
    me: Weak<RestoreScreen>,
    page: HWND,
    busy_note: HWND,
    mode_caption: HWND,
    mode: Combo,
    job: HWND,
    job_progress: HWND,
    folder_caption: HWND,
    folder: HWND,
    image_caption: HWND,
    image: HWND,
    pick_image: HWND,
    source_caption: HWND,
    source: Combo,
    summary: HWND,
    load_error: HWND,
    write_caption: HWND,
    write_path: HWND,
    write_choose: HWND,
    write_line: HWND,
    write_target: HWND,
    kind_caption: HWND,
    kind: Combo,
    device: Combo,
    close_target: HWND,
    scan: HWND,
    target_image: HWND,
    save_as: HWND,
    warning: HWND,
    readout: HWND,
    part_caption: HWND,
    part: Combo,
    scan_error: HWND,
    table_caption: HWND,
    table: Combo,
    new_align_caption: HWND,
    new_align: Combo,
    new_custom: HWND,
    bootable: HWND,
    start_lba: HWND,
    expand: HWND,
    free_caption: HWND,
    free: HWND,
    extend: Combo,
    expand_line: HWND,
    current_caption: HWND,
    current: HWND,
    after_caption: HWND,
    after: HWND,
    align_caption: HWND,
    align: Combo,
    custom: HWND,
    heads: [HWND; 6],
    cells: RefCell<Vec<SizeCells>>,
    notes: HWND,
    mac_driver: HWND,
    caveats: HWND,
    target_line: HWND,
    outcome: HWND,
    run: HWND,
    progress: HWND,
    start: HWND,
    cancel: HWND,
    why: HWND,
    shape: RefCell<Vec<usize>>,
}

impl RestoreScreen {
    pub fn build(
        page: HWND,
        notify: Arc<dyn Notify>,
        recent: RecentHandle,
        host: EffectHost,
    ) -> Rc<RestoreScreen> {
        let split = WINDOW_STYLE(BS_SPLITBUTTON as u32 | WS_TABSTOP.0);
        let folder = ui::child(
            page,
            w!("BUTTON"),
            "",
            split,
            WINDOW_EX_STYLE(0),
            ids::BACKUP_FOLDER,
        );
        let progress = || {
            ui::child(
                page,
                w!("msctls_progress32"),
                "",
                WINDOW_STYLE(0),
                WINDOW_EX_STYLE(0),
                "",
            )
        };
        let modes: Vec<&str> = MODES.iter().map(|m| m.1).collect();
        let tables: Vec<&str> = TABLES.iter().map(|t| t.1).collect();
        let new_aligns: Vec<&str> = NEW_ALIGNMENTS.iter().map(|a| a.1).collect();
        let heads = [
            "#",
            "Type",
            "Original",
            "Minimum",
            "Size Mode",
            "Size (MiB)",
        ]
        .map(|t| ui::cell_label(page, t));
        let current = partition_bar::build(page, Box::new(|_| {}));
        let after = partition_bar::build(page, Box::new(|_| {}));
        partition_bar::set_options(current, 1.0, false);
        partition_bar::set_options(after, 1.0, true);
        let screen = Rc::new_cyclic(|me: &Weak<RestoreScreen>| RestoreScreen {
            ctrl: RefCell::new(RestoreController::new(RestoreServices {
                jobs: JobRegistry::new(),
                notify: notify.clone(),
            })),
            host,
            writer: RefCell::default(),
            notify,
            recent_files: RefCell::new(recent.list(RecentMode::Restore)),
            recent,
            devices: RefCell::default(),
            busy: Cell::new(None),
            sync: SyncGuard::default(),
            me: me.clone(),
            page,
            busy_note: ui::label(page, ""),
            mode_caption: ui::cell_label(page, "Mode:"),
            mode: Combo::build(page, ids::MODE, &modes),
            job: ui::cell_label(page, ""),
            job_progress: progress(),
            folder_caption: ui::cell_label(page, &format!("{}:", labels::BACKUP_FOLDER)),
            folder,
            image_caption: ui::cell_label(page, "Or Image File:"),
            image: ui::path_label(page, ""),
            pick_image: ui::button(page, "Browse...", ids::PICK_SOURCE_IMAGE),
            source_caption: ui::cell_label(page, "Source Partition:"),
            source: Combo::build(page, ids::SOURCE_PARTITION, &[]),
            summary: ui::path_label(page, ""),
            load_error: ui::label(page, ""),
            write_caption: ui::cell_label(page, "Image File:"),
            write_path: ui::path_label(page, ""),
            write_choose: ui::button(page, "Choose...", ids::WRITE_IMAGE),
            write_line: ui::label(page, ""),
            write_target: ui::button(page, "Choose target and write...", ids::WRITE_TARGET),
            kind_caption: ui::cell_label(page, &format!("{}:", labels::TARGET_KIND)),
            kind: Combo::build(page, ids::TARGET_KIND, &KINDS),
            device: Combo::build(page, ids::TARGET_DEVICE, &[]),
            close_target: ui::button(page, "Close Drive", ids::CLOSE_TARGET),
            scan: ui::button(page, "Scan Target", ids::SCAN_TARGET),
            target_image: ui::edit(page, ids::TARGET_IMAGE),
            save_as: ui::button(page, "Save As...", ids::PICK_TARGET_IMAGE),
            warning: ui::label(page, ""),
            readout: ui::label(page, ""),
            part_caption: ui::cell_label(page, "Target Partition:"),
            part: Combo::build(page, ids::TARGET_PARTITION, &[]),
            scan_error: ui::label(page, ""),
            table_caption: ui::cell_label(page, "Table Type:"),
            table: Combo::build(page, ids::NEW_DISK_TABLE, &tables),
            new_align_caption: ui::cell_label(page, "Alignment:"),
            new_align: Combo::build(page, ids::NEW_DISK_ALIGNMENT, &new_aligns),
            new_custom: number(page, ids::NEW_DISK_CUSTOM),
            bootable: ui::checkbox(page, "Mark partition as bootable", ids::NEW_DISK_BOOTABLE),
            start_lba: ui::cell_label(page, ""),
            expand: ui::checkbox(page, hints::EXPAND, ids::EXPAND),
            free_caption: ui::cell_label(page, "Free space (MiB):"),
            free: number(page, ids::EXPAND_MIB),
            extend: Combo::build(
                page,
                ids::EXPAND_EXTEND,
                &[hints::EXPAND_LEAVE, hints::EXPAND_EXTEND],
            ),
            expand_line: ui::cell_label(page, ""),
            current_caption: ui::cell_label(page, "Current:"),
            current,
            after_caption: ui::cell_label(page, ""),
            after,
            align_caption: ui::cell_label(page, "Partition starts:"),
            align: Combo::build(page, ids::ALIGNMENT, &[]),
            custom: number(page, ids::CUSTOM_ALIGNMENT),
            heads,
            cells: RefCell::default(),
            notes: ui::label(page, ""),
            mac_driver: ui::checkbox(page, hints::MAC_DRIVER, ids::MAC_DRIVER),
            caveats: ui::label(page, ""),
            target_line: ui::path_label(page, ""),
            outcome: ui::label(page, ""),
            run: ui::cell_label(page, ""),
            progress: progress(),
            start: ui::button(page, labels::START, ids::START),
            cancel: ui::button(page, "Cancel", ids::CANCEL),
            why: ui::cell_label(page, ""),
            shape: RefCell::default(),
        });
        let weak: Weak<dyn Handler> = screen.me.clone();
        panel::register(page, weak);
        screen.refresh();
        screen
    }

    /// The app's device list; a target's row is reconciled before anything shows it.
    pub fn set_devices(&self, devices: &[DiskDevice]) {
        self.ctrl.borrow_mut().reconcile_devices(devices);
        let writer = self.writer.borrow().clone();
        if let Some(d) = writer {
            d.set_devices(devices);
        }
        *self.devices.borrow_mut() = devices.to_vec();
    }

    /// Another tab's job, if any, which locks this tab's start controls.
    pub fn set_busy(&self, busy: Option<&'static str>) {
        self.busy.set(busy);
    }

    /// Settings cleared the recent files: the picker forgets its copy too.
    pub fn forget_recents(&self) {
        self.recent_files.borrow_mut().clear();
    }

    pub fn is_running(&self) -> bool {
        let writing = self
            .writer
            .borrow()
            .as_ref()
            .is_some_and(|d| d.is_writing());
        self.ctrl.borrow().is_running() || writing
    }

    /// The backup folder open here, for the share with Inspect.
    pub fn loaded_backup(&self) -> Option<PathBuf> {
        self.ctrl.borrow().backup_folder().map(Path::to_path_buf)
    }

    /// Nothing is open, so a shared backup may be adopted.
    pub fn is_empty(&self) -> bool {
        let ctrl = self.ctrl.borrow();
        !ctrl.has_backup() && !ctrl.has_other_source()
    }

    pub fn load_backup(&self, folder: &Path) {
        self.apply(RestoreAction::OpenBackup(folder.to_path_buf()));
    }

    pub fn clear_backup(&self) {
        self.apply(RestoreAction::ClearBackup);
    }

    pub fn apply(&self, action: RestoreAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let writer = self.writer.borrow().clone();
        if let Some(d) = writer {
            d.tick();
            if !d.is_open() {
                self.writer.replace(None);
            }
        }
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
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
            Reply::Picked { purpose, path } => RestoreAction::Picked { purpose, path },
            Reply::Confirmed { purpose, yes } => {
                // A job another tab started behind the question keeps this one from starting.
                let locked = self.busy.get().map(busy_elsewhere_notice);
                if let Some(why) = locked.as_ref().filter(|_| yes) {
                    crate::app::append_log(vec![(
                        LogLevel::Warning,
                        format!("Restore not started. {why}"),
                    )]);
                }
                RestoreAction::Confirmed {
                    purpose,
                    yes: yes && locked.is_none(),
                }
            }
            Reply::Answered { purpose, text } => RestoreAction::Answered { purpose, text },
            // The restore asks for no choice and has no screen to leave.
            Reply::Chose { .. } | Reply::CloseScreen => return,
        };
        self.apply(action);
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let (requests, log) = {
            let mut ctrl = self.ctrl.borrow_mut();
            (ctrl.take_requests(), ctrl.take_log())
        };
        for request in requests {
            match request {
                HostRequest::RememberRecent(path) => self.remember(&path),
                HostRequest::WriteImage(open) => self.open_writer(open),
            }
        }
        crate::app::append_log(log);
        // Off screen nothing needs it, and a job wakes this many times a second.
        if ui::shown(self.page) {
            self.refresh();
        }
    }

    /// Write Image File: the image goes to the disk writer's window, one at a time.
    fn open_writer(&self, open: rusty_backup::controller::restore::WriteOpen) {
        if self.writer.borrow().as_ref().is_some_and(|d| d.is_open()) {
            return;
        }
        // SAFETY: a plain query of our own window.
        let owner = unsafe { GetAncestor(self.page, GA_ROOT) };
        let d = DiskWriteWindow::open(
            owner,
            self.notify.clone(),
            self.host.clone(),
            &self.devices.borrow(),
            open,
        );
        *self.writer.borrow_mut() = Some(d);
    }

    fn remember(&self, path: &Path) {
        let list = self
            .recent
            .remember(RecentMode::Restore, &path.display().to_string());
        *self.recent_files.borrow_mut() = list;
    }

    /// Start's whole rule: asked again at the click, not only when the button was last drawn.
    fn may_start(&self) -> bool {
        self.ctrl.borrow().can_start() && self.busy.get().is_none()
    }

    /// The folder button's menu: recent backups, then Open Backup Folder.
    fn folder_menu(&self) {
        let recent = self.recent_files.borrow().clone();
        let mut entries: Vec<(String, bool)> = recent.iter().map(|p| (p.clone(), true)).collect();
        if !recent.is_empty() {
            entries.push((String::new(), false));
        }
        entries.push(("Open Backup Folder...".into(), true));
        let mut r = RECT::default();
        // SAFETY: plain queries of our own windows.
        let picked = unsafe {
            let _ = GetWindowRect(self.folder, &mut r);
            let owner = GetAncestor(self.page, GA_ROOT);
            crate::menu::choose(owner, &entries, r.left, r.bottom)
        };
        match picked {
            Some(i) if i < recent.len() => {
                self.apply(RestoreAction::OpenDropped(PathBuf::from(&recent[i])))
            }
            Some(i) if i == entries.len() - 1 => self.apply(RestoreAction::PickBackupFolder),
            _ => {}
        }
    }

    fn read(&self) -> Shown {
        let ctrl = self.ctrl.borrow();
        let mode = ctrl.mode();
        let slot = slot_of(mode).unwrap_or(TargetSlot::FullDisk);
        let shown_path = |p: Option<&Path>| p.map(|p| p.display().to_string());
        let job = [
            ctrl.decoding(),
            ctrl.loading(),
            ctrl.probing(),
            ctrl.target_scan(),
        ]
        .into_iter()
        .flatten()
        .next();
        let readout = ctrl
            .target_readout()
            .filter(|_| mode == RestoreMode::FullDisk)
            .map(|c| (tone_of(std::iter::once(c.level)), c.text));
        let notes = ctrl.size_notes();
        let notes = (!notes.is_empty()).then(|| {
            let text: Vec<String> = notes.iter().map(|n| n.text.clone()).collect();
            (tone_of(notes.iter().map(|n| n.level)), text.join("\n"))
        });
        let caveats = if slot_of(mode).is_some() {
            ctrl.size_caveats()
        } else {
            Vec::new()
        };
        let caveats = (!caveats.is_empty()).then(|| {
            let text: Vec<String> = caveats.iter().map(|c| c.line()).collect();
            (
                tone_of(caveats.iter().map(|c| c.caveat.level)),
                text.join("\n"),
            )
        });
        let rows = ctrl
            .size_rows()
            .iter()
            .map(|r| (r.clone(), ctrl.size_mode_choices(r.index)))
            .collect();
        let (run, pct) = match ctrl.progress() {
            Some(b) => {
                let pct = (b.total_bytes > 0)
                    .then(|| (b.current_bytes.saturating_mul(100) / b.total_bytes).min(100) as u32);
                let line = format!(
                    "{}: {} of {}",
                    b.operation,
                    format_size(b.current_bytes),
                    format_size(b.total_bytes)
                );
                (Some(line), pct)
            }
            None => (None, None),
        };
        Shown {
            mode,
            job,
            folder: shown_path(ctrl.backup_folder()),
            image: shown_path(ctrl.source_image()),
            summary: ctrl.backup_summary(),
            native: ctrl.has_native_backup(),
            sources: ctrl.source_partitions(),
            source: ctrl.source_partition(),
            load_error: ctrl.load_error().map(str::to_string),
            write_path: ctrl.write_image().map(|(p, _)| p.display().to_string()),
            write_line: ctrl.write_image_line(),
            is_device: ctrl.target_is_device(slot),
            device_row: ctrl.device_row(slot),
            has_device: ctrl.device(slot).is_some(),
            target_image: shown_path(ctrl.target_image(slot)),
            warning: ctrl.system_disk_warning(slot),
            readout,
            scanning: ctrl.target_scan().is_some(),
            parts: ctrl.target_partitions(),
            part: ctrl.target_partition(),
            scan_error: ctrl.target_scan_error().map(str::to_string),
            new_disk: ctrl.new_disk(),
            start_lba: ctrl.new_disk_start_lba(),
            has_backup: ctrl.has_backup(),
            alignment: ctrl.alignment(),
            original_label: ctrl.original_alignment_label(),
            rows,
            notes,
            expand_shown: ctrl.expand_shown(),
            expand: ctrl.expand(),
            expand_line: ctrl.expand_size_line(),
            preview: ctrl.expand_preview(),
            mac_driver: ctrl.mac_driver_missing().then(|| ctrl.install_mac_driver()),
            caveats,
            write_target: ctrl.write_target(),
            running: ctrl.is_running(),
            cancelling: ctrl.cancelling(),
            can_start: false,
            why: ctrl
                .validate()
                .err()
                .or_else(|| self.busy.get().map(busy_elsewhere_notice)),
            outcome: ctrl.last_result().cloned(),
            run,
            pct,
        }
    }

    /// Read the controller first and release it, then set the controls inside the guard.
    fn refresh(&self) {
        let mut shown = self.read();
        shown.can_start = self.may_start();
        self.sync.run(|| self.show(shown));
        if *self.shape.borrow() != self.shape_now() {
            self.layout();
        }
    }

    /// The field has the keyboard: what is typed there wins over the controller's copy.
    fn typing(&self, h: HWND) -> bool {
        // SAFETY: a plain query.
        unsafe { GetFocus() == h }
    }

    fn set_field(&self, h: HWND, text: &str) {
        if !self.typing(h) {
            ui::set_text(h, text);
        }
    }

    fn set_line(&self, h: HWND, text: Option<&str>, tone: Option<Tone>) {
        ui::set_text(h, text.unwrap_or(""));
        ui::set_visible(h, text.is_some());
        crate::theme::set_tone(h, tone);
    }

    fn show(&self, s: Shown) {
        let notice = self.busy.get().map(busy_elsewhere_notice);
        self.set_line(self.busy_note, notice.as_deref(), Some(Tone::Warning));
        let index = MODES.iter().position(|m| m.0 == s.mode).unwrap_or(0);
        self.mode.select(index);
        let (line, pct) = s
            .job
            .as_ref()
            .map(crate::inspect::job_line)
            .unwrap_or_default();
        ui::set_text(self.job, &line);
        ui::set_visible(self.job, s.job.is_some());
        crate::inspect::set_progress(self.job_progress, s.job.is_some(), pct);
        self.show_source(&s);
        self.show_target(&s);
        self.show_new_disk(&s);
        self.show_sizes(&s);
        let mac = s.mac_driver.is_some();
        ui::set_visible(self.mac_driver, mac && s.mode != RestoreMode::WriteImage);
        set_check(self.mac_driver, s.mac_driver.unwrap_or(false));
        let (tone, text) = s.caveats.clone().unzip();
        self.set_line(self.caveats, text.as_deref(), tone.flatten());
        self.show_footer(&s);
        let free = !s.running && self.busy.get().is_none();
        for h in self.form_controls() {
            ui::set_enabled(h, free);
        }
    }

    fn show_source(&self, s: &Shown) {
        let writing = s.mode == RestoreMode::WriteImage;
        let full = s.mode == RestoreMode::FullDisk;
        for h in [self.folder_caption, self.folder] {
            ui::set_visible(h, !writing);
        }
        ui::set_text(
            self.folder,
            s.folder.as_deref().unwrap_or("No folder selected"),
        );
        for h in [self.image_caption, self.image, self.pick_image] {
            ui::set_visible(h, !writing && !full);
        }
        ui::set_text(self.image, s.image.as_deref().unwrap_or("No file selected"));
        let parts = !writing && !full && s.native;
        for h in [self.source_caption, self.source.h] {
            ui::set_visible(h, parts);
        }
        let mut items = vec!["Select a partition...".to_string()];
        items.extend(s.sources.iter().map(|p| p.label.clone()));
        self.source.set_items(&items);
        let at = s
            .source
            .and_then(|i| s.sources.iter().position(|p| p.index == i));
        self.source.select(at.map_or(0, |i| i + 1));
        let whole = s.mode == RestoreMode::SinglePartition && !s.native && s.image.is_some();
        let summary = match (&s.summary, whole) {
            (_, true) => Some(hints::ENTIRE_IMAGE.to_string()),
            (Some((info, origin)), false) if full => {
                Some(format!("Backup Info: {info}   Source: {origin}"))
            }
            _ => None,
        };
        self.set_line(self.summary, summary.as_deref().filter(|_| !writing), None);
        let error = s.load_error.as_deref().filter(|_| !writing);
        self.set_line(self.load_error, error, Some(Tone::Warning));

        for h in [self.write_caption, self.write_path, self.write_choose] {
            ui::set_visible(h, writing);
        }
        ui::set_text(
            self.write_path,
            s.write_path.as_deref().unwrap_or("(none selected)"),
        );
        let note = match (&s.write_path, &s.write_line) {
            (_, Some(l)) => Some(l.as_str()),
            (Some(_), None) => Some(hints::WRITE_IMAGE_UNREADABLE),
            (None, None) => Some(hints::WRITE_IMAGE_EMPTY),
        };
        self.set_line(self.write_line, note.filter(|_| writing), None);
        ui::set_visible(self.write_target, writing && s.write_line.is_some());
    }

    fn show_target(&self, s: &Shown) {
        let slot = slot_of(s.mode);
        let single = slot == Some(TargetSlot::SinglePartition);
        let on = slot.is_some();
        ui::set_visible(self.kind_caption, on);
        ui::set_visible(self.kind.h, on && !single);
        self.kind.select(usize::from(!s.is_device));
        let on_device = on && (single || s.is_device);
        ui::set_visible(self.device.h, on_device);
        let devices = self.devices.borrow();
        let mut items = vec![if devices.is_empty() {
            no_devices_hint().to_string()
        } else {
            "Select a target device...".to_string()
        }];
        items.extend(devices.iter().map(device_label));
        self.device.set_items(&items);
        self.device.select(s.device_row.map_or(0, |r| r + 1));
        ui::set_visible(
            self.close_target,
            slot == Some(TargetSlot::FullDisk) && s.device_row.is_some() && on_device,
        );
        ui::set_visible(self.scan, single && s.has_device && !s.scanning);
        let on_image = on && !single && !s.is_device;
        ui::set_visible(self.target_image, on_image);
        ui::set_visible(self.save_as, on_image);
        self.set_field(self.target_image, s.target_image.as_deref().unwrap_or(""));
        self.set_line(
            self.warning,
            s.warning.filter(|_| on_device),
            Some(Tone::Error),
        );
        let (tone, text) = s.readout.clone().unzip();
        self.set_line(self.readout, text.as_deref(), tone.flatten());
        let parts = single && !s.parts.is_empty();
        for h in [self.part_caption, self.part.h] {
            ui::set_visible(h, parts);
        }
        let mut items = vec!["Select a target partition...".to_string()];
        items.extend(s.parts.iter().map(|p| {
            let fit = match p.fits {
                Some(false) => " - too small",
                Some(true) => " - OK",
                None => "",
            };
            format!(
                "{}: {} ({}){fit}",
                p.position,
                ui::plain(&p.type_name),
                format_size(p.size_bytes)
            )
        }));
        self.part.set_items(&items);
        let at = s
            .part
            .and_then(|i| s.parts.iter().position(|p| p.index == i));
        self.part.select(at.map_or(0, |i| i + 1));
        let error = s.scan_error.as_deref().filter(|_| single);
        self.set_line(self.scan_error, error, Some(Tone::Warning));
    }

    fn show_new_disk(&self, s: &Shown) {
        let on = s.mode == RestoreMode::NewDisk;
        let c = s.new_disk;
        for h in [
            self.table_caption,
            self.table.h,
            self.new_align_caption,
            self.new_align.h,
            self.start_lba,
        ] {
            ui::set_visible(h, on);
        }
        let table = TABLES.iter().position(|t| t.0 == c.table).unwrap_or(0);
        self.table.select(table);
        let at = NEW_ALIGNMENTS.iter().position(|a| a.0 == c.alignment);
        self.new_align.select(at.unwrap_or(0));
        ui::set_visible(
            self.new_custom,
            on && c.alignment == AlignmentChoice::Custom,
        );
        self.set_field(self.new_custom, &c.custom_alignment.to_string());
        ui::set_visible(self.bootable, on && c.table == NewTableType::Mbr);
        set_check(self.bootable, c.bootable);
        let start = format!("Partition will start at LBA {}", s.start_lba);
        ui::set_text(self.start_lba, &start);
    }

    fn show_sizes(&self, s: &Shown) {
        let full = s.mode == RestoreMode::FullDisk;
        let expand = full && s.expand_shown;
        ui::set_visible(self.expand, expand);
        set_check(self.expand, s.expand.enabled);
        let on = expand && s.expand.enabled;
        for h in [
            self.free_caption,
            self.free,
            self.extend.h,
            self.expand_line,
            self.current_caption,
            self.current,
            self.after_caption,
            self.after,
        ] {
            ui::set_visible(h, on);
        }
        self.set_field(self.free, &s.expand.free_mib.to_string());
        self.extend.select(usize::from(s.expand.extend_last));
        ui::set_text(self.expand_line, &s.expand_line);
        let after = format!("After (+ {} free):", format_size(s.preview.added));
        ui::set_text(self.after_caption, &after);
        if on {
            let p = &s.preview;
            partition_bar::set_options(self.current, p.scale, false);
            partition_bar::show(self.current, segments(&p.current), None);
            let mut bars = segments(&p.after);
            if p.free_tail > 0 {
                bars.push(LayoutSegment {
                    label: String::new(),
                    fs: String::new(),
                    size_bytes: p.free_tail,
                    kind: SegmentKind::Free,
                    partition: None,
                });
            }
            partition_bar::show(self.after, bars, None);
        }

        let align = full && s.has_backup;
        ui::set_visible(self.align_caption, align);
        ui::set_visible(self.align.h, align);
        let items = [
            s.original_label.clone(),
            "1 MB boundaries (modern, LBA 2048)".to_string(),
            "Custom".to_string(),
        ];
        self.align.set_items(&items);
        let (choice, sectors) = s.alignment;
        let at = ALIGNMENTS.iter().position(|c| *c == choice).unwrap_or(0);
        self.align.select(at);
        ui::set_visible(self.custom, align && choice == AlignmentChoice::Custom);
        self.set_field(self.custom, &sectors.to_string());

        let rows: &[(SizeRow, Vec<SizeMode>)] = if full { &s.rows } else { &[] };
        self.show_rows(rows);
        let (tone, text) = s.notes.clone().filter(|_| full).unzip();
        self.set_line(self.notes, text.as_deref(), tone.flatten());
    }

    /// Build the size rows again when the partitions or their choices change, then show each.
    fn show_rows(&self, rows: &[(SizeRow, Vec<SizeMode>)]) {
        let same = {
            let held = self.cells.borrow();
            held.len() == rows.len()
                && held
                    .iter()
                    .zip(rows)
                    .all(|(c, (r, m))| c.index == r.index && c.choices == *m)
        };
        if !same {
            for c in self.cells.borrow_mut().drain(..) {
                for h in c.all() {
                    // SAFETY: our own controls, built again for the new rows.
                    let _ = unsafe { DestroyWindow(h) };
                }
            }
            let built = rows
                .iter()
                .enumerate()
                .map(|(pos, (r, m))| SizeCells::build(self.page, pos, r, m.clone()))
                .collect();
            *self.cells.borrow_mut() = built;
        }
        for (c, (row, _)) in self.cells.borrow().iter().zip(rows) {
            ui::set_text(c.original, &format_size(row.original_size));
            ui::set_text(c.minimum, &format_size(row.minimum_size));
            let at = c.choices.iter().position(|m| *m == row.mode).unwrap_or(0);
            c.mode.select(at);
            let custom = row.mode == SizeMode::Custom;
            let text = match row.mode {
                SizeMode::Custom => row.custom_mib_effective().to_string(),
                SizeMode::FillRemaining => String::new(),
                _ => (row.effective_size() / MIB).to_string(),
            };
            self.set_field(c.size, &text);
            ui::set_enabled(c.size, custom);
        }
        for h in self.heads {
            ui::set_visible(h, !rows.is_empty());
        }
    }

    fn show_footer(&self, s: &Shown) {
        let writing = s.mode == RestoreMode::WriteImage;
        let target = s.write_target.as_ref().map(|t| format!("Writes to: {t}"));
        self.set_line(self.target_line, target.as_deref(), None);
        ui::set_visible(self.start, !s.running && !writing);
        ui::set_enabled(self.start, s.can_start);
        ui::set_visible(self.cancel, s.running);
        ui::set_enabled(self.cancel, !s.cancelling);
        let cancel = if s.cancelling {
            "Cancelling..."
        } else {
            "Cancel"
        };
        ui::set_text(self.cancel, cancel);
        ui::set_text(self.run, s.run.as_deref().unwrap_or(""));
        ui::set_visible(self.run, s.run.is_some());
        crate::inspect::set_progress(
            self.progress,
            s.run.is_some(),
            s.pct.or(Some(0)).filter(|_| s.run.is_some()),
        );
        // The footer shows one line: progress, else why Start is off, else the outcome.
        ui::set_text(self.why, s.why.as_deref().unwrap_or(""));
        ui::set_visible(self.why, !s.running && !s.can_start && !writing);
        let (outcome, tone) = match &s.outcome {
            Some(Ok(())) => (labels::DONE.to_string(), None),
            Some(Err(e)) => (e.clone(), Some(Tone::Error)),
            None => (String::new(), None),
        };
        ui::set_text(self.outcome, &outcome);
        crate::theme::set_tone(self.outcome, tone);
        ui::set_visible(
            self.outcome,
            s.run.is_none() && !ui::shown(self.why) && s.outcome.is_some(),
        );
    }

    /// The controls a running restore or another tab's job locks.
    fn form_controls(&self) -> Vec<HWND> {
        let mut v = vec![
            self.mode.h,
            self.folder,
            self.pick_image,
            self.source.h,
            self.write_choose,
            self.write_target,
            self.kind.h,
            self.device.h,
            self.close_target,
            self.scan,
            self.target_image,
            self.save_as,
            self.part.h,
            self.table.h,
            self.new_align.h,
            self.new_custom,
            self.bootable,
            self.expand,
            self.free,
            self.extend.h,
            self.align.h,
            self.custom,
            self.mac_driver,
        ];
        for c in self.cells.borrow().iter() {
            v.push(c.mode.h);
        }
        v
    }

    /// What decides the layout: the controls shown, the size rows and each note's line count.
    fn shape_now(&self) -> Vec<usize> {
        let all = [
            self.busy_note,
            self.job,
            self.folder,
            self.image,
            self.source.h,
            self.summary,
            self.load_error,
            self.write_path,
            self.write_line,
            self.write_target,
            self.kind_caption,
            self.kind.h,
            self.device.h,
            self.close_target,
            self.scan,
            self.target_image,
            self.warning,
            self.readout,
            self.part.h,
            self.scan_error,
            self.table.h,
            self.new_custom,
            self.bootable,
            self.expand,
            self.free,
            self.align.h,
            self.custom,
            self.notes,
            self.mac_driver,
            self.caveats,
            self.target_line,
            self.run,
            self.cancel,
            self.why,
            self.outcome,
        ];
        let mut shape: Vec<usize> = all.iter().map(|h| usize::from(ui::shown(*h))).collect();
        shape.push(self.cells.borrow().len());
        for h in [self.notes, self.caveats, self.readout, self.summary] {
            shape.push(ui::text(h).lines().count());
        }
        shape
    }

    /// A compact form: the screen shares the window with the log.
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
        let caption = 120;
        let mut col = Vec::new();
        let put = |col: &mut Vec<Node<HWND>>, h: HWND| {
            if ui::shown(h) {
                col.push(lines(h));
            }
        };
        put(&mut col, self.busy_note);
        let mut top = vec![fixed(self.mode_caption, caption), fixed(self.mode.h, 220)];
        if ui::shown(self.job) {
            top.push(stretch(self.job));
            top.push(Node::leaf(
                self.job_progress,
                Size::Fixed(160),
                Size::Fixed(layout::ROW - 6),
            ));
        } else {
            top.push(Node::Space(Size::Stretch(1)));
        }
        col.push(Node::row(top));
        if ui::shown(self.folder) {
            col.push(Node::row(vec![
                fixed(self.folder_caption, caption),
                stretch(self.folder),
            ]));
        }
        if ui::shown(self.image) {
            col.push(Node::row(vec![
                fixed(self.image_caption, caption),
                stretch(self.image),
                Node::button(self.pick_image, 85),
            ]));
        }
        if ui::shown(self.source.h) {
            col.push(Node::row(vec![
                fixed(self.source_caption, caption),
                stretch(self.source.h),
            ]));
        }
        put(&mut col, self.summary);
        put(&mut col, self.load_error);
        if ui::shown(self.write_path) {
            col.push(Node::row(vec![
                fixed(self.write_caption, caption),
                stretch(self.write_path),
                Node::button(self.write_choose, 85),
            ]));
        }
        put(&mut col, self.write_line);
        if ui::shown(self.write_target) {
            col.push(Node::row(vec![
                Node::button(self.write_target, 200),
                Node::Space(Size::Stretch(1)),
            ]));
        }
        if ui::shown(self.kind_caption) {
            let mut row = vec![fixed(self.kind_caption, caption)];
            if ui::shown(self.kind.h) {
                row.push(fixed(self.kind.h, 160));
            }
            if ui::shown(self.device.h) {
                row.push(stretch(self.device.h));
            }
            if ui::shown(self.target_image) {
                row.push(Node::field(self.target_image));
                row.push(Node::button(self.save_as, 85));
            }
            for (h, w) in [(self.close_target, 95), (self.scan, 100)] {
                if ui::shown(h) {
                    row.push(Node::button(h, w));
                }
            }
            col.push(Node::row(row));
        }
        put(&mut col, self.warning);
        put(&mut col, self.readout);
        if ui::shown(self.part.h) {
            col.push(Node::row(vec![
                fixed(self.part_caption, caption),
                stretch(self.part.h),
            ]));
        }
        put(&mut col, self.scan_error);
        if ui::shown(self.table.h) {
            let mut row = vec![
                fixed(self.table_caption, caption),
                fixed(self.table.h, 90),
                fixed(self.new_align_caption, 75),
                fixed(self.new_align.h, 150),
            ];
            if ui::shown(self.new_custom) {
                row.push(fixed(self.new_custom, 70));
            }
            if ui::shown(self.bootable) {
                row.push(stretch(self.bootable));
            } else {
                row.push(Node::Space(Size::Stretch(1)));
            }
            col.push(Node::row(row));
            col.push(Node::row(vec![
                Node::Space(Size::Fixed(caption)),
                stretch(self.start_lba),
            ]));
        }
        if ui::shown(self.expand) {
            col.push(Node::row(vec![stretch(self.expand)]));
        }
        if ui::shown(self.free) {
            col.push(Node::row(vec![
                fixed(self.free_caption, caption),
                fixed(self.free, 80),
                fixed(self.extend.h, 320),
                stretch(self.expand_line),
            ]));
            col.push(Node::row(vec![
                fixed(self.current_caption, caption),
                Node::leaf(
                    self.current,
                    Size::Stretch(1),
                    Size::Fixed(partition_bar::BAR_ONLY),
                ),
            ]));
            col.push(Node::row(vec![
                fixed(self.after_caption, caption),
                Node::leaf(
                    self.after,
                    Size::Stretch(1),
                    Size::Fixed(partition_bar::HEIGHT),
                ),
            ]));
        }
        if ui::shown(self.align.h) {
            let mut row = vec![fixed(self.align_caption, caption), fixed(self.align.h, 300)];
            if ui::shown(self.custom) {
                row.push(fixed(self.custom, 70));
            }
            row.push(Node::Space(Size::Stretch(1)));
            col.push(Node::row(row));
        }
        let cells = self.cells.borrow();
        if !cells.is_empty() {
            let widths = [30, 200, 100, 100, 150, 100];
            col.push(Node::row(
                self.heads
                    .iter()
                    .zip(widths)
                    .map(|(h, w)| fixed(*h, w))
                    .collect(),
            ));
            for c in cells.iter() {
                col.push(Node::row(
                    c.all()
                        .iter()
                        .zip(widths)
                        .map(|(h, w)| fixed(*h, w))
                        .collect(),
                ));
            }
        }
        drop(cells);
        put(&mut col, self.notes);
        if ui::shown(self.mac_driver) {
            col.push(Node::row(vec![stretch(self.mac_driver)]));
        }
        put(&mut col, self.caveats);
        col.push(Node::Space(Size::Stretch(1)));
        put(&mut col, self.target_line);
        // One footer row: the buttons, then the progress, or why Start is off, or how it went.
        let mut foot = Vec::new();
        for (h, w) in [(self.start, 120), (self.cancel, 110)] {
            if ui::shown(h) {
                foot.push(Node::button(h, w));
            }
        }
        if ui::shown(self.run) {
            foot.push(fixed(self.progress, 200));
            foot.push(stretch(self.run));
        } else if ui::shown(self.why) {
            foot.push(stretch(self.why));
        } else {
            foot.push(stretch(self.outcome));
        }
        col.push(Node::row(foot));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in layout::layout(&tree, area, dpi) {
            if ui::shown(h) {
                ui::move_to(h, r);
            }
        }
    }

    /// The new-disk form with one change.
    fn new_disk(&self, change: impl FnOnce(&mut NewDiskChoice)) -> RestoreAction {
        let mut c = self.ctrl.borrow().new_disk();
        change(&mut c);
        RestoreAction::SetNewDisk(c)
    }

    fn slot(&self) -> TargetSlot {
        slot_of(self.ctrl.borrow().mode()).unwrap_or(TargetSlot::FullDisk)
    }

    fn changed(&self, from: HWND) -> Option<RestoreAction> {
        let text = ui::text(from);
        let n = text.trim().parse::<u32>().ok();
        if from == self.target_image {
            let path = (!text.trim().is_empty()).then(|| PathBuf::from(text.trim()));
            return Some(RestoreAction::SetTargetImage(self.slot(), path));
        }
        if from == self.free {
            return n.map(RestoreAction::SetExpandMib);
        }
        if from == self.custom {
            return n
                .filter(|n| (1..=ALIGN_SECTORS_MAX).contains(n))
                .map(RestoreAction::SetCustomAlignment);
        }
        if from == self.new_custom {
            let n = n.filter(|n| (1..=ALIGN_SECTORS_MAX).contains(n))?;
            return Some(self.new_disk(|c| c.custom_alignment = n));
        }
        let cells = self.cells.borrow();
        let c = cells.iter().find(|c| c.size == from)?;
        let mode = c.choices.get(c.mode.picked()?).copied()?;
        (mode == SizeMode::Custom).then(|| RestoreAction::SetPartitionSize {
            index: c.index,
            mode,
            custom_mib: n.unwrap_or(0),
        })
    }

    fn selected(&self, from: HWND) -> Option<RestoreAction> {
        if from == self.mode.h {
            return MODES
                .get(self.mode.picked()?)
                .map(|m| RestoreAction::SetMode(m.0));
        }
        if from == self.source.h {
            let i = self.source.picked()?.checked_sub(1);
            let index = i.and_then(|i| {
                self.ctrl
                    .borrow()
                    .source_partitions()
                    .get(i)
                    .map(|p| p.index)
            });
            return Some(RestoreAction::SetSourcePartition(index));
        }
        if from == self.kind.h {
            return Some(RestoreAction::SetTargetIsDevice(
                self.slot(),
                self.kind.picked()? == 0,
            ));
        }
        if from == self.device.h {
            let slot = self.slot();
            let Some(row) = self.device.picked()?.checked_sub(1) else {
                return Some(RestoreAction::ClearDevice(slot));
            };
            let device = self.devices.borrow().get(row).cloned()?;
            return Some(RestoreAction::SelectDevice { slot, row, device });
        }
        if from == self.part.h {
            let i = self.part.picked()?.checked_sub(1)?;
            let index = self.ctrl.borrow().target_partitions().get(i)?.index;
            return Some(RestoreAction::SetTargetPartition(index));
        }
        if from == self.table.h {
            let t = TABLES.get(self.table.picked()?)?.0;
            return Some(self.new_disk(|c| c.table = t));
        }
        if from == self.new_align.h {
            let a = NEW_ALIGNMENTS.get(self.new_align.picked()?)?.0;
            return Some(self.new_disk(|c| c.alignment = a));
        }
        if from == self.extend.h {
            return Some(RestoreAction::SetExtendLast(self.extend.picked()? == 1));
        }
        if from == self.align.h {
            return ALIGNMENTS
                .get(self.align.picked()?)
                .map(|a| RestoreAction::SetAlignment(*a));
        }
        let cells = self.cells.borrow();
        let c = cells.iter().find(|c| c.mode.h == from)?;
        let mode = c.choices.get(c.mode.picked()?).copied()?;
        let custom = ui::text(c.size).trim().parse::<u32>().unwrap_or(0);
        Some(RestoreAction::SetPartitionSize {
            index: c.index,
            mode,
            custom_mib: custom,
        })
    }

    fn clicked(&self, from: HWND) -> Option<RestoreAction> {
        let slot = self.slot();
        let action = if from == self.folder {
            RestoreAction::PickBackupFolder
        } else if from == self.pick_image {
            RestoreAction::PickSourceImage
        } else if from == self.write_choose {
            RestoreAction::PickWriteImage
        } else if from == self.write_target {
            RestoreAction::WriteImageTarget
        } else if from == self.close_target {
            RestoreAction::ClearDevice(slot)
        } else if from == self.scan {
            RestoreAction::ScanTarget
        } else if from == self.save_as {
            RestoreAction::PickTargetImage(slot)
        } else if from == self.bootable {
            let on = checked(from);
            self.new_disk(|c| c.bootable = on)
        } else if from == self.expand {
            RestoreAction::SetExpand(checked(from))
        } else if from == self.mac_driver {
            RestoreAction::SetInstallMacDriver(checked(from))
        } else if from == self.start {
            // Start only asks; the controller's own question begins the write.
            if !self.may_start() {
                return None;
            }
            RestoreAction::Start
        } else if from == self.cancel {
            RestoreAction::Cancel
        } else {
            return None;
        };
        Some(action)
    }
}

impl Handler for RestoreScreen {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        let action = match code {
            EN_CHANGE => self.changed(from),
            CBN_SELCHANGE => self.selected(from),
            BN_CLICKED => self.clicked(from),
            _ => None,
        };
        if let Some(a) = action {
            self.apply(a);
        }
    }

    fn notify(&self, hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        if hdr.hwndFrom == self.folder && hdr.code == BCN_DROPDOWN {
            self.folder_menu();
            return Some(LRESULT(0));
        }
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {}
}
