//! The Restore screen over `controller::restore::RestoreController`; the Win32 shell's
//! `restore.rs` is the reference.
//!
//! Slice 4: the four modes, the backup folder (a button that picks one, beside a pull-down of
//! recent folders) or a partition image, each mode's target (a device, an image file or a
//! scanned partition), the new disk's table, the full-disk sizes with alignment and trailing free
//! space with its before and after bars, the caveats, and Restore / Cancel with the progress line
//! and the outcome. Start only ever sends `Start`: the write begins on the controller's own
//! `Confirm`. Write Image File opens the disk writer; and a full-disk restore can go to a daemon's drive or image file.

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::{NSProgressIndicator, NSView};
use rusty_backup::controller::app::{
    busy_elsewhere_notice, no_devices_hint, DiskDevice, Notify, RecentHandle, RecentMode,
};
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::inspect::{LayoutSegment, SegmentKind};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::restore::{
    device_label, format_size, hints, ids, labels, AlignmentChoice, CaveatLevel, HostRequest,
    JobRegistry, NewDiskChoice, NewTableType, PlanSegment, RemoteDrive, RestoreAction,
    RestoreController, RestoreMode, RestoreServices, SizeMode, TargetSlot, ALIGN_SECTORS_MAX,
    MODES,
};
use rusty_backup::controller::{Controller, Effect};

use crate::disk_write::DiskWriteWindow;
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::partition_bar::{self, Bar};
use crate::sync::SyncGuard;
use crate::table::Table;

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
    list.iter()
        .enumerate()
        .map(|(color_index, s)| LayoutSegment {
            label: s.label.clone(),
            fs: s.fs.clone(),
            size_bytes: s.size_bytes,
            kind: SegmentKind::Partition { color_index },
            partition: None,
        })
        .collect()
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    BusyNote,
    ModeCaption,
    Mode,
    Job,
    JobProgress,
    FolderCaption,
    Folder,
    Recent,
    RecentItem(usize),
    ImageCaption,
    Image,
    PickImage,
    SourceCaption,
    Source,
    Summary,
    LoadError,
    WriteCaption,
    WritePath,
    WriteChoose,
    WriteLine,
    WriteTarget,
    RemoteSwitch,
    RemoteCaption,
    RemoteAddress,
    RemoteConnect,
    RemoteBusy,
    RemoteError,
    RemoteNote,
    RemoteList,
    RemoteImageCaption,
    RemoteImage,
    RemoteUseImage,
    RemoteSelected,
    KindCaption,
    Kind,
    Device,
    CloseTarget,
    Scan,
    TargetImage,
    SaveAs,
    Warning,
    Readout,
    PartCaption,
    Part,
    ScanError,
    TableCaption,
    Table,
    NewAlignCaption,
    NewAlign,
    NewCustom,
    Bootable,
    StartLba,
    Expand,
    FreeCaption,
    Free,
    Extend,
    ExpandLine,
    CurrentCaption,
    Current,
    AfterCaption,
    After,
    AlignCaption,
    Align,
    Custom,
    Head(usize),
    Number(usize),
    Kindcell(usize),
    Original(usize),
    Minimum(usize),
    RowMode(usize),
    RowSize(usize),
    Notes,
    MacDriver,
    Caveats,
    TargetLine,
    Outcome,
    Run,
    Progress,
    Start,
    Cancel,
    Why,
}

pub struct RestoreScreen {
    ctrl: RefCell<RestoreController>,
    mtm: MainThreadMarker,
    notify: Arc<dyn Notify>,
    /// The daemon's drives, as listed in the remote target's table.
    remote_list: Table,
    remote_drives: RefCell<Vec<RemoteDrive>>,
    /// Write Image File's disk writer, while its controller is open.
    writer: RefCell<Option<Rc<DiskWriteWindow>>>,
    host: EffectHost,
    recent: RecentHandle,
    recent_files: RefCell<Vec<String>>,
    devices: RefCell<Vec<DiskDevice>>,
    /// Another screen's disk job, which locks this one.
    busy: Cell<Option<&'static str>>,
    sync: SyncGuard,
    me: Weak<RestoreScreen>,
    form: Form<K>,
    current: Retained<Bar>,
    after: Retained<Bar>,
    /// The size rows' partitions and their mode choices, as built.
    rows: RefCell<Vec<(usize, Vec<SizeMode>)>>,
}

fn progress_of(form: &Form<K>, k: K) -> Retained<NSProgressIndicator> {
    form.v(k)
        .downcast::<NSProgressIndicator>()
        .expect("a progress bar")
}

impl RestoreScreen {
    pub fn build(
        mtm: MainThreadMarker,
        body: Retained<NSView>,
        notify: Arc<dyn Notify>,
        recent: RecentHandle,
        host: EffectHost,
    ) -> Rc<RestoreScreen> {
        let screen = Rc::new_cyclic(|me: &Weak<RestoreScreen>| {
            let fire = me.clone();
            let form = Form::new(
                mtm,
                body,
                Rc::new(move |k| {
                    if let Some(s) = fire.upgrade() {
                        s.fired(k);
                    }
                }),
            );
            let strings = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
            form.wrap(K::BusyNote, "");
            form.label(K::ModeCaption, "Mode:");
            let modes: Vec<&str> = MODES.iter().map(|m| m.1).collect();
            form.popup(K::Mode, &strings(&modes), ids::MODE);
            form.label(K::Job, "");
            form.progress(K::JobProgress);
            form.label(K::FolderCaption, &format!("{}:", labels::BACKUP_FOLDER));
            form.button(K::Folder, "No folder selected", ids::BACKUP_FOLDER);
            form.pulldown(K::Recent, ids::RECENT);
            form.label(K::ImageCaption, "Or Image File:");
            form.label(K::Image, "");
            form.button(K::PickImage, "Browse...", ids::PICK_SOURCE_IMAGE);
            form.label(K::SourceCaption, "Source Partition:");
            form.popup(K::Source, &[], ids::SOURCE_PARTITION);
            form.wrap(K::Summary, "");
            form.wrap(K::LoadError, "");
            form.label(K::WriteCaption, "Image File:");
            form.label(K::WritePath, "");
            form.button(K::WriteChoose, "Choose...", ids::WRITE_IMAGE);
            form.wrap(K::WriteLine, "");
            form.button(
                K::WriteTarget,
                "Choose target and write...",
                ids::WRITE_TARGET,
            );
            form.check(K::RemoteSwitch, hints::REMOTE, ids::REMOTE_TARGET);
            form.label(K::RemoteCaption, "Host:");
            form.field(K::RemoteAddress, ids::REMOTE_ADDRESS, "Host", false);
            form.button(K::RemoteConnect, "Connect", ids::REMOTE_CONNECT);
            form.label(K::RemoteBusy, "Connecting...");
            form.wrap(K::RemoteError, "");
            form.wrap(K::RemoteNote, "");
            let remote_list = Table::new(mtm, ids::REMOTE_DRIVE, &[("Drive", 460.0, false)]);
            remote_list.view.setHeaderView(None);
            let activate = me.clone();
            remote_list.on_activate(
                mtm,
                Rc::new(move || {
                    if let Some(s) = activate.upgrade() {
                        s.remote_picked();
                    }
                }),
            );
            form.add(K::RemoteList, &remote_list.scroll);
            form.label(K::RemoteImageCaption, "or image file:");
            form.field(
                K::RemoteImage,
                ids::REMOTE_IMAGE_NAME,
                "Image file name",
                false,
            );
            form.button(K::RemoteUseImage, "Use file", ids::REMOTE_USE_IMAGE);
            form.label(K::RemoteSelected, "");
            form.label(K::KindCaption, &format!("{}:", labels::TARGET_KIND));
            form.popup(K::Kind, &strings(&KINDS), ids::TARGET_KIND);
            form.popup(K::Device, &[], ids::TARGET_DEVICE);
            form.button(K::CloseTarget, "Close Drive", ids::CLOSE_TARGET);
            form.button(K::Scan, "Scan Target", ids::SCAN_TARGET);
            form.field(
                K::TargetImage,
                ids::TARGET_IMAGE,
                labels::TARGET_IMAGE,
                false,
            );
            form.button(K::SaveAs, "Save As...", ids::PICK_TARGET_IMAGE);
            form.wrap(K::Warning, "");
            form.wrap(K::Readout, "");
            form.label(K::PartCaption, "Target Partition:");
            form.popup(K::Part, &[], ids::TARGET_PARTITION);
            form.wrap(K::ScanError, "");
            form.label(K::TableCaption, "Table Type:");
            let tables: Vec<&str> = TABLES.iter().map(|t| t.1).collect();
            form.popup(K::Table, &strings(&tables), ids::NEW_DISK_TABLE);
            form.label(K::NewAlignCaption, "Alignment:");
            let aligns: Vec<&str> = NEW_ALIGNMENTS.iter().map(|a| a.1).collect();
            form.popup(K::NewAlign, &strings(&aligns), ids::NEW_DISK_ALIGNMENT);
            form.field(
                K::NewCustom,
                ids::NEW_DISK_CUSTOM,
                "Custom alignment (sectors)",
                false,
            );
            form.check(
                K::Bootable,
                "Mark partition as bootable",
                ids::NEW_DISK_BOOTABLE,
            );
            form.label(K::StartLba, "");
            form.check(K::Expand, hints::EXPAND, ids::EXPAND);
            form.label(K::FreeCaption, "Free space (MiB):");
            form.field(K::Free, ids::EXPAND_MIB, "Free space (MiB)", false);
            form.popup(
                K::Extend,
                &strings(&[hints::EXPAND_LEAVE, hints::EXPAND_EXTEND]),
                ids::EXPAND_EXTEND,
            );
            form.label(K::ExpandLine, "");
            form.label(K::CurrentCaption, "Current:");
            let current = Bar::new(mtm, "restore.current_bar");
            current.set_options(1.0, false);
            form.add(K::Current, &current);
            form.label(K::AfterCaption, "");
            let after = Bar::new(mtm, "restore.after_bar");
            form.add(K::After, &after);
            form.label(K::AlignCaption, "Partition starts:");
            form.popup(K::Align, &[], ids::ALIGNMENT);
            form.field(
                K::Custom,
                ids::CUSTOM_ALIGNMENT,
                "Custom alignment (sectors)",
                false,
            );
            for (i, t) in [
                "#",
                "Type",
                "Original",
                "Minimum",
                "Size Mode",
                "Size (MiB)",
            ]
            .iter()
            .enumerate()
            {
                form.label(K::Head(i), t);
            }
            form.wrap(K::Notes, "");
            form.check(K::MacDriver, hints::MAC_DRIVER, ids::MAC_DRIVER);
            form.wrap(K::Caveats, "");
            form.label(K::TargetLine, "");
            form.label(K::Outcome, "");
            form.label(K::Run, "");
            form.progress(K::Progress);
            form.button(K::Start, labels::START, ids::START);
            form.button(K::Cancel, "Cancel", ids::CANCEL);
            form.label(K::Why, "");
            RestoreScreen {
                ctrl: RefCell::new(RestoreController::new(RestoreServices {
                    jobs: JobRegistry::new(),
                    notify: notify.clone(),
                })),
                mtm,
                notify,
                remote_list,
                remote_drives: RefCell::default(),
                writer: RefCell::default(),
                host,
                recent_files: RefCell::new(recent.list(RecentMode::Restore)),
                recent,
                devices: RefCell::default(),
                busy: Cell::new(None),
                sync: SyncGuard::default(),
                me: me.clone(),
                form,
                current,
                after,
                rows: RefCell::default(),
            }
        });
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

    /// Another screen's job, if any, which locks this screen's start controls.
    /// Settings cleared the recent files: the picker forgets its copy too.
    pub fn forget_recents(&self) {
        self.recent_files.borrow_mut().clear();
    }

    pub fn set_busy(&self, busy: Option<&'static str>) {
        self.busy.set(busy);
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
                // A job another screen started behind the question keeps this one from starting.
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
        self.refresh();
    }

    /// Write Image File: the image goes to the disk writer's window, one at a time.
    fn open_writer(&self, open: rusty_backup::controller::restore::WriteOpen) {
        if self.writer.borrow().as_ref().is_some_and(|d| d.is_open()) {
            return;
        }
        let d = DiskWriteWindow::open(
            self.mtm,
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

    fn line(&self, k: K, text: Option<&str>, tone: Option<Tone>) {
        self.form.set_text(k, text.unwrap_or(""));
        self.form.show(k, text.is_some());
        self.form.set_tone(k, tone);
    }

    /// Read the controller first and release it, then set the views inside the guard.
    fn refresh(&self) {
        let can_start = self.may_start();
        self.sync.run(|| self.show(can_start));
        self.relayout();
    }

    fn show(&self, can_start: bool) {
        let f = &self.form;
        let ctrl = self.ctrl.borrow();
        let mode = ctrl.mode();
        let notice = self.busy.get().map(busy_elsewhere_notice);
        self.line(K::BusyNote, notice.as_deref(), Some(Tone::Warning));
        f.select(K::Mode, MODES.iter().position(|m| m.0 == mode).unwrap_or(0));
        let job = [
            ctrl.decoding(),
            ctrl.loading(),
            ctrl.probing(),
            ctrl.target_scan(),
        ]
        .into_iter()
        .flatten()
        .next();
        let (line, pct) = job
            .as_ref()
            .map(crate::inspect::job_line)
            .unwrap_or_default();
        f.set_text(K::Job, &line);
        f.show(K::Job, job.is_some());
        crate::ui::set_progress(&progress_of(f, K::JobProgress), job.is_some(), pct);

        // The source: a backup folder, a partition image, or Write Image File's image.
        let writing = mode == RestoreMode::WriteImage;
        let full = mode == RestoreMode::FullDisk;
        let folder = ctrl.backup_folder().map(|p| p.display().to_string());
        for k in [K::FolderCaption, K::Folder, K::Recent] {
            f.show(k, !writing);
        }
        f.set_title(K::Folder, folder.as_deref().unwrap_or("No folder selected"));
        let recent = self.recent_files.borrow().clone();
        let mut items: Vec<(String, Option<K>, bool)> = recent
            .iter()
            .enumerate()
            .map(|(i, p)| (p.clone(), Some(K::RecentItem(i)), true))
            .collect();
        if items.is_empty() {
            items.push(("(no recent backups)".into(), None, false));
        }
        f.set_pulldown(K::Recent, "Recent", &items);
        let image = ctrl.source_image().map(|p| p.display().to_string());
        for k in [K::ImageCaption, K::Image, K::PickImage] {
            f.show(k, !writing && !full);
        }
        f.set_text(K::Image, image.as_deref().unwrap_or("No file selected"));
        let native = ctrl.has_native_backup();
        let parts = !writing && !full && native;
        f.show(K::SourceCaption, parts);
        f.show(K::Source, parts);
        let sources = ctrl.source_partitions();
        let mut names = vec!["Select a partition...".to_string()];
        names.extend(sources.iter().map(|p| p.label.clone()));
        f.set_items(K::Source, &names);
        let at = ctrl
            .source_partition()
            .and_then(|i| sources.iter().position(|p| p.index == i));
        f.select(K::Source, at.map_or(0, |i| i + 1));
        let whole = mode == RestoreMode::SinglePartition && !native && image.is_some();
        let summary = match (ctrl.backup_summary(), whole) {
            (_, true) => Some(hints::ENTIRE_IMAGE.to_string()),
            (Some((info, origin)), false) if full => {
                Some(format!("Backup Info: {info}   Source: {origin}"))
            }
            _ => None,
        };
        self.line(K::Summary, summary.as_deref().filter(|_| !writing), None);
        let error = ctrl.load_error().map(str::to_string);
        self.line(
            K::LoadError,
            error.as_deref().filter(|_| !writing),
            Some(Tone::Warning),
        );
        for k in [K::WriteCaption, K::WritePath, K::WriteChoose] {
            f.show(k, writing);
        }
        let write_path = ctrl.write_image().map(|(p, _)| p.display().to_string());
        let write_line = ctrl.write_image_line();
        f.set_text(
            K::WritePath,
            write_path.as_deref().unwrap_or("(none selected)"),
        );
        let note = match (&write_path, &write_line) {
            (_, Some(l)) => Some(l.as_str()),
            (Some(_), None) => Some(hints::WRITE_IMAGE_UNREADABLE),
            (None, None) => Some(hints::WRITE_IMAGE_EMPTY),
        };
        self.line(K::WriteLine, note.filter(|_| writing), None);
        f.show(K::WriteTarget, writing && write_line.is_some());

        // The target: a device, an image file or a partition the scan found.
        let slot = slot_of(mode);
        let single = slot == Some(TargetSlot::SinglePartition);
        let s = slot.unwrap_or(TargetSlot::FullDisk);
        let is_device = ctrl.target_is_device(s);
        let device_row = ctrl.device_row(s);
        // A daemon target replaces the local device and image rows.
        let remote = self.show_remote(&ctrl, mode == RestoreMode::FullDisk);
        let on = slot.is_some() && !remote;
        f.show(K::KindCaption, on);
        f.show(K::Kind, on && !single);
        f.select(K::Kind, usize::from(!is_device));
        let on_device = on && (single || is_device);
        f.show(K::Device, on_device);
        let devices = self.devices.borrow();
        let mut names = vec![if devices.is_empty() {
            no_devices_hint().to_string()
        } else {
            "Select a target device...".to_string()
        }];
        names.extend(devices.iter().map(device_label));
        f.set_items(K::Device, &names);
        f.select(K::Device, device_row.map_or(0, |r| r + 1));
        f.show(
            K::CloseTarget,
            slot == Some(TargetSlot::FullDisk) && device_row.is_some() && on_device,
        );
        let scanning = ctrl.target_scan().is_some();
        f.show(K::Scan, single && ctrl.device(s).is_some() && !scanning);
        let on_image = on && !single && !is_device;
        f.show(K::TargetImage, on_image);
        f.show(K::SaveAs, on_image);
        let target = ctrl.target_image(s).map(|p| p.display().to_string());
        f.set_text(K::TargetImage, target.as_deref().unwrap_or(""));
        let warning = ctrl.system_disk_warning(s);
        self.line(K::Warning, warning.filter(|_| on_device), Some(Tone::Error));
        let readout = ctrl
            .target_readout()
            .filter(|_| mode == RestoreMode::FullDisk)
            .map(|c| (tone_of(std::iter::once(c.level)), c.text));
        let (tone, text) = readout.unzip();
        self.line(K::Readout, text.as_deref(), tone.flatten());
        let tparts = ctrl.target_partitions();
        let show_parts = single && !tparts.is_empty();
        f.show(K::PartCaption, show_parts);
        f.show(K::Part, show_parts);
        let mut names = vec!["Select a target partition...".to_string()];
        names.extend(tparts.iter().map(|p| {
            let fit = match p.fits {
                Some(false) => " - too small",
                Some(true) => " - OK",
                None => "",
            };
            format!(
                "{}: {} ({}){fit}",
                p.position,
                p.type_name,
                format_size(p.size_bytes)
            )
        }));
        f.set_items(K::Part, &names);
        let at = ctrl
            .target_partition()
            .and_then(|i| tparts.iter().position(|p| p.index == i));
        f.select(K::Part, at.map_or(0, |i| i + 1));
        let scan_error = ctrl.target_scan_error().map(str::to_string);
        self.line(
            K::ScanError,
            scan_error.as_deref().filter(|_| single),
            Some(Tone::Warning),
        );

        // A new disk's table.
        let new = mode == RestoreMode::NewDisk;
        let c: NewDiskChoice = ctrl.new_disk();
        for k in [
            K::TableCaption,
            K::Table,
            K::NewAlignCaption,
            K::NewAlign,
            K::StartLba,
        ] {
            f.show(k, new);
        }
        f.select(
            K::Table,
            TABLES.iter().position(|t| t.0 == c.table).unwrap_or(0),
        );
        f.select(
            K::NewAlign,
            NEW_ALIGNMENTS
                .iter()
                .position(|a| a.0 == c.alignment)
                .unwrap_or(0),
        );
        f.show(K::NewCustom, new && c.alignment == AlignmentChoice::Custom);
        f.set_text(K::NewCustom, &c.custom_alignment.to_string());
        f.show(K::Bootable, new && c.table == NewTableType::Mbr);
        f.set_check(K::Bootable, c.bootable);
        f.set_text(
            K::StartLba,
            &format!("Partition will start at LBA {}", ctrl.new_disk_start_lba()),
        );

        // A full disk's sizes, alignment and trailing free space.
        let expand_on = full && ctrl.expand_shown();
        let plan = ctrl.expand();
        f.show(K::Expand, expand_on);
        f.set_check(K::Expand, plan.enabled);
        let free_on = expand_on && plan.enabled;
        for k in [
            K::FreeCaption,
            K::Free,
            K::Extend,
            K::ExpandLine,
            K::CurrentCaption,
            K::Current,
            K::AfterCaption,
            K::After,
        ] {
            f.show(k, free_on);
        }
        f.set_text(K::Free, &plan.free_mib.to_string());
        f.select(K::Extend, usize::from(plan.extend_last));
        f.set_text(K::ExpandLine, &ctrl.expand_size_line());
        let preview = ctrl.expand_preview();
        f.set_text(
            K::AfterCaption,
            &format!("After (+ {} free):", format_size(preview.added)),
        );
        if free_on {
            self.current.set_options(preview.scale, false);
            self.current.show(segments(&preview.current), None);
            let mut bars = segments(&preview.after);
            if preview.free_tail > 0 {
                bars.push(LayoutSegment {
                    label: String::new(),
                    fs: String::new(),
                    size_bytes: preview.free_tail,
                    kind: SegmentKind::Free,
                    partition: None,
                });
            }
            self.after.show(bars, None);
        }
        let align = full && ctrl.has_backup();
        f.show(K::AlignCaption, align);
        f.show(K::Align, align);
        let items = [
            ctrl.original_alignment_label(),
            "1 MB boundaries (modern, LBA 2048)".to_string(),
            "Custom".to_string(),
        ];
        f.set_items(K::Align, &items);
        let (choice, sectors) = ctrl.alignment();
        f.select(
            K::Align,
            ALIGNMENTS.iter().position(|c| *c == choice).unwrap_or(0),
        );
        f.show(K::Custom, align && choice == AlignmentChoice::Custom);
        f.set_text(K::Custom, &sectors.to_string());
        let rows: Vec<_> = if full {
            ctrl.size_rows()
                .iter()
                .map(|r| (r.clone(), ctrl.size_mode_choices(r.index)))
                .collect()
        } else {
            Vec::new()
        };
        self.show_rows(&rows);
        let notes = ctrl.size_notes();
        let notes = (!notes.is_empty() && full).then(|| {
            let text: Vec<String> = notes.iter().map(|n| n.text.clone()).collect();
            (tone_of(notes.iter().map(|n| n.level)), text.join("\n"))
        });
        let (tone, text) = notes.unzip();
        self.line(K::Notes, text.as_deref(), tone.flatten());
        let mac = ctrl.mac_driver_missing().then(|| ctrl.install_mac_driver());
        f.show(K::MacDriver, mac.is_some() && !writing);
        f.set_check(K::MacDriver, mac.unwrap_or(false));
        let caveats = if slot.is_some() {
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
        let (tone, text) = caveats.unzip();
        self.line(K::Caveats, text.as_deref(), tone.flatten());

        // The footer: Restore / Cancel, then the progress, or why Restore is off, or how it went.
        let running = ctrl.is_running();
        let cancelling = ctrl.cancelling();
        let target = ctrl.write_target().map(|t| format!("Writes to: {t}"));
        self.line(K::TargetLine, target.as_deref(), None);
        f.show(K::Start, !running && !writing);
        f.enable(K::Start, can_start);
        f.show(K::Cancel, running);
        f.enable(K::Cancel, !cancelling);
        f.set_title(
            K::Cancel,
            if cancelling {
                "Cancelling..."
            } else {
                "Cancel"
            },
        );
        let run = ctrl.progress().map(|b| {
            let pct = (b.total_bytes > 0)
                .then(|| (b.current_bytes.saturating_mul(100) / b.total_bytes).min(100) as u32);
            let line = format!(
                "{}: {} of {}",
                b.operation,
                format_size(b.current_bytes),
                format_size(b.total_bytes)
            );
            (line, pct)
        });
        f.set_text(K::Run, run.as_ref().map_or("", |r| r.0.as_str()));
        f.show(K::Run, run.is_some());
        crate::ui::set_progress(
            &progress_of(f, K::Progress),
            run.is_some(),
            run.as_ref().map(|r| r.1.unwrap_or(0)),
        );
        let why = ctrl
            .validate()
            .err()
            .or_else(|| self.busy.get().map(busy_elsewhere_notice));
        f.set_text(K::Why, why.as_deref().unwrap_or(""));
        let why_on = !running && !can_start && !writing;
        f.show(K::Why, why_on);
        let (text, tone) = match ctrl.last_result() {
            Some(Ok(())) => (labels::DONE.to_string(), None),
            Some(Err(e)) => (e.clone(), Some(Tone::Error)),
            None => (String::new(), None),
        };
        let has_outcome = ctrl.last_result().is_some();
        drop(devices);
        drop(ctrl);
        f.set_text(K::Outcome, &text);
        f.set_tone(K::Outcome, tone);
        f.show(K::Outcome, run.is_none() && !why_on && has_outcome);
        let free = !running && self.busy.get().is_none();
        for k in self.form_controls() {
            f.enable(k, free);
        }
    }

    /// Build the size rows again when the partitions or their choices change, then show each.
    fn show_rows(&self, rows: &[(rusty_backup::controller::restore::SizeRow, Vec<SizeMode>)]) {
        let f = &self.form;
        let want: Vec<(usize, Vec<SizeMode>)> =
            rows.iter().map(|(r, m)| (r.index, m.clone())).collect();
        if *self.rows.borrow() != want {
            f.remove(|k| {
                matches!(
                    k,
                    K::Number(_)
                        | K::Kindcell(_)
                        | K::Original(_)
                        | K::Minimum(_)
                        | K::RowMode(_)
                        | K::RowSize(_)
                )
            });
            for (pos, (r, m)) in rows.iter().enumerate() {
                let i = r.index;
                f.label(K::Number(i), &(pos + 1).to_string());
                f.label(K::Kindcell(i), &r.type_name);
                f.label(K::Original(i), "");
                f.label(K::Minimum(i), "");
                let names: Vec<String> = m.iter().map(|x| x.label().to_string()).collect();
                f.popup(K::RowMode(i), &names, &format!("{}.{i}", ids::ROW_MODE));
                f.field(
                    K::RowSize(i),
                    &format!("{}.{i}", ids::ROW_SIZE),
                    "Size (MiB)",
                    false,
                );
            }
            *self.rows.borrow_mut() = want;
        }
        for (r, m) in rows {
            let i = r.index;
            f.set_text(K::Original(i), &format_size(r.original_size));
            f.set_text(K::Minimum(i), &format_size(r.minimum_size));
            f.select(
                K::RowMode(i),
                m.iter().position(|x| *x == r.mode).unwrap_or(0),
            );
            let text = match r.mode {
                SizeMode::Custom => r.custom_mib_effective().to_string(),
                SizeMode::FillRemaining => String::new(),
                _ => (r.effective_size() / MIB).to_string(),
            };
            f.set_text(K::RowSize(i), &text);
            f.enable(K::RowSize(i), r.mode == SizeMode::Custom);
        }
        for h in 0..6 {
            f.show(K::Head(h), !rows.is_empty());
        }
    }

    /// The remote target rows; true while they stand in for the local target.
    fn show_remote(&self, c: &RestoreController, full: bool) -> bool {
        let f = &self.form;
        let on = full && c.remote_active();
        f.show(K::RemoteSwitch, full);
        f.set_check(K::RemoteSwitch, on);
        for k in [K::RemoteCaption, K::RemoteAddress, K::RemoteConnect] {
            f.show(k, on);
        }
        f.set_text(K::RemoteAddress, c.remote_address());
        f.enable(K::RemoteConnect, !c.remote_busy());
        let label = if c.remote_connected() {
            "Reconnect"
        } else {
            "Connect"
        };
        f.set_title(K::RemoteConnect, label);
        f.show(K::RemoteBusy, on && c.remote_busy());
        f.line(
            K::RemoteError,
            c.remote_error().filter(|_| on),
            Some(Tone::Error),
        );
        let linked = on && c.remote_connected();
        let drives = if linked {
            c.remote_drives()
        } else {
            Vec::new()
        };
        let note = match (linked, drives.is_empty()) {
            (true, true) => Some(hints::NO_REMOTE_DRIVES),
            (true, false) => Some("Restore target on the remote machine (double-click or Enter):"),
            (false, _) => None,
        };
        f.line(K::RemoteNote, note, None);
        f.show(K::RemoteList, linked && !drives.is_empty());
        if *self.remote_drives.borrow() != drives {
            self.remote_list
                .set_rows(drives.iter().map(|d| vec![d.label.clone()]).collect());
            *self.remote_drives.borrow_mut() = drives;
        }
        for k in [K::RemoteImageCaption, K::RemoteImage, K::RemoteUseImage] {
            f.show(k, linked);
        }
        f.set_text(K::RemoteImage, c.remote_image_name());
        f.enable(K::RemoteUseImage, !c.remote_image_name().trim().is_empty());
        let chosen = c
            .remote_target_label()
            .filter(|_| linked)
            .map(|t| format!("Selected target: {t}"));
        f.line(K::RemoteSelected, chosen.as_deref(), None);
        on
    }

    /// Return or a double-click on a daemon's drive makes it the target.
    fn remote_picked(&self) {
        let Some(d) = self
            .remote_list
            .selected()
            .and_then(|i| self.remote_drives.borrow().get(i).cloned())
        else {
            return;
        };
        let me = self.me.clone();
        crate::app::later(Box::new(move || {
            if let Some(s) = me.upgrade() {
                s.apply(RestoreAction::RemotePickDevice {
                    path: d.path,
                    size: d.size_bytes,
                });
            }
        }));
    }

    /// The controls a running restore or another screen's job locks.
    fn form_controls(&self) -> Vec<K> {
        let mut v = vec![
            K::RemoteSwitch,
            K::RemoteAddress,
            K::RemoteConnect,
            K::RemoteImage,
            K::RemoteUseImage,
            K::Mode,
            K::Folder,
            K::Recent,
            K::PickImage,
            K::Source,
            K::WriteChoose,
            K::Kind,
            K::Device,
            K::CloseTarget,
            K::Scan,
            K::TargetImage,
            K::SaveAs,
            K::Part,
            K::Table,
            K::NewAlign,
            K::NewCustom,
            K::Bootable,
            K::Expand,
            K::Free,
            K::Extend,
            K::Align,
            K::Custom,
            K::MacDriver,
        ];
        for (i, _) in self.rows.borrow().iter() {
            v.push(K::RowMode(*i));
        }
        v
    }

    pub fn relayout(&self) {
        let row_h = Size::Fixed(layout::ROW);
        let caption = 130;
        let cap = |k: K| Node::leaf(k, Size::Fixed(caption), Size::Fixed(layout::ROW - 4));
        let fixed = |k: K, w: i32| Node::leaf(k, Size::Fixed(w), row_h);
        let pop = |k: K, w: Size| Node::leaf(k, w, Size::Fixed(layout::ROW + 2));
        let stretch = |k: K| Node::leaf(k, Size::Stretch(1), row_h);
        let lines = |k: K| {
            let n = self.form.text(k).lines().count().max(1) as i32;
            Node::leaf(k, Size::Stretch(1), Size::Fixed((layout::ROW - 6) * n + 4))
        };
        let mut col = vec![
            lines(K::BusyNote),
            Node::row(vec![
                cap(K::ModeCaption),
                pop(K::Mode, Size::Fixed(240)),
                stretch(K::Job),
                Node::leaf(
                    K::JobProgress,
                    Size::Fixed(160),
                    Size::Fixed(layout::ROW - 4),
                ),
            ]),
            Node::row(vec![
                cap(K::FolderCaption),
                stretch(K::Folder),
                pop(K::Recent, Size::Fixed(100)),
            ]),
            Node::row(vec![
                cap(K::ImageCaption),
                stretch(K::Image),
                Node::button(K::PickImage, 90),
            ]),
            Node::row(vec![
                cap(K::SourceCaption),
                pop(K::Source, Size::Stretch(1)),
            ]),
            lines(K::Summary),
            lines(K::LoadError),
            Node::row(vec![
                cap(K::WriteCaption),
                stretch(K::WritePath),
                Node::button(K::WriteChoose, 90),
            ]),
            lines(K::WriteLine),
            Node::row(vec![
                Node::button(K::WriteTarget, 220),
                Node::Space(Size::Stretch(1)),
            ]),
            Node::row(vec![
                Node::Space(Size::Fixed(caption)),
                stretch(K::RemoteSwitch),
            ]),
            Node::row(vec![
                cap(K::RemoteCaption),
                Node::field(K::RemoteAddress),
                Node::button(K::RemoteConnect, 100),
                fixed(K::RemoteBusy, 100),
            ]),
            lines(K::RemoteError),
            lines(K::RemoteNote),
            Node::leaf(
                K::RemoteList,
                Size::Stretch(1),
                Size::Fixed(layout::ROW * 3),
            ),
            Node::row(vec![
                cap(K::RemoteImageCaption),
                Node::field(K::RemoteImage),
                Node::button(K::RemoteUseImage, 90),
            ]),
            stretch(K::RemoteSelected),
            Node::row(vec![
                cap(K::KindCaption),
                pop(K::Kind, Size::Fixed(170)),
                pop(K::Device, Size::Stretch(1)),
                Node::field(K::TargetImage),
                Node::button(K::SaveAs, 95),
                Node::button(K::CloseTarget, 100),
                Node::button(K::Scan, 110),
            ]),
            lines(K::Warning),
            lines(K::Readout),
            Node::row(vec![cap(K::PartCaption), pop(K::Part, Size::Stretch(1))]),
            lines(K::ScanError),
            Node::row(vec![
                cap(K::TableCaption),
                pop(K::Table, Size::Fixed(90)),
                fixed(K::NewAlignCaption, 75),
                pop(K::NewAlign, Size::Fixed(160)),
                fixed(K::NewCustom, 70),
                stretch(K::Bootable),
            ]),
            Node::row(vec![
                Node::Space(Size::Fixed(caption)),
                stretch(K::StartLba),
            ]),
            Node::row(vec![stretch(K::Expand)]),
            Node::row(vec![
                cap(K::FreeCaption),
                fixed(K::Free, 80),
                pop(K::Extend, Size::Fixed(330)),
                stretch(K::ExpandLine),
            ]),
            Node::row(vec![
                cap(K::CurrentCaption),
                Node::leaf(
                    K::Current,
                    Size::Stretch(1),
                    Size::Fixed(partition_bar::BAR_ONLY),
                ),
            ]),
            Node::row(vec![
                cap(K::AfterCaption),
                Node::leaf(
                    K::After,
                    Size::Stretch(1),
                    Size::Fixed(partition_bar::HEIGHT),
                ),
            ]),
            Node::row(vec![
                cap(K::AlignCaption),
                pop(K::Align, Size::Fixed(310)),
                fixed(K::Custom, 70),
                Node::Space(Size::Stretch(1)),
            ]),
        ];
        let widths = [30, 200, 100, 100, 160, 100];
        col.push(Node::row(
            (0..6)
                .zip(widths)
                .map(|(h, w)| fixed(K::Head(h), w))
                .collect(),
        ));
        for (i, _) in self.rows.borrow().iter() {
            let i = *i;
            col.push(Node::row(vec![
                fixed(K::Number(i), widths[0]),
                fixed(K::Kindcell(i), widths[1]),
                fixed(K::Original(i), widths[2]),
                fixed(K::Minimum(i), widths[3]),
                pop(K::RowMode(i), Size::Fixed(widths[4])),
                fixed(K::RowSize(i), widths[5]),
            ]));
        }
        col.push(lines(K::Notes));
        col.push(Node::row(vec![stretch(K::MacDriver)]));
        col.push(lines(K::Caveats));
        col.push(Node::Space(Size::Stretch(1)));
        col.push(stretch(K::TargetLine));
        col.push(Node::row(vec![
            Node::button(K::Start, 130),
            Node::button(K::Cancel, 120),
            fixed(K::Progress, 200),
            stretch(K::Run),
            stretch(K::Why),
            stretch(K::Outcome),
        ]));
        self.form
            .place_page(Node::inset(Node::column(col), layout::MARGIN));
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

    /// A control's action or a field's change.
    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let f = &self.form;
        let n = || f.text(k).trim().parse::<u32>().ok();
        let slot = self.slot();
        let action = match k {
            K::Mode => f
                .picked(k)
                .and_then(|i| MODES.get(i))
                .map(|m| RestoreAction::SetMode(m.0)),
            K::Folder => Some(RestoreAction::PickBackupFolder),
            K::RecentItem(i) => self
                .recent_files
                .borrow()
                .get(i)
                .map(|p| RestoreAction::OpenDropped(PathBuf::from(p))),
            K::PickImage => Some(RestoreAction::PickSourceImage),
            K::Source => {
                let index = f.picked(k).and_then(|i| i.checked_sub(1)).and_then(|i| {
                    self.ctrl
                        .borrow()
                        .source_partitions()
                        .get(i)
                        .map(|p| p.index)
                });
                Some(RestoreAction::SetSourcePartition(index))
            }
            K::WriteChoose => Some(RestoreAction::PickWriteImage),
            K::WriteTarget => Some(RestoreAction::WriteImageTarget),
            K::RemoteSwitch => Some(RestoreAction::SetRemote(f.checked(k))),
            K::RemoteConnect => Some(RestoreAction::RemoteConnect),
            K::RemoteUseImage => Some(RestoreAction::RemoteUseImage),
            K::RemoteAddress => Some(RestoreAction::RemoteSetAddress(f.text(k))),
            K::RemoteImage => Some(RestoreAction::RemoteSetImageName(f.text(k))),
            K::Kind => f
                .picked(k)
                .map(|i| RestoreAction::SetTargetIsDevice(slot, i == 0)),
            K::Device => match f.picked(k).and_then(|i| i.checked_sub(1)) {
                None => Some(RestoreAction::ClearDevice(slot)),
                Some(row) => self
                    .devices
                    .borrow()
                    .get(row)
                    .cloned()
                    .map(|device| RestoreAction::SelectDevice { slot, row, device }),
            },
            K::CloseTarget => Some(RestoreAction::ClearDevice(slot)),
            K::Scan => Some(RestoreAction::ScanTarget),
            K::TargetImage => {
                let text = f.text(k);
                let path = (!text.trim().is_empty()).then(|| PathBuf::from(text.trim()));
                Some(RestoreAction::SetTargetImage(slot, path))
            }
            K::SaveAs => Some(RestoreAction::PickTargetImage(slot)),
            K::Part => {
                let i = f.picked(k).and_then(|i| i.checked_sub(1));
                i.and_then(|i| {
                    self.ctrl
                        .borrow()
                        .target_partitions()
                        .get(i)
                        .map(|p| p.index)
                })
                .map(RestoreAction::SetTargetPartition)
            }
            K::Table => f
                .picked(k)
                .and_then(|i| TABLES.get(i))
                .map(|t| self.new_disk(|c| c.table = t.0)),
            K::NewAlign => f
                .picked(k)
                .and_then(|i| NEW_ALIGNMENTS.get(i))
                .map(|a| self.new_disk(|c| c.alignment = a.0)),
            K::NewCustom => n()
                .filter(|n| (1..=ALIGN_SECTORS_MAX).contains(n))
                .map(|n| self.new_disk(|c| c.custom_alignment = n)),
            K::Bootable => {
                let on = f.checked(k);
                Some(self.new_disk(|c| c.bootable = on))
            }
            K::Expand => Some(RestoreAction::SetExpand(f.checked(k))),
            K::Free => n().map(RestoreAction::SetExpandMib),
            K::Extend => f.picked(k).map(|i| RestoreAction::SetExtendLast(i == 1)),
            K::Align => f
                .picked(k)
                .and_then(|i| ALIGNMENTS.get(i))
                .map(|a| RestoreAction::SetAlignment(*a)),
            K::Custom => n()
                .filter(|n| (1..=ALIGN_SECTORS_MAX).contains(n))
                .map(RestoreAction::SetCustomAlignment),
            K::RowMode(i) | K::RowSize(i) => {
                let choices = self
                    .rows
                    .borrow()
                    .iter()
                    .find(|(x, _)| *x == i)
                    .map(|(_, m)| m.clone())
                    .unwrap_or_default();
                let mode = f
                    .picked(K::RowMode(i))
                    .and_then(|p| choices.get(p).copied());
                let custom_mib = f.text(K::RowSize(i)).trim().parse::<u32>().unwrap_or(0);
                match (k, mode) {
                    (K::RowSize(_), Some(SizeMode::Custom)) | (K::RowMode(_), Some(_)) => {
                        mode.map(|mode| RestoreAction::SetPartitionSize {
                            index: i,
                            mode,
                            custom_mib,
                        })
                    }
                    _ => None,
                }
            }
            K::MacDriver => Some(RestoreAction::SetInstallMacDriver(f.checked(k))),
            // Start only asks; the controller's own question begins the write.
            K::Start => self.may_start().then_some(RestoreAction::Start),
            K::Cancel => Some(RestoreAction::Cancel),
            _ => None,
        };
        if let Some(a) = action {
            self.apply(a);
        }
    }
}
