//! The Backup screen over `controller::backup::BackupController`; the Win32 shell's `backup.rs`
//! is the reference.
//!
//! Slice 4: the source (a pull-down of recent images and devices, with Open Image File, Close
//! Drive and Load partition info), the partition rows with Include, Calc min and Compact, the
//! destination, name, output format with the shared CHD control, the sizing and split options,
//! the checksum, and Start / Cancel with the progress line and the outcome. The VHD options
//! window is `backup_vhd.rs`, the Remote Drive window `backup_remote.rs`.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::NSView;
use rusty_backup::controller::app::chd::ChdOptionsControl;
use rusty_backup::controller::app::{
    busy_elsewhere_notice, no_devices_hint, DiskDevice, Notify, RecentHandle, RecentMode,
};
use rusty_backup::controller::backup::{
    format_size, hints, ids, labels, BackupAction, BackupController, BackupServices, ChdChoice,
    ChdProfile, CompactCell, CompressionType, HostRequest, JobRegistry, MinCell, PartitionRow,
    CHECKSUMS, FORMATS, SPLIT_MIB_MAX, SPLIT_MIB_MIN,
};
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::{Controller, Effect};

use crate::backup_remote::{RemoteShown, RemoteWindow};
use crate::backup_vhd::{VhdShown, VhdWindow};
use crate::chd::{ChdControl, ChdIds, ChdKey};
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::sync::SyncGuard;

/// The source menu's picks: a recent image, a device, or one of the ways to open one.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Pick {
    Recent(usize),
    Device(usize),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    BusyNote,
    SourceCaption,
    Source,
    Pick(Pick),
    OpenImage,
    RemoteOpen,
    Close,
    ClearRemote,
    Scan,
    Job,
    JobProgress,
    CancelScan,
    PartsCaption,
    All,
    None,
    Table,
    Head(usize),
    Include(usize),
    Position(usize),
    Kind(usize),
    Size(usize),
    Min(usize),
    Calc(usize),
    Frag(usize),
    Compact(usize),
    Error,
    DestinationCaption,
    Destination,
    Browse,
    NameCaption,
    Name,
    FormatCaption,
    Format,
    Chd(ChdKey),
    SectorBySector,
    Resize,
    KeepSwap,
    Split,
    SplitCaption,
    SplitMib,
    ChecksumCaption,
    Checksum,
    Outcome,
    Run,
    Progress,
    Start,
    CancelPending,
    Cancel,
    Why,
}

fn chd_key(k: ChdKey) -> K {
    K::Chd(k)
}

pub struct BackupScreen {
    ctrl: RefCell<BackupController>,
    host: EffectHost,
    mtm: MainThreadMarker,
    recent: RecentHandle,
    recent_files: RefCell<Vec<String>>,
    devices: RefCell<Vec<DiskDevice>>,
    /// Another screen's disk job, which locks this one.
    busy: Cell<Option<&'static str>>,
    sync: SyncGuard,
    me: Weak<BackupScreen>,
    form: Form<K>,
    chd: ChdControl<K>,
    /// The partitions the rows were built for.
    rows: RefCell<Vec<usize>>,
    vhd: RefCell<Option<Rc<VhdWindow>>>,
    /// The Remote Drive window, while the controller holds it open.
    remote: RefCell<Option<Rc<RemoteWindow>>>,
}

impl BackupScreen {
    pub fn build(
        mtm: MainThreadMarker,
        body: Retained<NSView>,
        notify: Arc<dyn Notify>,
        recent: RecentHandle,
        host: EffectHost,
    ) -> Rc<BackupScreen> {
        let screen = Rc::new_cyclic(|me: &Weak<BackupScreen>| {
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
            form.label(K::BusyNote, "");
            form.label(K::SourceCaption, "Source:");
            form.pulldown(K::Source, ids::SOURCE);
            form.button(K::OpenImage, "Open Image File...", ids::OPEN_IMAGE);
            form.button(K::RemoteOpen, "Remote Drive...", ids::REMOTE_OPEN);
            form.button(K::Close, "Close Drive", ids::CLOSE_SOURCE);
            form.button(K::ClearRemote, "Disconnect", ids::CLEAR_REMOTE);
            form.button(K::Scan, "Load partition info", ids::LOAD_PARTITIONS);
            form.label(K::Job, "");
            form.progress(K::JobProgress);
            form.button(K::CancelScan, "Cancel", ids::CANCEL_SCAN);
            form.label(K::PartsCaption, "Partitions");
            form.button(K::All, "All", ids::SELECT_ALL);
            form.button(K::None, "None", ids::SELECT_NONE);
            form.label(K::Table, "");
            for (i, t) in ["", "#", "Type", "Size", "Min", "Frag", "Compact"]
                .iter()
                .enumerate()
            {
                form.label(K::Head(i), t);
            }
            form.wrap(K::Error, "");
            form.label(K::DestinationCaption, &format!("{}:", labels::DESTINATION));
            form.field(K::Destination, ids::DESTINATION, labels::DESTINATION, false);
            form.button(K::Browse, "Browse...", ids::PICK_DESTINATION);
            form.label(K::NameCaption, &format!("{}:", labels::NAME));
            form.field(K::Name, ids::NAME, labels::NAME, false);
            form.label(K::FormatCaption, &format!("{}:", labels::FORMAT));
            let formats: Vec<String> = FORMATS.iter().map(|f| f.1.to_string()).collect();
            form.popup(K::Format, &formats, ids::FORMAT);
            let mut custom = ChdOptionsControl::new(ChdProfile::Hd);
            custom.custom = BackupController::saved_chd_custom(custom.custom.clone());
            let ids_chd = ChdIds {
                mode: ids::CHD_MODE,
                hunk: ids::CHD_HUNK,
                codec: ids::CHD_CODEC,
            };
            let chd = ChdControl::build(&form, chd_key, ids_chd, custom, ChdProfile::Hd);
            form.check(
                K::SectorBySector,
                hints::SECTOR_BY_SECTOR,
                ids::SECTOR_BY_SECTOR,
            );
            form.check(K::Resize, hints::RESIZE, ids::RESIZE);
            form.check(K::KeepSwap, hints::KEEP_SWAP, ids::KEEP_SWAP);
            form.check(K::Split, "Split archives", ids::SPLIT);
            form.label(K::SplitCaption, "Split size (MiB):");
            form.field(K::SplitMib, ids::SPLIT_MIB, "Split size (MiB)", false);
            form.label(K::ChecksumCaption, "Checksum:");
            let checksums: Vec<String> = CHECKSUMS.iter().map(|c| c.1.to_string()).collect();
            form.popup(K::Checksum, &checksums, ids::CHECKSUM);
            form.label(K::Outcome, "");
            form.label(K::Run, "");
            form.progress(K::Progress);
            form.button(K::Start, labels::START, ids::START);
            form.button(K::CancelPending, "Cancel", ids::CANCEL_PENDING);
            form.button(K::Cancel, "Cancel", ids::CANCEL);
            form.label(K::Why, "");
            BackupScreen {
                ctrl: RefCell::new(BackupController::new(BackupServices {
                    jobs: JobRegistry::new(),
                    notify,
                })),
                host,
                mtm,
                recent_files: RefCell::new(recent.list(RecentMode::Backup)),
                recent,
                devices: RefCell::default(),
                busy: Cell::new(None),
                sync: SyncGuard::default(),
                me: me.clone(),
                form,
                chd,
                rows: RefCell::default(),
                vhd: RefCell::default(),
                remote: RefCell::default(),
            }
        });
        screen.refresh();
        screen
    }

    /// The app's device list; a change under the open device closes or re-reads it.
    pub fn set_devices(&self, devices: &[DiskDevice]) {
        self.ctrl.borrow_mut().reconcile_devices(devices);
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
        self.ctrl.borrow().is_running()
    }

    pub fn apply(&self, action: BackupAction) {
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
            let Some(screen) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => BackupAction::Picked { purpose, path },
                // Backup only ever opens pickers.
                Reply::Confirmed { .. }
                | Reply::Answered { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => return,
            };
            screen.apply(action);
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
                HostRequest::RememberRecent(path) => self.remember(path),
            }
        }
        crate::app::append_log(log);
        self.show_vhd();
        self.show_remote();
        self.refresh();
    }

    fn remember(&self, path: PathBuf) {
        let list = self
            .recent
            .remember(RecentMode::Backup, &path.display().to_string());
        *self.recent_files.borrow_mut() = list;
    }

    /// What Start carries from the CHD control.
    fn chd_choice(&self) -> Option<ChdChoice> {
        let chd = self.ctrl.borrow().options().compression == CompressionType::Chd;
        chd.then(|| {
            let (options, custom) = self.chd.choice();
            ChdChoice { options, custom }
        })
    }

    /// The Remote Drive window follows the controller: open while it says so.
    fn show_remote(&self) {
        let shown = {
            let ctrl = self.ctrl.borrow();
            ctrl.remote_dialog_open().then(|| RemoteShown {
                address: ctrl.remote_address().to_string(),
                busy: ctrl.remote_busy(),
                error: ctrl.remote_error().map(str::to_string),
                drives: ctrl.remote_listed().then(|| ctrl.remote_drives()),
            })
        };
        let Some(shown) = shown else {
            self.remote.replace(None);
            return;
        };
        let held = self.remote.borrow().clone();
        let w = held.unwrap_or_else(|| {
            let me = self.me.clone();
            let send: Rc<dyn Fn(BackupAction)> = Rc::new(move |a| {
                if let Some(s) = me.upgrade() {
                    s.apply(a);
                }
            });
            let w = RemoteWindow::open(self.mtm, send);
            *self.remote.borrow_mut() = Some(w.clone());
            w
        });
        w.show(shown);
    }

    /// The VHD options window follows the controller: open while it says so, rebuilt for new rows.
    fn show_vhd(&self) {
        let shown = {
            let ctrl = self.ctrl.borrow();
            ctrl.vhd_open().then(|| VhdShown {
                whole_disk: ctrl.vhd_whole_disk(),
                rows: ctrl
                    .vhd_rows()
                    .iter()
                    .map(|r| (r.clone(), ctrl.min_phase(r.index)))
                    .collect(),
                can_start: !ctrl.is_running() && self.busy.get().is_none(),
            })
        };
        let Some(shown) = shown else {
            self.vhd.replace(None);
            return;
        };
        let rows: Vec<usize> = shown.rows.iter().map(|(r, _)| r.index).collect();
        let held = self.vhd.borrow().clone().filter(|w| w.indices() == rows);
        let w = held.unwrap_or_else(|| {
            self.vhd.replace(None);
            let me = self.me.clone();
            let send: Rc<dyn Fn(BackupAction)> = Rc::new(move |a| {
                if let Some(s) = me.upgrade() {
                    s.apply(a);
                }
            });
            let list: Vec<_> = shown.rows.iter().map(|(r, _)| r.clone()).collect();
            let w = VhdWindow::open(self.mtm, send, &list);
            *self.vhd.borrow_mut() = Some(w.clone());
            w
        });
        w.show(shown);
    }

    /// Start's whole rule: asked again at the click, not only when the button was last drawn.
    fn may_start(&self) -> bool {
        let ctrl = self.ctrl.borrow();
        let chd_ok = ctrl.options().compression != CompressionType::Chd || self.chd.is_valid();
        ctrl.can_start() && chd_ok && self.busy.get().is_none()
    }

    /// The source pull-down: recent images, devices, then the ways to open one.
    fn show_source(&self, title: &str) {
        let recent = self.recent_files.borrow().clone();
        let devices = self.devices.borrow().clone();
        let mut items: Vec<(String, Option<K>, bool)> = recent
            .iter()
            .enumerate()
            .map(|(i, p)| (p.clone(), Some(K::Pick(Pick::Recent(i))), true))
            .collect();
        if devices.is_empty() {
            items.push((no_devices_hint().to_string(), None, false));
        }
        for (i, d) in devices.iter().enumerate() {
            items.push((d.display_name(), Some(K::Pick(Pick::Device(i))), true));
        }
        items.push(("Open Image File...".into(), Some(K::OpenImage), true));
        items.push(("Remote Drive...".into(), Some(K::RemoteOpen), true));
        let title = if title.is_empty() {
            "Select a device or image..."
        } else {
            title
        };
        self.form.set_pulldown(K::Source, title, &items);
    }

    /// Read the controller first and release it, then set the views inside the guard.
    fn refresh(&self) {
        let can_start = self.may_start();
        let f = &self.form;
        let ctrl = self.ctrl.borrow();
        let o = ctrl.options().clone();
        let devices = self.devices.borrow().clone();
        let source = ctrl.source_label(devices.as_slice());
        let has_local = ctrl.has_local_source();
        let remote = ctrl.remote_active();
        let can_scan = ctrl.can_scan_device().then(|| ctrl.scan_button());
        let mut jobs = Vec::new();
        jobs.extend(ctrl.decoding().map(|s| (s, false)));
        jobs.extend(ctrl.scanning().map(|s| (s, true)));
        let job = jobs.into_iter().next();
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
        let table = ctrl.table_line();
        let rows = ctrl.rows();
        let error = ctrl.load_error().map(str::to_string);
        let destination = ctrl
            .destination()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        let name = ctrl.name().to_string();
        let running = ctrl.is_running();
        let cancelling = ctrl.cancelling();
        let pending = ctrl.start_pending();
        let start_label = ctrl.start_label();
        let why = ctrl
            .validate()
            .err()
            .or_else(|| self.busy.get().map(busy_elsewhere_notice));
        let outcome = ctrl.last_result().cloned();
        drop(ctrl);
        self.sync.run(|| {
            self.show_source(&source);
            let notice = self.busy.get().map(busy_elsewhere_notice);
            f.set_text(K::BusyNote, notice.as_deref().unwrap_or(""));
            f.show(K::BusyNote, notice.is_some());
            f.show(K::Close, has_local);
            f.show(K::ClearRemote, remote);
            f.show(K::Scan, can_scan.is_some());
            if let Some((label, _)) = can_scan {
                f.set_title(K::Scan, label);
            }
            let (line, jpct) = job
                .as_ref()
                .map(|(j, _)| crate::inspect::job_line(j))
                .unwrap_or_default();
            f.set_text(K::Job, &line);
            f.show(K::Job, job.is_some());
            if let Some(p) = f
                .v(K::JobProgress)
                .downcast_ref::<objc2_app_kit::NSProgressIndicator>()
            {
                crate::ui::set_progress(p, job.is_some(), jpct);
            }
            f.show(K::CancelScan, job.as_ref().is_some_and(|(_, c)| *c));
            f.set_text(K::Table, table.as_deref().unwrap_or(""));
            self.show_rows(&rows);
            f.set_text(K::Error, error.as_deref().unwrap_or(""));
            f.show(K::Error, error.is_some());
            f.set_text(K::Destination, &destination);
            f.set_text(K::Name, &name);
            f.select(
                K::Format,
                FORMATS
                    .iter()
                    .position(|x| x.0 == o.compression)
                    .unwrap_or(0),
            );
            self.chd
                .set_visible(f, o.compression == CompressionType::Chd);
            f.set_check(K::SectorBySector, o.sector_by_sector);
            f.set_check(K::Resize, o.resize);
            f.set_check(K::KeepSwap, o.keep_swap);
            f.show(K::Split, o.split_shown());
            f.set_check(K::Split, o.split);
            let mib = o.split_shown() && o.split && o.split_editable();
            f.show(K::SplitCaption, mib);
            f.show(K::SplitMib, mib);
            f.set_text(K::SplitMib, &o.split_mib.to_string());
            f.select(
                K::Checksum,
                CHECKSUMS
                    .iter()
                    .position(|c| c.0 == o.checksum)
                    .unwrap_or(0),
            );
            let free = !running && self.busy.get().is_none();
            for k in self.form_controls() {
                f.enable(k, free);
            }
            f.enable(K::Split, free && o.split_editable());
            f.enable(K::Resize, free && o.size_options_editable());
            f.enable(K::KeepSwap, free && o.size_options_editable());
            f.show(K::Start, !running);
            f.enable(K::Start, can_start);
            f.set_title(K::Start, start_label);
            f.show(K::CancelPending, pending);
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
            f.set_text(K::Run, run.as_deref().unwrap_or(""));
            f.show(K::Run, run.is_some());
            if let Some(p) = f
                .v(K::Progress)
                .downcast_ref::<objc2_app_kit::NSProgressIndicator>()
            {
                crate::ui::set_progress(
                    p,
                    run.is_some(),
                    pct.or(Some(0)).filter(|_| run.is_some()),
                );
            }
            f.set_text(K::Why, why.as_deref().unwrap_or(""));
            f.show(K::Why, !running && !can_start);
            // The footer shows one line: progress, else why Start is off, else the outcome.
            f.show(K::Outcome, run.is_none() && (running || can_start));
            let (text, tone) = match &outcome {
                Some(Ok(())) => (labels::DONE.to_string(), None),
                Some(Err(e)) => (e.clone(), Some(Tone::Error)),
                None => (String::new(), None),
            };
            f.set_text(K::Outcome, &text);
            f.set_tone(K::Outcome, tone);
        });
        self.relayout();
    }

    /// The controls a running backup or another screen's job locks.
    fn form_controls(&self) -> Vec<K> {
        let mut v = vec![
            K::Source,
            K::OpenImage,
            K::Close,
            K::Scan,
            K::All,
            K::None,
            K::Destination,
            K::Browse,
            K::Name,
            K::Format,
            K::SectorBySector,
            K::SplitMib,
            K::Checksum,
            K::Chd(ChdKey::Mode),
            K::Chd(ChdKey::Hunk),
        ];
        v.extend((0..4).map(|i| K::Chd(ChdKey::Codec(i))));
        for i in self.rows.borrow().iter() {
            v.extend([K::Include(*i), K::Calc(*i)]);
        }
        v
    }

    /// Build the rows again when the partitions change, then show each.
    fn show_rows(&self, rows: &[PartitionRow]) {
        let f = &self.form;
        let want: Vec<usize> = rows.iter().map(|r| r.index).collect();
        if *self.rows.borrow() != want {
            f.remove(|k| {
                matches!(
                    k,
                    K::Include(_)
                        | K::Position(_)
                        | K::Kind(_)
                        | K::Size(_)
                        | K::Min(_)
                        | K::Calc(_)
                        | K::Frag(_)
                        | K::Compact(_)
                )
            });
            for r in rows {
                let i = r.index;
                f.check(K::Include(i), "", &format!("{}.{i}", ids::ROW_INCLUDE));
                f.label(K::Position(i), &r.position.to_string());
                f.label(K::Kind(i), &r.type_name);
                f.label(K::Size(i), &format_size(r.size_bytes));
                f.label(K::Min(i), "");
                f.button(
                    K::Calc(i),
                    "Calc min",
                    &format!("{}.{i}", ids::ROW_CALC_MIN),
                );
                f.label(K::Frag(i), "");
                f.check(K::Compact(i), "", &format!("{}.{i}", ids::ROW_COMPACT));
            }
            *self.rows.borrow_mut() = want;
        }
        for r in rows {
            let i = r.index;
            f.set_check(K::Include(i), r.selected);
            let min = match &r.min {
                MinCell::Size(min) => format_size(*min),
                MinCell::Pending(phase) => phase.clone(),
                MinCell::CanCalc | MinCell::Empty => String::new(),
            };
            f.set_text(K::Min(i), &min);
            let calc = r.min == MinCell::CanCalc;
            f.show(K::Calc(i), calc);
            f.show(K::Min(i), !calc);
            let frag = r.fragmentation.map(|p| format!("{p:.0}%"));
            f.set_text(K::Frag(i), frag.as_deref().unwrap_or(""));
            match r.compact {
                CompactCell::Hidden => f.show(K::Compact(i), false),
                CompactCell::NotImplemented => {
                    f.show(K::Compact(i), true);
                    f.enable(K::Compact(i), false);
                    f.set_check(K::Compact(i), false);
                }
                CompactCell::Toggle(on) => {
                    f.show(K::Compact(i), true);
                    f.enable(K::Compact(i), true);
                    f.set_check(K::Compact(i), on);
                }
            }
        }
        let any = !rows.is_empty();
        for k in [K::PartsCaption, K::All, K::None, K::Table] {
            f.show(k, any);
        }
        for h in 0..7 {
            f.show(K::Head(h), any);
        }
    }

    pub fn relayout(&self) {
        let row_h = Size::Fixed(layout::ROW);
        let line = |k: K| Node::leaf(k, Size::Stretch(1), Size::Fixed(layout::ROW - 4));
        let fixed = |k: K, w: i32| Node::leaf(k, Size::Fixed(w), row_h);
        let mut col = vec![
            line(K::BusyNote),
            Node::row(vec![
                Node::leaf(
                    K::SourceCaption,
                    Size::Fixed(60),
                    Size::Fixed(layout::ROW - 4),
                ),
                Node::leaf(K::Source, Size::Stretch(1), Size::Fixed(layout::ROW + 2)),
                Node::button(K::OpenImage, 140),
                Node::button(K::RemoteOpen, 125),
                Node::button(K::Close, 100),
                Node::button(K::ClearRemote, 95),
                Node::button(K::Scan, 150),
            ]),
            Node::row(vec![
                Node::leaf(K::Job, Size::Stretch(1), Size::Fixed(layout::ROW - 4)),
                Node::leaf(
                    K::JobProgress,
                    Size::Fixed(160),
                    Size::Fixed(layout::ROW - 4),
                ),
                Node::button(K::CancelScan, 75),
            ]),
            Node::row(vec![
                fixed(K::PartsCaption, 80),
                Node::button(K::All, 55),
                Node::button(K::None, 60),
                Node::leaf(K::Table, Size::Stretch(1), Size::Fixed(layout::ROW - 4)),
            ]),
        ];
        let widths = [24, 30, 220, 90, 160, 60, 60];
        col.push(Node::row(
            (0..7)
                .zip(widths)
                .map(|(h, w)| fixed(K::Head(h), w))
                .collect(),
        ));
        for i in self.rows.borrow().iter() {
            let i = *i;
            col.push(Node::row(vec![
                fixed(K::Include(i), widths[0]),
                fixed(K::Position(i), widths[1]),
                fixed(K::Kind(i), widths[2]),
                fixed(K::Size(i), widths[3]),
                fixed(K::Min(i), widths[4]),
                Node::button(K::Calc(i), widths[4]),
                fixed(K::Frag(i), widths[5]),
                fixed(K::Compact(i), widths[6]),
            ]));
        }
        col.push(Node::leaf(
            K::Error,
            Size::Stretch(1),
            Size::Fixed(2 * (layout::ROW - 4)),
        ));
        col.push(Node::row(vec![
            Node::leaf(
                K::DestinationCaption,
                Size::Fixed(110),
                Size::Fixed(layout::ROW - 4),
            ),
            Node::field(K::Destination),
            Node::button(K::Browse, 95),
        ]));
        col.push(Node::row(vec![
            Node::leaf(
                K::NameCaption,
                Size::Fixed(110),
                Size::Fixed(layout::ROW - 4),
            ),
            Node::field(K::Name),
            Node::leaf(
                K::FormatCaption,
                Size::Fixed(105),
                Size::Fixed(layout::ROW - 4),
            ),
            Node::leaf(K::Format, Size::Fixed(170), Size::Fixed(layout::ROW + 2)),
            Node::leaf(
                K::ChecksumCaption,
                Size::Fixed(75),
                Size::Fixed(layout::ROW - 4),
            ),
            Node::leaf(K::Checksum, Size::Fixed(130), Size::Fixed(layout::ROW + 2)),
        ]));
        col.extend(self.chd.rows());
        col.push(Node::row(vec![
            Node::leaf(K::SectorBySector, Size::Stretch(1), row_h),
            Node::leaf(K::Resize, Size::Stretch(1), row_h),
        ]));
        col.push(Node::row(vec![
            Node::leaf(K::KeepSwap, Size::Stretch(1), row_h),
            Node::row(vec![
                fixed(K::Split, 140),
                fixed(K::SplitCaption, 125),
                fixed(K::SplitMib, 70),
                Node::Space(Size::Stretch(1)),
            ]),
        ]));
        col.push(Node::Space(Size::Stretch(1)));
        // One footer row: the buttons, then the progress, or why Start is off, or how it went.
        col.push(Node::row(vec![
            Node::button(K::Start, 130),
            Node::button(K::CancelPending, 85),
            Node::button(K::Cancel, 110),
            fixed(K::Progress, 200),
            Node::leaf(K::Run, Size::Stretch(1), row_h),
            Node::leaf(K::Why, Size::Stretch(1), row_h),
            Node::leaf(K::Outcome, Size::Stretch(1), row_h),
        ]));
        self.form
            .place_page(Node::inset(Node::column(col), layout::MARGIN));
    }

    /// A control's action or a field's change.
    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let f = &self.form;
        let action = match k {
            K::Chd(c) => {
                if self.chd.handle(f, c) {
                    self.refresh();
                }
                return;
            }
            K::Pick(Pick::Recent(i)) => {
                let Some(path) = self.recent_files.borrow().get(i).cloned() else {
                    return;
                };
                BackupAction::OpenImage {
                    path: PathBuf::from(path),
                    rename: true,
                }
            }
            K::Pick(Pick::Device(i)) => {
                let Some(device) = self.devices.borrow().get(i).cloned() else {
                    return;
                };
                BackupAction::SelectDevice { slot: i, device }
            }
            K::Destination => {
                let text = f.text(k);
                BackupAction::SetDestination(
                    (!text.trim().is_empty()).then(|| PathBuf::from(text.trim())),
                )
            }
            K::Name => BackupAction::SetName(f.text(k)),
            K::SplitMib => match f.text(k).trim().parse::<u32>() {
                Ok(n) if (SPLIT_MIB_MIN..=SPLIT_MIB_MAX).contains(&n) => {
                    BackupAction::SetSplitMib(n)
                }
                _ => return,
            },
            K::Format => match f.picked(k).and_then(|i| FORMATS.get(i)) {
                Some(x) => BackupAction::SetFormat(x.0),
                None => return,
            },
            K::Checksum => match f.picked(k).and_then(|i| CHECKSUMS.get(i)) {
                Some(c) => BackupAction::SetChecksum(c.0),
                None => return,
            },
            K::Include(i) => BackupAction::SetPartition(i, f.checked(k)),
            K::Calc(i) => BackupAction::CalcMin(i),
            K::Compact(i) => BackupAction::SetCompact(i, f.checked(k)),
            K::OpenImage => BackupAction::PickImage,
            K::RemoteOpen => BackupAction::RemoteOpen,
            K::Close => BackupAction::CloseSource,
            K::ClearRemote => BackupAction::RemoteClear,
            K::Scan => BackupAction::ScanPartitions,
            K::CancelScan => BackupAction::CancelScan,
            K::All => BackupAction::SelectAll,
            K::None => BackupAction::SelectNone,
            K::Browse => BackupAction::PickDestination,
            K::SectorBySector => BackupAction::SetSectorBySector(f.checked(k)),
            K::Resize => BackupAction::SetResize(f.checked(k)),
            K::KeepSwap => BackupAction::SetKeepSwap(f.checked(k)),
            K::Split => BackupAction::SetSplit(f.checked(k)),
            K::Start => {
                if !self.may_start() {
                    return;
                }
                BackupAction::Start(self.chd_choice())
            }
            K::CancelPending => BackupAction::CancelPendingStart,
            K::Cancel => BackupAction::Cancel,
            _ => return,
        };
        self.apply(action);
    }
}
