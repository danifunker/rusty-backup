//! Physical Disk Export over `controller::disk_write`; the Win32 shell's `disk_write.rs` is the
//! reference.
//!
//! Inspect's Export and Restore's Write Image File hand an image over; the window lives while the
//! controller is open. The name above Write is the controller's `write_target_name()`, the name
//! its own question (a typed device name, or a Confirm for an image file) uses.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::{NSProgressIndicator, NSWindow};
use rusty_backup::controller::app::{DiskDevice, Notify, Snapshot};
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::disk_write::{
    format_size, ids, labels, DiskWriteAction, DiskWriteController, DiskWriteServices, JobRegistry,
    SizeMode, TableKind, TargetPartition, WrapAlign, WrapForm, WriteOpen, WriteSource,
    TYPE_BYTE_PRESETS,
};
use rusty_backup::controller::{Controller, Effect};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::sync::SyncGuard;
use crate::ui::{self, CloseDelegate};

const ALIGNS: [(WrapAlign, &str); 3] = [
    (WrapAlign::DosTraditional, "DOS legacy (LBA 63)"),
    (WrapAlign::Modern1MB, "Modern (LBA 2048)"),
    (WrapAlign::Custom, "Custom"),
];
const TABLES: [(TableKind, &str); 2] = [(TableKind::Mbr, "MBR"), (TableKind::Gpt, "GPT")];
const MIB: u64 = 1024 * 1024;

/// Everything a refresh shows, read while the controller is borrowed.
struct Shown {
    opening: Option<Snapshot>,
    source: Option<WriteSource>,
    has_run: bool,
    progress: Option<Snapshot>,
    writing: bool,
    result: Option<Result<u64, String>>,
    target_row: Option<usize>,
    target_image: Option<String>,
    device_size: Option<u64>,
    scan: Option<Snapshot>,
    parts: Vec<TargetPartition>,
    part: Option<usize>,
    warning: Option<String>,
    can_synth: bool,
    form: WrapForm,
    choices: [SizeMode; 3],
    bounds: (u32, u32),
    error: Option<String>,
    target: Option<String>,
    can_write: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    Job,
    JobProgress,
    SourceHead,
    Source,
    DeviceCaption,
    Device,
    PickImage,
    Image,
    RegionCaption,
    Region,
    RegionNote,
    Warning,
    Synth,
    TableCaption,
    Table,
    AlignCaption,
    Align,
    CustomAlign,
    TypeCaption,
    TypePick,
    TypeHexCaption,
    TypeHex,
    SizeCaption,
    SizeMode,
    SizeMib,
    Error,
    Result,
    Target,
    Write,
    CancelWrite,
    Close,
}

pub struct DiskWriteWindow {
    ctrl: RefCell<DiskWriteController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<DiskWriteWindow>,
    devices: RefCell<Vec<DiskDevice>>,
    window: Retained<NSWindow>,
    _delegate: Retained<CloseDelegate>,
    form: Form<K>,
    region_keys: RefCell<Vec<Option<usize>>>,
}

impl DiskWriteWindow {
    /// Open the window on an image another screen hands over.
    pub fn open(
        mtm: MainThreadMarker,
        notify: Arc<dyn Notify>,
        host: EffectHost,
        devices: &[DiskDevice],
        open: WriteOpen,
    ) -> Rc<DiskWriteWindow> {
        let me = Rc::new_cyclic(|me: &Weak<DiskWriteWindow>| {
            let close = me.clone();
            let (window, delegate) = ui::window(
                mtm,
                "Physical Disk Export",
                640.0,
                600.0,
                Box::new(move || {
                    if let Some(d) = close.upgrade() {
                        d.send(DiskWriteAction::Close);
                    }
                }),
            );
            let resize = me.clone();
            delegate.on_resize(Box::new(move || {
                if let Some(d) = resize.upgrade() {
                    d.layout();
                }
            }));
            let body = window.contentView().expect("the window's content view");
            let fire = me.clone();
            let form = Form::new(
                mtm,
                body,
                Rc::new(move |k| {
                    if let Some(d) = fire.upgrade() {
                        d.fired(k);
                    }
                }),
            );
            let f = &form;
            let strings = |v: Vec<&str>| v.into_iter().map(str::to_string).collect::<Vec<_>>();
            f.label(K::Job, "");
            f.progress(K::JobProgress);
            f.label(K::SourceHead, "Source");
            f.wrap(K::Source, "");
            f.label(K::DeviceCaption, "Target disk:");
            f.popup(K::Device, &[], ids::TARGET_DEVICE);
            f.button(K::PickImage, "Image file...", ids::TARGET_IMAGE);
            f.label(K::Image, "");
            f.label(K::RegionCaption, "Write to:");
            f.popup(K::Region, &[], ids::TARGET_REGION);
            f.wrap(
                K::RegionNote,
                "No partition table detected on the target, so only a whole-disk write is available.",
            );
            f.wrap(K::Warning, "");
            f.check(K::Synth, "Synthesize partition table", ids::SYNTHESIZE);
            f.label(K::TableCaption, "Table:");
            f.popup(
                K::Table,
                &strings(TABLES.iter().map(|t| t.1).collect()),
                ids::TABLE_KIND,
            );
            f.label(K::AlignCaption, "Alignment:");
            f.popup(
                K::Align,
                &strings(ALIGNS.iter().map(|a| a.1).collect()),
                ids::ALIGNMENT,
            );
            f.field(
                K::CustomAlign,
                "disk_write.custom_alignment",
                "Custom alignment (sectors)",
                false,
            );
            f.label(K::TypeCaption, "Partition type:");
            f.popup(
                K::TypePick,
                &strings(TYPE_BYTE_PRESETS.iter().map(|t| t.1).collect()),
                ids::TYPE_BYTE,
            );
            f.label(K::TypeHexCaption, "or 0x");
            f.field(K::TypeHex, "disk_write.type_hex", "Type byte (hex)", false);
            f.label(K::SizeCaption, "Partition size:");
            f.popup(K::SizeMode, &[], ids::PARTITION_SIZE);
            f.field(
                K::SizeMib,
                "disk_write.partition_size_mib",
                "Partition size (MiB)",
                false,
            );
            f.wrap(K::Error, "");
            f.wrap(K::Result, "");
            f.label(K::Target, "");
            f.button(K::Write, "Write to disk...", ids::WRITE);
            f.button(K::CancelWrite, "Cancel write", ids::CANCEL_WRITE);
            f.button(K::Close, "Cancel", ids::CLOSE);
            ui::present(&window);
            DiskWriteWindow {
                ctrl: RefCell::new(DiskWriteController::new(DiskWriteServices {
                    jobs: JobRegistry::new(),
                    notify,
                })),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                devices: RefCell::new(devices.to_vec()),
                window,
                _delegate: delegate,
                form,
                region_keys: RefCell::default(),
            }
        });
        me.apply(DiskWriteAction::Open(open));
        me
    }

    pub fn apply(&self, action: DiskWriteAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// The app's device list: the target follows its own disk, and never another one.
    pub fn set_devices(&self, devices: &[DiskDevice]) {
        self.ctrl.borrow_mut().reconcile_devices(devices);
        *self.devices.borrow_mut() = devices.to_vec();
    }

    pub fn is_open(&self) -> bool {
        self.ctrl.borrow().is_open()
    }

    pub fn is_writing(&self) -> bool {
        self.ctrl.borrow().is_writing()
    }

    /// Send `action` once the current handler returns: the window may close with it.
    fn send(&self, action: DiskWriteAction) {
        let me = self.me.clone();
        crate::app::later(Box::new(move || {
            if let Some(d) = me.upgrade() {
                d.apply(action);
            }
        }));
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(d) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => DiskWriteAction::Picked { purpose, path },
                Reply::Confirmed { purpose, yes } => DiskWriteAction::Confirmed { purpose, yes },
                Reply::Answered { purpose, text } => DiskWriteAction::Answered { purpose, text },
                // The writer offers no choice and has no screen to leave.
                Reply::Chose { .. } | Reply::CloseScreen => return,
            };
            d.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let log = self.ctrl.borrow_mut().take_log();
        crate::app::append_log(log);
        if !self.ctrl.borrow().is_open() {
            self.window.orderOut(None);
            return;
        }
        let shown = self.read();
        self.sync.run(|| self.show(shown));
        self.layout();
    }

    fn read(&self) -> Shown {
        let ctrl = self.ctrl.borrow();
        Shown {
            opening: ctrl.opening(),
            source: ctrl.source().cloned(),
            has_run: ctrl.has_run(),
            progress: ctrl.progress(),
            writing: ctrl.is_writing(),
            result: ctrl.result().cloned(),
            target_row: ctrl.target_row(),
            target_image: ctrl.target_image().map(|p| p.display().to_string()),
            device_size: ctrl.target_device().map(|d| d.size_bytes),
            scan: ctrl.target_scan(),
            parts: ctrl.target_partitions(),
            part: ctrl.target_partition(),
            warning: ctrl.partition_warning(),
            can_synth: ctrl.can_synthesize(),
            form: *ctrl.form(),
            choices: ctrl.size_choices(),
            bounds: ctrl.custom_bounds_mib(),
            error: ctrl.error().map(str::to_string),
            target: ctrl.write_target_name().map(|t| format!("Writes to: {t}")),
            can_write: ctrl.can_write(),
        }
    }

    fn show(&self, s: Shown) {
        let f = &self.form;
        let job = s
            .opening
            .as_ref()
            .or(s.scan.as_ref())
            .or(s.progress.as_ref());
        let (line, pct) = job.map(crate::inspect::job_line).unwrap_or_default();
        f.set_text(K::Job, &line);
        f.show(K::Job, job.is_some());
        let bar = f
            .v(K::JobProgress)
            .downcast::<NSProgressIndicator>()
            .expect("a progress bar");
        ui::set_progress(&bar, job.is_some(), pct);
        f.show(K::CancelWrite, s.writing);
        let source = s.source.as_ref().map(|src| {
            let table = if src.has_partition_table {
                "has partition table"
            } else {
                "superfloppy (no partition table)"
            };
            format!(
                "Path: {}\nSize: {} bytes  -  Filesystem: {}  -  {table}",
                src.path.display(),
                src.size_bytes,
                src.fs_hint
            )
        });
        f.set_text(K::Source, source.as_deref().unwrap_or(""));
        let form = s.opening.is_none() && !s.has_run;
        for k in [K::DeviceCaption, K::Device, K::PickImage, K::Synth] {
            f.show(k, form);
        }
        let mut items = vec!["Select a device...".to_string()];
        items.extend(self.devices.borrow().iter().map(|d| d.display_name()));
        f.set_items(K::Device, &items);
        f.select(K::Device, s.target_row.map_or(0, |r| r + 1));
        let image = s
            .target_image
            .as_ref()
            .map(|p| format!("Target image: {p}"));
        f.line(K::Image, image.as_deref().filter(|_| form), None);
        let region = form && s.device_size.is_some() && s.scan.is_none();
        f.show(K::RegionCaption, region);
        f.show(K::Region, region);
        if let Some(size) = s.device_size {
            let mut keys: Vec<Option<usize>> = vec![None];
            keys.extend(s.parts.iter().map(|p| Some(p.index)));
            let mut items = vec![format!(
                "Entire disk ({}) - replaces the partition table",
                format_size(size)
            )];
            items.extend(s.parts.iter().map(|p| p.label.clone()));
            f.set_items(K::Region, &items);
            let at = keys.iter().position(|k| *k == s.part).unwrap_or(0);
            f.select(K::Region, at);
            *self.region_keys.borrow_mut() = keys;
        }
        f.show(K::RegionNote, region && s.parts.is_empty());
        let warning = s.warning.as_deref().filter(|_| form);
        f.line(K::Warning, warning, Some(Tone::Error));
        self.show_synth(&s, form);
        let error = s.error.as_deref().filter(|_| form);
        f.line(K::Error, error, Some(Tone::Error));
        let result = match &s.result {
            Some(Ok(_)) => Some((labels::DONE.to_string(), None)),
            Some(Err(e)) => Some((format!("Error: {e}"), Some(Tone::Error))),
            None => None,
        };
        let (text, tone) = result.unzip();
        f.line(K::Result, text.as_deref(), tone.flatten());
        f.line(K::Target, s.target.as_deref(), None);
        f.show(K::Write, !s.has_run);
        f.enable(K::Write, s.can_write);
        f.enable(K::Close, !s.writing);
        f.set_title(K::Close, if s.has_run { "Close" } else { "Cancel" });
    }

    fn show_synth(&self, s: &Shown, form: bool) {
        let f = &self.form;
        let label = if s.can_synth {
            "Synthesize partition table"
        } else {
            "Synthesize partition table (source already has one - disabled)"
        };
        f.set_title(K::Synth, label);
        f.enable(K::Synth, s.can_synth);
        f.set_check(K::Synth, s.form.synthesize);
        let w = &s.form;
        let on = form && s.can_synth && w.synthesize;
        for k in [
            K::TableCaption,
            K::Table,
            K::AlignCaption,
            K::Align,
            K::TypeCaption,
            K::TypePick,
            K::TypeHexCaption,
            K::TypeHex,
            K::SizeCaption,
            K::SizeMode,
            K::SizeMib,
        ] {
            f.show(k, on);
        }
        f.select(K::Table, usize::from(w.table_kind == TableKind::Gpt));
        let at = ALIGNS.iter().position(|a| a.0 == w.alignment).unwrap_or(0);
        f.select(K::Align, at);
        f.show(K::CustomAlign, on && w.alignment == WrapAlign::Custom);
        f.set_text(K::CustomAlign, &w.custom_alignment_sectors.to_string());
        if let Some(i) = TYPE_BYTE_PRESETS
            .iter()
            .position(|(b, _)| *b == w.type_byte)
        {
            f.select(K::TypePick, i);
        }
        let typed = u8::from_str_radix(f.text(K::TypeHex).trim(), 16).ok();
        if typed != Some(w.type_byte) {
            f.set_text(K::TypeHex, &format!("{:02X}", w.type_byte));
        }
        let names: Vec<String> = s.choices.iter().map(|m| m.label().to_string()).collect();
        f.set_items(K::SizeMode, &names);
        let at = s
            .choices
            .iter()
            .position(|m| *m == w.size_mode)
            .unwrap_or(0);
        f.select(K::SizeMode, at);
        let custom = w.size_mode == SizeMode::Custom;
        let size = s.source.as_ref().map_or(0, |src| src.size_bytes);
        let mib = if custom {
            w.custom_size_mib.clamp(s.bounds.0, s.bounds.1)
        } else {
            size.div_ceil(MIB) as u32
        };
        f.set_text(K::SizeMib, &mib.to_string());
        f.enable(K::SizeMib, custom);
    }

    fn layout(&self) {
        let Some(body) = self.window.contentView() else {
            return;
        };
        let f = &self.form;
        let row_h = Size::Fixed(layout::ROW);
        let line =
            |k: K, n: i32| Node::leaf(k, Size::Stretch(1), Size::Fixed((layout::ROW - 6) * n + 4));
        let fixed = |k: K, w: i32| Node::leaf(k, Size::Fixed(w), row_h);
        let pop = |k: K, w: Size| Node::leaf(k, w, Size::Fixed(layout::ROW + 2));
        let stretch = |k: K| Node::leaf(k, Size::Stretch(1), row_h);
        let caption = 110;
        let mut col = vec![line(K::SourceHead, 1), line(K::Source, 2)];
        if f.shown(K::Job) {
            let mut row = vec![
                stretch(K::Job),
                Node::leaf(
                    K::JobProgress,
                    Size::Fixed(180),
                    Size::Fixed(layout::ROW - 4),
                ),
            ];
            if f.shown(K::CancelWrite) {
                row.push(Node::button(K::CancelWrite, 115));
            }
            col.push(Node::row(row));
        }
        col.push(Node::row(vec![
            fixed(K::DeviceCaption, caption),
            pop(K::Device, Size::Stretch(1)),
            Node::button(K::PickImage, 115),
        ]));
        col.push(line(K::Image, 1));
        col.push(Node::row(vec![
            fixed(K::RegionCaption, caption),
            pop(K::Region, Size::Stretch(1)),
        ]));
        col.push(line(K::RegionNote, 2));
        col.push(line(K::Warning, 2));
        col.push(Node::row(vec![stretch(K::Synth)]));
        col.push(Node::row(vec![
            fixed(K::TableCaption, caption),
            pop(K::Table, Size::Fixed(90)),
            Node::Space(Size::Stretch(1)),
        ]));
        col.push(Node::row(vec![
            fixed(K::AlignCaption, caption),
            pop(K::Align, Size::Fixed(190)),
            fixed(K::CustomAlign, 90),
            Node::Space(Size::Stretch(1)),
        ]));
        col.push(Node::row(vec![
            fixed(K::TypeCaption, caption),
            pop(K::TypePick, Size::Fixed(190)),
            fixed(K::TypeHexCaption, 40),
            fixed(K::TypeHex, 50),
            Node::Space(Size::Stretch(1)),
        ]));
        col.push(Node::row(vec![
            fixed(K::SizeCaption, caption),
            pop(K::SizeMode, Size::Fixed(160)),
            fixed(K::SizeMib, 90),
            Node::Space(Size::Stretch(1)),
        ]));
        col.push(line(K::Error, 2));
        col.push(line(K::Result, 2));
        col.push(Node::Space(Size::Stretch(1)));
        col.push(line(K::Target, 1));
        col.push(Node::row(vec![
            Node::Space(Size::Stretch(1)),
            Node::button(K::Write, 135),
            Node::button(K::Close, 90),
        ]));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        f.place_shown(tree, ui::area(&body));
    }

    fn action(&self, k: K) -> Option<DiskWriteAction> {
        let f = &self.form;
        let text = || f.text(k).trim().to_string();
        Some(match k {
            K::Device => {
                let row = f.picked(k)?.checked_sub(1)?;
                let device = self.devices.borrow().get(row).cloned()?;
                DiskWriteAction::SelectDevice { row, device }
            }
            K::Region => {
                let key = *self.region_keys.borrow().get(f.picked(k)?)?;
                DiskWriteAction::SetTargetPartition(key)
            }
            K::Table => DiskWriteAction::SetTableKind(TABLES.get(f.picked(k)?)?.0),
            K::Align => DiskWriteAction::SetAlignment(ALIGNS.get(f.picked(k)?)?.0),
            K::TypePick => DiskWriteAction::SetTypeByte(TYPE_BYTE_PRESETS.get(f.picked(k)?)?.0),
            K::SizeMode => {
                let mode = *self.ctrl.borrow().size_choices().get(f.picked(k)?)?;
                let custom_mib = f.text(K::SizeMib).trim().parse().unwrap_or(0);
                DiskWriteAction::SetSize { mode, custom_mib }
            }
            K::CustomAlign => {
                let n = text().parse::<u64>().ok().filter(|n| *n > 0)?;
                DiskWriteAction::SetCustomAlignment(n)
            }
            K::TypeHex => DiskWriteAction::SetTypeByte(u8::from_str_radix(&text(), 16).ok()?),
            K::SizeMib => {
                let custom_mib = text().parse::<u32>().ok()?;
                if self.ctrl.borrow().form().size_mode != SizeMode::Custom {
                    return None;
                }
                DiskWriteAction::SetSize {
                    mode: SizeMode::Custom,
                    custom_mib,
                }
            }
            K::PickImage => DiskWriteAction::PickTargetImage,
            K::Synth => DiskWriteAction::SetSynthesize(f.checked(k)),
            // Write asks the controller again at the click; the question is its own.
            K::Write if self.ctrl.borrow().can_write() => DiskWriteAction::Write,
            K::CancelWrite => DiskWriteAction::Cancel,
            K::Close => DiskWriteAction::Close,
            _ => return None,
        })
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        if let Some(a) = self.action(k) {
            self.send(a);
        }
    }
}

impl Drop for DiskWriteWindow {
    fn drop(&mut self) {
        self.window.setDelegate(None);
        self.window.orderOut(None);
    }
}
