//! Physical Disk Export over `controller::disk_write`; GTK's `volume/disk_write.rs` is the reference.
//!
//! Inspect's Export and Restore's Write Image File hand an image over; the window lives while the
//! controller is open. The name above Write is the controller's `write_target_name()`, the name
//! its own question (a typed device name, or a Confirm for an image file) uses.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::{DiskDevice, Notify, Snapshot};
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::disk_write::{
    format_size, ids, labels, DiskWriteAction, DiskWriteController, DiskWriteServices, JobRegistry,
    SizeMode, TableKind, TargetPartition, WrapAlign, WrapForm, WriteOpen, WriteSource,
    TYPE_BYTE_PRESETS,
};
use rusty_backup::controller::{Controller, Effect};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::UI::Controls::NMHDR;
use windows::Win32::UI::Input::KeyboardAndMouse::GetFocus;
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, SendMessageW, ShowWindow, BM_GETCHECK, BM_SETCHECK, BN_CLICKED, CBN_SELCHANGE,
    EN_CHANGE, ES_NUMBER, SW_SHOW, WINDOW_STYLE, WS_EX_CLIENTEDGE, WS_TABSTOP,
};

use crate::combo::Combo;
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;

const ALIGNS: [(WrapAlign, &str); 3] = [
    (WrapAlign::DosTraditional, "DOS legacy (LBA 63)"),
    (WrapAlign::Modern1MB, "Modern (LBA 2048)"),
    (WrapAlign::Custom, "Custom"),
];
const TABLES: [(TableKind, &str); 2] = [(TableKind::Mbr, "MBR"), (TableKind::Gpt, "GPT")];
const MIB: u64 = 1024 * 1024;

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

/// Everything a refresh shows, read while the controller is borrowed.
struct Shown {
    open: bool,
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

pub struct DiskWriteWindow {
    ctrl: RefCell<DiskWriteController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<DiskWriteWindow>,
    devices: RefCell<Vec<DiskDevice>>,
    window: HWND,
    job: HWND,
    job_progress: HWND,
    source_head: HWND,
    source: HWND,
    device_caption: HWND,
    device: Combo,
    pick_image: HWND,
    image: HWND,
    region_caption: HWND,
    region: Combo,
    region_keys: RefCell<Vec<Option<usize>>>,
    region_note: HWND,
    warning: HWND,
    synth: HWND,
    table_caption: HWND,
    table: Combo,
    align_caption: HWND,
    align: Combo,
    custom_align: HWND,
    type_caption: HWND,
    type_pick: Combo,
    type_hex_caption: HWND,
    type_hex: HWND,
    size_caption: HWND,
    size_mode: Combo,
    size_mib: HWND,
    error: HWND,
    result: HWND,
    target: HWND,
    write: HWND,
    cancel_write: HWND,
    close: HWND,
    shape: RefCell<Vec<bool>>,
}

impl DiskWriteWindow {
    /// Open the window on an image another screen hands over.
    pub fn open(
        owner: HWND,
        notify: Arc<dyn Notify>,
        host: EffectHost,
        devices: &[DiskDevice],
        open: WriteOpen,
    ) -> Rc<Self> {
        let window = panel::dialog(owner, "Physical Disk Export", 640, 600);
        let number = |id: &str| {
            let style = WINDOW_STYLE(ES_NUMBER as u32 | WS_TABSTOP.0);
            ui::child(
                window,
                windows::core::w!("EDIT"),
                "",
                style,
                WS_EX_CLIENTEDGE,
                id,
            )
        };
        let progress = ui::child(
            window,
            windows::core::w!("msctls_progress32"),
            "",
            WINDOW_STYLE(0),
            windows::Win32::UI::WindowsAndMessaging::WINDOW_EX_STYLE(0),
            "",
        );
        let aligns: Vec<&str> = ALIGNS.iter().map(|a| a.1).collect();
        let tables: Vec<&str> = TABLES.iter().map(|t| t.1).collect();
        let types: Vec<&str> = TYPE_BYTE_PRESETS.iter().map(|t| t.1).collect();
        let me = Rc::new_cyclic(|me: &Weak<DiskWriteWindow>| {
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
            job: ui::cell_label(window, ""),
            job_progress: progress,
            source_head: ui::label(window, "Source"),
            source: ui::label(window, ""),
            device_caption: ui::cell_label(window, "Target disk:"),
            device: Combo::build(window, ids::TARGET_DEVICE, &[]),
            pick_image: ui::button(window, "Image file...", ids::TARGET_IMAGE),
            image: ui::path_label(window, ""),
            region_caption: ui::cell_label(window, "Write to:"),
            region: Combo::build(window, ids::TARGET_REGION, &[]),
            region_keys: RefCell::default(),
            region_note: ui::label(
                window,
                "No partition table detected on the target, so only a whole-disk write is available.",
            ),
            warning: ui::label(window, ""),
            synth: ui::checkbox(window, "Synthesize partition table", ids::SYNTHESIZE),
            table_caption: ui::cell_label(window, "Table:"),
            table: Combo::build(window, ids::TABLE_KIND, &tables),
            align_caption: ui::cell_label(window, "Alignment:"),
            align: Combo::build(window, ids::ALIGNMENT, &aligns),
            custom_align: number("disk_write.custom_alignment"),
            type_caption: ui::cell_label(window, "Partition type:"),
            type_pick: Combo::build(window, ids::TYPE_BYTE, &types),
            type_hex_caption: ui::cell_label(window, "or 0x"),
            type_hex: ui::edit(window, "disk_write.type_hex"),
            size_caption: ui::cell_label(window, "Partition size:"),
            size_mode: Combo::build(window, ids::PARTITION_SIZE, &[]),
            size_mib: number("disk_write.partition_size_mib"),
            error: ui::label(window, ""),
            result: ui::label(window, ""),
            target: ui::path_label(window, ""),
            write: ui::button(window, "Write to disk...", ids::WRITE),
            cancel_write: ui::button(window, "Cancel write", ids::CANCEL_WRITE),
            close: ui::button(window, "Cancel", ids::CLOSE),
            shape: RefCell::default(),
        }
        });
        let weak: Weak<dyn Handler> = me.me.clone();
        panel::register(window, weak);
        me.apply(DiskWriteAction::Open(open));
        // SAFETY: showing the window we created.
        unsafe {
            let _ = ShowWindow(window, SW_SHOW);
        }
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
            panel::unregister(self.window);
            // SAFETY: the controller closed; the window goes with it.
            let _ = unsafe { DestroyWindow(self.window) };
            return;
        }
        let shown = self.read();
        self.sync.run(|| self.show(shown));
        if *self.shape.borrow() != self.shape_now() {
            self.layout();
        }
    }

    fn read(&self) -> Shown {
        let ctrl = self.ctrl.borrow();
        Shown {
            open: ctrl.is_open(),
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

    fn set_line(&self, h: HWND, text: Option<&str>, tone: Option<Tone>) {
        ui::set_text(h, text.unwrap_or(""));
        ui::set_visible(h, text.is_some());
        crate::theme::set_tone(h, tone);
    }

    fn set_field(&self, h: HWND, text: &str) {
        // SAFETY: a plain focus query: the field being typed in keeps the user's text.
        if unsafe { GetFocus() } != h {
            ui::set_text(h, text);
        }
    }

    fn show(&self, s: Shown) {
        if !s.open {
            return;
        }
        let job = s
            .opening
            .as_ref()
            .or(s.scan.as_ref())
            .or(s.progress.as_ref());
        let (line, pct) = job.map(crate::inspect::job_line).unwrap_or_default();
        ui::set_text(self.job, &line);
        ui::set_visible(self.job, job.is_some());
        crate::inspect::set_progress(self.job_progress, job.is_some(), pct);
        ui::set_visible(self.cancel_write, s.writing);
        let source = s.source.as_ref().map(|src| {
            let table = if src.has_partition_table {
                "has partition table"
            } else {
                "superfloppy (no partition table)"
            };
            format!(
                "Path: {}\r\nSize: {} bytes  -  Filesystem: {}  -  {table}",
                src.path.display(),
                src.size_bytes,
                src.fs_hint
            )
        });
        ui::set_text(self.source, source.as_deref().unwrap_or(""));
        let form = s.opening.is_none() && !s.has_run;
        for h in [
            self.device_caption,
            self.device.h,
            self.pick_image,
            self.synth,
        ] {
            ui::set_visible(h, form);
        }
        self.show_devices(s.target_row);
        let image = s
            .target_image
            .as_ref()
            .map(|p| format!("Target image: {p}"));
        self.set_line(self.image, image.as_deref().filter(|_| form), None);
        let region = form && s.device_size.is_some() && s.scan.is_none();
        ui::set_visible(self.region_caption, region);
        ui::set_visible(self.region.h, region);
        if let Some(size) = s.device_size {
            self.show_region(size, &s.parts, s.part);
        }
        ui::set_visible(self.region_note, region && s.parts.is_empty());
        let warning = s.warning.as_deref().filter(|_| form);
        self.set_line(self.warning, warning, Some(Tone::Error));
        self.show_synth(&s, form);
        let error = s.error.as_deref().filter(|_| form);
        self.set_line(self.error, error, Some(Tone::Error));
        let result = match &s.result {
            Some(Ok(_)) => Some((labels::DONE.to_string(), None)),
            Some(Err(e)) => Some((format!("Error: {e}"), Some(Tone::Error))),
            None => None,
        };
        let (text, tone) = result.unzip();
        self.set_line(self.result, text.as_deref(), tone.flatten());
        self.set_line(self.target, s.target.as_deref(), None);
        ui::set_visible(self.write, !s.has_run);
        ui::set_enabled(self.write, s.can_write);
        ui::set_enabled(self.close, !s.writing);
        ui::set_text(self.close, if s.has_run { "Close" } else { "Cancel" });
    }

    /// The device list; row 0 is "Select a device...".
    fn show_devices(&self, selected: Option<usize>) {
        let mut items = vec!["Select a device...".to_string()];
        items.extend(
            self.devices
                .borrow()
                .iter()
                .map(|d| ui::plain(&d.display_name())),
        );
        self.device.set_items(&items);
        self.device.select(selected.map_or(0, |r| r + 1));
    }

    fn show_region(&self, size: u64, parts: &[TargetPartition], part: Option<usize>) {
        let mut keys: Vec<Option<usize>> = vec![None];
        keys.extend(parts.iter().map(|p| Some(p.index)));
        let mut items = vec![format!(
            "Entire disk ({}) - replaces the partition table",
            format_size(size)
        )];
        items.extend(parts.iter().map(|p| ui::plain(&p.label)));
        self.region.set_items(&items);
        let at = keys.iter().position(|k| *k == part).unwrap_or(0);
        self.region.select(at);
        *self.region_keys.borrow_mut() = keys;
    }

    fn show_synth(&self, s: &Shown, form: bool) {
        let label = if s.can_synth {
            "Synthesize partition table"
        } else {
            "Synthesize partition table (source already has one - disabled)"
        };
        ui::set_text(self.synth, label);
        ui::set_enabled(self.synth, s.can_synth);
        set_check(self.synth, s.form.synthesize);
        let f = &s.form;
        let on = form && s.can_synth && f.synthesize;
        for h in [
            self.table_caption,
            self.table.h,
            self.align_caption,
            self.align.h,
            self.type_caption,
            self.type_pick.h,
            self.type_hex_caption,
            self.type_hex,
            self.size_caption,
            self.size_mode.h,
            self.size_mib,
        ] {
            ui::set_visible(h, on);
        }
        self.table
            .select(usize::from(f.table_kind == TableKind::Gpt));
        let at = ALIGNS.iter().position(|a| a.0 == f.alignment).unwrap_or(0);
        self.align.select(at);
        ui::set_visible(self.custom_align, on && f.alignment == WrapAlign::Custom);
        self.set_field(self.custom_align, &f.custom_alignment_sectors.to_string());
        if let Some(i) = TYPE_BYTE_PRESETS
            .iter()
            .position(|(b, _)| *b == f.type_byte)
        {
            self.type_pick.select(i);
        }
        let typed = u8::from_str_radix(ui::text(self.type_hex).trim(), 16).ok();
        if typed != Some(f.type_byte) {
            self.set_field(self.type_hex, &format!("{:02X}", f.type_byte));
        }
        let names: Vec<String> = s.choices.iter().map(|m| m.label().to_string()).collect();
        self.size_mode.set_items(&names);
        let at = s
            .choices
            .iter()
            .position(|m| *m == f.size_mode)
            .unwrap_or(0);
        self.size_mode.select(at);
        let custom = f.size_mode == SizeMode::Custom;
        let size = s.source.as_ref().map_or(0, |src| src.size_bytes);
        let mib = if custom {
            f.custom_size_mib.clamp(s.bounds.0, s.bounds.1)
        } else {
            (size.div_ceil(MIB)) as u32
        };
        self.set_field(self.size_mib, &mib.to_string());
        ui::set_enabled(self.size_mib, custom);
    }

    fn shape_now(&self) -> Vec<bool> {
        [
            self.job,
            self.device.h,
            self.image,
            self.region.h,
            self.region_note,
            self.warning,
            self.table.h,
            self.custom_align,
            self.error,
            self.result,
            self.target,
            self.write,
            self.cancel_write,
        ]
        .iter()
        .map(|h| ui::shown(*h))
        .collect()
    }

    fn layout(&self) {
        *self.shape.borrow_mut() = self.shape_now();
        let (area, dpi) = panel::client(self.window);
        let row_h = Size::Fixed(layout::ROW);
        let line =
            |h: HWND, n: i32| Node::leaf(h, Size::Stretch(1), Size::Fixed((layout::ROW - 8) * n));
        let fixed = |h: HWND, w: i32| Node::leaf(h, Size::Fixed(w), row_h);
        let stretch = |h: HWND| Node::leaf(h, Size::Stretch(1), row_h);
        let caption = 110;
        let mut col = vec![line(self.source_head, 1), line(self.source, 2)];
        if ui::shown(self.job) {
            let mut row = vec![
                stretch(self.job),
                Node::leaf(
                    self.job_progress,
                    Size::Fixed(180),
                    Size::Fixed(layout::ROW - 6),
                ),
            ];
            if ui::shown(self.cancel_write) {
                row.push(Node::button(self.cancel_write, 110));
            }
            col.push(Node::row(row));
        }
        if ui::shown(self.device.h) {
            col.push(Node::row(vec![
                fixed(self.device_caption, caption),
                stretch(self.device.h),
                Node::button(self.pick_image, 110),
            ]));
        }
        if ui::shown(self.image) {
            col.push(line(self.image, 1));
        }
        if ui::shown(self.region.h) {
            col.push(Node::row(vec![
                fixed(self.region_caption, caption),
                stretch(self.region.h),
            ]));
        }
        for h in [self.region_note, self.warning] {
            if ui::shown(h) {
                col.push(line(h, 2));
            }
        }
        if ui::shown(self.synth) {
            col.push(Node::row(vec![stretch(self.synth)]));
        }
        if ui::shown(self.table.h) {
            let mut align = vec![fixed(self.align_caption, caption), fixed(self.align.h, 190)];
            if ui::shown(self.custom_align) {
                align.push(fixed(self.custom_align, 90));
            }
            align.push(Node::Space(Size::Stretch(1)));
            col.push(Node::row(vec![
                fixed(self.table_caption, caption),
                fixed(self.table.h, 90),
                Node::Space(Size::Stretch(1)),
            ]));
            col.push(Node::row(align));
            col.push(Node::row(vec![
                fixed(self.type_caption, caption),
                fixed(self.type_pick.h, 190),
                fixed(self.type_hex_caption, 40),
                fixed(self.type_hex, 50),
                Node::Space(Size::Stretch(1)),
            ]));
            col.push(Node::row(vec![
                fixed(self.size_caption, caption),
                fixed(self.size_mode.h, 150),
                fixed(self.size_mib, 90),
                Node::Space(Size::Stretch(1)),
            ]));
        }
        for h in [self.error, self.result] {
            if ui::shown(h) {
                col.push(line(h, 2));
            }
        }
        col.push(Node::Space(Size::Stretch(1)));
        if ui::shown(self.target) {
            col.push(line(self.target, 1));
        }
        let mut buttons = vec![Node::Space(Size::Stretch(1))];
        if ui::shown(self.write) {
            buttons.push(Node::button(self.write, 130));
        }
        buttons.push(Node::button(self.close, 90));
        col.push(Node::row(buttons));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in layout::layout(&tree, area, dpi) {
            if ui::shown(h) {
                ui::move_to(h, r);
            }
        }
    }

    fn selected(&self, from: HWND) -> Option<DiskWriteAction> {
        if from == self.device.h {
            let row = self.device.picked()?.checked_sub(1)?;
            let device = self.devices.borrow().get(row).cloned()?;
            return Some(DiskWriteAction::SelectDevice { row, device });
        }
        if from == self.region.h {
            let key = *self.region_keys.borrow().get(self.region.picked()?)?;
            return Some(DiskWriteAction::SetTargetPartition(key));
        }
        if from == self.table.h {
            return Some(DiskWriteAction::SetTableKind(
                TABLES.get(self.table.picked()?)?.0,
            ));
        }
        if from == self.align.h {
            return Some(DiskWriteAction::SetAlignment(
                ALIGNS.get(self.align.picked()?)?.0,
            ));
        }
        if from == self.type_pick.h {
            return Some(DiskWriteAction::SetTypeByte(
                TYPE_BYTE_PRESETS.get(self.type_pick.picked()?)?.0,
            ));
        }
        if from == self.size_mode.h {
            let mode = *self
                .ctrl
                .borrow()
                .size_choices()
                .get(self.size_mode.picked()?)?;
            let custom_mib = ui::text(self.size_mib).trim().parse().unwrap_or(0);
            return Some(DiskWriteAction::SetSize { mode, custom_mib });
        }
        None
    }

    fn changed(&self, from: HWND) -> Option<DiskWriteAction> {
        let text = ui::text(from);
        let text = text.trim();
        if from == self.custom_align {
            return text
                .parse::<u64>()
                .ok()
                .filter(|n| *n > 0)
                .map(DiskWriteAction::SetCustomAlignment);
        }
        if from == self.type_hex {
            return u8::from_str_radix(text, 16)
                .ok()
                .map(DiskWriteAction::SetTypeByte);
        }
        if from == self.size_mib {
            let custom_mib = text.parse::<u32>().ok()?;
            let custom = self.ctrl.borrow().form().size_mode == SizeMode::Custom;
            return custom.then_some(DiskWriteAction::SetSize {
                mode: SizeMode::Custom,
                custom_mib,
            });
        }
        None
    }

    fn clicked(&self, from: HWND) -> Option<DiskWriteAction> {
        if from == self.pick_image {
            Some(DiskWriteAction::PickTargetImage)
        } else if from == self.synth {
            Some(DiskWriteAction::SetSynthesize(checked(from)))
        } else if from == self.write {
            // Write asks the controller again at the click; the question is its own.
            self.ctrl
                .borrow()
                .can_write()
                .then_some(DiskWriteAction::Write)
        } else if from == self.cancel_write {
            Some(DiskWriteAction::Cancel)
        } else if from == self.close {
            Some(DiskWriteAction::Close)
        } else {
            None
        }
    }
}

impl Handler for DiskWriteWindow {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        let action = match code {
            CBN_SELCHANGE => self.selected(from),
            EN_CHANGE => self.changed(from),
            BN_CLICKED => self.clicked(from),
            _ => None,
        };
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

    fn close(&self) {
        self.apply(DiskWriteAction::Close);
    }
}
