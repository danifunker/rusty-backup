//! Export Disk Image over the Inspect controller's export settings; egui's `show_export_popup` is
//! the reference. The window is open while `InspectController::export_open()` says so.
//! Physical Disk Export hands the image to the disk writer window (`disk_write.rs`).

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use rusty_backup::controller::app::chd::{ChdOptionsControl, ChdProfile};
use rusty_backup::controller::inspect::{
    after_caption, ids, per_partition_refusal, ExportFormat, InspectAction, InspectController,
    LayoutSegment, PartitionExportConfig, EXPORT_FORMATS, EXPORT_INTRO, HFV_WHOLE_REFUSAL,
};
use rusty_backup::controller::restore::SizeMode;
use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::UI::Controls::NMHDR;
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, SendMessageW, ShowWindow, BM_GETCHECK, BM_SETCHECK, BN_CLICKED,
    BS_AUTORADIOBUTTON, CBN_SELCHANGE, CBS_DROPDOWNLIST, CB_ADDSTRING, CB_GETCURSEL,
    CB_RESETCONTENT, CB_SETCURSEL, EN_CHANGE, ES_NUMBER, SW_SHOW, WINDOW_EX_STYLE, WINDOW_STYLE,
    WS_EX_CLIENTEDGE, WS_GROUP, WS_TABSTOP, WS_VSCROLL,
};

use crate::chd::{ChdControl, ChdIds};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::partition_bar;
use crate::sync::SyncGuard;
use crate::ui;

/// What the window shows, read while the controller is borrowed.
pub struct Shown {
    formats: Vec<(ExportFormat, bool)>,
    whole: bool,
    whole_only: bool,
    per_only: bool,
    format: ExportFormat,
    configs: Vec<PartitionExportConfig>,
    current: Vec<LayoutSegment>,
    after: Vec<LayoutSegment>,
    after_caption: String,
    notes: Vec<String>,
    can_physical: bool,
}

impl Shown {
    /// `None` while the window is closed.
    pub fn read(c: &mut InspectController) -> Option<Shown> {
        if !c.export_open() {
            return None;
        }
        let formats = c
            .export_formats()
            .into_iter()
            .map(|(f, ok)| (f.format, ok))
            .collect();
        let layout = c.export_layout();
        let after_caption = after_caption(&layout);
        let notes = c.export_notes().lines();
        let can_physical = c.can_export_physical();
        let s = c.export_settings_mut();
        Some(Shown {
            formats,
            whole: s.whole_disk,
            whole_only: s.whole_disk_only(),
            per_only: s.per_partition_only(),
            format: s.format,
            configs: s.configs.clone(),
            current: layout.current,
            after: layout.after,
            after_caption,
            notes,
            can_physical,
        })
    }
}

/// One partition's size row: its number, type, mode and Custom size.
struct SizeRow {
    index: usize,
    number: HWND,
    kind: HWND,
    mode: HWND,
    choices: RefCell<Vec<SizeMode>>,
    mib: HWND,
}

pub struct ExportWindow {
    window: HWND,
    send: Rc<dyn Fn(InspectAction)>,
    /// Physical Disk Export: the screen hands the image to the disk writer.
    physical_send: Rc<dyn Fn()>,
    sync: SyncGuard,
    me: Weak<ExportWindow>,
    intro: HWND,
    current_caption: HWND,
    current: HWND,
    after_caption: HWND,
    after: HWND,
    whole: HWND,
    per: HWND,
    format_caption: HWND,
    formats: Vec<(ExportFormat, HWND)>,
    chd: ChdControl,
    sizes_title: HWND,
    rows: RefCell<Vec<SizeRow>>,
    /// The size rows and CHD lists last laid out; a change lays the window out again.
    shape: std::cell::Cell<(usize, usize)>,
    notes: HWND,
    start: HWND,
    physical: HWND,
    cancel: HWND,
}

fn radio(parent: HWND, text: &str, first: bool, id: &str) -> HWND {
    let group = if first { WS_GROUP.0 } else { 0 };
    let style = WINDOW_STYLE((BS_AUTORADIOBUTTON as u32) | WS_TABSTOP.0 | group);
    ui::child(parent, w!("BUTTON"), text, style, WINDOW_EX_STYLE(0), id)
}

fn set_check(h: HWND, on: bool) {
    // SAFETY: plain button messages.
    unsafe {
        if (SendMessageW(h, BM_GETCHECK, None, None).0 == 1) != on {
            SendMessageW(h, BM_SETCHECK, Some(WPARAM(on as usize)), None);
        }
    }
}

impl ExportWindow {
    /// The window; `send` delivers its edits to the Inspect controller.
    pub fn open(owner: HWND, send: Rc<dyn Fn(InspectAction)>, physical: Rc<dyn Fn()>) -> Rc<Self> {
        let window = panel::dialog(owner, "Export Disk Image", 640, 700);
        let intro = ui::label(window, EXPORT_INTRO);
        let current_caption = ui::label(window, "Current:");
        let current = partition_bar::build(window, Box::new(|_| {}));
        let after_caption = ui::label(window, "");
        let after = partition_bar::build(window, Box::new(|_| {}));
        let whole = radio(window, "Whole Disk (single file)", true, ids::EXPORT_WHOLE);
        let per = radio(
            window,
            "Per Partition (one file per partition)",
            false,
            ids::EXPORT_PER_PARTITION,
        );
        let format_caption = ui::label(window, "Format:");
        let formats = EXPORT_FORMATS
            .iter()
            .enumerate()
            .map(|(i, c)| {
                (
                    c.format,
                    radio(
                        window,
                        c.label,
                        i == 0,
                        if i == 0 { ids::EXPORT_FORMAT } else { "" },
                    ),
                )
            })
            .collect();
        let chd = ChdControl::build(
            window,
            ChdIds {
                mode: ids::EXPORT_CHD_MODE,
                hunk: ids::EXPORT_CHD_HUNK,
                codec: ids::EXPORT_CHD_CODEC,
            },
            ChdOptionsControl::new(ChdProfile::Hd),
            ChdProfile::Hd,
        );
        let sizes_title = ui::label(window, "Partition Sizes:");
        let notes = ui::label(window, "");
        let start = ui::button(window, "Export...", ids::EXPORT_START);
        let physical_button = ui::button(window, "Physical Disk Export...", ids::EXPORT_PHYSICAL);
        let cancel = ui::button(window, "Cancel", ids::EXPORT_CLOSE);
        partition_bar::set_options(current, 1.0, false);
        let me = Rc::new_cyclic(|me: &Weak<ExportWindow>| ExportWindow {
            window,
            send,
            physical_send: physical,
            sync: SyncGuard::default(),
            me: me.clone(),
            intro,
            current_caption,
            current,
            after_caption,
            after,
            whole,
            per,
            format_caption,
            formats,
            chd,
            sizes_title,
            rows: RefCell::default(),
            shape: std::cell::Cell::new((usize::MAX, 0)),
            notes,
            start,
            physical: physical_button,
            cancel,
        });
        let weak: Weak<dyn Handler> = me.me.clone();
        panel::register(window, weak);
        // SAFETY: showing the window we created.
        unsafe {
            let _ = ShowWindow(window, SW_SHOW);
        }
        me
    }

    pub fn show(&self, s: Shown) {
        self.sync.run(|| {
            set_check(self.whole, s.whole);
            set_check(self.per, !s.whole);
            ui::set_enabled(self.whole, !s.per_only);
            ui::set_enabled(self.per, !s.whole_only);
            let refusal = if s.per_only {
                Some(HFV_WHOLE_REFUSAL)
            } else if s.whole_only {
                per_partition_refusal(s.format)
            } else {
                None
            };
            for (format, h) in &self.formats {
                let allowed = s.formats.iter().any(|(f, ok)| f == format && *ok);
                ui::set_enabled(*h, allowed);
                set_check(*h, *format == s.format);
            }
            self.chd.set_visible(s.format == ExportFormat::Chd);
            let total = |l: &[LayoutSegment]| l.iter().map(|x| x.size_bytes).sum::<u64>();
            let (now, then) = (total(&s.current), total(&s.after));
            let most = now.max(then).max(1) as f32;
            partition_bar::set_options(self.current, now as f32 / most, false);
            partition_bar::set_options(self.after, then as f32 / most, true);
            partition_bar::show(self.current, s.current, None);
            partition_bar::show(self.after, s.after, None);
            ui::set_text(self.after_caption, &s.after_caption);
            ui::set_enabled(self.physical, s.can_physical);
            let reshaped = self.show_sizes(&s.configs);
            let mut notes = s.notes;
            notes.extend(refusal.map(str::to_string));
            ui::set_text(self.notes, &notes.join("\r\n"));
            let shape = (
                self.rows.borrow().len(),
                self.chd.combos().iter().filter(|h| ui::shown(**h)).count(),
            );
            if reshaped || self.shape.replace(shape) != shape {
                self.layout();
            }
        });
    }

    /// The rows are rebuilt only when the partitions change; true when they were.
    fn show_sizes(&self, configs: &[PartitionExportConfig]) -> bool {
        ui::set_visible(self.sizes_title, !configs.is_empty());
        let same = {
            let rows = self.rows.borrow();
            rows.len() == configs.len() && rows.iter().zip(configs).all(|(r, c)| r.index == c.index)
        };
        if !same {
            for r in self.rows.borrow_mut().drain(..) {
                for h in [r.number, r.kind, r.mode, r.mib] {
                    // SAFETY: our own controls, rebuilt for the new partitions.
                    let _ = unsafe { DestroyWindow(h) };
                }
            }
            let w = self.window;
            let rows = configs
                .iter()
                .map(|c| {
                    let style =
                        WINDOW_STYLE((CBS_DROPDOWNLIST as u32) | WS_TABSTOP.0 | WS_VSCROLL.0);
                    SizeRow {
                        index: c.index,
                        number: ui::label(w, &c.index.to_string()),
                        kind: ui::label(w, &c.type_name),
                        mode: ui::child(
                            w,
                            w!("COMBOBOX"),
                            "",
                            style,
                            WINDOW_EX_STYLE(0),
                            ids::EXPORT_SIZE_MODE,
                        ),
                        choices: RefCell::default(),
                        mib: ui::child(
                            w,
                            w!("EDIT"),
                            "",
                            WINDOW_STYLE((ES_NUMBER as u32) | WS_TABSTOP.0),
                            WS_EX_CLIENTEDGE,
                            ids::EXPORT_SIZE_MIB,
                        ),
                    }
                })
                .collect();
            *self.rows.borrow_mut() = rows;
        }
        for (row, cfg) in self.rows.borrow().iter().zip(configs) {
            let choices = cfg.mode_choices();
            if *row.choices.borrow() != choices {
                // SAFETY: plain combo box messages; the control copies each string.
                unsafe {
                    SendMessageW(row.mode, CB_RESETCONTENT, None, None);
                    for m in &choices {
                        let t = ui::wide(m.label());
                        SendMessageW(
                            row.mode,
                            CB_ADDSTRING,
                            None,
                            Some(LPARAM(t.as_ptr() as isize)),
                        );
                    }
                }
                *row.choices.borrow_mut() = choices.clone();
            }
            let at = choices.iter().position(|m| *m == cfg.choice).unwrap_or(0);
            // SAFETY: plain combo box messages.
            unsafe {
                if SendMessageW(row.mode, CB_GETCURSEL, None, None).0 as usize != at {
                    SendMessageW(row.mode, CB_SETCURSEL, Some(WPARAM(at)), None);
                }
            }
            let custom = cfg.choice == SizeMode::Custom;
            ui::set_enabled(row.mib, custom);
            let shown = if custom {
                cfg.custom_mib_effective() as u64
            } else {
                cfg.effective_size() / (1024 * 1024)
            };
            if ui::text(row.mib).trim().parse::<u64>().ok() != Some(shown) {
                ui::set_text(row.mib, &shown.to_string());
            }
        }
        !same
    }

    fn layout(&self) {
        let (area, dpi) = panel::client(self.window);
        let line =
            |h: HWND, n: i32| Node::leaf(h, Size::Stretch(1), Size::Fixed((layout::ROW - 8) * n));
        let mut rows = vec![
            line(self.intro, 3),
            line(self.current_caption, 1),
            Node::leaf(
                self.current,
                Size::Stretch(1),
                Size::Fixed(partition_bar::BAR_ONLY),
            ),
            line(self.after_caption, 1),
            Node::leaf(
                self.after,
                Size::Stretch(1),
                Size::Fixed(partition_bar::HEIGHT),
            ),
            Node::leaf(self.whole, Size::Stretch(1), Size::Fixed(layout::ROW)),
            Node::leaf(self.per, Size::Stretch(1), Size::Fixed(layout::ROW)),
            line(self.format_caption, 1),
        ];
        for pair in self.formats.chunks(3) {
            rows.push(Node::row(
                pair.iter()
                    .map(|(_, h)| Node::leaf(*h, Size::Stretch(1), Size::Fixed(layout::ROW)))
                    .collect(),
            ));
        }
        rows.extend(self.chd.rows());
        if ui::shown(self.sizes_title) {
            rows.push(line(self.sizes_title, 1));
            for r in self.rows.borrow().iter() {
                rows.push(Node::row(vec![
                    Node::leaf(r.number, Size::Fixed(30), Size::Fixed(layout::ROW)),
                    Node::leaf(r.kind, Size::Fixed(170), Size::Fixed(layout::ROW)),
                    Node::leaf(r.mode, Size::Fixed(150), Size::Fixed(layout::ROW)),
                    Node::leaf(r.mib, Size::Fixed(90), Size::Fixed(layout::ROW)),
                ]));
            }
        }
        rows.push(Node::leaf(self.notes, Size::Stretch(1), Size::Stretch(1)));
        rows.push(Node::row(vec![
            Node::button(self.physical, 170),
            Node::Space(Size::Stretch(1)),
            Node::button(self.cancel, 90),
            Node::button(self.start, 90),
        ]));
        let tree = Node::inset(Node::column(rows), layout::MARGIN);
        let combos: Vec<HWND> = self
            .chd
            .combos()
            .into_iter()
            .chain(self.rows.borrow().iter().map(|r| r.mode))
            .collect();
        for (h, mut r) in layout::layout(&tree, area, dpi) {
            if combos.contains(&h) {
                // A drop-down list's window height includes its open list.
                r.h *= 8;
            }
            ui::move_to(h, r);
        }
    }
}

impl Drop for ExportWindow {
    fn drop(&mut self) {
        panel::unregister(self.window);
        // SAFETY: the controller closed Export; the window goes with it.
        let _ = unsafe { DestroyWindow(self.window) };
    }
}

impl Handler for ExportWindow {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        if self.chd.command(from, code) {
            ui::set_enabled(self.start, self.chd.is_valid());
            self.chd.set_visible(true);
            self.layout();
            return;
        }
        let action = if code == BN_CLICKED && from == self.whole {
            InspectAction::SetExportWholeDisk(true)
        } else if code == BN_CLICKED && from == self.per {
            InspectAction::SetExportWholeDisk(false)
        } else if let (BN_CLICKED, Some((format, _))) =
            (code, self.formats.iter().find(|(_, h)| *h == from))
        {
            InspectAction::SetExportFormat(*format)
        } else if code == BN_CLICKED && from == self.start {
            if !self.chd.is_valid() {
                return;
            }
            InspectAction::StartExport(self.chd.choice().0)
        } else if code == BN_CLICKED && from == self.physical {
            (self.physical_send)();
            return;
        } else if code == BN_CLICKED && from == self.cancel {
            InspectAction::CloseExport
        } else {
            let rows = self.rows.borrow();
            let Some(r) = rows.iter().find(|r| r.mode == from || r.mib == from) else {
                return;
            };
            let custom_mib = ui::text(r.mib).trim().parse::<u32>().unwrap_or(0);
            let mode = if code == CBN_SELCHANGE && from == r.mode {
                // SAFETY: a plain combo box query.
                let i = unsafe { SendMessageW(r.mode, CB_GETCURSEL, None, None).0 };
                match usize::try_from(i)
                    .ok()
                    .and_then(|i| r.choices.borrow().get(i).copied())
                {
                    Some(m) => m,
                    None => return,
                }
            } else if code == EN_CHANGE && from == r.mib {
                SizeMode::Custom
            } else {
                return;
            };
            InspectAction::SetExportSize {
                index: r.index,
                mode,
                custom_mib,
            }
        };
        (self.send)(action);
    }

    fn notify(&self, _hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {
        (self.send)(InspectAction::CloseExport);
    }
}
