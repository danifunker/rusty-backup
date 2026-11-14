//! Export Disk Image over the Inspect controller's export settings; the Win32 shell's `export.rs`
//! is the reference. The window is open while `InspectController::export_open()` says so.
//! Physical Disk Export hands the image to the disk writer window (`disk_write.rs`).

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::NSWindow;
use rusty_backup::controller::app::chd::{ChdOptionsControl, ChdProfile};
use rusty_backup::controller::inspect::{
    after_caption, ids, per_partition_refusal, ExportFormat, InspectAction, InspectController,
    LayoutSegment, PartitionExportConfig, EXPORT_FORMATS, EXPORT_INTRO, HFV_WHOLE_REFUSAL,
};
use rusty_backup::controller::restore::SizeMode;

use crate::chd::{ChdControl, ChdIds, ChdKey};
use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::partition_bar::{self, Bar};
use crate::sync::SyncGuard;
use crate::ui::{self, CloseDelegate};

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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    Intro,
    CurrentCaption,
    Current,
    AfterCaption,
    After,
    Whole,
    Per,
    FormatCaption,
    Format(usize),
    Chd(ChdKey),
    SizesTitle,
    Number(usize),
    Kind(usize),
    Mode(usize),
    Mib(usize),
    Notes,
    Start,
    Physical,
    Cancel,
}

pub struct ExportWindow {
    window: Retained<NSWindow>,
    _delegate: Retained<CloseDelegate>,
    form: Form<K>,
    current: Retained<Bar>,
    after: Retained<Bar>,
    chd: ChdControl<K>,
    send: Rc<dyn Fn(InspectAction)>,
    /// Physical Disk Export: the screen hands the image to the disk writer.
    physical: Rc<dyn Fn()>,
    sync: SyncGuard,
    /// Each size row's partition and its mode choices, as built.
    rows: RefCell<Vec<(usize, Vec<SizeMode>)>>,
}

impl ExportWindow {
    /// The window; `send` delivers its edits to the Inspect controller.
    pub fn open(
        mtm: MainThreadMarker,
        send: Rc<dyn Fn(InspectAction)>,
        physical: Rc<dyn Fn()>,
    ) -> Rc<ExportWindow> {
        Rc::new_cyclic(|me: &Weak<ExportWindow>| {
            let close = send.clone();
            let (window, delegate) = ui::window(
                mtm,
                "Export Disk Image",
                640.0,
                700.0,
                Box::new(move || {
                    let close = close.clone();
                    crate::app::later(Box::new(move || close(InspectAction::CloseExport)));
                }),
            );
            let resize = me.clone();
            delegate.on_resize(Box::new(move || {
                if let Some(w) = resize.upgrade() {
                    w.layout();
                }
            }));
            let body = window.contentView().expect("the window's content view");
            let fire = me.clone();
            let form = Form::new(
                mtm,
                body,
                Rc::new(move |k| {
                    if let Some(w) = fire.upgrade() {
                        w.fired(k);
                    }
                }),
            );
            let f = &form;
            f.wrap(K::Intro, EXPORT_INTRO);
            f.label(K::CurrentCaption, "Current:");
            let current = Bar::new(mtm, "export.current_bar");
            current.set_options(1.0, false);
            f.add(K::Current, &current);
            f.label(K::AfterCaption, "");
            let after = Bar::new(mtm, "export.after_bar");
            f.add(K::After, &after);
            f.radio(K::Whole, 0, "Whole Disk (single file)", ids::EXPORT_WHOLE);
            f.radio(
                K::Per,
                0,
                "Per Partition (one file per partition)",
                ids::EXPORT_PER_PARTITION,
            );
            f.label(K::FormatCaption, "Format:");
            for (i, c) in EXPORT_FORMATS.iter().enumerate() {
                let id = if i == 0 { ids::EXPORT_FORMAT } else { "" };
                f.radio(K::Format(i), 1, c.label, id);
            }
            let chd = ChdControl::build(
                f,
                K::Chd,
                ChdIds {
                    mode: ids::EXPORT_CHD_MODE,
                    hunk: ids::EXPORT_CHD_HUNK,
                    codec: ids::EXPORT_CHD_CODEC,
                },
                ChdOptionsControl::new(ChdProfile::Hd),
                ChdProfile::Hd,
            );
            f.label(K::SizesTitle, "Partition Sizes:");
            f.wrap(K::Notes, "");
            f.button(K::Start, "Export...", ids::EXPORT_START);
            f.button(K::Physical, "Physical Disk Export...", ids::EXPORT_PHYSICAL);
            f.button(K::Cancel, "Cancel", ids::EXPORT_CLOSE);
            ui::present(&window);
            ExportWindow {
                window,
                _delegate: delegate,
                form,
                current,
                after,
                chd,
                send,
                physical,
                sync: SyncGuard::default(),
                rows: RefCell::default(),
            }
        })
    }

    pub fn show(&self, s: Shown) {
        let f = &self.form;
        self.sync.run(|| {
            f.set_check(K::Whole, s.whole);
            f.set_check(K::Per, !s.whole);
            f.enable(K::Whole, !s.per_only);
            f.enable(K::Per, !s.whole_only);
            let refusal = if s.per_only {
                Some(HFV_WHOLE_REFUSAL)
            } else if s.whole_only {
                per_partition_refusal(s.format)
            } else {
                None
            };
            for (i, c) in EXPORT_FORMATS.iter().enumerate() {
                let allowed = s.formats.iter().any(|(x, ok)| *x == c.format && *ok);
                f.enable(K::Format(i), allowed);
                f.set_check(K::Format(i), c.format == s.format);
            }
            self.chd.set_visible(f, s.format == ExportFormat::Chd);
            let total = |l: &[LayoutSegment]| l.iter().map(|x| x.size_bytes).sum::<u64>();
            let (now, then) = (total(&s.current), total(&s.after));
            let most = now.max(then).max(1) as f32;
            self.current.set_options(now as f32 / most, false);
            self.after.set_options(then as f32 / most, true);
            self.current.show(s.current, None);
            self.after.show(s.after, None);
            f.set_text(K::AfterCaption, &s.after_caption);
            f.enable(K::Physical, s.can_physical);
            self.show_sizes(&s.configs);
            let mut notes = s.notes;
            notes.extend(refusal.map(str::to_string));
            f.set_text(K::Notes, &notes.join("\n"));
        });
        self.layout();
    }

    /// The rows are built again only when the partitions or their choices change.
    fn show_sizes(&self, configs: &[PartitionExportConfig]) {
        let f = &self.form;
        f.show(K::SizesTitle, !configs.is_empty());
        let want: Vec<(usize, Vec<SizeMode>)> = configs
            .iter()
            .map(|c| (c.index, c.mode_choices()))
            .collect();
        if *self.rows.borrow() != want {
            f.remove(|k| matches!(k, K::Number(_) | K::Kind(_) | K::Mode(_) | K::Mib(_)));
            for (c, (_, choices)) in configs.iter().zip(&want) {
                let i = c.index;
                f.label(K::Number(i), &i.to_string());
                f.label(K::Kind(i), &c.type_name);
                let names: Vec<String> = choices.iter().map(|m| m.label().to_string()).collect();
                f.popup(
                    K::Mode(i),
                    &names,
                    &format!("{}.{i}", ids::EXPORT_SIZE_MODE),
                );
                f.field(
                    K::Mib(i),
                    &format!("{}.{i}", ids::EXPORT_SIZE_MIB),
                    "Size (MiB)",
                    false,
                );
            }
            *self.rows.borrow_mut() = want;
        }
        for cfg in configs {
            let i = cfg.index;
            let choices = cfg.mode_choices();
            let at = choices.iter().position(|m| *m == cfg.choice).unwrap_or(0);
            f.select(K::Mode(i), at);
            let custom = cfg.choice == SizeMode::Custom;
            f.enable(K::Mib(i), custom);
            let shown = if custom {
                cfg.custom_mib_effective() as u64
            } else {
                cfg.effective_size() / (1024 * 1024)
            };
            if f.text(K::Mib(i)).trim().parse::<u64>().ok() != Some(shown) {
                f.set_text(K::Mib(i), &shown.to_string());
            }
        }
    }

    fn layout(&self) {
        let Some(body) = self.window.contentView() else {
            return;
        };
        let line =
            |k: K, n: i32| Node::leaf(k, Size::Stretch(1), Size::Fixed((layout::ROW - 6) * n + 4));
        let row_h = Size::Fixed(layout::ROW);
        let mut rows = vec![
            line(K::Intro, 3),
            line(K::CurrentCaption, 1),
            Node::leaf(
                K::Current,
                Size::Stretch(1),
                Size::Fixed(partition_bar::BAR_ONLY),
            ),
            line(K::AfterCaption, 1),
            Node::leaf(
                K::After,
                Size::Stretch(1),
                Size::Fixed(partition_bar::HEIGHT),
            ),
            Node::leaf(K::Whole, Size::Stretch(1), row_h),
            Node::leaf(K::Per, Size::Stretch(1), row_h),
            line(K::FormatCaption, 1),
        ];
        let formats: Vec<usize> = (0..EXPORT_FORMATS.len()).collect();
        for three in formats.chunks(3) {
            rows.push(Node::row(
                three
                    .iter()
                    .map(|i| Node::leaf(K::Format(*i), Size::Stretch(1), row_h))
                    .collect(),
            ));
        }
        rows.extend(self.chd.rows());
        rows.push(line(K::SizesTitle, 1));
        for (i, _) in self.rows.borrow().iter() {
            let i = *i;
            rows.push(Node::row(vec![
                Node::leaf(K::Number(i), Size::Fixed(30), row_h),
                Node::leaf(K::Kind(i), Size::Fixed(170), row_h),
                Node::leaf(K::Mode(i), Size::Fixed(160), Size::Fixed(layout::ROW + 2)),
                Node::leaf(K::Mib(i), Size::Fixed(90), row_h),
            ]));
        }
        rows.push(Node::leaf(K::Notes, Size::Stretch(1), Size::Stretch(1)));
        rows.push(Node::row(vec![
            Node::button(K::Physical, 180),
            Node::Space(Size::Stretch(1)),
            Node::button(K::Cancel, 90),
            Node::button(K::Start, 95),
        ]));
        let tree = Node::inset(Node::column(rows), layout::MARGIN);
        self.form.place_shown(tree, ui::area(&body));
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let f = &self.form;
        let action = match k {
            K::Chd(c) => {
                if self.chd.handle(f, c) {
                    f.enable(K::Start, self.chd.is_valid());
                    self.chd.set_visible(f, true);
                    self.layout();
                }
                return;
            }
            K::Whole => InspectAction::SetExportWholeDisk(true),
            K::Per => InspectAction::SetExportWholeDisk(false),
            K::Format(i) => match EXPORT_FORMATS.get(i) {
                Some(c) => InspectAction::SetExportFormat(c.format),
                None => return,
            },
            K::Start if self.chd.is_valid() => InspectAction::StartExport(self.chd.choice().0),
            K::Physical => {
                let physical = self.physical.clone();
                crate::app::later(Box::new(move || physical()));
                return;
            }
            K::Cancel => InspectAction::CloseExport,
            K::Mode(index) | K::Mib(index) => {
                let custom_mib = f.text(K::Mib(index)).trim().parse::<u32>().unwrap_or(0);
                let mode = if let K::Mode(_) = k {
                    let rows = self.rows.borrow();
                    let picked = rows
                        .iter()
                        .find(|(i, _)| *i == index)
                        .and_then(|(_, m)| m.get(f.picked(k)?).copied());
                    match picked {
                        Some(m) => m,
                        None => return,
                    }
                } else {
                    SizeMode::Custom
                };
                InspectAction::SetExportSize {
                    index,
                    mode,
                    custom_mib,
                }
            }
            _ => return,
        };
        // The window may close or rebuild with what this sends: act once the handler returns.
        let send = self.send.clone();
        crate::app::later(Box::new(move || send(action)));
    }
}

impl Drop for ExportWindow {
    fn drop(&mut self) {
        self.window.setDelegate(None);
        self.window.orderOut(None);
    }
}
