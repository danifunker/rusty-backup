//! Export Disk Image over the Inspect controller's export settings; egui's `show_export_popup`
//! in `src/gui/inspect_tab.rs` is the reference.

use std::cell::RefCell;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::app::chd::{ChdOptionsControl, ChdProfile};
use rusty_backup::controller::inspect::{
    after_caption, chd_estimate_line, ids, max_size_hint, per_partition_refusal, pfs3_affected,
    ExportFormat, ExportFormatChoice, ExportNotes, InspectAction, InspectController, LayoutBar,
    PartitionExportConfig, EXPORT_INTRO, HFV_WHOLE_REFUSAL, PFS3_SHRINK_LINES, PFS3_SHRINK_TITLE,
    PHYSICAL_REFUSAL, SECTOR_BY_SECTOR_WARNING, SIZES_HINT,
};

use super::bar::PartitionBar;
use super::ActionFn;
use crate::browser::dialogs::{page, window, wrapped};
use crate::chd::{ChdControl, ChdIds};
use crate::form::{button, grid, head, Notes, Tone};
use crate::size_row::{SizeCell, SizeShown};
use crate::widgets::{flow, set_text};

/// What the window shows, read while the controller is borrowed.
pub struct ExportShown {
    formats: Vec<(ExportFormatChoice, bool)>,
    whole: bool,
    whole_only: bool,
    per_only: bool,
    format: ExportFormat,
    configs: Vec<PartitionExportConfig>,
    layout: LayoutBar,
    after: String,
    notes: ExportNotes,
    can_physical: bool,
}

impl ExportShown {
    /// `None` while the window is closed.
    pub fn read(ctrl: &mut InspectController) -> Option<ExportShown> {
        if !ctrl.export_open() {
            return None;
        }
        let formats = ctrl.export_formats();
        let can_physical = ctrl.can_export_physical();
        let layout = ctrl.export_layout();
        let after = after_caption(&layout);
        let notes = ctrl.export_notes();
        let s = ctrl.export_settings_mut();
        Some(ExportShown {
            formats,
            whole: s.whole_disk,
            whole_only: s.whole_disk_only(),
            per_only: s.per_partition_only(),
            format: s.format,
            configs: s.configs.clone(),
            layout,
            after,
            notes,
            can_physical,
        })
    }
}

/// One partition's size row.
struct SizeRow {
    index: usize,
    cell: SizeCell,
}

pub struct ExportParts {
    whole: gtk::CheckButton,
    per: gtk::CheckButton,
    formats: Vec<(ExportFormat, gtk::CheckButton)>,
    chd: Rc<ChdControl>,
    current: PartitionBar,
    after_caption: gtk::Label,
    after: PartitionBar,
    sizes_title: gtk::Label,
    sizes: gtk::Grid,
    rows: RefCell<Vec<SizeRow>>,
    send: ActionFn,
    notes: Notes,
    physical: gtk::Button,
}

impl ExportParts {
    /// The window and its parts; `physical` hands the image to Physical Disk Export.
    pub fn build(send: &ActionFn, physical: Rc<dyn Fn()>) -> (adw::Dialog, ExportParts) {
        let body = page();
        body.set_spacing(8);
        body.append(&wrapped(EXPORT_INTRO));
        body.append(&gtk::Label::builder().label("Current:").xalign(0.0).build());
        let current = PartitionBar::build(|_| {});
        current.hide_legend();
        body.append(current.root());
        let after_caption = gtk::Label::builder().xalign(0.0).build();
        body.append(&after_caption);
        let after = PartitionBar::build(|_| {});
        body.append(after.root());

        let whole = gtk::CheckButton::with_label("Whole Disk (single file)");
        whole.set_widget_name(ids::EXPORT_WHOLE);
        let per = gtk::CheckButton::with_label("Per Partition (one file per partition)");
        per.set_widget_name(ids::EXPORT_PER_PARTITION);
        per.set_group(Some(&whole));
        body.append(&whole);
        body.append(&per);

        let format_title = gtk::Label::builder().label("Format:").xalign(0.0).build();
        format_title.add_css_class("heading");
        body.append(&format_title);
        let format_flow = flow();
        format_flow.set_widget_name(ids::EXPORT_FORMAT);
        let mut formats: Vec<(ExportFormat, gtk::CheckButton)> = Vec::new();
        for choice in rusty_backup::controller::inspect::EXPORT_FORMATS {
            let radio = gtk::CheckButton::with_label(choice.label);
            if !choice.hint.is_empty() {
                radio.set_tooltip_text(Some(choice.hint));
            }
            if let Some((_, first)) = formats.first() {
                radio.set_group(Some(first));
            }
            format_flow.append(&radio);
            formats.push((choice.format, radio));
        }
        body.append(&format_flow);

        let chd = ChdControl::build(
            "CHD",
            "",
            ChdIds {
                mode: ids::EXPORT_CHD_MODE,
                hunk: ids::EXPORT_CHD_HUNK,
                codec: ids::EXPORT_CHD_CODEC,
            },
            ChdOptionsControl::new(ChdProfile::Hd),
            ChdProfile::Hd,
            Rc::new(|| {}),
        );
        body.append(chd.root());

        let sizes_title = gtk::Label::builder()
            .label("Partition Sizes:")
            .xalign(0.0)
            .tooltip_text(SIZES_HINT)
            .build();
        sizes_title.add_css_class("heading");
        body.append(&sizes_title);
        let sizes = grid();
        body.append(&sizes);
        let notes = Notes::new();
        body.append(notes.root());

        let start = button("Export...", ids::EXPORT_START);
        start.add_css_class("suggested-action");
        let physical_button = button("Physical Disk Export...", ids::EXPORT_PHYSICAL);
        let cancel = button("Cancel", ids::EXPORT_CLOSE);
        body.append(&crate::volume::button_row(&[
            &cancel,
            &physical_button,
            &start,
        ]));

        let s = send.clone();
        whole.connect_toggled(move |b| {
            if b.is_active() {
                s(InspectAction::SetExportWholeDisk(true));
            }
        });
        let s = send.clone();
        per.connect_toggled(move |b| {
            if b.is_active() {
                s(InspectAction::SetExportWholeDisk(false));
            }
        });
        for (format, radio) in &formats {
            let (s, format) = (send.clone(), *format);
            radio.connect_toggled(move |b| {
                if b.is_active() {
                    s(InspectAction::SetExportFormat(format));
                }
            });
        }
        let (s, control) = (send.clone(), chd.clone());
        start.connect_clicked(move |_| {
            if control.is_valid() {
                s(InspectAction::StartExport(control.choice().0));
            }
        });
        physical_button.connect_clicked(move |_| physical());
        let s = send.clone();
        cancel.connect_clicked(move |_| s(InspectAction::CloseExport));

        let scroll = crate::volume::scroller(&body);
        let dialog = window("Export Disk Image", 620, 680, &scroll);
        let parts = ExportParts {
            whole,
            per,
            formats,
            chd,
            current,
            after_caption,
            after,
            sizes_title,
            sizes,
            rows: RefCell::default(),
            send: send.clone(),
            notes,
            physical: physical_button,
        };
        (dialog, parts)
    }

    /// Run inside the screen's `SyncGuard`.
    pub fn show(&self, s: ExportShown) {
        if self.whole.is_active() != s.whole {
            self.whole.set_active(s.whole);
        }
        if self.per.is_active() == s.whole {
            self.per.set_active(!s.whole);
        }
        self.whole.set_sensitive(!s.per_only);
        self.whole
            .set_tooltip_text(s.per_only.then_some(HFV_WHOLE_REFUSAL));
        self.per.set_sensitive(!s.whole_only);
        self.per.set_tooltip_text(per_partition_refusal(s.format));
        for (format, radio) in &self.formats {
            let allowed = s.formats.iter().any(|(c, ok)| c.format == *format && *ok);
            radio.set_sensitive(allowed);
            if *format == s.format && !radio.is_active() {
                radio.set_active(true);
            }
        }
        self.chd.root().set_visible(s.format == ExportFormat::Chd);

        let total = |list: &[rusty_backup::controller::inspect::LayoutSegment]| {
            list.iter().map(|x| x.size_bytes).sum::<u64>()
        };
        let (now, then) = (total(&s.layout.current), total(&s.layout.after));
        let most = now.max(then).max(1) as f32;
        self.current.set_scale(now as f32 / most);
        self.current.show(s.layout.current, None);
        set_text(&self.after_caption, &s.after);
        self.after.set_scale(then as f32 / most);
        self.after.show(s.layout.after, None);

        self.show_sizes(&s.configs);
        self.notes.show(notes(&s.notes));
        self.physical.set_sensitive(s.can_physical);
        self.physical
            .set_tooltip_text((!s.can_physical).then_some(PHYSICAL_REFUSAL));
    }

    /// The rows are rebuilt only when the partitions change.
    fn show_sizes(&self, configs: &[PartitionExportConfig]) {
        self.sizes_title.set_visible(!configs.is_empty());
        let same = {
            let rows = self.rows.borrow();
            rows.len() == configs.len() && rows.iter().zip(configs).all(|(r, c)| r.index == c.index)
        };
        if !same {
            crate::form::clear_grid(&self.sizes);
            head(&self.sizes, 0, "#", None);
            head(&self.sizes, 1, "Type", None);
            head(&self.sizes, 2, "Size Mode", None);
            head(&self.sizes, 3, "Size (MiB)", None);
            let mut rows = Vec::new();
            for (i, cfg) in configs.iter().enumerate() {
                let line = i as i32 + 1;
                let number = crate::form::cell(0.0);
                set_text(&number, &cfg.index.to_string());
                let kind = crate::form::cell(0.0);
                set_text(&kind, &cfg.type_name);
                self.sizes.attach(&number, 0, line, 1, 1);
                self.sizes.attach(&kind, 1, line, 1, 1);
                let (send, index) = (self.send.clone(), cfg.index);
                let cell = SizeCell::new(
                    (ids::EXPORT_SIZE_MODE, ids::EXPORT_SIZE_MIB),
                    Rc::new(move |mode, custom_mib| {
                        send(InspectAction::SetExportSize {
                            index,
                            mode,
                            custom_mib,
                        })
                    }),
                    Rc::new(|| {}),
                );
                cell.attach(&self.sizes, 2, line);
                rows.push(SizeRow {
                    index: cfg.index,
                    cell,
                });
            }
            *self.rows.borrow_mut() = rows;
        }
        for (row, cfg) in self.rows.borrow().iter().zip(configs) {
            row.cell.show(SizeShown {
                choices: cfg.mode_choices(),
                mode: cfg.choice,
                custom_mib: cfg.custom_mib_effective(),
                bounds: cfg.custom_bounds_mib(),
                effective: cfg.effective_size(),
                deferred: None,
            });
            let hint = cfg.max_size.map(max_size_hint);
            row.cell.set_tooltip(hint.as_deref());
        }
    }
}

fn notes(n: &ExportNotes) -> Vec<(Tone, String)> {
    let mut out = Vec::new();
    if !n.pfs3_shrink.is_empty() {
        out.push((Tone::Warning, PFS3_SHRINK_TITLE.to_string()));
        out.push((Tone::Plain, pfs3_affected(&n.pfs3_shrink)));
        out.extend(
            PFS3_SHRINK_LINES
                .iter()
                .map(|l| (Tone::Plain, l.to_string())),
        );
    }
    if let Some(workflow) = n.chd_workflow {
        out.push((Tone::Info, workflow.to_string()));
        if let Some(estimate) = n.chd_estimate {
            out.push((Tone::Plain, chd_estimate_line(estimate)));
        }
        if n.sector_by_sector_warning {
            out.push((Tone::Warning, SECTOR_BY_SECTOR_WARNING.to_string()));
        }
    }
    out
}
