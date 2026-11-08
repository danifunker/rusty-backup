//! The full-disk form: alignment, per-partition sizes, and the trailing free space with its bars.

use std::cell::RefCell;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::inspect::{LayoutSegment, SegmentKind};
use rusty_backup::controller::restore::{
    format_size, hints, ids, AlignmentChoice, ExpandPlan, ExpandPreview, PlanSegment,
    RestoreAction, RestoreController, SizeMode, SizeRow, ALIGN_SECTORS_MAX,
};

use super::{tone, ActionFn};
use crate::form::{
    cell, clear_grid, combo, grid, group, head, line, set_combo, set_labels, set_line, set_spin,
    set_switch, spin, switch, Notes, Tone,
};
use crate::inspect::bar::PartitionBar;
use crate::size_row::{SizeCell, SizeShown};
use crate::widgets::plain;

const CHOICES: [AlignmentChoice; 3] = [
    AlignmentChoice::Original,
    AlignmentChoice::Modern1MB,
    AlignmentChoice::Custom,
];

/// What the form shows.
pub struct SizesShown {
    has_backup: bool,
    alignment: (AlignmentChoice, u32),
    original_label: String,
    rows: Vec<(SizeRow, Vec<SizeMode>)>,
    notes: Vec<(Tone, String)>,
    expand_shown: bool,
    expand: ExpandPlan,
    cap_mib: u32,
    expand_line: String,
    preview: ExpandPreview,
}

impl SizesShown {
    pub fn read(ctrl: &RestoreController) -> SizesShown {
        let rows = ctrl.size_rows().iter().map(|r| {
            let choices = ctrl.size_mode_choices(r.index);
            (r.clone(), choices)
        });
        let notes = ctrl.size_notes().into_iter();
        SizesShown {
            has_backup: ctrl.has_backup(),
            alignment: ctrl.alignment(),
            original_label: ctrl.original_alignment_label(),
            rows: rows.collect(),
            notes: notes.map(|n| (tone(n.level), n.text)).collect(),
            expand_shown: ctrl.expand_shown(),
            expand: ctrl.expand(),
            cap_mib: ctrl.expand_cap_mib(),
            expand_line: ctrl.expand_size_line(),
            preview: ctrl.expand_preview(),
        }
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

struct Row {
    index: usize,
    original: gtk::Label,
    minimum: gtk::Label,
    size: SizeCell,
}

pub struct Sizes {
    root: gtk::Box,
    alignment_group: adw::PreferencesGroup,
    alignment: adw::ComboRow,
    custom: adw::SpinRow,
    sizes_group: adw::PreferencesGroup,
    grid: gtk::Grid,
    rows: RefCell<Vec<Row>>,
    notes: Notes,
    expand_group: adw::PreferencesGroup,
    expand: adw::SwitchRow,
    free: adw::SpinRow,
    expand_line: gtk::Label,
    extend: adw::ComboRow,
    current_label: gtk::Label,
    current: PartitionBar,
    after_label: gtk::Label,
    after: PartitionBar,
    send: ActionFn,
}

impl Sizes {
    pub fn build(send: &ActionFn) -> Sizes {
        let expand_group = group(hints::EXPAND_TITLE);
        let expand = switch(hints::EXPAND, ids::EXPAND);
        expand.set_tooltip_text(Some(hints::EXPAND_HOVER));
        let s = send.clone();
        expand.connect_active_notify(move |r| s(RestoreAction::SetExpand(r.is_active())));
        let free = spin("Free space (MiB)", ids::EXPAND_MIB, 0.0, 1.0);
        let s = send.clone();
        free.connect_value_notify(move |r| s(RestoreAction::SetExpandMib(r.value() as u32)));
        let extend = combo(
            "Free space goes",
            ids::EXPAND_EXTEND,
            &[hints::EXPAND_LEAVE, hints::EXPAND_EXTEND],
        );
        let s = send.clone();
        extend.connect_selected_notify(move |r| {
            s(RestoreAction::SetExtendLast(r.selected() == 1));
            let hover = match r.selected() {
                1 => hints::EXPAND_EXTEND_HOVER,
                _ => hints::EXPAND_LEAVE_HOVER,
            };
            r.set_tooltip_text(Some(hover));
        });
        let expand_line = line();
        expand_line.add_css_class("dim-label");
        let current_label = line();
        let current = PartitionBar::build(|_| {});
        current.hide_legend();
        let after_label = line();
        let after = PartitionBar::build(|_| {});
        expand_group.add(&expand);
        expand_group.add(&free);
        expand_group.add(&extend);
        expand_group.add(&expand_line);
        expand_group.add(&current_label);
        expand_group.add(current.root());
        expand_group.add(&after_label);
        expand_group.add(after.root());

        let alignment_group = group("Alignment");
        let alignment = combo("Partition starts", ids::ALIGNMENT, &[]);
        let s = send.clone();
        alignment.connect_selected_notify(move |r| {
            if let Some(a) = CHOICES.get(r.selected() as usize) {
                s(RestoreAction::SetAlignment(*a));
            }
        });
        let custom = spin(
            "Custom (sectors)",
            ids::ALIGNMENT,
            1.0,
            f64::from(ALIGN_SECTORS_MAX),
        );
        custom.set_widget_name(ids::CUSTOM_ALIGNMENT);
        let s = send.clone();
        custom
            .connect_value_notify(move |r| s(RestoreAction::SetCustomAlignment(r.value() as u32)));
        alignment_group.add(&alignment);
        alignment_group.add(&custom);

        let sizes_group = group("Partition Sizes");
        let table = grid();
        let notes = Notes::new();
        sizes_group.add(&table);
        sizes_group.add(notes.root());

        let root = gtk::Box::new(gtk::Orientation::Vertical, 12);
        root.append(&expand_group);
        root.append(&alignment_group);
        root.append(&sizes_group);
        Sizes {
            root,
            alignment_group,
            alignment,
            custom,
            sizes_group,
            grid: table,
            rows: RefCell::default(),
            notes,
            expand_group,
            expand,
            free,
            expand_line,
            extend,
            current_label,
            current,
            after_label,
            after,
            send: send.clone(),
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    fn build_rows(&self, rows: &mut Vec<Row>, shown: &[(SizeRow, Vec<SizeMode>)]) {
        clear_grid(&self.grid);
        rows.clear();
        head(&self.grid, 0, "#", None);
        head(&self.grid, 1, "Type", None);
        head(&self.grid, 2, "Original", None);
        head(&self.grid, 3, "Minimum", None);
        head(&self.grid, 4, "Size Mode", None);
        head(&self.grid, 5, "Size (MiB)", None);
        for (pos, (row, _)) in shown.iter().enumerate() {
            let line = pos as i32 + 1;
            let index = row.index;
            // Position, matching `inspect`'s `#`; `row.index` is the table slot.
            let number = cell(1.0);
            number.set_label(&(pos + 1).to_string());
            let kind = cell(0.0);
            kind.set_label(&plain(&row.type_name));
            let original = cell(1.0);
            let minimum = cell(1.0);
            self.grid.attach(&number, 0, line, 1, 1);
            self.grid.attach(&kind, 1, line, 1, 1);
            self.grid.attach(&original, 2, line, 1, 1);
            self.grid.attach(&minimum, 3, line, 1, 1);
            let s = self.send.clone();
            let change = Rc::new(move |mode, custom_mib| {
                s(RestoreAction::SetPartitionSize {
                    index,
                    mode,
                    custom_mib,
                });
            });
            let id = format!("{}.{index}", ids::ROW_MODE);
            let size_id = format!("{}.{index}", ids::ROW_SIZE);
            let size = SizeCell::new((&id, &size_id), change, Rc::new(|| {}));
            size.attach(&self.grid, 4, line);
            rows.push(Row {
                index,
                original,
                minimum,
                size,
            });
        }
    }

    fn show_rows(&self, s: &SizesShown) {
        let mut rows = self.rows.borrow_mut();
        let same = rows.len() == s.rows.len()
            && rows.iter().zip(&s.rows).all(|(a, b)| a.index == b.0.index);
        if !same {
            self.build_rows(&mut rows, &s.rows);
        }
        for (widgets, (row, choices)) in rows.iter().zip(&s.rows) {
            widgets.original.set_label(&format_size(row.original_size));
            widgets.minimum.set_label(&format_size(row.minimum_size));
            widgets.size.show(SizeShown {
                choices: choices.clone(),
                mode: row.mode,
                custom_mib: row.custom_mib_effective(),
                bounds: row.custom_bounds_mib(),
                effective: row.effective_size(),
                deferred: None,
            });
        }
    }

    fn show_expand(&self, s: &SizesShown) {
        set_switch(&self.expand, s.expand.enabled);
        let cap = f64::from(s.cap_mib);
        let adjustment = self.free.adjustment();
        if adjustment.upper() != cap {
            adjustment.set_upper(cap);
        }
        set_spin(&self.free, f64::from(s.expand.free_mib));
        set_combo(&self.extend, u32::from(s.expand.extend_last));
        let on = s.expand.enabled;
        self.free.set_visible(on);
        self.extend.set_visible(on);
        set_line(&self.expand_line, on.then_some(s.expand_line.as_str()));
        set_line(&self.current_label, on.then_some("Current:"));
        let after = format!("After  (+ {} free):", format_size(s.preview.added));
        set_line(&self.after_label, on.then_some(after.as_str()));
        self.current.root().set_visible(on);
        self.after.root().set_visible(on);
        if !on {
            return;
        }
        self.current.set_scale(s.preview.scale);
        self.current.show(segments(&s.preview.current), None);
        let mut bars = segments(&s.preview.after);
        if s.preview.free_tail > 0 {
            bars.push(LayoutSegment {
                label: String::new(),
                fs: String::new(),
                size_bytes: s.preview.free_tail,
                kind: SegmentKind::Free,
                partition: None,
            });
        }
        self.after.show(bars, None);
    }

    /// Run inside the screen's `SyncGuard`; `full` is the full-disk mode, the only one sized here.
    pub fn show(&self, s: SizesShown, full: bool) {
        self.root.set_visible(full);
        self.expand_group.set_visible(s.expand_shown);
        self.alignment_group.set_visible(s.has_backup);
        self.sizes_group.set_visible(!s.rows.is_empty());
        if !full {
            return;
        }
        let labels = [
            s.original_label.clone(),
            "1 MB boundaries (modern, LBA 2048)".to_string(),
            "Custom".to_string(),
        ];
        set_labels(&self.alignment, &labels);
        let (choice, sectors) = s.alignment;
        let at = CHOICES.iter().position(|c| *c == choice).unwrap_or(0);
        set_combo(&self.alignment, at as u32);
        self.custom.set_visible(choice == AlignmentChoice::Custom);
        set_spin(&self.custom, f64::from(sectors));
        self.show_expand(&s);
        self.show_rows(&s);
        self.notes.show(s.notes);
    }
}
