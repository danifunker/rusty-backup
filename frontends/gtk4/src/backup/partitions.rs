//! The partition grid: Include, type, sizes, Calc min and Compact, one row per partition.

use std::cell::RefCell;

use adw::prelude::*;
use rusty_backup::controller::backup::{
    format_size, hints, ids, BackupAction, BackupController, CompactCell, MinCell, PartitionRow,
};

use super::ActionFn;
use crate::form::{button, cell, clear_grid, grid, group, head, line, set_line};
use crate::widgets::{plain, set_text};

/// What the grid shows.
pub struct GridShown {
    table: Option<String>,
    rows: Vec<PartitionRow>,
    error: Option<String>,
}

impl GridShown {
    pub fn read(ctrl: &BackupController) -> GridShown {
        GridShown {
            table: ctrl.table_line(),
            rows: ctrl.rows(),
            error: ctrl.load_error().map(str::to_string),
        }
    }
}

struct Row {
    index: usize,
    include: gtk::CheckButton,
    min: gtk::Label,
    spinner: gtk::Spinner,
    calc: gtk::Button,
    frag: gtk::Label,
    compact: gtk::CheckButton,
}

impl Row {
    fn attach(grid: &gtk::Grid, line: i32, row: &PartitionRow, send: &ActionFn) -> Row {
        let index = row.index;
        let include = gtk::CheckButton::new();
        include.set_widget_name(&format!("{}.{index}", ids::ROW_INCLUDE));
        let s = send.clone();
        include.connect_toggled(move |b| s(BackupAction::SetPartition(index, b.is_active())));

        let position = cell(1.0);
        position.set_label(&row.position.to_string());
        let kind = cell(0.0);
        kind.set_label(&plain(&row.type_name));
        let size = cell(1.0);
        size.set_label(&format_size(row.size_bytes));

        let min = cell(1.0);
        let spinner = gtk::Spinner::new();
        let calc = button("Calc min", &format!("{}.{index}", ids::ROW_CALC_MIN));
        calc.add_css_class("flat");
        calc.set_tooltip_text(Some(hints::CALC_MIN));
        let s = send.clone();
        calc.connect_clicked(move |_| s(BackupAction::CalcMin(index)));
        let min_cell = gtk::Box::new(gtk::Orientation::Horizontal, 0);
        min_cell.set_halign(gtk::Align::End);
        min_cell.append(&min);
        min_cell.append(&spinner);
        min_cell.append(&calc);

        let frag = cell(1.0);
        let compact = gtk::CheckButton::new();
        compact.set_widget_name(&format!("{}.{index}", ids::ROW_COMPACT));
        let s = send.clone();
        compact.connect_toggled(move |b| s(BackupAction::SetCompact(index, b.is_active())));

        grid.attach(&include, 0, line, 1, 1);
        grid.attach(&position, 1, line, 1, 1);
        grid.attach(&kind, 2, line, 1, 1);
        grid.attach(&size, 3, line, 1, 1);
        grid.attach(&min_cell, 4, line, 1, 1);
        grid.attach(&frag, 5, line, 1, 1);
        grid.attach(&compact, 6, line, 1, 1);
        Row {
            index,
            include,
            min,
            spinner,
            calc,
            frag,
            compact,
        }
    }

    fn show(&self, row: &PartitionRow) {
        if self.include.is_active() != row.selected {
            self.include.set_active(row.selected);
        }
        let min = match &row.min {
            MinCell::Size(min) => format_size(*min),
            MinCell::Pending(_) | MinCell::CanCalc | MinCell::Empty => String::new(),
        };
        set_text(&self.min, &min);
        let pending = matches!(row.min, MinCell::Pending(_));
        self.spinner.set_visible(pending);
        self.spinner.set_spinning(pending);
        if let MinCell::Pending(phase) = &row.min {
            self.spinner.set_tooltip_text(Some(&plain(phase)));
        }
        self.calc.set_visible(row.min == MinCell::CanCalc);
        let frag = row.fragmentation.map(|p| format!("{p:.0}%"));
        set_text(&self.frag, frag.as_deref().unwrap_or(""));

        match row.compact {
            CompactCell::Hidden => self.compact.set_visible(false),
            CompactCell::NotImplemented => {
                self.compact.set_visible(true);
                self.compact.set_sensitive(false);
                self.compact.set_active(false);
                self.compact
                    .set_tooltip_text(Some(hints::COMPACT_NOT_BUILT));
            }
            CompactCell::Toggle(on) => {
                self.compact.set_visible(true);
                self.compact.set_sensitive(true);
                self.compact.set_tooltip_text(Some(hints::COMPACT));
                if self.compact.is_active() != on {
                    self.compact.set_active(on);
                }
            }
        }
    }
}

pub struct PartitionGrid {
    root: adw::PreferencesGroup,
    grid: gtk::Grid,
    table: gtk::Label,
    error: gtk::Label,
    rows: RefCell<Vec<Row>>,
    send: ActionFn,
}

impl PartitionGrid {
    pub fn build(send: &ActionFn) -> PartitionGrid {
        let root = group("Partitions");
        let all = button("All", super::ids::SELECT_ALL);
        all.add_css_class("flat");
        let s = send.clone();
        all.connect_clicked(move |_| s(BackupAction::SelectAll));
        let none = button("None", super::ids::SELECT_NONE);
        none.add_css_class("flat");
        let s = send.clone();
        none.connect_clicked(move |_| s(BackupAction::SelectNone));
        let picks = gtk::Box::new(gtk::Orientation::Horizontal, 4);
        picks.append(&all);
        picks.append(&none);
        root.set_header_suffix(Some(&picks));

        let table = line();
        let grid = grid();
        let error = line();
        error.add_css_class("warning");
        root.add(&table);
        root.add(&grid);
        root.add(&error);
        PartitionGrid {
            root,
            grid,
            table,
            error,
            rows: RefCell::default(),
            send: send.clone(),
        }
    }

    pub fn root(&self) -> &adw::PreferencesGroup {
        &self.root
    }

    pub fn show(&self, s: GridShown) {
        self.root
            .set_visible(!s.rows.is_empty() || s.error.is_some());
        set_line(&self.table, s.table.as_deref());
        set_line(&self.error, s.error.as_deref());
        self.grid.set_visible(!s.rows.is_empty());

        let mut rows = self.rows.borrow_mut();
        let same =
            rows.len() == s.rows.len() && rows.iter().zip(&s.rows).all(|(a, b)| a.index == b.index);
        if !same {
            clear_grid(&self.grid);
            rows.clear();
            head(&self.grid, 0, "Include", None);
            head(&self.grid, 1, "#", None);
            head(&self.grid, 2, "Type", None);
            head(&self.grid, 3, "Size", None);
            head(&self.grid, 4, "Min Size", None);
            head(&self.grid, 5, "Frag.", Some(hints::FRAGMENTATION));
            head(&self.grid, 6, "Compact", Some(hints::COMPACT));
            for (i, row) in s.rows.iter().enumerate() {
                rows.push(Row::attach(&self.grid, i as i32 + 1, row, &self.send));
            }
        }
        for (widgets, row) in rows.iter().zip(&s.rows) {
            widgets.show(row);
        }
    }
}
