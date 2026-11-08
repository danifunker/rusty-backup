//! The middle column: copy either way, delete on the active pane, and the copy and export settings.

use std::cell::RefCell;

use adw::prelude::*;
use rusty_backup::controller::commander::{
    ids, CommanderAction as A, CommanderController, ExportFormat, ResourceForkMode, Side,
};

use crate::browser::SendFn;

#[derive(Clone, PartialEq)]
pub struct ColumnShown {
    copy_left: bool,
    copy_right: bool,
    copy_left_hint: String,
    copy_right_hint: String,
    delete: bool,
    delete_hint: String,
    keep_dates: bool,
    format: u32,
    fork: u32,
    loose: bool,
}

pub struct Column {
    root: gtk::Box,
    ltr: gtk::Button,
    rtl: gtk::Button,
    delete: gtk::Button,
    keep_dates: gtk::CheckButton,
    format: gtk::DropDown,
    fork: gtk::DropDown,
    fork_box: gtk::Box,
    shown: RefCell<Option<ColumnShown>>,
}

fn action_button(label: &str, icon: &str, id: &str, send: &SendFn, action: A) -> gtk::Button {
    let content = adw::ButtonContent::builder()
        .label(label)
        .icon_name(icon)
        .build();
    let b = gtk::Button::builder().child(&content).build();
    b.set_widget_name(id);
    let send = send.clone();
    b.connect_clicked(move |_| send(action.clone()));
    b
}

impl Column {
    pub fn build(send: &SendFn) -> Column {
        let ltr = action_button(
            "Copy",
            "go-next-symbolic",
            ids::COPY_LEFT_TO_RIGHT,
            send,
            A::Copy(Side::Left),
        );
        let rtl = action_button(
            "Copy",
            "go-previous-symbolic",
            ids::COPY_RIGHT_TO_LEFT,
            send,
            A::Copy(Side::Right),
        );
        let delete = action_button(
            "Delete",
            "edit-delete-symbolic",
            ids::DELETE,
            send,
            A::Delete,
        );
        let compare = gtk::Button::with_label("Compare");
        compare.set_widget_name(ids::COMPARE);
        compare.set_sensitive(false);
        compare.set_tooltip_text(Some("Compare - not implemented yet"));

        let keep_dates = gtk::CheckButton::with_label("Keep original dates");
        keep_dates.set_widget_name(ids::KEEP_DATES);
        keep_dates.set_tooltip_text(Some(
            "Reproduce each copied file's original timestamps on the destination (HFS catalog \
             dates / Amiga datestamp) instead of stamping the current time. Image-to-image \
             copies only.",
        ));
        let to_screen = send.clone();
        keep_dates.connect_toggled(move |b| to_screen(A::SetKeepDates(b.is_active())));

        let labels: Vec<&str> = ExportFormat::ALL.iter().map(|f| f.label()).collect();
        let format = gtk::DropDown::from_strings(&labels);
        format.set_widget_name(ids::EXPORT_FORMAT);
        format.set_tooltip_text(Some(
            "How the row menu's \"Export to hard drive...\" writes the selection out. The row \
             menu's \"Export as\" submenu can pick a format inline.",
        ));
        let to_screen = send.clone();
        format.connect_selected_notify(move |d| {
            if let Some(f) = ExportFormat::ALL.get(d.selected() as usize) {
                to_screen(A::SetExportFormat(*f));
            }
        });
        let labels: Vec<&str> = ResourceForkMode::ALL.iter().map(|m| m.label()).collect();
        let fork = gtk::DropDown::from_strings(&labels);
        fork.set_widget_name(ids::FORK_MODE);
        fork.set_tooltip_text(Some(
            "How to preserve a Mac file's resource fork when exporting loose files. Tar keeps \
             data forks only; Zip and StuffIt carry forks natively.",
        ));
        let to_screen = send.clone();
        fork.connect_selected_notify(move |d| {
            if let Some(m) = ResourceForkMode::ALL.get(d.selected() as usize) {
                to_screen(A::SetForkMode(*m));
            }
        });
        let fork_box = gtk::Box::new(gtk::Orientation::Vertical, 2);
        let caption = |text: &str| gtk::Label::builder().label(text).xalign(0.0).build();
        fork_box.append(&caption("Forks as:"));
        fork_box.append(&fork);

        let root = gtk::Box::new(gtk::Orientation::Vertical, 6);
        root.set_width_request(168);
        root.set_margin_start(6);
        root.set_margin_end(6);
        root.set_margin_top(48);
        root.append(&ltr);
        root.append(&rtl);
        root.append(&delete);
        root.append(&compare);
        root.append(&keep_dates);
        root.append(&caption("Export as:"));
        root.append(&format);
        root.append(&fork_box);
        Column {
            root,
            ltr,
            rtl,
            delete,
            keep_dates,
            format,
            fork,
            fork_box,
            shown: RefCell::default(),
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn read(ctl: &CommanderController) -> ColumnShown {
        let format = ctl.export_format();
        let mode = ctl.export_fork_mode();
        let at = |found: Option<usize>| found.unwrap_or(0) as u32;
        ColumnShown {
            copy_left: ctl.can_copy(Side::Left),
            copy_right: ctl.can_copy(Side::Right),
            copy_left_hint: ctl.copy_hint(Side::Right),
            copy_right_hint: ctl.copy_hint(Side::Left),
            delete: ctl.can_delete(),
            delete_hint: ctl.delete_hint(),
            keep_dates: ctl.keep_dates(),
            format: at(ExportFormat::ALL.iter().position(|f| *f == format)),
            fork: at(ResourceForkMode::ALL.iter().position(|m| *m == mode)),
            loose: format == ExportFormat::LooseFiles,
        }
    }

    pub fn show(&self, s: ColumnShown) {
        if self.shown.borrow().as_ref() == Some(&s) {
            return;
        }
        self.ltr.set_sensitive(s.copy_left);
        self.ltr.set_tooltip_text(Some(&s.copy_left_hint));
        self.rtl.set_sensitive(s.copy_right);
        self.rtl.set_tooltip_text(Some(&s.copy_right_hint));
        self.delete.set_sensitive(s.delete);
        self.delete.set_tooltip_text(Some(&s.delete_hint));
        if self.keep_dates.is_active() != s.keep_dates {
            self.keep_dates.set_active(s.keep_dates);
        }
        if self.format.selected() != s.format {
            self.format.set_selected(s.format);
        }
        if self.fork.selected() != s.fork {
            self.fork.set_selected(s.fork);
        }
        self.fork_box.set_sensitive(s.loose);
        *self.shown.borrow_mut() = Some(s);
    }
}
