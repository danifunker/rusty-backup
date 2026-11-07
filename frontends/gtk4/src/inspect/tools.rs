//! The tool buttons under the source picker.

use adw::prelude::*;
use rusty_backup::controller::inspect::{hints, ids, InspectAction};

use super::ActionFn;
use crate::widgets::coming;

/// What the buttons show, read from the controller.
pub struct ToolState {
    pub can_reinspect: bool,
    pub can_make_bootable: bool,
    pub can_close: bool,
    pub has_chd: bool,
    pub is_floppy: bool,
}

pub struct ToolBar {
    root: gtk::FlowBox,
    reinspect: gtk::Button,
    bootable: gtk::Button,
    floppy: gtk::Button,
    chd: gtk::Button,
    close: gtk::Button,
}

fn button(label: &str, id: &str) -> gtk::Button {
    let button = gtk::Button::with_label(label);
    button.set_widget_name(id);
    button
}

fn stub(label: &str, id: &str, what: &str) -> gtk::Button {
    let b = button(label, id);
    coming(&b, what);
    b
}

fn live(label: &str, id: &str, send: &ActionFn, action: fn() -> InspectAction) -> gtk::Button {
    let b = button(label, id);
    let send = send.clone();
    b.connect_clicked(move |_| send(action()));
    b
}

impl ToolBar {
    pub fn build(send: &ActionFn) -> ToolBar {
        let reinspect = live("Re-inspect", ids::REINSPECT, send, || {
            InspectAction::Reinspect
        });
        let export = stub(
            "Export Disk Image...",
            ids::EXPORT,
            "the Export Disk Image dialog",
        );
        let floppy = stub(
            "Convert Floppy Container...",
            ids::CONVERT_FLOPPY,
            "the floppy convert dialog",
        );
        let edit = stub(
            "Edit Partition Table...",
            ids::EDIT_TABLE,
            "the partition editor",
        );
        let add = stub(
            "Add Partition...",
            ids::ADD_PARTITION,
            "the partition editor",
        );
        let expand = stub(
            "Expand Image...",
            ids::EXPAND_IMAGE,
            "the Expand Image dialog",
        );
        let bootable = live("Make Bootable...", ids::MAKE_BOOTABLE, send, || {
            InspectAction::MakeBootable
        });
        bootable.set_tooltip_text(Some(hints::MAKE_BOOTABLE));
        let resize = stub("Resize Partitions...", ids::RESIZE, "the resize dialog");
        let chd = stub("CHD Info", ids::CHD_INFO, "the CHD Info window");
        let close = live("Close", ids::CLOSE, send, || InspectAction::Close);

        let root = gtk::FlowBox::builder()
            .selection_mode(gtk::SelectionMode::None)
            .homogeneous(false)
            .max_children_per_line(12)
            .column_spacing(4)
            .row_spacing(4)
            .build();
        for b in [
            &reinspect, &export, &floppy, &edit, &add, &expand, &bootable, &resize, &chd, &close,
        ] {
            root.append(b);
        }
        ToolBar {
            root,
            reinspect,
            bootable,
            floppy,
            chd,
            close,
        }
    }

    pub fn root(&self) -> &gtk::FlowBox {
        &self.root
    }

    /// A hidden button's `FlowBoxChild` stays behind, so hide that too.
    fn set_shown(button: &gtk::Button, on: bool) {
        button.set_visible(on);
        if let Some(child) = button.parent() {
            child.set_visible(on);
        }
    }

    pub fn show(&self, state: &ToolState) {
        self.reinspect.set_sensitive(state.can_reinspect);
        self.bootable.set_sensitive(state.can_make_bootable);
        Self::set_shown(&self.floppy, state.is_floppy);
        Self::set_shown(&self.chd, state.has_chd);
        Self::set_shown(&self.close, state.can_close);
    }
}
