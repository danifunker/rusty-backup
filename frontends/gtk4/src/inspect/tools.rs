//! The tool buttons under the source picker.

use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::inspect::{hints, ids, InspectAction};

use super::ActionFn;
use crate::widgets::{flow, flow_show};

/// What the buttons show, read from the controller.
pub struct ToolState {
    pub can_reinspect: bool,
    pub can_export: bool,
    pub can_edit_table: bool,
    pub can_add_partition: bool,
    pub add_hint: String,
    pub can_make_bootable: bool,
    /// Resize Image applies, and its wizard is not mid-write.
    pub can_resize_image: bool,
    /// Resize Partitions is open on a running write.
    pub resizing: bool,
    pub can_close: bool,
    pub has_chd: bool,
    pub can_convert: bool,
}

pub struct ToolBar {
    root: gtk::FlowBox,
    reinspect: gtk::Button,
    export: gtk::Button,
    edit: gtk::Button,
    add: gtk::Button,
    expand: gtk::Button,
    bootable: gtk::Button,
    resize: gtk::Button,
    convert: gtk::Button,
    chd: gtk::Button,
    close: gtk::Button,
}

fn button(label: &str, id: &str) -> gtk::Button {
    let button = gtk::Button::with_label(label);
    button.set_widget_name(id);
    button
}

fn live(label: &str, id: &str, send: &ActionFn, action: fn() -> InspectAction) -> gtk::Button {
    let b = button(label, id);
    let send = send.clone();
    b.connect_clicked(move |_| send(action()));
    b
}

fn view(label: &str, id: &str, run: Rc<dyn Fn()>) -> gtk::Button {
    let b = button(label, id);
    b.connect_clicked(move |_| run());
    b
}

impl ToolBar {
    /// `resize` and `resize_image` (behind Expand Image) open the view's own windows; `convert`
    /// hands over to Convert.
    pub fn build(
        send: &ActionFn,
        resize: Rc<dyn Fn()>,
        resize_image: Rc<dyn Fn()>,
        convert: Rc<dyn Fn()>,
    ) -> ToolBar {
        let reinspect = live("Re-inspect", ids::REINSPECT, send, || {
            InspectAction::Reinspect
        });
        let export = live("Export Disk Image...", ids::EXPORT, send, || {
            InspectAction::OpenExport
        });
        let convert = view("Convert...", ids::CONVERT, convert);
        convert.set_tooltip_text(Some(rusty_backup::controller::convert::hints::HANDOFF));
        let edit = live("Edit Partition Table...", ids::EDIT_TABLE, send, || {
            InspectAction::EditTable
        });
        let add = live("Add Partition...", ids::ADD_PARTITION, send, || {
            InspectAction::AddPartition
        });
        let expand = view("Expand Image...", ids::EXPAND_IMAGE, resize_image);
        let bootable = live("Make Bootable...", ids::MAKE_BOOTABLE, send, || {
            InspectAction::MakeBootable
        });
        bootable.set_tooltip_text(Some(hints::MAKE_BOOTABLE));
        let resize = view("Resize Partitions...", ids::RESIZE, resize);
        expand.set_tooltip_text(Some(
            "Give the image a new size and resize its partitions with their filesystems, \
             checking the volumes before and after.",
        ));
        let chd = live("CHD Info", ids::CHD_INFO, send, || InspectAction::ChdInfo);
        let close = live("Close", ids::CLOSE, send, || InspectAction::Close);

        let root = flow();
        for b in [
            &reinspect, &export, &convert, &edit, &add, &expand, &bootable, &resize, &chd, &close,
        ] {
            root.append(b);
        }
        ToolBar {
            root,
            reinspect,
            export,
            edit,
            add,
            expand,
            bootable,
            resize,
            convert,
            chd,
            close,
        }
    }

    pub fn root(&self) -> &gtk::FlowBox {
        &self.root
    }

    pub fn show(&self, state: &ToolState) {
        self.reinspect.set_sensitive(state.can_reinspect);
        self.export.set_sensitive(state.can_export);
        self.edit.set_sensitive(state.can_edit_table);
        self.add.set_sensitive(state.can_add_partition);
        self.add.set_tooltip_text(Some(&state.add_hint));
        self.bootable.set_sensitive(state.can_make_bootable);
        self.expand.set_sensitive(state.can_resize_image);
        self.resize
            .set_sensitive(state.can_edit_table && !state.resizing);
        flow_show(&self.convert, state.can_convert);
        flow_show(&self.chd, state.has_chd);
        flow_show(&self.close, state.can_close);
    }
}
