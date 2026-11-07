//! The volume dialogs with controllers of their own: Resize Partitions, Expand HFS, Resize Alto
//! Disk, the disk writer (Physical Disk Export / Write Image File) and Convert Floppy Container.
//!
//! Each is a retained view over its controller (docs/gtk4_shell_plan.md), built when it opens and
//! dropped when the controller closes. The window cannot close itself: a user close sends the
//! controller's Close, and the window goes only once the controller says it is closed.

pub mod alto;
pub mod disk_write;
pub mod expand_hfs;
pub mod floppy;
pub mod resize;

use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::inspect::{LayoutSegment, SegmentKind};
use rusty_backup::controller::partition_edit::{BarSegment, LayoutBar};

use crate::inspect::bar::PartitionBar;
use crate::widgets::set_text;

/// A window over `body` whose close asks `on_close` first, so a running write keeps it up.
/// `height` is a wish: a dialog taller than the app window shrinks to it and scrolls.
pub fn window(
    title: &str,
    width: i32,
    height: i32,
    body: &impl IsA<gtk::Widget>,
    on_close: Rc<dyn Fn()>,
) -> adw::Dialog {
    let dialog = crate::browser::dialogs::window(title, width, height, &scroller(body));
    dialog.set_can_close(false);
    dialog.connect_close_attempt(move |_| on_close());
    dialog
}

/// `body` in a scroller that fills what the dialog gives it.
pub fn scroller(body: &impl IsA<gtk::Widget>) -> gtk::ScrolledWindow {
    gtk::ScrolledWindow::builder()
        .child(body)
        .hscrollbar_policy(gtk::PolicyType::Automatic)
        .vexpand(true)
        .build()
}

/// The plain segments a layout bar draws.
pub fn segments(list: &[BarSegment]) -> Vec<LayoutSegment> {
    let each = list.iter().map(|s| LayoutSegment {
        label: s.label.clone(),
        fs: s.fs.clone(),
        size_bytes: s.size_bytes,
        kind: match s.color {
            Some(color_index) => SegmentKind::Partition { color_index },
            None => SegmentKind::Free,
        },
        partition: None,
    });
    each.collect()
}

/// A caption over a layout bar.
pub struct CaptionedBar {
    root: gtk::Box,
    caption: gtk::Label,
    bar: PartitionBar,
}

impl CaptionedBar {
    pub fn new(legend: bool) -> CaptionedBar {
        let caption = gtk::Label::builder().xalign(0.0).build();
        let bar = PartitionBar::build(|_| {});
        if !legend {
            bar.hide_legend();
        }
        let root = gtk::Box::new(gtk::Orientation::Vertical, 2);
        root.append(&caption);
        root.append(bar.root());
        CaptionedBar { root, caption, bar }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    /// Draw `bar` across `scale` of the width.
    pub fn show(&self, bar: &LayoutBar, scale: f32) {
        set_text(&self.caption, &bar.caption);
        self.bar.set_scale(scale);
        self.bar.show(segments(&bar.segments), None);
    }
}

/// Radio buttons in one group, in a row.
pub fn radios(labels: &[&str]) -> (gtk::Box, Vec<gtk::CheckButton>) {
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 12);
    let mut list: Vec<gtk::CheckButton> = Vec::new();
    for label in labels {
        let radio = gtk::CheckButton::with_label(label);
        if let Some(first) = list.first() {
            radio.set_group(Some(first));
        }
        row.append(&radio);
        list.push(radio);
    }
    (row, list)
}

/// Tick the radio at `index` when it is not already.
pub fn set_radio(list: &[gtk::CheckButton], index: usize) {
    if let Some(radio) = list.get(index) {
        if !radio.is_active() {
            radio.set_active(true);
        }
    }
}

/// A `title value` line, its value selectable.
pub fn fact_line() -> gtk::Label {
    gtk::Label::builder()
        .xalign(0.0)
        .wrap(true)
        .wrap_mode(gtk::pango::WrapMode::WordChar)
        .selectable(true)
        .build()
}

/// A right-aligned row of buttons.
pub fn button_row(buttons: &[&gtk::Button]) -> gtk::Box {
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    row.set_halign(gtk::Align::End);
    row.set_margin_top(6);
    for b in buttons {
        row.append(*b);
    }
    row
}
