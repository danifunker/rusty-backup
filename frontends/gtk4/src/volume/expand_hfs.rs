//! Expand HFS Volume over `controller::expand_hfs`; `src/gui/expand_hfs_dialog.rs` is the reference.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use rusty_backup::controller::app::Notify;
use rusty_backup::controller::expand_hfs::{
    ids, BlockChoice, ExpandHfsAction, ExpandHfsController, ExpandSource, LayoutSegment,
};
use rusty_backup::controller::{Controller, Effect};

use super::{button_row, fact_line, radios, set_radio, window};
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::{button, set_line};
use crate::inspect::bar::PartitionBar;
use crate::sync::SyncGuard;
use crate::widgets::{plain, set_text};

const MIB: u64 = 1024 * 1024;

const HFV_NOTE: &str = "Bare classic-HFS volume with no partition table - mountable in \
    BasiliskII / SheepShaver as `.hfv`, or usable as a raw single-partition image. Capped at \
    4063 MB (classic HFS limit).";

struct Shown {
    source: Option<ExpandSource>,
    partition: Option<String>,
    usage: Vec<LayoutSegment>,
    editable: bool,
    flat: bool,
    blocks: Vec<BlockChoice>,
    block: u32,
    bounds: (u32, u32),
    size: u32,
    output: Option<String>,
    phase: Option<String>,
    outcome: Option<Result<(), String>>,
    lines: Vec<String>,
    running: bool,
    can_start: bool,
}

pub struct ExpandHfsDialog {
    ctrl: RefCell<ExpandHfsController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<ExpandHfsDialog>,
    dialog: adw::Dialog,
    partition: gtk::Label,
    usage: PartitionBar,
    facts: gtk::Label,
    form: gtk::Box,
    format: Vec<gtk::CheckButton>,
    note: gtk::Label,
    block: gtk::DropDown,
    blocks: RefCell<Vec<BlockChoice>>,
    size: gtk::SpinButton,
    output: gtk::Label,
    spinner: gtk::Spinner,
    phase: gtk::Label,
    outcome: gtk::Label,
    lines: gtk::Label,
    expand: gtk::Button,
    close: gtk::Button,
}

fn row(title: &str, widget: &impl IsA<gtk::Widget>) -> gtk::Box {
    let b = gtk::Box::new(gtk::Orientation::Horizontal, 8);
    b.append(&gtk::Label::new(Some(title)));
    b.append(widget);
    b
}

impl ExpandHfsDialog {
    pub fn open(notify: Arc<dyn Notify>, host: EffectHost, source: ExpandSource) -> Rc<Self> {
        let d = Rc::new_cyclic(|me: &Weak<ExpandHfsDialog>| {
            let send: Rc<dyn Fn(ExpandHfsAction)> = {
                let me = me.clone();
                Rc::new(move |action| {
                    if let Some(d) = me.upgrade() {
                        if !d.sync.active() {
                            d.apply(action);
                        }
                    }
                })
            };
            let source_head = gtk::Label::builder().label("Source").xalign(0.0).build();
            source_head.add_css_class("heading");
            let partition = fact_line();
            let usage = PartitionBar::build(|_| {});
            let facts = fact_line();
            let target_head = gtk::Label::builder().label("Target").xalign(0.0).build();
            target_head.add_css_class("heading");

            let (format_row, format) = radios(&["APM disk (.hda)", "Bare HFS image (.hfv / .img)"]);
            format_row.set_widget_name(ids::OUTPUT_FORMAT);
            for (i, radio) in format.iter().enumerate() {
                let s = send.clone();
                radio.connect_toggled(move |r| {
                    if r.is_active() {
                        s(ExpandHfsAction::SetFlatHfv(i == 1));
                    }
                });
            }
            let note = fact_line();
            note.set_selectable(false);
            note.add_css_class("dim-label");
            note.add_css_class("caption");
            note.set_label(HFV_NOTE);

            let block = gtk::DropDown::from_strings(&[]);
            block.set_widget_name(ids::BLOCK_SIZE);
            let size = gtk::SpinButton::with_range(1.0, 1.0, 1.0);
            size.set_widget_name(ids::TARGET_SIZE);
            let s = send.clone();
            size.connect_value_changed(move |sp| s(ExpandHfsAction::SetSizeMib(sp.value() as u32)));
            let output = gtk::Label::builder()
                .xalign(0.0)
                .hexpand(true)
                .ellipsize(gtk::pango::EllipsizeMode::Middle)
                .build();
            let save = button("Save As...", ids::SAVE_AS);
            let s = send.clone();
            save.connect_clicked(move |_| s(ExpandHfsAction::PickOutput));
            let out_row = row("Output file:", &output);
            out_row.append(&save);

            let form = gtk::Box::new(gtk::Orientation::Vertical, 6);
            form.append(&row("Output format:", &format_row));
            form.append(&note);
            form.append(&row("Allocation block size:", &block));
            form.append(&row("Target size (MiB):", &size));
            form.append(&out_row);

            let spinner = gtk::Spinner::new();
            let phase = gtk::Label::builder().xalign(0.0).build();
            let status = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            status.append(&spinner);
            status.append(&phase);
            let outcome = fact_line();
            let lines = fact_line();
            lines.add_css_class("monospace");
            let lines_scroll = gtk::ScrolledWindow::builder()
                .child(&lines)
                .max_content_height(160)
                .propagate_natural_height(true)
                .build();

            let expand = button("Expand", ids::EXPAND);
            expand.add_css_class("suggested-action");
            let s = send.clone();
            expand.connect_clicked(move |_| s(ExpandHfsAction::Start));
            let close = button("Cancel", ids::CLOSE);
            let s = send.clone();
            close.connect_clicked(move |_| s(ExpandHfsAction::Close));

            let body = crate::browser::dialogs::page();
            body.append(&source_head);
            body.append(&partition);
            body.append(usage.root());
            body.append(&facts);
            body.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
            body.append(&target_head);
            body.append(&form);
            body.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
            body.append(&status);
            body.append(&outcome);
            body.append(&lines_scroll);
            body.append(&button_row(&[&expand, &close]));
            let s = send.clone();
            let dialog = window(
                "Expand HFS Volume",
                560,
                560,
                &body,
                Rc::new(move || s(ExpandHfsAction::Close)),
            );

            let blocks: RefCell<Vec<BlockChoice>> = RefCell::default();
            ExpandHfsDialog {
                ctrl: RefCell::new(ExpandHfsController::new(notify)),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                dialog,
                partition,
                usage,
                facts,
                form,
                format,
                note,
                block,
                blocks,
                size,
                output,
                spinner,
                phase,
                outcome,
                lines,
                expand,
                close,
            }
        });
        let me = Rc::downgrade(&d);
        d.block.connect_selected_notify(move |dd| {
            let Some(d) = me.upgrade() else {
                return;
            };
            let picked = d
                .blocks
                .borrow()
                .get(dd.selected() as usize)
                .map(|c| c.bytes);
            if let (Some(bytes), false) = (picked, d.sync.active()) {
                d.apply(ExpandHfsAction::SetBlockSize(bytes));
            }
        });
        d.apply(ExpandHfsAction::Open(source));
        d.host.present(&d.dialog);
        d
    }

    pub fn apply(&self, action: ExpandHfsAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    pub fn is_open(&self) -> bool {
        self.ctrl.borrow().is_open()
    }

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(d) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => ExpandHfsAction::Picked { purpose, path },
                // Expand HFS asks only for its output file.
                Reply::Confirmed { .. }
                | Reply::Answered { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => return,
            };
            d.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let log = self.ctrl.borrow_mut().take_log();
        crate::app::append_log(log);
        let shown = self.read();
        if shown.source.is_none() {
            self.dialog.force_close();
            return;
        }
        self.sync.run(|| self.show(shown));
    }

    fn read(&self) -> Shown {
        let ctrl = self.ctrl.borrow();
        Shown {
            source: ctrl.source().cloned(),
            partition: ctrl.partition_line(),
            usage: ctrl.usage_bar(),
            editable: ctrl.editable(),
            flat: ctrl.flat_hfv(),
            blocks: ctrl.block_choices(),
            block: ctrl.block_size(),
            bounds: ctrl.size_bounds(),
            size: ctrl.size_mib(),
            output: ctrl.output().map(|p| p.display().to_string()),
            phase: ctrl.progress().map(|s| {
                if s.phase.is_empty() {
                    "Starting...".to_string()
                } else {
                    s.phase
                }
            }),
            outcome: ctrl.outcome().cloned(),
            lines: ctrl.lines(),
            running: ctrl.is_running(),
            can_start: ctrl.can_start(),
        }
    }

    fn show(&self, s: Shown) {
        self.partition.set_visible(s.partition.is_some());
        set_text(&self.partition, s.partition.as_deref().unwrap_or_default());
        self.usage.show(s.usage.clone(), None);
        if let Some(src) = &s.source {
            let block = match src.source_block_size % 1024 {
                0 => format!("{} KiB", src.source_block_size / 1024),
                _ => format!("{} bytes", src.source_block_size),
            };
            let facts = format!(
                "Volume: {}\nSize: {} MiB ({} bytes)\nAllocation block size: {block}\nUsed: {} MiB across {} file(s) in {} folder(s)",
                src.volume_name,
                src.partition_size / MIB,
                src.partition_size,
                src.used_bytes / MIB,
                src.file_count,
                src.dir_count
            );
            set_text(&self.facts, &facts);
        }
        self.form.set_sensitive(s.editable);
        set_radio(&self.format, usize::from(s.flat));
        self.note.set_visible(s.flat);
        if *self.blocks.borrow() != s.blocks {
            let labels: Vec<String> = s.blocks.iter().map(|c| c.label.clone()).collect();
            let refs: Vec<&str> = labels.iter().map(String::as_str).collect();
            *self.blocks.borrow_mut() = s.blocks.clone();
            self.block.set_model(Some(&gtk::StringList::new(&refs)));
        }
        let at = s
            .blocks
            .iter()
            .position(|c| c.bytes == s.block)
            .unwrap_or(0) as u32;
        if self.block.selected() != at {
            self.block.set_selected(at);
        }
        let (lo, hi) = (f64::from(s.bounds.0), f64::from(s.bounds.1));
        let adj = self.size.adjustment();
        if adj.lower() != lo || adj.upper() != hi {
            self.size.set_range(lo, hi);
        }
        if self.size.value() != f64::from(s.size) {
            self.size.set_value(f64::from(s.size));
        }
        set_text(&self.output, s.output.as_deref().unwrap_or("(not chosen)"));
        self.spinner.set_visible(s.phase.is_some());
        self.spinner.set_spinning(s.phase.is_some());
        set_text(&self.phase, s.phase.as_deref().unwrap_or(""));
        self.outcome.remove_css_class("error");
        self.outcome.remove_css_class("success");
        let outcome = match &s.outcome {
            Some(Ok(())) => {
                self.outcome.add_css_class("success");
                Some("Expand complete.".to_string())
            }
            Some(Err(e)) => {
                self.outcome.add_css_class("error");
                Some(format!("Failed: {e}"))
            }
            None => None,
        };
        set_line(&self.outcome, outcome.as_deref());
        set_text(&self.lines, &plain(&s.lines.join("\n")));
        self.lines.set_visible(!s.lines.is_empty());
        self.expand.set_sensitive(s.can_start);
        self.close.set_sensitive(!s.running);
        let label = if s.outcome.is_some() {
            "Close"
        } else {
            "Cancel"
        };
        if self.close.label().as_deref() != Some(label) {
            self.close.set_label(label);
        }
    }
}
