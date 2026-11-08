//! The facts above the partition list: backup or Clonezilla metadata, the table and its alignment.

use adw::prelude::*;
use gtk::glib;
use rusty_backup::controller::inspect::{
    format_size, format_size_exact, hints, ids, AlignmentSummary, BackupSummary, ClonezillaSummary,
    InspectAction, TableSummary,
};

use super::ActionFn;
use crate::widgets::{fact, set_markup, set_text};

pub struct Details {
    root: gtk::Box,
    facts: gtk::Label,
    word_row: gtk::Box,
    word_order: gtk::Label,
    swap: gtk::Button,
    swapping: gtk::Spinner,
    alignment: gtk::Label,
}

fn markup_label() -> gtk::Label {
    gtk::Label::builder()
        .xalign(0.0)
        .use_markup(true)
        .wrap(true)
        .selectable(true)
        .build()
}

fn backup_lines(m: &BackupSummary) -> Vec<String> {
    let mut mode = format!(
        "{}   {}",
        fact("Compression:", &m.compression),
        fact("Checksum:", &m.checksum)
    );
    if m.sector_by_sector {
        mode.push_str(" (sector-by-sector)");
    }
    let size = if m.min_restore_bytes > 0 {
        Some(fact(
            "Minimum Size:",
            &format_size_exact(m.min_restore_bytes),
        ))
    } else if m.source_size_bytes > 0 {
        Some(fact("Source Size:", &format_size(m.source_size_bytes)))
    } else {
        None
    };
    let alignment = fact("Alignment:", &m.alignment);
    vec![
        fact("Backup Info:", &format!("Source: {}", m.source)),
        format!(
            "{}   {}",
            fact("Created:", &m.created),
            fact("Table:", &m.table)
        ),
        mode,
        match size {
            Some(size) => format!("{size}   {alignment}"),
            None => alignment,
        },
    ]
}

fn clonezilla_lines(c: &ClonezillaSummary) -> Vec<String> {
    let mut size = fact("Source Size:", &format_size_exact(c.source_size_bytes));
    if let Some((cyl, heads, sectors)) = c.chs {
        let chs = format!("  CHS: {cyl} / {heads} / {sectors}");
        size.push_str(&glib::markup_escape_text(&chs));
    }
    let mut lines = vec![
        fact("Clonezilla Image:", &format!("Disk: {}", c.disk_name)),
        size,
    ];
    lines.extend(c.created.iter().map(|d| fact("Created:", d)));
    lines
}

fn table_line(t: &TableSummary) -> String {
    let name = if t.superfloppy {
        "None (superfloppy)".to_string()
    } else {
        format!(
            "{} (disk signature: 0x{:08X})",
            t.type_name, t.disk_signature
        )
    };
    fact("Partition Table:", &name)
}

fn alignment_lines(a: &AlignmentSummary) -> String {
    let mut lba = fact("First LBA:", &a.first_lba.to_string());
    if a.heads > 0 {
        let chs = format!(
            "  CHS Geometry: {} heads x {} sectors/track",
            a.heads, a.sectors_per_track
        );
        lba.push_str(&glib::markup_escape_text(&chs));
    }
    format!("{}\n{lba}", fact("Alignment:", &a.kind))
}

impl Details {
    pub fn build(send: &ActionFn) -> Details {
        let facts = markup_label();
        let word_order = markup_label();
        let swap = gtk::Button::with_label("Swap Word Order...");
        swap.set_widget_name(ids::SWAP_WORD_ORDER);
        swap.set_tooltip_text(Some(hints::SWAP_WORD_ORDER));
        let send = send.clone();
        swap.connect_clicked(move |_| send(InspectAction::SwapWordOrder));
        let swapping = gtk::Spinner::new();
        let word_row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        word_row.append(&word_order);
        word_row.append(&swapping);
        word_row.append(&swap);
        let alignment = markup_label();
        let root = gtk::Box::new(gtk::Orientation::Vertical, 4);
        root.append(&facts);
        root.append(&word_row);
        root.append(&alignment);
        Details {
            root,
            facts,
            word_row,
            word_order,
            swap,
            swapping,
            alignment,
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn show(
        &self,
        backup: Option<&BackupSummary>,
        clonezilla: Option<&ClonezillaSummary>,
        table: Option<&TableSummary>,
        alignment: Option<&AlignmentSummary>,
        swabbing: bool,
    ) {
        let mut lines = Vec::new();
        lines.extend(backup.map(backup_lines).unwrap_or_default());
        lines.extend(clonezilla.map(clonezilla_lines).unwrap_or_default());
        lines.extend(table.map(table_line));
        set_markup(&self.facts, &lines.join("\n"));
        self.facts.set_visible(!lines.is_empty());

        let order = table.and_then(|t| t.byte_order);
        self.word_row.set_visible(order.is_some());
        if let Some(order) = order {
            let text = format!("{order} (16-bit words)");
            set_markup(&self.word_order, &fact("Word order:", &text));
        }
        self.swap.set_visible(!swabbing);
        self.swapping.set_visible(swabbing);
        self.swapping.set_spinning(swabbing);

        let aligned = alignment.filter(|_| table.is_some());
        self.alignment.set_visible(aligned.is_some());
        if let Some(a) = aligned {
            set_markup(&self.alignment, &alignment_lines(a));
        }
    }
}

/// Keep a plain warning label in step with `text`, hiding it when there is nothing to say.
pub fn show_note(label: &gtk::Label, text: Option<&str>) {
    label.set_visible(text.is_some());
    if let Some(text) = text {
        set_text(label, text);
    }
}
