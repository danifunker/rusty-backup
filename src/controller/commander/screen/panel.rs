//! What a view shows beside and above the listing: the volume line, the cursor row's side panel
//! and which of its actions apply.
//!
//! The rules came out of the GTK shell's `browser/detail.rs` and `browser/bars.rs`, so a second
//! view reads them here rather than writing them again.

use super::{BrowseCaps, CommanderController, FileDetail, Side, MAX_PREVIEW_SIZE};
use crate::controller::commander::{
    detect_content_type, entry_facts, extract_applies, hex_dump, is_known_prodos_type,
    preview_bytes, FileContent, MIN_NESTED_IMAGE_BYTES, PRODOS_UNKNOWN_TYPE_NOTE,
};
use disk_core::display::format_size;

/// The volume's line above the listing; an empty field is not shown.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VolumeLine {
    /// `[FAT16]`.
    pub fs: String,
    /// `x used / y (z free)`.
    pub usage: String,
    pub label: String,
    pub blessed: String,
}

/// What File Info shows of an entry's contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PanelPreview {
    None,
    Text(String),
    Hex(String),
    /// A line in place of the contents: a symlink's target, a file too large to read.
    Note(String),
}

impl PanelPreview {
    pub fn text(&self) -> &str {
        match self {
            PanelPreview::None => "",
            PanelPreview::Text(t) | PanelPreview::Hex(t) | PanelPreview::Note(t) => t,
        }
    }
}

/// The side panel's text for the cursor row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidePanel {
    pub name: String,
    pub facts: Vec<String>,
    /// A warning, such as a ProDOS type outside the registry.
    pub note: Option<String>,
    pub budget: Vec<String>,
    pub preview: PanelPreview,
}

/// Which of the side panel's actions the cursor row takes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PanelActions {
    /// The entry the actions act on; `None` inside a wrapper's listing.
    pub path: Option<String>,
    pub folder: bool,
    pub extractable: bool,
    pub export: bool,
    pub copying: bool,
    pub file: bool,
    pub nested: bool,
    pub archive: bool,
    pub mac: bool,
    pub forks: bool,
    pub prodos: bool,
    pub qnx: bool,
    pub text: bool,
    pub edit_mode: bool,
    pub metadata: bool,
}

impl FileDetail {
    /// The side panel's text.
    pub fn side_panel(&self, caps: BrowseCaps) -> SidePanel {
        let entry = &self.entry;
        let mut facts = entry_facts(entry, false, Some(&self.id_names));
        if self.loading {
            facts.push("Reading...".to_string());
        }
        let note = entry
            .prodos_file_type
            .filter(|t| caps.prodos && !is_known_prodos_type(*t))
            .map(|t| {
                let aux = entry.aux_type.unwrap_or(0);
                format!(
                    "Note: ${t:02X} is not in the ProDOS type registry - {PRODOS_UNKNOWN_TYPE_NOTE}. \
                     The type and aux (${aux:04X}) will be preserved on export via the CiderPress \
                     #{t:02X}{aux:04X} filename suffix."
                )
            });
        SidePanel {
            name: entry.name.clone(),
            facts,
            budget: self.size_budget_lines(),
            note,
            preview: self.panel_preview(),
        }
    }

    /// The contents File Info shows, for the side panel and the window alike.
    pub fn panel_preview(&self) -> PanelPreview {
        let entry = &self.entry;
        if entry.is_directory() {
            PanelPreview::None
        } else if entry.is_symlink() {
            let target = entry
                .symlink_target
                .as_deref()
                .unwrap_or("(unknown target)");
            PanelPreview::Note(format!("Symlink target: {target}"))
        } else if entry.is_special() {
            let kind = entry.special_type.as_deref().unwrap_or("special file");
            PanelPreview::Note(format!("{kind} -- no preview available"))
        } else if entry.size > MAX_PREVIEW_SIZE as u64 {
            PanelPreview::Note(format!(
                "File too large to preview ({}).",
                format_size(entry.size)
            ))
        } else {
            match &self.preview {
                None => PanelPreview::None,
                Some(data) => {
                    let shown = preview_bytes(&self.fs_type, data);
                    match detect_content_type(entry, &shown) {
                        FileContent::Text(t) => PanelPreview::Text(t),
                        FileContent::Binary(bytes) => PanelPreview::Hex(hex_dump(&bytes)),
                    }
                }
            }
        }
    }
}

impl FileDetail {
    /// The preview reads as text, without building the hex dump a binary one would show.
    fn previews_text(&self) -> bool {
        let e = &self.entry;
        let shown = match &self.preview {
            Some(data) if e.is_file() && e.size <= MAX_PREVIEW_SIZE as u64 => data,
            _ => return false,
        };
        let shown = preview_bytes(&self.fs_type, shown);
        matches!(detect_content_type(e, &shown), FileContent::Text(_))
    }
}

impl CommanderController {
    /// The volume's line; `prefix` is what the host put before the label (Inspect's partition).
    pub fn volume_line(&self, side: Side, prefix: &str) -> VolumeLine {
        let v = self.pane(side).volume();
        let usage = match v.total_size > 0 {
            true => format!(
                "{} used / {} ({} free)",
                format_size(v.used_size),
                format_size(v.total_size),
                format_size(v.total_size.saturating_sub(v.used_size))
            ),
            false => String::new(),
        };
        let label = match (prefix.is_empty(), v.label.is_empty()) {
            (false, false) => format!("Label: {prefix} - {}", v.label),
            (false, true) => format!("Label: {prefix}"),
            (true, false) => format!("Label: {}", v.label),
            (true, true) => String::new(),
        };
        VolumeLine {
            fs: match v.fs_type.is_empty() {
                true => String::new(),
                false => format!("[{}]", v.fs_type),
            },
            usage,
            label,
            blessed: v
                .blessed
                .as_ref()
                .map(|(_, name)| format!("Blessed: {name}"))
                .unwrap_or_default(),
        }
    }

    /// The cursor row's actions; `plain` is a browser that offers only Extract (a disc's).
    pub fn panel_actions(&self, side: Side, plain: bool) -> Option<PanelActions> {
        let d = self.side_detail(side)?;
        let pane = self.pane(side);
        let caps = self.browse_caps();
        let in_wrapper = pane.cursor_index().is_some_and(|i| pane.rows()[i].is_tree);
        let entry = &d.entry;
        let sniffed = pane.archive_sniff(&entry.path).flatten().is_some();
        let applies = extract_applies(&d.fs_type);
        let extras = !plain;
        let text = extras && d.previews_text();
        Some(PanelActions {
            path: (!in_wrapper).then(|| entry.path.clone()),
            folder: entry.is_directory(),
            extractable: !in_wrapper
                && (entry.is_file() || entry.is_directory() || entry.is_symlink()),
            export: extras,
            copying: self.copy_job().is_some(),
            file: entry.is_file(),
            nested: extras && entry.is_file() && entry.size >= MIN_NESTED_IMAGE_BYTES,
            archive: extras && caps.mac && sniffed,
            mac: extras && caps.mac,
            forks: applies.forks,
            prodos: applies.prodos_suffix,
            qnx: applies.rs_to_lf,
            text,
            edit_mode: self.edit_mode(),
            metadata: extras && d.editable,
        })
    }
}
