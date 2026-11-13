//! What a view shows beside and above the listing: the volume line and header tools, the edit
//! and mark bars, the cursor row's side panel and which of its actions apply.
//!
//! The rules came out of the GTK shell's `browser/detail.rs` and `browser/bars.rs`, so a second
//! view reads them here rather than writing them again.

use super::windows::Tone;
use super::{
    export_bar_choices, BrowseCaps, CommanderController, ExportChoice, ExportFormat, FileDetail,
    ResourceForkMode, Side, MAX_PREVIEW_SIZE,
};
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
    /// File Info as one block of text: the facts, extended attributes and the preview.
    pub fn info_text(&self, caps: BrowseCaps) -> String {
        let p = self.side_panel(caps);
        let mut lines = vec![p.name.clone(), String::new()];
        lines.extend(p.facts);
        lines.extend(p.note);
        lines.extend(p.budget);
        if !self.xattrs.is_empty() {
            lines.push(String::new());
            lines.push("Extended attributes:".into());
            lines.extend(
                self.xattrs
                    .iter()
                    .map(|x| format!("  {} = {}", x.name, x.value_display())),
            );
        }
        let preview = p.preview.text();
        if !preview.is_empty() {
            lines.push(String::new());
            lines.push(preview.to_string());
        }
        lines.join("\n")
    }

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

/// The header's tools beyond the volume line: which apply and what their labels say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderTools {
    pub caps: BrowseCaps,
    pub loaded: bool,
    pub edit_mode: bool,
    /// "Edit Mode" or "Edit Mode ON".
    pub edit_label: &'static str,
    pub edit_enabled: bool,
    pub export_volume: bool,
    pub check: bool,
    pub chd: bool,
    /// HFS in Edit Mode: whether the volume's first sector has boot blocks.
    pub boot: Option<bool>,
    pub show_deleted: bool,
    pub full_scan: bool,
    pub full_scan_hint: String,
}

/// The edit bar's state while Edit Mode is on.
#[derive(Debug, Clone, PartialEq)]
pub struct EditBar {
    pub on: bool,
    /// "Editing: /FOLDER".
    pub target: String,
    /// Why the edits land in a copy (an archive, a CHD's diff) and what Apply does then.
    pub copy_note: Option<&'static str>,
    pub caps: BrowseCaps,
    /// Rename, Delete and the ProDOS lock act on the cursor or the marked rows.
    pub deletable: bool,
    /// Bless acts on a folder under the cursor.
    pub folder: bool,
    pub staged: usize,
    pub apply_label: String,
    pub edits_label: String,
    pub apply_enabled: bool,
    pub apply_hint: Option<&'static str>,
    pub discard_enabled: bool,
    pub edits_shown: bool,
    /// "Free: 1.2 MiB".
    pub free: String,
    /// "After: 900 KiB", in red when the edits do not fit and amber when space runs short.
    pub after: Option<(String, Tone)>,
}

/// The browse view's one export bar: a format list and a button for the marked rows or else
/// the whole volume.
#[derive(Debug, Clone, PartialEq)]
pub struct ExportBar {
    pub choices: Vec<ExportChoice>,
    pub pick: usize,
    /// "Export Volume..." or "Export 3 Selected...".
    pub button: String,
    pub enabled: bool,
}

/// The bar shown while rows are marked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkBar {
    pub count: usize,
    /// "3 selected".
    pub label: String,
    /// The export format, an index into `ExportFormat::ALL`.
    pub format: usize,
}

impl CommanderController {
    pub fn header_tools(&self, side: Side) -> HeaderTools {
        let pane = self.pane(side);
        let caps = self.browse_caps();
        let edit = self.edit_mode();
        HeaderTools {
            caps,
            loaded: pane.listing.is_loaded(),
            edit_mode: edit,
            edit_label: if edit { "Edit Mode ON" } else { "Edit Mode" },
            edit_enabled: self.copy_job().is_none() && self.mode_working().is_none(),
            export_volume: !edit && self.copy_job().is_none(),
            check: caps.check && pane.tool_working().is_none(),
            chd: self.is_chd_source(),
            boot: pane.boot_blocks_present().filter(|_| edit && caps.hfs),
            show_deleted: self.show_deleted(),
            full_scan: self.carve_full_scan(),
            full_scan_hint: format!(
                "Off: scan only the first {} MB for recoverable text (fast). On: scan the entire \
                 image (slower on large disks). 'whole-disk.img' is always the full image.",
                self.carve_scan_limit_mb()
            ),
        }
    }

    pub fn edit_bar(&self, side: Side) -> EditBar {
        let pane = self.pane(side);
        let target = match pane.cwd_entry().filter(|e| e.path != "/") {
            Some(e) => format!("Editing: /{}", e.name),
            None => "Editing: /root".to_string(),
        };
        let cursor = pane.cursor_index().map(|i| &pane.rows()[i]);
        let copy_note = if self.editing_archive() {
            Some("Editing temporary copy. Click 'Apply Edits' to write changes.")
        } else if self.editing_chd() {
            Some("Editing CHD via diff. Click 'Apply Edits' to flush, or exit Edit Mode to merge into the CHD.")
        } else {
            None
        };
        let staged = pane.queue.len();
        let reading = self.copy_job().is_some();
        let busy = pane.is_busy();
        let space = self.space();
        EditBar {
            on: self.edit_mode(),
            target,
            copy_note,
            caps: self.browse_caps(),
            deletable: pane.has_selection() || cursor.is_some_and(|r| !r.is_parent()),
            folder: cursor.is_some_and(|r| r.is_dir && !r.is_parent()),
            staged,
            apply_label: format!("Apply Edits ({staged})"),
            edits_label: format!("Edits ({staged})"),
            apply_enabled: !reading && !busy,
            apply_hint: reading.then_some("Wait for the running extraction or export to finish"),
            discard_enabled: !busy,
            edits_shown: pane.edits_shown(),
            free: space
                .map(|s| format!("Free: {}", format_size(s.free)))
                .unwrap_or_default(),
            after: space.and_then(|s| {
                let tone = if s.overflows {
                    Tone::Error
                } else if s.tight {
                    Tone::Warning
                } else {
                    Tone::Success
                };
                s.after
                    .map(|a| (format!("After: {}", format_size(a)), tone))
            }),
        }
    }

    pub fn export_bar(&self, side: Side) -> ExportBar {
        let pane = self.pane(side);
        let choices = export_bar_choices(&pane.volume().fs_type);
        let (format, fork) = (self.export_format_for(side), self.export_fork_mode());
        let exact = |c: &ExportChoice| c.format == format && c.fork.unwrap_or(fork) == fork;
        let pick = choices.iter().position(exact);
        let pick = pick.or_else(|| choices.iter().position(|c| c.format == format));
        let marked = self.mark_bar(side).count;
        ExportBar {
            pick: pick.unwrap_or(0),
            choices,
            button: match marked {
                0 => "Export Volume...".to_string(),
                n => format!("Export {n} Selected..."),
            },
            enabled: pane.listing.is_loaded() && !self.edit_mode() && self.copy_job().is_none(),
        }
    }

    pub fn mark_bar(&self, side: Side) -> MarkBar {
        let pane = self.pane(side);
        let count = match pane.has_selection() {
            true => pane.selected_entries().len(),
            false => 0,
        };
        let format = self.export_format();
        MarkBar {
            count,
            label: format!("{count} selected"),
            format: ExportFormat::ALL
                .iter()
                .position(|f| *f == format)
                .unwrap_or(0),
        }
    }
}

/// One Commander side's bars: its source, volume and path rows.
#[derive(Debug, Clone, PartialEq)]
pub struct SideBars {
    /// The source button's text, and the recent sources its menu lists.
    pub source: String,
    pub recent: Vec<String>,
    /// A remote image is open: whether it can close (nothing staged).
    pub close_image: Option<bool>,
    pub can_close: bool,
    /// The pane stages edits for Apply, and how many it holds.
    pub stages: bool,
    pub staged: usize,
    pub apply_label: String,
    pub edits_label: String,
    pub apply_enabled: bool,
    pub edits_open: bool,
    pub loaded: bool,
    /// The partitions of an image, the one open, and what to say when there are none.
    pub parts: Vec<String>,
    pub part: Option<usize>,
    pub no_parts: &'static str,
    pub new_folder: bool,
    pub title: String,
    pub free: Option<String>,
    pub read_only: bool,
    /// The path: a prefix, then crumbs; a crumb with a target goes there when clicked.
    pub prefix: String,
    pub crumbs: Vec<(String, Option<String>)>,
    /// The source is locked: `Some(true)` for an APFS volume, which takes a passphrase.
    pub locked: Option<bool>,
    pub active: bool,
}

/// The column between Commander's panes: copy each way, delete, and the export settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyColumn {
    pub copy_left: bool,
    pub copy_right: bool,
    pub copy_left_hint: String,
    pub copy_right_hint: String,
    pub delete: bool,
    pub delete_hint: String,
    pub keep_dates: bool,
    /// Indexes into `ExportFormat::ALL` and `ResourceForkMode::ALL`.
    pub format: usize,
    pub fork: usize,
    /// The fork mode applies: the export writes loose files.
    pub loose: bool,
}

impl CommanderController {
    pub fn side_bars(&self, side: Side) -> SideBars {
        let pane = self.pane(side);
        let v = pane.volume();
        let host = pane.listing.is_host();
        let loaded = pane.listing.is_loaded();
        let staged = pane.queue.len();
        SideBars {
            source: pane.source_label(),
            recent: pane.recent().to_vec(),
            close_image: pane.is_remote_image().then(|| pane.queue.is_empty()),
            can_close: pane.source_path().is_some() || loaded,
            stages: pane.stages_edits(),
            staged,
            apply_label: format!("Apply ({staged})"),
            edits_label: format!("Edits ({staged})"),
            apply_enabled: staged > 0 && !pane.is_busy(),
            edits_open: pane.edits_shown(),
            loaded,
            parts: match host {
                true => Vec::new(),
                false => pane.partition_labels(),
            },
            part: pane.selected_partition(),
            no_parts: pane.no_partitions_label(),
            new_folder: pane.can_new_folder(),
            title: pane.volume_title(),
            free: (!host).then(|| {
                let free = v.total_size.saturating_sub(v.used_size);
                format!("free: {}", format_size(free))
            }),
            read_only: !host && pane.is_read_only(),
            prefix: pane.path_prefix(),
            crumbs: pane.path_crumbs(),
            locked: pane.needs_password().then(|| v.fs_type == "APFS"),
            active: self.active() == side,
        }
    }

    pub fn copy_column(&self) -> CopyColumn {
        let format = self.export_format();
        let mode = self.export_fork_mode();
        CopyColumn {
            copy_left: self.can_copy(Side::Left),
            copy_right: self.can_copy(Side::Right),
            copy_left_hint: self.copy_hint(Side::Right),
            copy_right_hint: self.copy_hint(Side::Left),
            delete: self.can_delete(),
            delete_hint: self.delete_hint(),
            keep_dates: self.keep_dates(),
            format: ExportFormat::ALL
                .iter()
                .position(|f| *f == format)
                .unwrap_or(0),
            fork: ResourceForkMode::ALL
                .iter()
                .position(|m| *m == mode)
                .unwrap_or(0),
            loose: format == ExportFormat::LooseFiles,
        }
    }
}
