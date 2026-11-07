//! The side panel: File Info for the cursor row, its actions, and the text or hex preview.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::browser::ids;
use rusty_backup::controller::commander::{
    detect_content_type, entry_facts, extract_applies, extract_fork_modes, format_size, hex_dump,
    preview_bytes, BrowseCaps, BrowserPaneAction as P, CommanderAction as A, CommanderController,
    ExportFormat, ExtractOptions, FileContent, FileDetail, ModeAction, ProdosExportMode,
    ResourceForkMode, Side, MAX_PREVIEW_SIZE, MIN_NESTED_IMAGE_BYTES,
};
use rusty_backup::controller::commander::{
    is_known_prodos_type, ArchiveUse, PRODOS_UNKNOWN_TYPE_NOTE,
};

use super::SendFn;
use crate::widgets::{coming, flow, flow_show, plain, set_text};

/// The panel's state; `body` is left out while the entry on screen has not changed.
pub struct DetailShown {
    key: Option<(u64, bool)>,
    body: Option<Body>,
    result: Option<String>,
    actions: Actions,
}

struct Body {
    name: String,
    facts: Vec<String>,
    budget: Vec<String>,
    note: Option<String>,
    preview: Preview,
}

enum Preview {
    None,
    Text(String),
    Hex(String),
    Note(String),
}

/// Which actions the row under the cursor takes.
#[derive(Default, Clone, PartialEq)]
struct Actions {
    path: Option<String>,
    folder: bool,
    extractable: bool,
    copying: bool,
    file: bool,
    nested: bool,
    archive: bool,
    mac: bool,
    forks: bool,
    prodos: bool,
    qnx: bool,
    text: bool,
    edit_mode: bool,
    metadata: bool,
}

pub struct Detail {
    root: gtk::Paned,
    name: gtk::Label,
    facts: gtk::Label,
    note: gtk::Label,
    budget: gtk::Label,
    result: gtk::Label,
    actions: gtk::Box,
    fork_row: gtk::Box,
    fork: gtk::DropDown,
    fork_modes: Vec<ResourceForkMode>,
    prodos_row: gtk::Box,
    prodos: gtk::DropDown,
    rs_to_lf: gtk::CheckButton,
    extract: gtk::Button,
    export: gtk::Button,
    nested: gtk::Button,
    save_archive: gtk::Button,
    browse_archive: gtk::Button,
    mac_row: gtk::FlowBox,
    binhex: gtk::Button,
    edit_text: gtk::Button,
    metadata: gtk::Button,
    preview: gtk::TextView,
    empty: gtk::Label,
    key: Cell<Option<(u64, bool)>>,
    is_text: Cell<bool>,
    shown: Rc<RefCell<Actions>>,
}

/// What a panel button sends for the entry path it acts on.
type OnPath = Box<dyn Fn(&Detail, String) -> Vec<A>>;

fn action_button(label: &str, id: &str) -> gtk::Button {
    let button = gtk::Button::with_label(label);
    button.set_widget_name(id);
    button
}

fn left_label() -> gtk::Label {
    gtk::Label::builder()
        .xalign(0.0)
        .wrap(true)
        .wrap_mode(gtk::pango::WrapMode::WordChar)
        .selectable(true)
        .build()
}

impl Detail {
    pub fn build(send: &SendFn) -> Rc<Detail> {
        let shown: Rc<RefCell<Actions>> = Rc::default();
        let name = left_label();
        name.add_css_class("heading");
        let facts = left_label();
        let note = left_label();
        note.add_css_class("warning");
        let budget = left_label();
        let result = left_label();
        result.add_css_class("success");

        let fork_modes = extract_fork_modes();
        let labels: Vec<&str> = fork_modes.iter().map(|m| m.label()).collect();
        let fork = gtk::DropDown::from_strings(&labels);
        let start = fork_modes
            .iter()
            .position(|m| *m == ResourceForkMode::AppleDouble)
            .unwrap_or(0);
        fork.set_selected(start as u32);
        let fork_row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        fork_row.append(&gtk::Label::new(Some("Resource forks:")));
        fork_row.append(&fork);

        let labels: Vec<&str> = ProdosExportMode::ALL.iter().map(|m| m.label()).collect();
        let prodos = gtk::DropDown::from_strings(&labels);
        let prodos_row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        prodos_row.append(&gtk::Label::new(Some("Export name:")));
        prodos_row.append(&prodos);

        let rs_to_lf = gtk::CheckButton::with_label("Text: RS to LF");
        rs_to_lf.set_tooltip_text(Some(
            "Convert QNX line separators (RS, 0x1E) to LF in files that look like text.",
        ));

        let extract = action_button("Extract...", ids::EXTRACT);
        let export = action_button("Export to .tgz...", ids::EXPORT_ENTRY);
        export.set_tooltip_text(Some(
            "Save the selection as a single .tar.gz / .tar.zst / .tar (preserves exact case + \
             symlinks; pick the extension in the save dialog)",
        ));
        let nested = action_button("Open as Disk Image", ids::OPEN_NESTED);
        nested.set_tooltip_text(Some(
            "Copy this file to a temporary folder and inspect it as its own disk image, e.g. a \
             ShapeShifter hardfile or a floppy image stored on this volume.",
        ));
        let save_archive = action_button("Save as Mac archive...", ids::SAVE_ARCHIVE);
        let browse_archive = action_button("Browse archive...", ids::BROWSE_ARCHIVE);
        let edit_text = action_button("Edit as text...", ids::EDIT_TEXT);
        let metadata = action_button("Edit Metadata...", ids::EDIT_METADATA);
        coming(&metadata, "the metadata editor");

        let row = flow();
        for b in [
            &extract,
            &export,
            &nested,
            &save_archive,
            &browse_archive,
            &edit_text,
            &metadata,
        ] {
            row.append(b);
        }
        let mac_row = flow();
        mac_row.append(&gtk::Label::new(Some("Export Mac archive:")));
        let binhex = gtk::Button::with_label("BinHex (.hqx)");
        let stuffit = gtk::Button::with_label("StuffIt (.sit)");
        let sit_hqx = gtk::Button::with_label("StuffIt-over-BinHex (.sit.hqx)");
        let mar = gtk::Button::with_label("MAR (.mar)");
        for b in [&binhex, &stuffit, &sit_hqx, &mar] {
            mac_row.append(b);
        }
        let options = flow();
        options.append(&fork_row);
        options.append(&prodos_row);
        options.append(&rs_to_lf);
        let actions = gtk::Box::new(gtk::Orientation::Vertical, 4);
        actions.append(&options);
        actions.append(&row);
        actions.append(&mac_row);

        let top = gtk::Box::new(gtk::Orientation::Vertical, 4);
        top.set_margin_start(8);
        top.set_margin_end(8);
        top.set_margin_top(6);
        top.set_margin_bottom(6);
        for w in [&name, &facts, &note, &budget, &result] {
            top.append(w);
        }
        top.append(&actions);
        let top_scroll = gtk::ScrolledWindow::builder()
            .child(&top)
            .hscrollbar_policy(gtk::PolicyType::Never)
            .propagate_natural_height(true)
            .build();

        let preview = gtk::TextView::builder()
            .editable(false)
            .monospace(true)
            .cursor_visible(false)
            .wrap_mode(gtk::WrapMode::None)
            .build();
        let preview_scroll = gtk::ScrolledWindow::builder()
            .child(&preview)
            .vexpand(true)
            .build();
        let empty = gtk::Label::new(Some("Select a file to view its contents."));
        empty.add_css_class("dim-label");
        let bottom = gtk::Box::new(gtk::Orientation::Vertical, 0);
        bottom.append(&empty);
        bottom.append(&preview_scroll);
        let root = gtk::Paned::builder()
            .orientation(gtk::Orientation::Vertical)
            .start_child(&top_scroll)
            .end_child(&bottom)
            .resize_start_child(false)
            .shrink_start_child(false)
            .shrink_end_child(false)
            .position(230)
            .build();
        root.set_widget_name(ids::DETAIL);

        let detail = Rc::new(Detail {
            root,
            name,
            facts,
            note,
            budget,
            result,
            actions,
            fork_row,
            fork,
            fork_modes,
            prodos_row,
            prodos,
            rs_to_lf,
            extract,
            export,
            nested,
            save_archive,
            browse_archive,
            mac_row,
            binhex,
            edit_text,
            metadata,
            preview,
            empty,
            key: Cell::new(None),
            is_text: Cell::new(false),
            shown,
        });
        detail.connect(send, &stuffit, &sit_hqx, &mar);
        detail.show_actions(&Actions::default());
        detail
    }

    fn connect(
        self: &Rc<Self>,
        send: &SendFn,
        stuffit: &gtk::Button,
        sit_hqx: &gtk::Button,
        mar: &gtk::Button,
    ) {
        let weak = Rc::downgrade(self);
        let on_path = move |button: &gtk::Button, make: OnPath| {
            let (weak, send) = (weak.clone(), send.clone());
            button.connect_clicked(move |_| {
                let Some(d) = weak.upgrade() else {
                    return;
                };
                let path = d.shown.borrow().path.clone();
                for action in path.map(|p| make(&d, p)).unwrap_or_default() {
                    send(action);
                }
            });
        };
        on_path(
            &self.extract,
            Box::new(|d, path| {
                vec![A::Extract {
                    from: Side::Left,
                    path,
                    options: d.extract_options(),
                    dest: None,
                }]
            }),
        );
        let export = |format: ExportFormat| -> OnPath {
            Box::new(move |d, path| {
                vec![
                    A::SetForkMode(d.fork_mode()),
                    A::ExportEntry {
                        from: Side::Left,
                        path,
                        format: Some(format),
                    },
                ]
            })
        };
        on_path(&self.export, export(ExportFormat::TarGz));
        on_path(&self.binhex, export(ExportFormat::BinHex));
        on_path(stuffit, export(ExportFormat::StuffIt));
        on_path(sit_hqx, export(ExportFormat::StuffItHqx));
        on_path(mar, export(ExportFormat::MacArchive));
        on_path(
            &self.nested,
            Box::new(|_, path| vec![A::Mode(ModeAction::OpenNested(path))]),
        );
        let archive = |purpose: ArchiveUse| -> OnPath {
            Box::new(move |_, path| vec![A::Pane(Side::Left, P::ReadArchive { path, purpose })])
        };
        on_path(&self.save_archive, archive(ArchiveUse::Extract));
        on_path(&self.browse_archive, archive(ArchiveUse::Browse));
        on_path(
            &self.edit_text,
            Box::new(|_, _| vec![A::Pane(Side::Left, P::EditText)]),
        );
    }

    pub fn root(&self) -> &gtk::Paned {
        &self.root
    }

    fn fork_mode(&self) -> ResourceForkMode {
        let i = self.fork.selected() as usize;
        self.fork_modes
            .get(i)
            .copied()
            .unwrap_or(ResourceForkMode::AppleDouble)
    }

    /// What the panel's option controls say, for an extract from a key press as well.
    pub fn extract_options(&self) -> ExtractOptions {
        let mode = ProdosExportMode::ALL.get(self.prodos.selected() as usize);
        ExtractOptions {
            fork_mode: self.fork_mode(),
            prodos_suffix: mode == Some(&ProdosExportMode::WithTypeSuffix),
            rs_to_lf: self.rs_to_lf.is_active(),
        }
    }

    pub fn read(&self, ctl: &CommanderController, caps: BrowseCaps) -> DetailShown {
        let pane = ctl.pane(Side::Left);
        let Some(d) = ctl.side_detail(Side::Left) else {
            return DetailShown {
                key: None,
                body: None,
                result: None,
                actions: Actions::default(),
            };
        };
        let key = Some((d.serial, d.loading));
        let body = (self.key.get() != key).then(|| body_of(d, caps));
        let in_wrapper = pane.cursor_index().is_some_and(|i| pane.rows()[i].is_tree);
        let entry = &d.entry;
        let sniffed = pane.archive_sniff(&entry.path).flatten().is_some();
        let text = match &body {
            Some(b) => matches!(b.preview, Preview::Text(_)),
            None => self.is_text.get(),
        };
        let applies = extract_applies(&d.fs_type);
        let actions = Actions {
            path: (!in_wrapper).then(|| entry.path.clone()),
            folder: entry.is_directory(),
            extractable: !in_wrapper
                && (entry.is_file() || entry.is_directory() || entry.is_symlink()),
            copying: ctl.copy_job().is_some(),
            file: entry.is_file(),
            nested: entry.is_file() && entry.size >= MIN_NESTED_IMAGE_BYTES,
            archive: caps.mac && sniffed,
            mac: caps.mac,
            forks: applies.forks,
            prodos: applies.prodos_suffix,
            qnx: applies.rs_to_lf,
            text,
            edit_mode: ctl.edit_mode(),
            metadata: d.editable,
        };
        DetailShown {
            key,
            body,
            result: d.result.clone(),
            actions,
        }
    }

    pub fn show(&self, s: DetailShown) {
        self.key.set(s.key);
        self.is_text.set(s.actions.text);
        if let Some(b) = s.body {
            set_text(&self.name, &b.name);
            set_text(&self.facts, &b.facts.join("   "));
            set_text(&self.note, b.note.as_deref().unwrap_or(""));
            self.note.set_visible(b.note.is_some());
            set_text(&self.budget, &b.budget.join("\n"));
            self.budget.set_visible(!b.budget.is_empty());
            let buffer = self.preview.buffer();
            match b.preview {
                Preview::None => buffer.set_text(""),
                Preview::Text(t) | Preview::Hex(t) | Preview::Note(t) => {
                    buffer.set_text(&plain(&t))
                }
            }
        }
        let has_row = s.key.is_some();
        self.empty.set_visible(!has_row);
        if !has_row {
            set_text(&self.name, "");
            set_text(&self.facts, "");
            self.note.set_visible(false);
            self.budget.set_visible(false);
            self.preview.buffer().set_text("");
        }
        set_text(&self.result, s.result.as_deref().unwrap_or(""));
        self.result.set_visible(s.result.is_some());
        if *self.shown.borrow() != s.actions {
            self.show_actions(&s.actions);
            *self.shown.borrow_mut() = s.actions;
        }
    }

    fn show_actions(&self, a: &Actions) {
        self.actions.set_visible(a.extractable);
        flow_show(&self.fork_row, a.forks);
        flow_show(&self.prodos_row, a.prodos);
        flow_show(&self.rs_to_lf, a.qnx);
        let (extract, export) = match a.folder {
            true => ("Extract Folder...", "Export Folder to .tgz..."),
            false => ("Extract File...", "Export File to .tgz..."),
        };
        self.extract.set_label(extract);
        self.export.set_label(export);
        self.export.set_sensitive(!a.copying);
        self.extract.set_sensitive(!a.copying);
        flow_show(&self.nested, a.nested);
        flow_show(&self.save_archive, a.archive);
        flow_show(&self.browse_archive, a.archive);
        self.mac_row.set_visible(a.mac);
        self.binhex.set_sensitive(a.file);
        flow_show(&self.edit_text, a.text);
        self.edit_text.set_sensitive(a.edit_mode);
        let hint = (!a.edit_mode).then_some("Turn on Edit Mode to edit this file.");
        self.edit_text.set_tooltip_text(hint);
        flow_show(&self.metadata, a.metadata);
    }
}

fn body_of(d: &FileDetail, caps: BrowseCaps) -> Body {
    let entry = &d.entry;
    let mut facts = entry_facts(entry, false, Some(&d.id_names));
    if d.loading {
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
    let preview = if entry.is_directory() {
        Preview::None
    } else if entry.is_symlink() {
        let target = entry
            .symlink_target
            .as_deref()
            .unwrap_or("(unknown target)");
        Preview::Note(format!("Symlink target: {target}"))
    } else if entry.is_special() {
        let kind = entry.special_type.as_deref().unwrap_or("special file");
        Preview::Note(format!("{kind} -- no preview available"))
    } else if entry.size > MAX_PREVIEW_SIZE as u64 {
        Preview::Note(format!(
            "File too large to preview ({}).",
            format_size(entry.size)
        ))
    } else {
        match &d.preview {
            None => Preview::None,
            Some(data) => {
                let shown = preview_bytes(&d.fs_type, data);
                match detect_content_type(entry, &shown) {
                    FileContent::Text(t) => Preview::Text(t),
                    FileContent::Binary(bytes) => Preview::Hex(hex_dump(&bytes)),
                }
            }
        }
    };
    Body {
        name: entry.name.clone(),
        facts,
        budget: d.size_budget_lines(),
        note,
        preview,
    }
}
