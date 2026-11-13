//! The bars around the listing: the volume header, the edit toolbar and the marked-rows bar.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::browser::ids;
use rusty_backup::controller::commander::{
    default_volume_export, format_size, volume_export_formats, BrowseCaps, BrowserPaneAction as P,
    CommanderAction as A, CommanderController, ExportFormat, ModeAction, Side, SpaceReadout,
    ViewItemKind, ViewMenuItem,
};

use super::SendFn;
use crate::widgets::{flow, flow_show, plain, set_text};

/// Opens the file picker and hands the chosen paths to the callback.
pub type AddFiles = Rc<dyn Fn(Rc<dyn Fn(Vec<PathBuf>)>)>;

fn pane(action: P) -> A {
    A::Pane(Side::Left, action)
}

fn button(label: &str, id: &str, send: &SendFn, action: A) -> gtk::Button {
    let b = gtk::Button::with_label(label);
    b.set_widget_name(id);
    let send = send.clone();
    b.connect_clicked(move |_| send(action.clone()));
    b
}

/// What the header shows.
#[derive(Clone, PartialEq)]
pub struct HeaderShown {
    loaded: bool,
    fs: String,
    usage: String,
    label: String,
    blessed: String,
    boot: Option<bool>,
    caps: BrowseCaps,
    edit_mode: bool,
    edit_busy: bool,
    export_volume: bool,
    /// The volume's filesystem, which picks the export formats on offer and the first one.
    fs_type: String,
    tool_free: bool,
    chd: bool,
    deleted: bool,
    full_scan: bool,
    scan_mb: u64,
}

pub struct Header {
    root: gtk::Box,
    fs: gtk::Label,
    usage: gtk::Label,
    label: gtk::Label,
    blessed: gtk::Label,
    boot: gtk::Label,
    edit: gtk::Button,
    export: gtk::Button,
    /// Export Volume's format; the list follows the filesystem it was filled for.
    volume_format: gtk::DropDown,
    volume_formats: Rc<RefCell<(String, Vec<ExportFormat>)>>,
    extract: gtk::Button,
    check: gtk::Button,
    journal: gtk::Button,
    chd: gtk::Button,
    tree: gtk::Button,
    view: gtk::MenuButton,
    deleted: gtk::CheckButton,
    full_scan: gtk::CheckButton,
    shown: RefCell<Option<HeaderShown>>,
}

impl Header {
    /// `items` reads the View menu; `extract_all` starts an extract of the whole volume.
    pub fn build(
        send: &SendFn,
        items: Rc<dyn Fn() -> Vec<ViewMenuItem>>,
        extract_all: Rc<dyn Fn()>,
    ) -> Header {
        let title = gtk::Label::new(Some("Filesystem Browser"));
        title.add_css_class("heading");
        let (fs, usage, label, blessed, boot) = (
            gtk::Label::new(None),
            gtk::Label::new(None),
            gtk::Label::new(None),
            gtk::Label::new(None),
            gtk::Label::new(None),
        );
        usage.set_tooltip_text(Some("Volume space: used / total (free)"));
        let edit = button(
            "Edit Mode",
            ids::EDIT_MODE,
            send,
            A::Mode(ModeAction::ToggleEditMode),
        );
        let export = gtk::Button::with_label("Export Volume...");
        export.set_widget_name(ids::EXPORT_VOLUME);
        export.set_tooltip_text(Some(
            "Archive the whole volume in the format beside this button",
        ));
        let volume_format = gtk::DropDown::from_strings(&[]);
        volume_format.set_widget_name(ids::EXPORT_VOLUME_FORMAT);
        let volume_formats: Rc<RefCell<(String, Vec<ExportFormat>)>> = Rc::default();
        let to_archive = send.clone();
        let (picked, formats) = (volume_format.clone(), volume_formats.clone());
        export.connect_clicked(move |_| {
            let format = formats
                .borrow()
                .1
                .get(picked.selected() as usize)
                .copied()
                .unwrap_or(ExportFormat::Zip);
            to_archive(A::ExportEntry {
                from: Side::Left,
                path: "/".to_string(),
                format: Some(format),
            });
        });
        let extract = gtk::Button::with_label("Extract All...");
        extract.set_widget_name(ids::EXTRACT_ALL);
        extract.connect_clicked(move |_| extract_all());
        let check = button("Check", ids::CHECK, send, pane(P::Fsck));
        let journal = button("Journal", ids::JOURNAL, send, pane(P::ShowJournal));
        let chd = button(
            "CHD Info",
            ids::CHD_INFO,
            send,
            A::Mode(ModeAction::ChdInfo),
        );
        let tree = button("Tree", ids::TREE, send, pane(P::ListTree { ids: false }));
        let view = gtk::MenuButton::builder().label("View").build();
        view.set_widget_name(ids::VIEW_MENU);
        let popup_send = send.clone();
        view.set_create_popup_func(move |b| {
            b.set_popover(Some(&view_popover(&items(), &popup_send)));
        });
        let close = button("Close", ids::CLOSE, send, A::Mode(ModeAction::RequestClose));
        let deleted = gtk::CheckButton::with_label("Show deleted");
        deleted.set_widget_name(ids::SHOW_DELETED);
        deleted.set_tooltip_text(Some(
            "List deleted entries whose data can still be read, named 'NAME [deleted]'.",
        ));
        let to_mode = send.clone();
        deleted.connect_toggled(move |b| to_mode(A::Mode(ModeAction::ShowDeleted(b.is_active()))));
        let full_scan = gtk::CheckButton::with_label("Full scan");
        full_scan.set_widget_name(ids::FULL_SCAN);
        let to_mode = send.clone();
        full_scan
            .connect_toggled(move |b| to_mode(A::Mode(ModeAction::CarveFullScan(b.is_active()))));

        let info = flow();
        info.set_column_spacing(12);
        info.append(&title);
        for w in [&fs, &usage, &label, &blessed, &boot] {
            info.append(w);
        }
        let tools = gtk::Box::new(gtk::Orientation::Horizontal, 4);
        tools.append(&full_scan);
        tools.append(&deleted);
        for b in [&edit, &export] {
            tools.append(b);
        }
        tools.append(&volume_format);
        for b in [&extract, &check, &journal, &chd, &tree] {
            tools.append(b);
        }
        tools.append(&view);
        tools.append(&close);
        let tools = gtk::ScrolledWindow::builder()
            .child(&tools)
            .vscrollbar_policy(gtk::PolicyType::Never)
            .propagate_natural_height(true)
            .build();
        let root = gtk::Box::new(gtk::Orientation::Vertical, 4);
        root.set_margin_start(8);
        root.set_margin_end(8);
        root.set_margin_top(6);
        root.append(&info);
        root.append(&tools);
        Header {
            root,
            fs,
            usage,
            label,
            blessed,
            boot,
            edit,
            export,
            volume_format,
            volume_formats,
            extract,
            check,
            journal,
            chd,
            tree,
            view,
            deleted,
            full_scan,
            shown: RefCell::default(),
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    /// The Export Volume key: what a click on the button does.
    pub fn export_volume(&self) {
        self.export.emit_clicked();
    }

    pub fn read(ctl: &CommanderController, prefix: &str) -> HeaderShown {
        let pane = ctl.pane(Side::Left);
        let v = pane.volume();
        let usage = (v.total_size > 0).then(|| {
            let free = v.total_size.saturating_sub(v.used_size);
            format!(
                "{} used / {} ({} free)",
                format_size(v.used_size),
                format_size(v.total_size),
                format_size(free)
            )
        });
        let label = match (prefix.is_empty(), v.label.is_empty()) {
            (false, false) => format!("Label: {prefix} - {}", v.label),
            (false, true) => format!("Label: {prefix}"),
            (true, false) => format!("Label: {}", v.label),
            (true, true) => String::new(),
        };
        let caps = ctl.browse_caps();
        HeaderShown {
            loaded: pane.listing.is_loaded(),
            fs: match v.fs_type.is_empty() {
                true => String::new(),
                false => format!("[{}]", v.fs_type),
            },
            usage: usage.unwrap_or_default(),
            label,
            blessed: v
                .blessed
                .as_ref()
                .map(|(_, name)| format!("Blessed: {name}"))
                .unwrap_or_default(),
            boot: pane
                .boot_blocks_present()
                .filter(|_| ctl.edit_mode() && caps.hfs),
            caps,
            edit_mode: ctl.edit_mode(),
            edit_busy: ctl.copy_job().is_some() || ctl.mode_working().is_some(),
            export_volume: !ctl.edit_mode() && ctl.copy_job().is_none(),
            fs_type: v.fs_type.clone(),
            tool_free: pane.tool_working().is_none(),
            chd: ctl.is_chd_source(),
            deleted: ctl.show_deleted(),
            full_scan: ctl.carve_full_scan(),
            scan_mb: ctl.carve_scan_limit_mb(),
        }
    }

    pub fn show(&self, s: HeaderShown, menu_up: bool) {
        if menu_up && !self.view.is_active() {
            self.view.popup();
        }
        if self.shown.borrow().as_ref() == Some(&s) {
            return;
        }
        set_text(&self.fs, &s.fs);
        set_text(&self.usage, &s.usage);
        set_text(&self.label, &s.label);
        flow_show(&self.label, !s.label.is_empty());
        set_text(&self.blessed, &s.blessed);
        flow_show(&self.blessed, !s.blessed.is_empty());
        flow_show(&self.boot, s.boot.is_some());
        match s.boot {
            Some(true) => {
                self.boot.remove_css_class("warning");
                set_text(&self.boot, "Boot blocks: present");
                self.boot
                    .set_tooltip_text(Some("The volume's first sector has the 'LK' boot loader."));
            }
            Some(false) => {
                self.boot.add_css_class("warning");
                set_text(&self.boot, "Boot blocks: absent");
                self.boot.set_tooltip_text(Some(
                    "This volume has no boot loader. Use 'Boot Blocks...' to copy them from a \
                     bootable donor disk, then bless the System Folder.",
                ));
            }
            None => {}
        }
        self.full_scan.set_visible(s.caps.carve);
        self.full_scan.set_active(s.full_scan);
        self.full_scan.set_tooltip_text(Some(&format!(
            "Off: scan only the first {} MB for recoverable text (fast).\n\
             On: scan the entire image (slower on large disks).\n\
             'whole-disk.img' is always the full image regardless.",
            s.scan_mb
        )));
        self.deleted.set_visible(s.caps.qnx2);
        self.deleted.set_active(s.deleted);
        self.edit.set_visible(s.caps.edit_toggle);
        self.edit.set_sensitive(!s.edit_busy);
        self.edit.set_label(match s.edit_mode {
            true => "Edit Mode ON",
            false => "Edit Mode",
        });
        let hint = (!s.edit_mode).then_some("Enable editing to add or delete files on this image");
        self.edit.set_tooltip_text(hint);
        self.export.set_visible(s.export_volume && s.loaded);
        self.volume_format.set_visible(s.export_volume && s.loaded);
        if self.volume_formats.borrow().0 != s.fs_type {
            // A new volume starts on its filesystem's own format; the same one keeps the pick.
            let formats = volume_export_formats(&s.fs_type);
            let first = default_volume_export(&s.fs_type);
            let labels: Vec<&str> = formats.iter().map(|f| f.label()).collect();
            self.volume_format
                .set_model(Some(&gtk::StringList::new(&labels)));
            let at = formats.iter().position(|f| *f == first).unwrap_or(0);
            self.volume_format.set_selected(at as u32);
            *self.volume_formats.borrow_mut() = (s.fs_type.clone(), formats);
        }
        self.extract.set_visible(s.export_volume && s.loaded);
        self.tree.set_visible(s.loaded);
        self.check.set_visible(s.caps.check && s.tool_free);
        self.journal.set_visible(s.caps.journal);
        self.chd.set_visible(s.chd);
        *self.shown.borrow_mut() = Some(s);
    }
}

/// What the disc browser's header shows.
#[derive(Clone, PartialEq)]
pub struct DiscShown {
    labels: Vec<String>,
    selected: u32,
    game: Option<String>,
    loaded: bool,
}

/// The disc browser's header: the filesystem picker (hybrid sides, boot areas), the game, Tree.
pub struct DiscHeader {
    root: gtk::Box,
    pick_title: gtk::Label,
    picker: gtk::DropDown,
    only: gtk::Label,
    game: gtk::Label,
    tree: gtk::Button,
    view: gtk::MenuButton,
    shown: RefCell<Option<DiscShown>>,
}

impl DiscHeader {
    pub fn build(send: &SendFn, items: Rc<dyn Fn() -> Vec<ViewMenuItem>>) -> DiscHeader {
        let title = gtk::Label::new(Some("Disc Browser"));
        title.add_css_class("heading");
        let pick_title = gtk::Label::new(Some("Filesystem:"));
        let picker = gtk::DropDown::from_strings(&[]);
        picker.set_widget_name(ids::DISC_FS);
        let s = send.clone();
        picker.connect_selected_notify(move |d| {
            s(pane(P::SelectPartition(d.selected() as usize)));
        });
        let only = gtk::Label::new(None);
        let game = gtk::Label::new(None);
        let tree = button("Tree", ids::TREE, send, pane(P::ListTree { ids: false }));
        let view = gtk::MenuButton::builder().label("View").build();
        view.set_widget_name(ids::VIEW_MENU);
        let popup_send = send.clone();
        view.set_create_popup_func(move |b| {
            b.set_popover(Some(&view_popover(&items(), &popup_send)));
        });
        let close = button("Close", ids::CLOSE, send, A::Mode(ModeAction::RequestClose));
        let root = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        root.set_margin_top(6);
        root.set_margin_start(8);
        root.set_margin_end(8);
        for widget in [
            title.upcast_ref::<gtk::Widget>(),
            pick_title.upcast_ref(),
            picker.upcast_ref(),
            only.upcast_ref(),
            game.upcast_ref(),
            tree.upcast_ref(),
            view.upcast_ref(),
            close.upcast_ref(),
        ] {
            root.append(widget);
        }
        DiscHeader {
            root,
            pick_title,
            picker,
            only,
            game,
            tree,
            view,
            shown: RefCell::default(),
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn read(ctl: &CommanderController) -> DiscShown {
        let pane = ctl.pane(Side::Left);
        DiscShown {
            labels: pane
                .partition_label_parts()
                .iter()
                .map(|p| p.joined())
                .collect(),
            selected: pane.selected_partition().unwrap_or(0) as u32,
            game: pane.disc_game().map(|g| format!("Game: {g}")),
            loaded: pane.listing.is_loaded(),
        }
    }

    pub fn show(&self, s: DiscShown, menu_up: bool) {
        if menu_up && !self.view.is_active() {
            self.view.popup();
        }
        if self.shown.borrow().as_ref() == Some(&s) {
            return;
        }
        let several = s.labels.len() > 1;
        self.pick_title.set_visible(several);
        self.picker.set_visible(several);
        let plain: Vec<String> = s.labels.iter().map(|l| plain(l).into_owned()).collect();
        let refs: Vec<&str> = plain.iter().map(String::as_str).collect();
        self.picker.set_model(Some(&gtk::StringList::new(&refs)));
        self.picker.set_selected(s.selected);
        self.only.set_visible(s.labels.len() == 1);
        let only = s.labels.first().map(|l| format!("[{l}]"));
        set_text(&self.only, only.as_deref().unwrap_or(""));
        self.game.set_visible(s.game.is_some());
        set_text(&self.game, s.game.as_deref().unwrap_or(""));
        self.tree.set_visible(s.loaded);
        *self.shown.borrow_mut() = Some(s);
    }
}

/// The View menu, one control per entry; the controller says what each does.
pub fn view_popover(items: &[ViewMenuItem], send: &SendFn) -> gtk::Popover {
    let popover = gtk::Popover::new();
    let list = gtk::Box::new(gtk::Orientation::Vertical, 2);
    let mut group: Option<gtk::CheckButton> = None;
    for item in items {
        let label = match item.hint.is_empty() {
            true => item.label.clone(),
            false => format!("{}    {}", item.label, item.hint),
        };
        let action = item.action.clone();
        let (send, shut) = (send.clone(), popover.downgrade());
        let run = move || {
            // Said first: a refresh while the menu still counts as up would open it again.
            send(A::ShowViewMenu(false));
            if let Some(p) = shut.upgrade() {
                p.popdown();
            }
            send(action.clone());
        };
        let widget: gtk::Widget = match item.kind {
            ViewItemKind::Style | ViewItemKind::Toggle => {
                let check = gtk::CheckButton::with_label(&label);
                if item.kind == ViewItemKind::Style {
                    check.set_group(group.as_ref());
                    group.get_or_insert_with(|| check.clone());
                }
                check.set_active(item.checked);
                check.connect_toggled(move |c| {
                    if c.is_active() || c.group_is_none() {
                        run();
                    }
                });
                check.upcast()
            }
            ViewItemKind::Command => {
                let b = gtk::Button::with_label(&label);
                b.add_css_class("flat");
                b.connect_clicked(move |_| run());
                b.upcast()
            }
        };
        widget.set_sensitive(item.enabled);
        list.append(&widget);
    }
    popover.set_child(Some(&list));
    let send_closed = send.clone();
    popover.connect_closed(move |_| send_closed(A::ShowViewMenu(false)));
    popover
}

trait CheckGroup {
    fn group_is_none(&self) -> bool;
}

impl CheckGroup for gtk::CheckButton {
    /// A toggle stands alone; only a radio's group decides by which one turned on.
    fn group_is_none(&self) -> bool {
        self.first_child()
            .is_some_and(|c| !c.has_css_class("radio"))
    }
}

/// What the edit toolbar shows.
#[derive(Clone, PartialEq)]
pub struct EditShown {
    on: bool,
    target: String,
    caps: BrowseCaps,
    deletable: bool,
    folder: bool,
    staged: usize,
    busy: bool,
    reading: bool,
    space: Option<SpaceReadout>,
    copy_note: Option<&'static str>,
    edits_shown: bool,
}

pub struct EditBar {
    root: gtk::Box,
    note: gtk::Label,
    target: gtk::Label,
    new_folder: gtk::Button,
    rename: gtk::Button,
    delete: gtk::Button,
    bless: gtk::Button,
    boot: gtk::Button,
    lock: gtk::Button,
    unlock: gtk::Button,
    apply: gtk::Button,
    discard: gtk::Button,
    edits: gtk::Button,
    free: gtk::Label,
    after: gtk::Label,
    shown: RefCell<Option<EditShown>>,
}

impl EditBar {
    /// `add_files` opens the multiple-file picker and hands the paths to the pane.
    pub fn build(send: &SendFn, add_files: AddFiles) -> EditBar {
        let note = gtk::Label::builder().xalign(0.0).wrap(true).build();
        note.add_css_class("accent");
        let target = gtk::Label::new(None);
        target.add_css_class("accent");
        let add = gtk::Button::with_label("Add File...");
        add.set_widget_name(ids::ADD_FILE);
        let to_pane = send.clone();
        add.connect_clicked(move |_| {
            let to_pane = to_pane.clone();
            add_files(Rc::new(move |paths| {
                if !paths.is_empty() {
                    to_pane(pane(P::AddPicked(paths)));
                }
            }));
        });
        let import = button(
            "Import Archive...",
            ids::IMPORT_ARCHIVE,
            send,
            pane(P::ImportTar),
        );
        let new_folder = button(
            "New Folder...",
            ids::NEW_FOLDER,
            send,
            pane(P::RequestNewFolder),
        );
        let rename = button("Rename...", ids::RENAME, send, pane(P::RenameAtCursor));
        let delete = button("Delete", ids::DELETE, send, pane(P::Delete));
        let bless = button("Bless Folder", ids::BLESS, send, pane(P::Bless));
        bless.set_tooltip_text(Some(
            "Mark the selected folder as the bootable System Folder. For the volume to actually \
             boot it also needs boot blocks (see 'Boot Blocks...').",
        ));
        let boot = button(
            "Boot Blocks...",
            ids::BOOT_BLOCKS,
            send,
            pane(P::BootBlocks),
        );
        boot.set_tooltip_text(Some(
            "Copy boot blocks from a bootable donor disk into this volume's first sector. Works \
             on a flat HFV and on the HFS partition of a full (APM) disk.",
        ));
        let lock = button("Lock", ids::PRODOS_LOCK, send, pane(P::SetProdosLock(true)));
        lock.set_tooltip_text(Some(
            "Set the ProDOS access byte to $21 (read + backup only).",
        ));
        let unlock = button(
            "Unlock",
            ids::PRODOS_UNLOCK,
            send,
            pane(P::SetProdosLock(false)),
        );
        unlock.set_tooltip_text(Some(
            "Set the ProDOS access byte to $C3 (read/write/destroy/rename).",
        ));
        let apply = button("Apply Edits", ids::APPLY, send, A::Mode(ModeAction::Apply));
        apply.add_css_class("suggested-action");
        let discard = button("Discard", ids::DISCARD, send, pane(P::Discard));
        let edits = gtk::Button::with_label("Edits");
        edits.set_widget_name(ids::EDITS);
        let free = gtk::Label::new(None);
        let after = gtk::Label::new(None);

        let bar = flow();
        bar.append(&target);
        for b in [
            &add,
            &import,
            &new_folder,
            &rename,
            &delete,
            &bless,
            &boot,
            &lock,
            &unlock,
            &apply,
            &discard,
            &edits,
        ] {
            bar.append(b);
        }
        bar.append(&free);
        bar.append(&after);
        let root = gtk::Box::new(gtk::Orientation::Vertical, 2);
        root.set_margin_start(8);
        root.set_margin_end(8);
        root.append(&note);
        root.append(&bar);
        let to_pane = send.clone();
        let shown_edits = edits.clone();
        let bar = EditBar {
            root,
            note,
            target,
            new_folder,
            rename,
            delete,
            bless,
            boot,
            lock,
            unlock,
            apply,
            discard,
            edits,
            free,
            after,
            shown: RefCell::default(),
        };
        shown_edits.connect_clicked(move |b| {
            let open = b.has_css_class("rb-open");
            to_pane(pane(P::ShowEdits(!open)));
        });
        bar
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn read(ctl: &CommanderController) -> EditShown {
        let pane = ctl.pane(Side::Left);
        let folder = pane.cwd_entry();
        let target = match folder.as_ref().filter(|e| e.path != "/") {
            Some(e) => format!("Editing: /{}", e.name),
            None => "Editing: /root".to_string(),
        };
        let cursor = pane.cursor_index().map(|i| &pane.rows()[i]);
        let copy_note = if ctl.editing_archive() {
            Some("Editing temporary copy. Click 'Apply Edits' to write changes.")
        } else if ctl.editing_chd() {
            Some("Editing CHD via diff. Click 'Apply Edits' to flush, or exit Edit Mode to merge into the CHD.")
        } else {
            None
        };
        EditShown {
            on: ctl.edit_mode(),
            target,
            caps: ctl.browse_caps(),
            deletable: pane.has_selection() || cursor.is_some_and(|r| !r.is_parent()),
            folder: cursor.is_some_and(|r| r.is_dir && !r.is_parent()),
            staged: pane.queue.len(),
            busy: pane.is_busy(),
            reading: ctl.copy_job().is_some(),
            space: ctl.space(),
            copy_note,
            edits_shown: pane.edits_shown(),
        }
    }

    pub fn show(&self, s: EditShown) {
        if self.shown.borrow().as_ref() == Some(&s) {
            return;
        }
        self.root.set_visible(s.on);
        set_text(&self.note, s.copy_note.unwrap_or(""));
        self.note.set_visible(s.copy_note.is_some());
        set_text(&self.target, &s.target);
        flow_show(&self.new_folder, s.caps.new_folder);
        self.rename.set_sensitive(s.deletable);
        self.delete.set_sensitive(s.deletable);
        flow_show(&self.bless, s.caps.hfs);
        self.bless.set_sensitive(s.folder);
        flow_show(&self.boot, s.caps.hfs);
        flow_show(&self.lock, s.caps.prodos);
        flow_show(&self.unlock, s.caps.prodos);
        self.lock.set_sensitive(s.deletable);
        self.unlock.set_sensitive(s.deletable);
        let any = s.staged > 0;
        flow_show(&self.apply, any);
        flow_show(&self.discard, any);
        flow_show(&self.edits, any);
        self.apply.set_label(&format!("Apply Edits ({})", s.staged));
        self.edits.set_label(&format!("Edits ({})", s.staged));
        self.apply.set_sensitive(!s.reading && !s.busy);
        let hint = s
            .reading
            .then_some("Wait for the running extraction or export to finish");
        self.apply.set_tooltip_text(hint);
        self.discard.set_sensitive(!s.busy);
        if s.edits_shown {
            self.edits.add_css_class("rb-open");
        } else {
            self.edits.remove_css_class("rb-open");
        }
        match s.space {
            Some(space) => {
                set_text(&self.free, &format!("Free: {}", format_size(space.free)));
                let after = space.after.map(|a| format!("After: {}", format_size(a)));
                set_text(&self.after, after.as_deref().unwrap_or(""));
                for (class, on) in [
                    ("error", space.overflows),
                    ("warning", space.tight && !space.overflows),
                    ("success", !space.tight && !space.overflows),
                ] {
                    if on {
                        self.after.add_css_class(class);
                    } else {
                        self.after.remove_css_class(class);
                    }
                }
            }
            None => {
                set_text(&self.free, "");
                set_text(&self.after, "");
            }
        }
        *self.shown.borrow_mut() = Some(s);
    }
}

/// The bar above the listing while rows are marked: the count, the export format, Export and Clear.
pub struct MarkBar {
    root: gtk::Box,
    count: gtk::Label,
    format: gtk::DropDown,
}

pub struct MarkShown {
    count: usize,
    format: usize,
}

impl MarkBar {
    /// `export` exports the marked rows in the given format.
    pub fn build(send: &SendFn, export: Rc<dyn Fn(ExportFormat)>) -> MarkBar {
        let count = gtk::Label::new(None);
        let labels: Vec<&str> = ExportFormat::ALL.iter().map(|f| f.label()).collect();
        let format = gtk::DropDown::from_strings(&labels);
        format.set_widget_name(ids::EXPORT_FORMAT);
        let to_screen = send.clone();
        format.connect_selected_notify(move |d| {
            if let Some(f) = ExportFormat::ALL.get(d.selected() as usize) {
                to_screen(A::SetExportFormat(*f));
            }
        });
        let go = gtk::Button::with_label("Export...");
        go.set_widget_name(ids::EXPORT_MARKED);
        let picked = format.clone();
        go.connect_clicked(move |_| {
            if let Some(f) = ExportFormat::ALL.get(picked.selected() as usize) {
                export(*f);
            }
        });
        let clear = button("Clear", ids::CLEAR_MARKS, send, pane(P::ClearSelection));
        let root = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        root.set_margin_start(8);
        root.set_margin_end(8);
        root.append(&count);
        root.append(&gtk::Label::new(Some("Export as:")));
        root.append(&format);
        root.append(&go);
        root.append(&clear);
        MarkBar {
            root,
            count,
            format,
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn read(ctl: &CommanderController) -> MarkShown {
        let pane = ctl.pane(Side::Left);
        let count = match pane.has_selection() {
            true => pane.selected_entries().len(),
            false => 0,
        };
        let format = ctl.export_format();
        MarkShown {
            count,
            format: ExportFormat::ALL
                .iter()
                .position(|f| *f == format)
                .unwrap_or(0),
        }
    }

    pub fn show(&self, s: MarkShown) {
        self.root.set_visible(s.count > 0);
        set_text(&self.count, &format!("{} selected", s.count));
        if self.format.selected() != s.format as u32 {
            self.format.set_selected(s.format as u32);
        }
    }
}
