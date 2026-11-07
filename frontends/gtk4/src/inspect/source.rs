//! The source picker: recent files, devices, Open File and Open Backup Folder, in one popover.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::inspect::ids;

use crate::widgets::set_text;

const RECENT_ROWS: usize = 5;

/// What the user picked in the list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pick {
    Recent(PathBuf),
    Device(usize),
    OpenFile,
    OpenBackup,
}

/// One line of the list: a heading when `pick` is `None` and `coming` is too.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    id: String,
    label: String,
    tooltip: Option<String>,
    pick: Option<Pick>,
    coming: bool,
}

impl Entry {
    fn heading(label: &str) -> Entry {
        Entry {
            id: String::new(),
            label: label.to_string(),
            tooltip: None,
            pick: None,
            coming: false,
        }
    }

    fn pick(id: String, label: String, tooltip: Option<String>, pick: Pick) -> Entry {
        Entry {
            id,
            label,
            tooltip,
            pick: Some(pick),
            coming: false,
        }
    }

    fn row(&self) -> gtk::ListBoxRow {
        let label = gtk::Label::builder()
            .label(self.label.as_str())
            .xalign(0.0)
            .margin_top(4)
            .margin_bottom(4)
            .margin_start(6)
            .margin_end(6)
            .build();
        let row = gtk::ListBoxRow::builder().child(&label).build();
        let live = self.pick.is_some();
        row.set_activatable(live);
        row.set_selectable(false);
        if !live {
            label.add_css_class("dim-label");
        }
        if !self.id.is_empty() {
            row.set_widget_name(&self.id);
        }
        row.set_tooltip_text(self.tooltip.as_deref());
        row.set_sensitive(!self.coming);
        row
    }
}

pub struct SourcePicker {
    root: gtk::Box,
    label: gtk::Label,
    popover: gtk::Popover,
    list: gtk::ListBox,
    entries: RefCell<Vec<Entry>>,
}

impl SourcePicker {
    pub fn build() -> Rc<SourcePicker> {
        let list = gtk::ListBox::builder()
            .selection_mode(gtk::SelectionMode::None)
            .activate_on_single_click(true)
            .build();
        let scroll = gtk::ScrolledWindow::builder()
            .child(&list)
            .hscrollbar_policy(gtk::PolicyType::Never)
            .propagate_natural_height(true)
            .propagate_natural_width(true)
            .max_content_height(480)
            .build();
        let popover = gtk::Popover::builder().child(&scroll).build();
        let label = gtk::Label::builder()
            .xalign(0.0)
            .ellipsize(gtk::pango::EllipsizeMode::Middle)
            .width_chars(44)
            .max_width_chars(60)
            .build();
        let button = gtk::MenuButton::builder()
            .child(&label)
            .popover(&popover)
            .always_show_arrow(true)
            .build();
        button.set_widget_name(ids::SOURCE);
        let root = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        root.append(&gtk::Label::new(Some("Source:")));
        root.append(&button);
        Rc::new(SourcePicker {
            root,
            label,
            popover,
            list,
            entries: RefCell::new(Vec::new()),
        })
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    /// Call `on_pick` with each choice; the popover closes first.
    pub fn connect_pick(self: &Rc<Self>, on_pick: impl Fn(Pick) + 'static) {
        let me = self.clone();
        self.list.connect_row_activated(move |_, row| {
            let index = row.index() as usize;
            let pick = me.entries.borrow().get(index).and_then(|e| e.pick.clone());
            me.popover.popdown();
            if let Some(pick) = pick {
                on_pick(pick);
            }
        });
    }

    /// Show the closed state's text and the choices; the list is rebuilt only when they change.
    pub fn show(&self, current: &str, recent: &[String], devices: &[String]) {
        set_text(&self.label, current);
        let entries = entries(recent, devices);
        if *self.entries.borrow() == entries {
            return;
        }
        self.list.remove_all();
        for entry in &entries {
            self.list.append(&entry.row());
        }
        *self.entries.borrow_mut() = entries;
    }
}

fn entries(recent: &[String], devices: &[String]) -> Vec<Entry> {
    let mut list = Vec::new();
    if !recent.is_empty() {
        list.push(Entry::heading("Recent"));
    }
    for (i, path) in recent.iter().take(RECENT_ROWS).enumerate() {
        let buf = PathBuf::from(path);
        let name = buf.file_name().map(|n| n.to_string_lossy().into_owned());
        list.push(Entry::pick(
            format!("{}.{i}", ids::SOURCE_RECENT),
            name.unwrap_or_else(|| path.clone()),
            Some(path.clone()),
            Pick::Recent(buf),
        ));
    }
    if !devices.is_empty() {
        list.push(Entry::heading("Devices"));
    }
    for (i, device) in devices.iter().enumerate() {
        list.push(Entry::pick(
            format!("{}.{i}", ids::SOURCE_DEVICE),
            device.clone(),
            None,
            Pick::Device(i),
        ));
    }
    list.push(Entry::heading("Open"));
    let open =
        |id: &str, label: &str, pick| Entry::pick(id.to_string(), label.to_string(), None, pick);
    list.push(open(ids::OPEN_FILE, "Open File...", Pick::OpenFile));
    list.push(open(
        ids::OPEN_BACKUP,
        "Open Backup Folder...",
        Pick::OpenBackup,
    ));
    list.push(Entry {
        id: ids::CONNECT_REMOTE.to_string(),
        label: "Connect to Remote...".to_string(),
        tooltip: Some("Coming soon: the remote picker".to_string()),
        pick: None,
        coming: true,
    });
    list
}
