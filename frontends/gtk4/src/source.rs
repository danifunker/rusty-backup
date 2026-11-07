//! The source picker: recent files, devices, Open File and Open Backup Folder, in one popover.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

use adw::prelude::*;

use crate::widgets::set_text;

const RECENT_ROWS: usize = 5;

/// What the user picked in the list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pick {
    Recent(PathBuf),
    Device(usize),
    OpenFile,
    OpenFolder,
    OpenBackup,
    Connect,
}

/// What one picker offers, and the widget ids it sets.
#[derive(Debug, Clone, Copy)]
pub struct PickerSpec {
    pub source: &'static str,
    pub recent: &'static str,
    pub device: &'static str,
    /// Offers Open File, with this id.
    pub open_file: Option<&'static str>,
    /// Offers Open Local Folder, with this id.
    pub open_folder: Option<&'static str>,
    /// Offers Open Backup Folder, with this id.
    pub open_backup: Option<&'static str>,
    /// Offers Connect to Remote, with this id.
    pub connect: Option<&'static str>,
    /// Connect to Remote is greyed out until the remote picker exists on this screen.
    pub connect_live: bool,
    /// Said under Devices when there are none.
    pub no_devices: Option<&'static str>,
    /// Widest the closed state's text goes, in characters.
    pub width_chars: i32,
    /// The caption before the button, if any.
    pub caption: Option<&'static str>,
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
    spec: PickerSpec,
}

impl SourcePicker {
    pub fn build(spec: PickerSpec) -> Rc<SourcePicker> {
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
            .width_chars(spec.width_chars)
            .max_width_chars(spec.width_chars + 16)
            .build();
        let button = gtk::MenuButton::builder()
            .child(&label)
            .popover(&popover)
            .always_show_arrow(true)
            .build();
        button.set_widget_name(spec.source);
        let root = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        if let Some(caption) = spec.caption {
            root.append(&gtk::Label::new(Some(caption)));
        }
        root.append(&button);
        Rc::new(SourcePicker {
            root,
            label,
            popover,
            list,
            entries: RefCell::new(Vec::new()),
            spec,
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
        let entries = entries(&self.spec, recent, devices);
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

fn entries(spec: &PickerSpec, recent: &[String], devices: &[String]) -> Vec<Entry> {
    let mut list = Vec::new();
    if !recent.is_empty() {
        list.push(Entry::heading("Recent"));
    }
    for (i, path) in recent.iter().take(RECENT_ROWS).enumerate() {
        let buf = PathBuf::from(path);
        let name = buf.file_name().map(|n| n.to_string_lossy().into_owned());
        list.push(Entry::pick(
            format!("{}.{i}", spec.recent),
            name.unwrap_or_else(|| path.clone()),
            Some(path.clone()),
            Pick::Recent(buf),
        ));
    }
    if !devices.is_empty() {
        list.push(Entry::heading("Devices"));
    } else if let Some(hint) = spec.no_devices {
        list.push(Entry::heading(hint));
    }
    for (i, device) in devices.iter().enumerate() {
        list.push(Entry::pick(
            format!("{}.{i}", spec.device),
            device.clone(),
            None,
            Pick::Device(i),
        ));
    }
    list.push(Entry::heading("Open"));
    let open =
        |id: &str, label: &str, pick| Entry::pick(id.to_string(), label.to_string(), None, pick);
    if let Some(id) = spec.open_file {
        list.push(open(id, "Open File...", Pick::OpenFile));
    }
    if let Some(id) = spec.open_folder {
        list.push(open(id, "Open Local Folder...", Pick::OpenFolder));
    }
    if let Some(id) = spec.open_backup {
        list.push(open(id, "Open Backup Folder...", Pick::OpenBackup));
    }
    if let Some(id) = spec.connect {
        list.push(match spec.connect_live {
            true => open(id, "Connect to Remote...", Pick::Connect),
            false => Entry {
                id: id.to_string(),
                label: "Connect to Remote...".to_string(),
                tooltip: Some("Coming soon: the remote picker".to_string()),
                pick: None,
                coming: true,
            },
        });
    }
    list
}
