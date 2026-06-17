//! Commander Mode -- runnable visual mock (throwaway).
//!
//! A non-functional, hardcoded-data prototype of the planned two-pane
//! "Commander Mode" file explorer. No real disk I/O happens here -- every
//! volume below is fake. The point is to agree on layout and interaction
//! before building the real feature. See `docs/commander_mode.md`.
//!
//! Run with:
//!     cargo run --example commander_mock --features mock_gui
//!
//! Demonstrated:
//!   - full-page two-pane layout with a middle action column
//!   - MULTI-SELECT: Ctrl/Cmd-click toggles a row, Shift-click selects a range,
//!     plain click selects one, empty-space click clears. (egui's cross-platform
//!     `Modifiers::command` = Cmd on macOS, Ctrl elsewhere.)
//!   - copy controls drawn as a copy icon + an animated direction arrow
//!   - right-click a row -> context menu: Copy to other pane / Delete /
//!     Undelete / Info / Export (host, zip, tgz, sit). Acts on the whole
//!     selection when the clicked row is part of it.
//!   - Delete toggles: stage delete, undelete a staged delete, or drop a
//!     staged copy back off the list
//!   - New Folder (staged) + folder delete
//!   - editable full path with a "@N" partition marker; ".." navigates up
//!   - sortable Name / Size / Modified / Type columns; per-pane Apply/Discard/Close
//!   - drag a real file onto a pane to "load" it as that pane's source
//!
//! ASCII only in all chrome, per the project's no-Unicode-glyph rule.

use eframe::egui;

const ROW_H: f32 = 20.0;
const MID_W: f32 = 152.0;

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1160.0, 720.0])
            .with_min_inner_size([840.0, 480.0])
            .with_title("Rusty Backup -- Commander Mode (MOCK)"),
        ..Default::default()
    };
    eframe::run_native(
        "Commander Mode (mock)",
        options,
        Box::new(|_cc| Ok(Box::new(CommanderMock::new()))),
    )
}

// ----------------------------------------------------------------------------
// Fake filesystem model
// ----------------------------------------------------------------------------

#[derive(Clone)]
enum Meta {
    /// FAT attribute bits: 0x01 read-only, 0x02 hidden, 0x04 system, 0x20 archive.
    Fat {
        attrs: u8,
    },
    /// HFS type/creator + resource fork size.
    Hfs {
        type_code: String,
        creator: String,
        rsrc: u64,
    },
    /// Unix mode/uid/gid.
    Unix {
        mode: u32,
        uid: u32,
        gid: u32,
    },
    /// Host file -- nothing extra.
    Host,
    Plain,
}

#[derive(Clone)]
struct Entry {
    name: String,
    is_dir: bool,
    size: u64,
    modified: String,
    meta: Meta,
    children: Vec<Entry>,
}

fn dir(name: &str, modified: &str, children: Vec<Entry>) -> Entry {
    Entry {
        name: name.into(),
        is_dir: true,
        size: 0,
        modified: modified.into(),
        meta: Meta::Plain,
        children,
    }
}
fn file(name: &str, size: u64, modified: &str, meta: Meta) -> Entry {
    Entry {
        name: name.into(),
        is_dir: false,
        size,
        modified: modified.into(),
        meta,
        children: vec![],
    }
}

struct Volume {
    label: String,
    fs: String,
    editable: bool,
    /// Partition number within the source (shown as "@N"). None for a flat
    /// host folder.
    partition: Option<u32>,
    root: Entry,
}

fn sample_volumes() -> Vec<Volume> {
    let fat = Volume {
        label: "DOS622 (FAT16)".into(),
        fs: "FAT16".into(),
        editable: true,
        partition: Some(1),
        root: dir(
            "",
            "",
            vec![
                dir(
                    "DOS",
                    "1994-05-31 06:22",
                    vec![
                        file(
                            "COMMAND.COM",
                            54_645,
                            "1994-05-31 06:22",
                            Meta::Fat { attrs: 0x20 },
                        ),
                        file(
                            "FORMAT.COM",
                            22_974,
                            "1994-05-31 06:22",
                            Meta::Fat { attrs: 0x21 },
                        ),
                        file(
                            "HIMEM.SYS",
                            29_136,
                            "1994-05-31 06:22",
                            Meta::Fat { attrs: 0x24 },
                        ),
                    ],
                ),
                dir(
                    "GAMES",
                    "1996-08-12 19:03",
                    vec![dir(
                        "DOOM",
                        "1996-08-12 19:05",
                        vec![
                            file(
                                "DOOM.EXE",
                                715_493,
                                "1995-09-01 00:00",
                                Meta::Fat { attrs: 0x20 },
                            ),
                            file(
                                "DOOM.WAD",
                                11_159_840,
                                "1995-09-01 00:00",
                                Meta::Fat { attrs: 0x20 },
                            ),
                        ],
                    )],
                ),
                file(
                    "AUTOEXEC.BAT",
                    312,
                    "1996-08-12 19:00",
                    Meta::Fat { attrs: 0x20 },
                ),
                file(
                    "CONFIG.SYS",
                    268,
                    "1996-08-12 19:00",
                    Meta::Fat { attrs: 0x20 },
                ),
                file(
                    "IO.SYS",
                    40_566,
                    "1994-05-31 06:22",
                    Meta::Fat { attrs: 0x27 },
                ),
            ],
        ),
    };
    let hfs = Volume {
        label: "Macintosh HD (HFS)".into(),
        fs: "HFS".into(),
        editable: true,
        partition: Some(1),
        root: dir(
            "",
            "",
            vec![
                dir(
                    "System Folder",
                    "1997-03-14 09:12",
                    vec![
                        file(
                            "System",
                            3_407_872,
                            "1997-03-14 09:12",
                            Meta::Hfs {
                                type_code: "zsys".into(),
                                creator: "MACS".into(),
                                rsrc: 3_400_000,
                            },
                        ),
                        file(
                            "Finder",
                            1_245_184,
                            "1997-03-14 09:12",
                            Meta::Hfs {
                                type_code: "FNDR".into(),
                                creator: "MACS".into(),
                                rsrc: 1_240_000,
                            },
                        ),
                    ],
                ),
                dir(
                    "Applications",
                    "1998-01-02 11:00",
                    vec![file(
                        "SimpleText",
                        81_920,
                        "1996-11-20 00:00",
                        Meta::Hfs {
                            type_code: "APPL".into(),
                            creator: "ttxt".into(),
                            rsrc: 80_000,
                        },
                    )],
                ),
                file(
                    "Read Me",
                    4_096,
                    "1998-01-02 11:05",
                    Meta::Hfs {
                        type_code: "TEXT".into(),
                        creator: "ttxt".into(),
                        rsrc: 0,
                    },
                ),
                file(
                    "Picture.pict",
                    262_144,
                    "1998-01-05 16:30",
                    Meta::Hfs {
                        type_code: "PICT".into(),
                        creator: "ttxt".into(),
                        rsrc: 261_000,
                    },
                ),
            ],
        ),
    };
    let ext = Volume {
        label: "linux-root (ext4)".into(),
        fs: "ext4".into(),
        editable: true,
        partition: Some(2),
        root: dir(
            "",
            "",
            vec![
                dir(
                    "home",
                    "2025-12-01 09:00",
                    vec![dir(
                        "dani",
                        "2026-06-10 12:00",
                        vec![
                            file(
                                ".bashrc",
                                3_771,
                                "2026-06-10 12:00",
                                Meta::Unix {
                                    mode: 0o100644,
                                    uid: 1000,
                                    gid: 1000,
                                },
                            ),
                            file(
                                "notes.md",
                                8_192,
                                "2026-06-12 14:22",
                                Meta::Unix {
                                    mode: 0o100600,
                                    uid: 1000,
                                    gid: 1000,
                                },
                            ),
                        ],
                    )],
                ),
                dir(
                    "etc",
                    "2026-05-20 08:00",
                    vec![file(
                        "fstab",
                        680,
                        "2026-05-20 08:00",
                        Meta::Unix {
                            mode: 0o100644,
                            uid: 0,
                            gid: 0,
                        },
                    )],
                ),
                file(
                    "vmlinuz",
                    11_534_336,
                    "2026-05-18 03:10",
                    Meta::Unix {
                        mode: 0o100644,
                        uid: 0,
                        gid: 0,
                    },
                ),
            ],
        ),
    };
    let host = Volume {
        label: "~/Downloads (host)".into(),
        fs: "host".into(),
        editable: true,
        partition: None,
        root: dir(
            "",
            "",
            vec![
                dir(
                    "isos",
                    "2026-02-01 10:00",
                    vec![file(
                        "dos622.img",
                        504_857_600,
                        "2026-01-15 22:14",
                        Meta::Host,
                    )],
                ),
                file("backup-notes.txt", 1_840, "2026-06-10 08:02", Meta::Host),
                file("quadra.hda", 1_073_741_824, "2026-05-30 14:41", Meta::Host),
            ],
        ),
    };
    vec![fat, hfs, ext, host]
}

// ----------------------------------------------------------------------------
// Pane + app state
// ----------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum Side {
    Left,
    Right,
}
impl Side {
    fn other(self) -> Side {
        match self {
            Side::Left => Side::Right,
            Side::Right => Side::Left,
        }
    }
    fn idx(self) -> usize {
        match self {
            Side::Left => 0,
            Side::Right => 1,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum SortCol {
    Name,
    Size,
    Modified,
    Type,
}

struct Pane {
    vol: Option<usize>,
    cwd: Vec<String>,
    sort: SortCol,
    desc: bool,
    /// Multi-selection: the set of selected entry names in the current dir.
    selected: Vec<String>,
    /// Anchor row for Shift-range selection.
    anchor: Option<String>,
    staged_adds: Vec<Entry>,
    staged_dels: Vec<String>,
    /// Editable path buffer shown in the header.
    path_edit: String,
}
impl Pane {
    fn new(vol: Option<usize>) -> Self {
        Pane {
            vol,
            cwd: vec![],
            sort: SortCol::Name,
            desc: false,
            selected: vec![],
            anchor: None,
            staged_adds: vec![],
            staged_dels: vec![],
            path_edit: "/".into(),
        }
    }
    fn staged_count(&self) -> usize {
        self.staged_adds.len() + self.staged_dels.len()
    }
    fn clear_selection(&mut self) {
        self.selected.clear();
        self.anchor = None;
    }
}

enum RowKind {
    Parent,
    Normal,
    PendingAdd,
    PendingDel,
}
struct DisplayRow {
    name: String,
    is_dir: bool,
    size: u64,
    modified: String,
    type_tag: String,
    kind: RowKind,
}

struct DetailSel {
    name: String,
    fs: String,
    size: u64,
    modified: String,
    meta: Meta,
    edit_type: String,
    edit_creator: String,
}

struct NewFolder {
    side: Side,
    name: String,
}

struct CommanderMock {
    volumes: Vec<Volume>,
    left: Pane,
    right: Pane,
    /// Which pane the middle buttons act on (last interacted).
    active: Side,
    detail: Option<DetailSel>,
    new_folder: Option<NewFolder>,
    unsaved: bool,
    status: String,
}

impl CommanderMock {
    fn new() -> Self {
        CommanderMock {
            volumes: sample_volumes(),
            left: Pane::new(Some(0)),
            right: Pane::new(Some(1)),
            active: Side::Left,
            detail: None,
            new_folder: None,
            unsaved: false,
            status: "Mock -- Ctrl/Cmd-click and Shift-click to multi-select; right-click for the menu; drag a file onto a pane to 'load' it.".into(),
        }
    }

    fn pane(&self, side: Side) -> &Pane {
        match side {
            Side::Left => &self.left,
            Side::Right => &self.right,
        }
    }
    fn pane_mut(&mut self, side: Side) -> &mut Pane {
        match side {
            Side::Left => &mut self.left,
            Side::Right => &mut self.right,
        }
    }

    fn current_dir<'a>(&'a self, side: Side) -> Option<&'a Entry> {
        let p = self.pane(side);
        let v = self.volumes.get(p.vol?)?;
        let mut node = &v.root;
        for name in &p.cwd {
            node = node.children.iter().find(|e| e.is_dir && &e.name == name)?;
        }
        Some(node)
    }

    fn apply_path(&mut self, side: Side, raw: &str) {
        let Some(vi) = self.pane(side).vol else {
            return;
        };
        let Some(v) = self.volumes.get(vi) else {
            return;
        };
        let mut node = &v.root;
        let mut resolved: Vec<String> = vec![];
        for seg in raw.split('/').filter(|s| !s.is_empty()) {
            match node.children.iter().find(|e| e.is_dir && e.name == seg) {
                Some(child) => {
                    resolved.push(child.name.clone());
                    node = child;
                }
                None => {
                    self.status = format!("Path not found: {raw}");
                    return;
                }
            }
        }
        let p = self.pane_mut(side);
        p.cwd = resolved;
        p.clear_selection();
    }

    fn build_rows(&self, side: Side) -> Vec<DisplayRow> {
        let p = self.pane(side);
        let mut rows: Vec<DisplayRow> = vec![];
        let at_root = p.cwd.is_empty();
        let is_host = p
            .vol
            .and_then(|i| self.volumes.get(i))
            .map(|v| v.fs == "host")
            .unwrap_or(false);
        if !at_root || is_host {
            rows.push(DisplayRow {
                name: "..".into(),
                is_dir: true,
                size: 0,
                modified: String::new(),
                type_tag: String::new(),
                kind: RowKind::Parent,
            });
        }
        if let Some(d) = self.current_dir(side) {
            let mut real: Vec<&Entry> = d.children.iter().collect();
            real.sort_by(|a, b| sort_key(a, p.sort).cmp(&sort_key(b, p.sort)));
            if p.desc {
                real.reverse();
            }
            real.sort_by_key(|e| !e.is_dir);
            for e in real {
                let del = p.staged_dels.iter().any(|n| n == &e.name);
                rows.push(DisplayRow {
                    name: e.name.clone(),
                    is_dir: e.is_dir,
                    size: e.size,
                    modified: e.modified.clone(),
                    type_tag: type_tag(e),
                    kind: if del {
                        RowKind::PendingDel
                    } else {
                        RowKind::Normal
                    },
                });
            }
        }
        for e in &p.staged_adds {
            rows.push(DisplayRow {
                name: e.name.clone(),
                is_dir: e.is_dir,
                size: e.size,
                modified: e.modified.clone(),
                type_tag: type_tag(e),
                kind: RowKind::PendingAdd,
            });
        }
        rows
    }

    // --- selection ---------------------------------------------------------

    /// Apply a click to the selection, honoring Ctrl/Cmd (toggle) and Shift (range).
    fn select_click(
        &mut self,
        side: Side,
        row_names: &[String],
        name: &str,
        command: bool,
        shift: bool,
    ) {
        let p = self.pane_mut(side);
        if shift {
            let anchor = p.anchor.clone().unwrap_or_else(|| name.to_string());
            let ia = row_names.iter().position(|n| n == &anchor);
            let ib = row_names.iter().position(|n| n == name);
            if let (Some(ia), Some(ib)) = (ia, ib) {
                let (lo, hi) = if ia <= ib { (ia, ib) } else { (ib, ia) };
                p.selected = row_names[lo..=hi].to_vec();
            } else {
                p.selected = vec![name.to_string()];
            }
        } else if command {
            if let Some(pos) = p.selected.iter().position(|n| n == name) {
                p.selected.remove(pos);
            } else {
                p.selected.push(name.to_string());
            }
            p.anchor = Some(name.to_string());
        } else {
            p.selected = vec![name.to_string()];
            p.anchor = Some(name.to_string());
        }
    }

    // --- actions -----------------------------------------------------------

    /// Stage a copy of the source pane's whole selection onto the other pane.
    fn copy_selection(&mut self, from: Side, to: Side) {
        let names = self.pane(from).selected.clone();
        if names.is_empty() {
            self.status = format!("Nothing selected in the {} pane to copy.", side_label(from));
            return;
        }
        let mut count = 0;
        for name in &names {
            let found = self
                .current_dir(from)
                .and_then(|d| d.children.iter().find(|e| &e.name == name).cloned());
            if let Some(e) = found {
                self.pane_mut(to).staged_adds.push(e);
                count += 1;
            }
        }
        self.status = format!(
            "Staged copy of {count} item(s) into {} pane. Use Apply to write.",
            side_label(to)
        );
    }

    /// Toggle delete on the whole selection (delete / undelete / un-stage a copy).
    fn toggle_delete_selection(&mut self, side: Side) {
        let names = self.pane(side).selected.clone();
        if names.is_empty() {
            self.status = format!("Nothing selected in the {} pane.", side_label(side));
            return;
        }
        for name in &names {
            let p = self.pane_mut(side);
            if let Some(pos) = p.staged_adds.iter().position(|e| &e.name == name) {
                p.staged_adds.remove(pos); // un-stage a copy / new folder
            } else if let Some(pos) = p.staged_dels.iter().position(|n| n == name) {
                p.staged_dels.remove(pos); // undelete
            } else {
                p.staged_dels.push(name.clone()); // stage delete
            }
        }
        self.status = format!(
            "Toggled delete on {} item(s) ({}). Use Apply to write.",
            names.len(),
            side_label(side)
        );
    }

    fn open_detail(&mut self, side: Side, name: &str) {
        let Some(d) = self.current_dir(side) else {
            return;
        };
        let Some(e) = d.children.iter().find(|e| e.name == name).cloned() else {
            self.status = format!("'{name}' has no stored details in this mock.");
            return;
        };
        let fs = self
            .pane(side)
            .vol
            .and_then(|i| self.volumes.get(i))
            .map(|v| v.fs.clone())
            .unwrap_or_default();
        let (edit_type, edit_creator) = match &e.meta {
            Meta::Hfs {
                type_code, creator, ..
            } => (type_code.clone(), creator.clone()),
            _ => (String::new(), String::new()),
        };
        self.detail = Some(DetailSel {
            name: e.name.clone(),
            fs,
            size: e.size,
            modified: e.modified.clone(),
            meta: e.meta.clone(),
            edit_type,
            edit_creator,
        });
    }
}

fn sort_key(e: &Entry, col: SortCol) -> String {
    match col {
        SortCol::Name => e.name.to_lowercase(),
        SortCol::Size => format!("{:020}", e.size),
        SortCol::Modified => e.modified.clone(),
        SortCol::Type => type_tag(e),
    }
}

fn type_tag(e: &Entry) -> String {
    if e.is_dir {
        return "<DIR>".into();
    }
    match &e.meta {
        Meta::Fat { attrs } => {
            let a = if attrs & 0x20 != 0 { 'A' } else { '-' };
            let h = if attrs & 0x02 != 0 { 'H' } else { '-' };
            let s = if attrs & 0x04 != 0 { 'S' } else { '-' };
            let r = if attrs & 0x01 != 0 { 'R' } else { '-' };
            format!("{a}{h}{s}{r}")
        }
        Meta::Hfs { type_code, .. } => type_code.clone(),
        Meta::Unix { mode, .. } => format!("{:04o}", mode & 0o7777),
        Meta::Host | Meta::Plain => e
            .name
            .rsplit_once('.')
            .map(|(_, x)| x.to_uppercase())
            .unwrap_or_default(),
    }
}

fn human_size(n: u64) -> String {
    const U: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < U.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", U[i])
    }
}

fn side_label(side: Side) -> &'static str {
    match side {
        Side::Left => "L",
        Side::Right => "R",
    }
}

// ----------------------------------------------------------------------------
// Column geometry
// ----------------------------------------------------------------------------

struct Cols {
    name_l: f32,
    name_r: f32,
    size_l: f32,
    size_r: f32,
    mod_l: f32,
    type_l: f32,
}
fn cols(rect: egui::Rect) -> Cols {
    let pad = 6.0;
    let gap = 10.0;
    let type_w = 56.0;
    let mod_w = 140.0;
    let size_w = 82.0;
    let name_l = rect.left() + pad;
    let name_w = (rect.width() - type_w - mod_w - size_w - 4.0 * gap).max(60.0);
    let name_r = name_l + name_w;
    let size_l = name_r + gap;
    let size_r = size_l + size_w;
    let mod_l = size_r + gap;
    let type_l = mod_l + mod_w + gap;
    Cols {
        name_l,
        name_r,
        size_l,
        size_r,
        mod_l,
        type_l,
    }
}

// ----------------------------------------------------------------------------
// eframe::App
// ----------------------------------------------------------------------------

impl eframe::App for CommanderMock {
    // NOTE: this crate uses a patched eframe whose `App` trait requires `ui()`
    // (taking `&mut egui::Ui`) and uses `egui::Panel::*::show_inside`, not the
    // stock `update()` + `TopBottomPanel`. See docs/commander_mode.md build note.
    fn ui(&mut self, root: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = root.ctx().clone();
        self.handle_drops(&ctx);

        let left_rows = self.build_rows(Side::Left);
        let right_rows = self.build_rows(Side::Right);

        egui::Panel::top("cmd_top").show_inside(root, |ui| {
            ui.add_space(2.0);
            ui.horizontal(|ui| {
                ui.heading("Commander Mode");
                ui.label("(MOCK)");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Close").clicked() {
                        if self.left.staged_count() + self.right.staged_count() > 0 {
                            self.unsaved = true;
                        } else {
                            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                        }
                    }
                });
            });
            ui.add_space(2.0);
        });

        egui::Panel::bottom("cmd_bottom").show_inside(root, |ui| {
            ui.add_space(2.0);
            ui.horizontal(|ui| {
                ui.label("Status:");
                ui.label(&self.status);
            });
            ui.add_space(2.0);
        });

        egui::CentralPanel::default().show_inside(root, |ui| {
            let full_h = ui.available_height();
            let full_w = ui.available_width();
            let pane_w = ((full_w - MID_W) / 2.0 - 8.0).max(220.0);
            ui.horizontal_top(|ui| {
                ui.allocate_ui_with_layout(
                    egui::vec2(pane_w, full_h),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| self.render_pane(ui, Side::Left, &left_rows),
                );
                ui.separator();
                ui.allocate_ui_with_layout(
                    egui::vec2(MID_W, full_h),
                    egui::Layout::top_down(egui::Align::Center),
                    |ui| self.render_middle(ui),
                );
                ui.separator();
                ui.allocate_ui_with_layout(
                    egui::vec2(pane_w, full_h),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| self.render_pane(ui, Side::Right, &right_rows),
                );
            });
        });

        self.render_detail(&ctx);
        self.render_new_folder(&ctx);
        self.render_unsaved(&ctx);
    }
}

impl CommanderMock {
    fn handle_drops(&mut self, ctx: &egui::Context) {
        let (dropped, hover_pos) =
            ctx.input(|i| (i.raw.dropped_files.clone(), i.pointer.hover_pos()));
        if dropped.is_empty() {
            return;
        }
        let center_x = ctx.content_rect().center().x;
        let side = match hover_pos {
            Some(p) if p.x >= center_x => Side::Right,
            _ => Side::Left,
        };
        if let Some(path) = dropped.iter().find_map(|f| f.path.clone()) {
            let label = path
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "dropped".into());
            let vol = Volume {
                label: format!("{label} (dropped)"),
                fs: "FAT16".into(),
                editable: true,
                partition: Some(1),
                root: dir(
                    "",
                    "",
                    vec![
                        file("READExME.TXT", 220, "now", Meta::Fat { attrs: 0x20 }),
                        dir("DATA", "now", vec![]),
                    ],
                ),
            };
            self.volumes.push(vol);
            let idx = self.volumes.len() - 1;
            let p = self.pane_mut(side);
            p.vol = Some(idx);
            p.cwd.clear();
            p.clear_selection();
            p.path_edit = "/".into();
            let which = if matches!(side, Side::Left) {
                "left"
            } else {
                "right"
            };
            self.status = format!("Loaded '{label}' into {which} pane (mock).");
        }
    }

    fn render_pane(&mut self, ui: &mut egui::Ui, side: Side, rows: &[DisplayRow]) {
        let other = side.other();

        let mut vol_local = self.pane(side).vol;
        let mut path_edit_local = self.pane(side).path_edit.clone();
        let cwd_now = self.pane(side).cwd.clone();
        let n = self.pane(side).staged_count();
        let editable = vol_local
            .and_then(|i| self.volumes.get(i))
            .map(|v| v.editable)
            .unwrap_or(false);
        let prefix = vol_local
            .and_then(|i| self.volumes.get(i))
            .map(|v| match v.partition {
                Some(p) => format!("@{p}"),
                None => "host".into(),
            })
            .unwrap_or_else(|| "(none)".into());
        let cur_label = vol_local
            .and_then(|i| self.volumes.get(i))
            .map(|v| v.label.clone())
            .unwrap_or_else(|| "(empty)".into());

        let mut act_open = false;
        let mut act_apply = false;
        let mut act_discard = false;
        let mut act_close = false;
        let mut path_submit = false;

        // Row 1: source + staged controls + close.
        ui.horizontal_wrapped(|ui| {
            if ui.button("Open...").clicked() {
                act_open = true;
            }
            egui::ComboBox::from_id_salt(("vol", side.idx()))
                .selected_text(cur_label)
                .width(200.0)
                .show_ui(ui, |ui| {
                    ui.selectable_value(&mut vol_local, None, "(empty)");
                    for (i, v) in self.volumes.iter().enumerate() {
                        ui.selectable_value(&mut vol_local, Some(i), &v.label);
                    }
                });
            ui.add_enabled_ui(n > 0 && editable, |ui| {
                if ui.button(format!("Apply ({n})")).clicked() {
                    act_apply = true;
                }
            });
            ui.add_enabled_ui(n > 0, |ui| {
                if ui.button("Discard").clicked() {
                    act_discard = true;
                }
            });
            ui.add_enabled_ui(vol_local.is_some(), |ui| {
                if ui.button("Close").clicked() {
                    act_close = true;
                }
            });
        });

        // Row 2: "@N" partition marker + editable path.
        ui.horizontal(|ui| {
            ui.monospace(&prefix);
            let resp = ui.add(
                egui::TextEdit::singleline(&mut path_edit_local)
                    .desired_width(ui.available_width() - 4.0)
                    .hint_text("/path"),
            );
            if resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                path_submit = true;
            }
            if !resp.has_focus() && !path_submit {
                path_edit_local = format!("/{}", cwd_now.join("/"));
            }
        });
        ui.separator();

        self.render_header(ui, side);

        // Rows + row context menus.
        let mut to_enter: Option<String> = None;
        let mut to_up = false;
        let mut click: Option<(String, bool, bool)> = None; // (name, command, shift)
        let mut dbl_detail: Option<String> = None;
        let mut ctx_rclick: Option<String> = None;
        let mut m_copy = false;
        let mut m_toggle_del = false;
        let mut m_detail = false;
        let mut m_export: Option<&'static str> = None;
        let mut bg_deselect = false;

        egui::ScrollArea::vertical()
            .id_salt(("cmd_rows", side.idx()))
            .auto_shrink([false, false])
            .show(ui, |ui| {
                let sel = self.pane(side).selected.clone();
                let mods = ui.input(|i| i.modifiers);
                for row in rows {
                    let (rect, resp) = ui.allocate_exact_size(
                        egui::vec2(ui.available_width(), ROW_H),
                        egui::Sense::click(),
                    );
                    let selected = sel.iter().any(|n| n == &row.name);
                    if selected {
                        ui.painter().rect_filled(
                            rect,
                            egui::CornerRadius::ZERO,
                            ui.visuals().selection.bg_fill,
                        );
                    } else if resp.hovered() {
                        ui.painter().rect_filled(
                            rect,
                            egui::CornerRadius::ZERO,
                            ui.visuals().widgets.hovered.bg_fill,
                        );
                    }
                    paint_row(ui, rect, row, selected);

                    if resp.double_clicked() {
                        match row.kind {
                            RowKind::Parent => to_up = true,
                            _ if row.is_dir => to_enter = Some(row.name.clone()),
                            _ => dbl_detail = Some(row.name.clone()),
                        }
                    } else if resp.clicked() {
                        match row.kind {
                            RowKind::Parent => to_up = true,
                            _ => click = Some((row.name.clone(), mods.command, mods.shift)),
                        }
                    }

                    if !matches!(row.kind, RowKind::Parent) {
                        let sel_count = if sel.iter().any(|n| n == &row.name) {
                            sel.len().max(1)
                        } else {
                            1
                        };
                        let menu = resp.context_menu(|ui| {
                            row_menu(
                                ui,
                                row,
                                other,
                                sel_count,
                                &mut m_copy,
                                &mut m_toggle_del,
                                &mut m_detail,
                                &mut m_export,
                            );
                        });
                        if menu.is_some() {
                            ctx_rclick = Some(row.name.clone());
                        }
                    }
                }

                // Click empty space below the rows to clear the selection.
                let remaining = ui.available_size();
                if remaining.y > 4.0 {
                    let (_r, bgr) = ui.allocate_exact_size(remaining, egui::Sense::click());
                    if bgr.clicked() {
                        bg_deselect = true;
                    }
                }
            });

        let interacted = to_up
            || to_enter.is_some()
            || click.is_some()
            || dbl_detail.is_some()
            || ctx_rclick.is_some()
            || m_copy
            || m_toggle_del
            || bg_deselect
            || act_open
            || act_close;

        // Pane-only simple mutations.
        {
            let pane = self.pane_mut(side);
            if pane.vol != vol_local {
                pane.vol = vol_local;
                pane.cwd.clear();
                pane.clear_selection();
                pane.path_edit = "/".into();
            } else {
                pane.path_edit = path_edit_local.clone();
            }
            if act_close {
                pane.vol = None;
                pane.cwd.clear();
                pane.clear_selection();
                pane.staged_adds.clear();
                pane.staged_dels.clear();
                pane.path_edit = "/".into();
            }
            if act_apply || act_discard {
                pane.staged_adds.clear();
                pane.staged_dels.clear();
            }
            if to_up {
                pane.cwd.pop();
                pane.clear_selection();
            }
            if let Some(name) = &to_enter {
                pane.cwd.push(name.clone());
                pane.clear_selection();
            }
            if bg_deselect {
                pane.clear_selection();
            }
        }

        // Selection update from a click (with modifiers).
        if let Some((name, command, shift)) = &click {
            let row_names: Vec<String> = rows
                .iter()
                .filter(|r| !matches!(r.kind, RowKind::Parent))
                .map(|r| r.name.clone())
                .collect();
            self.select_click(side, &row_names, name, *command, *shift);
        }

        // Right-click: if the clicked row isn't already selected, select just it,
        // so the menu acts on a sensible target.
        if let Some(name) = &ctx_rclick {
            if !self.pane(side).selected.iter().any(|n| n == name) {
                let p = self.pane_mut(side);
                p.selected = vec![name.clone()];
                p.anchor = Some(name.clone());
            }
        }

        if act_open {
            self.status = "Open... -> would launch the device/image/folder picker.".into();
        }
        if act_close {
            self.status = "Closed pane source.".into();
        }
        if act_apply {
            self.status = format!(
                "Applied staged edits ({}) -- mock, no real write.",
                side_label(side)
            );
        }
        if act_discard {
            self.status = format!("Discarded staged edits ({}).", side_label(side));
        }
        if path_submit {
            self.apply_path(side, &path_edit_local);
        }
        if interacted {
            self.active = side;
        }

        // Menu / double-click actions (after selection settled).
        if m_copy {
            self.copy_selection(side, other);
        }
        if m_toggle_del {
            self.toggle_delete_selection(side);
        }
        if m_detail {
            if let Some(first) = self.pane(side).selected.first().cloned() {
                self.open_detail(side, &first);
            }
        }
        if let Some(kind) = m_export {
            let names = self.pane(side).selected.clone();
            self.status = export_status(&names, kind);
        }
        if let Some(name) = dbl_detail {
            self.open_detail(side, &name);
        }
    }

    fn render_header(&mut self, ui: &mut egui::Ui, side: Side) {
        let (rect, resp) = ui.allocate_exact_size(
            egui::vec2(ui.available_width(), ROW_H),
            egui::Sense::click(),
        );
        let c = cols(rect);
        let mid = rect.center().y;
        let p = self.pane(side);
        let font = egui::FontId::proportional(13.0);
        let col = ui.visuals().strong_text_color();
        let caret = |active: bool| -> &'static str {
            if !active {
                ""
            } else if p.desc {
                " v"
            } else {
                " ^"
            }
        };
        let title = |name: &str, sc: SortCol| format!("{name}{}", caret(p.sort == sc));
        let pt = ui.painter();
        pt.text(
            egui::pos2(c.name_l, mid),
            egui::Align2::LEFT_CENTER,
            title("Name", SortCol::Name),
            font.clone(),
            col,
        );
        pt.text(
            egui::pos2(c.size_r, mid),
            egui::Align2::RIGHT_CENTER,
            title("Size", SortCol::Size),
            font.clone(),
            col,
        );
        pt.text(
            egui::pos2(c.mod_l, mid),
            egui::Align2::LEFT_CENTER,
            title("Modified", SortCol::Modified),
            font.clone(),
            col,
        );
        pt.text(
            egui::pos2(c.type_l, mid),
            egui::Align2::LEFT_CENTER,
            title("Type", SortCol::Type),
            font,
            col,
        );
        ui.painter().line_segment(
            [
                egui::pos2(rect.left(), rect.bottom()),
                egui::pos2(rect.right(), rect.bottom()),
            ],
            egui::Stroke::new(1.0, ui.visuals().window_stroke.color),
        );

        if let Some(pos) = resp.interact_pointer_pos() {
            if resp.clicked() {
                let clicked = if pos.x < c.size_l {
                    SortCol::Name
                } else if pos.x < c.mod_l {
                    SortCol::Size
                } else if pos.x < c.type_l {
                    SortCol::Modified
                } else {
                    SortCol::Type
                };
                let p = self.pane_mut(side);
                if p.sort == clicked {
                    p.desc = !p.desc;
                } else {
                    p.sort = clicked;
                    p.desc = false;
                }
            }
        }
    }

    fn render_middle(&mut self, ui: &mut egui::Ui) {
        let t = ui.input(|i| i.time);
        ui.ctx().request_repaint(); // keep the arrows animating

        ui.add_space(46.0);
        let r1 = copy_control(ui, true, t, "L -> R");
        if r1.clicked() {
            self.copy_selection(Side::Left, Side::Right);
        }
        ui.add_space(8.0);
        let r2 = copy_control(ui, false, t, "R -> L");
        if r2.clicked() {
            self.copy_selection(Side::Right, Side::Left);
        }

        ui.add_space(18.0);
        let bw = egui::vec2(MID_W - 24.0, 26.0);
        if ui
            .add_sized(bw, egui::Button::new("Delete / Undelete"))
            .clicked()
        {
            let a = self.active;
            self.toggle_delete_selection(a);
        }
        if ui.add_sized(bw, egui::Button::new("New Folder")).clicked() {
            let a = self.active;
            self.new_folder = Some(NewFolder {
                side: a,
                name: "NEW FOLDER".into(),
            });
        }
        if ui
            .add_sized(bw, egui::Button::new("Details / Info"))
            .clicked()
        {
            let a = self.active;
            let names = self.pane(a).selected.clone();
            match names.len() {
                0 => self.status = "Select a file first to view details.".into(),
                1 => self.open_detail(a, &names[0]),
                k => self.status = format!("Details shows one item; {k} selected."),
            }
        }
        ui.small(format!("active pane: {}", side_label(self.active)));

        ui.add_space(14.0);
        ui.add_enabled(
            false,
            egui::Button::new("Compare\n(later)").min_size(egui::vec2(MID_W - 24.0, 30.0)),
        );
    }

    fn render_detail(&mut self, ctx: &egui::Context) {
        let Some(d) = self.detail.as_mut() else {
            return;
        };
        let mut open = true;
        let mut stage_type_creator = false;
        let mut export_msg: Option<String> = None;
        egui::Window::new(format!("File Info: {}", d.name))
            .open(&mut open)
            .resizable(true)
            .default_width(380.0)
            .show(ctx, |ui| {
                egui::Grid::new("detail_grid")
                    .num_columns(2)
                    .striped(true)
                    .show(ui, |ui| {
                        meta_row(ui, "Name", &d.name);
                        meta_row(ui, "Filesystem", &d.fs);
                        meta_row(ui, "Size", &human_size(d.size));
                        meta_row(ui, "Modified", &d.modified);
                        match &d.meta {
                            Meta::Fat { attrs } => {
                                meta_row(ui, "Attributes", &fat_attr_string(*attrs));
                                ui.label("");
                                ui.weak("(FAT attribute editing: not in v1 -- see inventory)");
                                ui.end_row();
                            }
                            Meta::Hfs { rsrc, .. } => {
                                meta_row(ui, "Resource fork", &human_size(*rsrc));
                            }
                            Meta::Unix { mode, uid, gid } => {
                                meta_row(ui, "Mode", &format!("{:04o}", mode & 0o7777));
                                meta_row(ui, "Owner", &format!("{uid}:{gid}"));
                            }
                            Meta::Host | Meta::Plain => {}
                        }
                    });

                if matches!(d.meta, Meta::Hfs { .. }) {
                    ui.separator();
                    ui.label("Edit (staged):");
                    ui.horizontal(|ui| {
                        ui.label("Type");
                        ui.add(
                            egui::TextEdit::singleline(&mut d.edit_type)
                                .desired_width(56.0)
                                .char_limit(4),
                        );
                        ui.label("Creator");
                        ui.add(
                            egui::TextEdit::singleline(&mut d.edit_creator)
                                .desired_width(56.0)
                                .char_limit(4),
                        );
                    });
                    if ui.button("Stage type/creator change").clicked() {
                        stage_type_creator = true;
                    }
                }

                ui.separator();
                ui.label("Export:");
                ui.horizontal_wrapped(|ui| {
                    let one = [d.name.clone()];
                    if ui.button("To host...").clicked() {
                        export_msg = Some(export_status(&one, "host"));
                    }
                    if ui.button("Archive .zip").clicked() {
                        export_msg = Some(export_status(&one, "zip"));
                    }
                    if ui.button("Archive .tgz").clicked() {
                        export_msg = Some(export_status(&one, "tgz"));
                    }
                    if matches!(d.meta, Meta::Hfs { .. }) && ui.button("Archive .sit").clicked() {
                        export_msg = Some(export_status(&one, "sit"));
                    }
                });

                ui.separator();
                ui.collapsing("Hex preview", |ui| {
                    ui.monospace(fake_hex(&d.name));
                });
            });

        let dname = d.name.clone();
        if stage_type_creator {
            self.status = format!("Staged type/creator change for '{dname}' (mock).");
        }
        if let Some(m) = export_msg {
            self.status = m;
        }
        if !open {
            self.detail = None;
        }
    }

    fn render_new_folder(&mut self, ctx: &egui::Context) {
        let Some(nf) = self.new_folder.as_mut() else {
            return;
        };
        let mut create = false;
        let mut cancel = false;
        egui::Window::new("New Folder")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                ui.label(format!(
                    "Create a folder in the {} pane:",
                    side_label(nf.side)
                ));
                ui.add(egui::TextEdit::singleline(&mut nf.name).desired_width(220.0));
                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    if ui.button("Create").clicked() {
                        create = true;
                    }
                    if ui.button("Cancel").clicked() {
                        cancel = true;
                    }
                });
            });
        if create {
            let side = nf.side;
            let name = nf.name.trim().to_string();
            if !name.is_empty() {
                self.pane_mut(side)
                    .staged_adds
                    .push(dir(&name, "now", vec![]));
                self.status = format!(
                    "Staged new folder '{name}' ({}). Use Apply to write.",
                    side_label(side)
                );
            }
            self.new_folder = None;
        }
        if cancel {
            self.new_folder = None;
        }
    }

    fn render_unsaved(&mut self, ctx: &egui::Context) {
        if !self.unsaved {
            return;
        }
        let n = self.left.staged_count() + self.right.staged_count();
        let mut choice: Option<&'static str> = None;
        egui::Window::new("Unsaved Changes")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                ui.label(format!(
                    "You have {n} staged change(s) that have not been written to disk."
                ));
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if ui.button("Discard & Close").clicked() {
                        choice = Some("discard");
                    }
                    if ui.button("Apply & Close").clicked() {
                        choice = Some("apply");
                    }
                    if ui.button("Cancel").clicked() {
                        choice = Some("cancel");
                    }
                });
            });
        match choice {
            Some("cancel") => self.unsaved = false,
            Some(_) => {
                self.left.staged_adds.clear();
                self.left.staged_dels.clear();
                self.right.staged_adds.clear();
                self.right.staged_dels.clear();
                self.unsaved = false;
                ctx.send_viewport_cmd(egui::ViewportCommand::Close);
            }
            None => {}
        }
    }
}

/// Build a row's right-click context menu, recording the chosen action.
/// `sel_count` is how many rows the action will affect.
fn row_menu(
    ui: &mut egui::Ui,
    row: &DisplayRow,
    other: Side,
    sel_count: usize,
    m_copy: &mut bool,
    m_toggle_del: &mut bool,
    m_detail: &mut bool,
    m_export: &mut Option<&'static str>,
) {
    let plural = |word: &str| {
        if sel_count > 1 {
            format!("{word} {sel_count} items")
        } else {
            word.to_string()
        }
    };
    match row.kind {
        RowKind::PendingAdd => {
            ui.weak("(staged copy / new)");
            if ui.button("Remove from staging").clicked() {
                *m_toggle_del = true;
                ui.close();
            }
        }
        RowKind::PendingDel => {
            ui.weak("(staged for delete)");
            if ui.button(plural("Undelete")).clicked() {
                *m_toggle_del = true;
                ui.close();
            }
            if ui.button("Info / Details").clicked() {
                *m_detail = true;
                ui.close();
            }
        }
        _ => {
            if ui
                .button(format!("{} to {} pane", plural("Copy"), side_label(other)))
                .clicked()
            {
                *m_copy = true;
                ui.close();
            }
            if ui.button(plural("Delete")).clicked() {
                *m_toggle_del = true;
                ui.close();
            }
            if ui.button("Info / Details").clicked() {
                *m_detail = true;
                ui.close();
            }
            ui.menu_button("Export...", |ui| {
                if ui.button("To host...").clicked() {
                    *m_export = Some("host");
                    ui.close();
                }
                if ui.button("Archive to .zip").clicked() {
                    *m_export = Some("zip");
                    ui.close();
                }
                if ui.button("Archive to .tar.gz").clicked() {
                    *m_export = Some("tgz");
                    ui.close();
                }
                if ui.button("Archive to .sit (Mac)").clicked() {
                    *m_export = Some("sit");
                    ui.close();
                }
            });
        }
    }
}

fn export_status(names: &[String], kind: &str) -> String {
    let what = match names {
        [] => "(nothing selected)".to_string(),
        [one] => format!("'{one}'"),
        _ => format!("{} items", names.len()),
    };
    match kind {
        "host" => format!("Would export {what} to a host folder."),
        "zip" => format!("Would archive {what} to .zip (zip crate)."),
        "tgz" => format!("Would archive {what} to .tar.gz (tar + flate2)."),
        "sit" => format!("Would archive {what} to .sit (built-in StuffIt writer)."),
        _ => format!("Export {what}."),
    }
}

// ----------------------------------------------------------------------------
// Custom-painted copy control (icon + animated arrow)
// ----------------------------------------------------------------------------

fn copy_control(ui: &mut egui::Ui, dir_right: bool, t: f64, caption: &str) -> egui::Response {
    let size = egui::vec2(MID_W - 24.0, 50.0);
    let (rect, resp) = ui.allocate_exact_size(size, egui::Sense::click());
    let vis = ui.visuals();
    let bg = if resp.hovered() {
        vis.widgets.hovered.bg_fill
    } else {
        vis.widgets.inactive.bg_fill
    };
    let border = vis.widgets.inactive.fg_stroke.color;
    let icon_color = vis.strong_text_color();
    let cap_color = vis.weak_text_color();

    let p = ui.painter_at(rect);
    p.rect_filled(rect, egui::CornerRadius::same(4), bg);
    p.rect_stroke(
        rect,
        egui::CornerRadius::same(4),
        egui::Stroke::new(1.0, border),
        egui::StrokeKind::Inside,
    );

    paint_copy_icon(
        &p,
        egui::pos2(rect.center().x, rect.top() + 13.0),
        icon_color,
        bg,
    );

    let strip = egui::Rect::from_min_max(
        egui::pos2(rect.left() + 8.0, rect.bottom() - 22.0),
        egui::pos2(rect.right() - 8.0, rect.bottom() - 12.0),
    );
    paint_arrow(&ui.painter_at(strip), strip, dir_right, t);

    ui.painter().text(
        egui::pos2(rect.center().x, rect.bottom() - 5.0),
        egui::Align2::CENTER_CENTER,
        caption,
        egui::FontId::proportional(9.0),
        cap_color,
    );
    resp
}

fn paint_copy_icon(p: &egui::Painter, c: egui::Pos2, fg: egui::Color32, bg: egui::Color32) {
    let (w, h) = (11.0, 14.0);
    let back = egui::Rect::from_min_size(
        egui::pos2(c.x - w / 2.0 + 3.0, c.y - h / 2.0 - 3.0),
        egui::vec2(w, h),
    );
    p.rect_stroke(
        back,
        egui::CornerRadius::same(2),
        egui::Stroke::new(1.0, fg),
        egui::StrokeKind::Inside,
    );
    let front = egui::Rect::from_min_size(
        egui::pos2(c.x - w / 2.0 - 1.0, c.y - h / 2.0 + 3.0),
        egui::vec2(w, h),
    );
    p.rect_filled(front, egui::CornerRadius::same(2), bg);
    p.rect_stroke(
        front,
        egui::CornerRadius::same(2),
        egui::Stroke::new(1.2, fg),
        egui::StrokeKind::Inside,
    );
}

fn paint_arrow(p: &egui::Painter, strip: egui::Rect, right: bool, t: f64) {
    let s = 11.0_f32;
    let w = 6.0;
    let hh = 3.5;
    let mid = strip.center().y;
    let phase = ((t * 26.0) as f32).rem_euclid(s);
    let n = (strip.width() / s).ceil() as i32 + 2;
    for k in 0..n {
        let x = if right {
            strip.left() - s + phase + k as f32 * s
        } else {
            strip.right() + s - phase - k as f32 * s
        };
        let frac = ((x - strip.left()) / strip.width()).clamp(0.0, 1.0);
        let a = (200.0 * (1.0 - (frac - 0.5).abs() * 1.3)).clamp(40.0, 200.0) as u8;
        let col = egui::Color32::from_rgba_unmultiplied(120, 180, 255, a);
        let st = egui::Stroke::new(1.6, col);
        if right {
            p.line_segment(
                [
                    egui::pos2(x - w / 2.0, mid - hh),
                    egui::pos2(x + w / 2.0, mid),
                ],
                st,
            );
            p.line_segment(
                [
                    egui::pos2(x + w / 2.0, mid),
                    egui::pos2(x - w / 2.0, mid + hh),
                ],
                st,
            );
        } else {
            p.line_segment(
                [
                    egui::pos2(x + w / 2.0, mid - hh),
                    egui::pos2(x - w / 2.0, mid),
                ],
                st,
            );
            p.line_segment(
                [
                    egui::pos2(x - w / 2.0, mid),
                    egui::pos2(x + w / 2.0, mid + hh),
                ],
                st,
            );
        }
    }
}

fn paint_row(ui: &egui::Ui, rect: egui::Rect, row: &DisplayRow, _selected: bool) {
    let c = cols(rect);
    let mid = rect.center().y;
    let font = egui::FontId::proportional(13.0);
    let base = ui.visuals().text_color();
    let color = match row.kind {
        RowKind::Parent => ui.visuals().weak_text_color(),
        RowKind::PendingAdd => egui::Color32::from_rgb(90, 180, 90),
        RowKind::PendingDel => egui::Color32::from_rgb(150, 150, 150),
        RowKind::Normal if row.is_dir => egui::Color32::from_rgb(120, 160, 255),
        RowKind::Normal => base,
    };

    let display_name = match row.kind {
        RowKind::PendingAdd => format!("+ {}", row.name),
        RowKind::PendingDel => format!("- {}", row.name),
        _ if row.is_dir && row.name != ".." => format!("{}/", row.name),
        _ => row.name.clone(),
    };

    let name_cell = egui::Rect::from_min_max(
        egui::pos2(c.name_l, rect.top()),
        egui::pos2(c.name_r, rect.bottom()),
    );
    ui.painter_at(name_cell).text(
        egui::pos2(c.name_l, mid),
        egui::Align2::LEFT_CENTER,
        display_name,
        font.clone(),
        color,
    );

    if !row.is_dir {
        ui.painter().text(
            egui::pos2(c.size_r, mid),
            egui::Align2::RIGHT_CENTER,
            human_size(row.size),
            font.clone(),
            color,
        );
    }
    ui.painter().text(
        egui::pos2(c.mod_l, mid),
        egui::Align2::LEFT_CENTER,
        row.modified.clone(),
        font.clone(),
        ui.visuals().weak_text_color(),
    );
    ui.painter().text(
        egui::pos2(c.type_l, mid),
        egui::Align2::LEFT_CENTER,
        row.type_tag.clone(),
        font,
        ui.visuals().weak_text_color(),
    );

    if matches!(row.kind, RowKind::PendingDel) {
        ui.painter().line_segment(
            [egui::pos2(c.name_l, mid), egui::pos2(c.name_r, mid)],
            egui::Stroke::new(1.0, color),
        );
    }
}

fn meta_row(ui: &mut egui::Ui, k: &str, v: &str) {
    ui.label(k);
    ui.monospace(v);
    ui.end_row();
}

fn fat_attr_string(attrs: u8) -> String {
    let mut s = vec![];
    if attrs & 0x20 != 0 {
        s.push("archive");
    }
    if attrs & 0x02 != 0 {
        s.push("hidden");
    }
    if attrs & 0x04 != 0 {
        s.push("system");
    }
    if attrs & 0x01 != 0 {
        s.push("read-only");
    }
    if s.is_empty() {
        "(none)".into()
    } else {
        s.join(", ")
    }
}

fn fake_hex(name: &str) -> String {
    let bytes: Vec<u8> = name
        .bytes()
        .chain(b" -- mock file contents".iter().copied())
        .collect();
    let mut out = String::new();
    for (i, chunk) in bytes.chunks(16).enumerate().take(8) {
        let mut hex = String::new();
        let mut asc = String::new();
        for b in chunk {
            hex.push_str(&format!("{b:02X} "));
            asc.push(if b.is_ascii_graphic() || *b == b' ' {
                *b as char
            } else {
                '.'
            });
        }
        out.push_str(&format!("{:08X}  {:<48} |{}|\n", i * 16, hex, asc));
    }
    out
}
