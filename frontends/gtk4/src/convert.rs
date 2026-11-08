//! The Convert tab over `controller::convert`; `src/gui/convert_tab.rs` is the reference.
//!
//! One page of adw rows: the source and output, each kind's format and extension (a caption under
//! each says what the format is for), a CHD control per CHD format, hard disk sizing, the floppy
//! size, verify, the file list and Convert. A strip under the header shows a running conversion
//! from any tab.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use gtk::{gio, glib};
use rusty_backup::controller::app::chd::{ChdOptionsControl, ChdProfile};
use rusty_backup::controller::app::{busy_elsewhere_notice, Notify, Snapshot};
use rusty_backup::controller::convert::{
    floppy_size_choices, formats_for, hints, ids, kind_ids, ChdChoices, ConvertAction,
    ConvertController, ExtraUnit, FloppySize, Format, ImageKind, Row as FileRow, SizeChoice,
    ONLY_FILES,
};
use rusty_backup::controller::{Controller, Effect};

use crate::chd::{ChdControl, ChdIds};
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::{
    button, combo, group, line, page, path_row, set_combo, set_entry, set_line, set_spin,
    set_subtitle, set_switch, spin, switch,
};
use crate::progress::JobList;
use crate::sync::SyncGuard;
use crate::widgets::{plain, set_text};

const PROFILES: [ChdProfile; 3] = [ChdProfile::Hd, ChdProfile::Cd, ChdProfile::Dvd];

/// A kind's format and extension rows and the caption under them.
struct KindRows {
    kind: ImageKind,
    group: adw::PreferencesGroup,
    format: adw::ComboRow,
    extension: adw::EntryRow,
    caption: gtk::Label,
}

/// What `refresh` reads from the controller before it touches a widget.
struct Shown {
    source: String,
    folder: bool,
    recursive: bool,
    has_source: bool,
    output_label: &'static str,
    output: String,
    output_hint: &'static str,
    kinds: Vec<ImageKind>,
    formats: Vec<(ImageKind, Format, String)>,
    chd: Vec<ChdProfile>,
    multi_bin_shown: bool,
    multi_bin: bool,
    size_choice: SizeChoice,
    extra: u32,
    unit: ExtraUnit,
    measure_slow: bool,
    measure_enabled: bool,
    floppy_size: FloppySize,
    verify: bool,
    rows: Vec<FileRow>,
    summary: String,
    scan: Option<Snapshot>,
    scan_error: Option<String>,
    run: Option<Snapshot>,
    running: bool,
    problem: Option<String>,
    start_label: String,
    strip: Option<(f32, String)>,
}

pub struct ConvertScreen {
    ctrl: RefCell<ConvertController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<ConvertScreen>,
    busy: Cell<Option<&'static str>>,
    root: gtk::ScrolledWindow,
    source: adw::ActionRow,
    recursive: adw::SwitchRow,
    output: adw::ActionRow,
    output_button: gtk::Button,
    scan_jobs: JobList,
    scan_error: gtk::Label,
    kinds: Vec<KindRows>,
    chd: Vec<Rc<ChdControl>>,
    multi_bin: adw::SwitchRow,
    multi_bin_group: adw::PreferencesGroup,
    size_group: adw::PreferencesGroup,
    sizing: adw::ComboRow,
    sizing_help: gtk::Label,
    extra: adw::SpinRow,
    unit: adw::ComboRow,
    measure_slow: adw::SwitchRow,
    floppy_group: adw::PreferencesGroup,
    floppy_size: adw::ComboRow,
    floppy_help: gtk::Label,
    verify_group: adw::PreferencesGroup,
    verify: adw::SwitchRow,
    files_group: adw::PreferencesGroup,
    summary: gtk::Label,
    store: gio::ListStore,
    /// The rows in the store, so a tick only rebinds its own row.
    listed: RefCell<Vec<FileRow>>,
    start: gtk::Button,
    cancel: gtk::Button,
    problem: gtk::Label,
    run_jobs: JobList,
    strip: gtk::Revealer,
    strip_bar: gtk::ProgressBar,
}

impl ConvertScreen {
    pub fn build(notify: Arc<dyn Notify>, host: EffectHost) -> Rc<ConvertScreen> {
        let content = gtk::Box::new(gtk::Orientation::Vertical, 18);
        let intro = gtk::Label::builder()
            .label(hints::INTRO)
            .xalign(0.0)
            .wrap(true)
            .build();
        content.append(&intro);

        let source_group = group("Source");
        let pick_file = button("Choose Image...", ids::SOURCE_FILE);
        let pick_folder = button("Choose Folder...", ids::SOURCE_FOLDER);
        pick_folder.set_valign(gtk::Align::Center);
        let source = path_row("Image or folder", &pick_file);
        source.add_suffix(&pick_folder);
        source_group.add(&source);
        let recursive = switch("Include subfolders", ids::RECURSIVE);
        recursive.set_subtitle(hints::RECURSIVE);
        source_group.add(&recursive);
        let output_button = button("Choose...", ids::OUTPUT);
        let output = path_row("Save as", &output_button);
        source_group.add(&output);
        let scan_jobs = JobList::new(ids::CANCEL, Rc::new(|| {}));
        source_group.add(scan_jobs.root());
        let scan_error = line();
        scan_error.add_css_class("error");
        source_group.add(&scan_error);
        content.append(&source_group);

        let kinds: Vec<KindRows> = ImageKind::ALL
            .iter()
            .map(|&kind| {
                let (format_id, ext_id) = kind_ids(kind);
                let g = group(&format!("{}s", kind.label()));
                let labels: Vec<String> =
                    formats_for(kind).iter().map(|f| format_label(*f)).collect();
                let refs: Vec<&str> = labels.iter().map(String::as_str).collect();
                let format = combo("Format", format_id, &refs);
                let extension = adw::EntryRow::builder().title("Extension").build();
                extension.set_widget_name(ext_id);
                extension.set_tooltip_text(Some(hints::EXTENSION));
                let caption = caption();
                g.add(&format);
                g.add(&extension);
                g.add(&caption);
                content.append(&g);
                KindRows {
                    kind,
                    group: g,
                    format,
                    extension,
                    caption,
                }
            })
            .collect();

        let chd_box = gtk::Box::new(gtk::Orientation::Vertical, 12);
        content.append(&chd_box);
        let multi_bin = switch("One .bin per track", ids::MULTI_BIN);
        multi_bin.set_subtitle(hints::MULTI_BIN);
        let mb_group = group("");
        mb_group.add(&multi_bin);
        content.append(&mb_group);

        let size_group = group("Hard disk size");
        let size_labels: Vec<&str> = SizeChoice::ALL.iter().map(|c| c.label()).collect();
        let sizing = combo("Size", ids::SIZING, &size_labels);
        let sizing_help = caption();
        let extra = spin(
            "Extra on every partition",
            ids::EXTRA_AMOUNT,
            0.0,
            1_000_000.0,
        );
        extra.set_subtitle(hints::EXTRA);
        extra.set_subtitle_lines(0);
        let unit_labels: Vec<&str> = ExtraUnit::ALL.iter().map(|u| u.label()).collect();
        let unit = combo("Unit", ids::EXTRA_UNIT, &unit_labels);
        let measure_slow = switch("Measure slow filesystems too", ids::MEASURE_SLOW);
        measure_slow.set_subtitle(&hints::measure_slow());
        measure_slow.set_subtitle_lines(0);
        size_group.add(&sizing);
        size_group.add(&sizing_help);
        size_group.add(&extra);
        size_group.add(&unit);
        size_group.add(&measure_slow);
        content.append(&size_group);

        let floppy_group = group("Floppy size");
        let sizes: Vec<String> = floppy_size_choices().iter().map(|s| s.label()).collect();
        let size_refs: Vec<&str> = sizes.iter().map(String::as_str).collect();
        let floppy_size = combo("Size", ids::FLOPPY_SIZE, &size_refs);
        let floppy_help = caption();
        floppy_group.add(&floppy_size);
        floppy_group.add(&floppy_help);
        content.append(&floppy_group);

        let verify_group = group("");
        let verify = switch("Verify after converting", ids::VERIFY);
        verify.set_subtitle(hints::VERIFY);
        verify.set_subtitle_lines(0);
        verify_group.add(&verify);
        content.append(&verify_group);

        let files_group = group("Files");
        let summary = gtk::Label::builder().xalign(0.0).build();
        let all = button("Select All", ids::SELECT_ALL);
        let none = button("Select None", ids::SELECT_NONE);
        let picks = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        picks.append(&summary);
        let spacer = gtk::Box::builder().hexpand(true).build();
        picks.append(&spacer);
        picks.append(&all);
        picks.append(&none);
        files_group.add(&picks);
        let store = gio::ListStore::new::<glib::BoxedAnyObject>();
        content.append(&files_group);

        let start = button("Convert", ids::START);
        start.add_css_class("suggested-action");
        let cancel = button("Cancel", ids::CANCEL);
        let problem = line();
        problem.add_css_class("dim-label");
        let run_jobs = JobList::new(ids::CANCEL, Rc::new(|| {}));
        let actions = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        actions.append(&start);
        actions.append(&cancel);
        actions.append(&problem);
        content.append(&actions);
        content.append(run_jobs.root());

        let strip_bar = gtk::ProgressBar::builder()
            .show_text(true)
            .hexpand(true)
            .valign(gtk::Align::Center)
            .build();
        let strip_cancel = button("Cancel Convert", ids::CANCEL);
        let strip_row = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        strip_row.set_margin_start(8);
        strip_row.set_margin_end(8);
        strip_row.append(&strip_bar);
        strip_row.append(&strip_cancel);
        let strip = gtk::Revealer::builder().child(&strip_row).build();

        let root = page(&content);
        let screen = Rc::new_cyclic(|me: &Weak<ConvertScreen>| {
            let changed: Rc<dyn Fn()> = {
                let me = me.clone();
                Rc::new(move || {
                    if let Some(s) = me.upgrade() {
                        s.refresh();
                    }
                })
            };
            let chd: Vec<Rc<ChdControl>> = PROFILES
                .iter()
                .map(|p| {
                    ChdControl::build(
                        &format!("{} CHD options", profile_name(*p)),
                        "",
                        ChdIds {
                            mode: ids::CHD_MODE,
                            hunk: ids::CHD_HUNK,
                            codec: ids::CHD_CODEC,
                        },
                        ChdOptionsControl::new(*p),
                        *p,
                        changed.clone(),
                    )
                })
                .collect();
            for c in &chd {
                chd_box.append(c.root());
            }
            ConvertScreen {
                ctrl: RefCell::new(ConvertController::new(notify)),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                busy: Cell::new(None),
                root,
                source,
                recursive,
                output,
                output_button,
                scan_jobs,
                scan_error,
                kinds,
                chd,
                multi_bin,
                multi_bin_group: mb_group,
                size_group,
                sizing,
                sizing_help,
                extra,
                unit,
                measure_slow,
                floppy_group,
                floppy_size,
                floppy_help,
                verify_group,
                verify,
                files_group,
                summary,
                store,
                listed: RefCell::default(),
                start,
                cancel,
                problem,
                run_jobs,
                strip,
                strip_bar,
            }
        });
        screen.files_group.add(&screen.file_list());
        screen.connect(&pick_file, &pick_folder, &all, &none, &strip_cancel);
        screen.refresh();
        screen
    }

    pub fn root(&self) -> &gtk::ScrolledWindow {
        &self.root
    }

    /// The progress strip, for the window's top bars.
    pub fn strip(&self) -> &gtk::Revealer {
        &self.strip
    }

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    pub fn set_busy(&self, busy: Option<&'static str>) {
        if self.busy.get() != busy {
            self.busy.set(busy);
            self.refresh();
        }
    }

    /// Load a file or folder handed over by Inspect, Optical or a drop.
    pub fn open(&self, path: PathBuf) {
        self.apply(ConvertAction::SetSource(path));
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    fn apply(&self, action: ConvertAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let lines = self.ctrl.borrow_mut().take_log();
        crate::app::append_log(lines);
        self.refresh();
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(s) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => ConvertAction::Picked { purpose, path },
                Reply::Chose { purpose, choice } => ConvertAction::Chose {
                    purpose,
                    index: choice,
                },
                Reply::Confirmed { .. } | Reply::Answered { .. } | Reply::CloseScreen => return,
            };
            s.apply(action);
        })
    }

    /// The action closure the handlers call; it drops actions while a sync runs.
    fn sender(&self) -> Rc<dyn Fn(ConvertAction)> {
        let me = self.me.clone();
        Rc::new(move |action| {
            if let Some(s) = me.upgrade().filter(|s| !s.sync.active()) {
                s.apply(action);
            }
        })
    }

    fn connect(
        &self,
        pick_file: &gtk::Button,
        pick_folder: &gtk::Button,
        all: &gtk::Button,
        none: &gtk::Button,
        strip_cancel: &gtk::Button,
    ) {
        let send = self.sender();
        let on = |b: &gtk::Button, action: fn() -> ConvertAction| {
            let s = send.clone();
            b.connect_clicked(move |_| s(action()));
        };
        on(pick_file, || ConvertAction::PickSourceFile);
        on(pick_folder, || ConvertAction::PickSourceFolder);
        on(&self.output_button, || ConvertAction::PickOutput);
        on(all, || ConvertAction::SelectAll);
        on(none, || ConvertAction::SelectNone);
        on(&self.cancel, || ConvertAction::Cancel);
        on(strip_cancel, || ConvertAction::Cancel);
        let s = send.clone();
        self.recursive
            .connect_active_notify(move |r| s(ConvertAction::SetRecursive(r.is_active())));
        for k in &self.kinds {
            let (s, kind) = (send.clone(), k.kind);
            k.format.connect_selected_notify(move |row| {
                if let Some(f) = formats_for(kind).get(row.selected() as usize) {
                    s(ConvertAction::SetFormat(kind, *f));
                }
            });
            let s = send.clone();
            k.extension
                .connect_changed(move |e| s(ConvertAction::SetExtension(kind, e.text().into())));
        }
        let s = send.clone();
        self.multi_bin
            .connect_active_notify(move |r| s(ConvertAction::SetMultiBin(r.is_active())));
        let s = send.clone();
        self.sizing.connect_selected_notify(move |row| {
            if let Some(c) = SizeChoice::ALL.get(row.selected() as usize) {
                s(ConvertAction::SetSizing(*c));
            }
        });
        let s = send.clone();
        self.extra
            .connect_value_notify(move |r| s(ConvertAction::SetExtraAmount(r.value() as u32)));
        let s = send.clone();
        self.unit.connect_selected_notify(move |row| {
            if let Some(u) = ExtraUnit::ALL.get(row.selected() as usize) {
                s(ConvertAction::SetExtraUnit(*u));
            }
        });
        let s = send.clone();
        self.measure_slow
            .connect_active_notify(move |r| s(ConvertAction::SetMeasureSlow(r.is_active())));
        let s = send.clone();
        self.floppy_size.connect_selected_notify(move |row| {
            if let Some(f) = floppy_size_choices().get(row.selected() as usize) {
                s(ConvertAction::SetFloppySize(*f));
            }
        });
        let s = send.clone();
        self.verify
            .connect_active_notify(move |r| s(ConvertAction::SetVerify(r.is_active())));
        let me = self.me.clone();
        self.start.connect_clicked(move |_| {
            let Some(screen) = me.upgrade().filter(|s| !s.sync.active()) else {
                return;
            };
            if !screen.ctrl.borrow().can_start() || screen.busy.get().is_some() {
                return;
            }
            let chd = screen.chd_choices();
            screen.apply(ConvertAction::Start(chd));
        });
    }

    fn chd_choices(&self) -> ChdChoices {
        let mut out = ChdChoices::default();
        let profiles: Vec<ChdProfile> = self
            .ctrl
            .borrow()
            .chd_profiles()
            .into_iter()
            .map(|(_, p)| p)
            .collect();
        for (control, p) in self.chd.iter().zip(PROFILES) {
            if !profiles.contains(&p) {
                continue;
            }
            let opts = Some(control.choice().0);
            match p {
                ChdProfile::Hd => out.hd = opts,
                ChdProfile::Cd => out.cd = opts,
                ChdProfile::Dvd => out.dvd = opts,
            }
        }
        out
    }

    fn read(&self) -> Shown {
        let c = self.ctrl.borrow();
        let kinds = c.shown_kinds();
        Shown {
            source: ConvertController::path_text(c.source()),
            folder: c.source_is_folder(),
            recursive: c.recursive(),
            has_source: c.source().is_some(),
            output_label: c.output_label(),
            output: ConvertController::path_text(c.output()),
            output_hint: c.output_hint(),
            formats: ImageKind::ALL
                .iter()
                .map(|&k| (k, c.format(k), c.extension(k).to_string()))
                .collect(),
            kinds,
            chd: c.chd_profiles().into_iter().map(|(_, p)| p).collect(),
            multi_bin_shown: c.multi_bin_shown(),
            multi_bin: c.multi_bin(),
            size_choice: c.size_choice(),
            extra: c.extra_amount(),
            unit: c.extra_unit(),
            measure_slow: c.measure_slow(),
            measure_enabled: c.measure_slow_enabled(),
            floppy_size: c.floppy_size(),
            verify: c.verify(),
            rows: c.rows(),
            summary: c.summary(),
            scan: c.scan_snapshot(),
            scan_error: c.scan_error().map(str::to_string),
            run: c.run_snapshot(),
            running: c.is_running(),
            problem: self
                .busy
                .get()
                .map(busy_elsewhere_notice)
                .or_else(|| c.start_problem()),
            start_label: c.start_label(),
            strip: c.strip(),
        }
    }

    fn refresh(&self) {
        let s = self.read();
        self.sync.run(|| self.show(&s));
    }

    fn show(&self, s: &Shown) {
        self.strip.set_reveal_child(s.strip.is_some());
        if let Some((fraction, text)) = &s.strip {
            self.strip_bar.set_fraction(f64::from(*fraction));
            self.strip_bar.set_text(Some(&plain(text)));
        }
        set_subtitle(&self.source, &s.source);
        self.recursive.set_visible(s.folder);
        set_switch(&self.recursive, s.recursive);
        self.output.set_visible(s.has_source);
        if self.output.title() != s.output_label {
            self.output.set_title(s.output_label);
        }
        set_subtitle(&self.output, &format!("{}\n{}", s.output, s.output_hint));
        self.output.set_subtitle_lines(0);
        let scans: Vec<(Snapshot, bool)> = s.scan.iter().map(|j| (j.clone(), false)).collect();
        self.scan_jobs.show(&scans);
        set_line(&self.scan_error, s.scan_error.as_deref());

        for k in &self.kinds {
            let shown = s.kinds.contains(&k.kind);
            k.group.set_visible(shown);
            let Some((_, format, ext)) = s.formats.iter().find(|f| f.0 == k.kind) else {
                continue;
            };
            let at = formats_for(k.kind)
                .iter()
                .position(|f| f == format)
                .unwrap_or(0);
            set_combo(&k.format, at as u32);
            if !k.extension.has_focus() {
                set_entry(&k.extension, ext);
            }
            let help = match format.unavailable() {
                Some(why) => format!("Not in this version of Rusty Backup: {why}"),
                None => format.hint().to_string(),
            };
            set_line(&k.caption, Some(help.as_str()));
        }
        for (control, p) in self.chd.iter().zip(PROFILES) {
            control.root().set_visible(s.chd.contains(&p));
        }
        self.multi_bin_group.set_visible(s.multi_bin_shown);
        set_switch(&self.multi_bin, s.multi_bin);

        self.size_group
            .set_visible(s.kinds.contains(&ImageKind::HardDisk));
        let at = SizeChoice::ALL
            .iter()
            .position(|c| *c == s.size_choice)
            .unwrap_or(0);
        set_combo(&self.sizing, at as u32);
        set_line(&self.sizing_help, Some(hints::SIZING));
        let extra = s.size_choice == SizeChoice::Extra;
        self.extra.set_visible(extra);
        self.unit.set_visible(extra);
        set_spin(&self.extra, f64::from(s.extra));
        let at = ExtraUnit::ALL
            .iter()
            .position(|u| *u == s.unit)
            .unwrap_or(0);
        set_combo(&self.unit, at as u32);
        self.measure_slow.set_sensitive(s.measure_enabled);
        set_switch(&self.measure_slow, s.measure_slow);

        self.floppy_group
            .set_visible(s.kinds.contains(&ImageKind::Floppy));
        let sizes = floppy_size_choices();
        let at = sizes.iter().position(|f| *f == s.floppy_size).unwrap_or(0);
        set_combo(&self.floppy_size, at as u32);
        let floppy_help = if s.floppy_size == FloppySize::Same {
            hints::FLOPPY_SIZE.to_string()
        } else {
            format!("{} {ONLY_FILES}", hints::FLOPPY_SIZE)
        };
        set_line(&self.floppy_help, Some(floppy_help.as_str()));

        self.verify_group.set_visible(!s.kinds.is_empty());
        set_switch(&self.verify, s.verify);

        self.files_group.set_visible(!s.rows.is_empty());
        set_text(&self.summary, &s.summary);
        self.show_rows(&s.rows);

        let chd_ok = self
            .chd
            .iter()
            .zip(PROFILES)
            .all(|(c, p)| !s.chd.contains(&p) || c.is_valid());
        self.start
            .set_sensitive(s.problem.is_none() && !s.running && chd_ok);
        if self.start.label().as_deref() != Some(s.start_label.as_str()) {
            self.start.set_label(&s.start_label);
        }
        self.cancel.set_visible(s.running);
        set_line(&self.problem, s.problem.as_deref().filter(|_| !s.running));
        let runs: Vec<(Snapshot, bool)> = s.run.iter().map(|j| (j.clone(), true)).collect();
        self.run_jobs.show(&runs);
    }

    /// A new list replaces the rows; a changed row replaces only itself.
    fn show_rows(&self, rows: &[FileRow]) {
        let mut listed = self.listed.borrow_mut();
        let same_files =
            listed.len() == rows.len() && listed.iter().zip(rows).all(|(a, b)| a.name == b.name);
        if !same_files {
            let boxed: Vec<glib::BoxedAnyObject> = rows
                .iter()
                .cloned()
                .enumerate()
                .map(|(i, r)| glib::BoxedAnyObject::new((i, r)))
                .collect();
            *listed = rows.to_vec();
            drop(listed);
            self.store.splice(0, self.store.n_items(), &boxed);
            return;
        }
        let changed: Vec<usize> = (0..rows.len()).filter(|i| listed[*i] != rows[*i]).collect();
        *listed = rows.to_vec();
        drop(listed);
        for i in changed {
            let obj = glib::BoxedAnyObject::new((i, rows[i].clone()));
            self.store.splice(i as u32, 1, &[obj]);
        }
    }

    fn file_list(&self) -> gtk::ScrolledWindow {
        let view = gtk::ColumnView::new(Some(gtk::NoSelection::new(Some(self.store.clone()))));
        view.set_widget_name(ids::FILES);
        view.set_show_row_separators(true);
        view.append_column(&self.tick_column());
        view.append_column(&text_column("File", true, |r| r.name.clone()));
        view.append_column(&text_column("Kind", false, |r| r.kind.clone()));
        view.append_column(&text_column("Read as", false, |r| match &r.note {
            Some(n) => format!("{} - {n}", r.detail),
            None => r.detail.clone(),
        }));
        view.append_column(&text_column("Result", true, |r| r.outcome.clone()));
        gtk::ScrolledWindow::builder()
            .child(&view)
            .min_content_height(200)
            .max_content_height(420)
            .propagate_natural_height(true)
            .build()
    }

    fn tick_column(&self) -> gtk::ColumnViewColumn {
        let factory = gtk::SignalListItemFactory::new();
        let send = self.sender();
        factory.connect_setup(move |_, obj| {
            let item = list_item(obj);
            let tick = gtk::CheckButton::new();
            tick.set_halign(gtk::Align::Center);
            let weak = item.downgrade();
            let send = send.clone();
            tick.connect_toggled(move |b| {
                let row = weak.upgrade().and_then(|i| row_of(&i));
                if let Some(row) = row {
                    let index = row.borrow::<(usize, FileRow)>().0;
                    send(ConvertAction::SetSelected(index, b.is_active()));
                }
            });
            item.set_child(Some(&tick));
        });
        let me = self.me.clone();
        factory.connect_bind(move |_, obj| {
            let item = list_item(obj);
            let (Some(row), Some(tick), Some(screen)) = (
                row_of(item),
                item.child().and_downcast::<gtk::CheckButton>(),
                me.upgrade(),
            ) else {
                return;
            };
            let on = row.borrow::<(usize, FileRow)>().1.selected;
            // A recycled cell takes this row's state with the toggle muted.
            screen.sync.run(|| tick.set_active(on));
        });
        gtk::ColumnViewColumn::new(None, Some(factory))
    }
}

fn profile_name(p: ChdProfile) -> &'static str {
    match p {
        ChdProfile::Hd => "Hard disk",
        ChdProfile::Cd => "CD",
        ChdProfile::Dvd => "DVD",
    }
}

/// A format's list label; one this build cannot write says so.
fn format_label(f: Format) -> String {
    match f.unavailable() {
        Some(_) => format!("{} (not in this version)", f.label()),
        None => f.label().to_string(),
    }
}

/// A help caption under a group's rows.
fn caption() -> gtk::Label {
    let label = line();
    label.add_css_class("dim-label");
    label.add_css_class("caption");
    label.set_margin_top(4);
    label
}

fn list_item(obj: &glib::Object) -> &gtk::ListItem {
    obj.downcast_ref().expect("a column factory's item")
}

fn row_of(item: &gtk::ListItem) -> Option<glib::BoxedAnyObject> {
    item.item().and_downcast::<glib::BoxedAnyObject>()
}

fn text_column(title: &str, expand: bool, cell: fn(&FileRow) -> String) -> gtk::ColumnViewColumn {
    let factory = gtk::SignalListItemFactory::new();
    factory.connect_setup(move |_, obj| {
        let label = gtk::Label::builder()
            .xalign(0.0)
            .ellipsize(gtk::pango::EllipsizeMode::Middle)
            .build();
        list_item(obj).set_child(Some(&label));
    });
    factory.connect_bind(move |_, obj| {
        let item = list_item(obj);
        let (Some(row), Some(label)) = (row_of(item), item.child().and_downcast::<gtk::Label>())
        else {
            return;
        };
        let text = cell(&row.borrow::<(usize, FileRow)>().1);
        label.set_tooltip_text(Some(&plain(&text)));
        set_text(&label, &text);
    });
    let column = gtk::ColumnViewColumn::new(Some(title), Some(factory));
    column.set_resizable(true);
    column.set_expand(expand);
    column
}
