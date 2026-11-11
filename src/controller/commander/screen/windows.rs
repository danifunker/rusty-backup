//! The windows a browse view opens over its listing, described once for every view.
//!
//! Each [`PaneWindow`] is a title, parts (lines, lists, choices, fields) and buttons that carry
//! the actions they send; a view draws it with its own toolkit and sends what a button holds.
//! The words and rules came out of the GTK shell's `browser/dialogs.rs`. A view keeps only what
//! is its own: the option a [`Part::Choice`] has picked, a [`Part::Toggle`] with no actions, and
//! what is typed in a [`Part::Field`], which [`field_action`] or [`submit`] turns into actions.
//! A window whose `key` is unchanged keeps its fields and takes the new text of its lines.

use std::path::Path;

use super::{
    BudgetAction, BudgetForm, BudgetMode, CommanderController, HashAlgo, ModeAction, Side,
    UnsavedChoice,
};
use crate::controller::browser::metadata::known_type_creators;
use crate::controller::browser::{MetadataFields, MetadataForm, TextDoc};
use crate::controller::commander::{
    format_size, journal_status, journal_summary, journal_txn_rows, ApplyPreflight, ApplyReport,
    BrowserPaneAction as P, Checksums, CommanderAction as A, JournalDetail, JournalTone,
    OnConflict, ToolReport, TreeText, MAX_CONSECUTIVE_FAILURES,
};
use crate::controller::commander::{
    import_prompt, journal_txn_blocks, ArchiveSave, ArchiveViewer, ForkFormat, ImportChoice,
    LineEnding, MacArchiveKind, RenameNotice, TarChoice, TarPreview, TextShape,
};

/// How a line reads: a plain line, or one that warns, reports a failure or a success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    Plain,
    Warning,
    Error,
    Success,
}

/// One piece of a window's body, top to bottom.
#[derive(Debug, Clone)]
pub enum Part {
    /// A wrapped line.
    Text(String, Tone),
    /// A short list of names, scrolled when long.
    List(Vec<String>),
    /// A block of text in a fixed-width font, scrolled.
    Block(String),
    /// Options of which one is picked; a [`Press::PerChoice`] button sends the picked one's
    /// actions, and `send`, when set, is what picking each sends at once.
    Choice {
        options: Vec<String>,
        picked: usize,
        send: Option<Vec<Vec<A>>>,
        /// A line shown under the options for the one picked; empty for none.
        notes: Vec<String>,
    },
    /// A drop-down list; picking an option sends its actions.
    Select {
        label: String,
        options: Vec<String>,
        picked: usize,
        send: Vec<Vec<A>>,
    },
    /// A field to type in; `live` sends [`field_action`] on every change.
    Field {
        id: FieldId,
        label: String,
        value: String,
        hint: String,
        secret: bool,
        multiline: bool,
        live: bool,
    },
    /// A button whose menu fills fields: each option sets `(field, value)` pairs.
    Picker {
        label: String,
        options: Vec<(String, Vec<(FieldId, String)>)>,
    },
    /// Lines with tick boxes (`None`: no box); ticking one sends its actions.
    Checklist {
        lines: Vec<String>,
        ticked: Vec<Option<bool>>,
        send: Vec<Vec<A>>,
    },
    /// A list whose selected line shows its detail text below it.
    Browse {
        lines: Vec<String>,
        details: Vec<String>,
    },
    /// A tick box; `send` is what turning it on and off sends, `None` when the view keeps it.
    Toggle {
        label: String,
        on: bool,
        send: Option<(Vec<A>, Vec<A>)>,
    },
    /// Lines that each carry a button labelled `button`, sending that line's actions.
    Items {
        lines: Vec<String>,
        button: String,
        actions: Vec<Vec<A>>,
        enabled: bool,
    },
}

/// What a button does.
#[derive(Debug, Clone)]
pub enum Press {
    Send(Vec<A>),
    /// The actions of the option picked in the window's [`Part::Choice`].
    PerChoice(Vec<Vec<A>>),
    /// Put the text on the clipboard.
    Copy(String),
    /// Hand the window's fields and its picked option to [`submit`].
    Submit(Form),
}

/// A field a window asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldId {
    BudgetAbsolute,
    BudgetHeadroom,
    Password,
    MetaType,
    MetaCreator,
    MetaModified,
    MetaCreated,
    MetaMode,
    MetaOwner,
    EditorText,
}

/// What a [`Press::Submit`] button sends its fields to.
#[derive(Debug, Clone)]
pub enum Form {
    Password,
    Metadata(Side),
    /// The text editor: the file it writes back, in which encoding.
    Editor {
        name: String,
        folder: String,
        shape: TextShape,
    },
}

/// The line endings the editor offers, in its choice's order.
const ENDINGS: [(LineEnding, &str); 3] = [
    (LineEnding::Lf, "LF (Unix)"),
    (LineEnding::CrLf, "CRLF (DOS)"),
    (LineEnding::Cr, "CR (classic Mac)"),
];

/// What a live field's text sends; `None` while it does not parse.
pub fn field_action(id: FieldId, text: &str) -> Option<A> {
    let mib = || text.trim().parse::<u64>().ok().filter(|n| *n > 0);
    let budget = |a| A::Mode(ModeAction::Budget(a));
    match id {
        FieldId::BudgetAbsolute => mib().map(|n| budget(BudgetAction::SetAbsoluteMib(n))),
        FieldId::BudgetHeadroom => mib().map(|n| budget(BudgetAction::SetHeadroomMib(n))),
        _ => None,
    }
}

/// What a form's Submit sends for its typed `values` and the option its choice has `picked`.
pub fn submit(form: &Form, values: &[(FieldId, String)], picked: usize) -> Vec<A> {
    let get = |id| {
        values
            .iter()
            .find(|(f, _)| *f == id)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    };
    match form {
        Form::Password => vec![A::Mode(ModeAction::Password(get(FieldId::Password)))],
        Form::Metadata(side) => {
            let fields = MetadataFields {
                type_code: get(FieldId::MetaType),
                creator: get(FieldId::MetaCreator),
                modified: get(FieldId::MetaModified),
                created: get(FieldId::MetaCreated),
                mode: get(FieldId::MetaMode),
                owner: get(FieldId::MetaOwner),
            };
            vec![pane(*side, P::SubmitMetadata(Box::new(fields)))]
        }
        Form::Editor {
            name,
            folder,
            shape,
        } => {
            let mut shape = *shape;
            shape.ending = ENDINGS.get(picked).map_or(shape.ending, |(e, _)| *e);
            vec![pane(
                Side::Left,
                P::SaveText {
                    name: name.clone(),
                    folder: folder.clone(),
                    text: get(FieldId::EditorText),
                    shape,
                },
            )]
        }
    }
}

#[derive(Debug, Clone)]
pub struct WindowButton {
    pub label: String,
    pub press: Press,
    pub enabled: bool,
    pub hint: Option<String>,
}

impl WindowButton {
    fn send(label: &str, actions: Vec<A>) -> Self {
        WindowButton {
            label: label.to_string(),
            press: Press::Send(actions),
            enabled: true,
            hint: None,
        }
    }
}

/// Which window it is, so a view keeps one of each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowKind {
    Review,
    Report,
    Edits,
    StagingErrors,
    Tool,
    Tree,
    Unsaved,
    ChdInfo,
    Conflict,
    Skipped,
    Journal,
    Log,
    Help,
    Checksums,
    FileInfo,
    Tar,
    TarPreview,
    Import,
    ArchiveSave,
    ArchiveViewer,
    Metadata,
    Password,
    Budget,
    Editor,
    RenameNotice,
    Partitions,
}

#[derive(Debug, Clone)]
pub struct PaneWindow {
    pub kind: WindowKind,
    /// The pane a window belongs to; `None` for the screen's own.
    pub side: Option<Side>,
    /// Changes whenever the window's content does, so a view knows to build it again.
    pub key: String,
    pub title: String,
    /// Width and height in DIPs; a height of 0 fits the content.
    pub size: (i32, i32),
    pub parts: Vec<Part>,
    pub buttons: Vec<WindowButton>,
    /// What closing the window sends; `None` when only a button closes it.
    pub dismiss: Option<Vec<A>>,
}

fn pane(side: Side, action: P) -> A {
    A::Pane(side, action)
}

fn text(line: String) -> Part {
    Part::Text(line, Tone::Plain)
}

impl CommanderController {
    /// Every window open now for a one-pane view over `side`: the pane's and the screen's.
    pub fn pane_windows(&self, side: Side, debug: bool) -> Vec<PaneWindow> {
        let mut out = self.side_windows(side, debug);
        out.extend(self.screen_windows());
        out
    }

    /// The windows of the pane on `side`; in a two-pane screen their titles name the side.
    pub fn side_windows(&self, side: Side, debug: bool) -> Vec<PaneWindow> {
        let pane = self.pane(side);
        let mut out = Vec::new();
        if let Some(p) = pane.apply_review() {
            out.push(review_window(side, p));
        }
        if let Some(r) = pane.apply_report() {
            out.push(report_window(side, r, pane.queue.len()));
        }
        if pane.edits_shown() {
            out.push(edits_window(side, &pane.edit_lines(), pane.is_busy()));
        }
        if !pane.staging_errors().is_empty() {
            let fs = &pane.volume().fs_type;
            out.push(errors_window(side, pane.staging_errors(), fs));
        }
        if let Some(name) = pane.pending_tar() {
            out.push(tar_window(side, &name));
        }
        if let Some(p) = pane.tar_preview() {
            out.push(tar_preview_window(side, p, &pane.volume().fs_type));
        }
        if let Some((name, kind)) = pane.pending_import() {
            out.push(import_window(side, &name, kind));
        }
        if let Some(p) = pane.archive_save() {
            out.push(archive_save_window(side, p));
        }
        if let Some(w) = pane.archive_viewer() {
            out.push(archive_viewer_window(side, w));
        }
        if let Some(f) = pane.metadata_form() {
            out.push(metadata_window(side, f));
        }
        if let Some(r) = pane.tool_report() {
            let repairable = !self.has_archive_flow();
            out.push(tool_window(side, r, repairable, self.edit_mode(), debug));
        }
        let tag = match self.is_single() {
            true => String::new(),
            false => format!(" ({})", side.label()),
        };
        for w in &mut out {
            w.side = Some(side);
            w.title.push_str(&tag);
        }
        out
    }

    /// The windows of the screen itself, whichever pane asked for them.
    pub fn screen_windows(&self) -> Vec<PaneWindow> {
        let pane = self.pane(self.active());
        let mut out = Vec::new();
        if let Some(n) = self.rename_notice() {
            out.push(rename_notice_window(n));
        }
        if let Some(side) = self.partitions_shown() {
            let p = self.pane(side);
            out.push(partitions_window(
                side,
                &p.partition_labels(),
                p.selected_partition(),
            ));
        }
        if self.unsaved_shown() {
            out.push(unsaved_window(pane.queue.len(), pane.staged_summary()));
        }
        if let Some(info) = self.chd_info() {
            out.push(PaneWindow {
                side: None,
                kind: WindowKind::ChdInfo,
                key: info.to_string(),
                title: "CHD Info".into(),
                size: (560, 420),
                parts: vec![Part::Block(info.to_string())],
                buttons: Vec::new(),
                dismiss: Some(vec![A::Mode(ModeAction::CloseChdInfo)]),
            });
        }
        if let Some(names) = self.host_conflict() {
            out.push(conflict_window(names));
        }
        if let Some(r) = self.skipped_report() {
            let mut parts = vec![text(r.summary.clone())];
            if !r.skipped.is_empty() {
                parts.push(Part::List(
                    r.skipped.iter().map(|s| format!("Skipped {s}")).collect(),
                ));
            }
            out.push(PaneWindow {
                side: None,
                kind: WindowKind::Skipped,
                key: r.summary.clone(),
                title: r.title.clone(),
                size: (520, 0),
                parts,
                buttons: vec![WindowButton::send("OK", vec![A::SkippedReportOk])],
                dismiss: Some(vec![A::SkippedReportOk]),
            });
        }
        if self.log_shown() {
            out.push(log_window(self.log()));
        }
        if self.help_shown() {
            let rows = self.key_help();
            let width = rows.iter().map(|(l, _)| l.len()).max().unwrap_or(0);
            let lines: Vec<String> = rows
                .iter()
                .map(|(label, chord)| format!("{label:width$}  {chord}"))
                .collect();
            out.push(PaneWindow {
                side: None,
                kind: WindowKind::Help,
                key: "help".into(),
                title: "Keyboard shortcuts".into(),
                size: (460, 480),
                parts: vec![Part::Block(lines.join("\n")), text(self.key_help_footer())],
                buttons: Vec::new(),
                dismiss: Some(vec![A::ShowHelp(false)]),
            });
        }
        if let Some(c) = self.checksums() {
            out.push(checksums_window(c));
        }
        // A two-pane screen asks for a password inside the pane.
        let left = self.pane(Side::Left);
        if left.needs_password() && self.is_single() {
            out.push(password_window(left.volume().fs_type == "APFS"));
        }
        if let Some(form) = self.size_budget() {
            out.push(budget_window(form));
        }
        if let Some(d) = self.detail() {
            let text = d.info_text(self.browse_caps());
            out.push(PaneWindow {
                side: None,
                kind: WindowKind::FileInfo,
                key: format!("{}{}", d.serial, d.loading),
                title: format!("File Info: {}", d.entry.name),
                size: (620, 560),
                parts: vec![Part::Block(text)],
                buttons: Vec::new(),
                dismiss: Some(vec![A::CloseDetail]),
            });
        }
        out
    }
}

fn log_window(lines: &[String]) -> PaneWindow {
    let mut parts = vec![text(format!("{} operation(s) this session.", lines.len()))];
    parts.push(match lines.is_empty() {
        true => text(
            "No operations yet. Applied edits, host copies / exports, and immediate host              deletes / renames / new folders are recorded here."
                .into(),
        ),
        false => Part::Block(lines.join("\n")),
    });
    PaneWindow {
        side: None,
        kind: WindowKind::Log,
        key: format!("{lines:?}"),
        title: "Session log".into(),
        size: (560, 360),
        parts,
        buttons: vec![
            WindowButton {
                label: "Copy all".into(),
                press: Press::Send(vec![A::CopyLog]),
                enabled: !lines.is_empty(),
                hint: None,
            },
            WindowButton::send("Clear", vec![A::ClearLog]),
        ],
        dismiss: Some(vec![A::ShowLog(false)]),
    }
}

fn checksums_window(c: &Checksums) -> PaneWindow {
    let running = c.running();
    let mut parts = Vec::new();
    let mut buttons = Vec::new();
    if let Some(err) = &c.error {
        parts.push(Part::Text(err.clone(), Tone::Error));
    } else if let Some(snap) = &running {
        parts.push(text(format!("Computing... {}", snap.phase)));
    } else {
        buttons.push(WindowButton::send("Copy all", vec![A::CopyChecksums]));
        let mut lines = Vec::new();
        let mut actions = Vec::new();
        for (file, row) in c.rows.iter().enumerate() {
            if let Some(err) = &row.error {
                lines.push(format!("{}: failed: {err}", row.name));
                actions.push(Vec::new());
            }
            for (algo, value) in HashAlgo::ALL.iter().zip(row.values.iter().flatten()) {
                lines.push(format!("{}  {:<7} {value}", row.name, algo.label()));
                actions.push(vec![A::CopyChecksum { file, algo: *algo }]);
            }
        }
        parts.push(Part::Items {
            lines,
            button: "Copy".into(),
            actions,
            enabled: true,
        });
    }
    PaneWindow {
        side: None,
        kind: WindowKind::Checksums,
        key: format!(
            "{}{:?}{}{}",
            c.title,
            c.error,
            c.rows.len(),
            running.is_some()
        ),
        title: c.title.clone(),
        size: (600, 420),
        parts,
        buttons,
        dismiss: Some(vec![A::CloseChecksums]),
    }
}

fn review_window(side: Side, p: &ApplyPreflight) -> PaneWindow {
    let mut parts = Vec::new();
    if p.shortfall() > 0 {
        parts.push(Part::Text(
            format!(
                "Not enough space: needs about {}, {} free on {}.",
                format_size(p.bytes_needed),
                format_size(p.bytes_free),
                p.fs_type
            ),
            Tone::Error,
        ));
    }
    let conflicts = !p.scan.conflicts.is_empty();
    if conflicts {
        parts.push(text(format!(
            "{} item(s) already exist on {}:",
            p.scan.conflicts.len(),
            p.fs_type
        )));
        parts.push(Part::List(
            p.scan.conflicts.iter().map(|c| c.0.clone()).collect(),
        ));
        parts.push(Part::Choice {
            options: vec!["Skip existing".into(), "Overwrite".into()],
            picked: 0,
            send: None,
            notes: Vec::new(),
        });
    }
    if !p.scan.blocked_folders.is_empty() {
        parts.push(text(format!(
            "{} folder(s) can't be created because a file has the same name; their contents \
             will fail:",
            p.scan.blocked_folders.len()
        )));
        parts.push(Part::List(p.scan.blocked_folders.clone()));
    }
    if !p.scan.missing_folders.is_empty() {
        parts.push(text(format!(
            "{} destination folder(s) no longer exist; items going into them will fail:",
            p.scan.missing_folders.len()
        )));
        parts.push(Part::List(p.scan.missing_folders.clone()));
    }
    parts.push(text(
        "Items that fail are listed at the end; everything else is still written.".into(),
    ));
    let label = if p.shortfall() > 0 {
        "Apply anyway"
    } else {
        "Apply"
    };
    let apply = |c| vec![pane(side, P::ApplyReviewed(c))];
    let go = match conflicts {
        true => Press::PerChoice(vec![apply(OnConflict::Skip), apply(OnConflict::Replace)]),
        false => Press::Send(apply(OnConflict::Skip)),
    };
    let cancel = vec![pane(side, P::DismissReview)];
    PaneWindow {
        side: None,
        kind: WindowKind::Review,
        key: "review".into(),
        title: "Before applying".into(),
        size: (480, 0),
        parts,
        buttons: vec![
            WindowButton {
                label: label.into(),
                press: go,
                enabled: true,
                hint: None,
            },
            WindowButton::send("Cancel", cancel.clone()),
        ],
        dismiss: Some(cancel),
    }
}

fn report_window(side: Side, r: &ApplyReport, still: usize) -> PaneWindow {
    let mut parts = Vec::new();
    if let Some(fatal) = &r.fatal {
        parts.push(Part::Text(fatal.clone(), Tone::Error));
    }
    parts.push(text(format!(
        "Applied {} of {} edit(s). {still} still staged.",
        r.applied, r.total
    )));
    if !r.failed.is_empty() {
        parts.push(text(format!("{} item(s) failed:", r.failed.len())));
        parts.push(Part::List(
            r.failed
                .iter()
                .map(|f| format!("{}: {}", f.label, f.error))
                .collect(),
        ));
    }
    if r.not_tried > 0 {
        parts.push(text(format!(
            "Stopped after {MAX_CONSECUTIVE_FAILURES} failures in a row; {} edit(s) were not tried.",
            r.not_tried
        )));
    }
    let close = vec![pane(side, P::DismissReport)];
    let mut buttons = Vec::new();
    if still > 0 {
        buttons.push(WindowButton::send(
            "Retry",
            vec![pane(side, P::DismissReport), pane(side, P::Apply)],
        ));
        buttons.push(WindowButton::send(
            "Discard remaining",
            vec![pane(side, P::DismissReport), pane(side, P::Discard)],
        ));
    }
    buttons.push(WindowButton::send("Close", close.clone()));
    PaneWindow {
        side: None,
        kind: WindowKind::Report,
        key: format!("{}/{}/{still}/{}", r.applied, r.total, r.failed.len()),
        title: "Apply report".into(),
        size: (560, 0),
        parts,
        buttons,
        dismiss: Some(close),
    }
}

fn edits_window(side: Side, lines: &[String], busy: bool) -> PaneWindow {
    PaneWindow {
        side: None,
        kind: WindowKind::Edits,
        key: format!("{lines:?}{busy}"),
        title: "Pending edits".into(),
        size: (460, 360),
        parts: vec![
            text("Staged for this pane (applied in order on Apply):".into()),
            Part::Items {
                lines: lines
                    .iter()
                    .enumerate()
                    .map(|(i, l)| format!("{}. {l}", i + 1))
                    .collect(),
                button: "Remove".into(),
                actions: (0..lines.len())
                    .map(|i| vec![pane(side, P::Unstage(i))])
                    .collect(),
                enabled: !busy,
            },
        ],
        buttons: Vec::new(),
        dismiss: Some(vec![pane(side, P::ShowEdits(false))]),
    }
}

fn errors_window(side: Side, rows: &[(String, String)], fs: &str) -> PaneWindow {
    let lines: Vec<String> = rows
        .iter()
        .map(|(path, reason)| {
            let name = Path::new(path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(path);
            format!("{name} - {reason}\n    {path}")
        })
        .collect();
    let ok = vec![pane(side, P::DismissStagingErrors)];
    PaneWindow {
        side: None,
        kind: WindowKind::StagingErrors,
        key: format!("{rows:?}"),
        title: "Could not add some files".into(),
        size: (520, 320),
        parts: vec![
            Part::Text(
                format!(
                    "{} item(s) could not be staged for the [{fs}] filesystem:",
                    rows.len()
                ),
                Tone::Warning,
            ),
            Part::Block(lines.join("\n")),
        ],
        buttons: vec![WindowButton::send("OK", ok.clone())],
        dismiss: Some(ok),
    }
}

fn tool_window(
    side: Side,
    r: &ToolReport,
    repairable: bool,
    edit: bool,
    debug: bool,
) -> PaneWindow {
    let dismiss = Some(vec![pane(side, P::DismissToolReport)]);
    let title = "Filesystem Check Results".to_string();
    let key = format!("{r:?}{repairable}{edit}{debug}");
    let Some(check) = &r.check else {
        return PaneWindow {
            side: None,
            kind: WindowKind::Tool,
            key,
            title,
            size: (520, 360),
            parts: vec![Part::Block(r.lines.join("\n"))],
            buttons: Vec::new(),
            dismiss,
        };
    };
    let mut parts = vec![
        match check.clean {
            true => Part::Text("Filesystem is clean.".into(), Tone::Success),
            false => Part::Text(
                format!("{} error(s) found.", check.errors.len()),
                Tone::Error,
            ),
        },
        text(check.stats_line.clone()),
    ];
    let mut lines = Vec::new();
    if !check.errors.is_empty() {
        lines.push("Errors:".to_string());
        lines.extend(check.errors.iter().cloned());
    }
    if !check.orphans.is_empty() {
        let total: usize = check.orphans.iter().map(|(_, v)| v.len()).sum();
        let parents = check.orphans.len();
        let tail = if parents == 1 { "y" } else { "ies" };
        lines.push(format!(
            "{total} file(s)/folder(s) reference {parents} missing parent director{tail} - not repairable."
        ));
        lines.push(
            "These entries exist in the catalog but their parent directory is gone. This \
             typically indicates severe directory corruption."
                .to_string(),
        );
        for (parent, entries) in &check.orphans {
            lines.push(format!("Missing parent ID {parent}:"));
            lines.extend(entries.iter().map(|e| format!("    {e}")));
        }
    }
    let shown: Vec<&String> = check
        .warnings
        .iter()
        .filter(|(_, is_debug)| !is_debug || debug)
        .map(|(line, _)| line)
        .collect();
    if !shown.is_empty() {
        lines.push("Warnings:".to_string());
        lines.extend(shown.into_iter().cloned());
    }
    if let Some(repair) = &r.repair {
        lines.push("Repair Report:".to_string());
        lines.extend(repair.applied.iter().map(|f| format!("  {f}")));
        lines.extend(repair.failed.iter().map(|f| format!("  {f}")));
        if repair.unrepairable > 0 {
            lines.push(format!(
                "{} error(s) could not be repaired (missing parent directories).",
                repair.unrepairable
            ));
        }
    }
    if !lines.is_empty() {
        parts.push(Part::Block(lines.join("\n")));
    }
    if check.warnings.iter().any(|(_, d)| *d) {
        parts.push(Part::Toggle {
            label: "Show debug messages".into(),
            on: debug,
            send: None,
        });
    }
    let mut buttons = Vec::new();
    if check.repairable && repairable {
        buttons.push(WindowButton {
            label: "Repair".into(),
            press: Press::Send(vec![pane(side, P::Repair)]),
            enabled: edit,
            hint: (!edit).then(|| "Turn on Edit Mode to repair the volume.".to_string()),
        });
    }
    PaneWindow {
        side: None,
        kind: WindowKind::Tool,
        key,
        title,
        size: (520, 420),
        parts,
        buttons,
        dismiss,
    }
}

fn unsaved_window(n: usize, lines: Vec<String>) -> PaneWindow {
    let answer = |c| vec![A::Mode(ModeAction::Unsaved(c))];
    let mut parts = vec![text(format!("You have {n} unsaved edit(s)."))];
    parts.extend(lines.iter().map(|l| text(l.clone())));
    PaneWindow {
        side: None,
        kind: WindowKind::Unsaved,
        key: format!("{n}{lines:?}"),
        title: "Unsaved Changes".into(),
        size: (420, 0),
        parts,
        buttons: vec![
            WindowButton::send("Discard Edits", answer(UnsavedChoice::Discard)),
            WindowButton::send("Apply Edits", answer(UnsavedChoice::Apply)),
            WindowButton::send("Cancel", answer(UnsavedChoice::Cancel)),
        ],
        dismiss: Some(answer(UnsavedChoice::Cancel)),
    }
}

fn conflict_window(names: &[String]) -> PaneWindow {
    PaneWindow {
        side: None,
        kind: WindowKind::Conflict,
        key: format!("{names:?}"),
        title: "Files already exist".into(),
        size: (480, 0),
        parts: vec![
            text(format!(
                "{} item(s) already exist in the destination folder:",
                names.len()
            )),
            Part::List(names.to_vec()),
        ],
        buttons: vec![
            WindowButton::send("Overwrite", vec![A::HostConflict(OnConflict::Replace)]),
            WindowButton::send("Skip existing", vec![A::HostConflict(OnConflict::Skip)]),
            WindowButton::send("Cancel", vec![A::HostConflictCancel]),
        ],
        dismiss: Some(vec![A::HostConflictCancel]),
    }
}

/// The Tree View window over a listing the pane handed over; `plain` leaves out Show IDs.
pub fn tree_window(tree: &TreeText, plain: bool) -> PaneWindow {
    let mut parts = Vec::new();
    if !plain {
        let list = |ids| vec![pane(Side::Left, P::ListTree { ids })];
        parts.push(Part::Toggle {
            label: "Show IDs".into(),
            on: tree.ids,
            send: Some((list(true), list(false))),
        });
    }
    parts.push(Part::Block(tree.text.clone()));
    PaneWindow {
        side: None,
        kind: WindowKind::Tree,
        key: format!("{}{}", tree.ids, tree.text.len()),
        title: "Tree View".into(),
        size: (640, 500),
        parts,
        buttons: vec![
            WindowButton {
                label: "Copy to Clipboard".into(),
                press: Press::Copy(tree.text.clone()),
                enabled: true,
                hint: None,
            },
            WindowButton::send("Save to File...", vec![pane(Side::Left, P::SaveTree)]),
        ],
        dismiss: None,
    }
}

/// The volume journal's window: its summary, its state, and the transactions newest first.
pub fn journal_window(detail: &JournalDetail) -> PaneWindow {
    let mut parts: Vec<Part> = journal_summary(detail).into_iter().map(text).collect();
    for (tone, line) in journal_status(detail) {
        let tone = match tone {
            JournalTone::Good => Tone::Success,
            JournalTone::Warning => Tone::Warning,
            JournalTone::Fault => Tone::Error,
        };
        parts.push(Part::Text(line, tone));
    }
    parts.push(text("Transactions (newest first):".into()));
    let rows = journal_txn_rows(detail);
    parts.push(match rows.is_empty() {
        true => text("  (none)".into()),
        false => Part::Browse {
            details: rows
                .iter()
                .map(|(idx, _)| {
                    journal_txn_blocks(detail, *idx)
                        .map(|(title, lines)| format!("{title}\n{}", lines.join("\n")))
                        .unwrap_or_default()
                })
                .collect(),
            lines: rows.into_iter().map(|(_, l)| l).collect(),
        },
    });
    PaneWindow {
        side: None,
        kind: WindowKind::Journal,
        key: format!("{}", parts.len()),
        title: "HFS+ Journal".into(),
        size: (640, 520),
        parts,
        buttons: Vec::new(),
        dismiss: None,
    }
}

fn fork_select(side: Side, current: ForkFormat, viewer: bool, label: &str) -> Part {
    Part::Select {
        label: label.into(),
        options: ForkFormat::ALL
            .iter()
            .map(|f| f.label().to_string())
            .collect(),
        picked: ForkFormat::ALL
            .iter()
            .position(|f| *f == current)
            .unwrap_or(0),
        send: ForkFormat::ALL
            .iter()
            .map(|f| vec![pane(side, P::ArchiveFormat { viewer, format: *f })])
            .collect(),
    }
}

fn tar_window(side: Side, name: &str) -> PaneWindow {
    let choose = |c| vec![pane(side, P::ChooseTar(c))];
    PaneWindow {
        side: None,
        kind: WindowKind::Tar,
        key: name.to_string(),
        title: "Tar archive".into(),
        size: (460, 0),
        parts: vec![
            text(format!("\"{name}\" is a tar archive.")),
            text(
                "Import its contents into the current folder, or add the archive itself as a file?"
                    .into(),
            ),
        ],
        buttons: vec![
            WindowButton::send("Import contents", choose(TarChoice::Import)),
            WindowButton::send("Add as file", choose(TarChoice::AsFile)),
            WindowButton::send("Cancel", choose(TarChoice::Cancel)),
        ],
        dismiss: Some(choose(TarChoice::Cancel)),
    }
}

fn tar_preview_window(side: Side, p: &TarPreview, fs: &str) -> PaneWindow {
    let mut parts = vec![Part::Text(
        format!("This [{fs}] volume can't store everything in the archive:"),
        Tone::Warning,
    )];
    parts.extend(p.warnings.iter().map(|w| text(format!("- {w}"))));
    if p.sidecars > 0 {
        parts.push(text(format!(
            "- {} macOS ._ sidecar(s) will be skipped.",
            p.sidecars
        )));
    }
    parts.push(text(format!(
        "{} file(s) and {} dir(s) will be imported. Existing names are skipped.",
        p.files, p.dirs
    )));
    let answer = |yes| vec![pane(side, P::ConfirmTarImport(yes))];
    PaneWindow {
        side: None,
        kind: WindowKind::TarPreview,
        key: "preview".into(),
        title: "Import archive - some content can't be kept".into(),
        size: (520, 0),
        parts,
        buttons: vec![
            WindowButton::send("Continue", answer(true)),
            WindowButton::send("Cancel", answer(false)),
        ],
        dismiss: Some(answer(false)),
    }
}

fn import_window(side: Side, name: &str, kind: MacArchiveKind) -> PaneWindow {
    let prompt = import_prompt(name, kind);
    let choose = |c| vec![pane(side, P::ChooseImport(c))];
    let mut buttons = vec![WindowButton::send(
        prompt.expand,
        choose(ImportChoice::Expand),
    )];
    if let Some(label) = prompt.convert_only {
        buttons.push(WindowButton::send(label, choose(ImportChoice::ConvertOnly)));
    }
    buttons.push(WindowButton::send("Add as-is", choose(ImportChoice::AsIs)));
    buttons.push(WindowButton::send("Cancel", choose(ImportChoice::Cancel)));
    PaneWindow {
        side: None,
        kind: WindowKind::Import,
        key: format!("{name}{}", kind.label()),
        title: prompt.title.into(),
        size: (520, 0),
        parts: vec![text(prompt.intro), text(prompt.question)],
        buttons,
        dismiss: Some(choose(ImportChoice::Cancel)),
    }
}

fn archive_save_window(side: Side, p: &ArchiveSave) -> PaneWindow {
    let cancel = vec![pane(side, P::CancelSaveArchive)];
    PaneWindow {
        side: None,
        kind: WindowKind::ArchiveSave,
        key: p.entry_name.clone(),
        title: "Save Mac archive".into(),
        size: (520, 0),
        parts: vec![
            text(format!("{} is a {} archive.", p.entry_name, p.kind.label())),
            text("How would you like to save it?".into()),
            fork_select(side, p.format, false, "Decoded fork container:"),
        ],
        buttons: vec![
            WindowButton::send("Save as-is", vec![pane(side, P::SaveArchive { raw: true })]),
            WindowButton::send(
                "Decode and save contents to a folder...",
                vec![pane(side, P::SaveArchive { raw: false })],
            ),
            WindowButton::send("Cancel", cancel.clone()),
        ],
        dismiss: Some(cancel),
    }
}

/// A compact binary size, as the Archives tab shows it.
fn binary_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    match u {
        0 => format!("{bytes} B"),
        _ => format!("{v:.1} {}", UNITS[u]),
    }
}

fn archive_viewer_window(side: Side, w: &ArchiveViewer) -> PaneWindow {
    let lines = w
        .rows
        .iter()
        .map(|r| match r.is_dir {
            true => format!("[ {} ]", r.path),
            false => format!(
                "{}   {}   {}   {}",
                r.path,
                r.type_creator,
                binary_size(r.size),
                r.method
            ),
        })
        .collect();
    let ticked = w
        .rows
        .iter()
        .zip(&w.marked)
        .map(|(r, m)| (!r.is_dir).then_some(*m))
        .collect();
    let close = vec![pane(side, P::CloseArchive)];
    let marked = w.marked_files();
    PaneWindow {
        side: None,
        kind: WindowKind::ArchiveViewer,
        key: w.entry_name.clone(),
        title: format!("Archive: {} ({})", w.entry_name, w.kind.label()),
        size: (640, 480),
        parts: vec![
            text(format!(
                "{} file(s) in archive (read-only view).",
                w.file_count()
            )),
            Part::Checklist {
                lines,
                ticked,
                send: (0..w.rows.len())
                    .map(|i| vec![pane(side, P::ArchiveMark(i))])
                    .collect(),
            },
            fork_select(side, w.format, true, "Extract as:"),
        ],
        buttons: vec![
            WindowButton::send(
                "Extract All...",
                vec![pane(side, P::ArchiveExtract { all: true })],
            ),
            WindowButton {
                label: format!("Extract Selected... ({marked})"),
                press: Press::Send(vec![pane(side, P::ArchiveExtract { all: false })]),
                enabled: marked > 0,
                hint: None,
            },
            WindowButton::send("Close", close.clone()),
        ],
        dismiss: Some(close),
    }
}

fn field(id: FieldId, label: &str, value: &str, hint: &str) -> Part {
    Part::Field {
        id,
        label: label.into(),
        value: value.into(),
        hint: hint.into(),
        secret: false,
        multiline: false,
        live: false,
    }
}

fn metadata_window(side: Side, form: &MetadataForm) -> PaneWindow {
    let e = &form.entry;
    let init = &form.initial;
    let mut parts = vec![text(format!("Metadata for {}", e.name))];
    let mac = e.is_file() && e.type_code.is_some();
    if mac {
        parts.push(field(
            FieldId::MetaType,
            "Type:",
            &init.type_code,
            "4 characters",
        ));
        parts.push(field(
            FieldId::MetaCreator,
            "Creator:",
            &init.creator,
            "4 characters",
        ));
    }
    if e.mac_dates.is_some() {
        let hint = "YYYY-MM-DD HH:MM:SS (UTC)";
        parts.push(field(
            FieldId::MetaModified,
            "Modified:",
            &init.modified,
            hint,
        ));
        parts.push(field(FieldId::MetaCreated, "Created:", &init.created, hint));
    }
    if e.mode.is_some() {
        parts.push(field(
            FieldId::MetaMode,
            "Permissions:",
            &init.mode,
            "octal, e.g. 644",
        ));
        parts.push(field(FieldId::MetaOwner, "Owner:", &init.owner, "uid:gid"));
    }
    if mac {
        let code = |b: [u8; 4]| b.iter().map(|&c| c as char).collect::<String>();
        parts.push(Part::Picker {
            label: "Pick a known type...".into(),
            options: known_type_creators()
                .into_iter()
                .map(|(label, t, c)| {
                    let set = vec![
                        (FieldId::MetaType, code(t)),
                        (FieldId::MetaCreator, code(c)),
                    ];
                    (label, set)
                })
                .collect(),
        });
    }
    if let Some(err) = &form.error {
        parts.push(Part::Text(err.clone(), Tone::Error));
    }
    let cancel = vec![pane(side, P::CancelMetadata)];
    PaneWindow {
        side: None,
        kind: WindowKind::Metadata,
        key: e.path.clone(),
        title: "Edit Metadata".into(),
        size: (460, 0),
        parts,
        buttons: vec![
            WindowButton::send("Cancel", cancel.clone()),
            WindowButton {
                label: "Stage".into(),
                press: Press::Submit(Form::Metadata(side)),
                enabled: true,
                hint: None,
            },
        ],
        dismiss: Some(cancel),
    }
}

fn password_window(apfs: bool) -> PaneWindow {
    let mut parts = match apfs {
        true => vec![
            text("This APFS volume is encrypted with FileVault.".into()),
            text("Enter the volume password or personal recovery key.".into()),
        ],
        false => vec![text("This image is password-protected.".into())],
    };
    parts.push(Part::Field {
        id: FieldId::Password,
        label: String::new(),
        value: String::new(),
        hint: String::new(),
        secret: true,
        multiline: false,
        live: false,
    });
    let close = vec![A::Mode(ModeAction::RequestClose)];
    PaneWindow {
        side: None,
        kind: WindowKind::Password,
        key: apfs.to_string(),
        title: if apfs { "Passphrase" } else { "Password" }.into(),
        size: (420, 0),
        parts,
        buttons: vec![
            WindowButton::send("Close", close.clone()),
            WindowButton {
                label: "Unlock".into(),
                press: Press::Submit(Form::Password),
                enabled: true,
                hint: None,
            },
        ],
        dismiss: Some(close),
    }
}

fn budget_window(form: &BudgetForm) -> PaneWindow {
    let plan = *form.plan();
    let room = match (plan.capacity, plan.headroom) {
        (Some(cap), Some(head)) => format!("{} ({} unused)", format_size(cap), format_size(head)),
        _ => "unbounded - this file is the filesystem".to_string(),
    };
    let facts = [
        format!("Image occupies   {}", format_size(plan.image_len)),
        format!(
            "File content     {} uncompressed",
            format_size(plan.content_len)
        ),
        format!("Compressed to    {}", plan.describe_ratio()),
        format!("Room available   {room}"),
    ];
    let budget = |a| vec![A::Mode(ModeAction::Budget(a))];
    let modes = [BudgetMode::Fit, BudgetMode::Absolute, BudgetMode::Headroom];
    let ceiling = match form.ceiling() {
        Some(c) => format!("Ceiling: {}", format_size(c)),
        None => "Ceiling: none beyond what the container allows".to_string(),
    };
    let mut parts = vec![
        text(
            "SquashFS has no free space to report and no in-place write: saving an edit rebuilds \
             the whole image. Choose how large the result may be."
                .into(),
        ),
        Part::Block(facts.join("\n")),
        Part::Choice {
            options: vec![
                "Fit - accept whatever it becomes".into(),
                "At most (MiB)".into(),
                "Grow by at most (MiB)".into(),
            ],
            picked: modes.iter().position(|m| *m == form.mode()).unwrap_or(0),
            send: Some(
                modes
                    .iter()
                    .map(|m| budget(BudgetAction::SetMode(*m)))
                    .collect(),
            ),
            notes: Vec::new(),
        },
        Part::Field {
            id: FieldId::BudgetAbsolute,
            label: "At most (MiB):".into(),
            value: form.absolute_mib().to_string(),
            hint: String::new(),
            secret: false,
            multiline: false,
            live: true,
        },
        Part::Field {
            id: FieldId::BudgetHeadroom,
            label: "Grow by (MiB):".into(),
            value: form.headroom_mib().to_string(),
            hint: String::new(),
            secret: false,
            multiline: false,
            live: true,
        },
        text(ceiling),
    ];
    if form.over_capacity() {
        parts.push(Part::Text(
            format!(
                "That is more than the {} available where this image lives.",
                format_size(plan.capacity.unwrap_or(0))
            ),
            Tone::Error,
        ));
    }
    let cancel = budget(BudgetAction::Cancel);
    PaneWindow {
        side: None,
        kind: WindowKind::Budget,
        key: format!("budget{}", form.over_capacity()),
        title: "SquashFS size budget".into(),
        size: (480, 0),
        parts,
        buttons: vec![
            WindowButton {
                label: "Start editing".into(),
                press: Press::Send(budget(BudgetAction::Start)),
                enabled: !form.over_capacity(),
                hint: None,
            },
            WindowButton::send("Cancel", cancel.clone()),
        ],
        dismiss: Some(cancel),
    }
}

/// The text editor over `doc`; `error` is why the last Save was refused.
pub fn editor_window(doc: &TextDoc, error: Option<&str>) -> PaneWindow {
    let original = doc.shape.ending;
    let mut parts = vec![text(format!("Encoding: {}", doc.shape.encoding.label()))];
    parts.push(Part::Choice {
        options: ENDINGS.iter().map(|(_, l)| l.to_string()).collect(),
        picked: ENDINGS
            .iter()
            .position(|(e, _)| *e == original)
            .unwrap_or(0),
        send: None,
        notes: ENDINGS
            .iter()
            .map(|(e, _)| match *e == original {
                true => String::new(),
                false => format!("Will convert {} -> {}", original.label(), e.label()),
            })
            .collect(),
    });
    if doc.shape.mixed_endings {
        parts.push(Part::Text(
            "This file mixes line-ending styles; saving makes them all the same.".into(),
            Tone::Warning,
        ));
    }
    parts.push(Part::Field {
        id: FieldId::EditorText,
        label: String::new(),
        value: doc.text.clone(),
        hint: String::new(),
        secret: false,
        multiline: true,
        live: false,
    });
    parts.push(Part::Text(error.unwrap_or("").to_string(), Tone::Error));
    PaneWindow {
        side: None,
        kind: WindowKind::Editor,
        key: format!("{}/{}", doc.folder, doc.name),
        title: format!("Edit  {}", doc.name),
        size: (720, 520),
        parts,
        buttons: vec![
            WindowButton {
                label: "Save".into(),
                press: Press::Submit(Form::Editor {
                    name: doc.name.clone(),
                    folder: doc.folder.clone(),
                    shape: doc.shape,
                }),
                enabled: true,
                hint: Some("Stage the new text; Apply Edits writes it.".into()),
            },
            WindowButton::send("Cancel", Vec::new()),
        ],
        dismiss: None,
    }
}

fn rename_notice_window(notice: &RenameNotice) -> PaneWindow {
    let mut parts = Vec::new();
    for (head, lines) in [notice.renamed(), notice.dropped()].into_iter().flatten() {
        parts.push(text(head));
        parts.push(Part::List(lines));
    }
    parts.push(text(RenameNotice::TIP.to_string()));
    PaneWindow {
        side: None,
        kind: WindowKind::RenameNotice,
        key: format!("{notice:?}"),
        title: notice.title(),
        size: (520, 0),
        parts,
        buttons: vec![
            WindowButton::send("OK", vec![A::RenameNoticeOk]),
            WindowButton {
                label: "Undo copy".into(),
                press: Press::Send(vec![A::RenameNoticeUndo]),
                enabled: true,
                hint: Some("Remove this copy from the staged edits".into()),
            },
        ],
        dismiss: Some(vec![A::RenameNoticeOk]),
    }
}

fn partitions_window(side: Side, labels: &[String], selected: Option<usize>) -> PaneWindow {
    let lines = labels
        .iter()
        .enumerate()
        .map(|(i, l)| match selected == Some(i) {
            true => format!("{l}  (open)"),
            false => l.clone(),
        })
        .collect();
    PaneWindow {
        side: None,
        kind: WindowKind::Partitions,
        key: format!("{side:?}{labels:?}{selected:?}"),
        title: format!("Choose partition ({})", side.label()),
        size: (420, 300),
        parts: vec![Part::Items {
            lines,
            button: "Open".into(),
            actions: (0..labels.len())
                .map(|i| vec![pane(side, P::SelectPartition(i))])
                .collect(),
            enabled: true,
        }],
        buttons: Vec::new(),
        dismiss: Some(vec![A::ShowPartitions(false)]),
    }
}
