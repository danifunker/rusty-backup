//! The windows a browse view opens over its listing, described once for every view.
//!
//! Each [`PaneWindow`] is a title, parts (lines, lists, a choice) and buttons that carry the
//! actions they send; a view draws it with its own toolkit and sends what a button holds. The
//! words and rules came out of the GTK shell's `browser/dialogs.rs`. A view keeps only what is
//! its own: which option of a [`Part::Choice`] is picked, and a [`Part::Toggle`] with no actions.

use std::path::Path;

use super::{CommanderController, HashAlgo, ModeAction, Side, UnsavedChoice};
use crate::controller::commander::{
    format_size, journal_status, journal_summary, journal_txn_rows, ApplyPreflight, ApplyReport,
    BrowserPaneAction as P, Checksums, CommanderAction as A, JournalDetail, JournalTone,
    OnConflict, ToolReport, TreeText, MAX_CONSECUTIVE_FAILURES,
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
    /// Options of which one is picked; a [`Press::PerChoice`] button sends the picked one's actions.
    Choice { options: Vec<String>, picked: usize },
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
}

#[derive(Debug, Clone)]
pub struct PaneWindow {
    pub kind: WindowKind,
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
    /// The windows the browse view over `side` has open now; `debug` is the Check report's toggle.
    pub fn pane_windows(&self, side: Side, debug: bool) -> Vec<PaneWindow> {
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
        if let Some(r) = pane.tool_report() {
            let repairable = !self.has_archive_flow();
            out.push(tool_window(side, r, repairable, self.edit_mode(), debug));
        }
        if self.unsaved_shown() {
            out.push(unsaved_window(pane.queue.len(), pane.staged_summary()));
        }
        if let Some(info) = self.chd_info() {
            out.push(PaneWindow {
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
                kind: WindowKind::Help,
                key: "help".into(),
                title: "Keyboard shortcuts".into(),
                size: (460, 480),
                parts: vec![
                    Part::Block(lines.join(
                        "
",
                    )),
                    text(self.key_help_footer()),
                ],
                buttons: Vec::new(),
                dismiss: Some(vec![A::ShowHelp(false)]),
            });
        }
        if let Some(c) = self.checksums() {
            out.push(checksums_window(c));
        }
        if let Some(d) = self.detail() {
            let p = d.side_panel(self.browse_caps());
            let mut lines = vec![p.name.clone(), String::new()];
            lines.extend(p.facts);
            lines.extend(p.note);
            lines.extend(p.budget);
            if !d.xattrs.is_empty() {
                lines.push(String::new());
                lines.push("Extended attributes:".into());
                lines.extend(
                    d.xattrs
                        .iter()
                        .map(|x| format!("  {} = {}", x.name, x.value_display())),
                );
            }
            let preview = p.preview.text();
            if !preview.is_empty() {
                lines.push(String::new());
                lines.push(preview.to_string());
            }
            out.push(PaneWindow {
                kind: WindowKind::FileInfo,
                key: format!("{}{}", d.serial, d.loading),
                title: format!("File Info: {}", d.entry.name),
                size: (620, 560),
                parts: vec![Part::Block(lines.join(
                    "
",
                ))],
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
        false => Part::Block(lines.join("
")),
    });
    PaneWindow {
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
        false => Part::Block(rows.into_iter().map(|(_, l)| l).collect::<Vec<_>>().join(
            "
",
        )),
    });
    PaneWindow {
        kind: WindowKind::Journal,
        key: format!("{}", parts.len()),
        title: "HFS+ Journal".into(),
        size: (640, 520),
        parts,
        buttons: Vec::new(),
        dismiss: None,
    }
}
