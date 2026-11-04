//! Tier A UI tests: replay TOML scenarios from `tests/ui_scenarios/` against view-models.
//!
//! An integration test, so it builds against the library alone: `rb-cli-vintage`
//! runs it on Rust 1.73 too, where the library's own unit tests don't build.
//!
//! A scenario names a view-model (`vm`) and a list of steps. Each step may
//! send an `action` (with an optional `arg`), check the effects it returned
//! (`effects`, by kind: `CopyToClipboard`, `PickFile`, ...), wait for running
//! jobs to end (`settle = true`), and check state (`expect`, a table of probe
//! names to values; a string starting with `~` matches as a substring).
//! Probes and actions are each view-model's own vocabulary,
//! listed on its `Target` impl below. Scenarios use widget-level intents only,
//! never toolkit details, so the same files can later drive real shells (§6).

use std::path::{Path, PathBuf};

use serde::Deserialize;
use toml::Value;

use rusty_backup::controller::app::{AppAction, AppController, AppTab, DropTargets};
use rusty_backup::controller::{Controller, Effect};
use rusty_backup::model::job::{Job, JobCtx, Status};

#[derive(Debug, Deserialize)]
struct Scenario {
    name: String,
    vm: String,
    /// Cargo features the view-model needs; a build without one skips the scenario.
    #[serde(default)]
    requires: Vec<String>,
    #[serde(default)]
    step: Vec<Step>,
}

/// Whether this build has a feature a scenario `requires`.
fn has_feature(name: &str) -> bool {
    name == "optical" && cfg!(all(feature = "optical", feature = "native-zstd"))
}

#[derive(Debug, Deserialize)]
struct Step {
    action: Option<String>,
    arg: Option<Value>,
    effects: Option<Vec<String>>,
    #[serde(default)]
    settle: bool,
    expect: Option<toml::value::Table>,
}

/// A view-model as the scenario runner drives it.
trait Target {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String>;
    fn probe(&self, name: &str) -> Result<Value, String>;
    /// Block until no job this view-model started is running, then tick.
    fn settle(&mut self) -> Vec<Effect>;
}

fn effect_kind(e: &Effect) -> &'static str {
    match e {
        Effect::PickFile(_) => "PickFile",
        Effect::PickFolder(_) => "PickFolder",
        Effect::Confirm(_) => "Confirm",
        Effect::AskPassword(_) => "AskPassword",
        Effect::AskText(_) => "AskText",
        Effect::Choose(_) => "Choose",
        Effect::Message(_) => "Message",
        Effect::OpenUrl(_) => "OpenUrl",
        Effect::CopyToClipboard(_) => "CopyToClipboard",
        Effect::CloseScreen => "CloseScreen",
        Effect::Quit => "Quit",
    }
}

fn arg_str(arg: Option<&Value>) -> Result<&str, String> {
    arg.and_then(Value::as_str)
        .ok_or_else(|| "this action needs a string `arg`".to_string())
}

/// `AppController`. Actions: `CopyLog`, `ClearLog`, `SetLogAutoScroll` (bool arg), `SelectTab`
/// (`backup`, `restore`, `inspect`, `optical`, `archives`), `Drop` (a path, with no tab busy),
/// `ShareBackup` (a folder a tab opened) / `AdoptBackup` (an empty tab asks), `CloseBackup`, and
/// the test-only `LogInfo` / `LogWarn` / `LogError` (string arg). Probes: `log_lines`,
/// `log_text`, `log_auto_scroll`, `jobs_running`, `active_tab`, `last_route`, `shared_backup`.
impl Target for AppController {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        let a = match action {
            "CopyLog" => AppAction::CopyLog,
            "ClearLog" => AppAction::ClearLog,
            "SetLogAutoScroll" => AppAction::SetLogAutoScroll(
                arg.and_then(Value::as_bool)
                    .ok_or("SetLogAutoScroll needs a bool `arg`")?,
            ),
            "SelectTab" => AppAction::SelectTab(match arg_str(arg)? {
                "backup" => AppTab::Backup,
                "restore" => AppTab::Restore,
                "inspect" => AppTab::Inspect,
                "optical" => AppTab::Optical,
                "archives" => AppTab::Archives,
                other => return Err(format!("no tab {other:?}")),
            }),
            "Drop" => {
                let route = self.route_drop(PathBuf::from(arg_str(arg)?), DropTargets::default());
                self.log.info(format!("route: {route:?}"));
                return Ok(Vec::new());
            }
            "ShareBackup" | "AdoptBackup" => {
                let loaded =
                    (action == "ShareBackup").then(|| PathBuf::from(arg_str(arg).unwrap_or("")));
                let step = self.share_backup(loaded, action == "AdoptBackup");
                self.log.info(format!("share: {step:?}"));
                return Ok(Vec::new());
            }
            "CloseBackup" => {
                self.close_shared_backup();
                return Ok(Vec::new());
            }
            "LogInfo" => {
                self.log.info(arg_str(arg)?);
                return Ok(Vec::new());
            }
            "LogWarn" => {
                self.log.warn(arg_str(arg)?);
                return Ok(Vec::new());
            }
            "LogError" => {
                self.log.error(arg_str(arg)?);
                return Ok(Vec::new());
            }
            other => return Err(format!("AppController has no action {other:?}")),
        };
        Ok(self.apply(a))
    }

    fn probe(&self, name: &str) -> Result<Value, String> {
        Ok(match name {
            "log_lines" => Value::Integer(self.log.entries().len() as i64),
            "log_text" => Value::String(self.log.to_text()),
            "log_auto_scroll" => Value::Boolean(self.log_auto_scroll()),
            "jobs_running" => Value::Integer(self.jobs().running().len() as i64),
            "active_tab" => Value::String(format!("{:?}", self.active_tab())),
            "shared_backup" => Value::String(
                self.shared_backup()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default(),
            ),
            other => return Err(format!("AppController has no probe {other:?}")),
        })
    }

    fn settle(&mut self) -> Vec<Effect> {
        while !self.jobs().is_idle() {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        self.tick()
    }
}

/// A test-only view-model over one job, for the job lifecycle scenarios.
/// Actions: `Start` (arg: `ok`, `error`, `panic` or `wait_for_cancel`), `Cancel`.
/// Probes: `status` (`idle`, `running`, `done`, `failed`, `cancelled`), `error`, `result`.
struct JobProbeController {
    app: AppController,
    job: Option<Job<String>>,
}

impl Target for JobProbeController {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        match action {
            "Start" => {
                let kind = arg_str(arg)?.to_string();
                let body = move |ctx: &JobCtx| -> anyhow::Result<String> {
                    ctx.set_phase("Working");
                    match kind.as_str() {
                        "ok" => Ok("finished".to_string()),
                        "error" => anyhow::bail!("the disk is full"),
                        "panic" => panic!("index out of bounds"),
                        "wait_for_cancel" => loop {
                            ctx.check_cancel()?;
                            std::thread::sleep(std::time::Duration::from_millis(1));
                        },
                        other => anyhow::bail!("unknown Start kind {other:?}"),
                    }
                };
                let job = self
                    .app
                    .jobs()
                    .spawn("Probe", Vec::new(), self.app.notify(), body)
                    .map_err(|e| e.to_string())?;
                self.job = Some(job);
                Ok(Vec::new())
            }
            "Cancel" => {
                self.job.as_ref().ok_or("no job to cancel")?.cancel();
                Ok(Vec::new())
            }
            other => Err(format!("JobProbeController has no action {other:?}")),
        }
    }

    fn probe(&self, name: &str) -> Result<Value, String> {
        let snap = self.job.as_ref().map(Job::snapshot);
        Ok(match name {
            "status" => Value::String(
                match snap.as_ref().map(|s| &s.status) {
                    None => "idle",
                    Some(Status::Running) => "running",
                    Some(Status::Done) => "done",
                    Some(Status::Failed(_)) => "failed",
                    Some(Status::Cancelled) => "cancelled",
                }
                .to_string(),
            ),
            "error" => Value::String(match snap.map(|s| s.status) {
                Some(Status::Failed(msg)) => msg,
                _ => String::new(),
            }),
            "result" => Value::String(
                self.job
                    .as_ref()
                    .and_then(|j| j.take_result())
                    .unwrap_or_default(),
            ),
            other => return Err(format!("JobProbeController has no probe {other:?}")),
        })
    }

    fn settle(&mut self) -> Vec<Effect> {
        if let Some(job) = &self.job {
            job.wait();
        }
        self.app.settle()
    }
}

/// `InspectController`. Actions: `OpenImage` (arg: a path, or `$blank_fat` for a fresh 2 MiB FAT
/// image), `Close`, `Reinspect`, `Check` (arg: row number from 0), `SetBrowseDirty` (bool), `Confirm`
/// (bool, answers the pending question), `OpenExport`, `ExportFormat` (a label from the window's
/// list), `ExportWholeDisk` (bool), `ExportSizeMode` / `ExportCustomMib` (the first row). Probes:
/// `source`, `source_label`, `can_close`, `partitions`, `superfloppy`, `error`, `check_clean`,
/// `export_format`, `export_whole_disk`, `export_modes`, `export_size_mib`, `log`.
/// The table editor: `OpenImage` also takes `$mbr_gap` (16 MiB, one 4 MiB FAT16 partition, the
/// rest free); actions `EditTable`, `AddPartition`, `CloseEditor`, `TableMode` (row 0: `Original`,
/// `Custom`), `TableDelete` / `TableUndo` (a row), `TableAdd`, `TableValidate`, `ApplyTable`;
/// probes `can_add_partition`, `editor_open`, `table_rows`, `table_modes` (row 0's choices),
/// `table_can_delete` (row 0), `table_can_apply`, `table_errors`, `floppy_source`, `alto_source`.
struct InspectTarget {
    ctl: rusty_backup::controller::inspect::InspectController,
    dir: tempfile::TempDir,
    asked: Option<&'static str>,
    log: Vec<String>,
    can_add: bool,
}

impl InspectTarget {
    fn new() -> Self {
        use rusty_backup::controller::inspect::{
            InspectController, InspectServices, JobRegistry, NoNotify,
        };
        InspectTarget {
            ctl: InspectController::new(InspectServices {
                jobs: JobRegistry::new(),
                notify: std::sync::Arc::new(NoNotify),
            }),
            dir: tempfile::tempdir().expect("temp dir"),
            asked: None,
            log: Vec::new(),
            can_add: false,
        }
    }

    fn note(&mut self, fx: Vec<Effect>) -> Vec<Effect> {
        for e in &fx {
            if let Effect::Confirm(c) = e {
                self.asked = Some(c.purpose);
            }
        }
        self.log
            .extend(self.ctl.take_log().into_iter().map(|(_, m)| m));
        self.can_add = self.ctl.can_add_partition();
        fx
    }

    /// A 16 MiB MBR disk: one 4 MiB FAT16 partition at LBA 2048, the rest free.
    fn mbr_gap(&self) -> Result<PathBuf, String> {
        let vol = fs_pc::fat::create_blank_fat(4 << 20, Some("GAP")).map_err(|e| e.to_string())?;
        let mut bytes = vec![0u8; 16 << 20];
        bytes[2048 * 512..2048 * 512 + vol.len()].copy_from_slice(&vol);
        bytes[0x1BE + 4] = 0x06;
        bytes[0x1BE + 8..0x1BE + 12].copy_from_slice(&2048u32.to_le_bytes());
        bytes[0x1BE + 12..0x1BE + 16].copy_from_slice(&((vol.len() / 512) as u32).to_le_bytes());
        bytes[510] = 0x55;
        bytes[511] = 0xAA;
        let p = self.dir.path().join("gap.img");
        std::fs::write(&p, bytes).map_err(|e| e.to_string())?;
        Ok(p)
    }

    /// The table editor's actions.
    fn table_act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        use rusty_backup::controller::inspect::InspectAction;
        use rusty_backup::controller::partition_edit::{SizeMode, TableEditAction};
        let row = || arg.and_then(Value::as_integer).map(|n| n as usize);
        let a = match action {
            "EditTable" => InspectAction::EditTable,
            "AddPartition" => InspectAction::AddPartition,
            "CloseEditor" => InspectAction::CloseEditor,
            "TableMode" => InspectAction::Table(TableEditAction::SetSizeMode {
                row: 0,
                mode: match arg_str(arg)? {
                    "Original" => SizeMode::Original,
                    "Custom" => SizeMode::Custom,
                    other => return Err(format!("no table size mode {other:?}")),
                },
            }),
            "TableDelete" => InspectAction::Table(TableEditAction::Delete(row().ok_or("a row")?)),
            "TableUndo" => InspectAction::Table(TableEditAction::Undo(row().ok_or("a row")?)),
            "TableAdd" => InspectAction::Table(TableEditAction::Add),
            "TableValidate" => InspectAction::Table(TableEditAction::Validate),
            _ => InspectAction::ApplyTableEdits,
        };
        let fx = self.ctl.apply(a);
        Ok(self.note(fx))
    }

    fn table_probe(&self, name: &str) -> Result<Value, String> {
        let view = self.ctl.table_editor();
        let first = view.as_ref().and_then(|v| v.rows.first().cloned());
        Ok(match name {
            "can_add_partition" => Value::Boolean(self.can_add),
            "editor_open" => Value::Boolean(view.is_some()),
            "table_rows" => Value::Integer(view.as_ref().map_or(0, |v| v.rows.len() as i64)),
            "table_modes" => Value::String(
                first
                    .map(|r| {
                        r.mode_choices()
                            .iter()
                            .map(|m| m.label())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
                    .join(","),
            ),
            "table_can_delete" => Value::Boolean(first.is_some_and(|r| r.can_delete())),
            "table_can_apply" => Value::Boolean(view.as_ref().is_some_and(|v| v.can_apply)),
            "table_errors" => Value::String(view.map(|v| v.errors.join("; ")).unwrap_or_default()),
            "floppy_source" => Value::Boolean(self.ctl.floppy_convert_source().is_some()),
            _ => Value::Boolean(self.ctl.alto_resize_source().is_ok()),
        })
    }
}

impl InspectTarget {
    /// The Export window's settings as a shell edits them.
    fn export_edit(&mut self, action: &str, arg: Option<&Value>) -> Result<(), String> {
        use rusty_backup::controller::inspect::{SizeMode, EXPORT_FORMATS};
        let s = self.ctl.export_settings_mut();
        match action {
            "ExportFormat" => {
                let label = arg_str(arg)?;
                let c = EXPORT_FORMATS
                    .iter()
                    .find(|c| c.label == label)
                    .ok_or_else(|| format!("no format {label:?}"))?;
                s.format = c.format;
            }
            "ExportWholeDisk" => s.set_whole_disk(arg.and_then(Value::as_bool).ok_or("bool")?),
            "ExportSizeMode" => {
                let mode = match arg_str(arg)? {
                    "Original" => SizeMode::Original,
                    "Minimum" => SizeMode::Minimum,
                    "Custom" => SizeMode::Custom,
                    other => return Err(format!("no size mode {other:?}")),
                };
                s.configs.first_mut().ok_or("no rows")?.set_choice(mode);
            }
            _ => {
                let mib = arg.and_then(Value::as_integer).ok_or("needs a number")?;
                s.configs.first_mut().ok_or("no rows")?.custom_size_mib = mib as u32;
            }
        }
        Ok(())
    }

    fn export_probe(&self, name: &str) -> Result<Value, String> {
        use rusty_backup::controller::inspect::EXPORT_FORMATS;
        let s = self.ctl.export_settings();
        Ok(match name {
            "export_format" => Value::String(
                EXPORT_FORMATS
                    .iter()
                    .find(|c| c.format == s.format)
                    .map_or("", |c| c.label)
                    .to_string(),
            ),
            "export_whole_disk" => Value::Boolean(s.whole_disk),
            "export_modes" => {
                let cfg = s.configs.first().ok_or("no rows")?;
                let modes: Vec<String> = cfg
                    .mode_choices()
                    .iter()
                    .map(|m| format!("{m:?}"))
                    .collect();
                Value::String(modes.join(","))
            }
            _ => {
                let cfg = s.configs.first().ok_or("no rows")?;
                Value::Integer((cfg.effective_size() / (1024 * 1024)) as i64)
            }
        })
    }
}

impl Target for InspectTarget {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        use rusty_backup::controller::inspect::{InspectAction, InspectSource};
        let a = match action {
            "OpenImage" => {
                let arg = arg_str(arg)?;
                let path = if arg == "$blank_fat" {
                    let p = self.dir.path().join("vol.img");
                    let bytes = fs_pc::fat::create_blank_fat(2 * 1024 * 1024, Some("T"))
                        .map_err(|e| e.to_string())?;
                    std::fs::write(&p, bytes).map_err(|e| e.to_string())?;
                    p
                } else if arg == "$mbr_gap" {
                    self.mbr_gap()?
                } else {
                    PathBuf::from(arg)
                };
                InspectAction::Open(InspectSource::Image { path, temp: None })
            }
            "Close" => InspectAction::Close,
            "Reinspect" => InspectAction::Reinspect,
            "Check" => {
                let row = arg.and_then(Value::as_integer).ok_or("Check needs a row")? as usize;
                let index = self.ctl.partitions().get(row).ok_or("no such row")?.index;
                InspectAction::Check(index)
            }
            "SetBrowseDirty" => {
                self.ctl
                    .set_browse_dirty(arg.and_then(Value::as_bool).ok_or("needs a bool")?);
                return Ok(Vec::new());
            }
            "Confirm" => InspectAction::Confirmed {
                purpose: self.asked.take().ok_or("nothing was asked")?,
                yes: arg.and_then(Value::as_bool).ok_or("needs a bool")?,
            },
            "OpenExport" => InspectAction::OpenExport,
            "ExportFormat" | "ExportWholeDisk" | "ExportSizeMode" | "ExportCustomMib" => {
                return self.export_edit(action, arg).map(|_| Vec::new());
            }
            "EditTable" | "AddPartition" | "CloseEditor" | "TableMode" | "TableDelete"
            | "TableUndo" | "TableAdd" | "TableValidate" | "ApplyTable" => {
                return self.table_act(action, arg);
            }
            other => return Err(format!("InspectController has no action {other:?}")),
        };
        let fx = self.ctl.apply(a);
        Ok(self.note(fx))
    }

    fn probe(&self, name: &str) -> Result<Value, String> {
        Ok(match name {
            "source" => Value::String(
                if self.ctl.image_path().is_some() {
                    "image"
                } else if self.ctl.has_source() {
                    "other"
                } else {
                    "none"
                }
                .to_string(),
            ),
            "partitions" => Value::Integer(self.ctl.partitions().len() as i64),
            "superfloppy" => {
                Value::Boolean(self.ctl.table_summary().is_some_and(|t| t.superfloppy))
            }
            "error" => Value::String(self.ctl.last_error().unwrap_or("").to_string()),
            "source_label" => Value::String(self.ctl.source_label(&[])),
            "can_close" => Value::Boolean(self.ctl.can_close()),
            "export_format" | "export_whole_disk" | "export_modes" | "export_size_mib" => {
                self.export_probe(name)?
            }
            "check_clean" => {
                Value::Boolean(self.ctl.fsck_view().is_some_and(|v| v.result.is_clean()))
            }
            "log" => Value::String(self.log.join("\n")),
            "can_add_partition" | "editor_open" | "table_rows" | "table_modes"
            | "table_can_delete" | "table_can_apply" | "table_errors" | "floppy_source"
            | "alto_source" => self.table_probe(name)?,
            other => return Err(format!("InspectController has no probe {other:?}")),
        })
    }

    fn settle(&mut self) -> Vec<Effect> {
        let mut fx = Vec::new();
        loop {
            fx.extend(self.ctl.tick());
            if self.ctl.loading().is_none()
                && self.ctl.tool_jobs().is_empty()
                && !self.ctl.probing()
            {
                fx.extend(self.ctl.tick());
                return self.note(fx);
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
}

/// The Backup tab's controller. Actions: `OpenImage` (arg: a path, or `$blank_fat` for a fresh
/// FAT image), `SetDestination` (a path, or `$out` for a temp folder), `SetName`, `SetFormat`
/// (`zstd`, `vhd`, `chd`, `raw`), `SelectAll`, `SelectNone`, `SetSectorBySector` / `SetResize` /
/// `SetSplit` (bool), `SetSplitMib` (int), `Start`, `VhdClose`, `VhdWholeDisk` (bool), `VhdSize`
/// (`original`, `minimum` or a Custom MiB int, on the first row), `VhdStart`, `RemoteOpen`,
/// `Close`. Probes: `source`, `partitions`, `selected`, `error` (why Start is refused, empty when
/// it is not), `vhd_open`, `vhd_mode`, `vhd_custom_mib`, `vhd_at_minimum` (the Custom size is the
/// row's lowest), `split_size` (0 for none), `resize`, `remote_open`, `result` (`ok`, the failure,
/// or empty), `output` (the backup folder holds a `metadata.json`), `log` (every line so far).
struct BackupTarget {
    ctl: rusty_backup::controller::backup::BackupController,
    dir: tempfile::TempDir,
    log: Vec<String>,
}

impl BackupTarget {
    fn new() -> Self {
        use rusty_backup::controller::backup::{
            BackupController, BackupServices, JobRegistry, NoNotify,
        };
        BackupTarget {
            ctl: BackupController::new(BackupServices {
                jobs: JobRegistry::new(),
                notify: std::sync::Arc::new(NoNotify),
            }),
            dir: tempfile::tempdir().expect("temp dir"),
            log: Vec::new(),
        }
    }

    fn note(&mut self, fx: Vec<Effect>) -> Vec<Effect> {
        self.log
            .extend(self.ctl.take_log().into_iter().map(|(_, m)| m));
        fx
    }
}

impl Target for BackupTarget {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        use rusty_backup::controller::backup::{BackupAction, CompressionType};
        let a = match action {
            "OpenImage" => {
                let arg = arg_str(arg)?;
                let path = if arg == "$blank_fat" {
                    let p = self.dir.path().join("vol.img");
                    let bytes = fs_pc::fat::create_blank_fat(2 * 1024 * 1024, Some("T"))
                        .map_err(|e| e.to_string())?;
                    std::fs::write(&p, bytes).map_err(|e| e.to_string())?;
                    p
                } else {
                    PathBuf::from(arg)
                };
                BackupAction::OpenImage { path, rename: true }
            }
            "SetDestination" => {
                let arg = arg_str(arg)?;
                let path = if arg == "$out" {
                    let p = self.dir.path().join("out");
                    std::fs::create_dir_all(&p).map_err(|e| e.to_string())?;
                    p
                } else {
                    PathBuf::from(arg)
                };
                BackupAction::SetDestination(Some(path))
            }
            "SetName" => BackupAction::SetName(arg_str(arg)?.to_string()),
            "SetFormat" => BackupAction::SetFormat(match arg_str(arg)? {
                "zstd" => CompressionType::Zstd,
                "vhd" => CompressionType::Vhd,
                "chd" => CompressionType::Chd,
                "raw" => CompressionType::None,
                other => return Err(format!("unknown format {other:?}")),
            }),
            "SelectAll" => BackupAction::SelectAll,
            "SelectNone" => BackupAction::SelectNone,
            "SetSectorBySector" => {
                BackupAction::SetSectorBySector(arg.and_then(Value::as_bool).ok_or("needs a bool")?)
            }
            "SetResize" => {
                BackupAction::SetResize(arg.and_then(Value::as_bool).ok_or("needs a bool")?)
            }
            "SetSplit" => {
                BackupAction::SetSplit(arg.and_then(Value::as_bool).ok_or("needs a bool")?)
            }
            "SetSplitMib" => BackupAction::SetSplitMib(
                arg.and_then(Value::as_integer).ok_or("needs an int")? as u32,
            ),
            "Start" => BackupAction::Start(None),
            "VhdClose" => BackupAction::VhdClose,
            "VhdWholeDisk" => {
                BackupAction::VhdWholeDisk(arg.and_then(Value::as_bool).ok_or("needs a bool")?)
            }
            "VhdSize" => {
                use rusty_backup::controller::backup::SizeMode;
                let row = self.ctl.vhd_rows().first().cloned().ok_or("no VHD row")?;
                let (mode, custom_mib) = match arg {
                    Some(Value::String(m)) if m == "original" => {
                        (SizeMode::Original, row.custom_mib)
                    }
                    Some(Value::String(m)) if m == "minimum" => (SizeMode::Minimum, row.custom_mib),
                    Some(Value::Integer(n)) => (SizeMode::Custom, *n as u32),
                    _ => return Err("VhdSize needs original, minimum or a MiB int".into()),
                };
                BackupAction::VhdSize {
                    index: row.index,
                    mode,
                    custom_mib,
                }
            }
            "VhdStart" => BackupAction::VhdStart,
            "RemoteOpen" => BackupAction::RemoteOpen,
            "Close" => BackupAction::CloseSource,
            other => return Err(format!("BackupController has no action {other:?}")),
        };
        let fx = self.ctl.apply(a);
        Ok(self.note(fx))
    }

    fn probe(&self, name: &str) -> Result<Value, String> {
        Ok(match name {
            "source" => Value::String(
                if self.ctl.image_path().is_some() {
                    "image"
                } else if self.ctl.has_source() {
                    "other"
                } else {
                    "none"
                }
                .to_string(),
            ),
            "partitions" => Value::Integer(self.ctl.partition_count() as i64),
            "selected" => Value::Integer(self.ctl.selected_count() as i64),
            "error" => Value::String(self.ctl.validate().err().unwrap_or_default()),
            "vhd_open" => Value::Boolean(self.ctl.vhd_open()),
            "vhd_mode" => Value::String(
                self.ctl
                    .vhd_rows()
                    .first()
                    .map(|r| format!("{:?}", r.mode))
                    .unwrap_or_default(),
            ),
            "vhd_custom_mib" => Value::Integer(
                self.ctl
                    .vhd_rows()
                    .first()
                    .map(|r| r.custom_mib as i64)
                    .unwrap_or(-1),
            ),
            "vhd_at_minimum" => Value::Boolean(
                self.ctl
                    .vhd_rows()
                    .first()
                    .is_some_and(|r| r.custom_mib == r.custom_bounds_mib().0),
            ),
            "split_size" => Value::Integer(self.ctl.options().split_size().unwrap_or(0) as i64),
            "resize" => Value::Boolean(self.ctl.options().resize),
            "remote_open" => Value::Boolean(self.ctl.remote_dialog_open()),
            "result" => Value::String(match self.ctl.last_result() {
                Some(Ok(())) => "ok".to_string(),
                Some(Err(e)) => e.clone(),
                None => String::new(),
            }),
            "output" => Value::Boolean(
                self.dir
                    .path()
                    .join("out")
                    .join(self.ctl.name())
                    .join("metadata.json")
                    .exists(),
            ),
            "log" => Value::String(self.log.join("\n")),
            other => return Err(format!("BackupController has no probe {other:?}")),
        })
    }

    fn settle(&mut self) -> Vec<Effect> {
        self.ctl.wait_idle();
        self.ctl.wait_finished();
        let fx = self.ctl.tick();
        self.note(fx)
    }
}

/// The Restore tab's controller. Actions: `OpenBackup` (a path, `$backup` for a fresh zstd backup
/// of a blank FAT image, or `$empty` for an empty folder), `SetMode` (`full`, `single`, `new`,
/// `write`, `build`), `SetTargetIsDevice` (bool), `SetTargetImage` (a path, or `$out`),
/// `SetSize` (`original` or `minimum`, for every partition), `Start`, `Confirm`, `ClearBackup`, and
/// Build Disk's `BuildTarget` (an image target of this size), `BuildDefine`, `BuildRowSize`
/// (`row:size`), `BuildAddRow`, `BuildApply`, `SetCustomSize` / `SetCustomAlignment` /
/// `SetExpandMib` (integers), `OpenWriteImage` (`$superfloppy`), `WriteImageTarget`.
/// Probes: `backup` (one is loaded), `partitions`, `error` (why Start is refused, empty when it is
/// not), `load_error`, `result` (`ok`, the failure, or empty), `output` (the restored image equals
/// the image backed up), `log` (every line so far), `build_rows` (-1 with no plan open),
/// `build_layout` (`ok` or why it cannot be placed), `build_result`, `built_mbr`, `modes`,
/// `custom_mib`, `alignment`, `expand_mib`, `expand_cap`, `write_superfloppy`, `write_request`.
struct RestoreTarget {
    ctl: rusty_backup::controller::restore::RestoreController,
    dir: tempfile::TempDir,
    log: Vec<String>,
    requests: Vec<rusty_backup::controller::restore::HostRequest>,
}

impl RestoreTarget {
    fn new() -> Self {
        use rusty_backup::controller::restore::{
            JobRegistry, NoNotify, RestoreController, RestoreServices,
        };
        RestoreTarget {
            ctl: RestoreController::new(RestoreServices {
                jobs: JobRegistry::new(),
                notify: std::sync::Arc::new(NoNotify),
            }),
            dir: tempfile::tempdir().expect("temp dir"),
            log: Vec::new(),
            requests: Vec::new(),
        }
    }

    fn note(&mut self, fx: Vec<Effect>) -> Vec<Effect> {
        self.log
            .extend(self.ctl.take_log().into_iter().map(|(_, m)| m));
        self.requests.extend(self.ctl.take_requests());
        fx
    }

    /// A zstd backup of a blank FAT image, made by the Backup controller.
    fn make_backup(&self) -> Result<PathBuf, String> {
        use rusty_backup::controller::backup::{
            BackupAction, BackupController, BackupServices, JobRegistry, NoNotify,
        };
        let img = self.dir.path().join("vol.img");
        let bytes =
            fs_pc::fat::create_blank_fat(2 * 1024 * 1024, Some("T")).map_err(|e| e.to_string())?;
        std::fs::write(&img, bytes).map_err(|e| e.to_string())?;
        let mut b = BackupController::new(BackupServices {
            jobs: JobRegistry::new(),
            notify: std::sync::Arc::new(NoNotify),
        });
        b.apply(BackupAction::OpenImage {
            path: img,
            rename: false,
        });
        b.wait_idle();
        b.apply(BackupAction::SetDestination(Some(
            self.dir.path().to_path_buf(),
        )));
        b.apply(BackupAction::SetName("bk".into()));
        b.apply(BackupAction::Start(None));
        b.wait_finished();
        match b.last_result() {
            Some(Ok(())) => Ok(self.dir.path().join("bk")),
            other => Err(format!("the fixture backup failed: {other:?}")),
        }
    }
}

impl Target for RestoreTarget {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        use rusty_backup::controller::restore::{
            purpose, RestoreAction, RestoreMode, SizeMode, TargetSlot,
        };
        let a = match action {
            "OpenBackup" => {
                let path = match arg_str(arg)? {
                    "$backup" => self.make_backup()?,
                    "$empty" => {
                        let p = self.dir.path().join("empty");
                        std::fs::create_dir_all(&p).map_err(|e| e.to_string())?;
                        p
                    }
                    other => PathBuf::from(other),
                };
                RestoreAction::OpenBackup(path)
            }
            "SetMode" => RestoreAction::SetMode(match arg_str(arg)? {
                "full" => RestoreMode::FullDisk,
                "single" => RestoreMode::SinglePartition,
                "new" => RestoreMode::NewDisk,
                "write" => RestoreMode::WriteImage,
                "build" => RestoreMode::BuildDisk,
                other => return Err(format!("unknown mode {other:?}")),
            }),
            "SetTargetIsDevice" => RestoreAction::SetTargetIsDevice(
                TargetSlot::FullDisk,
                arg.and_then(Value::as_bool)
                    .ok_or_else(|| "SetTargetIsDevice needs a bool `arg`".to_string())?,
            ),
            "SetTargetImage" => {
                let path = match arg_str(arg)? {
                    "$out" => self.dir.path().join("restored.img"),
                    other => PathBuf::from(other),
                };
                RestoreAction::SetTargetImage(TargetSlot::FullDisk, Some(path))
            }
            "SetSize" => {
                let mode = match arg_str(arg)? {
                    "original" => SizeMode::Original,
                    "minimum" => SizeMode::Minimum,
                    other => return Err(format!("unknown size {other:?}")),
                };
                let rows: Vec<(usize, u32)> = self
                    .ctl
                    .size_rows()
                    .iter()
                    .map(|r| (r.index, r.custom_mib))
                    .collect();
                for (index, custom_mib) in rows {
                    self.ctl.apply(RestoreAction::SetPartitionSize {
                        index,
                        mode,
                        custom_mib,
                    });
                }
                return Ok(Vec::new());
            }
            "SetCustomSize" => {
                let mib = arg
                    .and_then(Value::as_integer)
                    .ok_or_else(|| "SetCustomSize needs an integer `arg`".to_string())?;
                let index = self.ctl.size_rows().first().ok_or("no size rows")?.index;
                RestoreAction::SetPartitionSize {
                    index,
                    mode: SizeMode::Custom,
                    custom_mib: mib as u32,
                }
            }
            "SetCustomAlignment" => RestoreAction::SetCustomAlignment(
                arg.and_then(Value::as_integer)
                    .ok_or_else(|| "SetCustomAlignment needs an integer `arg`".to_string())?
                    as u32,
            ),
            "SetExpandMib" => RestoreAction::SetExpandMib(
                arg.and_then(Value::as_integer)
                    .ok_or_else(|| "SetExpandMib needs an integer `arg`".to_string())?
                    as u32,
            ),
            "OpenWriteImage" => {
                let path = match arg_str(arg)? {
                    "$superfloppy" => {
                        let p = self.dir.path().join("vol.img");
                        let bytes = fs_pc::fat::create_blank_fat(2 * 1024 * 1024, Some("T"))
                            .map_err(|e| e.to_string())?;
                        std::fs::write(&p, bytes).map_err(|e| e.to_string())?;
                        p
                    }
                    other => PathBuf::from(other),
                };
                RestoreAction::OpenWriteImage(path)
            }
            "WriteImageTarget" => RestoreAction::WriteImageTarget,
            "Start" => RestoreAction::Start,
            "Confirm" => RestoreAction::Confirmed {
                purpose: purpose::RESTORE,
                yes: true,
            },
            "ClearBackup" => RestoreAction::ClearBackup,
            "BuildTarget" => {
                self.ctl
                    .apply(RestoreAction::SetMode(RestoreMode::BuildDisk));
                self.ctl.apply(RestoreAction::SetTargetIsDevice(
                    TargetSlot::BuildDisk,
                    false,
                ));
                self.ctl.apply(RestoreAction::SetTargetImage(
                    TargetSlot::BuildDisk,
                    Some(self.dir.path().join("built.img")),
                ));
                RestoreAction::SetBuildSize(arg_str(arg)?.to_string())
            }
            "BuildDefine" => RestoreAction::BuildDefine,
            "BuildRowSize" => {
                use rusty_backup::controller::partition_edit::BuildAction;
                let text = arg_str(arg)?;
                let (row, text) = text
                    .split_once(':')
                    .ok_or("BuildRowSize wants `row:size`")?;
                RestoreAction::Build(BuildAction::SetRowSize {
                    row: row.parse().map_err(|_| "bad row".to_string())?,
                    text: text.to_string(),
                })
            }
            "BuildAddRow" => {
                RestoreAction::Build(rusty_backup::controller::partition_edit::BuildAction::AddRow)
            }
            "BuildApply" => RestoreAction::BuildApply,
            other => return Err(format!("RestoreController has no action {other:?}")),
        };
        let fx = self.ctl.apply(a);
        Ok(self.note(fx))
    }

    fn probe(&self, name: &str) -> Result<Value, String> {
        Ok(match name {
            "backup" => Value::Boolean(self.ctl.has_backup()),
            "partitions" => Value::Integer(self.ctl.size_rows().len() as i64),
            "error" => Value::String(self.ctl.validate().err().unwrap_or_default()),
            "load_error" => Value::String(self.ctl.load_error().unwrap_or("").to_string()),
            "result" => Value::String(match self.ctl.last_result() {
                Some(Ok(())) => "ok".to_string(),
                Some(Err(e)) => e.clone(),
                None => String::new(),
            }),
            "output" => {
                let read = |n: &str| std::fs::read(self.dir.path().join(n)).ok();
                Value::Boolean(
                    read("restored.img").is_some() && read("restored.img") == read("vol.img"),
                )
            }
            "log" => Value::String(self.log.join("\n")),
            "modes" => Value::String(match self.ctl.size_rows().first() {
                Some(r) => self
                    .ctl
                    .size_mode_choices(r.index)
                    .iter()
                    .map(|m| format!("{m:?}"))
                    .collect::<Vec<_>>()
                    .join(","),
                None => String::new(),
            }),
            "custom_mib" => Value::Integer(
                self.ctl
                    .size_rows()
                    .first()
                    .map_or(-1, |r| r.custom_mib_effective() as i64),
            ),
            "alignment" => Value::Integer(self.ctl.alignment().1 as i64),
            "expand_mib" => Value::Integer(self.ctl.expand().free_mib as i64),
            "expand_cap" => Value::Integer(self.ctl.expand_cap_mib() as i64),
            "write_superfloppy" => {
                Value::String(self.ctl.write_image_superfloppy().unwrap_or("").to_string())
            }
            "write_request" => Value::String(
                match self.requests.iter().find_map(|r| match r {
                    rusty_backup::controller::restore::HostRequest::WriteImage(o) => Some(o),
                    _ => None,
                }) {
                    Some(o) => format!(
                        "{}:{}:{}",
                        o.size_bytes.unwrap_or(0),
                        o.superfloppy,
                        o.synthesize
                    ),
                    None => String::new(),
                },
            ),
            "build_rows" => {
                Value::Integer(self.ctl.build_view().map_or(-1, |v| v.rows.len() as i64))
            }
            "build_layout" => Value::String(match self.ctl.build_view().map(|v| v.layout) {
                Some(Ok(_)) => "ok".to_string(),
                Some(Err(e)) => e,
                None => String::new(),
            }),
            "build_result" => Value::String(match self.ctl.build_result() {
                Some(Ok(())) => "ok".to_string(),
                Some(Err(e)) => e.clone(),
                None => String::new(),
            }),
            "built_mbr" => Value::Boolean(
                std::fs::read(self.dir.path().join("built.img"))
                    .map(|d| d.len() > 512 && d[510..512] == [0x55, 0xAA])
                    .unwrap_or(false),
            ),
            other => return Err(format!("RestoreController has no probe {other:?}")),
        })
    }

    fn settle(&mut self) -> Vec<Effect> {
        self.ctl.wait_idle();
        self.ctl.wait_finished();
        let fx = self.ctl.tick();
        self.note(fx)
    }
}

/// The disk writer (Physical Disk Export, Write Image File). Actions: `Open` (`$image` for a
/// patterned 1 MiB image, `$superfloppy` for the same as a table-less FAT16 volume),
/// `SelectDevice` (`$disk` for an 8 MiB disk-image file standing in for a device, `$table` for one
/// with an MBR partition at LBA 2048), `SetTargetImage` (`$out`), `CyclePartition`,
/// `SetSynthesize` (bool), `Write`, `AnswerName` (a string, or `$name` for the device's own),
/// `Confirm`, `Close`. Probes: `open`, `size`, `synthesize`, `partitions`, `partition` (the
/// chosen index, -1 for the whole disk), `can_write`, `error`, `result` (`ok`, the failure or
/// empty), `written` (the source landed where the write was aimed, the table intact), `log`.
struct DiskWriteTarget {
    ctl: rusty_backup::controller::disk_write::DiskWriteController,
    dir: tempfile::TempDir,
    log: Vec<String>,
}

impl DiskWriteTarget {
    fn new() -> Self {
        use rusty_backup::controller::disk_write::{
            DiskWriteController, DiskWriteServices, JobRegistry, NoNotify,
        };
        DiskWriteTarget {
            ctl: DiskWriteController::new(DiskWriteServices {
                jobs: JobRegistry::new(),
                notify: std::sync::Arc::new(NoNotify),
            }),
            dir: tempfile::tempdir().expect("temp dir"),
            log: Vec::new(),
        }
    }

    fn note(&mut self, fx: Vec<Effect>) -> Vec<Effect> {
        self.log
            .extend(self.ctl.take_log().into_iter().map(|(_, m)| m));
        fx
    }

    fn source_bytes() -> Vec<u8> {
        (0..(1usize << 20)).map(|i| (i % 247) as u8).collect()
    }

    /// An 8 MiB disk-image file standing in for a device; tests never touch a real disk.
    fn fake_disk(&self, table: bool) -> Result<rusty_backup::device::DiskDevice, String> {
        let path = self.dir.path().join("disk.img");
        let mut d = vec![0u8; 8 << 20];
        if table {
            d[0x1BE + 4] = 0x06;
            d[0x1BE + 8..0x1BE + 12].copy_from_slice(&2048u32.to_le_bytes());
            d[0x1BE + 12..0x1BE + 16].copy_from_slice(&4096u32.to_le_bytes());
            d[510] = 0x55;
            d[511] = 0xAA;
        }
        std::fs::write(&path, &d).map_err(|e| e.to_string())?;
        std::fs::write(self.dir.path().join("disk.before"), &d).map_err(|e| e.to_string())?;
        Ok(rusty_backup::device::DiskDevice {
            name: "fake".into(),
            path,
            size_bytes: 8 << 20,
            is_removable: true,
            is_read_only: false,
            is_system: false,
            bus_protocol: "USB".into(),
            media_name: "Fake".into(),
            partitions: Vec::new(),
        })
    }

    /// Whether the source sits where the last write aimed it, and nothing else moved.
    fn written(&self) -> bool {
        let src = Self::source_bytes();
        let read = |n: &str| std::fs::read(self.dir.path().join(n)).ok();
        if let Some(out) = read("out.img") {
            return out == src;
        }
        let (Some(disk), Some(before)) = (read("disk.img"), read("disk.before")) else {
            return false;
        };
        let at = 1usize << 20;
        let body = disk[at..at + src.len()] == src[..];
        let wrapped = disk[510..512] == [0x55, 0xAA];
        // A partition write keeps the table; a synthesized one writes a fresh MBR.
        let head = if before[510] == 0x55 {
            disk[..512] == before[..512]
        } else {
            wrapped
        };
        body && head
    }
}

impl Target for DiskWriteTarget {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        use rusty_backup::controller::disk_write::{purpose, DiskWriteAction, WriteOpen};
        let a = match action {
            "Open" => {
                let path = self.dir.path().join("src.img");
                std::fs::write(&path, Self::source_bytes()).map_err(|e| e.to_string())?;
                DiskWriteAction::Open(match arg_str(arg)? {
                    "$image" => WriteOpen::image(path, 1 << 20),
                    "$superfloppy" => WriteOpen {
                        path,
                        size_bytes: None,
                        fs_hint: "FAT16".into(),
                        has_partition_table: false,
                        superfloppy: true,
                        synthesize: true,
                    },
                    other => return Err(format!("unknown source {other:?}")),
                })
            }
            "SelectDevice" => {
                let device = match arg_str(arg)? {
                    "$disk" => self.fake_disk(false)?,
                    "$table" => self.fake_disk(true)?,
                    other => return Err(format!("unknown device {other:?}")),
                };
                DiskWriteAction::SelectDevice { row: 0, device }
            }
            "SetTargetImage" => DiskWriteAction::SetTargetImage(match arg_str(arg)? {
                "$out" => self.dir.path().join("out.img"),
                other => PathBuf::from(other),
            }),
            "CyclePartition" => DiskWriteAction::CycleTargetPartition,
            "SetSynthesize" => DiskWriteAction::SetSynthesize(
                arg.and_then(Value::as_bool)
                    .ok_or_else(|| "SetSynthesize needs a bool `arg`".to_string())?,
            ),
            "Write" => DiskWriteAction::Write,
            "AnswerName" => {
                let text = match arg_str(arg)? {
                    "$name" => self.ctl.device_name().unwrap_or_default(),
                    other => other.to_string(),
                };
                DiskWriteAction::Answered {
                    purpose: purpose::DEVICE_NAME,
                    text: Some(text),
                }
            }
            "Confirm" => DiskWriteAction::Confirmed {
                purpose: purpose::OVERWRITE_FILE,
                yes: true,
            },
            "Close" => DiskWriteAction::Close,
            other => return Err(format!("DiskWriteController has no action {other:?}")),
        };
        let fx = self.ctl.apply(a);
        Ok(self.note(fx))
    }

    fn probe(&self, name: &str) -> Result<Value, String> {
        Ok(match name {
            "open" => Value::Boolean(self.ctl.is_open()),
            "size" => Value::Integer(self.ctl.source().map_or(0, |s| s.size_bytes) as i64),
            "synthesize" => Value::Boolean(self.ctl.form().synthesize),
            "partitions" => Value::Integer(self.ctl.target_partitions().len() as i64),
            "partition" => Value::Integer(self.ctl.target_partition().map_or(-1, |i| i as i64)),
            "can_write" => Value::Boolean(self.ctl.can_write()),
            "error" => Value::String(self.ctl.error().unwrap_or("").to_string()),
            "result" => Value::String(match self.ctl.result() {
                Some(Ok(_)) => "ok".to_string(),
                Some(Err(e)) => e.clone(),
                None => String::new(),
            }),
            "written" => Value::Boolean(self.written()),
            "log" => Value::String(self.log.join("\n")),
            other => return Err(format!("DiskWriteController has no probe {other:?}")),
        })
    }

    fn settle(&mut self) -> Vec<Effect> {
        self.ctl.wait_idle();
        self.ctl.wait_finished();
        let fx = self.ctl.tick();
        self.note(fx)
    }
}

/// `AltoResizeController`. Actions: `Open` (arg: `$diablo31` writes a blank Diablo 31 pack, or a
/// path), `SetTarget` (`31` or `44`), `SaveAs`, `Pick` (the output, `$out`), `Close`. Probes: `open`,
/// `summary`, `target`, `fit_warning`, `result`, `out_model` (the written pack's Diablo model).
struct AltoResizeTarget {
    ctl: rusty_backup::controller::alto_resize::AltoResizeController,
    dir: tempfile::TempDir,
}

impl AltoResizeTarget {
    fn new() -> Self {
        AltoResizeTarget {
            ctl: rusty_backup::controller::alto_resize::AltoResizeController::new(
                std::sync::Arc::new(rusty_backup::model::job::NoNotify),
            ),
            dir: tempfile::tempdir().expect("temp dir"),
        }
    }
}

impl Target for AltoResizeTarget {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        use rusty_backup::controller::alto_resize::{purpose, AltoResizeAction, AltoTarget};
        let a = match action {
            "Open" => AltoResizeAction::Open(match arg_str(arg)? {
                "$diablo31" => {
                    let p = self.dir.path().join("src.pdi");
                    let disk = fs_xerox::alto::write::create_blank(AltoTarget::Diablo31.geometry())
                        .map_err(|e| e.to_string())?;
                    std::fs::write(&p, fs_xerox::alto::pdi::write(&disk))
                        .map_err(|e| e.to_string())?;
                    p
                }
                other => PathBuf::from(other),
            }),
            "SetTarget" => AltoResizeAction::SetTarget(match arg.and_then(Value::as_integer) {
                Some(31) => AltoTarget::Diablo31,
                Some(44) => AltoTarget::Diablo44,
                _ => return Err("SetTarget needs 31 or 44".into()),
            }),
            "SaveAs" => AltoResizeAction::SaveAs,
            "Pick" => AltoResizeAction::Picked {
                purpose: purpose::OUTPUT,
                path: Some(self.dir.path().join("out.pdi")),
            },
            "Close" => AltoResizeAction::Close,
            other => return Err(format!("AltoResizeController has no action {other:?}")),
        };
        Ok(self.ctl.apply(a))
    }

    fn probe(&self, name: &str) -> Result<Value, String> {
        use rusty_backup::controller::alto_resize::AltoTarget;
        Ok(match name {
            "open" => Value::Boolean(self.ctl.is_open()),
            "summary" => Value::String(self.ctl.summary_line().unwrap_or("").to_string()),
            "target" => Value::Integer(match self.ctl.target() {
                AltoTarget::Diablo31 => 31,
                AltoTarget::Diablo44 => 44,
            }),
            "fit_warning" => Value::String(self.ctl.fit_warning().unwrap_or_default()),
            "result" => Value::String(match self.ctl.result() {
                Some(Ok(m)) => m.clone(),
                Some(Err(e)) => format!("error: {e}"),
                None => String::new(),
            }),
            "out_model" => {
                let bytes = std::fs::read(self.dir.path().join("out.pdi")).unwrap_or_default();
                Value::Integer(
                    fs_xerox::alto::open_pack(&bytes)
                        .map(|d| d.geometry.disk_model as i64)
                        .unwrap_or(0),
                )
            }
            other => return Err(format!("AltoResizeController has no probe {other:?}")),
        })
    }

    fn settle(&mut self) -> Vec<Effect> {
        self.ctl.wait_idle();
        self.ctl.tick()
    }
}

/// `ExpandHfsController`. Actions: `Open` (arg: `$hfv` writes and summarises a blank 4 MiB HFS
/// volume), `SetFlatHfv` (bool), `SetSizeMib` (int), `PickOutput`, `Pick` (the output, in the temp
/// folder), `Start`, `Close`. Probes: `open`, `flat_hfv`, `size_mib`, `can_start`, `outcome`
/// (`ok`, an error, or empty), `out_mib` (the output's size in whole MiB, rounded up).
struct ExpandHfsTarget {
    ctl: rusty_backup::controller::expand_hfs::ExpandHfsController,
    dir: tempfile::TempDir,
}

impl ExpandHfsTarget {
    fn new() -> Self {
        ExpandHfsTarget {
            ctl: rusty_backup::controller::expand_hfs::ExpandHfsController::new(
                std::sync::Arc::new(rusty_backup::model::job::NoNotify),
            ),
            dir: tempfile::tempdir().expect("temp dir"),
        }
    }
}

impl Target for ExpandHfsTarget {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        use rusty_backup::controller::expand_hfs::{purpose, ExpandHfsAction};
        let a = match action {
            "Open" => {
                if arg_str(arg)? != "$hfv" {
                    return Err("Open takes $hfv".into());
                }
                let p = self.dir.path().join("vol.hfv");
                let bytes = fs_mac::hfs::create_blank_hfs(4 << 20, 4096, "Scenario")
                    .map_err(|e| e.to_string())?;
                std::fs::write(&p, bytes).map_err(|e| e.to_string())?;
                let src = rusty_backup::model::hfs_expand_runner::summarize_source(&p, 0, 4 << 20)
                    .map_err(|e| e.to_string())?;
                ExpandHfsAction::Open(src)
            }
            "SetFlatHfv" => ExpandHfsAction::SetFlatHfv(
                arg.and_then(Value::as_bool)
                    .ok_or("SetFlatHfv needs a bool")?,
            ),
            "SetSizeMib" => ExpandHfsAction::SetSizeMib(
                arg.and_then(Value::as_integer)
                    .ok_or("SetSizeMib needs an int")? as u32,
            ),
            "PickOutput" => ExpandHfsAction::PickOutput,
            "Pick" => ExpandHfsAction::Picked {
                purpose: purpose::OUTPUT,
                path: Some(self.dir.path().join("out.img")),
            },
            "Start" => ExpandHfsAction::Start,
            "Close" => ExpandHfsAction::Close,
            other => return Err(format!("ExpandHfsController has no action {other:?}")),
        };
        Ok(self.ctl.apply(a))
    }

    fn probe(&self, name: &str) -> Result<Value, String> {
        Ok(match name {
            "open" => Value::Boolean(self.ctl.is_open()),
            "flat_hfv" => Value::Boolean(self.ctl.flat_hfv()),
            "size_mib" => Value::Integer(self.ctl.size_mib() as i64),
            "can_start" => Value::Boolean(self.ctl.can_start()),
            "outcome" => Value::String(match self.ctl.outcome() {
                Some(Ok(())) => "ok".to_string(),
                Some(Err(e)) => e.clone(),
                None => String::new(),
            }),
            "out_mib" => Value::Integer(
                std::fs::metadata(self.dir.path().join("out.img"))
                    .map(|m| m.len().div_ceil(1 << 20) as i64)
                    .unwrap_or(0),
            ),
            other => return Err(format!("ExpandHfsController has no probe {other:?}")),
        })
    }

    fn settle(&mut self) -> Vec<Effect> {
        self.ctl.wait_idle();
        self.ctl.tick()
    }
}

/// `FloppyConvertController`. Actions: `Open` (arg: `$xdf` writes a blank 1.2 MB XDF, or a path),
/// `SetTarget` (`xdf`, `hdm`, `dim`, `d88`), `PickOutput`, `Pick` (the output, `out.<ext>` in the
/// temp folder), `Start`, `Close`. Probes: `open`, `source_kind`, `source_error`, `target`,
/// `can_start`, `summary`, `out_bytes`.
struct FloppyConvertTarget {
    ctl: rusty_backup::controller::floppy_convert::FloppyConvertController,
    dir: tempfile::TempDir,
}

impl FloppyConvertTarget {
    fn new() -> Self {
        FloppyConvertTarget {
            ctl: rusty_backup::controller::floppy_convert::FloppyConvertController::new(
                std::sync::Arc::new(rusty_backup::model::job::NoNotify),
            ),
            dir: tempfile::tempdir().expect("temp dir"),
        }
    }

    fn out(&self) -> PathBuf {
        self.dir
            .path()
            .join(format!("out.{}", self.ctl.target().extension()))
    }
}

impl Target for FloppyConvertTarget {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        use rusty_backup::controller::floppy_convert::{
            purpose, FloppyConvertAction, FloppyTarget,
        };
        let a = match action {
            "Open" => FloppyConvertAction::Open(Some(match arg_str(arg)? {
                "$xdf" => {
                    let p = self.dir.path().join("disk.xdf");
                    std::fs::write(&p, vec![0xE5u8; 1_261_568]).map_err(|e| e.to_string())?;
                    p
                }
                other => PathBuf::from(other),
            })),
            "SetTarget" => {
                let want = arg_str(arg)?;
                let t = FloppyTarget::ALL
                    .iter()
                    .copied()
                    .find(|t| t.extension() == want)
                    .ok_or("SetTarget takes xdf, hdm, dim or d88")?;
                FloppyConvertAction::SetTarget(t)
            }
            "PickOutput" => FloppyConvertAction::PickOutput,
            "Pick" => FloppyConvertAction::Picked {
                purpose: purpose::OUTPUT,
                path: Some(self.out()),
            },
            "Start" => FloppyConvertAction::Start,
            "Close" => FloppyConvertAction::Close,
            other => return Err(format!("FloppyConvertController has no action {other:?}")),
        };
        Ok(self.ctl.apply(a))
    }

    fn probe(&self, name: &str) -> Result<Value, String> {
        Ok(match name {
            "open" => Value::Boolean(self.ctl.is_open()),
            "source_kind" => Value::String(self.ctl.source_kind().unwrap_or("").to_string()),
            "source_error" => Value::String(self.ctl.source_error().unwrap_or("").to_string()),
            "target" => Value::String(self.ctl.target().extension().to_string()),
            "can_start" => Value::Boolean(self.ctl.can_start()),
            "summary" => Value::String(match self.ctl.outcome() {
                Some(Ok(done)) => done.summary.clone(),
                Some(Err(e)) => format!("error: {e}"),
                None => String::new(),
            }),
            "out_bytes" => Value::Integer(
                std::fs::metadata(self.out())
                    .map(|m| m.len() as i64)
                    .unwrap_or(0),
            ),
            other => return Err(format!("FloppyConvertController has no probe {other:?}")),
        })
    }

    fn settle(&mut self) -> Vec<Effect> {
        self.ctl.wait_idle();
        self.ctl.tick()
    }
}

/// `BulkConvertController`. Actions: `Open`, `PickSource` / `PickOutput` (answered with the
/// scenario's folders: the source holds `a.img` and `b.img`, the output an `a.img` already),
/// `SetFormat` (an output extension, e.g. `img`, `vhd`), `Scan`, `SelectNone`, `SelectAll`,
/// `SetExtension`, `Start`, `Skip` / `Overwrite` / `Back` (the conflict answer), `Close`. Probes:
/// `open`, `phase`, `files`, `selected`, `extension`, `can_start`, `problem`, `conflicts`,
/// `running`, `result`, `log`, `outputs` (the output folder's names, comma-joined).
struct BulkConvertTarget {
    ctl: rusty_backup::controller::bulk_convert::BulkConvertController,
    src: tempfile::TempDir,
    out: tempfile::TempDir,
    log: Vec<String>,
}

impl BulkConvertTarget {
    fn new() -> Self {
        let src = tempfile::tempdir().expect("temp dir");
        let out = tempfile::tempdir().expect("temp dir");
        for n in ["a.img", "b.img"] {
            std::fs::write(src.path().join(n), vec![0u8; 8192]).expect("source file");
        }
        std::fs::write(out.path().join("a.img"), b"keep").expect("output file");
        BulkConvertTarget {
            ctl: rusty_backup::controller::bulk_convert::BulkConvertController::new(
                std::sync::Arc::new(rusty_backup::model::job::NoNotify),
            ),
            src,
            out,
            log: Vec::new(),
        }
    }
}

impl Target for BulkConvertTarget {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        use rusty_backup::controller::bulk_convert::{
            purpose, BulkConvertAction as A, ConflictChoice, FORMATS,
        };
        let a = match action {
            "Open" => A::Open,
            "PickSource" => A::PickSource,
            "PickOutput" => A::PickOutput,
            "AnswerSource" => A::Picked {
                purpose: purpose::SOURCE,
                path: Some(self.src.path().to_path_buf()),
            },
            "AnswerOutput" => A::Picked {
                purpose: purpose::OUTPUT,
                path: Some(self.out.path().to_path_buf()),
            },
            "SetFormat" => {
                let want = arg_str(arg)?;
                A::SetFormat(
                    *FORMATS
                        .iter()
                        .find(|f| f.extension() == want)
                        .ok_or("SetFormat takes an output extension")?,
                )
            }
            "Scan" => A::Scan,
            "SelectNone" => A::SelectNone,
            "SelectAll" => A::SelectAll,
            "SetExtension" => A::SetExtension(arg_str(arg)?.to_string()),
            "Start" => A::Start(None),
            "Skip" => A::ResolveConflicts(ConflictChoice::SkipConflicting),
            "Overwrite" => A::ResolveConflicts(ConflictChoice::Overwrite),
            "Back" => A::ResolveConflicts(ConflictChoice::Back),
            "Close" => A::Close,
            other => return Err(format!("BulkConvertController has no action {other:?}")),
        };
        Ok(self.ctl.apply(a))
    }

    fn probe(&self, name: &str) -> Result<Value, String> {
        use rusty_backup::controller::bulk_convert::BulkPhase;
        Ok(match name {
            "open" => Value::Boolean(self.ctl.is_open()),
            "phase" => Value::String(
                match self.ctl.phase() {
                    BulkPhase::Setup => "setup",
                    BulkPhase::Review => "review",
                }
                .to_string(),
            ),
            "files" => Value::Integer(self.ctl.files().len() as i64),
            "selected" => Value::Integer(self.ctl.selected_count() as i64),
            "extension" => Value::String(self.ctl.extension().to_string()),
            "can_start" => Value::Boolean(self.ctl.can_start()),
            "problem" => Value::String(self.ctl.start_problem().unwrap_or("").to_string()),
            "conflicts" => Value::Integer(self.ctl.conflicts().map_or(0, |c| c.len()) as i64),
            "running" => Value::Boolean(self.ctl.is_running()),
            "result" => Value::String(match self.ctl.last_result() {
                Some(Ok(r)) => format!("{} ok, {} failed", r.succeeded, r.failed),
                Some(Err(e)) => format!("error: {e}"),
                None => String::new(),
            }),
            "log" => Value::String(self.log.join("\n")),
            "outputs" => {
                let mut names: Vec<String> = std::fs::read_dir(self.out.path())
                    .map(|rd| {
                        rd.filter_map(|e| e.ok())
                            .map(|e| {
                                let len = e.metadata().map(|m| m.len()).unwrap_or(0);
                                format!("{}:{len}", e.file_name().to_string_lossy())
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                names.sort();
                Value::String(names.join(","))
            }
            other => return Err(format!("BulkConvertController has no probe {other:?}")),
        })
    }

    fn settle(&mut self) -> Vec<Effect> {
        self.ctl.wait_idle();
        let fx = self.ctl.tick();
        self.log
            .extend(self.ctl.take_log().into_iter().map(|(_, line)| line));
        fx
    }
}

/// `ArchivesController`. Actions: `Open` (arg: `$sit` writes a two-file StuffIt archive, or a
/// path), `Untick` (an entry index), `SetFormat` (`raw`, `binhex`, `macbinary`, `appledouble`),
/// `ExtractSelected`, `ExtractAll`, `Pick` (answers the folder picker with `out/`), `Close`.
/// Probes: `loaded`, `files`, `selected`, `error`, `extracted`, `log`, `outputs`.
struct ArchivesTarget {
    ctl: rusty_backup::controller::archives::ArchivesController,
    dir: tempfile::TempDir,
    asked: Option<&'static str>,
    log: Vec<String>,
}

impl ArchivesTarget {
    fn new() -> Self {
        ArchivesTarget {
            ctl: rusty_backup::controller::archives::ArchivesController::new(std::sync::Arc::new(
                rusty_backup::controller::archives::NoNotify,
            )),
            dir: tempfile::tempdir().expect("temp dir"),
            asked: None,
            log: Vec::new(),
        }
    }
}

impl Target for ArchivesTarget {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        use rusty_backup::controller::archives::{ArchivesAction as A, ForkFormat};
        let a = match action {
            "Open" => A::Open(match arg_str(arg)? {
                "$sit" => {
                    use arc_mac::stuffit::{build_archive, StuffItInput, WriteMethod};
                    let file = |name: &str, data: &[u8]| StuffItInput {
                        name: name.into(),
                        type_code: *b"TEXT",
                        creator_code: *b"ttxt",
                        finder_flags: 0,
                        create_date: 0,
                        mod_date: 0,
                        data_fork: data.to_vec(),
                        resource_fork: b"rsrc".to_vec(),
                    };
                    let bytes = build_archive(
                        &[file("one", b"first"), file("two", b"second")],
                        WriteMethod::Store,
                    )
                    .map_err(|e| e.to_string())?;
                    let p = self.dir.path().join("a.sit");
                    std::fs::write(&p, bytes).map_err(|e| e.to_string())?;
                    p
                }
                other => PathBuf::from(other),
            }),
            "Untick" => A::SetSelected(
                arg.and_then(Value::as_integer)
                    .ok_or("Untick needs an index")? as usize,
                false,
            ),
            "SetFormat" => A::SetFormat(match arg_str(arg)? {
                "raw" => ForkFormat::Raw,
                "binhex" => ForkFormat::BinHex,
                "macbinary" => ForkFormat::MacBinary,
                "appledouble" => ForkFormat::AppleDouble,
                _ => return Err("SetFormat takes raw, binhex, macbinary or appledouble".into()),
            }),
            "ExtractSelected" => A::ExtractSelected,
            "ExtractAll" => A::ExtractAll,
            "Pick" => A::Picked {
                purpose: self.asked.take().ok_or("no folder picker is open")?,
                path: Some(self.dir.path().join("out")),
            },
            "Close" => A::Close,
            other => return Err(format!("ArchivesController has no action {other:?}")),
        };
        let fx = self.ctl.apply(a);
        for e in &fx {
            if let Effect::PickFolder(spec) = e {
                self.asked = Some(spec.purpose);
            }
        }
        Ok(fx)
    }

    fn probe(&self, name: &str) -> Result<Value, String> {
        Ok(match name {
            "loaded" => Value::Boolean(self.ctl.is_loaded()),
            "files" => Value::Integer(self.ctl.file_count() as i64),
            "selected" => Value::Integer(self.ctl.selected_file_count() as i64),
            "error" => Value::String(
                self.ctl
                    .load_error()
                    .map(|e| e.message.clone())
                    .unwrap_or_default(),
            ),
            "extracted" => Value::Integer(match self.ctl.last_extract() {
                Some(Ok((stats, _))) => stats.files as i64,
                _ => -1,
            }),
            "log" => Value::String(self.log.join("\n")),
            "outputs" => {
                let mut names: Vec<String> = std::fs::read_dir(self.dir.path().join("out"))
                    .map(|rd| {
                        rd.filter_map(|e| e.ok())
                            .map(|e| e.file_name().to_string_lossy().into_owned())
                            .collect()
                    })
                    .unwrap_or_default();
                names.sort();
                Value::String(names.join(","))
            }
            other => return Err(format!("ArchivesController has no probe {other:?}")),
        })
    }

    fn settle(&mut self) -> Vec<Effect> {
        self.ctl.wait_idle();
        let fx = self.ctl.tick();
        self.log
            .extend(self.ctl.take_log().into_iter().map(|(_, line)| line));
        fx
    }
}

/// `SettingsController` over an in-memory config with two recent files. Actions: `Open`,
/// `SetUpdateCheck` (bool), `SetRepoUrl`, `ClearRecent`, `Save`, `Close`. Probes: `open`,
/// `update_check`, `repo_url`, `recent_count`, `status`, `cleared`.
struct SettingsTarget(rusty_backup::controller::settings::SettingsController);

impl SettingsTarget {
    fn new() -> Self {
        use rusty_backup::controller::app::{ConfigStore, MemoryConfig};
        let mut store = MemoryConfig::default();
        let mut cfg = store.load();
        cfg.remember_file(rusty_backup::update::RecentMode::Inspect, "/a.img");
        cfg.remember_file(rusty_backup::update::RecentMode::Archives, "/b.sit");
        store.save(&cfg);
        SettingsTarget(
            rusty_backup::controller::settings::SettingsController::with_store(Box::new(store)),
        )
    }
}

impl Target for SettingsTarget {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        use rusty_backup::controller::settings::SettingsAction as A;
        let a = match action {
            "Open" => A::Open,
            "SetUpdateCheck" => A::SetUpdateCheck(
                arg.and_then(Value::as_bool)
                    .ok_or("SetUpdateCheck needs a bool")?,
            ),
            "SetRepoUrl" => A::SetRepoUrl(arg_str(arg)?.to_string()),
            "ClearRecent" => A::ClearRecent,
            "Save" => A::Save,
            "Close" => A::Close,
            other => return Err(format!("SettingsController has no action {other:?}")),
        };
        Ok(self.0.apply(a))
    }

    fn probe(&self, name: &str) -> Result<Value, String> {
        Ok(match name {
            "open" => Value::Boolean(self.0.is_open()),
            "update_check" => Value::Boolean(self.0.update_check()),
            "repo_url" => Value::String(self.0.repo_url().to_string()),
            "recent_count" => Value::Integer(self.0.recent_count() as i64),
            "status" => Value::String(self.0.status().unwrap_or("").to_string()),
            other => return Err(format!("SettingsController has no probe {other:?}")),
        })
    }

    fn settle(&mut self) -> Vec<Effect> {
        self.0.tick()
    }
}

/// `ResizeController`, opened from Inspect's plan. Actions: `Open` (arg: `$sgi` writes a 64 MiB
/// SGI volume header + EFS disk image and inspects it), `Custom` / `Original` (the EFS row's
/// mode), `SetSize` (MiB as a string), `Preview`, `Apply`, `Close`. Probes: `open`, `rows`,
/// `plan_error`, `preview`, `can_apply`, `changed` (a write ended), `log`, `file_mib`.
struct ResizeTarget {
    ctl: rusty_backup::controller::resize::ResizeController,
    dir: tempfile::TempDir,
    efs: usize,
    changed: bool,
    log: Vec<String>,
}

impl ResizeTarget {
    fn new() -> Self {
        ResizeTarget {
            ctl: rusty_backup::controller::resize::ResizeController::new(std::sync::Arc::new(
                rusty_backup::model::job::NoNotify,
            )),
            dir: tempfile::tempdir().expect("temp dir"),
            efs: 0,
            changed: false,
            log: Vec::new(),
        }
    }

    fn note(&mut self) {
        self.changed |= self.ctl.take_disk_changed();
        self.log
            .extend(self.ctl.take_log().into_iter().map(|(_, m)| m));
    }

    fn open_sgi(&mut self) -> Result<rusty_backup::controller::resize::ResizePlan, String> {
        use rusty_backup::controller::inspect::{
            InspectAction, InspectController, InspectServices, InspectSource, JobRegistry, NoNotify,
        };
        use rusty_backup::partition::sgi_hdd_builder::{build_sgi_efs_hdd, SgiHddOptions};
        let (img, _) = build_sgi_efs_hdd(&SgiHddOptions::new(64 << 20, "scenario"))
            .map_err(|e| e.to_string())?;
        let path = self.dir.path().join("irix.img");
        std::fs::write(&path, img).map_err(|e| e.to_string())?;
        let mut inspect = InspectController::new(InspectServices {
            jobs: JobRegistry::new(),
            notify: std::sync::Arc::new(NoNotify),
        });
        inspect.apply(InspectAction::Open(InspectSource::Image {
            path,
            temp: None,
        }));
        while inspect.loading().is_some() {
            std::thread::sleep(std::time::Duration::from_millis(2));
            inspect.tick();
        }
        let plan = inspect.resize_plan().ok_or("no table to resize")?;
        self.efs = plan
            .partitions
            .iter()
            .find(|p| p.partition_type_byte == 0xA1)
            .map(|p| p.index)
            .ok_or("no EFS partition")?;
        Ok(plan)
    }
}

impl Target for ResizeTarget {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        use rusty_backup::controller::resize::{ResizeAction, SizeMode};
        let index = self.efs;
        let a = match action {
            "Open" => {
                if arg_str(arg)? != "$sgi" {
                    return Err("Open takes $sgi".into());
                }
                ResizeAction::Open(Box::new(self.open_sgi()?))
            }
            "Custom" => ResizeAction::SetMode {
                index,
                mode: SizeMode::Custom,
            },
            "Original" => ResizeAction::SetMode {
                index,
                mode: SizeMode::Original,
            },
            "SetSize" => ResizeAction::SetSizeText {
                index,
                text: arg_str(arg)?.to_string(),
            },
            "Preview" => ResizeAction::Preview,
            "Apply" => ResizeAction::Apply,
            "Close" => ResizeAction::Close,
            other => return Err(format!("ResizeController has no action {other:?}")),
        };
        let fx = self.ctl.apply(a);
        self.note();
        Ok(fx)
    }

    fn probe(&self, name: &str) -> Result<Value, String> {
        Ok(match name {
            "open" => Value::Boolean(self.ctl.is_open()),
            "rows" => Value::Integer(self.ctl.rows().len() as i64),
            "plan_error" => Value::String(self.ctl.plan_error().unwrap_or("").to_string()),
            "preview" => Value::String(
                self.ctl
                    .preview()
                    .unwrap_or(&[])
                    .iter()
                    .map(|r| format!("{} {}", r.new_size, r.action))
                    .collect::<Vec<_>>()
                    .join("; "),
            ),
            "can_apply" => Value::Boolean(self.ctl.can_apply()),
            "changed" => Value::Boolean(self.changed),
            "log" => Value::String(self.log.join("\n")),
            "file_mib" => Value::Integer(
                std::fs::metadata(self.dir.path().join("irix.img"))
                    .map(|m| (m.len() >> 20) as i64)
                    .unwrap_or(0),
            ),
            other => return Err(format!("ResizeController has no probe {other:?}")),
        })
    }

    fn settle(&mut self) -> Vec<Effect> {
        self.ctl.wait_idle();
        let fx = self.ctl.tick();
        self.note();
        fx
    }
}

/// `OpticalController` on an image file, hosting the disc browser (`disc_browser`, the browser
/// pane under `PaneProfile::optical`) the way a shell does on `HostRequest::OpenBrowser`.
/// Actions: `OpenImage` (arg `$hybrid`: the committed hybrid ISO 9660 + HFS+ disc),
/// `SetOutputFormat` (`iso`, `bincue`, `chd`), `PickOutput`, `Pick` (the output, `out.<ext>`),
/// `Start`, `BrowseContents`, `SelectFs` (a label substring), `Expand` / `Select` (a path),
/// `Extract`, `PickExtract`, `Tree`, `CloseDisc`. Probes: `format`, `can_start`, `result`,
/// `bin_bytes`, `browsing`, `choices`, `error`, `content`, `extract_result`, `extracted`, `tree`,
/// `source`.
#[cfg(all(feature = "optical", feature = "native-zstd"))]
struct OpticalTarget {
    ctl: rusty_backup::controller::optical::OpticalController,
    browser: Option<rusty_backup::controller::commander::CommanderController>,
    tree: String,
    dir: tempfile::TempDir,
}

#[cfg(all(feature = "optical", feature = "native-zstd"))]
impl OpticalTarget {
    fn new() -> Self {
        OpticalTarget {
            ctl: rusty_backup::controller::optical::OpticalController::new(std::sync::Arc::new(
                rusty_backup::controller::optical::NoNotify,
            )),
            browser: None,
            tree: String::new(),
            dir: tempfile::tempdir().expect("temp dir"),
        }
    }

    fn out(&self) -> PathBuf {
        self.dir
            .path()
            .join(format!("out.{}", self.ctl.output_format().extension()))
    }

    fn extract_dir(&self) -> PathBuf {
        self.dir.path().join("extract")
    }

    /// Open or close the disc browser as the controller asks, as a shell does.
    fn host(&mut self) {
        use rusty_backup::controller::optical::HostRequest;
        for req in self.ctl.take_requests() {
            match req {
                HostRequest::RememberRecent(_) => {}
                HostRequest::OpenBrowser(src) => {
                    let mut b = rusty_backup::controller::optical::disc_browser(
                        rusty_backup::controller::browser::PaneServices::headless(),
                        rusty_backup::controller::keys::KeyMap::for_this_platform(),
                    );
                    b.open_disc(src);
                    self.browser = Some(b);
                }
                HostRequest::CloseBrowser => self.browser = None,
            }
        }
    }

    fn browser(
        &mut self,
    ) -> Result<&mut rusty_backup::controller::commander::CommanderController, String> {
        self.browser
            .as_mut()
            .ok_or_else(|| "the disc browser is not open".to_string())
    }

    fn pane(&self) -> Option<&rusty_backup::controller::browser::BrowserPaneController> {
        let b = self.browser.as_ref()?;
        Some(b.pane(rusty_backup::controller::commander::Side::Left))
    }

    /// The node id of the browser row showing `path`.
    fn node(&self, path: &str) -> Result<String, String> {
        let p = self.pane().ok_or("the disc browser is not open")?;
        let row = p
            .rows()
            .iter()
            .find(|r| p.row_entry(r).is_some_and(|e| e.path == path));
        row.map(|r| r.node_id.clone())
            .ok_or_else(|| format!("no row {path}"))
    }

    fn pane_act(
        &mut self,
        a: rusty_backup::controller::browser::BrowserPaneAction,
    ) -> Result<Vec<Effect>, String> {
        use rusty_backup::controller::commander::{CommanderAction, Side};
        Ok(self.browser()?.apply(CommanderAction::Pane(Side::Left, a)))
    }
}

#[cfg(all(feature = "optical", feature = "native-zstd"))]
impl Target for OpticalTarget {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        use rusty_backup::controller::browser::BrowserPaneAction as P;
        use rusty_backup::controller::commander::screen::purpose as spurpose;
        use rusty_backup::controller::commander::{CommanderAction, ExtractOptions, Side};
        use rusty_backup::controller::optical::{purpose, OpticalAction, OutputFormat};
        let a = match action {
            "OpenImage" => OpticalAction::OpenImage(match arg_str(arg)? {
                "$hybrid" => {
                    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
                    let packed =
                        std::fs::read(root.join("tests/fixtures/optical/hybrid_rsrc.iso.zst"))
                            .map_err(|e| e.to_string())?;
                    let iso = zstd::stream::decode_all(std::io::Cursor::new(packed))
                        .map_err(|e| e.to_string())?;
                    let p = self.dir.path().join("disc.iso");
                    std::fs::write(&p, iso).map_err(|e| e.to_string())?;
                    p
                }
                other => PathBuf::from(other),
            }),
            "SetOutputFormat" => OpticalAction::SetOutputFormat(match arg_str(arg)? {
                "iso" => OutputFormat::Iso,
                "bincue" => OutputFormat::BinCue,
                "chd" => OutputFormat::Chd,
                other => return Err(format!("no output format {other:?}")),
            }),
            "PickOutput" => OpticalAction::PickOutput,
            "Pick" => OpticalAction::Picked {
                purpose: purpose::OUTPUT,
                path: Some(self.out()),
            },
            "Start" => OpticalAction::Start(None),
            "BrowseContents" => OpticalAction::BrowseContents,
            "SelectFs" => {
                let want = arg_str(arg)?;
                let labels = self
                    .pane()
                    .map(|p| p.partition_labels())
                    .unwrap_or_default();
                let idx = labels
                    .iter()
                    .position(|l| l.contains(want))
                    .ok_or_else(|| format!("no filesystem choice like {want:?}"))?;
                return self.pane_act(P::SelectPartition(idx));
            }
            "Expand" => {
                let id = self.node(arg_str(arg)?)?;
                return self.pane_act(P::ToggleExpand(id));
            }
            "Select" => {
                let id = self.node(arg_str(arg)?)?;
                return self.pane_act(P::CursorTo(id));
            }
            "Extract" => {
                let p = self.pane().ok_or("the disc browser is not open")?;
                let path = p
                    .cursor_index()
                    .and_then(|i| p.row_entry(&p.rows()[i]))
                    .map(|e| e.path)
                    .ok_or("no cursor row")?;
                return Ok(self.browser()?.apply(CommanderAction::Extract {
                    from: Side::Left,
                    path,
                    options: ExtractOptions::default(),
                    dest: None,
                }));
            }
            "PickExtract" => {
                let dir = self.extract_dir();
                std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
                return Ok(self.browser()?.apply(CommanderAction::Picked {
                    purpose: spurpose::EXTRACT,
                    path: Some(dir),
                }));
            }
            "Tree" => return self.pane_act(P::ListTree { ids: false }),
            "Block" => {
                let blocker = self.extract_dir().join(arg_str(arg)?);
                std::fs::create_dir_all(blocker).map_err(|e| e.to_string())?;
                return Ok(Vec::new());
            }
            "Overwrite" => {
                use rusty_backup::controller::commander::OnConflict;
                return Ok(self
                    .browser()?
                    .apply(CommanderAction::HostConflict(OnConflict::Replace)));
            }
            "Chose" => {
                let choice = arg.and_then(Value::as_integer).ok_or("an index `arg`")? as usize;
                return Ok(self.browser()?.apply(CommanderAction::Chose {
                    purpose: spurpose::ENTRY_FAILED,
                    choice,
                }));
            }
            "SkippedReportOk" => {
                return Ok(self.browser()?.apply(CommanderAction::SkippedReportOk));
            }
            "CloseDisc" => OpticalAction::CloseDisc,
            other => return Err(format!("OpticalController has no action {other:?}")),
        };
        let fx = self.ctl.apply(a);
        self.host();
        Ok(fx)
    }

    fn probe(&self, name: &str) -> Result<Value, String> {
        use rusty_backup::controller::commander::Side;
        let detail = self
            .browser
            .as_ref()
            .and_then(|b| b.side_detail(Side::Left));
        Ok(match name {
            "format" => Value::String(self.ctl.disc().map(|d| d.format).unwrap_or_default()),
            "source" => Value::String(
                self.ctl
                    .image()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default(),
            ),
            "can_start" => Value::Boolean(self.ctl.can_start()),
            "result" => Value::String(match self.ctl.last_result() {
                Some(Ok(())) => "ok".to_string(),
                Some(Err(e)) => format!("error: {e}"),
                None => String::new(),
            }),
            "bin_bytes" => Value::Integer(
                std::fs::metadata(self.out().with_extension("bin"))
                    .map(|m| m.len() as i64)
                    .unwrap_or(0),
            ),
            "browsing" => Value::Boolean(self.ctl.is_browsing() && self.browser.is_some()),
            "choices" => Value::Integer(
                self.pane()
                    .map(|p| p.partition_labels().len() as i64)
                    .unwrap_or(0),
            ),
            "error" => Value::String(
                self.pane()
                    .and_then(|p| p.source_error())
                    .unwrap_or("")
                    .to_string(),
            ),
            "content" => Value::String(
                detail
                    .and_then(|d| d.preview.as_ref())
                    .map(|b| String::from_utf8_lossy(b.as_slice()).into_owned())
                    .unwrap_or_default(),
            ),
            "extract_result" => Value::String(
                self.browser
                    .as_ref()
                    .map(|b| b.status().to_string())
                    .unwrap_or_default(),
            ),
            "extracted" => Value::String({
                let mut names: Vec<String> = std::fs::read_dir(self.extract_dir())
                    .map(|rd| {
                        rd.filter_map(|e| e.ok())
                            .map(|e| e.file_name().to_string_lossy().into_owned())
                            .collect()
                    })
                    .unwrap_or_default();
                names.sort();
                names.join(",")
            }),
            "tree" => Value::String(self.tree.clone()),
            "skipped" => Value::Integer(
                self.browser
                    .as_ref()
                    .and_then(|b| b.skipped_report())
                    .map(|r| r.skipped.len() as i64)
                    .unwrap_or(0),
            ),
            "skipped_report" => Value::Boolean(
                self.browser
                    .as_ref()
                    .is_some_and(|b| b.skipped_report().is_some()),
            ),
            other => return Err(format!("OpticalController has no probe {other:?}")),
        })
    }

    fn settle(&mut self) -> Vec<Effect> {
        self.ctl.wait_idle();
        let mut fx = self.ctl.tick();
        self.host();
        if let Some(b) = self.browser.as_mut() {
            let start = std::time::Instant::now();
            fx.extend(b.tick());
            // A job waiting on a question settles there: the step answers it.
            let asking = |fx: &[Effect]| fx.iter().any(|e| matches!(e, Effect::Choose(_)));
            while b.is_working() && !asking(&fx) && start.elapsed().as_secs() < 60 {
                std::thread::sleep(std::time::Duration::from_millis(5));
                fx.extend(b.tick());
            }
            if let Some(t) = b.host_pane().take_tree_text() {
                self.tree = t.text;
            }
        }
        fx
    }
}

fn target_for(vm: &str) -> Result<Box<dyn Target>, String> {
    match vm {
        "app" => Ok(Box::new(AppController::headless())),
        "backup" => Ok(Box::new(BackupTarget::new())),
        "inspect" => Ok(Box::new(InspectTarget::new())),
        "restore" => Ok(Box::new(RestoreTarget::new())),
        "disk_write" => Ok(Box::new(DiskWriteTarget::new())),
        "alto_resize" => Ok(Box::new(AltoResizeTarget::new())),
        "expand_hfs" => Ok(Box::new(ExpandHfsTarget::new())),
        "floppy_convert" => Ok(Box::new(FloppyConvertTarget::new())),
        "bulk_convert" => Ok(Box::new(BulkConvertTarget::new())),
        "archives" => Ok(Box::new(ArchivesTarget::new())),
        "settings" => Ok(Box::new(SettingsTarget::new())),
        "resize" => Ok(Box::new(ResizeTarget::new())),
        #[cfg(all(feature = "optical", feature = "native-zstd"))]
        "optical" => Ok(Box::new(OpticalTarget::new())),
        "job_probe" => Ok(Box::new(JobProbeController {
            app: AppController::headless(),
            job: None,
        })),
        other => Err(format!("unknown vm {other:?}")),
    }
}

fn run(path: &Path) -> Result<(), String> {
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let sc: Scenario = toml::from_str(&text).map_err(|e| e.to_string())?;
    if !sc.requires.iter().all(|f| has_feature(f.as_str())) {
        return Ok(());
    }
    let mut target = target_for(&sc.vm)?;
    for (i, step) in sc.step.iter().enumerate() {
        let at = |msg: String| format!("{} step {}: {msg}", sc.name, i + 1);
        let mut effects = Vec::new();
        if let Some(action) = &step.action {
            effects = target.act(action, step.arg.as_ref()).map_err(at)?;
        }
        if step.settle {
            effects.extend(target.settle());
        }
        if let Some(want) = &step.effects {
            let got: Vec<&str> = effects.iter().map(effect_kind).collect();
            if got != *want {
                return Err(at(format!("effects {got:?}, expected {want:?}")));
            }
        }
        for (name, want) in step.expect.iter().flatten() {
            let got = target.probe(name).map_err(at)?;
            let ok = match (&got, want) {
                (Value::String(g), Value::String(w)) if w.starts_with("~") => g.contains(&w[1..]),
                _ => &got == want,
            };
            if !ok {
                return Err(at(format!("{name} = {got}, expected {want}")));
            }
        }
    }
    Ok(())
}

/// `tests/ui_scenarios`, from the root manifest or from `rb-cli-vintage/`.
fn scenario_dir() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let here = root.join("tests/ui_scenarios");
    if here.is_dir() {
        here
    } else {
        root.join("../tests/ui_scenarios")
    }
}

#[test]
fn every_ui_scenario_passes() {
    let dir = scenario_dir();
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "toml"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no scenarios in {}", dir.display());
    let failures: Vec<String> = files
        .iter()
        .filter_map(|p| run(p).err().map(|e| format!("{}: {e}", p.display())))
        .collect();
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}
