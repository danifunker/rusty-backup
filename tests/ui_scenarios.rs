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

use rusty_backup::controller::app::{AppAction, AppController};
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

/// `AppController`. Actions: `CopyLog`, `ClearLog`, `SetLogAutoScroll` (bool arg), and the
/// test-only `LogInfo` / `LogWarn` / `LogError` (string arg). Probes: `log_lines`,
/// `log_text`, `log_auto_scroll`, `jobs_running`.
impl Target for AppController {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        let a = match action {
            "CopyLog" => AppAction::CopyLog,
            "ClearLog" => AppAction::ClearLog,
            "SetLogAutoScroll" => AppAction::SetLogAutoScroll(
                arg.and_then(Value::as_bool)
                    .ok_or("SetLogAutoScroll needs a bool `arg`")?,
            ),
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
/// (bool, answers the pending question). Probes: `source`, `partitions`, `superfloppy`, `error`,
/// `check_clean`, `log` (every line so far).
struct InspectTarget {
    ctl: rusty_backup::controller::inspect::InspectController,
    dir: tempfile::TempDir,
    asked: Option<&'static str>,
    log: Vec<String>,
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
        fx
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
            "check_clean" => {
                Value::Boolean(self.ctl.fsck_view().is_some_and(|v| v.result.is_clean()))
            }
            "log" => Value::String(self.log.join("\n")),
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
/// (`zstd`, `vhd`, `chd`, `raw`), `SelectAll`, `SelectNone`, `Start`, `VhdClose`, `Close`.
/// Probes: `source`, `partitions`, `selected`, `error` (why Start is refused, empty when it is
/// not), `vhd_open`, `result` (`ok`, the failure, or empty), `output` (the backup folder holds a
/// `metadata.json`), `log` (every line so far).
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
            "Start" => BackupAction::Start(None),
            "VhdClose" => BackupAction::VhdClose,
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
/// (`row:size`), `BuildAddRow`, `BuildApply`.
/// Probes: `backup` (one is loaded), `partitions`, `error` (why Start is refused, empty when it is
/// not), `load_error`, `result` (`ok`, the failure, or empty), `output` (the restored image equals
/// the image backed up), `log` (every line so far), `build_rows` (-1 with no plan open),
/// `build_layout` (`ok` or why it cannot be placed), `build_result`, `built_mbr`.
struct RestoreTarget {
    ctl: rusty_backup::controller::restore::RestoreController,
    dir: tempfile::TempDir,
    log: Vec<String>,
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
        }
    }

    fn note(&mut self, fx: Vec<Effect>) -> Vec<Effect> {
        self.log
            .extend(self.ctl.take_log().into_iter().map(|(_, m)| m));
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

/// `OpticalController` on an image file. Actions: `OpenImage` (arg `$hybrid`: the committed
/// hybrid ISO 9660 + HFS+ disc), `SetOutputFormat` (`iso`, `bincue`, `chd`), `PickOutput`, `Pick`
/// (the output, `out.<ext>`), `Start`, `BrowseContents`, `SelectFs` (a label substring),
/// `Expand` / `Select` (a `/`-separated path from the root), `Extract`, `PickExtract`, `Tree`,
/// `CloseDisc`. Probes: `format`, `can_start`, `result`, `bin_bytes`, `browsing`, `choices`,
/// `error`, `content`, `extract_result`, `extracted`, `tree`, `source`.
#[cfg(all(feature = "optical", feature = "native-zstd"))]
struct OpticalTarget {
    ctl: rusty_backup::controller::optical::OpticalController,
    dir: tempfile::TempDir,
}

#[cfg(all(feature = "optical", feature = "native-zstd"))]
impl OpticalTarget {
    fn new() -> Self {
        OpticalTarget {
            ctl: rusty_backup::controller::optical::OpticalController::new(std::sync::Arc::new(
                rusty_backup::controller::optical::NoNotify,
            )),
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

    /// The entry at `path` (`/`-separated names from the root), from the folders listed so far.
    fn entry(
        &self,
        path: &str,
    ) -> Result<rusty_backup::controller::optical::browse::FileEntry, String> {
        let b = self.ctl.browse();
        let mut cur = b.root().ok_or("no root")?.clone();
        for name in path.split('/').filter(|n| !n.is_empty()) {
            let kids = b
                .children(cur.path.as_str())
                .ok_or_else(|| format!("{} is not listed", cur.path))?;
            cur = kids
                .iter()
                .find(|e| e.name == name)
                .ok_or_else(|| format!("no {name} in {}", cur.path))?
                .clone();
        }
        Ok(cur)
    }
}

#[cfg(all(feature = "optical", feature = "native-zstd"))]
impl Target for OpticalTarget {
    fn act(&mut self, action: &str, arg: Option<&Value>) -> Result<Vec<Effect>, String> {
        use rusty_backup::controller::optical::browse::purpose as bpurpose;
        use rusty_backup::controller::optical::{
            purpose, DiscBrowseAction, OpticalAction, OutputFormat,
        };
        let browse = OpticalAction::Browse;
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
                let idx = self
                    .ctl
                    .browse()
                    .fs_choices()
                    .iter()
                    .position(|l| l.contains(want))
                    .ok_or_else(|| format!("no filesystem choice like {want:?}"))?;
                browse(DiscBrowseAction::SelectFs(idx))
            }
            "Expand" => browse(DiscBrowseAction::Expand(self.entry(arg_str(arg)?)?)),
            "Select" => browse(DiscBrowseAction::Select(self.entry(arg_str(arg)?)?)),
            "Extract" => browse(DiscBrowseAction::Extract),
            "PickExtract" => {
                let dir = self.extract_dir();
                std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
                OpticalAction::Picked {
                    purpose: bpurpose::EXTRACT_TO,
                    path: Some(dir),
                }
            }
            "Tree" => browse(DiscBrowseAction::Tree),
            "CloseDisc" => OpticalAction::CloseDisc,
            other => return Err(format!("OpticalController has no action {other:?}")),
        };
        Ok(self.ctl.apply(a))
    }

    fn probe(&self, name: &str) -> Result<Value, String> {
        use rusty_backup::controller::optical::browse::FileContent;
        let b = self.ctl.browse();
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
            "browsing" => Value::Boolean(b.is_active()),
            "choices" => Value::Integer(b.fs_choices().len() as i64),
            "error" => Value::String(b.error().unwrap_or("").to_string()),
            "content" => Value::String(match b.content() {
                Some(FileContent::Text(t)) => t.clone(),
                Some(FileContent::Binary(d)) => format!("{} bytes", d.len()),
                None => String::new(),
            }),
            "extract_result" => Value::String(b.result().unwrap_or("").to_string()),
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
            "tree" => Value::String(b.tree_text().unwrap_or("").to_string()),
            other => return Err(format!("OpticalController has no probe {other:?}")),
        })
    }

    fn settle(&mut self) -> Vec<Effect> {
        self.ctl.wait_idle();
        self.ctl.tick()
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
