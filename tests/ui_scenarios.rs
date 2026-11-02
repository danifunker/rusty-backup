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
    #[serde(default)]
    step: Vec<Step>,
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

fn target_for(vm: &str) -> Result<Box<dyn Target>, String> {
    match vm {
        "app" => Ok(Box::new(AppController::headless())),
        "backup" => Ok(Box::new(BackupTarget::new())),
        "inspect" => Ok(Box::new(InspectTarget::new())),
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
