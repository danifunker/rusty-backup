//! E2E flows: `ui` steps that drive a front-end, run by a per-shell driver (FLOWS.md).

use std::path::Path;
use std::time::Duration;

/// Backspaces `fill` sends to clear a field: more than any path a flow types.
#[cfg(unix)]
const FILL_CLEAR: usize = 512;

/// Terminal size every TUI flow runs at, so a flow's screen text is the same on every host.
pub const TUI_ROWS: u16 = 40;
pub const TUI_COLS: u16 = 120;

/// Start `rb-cli tui` with `home` as its HOME, holding a config that turns the update check off.
#[cfg(unix)]
pub fn start_tui(rb_cli: &Path, home: &Path, cwd: &Path) -> Result<crate::tui::Session, String> {
    let config = r#"{"update_check":{"enabled":false,"repository_url":""}}"#;
    for dir in [".config/rusty-backup", "Library/Application Support/rusty-backup"] {
        let d = home.join(dir);
        std::fs::create_dir_all(&d).map_err(|e| format!("{}: {}", d.display(), e))?;
        std::fs::write(d.join("config.json"), config).map_err(|e| e.to_string())?;
    }
    let env = vec![
        ("HOME".to_string(), home.display().to_string()),
        ("RB_TUI_ASCII".to_string(), "1".to_string()),
        ("XDG_CONFIG_HOME".to_string(), home.join(".config").display().to_string()),
    ];
    let s = crate::tui::Session::spawn(
        rb_cli,
        &["tui".to_string()],
        cwd,
        &env,
        TUI_ROWS,
        TUI_COLS,
    )
    .map_err(|e| format!("could not start {} tui: {}", rb_cli.display(), e))?;
    s.settle(Duration::from_millis(400), Duration::from_secs(10));
    Ok(s)
}

#[cfg(not(unix))]
pub fn cmd_tui_shot(_rb_cli: &Path, _scratch: &Path, _ops: &[String]) -> i32 {
    eprintln!("tui-shot needs a pseudo-terminal; Windows (ConPTY) is not supported yet");
    2
}

/// `rb-regress tui-shot OP...`: each OP is a key name, `type:TEXT` or `wait:TEXT`.
#[cfg(unix)]
pub fn cmd_tui_shot(rb_cli: &Path, scratch: &Path, ops: &[String]) -> i32 {
    let dir = scratch.join("_tui-shot");
    let _ = std::fs::remove_dir_all(&dir);
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("error: {}: {}", dir.display(), e);
        return 1;
    }
    let mut s = match start_tui(rb_cli, &dir.join("home"), &dir) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: {}", e);
            return 1;
        }
    };
    print_screen("start", &s);
    for op in ops {
        let ok = if let Some(t) = op.strip_prefix("type:") {
            s.send(t.as_bytes()).is_ok()
        } else if let Some(t) = op.strip_prefix("fill:") {
            let mut b = vec![0x7f_u8; FILL_CLEAR];
            b.extend_from_slice(t.as_bytes());
            s.send(&b).is_ok()
        } else if let Some(t) = op.strip_prefix("wait:") {
            s.wait_for(Duration::from_secs(60), |sc| sc.text().contains(t))
        } else {
            match crate::tui::key_bytes(op) {
                Some(b) => s.send(&b).is_ok(),
                None => {
                    eprintln!("error: unknown key '{}'", op);
                    return 2;
                }
            }
        };
        s.settle(Duration::from_millis(300), Duration::from_secs(10));
        print_screen(op, &s);
        if !ok {
            eprintln!("op '{}' did not complete", op);
            return 1;
        }
        if s.exited() {
            eprintln!("the TUI exited");
            return 1;
        }
    }
    0
}

#[cfg(unix)]
fn print_screen(label: &str, s: &crate::tui::Session) {
    println!("==== after {} ====", label);
    s.with_screen(|sc| {
        for r in 0..sc.rows() {
            let rev = sc.reverse_text(r);
            if rev.is_empty() {
                println!("{:2}|{}", r, sc.line(r));
            } else {
                println!("{:2}|{}    [rev: {}]", r, sc.line(r), rev);
            }
        }
    });
}

use crate::assertion::FailedAssertion;
use crate::manifest::UiOp;
use crate::report::Verdict;
use std::path::PathBuf;

pub enum UiError {
    /// The case cannot run here (no driver for the shell, no pty on this OS).
    Skip(Verdict, String),
    /// An op failed: what, and the screen at that moment.
    Fail(FailedAssertion, String),
}

/// One-line form of an op, for the failure bundle.
pub fn describe(op: &UiOp) -> String {
    let pairs = [
        ("tab", &op.tab),
        ("focus", &op.focus),
        ("choose", &op.choose),
        ("key", &op.key),
        ("type", &op.text),
        ("fill", &op.fill),
        ("activate", &op.activate),
        ("pick", &op.pick),
        ("wait", &op.wait),
        ("expect", &op.expect),
        ("expect_not", &op.expect_not),
    ];
    for (k, v) in pairs {
        if let Some(v) = v {
            return format!("{} = {:?}", k, v);
        }
    }
    match &op.keys {
        Some(ks) => format!("keys = {:?}", ks),
        None => "(empty)".to_string(),
    }
}

/// What a controller id is called in one shell (drivers/labels.toml).
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Control {
    pub label: Option<String>,
    pub key: Option<String>,
    pub pick: Option<String>,
    pub via: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ControlEntry {
    Label(String),
    Full(Control),
}

/// shell -> controller id -> control.
pub type Controls = std::collections::BTreeMap<String, std::collections::BTreeMap<String, Control>>;

pub fn load_controls(path: &Path) -> Result<Controls, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {}", path.display(), e))?;
    let raw: std::collections::BTreeMap<String, std::collections::BTreeMap<String, ControlEntry>> =
        toml::from_str(&text).map_err(|e| format!("{}: {}", path.display(), e))?;
    Ok(raw
        .into_iter()
        .map(|(shell, m)| {
            let m = m
                .into_iter()
                .map(|(id, e)| {
                    let c = match e {
                        ControlEntry::Label(l) => Control {
                            label: Some(l),
                            ..Control::default()
                        },
                        ControlEntry::Full(c) => c,
                    };
                    (id, c)
                })
                .collect();
            (shell, m)
        })
        .collect())
}

/// Resolve `name` for `shell`: a listed controller id, or a literal label.
fn control(controls: &Controls, shell: &str, name: &str) -> Control {
    controls
        .get(shell)
        .and_then(|m| m.get(name))
        .cloned()
        .unwrap_or_else(|| Control {
            label: Some(name.to_string()),
            ..Control::default()
        })
}

/// The shell a case drives, started at its first `ui` step and kept for the rest.
pub struct UiSession {
    shell: Option<String>,
    rb_cli: PathBuf,
    gtk: PathBuf,
    scratch: PathBuf,
    controls: Controls,
    /// The control the last `focus` landed on, which `fill` and `choose` act on.
    focused: Control,
    /// A `pick` waiting for the dialog it answers (the TUI's path prompt).
    pending_pick: Option<String>,
    #[cfg(unix)]
    tui: Option<crate::tui::Session>,
    gtk_driver: Option<crate::gtk::Driver>,
}

impl UiSession {
    pub fn new(
        shell: Option<String>,
        rb_cli: &Path,
        gtk: &Path,
        scratch: &Path,
        controls: Controls,
    ) -> Self {
        UiSession {
            shell,
            rb_cli: rb_cli.to_path_buf(),
            gtk: gtk.to_path_buf(),
            scratch: scratch.to_path_buf(),
            controls,
            focused: Control::default(),
            pending_pick: None,
            #[cfg(unix)]
            tui: None,
            gtk_driver: None,
        }
    }

    /// Everything the shell wrote so far, for the failure bundle.
    pub fn transcript(&self) -> String {
        #[cfg(unix)]
        if let Some(t) = &self.tui {
            if let Ok(b) = t.transcript.lock() {
                return String::from_utf8_lossy(&b).into_owned();
            }
        }
        if let Some(g) = &self.gtk_driver {
            return g.log();
        }
        String::new()
    }

    pub fn run(&mut self, ops: &[UiOp], resolve: &dyn Fn(&str) -> String) -> Result<(), UiError> {
        let shell = self.shell.clone().unwrap_or_default();
        match shell.as_str() {
            "tui" => self.run_tui(ops, resolve),
            "gtk" => self.run_gtk(ops, resolve),
            other => Err(UiError::Skip(
                Verdict::SkipTool,
                format!("no driver for the '{}' shell yet", other),
            )),
        }
    }

    fn run_gtk(&mut self, ops: &[UiOp], resolve: &dyn Fn(&str) -> String) -> Result<(), UiError> {
        if self.gtk_driver.is_none() {
            let d = crate::gtk::Driver::start(&self.gtk, &self.scratch.join("gtk"))?;
            self.gtk_driver = Some(d);
        }
        for op in ops {
            if op.only.as_deref().is_some_and(|o| o != "gtk") {
                continue;
            }
            let msg = self.gtk_message(op, resolve);
            let limit = Duration::from_millis(op.timeout_ms.unwrap_or(30_000));
            let d = self.gtk_driver.as_mut().expect("started above");
            if let Err((exp, obs)) = d.send(&msg, limit) {
                return Err(UiError::Fail(
                    FailedAssertion {
                        op: format!("ui {}", describe(op)),
                        selector: None,
                        expected: exp,
                        observed: obs.clone(),
                    },
                    obs,
                ));
            }
        }
        Ok(())
    }

    /// The JSON line the GTK driver gets for `op`, with controller ids resolved to labels.
    fn gtk_message(&mut self, op: &UiOp, resolve: &dyn Fn(&str) -> String) -> serde_json::Value {
        let ctl = |s: &Self, n: &str| control(&s.controls, "gtk", &resolve(n));
        let label = |c: &Control, n: &str| c.label.clone().unwrap_or_else(|| resolve(n));
        let mut m = serde_json::Map::new();
        let mut put = |k: &str, v: String| {
            m.insert(k.to_string(), serde_json::Value::String(v));
        };
        if let Some(t) = &op.tab {
            put("op", "tab".into());
            put("arg", resolve(t));
        } else if let Some(f) = &op.focus {
            let c = ctl(self, f);
            put("op", "focus".into());
            put("arg", label(&c, f));
            self.focused = c;
        } else if let Some(v) = &op.choose {
            put("op", "choose".into());
            put("arg", resolve(v));
        } else if let Some(a) = &op.activate {
            let c = ctl(self, a);
            put("op", "activate".into());
            put("arg", label(&c, a));
            if let Some(v) = &c.via {
                put("via", v.clone());
            }
        } else if let Some(p) = &op.pick {
            put("op", "pick".into());
            put("arg", resolve(p));
        } else if let Some(t) = &op.fill {
            put("op", "fill".into());
            put("arg", resolve(t));
            if let Some(p) = &self.focused.pick {
                put("pick", p.clone());
            }
        } else if let Some(k) = &op.key {
            put("op", "key".into());
            put("arg", resolve(k));
        } else if let Some(ks) = &op.keys {
            put("op", "keys".into());
            put("arg", ks.join(" "));
        } else if let Some(t) = &op.text {
            put("op", "type".into());
            put("arg", resolve(t));
        } else if let Some(w) = &op.wait {
            put("op", "wait".into());
            put("arg", label(&ctl(self, w), w));
        } else if let Some(e) = &op.expect {
            put("op", "expect".into());
            put("arg", label(&ctl(self, e), e));
        } else if let Some(e) = &op.expect_not {
            put("op", "expect_not".into());
            put("arg", label(&ctl(self, e), e));
        }
        m.insert(
            "timeout_ms".to_string(),
            serde_json::Value::from(op.timeout_ms.unwrap_or(30_000)),
        );
        serde_json::Value::Object(m)
    }

    #[cfg(not(unix))]
    fn run_tui(&mut self, _ops: &[UiOp], _r: &dyn Fn(&str) -> String) -> Result<(), UiError> {
        Err(UiError::Skip(
            Verdict::SkipPlatform,
            "the TUI driver needs a pseudo-terminal; ConPTY is not supported yet".to_string(),
        ))
    }

    #[cfg(unix)]
    fn run_tui(&mut self, ops: &[UiOp], resolve: &dyn Fn(&str) -> String) -> Result<(), UiError> {
        if self.tui.is_none() {
            let s = start_tui(&self.rb_cli, &self.scratch.join("home"), &self.scratch)
                .map_err(|e| UiError::Skip(Verdict::Error, e))?;
            self.tui = Some(s);
        }
        for op in ops {
            if op.only.as_deref().is_some_and(|o| o != "tui") {
                continue;
            }
            let limit = Duration::from_millis(op.timeout_ms.unwrap_or(30_000));
            let fail = |s: &crate::tui::Session, exp: String, obs: String| {
                UiError::Fail(
                    FailedAssertion {
                        op: format!("ui {}", describe(op)),
                        selector: None,
                        expected: exp,
                        observed: obs,
                    },
                    s.text(),
                )
            };
            let r = self.tui_op(op, resolve, limit);
            let s = self.tui.as_mut().expect("started above");
            if let Err((exp, obs)) = r {
                return Err(fail(s, exp, obs));
            }
            if s.exited() {
                return Err(fail(s, "the TUI still running".into(), "it exited".into()));
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    fn tui_op(
        &mut self,
        op: &UiOp,
        resolve: &dyn Fn(&str) -> String,
        limit: Duration,
    ) -> Result<(), (String, String)> {
        let ctl = control(&self.controls, "tui", &resolve(op_target(op)));
        let s = self.tui.as_mut().expect("started");
        let screen = |s: &crate::tui::Session| s.text();
        let label = ctl.label.clone().unwrap_or_default();
        if let Some(t) = &op.tab {
            tui_tab(s, &resolve(t))?;
        } else if op.focus.is_some() {
            if !seek(s, ("Down", "Up"), 60, &|s| focus_has(s, &label))? {
                return Err((format!("focus on '{}'", label), screen(s)));
            }
            self.focused = ctl;
        } else if let Some(c) = &op.choose {
            let c = resolve(c);
            if !seek(s, ("Right", "Left"), 40, &|s| focus_has(s, &c))? {
                return Err((format!("focused value '{}'", c), screen(s)));
            }
        } else if op.activate.is_some() {
            match &ctl.key {
                Some(k) => press(s, k)?,
                None => {
                    if !seek(s, ("Down", "Up"), 60, &|s| focus_has(s, &label))? {
                        return Err((format!("focus on '{}'", label), screen(s)));
                    }
                    press(s, "Enter")?;
                }
            }
            answer_prompt(s, &mut self.pending_pick)?;
        } else if let Some(p) = &op.pick {
            self.pending_pick = Some(resolve(p));
        } else if let Some(k) = &op.key {
            press(s, k)?;
            answer_prompt(s, &mut self.pending_pick)?;
        } else if let Some(ks) = &op.keys {
            for k in ks {
                press(s, k)?;
            }
            answer_prompt(s, &mut self.pending_pick)?;
        } else if let Some(t) = &op.text {
            s.send(resolve(t).as_bytes())
                .map_err(|e| ("text typed".to_string(), e.to_string()))?;
            s.settle(Duration::from_millis(120), Duration::from_secs(5));
        } else if let Some(t) = &op.fill {
            fill_field(s, &resolve(t))?;
        } else if op.wait.is_some() {
            let w = label.clone();
            if !s.wait_for(limit, |sc| sc.text().contains(w.as_str())) {
                return Err((format!("screen shows '{}'", w), screen(s)));
            }
        } else if op.expect.is_some() {
            let e = label.clone();
            s.settle(Duration::from_millis(200), Duration::from_secs(5));
            if !s.text().contains(e.as_str()) {
                return Err((format!("screen shows '{}'", e), screen(s)));
            }
        } else if op.expect_not.is_some() {
            let e = label.clone();
            s.settle(Duration::from_millis(200), Duration::from_secs(5));
            if s.text().contains(e.as_str()) {
                return Err((format!("screen without '{}'", e), screen(s)));
            }
        }
        Ok(())
    }
}

/// The control an op names, if it names one.
fn op_target(op: &UiOp) -> &str {
    op.focus
        .as_deref()
        .or(op.activate.as_deref())
        .or(op.wait.as_deref())
        .or(op.expect.as_deref())
        .or(op.expect_not.as_deref())
        .unwrap_or("")
}

#[cfg(unix)]
fn fill_field(s: &mut crate::tui::Session, text: &str) -> Result<(), (String, String)> {
    let mut b = vec![0x7f_u8; FILL_CLEAR];
    b.extend_from_slice(text.as_bytes());
    s.send(&b)
        .map_err(|e| ("field filled".to_string(), e.to_string()))?;
    s.settle(Duration::from_millis(150), Duration::from_secs(5));
    Ok(())
}

/// The TUI's file and folder dialogs are a path prompt; answer it with a waiting `pick`.
#[cfg(unix)]
fn answer_prompt(
    s: &mut crate::tui::Session,
    pending: &mut Option<String>,
) -> Result<(), (String, String)> {
    let prompt = |sc: &crate::tui::Screen| {
        let t = sc.text();
        t.contains("Tab browse") && t.contains("Esc cancel")
    };
    if pending.is_none() || !s.wait_for(Duration::from_secs(3), prompt) {
        return Ok(());
    }
    let path = pending.take().unwrap_or_default();
    fill_field(s, &path)?;
    press(s, "Enter")
}

#[cfg(unix)]
fn tui_tab(s: &mut crate::tui::Session, t: &str) -> Result<(), (String, String)> {
    // Row 0 is the tab bar, `|`-separated, the current tab in reverse video; screens may own the arrows.
    let (names, cur) = s.with_screen(|sc| {
        let names: Vec<String> = sc.line(0).split('|').map(|n| n.trim().to_string()).collect();
        let rev = sc.reverse_text(0);
        (names.clone(), names.iter().position(|n| *n == rev))
    });
    let want = names.iter().position(|n| n == t);
    let (cur, want) = match (cur, want) {
        (Some(c), Some(w)) => (c, w),
        _ => return Err((format!("tab '{}' in the tab bar", t), s.text())),
    };
    let key = if want > cur { "Right" } else { "Left" };
    for _ in 0..(want as isize - cur as isize).unsigned_abs() {
        press(s, key)?;
    }
    if s.with_screen(|sc| sc.reverse_text(0)) != t {
        return Err((format!("tab '{}' selected", t), s.text()));
    }
    Ok(())
}

#[cfg(unix)]
fn press(s: &mut crate::tui::Session, name: &str) -> Result<(), (String, String)> {
    let b = crate::tui::key_bytes(name)
        .ok_or_else(|| ("a known key name".to_string(), name.to_string()))?;
    s.send(&b).map_err(|e| ("the key sent".to_string(), e.to_string()))?;
    s.settle(Duration::from_millis(120), Duration::from_secs(5));
    Ok(())
}

/// The rows the TUI marks as focused: a `> ` marker or reverse video.
#[cfg(unix)]
pub fn focused_rows(sc: &crate::tui::Screen) -> Vec<String> {
    let mut out = Vec::new();
    for r in 0..sc.rows() {
        let line = sc.line(r);
        let rev = sc.reverse_text(r);
        if let Some(i) = line.find("> ") {
            out.push(line[i + 2..].to_string());
        } else if !rev.is_empty() {
            out.push(line);
        }
    }
    out
}

#[cfg(unix)]
fn focus_has(s: &crate::tui::Session, want: &str) -> bool {
    s.with_screen(|sc| focused_rows(sc).iter().any(|l| l.contains(want)))
}

/// Press `keys.0` until `done` holds, at most `max` times; then the same with `keys.1`.
#[cfg(unix)]
fn seek(
    s: &mut crate::tui::Session,
    keys: (&str, &str),
    max: usize,
    done: &dyn Fn(&crate::tui::Session) -> bool,
) -> Result<bool, (String, String)> {
    for k in [keys.0, keys.1] {
        for _ in 0..max {
            if done(s) {
                return Ok(true);
            }
            press(s, k)?;
        }
    }
    Ok(done(s))
}
