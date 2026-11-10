//! E2E flows: `ui` steps that drive a front-end, run by a per-shell driver (FLOWS.md).

use std::path::Path;
use std::time::Duration;

/// Backspaces `fill` sends to clear a field: more than any path a flow types.
#[cfg(any(unix, windows))]
const FILL_CLEAR: usize = 512;

/// Terminal size every TUI flow runs at, so a flow's screen text is the same on every host.
pub const TUI_ROWS: u16 = 40;
pub const TUI_COLS: u16 = 120;

/// Start `rb-cli tui` with `home` as its HOME, holding a config that turns the update check off.
#[cfg(any(unix, windows))]
pub fn start_tui(rb_cli: &Path, home: &Path, cwd: &Path) -> Result<crate::tui::Session, String> {
    let config = r#"{"update_check":{"enabled":false,"repository_url":""}}"#;
    for dir in [
        ".config/rusty-backup",
        "Library/Application Support/rusty-backup",
    ] {
        let d = home.join(dir);
        std::fs::create_dir_all(&d).map_err(|e| format!("{}: {}", d.display(), e))?;
        std::fs::write(d.join("config.json"), config).map_err(|e| e.to_string())?;
    }
    let picks = picks_file(cwd);
    std::fs::write(&picks, "").map_err(|e| format!("{}: {}", picks.display(), e))?;
    let env = vec![
        (
            "RUSTY_BACKUP_TEST_PICKS".to_string(),
            picks.display().to_string(),
        ),
        ("HOME".to_string(), home.display().to_string()),
        ("RB_TUI_ASCII".to_string(), "1".to_string()),
        (
            "XDG_CONFIG_HOME".to_string(),
            home.join(".config").display().to_string(),
        ),
        (
            "RUSTY_BACKUP_CONFIG_DIR".to_string(),
            home.join(".config/rusty-backup").display().to_string(),
        ),
        (
            "APPDATA".to_string(),
            home.join("AppData/Roaming").display().to_string(),
        ),
        (
            "LOCALAPPDATA".to_string(),
            home.join("AppData/Local").display().to_string(),
        ),
    ];
    let s = crate::tui::Session::spawn(rb_cli, &["tui".to_string()], cwd, &env, TUI_ROWS, TUI_COLS)
        .map_err(|e| format!("could not start {} tui: {}", rb_cli.display(), e))?;
    s.settle(Duration::from_millis(400), Duration::from_secs(10));
    Ok(s)
}

#[cfg(not(any(unix, windows)))]
pub fn cmd_tui_shot(_rb_cli: &Path, _scratch: &Path, _ops: &[String]) -> i32 {
    eprintln!("tui-shot needs a pseudo-terminal; Windows (ConPTY) is not supported yet");
    2
}

/// The scripted-pick file (`controller::picks`) a TUI started in `cwd` reads.
#[cfg(any(unix, windows))]
fn picks_file(cwd: &Path) -> PathBuf {
    cwd.join("picks.tsv")
}

/// Answer every later picker with `path`, as the GTK driver does.
#[cfg(any(unix, windows))]
fn set_pick(cwd: &Path, path: &str) -> std::io::Result<()> {
    std::fs::write(picks_file(cwd), format!("*\t{}\n", path))
}

/// `rb-regress tui-shot OP...`: a key name, `tab:NAME`, `type:TEXT`, `fill:TEXT`, `pick:PATH` or `wait:TEXT`.
#[cfg(any(unix, windows))]
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
        let ok = if let Some(t) = op.strip_prefix("tab:") {
            tui_tab(&mut s, t).is_ok()
        } else if let Some(t) = op.strip_prefix("type:") {
            s.send(t.as_bytes()).is_ok()
        } else if let Some(t) = op.strip_prefix("fill:") {
            let mut b = vec![0x7f_u8; FILL_CLEAR];
            b.extend_from_slice(t.as_bytes());
            s.send(&b).is_ok()
        } else if let Some(t) = op.strip_prefix("wait:") {
            s.wait_for(Duration::from_secs(60), |sc| sc.text().contains(t))
        } else if let Some(p) = op.strip_prefix("pick:") {
            set_pick(&dir, p).is_ok()
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

#[cfg(any(unix, windows))]
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
        ("wait_gone", &op.wait_gone),
        ("wait_file", &op.wait_file),
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
    /// Which of several same-named controls (0 first); on the `via` dropdown when there is one.
    pub nth: Option<usize>,
    /// This shell has no such step (GTK copies to a host folder at once; the TUI stages it).
    #[serde(default)]
    pub skip: bool,
    /// No table names it: the name is literal text (a file in a list), found anywhere in a row.
    #[serde(skip)]
    pub literal: bool,
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

/// Add the controllers' own labels (`rb-cli ui-labels`) as `[controller]`, below the table's entries.
pub fn add_controller_labels(controls: &mut Controls, rb_cli: &Path) {
    static TABLE: std::sync::OnceLock<Vec<(String, String)>> = std::sync::OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let out = match std::process::Command::new(rb_cli).arg("ui-labels").output() {
            Ok(o) if o.status.success() => o.stdout,
            _ => return Vec::new(),
        };
        String::from_utf8_lossy(&out)
            .lines()
            .filter_map(|l| l.split_once('\t'))
            .map(|(id, label)| (id.to_string(), label.to_string()))
            .collect()
    });
    let m = controls.entry("controller".to_string()).or_default();
    for (id, label) in table {
        m.insert(
            id.clone(),
            Control {
                label: Some(label.clone()),
                ..Control::default()
            },
        );
    }
}

/// Resolve `name` for `shell`: a listed controller id, or a literal label.
fn control(controls: &Controls, shell: &str, name: &str) -> Control {
    controls
        .get(shell)
        .and_then(|m| m.get(name))
        .or_else(|| controls.get("all").and_then(|m| m.get(name)))
        .or_else(|| controls.get("controller").and_then(|m| m.get(name)))
        .cloned()
        .unwrap_or_else(|| Control {
            label: Some(name.to_string()),
            literal: true,
            ..Control::default()
        })
}

/// The shell a case drives, started at its first `ui` step and kept for the rest.
pub struct UiSession {
    shell: Option<String>,
    rb_cli: PathBuf,
    gtk: PathBuf,
    win32: PathBuf,
    scratch: PathBuf,
    controls: Controls,
    /// The control the last `focus` landed on, which `fill` and `choose` act on.
    focused: Control,
    #[cfg(any(unix, windows))]
    tui: Option<crate::tui::Session>,
    gtk_driver: Option<crate::gtk::Driver>,
}

impl UiSession {
    pub fn new(
        shell: Option<String>,
        rb_cli: &Path,
        gtk: &Path,
        win32: &Path,
        scratch: &Path,
        controls: Controls,
    ) -> Self {
        UiSession {
            shell,
            rb_cli: rb_cli.to_path_buf(),
            gtk: gtk.to_path_buf(),
            win32: win32.to_path_buf(),
            scratch: scratch.to_path_buf(),
            controls,
            focused: Control::default(),
            #[cfg(any(unix, windows))]
            tui: None,
            gtk_driver: None,
        }
    }

    /// Everything the shell wrote so far, for the failure bundle.
    pub fn transcript(&self) -> String {
        #[cfg(any(unix, windows))]
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
            "gtk" | "win32" => self.run_driver(&shell, ops, resolve),
            other => Err(UiError::Skip(
                Verdict::SkipTool,
                format!("no driver for the '{}' shell yet", other),
            )),
        }
    }

    /// A shell driven by a JSON-lines driver script: GTK over AT-SPI, Win32 over UIA.
    fn run_driver(
        &mut self,
        shell: &str,
        ops: &[UiOp],
        resolve: &dyn Fn(&str) -> String,
    ) -> Result<(), UiError> {
        if self.gtk_driver.is_none() {
            let d = match shell {
                "win32" => crate::uia::start(&self.win32, &self.scratch.join("win32"))?,
                _ => crate::gtk::Driver::start(&self.gtk, &self.scratch.join("gtk"))?,
            };
            self.gtk_driver = Some(d);
        }
        for op in ops {
            if op.only.as_deref().is_some_and(|o| o != shell) {
                continue;
            }
            if let Some(f) = &op.wait_file {
                let limit = Duration::from_millis(op.timeout_ms.unwrap_or(30_000));
                if !wait_file(Path::new(&resolve(f)), limit) {
                    let screen = self
                        .gtk_driver
                        .as_mut()
                        .map(|d| d.log())
                        .unwrap_or_default();
                    return Err(UiError::Fail(
                        FailedAssertion {
                            op: format!("ui {}", describe(op)),
                            selector: None,
                            expected: format!("{} written", resolve(f)),
                            observed: "not there, or still growing".to_string(),
                        },
                        screen,
                    ));
                }
                continue;
            }
            let target = control(&self.controls, shell, &resolve(op_target(op)));
            if target.skip && !op_target(op).is_empty() {
                continue;
            }
            let mut msg = self.driver_message(shell, op, resolve);
            // Win32 finds a control by its AutomationId, the controller id itself.
            if shell == "win32" && (op.focus.is_some() || op.activate.is_some()) {
                if let Some(m) = msg.as_object_mut() {
                    m.insert("id".to_string(), serde_json::Value::String(resolve(op_target(op))));
                }
            }
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

    /// The JSON line a driver gets for `op`, with controller ids resolved to labels.
    fn driver_message(
        &mut self,
        shell: &str,
        op: &UiOp,
        resolve: &dyn Fn(&str) -> String,
    ) -> serde_json::Value {
        let ctl = |s: &Self, n: &str| control(&s.controls, shell, &resolve(n));
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
            if let Some(n) = c.nth {
                m.insert("nth".to_string(), serde_json::Value::from(n));
            }
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
            if let Some(n) = c.nth {
                m.insert("nth".to_string(), serde_json::Value::from(n));
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
        } else if let Some(w) = &op.wait_gone {
            put("op", "wait_gone".into());
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

    #[cfg(not(any(unix, windows)))]
    fn run_tui(&mut self, _ops: &[UiOp], _r: &dyn Fn(&str) -> String) -> Result<(), UiError> {
        Err(UiError::Skip(
            Verdict::SkipPlatform,
            "the TUI driver needs a pseudo-terminal; ConPTY is not supported yet".to_string(),
        ))
    }

    #[cfg(any(unix, windows))]
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
                    s.with_screen(|sc| sc.marked()),
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

    #[cfg(any(unix, windows))]
    fn tui_op(
        &mut self,
        op: &UiOp,
        resolve: &dyn Fn(&str) -> String,
        limit: Duration,
    ) -> Result<(), (String, String)> {
        let ctl = control(&self.controls, "tui", &resolve(op_target(op)));
        if ctl.skip && !op_target(op).is_empty() {
            return Ok(());
        }
        let s = self.tui.as_mut().expect("started");
        let screen = |s: &crate::tui::Session| s.text();
        let label = ctl.label.clone().unwrap_or_default();
        let literal = ctl.literal;
        let on = move |s: &crate::tui::Session| focus_names(s, &label, literal);
        let label = ctl.label.clone().unwrap_or_default();
        if let Some(t) = &op.tab {
            tui_tab(s, &resolve(t))?;
        } else if op.focus.is_some() {
            if !seek(s, ("Down", "Up"), 60, &on)? {
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
                Some(ks) => {
                    for k in ks.split_whitespace() {
                        press(s, k)?;
                    }
                }
                None => {
                    if !seek(s, ("Down", "Up"), 60, &on)? {
                        return Err((format!("focus on '{}'", label), screen(s)));
                    }
                    press(s, "Enter")?;
                }
            }
        } else if let Some(f) = &op.wait_file {
            let f = resolve(f);
            if !wait_file(Path::new(&f), limit) {
                return Err((format!("{} written", f), screen(s)));
            }
        } else if let Some(p) = &op.pick {
            set_pick(&self.scratch, &resolve(p))
                .map_err(|e| ("the pick written".to_string(), e.to_string()))?;
        } else if let Some(k) = &op.key {
            press(s, k)?;
        } else if let Some(ks) = &op.keys {
            for k in ks {
                press(s, k)?;
            }
        } else if let Some(t) = &op.text {
            s.send(resolve(t).as_bytes())
                .map_err(|e| ("text typed".to_string(), e.to_string()))?;
            s.settle(Duration::from_millis(120), Duration::from_secs(5));
        } else if let Some(t) = &op.fill {
            fill_field(s, &resolve(t))?;
        } else if op.wait.is_some() {
            let w = label.clone();
            if !s.wait_for(limit, |sc| shows(&sc.text(), &w)) {
                return Err((format!("screen shows '{}'", w), screen(s)));
            }
        } else if op.wait_gone.is_some() {
            let w = label.clone();
            if !s.wait_for(limit, |sc| !shows(&sc.text(), &w)) {
                return Err((format!("screen without '{}'", w), screen(s)));
            }
        } else if op.expect.is_some() {
            let e = label.clone();
            s.settle(Duration::from_millis(200), Duration::from_secs(5));
            if !shows(&s.text(), &e) {
                return Err((format!("screen shows '{}'", e), screen(s)));
            }
        } else if op.expect_not.is_some() {
            let e = label.clone();
            s.settle(Duration::from_millis(200), Duration::from_secs(5));
            if shows(&s.text(), &e) {
                return Err((format!("screen without '{}'", e), screen(s)));
            }
        }
        Ok(())
    }
}

/// Whether the screen holds the text, or any of its ` || `-separated alternatives.
fn shows(screen: &str, want: &str) -> bool {
    want.split(" || ").any(|w| screen.contains(w))
}

/// Wait until `path` exists and its size holds for half a second.
fn wait_file(path: &Path, limit: Duration) -> bool {
    let start = std::time::Instant::now();
    let mut last: Option<u64> = None;
    let mut since = std::time::Instant::now();
    while start.elapsed() < limit {
        let size = std::fs::metadata(path).ok().map(|m| m.len());
        if size.is_some() && size == last {
            if since.elapsed() >= Duration::from_millis(500) {
                return true;
            }
        } else {
            last = size;
            since = std::time::Instant::now();
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

/// The control an op names, if it names one.
fn op_target(op: &UiOp) -> &str {
    op.focus
        .as_deref()
        .or(op.activate.as_deref())
        .or(op.wait.as_deref())
        .or(op.wait_gone.as_deref())
        .or(op.expect.as_deref())
        .or(op.expect_not.as_deref())
        .unwrap_or("")
}

#[cfg(any(unix, windows))]
fn fill_field(s: &mut crate::tui::Session, text: &str) -> Result<(), (String, String)> {
    let mut b = vec![0x7f_u8; FILL_CLEAR];
    b.extend_from_slice(text.as_bytes());
    s.send(&b)
        .map_err(|e| ("field filled".to_string(), e.to_string()))?;
    s.settle(Duration::from_millis(150), Duration::from_secs(5));
    Ok(())
}

#[cfg(any(unix, windows))]
fn tui_tab(s: &mut crate::tui::Session, t: &str) -> Result<(), (String, String)> {
    // Row 0 is the tab bar, `|`-separated, the current tab in reverse video; screens may own the arrows.
    let (names, cur) = s.with_screen(|sc| {
        let names: Vec<String> = sc
            .line(0)
            .split('|')
            .map(|n| n.trim().to_string())
            .collect();
        let rev = sc.reverse_text(0);
        (names.clone(), names.iter().position(|n| *n == rev))
    });
    let want = names.iter().position(|n| n == t);
    let (cur, want) = match (cur, want) {
        (Some(c), Some(w)) => (c, w),
        _ => return Err((format!("tab '{}' in the tab bar", t), s.text())),
    };
    // Digits 1-9 jump straight to a tab (no screen in between to swallow arrows); arrows otherwise.
    if want < 9 && want != cur {
        press(s, &(want + 1).to_string())?;
    }
    let cur = s.with_screen(|sc| {
        let rev = sc.reverse_text(0);
        names.iter().position(|n| *n == rev).unwrap_or(cur)
    });
    let key = if want > cur { "Right" } else { "Left" };
    for _ in 0..(want as isize - cur as isize).unsigned_abs() {
        press(s, key)?;
    }
    if s.with_screen(|sc| sc.reverse_text(0)) != t {
        return Err((format!("tab '{}' selected", t), s.text()));
    }
    Ok(())
}

#[cfg(any(unix, windows))]
fn press(s: &mut crate::tui::Session, name: &str) -> Result<(), (String, String)> {
    let b = crate::tui::key_bytes(name)
        .ok_or_else(|| ("a known key name".to_string(), name.to_string()))?;
    s.send(&b)
        .map_err(|e| ("the key sent".to_string(), e.to_string()))?;
    s.settle(Duration::from_millis(120), Duration::from_secs(5));
    Ok(())
}

/// The rows the TUI marks as focused: a `> ` marker or reverse video.
#[cfg(any(unix, windows))]
pub fn focused_rows(sc: &crate::tui::Screen) -> Vec<String> {
    let mut out = Vec::new();
    for r in 0..sc.rows() {
        let line = sc.line(r);
        let rev = sc.reverse_text(r);
        // The marker opens a box's content (`|> Name:`); `<ARC> |` mid-row is not one.
        let marked: Vec<&str> = line
            .split('|')
            .filter_map(|seg| seg.strip_prefix("> "))
            .collect();
        if !marked.is_empty() {
            out.extend(marked.iter().map(|m| m.to_string()));
        } else if !rev.is_empty() {
            out.push(line);
        }
    }
    out
}

#[cfg(any(unix, windows))]
fn focus_has(s: &crate::tui::Session, want: &str) -> bool {
    s.with_screen(|sc| focused_rows(sc).iter().any(|l| l.contains(want)))
}

/// The focused row is the control `label` names; `literal` text may sit anywhere in it.
#[cfg(any(unix, windows))]
fn focus_names(s: &crate::tui::Session, label: &str, literal: bool) -> bool {
    if literal {
        return focus_has(s, label);
    }
    s.with_screen(|sc| focused_rows(sc).iter().any(|l| names(l, label)))
}

/// A row names a control when it opens with its label (then `:`, `]` or column padding),
/// or carries it as an inline button; a title or a sentence that merely mentions it does not.
#[cfg(any(unix, windows))]
fn names(row: &str, label: &str) -> bool {
    if row.contains(&format!("[{}]", label)) || row.contains(&format!("[ {} ]", label)) {
        return true;
    }
    let row = row.trim_start_matches(['|', ' ', '>', '[']);
    match row.strip_prefix(label) {
        Some(rest) => {
            let next = |p: &str| rest.starts_with(p);
            rest.is_empty() || [":", "]", "  ", " <", " ["].iter().any(|p| next(p))
        }
        None => false,
    }
}

/// Press `keys.0` until `done` holds, at most `max` times; then the same with `keys.1`.
#[cfg(any(unix, windows))]
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

#[cfg(all(test, any(unix, windows)))]
mod tests {
    use super::{focused_rows, names};
    use crate::tui::Screen;

    #[test]
    fn a_label_names_its_own_row_only() {
        assert!(names("[ Restore ]", "Restore"));
        assert!(names("Backup Folder: C:/bk", "Backup Folder"));
        assert!(names("Size to the folder  < on >", "Size to the folder"));
        assert!(names("Size to the folder < on >", "Size to the folder"));
        assert!(names("Output file:  C:/x.hfv  [Save As...]", "Save As..."));
        assert!(!names(
            "Remote:        [ ] Restore to a remote machine",
            "Restore"
        ));
        assert!(!names(" Warning+ Expand HFS Volume ----", "Expand"));
        assert!(!names("Target size (MiB):  8", "Target"));
    }

    #[test]
    fn focus_marker_opens_a_box_not_mid_row() {
        let mut sc = Screen::new(3, 60);
        sc.feed(b"|> Format:  < Zstd >\r\n|  [+] in.zip  144 B <ARC> ||  |");
        assert_eq!(focused_rows(&sc), vec!["Format:  < Zstd >".to_string()]);
    }
}
