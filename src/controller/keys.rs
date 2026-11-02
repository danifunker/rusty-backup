//! Shortcuts as data: named commands, their default chords, and the user's override file.
//!
//! Every view translates its native key event into a [`Key`] plus [`Mods`] and asks
//! [`KeyMap::lookup`] which [`Command`] it means; no view decides what a key does.
//! `Primary` in a chord is Ctrl on Windows and Linux and Cmd on macOS, so one entry
//! serves every platform; a `mac:` / `pc:` prefix limits an entry to one side.
//!
//! The user can rebind any command in `keybindings.toml` beside `config.json`:
//!
//! ```toml
//! [file-manager]
//! rename = ["F2", "Primary+R"]
//! apply = "F10"
//! delete = ["F8", "Delete", "mac:Backspace"]
//! ```
//!
//! A listed command replaces all of its default chords; unlisted commands keep theirs.
//! The parser reads only that subset of TOML, so the engine needs no TOML crate.

use std::fmt;
use std::path::{Path, PathBuf};

/// A key, independent of any toolkit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Key {
    F(u8),
    /// A printable key, compared case-insensitively (`Shift` is a separate modifier).
    Char(char),
    Tab,
    Enter,
    Escape,
    Backspace,
    Delete,
    Insert,
    Space,
    Up,
    Down,
    Left,
    Right,
    PageUp,
    PageDown,
    Home,
    End,
}

/// The modifiers held when a key was pressed, as the platform reports them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Mods {
    pub ctrl: bool,
    pub alt: bool,
    pub shift: bool,
    /// Cmd on macOS (the Windows / Super key elsewhere, which no binding uses).
    pub cmd: bool,
}

/// Which platforms a chord applies on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnPlatform {
    All,
    MacOnly,
    PcOnly,
}

/// A key plus the modifiers a binding requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Chord {
    pub key: Key,
    /// Ctrl on Windows / Linux, Cmd on macOS.
    pub primary: bool,
    pub alt: bool,
    pub shift: bool,
    pub on: OnPlatform,
}

/// What a file-manager shortcut asks for; each controller maps these to its own actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Command {
    Help,
    Rename,
    Info,
    Refresh,
    Copy,
    Edit,
    Delete,
    NewFolder,
    Apply,
    SwitchPane,
    Open,
    Parent,
    Mark,
    Close,
    CursorUp,
    CursorDown,
    PageUp,
    PageDown,
    Home,
    End,
    OpenImage,
    OpenFolder,
    OpenBackup,
    Connect,
    Partition,
    Sort,
    Checksums,
    Export,
    Edits,
    Log,
    /// Show the active pane's File Info in the other pane, following its cursor.
    QuickView,
    /// The View menu: listing style, quick view, the log.
    ViewMenu,
    /// Expand the cursor row in a tree (open the folder in a plain list).
    Expand,
    /// Collapse the cursor row in a tree, or go to its parent row.
    Collapse,
    /// Check the open filesystem (the Explorer's volume tools from here down).
    Fsck,
    Repair,
    Bless,
    Import,
    Metadata,
    Transforms,
}

impl Command {
    /// The commands the two-pane file manager binds.
    pub const FILE_MANAGER: [Command; 34] = [
        Command::Help,
        Command::Rename,
        Command::Info,
        Command::Refresh,
        Command::Copy,
        Command::Edit,
        Command::Delete,
        Command::NewFolder,
        Command::Apply,
        Command::SwitchPane,
        Command::Open,
        Command::Parent,
        Command::Mark,
        Command::Close,
        Command::CursorUp,
        Command::CursorDown,
        Command::PageUp,
        Command::PageDown,
        Command::Home,
        Command::End,
        Command::OpenImage,
        Command::OpenFolder,
        Command::OpenBackup,
        Command::Connect,
        Command::Partition,
        Command::Sort,
        Command::Checksums,
        Command::Export,
        Command::Edits,
        Command::Log,
        Command::QuickView,
        Command::ViewMenu,
        Command::Expand,
        Command::Collapse,
    ];

    pub const ALL: [Command; 40] = [
        Command::Help,
        Command::Rename,
        Command::Info,
        Command::Refresh,
        Command::Copy,
        Command::Edit,
        Command::Delete,
        Command::NewFolder,
        Command::Apply,
        Command::SwitchPane,
        Command::Open,
        Command::Parent,
        Command::Mark,
        Command::Close,
        Command::CursorUp,
        Command::CursorDown,
        Command::PageUp,
        Command::PageDown,
        Command::Home,
        Command::End,
        Command::OpenImage,
        Command::OpenFolder,
        Command::OpenBackup,
        Command::Connect,
        Command::Partition,
        Command::Sort,
        Command::Checksums,
        Command::Export,
        Command::Edits,
        Command::Log,
        Command::QuickView,
        Command::ViewMenu,
        Command::Expand,
        Command::Collapse,
        Command::Fsck,
        Command::Repair,
        Command::Bless,
        Command::Import,
        Command::Metadata,
        Command::Transforms,
    ];

    /// The commands the one-pane Explorer binds.
    pub const EXPLORER: [Command; 33] = [
        Command::Help,
        Command::Rename,
        Command::Info,
        Command::Refresh,
        Command::Edit,
        Command::Delete,
        Command::NewFolder,
        Command::Apply,
        Command::SwitchPane,
        Command::Open,
        Command::Parent,
        Command::Mark,
        Command::Close,
        Command::CursorUp,
        Command::CursorDown,
        Command::PageUp,
        Command::PageDown,
        Command::Home,
        Command::End,
        Command::Sort,
        Command::Checksums,
        Command::Export,
        Command::Edits,
        Command::Log,
        Command::ViewMenu,
        Command::Expand,
        Command::Collapse,
        Command::Fsck,
        Command::Repair,
        Command::Bless,
        Command::Import,
        Command::Metadata,
        Command::Transforms,
    ];

    /// The name used in `keybindings.toml`.
    pub fn name(self) -> &'static str {
        match self {
            Command::Help => "help",
            Command::Rename => "rename",
            Command::Info => "info",
            Command::Refresh => "refresh",
            Command::Copy => "copy",
            Command::Edit => "edit",
            Command::Delete => "delete",
            Command::NewFolder => "new-folder",
            Command::Apply => "apply",
            Command::SwitchPane => "switch-pane",
            Command::Open => "open",
            Command::Parent => "parent",
            Command::Mark => "mark",
            Command::Close => "close",
            Command::CursorUp => "cursor-up",
            Command::CursorDown => "cursor-down",
            Command::PageUp => "page-up",
            Command::PageDown => "page-down",
            Command::Home => "home",
            Command::End => "end",
            Command::OpenImage => "open-image",
            Command::OpenFolder => "open-folder",
            Command::OpenBackup => "open-backup",
            Command::Connect => "connect",
            Command::Partition => "partition",
            Command::Sort => "sort",
            Command::Checksums => "checksums",
            Command::Export => "export",
            Command::Edits => "edits",
            Command::Log => "log",
            Command::QuickView => "quick-view",
            Command::ViewMenu => "view-menu",
            Command::Expand => "expand",
            Command::Collapse => "collapse",
            Command::Fsck => "fsck",
            Command::Repair => "repair",
            Command::Bless => "bless",
            Command::Import => "import",
            Command::Metadata => "metadata",
            Command::Transforms => "transforms",
        }
    }

    pub fn from_name(name: &str) -> Option<Command> {
        Command::ALL.iter().copied().find(|c| c.name() == name)
    }

    /// A short label for help screens and menus.
    pub fn label(self) -> &'static str {
        match self {
            Command::Help => "Help",
            Command::Rename => "Rename",
            Command::Info => "Info",
            Command::Refresh => "Refresh",
            Command::Copy => "Copy",
            Command::Edit => "Edit",
            Command::Delete => "Delete",
            Command::NewFolder => "New folder",
            Command::Apply => "Apply",
            Command::SwitchPane => "Switch pane",
            Command::Open => "Open",
            Command::Parent => "Parent folder",
            Command::Mark => "Mark",
            Command::Close => "Close",
            Command::CursorUp => "Up",
            Command::CursorDown => "Down",
            Command::PageUp => "Page up",
            Command::PageDown => "Page down",
            Command::Home => "Top",
            Command::End => "Bottom",
            Command::OpenImage => "Open image",
            Command::OpenFolder => "Open folder",
            Command::OpenBackup => "Open backup",
            Command::Connect => "Connect to remote",
            Command::Partition => "Choose partition",
            Command::Sort => "Sort",
            Command::Checksums => "Checksums",
            Command::Export => "Export",
            Command::Edits => "Staged edits",
            Command::Log => "Session log",
            Command::QuickView => "Quick view",
            Command::ViewMenu => "View menu",
            Command::Expand => "Expand",
            Command::Collapse => "Collapse",
            Command::Fsck => "Check filesystem",
            Command::Repair => "Repair filesystem",
            Command::Bless => "Bless System Folder",
            Command::Import => "Import host file",
            Command::Metadata => "Edit metadata",
            Command::Transforms => "Transform image",
        }
    }
}

/// The file-manager defaults, as chord strings; [`KeyMap::defaults`] parses them.
const DEFAULTS: &[(Command, &[&str])] = &[
    (Command::Help, &["F1"]),
    (Command::Rename, &["F2"]),
    (Command::Info, &["F3", "Primary+I"]),
    (Command::Refresh, &["F5"]),
    (Command::Copy, &["F6"]),
    (Command::Edit, &["F7"]),
    (Command::Delete, &["F8", "Delete", "mac:Backspace"]),
    (Command::NewFolder, &["Primary+N"]),
    (Command::Apply, &["F10"]),
    (Command::SwitchPane, &["Tab"]),
    (Command::Open, &["Enter"]),
    (Command::Parent, &["pc:Backspace", "mac:Primary+Up"]),
    (Command::Mark, &["Space", "Insert"]),
    (Command::Close, &["Escape"]),
    (Command::CursorUp, &["Up"]),
    (Command::CursorDown, &["Down"]),
    (Command::PageUp, &["PageUp"]),
    (Command::PageDown, &["PageDown"]),
    (Command::Home, &["Home"]),
    (Command::End, &["End"]),
    (Command::OpenImage, &["Primary+O"]),
    (Command::OpenFolder, &["Primary+D"]),
    (Command::OpenBackup, &["Primary+B"]),
    (Command::Connect, &["Primary+Shift+K"]),
    (Command::Partition, &["Primary+P"]),
    (Command::Sort, &["Primary+S"]),
    (Command::Checksums, &["Primary+G"]),
    (Command::Export, &["Primary+E"]),
    (Command::Edits, &["Primary+U"]),
    (Command::Log, &["Primary+L"]),
    (Command::QuickView, &["Primary+K"]),
    (Command::ViewMenu, &["F9"]),
    (Command::Expand, &["Right"]),
    (Command::Collapse, &["Left"]),
];

/// Extra chords a terminal needs: most cannot tell Ctrl+Shift+K from Ctrl+K.
const TERMINAL_EXTRA: &[(Command, &[&str])] = &[(Command::Connect, &["Alt+K"])];

/// The Explorer's chords beyond the file-manager ones it keeps (no sources, no second pane).
const EXPLORER_EXTRA: &[(Command, &[&str])] = &[
    (Command::Fsck, &["Primary+F"]),
    (Command::Repair, &["Primary+R"]),
    (Command::Bless, &["Primary+B"]),
    (Command::Import, &["Primary+O"]),
    (Command::Metadata, &["Primary+A"]),
    (Command::Transforms, &["Primary+T"]),
];

/// The command each chord means, for one platform.
#[derive(Debug, Clone)]
pub struct KeyMap {
    mac: bool,
    bindings: Vec<(Chord, Command)>,
}

impl KeyMap {
    /// The built-in bindings for macOS (`mac`) or Windows / Linux.
    pub fn defaults(mac: bool) -> Self {
        let mut bindings = Vec::new();
        for (cmd, chords) in DEFAULTS {
            for text in *chords {
                let chord = Chord::parse(text).expect("built-in chord parses");
                bindings.push((chord, *cmd));
            }
        }
        Self { mac, bindings }
    }

    /// The Explorer's bindings: the file-manager ones for its commands, plus its volume tools.
    pub fn explorer_defaults(mac: bool) -> Self {
        let mut map = Self::defaults(mac);
        map.bindings.retain(|(_, c)| Command::EXPLORER.contains(c));
        // A terminal reads Ctrl+I as Tab and Ctrl+M as Enter, so those chords never arrive.
        let aliased = |c: &Chord| c.primary && matches!(c.key, Key::Char('i' | 'm' | 'h' | 'j'));
        map.bindings.retain(|(chord, _)| !aliased(chord));
        for (cmd, chords) in EXPLORER_EXTRA {
            for text in *chords {
                let chord = Chord::parse(text).expect("built-in chord parses");
                map.bindings.push((chord, *cmd));
            }
        }
        map
    }

    /// [`Self::explorer_defaults`] with the PC chords, for a terminal.
    pub fn explorer_terminal() -> Self {
        Self::explorer_defaults(false)
    }

    /// Put `table`'s chords first, so a hint names the chord that works here.
    fn add_first(&mut self, table: &[(Command, &[&str])]) {
        let mut first = Vec::new();
        for (cmd, chords) in table {
            for text in *chords {
                let chord = Chord::parse(text).expect("built-in chord parses");
                first.push((chord, *cmd));
            }
        }
        first.append(&mut self.bindings);
        self.bindings = first;
    }

    /// [`Self::explorer_terminal`] with `path`'s overrides applied, plus any problems found.
    pub fn load_explorer_terminal(path: Option<&Path>) -> (Self, Vec<String>) {
        Self::load_onto(Self::explorer_terminal(), path)
    }

    /// The defaults for the platform this was built for.
    pub fn for_this_platform() -> Self {
        Self::defaults(cfg!(target_os = "macos"))
    }

    /// The command a key press means here, if any.
    pub fn lookup(&self, key: Key, mods: Mods) -> Option<Command> {
        self.bindings
            .iter()
            .find(|(chord, _)| chord.matches(key, mods, self.mac))
            .map(|(_, cmd)| *cmd)
    }

    /// The chords bound to `cmd` on this platform, for help text and menu hints.
    pub fn chords_for(&self, cmd: Command) -> Vec<Chord> {
        self.bindings
            .iter()
            .filter(|(chord, c)| *c == cmd && chord.applies(self.mac))
            .map(|(chord, _)| *chord)
            .collect()
    }

    /// The first chord for `cmd` as this platform writes it (`Ctrl+N`, `Cmd+N`, `F2`), or "".
    pub fn hint(&self, cmd: Command) -> String {
        self.chords_for(cmd)
            .first()
            .map(|c| c.display(self.mac))
            .unwrap_or_default()
    }

    /// Applies a `keybindings.toml` text; returns one message per line it could not use.
    pub fn apply_overrides(&mut self, text: &str) -> Vec<String> {
        let mut problems = Vec::new();
        for (n, raw) in text.lines().enumerate() {
            let line = strip_comment(raw).trim();
            if line.is_empty() || line.starts_with('[') {
                continue;
            }
            let Some((name, value)) = line.split_once('=') else {
                problems.push(format!("line {}: expected `command = \"chord\"`", n + 1));
                continue;
            };
            let name = name.trim().trim_matches('"');
            let Some(cmd) = Command::from_name(name) else {
                problems.push(format!("line {}: unknown command `{name}`", n + 1));
                continue;
            };
            let mut chords = Vec::new();
            let mut ok = true;
            for text in parse_string_list(value.trim()) {
                match Chord::parse(&text) {
                    Ok(c) => chords.push(c),
                    Err(e) => {
                        problems.push(format!("line {}: {e}", n + 1));
                        ok = false;
                    }
                }
            }
            if ok {
                self.bindings.retain(|(_, c)| *c != cmd);
                self.bindings.extend(chords.into_iter().map(|c| (c, cmd)));
            }
        }
        problems
    }

    /// Where the user's override file lives: `keybindings.toml` beside `config.json`.
    pub fn user_file() -> Option<PathBuf> {
        crate::update::UpdateConfig::user_config_dir().map(|d| d.join("keybindings.toml"))
    }

    /// The PC chords on every platform: a terminal has no Cmd key, so Primary is Ctrl there too.
    pub fn for_terminal() -> Self {
        let mut map = Self::defaults(false);
        map.add_first(TERMINAL_EXTRA);
        map
    }

    /// The defaults for this platform with `path`'s overrides applied, plus any problems found.
    pub fn load(path: Option<&Path>) -> (Self, Vec<String>) {
        Self::load_onto(Self::for_this_platform(), path)
    }

    /// [`Self::for_terminal`] with `path`'s overrides applied, plus any problems found.
    pub fn load_terminal(path: Option<&Path>) -> (Self, Vec<String>) {
        Self::load_onto(Self::for_terminal(), path)
    }

    fn load_onto(mut map: Self, path: Option<&Path>) -> (Self, Vec<String>) {
        let mut problems = Vec::new();
        if let Some(path) = path {
            if let Ok(text) = std::fs::read_to_string(path) {
                problems = map
                    .apply_overrides(&text)
                    .into_iter()
                    .map(|p| format!("{}: {p}", path.display()))
                    .collect();
            }
        }
        (map, problems)
    }
}

impl Chord {
    fn applies(&self, mac: bool) -> bool {
        match self.on {
            OnPlatform::All => true,
            OnPlatform::MacOnly => mac,
            OnPlatform::PcOnly => !mac,
        }
    }

    /// True when `key` with exactly these modifiers is this chord on this platform.
    pub fn matches(&self, key: Key, mods: Mods, mac: bool) -> bool {
        if !self.applies(mac) {
            return false;
        }
        let (primary, other) = if mac {
            (mods.cmd, mods.ctrl)
        } else {
            (mods.ctrl, mods.cmd)
        };
        let same_key = match (self.key, key) {
            (Key::Char(a), Key::Char(b)) => a.eq_ignore_ascii_case(&b),
            (a, b) => a == b,
        };
        same_key
            && primary == self.primary
            && !other
            && mods.alt == self.alt
            && mods.shift == self.shift
    }

    /// Parses `F2`, `Primary+N`, `Ctrl+Shift+X`, `mac:Backspace`, `pc:Primary+Up` and the like.
    pub fn parse(text: &str) -> Result<Chord, String> {
        let text = text.trim();
        let (on, rest) = if let Some(r) = strip_prefix_ci(text, "mac:") {
            (OnPlatform::MacOnly, r)
        } else if let Some(r) = strip_prefix_ci(text, "pc:") {
            (OnPlatform::PcOnly, r)
        } else {
            (OnPlatform::All, text)
        };
        let mut chord = Chord {
            key: Key::Escape,
            primary: false,
            alt: false,
            shift: false,
            on,
        };
        let parts: Vec<&str> = rest.split('+').map(str::trim).collect();
        let Some((key_part, mods)) = parts.split_last() else {
            return Err(format!("empty chord `{text}`"));
        };
        for m in mods {
            match m.to_ascii_lowercase().as_str() {
                "primary" | "ctrl" | "control" | "cmd" | "command" => chord.primary = true,
                "alt" | "option" | "opt" => chord.alt = true,
                "shift" => chord.shift = true,
                other => return Err(format!("unknown modifier `{other}` in `{text}`")),
            }
        }
        chord.key =
            parse_key(key_part).ok_or_else(|| format!("unknown key `{key_part}` in `{text}`"))?;
        Ok(chord)
    }

    /// How this platform writes the chord: `Ctrl+N` / `Cmd+N`, `F2`, `Del`.
    pub fn display(&self, mac: bool) -> String {
        let mut s = String::new();
        if self.primary {
            s.push_str(if mac { "Cmd+" } else { "Ctrl+" });
        }
        if self.alt {
            s.push_str(if mac { "Option+" } else { "Alt+" });
        }
        if self.shift {
            s.push_str("Shift+");
        }
        s.push_str(&key_name(self.key));
        s
    }
}

impl fmt::Display for Chord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display(cfg!(target_os = "macos")))
    }
}

fn parse_key(s: &str) -> Option<Key> {
    let lower = s.to_ascii_lowercase();
    if let Some(n) = lower.strip_prefix('f') {
        if let Ok(n) = n.parse::<u8>() {
            if (1..=24).contains(&n) {
                return Some(Key::F(n));
            }
        }
    }
    Some(match lower.as_str() {
        "tab" => Key::Tab,
        "enter" | "return" => Key::Enter,
        "escape" | "esc" => Key::Escape,
        "backspace" => Key::Backspace,
        "delete" | "del" => Key::Delete,
        "insert" | "ins" => Key::Insert,
        "space" => Key::Space,
        "up" => Key::Up,
        "down" => Key::Down,
        "left" => Key::Left,
        "right" => Key::Right,
        "pageup" | "pgup" => Key::PageUp,
        "pagedown" | "pgdn" => Key::PageDown,
        "home" => Key::Home,
        "end" => Key::End,
        _ => {
            let mut chars = s.chars();
            match (chars.next(), chars.next()) {
                (Some(c), None) if !c.is_whitespace() => Key::Char(c.to_ascii_lowercase()),
                _ => return None,
            }
        }
    })
}

fn key_name(key: Key) -> String {
    match key {
        Key::F(n) => format!("F{n}"),
        Key::Char(c) => c.to_ascii_uppercase().to_string(),
        Key::Tab => "Tab".into(),
        Key::Enter => "Enter".into(),
        Key::Escape => "Esc".into(),
        Key::Backspace => "Backspace".into(),
        Key::Delete => "Del".into(),
        Key::Insert => "Ins".into(),
        Key::Space => "Space".into(),
        Key::Up => "Up".into(),
        Key::Down => "Down".into(),
        Key::Left => "Left".into(),
        Key::Right => "Right".into(),
        Key::PageUp => "PgUp".into(),
        Key::PageDown => "PgDn".into(),
        Key::Home => "Home".into(),
        Key::End => "End".into(),
    }
}

fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

/// Drops a `#` comment that is not inside a quoted string.
fn strip_comment(line: &str) -> &str {
    let mut quoted = false;
    for (i, c) in line.char_indices() {
        match c {
            '"' => quoted = !quoted,
            '#' if !quoted => return &line[..i],
            _ => {}
        }
    }
    line
}

/// `"F2"` or `["F2", "Primary+R"]` -> the strings inside the quotes.
fn parse_string_list(value: &str) -> Vec<String> {
    value
        .split('"')
        .enumerate()
        .filter(|(i, _)| i % 2 == 1)
        .map(|(_, s)| s.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_default_parses_and_every_command_has_a_key() {
        for mac in [false, true] {
            let map = KeyMap::defaults(mac);
            for cmd in Command::FILE_MANAGER {
                assert!(
                    !map.chords_for(cmd).is_empty(),
                    "{} unbound (mac {mac})",
                    cmd.name()
                );
            }
            let ex = KeyMap::explorer_defaults(mac);
            for cmd in Command::ALL {
                let bound = !ex.chords_for(cmd).is_empty();
                assert_eq!(bound, Command::EXPLORER.contains(&cmd), "{}", cmd.name());
            }
        }
    }

    #[test]
    fn the_explorer_keys_have_no_terminal_aliases() {
        // Ctrl+I is Tab, Ctrl+M is Enter and Ctrl+H is Backspace in a terminal.
        let ex = KeyMap::explorer_terminal();
        for c in ['i', 'm', 'h', 'j'] {
            let mods = Mods {
                ctrl: true,
                ..Mods::default()
            };
            assert_eq!(ex.lookup(Key::Char(c), mods), None, "Ctrl+{c}");
        }
        let ctrl = |c| {
            (
                Key::Char(c),
                Mods {
                    ctrl: true,
                    ..Mods::default()
                },
            )
        };
        let (k, m) = ctrl('f');
        assert_eq!(ex.lookup(k, m), Some(Command::Fsck));
        assert_eq!(
            ex.lookup(Key::Right, Mods::default()),
            Some(Command::Expand)
        );
        assert_eq!(
            ex.lookup(Key::Left, Mods::default()),
            Some(Command::Collapse)
        );
        assert_eq!(
            ex.lookup(Key::F(9), Mods::default()),
            Some(Command::ViewMenu)
        );
        assert_eq!(
            ex.lookup(Key::F(6), Mods::default()),
            None,
            "no second pane"
        );
    }

    #[test]
    fn the_agreed_file_manager_keys() {
        let pc = KeyMap::defaults(false);
        let none = Mods::default();
        let ctrl = Mods {
            ctrl: true,
            ..Mods::default()
        };
        assert_eq!(pc.lookup(Key::F(1), none), Some(Command::Help));
        assert_eq!(pc.lookup(Key::F(2), none), Some(Command::Rename));
        assert_eq!(pc.lookup(Key::F(3), none), Some(Command::Info));
        assert_eq!(pc.lookup(Key::Char('i'), ctrl), Some(Command::Info));
        assert_eq!(pc.lookup(Key::F(5), none), Some(Command::Refresh));
        assert_eq!(pc.lookup(Key::F(6), none), Some(Command::Copy));
        assert_eq!(pc.lookup(Key::F(7), none), Some(Command::Edit));
        assert_eq!(pc.lookup(Key::F(8), none), Some(Command::Delete));
        assert_eq!(pc.lookup(Key::Delete, none), Some(Command::Delete));
        assert_eq!(pc.lookup(Key::Char('n'), ctrl), Some(Command::NewFolder));
        assert_eq!(pc.lookup(Key::F(10), none), Some(Command::Apply));
        assert_eq!(pc.lookup(Key::F(9), none), Some(Command::ViewMenu));
        assert_eq!(pc.lookup(Key::Right, none), Some(Command::Expand));
        assert_eq!(pc.lookup(Key::Left, none), Some(Command::Collapse));
        assert_eq!(pc.lookup(Key::Backspace, none), Some(Command::Parent));
        assert_eq!(pc.lookup(Key::Escape, none), Some(Command::Close));
    }

    #[test]
    fn mac_uses_cmd_and_backspace_deletes() {
        let mac = KeyMap::defaults(true);
        let cmd = Mods {
            cmd: true,
            ..Mods::default()
        };
        let ctrl = Mods {
            ctrl: true,
            ..Mods::default()
        };
        assert_eq!(mac.lookup(Key::Char('n'), cmd), Some(Command::NewFolder));
        assert_eq!(
            mac.lookup(Key::Char('n'), ctrl),
            None,
            "Ctrl is not Primary on a Mac"
        );
        assert_eq!(
            mac.lookup(Key::Backspace, Mods::default()),
            Some(Command::Delete)
        );
        assert_eq!(mac.lookup(Key::Up, cmd), Some(Command::Parent));
        assert_eq!(mac.hint(Command::NewFolder), "Cmd+N");
        assert_eq!(KeyMap::defaults(false).hint(Command::NewFolder), "Ctrl+N");
    }

    #[test]
    fn the_source_and_window_commands_use_primary_letters() {
        let pc = KeyMap::defaults(false);
        let ctrl = |c| {
            (
                Key::Char(c),
                Mods {
                    ctrl: true,
                    ..Mods::default()
                },
            )
        };
        let cases = [
            ('o', Command::OpenImage),
            ('d', Command::OpenFolder),
            ('b', Command::OpenBackup),
            ('k', Command::QuickView),
            ('p', Command::Partition),
            ('s', Command::Sort),
            ('g', Command::Checksums),
            ('e', Command::Export),
            ('u', Command::Edits),
            ('l', Command::Log),
        ];
        for (c, cmd) in cases {
            let (k, m) = ctrl(c);
            assert_eq!(pc.lookup(k, m), Some(cmd), "Ctrl+{c}");
            assert_eq!(pc.lookup(k, Mods::default()), None, "a bare {c} stays free");
        }
        // Every chord means one command, so no new binding shadows an agreed one.
        for mac in [false, true] {
            let map = KeyMap::defaults(mac);
            for (i, (a, _)) in map.bindings.iter().enumerate() {
                for (b, _) in &map.bindings[i + 1..] {
                    let same = a.key == b.key && a.primary == b.primary && a.alt == b.alt;
                    let overlap = a.applies(mac) && b.applies(mac);
                    assert!(
                        !(same && a.shift == b.shift && overlap),
                        "{a:?} bound twice"
                    );
                }
            }
        }
    }

    #[test]
    fn quick_view_has_primary_k_and_connect_moved_to_primary_shift_k() {
        let ctrl_shift = Mods {
            ctrl: true,
            shift: true,
            ..Mods::default()
        };
        let pc = KeyMap::defaults(false);
        assert_eq!(
            pc.lookup(Key::Char('k'), ctrl_shift),
            Some(Command::Connect)
        );
        assert_eq!(pc.hint(Command::Connect), "Ctrl+Shift+K");
        let mac = KeyMap::defaults(true);
        assert_eq!(mac.hint(Command::QuickView), "Cmd+K");
        assert_eq!(mac.hint(Command::Connect), "Cmd+Shift+K");
        let term = KeyMap::for_terminal();
        let alt = Mods {
            alt: true,
            ..Mods::default()
        };
        assert_eq!(term.lookup(Key::Char('k'), alt), Some(Command::Connect));
        assert_eq!(term.hint(Command::Connect), "Alt+K");
        assert_eq!(term.hint(Command::QuickView), "Ctrl+K");
    }

    #[test]
    fn a_terminal_uses_ctrl_even_on_a_mac() {
        let term = KeyMap::for_terminal();
        let ctrl = Mods {
            ctrl: true,
            ..Mods::default()
        };
        assert_eq!(term.lookup(Key::Char('n'), ctrl), Some(Command::NewFolder));
        assert_eq!(term.hint(Command::OpenImage), "Ctrl+O");
    }

    #[test]
    fn extra_modifiers_do_not_match() {
        let pc = KeyMap::defaults(false);
        let shift = Mods {
            shift: true,
            ..Mods::default()
        };
        assert_eq!(pc.lookup(Key::F(2), shift), None);
    }

    #[test]
    fn overrides_replace_a_command_and_report_bad_lines() {
        let mut map = KeyMap::defaults(false);
        let problems = map.apply_overrides(
            "# mine\n[file-manager]\nrename = [\"F4\", \"Primary+R\"]  # two keys\napply = \"F11\"\nbogus = \"F3\"\nhelp = \"Hyper+F1\"\n",
        );
        assert_eq!(problems.len(), 2, "{problems:?}");
        let ctrl = Mods {
            ctrl: true,
            ..Mods::default()
        };
        assert_eq!(
            map.lookup(Key::F(4), Mods::default()),
            Some(Command::Rename)
        );
        assert_eq!(map.lookup(Key::Char('r'), ctrl), Some(Command::Rename));
        assert_eq!(map.lookup(Key::F(2), Mods::default()), None);
        assert_eq!(
            map.lookup(Key::F(11), Mods::default()),
            Some(Command::Apply)
        );
        assert_eq!(map.lookup(Key::F(10), Mods::default()), None);
        assert_eq!(
            map.lookup(Key::F(1), Mods::default()),
            Some(Command::Help),
            "a bad line keeps the default"
        );
    }
}
