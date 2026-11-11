//! The TUI driver: runs `rb-cli tui` in a pseudo-terminal, types keys and reads the screen.
//!
//! The screen model covers what ratatui and crossterm emit (cursor moves, erase, SGR,
//! private modes, OSC strings) and nothing more; see `regression-tests/FLOWS.md`.
//! Unix runs it behind `openpty`, Windows behind ConPTY (Windows 10 1809 or later).

#[cfg(any(unix, windows))]
pub use imp::Session;

/// One cell: its character and whether it is drawn in reverse video (the TUI's selection).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cell {
    pub ch: char,
    pub reverse: bool,
}

const BLANK: Cell = Cell {
    ch: ' ',
    reverse: false,
};

/// A terminal screen fed raw output bytes.
pub struct Screen {
    rows: usize,
    cols: usize,
    cells: Vec<Vec<Cell>>,
    row: usize,
    col: usize,
    saved: (usize, usize),
    reverse: bool,
    state: Parse,
    params: String,
    utf8: Vec<u8>,
    /// Replies the terminal owes the program (cursor position reports).
    pub replies: Vec<u8>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Parse {
    Ground,
    Esc,
    Csi,
    Osc,
    OscEsc,
    Charset,
}

impl Screen {
    pub fn new(rows: usize, cols: usize) -> Self {
        Screen {
            rows,
            cols,
            cells: vec![vec![BLANK; cols]; rows],
            row: 0,
            col: 0,
            saved: (0, 0),
            reverse: false,
            state: Parse::Ground,
            params: String::new(),
            utf8: Vec::new(),
            replies: Vec::new(),
        }
    }

    /// The screen as text, one line per row, trailing spaces trimmed.
    pub fn text(&self) -> String {
        let mut out = String::new();
        for r in 0..self.rows {
            out.push_str(&self.line(r));
            out.push('\n');
        }
        out
    }

    /// The screen as text with each row's reverse-video text noted beside it, for failure bundles.
    pub fn marked(&self) -> String {
        let mut out = String::new();
        for r in 0..self.rows {
            let rev = self.reverse_text(r);
            out.push_str(&self.line(r));
            if !rev.is_empty() {
                out.push_str(&format!("    [rev: {}]", rev));
            }
            out.push('\n');
        }
        out
    }

    pub fn line(&self, r: usize) -> String {
        let s: String = self.cells[r].iter().map(|c| c.ch).collect();
        s.trim_end().to_string()
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    /// The text of row `r`'s reverse-video cells, joined, or empty.
    pub fn reverse_text(&self, r: usize) -> String {
        let s: String = self.cells[r]
            .iter()
            .filter(|c| c.reverse)
            .map(|c| c.ch)
            .collect();
        s.trim().to_string()
    }

    pub fn feed(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.byte(b);
        }
    }

    fn byte(&mut self, b: u8) {
        match self.state {
            Parse::Ground => self.ground(b),
            Parse::Esc => self.esc(b),
            Parse::Csi => {
                if (0x40..=0x7e).contains(&b) {
                    self.csi(b as char);
                    self.state = Parse::Ground;
                } else {
                    self.params.push(b as char);
                }
            }
            Parse::Osc => match b {
                0x07 => self.state = Parse::Ground,
                0x1b => self.state = Parse::OscEsc,
                _ => {}
            },
            Parse::OscEsc => self.state = Parse::Ground,
            Parse::Charset => self.state = Parse::Ground,
        }
    }

    fn ground(&mut self, b: u8) {
        if !self.utf8.is_empty() || b >= 0x80 {
            self.utf8.push(b);
            match std::str::from_utf8(&self.utf8) {
                Ok(s) => {
                    let ch = s.chars().next().unwrap_or(' ');
                    self.utf8.clear();
                    self.put(ch);
                }
                Err(e) if e.error_len().is_some() || self.utf8.len() > 4 => {
                    self.utf8.clear();
                    self.put('?');
                }
                Err(_) => {}
            }
            return;
        }
        match b {
            0x1b => self.state = Parse::Esc,
            b'\r' => self.col = 0,
            b'\n' | 0x0b | 0x0c => self.linefeed(),
            0x08 => self.col = self.col.saturating_sub(1),
            b'\t' => self.col = ((self.col / 8) + 1) * 8,
            0x20..=0x7e => self.put(b as char),
            _ => {}
        }
        if self.col > self.cols {
            self.col = self.cols;
        }
    }

    fn esc(&mut self, b: u8) {
        self.state = Parse::Ground;
        match b {
            b'[' => {
                self.params.clear();
                self.state = Parse::Csi;
            }
            b']' => self.state = Parse::Osc,
            b'(' | b')' | b'*' | b'+' => self.state = Parse::Charset,
            b'7' => self.saved = (self.row, self.col),
            b'8' => {
                self.row = self.saved.0;
                self.col = self.saved.1;
            }
            b'D' => self.linefeed(),
            b'M' => self.row = self.row.saturating_sub(1),
            b'c' => *self = Screen::new(self.rows, self.cols),
            _ => {}
        }
    }

    fn put(&mut self, ch: char) {
        if self.col >= self.cols {
            self.col = 0;
            self.linefeed();
        }
        self.cells[self.row][self.col] = Cell {
            ch,
            reverse: self.reverse,
        };
        self.col += 1;
    }

    fn linefeed(&mut self) {
        if self.row + 1 >= self.rows {
            self.cells.remove(0);
            self.cells.push(vec![BLANK; self.cols]);
        } else {
            self.row += 1;
        }
    }

    fn nums(&self) -> Vec<usize> {
        self.params
            .trim_start_matches(['?', '>', '='])
            .split(';')
            .map(|p| p.parse().unwrap_or(0))
            .collect()
    }

    fn csi(&mut self, fin: char) {
        let n = self.nums();
        let first = |d: usize| match n.first() {
            Some(&v) if v > 0 => v,
            _ => d,
        };
        let private = self.params.starts_with('?');
        match fin {
            'H' | 'f' => {
                self.row = first(1).min(self.rows) - 1;
                let c = match n.get(1) {
                    Some(&v) if v > 0 => v,
                    _ => 1,
                };
                self.col = c.min(self.cols) - 1;
            }
            'A' => self.row = self.row.saturating_sub(first(1)),
            'B' => self.row = (self.row + first(1)).min(self.rows - 1),
            'C' => self.col = (self.col + first(1)).min(self.cols - 1),
            'D' => self.col = self.col.saturating_sub(first(1)),
            'G' => self.col = first(1).min(self.cols) - 1,
            'd' => self.row = first(1).min(self.rows) - 1,
            'J' => {
                let mode = n.first().copied().unwrap_or(0);
                let (r, c) = (self.row, self.col);
                match mode {
                    0 => {
                        self.erase_line(r, c, self.cols);
                        for rr in r + 1..self.rows {
                            self.erase_line(rr, 0, self.cols);
                        }
                    }
                    1 => {
                        for rr in 0..r {
                            self.erase_line(rr, 0, self.cols);
                        }
                        self.erase_line(r, 0, c + 1);
                    }
                    _ => {
                        for rr in 0..self.rows {
                            self.erase_line(rr, 0, self.cols);
                        }
                    }
                }
            }
            'K' => {
                let (r, c) = (self.row, self.col);
                match n.first().copied().unwrap_or(0) {
                    0 => self.erase_line(r, c, self.cols),
                    1 => self.erase_line(r, 0, c + 1),
                    _ => self.erase_line(r, 0, self.cols),
                }
            }
            'X' => {
                let (r, c) = (self.row, self.col);
                self.erase_line(r, c, c + first(1));
            }
            'm' => self.sgr(&n),
            'n' if !private && n.first() == Some(&6) => {
                let reply = format!("\x1b[{};{}R", self.row + 1, self.col + 1);
                self.replies.extend_from_slice(reply.as_bytes());
            }
            's' => self.saved = (self.row, self.col),
            'u' => {
                self.row = self.saved.0;
                self.col = self.saved.1;
            }
            _ => {}
        }
    }

    fn erase_line(&mut self, r: usize, from: usize, to: usize) {
        let to = to.min(self.cols);
        for c in from.min(to)..to {
            self.cells[r][c] = BLANK;
        }
    }

    fn sgr(&mut self, n: &[usize]) {
        if self.params.is_empty() {
            self.reverse = false;
            return;
        }
        let mut i = 0;
        while i < n.len() {
            match n[i] {
                0 => self.reverse = false,
                7 => self.reverse = true,
                27 => self.reverse = false,
                // Extended colours carry their own arguments, which must not read as attributes.
                38 | 48 | 58 => {
                    i += if n.get(i + 1) == Some(&5) { 2 } else { 4 };
                }
                _ => {}
            }
            i += 1;
        }
    }
}

/// The bytes a terminal sends for a named key, or `None` for an unknown name.
pub fn key_bytes(name: &str) -> Option<Vec<u8>> {
    let fixed: &[u8] = match name {
        "Enter" => b"\r",
        "Tab" => b"\t",
        "BackTab" | "Shift+Tab" => b"\x1b[Z",
        "Esc" | "Escape" => b"\x1b",
        "Backspace" => b"\x7f",
        "Delete" => b"\x1b[3~",
        "Insert" => b"\x1b[2~",
        "Space" => b" ",
        "Up" => b"\x1b[A",
        "Down" => b"\x1b[B",
        "Right" => b"\x1b[C",
        "Left" => b"\x1b[D",
        "Home" => b"\x1b[H",
        "End" => b"\x1b[F",
        "PageUp" => b"\x1b[5~",
        "PageDown" => b"\x1b[6~",
        "F1" => b"\x1bOP",
        "F2" => b"\x1bOQ",
        "F3" => b"\x1bOR",
        "F4" => b"\x1bOS",
        "F5" => b"\x1b[15~",
        "F6" => b"\x1b[17~",
        "F7" => b"\x1b[18~",
        "F8" => b"\x1b[19~",
        "F9" => b"\x1b[20~",
        "F10" => b"\x1b[21~",
        "F11" => b"\x1b[23~",
        "F12" => b"\x1b[24~",
        _ => {
            if let Some(rest) = name.strip_prefix("Ctrl+") {
                let mut cs = rest.chars();
                if let (Some(c), None) = (cs.next(), cs.next()) {
                    if c.is_ascii_alphabetic() {
                        return Some(vec![(c.to_ascii_lowercase() as u8) - b'a' + 1]);
                    }
                }
                return None;
            }
            let mut cs = name.chars();
            if let (Some(c), None) = (cs.next(), cs.next()) {
                let mut buf = [0u8; 4];
                return Some(c.encode_utf8(&mut buf).as_bytes().to_vec());
            }
            return None;
        }
    };
    Some(fixed.to_vec())
}

#[cfg(unix)]
mod imp {
    use super::Screen;
    use std::fs::File;
    use std::io::{Read, Write};
    use std::os::raw::{c_char, c_int, c_ulong, c_ushort, c_void};
    use std::os::unix::io::FromRawFd;
    use std::os::unix::process::CommandExt;
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    #[repr(C)]
    struct WinSize {
        ws_row: c_ushort,
        ws_col: c_ushort,
        ws_xpixel: c_ushort,
        ws_ypixel: c_ushort,
    }

    #[cfg_attr(all(target_os = "linux", target_env = "gnu"), link(name = "util"))]
    extern "C" {
        fn openpty(
            amaster: *mut c_int,
            aslave: *mut c_int,
            name: *mut c_char,
            termp: *const c_void,
            winp: *const WinSize,
        ) -> c_int;
        fn setsid() -> c_int;
        fn ioctl(fd: c_int, request: c_ulong, ...) -> c_int;
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    const TIOCSCTTY: c_ulong = 0x540E;
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    const TIOCSCTTY: c_ulong = 0x2000_7461;

    /// A running `rb-cli tui` behind a pseudo-terminal.
    pub struct Session {
        child: Child,
        master: File,
        screen: Arc<Mutex<Screen>>,
        /// Everything the program wrote, for the failure bundle.
        pub transcript: Arc<Mutex<Vec<u8>>>,
    }

    impl Session {
        pub fn spawn(
            program: &Path,
            args: &[String],
            cwd: &Path,
            env: &[(String, String)],
            rows: u16,
            cols: u16,
        ) -> std::io::Result<Session> {
            let (mut m, mut s) = (-1, -1);
            let ws = WinSize {
                ws_row: rows,
                ws_col: cols,
                ws_xpixel: 0,
                ws_ypixel: 0,
            };
            // SAFETY: out-pointers to locals; null name and termios are allowed.
            let rc =
                unsafe { openpty(&mut m, &mut s, std::ptr::null_mut(), std::ptr::null(), &ws) };
            if rc != 0 {
                return Err(std::io::Error::last_os_error());
            }
            // SAFETY: openpty returned two fresh descriptors we now own.
            let master = unsafe { File::from_raw_fd(m) };
            let slave = unsafe { File::from_raw_fd(s) };

            let mut cmd = Command::new(program);
            cmd.args(args)
                .current_dir(cwd)
                .env("TERM", "xterm-256color")
                .env("COLUMNS", cols.to_string())
                .env("LINES", rows.to_string())
                .stdin(Stdio::from(slave.try_clone()?))
                .stdout(Stdio::from(slave.try_clone()?))
                .stderr(Stdio::from(slave));
            for (k, v) in env {
                cmd.env(k, v);
            }
            // SAFETY: only async-signal-safe calls between fork and exec.
            unsafe {
                cmd.pre_exec(|| {
                    if setsid() < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    if ioctl(0, TIOCSCTTY, 0 as c_int) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let child = cmd.spawn()?;

            let screen = Arc::new(Mutex::new(Screen::new(rows as usize, cols as usize)));
            let transcript = Arc::new(Mutex::new(Vec::new()));
            let mut reader = master.try_clone()?;
            let mut replier = master.try_clone()?;
            let (scr, tr) = (screen.clone(), transcript.clone());
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                loop {
                    let n = match reader.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    if let Ok(mut t) = tr.lock() {
                        t.extend_from_slice(&buf[..n]);
                    }
                    let reply = match scr.lock() {
                        Ok(mut s) => {
                            s.feed(&buf[..n]);
                            std::mem::take(&mut s.replies)
                        }
                        Err(_) => break,
                    };
                    if !reply.is_empty() {
                        let _ = replier.write_all(&reply);
                    }
                }
            });
            Ok(Session {
                child,
                master,
                screen,
                transcript,
            })
        }

        pub fn send(&mut self, bytes: &[u8]) -> std::io::Result<()> {
            self.master.write_all(bytes)?;
            self.master.flush()
        }

        /// Run `f` over the current screen.
        pub fn with_screen<T>(&self, f: impl FnOnce(&Screen) -> T) -> T {
            match self.screen.lock() {
                Ok(s) => f(&s),
                Err(p) => f(&p.into_inner()),
            }
        }

        pub fn text(&self) -> String {
            self.with_screen(|s| s.text())
        }

        /// Wait until the screen stops changing for `quiet`, or `limit` passes.
        pub fn settle(&self, quiet: Duration, limit: Duration) {
            let start = Instant::now();
            let mut last = self.text();
            let mut since = Instant::now();
            while start.elapsed() < limit {
                std::thread::sleep(Duration::from_millis(25));
                let now = self.text();
                if now != last {
                    last = now;
                    since = Instant::now();
                } else if since.elapsed() >= quiet {
                    return;
                }
            }
        }

        /// Poll until `pred` holds on the screen; false on timeout.
        pub fn wait_for(&self, limit: Duration, pred: impl Fn(&Screen) -> bool) -> bool {
            let start = Instant::now();
            loop {
                if self.with_screen(&pred) {
                    return true;
                }
                if start.elapsed() >= limit {
                    return false;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }

        pub fn exited(&mut self) -> bool {
            matches!(self.child.try_wait(), Ok(Some(_)))
        }
    }

    impl Drop for Session {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[cfg(windows)]
mod imp {
    use super::Screen;
    use std::ffi::OsStr;
    use std::fs::File;
    use std::io::{Read, Write};
    use std::os::raw::c_void;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::FromRawHandle;
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    type Handle = *mut c_void;

    #[repr(C)]
    struct Coord {
        x: i16,
        y: i16,
    }

    #[repr(C)]
    struct StartupInfoW {
        cb: u32,
        reserved: *mut u16,
        desktop: *mut u16,
        title: *mut u16,
        dims: [u32; 7],
        flags: u32,
        show_window: u16,
        cb_reserved2: u16,
        reserved2: *mut u8,
        std_input: Handle,
        std_output: Handle,
        std_error: Handle,
    }

    #[repr(C)]
    struct StartupInfoExW {
        info: StartupInfoW,
        attributes: *mut c_void,
    }

    #[repr(C)]
    struct ProcessInformation {
        process: Handle,
        thread: Handle,
        pid: u32,
        tid: u32,
    }

    #[link(name = "kernel32")]
    extern "system" {
        fn CreatePipe(
            read: *mut Handle,
            write: *mut Handle,
            attrs: *const c_void,
            size: u32,
        ) -> i32;
        fn CreatePseudoConsole(
            size: Coord,
            input: Handle,
            output: Handle,
            flags: u32,
            pc: *mut Handle,
        ) -> i32;
        fn ClosePseudoConsole(pc: Handle);
        fn InitializeProcThreadAttributeList(
            list: *mut c_void,
            count: u32,
            flags: u32,
            size: *mut usize,
        ) -> i32;
        fn UpdateProcThreadAttribute(
            list: *mut c_void,
            flags: u32,
            attribute: usize,
            value: *const c_void,
            size: usize,
            previous: *mut c_void,
            returned: *mut usize,
        ) -> i32;
        fn DeleteProcThreadAttributeList(list: *mut c_void);
        fn CreateProcessW(
            app: *const u16,
            cmdline: *mut u16,
            process_attrs: *const c_void,
            thread_attrs: *const c_void,
            inherit: i32,
            flags: u32,
            env: *const c_void,
            cwd: *const u16,
            startup: *const StartupInfoExW,
            info: *mut ProcessInformation,
        ) -> i32;
        fn WaitForSingleObject(handle: Handle, ms: u32) -> u32;
        fn TerminateProcess(handle: Handle, code: u32) -> i32;
        fn CloseHandle(handle: Handle) -> i32;
    }

    const PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE: usize = 0x0002_0016;
    const EXTENDED_STARTUPINFO_PRESENT: u32 = 0x0008_0000;
    const CREATE_UNICODE_ENVIRONMENT: u32 = 0x0000_0400;
    const STARTF_USESTDHANDLES: u32 = 0x0000_0100;
    const INVALID_HANDLE_VALUE: Handle = -1isize as Handle;

    fn wide(s: &OsStr) -> Vec<u16> {
        s.encode_wide().chain(Some(0)).collect()
    }

    /// Quote one argument the way `CommandLineToArgvW` reads it back.
    fn quote(arg: &str, out: &mut String) {
        if !arg.is_empty() && !arg.contains([' ', '\t', '"']) {
            out.push_str(arg);
            return;
        }
        out.push('"');
        let mut slashes = 0;
        for c in arg.chars() {
            match c {
                '\\' => {
                    slashes += 1;
                    continue;
                }
                '"' => out.extend(std::iter::repeat('\\').take(slashes * 2 + 1)),
                _ => out.extend(std::iter::repeat('\\').take(slashes)),
            }
            slashes = 0;
            out.push(c);
        }
        out.extend(std::iter::repeat('\\').take(slashes * 2));
        out.push('"');
    }

    /// The child's environment block: ours, with `set` replacing names case-insensitively.
    fn env_block(set: &[(String, String)]) -> Vec<u16> {
        let mut vars: std::collections::BTreeMap<String, (String, String)> = std::env::vars_os()
            .map(|(k, v)| {
                let k = k.to_string_lossy().into_owned();
                (k.to_uppercase(), (k, v.to_string_lossy().into_owned()))
            })
            .collect();
        for (k, v) in set {
            vars.insert(k.to_uppercase(), (k.clone(), v.clone()));
        }
        let mut out = Vec::new();
        for (k, v) in vars.values() {
            out.extend(OsStr::new(&format!("{}={}", k, v)).encode_wide());
            out.push(0);
        }
        out.push(0);
        out
    }

    /// A running `rb-cli tui` behind a pseudo console.
    pub struct Session {
        process: Handle,
        console: Handle,
        input: File,
        screen: Arc<Mutex<Screen>>,
        /// Everything the program wrote, for the failure bundle.
        pub transcript: Arc<Mutex<Vec<u8>>>,
    }

    impl Session {
        pub fn spawn(
            program: &Path,
            args: &[String],
            cwd: &Path,
            env: &[(String, String)],
            rows: u16,
            cols: u16,
        ) -> std::io::Result<Session> {
            let err = std::io::Error::last_os_error;
            let (mut in_read, mut in_write) = (std::ptr::null_mut(), std::ptr::null_mut());
            let (mut out_read, mut out_write) = (std::ptr::null_mut(), std::ptr::null_mut());
            // SAFETY: out-pointers to locals; no security attributes, default buffer size.
            unsafe {
                if CreatePipe(&mut in_read, &mut in_write, std::ptr::null(), 0) == 0 {
                    return Err(err());
                }
                if CreatePipe(&mut out_read, &mut out_write, std::ptr::null(), 0) == 0 {
                    CloseHandle(in_read);
                    CloseHandle(in_write);
                    return Err(err());
                }
            }
            // SAFETY: the pipe handles are fresh and owned here; the File closes them.
            let input = unsafe { File::from_raw_handle(in_write) };
            let output = unsafe { File::from_raw_handle(out_read) };
            let size = Coord {
                x: cols as i16,
                y: rows as i16,
            };
            let mut console = std::ptr::null_mut();
            // SAFETY: the console duplicates its ends of the pipes, so ours close right after.
            let hr = unsafe {
                let hr = CreatePseudoConsole(size, in_read, out_write, 0, &mut console);
                CloseHandle(in_read);
                CloseHandle(out_write);
                hr
            };
            if hr < 0 {
                return Err(std::io::Error::from_raw_os_error(hr));
            }

            // The reader runs before the child starts: a console with nobody reading stalls.
            let screen = Arc::new(Mutex::new(Screen::new(rows as usize, cols as usize)));
            let transcript = Arc::new(Mutex::new(Vec::new()));
            let mut reader = output;
            let mut replier = input.try_clone()?;
            let (scr, tr) = (screen.clone(), transcript.clone());
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                loop {
                    let n = match reader.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    if let Ok(mut t) = tr.lock() {
                        t.extend_from_slice(&buf[..n]);
                    }
                    let reply = match scr.lock() {
                        Ok(mut s) => {
                            s.feed(&buf[..n]);
                            std::mem::take(&mut s.replies)
                        }
                        Err(_) => break,
                    };
                    if !reply.is_empty() {
                        let _ = replier.write_all(&reply);
                    }
                }
            });

            let mut line = String::new();
            quote(&program.to_string_lossy(), &mut line);
            for a in args {
                line.push(' ');
                quote(a, &mut line);
            }
            let mut cmdline = wide(OsStr::new(&line));
            let mut all = vec![
                ("TERM".to_string(), "xterm-256color".to_string()),
                ("COLUMNS".to_string(), cols.to_string()),
                ("LINES".to_string(), rows.to_string()),
            ];
            all.extend(env.iter().cloned());
            let block = env_block(&all);
            let dir = wide(cwd.as_os_str());

            let mut bytes = 0usize;
            // SAFETY: a null list only asks for the size it needs.
            unsafe { InitializeProcThreadAttributeList(std::ptr::null_mut(), 1, 0, &mut bytes) };
            let mut list = vec![0u64; bytes.div_ceil(8)];
            let list_ptr = list.as_mut_ptr() as *mut c_void;
            // SAFETY: `list` is at least `bytes` long and outlives CreateProcessW below.
            unsafe {
                if InitializeProcThreadAttributeList(list_ptr, 1, 0, &mut bytes) == 0 {
                    ClosePseudoConsole(console);
                    return Err(err());
                }
                if UpdateProcThreadAttribute(
                    list_ptr,
                    0,
                    PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE,
                    console,
                    std::mem::size_of::<Handle>(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                ) == 0
                {
                    let e = err();
                    DeleteProcThreadAttributeList(list_ptr);
                    ClosePseudoConsole(console);
                    return Err(e);
                }
            }
            // Invalid std handles keep the child off our redirected stdio; it takes the console's.
            let startup = StartupInfoExW {
                info: StartupInfoW {
                    cb: std::mem::size_of::<StartupInfoExW>() as u32,
                    reserved: std::ptr::null_mut(),
                    desktop: std::ptr::null_mut(),
                    title: std::ptr::null_mut(),
                    dims: [0; 7],
                    flags: STARTF_USESTDHANDLES,
                    show_window: 0,
                    cb_reserved2: 0,
                    reserved2: std::ptr::null_mut(),
                    std_input: INVALID_HANDLE_VALUE,
                    std_output: INVALID_HANDLE_VALUE,
                    std_error: INVALID_HANDLE_VALUE,
                },
                attributes: list_ptr,
            };
            let mut info = ProcessInformation {
                process: std::ptr::null_mut(),
                thread: std::ptr::null_mut(),
                pid: 0,
                tid: 0,
            };
            // SAFETY: every pointer is to a live local; the command line buffer is mutable.
            let ok = unsafe {
                let ok = CreateProcessW(
                    std::ptr::null(),
                    cmdline.as_mut_ptr(),
                    std::ptr::null(),
                    std::ptr::null(),
                    0,
                    EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT,
                    block.as_ptr() as *const c_void,
                    dir.as_ptr(),
                    &startup,
                    &mut info,
                );
                DeleteProcThreadAttributeList(list_ptr);
                ok
            };
            if ok == 0 {
                let e = err();
                // SAFETY: the console is ours and nothing else holds it.
                unsafe { ClosePseudoConsole(console) };
                return Err(e);
            }
            // SAFETY: the thread handle is ours and never used.
            unsafe { CloseHandle(info.thread) };
            Ok(Session {
                process: info.process,
                console,
                input,
                screen,
                transcript,
            })
        }

        pub fn send(&mut self, bytes: &[u8]) -> std::io::Result<()> {
            self.input.write_all(bytes)?;
            self.input.flush()
        }

        /// Run `f` over the current screen.
        pub fn with_screen<T>(&self, f: impl FnOnce(&Screen) -> T) -> T {
            match self.screen.lock() {
                Ok(s) => f(&s),
                Err(p) => f(&p.into_inner()),
            }
        }

        pub fn text(&self) -> String {
            self.with_screen(|s| s.text())
        }

        /// Wait until the screen stops changing for `quiet`, or `limit` passes.
        pub fn settle(&self, quiet: Duration, limit: Duration) {
            let start = Instant::now();
            let mut last = self.text();
            let mut since = Instant::now();
            while start.elapsed() < limit {
                std::thread::sleep(Duration::from_millis(25));
                let now = self.text();
                if now != last {
                    last = now;
                    since = Instant::now();
                } else if since.elapsed() >= quiet {
                    return;
                }
            }
        }

        /// Poll until `pred` holds on the screen; false on timeout.
        pub fn wait_for(&self, limit: Duration, pred: impl Fn(&Screen) -> bool) -> bool {
            let start = Instant::now();
            loop {
                if self.with_screen(&pred) {
                    return true;
                }
                if start.elapsed() >= limit {
                    return false;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }

        pub fn exited(&mut self) -> bool {
            // SAFETY: the process handle is ours until drop.
            unsafe { WaitForSingleObject(self.process, 0) == 0 }
        }
    }

    impl Drop for Session {
        fn drop(&mut self) {
            // SAFETY: both handles are ours; closing the console ends the reader thread's read.
            unsafe {
                TerminateProcess(self.process, 1);
                WaitForSingleObject(self.process, 5000);
                CloseHandle(self.process);
                ClosePseudoConsole(self.console);
            }
        }
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn quoting() {
            let q = |a: &str| {
                let mut s = String::new();
                super::quote(a, &mut s);
                s
            };
            assert_eq!(q("tui"), "tui");
            assert_eq!(
                q(r"C:\Program Files\rb-cli.exe"),
                r#""C:\Program Files\rb-cli.exe""#
            );
            assert_eq!(q(r"a b\"), r#""a b\\""#);
            assert_eq!(q(r#"say "hi""#), r#""say \"hi\"""#);
            assert_eq!(q(r#"a\"b"#), r#""a\\\"b""#);
            assert_eq!(q(""), r#""""#);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_moves_and_erase() {
        let mut s = Screen::new(5, 20);
        s.feed(b"\x1b[2;3Hhello\x1b[1;1Htop\x1b[7mSEL\x1b[0m");
        assert_eq!(s.line(0), "topSEL");
        assert_eq!(s.line(1), "  hello");
        assert_eq!(s.reverse_text(0), "SEL");
        s.feed(b"\x1b[2;5H\x1b[K");
        assert_eq!(s.line(1), "  he");
        s.feed(b"\x1b[2J");
        assert_eq!(s.text().trim(), "");
    }

    #[test]
    fn extended_colour_is_not_reverse() {
        let mut s = Screen::new(2, 10);
        s.feed(b"\x1b[38;5;7mab\x1b[48;2;7;7;7mcd");
        assert_eq!(s.reverse_text(0), "");
    }

    #[test]
    fn utf8_and_osc_and_position_report() {
        let mut s = Screen::new(2, 10);
        s.feed("\x1b]0;title\x07\u{2502}x\x1b[6n".as_bytes());
        assert_eq!(s.line(0), "\u{2502}x");
        assert_eq!(s.replies, b"\x1b[1;3R".to_vec());
    }

    #[test]
    fn keys() {
        assert_eq!(key_bytes("Ctrl+K"), Some(vec![11]));
        assert_eq!(key_bytes("q"), Some(b"q".to_vec()));
        assert_eq!(key_bytes("F10"), Some(b"\x1b[21~".to_vec()));
        assert_eq!(key_bytes("Nope"), None);
    }
}
