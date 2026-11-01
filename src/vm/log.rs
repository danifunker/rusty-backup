//! The app log every shell shows, and the global `log` sink that feeds it.
//!
//! [`LogStore`] is the log panel's content. [`capture`] is the `log::Log`
//! implementation installed at start-up: it mirrors records to stderr and
//! buffers them until the app view-model drains them into its store, so
//! `log::info!` from worker threads (the GHO reader's scan, for example) shows
//! in the UI.
//!
//! ## Why the wake is thread-gated
//!
//! `log()` can be re-entered from inside a shell's toolkit: egui's
//! `Context::end_pass` emits `log::warn!` (the debug-only id-instability check)
//! while holding its context lock for writing, and a repaint request from there
//! re-enters that lock on the same thread and deadlocks (a debug build panics
//! after 10 s with "Failed to acquire RwLock read"; a release build hangs).
//! Records logged on the UI thread are drained by the frame already in flight,
//! so only worker threads wake the shell.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Info,
    Warning,
    Error,
}

impl LogLevel {
    /// The fixed-width tag shown before each line.
    pub fn tag(self) -> &'static str {
        match self {
            LogLevel::Info => "INFO",
            LogLevel::Warning => "WARN",
            LogLevel::Error => "ERR ",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub level: LogLevel,
    pub message: String,
    /// Local wall-clock time, `HH:MM:SS`.
    pub timestamp: String,
}

impl LogEntry {
    /// The line as the panel shows it and Copy Log copies it.
    pub fn line(&self) -> String {
        format!("{} [{}] {}", self.timestamp, self.level.tag(), self.message)
    }
}

/// The log panel's content, oldest first.
#[derive(Debug, Clone, Default)]
pub struct LogStore {
    entries: Vec<LogEntry>,
}

impl LogStore {
    pub fn add(&mut self, level: LogLevel, message: impl Into<String>) {
        self.entries.push(LogEntry {
            level,
            message: message.into(),
            timestamp: chrono::Local::now().format("%H:%M:%S").to_string(),
        });
    }

    pub fn info(&mut self, message: impl Into<String>) {
        self.add(LogLevel::Info, message);
    }

    pub fn warn(&mut self, message: impl Into<String>) {
        self.add(LogLevel::Warning, message);
    }

    pub fn error(&mut self, message: impl Into<String>) {
        self.add(LogLevel::Error, message);
    }

    pub fn entries(&self) -> &[LogEntry] {
        &self.entries
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Every line joined with newlines, as Copy Log puts it on the clipboard.
    pub fn to_text(&self) -> String {
        self.entries
            .iter()
            .map(LogEntry::line)
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Append records taken from [`capture::drain`].
    pub fn extend_from_capture(&mut self, records: Vec<(log::Level, String)>) {
        for (level, msg) in records {
            match level {
                log::Level::Error => self.error(msg),
                log::Level::Warn => self.warn(msg),
                _ => self.info(msg),
            }
        }
    }
}

/// The global `log` sink; see the module header for the wake gating.
pub mod capture {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::thread::ThreadId;

    use log::{Level, LevelFilter, Log, Metadata, Record};

    use crate::model::job::Notify;

    static BUFFER: OnceLock<Mutex<VecDeque<(Level, String)>>> = OnceLock::new();

    /// The shell's wake handle and the UI thread it was registered on.
    static WAKE: OnceLock<(Arc<dyn Notify>, ThreadId)> = OnceLock::new();

    /// Cap on undrained records, so a long scan while nothing draws can't grow without bound.
    const MAX_BUFFERED: usize = 8000;

    struct Capture {
        level: LevelFilter,
    }

    impl Log for Capture {
        fn enabled(&self, metadata: &Metadata) -> bool {
            metadata.level() <= self.level
        }

        fn log(&self, record: &Record) {
            if !self.enabled(record.metadata()) {
                return;
            }
            let msg = format!("{}", record.args());
            eprintln!("[{}] {}: {}", record.level(), record.target(), msg);
            if let Some(buf) = BUFFER.get() {
                if let Ok(mut q) = buf.lock() {
                    if q.len() >= MAX_BUFFERED {
                        q.pop_front();
                    }
                    q.push_back((record.level(), msg));
                }
            }
            if let Some((wake, ui_thread)) = WAKE.get() {
                if std::thread::current().id() != *ui_thread {
                    wake.wake();
                }
            }
        }

        fn flush(&self) {}
    }

    /// Install the capturing logger; the level is a bare `RUST_LOG` level name, default `info`.
    pub fn init() {
        let level = std::env::var("RUST_LOG")
            .ok()
            .and_then(|s| s.trim().parse::<LevelFilter>().ok())
            .unwrap_or(LevelFilter::Info);
        let _ = BUFFER.set(Mutex::new(VecDeque::new()));
        if log::set_boxed_logger(Box::new(Capture { level })).is_ok() {
            log::set_max_level(level);
        }
    }

    /// Register the shell's wake handle; call once from the UI thread.
    pub fn set_wake(wake: Arc<dyn Notify>) {
        let _ = WAKE.set((wake, std::thread::current().id()));
    }

    /// Take the buffered records, oldest first; empty if the logger was never installed.
    pub fn drain() -> Vec<(Level, String)> {
        match BUFFER.get().and_then(|b| b.lock().ok()) {
            Some(mut q) => q.drain(..).collect(),
            None => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lines_carry_time_tag_and_message() {
        let mut log = LogStore::default();
        log.info("Opening disk.img");
        log.error("bad sector");
        let text = log.to_text();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].ends_with(" [INFO] Opening disk.img"));
        assert!(lines[1].ends_with(" [ERR ] bad sector"));
        assert_eq!(lines[0].len(), "HH:MM:SS [INFO] Opening disk.img".len());
    }

    #[test]
    fn captured_records_map_onto_the_three_levels() {
        let mut log = LogStore::default();
        log.extend_from_capture(vec![
            (::log::Level::Error, "e".into()),
            (::log::Level::Warn, "w".into()),
            (::log::Level::Debug, "d".into()),
        ]);
        let levels: Vec<LogLevel> = log.entries().iter().map(|e| e.level).collect();
        assert_eq!(levels, [LogLevel::Error, LogLevel::Warning, LogLevel::Info]);
    }
}
