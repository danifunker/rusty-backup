use fltk::{prelude::*, *};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogLevel {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone)]
struct LogEntry {
    level: LogLevel,
    message: String,
    timestamp: String,
}

/// Shared log state for thread-safe logging
#[derive(Clone)]
pub struct LogPanel {
    entries: Arc<Mutex<Vec<LogEntry>>>,
    display: text::TextDisplay,
    buffer: text::TextBuffer,
}

impl LogPanel {
    pub fn new(x: i32, y: i32, w: i32, h: i32) -> Self {
        let buffer = text::TextBuffer::default();
        let mut display = text::TextDisplay::new(x, y, w, h, None);
        display.set_buffer(buffer.clone());
        display.wrap_mode(text::WrapMode::AtBounds, 0);

        Self {
            entries: Arc::new(Mutex::new(Vec::new())),
            display,
            buffer,
        }
    }

    pub fn add(&self, level: LogLevel, message: impl Into<String>) {
        let now = chrono::Local::now().format("%H:%M:%S").to_string();
        let entry = LogEntry {
            level,
            message: message.into(),
            timestamp: now,
        };

        if let Ok(mut entries) = self.entries.lock() {
            entries.push(entry);
        }
    }

    pub fn info(&self, message: impl Into<String>) {
        self.add(LogLevel::Info, message);
    }

    pub fn warn(&self, message: impl Into<String>) {
        self.add(LogLevel::Warning, message);
    }

    pub fn error(&self, message: impl Into<String>) {
        self.add(LogLevel::Error, message);
    }

    /// Update the display with current log entries
    pub fn refresh(&mut self) {
        if let Ok(entries) = self.entries.lock() {
            let text: String = entries
                .iter()
                .map(|entry| {
                    let prefix = match entry.level {
                        LogLevel::Info => "INFO",
                        LogLevel::Warning => "WARN",
                        LogLevel::Error => "ERR ",
                    };
                    format!("{} [{}] {}", entry.timestamp, prefix, entry.message)
                })
                .collect::<Vec<_>>()
                .join("\n");

            self.buffer.set_text(&text);

            // Scroll to bottom
            let line_count = self.buffer.count_lines(0, self.buffer.length());
            self.display.scroll(line_count, 0);
        }
    }

    pub fn clear(&mut self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.clear();
        }
        self.buffer.set_text("");
    }

    pub fn get_text(&self) -> Option<String> {
        Some(self.buffer.text())
    }

    pub fn widget(&self) -> &text::TextDisplay {
        &self.display
    }

    pub fn widget_mut(&mut self) -> &mut text::TextDisplay {
        &mut self.display
    }
}
