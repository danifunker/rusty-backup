//! A `log::Log` implementation that both prints to stderr (preserving the
//! terminal output `env_logger` used to give us) and buffers records so the
//! GUI log panel can surface them. This is how `log::info!` calls from worker
//! threads — e.g. the GHO reader's lazy-scan progress in `rbformats::gho` —
//! become visible in the app's log panel instead of only on stderr.

use std::collections::VecDeque;
use std::sync::{Mutex, OnceLock};

use log::{Level, LevelFilter, Log, Metadata, Record};

static BUFFER: OnceLock<Mutex<VecDeque<(Level, String)>>> = OnceLock::new();

/// egui repaint handle, set once the app window exists. Lets a log call from
/// any thread wake the UI so buffered records are drained promptly instead of
/// waiting for the next user interaction.
static REPAINT: OnceLock<egui::Context> = OnceLock::new();

/// Cap on buffered-but-undrained records, so a long scan that runs while no
/// frame is painted can't grow the buffer without bound.
const MAX_BUFFERED: usize = 8000;

struct UiLogger {
    level: LevelFilter,
}

impl Log for UiLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let msg = format!("{}", record.args());
        // Keep the stderr stream so terminal/CI captures still work.
        eprintln!("[{}] {}: {}", record.level(), record.target(), msg);
        if let Some(buf) = BUFFER.get() {
            if let Ok(mut q) = buf.lock() {
                if q.len() >= MAX_BUFFERED {
                    q.pop_front();
                }
                q.push_back((record.level(), msg));
            }
        }
        if let Some(ctx) = REPAINT.get() {
            ctx.request_repaint();
        }
    }

    fn flush(&self) {}
}

/// Register the egui context so log calls from worker threads wake the UI.
/// Call once after the window is created.
pub fn set_repaint_ctx(ctx: egui::Context) {
    let _ = REPAINT.set(ctx);
}

/// Install the UI-capturing logger as the global `log` sink. The level comes
/// from a bare `RUST_LOG` level name (`info`, `debug`, ...), defaulting to
/// `info`. Safe to call once at startup.
pub fn init() {
    let level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|s| s.trim().parse::<LevelFilter>().ok())
        .unwrap_or(LevelFilter::Info);
    let _ = BUFFER.set(Mutex::new(VecDeque::new()));
    if log::set_boxed_logger(Box::new(UiLogger { level })).is_ok() {
        log::set_max_level(level);
    }
}

/// Drain buffered records (oldest first) for the GUI to display. Returns an
/// empty vec if the logger was never installed.
pub fn drain() -> Vec<(Level, String)> {
    match BUFFER.get().and_then(|b| b.lock().ok()) {
        Some(mut q) => q.drain(..).collect(),
        None => Vec::new(),
    }
}
