//! A `log::Log` implementation that both prints to stderr (preserving the
//! terminal output `env_logger` used to give us) and buffers records so the
//! GUI log panel can surface them. This is how `log::info!` calls from worker
//! threads — e.g. the GHO reader's lazy-scan progress in `rbformats::gho` —
//! become visible in the app's log panel instead of only on stderr.
//!
//! ## Why the repaint request is thread-gated
//!
//! `log()` can be re-entered from inside egui itself: `Context::end_pass`
//! emits `log::warn!` (the debug-only `warn_if_rect_changes_id` id-instability
//! check) while holding the context `RwLock` for writing. Calling
//! `Context::request_repaint()` from there re-enters `Context::read()` on the
//! same thread and self-deadlocks — a debug build panics after 10s with
//! "DEBUG PANIC: Failed to acquire RwLock read", a release build hangs
//! outright. Records logged on the UI thread are drained by the frame already
//! in flight, so only worker threads need the wake-up.

use std::collections::VecDeque;
use std::sync::{Mutex, OnceLock};
use std::thread::ThreadId;

use log::{Level, LevelFilter, Log, Metadata, Record};

static BUFFER: OnceLock<Mutex<VecDeque<(Level, String)>>> = OnceLock::new();

/// egui repaint handle plus the UI thread it was registered on, so a log call
/// from a worker thread can wake the UI. See the module header for the gating.
static REPAINT: OnceLock<(egui::Context, ThreadId)> = OnceLock::new();

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
        // Only a worker thread needs to wake the UI; requesting a repaint on
        // the UI thread can re-enter egui's context lock. See module header.
        if let Some((ctx, ui_thread)) = REPAINT.get() {
            if std::thread::current().id() != *ui_thread {
                ctx.request_repaint();
            }
        }
    }

    fn flush(&self) {}
}

/// Register the egui context so log calls from worker threads wake the UI.
/// Call once from the UI thread after the window is created.
pub fn set_repaint_ctx(ctx: egui::Context) {
    let _ = REPAINT.set((ctx, std::thread::current().id()));
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
