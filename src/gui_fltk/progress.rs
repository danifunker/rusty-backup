use fltk::{prelude::*, *};
use std::sync::{Arc, Mutex};

/// Progress state for long-running operations
#[derive(Clone)]
pub struct ProgressState {
    pub current_bytes: Arc<Mutex<u64>>,
    pub total_bytes: Arc<Mutex<u64>>,
    pub full_size_bytes: Arc<Mutex<u64>>,
    pub operation: Arc<Mutex<String>>,
    pub active: Arc<Mutex<bool>>,
}

impl Default for ProgressState {
    fn default() -> Self {
        Self {
            current_bytes: Arc::new(Mutex::new(0)),
            total_bytes: Arc::new(Mutex::new(0)),
            full_size_bytes: Arc::new(Mutex::new(0)),
            operation: Arc::new(Mutex::new(String::new())),
            active: Arc::new(Mutex::new(false)),
        }
    }
}

impl ProgressState {
    pub fn set_active(&self, active: bool) {
        if let Ok(mut a) = self.active.lock() {
            *a = active;
        }
    }

    pub fn is_active(&self) -> bool {
        self.active.lock().map(|a| *a).unwrap_or(false)
    }

    pub fn set_operation(&self, op: impl Into<String>) {
        if let Ok(mut o) = self.operation.lock() {
            *o = op.into();
        }
    }

    pub fn update(&self, current: u64, total: u64) {
        if let Ok(mut c) = self.current_bytes.lock() {
            *c = current;
        }
        if let Ok(mut t) = self.total_bytes.lock() {
            *t = total;
        }
    }
}

#[derive(Clone)]
pub struct ProgressWidget {
    frame: frame::Frame,
    bar: misc::Progress,
    state: ProgressState,
}

impl ProgressWidget {
    pub fn new(x: i32, y: i32, w: i32, h: i32, state: ProgressState) -> Self {
        let frame = frame::Frame::new(x, y, w, h / 2, None);
        let mut bar = misc::Progress::new(x, y + h / 2, w, h / 2, None);
        bar.set_minimum(0.0);
        bar.set_maximum(100.0);
        bar.hide();

        Self { frame, bar, state }
    }

    pub fn refresh(&mut self) {
        let active = self.state.is_active();

        if !active {
            self.frame.hide();
            self.bar.hide();
            return;
        }

        self.frame.show();
        self.bar.show();

        let operation = self
            .state
            .operation
            .lock()
            .ok()
            .map(|o| o.clone())
            .unwrap_or_default();
        let current = self
            .state
            .current_bytes
            .lock()
            .ok()
            .map(|c| *c)
            .unwrap_or(0);
        let total = self.state.total_bytes.lock().ok().map(|t| *t).unwrap_or(0);
        let full_size = self
            .state
            .full_size_bytes
            .lock()
            .ok()
            .map(|f| *f)
            .unwrap_or(0);

        self.frame.set_label(&operation);

        if total > 0 {
            let fraction = (current as f64 / total as f64 * 100.0) as f64;
            self.bar.set_value(fraction);

            let text = if full_size > total {
                format!(
                    "{} / {} to image of {} total ({:.0}%)",
                    format_size(current),
                    format_size(total),
                    format_size(full_size),
                    fraction
                )
            } else {
                format!(
                    "{} / {} ({:.0}%)",
                    format_size(current),
                    format_size(total),
                    fraction
                )
            };
            self.bar.set_label(&text);
        } else {
            self.bar.set_label("Working...");
        }
    }
}

fn format_size(bytes: u64) -> String {
    rusty_backup::partition::format_size(bytes)
}
