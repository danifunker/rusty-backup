//! A job's readout as the screens show it.

use rusty_backup::controller::app::{Progress, Snapshot};

/// A job's one-line summary (what runs, its phase, how far) and its percentage when known.
pub fn job_line(s: &Snapshot) -> (String, Option<u32>) {
    let pct = match &s.progress {
        Progress::Determinate { done, total, .. } if *total > 0 => {
            Some((done.saturating_mul(100) / total).min(100) as u32)
        }
        _ => None,
    };
    let mut line = s.label.clone();
    if !s.phase.is_empty() {
        line.push_str(": ");
        line.push_str(&s.phase);
    }
    if let Some(p) = pct {
        line.push_str(&format!(" ({}%)", p));
    }
    (line, pct)
}
