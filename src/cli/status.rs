//! `--status` / `--status=SECONDS`: a periodic status line for long-running verbs.
//!
//! A verb that runs a long operation calls [`watch`] with a closure that
//! samples its progress (step, bytes done, bytes total). When `--status` is on,
//! a thread samples it four times a second, tracks the rate, and every interval
//! prints `status 1m 12s | Writing partition 2 | 45% | 1.2 GB of 2.6 GB, 35 MB/s, 40s left`
//! to stderr. The line comes from [`crate::model::job::progress_line`], so it
//! reads the same as the GUI's and the TUI's progress detail.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::model::job::{progress_line, Progress, Unit};
use crate::model::rate_tracker::RateTracker;

/// One sample of a verb's progress.
pub struct Sample {
    pub step: String,
    pub done: u64,
    /// 0 when the total is not known.
    pub total: u64,
    pub finished: bool,
}

/// Stops the status thread when the verb is done with it.
pub struct StatusGuard {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for StatusGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Print status lines while `sample` reports work; `None` when `--status` is off.
pub fn watch(sample: impl Fn() -> Option<Sample> + Send + 'static) -> Option<StatusGuard> {
    let interval = crate::cli::logging::effective()?.status_interval?;
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = Arc::clone(&stop);
    let thread = crate::model::worker::spawn_detached("status", move || {
        let started = Instant::now();
        let mut rate = RateTracker::default();
        let mut next = started + interval;
        while !stop_thread.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(250));
            let Some(s) = sample() else {
                continue;
            };
            rate.record(s.done, &s.step);
            if s.finished {
                break;
            }
            if Instant::now() >= next {
                next += interval;
                crate::cli::logging::status_stderr(line(&s, &rate, started.elapsed()));
            }
        }
        Ok(())
    });
    Some(StatusGuard {
        stop,
        thread: Some(thread),
    })
}

/// `status <elapsed> | <step> | <percent> | <detail>`.
fn line(s: &Sample, rate: &RateTracker, elapsed: Duration) -> String {
    let progress = if s.total > 0 {
        Progress::Determinate {
            done: s.done,
            total: s.total,
            unit: Unit::Bytes,
        }
    } else if s.done > 0 {
        Progress::Counting {
            done: s.done,
            unit: Unit::Bytes,
        }
    } else {
        Progress::Indeterminate
    };
    let eta = if s.total > 0 {
        rate.eta_secs(s.done, s.total)
    } else {
        None
    };
    let detail = progress_line(progress, rate.rate_bytes_per_sec(), eta, elapsed);
    let mut out = format!(
        "status {} | {}",
        crate::model::rate_tracker::format_eta(elapsed.as_secs()),
        if s.step.is_empty() {
            "working"
        } else {
            &s.step
        }
    );
    if let Some(f) = progress.fraction() {
        out.push_str(&format!(" | {:.0}%", f * 100.0));
    }
    out.push_str(" | ");
    out.push_str(&detail);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_status_line_reads_step_percent_and_amounts() {
        let s = Sample {
            step: "Writing partition 2".into(),
            done: 1 << 20,
            total: 4 << 20,
            finished: false,
        };
        let l = line(&s, &RateTracker::default(), Duration::from_secs(72));
        assert!(
            l.starts_with("status 1m 12s | Writing partition 2 | 25% | "),
            "{l}"
        );
        assert!(l.ends_with("1m 12s elapsed"), "{l}");
        let unknown = Sample {
            step: String::new(),
            done: 0,
            total: 0,
            finished: false,
        };
        assert_eq!(
            line(&unknown, &RateTracker::default(), Duration::from_secs(3)),
            "status 3s | working | 3s elapsed"
        );
    }
}
