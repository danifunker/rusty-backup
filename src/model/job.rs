//! One "wait" abstraction for every slow operation (`docs/native_frontends_plan.md` §4.1).
//!
//! A [`Job`] runs its body on a worker thread under a panic guard, so it always
//! ends in [`Status::Done`], [`Status::Failed`] or [`Status::Cancelled`]. The
//! body reports through a [`JobCtx`] (phase, progress, cancel checks); a shell
//! reads [`Job::snapshot`] to draw and is woken through its [`Notify`], so no
//! shell needs a per-frame timer. Progress wakes are throttled to about 10 Hz;
//! phase changes and the end always wake.
//!
//! A body that needs the user mid-run (an entry failed: skip it or stop?) calls [`JobCtx::ask`]
//! and waits; the controller sees [`Job::question`], puts it to its shell as an effect and
//! hands the answer back with [`Job::answer`]. A cancel ends the wait.
//!
//! A [`JobRegistry`] tracks which [`Resource`]s running jobs touch, refuses a
//! second job on a busy resource, and can say which job holds it.
//!
//! Plain std only: this compiles on Rust 1.73 and under mrustc.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::worker::lock_status;

/// What a determinate progress count measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    Bytes,
    Items,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    Indeterminate,
    Determinate {
        done: u64,
        total: u64,
        unit: Unit,
    },
    /// Work done so far when the total is unknown: shows a count and a rate, but no bar.
    Counting {
        done: u64,
        unit: Unit,
    },
}

impl Progress {
    /// Completed fraction in `0.0..=1.0`, or `None` when indeterminate.
    pub fn fraction(&self) -> Option<f32> {
        match *self {
            Progress::Indeterminate | Progress::Counting { .. } => None,
            Progress::Determinate { total: 0, .. } => Some(0.0),
            Progress::Determinate { done, total, .. } => {
                Some((done.min(total) as f64 / total as f64) as f32)
            }
        }
    }
}

/// Where a job is; the result itself is taken with [`Job::take_result`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Running,
    Done,
    Failed(String),
    Cancelled,
}

/// A cheap copy of a job's visible state, for drawing.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub label: String,
    pub phase: String,
    pub progress: Progress,
    pub can_cancel: bool,
    pub cancel_requested: bool,
    pub status: Status,
    /// Bumped on every visible change, so a shell can skip redrawing an unchanged job.
    pub generation: u64,
    /// Time since the job started.
    pub elapsed: Duration,
    /// Units (bytes or items) per second over the last few seconds, once there is enough to say.
    pub rate: Option<f64>,
    /// Seconds left at that rate, for a determinate job.
    pub eta_secs: Option<u64>,
}

impl Snapshot {
    pub fn is_running(&self) -> bool {
        self.status == Status::Running
    }

    /// One line every shell shows under its bar: amounts, speed, time left or elapsed.
    pub fn detail(&self) -> String {
        progress_line(self.progress, self.rate, self.eta_secs, self.elapsed)
    }
}

/// The detail line for a progress state: amounts, speed, and time left (known total) or elapsed.
pub fn progress_line(
    progress: Progress,
    rate: Option<f64>,
    eta_secs: Option<u64>,
    elapsed: Duration,
) -> String {
    use super::rate_tracker::{format_eta, format_rate};
    let amount = |n: u64, unit: Unit| match unit {
        Unit::Bytes => disk_core::display::format_size(n),
        Unit::Items => n.to_string(),
    };
    let speed = |unit: Unit| {
        rate.filter(|r| *r > 0.0).map(|r| match unit {
            Unit::Bytes => format!("{}/s", format_rate(r)),
            Unit::Items => format!("{r:.1}/s"),
        })
    };
    let elapsed = format_eta(elapsed.as_secs());
    let mut parts: Vec<String> = Vec::new();
    match progress {
        Progress::Determinate { done, total, unit } => {
            parts.push(format!("{} of {}", amount(done, unit), amount(total, unit)));
            parts.extend(speed(unit));
            match eta_secs {
                Some(eta) => parts.push(format!("{} left", format_eta(eta))),
                None => parts.push(format!("{elapsed} elapsed")),
            }
        }
        Progress::Counting { done, unit } => {
            parts.push(format!("{} so far", amount(done, unit)));
            parts.extend(speed(unit));
            parts.push(format!("{elapsed} elapsed"));
        }
        Progress::Indeterminate => parts.push(format!("{elapsed} elapsed")),
    }
    parts.join(", ")
}

/// A question a running job waits on; the controller that owns the job puts it to the user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Question {
    /// Tells this question from the job's earlier ones, so it is put to the user once.
    pub id: u64,
    /// Which question it is, for the controller (such as `model::extract::ASK_ENTRY_FAILED`).
    pub kind: &'static str,
    pub subject: String,
    pub detail: String,
}

/// Wakes a shell's UI thread; implemented once per shell (egui: `request_repaint`).
pub trait Notify: Send + Sync {
    fn wake(&self);
}

/// For callers that poll, such as the CLI and tests.
pub struct NoNotify;

impl Notify for NoNotify {
    fn wake(&self) {}
}

/// Something a job touches; two running jobs never hold the same one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Resource {
    Device(PathBuf),
    Image(PathBuf),
    Folder(PathBuf),
}

impl Resource {
    pub fn path(&self) -> &Path {
        match self {
            Resource::Device(p) | Resource::Image(p) | Resource::Folder(p) => p,
        }
    }
}

/// Error returned by [`JobCtx::check_cancel`]; a body that returns it ends `Cancelled`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cancelled;

impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("cancelled")
    }
}

impl std::error::Error for Cancelled {}

const PROGRESS_WAKE_INTERVAL: Duration = Duration::from_millis(100);

struct State<R> {
    snapshot: Snapshot,
    result: Option<R>,
    last_progress_wake: Option<Instant>,
    started: Instant,
    rate: super::rate_tracker::RateTracker,
    question: Option<Question>,
    /// The answer to the question with this id.
    answer: Option<(u64, usize)>,
    questions_asked: u64,
}

struct Shared<R> {
    state: Mutex<State<R>>,
    cancel: Arc<AtomicBool>,
    notify: Arc<dyn Notify>,
}

impl<R> Shared<R> {
    fn update(&self, wake: bool, f: impl FnOnce(&mut Snapshot)) {
        {
            let mut st = lock_status(&self.state);
            f(&mut st.snapshot);
            st.snapshot.generation += 1;
        }
        if wake {
            self.notify.wake();
        }
    }
}

/// Handed to a job's body: report phase and progress, and check for cancel.
pub struct JobCtx {
    shared: Arc<dyn CtxTarget>,
    cancel: Arc<AtomicBool>,
}

trait CtxTarget: Send + Sync {
    fn set_phase(&self, phase: String);
    fn set_progress(&self, progress: Progress);
    fn set_can_cancel(&self, can: bool);
    fn ask(&self, kind: &'static str, subject: String, detail: String) -> Option<usize>;
}

impl<R: Send> CtxTarget for Shared<R> {
    fn set_phase(&self, phase: String) {
        self.update(true, |s| s.phase = phase);
    }

    fn set_progress(&self, progress: Progress) {
        let wake = {
            let mut st = lock_status(&self.state);
            if st.snapshot.progress == progress {
                return;
            }
            st.snapshot.progress = progress;
            st.snapshot.generation += 1;
            let done = match progress {
                Progress::Determinate { done, .. } | Progress::Counting { done, .. } => Some(done),
                Progress::Indeterminate => None,
            };
            if let Some(done) = done {
                let phase = st.snapshot.phase.clone();
                st.rate.record(done, &phase);
                st.snapshot.rate = st.rate.rate_bytes_per_sec();
                st.snapshot.eta_secs = match progress {
                    Progress::Determinate { done, total, .. } => st.rate.eta_secs(done, total),
                    _ => None,
                };
            }
            let now = Instant::now();
            let due = match st.last_progress_wake {
                None => true,
                Some(t) => now.duration_since(t) >= PROGRESS_WAKE_INTERVAL,
            };
            if due {
                st.last_progress_wake = Some(now);
            }
            due
        };
        if wake {
            self.notify.wake();
        }
    }

    fn set_can_cancel(&self, can: bool) {
        self.update(true, |s| s.can_cancel = can);
    }

    fn ask(&self, kind: &'static str, subject: String, detail: String) -> Option<usize> {
        let id = {
            let mut st = lock_status(&self.state);
            st.questions_asked += 1;
            let id = st.questions_asked;
            st.question = Some(Question {
                id,
                kind,
                subject,
                detail,
            });
            st.answer = None;
            st.snapshot.generation += 1;
            id
        };
        self.notify.wake();
        loop {
            {
                let mut st = lock_status(&self.state);
                let answered = st.answer.filter(|(a, _)| *a == id).map(|(_, c)| c);
                let cancelled = self.cancel.load(Ordering::Relaxed);
                if answered.is_some() || cancelled {
                    st.question = None;
                    st.answer = None;
                    st.snapshot.generation += 1;
                    return answered;
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl JobCtx {
    pub fn set_phase(&self, phase: impl Into<String>) {
        self.shared.set_phase(phase.into());
    }

    pub fn set_progress(&self, progress: Progress) {
        self.shared.set_progress(progress);
    }

    /// Shorthand for a determinate byte count.
    pub fn set_bytes(&self, done: u64, total: u64) {
        self.set_progress(Progress::Determinate {
            done,
            total,
            unit: Unit::Bytes,
        });
    }

    /// Bytes handled so far when the total isn't known: a count and a rate, no bar.
    pub fn set_bytes_so_far(&self, done: u64) {
        self.set_progress(Progress::Counting {
            done,
            unit: Unit::Bytes,
        });
    }

    /// Whether the shell offers Cancel; on by default, off for steps that must finish.
    pub fn set_can_cancel(&self, can: bool) {
        self.shared.set_can_cancel(can);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::Relaxed)
    }

    /// `Err(Cancelled)` once cancel was requested, for `ctx.check_cancel()?` in a loop.
    pub fn check_cancel(&self) -> Result<(), Cancelled> {
        if self.is_cancelled() {
            Err(Cancelled)
        } else {
            Ok(())
        }
    }

    /// Put a question to the user and wait: the index of the answer, `None` once cancelled.
    pub fn ask(
        &self,
        kind: &'static str,
        subject: impl Into<String>,
        detail: impl Into<String>,
    ) -> Option<usize> {
        self.shared.ask(kind, subject.into(), detail.into())
    }

    /// The cancel flag itself, for engine code that already takes an `Arc<AtomicBool>`.
    pub fn cancel_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancel)
    }
}

/// A running or finished slow operation producing `R`.
pub struct Job<R> {
    shared: Arc<Shared<R>>,
}

impl<R> Clone for Job<R> {
    fn clone(&self) -> Self {
        Job {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<R: Send + 'static> Job<R> {
    /// Run `body` on a new thread under the panic guard.
    pub fn spawn(
        label: impl Into<String>,
        notify: Arc<dyn Notify>,
        body: impl FnOnce(&JobCtx) -> anyhow::Result<R> + Send + 'static,
    ) -> Self {
        Self::spawn_with(label.into(), notify, Box::new(|| {}), body)
    }

    fn spawn_with(
        label: String,
        notify: Arc<dyn Notify>,
        on_end: Box<dyn FnOnce() + Send>,
        body: impl FnOnce(&JobCtx) -> anyhow::Result<R> + Send + 'static,
    ) -> Self {
        let cancel = Arc::new(AtomicBool::new(false));
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                snapshot: Snapshot {
                    label: label.clone(),
                    phase: String::new(),
                    progress: Progress::Indeterminate,
                    can_cancel: true,
                    cancel_requested: false,
                    status: Status::Running,
                    generation: 0,
                    elapsed: Duration::ZERO,
                    rate: None,
                    eta_secs: None,
                },
                result: None,
                last_progress_wake: None,
                started: Instant::now(),
                rate: super::rate_tracker::RateTracker::default(),
                question: None,
                answer: None,
                questions_asked: 0,
            }),
            cancel: Arc::clone(&cancel),
            notify,
        });
        let worker = Arc::clone(&shared);
        std::thread::spawn(move || {
            let ctx = JobCtx {
                shared: Arc::clone(&worker) as Arc<dyn CtxTarget>,
                cancel,
            };
            let outcome = catch(&label, || body(&ctx));
            let cancelled = ctx.is_cancelled();
            // Free resources first, so anyone who sees the end finds them free.
            on_end();
            {
                let mut st = lock_status(&worker.state);
                st.snapshot.elapsed = st.started.elapsed();
                st.snapshot.status = match outcome {
                    Ok(r) => {
                        st.result = Some(r);
                        Status::Done
                    }
                    Err(Failure::Cancelled) => Status::Cancelled,
                    Err(Failure::Error(_)) if cancelled => Status::Cancelled,
                    Err(Failure::Error(msg)) => Status::Failed(msg),
                };
                st.snapshot.generation += 1;
            }
            worker.notify.wake();
        });
        Job { shared }
    }
}

impl<R> Job<R> {
    pub fn snapshot(&self) -> Snapshot {
        let st = lock_status(&self.shared.state);
        let mut snap = st.snapshot.clone();
        if snap.status == Status::Running {
            snap.elapsed = st.started.elapsed();
        }
        snap
    }

    pub fn is_running(&self) -> bool {
        lock_status(&self.shared.state).snapshot.status == Status::Running
    }

    /// Ask the body to stop; it ends `Cancelled` once it notices.
    pub fn cancel(&self) {
        self.shared.cancel.store(true, Ordering::Relaxed);
        self.shared.update(true, |s| s.cancel_requested = true);
    }

    /// The question the body is waiting on, if any.
    pub fn question(&self) -> Option<Question> {
        lock_status(&self.shared.state).question.clone()
    }

    /// Answer question `id` with the index of a choice; a stale id is ignored.
    pub fn answer(&self, id: u64, choice: usize) {
        let mut st = lock_status(&self.shared.state);
        if st.question.as_ref().is_some_and(|q| q.id == id) {
            st.answer = Some((id, choice));
        }
    }

    /// The body's result, once: `Some` only after `Status::Done`.
    pub fn take_result(&self) -> Option<R> {
        lock_status(&self.shared.state).result.take()
    }

    /// Block until the job ends (CLI and tests); returns the final snapshot.
    pub fn wait(&self) -> Snapshot {
        loop {
            let s = self.snapshot();
            if !s.is_running() {
                return s;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

/// Why a guarded body did not produce a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Cancelled,
    Error(String),
}

/// Run `body`, turning an error or a panic into a [`Failure`]; the guard every job uses.
pub fn catch<R>(what: &str, body: impl FnOnce() -> anyhow::Result<R>) -> Result<R, Failure> {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(r)) => Ok(r),
        Ok(Err(e)) if e.downcast_ref::<Cancelled>().is_some() => Err(Failure::Cancelled),
        Ok(Err(e)) => Err(Failure::Error(format!("{e:#}"))),
        Err(payload) => Err(Failure::Error(format!(
            "{what} worker panicked: {}",
            panic_message(payload.as_ref())
        ))),
    }
}

pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Refused by [`JobRegistry::spawn`]: another job holds `resource`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Busy {
    pub resource: Resource,
    pub holder: String,
}

impl std::fmt::Display for Busy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} is in use by {}",
            self.resource.path().display(),
            self.holder
        )
    }
}

impl std::error::Error for Busy {}

struct Entry {
    id: u64,
    label: String,
    resources: Vec<Resource>,
}

/// The running jobs and the resources they hold; cheap to clone and share.
#[derive(Clone, Default)]
pub struct JobRegistry {
    entries: Arc<Mutex<Vec<Entry>>>,
    // AtomicUsize: 32-bit PowerPC has no 64-bit atomics.
    next_id: Arc<AtomicUsize>,
}

impl JobRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawn a job holding `resources`, or refuse if a running job holds one of them.
    pub fn spawn<R: Send + 'static>(
        &self,
        label: impl Into<String>,
        resources: Vec<Resource>,
        notify: Arc<dyn Notify>,
        body: impl FnOnce(&JobCtx) -> anyhow::Result<R> + Send + 'static,
    ) -> Result<Job<R>, Busy> {
        let label = label.into();
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) as u64;
        {
            let mut entries = lock_status(&self.entries);
            for r in &resources {
                if let Some(e) = entries.iter().find(|e| e.resources.contains(r)) {
                    return Err(Busy {
                        resource: r.clone(),
                        holder: e.label.clone(),
                    });
                }
            }
            entries.push(Entry {
                id,
                label: label.clone(),
                resources,
            });
        }
        let entries = Arc::clone(&self.entries);
        let on_end = Box::new(move || lock_status(&entries).retain(|e| e.id != id));
        Ok(Job::spawn_with(label, notify, on_end, body))
    }

    /// The label of the running job holding `resource`, if any.
    pub fn holder(&self, resource: &Resource) -> Option<String> {
        lock_status(&self.entries)
            .iter()
            .find(|e| e.resources.contains(resource))
            .map(|e| e.label.clone())
    }

    /// Labels of every running job, oldest first.
    pub fn running(&self) -> Vec<String> {
        lock_status(&self.entries)
            .iter()
            .map(|e| e.label.clone())
            .collect()
    }

    pub fn is_idle(&self) -> bool {
        lock_status(&self.entries).is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    struct CountWakes(AtomicUsize);

    impl Notify for CountWakes {
        fn wake(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn quiet() -> Arc<dyn Notify> {
        Arc::new(NoNotify)
    }

    #[test]
    fn a_body_that_returns_ends_done_with_its_result() {
        let job = Job::spawn("sum", quiet(), |_| Ok(2 + 2));
        assert_eq!(job.wait().status, Status::Done);
        assert_eq!(job.take_result(), Some(4));
        assert_eq!(job.take_result(), None);
    }

    fn wait_for_question<R>(job: &Job<R>) -> Question {
        let start = Instant::now();
        loop {
            if let Some(q) = job.question() {
                return q;
            }
            assert!(start.elapsed() < Duration::from_secs(10), "no question");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn a_question_waits_for_its_answer_and_a_cancel_ends_the_wait() {
        let job = Job::spawn("ask", quiet(), |ctx| {
            let first = ctx.ask("kind", "a", "why");
            let second = ctx.ask("kind", "b", "why");
            Ok((first, second))
        });
        let q = wait_for_question(&job);
        assert_eq!((q.kind, q.subject.as_str()), ("kind", "a"));
        // A stale id is ignored, so an answer can't land on the next question.
        job.answer(q.id + 1, 9);
        job.answer(q.id, 2);
        let q2 = wait_for_question(&job);
        let q2 = match q2.id == q.id {
            true => {
                std::thread::sleep(Duration::from_millis(50));
                wait_for_question(&job)
            }
            false => q2,
        };
        assert_eq!(q2.subject, "b");
        job.answer(q2.id, 0);
        assert_eq!(job.wait().status, Status::Done);
        assert_eq!(job.take_result(), Some((Some(2), Some(0))));
        assert_eq!(job.question(), None);

        let job = Job::spawn("ask", quiet(), |ctx| Ok(ctx.ask("kind", "a", "why")));
        wait_for_question(&job);
        job.cancel();
        job.wait();
        assert_eq!(job.question(), None);
    }

    #[test]
    fn a_panicking_body_ends_failed_with_the_panic_message() {
        let job: Job<()> = Job::spawn("probe", quiet(), |_| panic!("boom"));
        assert_eq!(
            job.wait().status,
            Status::Failed("probe worker panicked: boom".into())
        );
    }

    #[test]
    fn an_error_ends_failed_with_its_chain() {
        let job: Job<()> = Job::spawn("probe", quiet(), |_| {
            Err(anyhow::anyhow!("inner").context("outer"))
        });
        assert_eq!(job.wait().status, Status::Failed("outer: inner".into()));
    }

    #[test]
    fn cancel_ends_cancelled_whether_the_body_checks_or_just_errors() {
        let (go, wait) = mpsc::channel::<()>();
        let job: Job<()> = Job::spawn("loop", quiet(), move |ctx| {
            wait.recv().unwrap();
            ctx.check_cancel()?;
            Ok(())
        });
        job.cancel();
        assert!(job.snapshot().cancel_requested);
        go.send(()).unwrap();
        assert_eq!(job.wait().status, Status::Cancelled);

        let (go, wait) = mpsc::channel::<()>();
        let job: Job<()> = Job::spawn("io", quiet(), move |ctx| {
            wait.recv().unwrap();
            anyhow::ensure!(!ctx.cancel_flag().load(Ordering::Relaxed), "interrupted");
            Ok(())
        });
        job.cancel();
        go.send(()).unwrap();
        assert_eq!(job.wait().status, Status::Cancelled);
    }

    #[test]
    fn phase_and_progress_show_in_the_snapshot_and_wake_the_shell() {
        let wakes = Arc::new(CountWakes(AtomicUsize::new(0)));
        let (go, wait) = mpsc::channel::<()>();
        let (seen, check) = mpsc::channel::<()>();
        let job = Job::spawn("copy", wakes.clone() as Arc<dyn Notify>, move |ctx| {
            ctx.set_phase("Copying");
            ctx.set_bytes(512, 2048);
            seen.send(()).unwrap();
            wait.recv().unwrap();
            Ok(())
        });
        check.recv().unwrap();
        let s = job.snapshot();
        assert_eq!(s.phase, "Copying");
        assert_eq!(s.progress.fraction(), Some(0.25));
        assert!(s.generation >= 2);
        go.send(()).unwrap();
        job.wait();
        assert!(wakes.0.load(Ordering::Relaxed) >= 3);
    }

    #[test]
    fn the_detail_line_shows_amounts_and_time() {
        let mut snap = Job::spawn("x", quiet(), |_| Ok(())).wait();
        snap.elapsed = Duration::from_secs(75);
        assert_eq!(snap.detail(), "1m 15s elapsed");
        snap.progress = Progress::Determinate {
            done: 3,
            total: 10,
            unit: Unit::Items,
        };
        snap.rate = Some(2.0);
        snap.eta_secs = Some(4);
        assert_eq!(snap.detail(), "3 of 10, 2.0/s, 4s left");
        snap.progress = Progress::Counting {
            done: 3 << 20,
            unit: Unit::Bytes,
        };
        snap.rate = Some((1u64 << 20) as f64);
        assert_eq!(
            snap.detail(),
            format!(
                "{} so far, {}/s, 1m 15s elapsed",
                disk_core::display::format_size(3 << 20),
                crate::model::rate_tracker::format_rate((1u64 << 20) as f64)
            )
        );
    }

    #[test]
    fn progress_wakes_are_throttled() {
        let wakes = Arc::new(CountWakes(AtomicUsize::new(0)));
        let job = Job::spawn("tight", wakes.clone() as Arc<dyn Notify>, |ctx| {
            for i in 0..10_000u64 {
                ctx.set_bytes(i, 10_000);
            }
            Ok(())
        });
        job.wait();
        assert!(wakes.0.load(Ordering::Relaxed) < 100);
        assert_eq!(job.snapshot().progress.fraction(), Some(0.9999));
    }

    #[test]
    fn the_registry_refuses_a_busy_resource_and_frees_it_at_the_end() {
        let reg = JobRegistry::new();
        let disk = Resource::Image(PathBuf::from("/tmp/disk.img"));
        let (go, wait) = mpsc::channel::<()>();
        let job = reg
            .spawn("Backup", vec![disk.clone()], quiet(), move |_| {
                wait.recv().unwrap();
                Ok(())
            })
            .unwrap();
        assert_eq!(reg.holder(&disk), Some("Backup".into()));
        let refused = reg.spawn("Restore", vec![disk.clone()], quiet(), |_| Ok(()));
        assert_eq!(
            refused.err(),
            Some(Busy {
                resource: disk.clone(),
                holder: "Backup".into()
            })
        );
        go.send(()).unwrap();
        job.wait();
        assert!(reg.is_idle());
        assert!(reg
            .spawn("Restore", vec![disk], quiet(), |_| Ok(()))
            .is_ok());
    }

    #[test]
    fn a_panicking_job_still_frees_its_resources() {
        let reg = JobRegistry::new();
        let dev = Resource::Device(PathBuf::from("/dev/sdz"));
        let job: Job<()> = reg
            .spawn("Write", vec![dev.clone()], quiet(), |_| panic!("boom"))
            .unwrap();
        job.wait();
        assert_eq!(reg.holder(&dev), None);
    }
}
