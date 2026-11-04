//! `ShellTask`: one slow operation at a time for a shell that runs its own (the TUI's "Working"
//! box). The body runs as a [`Job`] off the shell's thread; a second start while one runs is
//! refused with [`BUSY`], and [`ShellTask::poll`] hands back how it ended once.

use std::sync::Arc;

use crate::model::job::{Job, Notify, Status};

pub use crate::model::job::{JobCtx, NoNotify, Snapshot};

/// Why a second task did not start.
pub const BUSY: &str = "Another operation is still running.";

/// How a task ended.
#[derive(Debug, PartialEq, Eq)]
pub enum TaskEnd<T> {
    Done(T),
    Failed { label: String, error: String },
    Cancelled { label: String },
}

/// At most one running job, woken through `notify`.
pub struct ShellTask<T> {
    job: Option<Job<T>>,
    notify: Arc<dyn Notify>,
}

impl<T: Send + 'static> ShellTask<T> {
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        ShellTask { job: None, notify }
    }

    /// Run `body` as a job, or say why not.
    pub fn start(
        &mut self,
        label: impl Into<String>,
        body: impl FnOnce(&JobCtx) -> anyhow::Result<T> + Send + 'static,
    ) -> Result<(), &'static str> {
        if self.job.is_some() {
            return Err(BUSY);
        }
        self.job = Some(Job::spawn(label, Arc::clone(&self.notify), body));
        Ok(())
    }

    /// A task was started and its end not yet taken.
    pub fn is_active(&self) -> bool {
        self.job.is_some()
    }

    pub fn is_running(&self) -> bool {
        self.job.as_ref().is_some_and(Job::is_running)
    }

    pub fn snapshot(&self) -> Option<Snapshot> {
        self.job.as_ref().map(Job::snapshot)
    }

    /// Ask the running task to stop, if it allows it.
    pub fn cancel(&self) {
        if let Some(job) = &self.job {
            if job.snapshot().can_cancel {
                job.cancel();
            }
        }
    }

    /// The task's end, once; `None` while it runs or when none was started.
    pub fn poll(&mut self) -> Option<TaskEnd<T>> {
        let snap = self.job.as_ref()?.snapshot();
        if snap.is_running() {
            return None;
        }
        let done = self.job.take().and_then(|j| j.take_result());
        Some(match (snap.status, done) {
            (Status::Done, Some(v)) => TaskEnd::Done(v),
            (Status::Failed(error), _) => TaskEnd::Failed {
                label: snap.label,
                error,
            },
            (Status::Done, None) => TaskEnd::Failed {
                label: snap.label,
                error: "no result".to_string(),
            },
            (Status::Cancelled, _) | (Status::Running, _) => {
                TaskEnd::Cancelled { label: snap.label }
            }
        })
    }

    /// Tests: block until the running task ends.
    pub fn wait(&self) {
        if let Some(job) = &self.job {
            job.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_task_at_a_time_and_each_end_is_reported_once() {
        let mut t = ShellTask::new(Arc::new(NoNotify));
        assert!(t.poll().is_none());
        t.start("Counting", |_| Ok(7)).unwrap();
        assert_eq!(t.start("Second", |_| Ok(8)), Err(BUSY));
        t.wait();
        assert_eq!(t.poll(), Some(TaskEnd::Done(7)));
        assert!(!t.is_active());
        assert!(t.poll().is_none());
        t.start("Exploding", |_| -> anyhow::Result<i32> { panic!("boom") })
            .unwrap();
        t.wait();
        match t.poll() {
            Some(TaskEnd::Failed { label, error }) => {
                assert_eq!(label, "Exploding");
                assert!(error.contains("boom"), "{error}");
            }
            other => panic!("{other:?}"),
        }
    }
}
