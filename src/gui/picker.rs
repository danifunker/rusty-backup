//! Non-blocking file pickers: rfd's `AsyncFileDialog` futures, polled from the UI each frame.
//!
//! No thread of ours is involved. rfd drives each dialog itself: GTK3 on its own global GTK
//! thread, macOS as a sheet on the main window, Windows on a thread it spawns at the first poll.
//! The future's waker requests a repaint, so the answer shows on the next frame without waiting
//! for mouse movement. One picker is open at a time, app-wide: [`Gate`] refuses a second while
//! one is on screen, and the caller treats the refusal as a cancel.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

/// The app-wide "a picker is on screen" flag.
pub(crate) struct Gate(AtomicBool);

impl Gate {
    pub(crate) const fn new() -> Self {
        Gate(AtomicBool::new(false))
    }

    fn claim(&self) -> bool {
        self.0
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn release(&self) {
        self.0.store(false, Ordering::Release);
    }

    #[cfg(test)]
    fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

static GATE: Gate = Gate::new();

/// The waker: the first wake (the dialog answered) frees the gate, every wake asks for a frame.
struct Signal {
    gate: &'static Gate,
    open: AtomicBool,
    repaint: Box<dyn Fn() + Send + Sync>,
}

impl Signal {
    fn close(&self) {
        if self.open.swap(false, Ordering::AcqRel) {
            self.gate.release();
        }
    }
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.close();
        (self.repaint)();
    }
}

/// A dialog on screen whose answer has not been taken yet.
pub(crate) struct Pending<T> {
    future: Option<Pin<Box<dyn Future<Output = T> + Send>>>,
    answer: Option<T>,
    signal: Arc<Signal>,
}

impl<T> Pending<T> {
    /// Open the dialog `open` builds, unless `gate` says one is already up (then `open` never runs).
    pub(crate) fn start<F>(
        gate: &'static Gate,
        repaint: impl Fn() + Send + Sync + 'static,
        open: impl FnOnce() -> F,
    ) -> Option<Self>
    where
        F: Future<Output = T> + Send + 'static,
    {
        if !gate.claim() {
            return None;
        }
        let signal = Arc::new(Signal {
            gate,
            open: AtomicBool::new(true),
            repaint: Box::new(repaint),
        });
        let mut pending = Pending {
            future: Some(Box::pin(open())),
            answer: None,
            signal,
        };
        // Windows' backend spawns its dialog thread on the first poll, so poll at once.
        pending.step();
        Some(pending)
    }

    fn step(&mut self) {
        let Some(future) = self.future.as_mut() else {
            return;
        };
        let waker = Waker::from(self.signal.clone());
        if let Poll::Ready(answer) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
            self.future = None;
            self.answer = Some(answer);
            self.signal.close();
        }
    }

    /// The answer once the dialog has closed; `None` while it is still on screen.
    pub(crate) fn poll(&mut self) -> Option<T> {
        self.step();
        self.answer.take()
    }
}

impl<T> Drop for Pending<T> {
    fn drop(&mut self) {
        self.signal.close();
    }
}

/// What a single-path dialog asks for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Kind {
    Open,
    Save,
    Folder,
}

/// Open `dialog` for one path; `None` when another picker is already on screen.
pub(crate) fn path(
    ctx: &egui::Context,
    dialog: rfd::AsyncFileDialog,
    kind: Kind,
) -> Option<Pending<Option<PathBuf>>> {
    let ctx = ctx.clone();
    Pending::start(
        &GATE,
        move || ctx.request_repaint(),
        move || {
            type Handle = Pin<Box<dyn Future<Output = Option<rfd::FileHandle>> + Send>>;
            let handle: Handle = match kind {
                Kind::Open => Box::pin(dialog.pick_file()),
                Kind::Save => Box::pin(dialog.save_file()),
                Kind::Folder => Box::pin(dialog.pick_folder()),
            };
            async move { handle.await.map(PathBuf::from) }
        },
    )
}

/// Open `dialog` for several files; `None` when another picker is already on screen.
pub(crate) fn paths(
    ctx: &egui::Context,
    dialog: rfd::AsyncFileDialog,
) -> Option<Pending<Option<Vec<PathBuf>>>> {
    let ctx = ctx.clone();
    Pending::start(
        &GATE,
        move || ctx.request_repaint(),
        move || {
            let handles = dialog.pick_files();
            async move {
                let handles = handles.await?;
                Some(handles.into_iter().map(PathBuf::from).collect())
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    /// A dialog the test answers by hand, waking the stored waker as rfd's backends do.
    type Answer = Option<Option<PathBuf>>;

    #[derive(Clone, Default)]
    struct FakeDialog(Arc<Mutex<(Answer, Option<Waker>)>>);

    impl FakeDialog {
        fn answer(&self, path: Option<PathBuf>) {
            let waker = {
                let mut s = self.0.lock().unwrap();
                s.0 = Some(path);
                s.1.take()
            };
            if let Some(w) = waker {
                w.wake();
            }
        }
    }

    impl Future for FakeDialog {
        type Output = Option<PathBuf>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut s = self.0.lock().unwrap();
            match s.0.take() {
                Some(answer) => Poll::Ready(answer),
                None => {
                    s.1 = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        }
    }

    fn counter() -> (Arc<AtomicUsize>, impl Fn() + Send + Sync + 'static) {
        let n = Arc::new(AtomicUsize::new(0));
        let m = n.clone();
        (n, move || {
            m.fetch_add(1, Ordering::SeqCst);
        })
    }

    #[test]
    fn answer_arrives_on_a_later_poll_and_repaints() {
        static G: Gate = Gate::new();
        let dialog = FakeDialog::default();
        let (repaints, repaint) = counter();
        let d = dialog.clone();
        let mut p = Pending::start(&G, repaint, move || d).expect("gate free");
        assert!(G.is_open());
        assert_eq!(p.poll(), None);
        assert_eq!(repaints.load(Ordering::SeqCst), 0);
        dialog.answer(Some(PathBuf::from("/tmp/a.img")));
        assert_eq!(repaints.load(Ordering::SeqCst), 1);
        assert!(
            !G.is_open(),
            "the answer frees the gate before the view takes it"
        );
        assert_eq!(p.poll(), Some(Some(PathBuf::from("/tmp/a.img"))));
        assert_eq!(p.poll(), None, "an answer is handed out once");
    }

    #[test]
    fn cancel_answers_none() {
        static G: Gate = Gate::new();
        let dialog = FakeDialog::default();
        let d = dialog.clone();
        let mut p = Pending::start(&G, || {}, move || d).unwrap();
        dialog.answer(None);
        assert_eq!(p.poll(), Some(None));
    }

    #[test]
    fn second_picker_is_refused_without_opening() {
        static G: Gate = Gate::new();
        let first = Pending::start(&G, || {}, FakeDialog::default).unwrap();
        let mut opened = false;
        let second = Pending::start(
            &G,
            || {},
            || {
                opened = true;
                FakeDialog::default()
            },
        );
        assert!(second.is_none());
        assert!(!opened, "a refused picker never reaches rfd");
        drop(first);
        assert!(!G.is_open(), "dropping a pending picker frees the gate");
        assert!(Pending::start(&G, || {}, FakeDialog::default).is_some());
    }

    #[test]
    fn a_dialog_that_answers_at_once_is_ready_on_the_first_poll() {
        static G: Gate = Gate::new();
        let mut p = Pending::start(&G, || {}, || async { Some(PathBuf::from("/x")) }).unwrap();
        assert!(!G.is_open());
        assert_eq!(p.poll(), Some(Some(PathBuf::from("/x"))));
    }
}
