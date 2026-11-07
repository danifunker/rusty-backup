//! Controller -> widget syncing without feedback: signal handlers skip while a sync runs.

use std::cell::Cell;

/// Set while a view copies controller state onto widgets; handlers check [`SyncGuard::active`].
#[derive(Default)]
pub struct SyncGuard(Cell<bool>);

impl SyncGuard {
    pub fn active(&self) -> bool {
        self.0.get()
    }

    /// Run `f` with handlers muted; a nested call leaves the guard on.
    pub fn run(&self, f: impl FnOnce()) {
        let was = self.0.replace(true);
        f();
        self.0.set(was);
    }
}
