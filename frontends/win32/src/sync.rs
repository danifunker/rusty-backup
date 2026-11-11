//! A guard against the change notifications Win32 sends for the shell's own sets.

use std::cell::Cell;

/// While active, `WM_COMMAND` / `WM_NOTIFY` handlers return early: the change came from `refresh`.
#[derive(Default)]
pub struct SyncGuard {
    depth: Cell<u32>,
}

impl SyncGuard {
    pub fn run<R>(&self, f: impl FnOnce() -> R) -> R {
        self.depth.set(self.depth.get() + 1);
        let r = f();
        self.depth.set(self.depth.get() - 1);
        r
    }

    pub fn active(&self) -> bool {
        self.depth.get() > 0
    }
}
