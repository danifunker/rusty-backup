CRATE = "signal-hook"
TARGETS = ["signal-hook/src/iterator/backend.rs"]
GAP = """\
signal-hook's internal `AddSignal` trait takes an *arbitrary self type*:

    trait AddSignal: Debug + Send + Sync {
        fn add_signal(self: Arc<Self>, write: Arc<dyn SelfPipeWrite>, ..)
    }

and the one call site invokes it with method syntax on a trait object,
`Arc::clone(&self.pending).add_signal(..)`. mrustc does not consider an
`Arc<Self>` receiver when resolving a method on `Arc<dyn AddSignal>`:

    backend.rs:199:88 error:0: No applicable methods for
      {alloc::sync::Arc<dyn signal_hook::iterator::backend::AddSignal, ..>}.add_signal

Spelling the call as UFCS names the trait outright, so there is no receiver
autoderef to do and mrustc lowers it fine.

Not avoidable by dropping a feature -- crossterm's `events` needs
signal-hook-mio for SIGWINCH, and that genuinely imports
`signal_hook::iterator::backend`, so the module is load bearing.
"""
UPSTREAM = None

APPLIED = r"AddSignal::add_signal\(Arc::clone\(&self\.pending\)"
MATCH = r"Arc::clone\(&self\.pending\)\.add_signal\("

OLD = "Arc::clone(&self.pending).add_signal(Arc::clone(&self.write), signal)"
NEW = "AddSignal::add_signal(Arc::clone(&self.pending), Arc::clone(&self.write), signal)"


def patch(text, path):
    return text.replace(OLD, NEW, 1)
