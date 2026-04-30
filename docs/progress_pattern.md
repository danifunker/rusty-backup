# Progress / Log Channel Pattern

This document codifies how long-running operations report progress, log
messages, errors, and cancellation back to the GUI. It addresses §10 item 3
of [`codecleanup.md`](codecleanup.md).

## TL;DR

Two layers, on purpose:

1. **Leaf I/O functions** (anything in `rbformats/`, `backup::run_backup`,
   `restore::*`, `fs::partition_minimum_size`, `fs::resize_*`) take **callbacks**:
   `progress_cb`, `cancel_check`, `log_cb`. They never depend on a `Status` type.
2. **Runner / orchestrator functions** (everything in `model/*_runner.rs`, plus
   the per-tab worker spawns still inline in `gui/`) own
   **`Arc<Mutex<XxxStatus>>`**. They spawn the worker thread, install closures
   that translate callback arguments into `Status` field writes, and return the
   `Arc` to the GUI.
3. **GUI** polls the `Status` once per frame, drains `log_messages` into
   `LogPanel`, snapshots scalars for rendering, and clears the handle when
   `finished`.

This is the dominant pattern already. We considered migrating to typed `mpsc`
channels and **rejected** it (see "Why not mpsc?" below).

## Status struct conventions

All `Status` structs live in `src/model/status.rs` (or, for ones whose worker is
also in `model/`, alongside their runner — e.g. `min_size_runner::MinSizeStatus`,
`archive_edit::ArchiveEditProgress`).

Required fields:

| Field | Type | Meaning |
|---|---|---|
| `finished` | `bool` | Worker has stopped; GUI should clear the handle. |
| `error` | `Option<String>` | Set when the worker exited via `Err`. Display once and clear the handle. |

Conditional fields (use the standard names below — don't invent new ones):

| Field | Type | Use when |
|---|---|---|
| `cancel_requested` | `bool` | Cancellation is supported. GUI sets it; worker polls it via the cancel callback. |
| `current_bytes` / `total_bytes` | `u64` / `u64` | Numeric progress with a known total. |
| `current_step` | `String` | Phase label (e.g. "Reading partition table…"). Use *instead of* bytes when there's no meaningful denominator, or alongside bytes for a sub-phase tag. |
| `log_messages` | `Vec<String>` or `Vec<(Level, String)>` | Worker emits human-readable log lines. GUI drains each frame. |
| Result fields | typed | Populated only on success; the GUI consumes them when `finished && error.is_none()`. Examples: `partition_table`, `clone_report`, `cache_path`. |

Naming: `XxxStatus` for terminal worker state, `XxxProgress` only when the
struct is also exposed by name through callbacks (legacy: `BackupProgress`,
`RestoreProgress`, `ExtractionProgress`, `ArchiveEditProgress`). New runners
should default to `XxxStatus`.

## Lock discipline

- Worker holds the lock **only while writing one batch of fields**. Never run I/O
  or compute while holding the lock.
- The cancel-check callback locks, reads `cancel_requested`, and drops the lock —
  one-shot.
- The progress and log callbacks lock, write, drop. They are invoked from inside
  the leaf I/O loop, so keep the critical section tiny.
- GUI poll: `let Ok(mut s) = arc.lock() else { return; };` then `drain(..)` the
  log buffer, copy out scalars, check `finished`. Never hold the lock across
  rendering.

Concretely, the standard worker shape is:

```rust
let status = Arc::new(Mutex::new(XxxStatus { /* defaults */ }));

let status_thread = Arc::clone(&status);
std::thread::spawn(move || {
    let progress_status = Arc::clone(&status_thread);
    let cancel_status = Arc::clone(&status_thread);
    let log_status = Arc::clone(&status_thread);

    let result = leaf_io_function(
        /* … */,
        move |bytes_done| {
            if let Ok(mut s) = progress_status.lock() {
                s.current_bytes = bytes_done;
            }
        },
        move || cancel_status.lock().map(|s| s.cancel_requested).unwrap_or(false),
        move |msg| {
            if let Ok(mut s) = log_status.lock() {
                s.log_messages.push(msg.to_string());
            }
        },
    );

    if let Ok(mut s) = status_thread.lock() {
        s.finished = true;
        if let Err(e) = result {
            s.error = Some(format!("{e:#}"));
        }
    }
});

status
```

The matching GUI poll:

```rust
fn poll_xxx(&mut self, ctx: &mut TabContext) {
    let Some(arc) = self.xxx_status.as_ref().map(Arc::clone) else { return };
    let Ok(mut s) = arc.lock() else { return };
    for msg in s.log_messages.drain(..) {
        ctx.log.info(msg);
    }
    let progress = (s.current_bytes, s.total_bytes);
    let finished = s.finished;
    let error = s.error.take();
    drop(s);

    /* … render `progress`, surface `error`, etc. … */

    if finished {
        self.xxx_status = None;
    }
}
```

Use `mem::take` / `Option::take` when extracting result fields so the next poll
doesn't see them again.

## Why not mpsc?

Considered: a typed `mpsc::Sender<XxxEvent>` per worker, with the GUI draining a
receiver each frame.

Rejected because:

- **All leaf I/O already uses callbacks** (`rbformats::compress_partition`,
  `rbformats::export::*`, `backup::run_backup`, `restore::*`, `fs::resize_*`,
  `fs::partition_minimum_size`, …). Migrating to mpsc means either rewriting
  every leaf to take a `Sender` (loses CLI/test usability) or wrapping each one
  with a `Sender → callback` adapter — same code shape, more boilerplate.
- **Lock contention isn't a measured problem.** GUI polls at frame rate
  (~16 ms); workers write to the lock from inside an I/O loop where each
  callback fires every few MB. Critical sections are microseconds.
- **Status structs serve as a snapshot.** Many fields aren't event-shaped
  (e.g. `partition_min_sizes: HashMap<usize, u64>` accumulates gradually; mpsc
  would require an "upsert" event variant per such field).
- **mpsc adds a failure mode** (sender dropped → `SendError`) without removing
  any existing one (lock poisoning is rarer in practice and we already handle
  it with `if let Ok(s) = arc.lock()`).

The decision can be revisited if we ever:
- Need to multiplex one worker's events into multiple consumers (e.g. CLI +
  GUI + log file). The current pattern handles this by having multiple
  callbacks on the leaf function — works fine for now.
- Hit measurable contention. Profile first.

## Existing runners

These follow the convention and are the reference implementations:

- `model::min_size_runner` — `MinSizeStatus`. Phase strings, no bytes.
- `model::archive_edit` — `ArchiveEditProgress`. Bytes + cancel.
- `model::export_runner` — `ExportStatus`. Bytes + cancel + log + three start fns.
- `model::fsck_runner` — synchronous; no Status (returns `Result` directly).

Per-tab spawns still inline in `gui/` (acceptable, but should follow the
field-naming + lock-discipline rules above; promote to a `*_runner.rs` module
when convenient):

| Site | Status type |
|---|---|
| `gui/inspect_tab.rs` (disk inspect, seekable cache, block-cache scan) | `InspectStatus`, `CacheStatus`, `BlockCacheScan` |
| `gui/backup_tab.rs` (run_backup, VHD whole-disk export) | `BackupProgress`, `VhdExportStatus` |
| `gui/restore_tab.rs` (full restore, single-partition, new-disk) | `RestoreProgress` |
| `gui/resize_popup.rs` (in-place resize) | `ResizeStatus` |
| `gui/browse_view.rs` (file extraction) | `ExtractionProgress` |
| `gui/expand_hfs_dialog.rs` (HFS clone + APM emit) | `ExpandStatus` |
| `gui/bulk_convert_dialog.rs` (folder bulk-convert) | `BulkConvertStatus` |
| `gui/optical_tab.rs` (optical dump) | tab-internal types |

## When adding a new long-running operation

1. Define `XxxStatus` in `src/model/status.rs` with `finished` + `error` plus
   whatever standard fields apply. Keep all fields `pub`.
2. Either:
   - Put the spawn-and-poll glue in a new `model/xxx_runner.rs` (preferred), or
   - Inline the spawn in the GUI tab if the tab is the only consumer. Follow
     the worker-shape template above; resist inventing field names.
3. Leaf I/O takes callbacks, not `Arc<Mutex<…>>`. The runner is the bridge.
4. The GUI tab's `show()` calls `poll_xxx(ctx)` once per frame before rendering.

## Migration / lint targets

These are the concrete things this doc codifies (none currently violate it,
but listing for future regressions):

- A worker that writes to `LogPanel` directly via a captured reference instead
  of pushing to `log_messages` — would block the GUI thread on any contended
  lock, and breaks the "no GUI types in workers" rule.
- A leaf I/O function that takes `Arc<Mutex<XxxStatus>>` instead of callbacks —
  forces every caller (CLI tools, tests, future remote runners) to fabricate a
  Status just to get progress.
- A `Status` struct with non-`pub` fields. Workers writing to it from another
  module need full access; encapsulation isn't valuable here.
