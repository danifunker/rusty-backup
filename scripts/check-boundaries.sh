#!/usr/bin/env bash
# Layer rules from docs/native_frontends_plan.md §4.
#
# Enforced (exit 1):
#   - src/controller has no toolkit code (egui, eframe, ratatui, crossterm, rfd, objc2, gtk, Win32 UI).
#   - src/controller spawns no raw threads; slow work goes through model::job::Job.
#   - shells (src/gui, the TUI, the optical browser) spawn no raw threads: use Job,
#     worker::spawn_guarded or worker::spawn_detached, which catch panics (Phase 1).
# Reported only (warn mode until Phase 2 makes the shells pure view):
#   - shells reaching past the view-models into the engine or model.
#
# Usage: scripts/check-boundaries.sh [-v]   (-v lists the files behind each count)
set -euo pipefail
cd "$(dirname "$0")/.."

VERBOSE=0
[ "${1:-}" = "-v" ] && VERBOSE=1

SHELLS=(src/gui src/cli/verbs/tui_app.rs src/optical/browse_view.rs)
ENGINE_MODS='fs|backup|restore|partition|rbformats|os|optical|remote|model'
SPLIT_CRATES='disk_core|fs_[a-z0-9_]+|dsk_[a-z0-9_]+|part_tables|arc_mac|qnx2fs'
TOOLKITS='egui|eframe|ratatui|crossterm|rfd|objc2[a-z0-9_]*|gtk4?|glib|libadwaita|windows::Win32::UI'

fail=0
warn() {
  if [ -n "${GITHUB_ACTIONS:-}" ]; then echo "::warning::$*"; else echo "warning: $*"; fi
}
error() {
  if [ -n "${GITHUB_ACTIONS:-}" ]; then echo "::error::$*"; else echo "error: $*"; fi
  fail=1
}

# --- enforced: src/controller stays toolkit-free and thread-free ---------------------
if hits=$(grep -rnE "\b($TOOLKITS)::|extern crate ($TOOLKITS)\b" src/controller); then
  error "src/controller uses toolkit code:"; echo "$hits"
fi
if hits=$(grep -rnE "thread::spawn|std::thread::Builder" src/controller); then
  error "src/controller spawns threads; use model::job::Job:"; echo "$hits"
fi

# --- reported: shells reaching past the view-models --------------------------
count_in_shells() {
  grep -rnoE "$1" "${SHELLS[@]}" 2>/dev/null || true
}
engine=$(count_in_shells "\b(rusty_backup|crate)::($ENGINE_MODS)\b|\b($SPLIT_CRATES)::")
spawns=$(count_in_shells "thread::spawn")
n_engine=$(printf '%s' "$engine" | grep -c . || true)
n_spawn=$(printf '%s' "$spawns" | grep -c . || true)
warn "shells reference the engine/model directly $n_engine times (target 0 after Phase 2)"
if [ "$n_spawn" -gt 0 ]; then
  error "shells call thread::spawn $n_spawn times; use Job, spawn_guarded or spawn_detached:"
  printf '%s\n' "$spawns"
fi
if [ "$VERBOSE" = 1 ]; then
  echo "--- engine/model references per file"
  printf '%s\n' "$engine" | cut -d: -f1 | sort | uniq -c | sort -rn
  echo "--- thread::spawn per file"
  printf '%s\n' "$spawns" | cut -d: -f1 | sort | uniq -c | sort -rn
fi

[ "$fail" = 0 ] && echo "boundaries: src/controller is clean and shells spawn no raw threads"
exit "$fail"
