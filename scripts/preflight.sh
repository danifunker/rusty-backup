#!/usr/bin/env bash
# Run what CI runs, before CI runs it.
#
# Exists because a green `cargo test` and a green `rb-regress run` are not a
# green pipeline. Two pushes in one day went red on things neither could see:
#
#   - a unit test that asserted a Windows path answer and failed on every Unix
#     job, because `Path::file_name` only treats `\` as a separator on Windows;
#   - `optical.rs` naming the `zstd` crate directly, which does not exist in the
#     MiSTer feature set (optical on, native-zstd off).
#
# Each check below is one of those classes. Run from the repo root.

set -uo pipefail
cd "$(dirname "$0")/.."

fail=0
run() {
    local label="$1"; shift
    printf '\n=== %s ===\n' "$label"
    if "$@"; then
        printf '  OK: %s\n' "$label"
    else
        printf '  FAILED: %s\n' "$label"
        fail=1
    fi
}

# CI's test step is --release, not a plain `cargo test`. Debug and release
# differ in overflow checks and in which assertions are compiled.
run "cargo test --release (what CI's Test step runs)" \
    cargo test --release

# The MiSTer armv7 leg: optical on, native-zstd off. Anything touching zstd has
# to go through crate::rbformats::zstd_compat or it breaks only here.
run "MiSTer feature set (optical on, native-zstd off)" \
    cargo check --bin rb-cli --no-default-features \
    --features chd,pure-zstd,remote,optical,tui

# The Rust 1.73 floor for engine code. A modern build cannot see a violation;
# this compiles the shared source under the vintage feature/dep set.
run "Rust 1.73 floor (vintage manifest)" \
    cargo build --manifest-path rb-cli-vintage/Cargo.toml \
    --no-default-features \
    --features native-zstd,remote,tui,rust173-polyfill,windows-legacy,yaml \
    --ignore-rust-version

# Documentation that claims something about the source. Cheap, and it is the
# only guard on the README tables and CONTRIBUTING's vintage command.
run "doc parity (README / CONTRIBUTING vs source)" \
    cargo test --test doc_parity

# The harness is its own crate, so nothing above compiles or tests it.
run "rb-regress (the harness's own tests)" \
    cargo test --manifest-path regression-tests/runner/Cargo.toml

# And its clippy, because the pre-commit hook runs it. Without this, preflight
# says "all checks passed" and the commit is then rejected by the hook — which
# is exactly what happened when this line was missing.
run "rb-regress clippy (what the pre-commit hook runs)" \
    cargo clippy --manifest-path regression-tests/runner/Cargo.toml --all-targets -- -D warnings

printf '\n'
if [ "$fail" -ne 0 ]; then
    printf 'preflight: FAILED - do not push\n'
    exit 1
fi
printf 'preflight: all checks passed\n'
printf 'After pushing, read the JOB LIST, not the run conclusion:\n'
printf '    gh run list --limit 1\n'
printf '    gh run view <run-id>\n'
printf 'Jobs marked continue-on-error (the MiSTer rb-cli-mini build is one) can\n'
printf 'fail while the run still reports success. That hid a compile error for a day.\n'
