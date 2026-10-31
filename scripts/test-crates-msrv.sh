#!/usr/bin/env bash
# Run every crate's tests on the Rust 1.73 floor (crate_split_plan.md §5).
# Cargo 1.73 can't read the root lockfile and won't test a non-member path crate
# with dev-dependencies, so the crates are copied into a throwaway workspace
# seeded with the vintage build's 1.73-compatible lockfile.
#
# Usage: scripts/test-crates-msrv.sh [crate ...]   (default: every crate)
# Toolchain: CARGO / RUSTC / RUSTDOC (default: the rustup 1.73.0 toolchain).
set -euo pipefail

RB_DIR="$(cd "$(dirname "$0")/.." && pwd)"
if [ -z "${CARGO:-}" ]; then
  T=$(ls -d "$HOME"/.rustup/toolchains/1.73.0-* 2>/dev/null | head -1)
  [ -n "$T" ] || { echo "no 1.73.0 toolchain; rustup toolchain install 1.73.0" >&2; exit 1; }
  CARGO="$T/bin/cargo"; RUSTC="$T/bin/rustc"; RUSTDOC="$T/bin/rustdoc"
fi
export RUSTC="${RUSTC:-rustc}" RUSTDOC="${RUSTDOC:-rustdoc}"
WS="${MSRV_WS:-$RB_DIR/target/crates-msrv}"

rm -rf "$WS/crates"
mkdir -p "$WS"
cp -R "$RB_DIR/crates" "$WS/crates"
cp "$RB_DIR/rb-cli-vintage/Cargo.lock" "$WS/Cargo.lock"
cat > "$WS/Cargo.toml" <<'TOML'
[workspace]
members = ["crates/*"]
resolver = "2"
TOML
# Fixture-reading tests resolve `CARGO_MANIFEST_DIR/../../tests/fixtures`.
ln -sfn "$RB_DIR/tests" "$WS/tests"
ln -sfn "$RB_DIR/scripts" "$WS/scripts"

crates=("$@")
if [ ${#crates[@]} -eq 0 ]; then
  for d in "$WS"/crates/*/; do crates+=("$(basename "$d")"); done
fi

cd "$WS"
fail=()
for c in "${crates[@]}"; do
  echo "=== $c (Rust 1.73)"
  if ! "$CARGO" test -p "$c" --features "$c/rust173-polyfill" --ignore-rust-version; then
    fail+=("$c")
  fi
done
if [ ${#fail[@]} -gt 0 ]; then
  echo "FAILED on 1.73: ${fail[*]}" >&2
  exit 1
fi
echo "all ${#crates[@]} crates pass on Rust 1.73"
