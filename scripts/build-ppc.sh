#!/usr/bin/env bash
#
# build-ppc.sh -- transpile the rusty-backup engine (rb-cli-ppc) to C via
# mrustc, toward a native PowerPC Mac OS X 10.4/10.5 `rb-cli`.
#
# This is the *modern-Mac half* of a two-machine pipeline (see
# docs/build-ppc-mrustc.md):
#
#     Modern Mac (this script)          G4 / PowerPC Tiger
#     -----------------------           ------------------
#     Rust --mrustc--> C99       --->   C99 --gcc-mp-10--> PowerPC Mach-O
#
# Nothing here compiles a PowerPC binary; it produces C. The G4 compiles it.
# The one exception is the HOST stage, which builds a native (this-Mac) rb-cli
# straight through mrustc as an end-to-end proof that the whole engine
# transpiles. Get HOST green before touching PPC.
#
# Reverse-engineered interactively 2026-07-24. Stages 1-4 + host-libs are
# validated; the host transpile and the whole PPC half are works in progress
# (the PPC half is blocked on a powerpc-apple-darwin libc -- see the doc).
#
# Usage:
#   scripts/build-ppc.sh            # run every stage in order (host path)
#   scripts/build-ppc.sh <stage>    # run a single stage
#   stages: mrustc overrides hostlibs vendor hostc host  ppclibs ppc
#     (hostc = emit the engine's C on this Mac, no PPC libc needed; the
#      fastest unblocked test that the whole engine transpiles)
#
set -euo pipefail

# ---- config -----------------------------------------------------------------
MRUSTC_DIR="${MRUSTC_DIR:-$HOME/repos/mrustc}"
RB_DIR="${RB_DIR:-$HOME/repos/rusty-backup}"
# rb-cli-ppc is the mrustc/PowerPC manifest (sibling of rb-cli-vintage). It
# reuses ../src but carries the dep deviations mrustc's C backend forces, so
# rb-cli-vintage stays pristine. See rb-cli-ppc/Cargo.toml for the deviations.
CRATE_DIR="$RB_DIR/rb-cli-ppc"
VENDOR_DIR="$CRATE_DIR/vendor"

RUSTC_VERSION="${RUSTC_VERSION:-1.74.0}"   # rustc source mrustc bootstraps from
export MRUSTC_TARGET_VER="${MRUSTC_TARGET_VER:-1.74}"  # language mode for mrustc
FEATURES="${FEATURES:-native-zstd,remote,tui,rust173-polyfill}"
HOST_ARCH="${HOST_ARCH:-aarch64}"          # aarch64 (Apple Silicon) or x86_64
PPC_TARGET="powerpc-apple-darwin"

HOST_LIBS="$MRUSTC_DIR/output-${RUSTC_VERSION}"
PPC_LIBS="$MRUSTC_DIR/output-${RUSTC_VERSION}-${PPC_TARGET}"
HOST_OUT="$MRUSTC_DIR/output-rb-host"
HOSTC_OUT="$MRUSTC_DIR/output-rb-hostc"
PPC_OUT="$MRUSTC_DIR/output-rb-ppc"
JOBS="$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)"

banner() { printf '\n\033[1;36m==== %s ====\033[0m\n' "$*"; }
note()   { printf '\033[33m%s\033[0m\n' "$*"; }
die()    { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

# ---- stage 1: build mrustc + minicargo (with our parser patches) ------------
stage_mrustc() {
  banner "1. build mrustc + minicargo"
  [ -d "$MRUSTC_DIR" ] || die "mrustc not found at $MRUSTC_DIR (set MRUSTC_DIR)"
  cd "$MRUSTC_DIR"

  # These two patches are REQUIRED to parse rusty-backup's manifest set. They
  # live on branch 'rb-cli-vintage-build' in the fork. Fail loudly if missing.
  grep -q "skip_composite_value" tools/common/toml.cpp \
    || die "mrustc missing the TOML-parser patch (nested arrays / escapes). Check out the rb-cli-vintage-build branch."
  grep -q 's == "rlib" || s == "lib"' tools/minicargo/manifest.cpp \
    || die "mrustc missing the crate-type=\"lib\" patch. Check out the rb-cli-vintage-build branch."

  make -C tools/common          # common_lib.a (shared TOML parser)
  make -C tools/minicargo/      # bin/minicargo
  make                          # bin/mrustc
  [ -x bin/mrustc ] && [ -x bin/minicargo ] || die "mrustc/minicargo did not build"
  note "mrustc + minicargo built."
}

# ---- stage 2: macOS build-script overrides for this rustc version -----------
# mrustc ships stable-<ver>-linux/-windows but not -macos for 1.74. Derive it
# from the linux set (only build_libc's first cfg + arch differ).
stage_overrides() {
  banner "2. ensure script-overrides/stable-${RUSTC_VERSION}-macos"
  cd "$MRUSTC_DIR"
  # mrustc ships stable-<ver>-linux and -windows; only -macos is missing.
  if [ "$(uname -s)" != "Darwin" ]; then
    note "non-macOS host: using mrustc's shipped stable-${RUSTC_VERSION}-$(uname -s | tr 'A-Z' 'a-z') override set (no synth needed)."
    return
  fi
  local dir="script-overrides/stable-${RUSTC_VERSION}-macos"
  if [ -f "$dir/build_compiler_builtins.txt" ]; then
    note "macos override set already present."
    return
  fi
  mkdir -p "$dir"
  printf '#cargo:compiler-rt=\ncargo:rustc-cfg=feature="unstable"\n' > "$dir/build_compiler_builtins.txt"
  {
    echo "cargo:rustc-cfg=darwin"        # linux set says freebsd11 here
    echo "cargo:rustc-cfg=libc_priv_mod_use"
    echo "cargo:rustc-cfg=libc_union"
    echo "cargo:rustc-cfg=libc_const_size_o"
    echo "cargo:rustc-cfg=libc_align"
    echo "cargo:rustc-cfg=libc_core_cvoid"
    echo "cargo:rustc-cfg=libc_packedN"
    echo "cargo:rustc-cfg=libc_cfg_target_vendor"
    echo "cargo:rustc-cfg=libc_thread_local"
    echo "cargo:rustc-cfg=libc_const_extern_fn"
  } > "$dir/build_libc.txt"
  printf 'cargo:rustc-cfg=backtrace_in_libstd\ncargo:rustc-env=STD_ENV_ARCH=%s\n' "$HOST_ARCH" > "$dir/build_std.txt"
  printf '# No output for macos\n' > "$dir/build_unwind.txt"
  note "created $dir"
}

# ---- stage 3: build the HOST standard library through mrustc ----------------
stage_hostlibs() {
  banner "3. build host libstd ($RUSTC_VERSION) -> $HOST_LIBS   [slow]"
  cd "$MRUSTC_DIR"
  # MRUSTC_TARGET_VER MUST be exported or the stdlib builds in 1.29 mode.
  MRUSTC_TARGET_VER="$MRUSTC_TARGET_VER" \
    make -f minicargo.mk LIBS RUSTC_VERSION="$RUSTC_VERSION"
  ls "$HOST_LIBS"/libstd.rlib >/dev/null 2>&1 || die "host libstd not produced"
  note "host libstd ready."
}

# ---- stage 4: vendor rb-cli-ppc's dependency sources ------------------------
stage_vendor() {
  banner "4. cargo vendor rb-cli-ppc deps -> $VENDOR_DIR"
  cd "$CRATE_DIR"
  cargo vendor --locked vendor >/dev/null
  note "vendored $(ls vendor | wc -l | tr -d ' ') crates."
}

# ---- mrustc-workaround patches on the vendored sources ----------------------
# Re-applied before every transpile because `cargo vendor` regenerates the
# tree. Each patch works around a specific mrustc gap; idempotent (the pattern
# stops matching once applied). See docs/build-ppc-mrustc.md.
patch_crc_vendor() {
  # crc 3.x: mrustc can't infer the const-generic impl params of
  # `Digest::<uN, Table<L>>::new` from the return type, so spell them out.
  local d="$VENDOR_DIR/crc/src" w
  [ -f "$d/crc128.rs" ] || return 0
  for w in 8 16 32 64 128; do
    sed "s/        Digest::new(self, value)/        Digest::<u${w}, Table<L>>::new(self, value)/" \
      "$d/crc${w}.rs" > "$d/crc${w}.rs.tmp" && mv "$d/crc${w}.rs.tmp" "$d/crc${w}.rs"
  done
  note "applied vendored-source workarounds (crc turbofish)."
}

# ---- stage 5: transpile+compile the engine for the HOST (native proof) ------
stage_host() {
  banner "5. transpile+build rb-cli for the HOST (native $HOST_ARCH proof)"
  cd "$MRUSTC_DIR"
  patch_crc_vendor
  mkdir -p "$HOST_OUT"
  MRUSTC_TARGET_VER="$MRUSTC_TARGET_VER" \
    bin/minicargo "$CRATE_DIR" \
      --vendor-dir "$VENDOR_DIR" \
      -L "$HOST_LIBS" \
      --output-dir "$HOST_OUT" \
      --no-default-features --features "$FEATURES" \
      -j "$JOBS"
  if [ -x "$HOST_OUT/rb-cli" ]; then
    note "HOST rb-cli built: $HOST_OUT/rb-cli"
    "$HOST_OUT/rb-cli" --help >/dev/null 2>&1 && note "and it runs." || note "(built; --help smoke test non-zero -- inspect)"
  else
    die "HOST rb-cli not produced -- this is where per-crate mrustc lowering errors surface"
  fi
}

# ---- stage 5b: emit the engine's C on the HOST (no PPC libc needed) ---------
# The host analog of `ppc`: transpile the whole engine to C against the working
# host libstd, codegen DEFERRED -- so you get every crate's .c plus a
# <crate>-codegen.sh, WITHOUT needing the final link to succeed. This is the
# fastest unblocked way to (a) prove mrustc digests the entire engine and
# (b) read the emitted C, months before the PPC libc is sorted.
stage_hostc() {
  banner "5b. emit host C for the whole engine (deferred codegen) -> $HOSTC_OUT"
  cd "$MRUSTC_DIR"
  [ -e "$HOST_LIBS/libstd.rlib" ] || die "run 'hostlibs' first (need $HOST_LIBS)"
  [ -d "$VENDOR_DIR" ] || die "run 'vendor' first"
  patch_crc_vendor
  mkdir -p "$HOSTC_OUT"
  MRUSTC_TARGET_VER="$MRUSTC_TARGET_VER" MINICARGO_DEFER_CODEGEN=1 \
    bin/minicargo "$CRATE_DIR" \
      --vendor-dir "$VENDOR_DIR" \
      -L "$HOST_LIBS" \
      --output-dir "$HOSTC_OUT" \
      --no-default-features --features "$FEATURES" \
      -j "$JOBS"
  note "emitted $(ls "$HOSTC_OUT"/*.c 2>/dev/null | wc -l | tr -d ' ') .c files in $HOSTC_OUT"
  note "engine C:  $HOSTC_OUT/librusty_backup-*.c"
  note "binary C:  $HOSTC_OUT/rb-cli.c   (+ per-crate <name>-codegen.sh)"
}

# ---- stage 6: PPC libs (BLOCKED: no powerpc-apple-darwin libc yet) ----------
stage_ppclibs() {
  banner "6. build PPC libstd -> $PPC_LIBS   [EXPERIMENTAL / EXPECTED TO FAIL]"
  note "Blocked on a powerpc-apple-darwin libc: the vendored libc crate has no"
  note "PowerPC-Apple arch definitions (apple/b32 is x86-only). Until that is"
  note "ported (or lifted from github.com/Scottcjn/rust-ppc-tiger), libstd for"
  note "$PPC_TARGET cannot build. Running anyway so the failure is on record:"
  cd "$MRUSTC_DIR"
  MRUSTC_TARGET_VER="$MRUSTC_TARGET_VER" \
    make -f minicargo.mk LIBS RUSTC_VERSION="$RUSTC_VERSION" MRUSTC_TARGET="$PPC_TARGET" || \
    note "PPC libstd failed as expected (see the libc gap above)."
}

# ---- stage 7: transpile the engine to C for PPC (codegen deferred) ----------
# Emits C only -- no compile here. Ship $PPC_OUT to the G4 and run gcc-mp-10.
stage_ppc() {
  banner "7. transpile rb-cli to C for $PPC_TARGET (deferred codegen)"
  [ -d "$PPC_LIBS" ] || die "PPC libs missing -- stage 6 must succeed first (blocked on libc)"
  cd "$MRUSTC_DIR"
  patch_crc_vendor
  mkdir -p "$PPC_OUT"
  MRUSTC_TARGET_VER="$MRUSTC_TARGET_VER" MINICARGO_DEFER_CODEGEN=1 \
    bin/minicargo "$CRATE_DIR" \
      --vendor-dir "$VENDOR_DIR" \
      -L "$PPC_LIBS" \
      --output-dir "$PPC_OUT" \
      --no-default-features --features "$FEATURES"
  note "C emitted under $PPC_OUT. Next: scp it to the G4 and compile with gcc-mp-10"
  note "(see docs/build-ppc-mrustc.md -- G4 half)."
}

# ---- driver -----------------------------------------------------------------
main() {
  local stage="${1:-all}"
  case "$stage" in
    mrustc)    stage_mrustc ;;
    overrides) stage_overrides ;;
    hostlibs)  stage_hostlibs ;;
    vendor)    stage_vendor ;;
    hostc)     stage_hostc ;;
    host)      stage_host ;;
    ppclibs)   stage_ppclibs ;;
    ppc)       stage_ppc ;;
    all)
      stage_mrustc
      stage_overrides
      stage_hostlibs
      stage_vendor
      stage_host
      banner "HOST path complete. PPC half is blocked on libc (stage 6); see the doc."
      ;;
    *) die "unknown stage '$stage' (mrustc|overrides|hostlibs|vendor|hostc|host|ppclibs|ppc|all)" ;;
  esac
}
main "$@"
