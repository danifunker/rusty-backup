#!/usr/bin/env bash
#
# build-ppc.sh -- transpile the rusty-backup engine (rb-cli-ppc) to C via
# mrustc, toward a native PowerPC Mac OS X 10.4/10.5 `rb-cli`.
#
# This is the driver for a two-machine pipeline (see docs/build-ppc-mrustc.md):
#
#     This machine (fast)               PowerPC Mac (over ssh)
#     -------------------               ----------------------
#     Rust --mrustc--> C99       --->   C99 --gcc10--> PowerPC Mach-O
#
# Nothing here compiles a PowerPC binary directly; mrustc produces C and
# scripts/ppc-cc-remote.py stands in as the C compiler, shipping each
# translation unit to the Mac and bringing the .o back. So minicargo's
# dependency graph and parallelism keep working across the two machines.
#
# The HOST stages build a native this-machine rb-cli straight through mrustc as
# an end-to-end proof that the whole engine transpiles. Get HOST green before
# spending time on PowerPC.
#
# Usage:
#   scripts/build-ppc.sh            # run every stage in order (host path)
#   scripts/build-ppc.sh <stage>    # run a single stage
#   stages: mrustc overrides hostlibs vendor hostc host  ppclibs ppc probe
#     (hostc = emit the engine's C on this machine, no PowerPC needed; the
#      fastest test that the whole engine transpiles)
#     (ppclibs/ppc/probe need PPC_HOST=<ssh dest of a PowerPC Mac>)
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

# The PowerPC Mac that compiles the emitted C. Nothing here cross-compiles:
# there is no usable powerpc-apple-darwin cross-gcc, so scripts/ppc-cc-remote.py
# stands in as the C compiler and ships each translation unit over ssh. Set
# PPC_HOST to an ssh destination with key auth already working.
export PPC_HOST="${PPC_HOST:-}"
PPC_CC_WRAPPER="$RB_DIR/scripts/ppc-cc-remote.py"
# mrustc picks its C compiler from CC_<triple with - replaced by _>.
export CC_powerpc_apple_darwin="$PPC_CC_WRAPPER"
# The G5 has 2 cores; the transpile is local and parallel, the compiles are not.
PPC_JOBS="${PPC_JOBS:-3}"
# OVERRIDE_SUFFIX is chosen from the *host* OS by minicargo.mk, so a cross build
# from Linux would pick -linux (whose build_libc.txt says freebsd11). Name the
# macOS/PowerPC set explicitly; it differs from -macos only in STD_ENV_ARCH.
PPC_OVERRIDE_SUFFIX="-macos-powerpc"

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

# ---- stage 6: build the PowerPC standard library ----------------------------
# Transpiles core/alloc/std/panic_unwind/test for powerpc-apple-darwin and
# compiles each emitted .c on the G5 via the remote-cc wrapper. The vendored
# libc needs no source changes to *compile* for this target - b32 is
# arch-agnostic and mrustc's own layout asserts pass - but several of its struct
# definitions describe modern macOS and are wrong for 10.4/10.5 at *runtime*.
# See docs/build-ppc-mrustc.md and scripts/ppc-libc-probe.py.
stage_ppclibs() {
  banner "6. build PPC libstd -> $PPC_LIBS"
  [ -n "$PPC_HOST" ] || die "PPC_HOST is not set (e.g. PPC_HOST=admin@g5.local)"
  [ -x "$PPC_CC_WRAPPER" ] || die "missing $PPC_CC_WRAPPER"
  cd "$MRUSTC_DIR"
  stage_ppc_overrides
  MRUSTC_TARGET_VER="$MRUSTC_TARGET_VER" MINICARGO_DEFER_CODEGEN=1 \
    make -f minicargo.mk LIBS \
      RUSTC_VERSION="$RUSTC_VERSION" \
      MRUSTC_TARGET="$PPC_TARGET" \
      OVERRIDE_SUFFIX="$PPC_OVERRIDE_SUFFIX" \
      PARLEVEL="$PPC_JOBS"
  ls "$PPC_LIBS"/libstd.rlib.o >/dev/null 2>&1 || die "PPC libstd not produced"
  note "PPC libstd ready ($(ls "$PPC_LIBS"/*.o | wc -l | tr -d ' ') PowerPC objects)."
}

# The macOS/PowerPC build-script override set (mrustc ships -linux/-windows).
stage_ppc_overrides() {
  local dir="$MRUSTC_DIR/script-overrides/stable-${RUSTC_VERSION}${PPC_OVERRIDE_SUFFIX}"
  [ -f "$dir/build_std.txt" ] && return 0
  mkdir -p "$dir"
  printf '#cargo:compiler-rt=\ncargo:rustc-cfg=feature="unstable"\n' > "$dir/build_compiler_builtins.txt"
  {
    echo "cargo:rustc-cfg=darwin"        # the linux set says freebsd11 here
    for c in libc_priv_mod_use libc_union libc_const_size_o libc_align \
             libc_core_cvoid libc_packedN libc_cfg_target_vendor \
             libc_thread_local libc_const_extern_fn; do
      echo "cargo:rustc-cfg=$c"
    done
  } > "$dir/build_libc.txt"
  printf 'cargo:rustc-cfg=backtrace_in_libstd\ncargo:rustc-env=STD_ENV_ARCH=powerpc\n' > "$dir/build_std.txt"
  printf '# No output for macos\n' > "$dir/build_unwind.txt"
  note "created $dir"
}

# ---- stage 7: build rb-cli for PowerPC --------------------------------------
# Transpiles the engine and compiles each emitted .c on the G5, same as stage 6.
stage_ppc() {
  banner "7. transpile+build rb-cli for $PPC_TARGET"
  [ -n "$PPC_HOST" ] || die "PPC_HOST is not set (e.g. PPC_HOST=admin@g5.local)"
  [ -e "$PPC_LIBS/libstd.rlib.o" ] || die "PPC libstd missing -- run 'ppclibs' first"
  cd "$MRUSTC_DIR"
  patch_crc_vendor
  mkdir -p "$PPC_OUT"
  MRUSTC_TARGET_VER="$MRUSTC_TARGET_VER" MINICARGO_DEFER_CODEGEN=1 \
    bin/minicargo "$CRATE_DIR" \
      --vendor-dir "$VENDOR_DIR" \
      -L "$PPC_LIBS" \
      --output-dir "$PPC_OUT" \
      --target "$PPC_TARGET" \
      --no-default-features --features "$FEATURES" \
      -j "$PPC_JOBS"
  note "PowerPC rb-cli under $PPC_OUT (compiled on $PPC_HOST)."
}

# ---- stage 8: capture libc ground truth from the PowerPC Mac ----------------
stage_probe() {
  banner "8. probe the PowerPC SDKs for libc ground truth"
  [ -n "$PPC_HOST" ] || die "PPC_HOST is not set"
  local libc="$MRUSTC_DIR/rustc-${RUSTC_VERSION}-src/vendor/libc"
  [ -d "$libc" ] || die "vendored libc not found at $libc"
  mkdir -p "$RB_DIR/rb-cli-ppc/probe"
  local sdk out
  for sdk in /Developer/SDKs/MacOSX10.4u.sdk /Developer/SDKs/MacOSX10.5.sdk; do
    out="$RB_DIR/rb-cli-ppc/probe/ppc-$(basename "$sdk" .sdk | sed 's/MacOSX//').tsv"
    "$RB_DIR/scripts/ppc-libc-probe.py" --libc "$libc" --host "$PPC_HOST" \
      --sdk "$sdk" --arch ppc --out "$out"
    "$RB_DIR/scripts/ppc-libc-compare.py" --libc "$libc" --probe "$out" \
      --quiet-missing > "${out%.tsv}.report.txt"
    note "wrote $out and ${out%.tsv}.report.txt"
  done
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
    probe)     stage_probe ;;
    all)
      stage_mrustc
      stage_overrides
      stage_hostlibs
      stage_vendor
      stage_host
      banner "HOST path complete. Run 'ppclibs' then 'ppc' for PowerPC (needs PPC_HOST)."
      ;;
    *) die "unknown stage '$stage' (mrustc|overrides|hostlibs|vendor|hostc|host|ppclibs|ppc|probe|all)" ;;
  esac
}
main "$@"
