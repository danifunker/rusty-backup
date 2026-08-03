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
# os-stub: swap src/os/macos.rs for the portable stand-in (objc2 cannot transpile).
# No `yaml`: serde_yml's libyml backend hits an mrustc macro-expansion gap.
FEATURES="${FEATURES:-native-zstd,remote,tui,rust173-polyfill,os-stub}"
HOST_ARCH="${HOST_ARCH:-aarch64}"          # aarch64 (Apple Silicon) or x86_64
PPC_TARGET="powerpc-apple-darwin"
# 10.4 floor: binds the plain symbols rather than the $UNIX2003 variants Tiger lacks.
export PPC_MIN_VERSION="${PPC_MIN_VERSION:-10.4}"

# ---- target CPU --------------------------------------------------------------
# PPC_CPU picks the oldest CPU the binary is allowed to run on. It decides two
# separate things, and both matter:
#
#   1. the Mach-O cpusubtype tag. Darwin's exec path grades it against the host,
#      so a `ppc7400` executable is refused outright on a 750 - the G3 never even
#      reaches main(). Older tags run on newer CPUs, so a 750 build covers every
#      PowerPC Mac from one binary.
#   2. whether AltiVec is on. gcc10-bootstrap defaults to `-mcpu=7400`, which
#      defines __ALTIVEC__ and lets the vectorizer emit vector ops; those are an
#      illegal instruction on a 750. See docs/build-ppc-mrustc.md "Targeting a G3".
#
# 750 is the default because AltiVec buys this workload nothing measurable: at
# -mcpu=7400 the whole 40 MB binary contained exactly two vector instructions
# (a vxor/stvx pair zeroing a buffer in zstd's HUF_buildCTable_wksp). Set
# PPC_CPU=7400 or 970 for a CPU-specific build; set PPC_CPU_FLAGS to bypass the
# mapping entirely.
PPC_CPU="${PPC_CPU:-750}"
if [ -z "${PPC_CPU_FLAGS:-}" ]; then
  case "$PPC_CPU" in
    g3|G3|750|740|745|755)  PPC_CPU_FLAGS="-mcpu=750 -mno-altivec" ;;
    603|603e|604|604e)      PPC_CPU_FLAGS="-mcpu=$PPC_CPU -mno-altivec" ;;
    g4|G4|7400|7410)        PPC_CPU_FLAGS="-mcpu=7400 -maltivec" ;;
    7450|7455)              PPC_CPU_FLAGS="-mcpu=$PPC_CPU -maltivec" ;;
    g5|G5|970)              PPC_CPU_FLAGS="-mcpu=970 -maltivec" ;;
    generic|ppc|none)       PPC_CPU_FLAGS="" ;;
    *)                      PPC_CPU_FLAGS="-mcpu=$PPC_CPU" ;;
  esac
fi
# Optional: schedule for a newer chip than the ISA floor. `PPC_CPU=750
# PPC_TUNE=7450` keeps every instruction legal on a G3 but orders them for a G4,
# which is the sane shape when one binary has to serve both.
[ -n "${PPC_TUNE:-}" ] && PPC_CPU_FLAGS="$PPC_CPU_FLAGS -mtune=$PPC_TUNE"
export PPC_CPU_FLAGS

# The download has to say which CPU it needs. A G4 or G5 build does not merely
# run slower on an older machine, it refuses to launch, so the artifacts are
# named per family rather than all landing on rb-cli-ppc.tar.gz.
case "$PPC_CPU" in
  g3|G3|750|740|745|755)      PPC_CPU_LABEL="g3" ;;
  603|603e|604|604e)          PPC_CPU_LABEL="ppc$PPC_CPU" ;;
  g4|G4|7400|7410|7450|7455)  PPC_CPU_LABEL="g4" ;;
  g5|G5|970)                  PPC_CPU_LABEL="g5" ;;
  generic|ppc|none)           PPC_CPU_LABEL="generic" ;;
  *)                          PPC_CPU_LABEL="$PPC_CPU" ;;
esac
export PPC_CPU_LABEL

# The PowerPC Mac that compiles the emitted C. Nothing here cross-compiles:
# there is no usable powerpc-apple-darwin cross-gcc, so scripts/ppc-cc-remote.py
# stands in as the C compiler and ships each translation unit over ssh. Set
# PPC_HOST to an ssh destination with key auth already working.
export PPC_HOST="${PPC_HOST:-}"
PPC_CC_WRAPPER="$RB_DIR/scripts/ppc-cc-remote.py"
PPC_AR_WRAPPER="$RB_DIR/scripts/ppc-ar-remote.py"
# mrustc picks its C compiler from CC_<triple with - replaced by _>.
export CC_powerpc_apple_darwin="$PPC_CC_WRAPPER"
# cc-rs uses the same convention, so the -sys crates' build scripts (bzip2-sys,
# zstd-sys) pick up the wrapper for free. They also need an *archiver*: the host
# `ar` writes a System V symbol table and Apple's linker wants a Mach-O
# __.SYMDEF, so the archive has to be built on the Mac as well.
export AR_powerpc_apple_darwin="$PPC_AR_WRAPPER"
# The compat shim (lgammaf_r, and the fcntl/poll overrides) has to be on the
# final link line or libstd's references to it go unresolved. ppc-cc-remote.py
# reads it from PPC_SHIM and simply omits it when unset - so a build driven by
# this script used to end in
#
#     Undefined symbols: "_lgammaf_r", referenced from: ... in libstd.rlib.o
#
# after every crate had compiled, with nothing in the log mentioning the shim.
# The manual link in docs/build-ppc-mrustc.md always passed it; the script did
# not. There is no build that wants it unset, so default it here.
export PPC_SHIM="${PPC_SHIM:-$CRATE_DIR/shim/ppc-compat.c}"
# The G5 has 2 cores; the transpile is local and parallel, the compiles are not.
PPC_JOBS="${PPC_JOBS:-3}"
# OVERRIDE_SUFFIX is chosen from the *host* OS by minicargo.mk, so a cross build
# from Linux would pick -linux (whose build_libc.txt says freebsd11). Name the
# macOS/PowerPC set explicitly; it differs from -macos only in STD_ENV_ARCH.
PPC_OVERRIDE_SUFFIX="-macos-powerpc"
# minicargo hands build scripts `RUSTC=<its own mrustc path>` so they can probe the
# compiler version, and derives that path from argv[0] - which is relative when it
# is invoked as `bin/minicargo`. Build scripts run with their cwd set to the crate
# directory, so a relative RUSTC fails to spawn: libc's build.rs then dies with
# "Failed to get rustc version" and takes the build with it. Pin it absolute.
export MRUSTC_PATH="${MRUSTC_PATH:-$MRUSTC_DIR/bin/mrustc}"

# ---- version stamp -----------------------------------------------------------
# The pipeline stamps a build date: `.github/workflows/release.yml` sets
# RELEASE_VERSION to `date -u +"%Y-%m-%d-%H-%M"`, ../build.rs reads it and emits
# `cargo:rustc-env=APP_VERSION=<it>`. That works unchanged here - minicargo does
# parse `cargo:rustc-env` and pass it to the crate compile - so all this has to
# do is set the variable and make sure it is not silently stale.
#
# The staleness matters, and differs from cargo. build.rs guards itself with
# `rerun-if-env-changed=RELEASE_VERSION`, but **minicargo does not implement
# rerun-if-env-changed**: once `build_rb-cli-ppc-*.txt` exists and looks current,
# the script is never re-run and APP_VERSION is pinned to whatever the first
# build stamped. So the version is recorded in a marker file and the build-script
# output is dropped only when it actually changes.
#
# Deliberately *not* re-stamped on every invocation: a changed build-script
# output makes the engine crate dirty, and re-transpiling the engine is the most
# expensive thing in this build. So the stamp is taken once and reused until you
# ask for a new one:
#
#   RELEASE_VERSION=$(date -u +%Y-%m-%d-%H-%M) scripts/build-ppc.sh ppc
#       The ONLY way to bake a new version in. Drops the build-script output, so
#       the engine re-transpiles - budget the full build time, not a relink.
#
#   rm <output>/.release-version
#       Does NOT re-stamp. With no marker this takes the first-stamp path below,
#       which adopts the current time into the marker and deliberately leaves
#       every existing object alone - so the binary keeps whatever APP_VERSION
#       was already baked into it, and `rb-cli --version` still reports the old
#       date. Use it to (re)establish a marker cheaply, never to refresh the
#       version. This comment used to advertise it as "re-stamp next run", which
#       sends you looking for the bug in build.rs instead of here.
#
# The stickiness is why a fresh PowerPC binary can report a date two days old:
# `env!("APP_VERSION")` is read from 12 sites inside the *lib* (src/cli/,
# src/gui/), so the version cannot change without re-transpiling the engine.
# Moving those reads into the bin crate would make re-stamping cheap.
stamp_version() {
  local marker="$PPC_OUT/.release-version"
  local prev=""
  [ -f "$marker" ] && prev="$(cat "$marker" 2>/dev/null)"
  if [ -z "${RELEASE_VERSION:-}" ]; then
    RELEASE_VERSION="${prev:-$(date -u +%Y-%m-%d-%H-%M)}"
  fi
  export RELEASE_VERSION
  mkdir -p "$PPC_OUT" "$PPC_OUT/host"

  if [ "$prev" = "$RELEASE_VERSION" ]; then
    note "APP_VERSION=$RELEASE_VERSION (unchanged)"
    return
  fi

  # First stamp on a tree that has no marker yet: adopt the version WITHOUT
  # invalidating anything. There is nothing stale to correct - whatever is built
  # was built before versioning existed - and dropping the build-script output
  # here costs a full re-transpile of the engine's 797 MB translation unit, which
  # is hours. Learned the hard way: introducing this function did exactly that to
  # an already-complete tree that only needed its final link.
  if [ -z "$prev" ]; then
    printf '%s' "$RELEASE_VERSION" > "$marker"
    note "APP_VERSION=$RELEASE_VERSION (first stamp - existing objects left alone;"
    note "  re-run with RELEASE_VERSION set, or delete $marker, to force it in)"
    return
  fi

  rm -f "$PPC_OUT"/host/build_rb-cli-ppc-*.txt
  printf '%s' "$RELEASE_VERSION" > "$marker"
  note "APP_VERSION=$RELEASE_VERSION (changed - build script will re-run, engine re-transpiles)"
}

HOST_LIBS="$MRUSTC_DIR/output-${RUSTC_VERSION}"
# Both PowerPC trees are keyed on the CPU family. The objects in them are
# compiled with $PPC_CPU_FLAGS, so sharing one tree across families silently
# links G5 AltiVec objects into a "G3" binary — the packaging guard catches it,
# but only after a full engine rebuild. One tree per family keeps each cache
# warm and makes a mixed binary impossible.
PPC_LIBS="$MRUSTC_DIR/output-${RUSTC_VERSION}-${PPC_TARGET}-${PPC_CPU_LABEL}"
HOST_OUT="$MRUSTC_DIR/output-rb-host"
HOSTC_OUT="$MRUSTC_DIR/output-rb-hostc"
PPC_OUT="$MRUSTC_DIR/output-rb-ppc-${PPC_CPU_LABEL}"
JOBS="$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)"

banner() { printf '\n\033[1;36m==== %s ====\033[0m\n' "$*"; }
note()   { printf '\033[33m%s\033[0m\n' "$*"; }
die()    { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

# ---- reaping orphaned compiles on the Mac -----------------------------------
# Interrupting this script used to leave cc1 running on the PowerPC Mac. Every
# remote compile is `ssh HOST gcc ...`, and with no tty on the far side there is
# nothing to deliver SIGHUP to the remote process group: when the local driver
# dies, each in-flight translation unit is orphaned and keeps burning a core.
# On a 2-core G5 a couple of those silently halve the speed of the *next* build,
# and the symptom - "why is this build twice as slow today" - points nowhere
# near the interrupted run that caused it.
#
# The match anchor is the toolchain prefix, not the build directory. cc1 is
# spawned with relative mirrored paths (`home/dani/repos/...`, no leading slash)
# and inherits its cwd, so its argv never mentions ppc-xbuild. Everything this
# pipeline runs on the Mac comes out of gcc10-bootstrap and nothing else on the
# machine does - notably not the root-owned ppc64 Linux CI leg, which is a
# different toolchain entirely.
#
# Killing loops because gcc respawns cc1 as it works through its queue: one pass
# kills the current unit and the driver immediately starts the next. Leopard's
# pkill has no -f pattern that matches these, so they go by explicit pid.
#
# Caveat: this reaps by toolchain, not by build, so two concurrent
# build-ppc.sh runs against the same Mac would kill each other's compiles. The
# pipeline is single-Mac by construction, so that is not a shape worth guarding.
ppc_reap_orphans() {
  [ -n "${PPC_HOST:-}" ] || return 0
  local result killed remaining
  result="$(ssh -o BatchMode=yes -o ConnectTimeout=10 "$PPC_HOST" '
    total=0
    for _ in 1 2 3 4 5; do
      pids=`ps ax -o pid,command | grep -E "[g]cc10-bootstrap|[p]pc-xbuild" | sed "s/^ *//; s/ .*//"`
      [ -z "$pids" ] && break
      for p in $pids; do kill -9 "$p" 2>/dev/null && total=`expr $total + 1`; done
      sleep 1
    done
    remaining=`ps ax -o pid,command | grep -cE "[g]cc10-bootstrap|[p]pc-xbuild"`
    echo "$total $remaining"
  ' 2>/dev/null)" || return 0
  killed="${result%% *}"
  remaining="${result##* }"
  [ -n "${killed:-}" ] || return 0
  [ "$killed" = 0 ] && return 0
  note "reaped $killed orphaned remote compile(s) on $PPC_HOST ($remaining still up)"
}

# ---- rustc source patch marker ----------------------------------------------
# A branch switch refreshes the patch's mtime without changing it; refresh dl-version so minicargo.mk won't re-apply it.
sync_rustc_src_patch() {
  local src="$MRUSTC_DIR/rustc-${RUSTC_VERSION}-src"
  local patch_file="$MRUSTC_DIR/rustc-${RUSTC_VERSION}-src.patch"
  [ -d "$src" ] && [ -f "$patch_file" ] && [ -f "$src/dl-version" ] || return 0
  [ "$patch_file" -nt "$src/dl-version" ] || return 0
  if (cd "$src" && patch -p0 -R --dry-run -f < "$patch_file" >/dev/null 2>&1); then
    find "$src" -name '*.rej' -delete 2>/dev/null || true
    touch "$src/dl-version"
    note "rustc source already carries rustc-${RUSTC_VERSION}-src.patch; refreshed dl-version (branch switch, identical content)"
  else
    note "rustc source does not match rustc-${RUSTC_VERSION}-src.patch - leaving it to minicargo to (re)apply"
  fi
}

_ppc_cleaned=0
ppc_cleanup() {
  local rc=$?
  [ "$_ppc_cleaned" = 1 ] && return $rc
  _ppc_cleaned=1
  ppc_reap_orphans
  return $rc
}
trap ppc_cleanup EXIT
trap 'ppc_cleanup; exit 130' INT
trap 'ppc_cleanup; exit 143' TERM

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
  apply_vendor_patches
}

# The mrustc-workaround patch set: one module per crate in rb-cli-ppc/patches/
# (see its README); the runner owns idempotency, mtime safety and loud failure
# when a crate version bump breaks a patch. Re-run before every transpile
# because `cargo vendor` regenerates the tree; a no-op run moves no mtimes.
apply_vendor_patches() {
  "$RB_DIR/scripts/apply-vendor-patches.py" --vendor-dir "$VENDOR_DIR"
}

# ---- stage 5: transpile+compile the engine for the HOST (native proof) ------
stage_host() {
  banner "5. transpile+build rb-cli for the HOST (native $HOST_ARCH proof)"
  stamp_version
  cd "$MRUSTC_DIR"
  apply_vendor_patches
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
  stamp_version
  cd "$MRUSTC_DIR"
  [ -e "$HOST_LIBS/libstd.rlib" ] || die "run 'hostlibs' first (need $HOST_LIBS)"
  [ -d "$VENDOR_DIR" ] || die "run 'vendor' first"
  apply_vendor_patches
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
  sync_rustc_src_patch
  [ -n "$PPC_HOST" ] || die "PPC_HOST is not set (e.g. PPC_HOST=admin@g5.local)"
  [ -x "$PPC_CC_WRAPPER" ] || die "missing $PPC_CC_WRAPPER"
  cd "$MRUSTC_DIR"
  stage_ppc_overrides
  # No debug_assertions: assert_unsafe_precondition! aborts in rt::init here (docs/build-ppc-mrustc.md "The alignment problem").
  # OUTDIR_SUF on the command line pins minicargo.mk's output directory to the
  # CPU-stamped one. minicargo.mk would otherwise derive
  # `output-<ver>-<target>` and append the target itself (its `:=` is ignored
  # for a command-line variable), so libstd would land in a tree shared by
  # every CPU family — the objects are built with $PPC_CPU_FLAGS and must not be.
  MRUSTC_TARGET_VER="$MRUSTC_TARGET_VER" MINICARGO_DEFER_CODEGEN=1 \
  MINICARGO_NO_DEBUG_ASSERTIONS=1 \
    make -f minicargo.mk LIBS \
      RUSTC_VERSION="$RUSTC_VERSION" \
      MRUSTC_TARGET="$PPC_TARGET" \
      OUTDIR_SUF="-${RUSTC_VERSION}-${PPC_TARGET}-${PPC_CPU_LABEL}" \
      OVERRIDE_SUFFIX="$PPC_OVERRIDE_SUFFIX" \
      PARLEVEL="$PPC_JOBS"
  ls "$PPC_LIBS"/libstd.rlib.o >/dev/null 2>&1 || die "PPC libstd not produced"
  note "PPC libstd ready ($(ls "$PPC_LIBS"/*.o | wc -l | tr -d ' ') PowerPC objects)."
}

# minicargo derives the *host* lib dir from the target one by dropping the
# target triple, so a CPU-stamped `output-<ver>-<triple>-g3` yields
# `output-<ver>-g3` — which nothing builds. Host libs are CPU-independent
# (they run here, not on the Mac), so alias the derived name at the real one.
ensure_host_lib_alias() {
  local alias_dir="$MRUSTC_DIR/output-${RUSTC_VERSION}-${PPC_CPU_LABEL}"
  [ -e "$alias_dir" ] && return 0
  ln -sfn "output-${RUSTC_VERSION}" "$alias_dir" \
    && note "aliased $(basename "$alias_dir") -> output-${RUSTC_VERSION} (host libs)"
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
  stamp_version
  [ -n "$PPC_HOST" ] || die "PPC_HOST is not set (e.g. PPC_HOST=admin@g5.local)"
  [ -x "$PPC_CC_WRAPPER" ] || die "missing $PPC_CC_WRAPPER"
  # Only this stage reaches the -sys crates, so only this stage needs an archiver.
  [ -x "$PPC_AR_WRAPPER" ] || die "missing $PPC_AR_WRAPPER"
  ensure_host_lib_alias
  [ -e "$PPC_LIBS/libstd.rlib.o" ] || die "PPC libstd missing -- run 'ppclibs' first"
  cd "$MRUSTC_DIR"
  apply_vendor_patches
  mkdir -p "$PPC_OUT"
  MRUSTC_TARGET_VER="$MRUSTC_TARGET_VER" MINICARGO_DEFER_CODEGEN=1 \
    bin/minicargo "$CRATE_DIR" \
      --vendor-dir "$VENDOR_DIR" \
      -L "$PPC_LIBS" \
      --output-dir "$PPC_OUT" \
      --target "$PPC_TARGET" \
      --no-default-features --features "$FEATURES" \
      -j "$PPC_JOBS"
  # Don't take a zero exit as proof. minicargo has returned success while
  # deadlocking ("Nothing runnable or running, but jobs are still waiting"), and
  # this note is the only thing the log then shows - it announced an rb-cli that
  # was never linked. stage_host has always checked for its binary; so does this.
  [ -e "$PPC_OUT/rb-cli" ] || die "minicargo exited 0 but produced no $PPC_OUT/rb-cli -- check the log for 'BUG:' and for a deadlock listing"
  note "PowerPC rb-cli under $PPC_OUT (compiled on $PPC_HOST)."
}

# ---- stage 9: package a self-contained tree -------------------------------
# The Mac-side work (dylib closure, CPU-floor guard, Tiger checks) lives in
# scripts/ppc-package.sh, fed over `ssh bash -s`; it also runs by hand there.
stage_dist() {
  banner "9. package a relocatable PowerPC tree"
  [ -n "$PPC_HOST" ] || die "PPC_HOST is not set"
  [ -e "$PPC_OUT/rb-cli" ] || die "no $PPC_OUT/rb-cli -- run 'ppc' first"
  # install_name_tool and otool only exist on the Mac, so the rewrite happens there.
  note "uploading rb-cli to $PPC_HOST for packaging"
  scp -q "$PPC_OUT/rb-cli" "$PPC_HOST:~/rb-cli-dist-src" || die "could not upload rb-cli"
  ssh "$PPC_HOST" \
    "RB_CPU=$(printf '%q' "$PPC_CPU") RB_CPU_FLAGS=$(printf '%q' "$PPC_CPU_FLAGS") RB_CPU_LABEL=$(printf '%q' "$PPC_CPU_LABEL") bash -s" \
    < "$RB_DIR/scripts/ppc-package.sh" || die "packaging failed on $PPC_HOST"
  mkdir -p "$RB_DIR/dist"
  local art="rb-cli-ppc-${PPC_CPU_LABEL}.tar.gz"
  scp -q "$PPC_HOST:~/$art" "$RB_DIR/dist/$art" || die "could not fetch the tarball"
  note "PowerPC bundle at $RB_DIR/dist/$art (unpack and run; no install step)."
  note "Needs a $(echo "$PPC_CPU_LABEL" | tr 'a-z' 'A-Z') or newer; older Macs refuse it at exec."
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
    dist)      stage_dist ;;
    all)
      stage_mrustc
      stage_overrides
      stage_hostlibs
      stage_vendor
      stage_host
      banner "HOST path complete. Run 'ppclibs', 'ppc' then 'dist' for PowerPC (needs PPC_HOST)."
      ;;
    *) die "unknown stage '$stage' (mrustc|overrides|hostlibs|vendor|hostc|host|ppclibs|ppc|probe|dist|all)" ;;
  esac
}
main "$@"
