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
PPC_LIBS="$MRUSTC_DIR/output-${RUSTC_VERSION}-${PPC_TARGET}"
HOST_OUT="$MRUSTC_DIR/output-rb-host"
HOSTC_OUT="$MRUSTC_DIR/output-rb-hostc"
PPC_OUT="$MRUSTC_DIR/output-rb-ppc"
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
  # Only write when the content actually changes. `sed >tmp && mv` unconditionally
  # rewrites the file, which bumps its mtime on EVERY run - and minicargo is
  # timestamp-driven, so crc went dirty every build, dragging lzma-rs and then the
  # engine with it. That silently forced a re-transpile of the 797 MB engine unit
  # on every single invocation.
  for w in 8 16 32 64 128; do
    sed "s/        Digest::new(self, value)/        Digest::<u${w}, Table<L>>::new(self, value)/" \
      "$d/crc${w}.rs" > "$d/crc${w}.rs.tmp"
    if cmp -s "$d/crc${w}.rs.tmp" "$d/crc${w}.rs"; then
      rm -f "$d/crc${w}.rs.tmp"
    else
      mv "$d/crc${w}.rs.tmp" "$d/crc${w}.rs"
    fi
  done
  note "applied vendored-source workarounds (crc turbofish)."
}

patch_chrono_vendor() {
  # chrono 0.4: `NaiveDateTime::UNIX_EPOCH` is defined as
  # `DateTime::UNIX_EPOCH.naive_utc()`. Nothing in that expression pins
  # `DateTime`'s `Tz`; rustc resolves it because only one inherent impl
  # (`impl DateTime<Utc>`) declares a `UNIX_EPOCH`, but mrustc can't infer an
  # impl's type parameter from which impl happens to carry the associated const.
  # Same class of gap as the crc turbofish above, same shape of fix. `Utc` is
  # already in scope in that module.
  # Two sites: `NaiveDateTime::UNIX_EPOCH` and `impl Default for NaiveDateTime`.
  # Skip doc comments and the `#[deprecated(note = ...)]` string, which mention
  # the path without using it. Idempotent - the rewritten form no longer matches.
  local f="$VENDOR_DIR/chrono/src/naive/datetime/mod.rs"
  [ -f "$f" ] || return 0
  # Only write on an actual change - see patch_crc_vendor for why an unconditional
  # rewrite is expensive here.
  sed -e '/^[[:space:]]*\/\/\//!{' -e '/#\[deprecated/!s/DateTime::UNIX_EPOCH/DateTime::<Utc>::UNIX_EPOCH/g' -e '}' \
    "$f" > "$f.tmp"
  if cmp -s "$f.tmp" "$f"; then
    rm -f "$f.tmp"
  else
    mv "$f.tmp" "$f"
  fi
  note "applied vendored-source workarounds (chrono UNIX_EPOCH turbofish)."
}

patch_rustversion_vendor() {
  # rustversion identifies the compiler from the *last* line of `rustc --version`,
  # which is right for real rustc (one line, possibly preceded by warnings) but
  # wrong for mrustc, which prints four lines with the `rustc <ver>` one first and
  # informational lines after it. Pick the last line that actually starts with
  # `rustc ` instead - identical behaviour on real rustc, and it keeps working if
  # mrustc's trailing lines change.
  #
  # Fixing this in mrustc instead is not obviously safe: the line order is load
  # bearing in both directions. libc's build.rs parses from the *start* of the
  # output, and mrustc's own comments note that `autoconfig` looks for the
  # `release:` line, so neither reordering nor trimming is free.
  local f="$VENDOR_DIR/rustversion/build/rustc.rs"
  [ -f "$f" ] || return 0
  python3 - "$f" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
old = "    let last_line = string.lines().last().unwrap_or(string);"
new = ("    // rb-cli-ppc: mrustc prints informational lines *after* the `rustc <ver>`\n"
       "    // line, so take the last line that actually looks like a version banner.\n"
       "    let last_line = string\n"
       "        .lines()\n"
       "        .filter(|l| l.trim_start().starts_with(\"rustc \"))\n"
       "        .last()\n"
       "        .or_else(|| string.lines().last())\n"
       "        .unwrap_or(string);")
if new.splitlines()[0] in s:
    sys.exit(0)          # already patched
if old not in s:
    sys.stderr.write("rustversion: expected line not found; skipping patch\n")
    sys.exit(0)
open(p, "w").write(s.replace(old, new, 1))
PY
  note "applied vendored-source workarounds (rustversion --version parsing)."
}

patch_signal_hook_vendor() {
  # signal-hook's internal `AddSignal` trait takes an *arbitrary self type*:
  #
  #   trait AddSignal: Debug + Send + Sync {
  #       fn add_signal(self: Arc<Self>, write: Arc<dyn SelfPipeWrite>, ..)
  #   }
  #
  # and the one call site invokes it with method syntax on a trait object,
  # `Arc::clone(&self.pending).add_signal(..)`. mrustc does not consider an
  # `Arc<Self>` receiver when resolving a method on `Arc<dyn AddSignal>`:
  #
  #   backend.rs:199:88 error:0: No applicable methods for
  #     {alloc::sync::Arc<dyn signal_hook::iterator::backend::AddSignal, ..>}.add_signal
  #
  # Spelling the call as UFCS names the trait outright, so there is no receiver
  # autoderef to do and mrustc lowers it fine. Same class of fix as the crc and
  # chrono turbofishes: say what mrustc cannot infer, change nothing else.
  #
  # Not avoidable by dropping a feature - crossterm's `events` needs
  # signal-hook-mio for SIGWINCH, and that genuinely imports
  # `signal_hook::iterator::backend`, so the module is load bearing.
  local f="$VENDOR_DIR/signal-hook/src/iterator/backend.rs"
  [ -f "$f" ] || return 0
  python3 - "$f" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
old = "Arc::clone(&self.pending).add_signal(Arc::clone(&self.write), signal)"
new = "AddSignal::add_signal(Arc::clone(&self.pending), Arc::clone(&self.write), signal)"
if new in s:
    sys.exit(0)          # already patched
if old not in s:
    sys.stderr.write("signal-hook: expected call site not found; skipping patch\n")
    sys.exit(0)
open(p, "w").write(s.replace(old, new, 1))
PY
  note "applied vendored-source workarounds (signal-hook UFCS add_signal)."
}

patch_signal_hook_mio_vendor() {
  # `implement_signals_with_pipe!` binds its `$pipe:path` argument with
  #
  #     use $pipe as Pipe;
  #
  # and mrustc cannot parse a `use` whose path is an interpolated fragment
  # followed by `as` - having consumed the path it insists on `::`:
  #
  #   signal-hook-mio/src/lib.rs:32:22 error:0:
  #     Unexpected token TOK_RWORD_AS, expected TOK_DOUBLE_COLON
  #
  # `Pipe` is only ever used as a *type* here - `SignalDelivery<Pipe, E>` and
  # `Pipe::pair()` - so a type alias binds it identically and sidesteps the use
  # statement entirely. Every invocation passes a plain type path
  # (`mio::net::UnixStream`, `mio_uds::UnixStream`).
  #
  # This is the same `TOK_RWORD_AS` family as the libyml gap that keeps the
  # `yaml` feature off for this target; fixing the parser would likely clear
  # both, and is the better long-term answer. See docs/build-ppc-mrustc.md.
  local f="$VENDOR_DIR/signal-hook-mio/src/lib.rs"
  [ -f "$f" ] || return 0
  python3 - "$f" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
old = "        use $pipe as Pipe;"
new = "        type Pipe = $pipe;"
if new in s:
    sys.exit(0)          # already patched
if old not in s:
    sys.stderr.write("signal-hook-mio: expected `use $pipe as Pipe;` not found; skipping patch\n")
    sys.exit(0)
open(p, "w").write(s.replace(old, new, 1))
PY
  note "applied vendored-source workarounds (signal-hook-mio \$pipe type alias)."
}

patch_instability_vendor() {
  # `instability` builds its doc strings with `indoc::formatdoc!`, and a proc
  # macro that *forwards* a token from its input loses that token's hygiene
  # context crossing mrustc's proc-macro bridge. `formatdoc!` re-emits the
  # trailing arguments verbatim, so
  #
  #     formatdoc! {"... version {}.", version.trim_start_matches('v')}
  #
  # expands to a `format!` whose `version` carries an empty hygiene context and
  # no longer resolves to the `if let Some(ref version)` binding around it:
  #
  #   MACRO<::"alloc"::format> error:0: Couldn't find variable name 'version'
  #
  # (Confirmed with MRUSTC_DEBUG=Expand: the expansion is correct token-for-token
  # - `format!{"...{}.", version.trim_start_matches('v')}` - and the forwarded
  # ident is the only one carrying `/*Rust2021 /**/*/`.)
  #
  # `format!` is a builtin, so writing these three call sites as `format!` with
  # the string already unindented keeps the output byte-identical and takes the
  # proc macro out of the picture. Fixing the bridge's hygiene is the real
  # answer and is filed as an open item; it is a much larger change than this.
  local f
  f="$VENDOR_DIR/instability/src/stable.rs"
  [ -f "$f" ] || return 0
  python3 - "$VENDOR_DIR" <<'PY'
import sys
v = sys.argv[1]

edits = [
    (v + "/instability/src/stable.rs",
     '            formatdoc! {"\n'
     '                # Stability\n'
     '\n'
     '                This API was stabilized in version {}.",\n'
     '                version.trim_start_matches(\'v\')\n'
     '            }\n',
     '            format!(\n'
     '                "# Stability\\n\\nThis API was stabilized in version {}.",\n'
     '                version.trim_start_matches(\'v\')\n'
     '            )\n'),
    (v + "/instability/src/stable.rs",
     '            formatdoc! {"\n'
     '                # Stability\n'
     '\n'
     '                This API is stable."}\n',
     '            format!("# Stability\\n\\nThis API is stable.")\n'),
    (v + "/instability/src/unstable.rs",
     '        let doc = formatdoc! {"\n'
     '            # Stability\n'
     '\n'
     '            **This API is marked as unstable** and is only available when the `{feature_flag}`\n'
     '            crate feature is enabled. This comes with no stability guarantees, and could be changed\n'
     '            or removed at any time."};\n',
     '        let doc = format!(\n'
     '            "# Stability\\n\\n**This API is marked as unstable** and is only available when the `{feature_flag}`\\n'
     'crate feature is enabled. This comes with no stability guarantees, and could be changed\\n'
     'or removed at any time."\n'
     '        );\n'),
]

for path, old, new in edits:
    try:
        s = open(path).read()
    except IOError:
        continue
    if new in s:
        continue                      # already patched
    if old not in s:
        sys.stderr.write("instability: expected formatdoc block not found in %s; skipping\n" % path)
        continue
    open(path, "w").write(s.replace(old, new, 1))
PY
  note "applied vendored-source workarounds (instability formatdoc -> format)."
}

patch_zstd_safe_vendor() {
  # zstd-safe passes its `OutBufferWrapper` / `InBufferWrapper` to
  #
  #     fn ptr_mut<B>(ptr_void: &mut B) -> *mut B
  #
  # as `ptr_mut(&mut output)`. `B` is only pinned by *deref-coercing*
  # `&mut OutBufferWrapper` to `&mut ZSTD_outBuffer` (the wrapper's `DerefMut`
  # target), driven by what the enclosing zstd_sys call expects. mrustc gets as
  # far as the autoderef and then aborts inside the coercion, with a bare C++
  # assertion and no span:
  #
  #   autoderef: Deref OutBufferWrapper<..> into ZSTD_outBuffer_s
  #   check_unsize_tys: From? ZSTD_outBuffer_s
  #   mrustc: src/hir/type.hpp:236: as_Borrow(): Assertion `m_tag == TAG_Borrow' failed.
  #
  # (`add_coerce_borrow` assumes the node it is handed is a borrow; on this path
  # it is the dereffed struct.) Writing the deref out - `&mut *output` - pins `B`
  # directly and removes the coercion. Same class of fix as the crc and chrono
  # turbofishes: say what mrustc cannot infer, change nothing else.
  local f="$VENDOR_DIR/zstd-safe/src/lib.rs"
  [ -f "$f" ] || return 0
  python3 - "$f" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
n = 0
for name in ("output", "input"):
    old = "ptr_mut(&mut %s)" % name
    new = "ptr_mut(&mut *%s)" % name
    n += s.count(old)
    s = s.replace(old, new)
if n:
    open(p, "w").write(s)
elif "ptr_mut(&mut *" not in s:
    sys.stderr.write("zstd-safe: no ptr_mut(&mut ..) call sites found; skipping patch\n")
PY
  note "applied vendored-source workarounds (zstd-safe explicit deref at ptr_mut)."
}

patch_zstd_sys_vendor() {
  # zstd-sys is the ONLY crate in this graph that asks for `cc`'s `parallel`
  # feature, and `parallel` is what drags in cc's async build-command runner
  # (src/parallel/{async_executor,command_runner}.rs). mrustc's `async fn`
  # support does not produce a `Future` impl for the generated async block:
  #
  #   cc/src/parallel/command_runner.rs:175:1 error:0:
  #     Cannot find an impl of ::"core"::future::future::Future for async[...]
  #
  # cc gates the whole module on the feature (`#[cfg(feature = "parallel")]
  # mod parallel;`) and keeps a `#[cfg(not(feature = "parallel"))]` serial arm in
  # command_helpers.rs, so dropping the feature deletes every async construct in
  # the crate - there are none outside src/parallel/ - and leaves cc functionally
  # identical, only compiling the C files one at a time.
  #
  # cc is a BUILD-dependency (of bzip2-sys and zstd-sys) and never reaches the
  # PowerPC binary, so this costs build-script wall clock and nothing else.
  # Chosen over pinning cc back to a pre-async 1.0.x, which would mean
  # re-vendoring the whole graph to work around a crate that is not shipped.
  local f="$VENDOR_DIR/zstd-sys/Cargo.toml"
  [ -f "$f" ] || return 0
  python3 - "$f" <<'PY'
import sys
p = sys.argv[1]
lines = open(p).read().splitlines(True)
out, section, dropped = [], None, 0
for ln in lines:
    s = ln.strip()
    if s.startswith("[") and s.endswith("]"):
        section = s
    if section == "[build-dependencies.cc]" and s == 'features = ["parallel"]':
        dropped += 1
        continue
    out.append(ln)
if dropped:
    open(p, "w").writelines(out)
elif not any(l.strip() == "[build-dependencies.cc]" for l in lines):
    sys.stderr.write("zstd-sys: no [build-dependencies.cc] section; skipping patch\n")
PY
  note "applied vendored-source workarounds (zstd-sys drops cc/parallel)."
}

# ---- stage 5: transpile+compile the engine for the HOST (native proof) ------
stage_host() {
  banner "5. transpile+build rb-cli for the HOST (native $HOST_ARCH proof)"
  stamp_version
  cd "$MRUSTC_DIR"
  patch_crc_vendor
  patch_chrono_vendor
  patch_rustversion_vendor
  patch_signal_hook_vendor
  patch_signal_hook_mio_vendor
  patch_instability_vendor
  patch_zstd_safe_vendor
  patch_zstd_sys_vendor
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
  patch_crc_vendor
  patch_chrono_vendor
  patch_rustversion_vendor
  patch_signal_hook_vendor
  patch_signal_hook_mio_vendor
  patch_instability_vendor
  patch_zstd_safe_vendor
  patch_zstd_sys_vendor
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
  MRUSTC_TARGET_VER="$MRUSTC_TARGET_VER" MINICARGO_DEFER_CODEGEN=1 \
  MINICARGO_NO_DEBUG_ASSERTIONS=1 \
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
  stamp_version
  [ -n "$PPC_HOST" ] || die "PPC_HOST is not set (e.g. PPC_HOST=admin@g5.local)"
  [ -x "$PPC_CC_WRAPPER" ] || die "missing $PPC_CC_WRAPPER"
  # Only this stage reaches the -sys crates, so only this stage needs an archiver.
  [ -x "$PPC_AR_WRAPPER" ] || die "missing $PPC_AR_WRAPPER"
  [ -e "$PPC_LIBS/libstd.rlib.o" ] || die "PPC libstd missing -- run 'ppclibs' first"
  cd "$MRUSTC_DIR"
  patch_crc_vendor
  patch_chrono_vendor
  patch_rustversion_vendor
  patch_signal_hook_vendor
  patch_signal_hook_mio_vendor
  patch_instability_vendor
  patch_zstd_safe_vendor
  patch_zstd_sys_vendor
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
# Copies the MacPorts dylibs the link named by absolute path and repoints them at @executable_path.
stage_dist() {
  banner "9. package a relocatable PowerPC tree"
  [ -n "$PPC_HOST" ] || die "PPC_HOST is not set"
  [ -e "$PPC_OUT/rb-cli" ] || die "no $PPC_OUT/rb-cli -- run 'ppc' first"
  # install_name_tool and otool only exist on the Mac, so the rewrite happens there.
  note "uploading rb-cli to $PPC_HOST for packaging"
  scp -q "$PPC_OUT/rb-cli" "$PPC_HOST:~/rb-cli-dist-src" || die "could not upload rb-cli"
  ssh "$PPC_HOST" "bash -s" <<'REMOTE' || die "packaging failed on $PPC_HOST"
set -e
BIN=~/rb-cli-dist-src
D=~/rb-cli-dist
rm -rf "$D"; mkdir -p "$D/lib"
cp "$BIN" "$D/rb-cli"; chmod u+w "$D/rb-cli"

# Walk the closure, not just the direct deps: libgcc_s.1.dylib is a stub in front of two more.
pending=$(otool -L "$D/rb-cli" | tail -n +2 | awk '{print $1}' | grep '^/opt/local/' || true)
seen=""
while [ -n "$pending" ]; do
  next=""
  for f in $pending; do
    case " $seen " in *" $f "*) continue;; esac
    seen="$seen $f"
    [ -e "$f" ] || { echo "missing dependency $f" >&2; exit 1; }
    cp "$f" "$D/lib/"; chmod u+w "$D/lib/$(basename "$f")"
    next="$next $(otool -L "$f" | tail -n +2 | awk '{print $1}' | grep '^/opt/local/' || true)"
  done
  pending="$next"
done

# MacPorts' host build imports fstat$INODE64 from libSystem, which Tiger lacks; the 10.4 SDK rebuild binds plain symbols and still exports both names.
LEG=$(ls -t /opt/local/var/macports/distfiles/legacy-support/macports-legacy-support-*.tar.gz 2>/dev/null | head -1)
SDK104=/Developer/SDKs/MacOSX10.4u.sdk
CACHE=~/.rb-cli-legacy104/libMacportsLegacySupport.dylib
if [ ! -e "$CACHE" ] && [ -n "$LEG" ] && [ -d "$SDK104" ]; then
  rm -rf /tmp/rb-legacy104 && mkdir -p /tmp/rb-legacy104 && cd /tmp/rb-legacy104
  tar xzf "$LEG"
  cd macports-legacy-support-*/
  MACOSX_DEPLOYMENT_TARGET=10.4 make \
    CC=/opt/local/libexec/gcc10-bootstrap/bin/gcc \
    CFLAGS="-O2 -mmacosx-version-min=10.4 -isysroot $SDK104" \
    PREFIX=/opt/local -j2 >/tmp/rb-legacy104/build.log 2>&1 \
    && mkdir -p "$(dirname "$CACHE")" && cp lib/libMacportsLegacySupport.dylib "$CACHE"
  cd ~
fi
if [ -e "$CACHE" ] && [ -e "$D/lib/libMacportsLegacySupport.dylib" ]; then
  cp "$CACHE" "$D/lib/libMacportsLegacySupport.dylib"
  chmod u+w "$D/lib/libMacportsLegacySupport.dylib"
  echo "legacy-support: using the 10.4-targeted rebuild (Tiger-capable)"
elif [ -e "$D/lib/libMacportsLegacySupport.dylib" ]; then
  echo "legacy-support: no 10.4 rebuild available - bundle is Leopard-only" >&2
fi

for f in "$D"/lib/*.dylib; do
  install_name_tool -id "@executable_path/lib/$(basename "$f")" "$f"
done
for f in $seen; do
  b=$(basename "$f")
  install_name_tool -change "$f" "@executable_path/lib/$b" "$D/rb-cli" 2>/dev/null || true
  for g in "$D"/lib/*.dylib; do
    install_name_tool -change "$f" "@executable_path/lib/$b" "$g" 2>/dev/null || true
  done
done

left=$(otool -L "$D/rb-cli" "$D"/lib/*.dylib | grep -c '/opt/local' || true)
[ "$left" -eq 0 ] || { echo "$left /opt/local reference(s) survived packaging" >&2; exit 1; }
(cd "$D" && ./rb-cli --version >/dev/null) || { echo "packaged rb-cli does not run" >&2; exit 1; }
# Tiger's libSystem has no $INODE64 symbols, so any left here means 10.4 refuses the bundle at load.
i64=0
for f in "$D/rb-cli" "$D"/lib/*.dylib; do
  i64=$((i64 + $(nm -mu "$f" 2>/dev/null | grep INODE64 | grep -c 'from libSystem' || true)))
done
[ "$i64" -eq 0 ] && echo "no \$INODE64 imports from libSystem: Tiger-capable" \
                 || echo "$i64 \$INODE64 import(s) from libSystem: Leopard-only" >&2
rm -f ~/rb-cli-ppc.tar.gz
(cd ~ && tar czf rb-cli-ppc.tar.gz rb-cli-dist)
echo "bundled $(ls "$D/lib" | wc -l | tr -d ' ') dylib(s); $(ls -l ~/rb-cli-ppc.tar.gz | awk '{print $5}') bytes"
REMOTE
  mkdir -p "$RB_DIR/dist"
  scp -q "$PPC_HOST:~/rb-cli-ppc.tar.gz" "$RB_DIR/dist/rb-cli-ppc.tar.gz" || die "could not fetch the tarball"
  note "PowerPC bundle at $RB_DIR/dist/rb-cli-ppc.tar.gz (unpack and run; no install step)."
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
