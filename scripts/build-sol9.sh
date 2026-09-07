#!/usr/bin/env bash
#
# build-sol9.sh -- transpile the rusty-backup engine (rb-cli-sol9) to C via
# mrustc and cross-compile it to a Solaris 9 SPARC `rb-cli` + TUI.
#
# Unlike scripts/build-ppc.sh this is a ONE-machine pipeline. A cross gcc
# targeting Solaris 9 exists (mrustc's docker/sol9-cross, or ~/sol9-toolchain),
# so there is no remote compiler, no remote archiver and no split-TU step:
#
#     This machine: Rust --mrustc--> C99 --sparcv9-...-gcc--> SPARC ELF
#                                                                  |
#                                                       scp to the Blade
#
# Everything the target needs beyond the engine is already in place and is NOT
# built here -- see docs/build-sol9-mrustc.md:
#   * the mrustc target `sparcv9-sun-solaris2.9` (branch sparc-solaris-10)
#   * the cross toolchain at $SOL9_TOOLCHAIN
#   * the Rust stdlib at $MRUSTC_DIR/output-$RUSTC_VERSION-$SOL9_TARGET
#
# Usage:
#   scripts/build-sol9.sh              # every stage in order
#   scripts/build-sol9.sh <stage>      # one stage
#   stages: check vendor sol9libs hostc sol9 dist smoke
#     check    - prove the toolchain, stdlib and shim are all present and sane
#     vendor   - cargo vendor + apply the mrustc workaround patches
#     sol9libs - build the target stdlib (already built; only needed after an
#                mrustc change -- see the rebuild-discipline note below)
#     hostc    - emit the engine's C for the HOST, no target libc needed; the
#                fastest proof the whole engine transpiles
#     sol9     - transpile + cross-compile + link rb-cli for Solaris 9
#     dist     - package a relocatable tarball into dist/
#     smoke    - copy to the Blade and run it (needs SOL9_HOST)
#
set -euo pipefail

# ---- config -----------------------------------------------------------------
MRUSTC_DIR="${MRUSTC_DIR:-$HOME/repos/mrustc}"
RB_DIR="${RB_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
# rb-cli-sol9 reuses ../src but carries the dep deviations mrustc and Solaris 9 force.
CRATE_DIR="$RB_DIR/rb-cli-sol9"
VENDOR_DIR="$CRATE_DIR/vendor"

RUSTC_VERSION="${RUSTC_VERSION:-1.74.0}"
export MRUSTC_TARGET_VER="${MRUSTC_TARGET_VER:-1.74}"
SOL9_TARGET="${SOL9_TARGET:-sparcv9-sun-solaris2.9}"

# No os-stub (os/mod.rs's fallback arms already select) and no yaml; see rb-cli-sol9/Cargo.toml.
FEATURES="${FEATURES:-native-zstd,remote,tui,rust173-polyfill}"

SOL9_TOOLCHAIN="${SOL9_TOOLCHAIN:-$HOME/sol9-toolchain}"
SOL9_BIN="${SOL9_BIN:-$SOL9_TOOLCHAIN/opt/bin}"
SOL9_SYSROOT="${SOL9_SYSROOT:-$SOL9_TOOLCHAIN/sysroot}"
# The C shim must reach the final link line, so default it here rather than in the environment.
SOL9_SHIM="${SOL9_SHIM:-$CRATE_DIR/shim/sol9-compat.c}"

# Rust's unwind crate names libgcc_s.so.1 in a #[link], defeating -static-libgcc; ship it alongside.
SOL9_LIBGCC="${SOL9_LIBGCC:-$SOL9_TOOLCHAIN/opt/$SOL9_TARGET/lib/sparcv9/libgcc_s.so.1}"

SOL9_LIBS="${SOL9_LIBS:-$MRUSTC_DIR/output-$RUSTC_VERSION-$SOL9_TARGET}"
SOL9_OUT="${SOL9_OUT:-$MRUSTC_DIR/output-rb-sol9}"
HOST_LIBS="${HOST_LIBS:-$MRUSTC_DIR/output-$RUSTC_VERSION}"
HOSTC_OUT="${HOSTC_OUT:-$MRUSTC_DIR/output-rb-sol9-hostc}"

# minicargo.mk picks OVERRIDE_SUFFIX from the host OS, so name the Solaris set explicitly.
SOL9_OVERRIDE_SUFFIX="${SOL9_OVERRIDE_SUFFIX:--solaris}"
SOL9_STD_ENV_ARCH="${SOL9_STD_ENV_ARCH:-sparc64}"

# Four jobs, the cap .cargo/config.toml sets; see docs/build-memory-crashes.md.
JOBS="${JOBS:-4}"

# Where the finished binary is run. Only `smoke` and `dist --push` need it.
SOL9_HOST="${SOL9_HOST:-}"

# Absolute path: the argv[0]-derived one is relative and fails to spawn from a crate cwd.
export MRUSTC_PATH="${MRUSTC_PATH:-$MRUSTC_DIR/bin/mrustc}"

# mrustc sanitises the whole triple into the variable name, so '.' becomes '_' too.
CC_VAR="CC_$(echo "$SOL9_TARGET" | tr '.-' '__')"
AR_VAR="AR_$(echo "$SOL9_TARGET" | tr '.-' '__')"
CFLAGS_VAR="CFLAGS_$(echo "$SOL9_TARGET" | tr '.-' '__')"

# ---- version stamp -----------------------------------------------------------
# APP_VERSION lives in a marker file; only RELEASE_VERSION bakes a new one in.
stamp_version() {
  local marker="$SOL9_OUT/.release-version"
  local prev=""
  [ -f "$marker" ] && prev="$(cat "$marker" 2>/dev/null)"
  if [ -z "${RELEASE_VERSION:-}" ]; then
    RELEASE_VERSION="${prev:-$(date -u +%Y-%m-%d-%H-%M)}"
  fi
  export RELEASE_VERSION
  mkdir -p "$SOL9_OUT" "$SOL9_OUT/host"

  if [ "$prev" = "$RELEASE_VERSION" ]; then
    note "APP_VERSION=$RELEASE_VERSION (unchanged)"
    return
  fi
  # First stamp on a markerless tree: adopt it without invalidating anything.
  if [ -z "$prev" ]; then
    printf '%s' "$RELEASE_VERSION" > "$marker"
    note "APP_VERSION=$RELEASE_VERSION (first stamp - existing objects left alone)"
    return
  fi
  rm -f "$SOL9_OUT"/host/build_rb-cli-sol9-*.txt
  printf '%s' "$RELEASE_VERSION" > "$marker"
  note "APP_VERSION=$RELEASE_VERSION (changed - build script re-runs, engine re-transpiles)"
}

banner() { printf '\n\033[1;36m==== %s ====\033[0m\n' "$*"; }
note()   { printf '\033[33m%s\033[0m\n' "$*"; }
die()    { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

# ---- the C compiler mrustc and cc-rs both see ------------------------------
# mrustc has no per-application link-flag hook, so a wrapper appends the shim and flags.
make_cc_wrapper() {
  mkdir -p "$SOL9_OUT"
  local shim_o="$SOL9_OUT/sol9-compat.o"
  if [ ! -e "$shim_o" ] || [ "$SOL9_SHIM" -nt "$shim_o" ]; then
    note "compiling the compat shim -> $shim_o"
    "$SOL9_BIN/$SOL9_TARGET-gcc" -std=gnu11 -m64 -mcpu=v9 -D__EXTENSIONS__ \
      -Wall -Wextra -c "$SOL9_SHIM" -o "$shim_o" \
      || die "the compat shim does not compile -- fix it before anything else"
  fi
  # A rebuilt shim does not make minicargo relink, so drop the binary to force it.
  if [ "$SOL9_SHIM" -nt "$SOL9_OUT/rb-cli" ] 2>/dev/null; then
    rm -f "$SOL9_OUT/rb-cli"
  fi
  local wrap="$SOL9_OUT/cc-wrapper.sh"
  cat > "$wrap" <<WRAP
#!/bin/sh
# Generated by scripts/build-sol9.sh -- do not edit; regenerate instead.
for a in "\$@"; do
    [ "\$a" = "-c" ] && exec "$SOL9_BIN/$SOL9_TARGET-gcc" "\$@"
done
exec "$SOL9_BIN/$SOL9_TARGET-gcc" "\$@" "$shim_o" '-R\$ORIGIN' ${SOL9_LDFLAGS:-}
WRAP
  chmod +x "$wrap"
  export "$CC_VAR=$wrap"
  export "$AR_VAR=$SOL9_BIN/$SOL9_TARGET-ar"
  # cc-rs runs on the host, so name the target compiler; __EXTENSIONS__ unhides POSIX.
  export TARGET_CC="$SOL9_BIN/$SOL9_TARGET-gcc"
  export TARGET_AR="$SOL9_BIN/$SOL9_TARGET-ar"
  export TARGET_CFLAGS="-std=gnu11 -m64 -mcpu=v9 -fPIC -D__EXTENSIONS__"
  export "$CFLAGS_VAR=$TARGET_CFLAGS"
}

# ---- stage: check -----------------------------------------------------------
stage_check() {
  banner "0. check the toolchain, the stdlib and the shim"
  [ -x "$SOL9_BIN/$SOL9_TARGET-gcc" ] || die "no cross gcc at $SOL9_BIN/$SOL9_TARGET-gcc (see docs/build-sol9-mrustc.md)"
  [ -d "$SOL9_SYSROOT" ]              || die "no sysroot at $SOL9_SYSROOT"
  [ -x "$MRUSTC_DIR/bin/mrustc" ]     || die "mrustc is not built at $MRUSTC_DIR/bin/mrustc"
  note "cross gcc: $("$SOL9_BIN/$SOL9_TARGET-gcc" --version | head -1)"

  # The target must exist in this mrustc, which means branch sparc-solaris-10.
  "$MRUSTC_DIR/bin/mrustc" --target "$SOL9_TARGET" --help >/dev/null 2>&1 \
    || note "(could not probe mrustc for $SOL9_TARGET -- if the build fails on an unknown target, check out branch sparc-solaris-10)"

  [ -e "$SOL9_LIBS/libstd.rlib" ] \
    || die "no target stdlib at $SOL9_LIBS -- run the 'sol9libs' stage"
  note "target stdlib: $(ls "$SOL9_LIBS"/*.rlib | wc -l | tr -d ' ') rlibs in $(basename "$SOL9_LIBS")"

  make_cc_wrapper
  note "shim OK, cc wrapper at $SOL9_OUT/cc-wrapper.sh ($CC_VAR)"
}

# ---- stage: vendor ----------------------------------------------------------
stage_vendor() {
  banner "1. cargo vendor rb-cli-sol9 deps -> $VENDOR_DIR"
  cd "$CRATE_DIR"
  cargo vendor --locked vendor >/dev/null 2>&1 || cargo vendor vendor >/dev/null
  note "vendored $(ls vendor | wc -l | tr -d ' ') crates."
  apply_vendor_patches
}

# The patch set is shared with the PowerPC build; every entry is an mrustc gap, not a PPC one.
apply_vendor_patches() {
  "$RB_DIR/scripts/apply-vendor-patches.py" --vendor-dir "$VENDOR_DIR"
}

# ---- stage: sol9libs --------------------------------------------------------
# minicargo.mk LIBS does not self-rebuild: delete $SOL9_LIBS after an mrustc codegen change.
stage_sol9libs() {
  banner "2. build the Solaris 9 stdlib -> $SOL9_LIBS"
  cd "$MRUSTC_DIR"
  make -f minicargo.mk LIBS \
    RUSTC_VERSION="$RUSTC_VERSION" \
    MRUSTC_TARGET="$SOL9_TARGET" \
    OVERRIDE_SUFFIX="$SOL9_OVERRIDE_SUFFIX" \
    STD_ENV_ARCH="$SOL9_STD_ENV_ARCH" \
    PARLEVEL="$JOBS"
  [ -e "$SOL9_LIBS/libstd.rlib" ] || die "target libstd not produced"
  note "stdlib ready ($(ls "$SOL9_LIBS"/*.rlib | wc -l | tr -d ' ') rlibs)."
}

# ---- stage: hostc -----------------------------------------------------------
# Does not transfer here: it builds for the host, whose os/linux.rs needs the nix this manifest drops.
stage_hostc() {
  banner "3. emit host C for the whole engine (deferred codegen) -> $HOSTC_OUT"
  case "$(uname -s)" in
    Linux) die "hostc does not work from a Linux host for this manifest -- os/linux.rs needs \`nix\`, which rb-cli-sol9 drops. Run 'sol9' instead; it builds the real target." ;;
  esac
  [ -e "$HOST_LIBS/libstd.rlib" ] || die "no host stdlib at $HOST_LIBS (make -f minicargo.mk LIBS)"
  [ -d "$VENDOR_DIR" ] || die "run 'vendor' first"
  cd "$MRUSTC_DIR"
  apply_vendor_patches
  mkdir -p "$HOSTC_OUT"
  MINICARGO_DEFER_CODEGEN=1 \
    bin/minicargo "$CRATE_DIR" \
      --vendor-dir "$VENDOR_DIR" \
      -L "$HOST_LIBS" \
      --output-dir "$HOSTC_OUT" \
      --no-default-features --features "$FEATURES" \
      -j "$JOBS"
  note "emitted $(ls "$HOSTC_OUT"/*.c 2>/dev/null | wc -l | tr -d ' ') .c files"
}

# ---- stage: sol9 ------------------------------------------------------------
stage_sol9() {
  banner "4. transpile + cross-compile rb-cli for $SOL9_TARGET"
  [ -d "$VENDOR_DIR" ] || die "run 'vendor' first"
  [ -e "$SOL9_LIBS/libstd.rlib" ] || die "no target stdlib -- run 'sol9libs' first"
  stamp_version
  make_cc_wrapper
  cd "$MRUSTC_DIR"
  apply_vendor_patches
  mkdir -p "$SOL9_OUT"
  bin/minicargo "$CRATE_DIR" \
    --vendor-dir "$VENDOR_DIR" \
    -L "$SOL9_LIBS" \
    --output-dir "$SOL9_OUT" \
    --target "$SOL9_TARGET" \
    --no-default-features --features "$FEATURES" \
    -j "$JOBS"
  # minicargo can exit 0 while deadlocked, so check for the binary, not the exit code.
  [ -e "$SOL9_OUT/rb-cli" ] || die "minicargo exited 0 but produced no $SOL9_OUT/rb-cli -- grep the log for 'BUG:' and for a deadlock listing"
  note "built: $(file "$SOL9_OUT/rb-cli" | cut -c1-120)"
}

# ---- stage: dist ------------------------------------------------------------
stage_dist() {
  banner "5. package a relocatable Solaris 9 tree"
  [ -e "$SOL9_OUT/rb-cli" ] || die "no $SOL9_OUT/rb-cli -- run 'sol9' first"
  local stage="$SOL9_OUT/dist/rb-cli-sol9"
  rm -rf "$SOL9_OUT/dist"; mkdir -p "$stage"
  cp "$SOL9_OUT/rb-cli" "$stage/"
  "$SOL9_BIN/$SOL9_TARGET-strip" "$stage/rb-cli" 2>/dev/null || true
  [ -e "$SOL9_LIBGCC" ] || die "no libgcc_s.so.1 at $SOL9_LIBGCC -- the bundle would not run on a stock Solaris 9"
  cp "$SOL9_LIBGCC" "$stage/"
  cat > "$stage/README.txt" <<'TXT'
rusty-backup rb-cli for Solaris 9 (SPARC V9, 64-bit)

Unpack anywhere and run ./rb-cli --help. No install step.

  ./rb-cli inspect <image>      inspect a disk image
  ./rb-cli tui                  full-screen browser
  ./rb-cli --help               everything else

libgcc_s.so.1 ships alongside because Solaris 9 has none of its own and Rust's
unwinder needs it. rb-cli finds it next to itself, so keep the two together;
nothing has to be installed system-wide.

Built with mrustc against a Solaris 9 sysroot. Raw device access is not
available on this platform; disk *images* work normally.
TXT
  ( cd "$SOL9_OUT/dist" && tar czf "$RB_DIR/dist/rb-cli-sol9.tar.gz" rb-cli-sol9 )
  note "bundle at $RB_DIR/dist/rb-cli-sol9.tar.gz ($(du -h "$RB_DIR/dist/rb-cli-sol9.tar.gz" | cut -f1))"
}

# ---- stage: smoke -----------------------------------------------------------
# SunSSH wants a SHA-1 RSA signature; a locked key fails as "Permission denied (publickey)".
stage_smoke() {
  banner "6. run it on $SOL9_HOST"
  [ -n "$SOL9_HOST" ] || die "SOL9_HOST is not set (e.g. SOL9_HOST=user@192.168.99.176)"
  [ -e "$SOL9_OUT/rb-cli" ] || die "no $SOL9_OUT/rb-cli -- run 'sol9' first"
  export SSH_AUTH_SOCK="${SSH_AUTH_SOCK_SOL9:-/run/user/$(id -u)/gcr/ssh}"
  scp -q "$SOL9_OUT/rb-cli" "$SOL9_HOST:~/rb-cli" || die "could not upload rb-cli"
  ssh "$SOL9_HOST" 'uname -a && ./rb-cli --version && ./rb-cli --help >/dev/null && echo "SMOKE OK"' \
    || die "the binary does not run on the target"
}

# ---- driver -----------------------------------------------------------------
main() {
  mkdir -p "$RB_DIR/dist"
  case "${1:-all}" in
    check)    stage_check ;;
    vendor)   stage_vendor ;;
    sol9libs) stage_sol9libs ;;
    hostc)    stage_hostc ;;
    sol9)     stage_sol9 ;;
    dist)     stage_dist ;;
    smoke)    stage_smoke ;;
    all)      stage_check; stage_vendor; stage_sol9; stage_dist ;;
    *)        die "unknown stage '$1' (check vendor sol9libs hostc sol9 dist smoke)" ;;
  esac
}

main "$@"
