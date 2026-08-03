#!/bin/bash
# ppc-package.sh -- runs ON the PowerPC Mac: packages ~/rb-cli-dist-src into a
# relocatable ~/rb-cli-ppc-<cpu>.tar.gz. Copies the MacPorts dylib closure,
# repoints it at @executable_path, rebuilds legacy-support against the 10.4
# SDK, enforces the CPU floor (AltiVec / cpusubtype) and checks Tiger-capability.
#
# Driven by build-ppc.sh's `dist` stage over `ssh bash -s`, but standalone on
# purpose: scp it to the Mac and run it by hand when packaging rejects a build.
#
#   env: RB_CPU (e.g. 750), RB_CPU_FLAGS (gcc flags), RB_CPU_LABEL (g3|g4|g5)
#   in:  ~/rb-cli-dist-src        the freshly linked rb-cli
#   out: ~/rb-cli-ppc-$RB_CPU_LABEL.tar.gz
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
# Cache per CPU: the flags change the cpusubtype, so one cache would hand a
# ppc7400 dylib to a 750-targeted bundle and the G3 would refuse it at load.
CACHE=~/.rb-cli-legacy104/libMacportsLegacySupport-${RB_CPU:-default}.dylib
if [ ! -e "$CACHE" ] && [ -n "$LEG" ] && [ -d "$SDK104" ]; then
  rm -rf /tmp/rb-legacy104 && mkdir -p /tmp/rb-legacy104 && cd /tmp/rb-legacy104
  tar xzf "$LEG"
  cd macports-legacy-support-*/
  MACOSX_DEPLOYMENT_TARGET=10.4 make \
    CC=/opt/local/libexec/gcc10-bootstrap/bin/gcc \
    CFLAGS="-O2 -mmacosx-version-min=10.4 -isysroot $SDK104 $RB_CPU_FLAGS" \
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

# ---- CPU floor: retag what is only mis-labelled, reject what is not ---------
# Two independent things decide whether the bundle runs on the target CPU: the
# cpusubtype tag (Darwin grades it at exec, so a ppc7400 binary never reaches
# main() on a 750) and the vector instructions actually present. MacPorts'
# prebuilt dylibs are compiled -mcpu=7400 and carry that tag even when they hold
# no vector code at all, so those are retagged rather than rebuilt.
VEC_RE='^[0-9a-f]+[[:space:]]+(v[a-z0-9_]+|lvx|lvxl|stvx|stvxl|lvebx|lvehx|lvewx|stvebx|stvehx|stvewx|lvsl|lvsr|dst|dstt|dstst|dststt|dss|dssall|mfvscr|mtvscr)[[:space:]]'
altivec_count() { otool -tv "$1" 2>/dev/null | grep -cE "$VEC_RE" || true; }
# libgcc's save_world/rest_world do carry vector code, but branch over it when
# libSystem's __cpu_has_altivec is 0. Importing that symbol is the proof, so the
# check verifies the gating instead of hardcoding a list of known-safe dylibs.
cpu_gated() { nm -mu "$1" 2>/dev/null | grep -q '__cpu_has_altivec'; }
retag_ppc_all() {
  # cpusubtype is the 3rd big-endian word of a Mach-O header; 0 is POWERPC_ALL.
  [ "$(od -An -tx1 -N4 "$1" | tr -d ' \n')" = "feedface" ] || return 1
  printf '\0\0\0\0' | dd of="$1" bs=1 seek=8 count=4 conv=notrunc 2>/dev/null
}

case "$RB_CPU_FLAGS" in *-mno-altivec*) NOVEC=1 ;; *) NOVEC=0 ;; esac
fail=0
echo "CPU floor: PPC_CPU=${RB_CPU:-default} ($RB_CPU_FLAGS)"
for f in "$D/rb-cli" "$D"/lib/*.dylib; do
  n=$(altivec_count "$f")
  gated=""
  if [ "$n" -gt 0 ] && cpu_gated "$f"; then gated=" (runtime-gated)"; fi
  if [ "$NOVEC" = 1 ] && [ "$n" -gt 0 ] && [ -z "$gated" ]; then
    echo "  FAIL $(basename "$f"): $n AltiVec instruction(s), not runtime-gated" >&2
    fail=1
  fi
  arch=$(lipo -info "$f" 2>/dev/null | sed 's/.*: //')
  if [ "$NOVEC" = 1 ] && [ "$n" -eq 0 ]; then
    case "$arch" in
      ppc|ppc750|ppc601|ppc603*|ppc604*) ;;
      *) retag_ppc_all "$f" && arch="$(lipo -info "$f" | sed 's/.*: //') (retagged)" ;;
    esac
  fi
  printf '  %-36s %-22s altivec=%s%s\n' "$(basename "$f")" "$arch" "$n" "$gated"
done
[ "$fail" -eq 0 ] || { echo "bundle carries unguarded AltiVec for this CPU floor" >&2; exit 1; }

(cd "$D" && ./rb-cli --version >/dev/null) || { echo "packaged rb-cli does not run" >&2; exit 1; }
# Tiger's libSystem has no $INODE64 symbols, so any left here means 10.4 refuses the bundle at load.
i64=0
for f in "$D/rb-cli" "$D"/lib/*.dylib; do
  i64=$((i64 + $(nm -mu "$f" 2>/dev/null | grep INODE64 | grep -c 'from libSystem' || true)))
done
[ "$i64" -eq 0 ] && echo "no \$INODE64 imports from libSystem: Tiger-capable" \
                 || echo "$i64 \$INODE64 import(s) from libSystem: Leopard-only" >&2

# The link records the *build box's* compat version for each system dylib, so a Leopard-only
# version requirement gets baked in silently and 10.4 refuses the binary at load.
SDK104=/Developer/SDKs/MacOSX10.4u.sdk
if [ -d "$SDK104" ]; then
  otool -L "$D/rb-cli" | tail -n +2 | grep -E '^\	(/usr/lib|/System)' | while read -r path rest; do
    want=$(echo "$rest" | sed -n 's/.*compatibility version \([0-9.]*\).*/\1/p')
    sdklib="$SDK104$path"
    [ -e "$sdklib" ] || continue
    have=$(otool -L "$sdklib" 2>/dev/null | sed -n "2s/.*compatibility version \([0-9.]*\).*/\1/p")
    [ -n "$want" ] && [ -n "$have" ] || continue
    if [ "$(printf '%s\n%s\n' "$want" "$have" | sort -t. -k1,1n -k2,2n -k3,3n | head -1)" != "$want" ]; then
      echo "10.4 will refuse $path: needs $want, Tiger has $have" >&2
    fi
  done
fi
TARBALL=~/rb-cli-ppc-${RB_CPU_LABEL:-unknown}.tar.gz
rm -f "$TARBALL"
(cd ~ && tar czf "$TARBALL" rb-cli-dist)
echo "bundled $(ls "$D/lib" | wc -l | tr -d ' ') dylib(s); $(ls -l "$TARBALL" | awk '{print $5}') bytes -> $(basename "$TARBALL")"
