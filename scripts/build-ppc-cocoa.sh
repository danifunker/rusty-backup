#!/usr/bin/env bash
# Builds "Rusty Backup.app" for PowerPC Macs (10.4+) - docs/cocoa_vintage_shell_plan.md § 3.
# The Objective-C host compiles on the PowerPC Mac with Apple's gcc-4.0 into librbcocoahost.a;
# the Rust half is the `rusty-backup-cocoa` bin of rb-cli-ppc, built by scripts/build-ppc.sh
# (mrustc here, C on the Mac), which links that archive. The bundle is put together and
# packed into a DMG on the Mac, since hdiutil there writes an image 10.4 can open.
#
#   PPC_HOST=admin@g5 scripts/build-ppc-cocoa.sh [host|app|relink|bundle|all]
#       -> dist/Rusty-Backup-Cocoa-macos-ppc-<cpu>-<version>.dmg
set -euo pipefail

RB_DIR="$(cd "$(dirname "$0")/.." && pwd)"
HERE="$RB_DIR/frontends-vintage/cocoa"
MRUSTC_DIR="${MRUSTC_DIR:-$HOME/repos/mrustc}"
PPC_HOST="${PPC_HOST:?set PPC_HOST, e.g. admin@g5.local}"
PPC_CPU="${PPC_CPU:-750}"
case "$PPC_CPU" in
  g3|G3|750) CPU_FLAGS="-mcpu=750"; PKG_FLAGS="-mcpu=750 -mno-altivec"; LABEL=g3 ;;
  g4|G4|7400|7450) CPU_FLAGS="-mcpu=7400"; PKG_FLAGS="-mcpu=7400 -maltivec"; LABEL=g4 ;;
  g5|G5|970) CPU_FLAGS="-mcpu=970"; PKG_FLAGS="-mcpu=970 -maltivec"; LABEL=g5 ;;
  *) echo "unknown PPC_CPU $PPC_CPU" >&2; exit 2 ;;
esac
SDK="${PPC_SDK:-/Developer/SDKs/MacOSX10.4u.sdk}"
# Per CPU: an archive built for one family would set the whole link's cpusubtype.
REMOTE="rb-cocoa-$LABEL"
PPC_OUT="$MRUSTC_DIR/output-rb-ppc-$LABEL"
VERSION="${RELEASE_VERSION:-$(sed -n 's/^version = "\(.*\)"/\1/p' "$RB_DIR/rb-cli-ppc/Cargo.toml" | head -1)}"

note() { printf '\033[33m%s\033[0m\n' "$*"; }

# The host's two files, compiled against the 10.4 SDK and archived on the Mac, where the link
# runs: PPC_LDFLAGS reach the Mac's gcc as they are, so its -L names the Mac's folder.
stage_host() {
  note "compiling the Objective-C host on $PPC_HOST"
  ssh "$PPC_HOST" "mkdir -p $REMOTE"
  for f in host.m main.m; do ssh "$PPC_HOST" "cat > $REMOTE/$f" < "$HERE/host/$f"; done
  ssh "$PPC_HOST" "cd $REMOTE && for m in host main; do \
      gcc-4.0 -arch ppc $CPU_FLAGS -mmacosx-version-min=10.4 -isysroot $SDK -DRB_RUST_MAIN \
        -Wall -Werror -Wno-deprecated-declarations -O2 -c \$m.m -o \$m.o || exit 1; done && \
      rm -f librbcocoahost.a && ar rcs librbcocoahost.a host.o main.o && ranlib librbcocoahost.a"
}

# The Rust half: rb-cli-ppc's bins, with the archive's directory on every link line.
stage_app() {
  ssh "$PPC_HOST" "test -f $REMOTE/librbcocoahost.a" || stage_host
  local home; home=$(ssh "$PPC_HOST" 'echo $HOME')
  PPC_LDFLAGS="${PPC_LDFLAGS:--L/opt/local/lib -latomic -lMacportsLegacySupport -lgcc_s.1} -L$home/$REMOTE" \
    PPC_HOST="$PPC_HOST" PPC_CPU="$PPC_CPU" "$RB_DIR/scripts/build-ppc.sh" ppc
  [ -e "$PPC_OUT/rusty-backup-cocoa" ] || { echo "no $PPC_OUT/rusty-backup-cocoa" >&2; exit 1; }
}

# After a host-only change: rebuild the archive and re-link the app; the engine stays as built.
stage_relink() {
  stage_host
  rm -f "$PPC_OUT/rusty-backup-cocoa"
  stage_app
}

# The bundle and its DMG, made on the Mac.
stage_bundle() {
  local app="$REMOTE/Rusty Backup.app" dmg="Rusty-Backup-Cocoa-macos-ppc-$LABEL-$VERSION.dmg"
  note "assembling the bundle on $PPC_HOST"
  ssh "$PPC_HOST" "rm -rf '$app' && mkdir -p '$app/Contents/MacOS' '$app/Contents/Resources'"
  ssh "$PPC_HOST" "cat > '$app/Contents/MacOS/Rusty Backup' && chmod +x '$app/Contents/MacOS/Rusty Backup'" \
    < "$PPC_OUT/rusty-backup-cocoa"
  sed -e "s/@VERSION@/$VERSION/g" -e 's|<string>10.7</string>|<string>10.4</string>|' \
    "$HERE/host/Info.plist" | ssh "$PPC_HOST" "cat > '$app/Contents/Info.plist'"
  printf 'APPL????' | ssh "$PPC_HOST" "cat > '$app/Contents/PkgInfo'"
  # The icons travel as one tar stream; the Mac's tar unpacks it into Resources.
  ( cd "$RB_DIR/assets/icons" && tar cf - icon.icns toolbar/png/*-48.png \
      rows/png/{file,file-plus,file-minus,folder,folder-plus,folder-minus,folder-open}-32.png ) |
    ssh "$PPC_HOST" "cd '$app/Contents/Resources' && tar xf - && mv toolbar/png/* rows/png/* . && rm -rf toolbar rows"
  # MacPorts' gcc10 runtime and legacy-support ride in Contents/Frameworks, as rb-cli's lib/ does.
  note "bundling the app's dylibs (scripts/ppc-package.sh)"
  ssh "$PPC_HOST" "RB_APP=\$HOME/'$app' RB_CPU=$PPC_CPU RB_CPU_FLAGS='$PKG_FLAGS' RB_CPU_LABEL=$LABEL bash -s" \
    < "$RB_DIR/scripts/ppc-package.sh" || { echo "packaging the app failed" >&2; exit 1; }
  ssh "$PPC_HOST" "cd $REMOTE && rm -rf dmgroot '$dmg' && mkdir dmgroot && cp -R 'Rusty Backup.app' dmgroot/ && \
      hdiutil create -quiet -fs HFS+ -volname 'Rusty Backup' -srcfolder dmgroot -format UDZO '$dmg' && rm -rf dmgroot"
  mkdir -p "$RB_DIR/dist"
  ssh "$PPC_HOST" "cat '$REMOTE/$dmg'" > "$RB_DIR/dist/$dmg"
  note "PowerPC app at $RB_DIR/dist/$dmg (and ~/$app on $PPC_HOST)"
}

case "${1:-all}" in
  host) stage_host ;;
  app) stage_app ;;
  relink) stage_relink ;;
  bundle) stage_bundle ;;
  all) stage_host; stage_app; stage_bundle ;;
  *) echo "usage: $0 [host|app|relink|bundle|all]" >&2; exit 2 ;;
esac
