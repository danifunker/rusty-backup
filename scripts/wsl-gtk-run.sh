#!/bin/bash
# Run a command where Xvfb works without root, for the GTK flows under WSL:
#   scripts/wsl-gtk-run.sh regression-tests/runner/target/release/rb-regress run --shell gtk --tiers 8
# A user namespace overlays ~/gtkenv's tools on /usr/bin (Xvfb runs xkbcomp from there) and
# gives a private /tmp/.X11-unix (WSLg's is read-only). Set up first with scripts/wsl-gtk-env.sh.
set -euo pipefail

export GTKENV=${GTKENV:-$HOME/gtkenv}
[ -f "$GTKENV/env.sh" ] || { echo "no $GTKENV/env.sh; run scripts/wsl-gtk-env.sh first" >&2; exit 1; }

exec unshare --user --map-root-user --mount bash -c '
  set -e
  W=$(mktemp -d /tmp/rbns.XXXXXX)
  mkdir -p "$W/upper" "$W/work"
  mount -t overlay overlay \
    -o "lowerdir=/usr/bin:$GTKENV/root/usr/bin,upperdir=$W/upper,workdir=$W/work" /usr/bin
  mount -t tmpfs tmpfs /tmp/.X11-unix && chmod 1777 /tmp/.X11-unix
  . "$GTKENV/env.sh"
  unset DISPLAY WAYLAND_DISPLAY
  exec "$@"
' run "$@"
