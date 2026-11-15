# Sourced by scripts/build-ppc.sh: keeps $MRUSTC_DIR on the commit scripts/mrustc.pin names.
# Needs MRUSTC_DIR, RB_DIR, RUSTC_VERSION, and the script's note / die helpers.

# shellcheck source=mrustc.pin
. "$RB_DIR/scripts/mrustc.pin"

# The commit bin/mrustc was built from, written by stage_mrustc.
MRUSTC_BUILT_STAMP=".rb-built-commit"

mrustc_head() { git -C "$MRUSTC_DIR" rev-parse HEAD 2>/dev/null; }

# Refuse to build from anything but the pinned commit, built.
mrustc_pin_check() {
  [ "${MRUSTC_UNPINNED:-0}" = 1 ] && { note "MRUSTC_UNPINNED=1: not checking $MRUSTC_DIR against the pin"; return 0; }
  [ -d "$MRUSTC_DIR/.git" ] \
    || die "no mrustc checkout at $MRUSTC_DIR; run 'scripts/build-ppc.sh mrustc-sync' (scripts/mrustc.pin)"
  local head; head="$(mrustc_head)"
  [ "$head" = "$MRUSTC_COMMIT" ] \
    || die "mrustc at $MRUSTC_DIR is ${head:0:10}, the pin is ${MRUSTC_COMMIT:0:10} ($MRUSTC_BRANCH); run 'scripts/build-ppc.sh mrustc-sync'"
  [ "$(cat "$MRUSTC_DIR/$MRUSTC_BUILT_STAMP" 2>/dev/null)" = "$MRUSTC_COMMIT" ] \
    || die "bin/mrustc was not built from the pinned commit; run 'scripts/build-ppc.sh mrustc'"
}

# Clone the fork, or move an existing clean checkout to the pinned commit.
mrustc_pin_sync() {
  if [ ! -d "$MRUSTC_DIR/.git" ]; then
    note "cloning $MRUSTC_REPO ($MRUSTC_BRANCH) into $MRUSTC_DIR"
    git clone --branch "$MRUSTC_BRANCH" "$MRUSTC_REPO" "$MRUSTC_DIR" || die "clone failed"
  fi
  local old; old="$(mrustc_head)"
  [ "$old" = "$MRUSTC_COMMIT" ] && { note "mrustc already at the pin (${MRUSTC_COMMIT:0:10})"; return 0; }
  git -C "$MRUSTC_DIR" diff --quiet HEAD -- \
    || die "$MRUSTC_DIR has uncommitted changes; commit or stash them first"
  git -C "$MRUSTC_DIR" cat-file -e "$MRUSTC_COMMIT^{commit}" 2>/dev/null \
    || git -C "$MRUSTC_DIR" fetch "$MRUSTC_REPO" "$MRUSTC_BRANCH" \
    || die "fetch from $MRUSTC_REPO failed"
  git -C "$MRUSTC_DIR" cat-file -e "$MRUSTC_COMMIT^{commit}" 2>/dev/null \
    || die "$MRUSTC_COMMIT is not on $MRUSTC_REPO $MRUSTC_BRANCH; push it to the fork first"
  git -C "$MRUSTC_DIR" checkout -q -B "$MRUSTC_BRANCH" "$MRUSTC_COMMIT" || die "checkout failed"
  # A different std patch leaves the extracted source patched the old way: set it aside to re-extract.
  local src="$MRUSTC_DIR/rustc-${RUSTC_VERSION}-src"
  if [ -d "$src" ] && [ -n "$old" ] \
     && ! git -C "$MRUSTC_DIR" diff --quiet "$old" "$MRUSTC_COMMIT" -- "rustc-${RUSTC_VERSION}-src.patch"; then
    mv "$src" "$src.prev-${old:0:10}"
    note "the std patch changed; moved the old source to $(basename "$src").prev-${old:0:10}"
  fi
  note "mrustc now at ${MRUSTC_COMMIT:0:10} ($MRUSTC_BRANCH); next: 'scripts/build-ppc.sh mrustc'"
}
