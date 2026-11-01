#!/usr/bin/env bash
# Run every emulator scenario in regression-tests/emulator/cases.txt: rb-cli
# changes a real volume, then the real operating system (FreeBSD, QNX on the
# ICON, ...) judges it.
#
#   regression-tests/scripts/regress-emulators.sh [ID-SUBSTRING]
#
# Environment: RB_CLI (the rb-cli under test, default <repo>/target/release/rb-cli),
# TMPDIR (work copies of fixtures; put it on a big disk). Emulators and the
# fixture corpus come from regression-tests/local.toml.
#
# Exit: 0 nothing unexpected; 1 a new finding, or a known one that now passes
# (close its bug); 2 a scenario gave no verdict.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EMU="$(cd "$HERE/../emulator" && pwd)"
FILTER="${1:-}"

pass=0; fail=0; known=0; fixed=0; skip=0; nov=0
summary=""
while read -r id bug script args; do
    case "$id" in ''|'#'*) continue ;; esac
    [ -n "$FILTER" ] && [[ "$id" != *"$FILTER"* ]] && continue
    printf '\n=== %s  (%s)\n' "$id" "$(date '+%H:%M:%S')"
    # shellcheck disable=SC2086
    python3 "$EMU/$script" $args
    rc=$?
    case "$rc" in
        0)  if [ "$bug" = "-" ]; then verdict=PASS; pass=$((pass+1))
            else verdict="PASS (known $bug no longer fails: close it)"; fixed=$((fixed+1)); fi ;;
        1)  if [ "$bug" = "-" ]; then verdict=FAIL; fail=$((fail+1))
            else verdict="known failure ($bug)"; known=$((known+1)); fi ;;
        77) verdict="skip (cannot run here)"; skip=$((skip+1)) ;;
        *)  verdict="NO VERDICT (exit $rc)"; nov=$((nov+1)) ;;
    esac
    summary+=$(printf '  %-22s %s' "$id" "$verdict")$'\n'
done < "$EMU/cases.txt"

printf '\n=== summary\n%s' "$summary"
printf '  %d pass, %d fail, %d known failure(s), %d fixed, %d skipped, %d without a verdict\n' \
    "$pass" "$fail" "$known" "$fixed" "$skip" "$nov"
[ "$fail" -gt 0 ] || [ "$fixed" -gt 0 ] && exit 1
[ "$nov" -gt 0 ] && exit 2
exit 0
