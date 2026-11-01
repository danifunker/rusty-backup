#!/usr/bin/env bash
# Run every emulator scenario in regression-tests/emulator/cases.txt: rb-cli
# changes a real volume, then the real operating system (FreeBSD, QNX on the
# ICON, ...) judges it.
#
#   regression-tests/scripts/regress-emulators.sh [--controls] [ID-SUBSTRING]
#
# --controls runs each scenario as its control (RB_EMU_CONTROL=1: the volume is
# damaged after the edits) and passes only when every checker catches it. Run
# it after changing an oracle or a scenario: a check that cannot fail proves nothing.
#
# Environment: RB_CLI (the rb-cli under test, default <repo>/target/release/rb-cli),
# TMPDIR (work copies of fixtures; put it on a big disk). Emulators and the
# fixture corpus come from regression-tests/local.toml.
#
# Exit: 0 nothing unexpected; 1 a new finding, a known one that now passes
# (close its bug), or a control nobody caught; 2 a scenario gave no verdict.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EMU="$(cd "$HERE/../emulator" && pwd)"
CONTROLS=0
if [ "${1:-}" = "--controls" ]; then CONTROLS=1; shift; fi
FILTER="${1:-}"

pass=0; fail=0; known=0; fixed=0; skip=0; nov=0
summary=""
while read -r id bug script args; do
    case "$id" in ''|'#'*) continue ;; esac
    [ -n "$FILTER" ] && [[ "$id" != *"$FILTER"* ]] && continue
    if [ "$CONTROLS" = 1 ] && [ "$bug" != "-" ]; then
        # It already fails on its own bug, so a damaged copy failing proves nothing.
        summary+=$(printf '  %-22s %s' "$id" "skip (known failure $bug)")$'\n'; skip=$((skip+1))
        continue
    fi
    printf '\n=== %s%s  (%s)\n' "$id" "$([ "$CONTROLS" = 1 ] && echo ' [control]')" "$(date '+%H:%M:%S')"
    # shellcheck disable=SC2086
    RB_EMU_CONTROL=$CONTROLS python3 "$EMU/$script" $args
    rc=$?
    if [ "$CONTROLS" = 1 ]; then
        case "$rc" in
            1)  verdict="control caught"; pass=$((pass+1)) ;;
            0)  verdict="CONTROL NOT CAUGHT: the checker passed a damaged volume"; fail=$((fail+1)) ;;
            77) verdict="skip (cannot run here)"; skip=$((skip+1)) ;;
            *)  verdict="NO VERDICT (exit $rc)"; nov=$((nov+1)) ;;
        esac
        summary+=$(printf '  %-22s %s' "$id" "$verdict")$'\n'
        continue
    fi
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
