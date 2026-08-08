#!/usr/bin/env bash
# Drive a full cross-OS regression with no human in the loop.
#
#   ./scripts/regress-all.sh [--skip-remote] [--no-pull]
#
# Runs on the orchestrating host (Windows via Git Bash, or any Unix). For every
# host in local.toml (see below) it pulls, builds, and runs `run` + `produce`;
# then it collects every artifact tree here, compares them with `parity`, runs
# `verify` locally, and consolidates.
#
# Exit code is the point. 0 means "nothing a human needs to look at": known
# failures are on the bug list and everything else passed. Non-zero means
# something changed. See §Exit codes below.
#
# The host list comes from `rb-regress query hosts`, which reads the gitignored
# regression-tests/local.toml — the one file that names real machines. It emits
# tab-separated `id  ssh-target  repo-path  shell` for every host that has both
# an `ssh` and a `repo`. See local.toml.example.
#
# The shell column matters: a non-interactive ssh session on macOS gets
# /usr/bin:/bin only, so Homebrew's cargo is invisible without a login shell.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGRESS="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$REGRESS/.." && pwd)"

SKIP_REMOTE=0
PULL=1
for a in "$@"; do
    case "$a" in
        --skip-remote) SKIP_REMOTE=1 ;;
        --no-pull)     PULL=0 ;;
        *) echo "unknown argument: $a" >&2; exit 2 ;;
    esac
done

# Git Bash cannot see a Windows named-pipe ssh-agent, so the Windows OpenSSH
# client is the one that can use the agent's keys. Falls back to plain ssh
# elsewhere. IdentitiesOnly=no because a global `IdentitiesOnly yes` in
# ~/.ssh/config would otherwise refuse to offer agent keys at all.
WIN_SSH="/c/Windows/System32/OpenSSH/ssh.exe"
if [ -x "$WIN_SSH" ]; then
    SSH="$WIN_SSH -o BatchMode=yes -o IdentitiesOnly=no"
    SCP="/c/Windows/System32/OpenSSH/scp.exe -o BatchMode=yes -o IdentitiesOnly=no"
else
    SSH="ssh -o BatchMode=yes"
    SCP="scp -o BatchMode=yes"
fi

RB="$REGRESS/runner/target/release/rb-regress"
[ -x "$RB.exe" ] && RB="$RB.exe"

FAILED=0
step() { printf '\n=== %s\n' "$*"; }
warn() { printf 'WARN: %s\n' "$*" >&2; }
die()  { printf 'ERROR: %s\n' "$*" >&2; exit 2; }

# --- local build -------------------------------------------------------------
step "local: build"
( cd "$REPO" && cargo build --release --bin rb-cli ) || die "local rb-cli build failed"
( cd "$REGRESS/runner" && cargo build --release )    || die "local runner build failed"

step "local: validate"
"$RB" validate || die "manifests or bug list are invalid"

# Fixture inventory drives test selection, so it runs BEFORE anything else.
# A corrupt fixture or a case naming a fixture the catalogue has never heard of
# makes every later result suspect, so those exit non-zero here. A merely
# missing fixture does not: the corpus is expected to be incomplete, and the
# blocked list says exactly what to fetch and what it would buy.
#
# RB_SYNC_FROM pre-fills the local corpus from a share first, so the run reads
# local disk rather than SMB.
step "local: fixture inventory"
"$RB" fixtures     ${RB_SYNC_FROM:+--sync-from "$RB_SYNC_FROM"}     ${RB_FIXTURE_ROOT:+--fixture-root "$RB_FIXTURE_ROOT"}     || die "fixture corpus is not trustworthy - see above"

# --- remote hosts ------------------------------------------------------------
# A host that cannot be reached is a warning, not a failure: the point is to
# reach the end and report, and a laptop being off is not a regression in
# rb-cli. It IS recorded, so a silently-absent host cannot be mistaken for a
# host that passed.
UNREACHABLE=""
if [ "$SKIP_REMOTE" -eq 0 ]; then
    while IFS="$(printf '	')" read -r id target path shell_cmd; do
        [ -n "$id" ] || continue
        step "$id: pull, build, run, produce"

        if ! $SSH "$target" true 2>/dev/null; then
            warn "$id ($target) unreachable - skipping"
            UNREACHABLE="$UNREACHABLE $id"
            continue
        fi

        pull_cmd=""
        [ "$PULL" -eq 1 ] && pull_cmd="git pull --ff-only &&"

        # -A forwards the agent so the remote can authenticate to GitHub
        # without a key of its own.
        remote="cd $path && $pull_cmd \
            cargo build --release --bin rb-cli && \
            (cd regression-tests/runner && cargo build --release) && \
            cd regression-tests && \
            ./runner/target/release/rb-regress run --tiers 0-4 ; RUN=\$? ; \
            ./runner/target/release/rb-regress produce ; PROD=\$? ; \
            echo \"RESULT $id run=\$RUN produce=\$PROD\""

        if [ "$shell_cmd" = "bash" ]; then
            out=$($SSH -A "$target" "$remote" 2>&1)
        else
            # Login shell, for hosts whose toolchain is not on the default PATH.
            out=$($SSH -A "$target" "$shell_cmd '$remote'" 2>&1)
        fi
        echo "$out" | tail -20

        line=$(echo "$out" | grep "^RESULT $id" | tail -1)
        if [ -z "$line" ]; then
            warn "$id: no RESULT line - the remote run did not complete"
            FAILED=1
            continue
        fi
        echo "$line" | grep -q "run=0 produce=0" || { warn "$id: $line"; FAILED=1; }

        step "$id: collect artifacts"
        for os in linux macos windows; do
            rm -rf "$REGRESS/artifacts/$os.incoming"
            if $SCP -q -r "$target:$path/regression-tests/artifacts/$os" \
                 "$REGRESS/artifacts/$os.incoming" 2>/dev/null; then
                rm -rf "$REGRESS/artifacts/$os"
                mv "$REGRESS/artifacts/$os.incoming" "$REGRESS/artifacts/$os"
                echo "  collected $os from $id"
            fi
            rm -rf "$REGRESS/artifacts/$os.incoming"
        done
    done <<EOF
$("$RB" query hosts 2>/dev/null)
EOF
    if [ -z "$("$RB" query hosts 2>/dev/null)" ]; then
        warn "no ssh-reachable host in local.toml - running locally only"
    fi
fi

# --- local run ---------------------------------------------------------------
step "local: run"
"$RB" run --tiers 0-4 ${RB_FIXTURE_ROOT:+--fixture-root "$RB_FIXTURE_ROOT"} || FAILED=1

step "local: produce"
"$RB" produce || FAILED=1

step "parity (cross-OS byte comparison)"
"$RB" parity || FAILED=1

step "verify (this host's oracles, over every producer's artifacts)"
"$RB" verify || FAILED=1

step "consolidate"
"$RB" consolidate

# --- verdict -----------------------------------------------------------------
# Exit codes:
#   0  nothing to look at - known failures only, parity clean, oracles agree
#   1  something changed - an unexpected failure, an XPASS, a parity divergence,
#      or a host that did not finish
#   2  the harness could not run at all
printf '\n========================================\n'
if [ -n "$UNREACHABLE" ]; then
    echo "unreachable hosts:$UNREACHABLE"
fi
if [ "$FAILED" -eq 0 ]; then
    echo "RESULT: clean - only known failures, parity matched, oracles agreed"
else
    echo "RESULT: attention needed - see the sections above"
fi
exit $FAILED
