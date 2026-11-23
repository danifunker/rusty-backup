#!/usr/bin/env bash
# apt-get with a time limit and retries, for CI. A stalled mirror once held a runner for
# 90 minutes; each try is cut off after APT_TRY_SECONDS and retried up to APT_TRIES times.
# Usage: scripts/ci/apt-retry.sh update | install -y PKG... (any apt-get arguments).
set -u
[ $# -gt 0 ] || { echo "usage: $0 APT-GET-ARGUMENTS..." >&2; exit 2; }
tries=${APT_TRIES:-3}
limit=${APT_TRY_SECONDS:-300}
for i in $(seq 1 "$tries"); do
  # -k: a try that ignores the TERM is killed 30 s later, so the next try still runs.
  if timeout -k 30 "$limit" sudo env DEBIAN_FRONTEND=noninteractive apt-get \
      -o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 \
      -o Dpkg::Lock::Timeout=120 "$@"; then
    exit 0
  fi
  echo "::warning::apt-get $1 failed or stalled (try $i of $tries)"
  # A try cut off mid-install leaves dpkg half done; finish that before the next one.
  sudo dpkg --configure -a || true
  [ "$i" -lt "$tries" ] && sleep 15
done
# With one try the caller runs its own retry loop, so a failure is not yet the step's error.
level=error; [ "$tries" -gt 1 ] || level=warning
echo "::$level::apt-get $* failed $tries times"
exit 1
