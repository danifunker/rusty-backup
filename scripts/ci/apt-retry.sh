#!/usr/bin/env bash
# apt-get with a time limit and retries, for CI. A stalled mirror once held a runner for
# 90 minutes; each try is cut off after APT_TRY_SECONDS and retried up to APT_TRIES times.
# Usage: scripts/ci/apt-retry.sh update | install -y PKG... (any apt-get arguments).
set -u
tries=${APT_TRIES:-3}
limit=${APT_TRY_SECONDS:-300}
for i in $(seq 1 "$tries"); do
  if timeout "$limit" sudo env DEBIAN_FRONTEND=noninteractive apt-get \
      -o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 \
      -o Dpkg::Lock::Timeout=120 "$@"; then
    exit 0
  fi
  echo "::warning::apt-get $1 failed or stalled (try $i of $tries)"
  # A try cut off mid-install leaves dpkg half done; finish that before the next one.
  sudo dpkg --configure -a || true
  [ "$i" -lt "$tries" ] && sleep 15
done
echo "::error::apt-get $* failed $tries times"
exit 1
