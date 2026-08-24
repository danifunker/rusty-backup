#!/usr/bin/env bash
# rb-regress check for the `motion` oracle: hand an EFS v1 volume we produced
# to a real IRIX 3.7 and see whether its kernel mounts it.
#
# Thin on purpose. The boot machinery lives in scripts/sgi-efs-v1-oracle.sh,
# which developers also drive by hand; duplicating it here is how the two would
# drift. This resolves the repo root from its own location and forwards.
#
# Usage: efs_v1_mount.sh <volume.img>
# Exits 0 pass, 1 IRIX disagreed, 77 nothing to run it with, 99 no verdict.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
exec "$repo/scripts/sgi-efs-v1-oracle.sh" mount "$@"
