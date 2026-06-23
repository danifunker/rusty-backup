#!/usr/bin/env bash
# NTFS formatter oracle harness.
#
# Cross-checks our clean-room NTFS formatter (src/fs/ntfs_format.rs) against the
# real NTFS tooling from ntfs-3g / ntfsprogs (mkntfs, ntfsfix, ntfsinfo). These
# tools operate on a plain image file (no root, no mount) for the read-only
# checks, so the matrix runs unprivileged.
#
# License note: we only *observe* mkntfs OUTPUT (a published format) — we never
# read or translate its GPL-2 source. See docs/ntfs_format_cleanroom_notes.md.
#
# Usage:
#   scripts/ntfs-oracle.sh deps                 # check the tools are installed
#   scripts/ntfs-oracle.sh make <img> <mib> [cluster] [sector] [label]
#                                               # format with OUR formatter
#   scripts/ntfs-oracle.sh fix <img>            # ntfsfix -n (read-only oracle)
#   scripts/ntfs-oracle.sh info <img>           # ntfsinfo -m
#   scripts/ntfs-oracle.sh mkntfs <img> <mib> <cluster> <sector>
#                                               # reference image via mkntfs
#   scripts/ntfs-oracle.sh cell <mib> <cluster> <sector>
#                                               # format + ntfsfix one geometry
#   scripts/ntfs-oracle.sh matrix               # full geometry matrix
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="${TMPDIR:-/tmp}/ntfs-oracle.$$"
mkdir -p "$TMP"
trap 'rm -rf "$TMP"' EXIT

make_ours() { # img mib [cluster] [sector] [label]
  cargo run -q --manifest-path "$ROOT/Cargo.toml" --example make_blank_ntfs -- \
    "$1" "$2" "${5:-RBNTFS}" "${3:-4096}" "${4:-512}"
}

cmd=${1:-help}
case "$cmd" in
  deps)
    ok=1
    for t in mkntfs ntfsfix ntfsinfo ntfs-3g; do
      if command -v "$t" >/dev/null 2>&1; then echo "OK   $t -> $(command -v "$t")"
      else echo "MISSING $t"; ok=0; fi
    done
    [ "$ok" = 1 ] || { echo "install ntfs-3g + ntfsprogs"; exit 1; }
    ;;
  make)  make_ours "$2" "$3" "${4:-}" "${5:-}" "${6:-}";;
  fix)   ntfsfix -n "$2";;
  info)  ntfsinfo -m "$2";;
  mkntfs)
    img=$2; mib=$3; clu=$4; sec=$5
    truncate -s "${mib}M" "$img"
    mkntfs -F -q -T -c "$clu" -s "$sec" -L REF "$img"
    ;;
  cell) # mib cluster sector
    mib=$2; clu=$3; sec=$4; img="$TMP/c${clu}_s${sec}_${mib}.img"
    make_ours "$img" "$mib" "$clu" "$sec" "RBCELL" >/dev/null
    if ntfsfix -n "$img" >"$TMP/log" 2>&1; then
      echo "PASS  ${mib}MiB cluster=$clu sector=$sec"
    else
      echo "FAIL  ${mib}MiB cluster=$clu sector=$sec"; sed 's/^/    /' "$TMP/log"; exit 1
    fi
    ;;
  matrix)
    fail=0
    # sector 512: cluster sweep + sizes
    for clu in 512 1024 2048 4096 8192 16384 32768 65536 131072; do
      for mib in 16 100 1024; do
        img="$TMP/m_${clu}_${mib}.img"
        make_ours "$img" "$mib" "$clu" 512 RBMTX >/dev/null 2>&1 || { echo "FAIL(format) c=$clu ${mib}MiB"; fail=1; continue; }
        if ntfsfix -n "$img" >"$TMP/log" 2>&1; then echo "PASS  ${mib}MiB cluster=$clu sector=512"
        else echo "FAIL  ${mib}MiB cluster=$clu sector=512"; sed 's/^/    /' "$TMP/log"; fail=1; fi
      done
    done
    # sector 4096
    for clu in 4096 8192 65536; do
      img="$TMP/s4k_${clu}.img"
      make_ours "$img" 128 "$clu" 4096 RBMTX >/dev/null 2>&1 || { echo "FAIL(format) c=$clu s=4096"; fail=1; continue; }
      if ntfsfix -n "$img" >"$TMP/log" 2>&1; then echo "PASS  128MiB cluster=$clu sector=4096"
      else echo "FAIL  128MiB cluster=$clu sector=4096"; sed 's/^/    /' "$TMP/log"; fail=1; fi
    done
    # odd, cluster-aligned but non-power-of-two size
    img="$TMP/odd.img"; make_ours "$img" 137 4096 512 RBODD >/dev/null 2>&1
    if ntfsfix -n "$img" >"$TMP/log" 2>&1; then echo "PASS  137MiB (odd) cluster=4096 sector=512"
    else echo "FAIL  137MiB (odd)"; sed 's/^/    /' "$TMP/log"; fail=1; fi
    [ "$fail" = 0 ] && echo "ALL PASS" || { echo "SOME FAILED"; exit 1; }
    ;;
  *) sed -n '2,30p' "$0"; exit 1;;
esac
