#!/usr/bin/env python3
"""Apply the mrustc-workaround patch set to a `cargo vendor` tree.

Each patch is one module in rb-cli-ppc/patches/, named for the crate it
serves; the contract (CRATE / TARGETS / GAP / UPSTREAM / APPLIED / MATCH /
patch()) is documented in rb-cli-ppc/patches/README.md. This runner owns the
invariants a patch author must not be able to get wrong:

  * Write only when the content actually changes. minicargo is
    timestamp-driven, so an unconditional rewrite marks the crate dirty and
    silently re-transpiles the ~800 MB engine unit on every single build.
  * Three-state detection per target file: APPLIED matches -> already
    patched, skip; else MATCH matches -> apply; else FAIL loudly naming the
    crate. A pattern that stops matching after a crate version bump must not
    silently no-op into a baffling mrustc error 40 minutes later.
  * A missing target file is skipped quietly -- feature flags change which
    crates are vendored at all.
  * Deterministic order (sorted by filename) and one summary line.

Patterns are searched with re.MULTILINE, so ^ and $ anchor per line.
"""

import argparse
import importlib.util
import re
import sys
from pathlib import Path

# The patch dir is committed; don't strew __pycache__ into it on every build.
sys.dont_write_bytecode = True

REQUIRED = ("CRATE", "TARGETS", "GAP", "UPSTREAM", "APPLIED", "MATCH", "patch")


def die(msg):
    sys.exit("apply-vendor-patches: ERROR: %s" % msg)


def load_patches(patches_dir):
    if not patches_dir.is_dir():
        die("patch dir %s not found" % patches_dir)
    mods = []
    for path in sorted(patches_dir.glob("*.py")):
        spec = importlib.util.spec_from_file_location(path.stem, path)
        mod = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(mod)
        except Exception as exc:
            die("%s failed to load: %s" % (path.name, exc))
        missing = [a for a in REQUIRED if not hasattr(mod, a)]
        if missing:
            die("%s is missing %s (see rb-cli-ppc/patches/README.md)"
                % (path.name, ", ".join(missing)))
        mods.append(mod)
    if not mods:
        die("no patches found under %s" % patches_dir)
    return mods


def apply_patch(mod, vendor):
    patched = current = absent = 0
    for rel in mod.TARGETS:
        target = vendor / rel
        if not target.is_file():
            absent += 1
            continue
        text = target.read_text(encoding="utf-8")
        if re.search(mod.APPLIED, text, re.M):
            current += 1
            continue
        if not re.search(mod.MATCH, text, re.M):
            die("%s: %s matches neither APPLIED nor MATCH -- the crate likely "
                "changed shape under a version bump; update (or retire) "
                "rb-cli-ppc/patches/%s.py" % (mod.CRATE, rel, mod.CRATE))
        new = mod.patch(text, rel)
        if new == text:
            die("%s: %s matched MATCH but patch() changed nothing -- fix "
                "rb-cli-ppc/patches/%s.py" % (mod.CRATE, rel, mod.CRATE))
        if not re.search(mod.APPLIED, new, re.M):
            die("%s: patched %s does not match its own APPLIED pattern -- fix "
                "rb-cli-ppc/patches/%s.py" % (mod.CRATE, rel, mod.CRATE))
        target.write_text(new, encoding="utf-8")
        patched += 1
    return patched, current, absent


def main():
    default_patches = Path(__file__).resolve().parent.parent / "rb-cli-ppc" / "patches"
    ap = argparse.ArgumentParser(
        description="apply the mrustc-workaround patches to a vendored dep tree")
    ap.add_argument("--vendor-dir", required=True, type=Path,
                    help="the `cargo vendor` output directory to patch")
    ap.add_argument("--patches-dir", type=Path, default=default_patches,
                    help="patch modules to apply (default: rb-cli-ppc/patches)")
    args = ap.parse_args()

    if not args.vendor_dir.is_dir():
        die("vendor dir %s not found -- run the vendor stage first" % args.vendor_dir)

    patched = current = absent = 0
    mods = load_patches(args.patches_dir)
    for mod in mods:
        p, c, a = apply_patch(mod, args.vendor_dir)
        patched += p
        current += c
        absent += a
    print("vendor patches: %d crate(s), %d target file(s) -- %d newly patched, "
          "%d already applied, %d absent"
          % (len(mods), patched + current + absent, patched, current, absent))


if __name__ == "__main__":
    main()
