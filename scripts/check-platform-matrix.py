#!/usr/bin/env python3
"""Fail if docs/platform_matrix.md and release.yml's CI targets disagree.

Every name in the `plan` job's ALL= list must be a row's "CI target" in the
matrix (or one of NOT_PLATFORMS), and every row's CI target must be in ALL=.
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
NOT_PLATFORMS = {"crates-msrv"}


def ci_targets():
    text = (ROOT / ".github/workflows/release.yml").read_text()
    m = re.search(r'^\s*ALL="([^"]+)"', text, re.M)
    if not m:
        sys.exit("check-platform-matrix: no ALL= list in release.yml")
    return set(m.group(1).split())


def matrix_targets():
    text = (ROOT / "docs/platform_matrix.md").read_text()
    m = re.search(r"<!-- matrix:begin -->(.*?)<!-- matrix:end -->", text, re.S)
    if not m:
        sys.exit("check-platform-matrix: no matrix:begin/end block in docs/platform_matrix.md")
    rows = [l for l in m.group(1).splitlines() if l.startswith("|")][2:]
    found = set()
    for row in rows:
        cells = [c.strip() for c in row.strip("|").split("|")]
        if len(cells) < 2:
            continue
        if cells[1] != "-":
            found.add(cells[1])
    return found


def main():
    ci = ci_targets()
    doc = matrix_targets()
    missing = sorted(ci - doc - NOT_PLATFORMS)
    unknown = sorted(doc - ci)
    for t in missing:
        print(f"::error::CI target '{t}' has no row in docs/platform_matrix.md")
    for t in unknown:
        print(f"::error::docs/platform_matrix.md lists CI target '{t}', which release.yml doesn't build")
    if missing or unknown:
        return 1
    print(f"platform matrix matches release.yml ({len(doc)} CI targets)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
