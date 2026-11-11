#!/usr/bin/env python3
"""Keep docs/capabilities.md's table in step with the tier 8 flows.

Every `capabilities = [...]` tag in regression-tests/cases/tier8/*.toml is a row; a shell's cell is
"yes" when a flow with that tag lists the shell. `--write` regenerates the table in place.
"""
import collections
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
DOC = ROOT / "docs/capabilities.md"
SHELLS = [("tui", "TUI"), ("gtk", "GTK"), ("win32", "Win32"), ("appkit", "AppKit")]
BEGIN, END = "<!-- capabilities:begin -->", "<!-- capabilities:end -->"


def flows():
    """capability -> [(flow id, shells)] from the tier 8 manifests."""
    caps = collections.defaultdict(list)
    for f in sorted((ROOT / "regression-tests/cases/tier8").glob("*.toml")):
        text = f.read_text(encoding="utf-8")
        for block in re.split(r"^\[\[case\]\]\s*$", text, flags=re.M)[1:]:
            cid = re.search(r'^id = "([^"]+)"', block, re.M).group(1)
            m = re.search(r"^shells = \[([^\]]*)\]", block, re.M)
            shells = set(re.findall(r'"([^"]+)"', m.group(1))) if m else set()
            m = re.search(r"^capabilities = \[([^\]]*)\]", block, re.M)
            tags = re.findall(r'"([^"]+)"', m.group(1)) if m else []
            if not tags:
                sys.exit(f"check-capabilities: {cid} ({f.name}) has no capabilities")
            for t in tags:
                caps[t].append((cid, shells))
    return caps


def table(caps):
    head = "| Capability | " + " | ".join(n for _, n in SHELLS) + " | Proved by |"
    rows = [head, "|" + "---|" * (len(SHELLS) + 2)]
    for cap in sorted(caps):
        on = set().union(*(s for _, s in caps[cap]))
        cells = ["yes" if key in on else "-" for key, _ in SHELLS]
        ids = ", ".join(f"`{i}`" for i in sorted({i for i, _ in caps[cap]}))
        rows.append(f"| `{cap}` | " + " | ".join(cells) + f" | {ids} |")
    return "\n".join(rows)


def main():
    text = DOC.read_text(encoding="utf-8")
    m = re.search(re.escape(BEGIN) + r"\n(.*?)" + re.escape(END), text, re.S)
    if not m:
        sys.exit(f"check-capabilities: no {BEGIN} / {END} block in docs/capabilities.md")
    want = table(flows()) + "\n"
    if "--write" in sys.argv:
        with open(DOC, "w", encoding="utf-8", newline="\n") as f:
            f.write(text[: m.start(1)] + want + text[m.end(1) :])
        print("check-capabilities: table rewritten")
    elif m.group(1) != want:
        sys.exit("check-capabilities: docs/capabilities.md is stale; run scripts/check-capabilities.py --write")
    else:
        print(f"check-capabilities: table matches the tier 8 flows ({len(want.splitlines()) - 2} capabilities)")


if __name__ == "__main__":
    main()
