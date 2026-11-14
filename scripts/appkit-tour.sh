#!/bin/bash
# The AppKit screenshot tour (docs/appkit_shell_plan.md): runs regression-tests/cases/tier9 in
# light and dark, then writes a contact sheet pairing each shot's two looks.
#
#   scripts/appkit-tour.sh [binary] [out-dir] [baseline-dir]
#
# binary       defaults to frontends/appkit/target/debug/rusty-backup-appkit
# out-dir      defaults to regression-tests/runs/tour-<time>; it gets light/, dark/, index.html
# baseline-dir an earlier approved tour's out-dir; a shot whose pixels differ from it by more
#              than 0.05% (scripts/shotdiff.swift) is marked "changed" with its share. A change
#              asks for a look, never fails the run: macOS versions differ in their pixels.
#
# Only the app's own windows are captured (screencapture -l), so the process running this needs
# the Screen Recording permission besides Accessibility.

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=${1:-$ROOT/frontends/appkit/target/debug/rusty-backup-appkit}
OUT=${2:-$ROOT/regression-tests/runs/tour-$(date +%Y%m%d-%H%M%S)}
BASE=${3:-}
BIN=$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)

cd "$ROOT/regression-tests"
cargo build -q --release --manifest-path runner/Cargo.toml
DIFF="$OUT/.shotdiff"
swiftc -O -o "$DIFF" "$ROOT/scripts/shotdiff.swift"
for look in light dark; do
    echo "== $look"
    RB_SHOTS_DIR="$OUT/$look" RUSTY_BACKUP_APPEARANCE=$look \
        ./runner/target/release/rb-regress run --tiers 9 --shell appkit --appkit "$BIN" \
        --no-prompt | grep -E "^(pass|fail|error|skip)" || true
done

python3 - "$OUT" "$BASE" "$DIFF" <<'PY'
import html, os, subprocess, sys
out, base, tool = sys.argv[1], sys.argv[2], sys.argv[3]
def share(a, b):
    return float(subprocess.run([tool, a, b], capture_output=True, text=True).stdout or "1")
names = sorted({f for look in ("light", "dark") if os.path.isdir(f"{out}/{look}")
                for f in os.listdir(f"{out}/{look}") if f.endswith(".png")})
rows, changed = [], 0
for n in names:
    cells = []
    for look in ("light", "dark"):
        p = f"{out}/{look}/{n}"
        if not os.path.exists(p):
            cells.append("<td class=missing>not shot</td>")
            continue
        mark = ""
        if base:
            old = f"{base}/{look}/{n}"
            if not os.path.exists(old):
                mark = "<span class=new>new</span>"
            else:
                d = share(old, p)
                if d > 0.0005:
                    mark, changed = f"<span class=chg>changed ({d:.2%})</span>", changed + 1
        cells.append(f'<td>{mark}<a href="{look}/{html.escape(n)}"><img src="{look}/{html.escape(n)}"></a></td>')
    rows.append(f"<tr><th>{html.escape(n[:-4])}</th>{''.join(cells)}</tr>")
page = f"""<!doctype html><meta charset=utf-8><title>AppKit tour</title>
<style>
body {{ font: 13px -apple-system, sans-serif; margin: 16px; background: #f4f4f4; }}
table {{ border-collapse: collapse; }} th {{ text-align: left; vertical-align: top; padding: 8px; }}
td {{ padding: 8px; vertical-align: top; }} img {{ width: 520px; border: 1px solid #bbb; }}
.chg, .new {{ display: block; font-weight: 600; color: #b00; }} .new {{ color: #06c; }}
.missing {{ color: #999; }}
</style>
<h1>AppKit tour</h1>
<p>{len(names)} shots{f", {changed} changed against {html.escape(base)}" if base else ""}.</p>
<table><tr><th></th><th>Light</th><th>Dark</th></tr>{''.join(rows)}</table>"""
open(f"{out}/index.html", "w").write(page)
print(f"{len(names)} shots; contact sheet: {out}/index.html" + (f"; {changed} changed" if base else ""))
PY
rm -f "$DIFF"
