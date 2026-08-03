CRATE = "rustversion"
TARGETS = ["rustversion/build/rustc.rs"]
GAP = """\
Not a language gap. rustversion identifies the compiler from the *last* line
of `rustc --version`, which is right for real rustc (one line, possibly
preceded by warnings) but wrong for mrustc, which prints four lines with the
`rustc <ver>` one first and informational lines after it. Picking the last
line that actually starts with `rustc ` behaves identically on real rustc and
keeps working if mrustc's trailing lines change.

Fixing this in mrustc instead is not obviously safe: the line order is load
bearing in both directions. libc's build.rs parses from the *start* of the
output, and mrustc's own comments note that `autoconfig` looks for the
`release:` line, so neither reordering nor trimming is free.
"""
UPSTREAM = None

APPLIED = r'\.filter\(\|l\| l\.trim_start\(\)\.starts_with\("rustc "\)\)'
MATCH = r"^    let last_line = string\.lines\(\)\.last\(\)\.unwrap_or\(string\);$"

OLD = "    let last_line = string.lines().last().unwrap_or(string);"
NEW = (
    "    // rb-cli-ppc: mrustc prints informational lines *after* the `rustc <ver>`\n"
    "    // line, so take the last line that actually looks like a version banner.\n"
    "    let last_line = string\n"
    "        .lines()\n"
    '        .filter(|l| l.trim_start().starts_with("rustc "))\n'
    "        .last()\n"
    "        .or_else(|| string.lines().last())\n"
    "        .unwrap_or(string);"
)


def patch(text, path):
    return text.replace(OLD, NEW, 1)
