#!/usr/bin/env python3
"""Ask the *real* PowerPC gcc what it thinks a struct's size/alignment is.

mrustc emits `sizeof_assert_*` / `alignof_assert_*` typedefs into its C so that
a layout disagreement between mrustc's model and the target C compiler is a
compile error rather than silent corruption. That is a good safety net, but it
only ever says "these disagree" - never *what gcc actually computed*. When the
disagreement is in a deeply nested aggregate, that is not enough to work with.

This gets gcc's real numbers out. The obvious approach - append a `main()` that
prints `sizeof` - does not work, because the emitted translation unit refers to
libcore symbols and will not link. So instead we never link at all: each probe
is a deliberately ill-typed initialiser

    char (*p)[sizeof(struct S)] = 1;

which gcc diagnoses as

    warning: initialization of 'char (*)[7976]' from 'int' ...

with the number we want baked into the printed type. A `#line` directive in
front of each probe renames the "file" to a label, so the diagnostics come back
pre-tagged and are trivial to parse. `-fsyntax-only` means no codegen, so a
5 MB translation unit answers in seconds.

Usage:
    PPC_HOST=admin@192.168.99.116 scripts/ppc-layout-probe.py <file.c> [name...]

With no names, every struct carrying a sizeof/alignof assertion in the file is
probed and the result is diffed against what mrustc asserted - which is the
report you usually want:

    struct                              mrustc        gcc          verdict
    ...FseCore0g                        7976/4        7976/8       ALIGN MISMATCH

Options:
    --synthetic   skip the input file; instead compile a small standalone set
                  of hand-written structs that isolate the power-alignment
                  rules (see SYNTHETIC below).
    --keep        leave the generated probe .c behind for inspection.
"""

import argparse
import os
import re
import shlex
import subprocess
import sys
import tempfile

HOST = os.environ.get("PPC_HOST")
REMOTE_CC = os.environ.get("PPC_CC", "/opt/local/libexec/gcc10-bootstrap/bin/gcc")
REMOTE_ROOT = os.environ.get("PPC_REMOTE_ROOT", "ppc-xbuild")

# mrustc's own assertion typedefs, which double as the list of things worth
# probing and as the "what mrustc believes" column of the report.
ASSERT_RE = re.compile(
    r"^typedef char (sizeof|alignof)_assert_(\S+?)\[\s*\("
    r"(?:sizeof|ALIGNOF)\(struct (\S+)\)\s*==\s*(\d+)\)",
    re.M,
)
# gcc renders the probe type as `char (*)[N]`; -Wint-conversion wording differs
# between gcc versions, so match on the type rather than the sentence.
DIAG_RE = re.compile(r"^(?P<label>PROBE\|[^|]*\|[^|]*):\d+:\d+:.*?char \(\*\)\[(?P<n>\d+)\]")

# Hand-written structs that isolate one rule each. The question these answer:
# Darwin/PowerPC's "power" ABI caps a *non-first* member's alignment at 4 - but
# does that cap also apply when the member's alignment came from an explicit
# __attribute__((aligned)) rather than from its own natural layout?
SYNTHETIC = r"""
#include <stdint.h>

/* --- baseline: the cap on interior scalars and interior aggregates --- */
struct nat_lead_u64  { uint64_t a; uint32_t b; };            /* align 8 */
struct nat_trail_u64 { uint32_t a; uint64_t b; };            /* capped -> 4 */
struct nat_nested    { uint16_t a; struct nat_lead_u64 b; }; /* interior aggregate */

/* --- the question: explicit alignment on an interior member --- */
struct att8 { uint8_t a, b; int16_t c; uint32_t d; } __attribute__((__aligned__(8)));
struct att8_interior { uint16_t a; struct att8 b; };
struct att8_leading  { struct att8 a; uint16_t b; };

/* --- and through an array wrapper, which is the shape mrustc emits --- */
struct att8_arr { struct att8 DATA[4]; };
struct att8_arr_wrapper_lead { struct att8_arr a; uint32_t b; };
struct att8_via_wrapper { uint16_t a; struct att8_arr_wrapper_lead b; };

/* --- does the exemption propagate from a member that is not itself first? --- */
struct att8_trailing { uint32_t a; struct att8 b; };
struct att8_trailing_interior { uint16_t a; struct att8_trailing b; };

/* --- does an explicit alignment *below* the 4-byte cap still confer the
       exemption on the struct that contains it? gcc ORs TYPE_USER_ALIGN in
       unconditionally, so `contaminated` should escape the cap even though its
       own 8-byte alignment is natural, coming from its leading uint64_t. --- */
struct att2 { uint16_t x; } __attribute__((__aligned__(2)));
struct contaminated { uint64_t lead; struct att2 tag; };
struct clean       { uint64_t lead; uint16_t tag; };
struct contaminated_interior { uint16_t a; struct contaminated b; };
struct clean_interior        { uint16_t a; struct clean b; };

/* --- mrustc pins every union's alignment with an explicit attribute. Does that
       attribute confer the cap exemption on enclosing types, and can an explicit
       attribute on the enclosing struct take it back? This is the BTreeMap
       `LeafNode<String, Metric>` shape, reduced. --- */
union mu8   { char unit; struct { double d; } var1; } __attribute__((__aligned__(8)));
union mu8n  { char unit; struct { double d; } var1; };   /* same, no attribute */
struct mu8_arr  { union mu8  DATA[2]; };
struct mu8n_arr { union mu8n DATA[2]; };
struct leaf_attr_noattr { uint16_t a; uint16_t b; struct mu8_arr  c; };
struct leaf_attr_attr4  { uint16_t a; uint16_t b; struct mu8_arr  c; } __attribute__((__aligned__(4)));
struct leaf_nat_noattr  { uint16_t a; uint16_t b; struct mu8n_arr c; };

/* --- can an explicit attribute lower a struct below its *natural* alignment? --- */
struct nat8_attr4 { uint64_t a; uint32_t b; } __attribute__((__aligned__(4)));

/* --- the exact FseCore shape, reduced --- */
struct fse_shape {
    uint16_t weights[360];
    uint32_t c, d, e;
    uint64_t f;
    struct att8_arr_wrapper_lead g;
};
"""
SYNTHETIC_NAMES = [
    ("struct nat_lead_u64", None),
    ("struct nat_trail_u64", None),
    ("struct nat_nested", None),
    ("struct att8", None),
    ("struct att8_interior", None),
    ("struct att8_leading", None),
    ("struct att8_arr", None),
    ("struct att8_arr_wrapper_lead", None),
    ("struct att8_via_wrapper", None),
    ("struct att8_trailing", None),
    ("struct att8_trailing_interior", None),
    ("struct contaminated", None),
    ("struct clean", None),
    ("struct contaminated_interior", None),
    ("struct clean_interior", None),
    ("union mu8", None),
    ("union mu8n", None),
    ("struct mu8_arr", None),
    ("struct mu8n_arr", None),
    ("struct leaf_attr_noattr", None),
    ("struct leaf_attr_attr4", None),
    ("struct leaf_nat_noattr", None),
    ("struct nat8_attr4", None),
    ("struct fse_shape", None),
]


def die(msg):
    sys.stderr.write("ppc-layout-probe: %s\n" % msg)
    sys.exit(1)


def probe_block(specs):
    """Emit the ill-typed initialisers, each tagged via #line with its label.

    `specs` is a list of (c_type_expression, label). The declared variables are
    never defined anywhere and we never link, so the names only have to be
    unique within the translation unit.
    """
    out = ["\n/* ---- layout probes (appended by ppc-layout-probe.py) ---- */\n"]
    for i, (ctype, label) in enumerate(specs):
        for kind, op in (("size", "sizeof"), ("align", "__alignof__")):
            out.append('#line 1 "PROBE|%s|%s"\n' % (kind, label))
            out.append("char (*rb_probe_%s_%d)[%s(%s)] = 1;\n" % (kind, i, op, ctype))
    # Restore a sane #line so anything after this (nothing, normally) is sane.
    out.append('#line 1 "probe-tail"\n')
    return "".join(out)


def parse_asserts(text):
    """-> {struct_name: {"size": n, "align": n}} as mrustc asserted them."""
    found = {}
    for kind, _sym, struct, value in ASSERT_RE.findall(text):
        key = "size" if kind == "sizeof" else "align"
        found.setdefault(struct, {})[key] = int(value)
    return found


def run_remote(local_c, extra_args):
    """Compile local_c on the Mac with -fsyntax-only; return gcc's stderr."""
    base = os.path.basename(local_c)
    remote_dir = "%s/layout-probe" % REMOTE_ROOT
    remote_c = "%s/%s" % (remote_dir, base)
    if subprocess.call(["ssh", HOST, "mkdir -p %s" % shlex.quote(remote_dir)]) != 0:
        die("failed to create the remote probe directory")
    if subprocess.call(["rsync", "-q", local_c, "%s:%s" % (HOST, remote_c)]) != 0:
        die("failed to upload the probe")
    cmd = "%s -fsyntax-only %s %s 2>&1" % (
        shlex.quote(REMOTE_CC),
        " ".join(shlex.quote(a) for a in extra_args),
        shlex.quote(remote_c),
    )
    # gcc exits non-zero because the probes are errors/warnings by design, and
    # because the assertions under test may themselves be failing. The
    # diagnostics *are* the result, so the exit status is not interesting.
    proc = subprocess.run(["ssh", HOST, cmd], stdout=subprocess.PIPE)
    return proc.stdout.decode("utf-8", "replace")


def collect(diags):
    """-> {label: {"size": n, "align": n}} from gcc's diagnostics."""
    got = {}
    for line in diags.splitlines():
        m = DIAG_RE.match(line)
        if not m:
            continue
        _, kind, label = m.group("label").split("|", 2)
        got.setdefault(label, {})[kind] = int(m.group("n"))
    return got


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("source", nargs="?", help="an mrustc-emitted .c file")
    ap.add_argument("names", nargs="*", help="struct names to probe (default: all asserted)")
    ap.add_argument("--synthetic", action="store_true", help="probe the hand-written ABI cases")
    ap.add_argument("--keep", action="store_true", help="keep the generated probe .c")
    ap.add_argument("--cflags", default="-Wno-psabi", help="extra flags for the remote gcc")
    args = ap.parse_args()

    if not HOST:
        die("PPC_HOST is not set (e.g. PPC_HOST=admin@192.168.99.116)")

    if args.synthetic:
        body = SYNTHETIC
        specs = SYNTHETIC_NAMES
        specs = [(t, t.replace("struct ", "").replace("union ", "")) for t, _ in specs]
        asserted = {}
    else:
        if not args.source:
            ap.error("a source file is required unless --synthetic is given")
        with open(args.source) as fh:
            body = fh.read()
        asserted = parse_asserts(body)
        names = args.names or sorted(asserted)
        missing = [n for n in names if n not in asserted and ("struct %s" % n) not in body]
        if missing:
            die("not found in %s: %s" % (args.source, ", ".join(missing)))
        specs = [("struct %s" % n, n) for n in names]

    tmpdir = tempfile.mkdtemp(prefix="ppc-layout-probe-")
    local_c = os.path.join(tmpdir, "probe.c")
    with open(local_c, "w") as fh:
        fh.write(body)
        fh.write(probe_block(specs))

    diags = run_remote(local_c, shlex.split(args.cflags))
    got = collect(diags)

    if not got:
        sys.stderr.write(diags[:4000])
        die("no probe diagnostics came back - see gcc output above")

    width = max(len(lbl) for _, lbl in specs)
    width = min(max(width, 24), 96)
    print("%-*s  %-12s %-12s %s" % (width, "struct", "mrustc", "gcc", "verdict"))
    print("-" * (width + 42))
    mismatches = 0
    for _ctype, label in specs:
        g = got.get(label, {})
        a = asserted.get(label, {})

        def fmt(d):
            return "%s/%s" % (d.get("size", "-"), d.get("align", "-"))

        bad = []
        if "size" in a and "size" in g and a["size"] != g["size"]:
            bad.append("SIZE")
        if "align" in a and "align" in g and a["align"] != g["align"]:
            bad.append("ALIGN")
        verdict = (" + ".join(bad) + " MISMATCH") if bad else ("ok" if a else "")
        if bad:
            mismatches += 1
        print("%-*s  %-12s %-12s %s" % (width, label[:width], fmt(a), fmt(g), verdict))

    if args.keep:
        print("\nprobe source kept at %s" % local_c)
    print("\n%d struct(s) probed, %d mismatch(es)." % (len(specs), mismatches))
    return 0


if __name__ == "__main__":
    sys.exit(main())
