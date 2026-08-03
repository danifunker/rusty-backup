#!/usr/bin/env python3
"""Diff `libc`'s Rust struct declarations against real PowerPC ground truth.

Feed it the TSV that `ppc-libc-probe.py` captured on a PowerPC Mac and it
reports, per struct, every place the Rust definition would lay memory out
differently from the C ABI the machine actually uses. That list is the worklist
for a `powerpc-apple-darwin` arch file.

Two things have to be modelled correctly or this reports mostly noise, and both
are handled here:

  * **The "power" alignment ABI.** Darwin/PowerPC gives a struct the alignment of
    its *first* member, so an 8-byte member that is not first is only 4-aligned.
    mrustc models this (`make_type_repr_struct__inner` in src/trans/target.cpp)
    and must, since it delegates layout to the C compiler - so `struct_layout`
    here mirrors it. Using plain max-member `repr(C)` alignment instead flags
    every struct with a non-leading 8-byte field, `stat` and `statfs` included,
    as broken when they are fine.

  * **Naming vs layout.** libc invents names the headers do not have - it
    flattens `st_atimespec` into `st_atime` + `st_atime_nsec`, and splits
    reserved tails its own way. Those are reported separately from real
    size/offset disagreements.

Usage:
    scripts/ppc-libc-compare.py --libc <vendored libc> --probe rb-cli-ppc/probe/ppc-10.4u.tsv
"""

import argparse
import importlib.util
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

# Reuse the probe script's libc parser so the two cannot drift apart.
_spec = importlib.util.spec_from_file_location(
    "ppc_libc_probe", os.path.join(HERE, "ppc-libc-probe.py")
)
probe = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(probe)

# powerpc-apple-darwin is ILP32 and big-endian. Alignments follow mrustc's
# ARCH_POWERPC table (src/trans/target.cpp) -- that, not the C ABI, is what
# governs the layout mrustc computes for a Rust struct.
PRIMITIVES = {
    "i8": (1, 1), "u8": (1, 1), "bool": (1, 1),
    "i16": (2, 2), "u16": (2, 2),
    "i32": (4, 4), "u32": (4, 4), "f32": (4, 4),
    "i64": (8, 8), "u64": (8, 8), "f64": (8, 8),
    "i128": (16, 8), "u128": (16, 8),
    "isize": (4, 4), "usize": (4, 4),
    "c_char": (1, 1), "c_schar": (1, 1), "c_uchar": (1, 1),
    "c_short": (2, 2), "c_ushort": (2, 2),
    "c_int": (4, 4), "c_uint": (4, 4),
    "c_long": (4, 4), "c_ulong": (4, 4),
    "c_longlong": (8, 8), "c_ulonglong": (8, 8),
    "c_float": (4, 4), "c_double": (8, 8),
    "c_void": (1, 1),
}
POINTER = (4, 4)

RE_ARRAY = re.compile(r"^\[\s*(.+?)\s*;\s*(.+?)\s*\]$")


def clean_type(t):
    return t.strip().lstrip(":").replace("::", "").strip()


class Layout:
    """Resolves a libc type name to (size, align) under the target's rules."""

    def __init__(self, types, structs):
        self.types = {k: clean_type(v) for k, v in types.items()}
        self.structs = {name: fields for name, fields, _ in structs}
        self.cache = {}

    def size_align(self, ty, depth=0):
        ty = ty.strip()
        if depth > 16:
            return None
        if ty.startswith("*"):
            return POINTER
        if ty.startswith("&"):
            return POINTER
        if ty.startswith("Option<") or ty.startswith("extern"):
            return POINTER  # fn pointers / nullable fn pointers
        m = RE_ARRAY.match(ty)
        if m:
            inner = self.size_align(m.group(1), depth + 1)
            try:
                n = int(m.group(2), 0)
            except ValueError:
                return None
            if inner is None:
                return None
            return (inner[0] * n, inner[1])
        key = clean_type(ty)
        if key in PRIMITIVES:
            return PRIMITIVES[key]
        if key in self.types:
            return self.size_align(self.types[key], depth + 1)
        if key in self.structs:
            lay = self.struct_layout(key, depth + 1)
            return None if lay is None else (lay[0], lay[1])
        return None

    def struct_layout(self, name, depth=0):
        """Lay out a `repr(C)` struct the way mrustc does for this target.

        Field order is preserved, and the Darwin/PowerPC "power" alignment rule
        applies: the first member keeps its natural alignment, later members with
        natural alignment between 4 and 8 are capped to 4. This mirrors
        `make_type_repr_struct__inner` in mrustc's src/trans/target.cpp - modelling
        plain max-member alignment here instead reports `stat`, `statfs` and every
        other struct with a non-leading 8-byte field as broken when it is fine.
        """
        if name in self.cache:
            return self.cache[name]
        if depth > 16 or name not in self.structs:
            return None
        offset, align, out = 0, 1, []
        is_first = True
        for fname, ftype in self.structs[name]:
            sa = self.size_align(ftype, depth + 1)
            if sa is None:
                self.cache[name] = None
                return None
            fsize, falign = sa
            if fsize > 0:
                if not is_first and 4 <= falign <= 8:
                    falign = 4
                is_first = False
            offset = (offset + falign - 1) // falign * falign
            out.append((fname, offset, fsize))
            offset += fsize
            align = max(align, falign)
        size = (offset + align - 1) // align * align
        self.cache[name] = (size, align, out)
        return self.cache[name]


def parse_with_types(libc_dir):
    """Like the probe's collect(), but keeping each field's declared type."""
    structs, types = [], {}
    seen = set()
    for rel in probe.LIBC_MODULES:
        path = os.path.join(libc_dir, rel)
        if not os.path.exists(path):
            continue
        text = probe.read_module(path)
        for m in probe.RE_STRUCT.finditer(text):
            name = m.group(1)
            if name in seen:
                continue
            seen.add(name)
            structs.append(
                (name, probe.parse_struct_fields(probe.struct_body(text, m.end())), rel)
            )
        for m in probe.RE_TYPE.finditer(text):
            types.setdefault(m.group(1), m.group(2).strip())
    return structs, types


def load_probe(path):
    """TSV -> ({struct: (size, align)}, {(struct, field): (offset, size)}).

    The probe measures each name under both spellings, `struct X` and plain `X`.
    Prefer the tagged one: several of these names (`flock`, `kevent`, `stat`,
    `mstats`) are also *functions* in the SDK, and gcc's `sizeof` extension
    happily reports 1 for a function type instead of erroring, so the bare
    spelling silently yields a bogus 1-byte "struct".
    """
    sizes, fields = {}, {}
    for line in open(path):
        if line.startswith("#"):
            continue
        p = line.rstrip("\n").split("\t")
        if p[0] == "S" and len(p) >= 5:
            tagged = p[2].startswith("struct ")
            if p[1] not in sizes or tagged:
                sizes[p[1]] = (int(p[3]), int(p[4]))
        elif p[0] == "F" and len(p) >= 5:
            # offsetof on a function type is a hard error, so field rows only
            # ever come from a spelling that really is a struct.
            fields[(p[1], p[2])] = (int(p[3]), int(p[4]))
    return sizes, fields


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--libc", required=True)
    ap.add_argument("--probe", required=True)
    ap.add_argument("--quiet-missing", action="store_true",
                    help="omit structs the SDK does not have at all")
    args = ap.parse_args()

    structs, types = parse_with_types(args.libc)
    lay = Layout(types, structs)
    c_sizes, c_fields = load_probe(args.probe)

    absent, unresolved, mismatched, name_only, ok = [], [], [], [], []

    for name, fields, _rel in structs:
        if name not in c_sizes:
            absent.append(name)
            continue
        rust = lay.struct_layout(name)
        if rust is None:
            unresolved.append(name)
            continue
        rsize, ralign, roff = rust
        csize, calign = c_sizes[name]
        problems, naming = [], []
        if (rsize, ralign) != (csize, calign):
            problems.append(
                "  size/align: rust=%d/%d  C=%d/%d" % (rsize, ralign, csize, calign)
            )
        for fname, off, fsize in roff:
            key = (name, fname)
            if key not in c_fields:
                # A name libc invents rather than a layout error: it flattens
                # `st_atimespec` into `st_atime` + `st_atime_nsec`, and splits
                # reserved tails its own way. Only a size/align or offset
                # disagreement is evidence of a real layout problem.
                naming.append("  field %-24s no such name in C" % fname)
                continue
            coff, csz = c_fields[key]
            if off != coff or fsize != csz:
                problems.append(
                    "  field %-24s rust=@%d(%d)  C=@%d(%d)" % (fname, off, fsize, coff, csz)
                )
        if problems:
            mismatched.append((name, problems + naming))
        elif naming:
            name_only.append((name, naming))
        else:
            ok.append(name)

    print("== %d structs match the PowerPC ABI exactly" % len(ok))
    print("== %d structs differ only in field naming (layout agrees)" % len(name_only))
    print("== %d structs MISMATCH\n" % len(mismatched))
    for name, problems in sorted(mismatched):
        print("%s:" % name)
        for p in problems:
            print(p)
        print()

    if name_only:
        print("== name-only differences (sizes, alignments and offsets all agree):")
        for nm, probs in sorted(name_only):
            print("%s:" % nm)
            for pr in probs:
                print(pr)
        print()

    if unresolved:
        print("== %d structs whose Rust types could not be resolved "
              "(unions/fn-ptrs; check by hand):" % len(unresolved))
        print("   " + ", ".join(sorted(unresolved)))
    if absent and not args.quiet_missing:
        print("\n== %d structs libc declares that this SDK does not have:" % len(absent))
        print("   " + ", ".join(sorted(absent)))


if __name__ == "__main__":
    main()
