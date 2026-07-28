#!/usr/bin/env python3
"""Split one mrustc-generated C translation unit into a header plus N units.

mrustc emits **one `.c` per crate** and has no codegen-units concept, so the
engine arrives here as a single 797 MB translation unit that compiles to a
61.8 MB `__text`. Two separate ceilings sit on top of an object that big, and
this script exists to get under both.

**The linker one, which is the blocker.** A PowerPC `bl` reaches +/-16 MB, and
`ld64-85.2.1` (Leopard) does know how to fix that up - it inserts branch
islands, and names them in its own diagnostics (`_main$island`). What it cannot
do is place an island *inside* an input object whose own `__text` is oversized.
Measured on the G5 with synthetic objects, holding everything else constant:

    61 MB of small atoms in ONE object, direct `bl` to an external helper
        -> ld: bl out of range (67553480 max is +/-16M) ... to _helper
    the same 61 MB of code as 8 objects            -> links clean
    the same 61 MB of code as 2 objects of 30 MB   -> links clean

So the limit is per *input object*, and it sits between 30 MB and 61 MB of
`__text` - consistent with a signed 26-bit branch displacement (+/-32 MB) in
ld's own arithmetic. That is why `-dead_strip` and supplying a local
`darwin-gpsave.o` both failed to fix `restGPRx`: neither gets the *calling*
object under the ceiling. Splitting does.

The same +/-32 MB shows up one stage earlier, in the assembler, which is what
`-mlongcall` was really working around:

    /var/tmp//ccRt2eNL.s:832036: Fixup of -67328096 too large for field width
    of 26 bits

**The compiler one.** `cc1` here is a 32-bit binary, so at `-O1` the whole unit
exhausted its own address space (~3.5 GB) after 25 minutes. That is what forced
`-O0` on the engine. Units a fraction of the size fit comfortably, so splitting
is also the route back to an optimised build.

## What the split looks like

mrustc's output is regular enough to split by structure rather than by parsing
C: a preamble of `static inline` helpers, then type definitions with their
`sizeof`/`alignof` asserts, then a `// PROTO` declaration for every function,
then the bodies. Top-level constructs always start at column 0 and always end
at column 0 (`}`, `};`, or a lone `;`), while everything inside a body is
tab-indented - with one exception that matters, MIR basic-block labels (`bb2:`)
which sit at column 0 too, so the chunker only treats `}` as a terminator while
it knows it is inside a body.

Every definition moves to a unit; everything else stays in the header, in
source order, and each definition leaves a *declaration* behind at the point it
was removed from. That ordering is what keeps the header valid: a declaration
can never depend on a definition, so hoisting the definitions out cannot break
anything the header still needs.

The one real hazard is linkage. mrustc gives crate-local monomorphisations
internal linkage (115k `static` items in the engine), and a `static` definition
in unit 3 is invisible to a caller in unit 5. They are therefore promoted to
external linkage - and **renamed**, which is not optional. "It was `static`, so
no other object can be defining it" sounds right and is false: mrustc emits a
crate-local copy of some items that the crate they belong to also defines
globally, so promoting one is a duplicate symbol against that crate's object:

    ld: duplicate symbol _ZRG3cF10alloc..vec_deque10wrap_index0g in
        librusty_backup-...rlib.o and liballoc.rlib.o

The rename is done by the preprocessor rather than by rewriting 800 MB of text:
`promoted.h` gets one `#define NAME NAME__rbsplit` per promoted name and each
unit includes it first, so the definition and every reference move together at
no cost. mrustc uses the same trick itself (`#define ...panic_fmt0g
rust_begin_unwind`) - and a name it has already pointed somewhere cannot be
renamed on top of that, because its `#define` comes later and wins. The engine
has exactly one, a local copy of `panic_fmt` that the macro turns into a
definition of `rust_begin_unwind`, which std also defines. Being `static` was
what kept that from colliding; it is made weak instead, which keeps it visible
to the other units while letting std's definition win.

`static inline` helpers are left exactly as they are: they belong in the header,
and duplicating them per unit is what inline means.

## Usage

    ppc-split-tu.py <source.c> [-n UNITS] [-o OUTDIR]

Writes `<OUTDIR>/tu.h` and `<OUTDIR>/u0.c` ... and prints the unit paths, one
per line. `OUTDIR` defaults to `<source without .c>.split`. The split is
skipped when it is already newer than the source, so re-running a build does
not redo it.
"""

import argparse
import os
import re
import sys

# Each unit's __text has to stay well under the ~32 MB ceiling measured above.
# The engine is 61.8 MB of __text for 797 MB of C, so a unit's share of the
# source is a good enough proxy: four units put each at ~16 MB of text, half
# the ceiling, and the number is only a default - `-n` overrides it.
DEFAULT_UNITS = 4

# Words that are part of a type, not the name being declared.
TAG_WORDS = frozenset((
    b"struct", b"union", b"enum", b"const", b"volatile", b"static", b"inline",
    b"unsigned", b"signed", b"short", b"long", b"__thread", b"register",
    b"_Atomic", b"extern", b"restrict", b"__restrict"))
ATTRIBUTE = re.compile(rb"__attribute__\s*\(\(.*?\)\)", re.S)
WEAK = re.compile(rb"__attribute__\s*\(\(\s*weak")
BRACED = re.compile(rb"\{[^{}]*\}", re.S)
SUBSCRIPT = re.compile(rb"\[[^\]]*\]", re.S)
# `$` is part of a name here: mrustc puts a hash suffix on long mangled names
# (`...InternalNode2gG3cM21aho_cora$5bc2baf7d7f353bd`), and Darwin's C accepts
# it. Leaving `$` out of the pattern split one name into two and made every
# forward-declared union look like it was declaring an object.
IDENT = re.compile(rb"[A-Za-z_$][A-Za-z0-9_$]*")
MACRO_NAME = re.compile(rb"#\s*define\s+([A-Za-z_$][A-Za-z0-9_$]*)")
# Appended to every crate-local name that gets external linkage. mrustc's
# mangling never produces this ending, so it cannot collide with a real symbol.
RENAME_SUFFIX = b"__rbsplit"


# Chunk kinds produced by iter_chunks().
PP = "pp"          # a preprocessor line
DECL = "decl"      # ended at a top-level `;`
BLOCK = "block"    # had a `{ ... }` body opened at column 0
ONELINE = "oneline"  # opened and closed its body on one line


def uncommented(line):
    """The line with any trailing `// ...` removed.

    Top-level lines routinely carry a trailing comment - mrustc annotates
    declarations with the Rust item they came from - and a chunk that ends
    `...;\t// static ::"cipher-0_4_4"::block::#0::FRAGMENTS` does not *look*
    terminated unless the comment is taken off first. String literals are
    tracked so a `//` inside one is not mistaken for a comment.
    """
    i = 0
    n = len(line)
    quote = None
    while i < n:
        c = line[i:i + 1]
        if quote is not None:
            if c == b"\\":
                i += 2
                continue
            if c == quote:
                quote = None
        elif c == b'"' or c == b"'":
            quote = c
        elif c == b"/" and line[i + 1:i + 2] == b"/":
            return line[:i]
        i += 1
    return line


def uncommented_text(text):
    """`uncommented` applied to every line - used only for classification."""
    return b"".join(uncommented(l) for l in text.splitlines(True))


def iter_chunks(fh):
    """Yield (kind, lines) for each top-level construct of a generated .c.

    Lines are bytes, and are yielded verbatim - the concatenation of every
    chunk reproduces the input exactly.
    """
    buf = []
    in_body = False
    for line in fh:
        buf.append(line)
        first = line[:1]
        # Continuations, body lines and blanks never terminate a chunk. This
        # is also what keeps `bb2:` labels - which are at column 0 inside a
        # body - from being mistaken for anything.
        if first in (b"\t", b" ", b"\n", b"\r", b""):
            continue
        if in_body:
            if first == b"}":
                in_body = False
                yield BLOCK, buf
                buf = []
            continue
        stripped = uncommented(line).rstrip()
        if not stripped:
            continue
        if first == b"#":
            yield PP, buf
            buf = []
            continue
        if stripped.endswith(b"{"):
            in_body = True
            continue
        if stripped.endswith(b"}"):
            yield ONELINE, buf
            buf = []
            continue
        if stripped.endswith(b";"):
            yield DECL, buf
            buf = []
            continue
        # Anything else is a construct still in progress - typically the first
        # line of a multi-line function signature.
    if buf:
        # Trailing comments or a truncated file; keep them in the header.
        yield PP, buf


def code_offset(text):
    """Offset of the chunk's first line that is neither blank nor a comment.

    Chunks routinely open with mrustc's `// PROTO ...` line, so the leading
    keyword has to be looked for past those rather than at byte zero.
    """
    pos = 0
    end = len(text)
    while pos < end:
        eol = text.find(b"\n", pos)
        if eol == -1:
            eol = end
        line = text[pos:eol].strip()
        if line and not line.startswith(b"//"):
            return pos
        pos = eol + 1
    return end


def strip_static(text):
    """Drop a leading `static ` so a definition gets external linkage.

    `static inline` is left alone - those stay in the header and are meant to
    be duplicated into every unit.
    """
    at = code_offset(text)
    lead = text[at:]
    if lead.startswith(b"static inline"):
        return text
    if not lead.startswith(b"static "):
        return text
    return text[:at] + lead[len(b"static "):]


def find_code(text, needle):
    """Offset of the first `needle` that is code rather than comment, or -1."""
    pos = 0
    for line in text.splitlines(True):
        at = uncommented(line).find(needle)
        if at != -1:
            return pos + at
        pos += len(line)
    return -1


def matching_brace(text, open_at):
    """Offset of the `}` closing the `{` at `open_at`, ignoring comments."""
    depth = 0
    pos = 0
    for line in text.splitlines(True):
        code = uncommented(line)
        for i in range(len(code)):
            if pos + i < open_at:
                continue
            c = code[i:i + 1]
            if c == b"{":
                depth += 1
            elif c == b"}":
                depth -= 1
                if depth == 0:
                    return pos + i
        pos += len(line)
    return -1


def without_aggregate_body(text):
    """`union u_X{ ... } NAME;` -> `union u_X NAME;`.

    The type itself stays in the header, so the unit that holds the storage
    must refer to it rather than define it again.
    """
    open_at = find_code(text, b"{")
    if open_at == -1:
        return text
    close_at = matching_brace(text, open_at)
    if close_at == -1:
        return text
    return text[:open_at] + text[close_at + 1:]


def declared_name(prefix):
    """The identifier a declaration is declaring, or None.

    `prefix` is the text up to whatever ends the declarator - the `=` of an
    initialiser, or the whole declaration for a tentative definition. Aggregate
    bodies, array bounds and attributes are removed first so that what is left
    ends in the name.
    """
    code = ATTRIBUTE.sub(b" ", uncommented_text(prefix))
    while True:
        stripped = BRACED.sub(b" ", code)
        if stripped == code:
            break
        code = stripped
    code = SUBSCRIPT.sub(b" ", code)
    names = [t for t in IDENT.findall(code) if t not in TAG_WORDS]
    return names[-1] if names else None


def declarator(prefix):
    """Turn the text before a body or initialiser into a declaration.

    The `;` goes on a line of its own - which is how mrustc writes its own
    prototypes, and necessary because the last line of a signature usually
    ends in a `// -> ()` comment that would swallow it.
    """
    prefix = strip_static(prefix.rstrip())
    return prefix[code_offset(prefix):].rstrip() + b"\n;\n"


def declares_storage(text):
    """Is this `;`-terminated declaration a *tentative definition*?

    mrustc forward-declares each of its statics before the initialised
    definition that follows later in the file, and writes the forward
    declaration without `extern`:

        union u_static_X{ struct s_Y val; uintptr_t raw[1]; } NAME;
        ...
        union u_static_X NAME = { .raw = {0x7ffffffeull} };

    In one translation unit those are one object. Copied into a header that
    every unit includes, the first line becomes a tentative definition in each
    of them - and gcc 10 defaults to `-fno-common`, so each one allocates and
    the link fails on duplicate symbols. Recognising them is what lets the
    header keep the type while declaring the object `extern`.
    """
    code = ATTRIBUTE.sub(b" ", uncommented_text(text))
    lead = code[code_offset(code):]
    if lead.startswith((b"extern", b"typedef")):
        return False
    if b"(" in code:
        return False  # a prototype; attributes are already out of the way
    # With aggregate bodies and array bounds gone, a declaration that still has
    # a name left over after its type is declaring an object.
    while True:
        stripped = BRACED.sub(b" ", code)
        if stripped == code:
            break
        code = stripped
    code = SUBSCRIPT.sub(b" ", code)
    return len([t for t in IDENT.findall(code) if t not in TAG_WORDS]) >= 2


# The classifications a chunk can get. HEADER stays put verbatim and EXTERN
# stays put as a declaration; the other two move to a unit and leave a
# declaration behind.
HEADER, EXTERN, FUNCTION, DATA = "header", "extern", "fn", "data"


def classify(text, kind):
    """Decide where a chunk goes, and what each side gets.

    Returns (placement, header_text, unit_text, name). `name` ties a tentative
    definition to the initialised definition of the same object, so both land
    in the same unit and merge there exactly as they did in the original single
    translation unit - two objects, one tentative and one initialised, would be
    a duplicate symbol instead.

    The declarator is never reconstructed - it is cut verbatim out of what
    mrustc already wrote, at the `{` that opens the body or at the `=` that
    starts an initialiser - so anything mrustc can
    spell (attributes, array bounds, function-pointer types) survives.

    The rule is deliberately conservative: a chunk moves only when it is
    recognisably a definition, and everything else stays in the header. A
    misfiled definition would be a duplicate symbol or a silently duplicated
    static, both of which are worse than an over-full header.
    """
    if kind == PP:
        return HEADER, text, None, None
    lead = text[code_offset(text):]
    if lead.startswith(b"static inline") or lead.startswith(b"inline "):
        return HEADER, text, None, None
    if lead.startswith(b"typedef "):
        # Includes mrustc's sizeof_assert/alignof_assert typedefs, whose `==`
        # must not be mistaken for an initialiser.
        return HEADER, text, None, None

    # Comments can hold anything, braces and `=` included, so the shape of a
    # chunk is read from its code alone.
    body_at = find_code(text, b"{")
    eq_at = find_code(text, b"=")
    if eq_at != -1 and (body_at == -1 or eq_at < body_at):
        # Data with an initialiser: `static union u X = { ... };`. It has to
        # live in exactly one unit - duplicating it would give each unit its
        # own copy, at its own address.
        return (DATA, b"extern " + declarator(text[:eq_at]), strip_static(text),
                declared_name(text[:eq_at]))
    if kind == DECL or body_at == -1:
        # A declaration, including every `// PROTO` prototype. The definition
        # it belongs to is moving to a unit, so it must not stay internal.
        if declares_storage(text) and not WEAK.search(text):
            # A tentative definition - mrustc forward-declares each static
            # this way. The header keeps the type and declares the object
            # `extern`; the storage moves to a unit, joining the initialised
            # definition of the same name when there is one. (Weak ones are
            # left alone: mrustc marks vtables and type ids weak precisely so
            # the copies coalesce, and duplicating those across units is how
            # that is meant to work, exactly as it already does across crates.)
            body = strip_static(text)
            at = code_offset(body)
            return (EXTERN, body[:at] + b"extern " + body[at:],
                    strip_static(without_aggregate_body(text)),
                    declared_name(text))
        return HEADER, text, None, None
    if b"(" not in uncommented_text(text[:body_at]):
        # No parameter list before the body: a struct/union/enum definition,
        # not a function. (The keyword is no use here - plenty of functions
        # return a struct, so their signature starts with one too.)
        return HEADER, text, None, None
    return (FUNCTION, declarator(text[:body_at]), strip_static(text),
            declared_name(text[:find_code(text, b"(")]))


def split(source, outdir, units, force=False, quiet=False):
    base = os.path.basename(source)
    header_name = "tu.h"
    promoted_name = "promoted.h"
    header_path = os.path.join(outdir, header_name)
    promoted_path = os.path.join(outdir, promoted_name)
    unit_paths = [os.path.join(outdir, "u%d.c" % i) for i in range(units)]
    stamp_path = os.path.join(outdir, ".stamp")
    stamp = "%d %d %d" % (os.path.getmtime(source), os.path.getsize(source), units)

    if not force and os.path.exists(stamp_path):
        with open(stamp_path) as fh:
            if fh.read().strip() == stamp and all(
                    os.path.exists(p) for p in [header_path] + unit_paths):
                if not quiet:
                    sys.stderr.write(
                        "ppc-split-tu: %s already split into %d units\n"
                        % (base, units))
                return unit_paths

    os.makedirs(outdir, exist_ok=True)
    counts = {"header": 0, "extern": 0, "fn": 0, "data": 0, "weakened": 0}
    sizes = [0] * units

    with open(source, "rb") as src, open(header_path, "wb") as hdr:
        hdr.write(b"/* Generated by ppc-split-tu.py from %s - do not edit. */\n"
                  % base.encode())
        out = [open(p, "wb") for p in unit_paths]
        try:
            for fh in out:
                fh.write(b'#include "%s"\n#include "%s"\n'
                         % (promoted_name.encode(), header_name.encode()))
            homes = {}
            promoted = set()
            macros = set()
            for kind, lines in iter_chunks(src):
                text = b"".join(lines)
                placement, header_text, unit_text, name = classify(text, kind)
                counts[placement] += 1
                if kind == PP:
                    defined = MACRO_NAME.match(text[code_offset(text):])
                    if defined:
                        macros.add(defined.group(1))
                if placement == HEADER:
                    # Prototypes have to lose their `static` here too: a
                    # `static` declaration in scope keeps the definition
                    # internal no matter how the definition itself is spelled.
                    hdr.write(strip_static(header_text) if kind == DECL
                              else header_text)
                    continue
                # A definition: its declaration stays here, in the position it
                # was removed from, and the body goes to a unit. Anything that
                # names an object already placed follows it, so a tentative and
                # an initialised definition of the same name stay together;
                # everything else goes to the least-loaded unit, which keeps
                # the units evenly sized.
                hdr.write(header_text)
                if name is not None and strip_static(text) != text:
                    if name in macros:
                        # mrustc has already pointed this name at another
                        # symbol (`#define ...panic_fmt0g rust_begin_unwind`),
                        # and its `#define` comes later in the header, so ours
                        # would just be overridden. Being `static` was what
                        # stopped the definition colliding with the crate that
                        # really owns the symbol; weak does the same job
                        # without hiding it from the other units.
                        at = code_offset(unit_text)
                        unit_text = (unit_text[:at] + b"__attribute__((weak)) "
                                     + unit_text[at:])
                        counts["weakened"] += 1
                    else:
                        promoted.add(name)
                idx = homes.get(name)
                if idx is None:
                    idx = sizes.index(min(sizes))
                    if name is not None:
                        homes[name] = idx
                out[idx].write(unit_text)
                sizes[idx] += len(unit_text)
        finally:
            for fh in out:
                fh.close()

    # Every promoted name is renamed, because "it was `static`, so nothing else
    # can define it" is not true: mrustc emits a crate-local copy of some items
    # that the owning crate also defines *globally*, and promoting one of those
    # is a duplicate symbol at link time -
    #
    #   ld: duplicate symbol _ZRG3cF10alloc..vec_deque10wrap_index0g in
    #   librusty_backup-...rlib.o and liballoc.rlib.o
    #
    # The rename is left to the preprocessor rather than done by rewriting 800
    # MB of text: one `#define` per name, in a header the units include first,
    # so definitions and references move together for free. mrustc uses the
    # same trick itself (`#define ...panic_fmt0g rust_begin_unwind`), and names
    # it has already given a macro to are left alone - redefining those would
    # fight with it.
    with open(promoted_path, "wb") as fh:
        fh.write(b"/* Generated by ppc-split-tu.py - crate-local names, renamed"
                 b" so promoting them to external linkage cannot collide. */\n")
        for name in sorted(promoted):
            fh.write(b"#define %s %s%s\n" % (name, name, RENAME_SUFFIX))

    with open(stamp_path, "w") as fh:
        fh.write(stamp)

    if not quiet:
        sys.stderr.write(
            "ppc-split-tu: %s -> %d units (header %.1f MB, units %s MB); "
            "%d functions, %d data, %d externed, %d kept in the header; "
            "%d names renamed, %d weakened\n"
            % (base, units, os.path.getsize(header_path) / 1048576.0,
               "/".join("%.0f" % (s / 1048576.0) for s in sizes),
               counts["fn"], counts["data"], counts["extern"], counts["header"],
               len(promoted), counts["weakened"]))
    return unit_paths


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("source", help="the generated .c to split")
    ap.add_argument("-n", "--units", type=int, default=DEFAULT_UNITS,
                    help="how many translation units to produce (default %d)"
                         % DEFAULT_UNITS)
    ap.add_argument("-o", "--outdir", default=None,
                    help="output directory (default <source>.split)")
    ap.add_argument("-f", "--force", action="store_true",
                    help="split even if the existing output looks current")
    ap.add_argument("-q", "--quiet", action="store_true")
    args = ap.parse_args()

    if args.units < 1:
        sys.stderr.write("ppc-split-tu: --units must be at least 1\n")
        return 2
    outdir = args.outdir
    if outdir is None:
        stem = args.source[:-2] if args.source.endswith(".c") else args.source
        outdir = stem + ".split"
    for path in split(args.source, outdir, args.units,
                      force=args.force, quiet=args.quiet):
        print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
