#!/usr/bin/env python3
"""Capture ground truth for a `powerpc-apple-darwin` libc port, from real PowerPC.

No Rust `libc` arch file exists for PowerPC Macs -- the crate postdates them --
so one has to be written. The failure mode when writing one by reading headers
is silent: a struct field lands at the wrong offset and every syscall through it
returns plausible garbage. Big-endian PowerPC makes that worse, and Darwin/PPC
uses the "power" alignment ABI (a struct's alignment follows its *first* member,
so a `long long` that is not first is only 4-aligned) which differs from the
max-member rule Rust's `repr(C)` applies.

So don't read the headers. Compile probes on the real machine against the real
SDK and let the C compiler answer.

This script:
  1. parses the `libc` sources that a `powerpc-apple-darwin` build selects,
     collecting every struct, type alias and constant it declares;
  2. generates a C program that prints sizeof / alignof / offsetof for each;
  3. compiles and runs it on a PowerPC Mac over ssh, iteratively dropping the
     probe lines the compiler rejects (a name libc declares that this SDK does
     not have is itself a finding, and is reported);
  4. writes the results as TSV.

Run it once per (arch, SDK) pair and diff: `ppc` vs `i386` against the same SDK
isolates what is genuinely PowerPC, and 10.4u vs 10.5 isolates what is version
drift (`$INODE64`, `$UNIX2003`, birthtime-era `struct stat`).

Usage:
    scripts/ppc-libc-probe.py --libc ~/repos/mrustc/rustc-1.74.0-src/vendor/libc \\
        --host user@ppc-host \\
        --sdk /Developer/SDKs/MacOSX10.4u.sdk --arch ppc \\
        --out probe-ppc-10.4.tsv
"""

import argparse
import os
import re
import shlex
import subprocess
import sys

# Headers to pull in before the probes. Deliberately broad: a probe that
# references a type we did not include just gets dropped, which would read as
# "this SDK lacks it" and be a false finding.
HEADERS = """
#include <stdio.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>
#include <termios.h>
#include <utime.h>
#include <poll.h>
#include <sched.h>
#include <semaphore.h>
#include <glob.h>
#include <regex.h>
#include <langinfo.h>
#include <locale.h>
#include <netdb.h>
#include <grp.h>
#include <pwd.h>
#include <aio.h>
#include <spawn.h>
#include <syslog.h>
#include <wchar.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/mount.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/select.h>
#include <sys/sysctl.h>
#include <sys/utsname.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/msg.h>
#include <sys/param.h>
#include <sys/ucred.h>
#include <sys/event.h>
#include <sys/attr.h>
#include <sys/xattr.h>
#include <sys/quota.h>
#include <sys/proc.h>
#include <sys/user.h>
#include <net/if.h>
#include <net/if_dl.h>
#include <net/route.h>
#include <net/bpf.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <mach/mach_time.h>
#include <mach/mach_init.h>
#include <mach/task_info.h>
#include <mach-o/dyld.h>
#include <copyfile.h>
#include <libproc.h>
#include <malloc/malloc.h>
"""

# The cfg values a `powerpc-apple-darwin` build sees. The `libc_*` flags come
# from the build-script override set (script-overrides/stable-1.74.0-macos-powerpc/
# build_libc.txt) -- libc gates real type definitions on them, so guessing wrong
# silently changes layouts.
TARGET_CFG = {
    "target_os": "macos",
    "target_arch": "powerpc",
    "target_pointer_width": "32",
    "target_endian": "big",
    "target_vendor": "apple",
    "target_family": "unix",
    "target_env": "",
}
TARGET_FLAGS = {
    "unix",
    "darwin",
    "libc_priv_mod_use",
    "libc_union",
    "libc_const_size_of",
    "libc_align",
    "libc_core_cvoid",
    "libc_packedN",
    "libc_cfg_target_vendor",
    "libc_thread_local",
    "libc_const_extern_fn",
}

RE_CFG_PRED = re.compile(r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:=\s*"([^"]*)")?\s*$')


def _split_args(s):
    """Split `a, b(c, d), e` on top-level commas."""
    out, depth, cur = [], 0, ""
    for ch in s:
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        if ch == "," and depth == 0:
            out.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip():
        out.append(cur)
    return out


def eval_cfg(expr):
    """Evaluate a `cfg(..)` predicate for this target. None means 'unknown'."""
    expr = expr.strip()
    for fn, combine in (("any", any), ("all", all)):
        if expr.startswith(fn + "(") and expr.endswith(")"):
            parts = [eval_cfg(a) for a in _split_args(expr[len(fn) + 1 : -1])]
            if None in parts:
                # Unknown operands can still be decided: `any` is true if some
                # known operand is true, `all` false if some known one is false.
                if combine is any and True in parts:
                    return True
                if combine is all and False in parts:
                    return False
                return None
            return combine(parts)
    if expr.startswith("not(") and expr.endswith(")"):
        inner = eval_cfg(expr[4:-1])
        return None if inner is None else not inner
    m = RE_CFG_PRED.match(expr)
    if not m:
        return None
    key, val = m.group(1), m.group(2)
    if val is None:
        return True if key in TARGET_FLAGS else (False if key.startswith("libc_") else None)
    if key in TARGET_CFG:
        return TARGET_CFG[key] == val
    if key == "feature":
        # No non-default features are enabled for the stdlib build of libc.
        return False
    return None


def select_cfg_if(text):
    """Replace each `cfg_if! { .. }` with only the branch this target selects.

    libc declares the same type several times over (`uid_t` is `c_ushort`,
    `i32` or `u32` depending on the OS), so scanning the raw text and taking
    the first hit picks an arbitrary target's definition. Branches whose
    predicate we cannot decide are kept, which degrades to the old behaviour
    rather than dropping real declarations.
    """
    out, i = [], 0
    while True:
        j = text.find("cfg_if!", i)
        if j < 0:
            out.append(text[i:])
            return "".join(out)
        out.append(text[i:j])
        k = text.find("{", j)
        if k < 0:
            out.append(text[j:])
            return "".join(out)
        depth, end = 1, k + 1
        while end < len(text) and depth:
            if text[end] == "{":
                depth += 1
            elif text[end] == "}":
                depth -= 1
            end += 1
        out.append(_select_branch(text[k + 1 : end - 1]))
        i = end


def _select_branch(body):
    """Pick the taken arm of an `if #[cfg(..)] {..} else if .. {..} else {..}` chain."""
    arms, pos, else_body = [], 0, None
    while pos < len(body):
        m = re.compile(r"\bif\s*#\[cfg\((.*?)\)\]\s*\{", re.S).search(body, pos)
        if not m:
            m_else = re.compile(r"\belse\s*\{").search(body, pos)
            if m_else:
                depth, end = 1, m_else.end()
                while end < len(body) and depth:
                    if body[end] == "{":
                        depth += 1
                    elif body[end] == "}":
                        depth -= 1
                    end += 1
                else_body = body[m_else.end() : end - 1]
            break
        depth, end = 1, m.end()
        while end < len(body) and depth:
            if body[end] == "{":
                depth += 1
            elif body[end] == "}":
                depth -= 1
            end += 1
        arms.append((m.group(1), body[m.end() : end - 1]))
        pos = end

    for cond, arm in arms:
        v = eval_cfg(cond)
        if v is True:
            return select_cfg_if(arm)
        if v is None:
            # Undecidable: keep every remaining arm rather than guess.
            return select_cfg_if(body)
    return select_cfg_if(else_body) if else_body is not None else ""


# `pub struct NAME {` inside any of libc's struct-declaring macros. Applied
# after cfg_if selection, so only definitions this target actually sees remain.
RE_STRUCT = re.compile(r"\bpub struct\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{")
RE_TYPE = re.compile(r"^\s*pub type\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([^;]+);", re.M)
RE_CONST = re.compile(
    r"^\s*pub const\s+([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([^=]+?)\s*=\s*([^;]+);", re.M
)
RE_FIELD = re.compile(r"^\s*(?:pub\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.+?),?\s*$")

# libc source files a powerpc-apple-darwin build pulls in, most-specific last.
LIBC_MODULES = [
    "src/unix/mod.rs",
    "src/unix/bsd/mod.rs",
    "src/unix/bsd/apple/mod.rs",
    "src/unix/bsd/apple/b32/mod.rs",
    "src/unix/bsd/apple/b32/align.rs",
]


def strip_comments(text):
    text = re.sub(r"//[^\n]*", "", text)
    return re.sub(r"/\*.*?\*/", "", text, flags=re.S)


def read_module(path):
    """Read a libc module, reduced to the declarations this target selects."""
    return select_cfg_if(strip_comments(open(path, encoding="utf-8", errors="replace").read()))


RE_FIELD_CFG = re.compile(r"^#\[cfg\((.*)\)\]$")


def parse_struct_fields(body):
    """Return [(name, type)] for a struct body, honouring per-field `#[cfg(..)]`.

    libc declares alternative fields in place rather than alternative structs --
    `timespec::tv_nsec` is `i64` on x86_64-with-32-bit-pointers and `c_long`
    everywhere else. Taking both would produce a struct that exists nowhere.
    """
    fields, skip_next = [], False
    for line in body.split("\n"):
        line = line.strip()
        if not line:
            continue
        cm = RE_FIELD_CFG.match(line)
        if cm:
            # Unknown predicates keep the field, matching select_cfg_if().
            skip_next = eval_cfg(cm.group(1)) is False
            continue
        if line.startswith("#"):
            continue
        fm = RE_FIELD.match(line)
        if fm and fm.group(1) not in ("pub", "struct"):
            if skip_next:
                skip_next = False
                continue
            fields.append((fm.group(1), fm.group(2).rstrip(",")))
        skip_next = False
    return fields


def struct_body(text, start):
    """Brace-match a struct body starting just after its opening `{`."""
    depth, i = 1, start
    while i < len(text) and depth:
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
        i += 1
    return text[start : i - 1]


def parse_structs(text):
    """Return [(name, [field, ...])] for every `pub struct X { .. }` in `text`."""
    out = []
    for m in RE_STRUCT.finditer(text):
        body = struct_body(text, m.end())
        out.append((m.group(1), [f for f, _t in parse_struct_fields(body)]))
    return out


def collect(libc_dir):
    structs, types, consts = [], {}, []
    seen_struct = set()
    for rel in LIBC_MODULES:
        path = os.path.join(libc_dir, rel)
        if not os.path.exists(path):
            continue
        text = read_module(path)
        for name, fields in parse_structs(text):
            if name in seen_struct:
                continue
            seen_struct.add(name)
            structs.append((name, fields, rel))
        for m in RE_TYPE.finditer(text):
            types.setdefault(m.group(1), m.group(2).strip())
        for m in RE_CONST.finditer(text):
            consts.append((m.group(1), m.group(2).strip(), m.group(3).strip(), rel))
    return structs, types, consts


def generate_c(structs, consts):
    """Emit the probe program, one probe per line so failures can be dropped."""
    # One source line per list entry, so a compiler-reported line number indexes
    # straight into `lines` / `index`.
    lines = HEADERS.strip("\n").split("\n") + ["int main(void) {"]
    index = [None] * len(lines)  # parallel to `lines`: what each probe measures

    def emit(code, what):
        lines.append(code)
        index.append(what)

    for name, fields, _rel in structs:
        # Try both spellings; libc's name may be a C tag (`struct stat`) or a
        # typedef (`pthread_attr_t`). Whichever does not exist gets dropped.
        for cname in ("struct %s" % name, name):
            emit(
                '  printf("S\\t%s\\t%s\\t%d\\t%d\\n", "{n}", "{c}", (int)sizeof({c}), (int)__alignof__({c}));'.format(
                    n=name, c=cname
                ),
                ("struct", name),
            )
            for f in fields:
                emit(
                    '  printf("F\\t%s\\t%s\\t%d\\t%d\\n", "{n}", "{f}", (int)offsetof({c}, {f}), (int)sizeof((({c}*)0)->{f}));'.format(
                        n=name, c=cname, f=f
                    ),
                    ("field", "%s.%s" % (name, f)),
                )

    for name, _ty, _val, _rel in consts:
        emit(
            '  printf("C\\t%s\\t%lld\\n", "{n}", (long long)({n}));'.format(n=name),
            ("const", name),
        )

    lines.append("  return 0;")
    lines.append("}")
    index.append(None)
    index.append(None)
    return lines, index


def remote(host, cmd):
    return subprocess.run(
        ["ssh", host, cmd], capture_output=True, text=True, errors="replace"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--libc", required=True, help="path to the vendored libc crate")
    ap.add_argument("--host", required=True, help="ssh destination of the PowerPC Mac")
    ap.add_argument("--sdk", default="/Developer/SDKs/MacOSX10.4u.sdk")
    ap.add_argument("--arch", default="ppc")
    ap.add_argument("--cc", default="/usr/bin/gcc-4.0", help="probes need no C11")
    ap.add_argument(
        "--legacy-inode",
        action="store_true",
        help="probe the pre-$INODE64 struct layouts instead. Off by default: "
        "libc binds stat$INODE64 / readdir$INODE64 / opendir$INODE64 (see "
        "libc's src/unix/mod.rs), so the 64-bit-inode layout is the one its "
        "struct definitions have to match. 10.4's headers predate the macro "
        "and ignore it, which is exactly the mismatch worth seeing.",
    )
    ap.add_argument("--out", required=True)
    ap.add_argument("--remote-dir", default="/tmp/libc-probe")
    ap.add_argument("--max-rounds", type=int, default=25)
    args = ap.parse_args()

    structs, types, consts = collect(args.libc)
    sys.stderr.write(
        "parsed %d structs, %d type aliases, %d constants\n"
        % (len(structs), len(types), len(consts))
    )

    lines, index = generate_c(structs, consts)
    dropped = []

    tag = "%s-%s" % (args.arch, os.path.basename(args.sdk))
    remote_c = "%s/probe-%s.c" % (args.remote_dir, tag)
    remote_bin = "%s/probe-%s" % (args.remote_dir, tag)
    remote(args.host, "mkdir -p %s" % shlex.quote(args.remote_dir))

    # No -fmax-errors: it postdates gcc-4.0, and gcc reports every error anyway.
    inode = "" if args.legacy_inode else "-D_DARWIN_USE_64_BIT_INODE "
    compile_cmd = (
        "%s -arch %s -isysroot %s -mmacosx-version-min=10.4 %s"
        "-Wno-deprecated-declarations -o %s %s"
        % (
            shlex.quote(args.cc),
            shlex.quote(args.arch),
            shlex.quote(args.sdk),
            inode,
            shlex.quote(remote_bin),
            shlex.quote(remote_c),
        )
    )

    for rnd in range(args.max_rounds):
        src = "\n".join(lines)
        p = subprocess.run(
            ["ssh", args.host, "cat > %s" % shlex.quote(remote_c)],
            input=src,
            text=True,
            capture_output=True,
        )
        if p.returncode != 0:
            sys.exit("failed to upload probe: %s" % p.stderr)

        r = remote(args.host, compile_cmd)
        if r.returncode == 0:
            break

        # Drop every probe line the compiler complained about, and go again.
        bad = set()
        for m in re.finditer(
            r"^[^\s:]*probe-[^\s:]*\.c:(\d+):.*?\berror\b", r.stderr, re.M
        ):
            bad.add(int(m.group(1)) - 1)  # 1-indexed -> list index
        # Header lines can fail too - Tiger has no <spawn.h>/<copyfile.h>/<libproc.h>,
        # those arrived in 10.5 - so they are droppable as well, just not reported
        # as libc findings.
        bad = {b for b in bad if 0 <= b < len(lines) and lines[b].strip() != "int main(void) {"}
        if not bad:
            sys.stderr.write(r.stderr[-4000:])
            sys.exit("compile failed with no attributable probe line (round %d)" % rnd)
        for b in bad:
            if index[b] is not None:
                dropped.append(index[b])
                lines[b] = "  /* dropped: %s */" % (index[b],)
            else:
                sys.stderr.write("  (dropping unavailable header: %s)\n" % lines[b].strip())
                lines[b] = "/* dropped: %s */" % lines[b].strip()
        sys.stderr.write("round %d: dropped %d probes\n" % (rnd, len(bad)))
    else:
        sys.exit("did not converge in %d rounds" % args.max_rounds)

    r = remote(args.host, remote_bin)
    if r.returncode != 0:
        sys.exit("probe binary failed: %s" % r.stderr)

    with open(args.out, "w") as fh:
        fh.write("# arch=%s sdk=%s cc=%s inode64=%s\n"
                 % (args.arch, args.sdk, args.cc, not args.legacy_inode))
        fh.write(r.stdout)
    sys.stderr.write("wrote %s (%d lines)\n" % (args.out, r.stdout.count("\n")))

    # A name libc declares that this SDK does not have is a real finding, not
    # noise -- report it rather than swallowing it.
    if dropped:
        missing = args.out + ".missing"
        with open(missing, "w") as fh:
            for kind, name in sorted(set(dropped)):
                fh.write("%s\t%s\n" % (kind, name))
        sys.stderr.write(
            "wrote %s (%d names libc declares but this SDK/arch rejected;\n"
            "  note each struct is probed under two spellings, so one drop per\n"
            "  struct is expected and only means the other spelling was right)\n"
            % (missing, len(set(dropped)))
        )


if __name__ == "__main__":
    main()
