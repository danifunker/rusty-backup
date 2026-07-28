#!/usr/bin/env python3
"""A drop-in `cc` that compiles on a real PowerPC Mac over ssh.

mrustc is a transpiler: Rust in, C99 out. It never emits a PowerPC binary --
a PowerPC-Darwin C compiler does that, and there is no sane cross-gcc for
`powerpc-apple-darwin` on a modern host. So the C has to be compiled on real
PowerPC hardware.

Rather than run the pipeline in two manual halves, this script *is* the
compiler as far as mrustc is concerned. Point `CC_powerpc_apple_darwin` at it
and every codegen step transparently ships its `.c` to the Mac, runs gcc there,
and copies the `.o` back. minicargo's dependency graph, parallelism and
incremental rebuilds all keep working.

    export PPC_HOST=admin@192.168.99.116
    export CC_powerpc_apple_darwin=/path/to/scripts/ppc-cc-remote.py

mrustc invokes the compiler as a single shell string with every argument in a
response file (`cc @cmdfile`), so that form is handled here too.

Environment:
  PPC_HOST         ssh destination (required), e.g. admin@192.168.99.116
  PPC_CC           remote compiler (default: MacPorts gcc10 -- mrustc's C
                   needs C11 <stdatomic.h>, which Xcode's gcc-4.x lacks)
  PPC_REMOTE_ROOT  remote mirror of the local build tree (default: ppc-xbuild)
  PPC_SDK          -isysroot to pass, e.g. /Developer/SDKs/MacOSX10.4u.sdk
  PPC_MIN_VERSION  -mmacosx-version-min value, e.g. 10.4
  PPC_LDFLAGS      extra flags for link steps (see DEFAULT_LDFLAGS)
  PPC_CC_VERBOSE   set to echo each remote command
"""

import glob
import os
import shlex
import subprocess
import sys

HOST = os.environ.get("PPC_HOST")
REMOTE_CC = os.environ.get("PPC_CC", "/opt/local/libexec/gcc10-bootstrap/bin/gcc")
REMOTE_ROOT = os.environ.get("PPC_REMOTE_ROOT", "ppc-xbuild")
SDK = os.environ.get("PPC_SDK")
MIN_VERSION = os.environ.get("PPC_MIN_VERSION")
VERBOSE = bool(os.environ.get("PPC_CC_VERBOSE"))

# What a Rust libstd needs on this platform beyond libSystem:
#   -latomic                  32-bit PowerPC has no lock-free 8-byte atomic, so
#                             the __atomic_*_8 calls mrustc emits for AtomicU64
#                             resolve in libatomic (gcc10 ships it).
#   -lMacportsLegacySupport   pthread_setname_np is 10.6+; legacy-support
#                             backfills it (and clock_gettime, and friends).
#   -lgcc_s.1                 _Unwind_GetIPInfo, used by std's DWARF personality
#                             routine. NOTE: present in Leopard's libgcc_s.1 but
#                             *not* in libgcc_s.10.4, so a true 10.4 target needs
#                             gcc10's own unwinder or panic=abort.
DEFAULT_LDFLAGS = "-L/opt/local/lib -latomic -lMacportsLegacySupport -lgcc_s.1"
LDFLAGS = shlex.split(os.environ.get("PPC_LDFLAGS", DEFAULT_LDFLAGS))
# A C file of stand-ins for libc/libm entry points this OS lacks (lgammaf_r).
# Compiled once on the Mac and added to every link.
SHIM = os.environ.get("PPC_SHIM")

# Args that take a separate value we must not mistake for an input file.
VALUE_FLAGS = {"-o", "-include", "-isysroot", "-x", "-Xlinker", "-u"}
# Extensions we treat as files to ship to the Mac. Matched case-insensitively:
# zstd builds `huf_decompress_amd64.S`, and an upper-case `.S` (assembly *with*
# the C preprocessor run over it) is a different thing to gcc than `.s`. Matching
# only the lower-case form meant the file was silently never uploaded and the
# remote gcc failed with a bare "No such file or directory".
INPUT_EXTS = (".c", ".cc", ".cpp", ".cxx", ".o", ".a", ".s", ".h", ".hpp", ".inc")
# Flags naming a *directory* whose contents the remote compiler needs. mrustc
# only ever passes files, but the `-sys` crates' build scripts drive this script
# through cc-rs, which compiles a C source tree: bzip2-sys passes
# `-I bzip2-1.0.8` and zstd-sys a handful of `-I zstd/lib/...`. Shipping the
# named file alone leaves every #include unresolved on the Mac.
INCLUDE_DIR_FLAGS = ("-I", "-isystem", "-iquote", "-idirafter")
# `-L` is handled separately. A library search directory is not wanted for its
# contents the way an include directory is - only for the few archives `-l` can
# resolve out of it - and mirroring it wholesale is actively wrong here: mrustc
# passes `-L <stdlib output dir>` on every link, and that directory is 75 MB of
# .c/.o/.rlib whose object files the link already names explicitly.
LIB_DIR_FLAGS = ("-L",)
# What `-l` can actually resolve, and so all that needs shipping from a `-L` dir.
LIB_PATTERNS = ("*.a", "*.dylib", "*.so", "*.so.*")
DIR_FLAGS = INCLUDE_DIR_FLAGS + LIB_DIR_FLAGS
# ...but a directory argument is not automatically ours to mirror. PPC_LDFLAGS
# names paths that exist on the *Mac* (`-L/opt/local/lib`), and some of those
# prefixes also exist on this machine with entirely different contents - so a
# blind mirror of `-L/usr/lib` would upload a Linux userland onto Leopard. Treat
# anything under a system prefix as a remote path and pass it through untouched.
SYSTEM_PREFIXES = ("/usr/", "/lib/", "/lib64/", "/opt/", "/bin/", "/sbin/",
                   "/etc/", "/var/", "/System/", "/Library/", "/Developer/")
# A guard against mirroring something enormous by accident (a `-I` pointing at a
# home directory, say). Loud failure beats a silent multi-gigabyte rsync.
MAX_MIRROR_BYTES = 64 * 1024 * 1024

# --- the huge-translation-unit problem ---------------------------------------
# mrustc emits one .c per crate, and for the engine crate that is ~800 MB. gcc's
# peak memory scales with the whole unit's internal representation, and `cc1`
# here is a 32-bit PowerPC binary - so it runs out of *address space* (~3.5 GB on
# Darwin) long before the machine runs out of RAM or swap:
#
#     cc1: out of memory allocating 65536 bytes      (at ~2.9 GB RSS)
#
# No amount of swap helps with that; Darwin grows swap on demand and had plenty.
# The levers that do help, applied only to the oversized unit so every other
# crate keeps its normal flags:
#
#   -O0                     the big one. At -O1 gcc runs inter-procedural passes
#                           that need many function bodies live at once; at -O0
#                           it can emit each function and release it. Costs
#                           runtime performance, which is the right trade for
#                           getting a working binary at all.
#   --param ggc-min-expand  make gcc's garbage collector run far more often
#   --param ggc-min-heapsize  (default is to let the heap grow 30% between
#                           collections) - trades compile time for peak memory.
#   -mlongcall              a PowerPC `bl` reaches +/-16 MB, and this unit's
#                           __text alone is 55 MB, so its own internal calls do
#                           not reach:
#                             ld: bl out of range (-26114656 max is +/-16M)
#                           -mlongcall loads the target address and branches
#                           indirectly, removing the limit. Bigger and slower per
#                           call, which is moot next to not linking at all. (-O0
#                           is what makes the text this large; at -O1 it would be
#                           smaller but still well over 16 MB, and -O1 is what
#                           exhausts the address space.)
#
# Tunable, because the right threshold depends on the machine: PPC_BIG_TU_BYTES=0
# disables the special-casing entirely.
BIG_TU_BYTES = int(os.environ.get("PPC_BIG_TU_BYTES", 64 * 1024 * 1024))
BIG_TU_ARGS = shlex.split(os.environ.get(
    "PPC_BIG_TU_ARGS",
    "-O0 -mlongcall -mdynamic-no-pic "
    "--param ggc-min-expand=10 --param ggc-min-heapsize=32768"))
#   -mdynamic-no-pic        `-fPIC` gives every function a PIC base register, and
#                           the offsets from it cannot span an object this size:
#                             ld: 32-bit pic-base out of range in ___mrustc_bitrev32
#                           This is an *executable*, not a dylib, so PIC buys
#                           nothing. `-mdynamic-no-pic` is Darwin/PowerPC's
#                           "non-relocatable code, relocatable external
#                           references" mode, which is exactly right for a
#                           program and drops the PIC base entirely.
#
# Flags to strip when BIG_TU_ARGS takes over: mrustc emits -O1 and -fPIC, and both
# are replaced above.
OPT_FLAGS = ("-O", "-O0", "-O1", "-O2", "-O3", "-Os", "-Ofast",
             "-fPIC", "-fpic", "-fPIE", "-fpie")


def die(msg):
    sys.stderr.write("ppc-cc-remote: %s\n" % msg)
    sys.exit(1)


def expand_response_files(argv):
    """Expand mrustc's `@cmdfile` arguments.

    mrustc writes each argument double-quoted and space-separated, which is
    exactly what shlex reads back.
    """
    out = []
    for a in argv:
        if a.startswith("@"):
            with open(a[1:]) as fh:
                out.extend(shlex.split(fh.read()))
        else:
            out.append(a)
    return out


def run(cmd):
    if VERBOSE:
        sys.stderr.write("ppc-cc-remote: %s\n" % " ".join(shlex.quote(c) for c in cmd))
    return subprocess.call(cmd)


def ensure_shim():
    """Compile the platform shim on the Mac once; return its remote .o path."""
    if not SHIM:
        return None
    if not os.path.isfile(SHIM):
        die("PPC_SHIM does not exist: %s" % SHIM)
    # Paths on the link line are resolved from inside REMOTE_ROOT (the remote
    # command cd's there), so hand back the root-relative form.
    rel_c = "shim/%s" % os.path.basename(SHIM)
    rel_o = rel_c[:-2] + ".o"
    remote_c = "%s/%s" % (REMOTE_ROOT, rel_c)
    remote_o = "%s/%s" % (REMOTE_ROOT, rel_o)
    # Rebuild whenever the source is newer, so editing the shim takes effect.
    if run(["ssh", HOST, "mkdir -p %s/shim" % shlex.quote(REMOTE_ROOT)]) != 0:
        die("failed to create remote shim directory")
    if run(["rsync", "-q", SHIM, "%s:%s" % (HOST, remote_c)]) != 0:
        die("failed to upload the shim")
    rc = run([
        "ssh", HOST,
        "test %s -nt %s || %s -c -O1 -fPIC -o %s %s"
        % (shlex.quote(remote_o), shlex.quote(remote_c),
           shlex.quote(REMOTE_CC), shlex.quote(remote_o), shlex.quote(remote_c)),
    ])
    if rc != 0:
        die("failed to compile the shim")
    return rel_o


def reframework(args):
    """Rewrite `-l Foo` to `-framework Foo` where Foo is a framework, not a library.

    mrustc drops the `kind` from `#[link(name = "CoreFoundation", kind =
    "framework")]` - `src/expand/codegen.cpp` parses it and says
    `// TODO: save and use the kind` - so a framework arrives here as a plain
    library name and the link fails:

        ld: library not found for -lCoreFoundation

    mrustc's own codegen already knows how to emit `-framework X`; it just never
    sees the `framework=` prefix that would trigger it. Fixing it there is the
    right answer, but it is an mrustc change and so costs a full rebuild of every
    crate plus the engine - hours, for a link flag. Done here instead; see the
    open items in docs/build-ppc-mrustc.md.

    The test is deliberately *not* "does a framework of this name exist": on
    Darwin `System` is both `System.framework` and `libSystem.dylib`, and it must
    stay `-lSystem`. Only a name with no library but a framework is rewritten.
    """
    names = []
    i = 0
    while i < len(args):
        if args[i] == "-l" and i + 1 < len(args):
            names.append(args[i + 1])
            i += 2
        else:
            i += 1
    # Frameworks are conventionally capitalised; skip the obvious libraries so the
    # common case costs no round trip at all.
    cands = sorted({n for n in names if n[:1].isupper()})
    if not cands:
        return args

    script = "; ".join(
        'lib=no; for d in /usr/lib /opt/local/lib; do for e in dylib a; do '
        '[ -e "$d/lib%s.$e" ] && lib=yes; done; done; '
        '[ $lib = no ] && [ -d /System/Library/Frameworks/%s.framework ] && echo %s'
        % (n, n, n) for n in cands)
    # ...and a trailing `true`: the last `[ ] && echo` exits non-zero whenever the
    # name is *not* a framework (which is the common case), which would make ssh
    # itself fail and throw the whole classification away.
    script += "; true"
    try:
        out = subprocess.check_output(["ssh", HOST, script]).decode()
    except (subprocess.CalledProcessError, OSError):
        return args                     # classification failed - leave it alone
    fw = set(out.split())
    if not fw:
        return args

    out_args, i = [], 0
    while i < len(args):
        if args[i] == "-l" and i + 1 < len(args) and args[i + 1] in fw:
            out_args += ["-framework", args[i + 1]]
            i += 2
        else:
            out_args.append(args[i])
            i += 1
    sys.stderr.write("ppc-cc-remote: linking %s as framework(s)\n" % ", ".join(sorted(fw)))
    return out_args


def mirrored(path):
    """Where `path` lives inside REMOTE_ROOT on the Mac.

    Always keyed on the *absolute* local path minus its leading slash, which is
    also what `rsync -R` produces. Resolving relative paths against the cwd
    matters now that build scripts drive this script: minicargo runs each one
    with its cwd set to that crate's directory, so `src/foo.c` from two
    different crates would otherwise collide at the same remote path and one
    crate would silently be compiled from the other's source.
    """
    return os.path.abspath(path).lstrip("/")


def tree_size(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
            if total > MAX_MIRROR_BYTES:
                return total
    return total


def libs_in(path):
    """The files in `path` that `-l` could resolve - all a `-L` dir needs."""
    out = []
    for pat in LIB_PATTERNS:
        out.extend(glob.glob(os.path.join(path, pat)))
    return [p for p in out if os.path.isfile(p)]


def should_mirror_dir(path, full):
    """True if `path` is a local directory we should ship to the Mac.

    `full` distinguishes an include directory, which is mirrored recursively for
    the headers in it, from a `-L` directory, where only the handful of archives
    `-l` can resolve are wanted and the size guard does not apply.
    """
    absolute = os.path.abspath(path)
    if any(absolute.startswith(p) for p in SYSTEM_PREFIXES):
        return False        # names a path on the Mac, not here
    if not os.path.isdir(absolute):
        return False        # remote-only, or simply not a directory
    if not full:
        return bool(libs_in(absolute))
    size = tree_size(absolute)
    if size > MAX_MIRROR_BYTES:
        die("refusing to mirror %s (%.1f MB > %.0f MB limit); if this really "
            "is a build directory, raise MAX_MIRROR_BYTES"
            % (absolute, size / 1048576.0, MAX_MIRROR_BYTES / 1048576.0))
    return True


def main():
    if not HOST:
        die("PPC_HOST is not set (e.g. PPC_HOST=admin@192.168.99.116)")

    args = expand_response_files(sys.argv[1:])
    if not args:
        die("no arguments")

    output = None
    inputs = []
    dirs = []
    lib_dirs = []
    # Rewrites applied to the remote command line, keyed on the exact argument
    # string. Covers both the `-I dir` and `-Idir` spellings.
    remap = {}

    i = 0
    while i < len(args):
        a = args[i]
        if a == "-o" and i + 1 < len(args):
            output = args[i + 1]
            i += 2
            continue
        # A directory-valued flag, in either spelling.
        flag = next((f for f in DIR_FLAGS if a == f or
                     (a.startswith(f) and len(a) > len(f))), None)
        if flag is not None:
            target = dirs if flag in INCLUDE_DIR_FLAGS else lib_dirs
            if a == flag and i + 1 < len(args):
                if should_mirror_dir(args[i + 1], full=flag in INCLUDE_DIR_FLAGS):
                    target.append(args[i + 1])
                    remap[args[i + 1]] = mirrored(args[i + 1])
                i += 2
            else:
                value = a[len(flag):]
                if should_mirror_dir(value, full=flag in INCLUDE_DIR_FLAGS):
                    target.append(value)
                    remap[a] = flag + mirrored(value)
                i += 1
            continue
        if a in VALUE_FLAGS:
            i += 2
            continue
        if not a.startswith("-") and a.lower().endswith(INPUT_EXTS) and os.path.isfile(a):
            inputs.append(a)
        i += 1

    if output is None:
        die("no -o in command line: %s" % " ".join(args))

    for p in inputs + [output]:
        remap[p] = mirrored(p)

    remote_dirs = sorted(
        {os.path.dirname(mirrored(p)) for p in inputs + [output]}
        | {mirrored(d) for d in dirs + lib_dirs}
    )
    mkdir = "mkdir -p %s" % " ".join(
        shlex.quote("%s/%s" % (REMOTE_ROOT, d)) for d in remote_dirs if d
    ) if remote_dirs else "true"
    if run(["ssh", HOST, "mkdir -p %s && %s" % (shlex.quote(REMOTE_ROOT), mkdir)]) != 0:
        die("failed to create remote directories")

    # -R rebuilds each source's absolute path under REMOTE_ROOT, so the remote
    # tree mirrors ours. Directories go up recursively: a `-I` directory is
    # wanted for the headers it holds, not for itself.
    uploads = [os.path.abspath(p) for p in inputs] + [os.path.abspath(d) for d in dirs]
    for d in lib_dirs:
        uploads.extend(libs_in(os.path.abspath(d)))
    if uploads:
        rc = run(["rsync", "-qR", "-r", "--"] + uploads + ["%s:%s/" % (HOST, REMOTE_ROOT)])
        if rc != 0:
            die("failed to upload inputs")

    remote_args = [remap.get(a, a) for a in args]
    is_link = "-c" not in remote_args
    if SDK:
        remote_args = ["-isysroot", SDK] + remote_args
    if MIN_VERSION:
        remote_args = ["-mmacosx-version-min=" + MIN_VERSION] + remote_args
    if is_link:
        shim_obj = ensure_shim()
        if shim_obj:
            remote_args.append(shim_obj)
        remote_args.extend(LDFLAGS)
        remote_args = reframework(remote_args)
    elif BIG_TU_BYTES:
        # An oversized translation unit needs its own flags or 32-bit cc1 runs
        # out of address space - see BIG_TU_ARGS above.
        biggest = max([os.path.getsize(p) for p in inputs
                       if p.lower().endswith((".c", ".cc", ".cpp", ".cxx"))] or [0])
        if biggest > BIG_TU_BYTES:
            remote_args = [a for a in remote_args if a not in OPT_FLAGS] + BIG_TU_ARGS
            sys.stderr.write(
                "ppc-cc-remote: %.0f MB translation unit - compiling with %s\n"
                % (biggest / 1048576.0, " ".join(BIG_TU_ARGS)))

    remote_cmd = "cd %s && %s %s" % (
        shlex.quote(REMOTE_ROOT),
        shlex.quote(REMOTE_CC),
        " ".join(shlex.quote(a) for a in remote_args),
    )
    rc = run(["ssh", HOST, remote_cmd])
    if rc != 0:
        return rc

    # Bring the artifact home so mrustc/minicargo see it where they expect.
    out_dir = os.path.dirname(output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    rc = run(["rsync", "-q", "%s:%s/%s" % (HOST, REMOTE_ROOT, remap[output]), output])
    if rc != 0:
        die("failed to retrieve %s" % output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
