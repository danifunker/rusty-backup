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
# Extensions we treat as files to ship to the Mac.
INPUT_EXTS = (".c", ".o", ".a", ".s", ".h")


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


def main():
    if not HOST:
        die("PPC_HOST is not set (e.g. PPC_HOST=admin@192.168.99.116)")

    args = expand_response_files(sys.argv[1:])
    if not args:
        die("no arguments")

    local_root = os.getcwd()
    output = None
    inputs = []

    i = 0
    while i < len(args):
        a = args[i]
        if a == "-o" and i + 1 < len(args):
            output = args[i + 1]
            i += 2
            continue
        if a in VALUE_FLAGS:
            i += 2
            continue
        if not a.startswith("-") and a.endswith(INPUT_EXTS) and os.path.isfile(a):
            inputs.append(a)
        i += 1

    if output is None:
        die("no -o in command line: %s" % " ".join(args))

    # Mirror every path under REMOTE_ROOT. minicargo passes paths relative to
    # mrustc's cwd for a stdlib build but absolute ones when driven with an
    # absolute --output-dir, so both have to work; an absolute path keeps its
    # shape minus the leading slash, which is also what `rsync -R` produces.
    def mirrored(path):
        rel = path.lstrip("/") if os.path.isabs(path) else path
        if rel.startswith(".." + os.sep):
            die("path escapes the build root (not mirrorable): %s" % path)
        return rel

    remap = {p: mirrored(p) for p in inputs + [output]}

    remote_dirs = sorted({os.path.dirname(p) for p in remap.values() if os.path.dirname(p)})
    mkdir = "mkdir -p %s" % " ".join(
        shlex.quote("%s/%s" % (REMOTE_ROOT, d)) for d in remote_dirs
    ) if remote_dirs else "true"
    if run(["ssh", HOST, "mkdir -p %s && %s" % (shlex.quote(REMOTE_ROOT), mkdir)]) != 0:
        die("failed to create remote directories")

    if inputs:
        # -R keeps each source's relative path, so the remote tree mirrors ours.
        rc = run(["rsync", "-qR", "--"] + inputs + ["%s:%s/" % (HOST, REMOTE_ROOT)])
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
