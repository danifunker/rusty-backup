#!/usr/bin/env python3
"""A drop-in `ar` that archives on a real PowerPC Mac over ssh.

The companion to `ppc-cc-remote.py`. Once a build script starts compiling a C
source tree for `powerpc-apple-darwin` (bzip2-sys and zstd-sys both do, through
cc-rs), the objects have to be collected into a static library - and that is not
something the host's `ar` can do. GNU ar writes a System V symbol table; Apple's
linker wants a Mach-O `__.SYMDEF`, so an archive built here would be rejected on
the Mac. So the archive is built where its objects were: on the Mac.

    export PPC_HOST=admin@192.168.99.116
    export AR_powerpc_apple_darwin=/path/to/scripts/ppc-ar-remote.py

cc-rs drives this in two shapes (see its `assemble` / `assemble_progressive`):

    ar cq  <archive> <obj>...    create, then append one batch per call
    ar sD  <archive>             add the symbol table, "deterministic" variant
    ar s   <archive>             ...and the plain fallback

Leopard's `/usr/bin/ar` takes `cq` and `s` but rejects `D` ("illegal option"),
which is exactly the probe cc-rs makes: it tries `sD`, and on failure retries
`s` with ZERO_AR_DATE=1. So a non-zero exit is a legitimate answer here and is
passed straight back rather than being turned into an error.

Environment:
  PPC_HOST         ssh destination (required), e.g. admin@192.168.99.116
  PPC_AR           remote archiver (default: /usr/bin/ar -- Apple's cctools ar,
                   not gcc10's, so the archive is native Mach-O)
  PPC_REMOTE_ROOT  remote mirror of the local build tree (default: ppc-xbuild)
  PPC_CC_VERBOSE   set to echo each remote command
"""

import os
import shlex
import subprocess
import sys

HOST = os.environ.get("PPC_HOST")
REMOTE_AR = os.environ.get("PPC_AR", "/usr/bin/ar")
REMOTE_ROOT = os.environ.get("PPC_REMOTE_ROOT", "ppc-xbuild")
VERBOSE = bool(os.environ.get("PPC_CC_VERBOSE"))

# Passed through to the remote ar: it zeroes the timestamps in the archive, and
# cc-rs sets it whenever it has fallen back from the `D` modifier.
PASS_ENV = ("ZERO_AR_DATE",)


def die(msg):
    sys.stderr.write("ppc-ar-remote: %s\n" % msg)
    sys.exit(1)


def run(cmd):
    if VERBOSE:
        sys.stderr.write("ppc-ar-remote: %s\n" % " ".join(shlex.quote(c) for c in cmd))
    return subprocess.call(cmd)


def mirrored(path):
    """Where `path` lives inside REMOTE_ROOT -- same rule as ppc-cc-remote.py."""
    return os.path.abspath(path).lstrip("/")


def main():
    if not HOST:
        die("PPC_HOST is not set (e.g. PPC_HOST=admin@192.168.99.116)")

    args = sys.argv[1:]
    if not args:
        die("no arguments")

    # The archive is the first `.a` on the command line; everything before it is
    # ar's mode/modifier word, everything after is a member to add.
    archive = next((a for a in args if a.endswith(".a")), None)
    if archive is None:
        die("no archive (.a) in command line: %s" % " ".join(args))
    members = [a for a in args[args.index(archive) + 1:] if os.path.isfile(a)]

    remap = {p: mirrored(p) for p in members}
    remap[archive] = mirrored(archive)

    remote_archive = "%s/%s" % (REMOTE_ROOT, remap[archive])
    uploads = [os.path.abspath(p) for p in members]
    # cc-rs deletes the archive locally before its first `cq` batch and then
    # appends batch by batch, so the local file is the state of record. Upload it
    # when it exists, and clear the remote one when it does not - otherwise a
    # stale archive from an earlier run would silently accumulate duplicate
    # members across builds.
    if os.path.isfile(archive):
        uploads.append(os.path.abspath(archive))
        clear = "true"
    else:
        clear = "rm -f %s" % shlex.quote(remote_archive)

    remote_dirs = sorted({os.path.dirname(p) for p in remap.values() if os.path.dirname(p)})
    mkdir = " ".join(
        "mkdir -p %s;" % shlex.quote("%s/%s" % (REMOTE_ROOT, d)) for d in remote_dirs
    )
    if run(["ssh", HOST, "mkdir -p %s && %s %s"
            % (shlex.quote(REMOTE_ROOT), mkdir, clear)]) != 0:
        die("failed to prepare remote directories")

    if uploads:
        rc = run(["rsync", "-qR", "--"] + uploads + ["%s:%s/" % (HOST, REMOTE_ROOT)])
        if rc != 0:
            die("failed to upload archive members")

    env_prefix = "".join(
        "%s=%s " % (k, shlex.quote(os.environ[k])) for k in PASS_ENV if k in os.environ
    )
    remote_args = [remap.get(a, a) for a in args]
    remote_cmd = "cd %s && %s%s %s" % (
        shlex.quote(REMOTE_ROOT),
        env_prefix,
        shlex.quote(REMOTE_AR),
        " ".join(shlex.quote(a) for a in remote_args),
    )
    rc = run(["ssh", HOST, remote_cmd])
    if rc != 0:
        # A failure here is routine - it is how cc-rs discovers that this `ar`
        # has no `D` modifier. Report it faithfully and let the caller retry.
        return rc

    out_dir = os.path.dirname(archive)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    if run(["rsync", "-q", "%s:%s" % (HOST, remote_archive), archive]) != 0:
        die("failed to retrieve %s" % archive)
    return 0


if __name__ == "__main__":
    sys.exit(main())
