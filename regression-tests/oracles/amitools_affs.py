#!/usr/bin/env python3
"""Read an AFFS volume with amitools' xdftool, as a second implementation.

Two things make xdftool awkward to call directly from a `check` line, and both
cost an afternoon to discover:

1. **It picks geometry from the file extension.** A 2 MB volume named `.adf` is
   rejected outright — "invalid ADF images size: 2097152" — because `.adf` means
   floppy. The same bytes named `.hdf` are read as a hard-disk volume.
2. **It opens read-write before it parses.** Produced artifacts are read-only,
   so it fails with EACCES having never looked at the image. `-r` does not help:
   the open happens first.

So this copies the artifact to a writable `.hdf` in a temp directory, runs
`xdftool list`, and cleans up. Exit status is xdftool's.

Not authoritative. amitools is a reimplementation, so agreement does not prove a
real Amiga will mount the volume and disagreement does not prove it will not.
What it gives is a second opinion on the same bytes, which every AFFS check
before this one lacked — they were our code agreeing with itself.
"""

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: amitools_affs.py <image>", file=sys.stderr)
        return 2
    src = Path(sys.argv[1])
    if not src.is_file():
        print(f"{src}: no such file", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="rb-amitools-") as tmp:
        # .hdf, because the extension is the geometry.
        work = Path(tmp) / "volume.hdf"
        shutil.copyfile(src, work)
        work.chmod(0o644)
        proc = subprocess.run(
            ["xdftool", str(work), "list"],
            capture_output=True,
            text=True,
        )
        sys.stdout.write(proc.stdout)
        sys.stderr.write(proc.stderr)
        if proc.returncode != 0:
            # xdftool reports structural problems on stdout with a zero-ish
            # look; surface the reason on stderr so the verdict carries it.
            print(
                f"xdftool rejected the volume (exit {proc.returncode})",
                file=sys.stderr,
            )
        return proc.returncode


if __name__ == "__main__":
    sys.exit(main())
