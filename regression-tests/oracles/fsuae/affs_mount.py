#!/usr/bin/env python3
"""Ask a real AmigaOS whether it will mount an AFFS volume we wrote.

This is R-020's oracle. Every AFFS check before it was our formatter agreeing
with our fsck, or — once amitools arrived — a reimplementation agreeing with
our bytes. Neither answers the question the finding actually asks, which is
whether Kickstart's own filesystem mounts the volume or reports
"Not a DOS disk".

## How the verdict gets out

There is no screen scraping. FS-UAE mounts a **host directory** as an Amiga
volume, so the guest writes a file and the host reads it:

    DH0:  bootable Workbench      (host dir, unpacked from the WB1.3 fixture)
    DH1:  the volume under test   (hardfile)
    DH2:  RESULTS                 (host dir, the verdict channel)

A probe appended to `S/Startup-Sequence` runs `Info` and `List DH1:`, redirects
both into `RESULTS:`, then writes `done.txt` as a sentinel. The host polls for
the sentinel and kills the emulator. No sentinel within the timeout is a
*third* outcome, distinct from a bad volume: the guest never got there.

## Why the boot volume is unpacked rather than mounted

DH0 must be writable to take the probe, and the WB1.3 fixture is sha256-pinned
corpus. `xdftool unpack` turns it into a host directory, which is both
modifiable and mountable by FS-UAE directly, so the fixture is only ever read.

## Always run a control

A volume that fails to mount and a harness that never booted look identical
from the host, which is why `--control` exists: it points DH1 at the WB1.3
fixture, a volume a real Amiga certainly mounts. A run whose control fails
proves nothing about the artifact, and this script says so rather than
reporting a defect.

Exit status:
    0  the guest mounted DH1: and reported a name
    1  the guest booted and refused DH1: (this is the R-020 symptom)
    3  the guest never reached the sentinel — harness failure, not a verdict
    2  usage / setup error
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

# The probe *replaces* the fixture's Startup-Sequence rather than being
# appended to it.
#
# Appending was tried first and does not boot. The Workbench 1.3 fixture's
# startup is a real one — `Mount NEWCON:`, `Resident`, RAM: assigns, then
# `Execute S:AFShared-Startup`, an Amiga Forever addition — and under
# Kickstart 3.1 something in that chain never reaches the probe, so no
# sentinel ever appears. Every one of those steps is scenery for this test:
# all it needs is a shell, `Info`, `List` and `Echo`, which are in `c/`.
#
# FAILAT 21 stops AmigaDOS aborting the script when a command against an
# unrecognised volume returns ERROR(20). Without it the run dies before
# writing done.txt, and the host cannot tell "the volume is bad" from "never
# booted" — the two outcomes that most need distinguishing.
PROBE = """FAILAT 21
Echo "rb-regress probe"
Info >RESULTS:info.txt
List DH1: >RESULTS:listing.txt
Echo "DONE" >RESULTS:done.txt
"""

DEFAULT_TIMEOUT = 180


def find_fs_uae() -> Optional[str]:
    """FS-UAE is a per-user install on Windows and not on PATH."""
    on_path = shutil.which("fs-uae")
    if on_path:
        return on_path
    local = os.environ.get("LOCALAPPDATA")
    if local:
        cand = (
            Path(local)
            / "Programs"
            / "FS-UAE"
            / "FS-UAE"
            / "Windows"
            / "x86-64"
            / "fs-uae.exe"
        )
        if cand.is_file():
            return str(cand)
    return None


def find_kickstart() -> Optional[Path]:
    """A 3.1 A1200 ROM out of the licensed Amiga Forever set.

    Kickstart 1.3 has no FFS in ROM — it loaded the handler off the RDB — and
    our volumes are bare DOS\\1 with no RDB, so 1.3 would refuse them for a
    reason that has nothing to do with the bytes under test. 3.1 has FFS in
    ROM and is what R-020 was originally observed on.
    """
    docs = Path.home() / "Documents" / "FS-UAE" / "Kickstarts"
    for name in ("amiga-os-310-a1200.rom", "amiga-os-310-a4000.rom", "amiga-os-310.rom"):
        p = docs / name
        if p.is_file():
            return p
    return None


def build_boot_dir(workdir: Path, wb_fixture: Path) -> Path:
    """Unpack the Workbench fixture to a host directory and add the probe.

    Cached: unpacking is slow and the tree never changes between runs.
    """
    system = workdir / "System"
    stamp = workdir / ".boot-ready"
    if stamp.is_file():
        return system

    if system.exists():
        shutil.rmtree(system)
    # xdftool opens read-write and picks geometry from the extension, so work
    # from a writable .hdf copy rather than the pinned fixture.
    tmp_hdf = workdir / "wb-boot.hdf"
    shutil.copyfile(wb_fixture, tmp_hdf)
    tmp_hdf.chmod(0o644)
    proc = subprocess.run(
        ["xdftool", str(tmp_hdf), "unpack", str(system)],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"xdftool unpack failed: {proc.stderr.strip()}")

    seq = system / "s" / "Startup-Sequence"
    if not seq.parent.is_dir():
        raise RuntimeError(f"no S: directory in the unpacked tree at {seq.parent}")
    # Overwrite, don't append — see the PROBE comment for why the fixture's
    # own startup cannot be reached under Kickstart 3.1.
    seq.write_text(PROBE)

    tmp_hdf.unlink(missing_ok=True)
    stamp.write_text("ok\n")
    return system


def write_config(
    workdir: Path, boot: Path, artifact: Path, results: Path, kickstart: Path
) -> Path:
    cfg = workdir / "probe.fs-uae"
    # Forward slashes throughout: FS-UAE's config parser treats a backslash as
    # an escape, so a Windows path silently mangles.
    cfg.write_text(
        "# rb-regress AFFS mount oracle - generated, do not edit\n"
        "amiga_model = A1200\n"
        f"kickstart_file = {kickstart.as_posix()}\n"
        "fast_memory = 8192\n"
        "\n"
        f"hard_drive_0 = {boot.as_posix()}\n"
        "hard_drive_0_label = Workbench\n"
        "\n"
        f"hard_drive_1 = {artifact.as_posix()}\n"
        "\n"
        f"hard_drive_2 = {results.as_posix()}\n"
        "hard_drive_2_label = RESULTS\n"
        "\n"
        "fullscreen = 0\n"
        "window_width = 640\n"
        "window_height = 480\n"
        "floppy_drive_volume = 0\n"
        "automatic_input_grab = 0\n"
    )
    return cfg


def parse_verdict(info: str) -> Tuple[str, str]:
    """Read the guest's `Info` output for what became of DH1:.

    Amiga `Info` prints one line per mounted unit. An unrecognised volume shows
    as `DH1:      Not a DOS disk`; a good one carries size/used/free and a name.
    """
    for line in info.splitlines():
        if not line.strip().startswith("DH1"):
            continue
        if re.search(r"not a dos disk", line, re.I):
            return "refused", line.strip()
        return "mounted", line.strip()
    return "absent", "no DH1: line in Info output"


def run(args) -> int:
    fs_uae = find_fs_uae()
    if not fs_uae:
        print("fs-uae not found (PATH or %LOCALAPPDATA%/Programs/FS-UAE)", file=sys.stderr)
        return 2
    kickstart = find_kickstart()
    if not kickstart:
        print("no Kickstart 3.1 ROM under ~/Documents/FS-UAE/Kickstarts", file=sys.stderr)
        return 2

    workdir = Path(args.workdir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    results = workdir / "results"
    if results.exists():
        shutil.rmtree(results)
    results.mkdir()

    try:
        boot = build_boot_dir(workdir, Path(args.workbench).resolve())
    except RuntimeError as e:
        print(f"boot volume: {e}", file=sys.stderr)
        return 2

    # FS-UAE wants a hardfile extension it recognises, and the artifact is
    # read-only in the tree; copy it in either way.
    under_test = workdir / "under-test.hdf"
    shutil.copyfile(Path(args.image).resolve(), under_test)
    under_test.chmod(0o644)

    cfg = write_config(workdir, boot, under_test, results, kickstart)

    print(f"fs-uae     : {fs_uae}")
    print(f"kickstart  : {kickstart.name}")
    print(f"under test : {args.image}")
    print(f"config     : {cfg}")

    proc = subprocess.Popen(
        [fs_uae, str(cfg)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    sentinel = results / "done.txt"
    deadline = time.time() + args.timeout
    try:
        while time.time() < deadline:
            if sentinel.is_file():
                break
            if proc.poll() is not None:
                break
            time.sleep(1.0)
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()

    if not sentinel.is_file():
        print(
            "\nthe guest never reached the sentinel — this is a harness result, "
            "not a verdict on the volume",
            file=sys.stderr,
        )
        return 3

    info = (results / "info.txt").read_text(errors="replace") if (results / "info.txt").is_file() else ""
    listing = (
        (results / "listing.txt").read_text(errors="replace")
        if (results / "listing.txt").is_file()
        else ""
    )
    verdict, line = parse_verdict(info)

    print("\n--- guest Info ---")
    print(info.strip() or "(empty)")
    if listing.strip():
        print("\n--- guest List DH1: ---")
        print(listing.strip()[:1000])

    print(f"\nverdict: {verdict}  ({line})")
    if verdict == "mounted":
        return 0
    if verdict == "refused":
        return 1
    return 3


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("image", help="the AFFS volume to put in front of AmigaOS")
    ap.add_argument(
        "--workbench",
        default="regression-tests/fixtures/fs.affs.workbench13.hd.hdf",
        help="bootable Workbench fixture, unpacked to a host dir for DH0",
    )
    ap.add_argument("--workdir", default="regression-tests/scratch/fsuae")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    return run(ap.parse_args())


if __name__ == "__main__":
    sys.exit(main())
