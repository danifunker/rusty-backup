#!/usr/bin/env python3
"""Ask a real AmigaOS whether it will mount an SFS volume we wrote.

The F-009 counterpart of `affs_mount.py`, and the same argument applies: every
SFS check before it was our writer agreeing with our reader. This boots
Kickstart, hands it the volume, and reads back what SFS itself made of it.

## What is different from the AFFS oracle

Two things, both consequences of SFS not being in ROM.

**The handler has to be staged.** Kickstart has an FFS in ROM and no SFS, so
`L:SmartFilesystem` is copied into the boot volume. Neither Amiga Forever nor
AmigaVision ships one; the corpus carries a copy taken from the SFS reference
fixture's own `L:`.

**The volume has to be mounted by hand.** An AFFS superfloppy is mounted by
UAE + Kickstart with no help. An SFS one cannot be: with no RDB there is
nothing on the disk naming its filesystem, so UAE attaches the unit, Kickstart
tries FFS, and reports `Not a DOS disk` — which would look exactly like the
defect under test. So the probe mounts it explicitly from a generated
MountList that names the handler and DosType, and asks about `SFSTEST:`
rather than `DH1:`.

Geometry in that MountList is computed to cover the image exactly. A
`HighCyl` short of the end silently hides the tail of the volume, and one past
it makes the handler read off the end — either way the mount fails for a
reason that has nothing to do with the bytes under test.

## Always run a control

Same rule as the AFFS oracle, and more load-bearing here because a mount can
now fail for two extra reasons (handler missing, MountList wrong). `--control`
puts the untouched reference fixture through the identical config. A run whose
control does not mount says nothing about the artifact, and this script reports
that rather than a defect.

Exit status:
    0  the guest mounted the volume and reported a volume name
    1  the guest ran and declined it. A verdict.
    3  the guest never reached the sentinel — harness failure, NOT a verdict
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

# `Mount` before `Info` so the volume exists to be asked about, and `FAILAT 21`
# so a failed mount does not abort the script before the sentinel is written —
# the failure is the result we came for.
PROBE = """FAILAT 21
Echo "rb-regress sfs probe"
Echo "BOOTED" >RESULTS:booted.txt
Assign L: SYS:l
List SYS:l >RESULTS:lseg.txt
Version >RESULTS:version.txt
SYS:l/Mount-3x SFSTEST: >RESULTS:mount.txt
Why >RESULTS:why.txt
Echo "MOUNT RETURNED" >RESULTS:mounted.txt
Info >RESULTS:info.txt
Echo "DONE" >RESULTS:done.txt
List SFSTEST: >RESULTS:listing.txt
"""

# `Info` carries the verdict, so the sentinel is written before `List` rather
# than after it. On a volume that did not mount, `List` blocks on AmigaDOS's
# "please insert volume" requester until the timeout — which looked exactly
# like a guest that never booted, the one outcome this oracle must never
# confuse with a real verdict.

# SFS puts its root block at partition-relative block 0, so no blocks are
# reserved ahead of it the way OFS/FFS reserve boot blocks.
MOUNTLIST = """SFSTEST:
    Device         = uaehf.device
    Unit           = {unit}
    Flags          = 0
    Surfaces       = {surfaces}
    BlocksPerTrack = {sectors}
    Reserved       = {reserved}
    Interleave     = 0
    LowCyl         = 0
    HighCyl        = {high_cyl}
    Buffers        = 30
    BufMemType     = 1
    StackSize      = 8192
    Priority       = 10
    GlobVec        = -1
    DosType        = 0x53465300
    FileSystem     = SYS:l/SmartFilesystem
    Mount          = 0
#
"""

DEFAULT_TIMEOUT = 240
BLOCK = 512


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

    3.1 for the same reason the AFFS oracle uses it: 1.3 loaded its filesystem
    off the RDB, and these volumes have none.
    """
    docs = Path.home() / "Documents" / "FS-UAE" / "Kickstarts"
    for name in ("amiga-os-310-a1200.rom", "amiga-os-310-a4000.rom", "amiga-os-310.rom"):
        p = docs / name
        if p.is_file():
            return p
    return None


def geometry(size_bytes: int, reserved: int) -> Tuple[int, int, int]:
    """Surfaces / sectors / HighCyl covering `size_bytes` exactly.

    Tries the conventional shapes first and takes the first that divides the
    image evenly, so the mount spans the whole volume and not a byte more.
    Falls back to a one-block cylinder, which always divides.
    """
    blocks = size_bytes // BLOCK
    for surfaces, sectors in ((16, 63), (8, 63), (4, 63), (16, 32), (1, 32), (1, 1)):
        per_cyl = surfaces * sectors
        if blocks % per_cyl == 0:
            return surfaces, sectors, blocks // per_cyl - 1
    return 1, 1, blocks - 1


def write_probe(system: Path, image_size: int, reserved: int, unit: int):
    """Write the probe and its MountList as bytes with LF endings.

    `write_text` is wrong here and cost the AFFS oracle an afternoon: on
    Windows it turns every newline into CRLF, and AmigaDOS scripts are
    LF-terminated. The guest then tries to redirect into a filename ending in
    a carriage return, which on a host-directory volume is an illegal Windows
    name, so every redirect fails silently.
    """
    seq = system / "s" / "Startup-Sequence"
    if not seq.parent.is_dir():
        raise RuntimeError(f"no S: directory in the unpacked tree at {seq.parent}")
    seq.write_bytes(PROBE.replace("\r\n", "\n").encode("ascii"))

    surfaces, sectors, high_cyl = geometry(image_size, reserved)
    devs = system / "devs"
    devs.mkdir(exist_ok=True)
    entry = MOUNTLIST.format(
        surfaces=surfaces,
        sectors=sectors,
        reserved=reserved,
        high_cyl=high_cyl,
        unit=unit,
    )
    # Workbench 1.3's `Mount` predates `Mount <name> from <file>` — it reads
    # DEVS:MountList and nothing else — so the entry is appended to the
    # fixture's own list. `.orig` is kept from the first unpack so repeated
    # runs re-append to a clean base instead of stacking duplicates.
    mountlist = devs / "MountList"
    pristine = devs / "MountList.orig"
    if not pristine.is_file():
        pristine.write_bytes(mountlist.read_bytes() if mountlist.is_file() else b"")
    base = pristine.read_bytes()
    if base and not base.endswith(b"\n"):
        base += b"\n"
    mountlist.write_bytes(base + entry.replace("\r\n", "\n").encode("ascii"))
    return surfaces, sectors, high_cyl


def stage_handler(system: Path, handler: Path) -> None:
    """Copy the SFS handler into the boot volume's `L:`.

    Named `SmartFilesystem` to match the MountList's `FileSystem` line; the
    corpus copy carries a longer name.
    """
    if not handler.is_file():
        raise RuntimeError(
            f"no SFS handler at {handler} — it is corpus, not repo; pass --handler "
            "or run `rb-regress fixtures --sync`"
        )
    lib = system / "l"
    lib.mkdir(exist_ok=True)
    shutil.copyfile(handler, lib / "SmartFilesystem")
    # Workbench 1.3's own `Mount` (34.x) builds a device node the SFS handler
    # will not start from — it predates the filesystem-mounting conventions
    # OS 2.0 introduced. The 3.x `Mount` staged here comes out of the SFS
    # reference fixture's own C:, read with rb-cli.
    mount3x = handler.parent / "Mount-3x"
    if mount3x.is_file():
        shutil.copyfile(mount3x, lib / "Mount-3x")


def build_boot_dir(workdir: Path, wb_fixture: Path, handler: Path) -> Path:
    """Unpack the Workbench fixture to a host directory, add probe and handler.

    Cached: unpacking is slow and the tree never changes between runs. The
    probe, MountList and handler are rewritten every run regardless — they are
    small, and letting them drift from source would cost far more than the copy.
    """
    system = workdir / "System"
    stamp = workdir / ".boot-ready"
    if stamp.is_file():
        return system

    if system.exists():
        shutil.rmtree(system)
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
    tmp_hdf.unlink(missing_ok=True)
    stamp.write_text("ok\n")
    return system


def write_config(
    workdir: Path,
    boot: Path,
    artifact: Path,
    results: Path,
    kickstart: Path,
    handler: Optional[Path] = None,
    surfaces: int = 0,
    sectors: int = 0,
    reserved: int = 0,
) -> Path:
    cfg = workdir / "sfs-probe.fs-uae"
    # Forward slashes throughout: FS-UAE's config parser treats a backslash as
    # an escape, so a Windows path silently mangles.
    cfg.write_text(
        "# rb-regress SFS mount oracle - generated, do not edit\n"
        "amiga_model = A1200\n"
        f"kickstart_file = {kickstart.as_posix()}\n"
        "fast_memory = 8192\n"
        "\n"
        f"hard_drive_0 = {boot.as_posix()}\n"
        "hard_drive_0_label = Workbench\n"
        "\n"
        # Unit 1 on uaehf.device, which is what the MountList addresses. It is
        # deliberately not given a label: Kickstart will still try to mount it
        # as FFS and fail, and that failure is expected noise, not the verdict.
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
    """Read the guest's `Info` for what became of SFSTEST:.

    Three guest-side states: a size/used/free line means SFS mounted it;
    `Not a DOS disk` means the handler was reached and declined the bytes; no
    line at all means the mount never produced a volume, usually the handler
    failing to load. Only a missing sentinel is a harness failure, and the
    caller decides that.
    """
    for line in info.splitlines():
        stripped = line.strip()
        if not stripped.upper().startswith("SFSTEST"):
            continue
        if re.search(r"not a dos disk", stripped, re.I):
            return "refused", stripped
        return "mounted", stripped
    return "absent", "no SFSTEST: volume appeared"


def run(args) -> int:
    fs_uae = find_fs_uae()
    if not fs_uae:
        print("fs-uae not found (PATH or %LOCALAPPDATA%/Programs/FS-UAE)", file=sys.stderr)
        return 2
    kickstart = find_kickstart()
    if not kickstart:
        print("no Kickstart 3.1 ROM under ~/Documents/FS-UAE/Kickstarts", file=sys.stderr)
        return 2

    image = Path(args.control if args.control else args.image).resolve()
    if not image.is_file():
        print(f"no image at {image}", file=sys.stderr)
        return 2

    workdir = Path(args.workdir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    results = workdir / "results"
    if results.exists():
        shutil.rmtree(results)
    results.mkdir()

    try:
        boot = build_boot_dir(workdir, Path(args.workbench).resolve(), Path(args.handler))
        stage_handler(boot, Path(args.handler).resolve())
        geo = write_probe(boot, image.stat().st_size, args.reserved, args.unit)
    except RuntimeError as e:
        print(f"boot volume: {e}", file=sys.stderr)
        return 2

    under_test = workdir / "under-test.hdf"
    shutil.copyfile(image, under_test)
    under_test.chmod(0o644)
    cfg = write_config(
        workdir,
        boot,
        under_test,
        results,
        kickstart,
        Path(args.handler).resolve() if args.attach else None,
        geo[0],
        geo[1],
        args.reserved,
    )

    print(f"fs-uae     : {fs_uae}")
    print(f"kickstart  : {kickstart.name}")
    print(f"under test : {image}{'  (CONTROL)' if args.control else ''}")
    print(f"geometry   : {geo[0]} surfaces x {geo[1]} sectors, HighCyl {geo[2]}, "
        f"Reserved {args.reserved}, unit {args.unit}")
    print(f"config     : {cfg}")

    proc = subprocess.Popen(
        [fs_uae, str(cfg)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    sentinel = results / "done.txt"
    deadline = time.time() + args.timeout
    try:
        while time.time() < deadline:
            if sentinel.is_file() or proc.poll() is not None:
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
            "\nthe guest never reached the sentinel — a harness result, not a "
            "verdict on the volume",
            file=sys.stderr,
        )
        return 3

    def read(name: str) -> str:
        p = results / name
        return p.read_text(errors="replace") if p.is_file() else ""

    info, listing = read("info.txt"), read("listing.txt")
    verdict, line = parse_verdict(info)

    print("\n--- guest Info ---")
    print(info.strip() or "(empty)")
    if listing.strip():
        print("\n--- guest List SFSTEST: ---")
        print(listing.strip()[:1500])

    print(f"\nverdict: {verdict}  ({line})")
    return 0 if verdict == "mounted" else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("image", nargs="?", help="the SFS volume to put in front of AmigaOS")
    ap.add_argument(
        "--control",
        metavar="IMAGE",
        help="mount this instead — a volume known good, to prove the harness discriminates",
    )
    ap.add_argument(
        "--workbench",
        default="regression-tests/fixtures/fs.affs.workbench13.hd.hdf",
        help="bootable Workbench fixture, unpacked to a host dir for DH0",
    )
    ap.add_argument(
        "--handler",
        default="regression-tests/fixtures/oracle-assets/amiga/SmartFileSystemFixed",
        help="SFS handler binary, staged into the guest's L: (corpus, not repo)",
    )
    ap.add_argument(
        "--unit",
        type=int,
        default=1,
        help="uaehf.device unit for the volume. FS-UAE numbers units across "
        "hardfiles only, so the directory drives on hard_drive_0/2 do not "
        "consume one and the artifact is unit 0, not 1",
    )
    ap.add_argument(
        "--reserved",
        type=int,
        default=1,
        help="MountList Reserved blocks ahead of the SFS root block",
    )
    ap.add_argument(
        "--attach",
        action="store_true",
        help="attach the SFS handler to the hardfile via uae_hardfile2 instead "
        "of mounting from the guest MountList",
    )
    ap.add_argument("--workdir", default="regression-tests/scratch/fsuae-sfs")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = ap.parse_args()
    if not args.image and not args.control:
        ap.error("give an image, or --control IMAGE")
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
