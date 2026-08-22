#!/usr/bin/env python3
"""Copperline AFFS mount oracle: does a real Kickstart mount our volume?

Boots AmigaOS against the artifact and reports what it makes of it. Unlike the
FS-UAE oracle this needs no results volume and no sentinel poll: Copperline
bridges Paula's serial port to its own stdout, so the guest's `Info` output
comes back on the pipe. Headless, deterministic, and it exits on its own.

    exit 0   the guest mounted the volume Read/Write            (a verdict)
    exit 1   the guest did not mount it                         (a verdict)
    exit 77  Copperline, a Kickstart or a fixture is missing    (not a verdict)
    exit 99  the guest never ran the probe                      (not a verdict)

The disk layout is forced by two things a real Amiga insists on. Stock
Kickstart probes only the IDE *master*, so the artifact has to share a disk
with the boot volume rather than sit on its own; and AFFS stores no size, so
each volume must exactly fill its partition or the guest computes where the
root block should be and misses it — R-042 from the guest's side. An 8-block
cylinder satisfies both: it divides every artifact we produce, and AFFS needs
no FileSystemHeader chain to widen the RDB reserve for, because Kickstart
carries FFS in ROM.
"""
import os
import re
import shutil
import struct
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
BEGIN, END = "RB-PROBE-BEGIN", "RB-PROBE-END"
# Kickstart 3.1 against an A1200 profile. The pairing matters: the same ROM
# under a profile it was not built for boots to a bare shell and runs no
# startup at all, which looks exactly like a volume that would not mount.
PROFILE = "A1200"
TIMEOUT = 240

PROBE = b"""FAILAT 21
Mount SER:
Echo >SER: "RB-PROBE-BEGIN"
Info >SER:
Echo >SER: "RB-PROBE-END"
"""

# Workbench 1.3's MountList has no SER: entry, and its own `Mount` builds a
# device node the handler will not start from — hence the 3.x Mount staged
# beside it in the corpus.
SER_ENTRY = b"""
SER:
    Handler   = L:Port-Handler
    Stacksize = 1000
    Priority  = 5
    GlobVec   = -1
#
"""


def find_copperline():
    env = os.environ.get("COPPERLINE")
    if env and Path(env).is_file():
        return Path(env)
    for c in [
        REPO.parent / "Copperline/target/release/copperline.exe",
        REPO.parent / "Copperline/target/release/copperline",
    ]:
        if c.is_file():
            return c
    found = shutil.which("copperline")
    return Path(found) if found else None


def find_rb_cli():
    for c in [REPO / "target/release/rb-cli.exe", REPO / "target/release/rb-cli"]:
        if c.is_file():
            return c
    found = shutil.which("rb-cli")
    return Path(found) if found else None


def find_kickstart(work):
    """A decrypted Kickstart 3.1 A1200 image, decoding the licensed one once.

    Copperline unwraps Amiga Forever's AMIROMTYPE1 container only on its
    WHDLoad path, not in the main ROM loader, so the key is applied here.
    """
    cached = work / "kick31-a1200.rom"
    if cached.is_file():
        return cached
    ks = Path.home() / "Documents/FS-UAE/Kickstarts"
    rom, key = ks / "amiga-os-310-a1200.rom", ks / "rom.key"
    if not rom.is_file():
        return None
    data = rom.read_bytes()
    tag = b"AMIROMTYPE1"
    if data.startswith(tag):
        if not key.is_file():
            return None
        k = key.read_bytes()
        data = bytes(b ^ k[i % len(k)] for i, b in enumerate(data[len(tag):]))
    cached.write_bytes(data)
    return cached


def run(cmd):
    return subprocess.run([str(c) for c in cmd], capture_output=True, text=True)


def carve_boot_volume(fixture, dest):
    """The Workbench fixture's DH0 partition as a flat volume, cached.

    Filling a partition needs the volume's bytes, not the disk's; the fixture
    is an RDB image, so its PART entry says where its one partition lives.
    """
    raw = fixture.read_bytes()
    part_blk = struct.unpack_from(">I", raw, 28)[0]
    pb = raw[part_blk * 512:(part_blk + 1) * 512]
    surfaces = struct.unpack_from(">I", pb, 140)[0]
    per_track = struct.unpack_from(">I", pb, 148)[0]
    low = struct.unpack_from(">I", pb, 164)[0]
    high = struct.unpack_from(">I", pb, 168)[0]
    cyl = surfaces * per_track
    dest.write_bytes(raw[low * cyl * 512:(high + 1) * cyl * 512])
    return dest


def build_disk(rb, work, artifact, boot, mount3x):
    """One RDB disk: DH0 the bootable Workbench, DH1 the artifact."""
    disk = work / "run.hdf"
    if disk.exists():
        disk.unlink()
    size = artifact.stat().st_size
    if size % 4096:
        return None, f"artifact is {size} bytes, not a whole 8-block cylinder"
    boot_size = boot.stat().st_size
    total = boot_size + size + (1 << 20)
    r = run([rb, "new", "hd", "rdb", "--size", str(total),
             "--heads", "1", "--sectors", "8",
             "--partition", f"{boot_size}:DOS\\1:DH0",
             "--partition", f"{size}:DOS\\1:DH1",
             "--fill", f"1={boot}", "--fill", f"2={artifact}", disk])
    if r.returncode:
        return None, f"new hd rdb: {r.stderr.strip()[:200]}"
    # Without this the ROM has nothing to boot from: `new hd rdb` leaves
    # pb_Flags at 0, so no partition is marked bootable.
    r = run([rb, "partmap", "set-bootable", disk, "1", "--bootable"])
    if r.returncode:
        return None, f"set-bootable: {r.stderr.strip()[:200]}"

    ml = work / "MountList"
    if run([rb, "get", f"{disk}@1", "/devs/MountList", ml]).returncode == 0:
        cur = ml.read_bytes()
        if b"SER:" not in cur:
            ml.write_bytes(cur + SER_ENTRY)
        run([rb, "put", f"{disk}@1", ml, "/devs/MountList", "--force"])
    ss = work / "Startup-Sequence"
    ss.write_bytes(PROBE)
    for src, dst in [(mount3x, "/c/Mount"), (ss, "/s/Startup-Sequence")]:
        r = run([rb, "put", f"{disk}@1", src, dst, "--force"])
        if r.returncode:
            return None, f"put {dst}: {r.stderr.strip()[:200]}"
    return disk, None


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: affs_mount.py <artifact>", file=sys.stderr)
        return 2
    artifact = Path(sys.argv[1]).resolve()
    if not artifact.is_file():
        print(f"no such artifact: {artifact}", file=sys.stderr)
        return 2

    cl = find_copperline()
    if not cl:
        print("unavailable: no Copperline binary (set COPPERLINE, or build the "
              "sibling checkout with `cargo build --release`)", file=sys.stderr)
        return 77
    rb = find_rb_cli()
    if not rb:
        print("unavailable: no rb-cli binary to assemble the disk with",
              file=sys.stderr)
        return 77
    fixture = REPO / "regression-tests/fixtures/fs.affs.workbench13.hd.hdf"
    mount3x = REPO / "regression-tests/fixtures/oracle-assets/amiga/Mount-3x"
    if not fixture.is_file() or not mount3x.is_file():
        print("unavailable: the Workbench fixture or the 3.x Mount is not in "
              "this host's corpus", file=sys.stderr)
        return 77

    work = REPO / "regression-tests/scratch/copperline"
    work.mkdir(parents=True, exist_ok=True)
    kick = find_kickstart(work)
    if not kick:
        print("unavailable: no Kickstart 3.1 A1200 ROM (plus rom.key for an "
              "Amiga Forever image) under ~/Documents/FS-UAE/Kickstarts",
              file=sys.stderr)
        return 77
    boot = work / "wb13.affs"
    if not boot.is_file():
        carve_boot_volume(fixture, boot)

    disk, err = build_disk(rb, work, artifact, boot, mount3x)
    if disk is None:
        print(f"could not assemble the probe disk: {err}", file=sys.stderr)
        return 99

    cfg = work / "probe.toml"
    cfg.write_text(
        f'rom = "{kick.as_posix()}"\n\n'
        f'[machine]\nprofile = "{PROFILE}"\n\n'
        f'[memory]\nchip = "2M"\nfast = "8M"\n\n'
        f'[serial]\nmode = "stdout"\n\n'
        f'[ide]\nmaster = "{disk.as_posix()}"\n'
    )
    shot = work / "probe.png"
    try:
        r = subprocess.run(
            [str(cl), "--config", str(cfg), "--noaudio",
             "--screenshot-after", "40", str(shot)],
            capture_output=True, text=True, timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        print("the emulator did not exit on its own; no verdict",
              file=sys.stderr)
        return 99

    out = f"{r.stdout}\n{r.stderr}"
    if BEGIN not in out or END not in out:
        print("the guest never ran the probe - a harness result, not a verdict "
              f"on the volume (screenshot: {shot})", file=sys.stderr)
        return 99
    body = out.split(BEGIN, 1)[1].split(END, 1)[0]
    # DH1 is the artifact. DH0 is the boot volume and must never be the answer,
    # or the oracle would pass on any disk that boots at all.
    hit = re.search(r"^DH1:.*", body, re.M)
    if hit and "Read/Write" in hit.group(0):
        print(f"verdict: mounted  ({hit.group(0).strip()})")
        return 0
    print("verdict: AmigaOS did not mount the volume")
    print(body.strip())
    return 1


if __name__ == "__main__":
    sys.exit(main())
