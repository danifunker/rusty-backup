#!/usr/bin/env python3
"""Attach a produced Amiga volume to Copperline and report what it made of it.

Copperline is a headless, deterministic, CLI-driven Amiga emulator, which is
what makes it the first emulator oracle here that `verify` can actually run —
FS-UAE and WinUAE both need a hand-configured guest, so their pairs resolve to
skip-manual.

**What this checks, and what it does not.** Copperline decides how to attach an
image before any ROM runs: a whole-disk image is used as-is if it carries an
RDSK, and a bare partition hardfile is wrapped in a synthesized RDB carrying
*the dostype read out of the image's own boot block*. So this verifies the
container and the boot block — the dostype we wrote, and a length the geometry
can express — from a second implementation. It does **not** verify the
filesystem: nothing has mounted the volume. That needs a guest OS, which means
a Kickstart ROM and a Workbench, neither of which is redistributable; see the
oracle README for the shape that takes.

Only **bare** volumes are covered. A whole-disk image with its own RDSK is
attached with no log line to key on, and Copperline attaches an image it
cannot classify — noise included — just as silently, so there is no signal
there to tell the two apart. Claiming a pass on that absence is how an oracle
starts approving everything.

The ROM is Copperline's own redistributable AROS image, so the check needs
no licensed Kickstart. The run is capped at a couple of emulated seconds
because the answer is already in the log by then.

    exit 0  Copperline recognised a bare Amiga volume and read the dostype
            we wrote out of its boot block
    exit 1  it did not recognise one, or read a different dostype  (a verdict)
    exit 2  setup problem — no Copperline binary  (not a verdict)
"""

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

# Where a Copperline build is looked for, in order. The sibling checkout is
# how this project keeps it; COPPERLINE overrides for anyone else.
CANDIDATES = [
    Path("../Copperline/target/release/copperline"),
    Path("../Copperline/target/release/copperline.exe"),
]

CONFIG = """rom = "{rom}"

[machine]
profile = "A1200"

[memory]
chip = "2M"

[ide]
master = "{image}"
"""


def find_copperline():
    env = os.environ.get("COPPERLINE")
    if env:
        p = Path(env)
        return p if p.is_file() else None
    here = Path(__file__).resolve().parents[3]
    for rel in CANDIDATES:
        p = (here / rel).resolve()
        if p.is_file():
            return p
    from shutil import which

    found = which("copperline")
    return Path(found) if found else None


def aros_rom(exe: Path):
    """The AROS ROM shipped in Copperline's own tree, relative to the binary.

    Using it rather than a Kickstart is what keeps this check portable: AROS is
    freely redistributable, so no host needs a licensed ROM to run the oracle.
    """
    for up in (2, 3):
        try:
            cand = exe.parents[up] / "assets" / "aros" / "aros-amiga-m68k-rom.bin"
        except IndexError:
            continue
        if cand.is_file():
            return cand
    return None


def main():
    if len(sys.argv) < 2:
        print("usage: attach_check.py <image> [expected-dostype-hex]", file=sys.stderr)
        return 2
    image = Path(sys.argv[1]).resolve()
    want = sys.argv[2].lower() if len(sys.argv) > 2 else None
    if not image.is_file():
        print(f"setup: no such image: {image}", file=sys.stderr)
        return 2

    exe = find_copperline()
    if exe is None:
        print(
            "setup: no Copperline binary (set COPPERLINE, or build the sibling "
            "checkout with `cargo build --release`)",
            file=sys.stderr,
        )
        return 2

    with tempfile.TemporaryDirectory() as td:
        cfg = Path(td) / "attach.toml"
        # An A1200 for its IDE port, booting Copperline's own AROS ROM so
        # nothing here depends on a licensed Kickstart.
        rom = aros_rom(exe)
        if rom is None:
            print(
                "setup: no AROS ROM in the Copperline tree "
                "(assets/aros/aros-amiga-m68k-rom.bin)",
                file=sys.stderr,
            )
            return 2
        rom = aros_rom(exe)
        if rom is None:
            print(
                "setup: no AROS ROM in the Copperline tree "
                "(assets/aros/aros-amiga-m68k-rom.bin)",
                file=sys.stderr,
            )
            return 2
        cfg.write_text(
            CONFIG.format(rom=rom.as_posix(), image=image.as_posix()),
            encoding="utf-8",
        )
        shot = Path(td) / "boot.png"
        env = dict(os.environ, RUST_LOG="info")
        try:
            r = subprocess.run(
                [
                    str(exe), "--config", str(cfg), "--noaudio",
                    "--screenshot-after", "2", str(shot),
                ],
                capture_output=True, text=True, timeout=180, env=env,
            )
        except subprocess.TimeoutExpired:
            print("setup: Copperline did not exit within 180s", file=sys.stderr)
            return 2

    log = r.stdout + r.stderr
    # A config Copperline refuses never reaches the emulator at all, and its
    # message is the finding.
    if "ide:" not in log:
        first = next(
            (ln for ln in log.splitlines() if ln.strip().startswith("Error:")),
            "(no diagnostic)",
        )
        print(f"verdict: rejected  ({first.strip()})")
        return 1

    wrap = re.search(r"bare partition hardfile \(dostype ([0-9A-Fa-f]{8})\)", log)
    if wrap is None:
        # Copperline attaches an image it cannot classify as a plain raw disk,
        # silently. Treating that as success made 2 MB of urandom pass, which
        # is the one failure mode an oracle must not have: a harness that
        # approves anything cannot distinguish a good volume from noise.
        print(
            "verdict: not-recognised  (no bare-partition wrap; Copperline took it "
            "as an unclassified raw disk, which is what it does with noise)"
        )
        return 1

    got = wrap.group(1).lower()
    if want and got != want:
        print(f"verdict: wrong-dostype  (read {got}, expected {want})")
        return 1
    print(f"verdict: attached  (bare partition, dostype {got}, RDB synthesized)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
