#!/usr/bin/env python3
"""QNX 2: rb-cli edits a real LEXICON 10 MB disk, then QNX 4.10.01's chkfsys judges it.

Boots the disk in icon-emu (the ICON fileserver), logs in, and runs
`chkfsys 1` and `files 1:/rbtest`. Passes when chkfsys rebuilds a bitmap that
agrees with the one rb-cli left and reports no error, and QNX lists the files
rb-cli wrote.

local.toml:
    [emulators]
    icon_cli = "/path/to/icon-cli"            # built from github.com/danifunker/icon-emu
    icon_rom = "/path/to/icon1_rom_combined.bin"
"""

import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scenario as sc  # noqa: E402

FIXTURE = "fs.qnx2.lexicon-10mb.hd.img"


def main():
    icon_cli = sc.serialvm.emulator_path("icon_cli")
    rom = sc.serialvm.emulator_path("icon_rom")
    work = sc.workdir()
    img = sc.fixture(FIXTURE, work)
    vol = f"{img}@1"

    note = sc.host_file(work, "note.txt", b"hello from rusty-backup\n")
    big = sc.host_file(work, "big.bin", bytes((i * 7) % 251 for i in range(200 * 1024)))
    gone = sc.host_file(work, "gone.txt", b"deleted again\n")
    sc.rb("mkdir", vol, "/rbtest")
    sc.rb("mkdir", vol, "/rbtest/sub")
    sc.rb("put", vol, note, "/rbtest/note.txt")
    sc.rb("put", vol, big, "/rbtest/sub/big.bin")
    sc.rb("put", vol, gone, "/rbtest/gone.txt")
    sc.rb("mv", vol, "/rbtest/note.txt", "/rbtest/renamed.txt")
    sc.rb("rm", vol, "/rbtest/gone.txt")

    try:
        p = subprocess.run(
            [icon_cli, "session", "--rom", rom, "--hd", img,
             "--cmd", "chkfsys 1", "--cmd", "files 1:/rbtest", "--cmd", "files 1:/rbtest/sub"],
            capture_output=True, text=True, timeout=600,
        )
        out, finished = p.stdout + p.stderr, p.returncode == 0
    except subprocess.TimeoutExpired as e:
        out, finished = (e.stdout or b"").decode(errors="replace"), False
    # A damaged volume stops chkfsys at "Type <CR> to continue?", so the session times
    # out; the error on screen is still the verdict.
    if re.search(r"ERROR|Corrupt", out):
        print(out[-2000:])
        print("FINDING: chkfsys reported an error")
        sc.finish(sc.FINDING, work)
    if not finished or "=== END" not in out:
        print(out[-2000:])
        sc.finish(sc.NO_VERDICT, work)
    chk = out.split("=== CMD chkfsys 1", 1)[-1].split("=== CMD files", 1)[0]
    listing = out.split("=== CMD files 1:/rbtest", 1)[-1]
    print(chk[-1200:])
    print(listing)

    problems = []
    if "Rebuilt bitmap agrees with existing bitmap" not in chk:
        problems.append("chkfsys did not report an agreeing bitmap")
    for name in ("renamed.txt", "big.bin"):
        if name not in listing:
            problems.append(f"QNX does not list {name}")
    if "gone.txt" in listing or "note.txt" in listing:
        problems.append("a deleted or renamed name is still listed")
    if problems:
        print("FINDING: " + "; ".join(problems))
        sc.finish(sc.FINDING, work)
    print("PASS: QNX chkfsys accepts rb-cli's edits and lists the files")
    sc.finish(sc.PASS, work)


if __name__ == "__main__":
    main()
