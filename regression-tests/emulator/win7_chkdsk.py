#!/usr/bin/env python3
"""Windows 7: rb-cli writes an NTFS or FAT volume, then Windows' chkdsk judges it.

    win7_chkdsk.py ntfs|ntfs-blank|fat-blank|fat32-blank|<corpus>-pristine

ntfs edits the corpus volume fs.ntfs.base.hd; the -blank variants edit a fresh
`rb-cli new volume` (fat-blank is FAT16 at 64 MB, fat32-blank a 300 MB --fat32).
`ntfs-pristine` / `fat32-pristine` check a corpus volume untouched, as a
baseline: fs.fat32.base.hd is mkfs.fat's 16 MB FAT32 with 32,232 clusters, under
FAT32's 65,525 minimum, and Windows' chkdsk fails on it before any edit. Each volume gets an MBR
so Windows gives it a letter, and a FAT results disk carries rbrun.bat. The
guest is driven by keyboard (oracles/win7/win7vm.py): an elevated prompt runs
rbrun.bat, which runs `chkdsk` (read-only) and `dir /s` on the test volume,
writes both to the results disk and shuts Windows down. Passes when chkdsk
exits 0, says it found no problems, and Windows lists what rb-cli wrote.

local.toml: `[emulators] win7`, an overlay settled by `win7vm.py settle`.
"""

import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scenario as sc  # noqa: E402

sys.path.insert(0, os.path.join(sc.REGRESS, "oracles", "win7"))
import win7vm  # noqa: E402

FIXTURES = {"ntfs": "fs.ntfs.base.hd.img", "fat32": "fs.fat32.base.hd.img"}
BLANK = {
    "ntfs-blank": ("ntfs", "64M", []),
    "fat-blank": ("fat", "64M", []),
    "fat32-blank": ("fat", "300M", ["--fat32"]),
}

RUN_BAT = r"""@echo off
set R=%1
mountvol > %R%\vols.txt
(echo list disk& echo list volume) | diskpart > %R%\disks.txt
for %%d in (D E F G H I J) do if exist %%d:\ if /i not "%%d:"=="%R%" call :check %%d
shutdown /s /t 3
goto :eof
:check
echo n| chkdsk %1: > %R%\chk.txt 2>&1
> %R%\rc.txt echo %errorlevel%
dir /s /b %1:\rbtest > %R%\dir.txt 2>&1
goto :eof
"""


def mbr_wrap(volume, disk, ptype, signature):
    """`disk` = a 1 MiB MBR header with one partition at LBA 2048, then `volume`."""
    size = os.path.getsize(volume)
    head = bytearray(1 << 20)
    struct.pack_into("<I", head, 440, signature)
    entry = struct.pack("<B3sB3sII", 0, b"\xfe\xff\xff", ptype, b"\xfe\xff\xff", 2048, size // 512)
    head[446:462] = entry
    head[510:512] = b"\x55\xaa"
    with open(disk, "wb") as out, open(volume, "rb") as src:
        out.write(head)
        while True:
            chunk = src.read(1 << 20)
            if not chunk:
                break
            out.write(chunk)


def control_ranges(vol, ntfs):
    """Where a control run damages the volume: the root directory's MFT record, or the first FAT."""
    with open(vol, "rb") as f:
        boot = f.read(512)
    bps = struct.unpack_from("<H", boot, 11)[0]
    spc = boot[13]
    if ntfs:
        # Win7 self-heals a damaged MFT record on mount; a free cluster marked used in $Bitmap it does not.
        mft = struct.unpack_from("<Q", boot, 0x30)[0] * bps * spc
        raw = struct.unpack_from("<b", boot, 0x40)[0]
        rec = 1 << -raw if raw < 0 else raw * bps * spc
        with open(vol, "rb") as f:
            f.seek(mft + 6 * rec)
            r = f.read(rec)
        at = struct.unpack_from("<H", r, 0x14)[0]
        while struct.unpack_from("<I", r, at)[0] not in (0x80, 0xFFFFFFFF):
            at += struct.unpack_from("<I", r, at + 4)[0]
        runs = at + struct.unpack_from("<H", r, at + 0x20)[0]
        head = r[runs]
        nlen, noff = head & 0xF, head >> 4
        lcn = int.from_bytes(r[runs + 1 + nlen:runs + 1 + nlen + noff], "little", signed=True)
        size = struct.unpack_from("<Q", r, at + 0x30)[0]
        return [(lcn * bps * spc, size)]
    reserved = struct.unpack_from("<H", boot, 14)[0]
    return [(reserved * bps + 4096, 64)]


def main():
    variant = sys.argv[1] if len(sys.argv) > 1 else "ntfs"
    pristine = variant.endswith("-pristine")
    base = variant[: -len("-pristine")] if pristine else variant
    overlay = sc.serialvm.emulator_path("win7")
    work = sc.workdir()
    if base in FIXTURES:
        vol = sc.fixture(FIXTURES[base], work)
    elif base in BLANK and not pristine:
        fs, size, extra = BLANK[base]
        vol = os.path.join(work, "blank.img")
        sc.rb("new", "volume", fs, vol, "--size", size, *extra)
    else:
        print(__doc__)
        sys.exit(2)
    ntfs = base.startswith("ntfs")

    if not pristine:
        note = sc.host_file(work, "note.txt", b"hello from rusty-backup\r\n")
        big = sc.host_file(work, "big.bin", bytes((i * 17) % 251 for i in range(600 * 1024)))
        gone = sc.host_file(work, "gone.txt", b"deleted again\r\n")
        sc.rb("mkdir", vol, "/rbtest")
        sc.rb("mkdir", vol, "/rbtest/sub")
        sc.rb("put", vol, note, "/rbtest/note.txt")
        sc.rb("put", vol, big, "/rbtest/sub/big.bin")
        sc.rb("put", vol, gone, "/rbtest/gone.txt")
        sc.rb("mv", vol, "/rbtest/note.txt", "/rbtest/renamed.txt")
        sc.rb("rm", vol, "/rbtest/gone.txt")
    sc.damage(vol, control_ranges(vol, ntfs))
    test_disk = os.path.join(work, "test-disk.img")
    mbr_wrap(vol, test_disk, 0x07 if ntfs else 0x0C, 0x52420001)

    res_vol = os.path.join(work, "results.img")
    sc.rb("new", "volume", "fat", res_vol, "--size", "32M")
    bat = sc.host_file(work, "rbrun.bat", RUN_BAT.replace("\n", "\r\n").encode())
    sc.rb("put", res_vol, bat, "/rbrun.bat")
    res_disk = os.path.join(work, "results-disk.img")
    mbr_wrap(res_vol, res_disk, 0x06, 0x52420002)

    # Writable: Windows may self-heal a damaged MFT record or bitmap on mount, before chkdsk (see README).
    vm = win7vm.Win7VM(overlay, [(res_disk, True), (test_disk, False)])
    try:
        vm.wait_desktop()
        vm.elevated_prompt()
        vm.type(r"for %d in (D E F G H) do @if exist %d:\rbrun.bat %d:\rbrun.bat %d:" + "\n")
        if not vm.wait_exit(900):
            vm.screendump(os.path.join(work, "stuck.ppm"))
            print("no verdict: Windows did not shut down; see stuck.ppm")
            vm.close()
            sc.finish(sc.NO_VERDICT, work)
    finally:
        vm.close()

    # Read the answers off the results volume (the disk minus its 1 MiB MBR header).
    with open(res_disk, "rb") as f:
        f.seek(1 << 20)
        with open(res_vol, "wb") as out:
            out.write(f.read())
    got = {}
    for name in ("chk.txt", "rc.txt", "dir.txt", "vols.txt", "disks.txt"):
        dest = os.path.join(work, "out-" + name)
        sc.rb("get", res_vol, "/" + name, dest, check=False)
        if os.path.exists(dest):
            got[name] = open(dest, "rb").read().decode("cp437", errors="replace")
    if "rc.txt" not in got:
        print(got.get("disks.txt", "(rbrun.bat never ran)").strip())
        print("no verdict: rbrun.bat found no test volume to check")
        sc.finish(sc.NO_VERDICT, work)
    chk = "\n".join(l for l in got.get("chk.txt", "").splitlines() if "percent complete" not in l)
    print(chk.strip())
    print("--- dir /s /b rbtest\n" + got.get("dir.txt", "").strip())
    rc = got["rc.txt"].strip()
    listing = got.get("dir.txt", "").lower()
    problems = []
    if rc != "0":
        problems.append(f"chkdsk exited {rc}")
    if "found no problems" not in got.get("chk.txt", ""):
        problems.append("chkdsk did not say it found no problems")
    for name in () if pristine else ("renamed.txt", "big.bin"):
        if name not in listing:
            problems.append(f"Windows does not list {name}")
    if "gone.txt" in listing or "note.txt" in listing:
        problems.append("a deleted or renamed name is still listed")
    if problems:
        print("FINDING: " + "; ".join(problems))
        sc.finish(sc.FINDING, work)
    print("PASS: Windows 7 chkdsk accepts rb-cli's volume and lists the files")
    sc.finish(sc.PASS, work)


if __name__ == "__main__":
    main()
