#!/usr/bin/env python3
"""HP-UX HFS: rb-cli edits a real HP-UX 11.11 /stand volume, then HP-UX's fsck judges it.

Boots HP-UX 11.11 on QEMU's B160L from local.toml's `[emulators] hpux_boot`
(a bootable HP 715 disk; root has no password) with -snapshot, then hot-plugs
the edited volume at SCSI ID 5 (a disk present at boot breaks SeaBIOS, which
boots the lowest ID), runs `insf -e`, and `fsck -F hfs -f -n /dev/rdsk/c0t5d0`.
Passes when fsck exits 0 and declines no repair.
"""

import os
import re
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scenario as sc  # noqa: E402

FIXTURE = "fs.hpux-hfs.stand1111-hp715.hd.img"


# fsck -n answers every repair question "no"; Solaris pads it with two spaces.
DECLINED = re.compile(r"\?\s+no\s*$")

def main():
    boot = sc.serialvm.emulator_path("hpux_boot")
    qemu = sc.serialvm.require_program("qemu-system-hppa")
    work = sc.workdir()
    img = sc.fixture(FIXTURE, work)
    note = sc.host_file(work, "note.txt", b"hello from rusty-backup\n")
    big = sc.host_file(work, "big.bin", bytes((i * 11) % 251 for i in range(300 * 1024)))
    gone = sc.host_file(work, "gone.txt", b"deleted again\n")
    sc.rb("mkdir", img, "/rbtest")
    sc.rb("mkdir", img, "/rbtest/sub")
    sc.rb("put", img, note, "/rbtest/note.txt")
    sc.rb("put", img, big, "/rbtest/sub/big.bin")
    sc.rb("put", img, gone, "/rbtest/gone.txt")
    sc.rb("mv", img, "/rbtest/note.txt", "/rbtest/renamed.txt")
    sc.rb("rm", img, "/rbtest/gone.txt")
    sc.damage(img, [(sc.ffs_first_cg(img), 2048)])

    log = os.path.join(work, "console.log")
    vm = sc.serialvm.SerialVM(
        [qemu, "-M", "B160L", "-m", "512", "-vga", "none", "-snapshot",
         "-drive", f"if=scsi,bus=0,index=6,file={boot},format=raw"],
        log_path=log, monitor=True,
    )
    try:
        # Boot time varies a lot on emulated PA-RISC; one boot took over 25 minutes to reach login.
        vm.expect(r"[Ll]ogin:", timeout=2400)
        vm.send("root\r")
        vm.expect(r"[#$] ", timeout=180)
        vm.run("export PATH=/usr/sbin:/sbin:/usr/bin:$PATH", timeout=60)
        vm.monitor(f"drive_add 0 if=none,id=rbt,file={img},format=raw")
        vm.monitor("device_add scsi-hd,bus=scsi.0,scsi-id=5,drive=rbt")
        vm.run("ioscan -fnC disk >/dev/null 2>&1; insf -e >/dev/null 2>&1", timeout=300)
        status, out = vm.run("fsck -F hfs -f -n /dev/rdsk/c0t5d0", timeout=600)
        print(out)
        questions = [l for l in out.splitlines() if DECLINED.search(l)]
        if "No such device" in out or "cannot open" in out.lower():
            print("no verdict: the hot-plugged disk did not appear")
            vm.close()
            sc.finish(sc.NO_VERDICT, work)
        if status == 0 and not questions:
            print("PASS: HP-UX fsck -F hfs -n exited 0 with nothing to repair")
            code = sc.PASS
        else:
            print(f"FINDING: fsck exited {status}, {len(questions)} repair(s) declined")
            code = sc.FINDING
    finally:
        vm.close()
    sc.finish(code, work)


if __name__ == "__main__":
    main()
