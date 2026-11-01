#!/usr/bin/env python3
"""Solaris UFS: rb-cli writes a big-endian UFS slice, then Solaris 9's own fsck judges it.

    solaris_ufs_fsck.py blank|sol-edit

blank formats the slice with `rb-cli new volume ufs --big-endian`; sol-edit
lets Solaris `newfs` it on a first boot, so rb-cli edits a volume Solaris made.
Either way rb-cli then runs mkdir / put (past the direct blocks) / mv / rm, the
slice goes into a Sun-labelled disk at SCSI target 1, and Solaris runs
`fsck -F ufs -n` on c0t1d0s0, then mounts it read-only and lists /rbtest.
Passes when fsck exits 0 and declines no repair, and Solaris lists the files.

local.toml: `[emulators] solaris9`, a raw sun4m Solaris 9 disk that boots on
`qemu-system-sparc -M SS-5` with OpenBIOS and has no root password. Boots take
around ten minutes under TCG.
"""

import os
import re
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scenario as sc  # noqa: E402

SLICE = "32M"
DEV = "c0t1d0s0"


# fsck -n answers every repair question "no"; Solaris pads it with two spaces.
DECLINED = re.compile(r"\?\s+no\s*$")

def boot(base, disk, log, keep_disk=False):
    """Solaris 9 on an SS-5 with `disk` at target 1, logged in as root; -nographic puts OpenBIOS on ttya."""
    qemu = sc.serialvm.require_program("qemu-system-sparc")
    keep = ",snapshot=off" if keep_disk else ""
    vm = sc.serialvm.SerialVM(
        [qemu, "-M", "SS-5", "-m", "256", "-nographic", "-snapshot",
         "-drive", f"file={base},if=scsi,bus=0,unit=0,format=raw",
         "-drive", f"file={disk},if=scsi,bus=0,unit=1,format=raw{keep}"],
        log_path=log,
    )
    vm.expect(r"console login: ", 3000)
    vm.send("root\r")
    # The root shell is slow to print its prompt under emulation.
    vm.expect(r"# ", 600)
    vm.run("PATH=/usr/sbin:/usr/bin:/sbin; export PATH", timeout=120)
    vm.run("devfsadm -c disk", timeout=600)
    return vm


def main():
    variant = sys.argv[1] if len(sys.argv) > 1 else "blank"
    if variant not in ("blank", "sol-edit"):
        print(__doc__)
        sys.exit(2)
    base = sc.serialvm.emulator_path("solaris9")
    work = sc.workdir()
    disk = os.path.join(work, "test-disk.img")
    vol = f"{disk}@1"
    if variant == "blank":
        raw = os.path.join(work, "ufs.img")
        sc.rb("new", "volume", "ufs", raw, "--size", SLICE, "--big-endian")
        sc.rb("new", "hd", "sun", disk, "--size", "40M", "--partition", f"{SLICE}:home", "--fill", f"1={raw}")
    else:
        empty = os.path.join(work, "empty.img")
        with open(empty, "wb") as f:
            f.truncate(32 << 20)
        sc.rb("new", "hd", "sun", disk, "--size", "40M", "--partition", f"{SLICE}:home", "--fill", f"1={empty}")
        vm = boot(base, disk, os.path.join(work, "newfs.log"), keep_disk=True)
        try:
            st, out = vm.run(f"echo y | newfs /dev/rdsk/{DEV}", timeout=900)
            print(out[-600:])
            vm.run("sync; sync", timeout=120)
        finally:
            vm.close()
        shutil.copyfile(disk, os.path.join(work, "solaris-newfs.img"))
        if st != 0:
            print(f"no verdict: Solaris newfs exited {st}")
            sc.finish(sc.NO_VERDICT, work)

    note = sc.host_file(work, "note.txt", b"hello from rusty-backup\n")
    big = sc.host_file(work, "big.bin", bytes((i * 19) % 251 for i in range(600 * 1024)))
    gone = sc.host_file(work, "gone.txt", b"deleted again\n")
    sc.rb("mkdir", vol, "/rbtest")
    sc.rb("mkdir", vol, "/rbtest/sub")
    sc.rb("put", vol, note, "/rbtest/note.txt")
    sc.rb("put", vol, big, "/rbtest/sub/big.bin")
    sc.rb("put", vol, gone, "/rbtest/gone.txt")
    sc.rb("mv", vol, "/rbtest/note.txt", "/rbtest/renamed.txt")
    sc.rb("rm", vol, "/rbtest/gone.txt")
    # Slice 0 starts at LBA 1008 (cylinder-aligned by `new hd sun`).
    sc.damage(disk, [(1008 * 512 + sc.ffs_first_cg(disk, base=1008 * 512), 2048)])

    vm = boot(base, disk, os.path.join(work, "console.log"))
    try:
        status, out = vm.run(f"fsck -F ufs -n /dev/rdsk/{DEV}", timeout=900)
        print(out)
        _, listing = vm.run(f"mount -F ufs -o ro /dev/dsk/{DEV} /mnt && ls -R /mnt/rbtest; umount /mnt", timeout=300)
        print("--- ls -R /rbtest\n" + listing)
    finally:
        vm.close()
    questions = [l for l in out.splitlines() if DECLINED.search(l)]
    problems = []
    if status != 0 or questions:
        problems.append(f"fsck exited {status}, {len(questions)} repair(s) declined")
    for name in ("renamed.txt", "big.bin"):
        if name not in listing:
            problems.append(f"Solaris does not list {name}")
    if "gone.txt" in listing or "note.txt" in listing:
        problems.append("a deleted or renamed name is still listed")
    if problems:
        print("FINDING: " + "; ".join(problems))
        sc.finish(sc.FINDING, work)
    print("PASS: Solaris 9 fsck accepts rb-cli's slice and lists the files")
    sc.finish(sc.PASS, work)


if __name__ == "__main__":
    main()
