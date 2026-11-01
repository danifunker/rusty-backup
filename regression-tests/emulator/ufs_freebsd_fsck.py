#!/usr/bin/env python3
"""UFS: rb-cli writes to a UFS volume, then FreeBSD 15.1's fsck_ffs judges it.

    ufs_freebsd_fsck.py ufs1|ufs2|blank-ufs1|blank-ufs1-edit

ufs1 / ufs2 edit volumes FreeBSD itself made (fs.ufs1.freebsd151.hd,
fs.ufs2.freebsd151-ckhash.hd, the latter with FreeBSD 12+ CRC32C check-hashes);
blank-ufs1 judges a fresh `rb-cli new volume ufs`, and blank-ufs1-edit the same
volume after the edits. The check is
oracles/freebsd/fsck_ffs.py, which needs local.toml's `[emulators] freebsd15`.
"""

import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scenario as sc  # noqa: E402

FIXTURES = {
    "ufs1": "fs.ufs1.freebsd151.hd.img",
    "ufs2": "fs.ufs2.freebsd151-ckhash.hd.img",
}
ORACLE = os.path.join(sc.REGRESS, "oracles", "freebsd", "fsck_ffs.py")


def main():
    variant = sys.argv[1] if len(sys.argv) > 1 else "ufs2"
    sc.serialvm.emulator_path("freebsd15")
    work = sc.workdir()
    if variant.startswith("blank-ufs1"):
        img = os.path.join(work, "blank.img")
        sc.rb("new", "volume", "ufs", img, "--size", "16M")
    elif variant in FIXTURES:
        img = sc.fixture(FIXTURES[variant], work)
    else:
        print(__doc__)
        sys.exit(2)
    if variant != "blank-ufs1":
        note = sc.host_file(work, "note.txt", b"hello from rusty-backup\n")
        # Past the 12 direct blocks, so an indirect block is written too.
        big = sc.host_file(work, "big.bin", bytes((i * 13) % 251 for i in range(600 * 1024)))
        gone = sc.host_file(work, "gone.txt", b"deleted again\n")
        sc.rb("mkdir", img, "/rbtest")
        sc.rb("mkdir", img, "/rbtest/sub")
        sc.rb("put", img, note, "/rbtest/note.txt")
        sc.rb("put", img, big, "/rbtest/sub/big.bin")
        sc.rb("put", img, gone, "/rbtest/gone.txt")
        sc.rb("mv", img, "/rbtest/note.txt", "/rbtest/renamed.txt")
        sc.rb("rm", img, "/rbtest/gone.txt")
    sc.damage(img, [(sc.ffs_first_cg(img), 2048)])
    p = subprocess.run([sys.executable, ORACLE, img], capture_output=True, text=True, timeout=900)
    print(p.stdout[-2500:] + p.stderr[-500:])
    sc.finish(p.returncode, work)


if __name__ == "__main__":
    main()
