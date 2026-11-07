#!/usr/bin/env python3
"""NeXT disk label: NeXTSTEP 3.3's own `disk` tool reads rb-cli's label and prints it.

    nextstep_disk_label.py <artifact>

The artifact goes in at SCSI target 1, read-only, on a fresh copy of the boot disk. A disk with
a bad label answers "no label on disk". Passes when the label is there and the partition NeXTSTEP
reports sits where rb-cli says it does, which catches a unit mistake that a parse would not.
RB_EMU_CONTROL=1 damages the four label copies first, and that must fail.

Exits 0 pass, 1 finding, 77 cannot run here, 99 no verdict. Boots take about two minutes.
"""

import os
import re
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "emulator"))
import nextstep_ufs_fsck as nx  # noqa: E402
import scenario as sc  # noqa: E402

LABEL_COPIES = (0, 15, 30, 45)
PRINT = "(echo label; echo print; echo quit; echo quit; echo quit) | /usr/etc/disk /dev/rsd1a 2>&1"


def rb_partition(disk):
    """Byte offset and length of partition 1, as rb-cli reads them."""
    out = subprocess.run([sc.rb_cli(), "ls", f"{disk}@1", "/"], capture_output=True, text=True)
    m = re.search(r"@ LBA (\d+), (\d+) bytes", out.stdout + out.stderr)
    if not m:
        sc.serialvm.no_verdict("rb-cli did not report partition 1")
    return int(m.group(1)) * 512, int(m.group(2))


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    binary = sc.serialvm.emulator_path("previous")
    boot_src = sc.serialvm.emulator_path("nextstep_boot")
    work = sc.workdir()
    disk = os.path.join(work, "label.img")
    shutil.copyfile(sys.argv[1], disk)
    want_offset, want_size = rb_partition(disk)
    sc.damage(disk, [(block * 512, 512) for block in LABEL_COPIES])

    # The guest writes to its boot disk, so every run boots a fresh copy.
    boot = os.path.join(work, "boot.hda")
    shutil.copyfile(boot_src, boot)
    vm = nx.Previous(binary, boot, disk, os.path.join(work, "console.log"), work)
    try:
        vm.expect(r"PREVIOUS-READY", 1800)
        time.sleep(2)
        vm.run("PS1='rb# '; export PS1", timeout=60)
        _, out = vm.run(PRINT, timeout=120)
    finally:
        vm.close()
    print(out)

    sector = re.search(r"sector_size (\d+) front_porch (\d+)", out)
    part = re.search(r"^a\s+(\d+)\s+(\d+)\s", out, re.M)
    if "no label on disk" in out or not sector or not part:
        print("FINDING: NeXTSTEP does not accept the label")
        sc.finish(sc.FINDING, work)
    size, front = int(sector.group(1)), int(sector.group(2))
    offset = (front + int(part.group(1))) * size
    length = int(part.group(2)) * size
    if (offset, length) != (want_offset, want_size):
        print(f"FINDING: NeXTSTEP puts partition a at {offset}+{length}, "
              f"rb-cli at {want_offset}+{want_size}")
        sc.finish(sc.FINDING, work)
    print(f"PASS: NeXTSTEP 3.3 reads the label; partition a at byte {offset}, {length} bytes")
    sc.finish(sc.PASS, work)


if __name__ == "__main__":
    sys.exit(main())
