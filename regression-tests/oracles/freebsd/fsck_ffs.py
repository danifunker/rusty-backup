#!/usr/bin/env python3
"""FreeBSD's own fsck_ffs over a UFS artifact, in a FreeBSD 15 VM.

    fsck_ffs.py <artifact> [--device vtbd1]

Boots the FreeBSD VM named by local.toml's `[emulators] freebsd15` (a directory
holding `fbsd.qcow2`, an overlay whose loader.conf puts the console on serial)
with `-snapshot`, so neither the VM nor the artifact can change. The artifact
is the second virtio disk (/dev/vtbd1); `--device` names a slice or partition
instead (vtbd1s1, vtbd1p2) for a partitioned image. Runs `fsck_ffs -fn` (force,
read-only). With -n, fsck_ffs answers "no" to every repair and still exits 0, so
the artifact passes only when it exits 0 *and* asked no repair question.

Exit codes: see oracles/serialvm.py (0 pass, 1 finding, 77 unavailable, 99 no verdict).
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import serialvm  # noqa: E402


# fsck -n answers every repair question "no"; Solaris pads it with two spaces.
DECLINED = re.compile(r"\?\s+no\s*$")

def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return 2
    artifact = os.path.abspath(args[0])
    device = "vtbd1"
    if "--device" in args:
        device = args[args.index("--device") + 1]
    if not os.path.exists(artifact):
        print(f"no such artifact: {artifact}")
        return 2

    vm_dir = serialvm.emulator_path("freebsd15")
    overlay = os.path.join(vm_dir, "fbsd.qcow2")
    if not os.path.exists(overlay):
        serialvm.unavailable(f"{overlay} is missing")
    qemu = serialvm.require_program("qemu-system-x86_64")
    accel = ["-enable-kvm"] if os.access("/dev/kvm", os.W_OK) else []

    vm = serialvm.SerialVM([
        qemu, *accel, "-m", "1024", "-smp", "2", "-snapshot", "-nic", "none",
        "-drive", f"file={overlay},if=virtio,format=qcow2",
        "-drive", f"file={artifact},if=virtio,format=raw",
    ])
    try:
        vm.expect(r"login:", timeout=300)
        vm.send("root\r")
        vm.expect(r"[#$] ", timeout=60)
        status, out = vm.run(f"fsck_ffs -fn /dev/{device}", timeout=300)
        print(out)
        # Every repair fsck_ffs would make shows as a "...? no" question under -n.
        questions = [l.strip() for l in out.splitlines() if DECLINED.search(l)]
        if status == 0 and not questions:
            print(f"PASS: fsck_ffs -fn /dev/{device} exited 0 with nothing to repair")
            return serialvm.PASS
        print(f"FINDING: fsck_ffs -fn /dev/{device} exited {status}, {len(questions)} repair(s) declined")
        return serialvm.FINDING
    finally:
        vm.close()


if __name__ == "__main__":
    sys.exit(main())
