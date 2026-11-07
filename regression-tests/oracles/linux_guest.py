#!/usr/bin/env python3
"""A real Linux kernel in a small VM judges what no other tool here reads.

    linux_guest.py bfs|solaris-x86 <artifact>

bfs: rb-cli adds files to a copy of the volume, then the kernel's own befs driver mounts it and
the guest checksums every file. solaris-x86: the kernel's partition parser reads the VTOC, and
every slice must sit exactly where rb-cli says. The kernel code shares nothing with rb-cli, but
befs is read-only, so this proves a volume reads back and not that it is free of damage.
RB_EMU_CONTROL=1 damages the structure first, and that must fail.

local.toml: `[emulators] linux_kernel`, a vmlinuz unpacked from a linux-image .deb (no root needed:
`apt-get download` and `dpkg-deb -x`). befs.ko comes from /lib/modules/<same version>.
Exits 0 pass, 1 finding, 77 cannot run here, 99 no verdict. A boot takes seconds with KVM.
"""

import hashlib
import os
import re
import shutil
import stat
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "emulator"))
import scenario as sc  # noqa: E402

INIT = """#!/bin/busybox sh
/bin/busybox --install -s /bin
mount -t proc proc /proc
mount -t sysfs sys /sys
mount -t devtmpfs dev /dev
sleep 1
echo RB-BEGIN
{body}
echo RB-END
poweroff -f
"""

BFS = """insmod /befs.ko
mount -t befs -o ro /dev/vda /mnt
echo mount=$?
cd /mnt && find . -type f -exec md5sum {} +
"""

SLICES = """for p in /sys/block/vda/vda*; do
  echo slice $(cat $p/start) $(cat $p/size)
done
"""


def initramfs(work, body, module):
    root = os.path.join(work, "root")
    for d in ("bin", "proc", "sys", "dev", "mnt"):
        os.makedirs(os.path.join(root, d))
    shutil.copy(sc.serialvm.require_program("busybox"), os.path.join(root, "bin", "busybox"))
    init = os.path.join(root, "init")
    with open(init, "w") as f:
        f.write(INIT.format(body=body))
    os.chmod(init, os.stat(init).st_mode | stat.S_IXUSR)
    if module:
        with open(os.path.join(root, "befs.ko"), "wb") as out:
            subprocess.run(["zstd", "-dc", module], stdout=out, check=True)
    image = os.path.join(work, "initrd.cpio")
    with open(image, "wb") as out:
        names = subprocess.run(["find", "."], cwd=root, capture_output=True, check=True).stdout
        subprocess.run(["cpio", "-o", "-H", "newc", "--quiet"], cwd=root, input=names, stdout=out,
                       check=True)
    return image


def boot(kernel, initrd, disk):
    argv = [sc.serialvm.require_program("qemu-system-x86_64"), "-m", "256", "-nographic",
            "-no-reboot", "-kernel", kernel, "-initrd", initrd,
            "-append", "console=ttyS0 panic=-1 loglevel=3",
            "-drive", f"file={disk},if=virtio,format=raw,readonly=on"]
    if os.access("/dev/kvm", os.R_OK | os.W_OK):
        argv += ["-enable-kvm"]
    try:
        p = subprocess.run(argv, capture_output=True, timeout=300)
    except subprocess.TimeoutExpired:
        sc.serialvm.no_verdict("the guest did not finish in 300 s")
    out = p.stdout.decode(errors="replace").replace("\r", "")
    m = re.search(r"RB-BEGIN\n(.*?)RB-END", out, re.S)
    if not m:
        sc.serialvm.no_verdict(f"no result from the guest: {out[-400:]!r}")
    return m.group(1)


def check_bfs(kernel, module, artifact, work):
    disk = os.path.join(work, "bfs.img")
    shutil.copyfile(artifact, disk)
    files = {
        "rbtest/renamed.txt": b"hello from rusty-backup\n",
        "rbtest/sub/big.bin": bytes((i * 23) % 251 for i in range(600 * 1024)),
    }
    sc.rb("mkdir", disk, "/rbtest")
    sc.rb("mkdir", disk, "/rbtest/sub")
    sc.rb("put", disk, sc.host_file(work, "note", files["rbtest/renamed.txt"]), "/rbtest/note.txt")
    sc.rb("put", disk, sc.host_file(work, "big", files["rbtest/sub/big.bin"]), "/rbtest/sub/big.bin")
    sc.rb("put", disk, sc.host_file(work, "gone", b"deleted again\n"), "/rbtest/gone.txt")
    sc.rb("mv", disk, "/rbtest/note.txt", "/rbtest/renamed.txt")
    sc.rb("rm", disk, "/rbtest/gone.txt")
    sc.damage(disk, [(512, 512)])

    out = boot(kernel, initramfs(work, BFS, module), disk)
    print(out)
    found = {}
    for line in out.splitlines():
        m = re.match(r"([0-9a-f]{32})\s+\./(.+)$", line)
        if m:
            found[m.group(2)] = m.group(1)
    problems = []
    if "mount=0" not in out:
        problems.append("the kernel refused to mount it")
    for name, data in files.items():
        if found.get(name) != hashlib.md5(data).hexdigest():
            problems.append(f"{name} does not read back")
    if "rbtest/gone.txt" in found or "rbtest/note.txt" in found:
        problems.append("a deleted or renamed name is still listed")
    return problems, "Linux befs mounts rb-cli's volume and reads the files back"


def rb_slices(disk):
    """Each slice rb-cli lists, as (start LBA, sectors)."""
    rows = [l.split()[0] for l in sc.rb("inspect", disk).splitlines() if re.match(r"\s*\d+\s+\d+\s", l)]
    slices = set()
    for row in rows:
        p = subprocess.run([sc.rb_cli(), "ls", f"{disk}@{row}", "/"], capture_output=True, text=True)
        m = re.search(r"@ LBA (\d+), (\d+) bytes", p.stdout + p.stderr)
        if not m:
            sc.serialvm.no_verdict(f"rb-cli did not report partition {row}")
        slices.add((int(m.group(1)), int(m.group(2)) // 512))
    return slices


def check_solaris(kernel, artifact, work):
    disk = os.path.join(work, "vtoc.img")
    shutil.copyfile(artifact, disk)
    want = rb_slices(disk)
    with open(disk, "rb") as f:
        mbr = f.read(512)
    fdisk_start = int.from_bytes(mbr[0x1BE + 8:0x1BE + 12], "little")
    sc.damage(disk, [((fdisk_start + 1) * 512, 512)])

    out = boot(kernel, initramfs(work, SLICES, None), disk)
    print(out)
    seen = {(int(a), int(b)) for a, b in re.findall(r"^slice (\d+) (\d+)$", out, re.M)}
    missing = sorted(want - seen)
    problems = [f"the kernel has no slice at LBA {s} of {n} sectors" for s, n in missing]
    return problems, f"the Linux parser finds all {len(want)} slices where rb-cli wrote them"


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in ("bfs", "solaris-x86"):
        print(__doc__)
        return 2
    kernel = sc.serialvm.emulator_path("linux_kernel")
    version = re.search(r"vmlinuz-(.+)$", kernel).group(1)
    work = sc.workdir()
    if sys.argv[1] == "bfs":
        module = f"/lib/modules/{version}/kernel/fs/befs/befs.ko.zst"
        if not os.path.exists(module):
            sc.serialvm.unavailable(f"no {module}")
        problems, verdict = check_bfs(kernel, module, sys.argv[2], work)
    else:
        problems, verdict = check_solaris(kernel, sys.argv[2], work)
    if problems:
        print("FINDING: " + "; ".join(problems))
        sc.finish(sc.FINDING, work)
    print("PASS: " + verdict)
    sc.finish(sc.PASS, work)


if __name__ == "__main__":
    sys.exit(main())
