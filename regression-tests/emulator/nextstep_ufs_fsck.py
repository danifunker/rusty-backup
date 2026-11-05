#!/usr/bin/env python3
"""NeXTSTEP UFS: rb-cli writes a 4.3BSD UFS partition, then NeXTSTEP 3.3's own fsck judges it.

    nextstep_ufs_fsck.py blank|next-edit|blank-pristine

blank formats the partition with `rb-cli new volume ufs-43bsd` inside a
`new hd next` label; next-edit edits a copy of the NeXTSTEP boot disk itself,
a volume NeXTSTEP made; blank-pristine checks the blank volume with no edits,
as a baseline. rb-cli then runs mkdir / put (past the direct blocks)
/ mv / rm, the disk goes in at SCSI target 1 (write-protected) and NeXTSTEP
runs `fsck -n` on /dev/rsd1a, then mounts it read-only and lists /rbtest.

Driven through Previous (github.com/danifunker/previous-fork) with no window:
the boot disk's rc.local starts a root shell on serial port A, which Previous
connects to two FIFOs (see the fork's automation.howto.txt).

local.toml:
    [emulators]
    previous = "/path/to/previous"            # the fork's binary; ROMs sit beside it
    nextstep_boot = "/path/to/NeXTSTEP3-3_Disk-serial.hda"
"""

import os
import re
import select
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scenario as sc  # noqa: E402

DEV = "sd1a"
DECLINED = re.compile(r"\?\s+no\s*$")


class Previous:
    """A headless Previous with serial port A on FIFOs; `run` executes a guest shell command."""

    def __init__(self, binary, boot, test, log_path, work, test_writable=False):
        self.work = work
        self.to_guest = os.path.join(work, "to-guest")
        self.from_guest = os.path.join(work, "from-guest")
        for p in (self.to_guest, self.from_guest):
            os.mkfifo(p)
        self.rfd = os.open(self.from_guest, os.O_RDONLY | os.O_NONBLOCK)
        self.log = open(log_path, "wb")
        env = dict(os.environ, SDL_VIDEODRIVER="dummy", SDL_AUDIODRIVER="dummy")
        self.proc = subprocess.Popen(
            [binary, "--dialogs", "off", "--sound", "off",
             "--scc-a-in", self.to_guest, "--scc-a-out", self.from_guest,
             "--cmd-fifo", os.path.join(work, "control"),
             "--machine", "station", "--memsize", "32", "--boot", "scsi",
             "--scsi", f"0={boot}", "--scsi" if test_writable else "--scsi-ro", f"1={test}",
             "--timeout", "3600"],
            cwd=os.path.dirname(binary), env=env, stdout=self.log, stderr=subprocess.STDOUT,
            preexec_fn=sc.serialvm.die_with_parent,
        )
        self.wfd = None
        self.buf = b""

    def _writer(self):
        while self.wfd is None:
            try:
                self.wfd = os.open(self.to_guest, os.O_WRONLY | os.O_NONBLOCK)
            except OSError:
                if self.proc.poll() is not None:
                    sc.serialvm.no_verdict(f"Previous exited ({self.proc.returncode})")
                time.sleep(0.2)
        return self.wfd

    def expect(self, pattern, timeout):
        rx = re.compile(pattern.encode())
        end = time.time() + timeout
        while time.time() < end:
            m = rx.search(self.buf)
            if m:
                self.buf = self.buf[m.end():]
                return m
            if self.proc.poll() is not None:
                sc.serialvm.no_verdict(f"Previous exited ({self.proc.returncode}) waiting for {pattern!r}")
            r, _, _ = select.select([self.rfd], [], [], 1.0)
            if r:
                try:
                    d = os.read(self.rfd, 65536)
                except BlockingIOError:
                    d = b""
                self.buf += d
                self.log.write(d)
                self.log.flush()
        sc.serialvm.no_verdict(f"timed out waiting for {pattern!r}; last output {self.buf[-300:]!r}")

    def run(self, command, timeout=600):
        tag = f"RBX{int(time.time() * 1000) % 1000000}"
        self.buf = b""
        os.write(self._writer(), f"{command}; echo {tag}=$?\n".encode())
        m = self.expect(rf"([\s\S]*?){tag}=(\d+)", timeout)
        out = m.group(1).decode(errors="replace").replace("\r", "")
        return int(m.group(2)), out.strip()

    def close(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        for fd in (self.rfd, self.wfd):
            if fd is not None:
                os.close(fd)
        self.log.close()


def main():
    variant = sys.argv[1] if len(sys.argv) > 1 else "blank"
    pristine = variant.endswith("-pristine")
    variant = variant[: -len("-pristine")] if pristine else variant
    if variant not in ("blank", "next-edit"):
        print(__doc__)
        sys.exit(2)
    binary = sc.serialvm.emulator_path("previous")
    boot_src = sc.serialvm.emulator_path("nextstep_boot")
    work = sc.workdir()
    disk = os.path.join(work, "test-disk.img")
    if variant == "blank":
        raw = os.path.join(work, "ufs.img")
        sc.rb("new", "volume", "ufs-43bsd", raw, "--size", "32M")
        sc.rb("new", "hd", "next", disk, "--size", "40M", "--partition", "32M:4.3BSD", "--fill", f"1={raw}")
    else:
        shutil.copyfile(boot_src, disk)
    vol = f"{disk}@1"

    if not pristine:
        note = sc.host_file(work, "note.txt", b"hello from rusty-backup\n")
        big = sc.host_file(work, "big.bin", bytes((i * 23) % 251 for i in range(600 * 1024)))
        gone = sc.host_file(work, "gone.txt", b"deleted again\n")
        sc.rb("mkdir", vol, "/rbtest")
        sc.rb("mkdir", vol, "/rbtest/sub")
        sc.rb("put", vol, note, "/rbtest/note.txt")
        sc.rb("put", vol, big, "/rbtest/sub/big.bin")
        sc.rb("put", vol, gone, "/rbtest/gone.txt")
        sc.rb("mv", vol, "/rbtest/note.txt", "/rbtest/renamed.txt")
        sc.rb("rm", vol, "/rbtest/gone.txt")
    part_off = partition_offset(disk)
    sc.damage(disk, [(part_off + sc.ffs_first_cg(disk, base=part_off), 2048)])

    # The guest writes to its boot disk, so every run boots a fresh copy.
    boot = os.path.join(work, "boot.hda")
    shutil.copyfile(boot_src, boot)
    vm = Previous(binary, boot, disk, os.path.join(work, "console.log"), work)
    try:
        vm.expect(r"PREVIOUS-READY", 1800)
        time.sleep(2)
        vm.run("PS1='rb# '; export PS1", timeout=60)
        status, out = vm.run(f"/usr/etc/fsck -n /dev/r{DEV}", timeout=900)
        print(out)
        vm.run("mkdir /tmp/rbm", timeout=60)
        _, listing = vm.run(f"/usr/etc/mount -r /dev/{DEV} /tmp/rbm && ls -R /tmp/rbm/rbtest; /usr/etc/umount /tmp/rbm", timeout=300)
        print("--- ls -R /rbtest\n" + listing)
    finally:
        vm.close()
    questions = [l for l in out.splitlines() if DECLINED.search(l)]
    problems = []
    if status != 0 or questions:
        problems.append(f"fsck exited {status}, {len(questions)} repair(s) declined")
    for name in () if pristine else ("renamed.txt", "big.bin"):
        if name not in listing:
            problems.append(f"NeXTSTEP does not list {name}")
    if "gone.txt" in listing or "note.txt" in listing:
        problems.append("a deleted or renamed name is still listed")
    if problems:
        print("FINDING: " + "; ".join(problems))
        sc.finish(sc.FINDING, work)
    print("PASS: NeXTSTEP 3.3 fsck accepts rb-cli's partition and lists the files")
    sc.finish(sc.PASS, work)


def partition_offset(disk):
    """Byte offset of partition a, from rb-cli's own reading of the NeXT label."""
    for line in sc.rb("inspect", disk).splitlines():
        cols = line.split()
        if cols and cols[0] == "1":
            return int(cols[cols.index("(4.3BSD)") + 1]) * 512
    sc.serialvm.no_verdict("rb-cli inspect found no partition 1")


if __name__ == "__main__":
    main()
