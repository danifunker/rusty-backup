"""Drive a Windows 7 QEMU guest through its keyboard, for the emulator oracles.

Windows 7 has no serial console, so the guest is driven the way a person would:
QEMU's monitor types keys (`sendkey`) and `screendump` shows when the desktop
has settled. Nothing is read off the screen. The work happens in a batch file
the host puts on a small FAT results disk beforehand; the guest runs it from an
elevated prompt, writes its answers to that disk and shuts itself down, and the
host reads the answers back with rb-cli.

local.toml:
    [emulators]
    win7 = "/path/to/win7.qcow2"   # an overlay on the Win7 image, settled by `win7vm.py settle`

The overlay is booted once with exactly the hardware the scenarios use (AHCI,
three disks with fixed serials, no NIC), so Windows has its drivers and never
asks to restart. Every scenario run adds -snapshot on top, so nothing persists.

    win7vm.py settle <overlay>      boot the overlay with scenario hardware and wait;
                                    accept the restart prompt with Enter, then shut down
"""

import hashlib
import os
import socket
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import serialvm  # noqa: E402

# US-layout qcodes for the characters a command line needs; shifted ones carry a prefix.
_KEYS = {" ": "spc", "\\": "backslash", ".": "dot", "-": "minus", "/": "slash",
         "=": "equal", ",": "comma", ";": "semicolon", "'": "apostrophe", "\n": "ret",
         ":": "shift-semicolon", "%": "shift-5", "@": "shift-2", ">": "shift-dot",
         "<": "shift-comma", "_": "shift-minus", "(": "shift-9", ")": "shift-0",
         '"': "shift-apostrophe", "&": "shift-7", "|": "shift-backslash", "*": "shift-8"}


def qcode(ch):
    if ch.isdigit() or ("a" <= ch <= "z"):
        return ch
    if "A" <= ch <= "Z":
        return "shift-" + ch.lower()
    if ch in _KEYS:
        return _KEYS[ch]
    raise ValueError(f"no key for {ch!r}")


def desktop_visible(ppm):
    """True when the screen shows the Win7 taskbar: a grey band along the bottom with a bright start orb."""
    try:
        header, rest = ppm.split(b"\n", 1)[0], ppm.split(b"\n", 3)
        w, h = (int(x) for x in rest[1].split())
        pix = rest[3]
    except (ValueError, IndexError):
        return False
    if header != b"P6" or len(pix) < w * h * 3:
        return False

    def at(x, y):
        i = (y * w + x) * 3
        return pix[i], pix[i + 1], pix[i + 2]

    orb = at(25, h - 20)
    band = [at(x, h - 15) for x in range(60, max(61, w - 200), 7)]
    red = sum(p[0] for p in band) / len(band)
    return sum(orb) > 500 and red > 110


class Win7VM:
    """Windows 7 on QEMU with a monitor socket.

    `disks` are (path, keep) pairs on AHCI ports 1..n; `readonly` disks are
    write-protected USB mass-storage devices on EHCI (QEMU's IDE refuses read-only
    drives and Win7 has no driver for its SCSI HBAs). Windows mounts NTFS on a
    write-protected disk as RAW unless its $LogFile is clean (an mkntfs volume is
    not), so the scenarios keep their test disk writable.
    """

    def __init__(self, overlay, disks, snapshot=True, memory="2048", readonly=()):
        sock_root = os.environ.get("XDG_RUNTIME_DIR") or None
        self.dir = tempfile.mkdtemp(prefix="rb-win7-", dir=sock_root)
        self.mon = os.path.join(self.dir, "mon.sock")
        accel = ["-enable-kvm"] if os.access("/dev/kvm", os.W_OK) else []
        argv = [serialvm.require_program("qemu-system-x86_64"), *accel, "-m", memory, "-smp", "2",
                "-drive", f"file={overlay},if=none,id=d0,format=qcow2",
                "-device", "ich9-ahci,id=ahci",
                "-device", "ide-hd,drive=d0,bus=ahci.0,serial=RBSYS"]
        if snapshot:
            argv.append("-snapshot")
        for i, (path, keep) in enumerate(disks, start=1):
            snap = ",snapshot=off" if keep else ""
            argv += ["-drive", f"file={path},if=none,id=d{i},format=raw{snap}",
                     "-device", f"ide-hd,drive=d{i},bus=ahci.{i},serial=RBDISK{i}"]
        # Always present, so the hardware matches the settled overlay whether or not a USB disk is attached.
        argv += ["-device", "ich9-usb-ehci1,id=ehci"]
        for i, path in enumerate(readonly):
            argv += ["-drive", f"file={path},if=none,id=r{i},format=raw,readonly=on",
                     "-device", f"usb-storage,bus=ehci.0,drive=r{i},serial=RBRO{i}"]
        argv += ["-monitor", f"unix:{self.mon},server,nowait",
                 "-vga", "std", "-display", "none", "-nic", "none"]
        self.proc = subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                     preexec_fn=serialvm.die_with_parent)
        for _ in range(300):
            if self.proc.poll() is not None:
                serialvm.no_verdict("qemu exited at start: "
                                    + self.proc.stderr.read().decode(errors="replace").strip())
            if os.path.exists(self.mon):
                break
            time.sleep(0.1)

    def hmp(self, command, wait=0.3):
        s = socket.socket(socket.AF_UNIX)
        s.connect(self.mon)
        s.settimeout(wait)
        try:
            s.recv(65536)
        except socket.timeout:
            pass
        s.sendall(command.encode() + b"\n")
        time.sleep(wait)
        s.close()

    def screendump(self, path):
        """Write the screen to `path` (PPM); retried because screendump is asynchronous."""
        self.hmp(f"screendump {path}", wait=0.5)
        for _ in range(40):
            try:
                with open(path, "rb") as f:
                    data = f.read()
                if data.startswith(b"P6") and len(data) > 1000:
                    w, h = (int(x) for x in data.split(b"\n")[1].split())
                    if len(data) >= w * h * 3:
                        return data
            except (OSError, ValueError, IndexError):
                pass
            time.sleep(0.25)
        return b""

    def wait_desktop(self, timeout=600, settle=3, min_wait=40):
        """Wait until the taskbar shows and the screen stops changing for `settle` looks 5 s apart."""
        start = time.time()
        last, same = None, 0
        shot = os.path.join(self.dir, "s.ppm")
        while time.time() - start < timeout:
            if self.proc.poll() is not None:
                serialvm.no_verdict("Windows exited before reaching the desktop")
            ppm = self.screendump(shot)
            digest = hashlib.sha1(ppm).hexdigest()
            same = same + 1 if digest == last and desktop_visible(ppm) else 0
            last = digest
            if same >= settle and time.time() - start >= min_wait:
                return
            time.sleep(5)
        serialvm.no_verdict("the Windows desktop never settled")

    def key(self, combo, wait=0.15):
        self.hmp(f"sendkey {combo}", wait=wait)

    def type(self, text):
        for ch in text:
            self.key(qcode(ch), wait=0.08)

    def elevated_prompt(self):
        """Start menu, `cmd`, Ctrl+Shift+Enter, then Alt+Y on the UAC prompt."""
        self.key("esc")
        time.sleep(1)
        self.key("meta_l")
        time.sleep(2)
        self.type("cmd")
        time.sleep(2)
        self.key("ctrl-shift-ret")
        time.sleep(4)
        self.key("alt-y")
        time.sleep(4)

    def wait_exit(self, timeout):
        try:
            self.proc.wait(timeout=timeout)
            return True
        except subprocess.TimeoutExpired:
            return False

    def close(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def settle(overlay):
    """Boot the overlay with scenario hardware so Windows installs its drivers once."""
    work = tempfile.mkdtemp(prefix="rb-win7-settle-")
    disks = []
    for i in (1, 2, 3):
        p = os.path.join(work, f"blank{i}.img")
        with open(p, "wb") as f:
            f.truncate(64 << 20)
        disks.append((p, False))
    vm = Win7VM(overlay, disks[1:], snapshot=False, readonly=[disks[0][0]])
    for verb in ("/r", "/s"):
        # Let the new devices' drivers finish installing before each restart / shutdown.
        vm.wait_desktop()
        time.sleep(90)
        vm.screendump(os.path.join(work, f"before{verb[1]}.ppm"))
        vm.key("esc")
        vm.key("meta_l-r")
        time.sleep(3)
        vm.type(f"shutdown {verb} /t 0\n")
        if verb == "/r":
            print("restarting; waiting for the desktop again")
            time.sleep(60)
    ok = vm.wait_exit(300)
    vm.close()
    print("settled" if ok else f"did not shut down; see {work}/befores.ppm")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "settle":
        settle(sys.argv[2])
    else:
        print(__doc__)
