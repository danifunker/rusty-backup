"""Drive a QEMU guest over its serial console, for the emulator oracles.

An oracle boots a real operating system, hands it the artifact as a disk, runs
that OS's own checker, and turns the answer into an exit code:

    0   the guest's checker passed the artifact
    1   the guest's checker rejected it (a finding against our bytes)
    77  this host cannot run the oracle (emulator, VM image or config missing)
    99  the guest never produced a verdict (boot hang, login failure, timeout)

77 and 99 are deliberately not 1: a missing VM or a hung boot says nothing
about the artifact, and `rb-regress verify` reports them as skips, not failures.

VM locations are machine-specific, so they come from the gitignored
regression-tests/local.toml:

    [emulators]
    freebsd15 = "/path/to/freebsd15"      # holds the base qcow2 + overlay

Used by oracles/freebsd/fsck_ffs.py and the scenario scripts under
regression-tests/emulator/.
"""

import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time

try:
    import tomllib
except ImportError:  # Python < 3.11
    tomllib = None

PASS, FINDING, UNAVAILABLE, NO_VERDICT = 0, 1, 77, 99

HERE = os.path.dirname(os.path.abspath(__file__))
REGRESS = os.path.dirname(HERE)


def unavailable(why):
    """Exit 77: this host cannot run the oracle."""
    print(f"oracle unavailable: {why}")
    sys.exit(UNAVAILABLE)


def no_verdict(why):
    """Exit 99: the guest never answered."""
    print(f"no verdict: {why}")
    sys.exit(NO_VERDICT)


def emulator_path(key):
    """The path local.toml's [emulators] table gives `key`, or exit 77."""
    cfg = os.path.join(REGRESS, "local.toml")
    if tomllib is None:
        unavailable("python has no tomllib (needs 3.11+)")
    if not os.path.exists(cfg):
        unavailable(f"no {cfg}")
    with open(cfg, "rb") as f:
        data = tomllib.load(f)
    path = data.get("emulators", {}).get(key)
    if not path:
        unavailable(f"local.toml has no [emulators] {key} entry")
    path = os.path.expanduser(path)
    if not os.path.exists(path):
        unavailable(f"{key} path {path} does not exist")
    return path


def require_program(name):
    path = shutil.which(name)
    if not path:
        unavailable(f"{name} is not installed")
    return path


class SerialVM:
    """A QEMU guest whose serial console is a unix socket we read and type into."""

    def __init__(self, argv, log_path=None):
        # Keep the socket path short and local (unix socket paths are capped at 108 bytes).
        sock_root = os.environ.get("XDG_RUNTIME_DIR") or "/tmp"
        if not os.path.isdir(sock_root):
            sock_root = None
        self.dir = tempfile.mkdtemp(prefix="rb-oracle-", dir=sock_root)
        self.sock_path = os.path.join(self.dir, "serial.sock")
        self.log = open(log_path or os.path.join(self.dir, "console.log"), "wb")
        argv = list(argv) + [
            "-serial", f"unix:{self.sock_path},server,nowait",
            "-display", "none",
            "-monitor", "none",
        ]
        self.proc = subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        self.sock = socket.socket(socket.AF_UNIX)
        # QEMU creates the socket only once it is up, which can take a while on a busy host.
        for _ in range(300):
            if self.proc.poll() is not None:
                err = self.proc.stderr.read().decode(errors="replace")
                no_verdict(f"emulator exited at start: {err.strip()}")
            try:
                self.sock.connect(self.sock_path)
                break
            except OSError:
                time.sleep(0.1)
        else:
            no_verdict("serial console never appeared")
        self.sock.setblocking(False)
        self.buf = b""

    def _pump(self, timeout):
        end = time.time() + timeout
        while time.time() < end:
            try:
                d = self.sock.recv(65536)
                if not d:
                    return
                self.buf += d
                self.log.write(d)
                self.log.flush()
                return
            except BlockingIOError:
                time.sleep(0.05)

    def expect(self, pattern, timeout):
        """Wait until `pattern` (a regex) appears in fresh output; return the match."""
        rx = re.compile(pattern.encode() if isinstance(pattern, str) else pattern)
        end = time.time() + timeout
        while time.time() < end:
            m = rx.search(self.buf)
            if m:
                self.buf = self.buf[m.end():]
                return m
            if self.proc.poll() is not None:
                no_verdict(f"emulator exited while waiting for {pattern!r}")
            self._pump(min(1.0, max(0.05, end - time.time())))
        tail = self.buf[-400:].decode(errors="replace")
        no_verdict(f"timed out waiting for {pattern!r}; last output: {tail!r}")

    def send(self, text, delay=0.02):
        """Type `text` one byte at a time (serial consoles drop bursts)."""
        for ch in text.encode():
            self.sock.send(bytes([ch]))
            time.sleep(delay)

    def run(self, command, timeout=120):
        """Run a shell command in the guest; return (exit status, output)."""
        tag = f"RBX{int(time.time() * 1000) % 1000000}"
        self.buf = b""
        self.send(f"{command}; echo {tag}=$?\r")
        m = self.expect(rf"\n{tag}=(\d+)", timeout)
        out = self.log_tail_before(tag)
        return int(m.group(1)), out

    def log_tail_before(self, tag):
        """What the guest printed for the last command, from the console log."""
        self.log.flush()
        with open(self.log.name, "rb") as f:
            data = f.read().decode(errors="replace")
        end = data.rfind(f"{tag}=")
        start = data.rfind(f"echo {tag}=", 0, end)
        chunk = data[start:end] if start >= 0 else data[max(0, end - 4000):end]
        lines = chunk.splitlines()[1:]
        return "\n".join(lines).strip()

    def close(self):
        try:
            self.proc.terminate()
            self.proc.wait(timeout=10)
        except Exception:
            self.proc.kill()
        self.log.close()
        shutil.rmtree(self.dir, ignore_errors=True)
