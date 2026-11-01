"""Shared plumbing for the emulator scenarios in this directory.

A scenario copies a real fixture (or makes a blank volume), changes it with
rb-cli, then hands it to the real operating system to judge. Each script exits
with the oracle codes from oracles/serialvm.py: 0 pass, 1 finding, 77 cannot
run here, 99 no verdict. scripts/regress-emulators.sh runs them all.

Inputs, all optional:
    RB_CLI          the rb-cli under test (default: <repo>/target/release/rb-cli)
    RB_EMU_CONTROL  set to 1 to run the scenario as its control: after the edits,
                    damage() wrecks a structure the checker must notice, so the
                    run is expected to end in a finding (exit 1)
    local.toml      `fixture_root` for the corpus, `[emulators]` for the VMs
"""

import os
import shutil
import struct
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REGRESS = os.path.dirname(HERE)
REPO = os.path.dirname(REGRESS)
sys.path.insert(0, os.path.join(REGRESS, "oracles"))
import serialvm  # noqa: E402

PASS, FINDING, UNAVAILABLE, NO_VERDICT = (
    serialvm.PASS,
    serialvm.FINDING,
    serialvm.UNAVAILABLE,
    serialvm.NO_VERDICT,
)


def rb_cli():
    path = os.environ.get("RB_CLI") or os.path.join(REPO, "target", "release", "rb-cli")
    if not os.access(path, os.X_OK):
        serialvm.unavailable(f"no rb-cli at {path} (set RB_CLI)")
    return path


def rb(*args, check=True):
    """Run rb-cli; a failing command is a finding against rb-cli itself."""
    argv = [rb_cli(), "-q", *args]
    p = subprocess.run(argv, capture_output=True, text=True)
    if check and p.returncode != 0:
        print(f"FINDING: rb-cli {' '.join(args)} exited {p.returncode}:\n{p.stderr}{p.stdout}")
        sys.exit(FINDING)
    return p.stdout


def local_config():
    import tomllib

    cfg = os.path.join(REGRESS, "local.toml")
    if not os.path.exists(cfg):
        return {}
    with open(cfg, "rb") as f:
        return tomllib.load(f)


def fixture(name, workdir):
    """Copy fixture `name` into `workdir` (unpacking .zst) and return the copy's path."""
    root = local_config().get("fixture_root")
    if not root:
        serialvm.unavailable("local.toml has no fixture_root")
    candidates = [
        os.path.join(root, name),
        os.path.join(os.path.dirname(root), "fixtures-large", name),
        os.path.join(REPO, "tests", "fixtures", name),
    ]
    for c in candidates:
        for path, packed in ((c, False), (c + ".zst", True)):
            if os.path.exists(path):
                out = os.path.join(workdir, name)
                if packed:
                    zstd = shutil.which("zstd") or serialvm.unavailable("zstd is not installed")
                    subprocess.run([zstd, "-q", "-d", path, "-o", out], check=True)
                else:
                    shutil.copyfile(path, out)
                return out
    serialvm.unavailable(f"fixture {name} is not in the corpus here")


def workdir():
    base = os.environ.get("TMPDIR") or None
    return tempfile.mkdtemp(prefix="rb-emu-", dir=base)


def host_file(workdir, name, data):
    path = os.path.join(workdir, name)
    with open(path, "wb") as f:
        f.write(data)
    return path


def finish(code, workdir_path):
    """Exit with `code`, keeping the work directory only when something needs a look."""
    # A control that passes is the failure worth a look, so its work directory is kept.
    if code == PASS and not is_control():
        shutil.rmtree(workdir_path, ignore_errors=True)
    else:
        print(f"kept for inspection: {workdir_path}")
    sys.exit(code)


def is_control():
    return os.environ.get("RB_EMU_CONTROL") == "1"


def damage(path, ranges):
    """In a control run, overwrite each (offset, length) of `path` with 0xA5 bytes."""
    if not is_control():
        return
    with open(path, "r+b") as f:
        for off, length in ranges:
            f.seek(off)
            f.write(b"\xa5" * length)
    print(f"control: damaged {len(ranges)} range(s) of {os.path.basename(path)}")


def ffs_first_cg(path):
    """Byte offset of cylinder group 0 of a UFS / HP-UX HFS volume (either byte order)."""
    with open(path, "rb") as f:
        for sb in (8192, 65536):
            f.seek(sb)
            b = f.read(1376)
            for e in "<>":
                if len(b) == 1376 and struct.unpack(e + "I", b[1372:1376])[0] in (
                    0x011954, 0x19540119, 0x095014,
                ):
                    cblkno = struct.unpack(e + "i", b[12:16])[0]
                    fsize = struct.unpack(e + "i", b[52:56])[0]
                    return cblkno * fsize
    raise SystemExit(f"no UFS superblock in {path}")
