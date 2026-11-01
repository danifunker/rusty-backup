"""Shared plumbing for the emulator scenarios in this directory.

A scenario copies a real fixture (or makes a blank volume), changes it with
rb-cli, then hands it to the real operating system to judge. Each script exits
with the oracle codes from oracles/serialvm.py: 0 pass, 1 finding, 77 cannot
run here, 99 no verdict. scripts/regress-emulators.sh runs them all.

Inputs, all optional:
    RB_CLI          the rb-cli under test (default: <repo>/target/release/rb-cli)
    local.toml      `fixture_root` for the corpus, `[emulators]` for the VMs
"""

import os
import shutil
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
    if code == PASS:
        shutil.rmtree(workdir_path, ignore_errors=True)
    else:
        print(f"kept for inspection: {workdir_path}")
    sys.exit(code)
