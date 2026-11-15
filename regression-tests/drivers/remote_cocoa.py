#!/usr/bin/env python3
"""Run the Objective-C AX driver on another Mac, for the cocoa flows on a real 10.7 machine.

The runner starts this in place of ax_driver_objc (RB_COCOA_DRIVER) with the same two
arguments. It mirrors the case's scratch folder to the same path on the remote Mac, starts
the driver there over ssh, and relays its JSON lines: pushing the scratch folder before each
op (the runner writes picks and inputs there) and pulling it back after each answer and every
few seconds (the runner waits for the files the app writes). Paths only match if the runner's
scratch and cases roots are somewhere both Macs share a layout, such as /tmp.

    RB_COCOA_REMOTE      ssh host (an alias with ControlMaster keeps rsync quick)
    RB_COCOA_REMOTE_APP  the app's executable on the remote Mac
    RB_COCOA_REMOTE_DRV  ax_driver_objc on the remote Mac, built for it
    RB_COCOA_SYNC        extra folders to push once, colon-separated (the cases root)

An op naming 127.0.0.1:<port> (a server the runner started) gets that port forwarded back
here, through the alias's ControlMaster connection.
"""
import os
import re
import subprocess
import sys
import threading
import time

HOST = os.environ["RB_COCOA_REMOTE"]
APP = os.environ["RB_COCOA_REMOTE_APP"]
DRV = os.environ["RB_COCOA_REMOTE_DRV"]
EXTRA = [p for p in os.environ.get("RB_COCOA_SYNC", "").split(":") if p]

work = os.path.abspath(sys.argv[2])
scratch = os.path.dirname(work)
lock = threading.Lock()
done = threading.Event()


def log(msg):
    sys.stderr.write("remote_cocoa: %s\n" % msg)
    sys.stderr.flush()


# Each file's (mtime, size) as of the last sync: a file that differs is the runner's own change.
synced = {}


def snapshot(skip=()):
    for root, _, files in os.walk(scratch):
        for f in files:
            p = os.path.join(root, f)
            if p not in skip:
                try:
                    st = os.stat(p)
                except OSError:
                    continue
                synced[p] = (st.st_mtime_ns, st.st_size)


def dirty():
    out = []
    for root, _, files in os.walk(scratch):
        for f in files:
            p = os.path.join(root, f)
            try:
                st = os.stat(p)
            except OSError:
                continue
            if synced.get(p) != (st.st_mtime_ns, st.st_size):
                out.append(p)
    return out


def rsync(args):
    # Compressed: the images are mostly zeros, and a slow link otherwise stalls an op.
    subprocess.call(["rsync", "-az", "-e", "ssh"] + args,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def listed(paths):
    rel = "".join("/" + os.path.relpath(p, scratch) + "\n" for p in paths)
    tmp = os.path.join(os.path.dirname(scratch), ".remote_cocoa.%d" % os.getpid())
    with open(tmp, "w") as fh:
        fh.write(rel)
    return tmp


def push():
    """Send what the runner wrote (picks, rb-cli's outputs) since the last sync."""
    with lock:
        changed = dirty()
        if changed:
            rsync(["--files-from=" + listed(changed), scratch + "/", "%s:%s/" % (HOST, scratch)])
            snapshot()


def pull():
    """Fetch what the app wrote, by checksum: an image is sized first and filled within the
    same second, which size and mtime alone miss. The runner's unsent changes stay put."""
    with lock:
        changed = dirty()
        rsync(["-c", "--exclude-from=" + listed(changed),
               "%s:%s/" % (HOST, scratch), scratch + "/"])
        snapshot(set(changed))


tunnels = set()


def tunnel(line):
    """A server the runner started here (rb-cli serve on 127.0.0.1) is reached over ssh -R."""
    for port in re.findall(r"127\.0\.0\.1:(\d+)", line):
        if port not in tunnels:
            tunnels.add(port)
            subprocess.call(["ssh", "-O", "forward", "-R", "%s:127.0.0.1:%s" % (port, port), HOST],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    dirs = " ".join("'%s'" % d for d in [scratch] + EXTRA)
    # A fresh mirror of the case's scratch, and no driver (or the app it started) left over from
    # a case the runner killed first. Only the driver's children: never an app the user opened.
    reap = ("for d in $(ps axo pid=,comm= | awk '$2 ~ /%s$/ {print $1}'); do "
            "ps axo pid=,ppid= | awk -v d=$d '$2==d {print $1}' | xargs kill 2>/dev/null; "
            "kill $d; done" % os.path.basename(DRV))
    subprocess.call(["ssh", HOST, "%s; rm -rf '%s' && mkdir -p %s" % (reap, scratch, dirs)])
    for d in EXTRA:
        rsync([d + "/", "%s:%s/" % (HOST, d)])
    with lock:
        rsync([scratch + "/", "%s:%s/" % (HOST, scratch)])
        snapshot()
    proc = subprocess.Popen(["ssh", HOST, "'%s' '%s' '%s'" % (DRV, APP, work)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            universal_newlines=True, bufsize=1)

    def answers():
        for line in proc.stdout:
            pull()
            sys.stdout.write(line)
            sys.stdout.flush()
        done.set()

    def poll():
        while not done.wait(3):
            pull()

    threading.Thread(target=answers, daemon=True).start()
    threading.Thread(target=poll, daemon=True).start()
    try:
        for line in sys.stdin:
            push()
            tunnel(line)
            proc.stdin.write(line)
            proc.stdin.flush()
    except (BrokenPipeError, KeyboardInterrupt):
        pass
    try:
        proc.stdin.close()
    except BrokenPipeError:
        pass
    # The runner closing stdin ends the case; it kills this relay 5 s later.
    deadline = time.time() + 4
    while proc.poll() is None and time.time() < deadline:
        time.sleep(0.2)
    if proc.poll() is None:
        proc.kill()
        subprocess.call(["ssh", HOST, reap])
    pull()
    done.set()


if __name__ == "__main__":
    main()
