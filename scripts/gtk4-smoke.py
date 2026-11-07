#!/usr/bin/env python3
"""Tier B smoke for the GTK4 shell over AT-SPI (docs/native_frontends_plan.md section 6).

Run by scripts/gtk4-smoke.sh inside a D-Bus session with an X server. The app must already be
running with GTK_A11Y=atspi on a FAT image holding HELLO.TXT. Steps: the window and its six tabs
show, the image's partition is listed, Browse opens it and lists HELLO.TXT, Close returns.
Widgets are found by accessible id when GTK exposes it, else by role and name.
"""

import sys
import time

import gi

gi.require_version("Atspi", "2.0")
from gi.repository import Atspi  # noqa: E402

TABS = ["Backup", "Restore", "Build Disk", "Inspect", "Optical", "Archives"]


def log(msg):
    print(f"smoke: {msg}", flush=True)


def app_node(name, wait=60.0):
    end = time.time() + wait
    while time.time() < end:
        desk = Atspi.get_desktop(0)
        for i in range(desk.get_child_count()):
            app = desk.get_child_at_index(i)
            if app is not None and name in (app.get_name() or ""):
                return app
        time.sleep(0.5)
    return None


def nodes(root, limit=20000):
    """Every node under root, breadth first."""
    out, queue = [], [root]
    while queue and len(out) < limit:
        n = queue.pop(0)
        if n is None:
            continue
        out.append(n)
        try:
            queue.extend(n.get_child_at_index(i) for i in range(n.get_child_count()))
        except Exception:  # A node can vanish while the tree is walked.
            pass
    return out


def describe(n):
    try:
        return f"{n.get_role_name()} {n.get_name()!r} id={n.get_accessible_id()!r}"
    except Exception as e:
        return f"<gone: {e}>"


def find(root, name=None, role=None, wid=None, wait=20.0):
    end = time.time() + wait
    while time.time() < end:
        for n in nodes(root):
            try:
                if wid and n.get_accessible_id() == wid:
                    return n
                if name is not None and n.get_name() != name:
                    continue
                if role is not None and n.get_role_name() != role:
                    continue
                if name is not None or role is not None:
                    return n
            except Exception:
                continue
        time.sleep(0.5)
    return None


def press(n):
    action = n.get_action_iface()
    if action is None or action.get_n_actions() == 0:
        raise RuntimeError(f"no action on {describe(n)}")
    action.do_action(0)


def fail(app, msg):
    log(f"FAIL: {msg}")
    for n in nodes(app)[:400]:
        log("  " + describe(n))
    sys.exit(1)


def main():
    app = app_node(sys.argv[1] if len(sys.argv) > 1 else "rusty")
    if app is None:
        log("FAIL: the app never appeared on the accessibility bus")
        sys.exit(1)
    log(f"app: {describe(app)}")
    every = nodes(app)
    with_id = [n for n in every if n.get_accessible_id()]
    log(f"{len(every)} nodes, {len(with_id)} with an accessible id")

    for tab in TABS:
        if find(app, name=tab, wait=10.0) is None:
            fail(app, f"no {tab} tab")
    log("all six tabs show")

    if find(app, name="FAT12", wait=30.0) is None and find(app, name="FAT16", wait=1.0) is None:
        fail(app, "the fixture's partition is not listed")
    log("the fixture's partition is listed")

    browse = find(app, wid="inspect.row.browse", wait=1.0) or find(app, name="Browse", role="button")
    if browse is None:
        fail(app, "no Browse button")
    press(browse)
    if find(app, name="HELLO.TXT", wait=30.0) is None:
        fail(app, "Browse did not list HELLO.TXT")
    log("Browse lists HELLO.TXT")

    close = find(app, name="Close", role="button", wait=10.0)
    if close is None:
        fail(app, "no Close button in the browse view")
    press(close)
    if find(app, name="FAT12", wait=20.0) is None and find(app, name="FAT16", wait=1.0) is None:
        fail(app, "Close did not return to the partition table")
    log("Close returns to Inspect")
    log("PASS")


if __name__ == "__main__":
    main()
