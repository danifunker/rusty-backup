#!/usr/bin/env python3
"""GUI regression suite for the GTK4 shell over AT-SPI (docs/native_frontends_plan.md section 6).

Run by scripts/gtk4-gui-suite.sh inside a D-Bus session with an X server, which starts the app
with GTK_A11Y=atspi on a FAT image holding HELLO.TXT and passes its work folder. Pickers answer
from WORK/picks.tsv (RUSTY_BACKUP_TEST_PICKS) rather than a dialog. GTK 4.14 exposes no
accessible id over AT-SPI, so widgets are found by role and name.

Cases run in order in one app; name some on the command line to run only those (the browse case
always runs first, as it waits for the window). Each CD case builds a real image through the
Build Disk screen and checks its bytes for what the options promise.
"""

import os
import subprocess
import sys
import time

import gi

gi.require_version("Atspi", "2.0")
from gi.repository import Atspi  # noqa: E402

TABS = ["Backup", "Restore", "Build Disk", "Inspect", "Optical", "Archives"]
CD_KIND = "CD-ROM image"
LAYOUTS = [
    "IRIX EFS CD-ROM",
    "Classic Mac CD-ROM (HFS)",
    "Mac CD-ROM (HFS+, Mac OS 8.1 and later)",
    "NeXTSTEP CD-ROM (UFS)",
]
OPTIONS = [
    "Unpack archives",
    "Unpack every archive into the root",
    "Decompress .gz files",
    "Copy files out of disk images",
    "Replace files with the same name",
    "Ignore this computer's permissions",
]
HELP_PHRASES = [
    "are opened and their contents put on the disc",
    "every archive's contents go",
    "Files compressed with gzip go on the disc uncompressed",
    "Disk images in the folder",
    "the later one replaces the earlier one",
    "Normally each file keeps the owner",
    "stores each file's Mac data",
]
SIDECARS = "macOS ._ files"
SIDECAR_CHOICES = {
    "efs": ["Exclude", "Leave as-is"],
    "mac": ["Use as resource forks", "Leave as-is", "Exclude"],
}

WORK = ""
WINDOW = ""


class Fail(Exception):
    pass


def log(msg):
    print(f"suite: {msg}", flush=True)


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


def nodes(root, limit=40000):
    """Every node under root, breadth first."""
    out, queue, seen = [], [root], set()
    while queue and len(out) < limit:
        n = queue.pop(0)
        if n is None or n in seen:
            continue
        seen.add(n)
        out.append(n)
        try:
            queue.extend(n.get_child_at_index(i) for i in range(n.get_child_count()))
        except Exception:  # A node can vanish while the tree is walked.
            pass
    return out


def describe(n):
    try:
        desc = n.get_description() or ""
        extra = f" desc={desc[:60]!r}" if desc else ""
        action = n.get_action_iface()
        if action is not None and action.get_n_actions() > 0:
            names = [action.get_action_name(i) for i in range(action.get_n_actions())]
            extra += f" actions={names}"
        states = n.get_state_set().get_states()
        flags = [Atspi.StateType(st).value_nick for st in states
                 if Atspi.StateType(st).value_nick in ("sensitive", "enabled", "checked", "focused")]
        extra += f" [{','.join(flags)}]"
        return f"{n.get_role_name()} {n.get_name()!r}{extra}"
    except Exception as e:
        return f"<gone: {e}>"


def has_state(n, state):
    try:
        return n.get_state_set().contains(state)
    except Exception:
        return False


def is_showing(n):
    return has_state(n, Atspi.StateType.SHOWING)


def is_sensitive(n):
    return has_state(n, Atspi.StateType.SENSITIVE)


def is_on(n):
    return has_state(n, Atspi.StateType.CHECKED) or has_state(n, Atspi.StateType.PRESSED)


def find(root, name=None, role=None, wait=20.0, contains=None, showing=False, roles=None):
    end = time.time() + wait
    while True:
        for n in nodes(root):
            try:
                if showing and not is_showing(n):
                    continue
                if name is not None and n.get_name() != name:
                    continue
                if contains is not None and contains not in (n.get_name() or ""):
                    continue
                if role is not None and n.get_role_name() != role:
                    continue
                if roles is not None and n.get_role_name() not in roles:
                    continue
                return n
            except Exception:
                continue
        if time.time() >= end:
            return None
        time.sleep(0.5)


def need(root, what, **kw):
    n = find(root, **kw)
    if n is None:
        raise Fail(f"not found: {what}")
    return n


def sibling_button(root, beside, name, wait=20.0):
    """A showing push button called `name` whose parent also holds one called `beside`."""
    end = time.time() + wait
    while time.time() < end:
        for n in nodes(root):
            try:
                if n.get_role_name() != "push button" or n.get_name() != beside:
                    continue
                parent = n.get_parent()
                for i in range(parent.get_child_count()):
                    c = parent.get_child_at_index(i)
                    if c.get_name() == name and c.get_role_name() == "push button" and is_showing(c):
                        return c
            except Exception:
                continue
        time.sleep(0.5)
    return None


def xdo(*args):
    out = subprocess.run(["xdotool", *map(str, args)], check=True, capture_output=True, text=True)
    return out.stdout


def main_window():
    """The app's largest visible X window, raised to a size where no page needs scrolling."""
    best = None
    for wid in xdo("search", "--onlyvisible", "--name", ".").split():
        geo = dict(
            line.strip().split("=", 1)
            for line in xdo("getwindowgeometry", "--shell", wid).splitlines()
            if "=" in line
        )
        area = int(geo["WIDTH"]) * int(geo["HEIGHT"])
        if best is None or area > best[0]:
            best = (area, wid)
    if best is None:
        raise Fail("no X window for the app")
    return best[1]


def window_origin():
    geo = dict(
        line.strip().split("=", 1)
        for line in xdo("getwindowgeometry", "--shell", WINDOW).splitlines()
        if "=" in line
    )
    return int(geo["X"]), int(geo["Y"])


def click(n):
    """A real pointer click in the middle of the node, from its window-relative extents."""
    comp = n.get_component_iface()
    if comp is None:
        raise Fail(f"no extents for {describe(n)}")
    ext = comp.get_extents(Atspi.CoordType.WINDOW)
    if ext.width <= 0 or ext.height <= 0:
        raise Fail(f"{describe(n)} has no size on screen")
    ox, oy = window_origin()
    x, y = ox + ext.x + ext.width // 2, oy + ext.y + ext.height // 2
    xdo("mousemove", x, y)
    time.sleep(0.2)
    xdo("click", 1)
    time.sleep(0.5)


def key(name, times=1):
    for _ in range(times):
        xdo("key", name)
        time.sleep(0.15)


def press(n):
    """Run a button's action; anything else gets a real click."""
    action = n.get_action_iface()
    if n.get_role_name() == "push button" and action is not None and action.get_n_actions() > 0:
        action.do_action(0)
        time.sleep(0.4)
    else:
        click(n)


def button(root, name, wait=20.0):
    return need(root, f"button {name!r}", name=name, role="push button", showing=True, wait=wait)


# Controls a row shows as: a switch row, a combo row, a check row.
SWITCH_ROLES = ("switch", "toggle button", "check box")
COMBO_ROLES = ("combo box",)


def option(root, label):
    """An option's toggle: the switch inside its row, named after the row."""
    return need(root, f"option {label!r}", name=label, roles=SWITCH_ROLES, showing=True)


def option_row(root, label):
    """An option's row, which carries the greyed-out state."""
    return need(root, f"row {label!r}", name=label, role="list item", showing=True)


def set_option(root, label, on):
    n = option(root, label)
    if is_on(n) != on:
        action = n.get_action_iface()
        if action is not None and action.get_n_actions() > 0:
            action.do_action(0)
        else:
            click(n)
        end = time.time() + 5
        while is_on(option(root, label)) != on:
            if time.time() > end:
                raise Fail(f"{label!r} did not turn {'on' if on else 'off'}")
            time.sleep(0.3)


def combo_choice(root, label, choices, want):
    """Pick `want` in the combo row `label`, whose list is `choices`, by keyboard."""
    n = need(root, f"combo {label!r}", name=label, roles=COMBO_ROLES, showing=True)
    now = combo_value(n, choices)
    if now == want:
        return
    # The popover opens on the current choice, so walk from there.
    click(n)
    time.sleep(0.6)
    step = choices.index(want) - choices.index(now)
    key("Down" if step > 0 else "Up", abs(step))
    key("Return")
    end = time.time() + 5
    while combo_value(need(root, label, name=label, roles=COMBO_ROLES, showing=True), choices) != want:
        if time.time() > end:
            raise Fail(f"{label!r} did not change to {want!r}")
        time.sleep(0.3)


def combo_value(n, choices):
    """The choice a combo row shows, read from the labels inside it."""
    for c in nodes(n, limit=200):
        try:
            name = c.get_name() or ""
        except Exception:
            continue
        if c is not n and name in choices and is_showing(c):
            return name
    raise Fail(f"cannot read the value of {describe(n)}; it holds "
               + "; ".join(describe(c) for c in nodes(n, limit=40)))


def shows_text(root, text, wait=10.0):
    """Some showing node carries `text` in its name or description (a row's subtitle is either)."""
    end = time.time() + wait
    while True:
        for n in nodes(root):
            try:
                if not is_showing(n):
                    continue
                if text in (n.get_name() or "") or text in (n.get_description() or ""):
                    return True
            except Exception:
                continue
        if time.time() >= end:
            return False
        time.sleep(0.5)


def need_text(root, text, wait=10.0):
    if not shows_text(root, text, wait):
        raise Fail(f"nothing on screen says {text!r}")


def set_picks(**answers):
    """Answer the next pickers: purpose suffix -> path."""
    with open(os.path.join(WORK, "picks.tsv"), "w") as f:
        for purpose, path in answers.items():
            f.write(f"build_disk.{purpose}\t{path}\n")


def contains(path, needle):
    """The image holds `needle`, read in chunks so a large image stays out of memory."""
    tail = b""
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                return False
            if needle in tail + chunk:
                return True
            tail = chunk[-len(needle):]


def check_bytes(image, present=(), absent=()):
    for b in present:
        if not contains(image, b):
            raise Fail(f"{os.path.basename(image)} is missing {b!r}")
    for b in absent:
        if contains(image, b):
            raise Fail(f"{os.path.basename(image)} holds {b!r}, which it should not")


# --- cases -----------------------------------------------------------------------------------


def case_browse(app):
    for tab in TABS:
        need(app, f"{tab} tab", name=tab, wait=10.0)
    log("all six tabs show")
    need(app, "the fixture's partition", contains="[FAT", wait=30.0)
    press(button(app, "Browse"))
    need(app, "HELLO.TXT in Browse", name="HELLO.TXT", wait=30.0)
    # The window's own title-bar Close has the same name: take the one beside Tree.
    close = sibling_button(app, beside="Tree", name="Close")
    if close is None:
        raise Fail("no Close button in the browse view")
    press(close)
    need(app, "the partition table after Close", contains="[FAT", wait=20.0)


def open_build(app):
    press(need(app, "Build Disk tab", name="Build Disk", wait=10.0))


def to_cd_disk_step(app, image):
    """From the Kind step: CD-ROM, save as `image`, on to the Disk step."""
    kind = need(app, "CD-ROM kind", contains=CD_KIND, showing=True)
    click(kind)
    press(button(app, "Next"))
    set_picks(output_image=image, cd_folder=os.path.join(WORK, "cdsrc"))
    press(button(app, "Save As..."))
    need_text(app, os.path.basename(image))
    press(button(app, "Next"))
    need(app, "the Layout row", name="Layout", roles=COMBO_ROLES, showing=True)


def build_it(app, image):
    press(button(app, "Next"))
    press(button(app, "Create Disk"))
    button(app, "Open in Inspect", wait=180.0)
    if not os.path.exists(image):
        raise Fail(f"the build finished but {image} is not there")
    press(button(app, "Build Another"))


def case_cd_options(app):
    """Every option is listed with its help, refused before a folder, and flatten follows unpack."""
    open_build(app)
    to_cd_disk_step(app, os.path.join(WORK, "efs.iso"))
    for label in OPTIONS:
        if is_sensitive(option_row(app, label)):
            raise Fail(f"{label!r} can be changed before a folder is chosen")
    need_text(app, "Not available: Choose a folder to fill the disc from first.")
    log("every option listed and greyed out with a reason")

    press(button(app, "Choose Folder..."))
    need_text(app, "cdsrc")
    if is_sensitive(option_row(app, OPTIONS[1])):
        raise Fail("flatten can be changed with Unpack archives off")
    set_option(app, "Unpack archives", True)
    end = time.time() + 5
    while not is_sensitive(option_row(app, OPTIONS[1])):
        if time.time() > end:
            raise Fail("flatten stayed greyed out with Unpack archives on")
        time.sleep(0.3)
    # A phrase from each option's help, so a dropped subtitle is caught.
    for phrase in HELP_PHRASES:
        need_text(app, phrase)
    sidecars = need(app, SIDECARS, name=SIDECARS, roles=COMBO_ROLES, showing=True)
    if combo_value(sidecars, SIDECAR_CHOICES["efs"]) != "Exclude":
        raise Fail("an EFS disc does not default to Exclude")
    log("options follow the folder and each other; EFS sidecars default to Exclude")


def case_cd_efs_unpack_exclude(app):
    """EFS, archives unpacked, ._ files excluded."""
    image = os.path.join(WORK, "efs.iso")
    set_option(app, "Size to the folder", True)
    set_option(app, "Unpack archives", True)
    combo_choice(app, SIDECARS, SIDECAR_CHOICES["efs"], "Exclude")
    build_it(app, image)
    check_bytes(image, present=[b"hello.txt", b"inner.txt", b"inside"], absent=[b"._hello", b"ustar"])


def case_cd_efs_keep(app):
    """EFS again: archives copied whole, ._ files left as they are."""
    image = os.path.join(WORK, "efs-keep.iso")
    to_cd_disk_step(app, image)
    set_option(app, "Unpack archives", False)
    combo_choice(app, SIDECARS, SIDECAR_CHOICES["efs"], "Leave as-is")
    build_it(app, image)
    check_bytes(image, present=[b"hello.txt", b"._hello", b"ustar"])


def case_cd_hfs_fork(app):
    """Classic Mac: three sidecar choices, no permissions option, ._ taken as forks."""
    image = os.path.join(WORK, "hfs.iso")
    to_cd_disk_step(app, image)
    combo_choice(app, "Layout", LAYOUTS, LAYOUTS[1])
    combo_choice(app, SIDECARS, SIDECAR_CHOICES["mac"], "Use as resource forks")
    if is_sensitive(option_row(app, "Ignore this computer's permissions")):
        raise Fail("an HFS disc offers to ignore permissions")
    build_it(app, image)
    check_bytes(image, present=[b"hello.txt"], absent=[b"._hello"])


CASES = [
    ("browse", case_browse),
    ("cd_options", case_cd_options),
    ("cd_efs_unpack_exclude", case_cd_efs_unpack_exclude),
    ("cd_efs_keep", case_cd_efs_keep),
    ("cd_hfs_fork", case_cd_hfs_fork),
]


def main():
    global WORK, WINDOW
    WORK = sys.argv[1]
    only = set(sys.argv[2:])
    app = app_node("rusty")
    if app is None:
        log("FAIL: the app never appeared on the accessibility bus")
        sys.exit(1)
    log(f"app: {describe(app)}")
    WINDOW = main_window()
    # Tall enough that the Build Disk pages never scroll, so every row has a place to click.
    xdo("windowmove", WINDOW, 0, 0)
    xdo("windowsize", WINDOW, 1600, 2300)
    time.sleep(1.0)
    failed = []
    for name, case in CASES:
        if only and name not in only and name != "browse":
            continue
        start = time.time()
        try:
            case(app)
        except Fail as e:
            log(f"FAIL {name}: {e}")
            for n in nodes(app)[:600]:
                if is_showing(n):
                    log("  " + describe(n))
            failed.append(name)
            # The CD cases build on each other, so one failure stops the rest.
            break
        log(f"ok   {name} ({time.time() - start:.1f}s)")
    if failed:
        log(f"FAILED: {', '.join(failed)}")
        sys.exit(1)
    log("PASS")


if __name__ == "__main__":
    main()
