#!/usr/bin/env python3
"""The GTK driver for rb-regress flows (regression-tests/FLOWS.md).

rb-regress runs this inside `dbus-run-session -- xvfb-run`; it starts the AT-SPI bus and the
GTK shell, prints {"ready": true}, then reads one JSON op per line on stdin and answers each
with {"ok": true} or {"ok": false, "expected": ..., "observed": ...}. Controls arrive already
resolved to GTK labels (rb-regress reads drivers/labels.toml). The AT-SPI helpers follow
scripts/gtk4-gui-suite.py.

Usage: gtk_driver.py <rusty-backup-gtk> <work dir>
"""

import json
import os
import signal
import subprocess
import sys
import time

import gi

gi.require_version("Atspi", "2.0")
from gi.repository import Atspi  # noqa: E402

BUTTON_ROLES = ("push button", "button")
COMBO_ROLES = ("combo box",)
ENTRY_ROLES = ("text", "entry", "password text", "spin button")
PICKS = ""
APP = None
WINDOW = ""
CURRENT = None


class Fail(Exception):
    def __init__(self, expected, observed=""):
        super().__init__(expected)
        self.expected = expected
        self.observed = observed


def nodes(root, limit=40000):
    out, queue, seen = [], [root], set()
    while queue and len(out) < limit:
        n = queue.pop(0)
        if n is None or n in seen:
            continue
        seen.add(n)
        out.append(n)
        try:
            queue.extend(n.get_child_at_index(i) for i in range(n.get_child_count()))
        except Exception:
            pass
    return out


def has_state(n, state):
    try:
        return n.get_state_set().contains(state)
    except Exception:
        return False


def showing(n):
    return has_state(n, Atspi.StateType.SHOWING)


def has_action(n):
    try:
        a = n.get_action_iface()
        return a is not None and a.get_n_actions() > 0
    except Exception:
        return False


def name_of(n):
    try:
        return n.get_name() or ""
    except Exception:
        return ""


def role_of(n):
    try:
        return n.get_role_name()
    except Exception:
        return ""


def visible_text():
    """Every showing node's name and description: the failure bundle's 'screen'."""
    lines = []
    for n in nodes(APP):
        if not showing(n):
            continue
        name, role = name_of(n), role_of(n)
        try:
            desc = n.get_description() or ""
        except Exception:
            desc = ""
        text = ""
        try:
            t = n.get_text_iface()
            if t is not None and role in ENTRY_ROLES:
                text = t.get_text(0, t.get_character_count())
        except Exception:
            pass
        if name or desc or text:
            lines.append(f"{role}: {name}" + (f" -- {desc}" if desc else "")
                         + (f" [{text}]" if text else ""))
    return "\n".join(lines)


# Roles that are controls; a label also has (clipboard) actions, so actions alone don't count.
CONTROL_ROLES = BUTTON_ROLES + COMBO_ROLES + ENTRY_ROLES + (
    "toggle button", "switch", "check box", "radio button", "page tab", "spin button",
    "list item", "menu item", "table cell", "table row", "tree item", "slider")
# Rows carry every column in their name; they match on the first column.
ROW_ROLES = ("table row", "table cell", "tree item", "list item")


def named(n, want):
    nm = name_of(n).rstrip(":").strip()
    if nm == want:
        return True
    return role_of(n) in ROW_ROLES and nm.startswith(want + " ")


def rank(n):
    role = role_of(n)
    if role in CONTROL_ROLES:
        return 0
    if has_action(n) and role not in ("label", "static"):
        return 1
    return 2


def find(label, wait=20.0, pred=None, nth=0):
    """The `nth` showing node named `label` (trailing ':' ignored), a control before a mere label."""
    want = label.rstrip(":").strip()
    end = time.time() + wait
    while True:
        controls, best = [], None
        for n in nodes(APP):
            if not named(n, want) or not showing(n):
                continue
            if pred is not None and not pred(n):
                continue
            r = rank(n)
            if r == 0:
                # A toggle button and its inner button share a name; count each control once.
                if not any(is_inside(n, c) or is_inside(c, n) for c in controls):
                    controls.append(n)
            elif best is None or r < best[0]:
                best = (r, n)
        if len(controls) > nth:
            return controls[nth]
        if nth == 0 and best is not None:
            return best[1]
        if time.time() >= end:
            raise Fail(f"a control labelled {label!r}", visible_text())
        time.sleep(0.4)


def shows(text):
    for n in nodes(APP):
        if not showing(n):
            continue
        try:
            if text in name_of(n) or text in (n.get_description() or ""):
                return True
            t = n.get_text_iface()
            if t is not None and text in t.get_text(0, t.get_character_count()):
                return True
        except Exception:
            continue
    return False


def xdo(*args):
    return subprocess.run(["xdotool", *map(str, args)], check=True, capture_output=True,
                          text=True).stdout


def main_window():
    best = None
    for wid in xdo("search", "--onlyvisible", "--name", ".").split():
        geo = dict(l.strip().split("=", 1) for l in xdo("getwindowgeometry", "--shell", wid)
                   .splitlines() if "=" in l)
        area = int(geo["WIDTH"]) * int(geo["HEIGHT"])
        if best is None or area > best[0]:
            best = (area, wid)
    return best[1] if best else ""


def window_origin():
    geo = dict(l.strip().split("=", 1) for l in xdo("getwindowgeometry", "--shell", WINDOW)
               .splitlines() if "=" in l)
    return int(geo["X"]), int(geo["Y"])


def click(n):
    """A real click; AT-SPI's screen coordinates are wrong under Xvfb, so window ones plus X's origin."""
    comp = n.get_component_iface()
    ext = comp.get_extents(Atspi.CoordType.WINDOW) if comp else None
    if ext is None or ext.width <= 0 or ext.height <= 0:
        raise Fail(f"{label_of(n)} on screen", visible_text())
    ox, oy = window_origin()
    xdo("mousemove", ox + ext.x + ext.width // 2, oy + ext.y + ext.height // 2)
    time.sleep(0.15)
    xdo("click", 1)
    time.sleep(0.4)


def label_of(n):
    return f"{role_of(n)} {name_of(n)!r}"


def activate(n):
    """A button's action; a list row (a popover entry) by focus and Return; else a real click."""
    if role_of(n) in BUTTON_ROLES and has_action(n):
        n.get_action_iface().do_action(0)
        time.sleep(0.4)
        return
    row = row_of(n)
    if row is not None:
        walk_to(row, name_of(n))
        return
    click(n)


def row_of(n):
    """The list row holding `n` (a popover entry), if any."""
    for _ in range(4):
        if n is None or role_of(n) == "list item":
            break
        n = n.get_parent()
    return n if n is not None and role_of(n) == "list item" else None


def walk_to(row, what):
    """GTK 4 has no AT-SPI grab_focus; an open popover holds the keyboard, so walk to the row."""
    for direction in ("Down", "Up"):
        for _ in range(30):
            if has_state(row, Atspi.StateType.FOCUSED) or has_state(row, Atspi.StateType.SELECTED):
                xdo("key", "Return")
                time.sleep(0.4)
                return
            xdo("key", direction)
            time.sleep(0.12)
    raise Fail(f"keyboard focus to reach {what!r}", visible_text())


XKEYS = {"Enter": "Return", "Esc": "Escape", "Backspace": "BackSpace", "Space": "space",
         "PageUp": "Prior", "PageDown": "Next", "BackTab": "shift+Tab"}


def xkey(name):
    if name.startswith("Ctrl+"):
        return "ctrl+" + name[5:].lower()
    return XKEYS.get(name, name)


def set_pick(path):
    with open(PICKS, "w") as f:
        f.write(f"*\t{path}\n")


def combo_value(n, choices=None):
    """The value a combo row shows: a label inside it that is not the row's own title."""
    title = name_of(n)
    for c in nodes(n, limit=200):
        if c is n or not showing(c):
            continue
        nm = name_of(c)
        # Breadth first: the shown value is shallower than the popover's rows.
        if not nm or nm == title:
            continue
        if (choices is None or nm in choices) and role_of(c) in ("label", "text", "static"):
            return nm
    return ""




def choose(n, value):
    """Set a combo row, switch or spin to `value`."""
    role = role_of(n)
    if role in COMBO_ROLES:
        if combo_value(n).casefold() == value.casefold():
            return
        click(n)
        time.sleep(0.5)
        want = value.casefold()
        row = None
        end = time.time() + 5
        while row is None and time.time() < end:
            for c in nodes(APP):
                if c is not n and showing(c) and name_of(c).casefold() == want \
                        and not is_inside(c, n) and row_of(c) is not None:
                    row = row_of(c)
                    break
            time.sleep(0.2)
        if row is not None:
            # A drop-down may move its value as the keyboard moves, so either sign means "there".
            here = lambda: (has_state(row, Atspi.StateType.FOCUSED)
                            or has_state(row, Atspi.StateType.SELECTED)
                            or combo_value(n).casefold() == want)
            for direction in ("Down", "Up"):
                for _ in range(30):
                    if here():
                        xdo("key", "Return")
                        time.sleep(0.4)
                        if wait_until(lambda: combo_value(n).casefold() == want, 3):
                            return
                        break
                    xdo("key", direction)
                    time.sleep(0.12)
        raise Fail(f"{name_of(n)!r} set to {value!r}", visible_text())
    if value in ("on", "off"):
        on = has_state(n, Atspi.StateType.CHECKED) or has_state(n, Atspi.StateType.PRESSED)
        if on != (value == "on"):
            activate(n)
        return
    raise Fail(f"{label_of(n)} to take a choice", visible_text())


def is_inside(c, n):
    """`c` is drawn in `n` itself (the combo's current value), not in its popover list."""
    p = c.get_parent()
    for _ in range(3):
        if p is None:
            return False
        if p == n:
            return True
        p = p.get_parent()
    return False


def shows_value(n, value):
    for c in nodes(n, limit=200):
        if c is not n and showing(c) and name_of(c).casefold() == value.casefold():
            return True
    return False


def fill(n, text, pick=None):
    """Type into an entry; a path row filled through a dialog gets the dialog's answer."""
    if pick:
        set_pick(text)
        activate(find(pick))
        if not wait_until(lambda: shows(os.path.basename(text.rstrip("/"))), 10):
            raise Fail(f"{text!r} shown after the dialog", visible_text())
        return
    target = n
    if role_of(n) not in ENTRY_ROLES:
        for c in nodes(n, limit=100):
            if role_of(c) in ENTRY_ROLES:
                target = c
                break
    if role_of(target) == "spin button":
        # Its Value interface sets the number; a click to commit typed text can land on "-".
        val = target.get_value_iface()
        if val is not None and val.set_current_value(float(text)):
            if wait_until(lambda: abs(val.get_current_value() - float(text)) < 1e-6, 2):
                return
        raise Fail(f"spin button set to {text}", visible_text())
    et = target.get_editable_text_iface() if target else None
    if et is not None and et.set_text_contents(text):
        time.sleep(0.3)
        return
    click(target)
    xdo("key", "ctrl+a")
    xdo("type", "--delay", "5", text)
    time.sleep(0.3)


def field_beside(label):
    """The unnamed field a caption labels: its next sibling that takes input."""
    parent = label.get_parent()
    if parent is None:
        return None
    seen = False
    for i in range(parent.get_child_count()):
        c = parent.get_child_at_index(i)
        if c == label:
            seen = True
        elif seen and role_of(c) in ENTRY_ROLES + COMBO_ROLES + ("switch", "check box"):
            return c
    return None


def wait_until(pred, secs):
    end = time.time() + secs
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.4)
    return pred()


def run(op):
    global CURRENT
    kind, arg = op["op"], op.get("arg", "")
    limit = op.get("timeout_ms", 30000) / 1000.0
    if kind == "tab":
        activate(find(arg))
    elif kind == "focus":
        CURRENT = find(arg, nth=op.get("nth", 0))
        if role_of(CURRENT) == "label":
            CURRENT = field_beside(CURRENT) or CURRENT
        if role_of(CURRENT) in ROW_ROLES and row_of(CURRENT) is None:
            click(CURRENT)  # select a row in a list or table; popover rows are walked instead
    elif kind == "choose":
        choose(CURRENT or find(op["label"]), arg)
    elif kind == "activate":
        nth = op.get("nth", 0)
        if op.get("via"):
            # Some menu buttons ignore the AT-SPI action; a dropdown is in the main window, so click it.
            click(find(op["via"], nth=nth))
            time.sleep(0.4)
            nth = 0
        activate(find(arg, nth=nth))
    elif kind == "pick":
        set_pick(arg)
    elif kind == "key":
        if arg == "Enter" and CURRENT is not None and role_of(CURRENT) in BUTTON_ROLES:
            activate(CURRENT)
        else:
            xdo("key", xkey(arg))
            time.sleep(0.2)
    elif kind == "keys":
        for k in arg.split():
            xdo("key", xkey(k))
            time.sleep(0.15)
    elif kind == "type":
        xdo("type", "--delay", "5", arg)
    elif kind == "fill":
        fill(CURRENT, arg, op.get("pick"))
    elif kind == "wait":
        if not wait_until(lambda: shows(arg), limit):
            raise Fail(f"the window shows {arg!r}", visible_text())
    elif kind == "expect":
        if not wait_until(lambda: shows(arg), 2):
            raise Fail(f"the window shows {arg!r}", visible_text())
    elif kind == "expect_not":
        time.sleep(0.5)
        if shows(arg):
            raise Fail(f"the window without {arg!r}", visible_text())
    elif kind == "ancestors":
        n = find(arg)
        out = []
        while n is not None and len(out) < 8:
            acts = []
            if has_action(n):
                ai = n.get_action_iface()
                acts = [ai.get_action_name(i) for i in range(ai.get_n_actions())]
            out.append(f"{role_of(n)} {name_of(n)[:40]!r} {acts}")
            n = n.get_parent()
        return {"ok": True, "text": "\n".join(out)}
    elif kind == "click":
        click(find(arg))
    elif kind == "tree":
        out = []

        def walk(n, depth):
            if n is None or depth > 14 or len(out) > 300:
                return
            flags = [Atspi.StateType(st).value_nick for st in n.get_state_set().get_states()
                     if Atspi.StateType(st).value_nick in ("showing", "focused", "selected",
                                                           "checked", "expanded")]
            out.append("  " * depth + f"{role_of(n)} {name_of(n)[:50]!r} {flags}")
            for i in range(n.get_child_count()):
                walk(n.get_child_at_index(i), depth + 1)

        walk(find(arg), 0)
        return {"ok": True, "text": "\n".join(out)}
    elif kind == "text":
        return {"ok": True, "text": visible_text()}
    else:
        raise Fail(f"a known op, not {kind!r}")
    return {"ok": True}


def reply(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def main():
    global PICKS, APP, WINDOW
    binary, work = sys.argv[1], sys.argv[2]
    home = os.path.join(work, "home")
    os.makedirs(home, exist_ok=True)
    for d in (".config/rusty-backup",):
        os.makedirs(os.path.join(home, d), exist_ok=True)
        with open(os.path.join(home, d, "config.json"), "w") as f:
            f.write('{"update_check":{"enabled":false,"repository_url":""}}')
    PICKS = os.path.join(work, "picks.tsv")
    open(PICKS, "w").close()
    # Its daemons must not inherit stdout, or the pipe to rb-regress outlives the driver.
    bus = subprocess.Popen(["/usr/libexec/at-spi-bus-launcher", "--launch-immediately"],
                           stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, start_new_session=True)
    time.sleep(1)
    env = dict(os.environ, HOME=home, XDG_CONFIG_HOME=os.path.join(home, ".config"),
               GTK_A11Y="atspi", GDK_BACKEND="x11", GDK_DEBUG="no-portals", NO_AT_BRIDGE="0",
               RUSTY_BACKUP_TEST_PICKS=PICKS)
    log = open(os.path.join(work, "gtk-app.log"), "w")
    app = subprocess.Popen([binary], env=env, stdin=subprocess.DEVNULL, stdout=log,
                           stderr=subprocess.STDOUT, start_new_session=True)
    try:
        end = time.time() + 60
        while time.time() < end and APP is None:
            desk = Atspi.get_desktop(0)
            for i in range(desk.get_child_count()):
                a = desk.get_child_at_index(i)
                if a is not None and "rusty" in name_of(a).lower():
                    APP = a
            time.sleep(0.5)
        if APP is None:
            reply({"ready": False, "why": "the GTK shell never appeared on the AT-SPI bus"})
            return
        WINDOW = main_window()
        if WINDOW:
            xdo("windowsize", WINDOW, 1600, 2200)
            xdo("windowmove", WINDOW, 0, 0)
            time.sleep(0.5)
        reply({"ready": True})
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                reply(run(json.loads(line)))
            except Fail as f:
                reply({"ok": False, "expected": f.expected, "observed": f.observed})
            except Exception as e:  # A vanished node or a failed xdotool is a failed op.
                reply({"ok": False, "expected": "the op to complete",
                       "observed": f"{type(e).__name__}: {e}\n{visible_text()}"})
            if app.poll() is not None:
                reply({"ok": False, "expected": "the GTK shell running", "observed": "it exited"})
                return
    finally:
        # Each child leads its own group; the bus launcher's dbus-daemon would outlive a plain kill.
        for p in (app, bus):
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except OSError:
                pass


if __name__ == "__main__":
    main()
