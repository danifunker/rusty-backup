"""The Win32 shell's flow driver: UI Automation over comtypes, one JSON op per line (FLOWS.md).

Run by runner/src/uia.rs as `python -I uia_driver.py <rusty-backup-win32.exe> <work dir>`. It starts
the shell with a scratch config and a picks file, answers {"ready": true}, then reads ops from stdin
and answers {"ok": true} or {"ok": false, "expected": ..., "observed": ...} for each.

Controls are found by AutomationId, which the shell sets to the controller id (Gate 0), so this
driver needs no labels.toml section. A name that is no id (a file in a list, a tab title, a
message) is matched against element names and values instead. Acting goes through UIA patterns:
no synthetic mouse clicks.
"""

import ctypes
import json
import os
import subprocess
import sys
import time

import comtypes
import comtypes.client

comtypes.client.GetModule("UIAutomationCore.dll")
from comtypes.gen import UIAutomationClient as U  # noqa: E402

UIA = None
WINDOW = None
APP = None
PICKS = None
CURRENT = None


class Fail(Exception):
    def __init__(self, expected, observed=""):
        super().__init__(expected)
        self.expected = expected
        self.observed = observed


def cond(prop, value):
    return UIA.CreatePropertyCondition(prop, value)


def elements(scope_cond=None):
    """Every element under the window, in tree order."""
    c = scope_cond or UIA.CreateTrueCondition()
    arr = WINDOW.FindAll(U.TreeScope_Descendants, c)
    return [arr.GetElement(i) for i in range(arr.Length)]


def value_of(e):
    try:
        p = e.GetCurrentPattern(U.UIA_ValuePatternId)
        if p:
            return p.QueryInterface(U.IUIAutomationValuePattern).CurrentValue or ""
    except comtypes.COMError:
        pass
    return ""


def name_of(e):
    try:
        return e.CurrentName or ""
    except comtypes.COMError:
        return ""


def showing(e):
    try:
        return not e.CurrentIsOffscreen
    except comtypes.COMError:
        return False


def find(id_or_name, label=None, nth=0, secs=10):
    """By AutomationId first, then by a showing element's name (the label the runner resolved)."""
    end = time.time() + secs
    while True:
        hits = [e for e in elements(cond(U.UIA_AutomationIdPropertyId, id_or_name)) if showing(e)]
        if not hits:
            for want in (label, id_or_name):
                if want:
                    hits = [e for e in elements() if showing(e) and name_of(e) == want]
                    if not hits:
                        hits = [e for e in elements() if showing(e) and want in name_of(e)]
                if hits:
                    break
        if len(hits) > nth:
            return hits[nth]
        if time.time() >= end:
            raise Fail(f"a control {id_or_name!r}" + (f" ({label!r})" if label else ""), visible_text())
        time.sleep(0.2)


def pattern(e, pid, iface):
    p = e.GetCurrentPattern(pid)
    return p.QueryInterface(iface) if p else None


def activate(e):
    for pid, iface, call in (
        (U.UIA_InvokePatternId, U.IUIAutomationInvokePattern, lambda p: p.Invoke()),
        (U.UIA_SelectionItemPatternId, U.IUIAutomationSelectionItemPattern, lambda p: p.Select()),
        (U.UIA_TogglePatternId, U.IUIAutomationTogglePattern, lambda p: p.Toggle()),
        (U.UIA_ExpandCollapsePatternId, U.IUIAutomationExpandCollapsePattern, lambda p: p.Expand()),
    ):
        p = pattern(e, pid, iface)
        if p:
            call(p)
            time.sleep(0.2)
            return
    raise Fail(f"{name_of(e)!r} to take an action", visible_text())


def choose(e, value):
    if e is None:
        raise Fail("a focused control to choose on")
    want = value.lower()
    toggle = pattern(e, U.UIA_TogglePatternId, U.IUIAutomationTogglePattern)
    if toggle and want in ("on", "off"):
        if (toggle.CurrentToggleState == 1) != (want == "on"):
            toggle.Toggle()
        return
    exp = pattern(e, U.UIA_ExpandCollapsePatternId, U.IUIAutomationExpandCollapsePattern)
    if exp:
        exp.Expand()
        time.sleep(0.3)
    items = e.FindAll(U.TreeScope_Descendants, cond(U.UIA_ControlTypePropertyId, U.UIA_ListItemControlTypeId))
    for i in range(items.Length):
        it = items.GetElement(i)
        if name_of(it).lower() == want or want in name_of(it).lower():
            pattern(it, U.UIA_SelectionItemPatternId, U.IUIAutomationSelectionItemPattern).Select()
            if exp:
                exp.Collapse()
            time.sleep(0.2)
            return
    raise Fail(f"a choice {value!r} on {name_of(e)!r}", visible_text())


def fill(e, text):
    if e is None:
        raise Fail("a focused field to fill")
    v = pattern(e, U.UIA_ValuePatternId, U.IUIAutomationValuePattern)
    if not v:
        raise Fail(f"{name_of(e)!r} to take text", visible_text())
    v.SetValue(text)
    time.sleep(0.2)


def visible_text():
    out = []
    try:
        all_elements = elements()
    except (comtypes.COMError, ValueError) as e:
        return f"(the window cannot be read: {e})"
    for e in all_elements:
        if not showing(e):
            continue
        n, v = name_of(e), value_of(e)
        aid = ""
        try:
            aid = e.CurrentAutomationId or ""
        except comtypes.COMError:
            pass
        line = " ".join(x for x in (f"[{aid}]" if aid else "", n, f"= {v}" if v and v != n else "") if x)
        if line:
            out.append(line[:300])
    return "\n".join(out)


def shows(text):
    for e in elements():
        if showing(e) and (text in name_of(e) or text in value_of(e)):
            return True
    return False


def wait_until(pred, secs):
    end = time.time() + secs
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.3)
    return pred()


VK = {"Enter": 0x0D, "Tab": 0x09, "Esc": 0x1B, "Escape": 0x1B, "Up": 0x26, "Down": 0x28,
      "Left": 0x25, "Right": 0x27, "Space": 0x20, "Delete": 0x2E, "Backspace": 0x08,
      "Home": 0x24, "End": 0x23, "Insert": 0x2D}
VK.update({f"F{i}": 0x6F + i for i in range(1, 13)})


def press(name):
    user32 = ctypes.windll.user32
    user32.SetForegroundWindow(WINDOW.CurrentNativeWindowHandle)
    mods = []
    while "+" in name and len(name) > 1:
        m, name = name.split("+", 1)
        mods.append({"Ctrl": 0x11, "Shift": 0x10, "Alt": 0x12}[m])
    vk = VK.get(name) or (ord(name.upper()) if len(name) == 1 else None)
    if vk is None:
        raise Fail(f"a known key, not {name!r}")
    for m in mods:
        user32.keybd_event(m, 0, 0, 0)
    user32.keybd_event(vk, 0, 0, 0)
    user32.keybd_event(vk, 0, 2, 0)
    for m in reversed(mods):
        user32.keybd_event(m, 0, 2, 0)
    time.sleep(0.15)


def set_pick(path):
    with open(PICKS, "w", encoding="utf-8") as f:
        f.write(f"*\t{path}\n")


def run(op):
    global CURRENT
    kind, arg = op["op"], op.get("arg", "")
    ident = op.get("id", arg)
    limit = op.get("timeout_ms", 30000) / 1000.0
    if kind == "tab":
        # A screen is a button on the screen toolbar (D9), found by its label.
        activate(find(arg, arg))
        time.sleep(0.2)
    elif kind == "focus":
        CURRENT = find(ident, arg, nth=op.get("nth", 0))
        try:
            CURRENT.SetFocus()
        except comtypes.COMError:
            pass
    elif kind == "choose":
        choose(CURRENT, arg)
    elif kind == "activate":
        activate(find(ident, arg, nth=op.get("nth", 0)))
    elif kind == "pick":
        set_pick(arg)
    elif kind == "key":
        press(arg)
    elif kind == "keys":
        for k in arg.split():
            press(k)
    elif kind == "type":
        for ch in arg:
            press(ch)
    elif kind == "fill":
        fill(CURRENT, arg)
    elif kind == "wait":
        if not wait_until(lambda: shows(arg), limit):
            raise Fail(f"the window shows {arg!r}", visible_text())
    elif kind == "wait_gone":
        if not wait_until(lambda: not shows(arg), limit):
            raise Fail(f"the window without {arg!r}", visible_text())
    elif kind == "expect":
        if not wait_until(lambda: shows(arg), 2):
            raise Fail(f"the window shows {arg!r}", visible_text())
    elif kind == "expect_not":
        time.sleep(0.5)
        if shows(arg):
            raise Fail(f"the window without {arg!r}", visible_text())
    elif kind == "text":
        return {"ok": True, "text": visible_text()}
    else:
        raise Fail(f"a known op, not {kind!r}")
    return {"ok": True}


def reply(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def main():
    global UIA, WINDOW, APP, PICKS
    binary, work = sys.argv[1], sys.argv[2]
    home = os.path.join(work, "home")
    config = os.path.join(home, ".config", "rusty-backup")
    os.makedirs(config, exist_ok=True)
    with open(os.path.join(config, "config.json"), "w") as f:
        f.write('{"update_check":{"enabled":false,"repository_url":""}}')
    PICKS = os.path.join(work, "picks.tsv")
    open(PICKS, "w").close()
    env = dict(os.environ, RUSTY_BACKUP_CONFIG_DIR=config, RUSTY_BACKUP_TEST_PICKS=PICKS,
               APPDATA=os.path.join(home, "AppData", "Roaming"),
               LOCALAPPDATA=os.path.join(home, "AppData", "Local"))
    log = open(os.path.join(work, "win32-app.log"), "w")
    APP = subprocess.Popen([binary], env=env, cwd=work, stdin=subprocess.DEVNULL, stdout=log,
                           stderr=subprocess.STDOUT)
    try:
        UIA = comtypes.client.CreateObject(U.CUIAutomation, interface=U.IUIAutomation)
        root = UIA.GetRootElement()
        end = time.time() + 60
        # FindFirst answers a NULL pointer, not None, while nothing matches.
        while time.time() < end and not WINDOW:
            WINDOW = root.FindFirst(U.TreeScope_Children, cond(U.UIA_ProcessIdPropertyId, APP.pid))
            if not WINDOW:
                time.sleep(0.3)
        if not WINDOW:
            reply({"ready": False, "why": "the Win32 shell's window never appeared"})
            return
        reply({"ready": True})
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                reply(run(json.loads(line)))
            except Fail as f:
                reply({"ok": False, "expected": f.expected, "observed": f.observed})
            except Exception as e:  # A vanished element is a failed op, not a dead driver.
                reply({"ok": False, "expected": "the op to complete",
                       "observed": f"{type(e).__name__}: {e}\n{visible_text()}"})
            if APP.poll() is not None:
                reply({"ok": False, "expected": "the Win32 shell running", "observed": "it exited"})
                return
    finally:
        if APP.poll() is None:
            APP.kill()


if __name__ == "__main__":
    main()
