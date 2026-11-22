#!/usr/bin/env python3
"""Run the tests a change touched, and only those.

CONTRIBUTING.md § Regression asks every change to run "the tests for what it
touched" and a milestone to run everything. This script turns the first half
into one command: it reads the changed paths from git, looks each one up in
regression-tests/data/change-map.tsv (plus a few automatic rules), and runs the
union of what they need, one cargo invocation per kind so each builds once.

    scripts/regress-changed.py                 # change level, against main
    scripts/regress-changed.py --plan          # print the commands, run nothing
    scripts/regress-changed.py --base HEAD~3   # diff against another base
    scripts/regress-changed.py --files a b     # these paths instead of git's
    scripts/regress-changed.py --level batch   # preflight + every case, no emulators
    scripts/regress-changed.py --level milestone

Levels:
    change     the selected tests (default). A path too central to select from
               (Cargo.toml, disk-core, src/fs/mod.rs, ...) or one nothing maps
               escalates the run to batch, and says which path did it.
    batch      scripts/preflight.sh, check-boundaries, then regress-local.sh
               --skip-emulators: what to run once per batch of fixes.
    milestone  regress-local.sh in full, then a reminder of the other OS legs.

Exit: 0 everything passed, 1 something failed, 2 the script itself could not run.
Python 3.9, standard library only.
"""

import argparse
import fnmatch
import os
import platform
import re
import shutil
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Integration-test targets of their own; every other `test:` name is a module of `integration`.
STANDALONE_TESTS = {"doc_parity", "ui_scenarios"}
MAP_PATH = os.path.join(REPO, "regression-tests", "data", "change-map.tsv")
EMU_CASES = os.path.join(REPO, "regression-tests", "emulator", "cases.txt")
RUNNER = os.path.join(REPO, "regression-tests", "runner")
RB_REGRESS = os.path.join(RUNNER, "target", "release", "rb-regress")
RB_CLI = os.path.join(REPO, "target", "release", "rb-cli")
ROOT_PKG = "rusty-backup"

# std APIs newer than Rust 1.73 that engine code must not call directly.
# (pattern, why, a pattern whose presence in the file makes the hit fine)
POST_173 = [
    (r"io::Error::other\(", "io::Error::other is 1.74: use crate::compat::io_other", None),
    (r"(?<!compat::)\brepeat_n\(", "iter::repeat_n is 1.82: use crate::compat::repeat_n", None),
    (r"\.is_multiple_of\(", "is_multiple_of is 1.87: import the rust173_compat trait",
     r"rust173_compat|rust173::"),
    (r"\.is_none_or\(", "is_none_or is 1.82: import the rust173_compat trait",
     r"rust173_compat|rust173::"),
    (r"\bLazyLock\b", "LazyLock is 1.80", None),
    (r"\.inspect_err\(", "inspect_err is 1.76", None),
    (r"\.is_sorted\(", "is_sorted is 1.82", None),
    (r"#\[expect\(", "#[expect] is 1.81", None),
    (r"\.split_at_checked\(", "split_at_checked is 1.80", None),
]


class Plan:
    """What one run needs, accumulated from every changed path."""

    def __init__(self):
        self.crates = set()       # cargo test -p
        self.msrv = set()         # scripts/test-crates-msrv.sh
        self.lib = set()          # cargo test --lib filters
        self.tests = set()        # cargo test --test targets
        self.cases = set()        # rb-regress --filter substrings
        self.emu = set()          # regress-emulators.sh substrings
        self.fronts = set()
        self.checks = set()
        self.wide = []            # (path, reason) that force the batch level
        self.reasons = {}         # action -> first path that asked for it

    def add(self, action, path):
        self.reasons.setdefault(action, path)
        kind, _, arg = action.partition(":")
        if kind == "case":
            self.cases.update(s for s in arg.split(",") if s)
        elif kind == "emu":
            self.emu.add(arg)
        elif kind == "lib":
            self.lib.add(arg)
        elif kind == "test":
            self.tests.add(arg)
        elif kind == "front":
            self.fronts.add(arg)
        elif kind == "check":
            self.checks.add(arg)
        elif kind == "crate":
            self.crates.add(arg)
        elif kind == "msrv":
            self.msrv.add(arg)
        elif kind == "wide":
            self.wide.append((path, "change-map marks it wide"))
        elif kind != "skip":
            raise ValueError("unknown action %r for %s" % (action, path))


def load_map(path=MAP_PATH):
    rules = []
    with open(path, encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            if "\t" not in line:
                raise ValueError("%s:%d: expected <glob><TAB><actions>" % (path, n))
            glob, actions = line.split("\t", 1)
            rules.append((glob.strip(), actions.split()))
    return rules


def module_filter(path):
    """src/controller/restore/mod.rs -> controller::restore (a libtest name filter)."""
    rel = path[len("src/"):]
    if rel.endswith(".rs"):
        rel = rel[:-3]
    parts = [p for p in rel.split("/") if p]
    if parts and parts[-1] == "mod":
        parts.pop()
    return "::".join(parts)


def case_ids(path):
    """Every case ID a manifest declares; a deleted manifest declares none."""
    full = os.path.join(REPO, path)
    if not os.path.isfile(full):
        return []
    with open(full, encoding="utf-8") as f:
        return re.findall(r'^\s*id\s*=\s*"([^"]+)"', f.read(), re.M)


def emulator_ids(script):
    """Scenario IDs in cases.txt that run this script; all of them for a shared one."""
    ids, mine = [], []
    if not os.path.isfile(EMU_CASES):
        return []
    with open(EMU_CASES, encoding="utf-8") as f:
        for line in f:
            cols = line.split()
            if len(cols) >= 3 and not line.lstrip().startswith("#"):
                ids.append(cols[0])
                if cols[2] == script:
                    mine.append(cols[0])
    return mine if mine else ids


def automatic(path, plan):
    """Rules that need no map row. Returns True when the path is handled."""
    m = re.match(r"crates/([^/]+)/", path)
    if m:
        plan.add("crate:" + m.group(1), path)
        plan.add("msrv:" + m.group(1), path)
        # App-side tests of moved code: dispatch and detection over the crates.
        plan.add("lib:moved_tests", path)
        return True
    if path.startswith("src/") and path.endswith(".rs"):
        plan.add("lib:" + module_filter(path), path)
        return True
    # Most integration tests are modules of the one `integration` binary; a module name selects them.
    m = re.match(r"tests/integration/([^/]+?)(?:\.rs$|/)", path)
    if m:
        plan.add("test:" + ("integration" if m.group(1) == "main" else m.group(1)), path)
        return True
    m = re.match(r"tests/([^/]+?)(?:\.rs$|/)", path)
    if m and m.group(1) not in ("fixtures", "ui_scenarios"):
        plan.add("test:" + m.group(1), path)
        return True
    if path.startswith("regression-tests/cases/") and path.endswith(".toml"):
        for cid in case_ids(path):
            plan.add("case:" + cid, path)
        plan.add("check:runner", path)
        return True
    m = re.match(r"regression-tests/emulator/([^/]+\.py)$", path)
    if m:
        for sid in emulator_ids(m.group(1)):
            plan.add("emu:" + sid, path)
        return True
    return False


def build_plan(paths, rules):
    plan = Plan()
    for path in paths:
        handled = automatic(path, plan)
        for glob, actions in rules:
            if fnmatch.fnmatchcase(path, glob):
                handled = True
                for a in actions:
                    plan.add(a, path)
        if not handled:
            plan.wide.append((path, "no change-map row and no automatic rule"))
    return plan


def git(*args):
    out = subprocess.run(["git"] + list(args), cwd=REPO, check=True,
                         stdout=subprocess.PIPE, universal_newlines=True).stdout
    return [l for l in out.splitlines() if l]


def default_base():
    for ref in ("origin/main", "main"):
        try:
            return git("merge-base", "HEAD", ref)[0]
        except (subprocess.CalledProcessError, IndexError):
            continue
    raise SystemExit("regress-changed: no main or origin/main to diff against; pass --base")


def changed_paths(base):
    paths = set(git("diff", "--name-only", base))
    paths.update(git("ls-files", "--others", "--exclude-standard"))
    return sorted(paths)


def added_lines(base, paths):
    """(path, line) for every line the change adds in .rs files under src/ or crates/."""
    rs = [p for p in paths if p.endswith(".rs") and (p.startswith("src/") or p.startswith("crates/"))]
    out = []
    if not rs:
        return out
    tracked = set(git("ls-files", "--", *rs))
    diff = subprocess.run(["git", "diff", "-U0", base, "--"] + sorted(tracked), cwd=REPO,
                          stdout=subprocess.PIPE, universal_newlines=True).stdout
    cur = None
    for line in diff.splitlines():
        if line.startswith("+++ b/"):
            cur = line[6:]
        elif line.startswith("+") and not line.startswith("+++") and cur:
            out.append((cur, line[1:]))
    for p in rs:
        if p not in tracked and os.path.isfile(os.path.join(REPO, p)):
            with open(os.path.join(REPO, p), encoding="utf-8", errors="replace") as f:
                out.extend((p, l.rstrip("\n")) for l in f)
    return out


def lint_post_173(lines):
    """Warnings for post-1.73 std APIs on added lines. A modern build cannot see these."""
    cache, hits = {}, []
    for path, line in lines:
        if line.lstrip().startswith("//"):
            continue
        for pat, why, ok_if in POST_173:
            if not re.search(pat, line):
                continue
            if ok_if:
                if path not in cache:
                    try:
                        with open(os.path.join(REPO, path), encoding="utf-8", errors="replace") as f:
                            cache[path] = f.read()
                    except OSError:
                        cache[path] = ""
                if re.search(ok_if, cache[path]):
                    continue
            hits.append("%s: %s\n    %s" % (path, why, line.strip()))
    return hits


def host_can_build(front):
    system = platform.system()
    if front == "appkit":
        return system == "Darwin", "AppKit builds on macOS only"
    if front == "win32":
        return system == "Windows", "Win32 builds on Windows only"
    if front == "gtk4":
        ok = shutil.which("pkg-config") and subprocess.run(
            ["pkg-config", "--exists", "gtk4"]).returncode == 0
        return bool(ok), "no gtk4 development files (pkg-config gtk4)"
    return True, ""


def commands(plan, opts):
    """The ordered (label, argv) list for the change level: cheap checks first."""
    cmds, notes = [], []
    if "boundaries" in plan.checks:
        cmds.append(("layer boundaries", ["scripts/check-boundaries.sh"]))
    if "matrix" in plan.checks:
        cmds.append(("platform matrix", ["python3", "scripts/check-platform-matrix.py"]))
    if "docparity" in plan.checks:
        plan.tests.add("doc_parity")
    if "runner" in plan.checks:
        cmds.append(("rb-regress tests", ["cargo", "test", "--release", "--manifest-path",
                                          "regression-tests/runner/Cargo.toml"]))
        cmds.append(("rb-regress clippy", ["cargo", "clippy", "--manifest-path",
                                           "regression-tests/runner/Cargo.toml", "--all-targets",
                                           "--", "-D", "warnings"]))
    if plan.crates:
        argv = ["cargo", "test"]
        for c in sorted(plan.crates):
            argv += ["-p", c]
        cmds.append(("crate tests", argv))
    if plan.msrv and not opts.no_msrv:
        cmds.append(("crate tests on Rust 1.73", ["scripts/test-crates-msrv.sh"] + sorted(plan.msrv)))
    if plan.lib:
        filters = sorted(plan.lib)
        if "" in filters:
            filters = []
        cmds.append(("app unit tests", ["cargo", "test", "-p", ROOT_PKG, "--lib", "--"] + filters))
    if plan.tests:
        # `doc_parity` and `ui_scenarios` are targets of their own; every other name is a module
        # of the `integration` binary, selected by a `name::` filter (`integration` = all of it).
        own = sorted(t for t in plan.tests if t in STANDALONE_TESTS)
        mods = sorted(t for t in plan.tests if t not in STANDALONE_TESTS)
        if own:
            argv = ["cargo", "test", "-p", ROOT_PKG]
            for t in own:
                argv += ["--test", t]
            cmds.append(("app integration tests", argv))
        if mods:
            argv = ["cargo", "test", "-p", ROOT_PKG, "--test", "integration"]
            if "integration" not in mods:
                argv += ["--"] + [m + "::" for m in mods]
            cmds.append(("app integration tests (integration binary)", argv))
    for front in sorted(plan.fronts):
        if front == "egui":
            cmds.append(("egui shell compiles", ["cargo", "check", "--features", "gui"]))
            continue
        ok, why = host_can_build(front)
        if ok:
            cmds.append(("%s front-end tests" % front,
                         ["cargo", "test", "--manifest-path", "frontends/%s/Cargo.toml" % front]))
        else:
            notes.append("not run: %s front-end tests (%s); CI's %s leg covers it" % (front, why, front))
    if plan.cases and not opts.no_cases:
        cmds.append(("build rb-cli (release)", ["cargo", "build", "--release", "--bin", "rb-cli"]))
        cmds.append(("build rb-regress", ["cargo", "build", "--release", "--manifest-path",
                                          "regression-tests/runner/Cargo.toml"]))
        cmds.append(("regression cases", [RB_REGRESS, "run", "--rb-cli", RB_CLI,
                                          "--filter", ",".join(sorted(plan.cases))]))
    if plan.emu and not opts.no_emulators:
        if not (plan.cases and not opts.no_cases):
            cmds.append(("build rb-cli (release)", ["cargo", "build", "--release", "--bin", "rb-cli"]))
        for sub in sorted(plan.emu):
            cmds.append(("emulator %s" % (sub or "(all)"),
                         ["regression-tests/scripts/regress-emulators.sh"] + ([sub] if sub else [])))
    elif plan.emu:
        notes.append("not run (--no-emulators): emulator scenarios matching %s" % ", ".join(sorted(plan.emu)))
    return cmds, notes


def level_commands(level):
    if level == "batch":
        return [("preflight (what CI runs)", ["scripts/preflight.sh"]),
                ("layer boundaries", ["scripts/check-boundaries.sh"]),
                ("every case + oracles", ["regression-tests/scripts/regress-local.sh", "--skip-emulators"])], []
    return [("full local regression", ["regression-tests/scripts/regress-local.sh"])], [
        "a milestone also needs the Windows and Linux legs and rb-regress parity / verify "
        "across all three (regression-tests/scripts/regress-all.sh, RUNBOOK.md)"]


def describe(plan, paths):
    print("regress-changed: %d changed path(s)" % len(paths))
    for label, items in (("crates", plan.crates), ("lib filters", plan.lib), ("test targets", plan.tests),
                         ("case filters", plan.cases), ("emulator", plan.emu),
                         ("front-ends", plan.fronts), ("checks", plan.checks)):
        if items:
            print("  %-13s %s" % (label + ":", ", ".join(sorted(i or "(all)" for i in items))))


def run_all(cmds, keep_going):
    results = []
    for label, argv in cmds:
        print("\n=== %s\n    %s" % (label, " ".join(argv)), flush=True)
        t = time.time()
        # Shell scripts go through bash: not every one in scripts/ carries the execute bit.
        run_argv = ["bash"] + argv if argv[0].endswith(".sh") else argv
        try:
            code = subprocess.run(run_argv, cwd=REPO).returncode
        except OSError as e:
            print("    could not start: %s" % e)
            code = 127
        results.append((label, code, time.time() - t))
        if code != 0 and not keep_going:
            break
    print("\n=== summary")
    for label, code, secs in results:
        print("  %-6s %6.1fs  %s" % ("ok" if code == 0 else "FAIL", secs, label))
    skipped = len(cmds) - len(results)
    if skipped:
        print("  (%d step(s) not run after the first failure; --keep-going runs them)" % skipped)
    return 0 if all(c == 0 for _, c, _ in results) and not skipped else 1


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--level", choices=("change", "batch", "milestone"), default="change")
    ap.add_argument("--base", help="git ref to diff against [default: merge-base with main]")
    ap.add_argument("--files", nargs="+", help="use these repo-relative paths, not git's")
    ap.add_argument("--plan", action="store_true", help="print the commands, run nothing")
    ap.add_argument("--no-cases", action="store_true", help="skip the rb-regress cases (and their release build)")
    ap.add_argument("--no-emulators", action="store_true", help="skip emulator scenarios")
    ap.add_argument("--no-msrv", action="store_true", help="skip the crates' Rust 1.73 run")
    ap.add_argument("--keep-going", action="store_true", help="run every step even after a failure")
    opts = ap.parse_args(argv)

    notes, lint = [], []
    if opts.level == "change":
        base = None if opts.files else (opts.base or default_base())
        paths = sorted(opts.files) if opts.files else changed_paths(base)
        if not paths:
            print("regress-changed: nothing changed against %s" % base)
            return 0
        plan = build_plan(paths, load_map())
        describe(plan, paths)
        if base:
            lint = lint_post_173(added_lines(base, paths))
        if plan.wide:
            print("\nescalating to the batch level:")
            for path, why in plan.wide:
                print("  %s (%s)" % (path, why))
            cmds, notes = level_commands("batch")
        else:
            cmds, notes = commands(plan, opts)
    else:
        cmds, notes = level_commands(opts.level)

    for hit in lint:
        print("\nwarning: Rust 1.73 floor: " + hit)
    for n in notes:
        print("note: " + n)
    if not cmds:
        print("regress-changed: the change needs no tests")
        return 0
    if opts.plan:
        print("\nwould run:")
        for label, argv in cmds:
            print("  %-26s %s" % (label, " ".join(argv)))
        return 0
    return run_all(cmds, opts.keep_going)


if __name__ == "__main__":
    sys.exit(main())
