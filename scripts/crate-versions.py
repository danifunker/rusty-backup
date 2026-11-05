#!/usr/bin/env python3
"""Report and change the versions of the crates under crates/ and the requirements between them.

    scripts/crate-versions.py                      # status: local vs crates.io, every requirement
    scripts/crate-versions.py require disk-core latest
    scripts/crate-versions.py require --all latest # every requirement -> that crate's local version
    scripts/crate-versions.py require fs-mac 1     # an explicit requirement (caret: any 1.x)
    scripts/crate-versions.py bump disk-core 1.0.0 # the crate's own version + its dependents
    ... --write                                    # apply; without it, only show the edits

A requirement is a minimum with Cargo's caret rule: "0.2.1" accepts any 0.2.x >= 0.2.1 and
"1.0" any 1.x, so consumers already get the newest compatible release. `latest` raises the
minimum to the version in this tree; `bump` moves a crate across a compatibility boundary
(e.g. 0.x -> 1.0) and rewrites the dependents' requirements to `MAJOR.MINOR` (0.x) or
`MAJOR` (>= 1). Manifests are edited as text, so comments and layout survive. After --write,
update the lockfiles (root, rb-cli-vintage, rb-cli-ppc, rb-cli-sol9) and run
scripts/test-crates-msrv.sh; publishing stays with publish-crates.yml.
"""
import argparse
import json
import pathlib
import re
import sys
import tomllib
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
CRATES = ROOT / "crates"
# Manifests outside crates/ that may carry versioned requirements on our crates.
EXTRA_MANIFESTS = ["Cargo.toml", "rb-cli-vintage/Cargo.toml", "rb-cli-ppc/Cargo.toml", "rb-cli-sol9/Cargo.toml"]
DEP_TABLES = ("dependencies", "dev-dependencies", "build-dependencies")
INDEX = "https://index.crates.io"
UA = "rusty-backup-crate-versions (https://github.com/danifunker/rusty-backup)"


def index_path(name):
    n = name.lower()
    if len(n) <= 2:
        return f"{len(n)}/{n}"
    if len(n) == 3:
        return f"3/{n[0]}/{n}"
    return f"{n[:2]}/{n[2:4]}/{n}"


def parse_version(v):
    core = v.split("+")[0].split("-")[0]
    parts = [int(p) for p in core.split(".")]
    return tuple(parts + [0] * (3 - len(parts)))


def crates_io_latest(name):
    """Newest non-yanked, non-prerelease version on crates.io, or None if unpublished."""
    req = urllib.request.Request(f"{INDEX}/{index_path(name)}", headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            lines = r.read().decode().splitlines()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    vers = [json.loads(l) for l in lines if l.strip()]
    ok = [v["vers"] for v in vers if not v.get("yanked") and "-" not in v["vers"]]
    return max(ok, key=parse_version) if ok else None


def caret_accepts(req, version):
    """Whether Cargo's default (caret) requirement `req` admits `version`."""
    req = req.strip().lstrip("^")
    if not re.fullmatch(r"\d+(\.\d+){0,2}", req):
        return None  # ~, =, ranges, wildcards: not judged here
    r = [int(p) for p in req.split(".")]
    v = parse_version(version)
    lo = tuple(r + [0] * (3 - len(r)))
    if v < lo:
        return False
    if r[0] > 0:
        return v[0] == r[0]
    if len(r) > 1 and r[1] > 0:
        return v[0] == 0 and v[1] == r[1]
    if len(r) > 2:
        return v[:3] == lo
    return v[0] == 0 and (len(r) == 1 or v[1] == r[1])


def compat_req(version):
    """The requirement that follows a version across its compatible range."""
    v = parse_version(version)
    return f"{v[0]}" if v[0] > 0 else f"0.{v[1]}"


class Manifest:
    def __init__(self, path):
        self.path = path
        self.text = path.read_text()
        self.data = tomllib.loads(self.text)
        self.edits = []

    @property
    def rel(self):
        return self.path.relative_to(ROOT)

    def package(self):
        return self.data.get("package", {})

    def requirements(self, names):
        """(dep, table, requirement-or-None, has_path) for every dependency on one of `names`."""
        out = []
        tables = [(t, self.data.get(t, {})) for t in DEP_TABLES]
        for target, spec in self.data.get("target", {}).items():
            tables += [(f"target.{target}.{t}", spec.get(t, {})) for t in DEP_TABLES]
        for table, deps in tables:
            for dep, spec in deps.items():
                real = spec.get("package", dep) if isinstance(spec, dict) else dep
                if real not in names:
                    continue
                if isinstance(spec, str):
                    out.append((real, table, spec, False))
                else:
                    out.append((real, table, spec.get("version"), "path" in spec))
        return out

    def set_requirement(self, dep, new):
        """Rewrite the `version` of every inline dependency on `dep` that already has one."""
        pat_inline = re.compile(
            rf'^(\s*{re.escape(dep)}\s*=\s*\{{[^}}\n]*?\bversion\s*=\s*")([^"]*)(")', re.M)
        pat_plain = re.compile(rf'^(\s*{re.escape(dep)}\s*=\s*")([^"]*)(")', re.M)
        for pat in (pat_inline, pat_plain):
            def sub(m):
                if m.group(2) != new:
                    self.edits.append(f'{dep}: "{m.group(2)}" -> "{new}"')
                return m.group(1) + new + m.group(3)
            self.text = pat.sub(sub, self.text)

    def set_package_version(self, new):
        m = re.search(r'(?ms)^\[package\].*?^version\s*=\s*"([^"]*)"', self.text)
        if not m:
            sys.exit(f"{self.rel}: no [package] version line")
        start, end = m.span(1)
        if m.group(1) != new:
            self.edits.append(f'version: "{m.group(1)}" -> "{new}"')
            self.text = self.text[:start] + new + self.text[end:]

    def save(self):
        self.path.write_text(self.text)


def load():
    crates = {}
    for p in sorted(CRATES.glob("*/Cargo.toml")):
        m = Manifest(p)
        name = m.package().get("name")
        if name:
            crates[name] = m
    extras = [Manifest(ROOT / e) for e in EXTRA_MANIFESTS if (ROOT / e).exists()]
    return crates, extras


def cmd_status(crates, extras, offline, verbose):
    names = set(crates)
    local = {n: m.package()["version"] for n, m in crates.items()}
    print(f"{'crate':<16} {'local':<9} {'crates.io':<10} state")
    for n in sorted(crates):
        pub = None if offline else crates_io_latest(n)
        if offline:
            state = "-"
        elif pub is None:
            state = "unpublished"
        elif parse_version(local[n]) > parse_version(pub):
            state = "local ahead (publish)"
        elif parse_version(local[n]) < parse_version(pub):
            state = "crates.io ahead"
        else:
            state = "published"
        print(f"{n:<16} {local[n]:<9} {pub or '-':<10} {state}")
    print()
    print("Requirements between them (flag: REJECTS = the tree's version is outside the")
    print("requirement; behind = the minimum is older than the tree's version):")
    rows = []
    for m in list(crates.values()) + extras:
        for dep, table, req, has_path in m.requirements(names):
            if req is None:
                if has_path and not verbose:
                    continue  # the app's own path dependencies are never published
                flag = "path only" if has_path else "no version"
            else:
                ok = caret_accepts(req, local[dep])
                if ok is False:
                    flag = "REJECTS"
                elif ok is None:
                    flag = "not caret"
                elif parse_version(req.lstrip("^")) < parse_version(local[dep]):
                    flag = "behind"
                else:
                    flag = "current"
            rows.append((str(m.rel), dep, table, req or "-", flag))
    for r in sorted(rows):
        print(f"  {r[0]:<34} {r[1]:<14} {r[2]:<17} {r[3]:<8} {r[4]}")


def cmd_require(crates, extras, deps, req, write):
    targets = list(crates.values()) + extras
    for dep in deps:
        if dep not in crates:
            sys.exit(f"unknown crate {dep}")
        new = crates[dep].package()["version"] if req == "latest" else req
        for m in targets:
            m.set_requirement(dep, new)
    report(targets, write)


def cmd_bump(crates, extras, name, version, write):
    if name not in crates:
        sys.exit(f"unknown crate {name}")
    parse_version(version)
    crates[name].set_package_version(version)
    targets = list(crates.values()) + extras
    for m in targets:
        m.set_requirement(name, compat_req(version))
    report(targets, write)


def report(manifests, write):
    changed = [m for m in manifests if m.edits]
    if not changed:
        print("Nothing to change.")
        return
    for m in changed:
        print(f"{m.rel}:")
        for e in dict.fromkeys(m.edits):
            print(f"  {e}")
        if write:
            m.save()
    if write:
        print("\nWritten. Next: update the four lockfiles and run scripts/test-crates-msrv.sh.")
    else:
        print("\nDry run; pass --write to apply.")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd")
    st = sub.add_parser("status", help="local vs crates.io versions and every requirement")
    st.add_argument("--offline", action="store_true", help="skip the crates.io lookup")
    st.add_argument("--verbose", action="store_true", help="also list path-only dependencies")
    rq = sub.add_parser("require", help="set the requirement other crates use for a crate")
    rq.add_argument("crate", nargs="?")
    rq.add_argument("req", help='a caret requirement ("1", "0.2.1") or "latest"')
    rq.add_argument("--all", action="store_true", help="every crate in crates/ (use with latest)")
    rq.add_argument("--write", action="store_true")
    bp = sub.add_parser("bump", help="set a crate's own version and follow it in dependents")
    bp.add_argument("crate")
    bp.add_argument("version")
    bp.add_argument("--write", action="store_true")
    args = ap.parse_args()

    crates, extras = load()
    if args.cmd in (None, "status"):
        cmd_status(crates, extras, getattr(args, "offline", False), getattr(args, "verbose", False))
    elif args.cmd == "require":
        if args.all:
            if args.req != "latest":
                sys.exit("--all only makes sense with latest")
            deps = sorted(crates)
        elif args.crate:
            deps = [args.crate]
        else:
            sys.exit("name a crate or pass --all")
        cmd_require(crates, extras, deps, args.req, args.write)
    elif args.cmd == "bump":
        cmd_bump(crates, extras, args.crate, args.version, args.write)


if __name__ == "__main__":
    main()
