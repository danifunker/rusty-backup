#!/usr/bin/env python3
"""Print the crates under crates/ whose Cargo.toml version is not on crates.io yet.

One name per line, nothing if all are published. Used by publish-crates.yml;
reads crates.io's sparse index, which needs no token.
"""
import json
import subprocess
import sys
import urllib.error
import urllib.request

INDEX = "https://index.crates.io"
UA = "rusty-backup-publish (https://github.com/danifunker/rusty-backup)"


def index_path(name):
    n = name.lower()
    if len(n) <= 2:
        return f"{len(n)}/{n}"
    if len(n) == 3:
        return f"3/{n[0]}/{n}"
    return f"{n[:2]}/{n[2:4]}/{n}"


def published_versions(name):
    req = urllib.request.Request(f"{INDEX}/{index_path(name)}", headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = r.read().decode()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return set()
        raise
    return {json.loads(line)["vers"] for line in body.splitlines() if line.strip()}


def main():
    meta = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"]))
    for p in sorted(meta["packages"], key=lambda p: p["name"]):
        if "/crates/" not in p["manifest_path"].replace("\\", "/"):
            continue
        if p.get("publish") == []:
            continue
        if p["version"] not in published_versions(p["name"]):
            print(p["name"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
