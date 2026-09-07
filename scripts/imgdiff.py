#!/usr/bin/env python3
"""Report every byte run that differs between two disk images.

Written for the PowerPC parity work. The question there is never "did the
command succeed" - a log line saying `Preserving type/creator TEXT/MSWD` was
how a real bug hid - but "did the two builds write the *same bytes to the same
offsets*". Diffing before/after on each host and comparing the two reports
answers that without needing to know any filesystem's on-disk layout, and it
catches a byte-order bug directly: a swapped u32 shows up as the same offset
with different bytes.

Usage:
    imgdiff.py BEFORE AFTER [--mask-pair A B]

Output is one line per changed run, `OFFSET LEN old=HEX new=HEX`, which is
stable enough to diff between two hosts.

`--mask-pair A B` handles the one thing that legitimately differs between two
hosts: time. A and B are two runs of the *same* command on the *same* machine,
so any byte that differs between them is time-varying (an ext inode's
atime/ctime/mtime, HFS's drLsMod, a FAT dirent's creation-time tenths) and
cannot be compared across machines that ran seconds apart. Those bytes render as
`..`, and a run that is entirely volatile is dropped, since its boundaries move
with the values. Everything else still has to match exactly - a mode, uid or
protection longword is not time-varying, so a real disagreement still surfaces.
"""

import sys


def runs(a: bytes, b: bytes, gap: int = 4):
    """Yield (offset, length) for each differing run, merging runs closer than `gap`."""
    n = max(len(a), len(b))
    a = a.ljust(n, b"\0")
    b = b.ljust(n, b"\0")
    out = []
    i = 0
    while i < n:
        if a[i] != b[i]:
            start = i
            last = i
            j = i + 1
            # Merge nearby differences so a changed u32 reads as one run, not four.
            while j < n and (j - last) <= gap:
                if a[j] != b[j]:
                    last = j
                j += 1
            out.append((start, last - start + 1))
            i = last + 1
        else:
            i += 1
    return out


def read(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def volatile_set(a: bytes, b: bytes) -> set:
    """Byte offsets that differ between two runs of the same command."""
    n = max(len(a), len(b))
    a = a.ljust(n, b"\0")
    b = b.ljust(n, b"\0")
    seen = {i for i in range(n) if a[i] != b[i]}
    # Widen to the containing 4-byte word: the other host's clock is offset, not merely later.
    out = set(seen)
    for i in seen:
        base = i - (i % 4)
        out.update(range(base, min(base + 4, n)))
    return out


def render(buf: bytes, off: int, length: int, volatile: set) -> str:
    return "".join(
        ".." if off + k in volatile else f"{buf[off + k]:02x}" for k in range(length)
    )


def main() -> int:
    argv = sys.argv[1:]
    volatile = set()
    if "--mask-pair" in argv:
        i = argv.index("--mask-pair")
        if len(argv) < i + 3:
            print("--mask-pair needs two image paths", file=sys.stderr)
            return 2
        volatile = volatile_set(read(argv[i + 1]), read(argv[i + 2]))
        del argv[i : i + 3]

    args = [x for x in argv if not x.startswith("--")]
    if len(args) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    before = read(args[0])
    after = read(args[1])

    if len(before) != len(after):
        print(f"SIZE {len(before)} -> {len(after)}")

    changed = runs(before, after)
    # A wholly time-varying run is dropped, not masked: its bounds move with the values.
    changed = [
        (off, length)
        for off, length in changed
        if any(off + k not in volatile for k in range(length))
    ]
    if not changed:
        print("IDENTICAL")
        return 0

    total = 0
    for off, length in changed:
        total += length
        old = render(before, off, length, volatile)
        new = render(after, off, length, volatile)
        print(f"{off:#010x} {length:3d} old={old} new={new}")
    print(f"# {len(changed)} run(s), {total} byte(s) changed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
