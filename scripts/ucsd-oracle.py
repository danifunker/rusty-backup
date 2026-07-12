#!/usr/bin/env python3
"""Clean-room UCSD p-System filesystem oracle.

An independent reference implementation (mkfs / addfile / list / fsck) derived
solely from the ucsd-psystem-fs(5) on-disk format specification, used to
cross-validate rusty-backup's Rust UCSD driver in both directions (our writer's
output must pass this fsck; this writer's output must read back through our
driver). This mirrors the "independent clean-room reader/writer" validation the
repo already uses for RS-DOS / CoCo Disk BASIC.

On-disk facts (from ucsdpsys_fs.5):
  - 512-byte blocks. Blocks 0-1 = bootstrap. Directory at block 2, 4 blocks.
  - 26-byte directory entries; entry 0 = volume label, entries 1..77 = files.
  - Files are CONTIGUOUS: entry occupies blocks [FIRSTBLK, DLASTBLK).
  - Byte sex = host CPU's; detect from the volume label DLASTBLK (=6 or 10):
    its low byte holds 6/10 on little-endian, the high byte on big-endian.
  - Date word: bits 0-4 day (1..31), 5-8 month (1..12), 9-15 year (0..99).

Usage:
  ucsd-oracle.py mkfs   <img> [blocks] [volname]
  ucsd-oracle.py addfile <img> <name> <kind> <host-file>
  ucsd-oracle.py list   <img>
  ucsd-oracle.py fsck   <img>          # exit 0 = clean, 1 = problems
"""
import struct
import sys

BLOCK = 512
DIR_BLOCK = 2
DIR_BLOCKS = 4
ENTRY = 26
DIR_END = DIR_BLOCK + DIR_BLOCKS  # 6: first block past a single directory
KINDS = ["untyped", "xdsk", "code", "text", "info", "data", "graf", "foto", "securedir"]


class ByteSex:
    def __init__(self, little):
        self.f = "<" if little else ">"

    def u16(self, buf, off):
        return struct.unpack_from(self.f + "H", buf, off)[0]

    def p16(self, val):
        return struct.pack(self.f + "H", val & 0xFFFF)


def detect_sex(dirbuf):
    # Volume label DLASTBLK (bytes 2..3) is 6 or 10.
    if dirbuf[2] in (6, 10) and dirbuf[3] == 0:
        return ByteSex(True)
    if dirbuf[3] in (6, 10) and dirbuf[2] == 0:
        return ByteSex(False)
    return None


def read_dir(img):
    with open(img, "rb") as fh:
        fh.seek(DIR_BLOCK * BLOCK)
        return bytearray(fh.read(DIR_BLOCKS * BLOCK))


def write_dir(img, dirbuf):
    with open(img, "r+b") as fh:
        fh.seek(DIR_BLOCK * BLOCK)
        fh.write(dirbuf)


def get_name(buf, off, maxlen):
    n = buf[off]
    if n == 0 or n > maxlen:
        return None
    return bytes(buf[off + 1 : off + 1 + n]).decode("ascii", "replace")


def put_name(buf, off, name, maxlen):
    nb = name.upper().encode("ascii")[:maxlen]
    buf[off] = len(nb)
    buf[off + 1 : off + 1 + len(nb)] = nb


def today_word():
    return (1 << 9) | (1 << 5) | 1  # 1 Jan year-1: deterministic, valid


def cmd_mkfs(img, blocks=280, volname="RBTEST"):
    blocks = int(blocks)
    data = bytearray(blocks * BLOCK)
    sex = ByteSex(True)
    d = bytearray(DIR_BLOCKS * BLOCK)
    d[0:2] = sex.p16(0)  # FIRSTBLK
    d[2:4] = sex.p16(DIR_END)  # DLASTBLK = 6
    d[4:6] = sex.p16(0)  # kind (untyped)
    put_name(d, 6, volname, 7)
    d[14:16] = sex.p16(blocks)  # DEOVBLK
    d[16:18] = sex.p16(0)  # DNUMFILES
    d[18:20] = sex.p16(0)
    d[20:22] = sex.p16(today_word())
    data[DIR_BLOCK * BLOCK : DIR_BLOCK * BLOCK + len(d)] = d
    with open(img, "wb") as fh:
        fh.write(data)
    print(f"mkfs: {img} {blocks} blocks, volume {volname!r}")


def parse_files(d, sex):
    nfiles = sex.u16(d, 16)
    files = []
    for i in range(1, nfiles + 1):
        off = i * ENTRY
        first = sex.u16(d, off)
        last = sex.u16(d, off + 2)
        kind = sex.u16(d, off + 4) & 0xF
        name = get_name(d, off + 6, 15)
        lastbyte = sex.u16(d, off + 22)
        files.append((first, last, kind, name, lastbyte))
    return nfiles, files


def cmd_list(img):
    d = read_dir(img)
    sex = detect_sex(d)
    if not sex:
        print("list: not a UCSD volume")
        return 1
    vol = get_name(d, 6, 7)
    eov = sex.u16(d, 14)
    nfiles, files = parse_files(d, sex)
    print(f"volume {vol!r}  blocks={eov}  files={nfiles}")
    for first, last, kind, name, lastbyte in files:
        size = (last - first - 1) * BLOCK + lastbyte if last > first else 0
        ext = "." + KINDS[kind].upper() if kind < len(KINDS) else "?"
        print(f"  {name!r:20} {KINDS[kind]:9} blk {first}..{last}  {size} bytes  {ext}")
    return 0


def cmd_addfile(img, name, kind, hostfile):
    kind = int(kind)
    with open(hostfile, "rb") as fh:
        payload = fh.read()
    d = read_dir(img)
    sex = detect_sex(d)
    eov = sex.u16(d, 14)
    nfiles, files = parse_files(d, sex)
    if nfiles >= 77:
        print("addfile: directory full")
        return 1
    need = max(1, (len(payload) + BLOCK - 1) // BLOCK)
    # Find a contiguous free gap (files are kept sorted by FIRSTBLK).
    occ = sorted((f[0], f[1]) for f in files)
    cursor = DIR_END
    start = None
    for a, b in occ:
        if a - cursor >= need:
            start = cursor
            break
        cursor = max(cursor, b)
    if start is None and eov - cursor >= need:
        start = cursor
    if start is None:
        print("addfile: no contiguous free space")
        return 1
    last = start + need
    lastbyte = len(payload) - (need - 1) * BLOCK if payload else 0
    if lastbyte == 0:
        lastbyte = BLOCK
    # write payload
    with open(img, "r+b") as fh:
        fh.seek(start * BLOCK)
        fh.write(payload)
        pad = need * BLOCK - len(payload)
        if pad:
            fh.write(b"\x00" * pad)
    # insert entry, keep sorted by FIRSTBLK
    files.append((start, last, kind, name.upper(), lastbyte))
    files.sort(key=lambda e: e[0])
    for i, (first, lst, knd, nm, lb) in enumerate(files, start=1):
        off = i * ENTRY
        d[off : off + ENTRY] = bytes(ENTRY)
        d[off : off + 2] = sex.p16(first)
        d[off + 2 : off + 4] = sex.p16(lst)
        d[off + 4 : off + 6] = sex.p16(knd)
        put_name(d, off + 6, nm, 15)
        d[off + 22 : off + 24] = sex.p16(lb)
        d[off + 24 : off + 26] = sex.p16(today_word())
    d[16:18] = sex.p16(len(files))
    write_dir(img, d)
    print(f"addfile: {name!r} kind={kind} at blk {start}..{last} ({len(payload)} bytes)")
    return 0


def cmd_fsck(img):
    import os

    d = read_dir(img)
    sex = detect_sex(d)
    problems = []
    if not sex:
        print("fsck: bad volume label byte-sex / DLASTBLK")
        return 1
    if sex.u16(d, 0) != 0:
        problems.append("volume label FIRSTBLK != 0")
    if sex.u16(d, 2) not in (6, 10):
        problems.append("volume label DLASTBLK not 6/10")
    if sex.u16(d, 4) != 0:
        problems.append("volume label kind != 0")
    if get_name(d, 6, 7) is None:
        problems.append("volume label name length invalid")
    eov = sex.u16(d, 14)
    disk_blocks = os.path.getsize(img) // BLOCK
    if eov > disk_blocks:
        problems.append(f"DEOVBLK {eov} exceeds disk {disk_blocks}")
    nfiles, files = parse_files(d, sex)
    prev_end = DIR_END
    for idx, (first, last, kind, name, lastbyte) in enumerate(files, start=1):
        if name is None:
            problems.append(f"file {idx}: name length invalid")
        if not (first < last):
            problems.append(f"file {idx} {name!r}: FIRSTBLK {first} >= DLASTBLK {last}")
        if first < DIR_END:
            problems.append(f"file {idx} {name!r}: overlaps directory (first {first})")
        if last > eov:
            problems.append(f"file {idx} {name!r}: DLASTBLK {last} past DEOVBLK {eov}")
        if first < prev_end:
            problems.append(f"file {idx} {name!r}: overlaps previous (first {first} < {prev_end})")
        if not (1 <= lastbyte <= BLOCK):
            problems.append(f"file {idx} {name!r}: DLASTBYTE {lastbyte} out of range")
        if kind > 8:
            problems.append(f"file {idx} {name!r}: invalid kind {kind}")
        prev_end = last
    if problems:
        for p in problems:
            print("fsck: " + p)
        return 1
    print(f"fsck: clean ({nfiles} files)")
    return 0


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    cmd, img = sys.argv[1], sys.argv[2]
    rest = sys.argv[3:]
    if cmd == "mkfs":
        return cmd_mkfs(img, *rest) or 0
    if cmd == "addfile":
        return cmd_addfile(img, *rest)
    if cmd == "list":
        return cmd_list(img)
    if cmd == "fsck":
        return cmd_fsck(img)
    print(f"unknown command {cmd!r}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
