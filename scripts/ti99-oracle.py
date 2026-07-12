#!/usr/bin/env python3
"""Clean-room TI-99/4A disk filesystem oracle.

An independent reference implementation (mkfs / addfile / delete / list / fsck)
derived solely from the published TI-99/4A disk format, used to cross-validate
rusty-backup's Rust TI-99 driver in both directions. Unusually we ALSO have a
real-tool cross-check: MAME's `imgtool` (v9t9 format) reads TI-99 disks
correctly (its `dir` / `get`), so a disk this oracle writes must also list
through `imgtool dir`, and a disk our Rust writer produces must too. (imgtool's
TI-99 *put* is broken in the packaged build, which is why this oracle exists to
create disks-with-files at all.)

On-disk facts (all multi-byte fields BIG-endian — TMS9900):
  - 256-byte sectors. Sector 0 = Volume Information Block (VIB), sector 1 =
    File Descriptor Index Record (FDIR). Data + FDRs from sector 2 on.
  - VIB: name[0..10], total sectors[0x0A..0x0C] BE, sectors/track[0x0C],
    "DSK"[0x0D..0x10], protection[0x10], tracks/side[0x11], sides[0x12],
    density[0x13], reserved[0x14..0x38], allocation bitmap[0x38..0xEC] (180 B =
    1440 bits; bit set = used, LSB of byte 0x38 = sector 0), reserved[0xEC..0x100]
    = 0xFF. Sectors 0 and 1 are always used; sectors past the disk end are
    marked used too.
  - FDIR: up to 127 big-endian 2-byte pointers to FDR sectors, sorted by the
    FDR's 10-char name, 0x0000-terminated.
  - FDR (one per file): name[0..10], flags[0x0C], recs/sector[0x0D],
    sectors allocated[0x0E..0x10] BE (excludes the FDR sector), EOF offset in
    last sector[0x10], record length[0x11], record count[0x12..0x14] BE,
    reserved[0x14..0x1C], cluster chain[0x1C..0x100] (3 bytes each): the 12-bit
    start AU = b0 | ((b1 & 0x0F) << 8); the 12-bit file-relative index of the
    cluster's last AU = (b1 >> 4) | (b2 << 4). A zero start ends the chain.
  - This oracle writes PROGRAM files (flags = 0x01): raw bytes, size =
    (sectors-1)*256 + (eof or 256).

Usage:
  ti99-oracle.py mkfs    <img> [geom] [name]     # geom: sssd dssd ssdd dsdd
  ti99-oracle.py addfile <img> <name> <host-file>
  ti99-oracle.py delete  <img> <name>
  ti99-oracle.py list    <img>
  ti99-oracle.py fsck    <img>                    # exit 0 = clean, 1 = problems
"""
import struct
import sys

SECTOR = 256
VIB_SECTOR = 0
FDIR_SECTOR = 1
FIRST_DATA = 2
BITMAP_OFF = 0x38
BITMAP_BITS = 1440  # 180 bytes at 0x38..0xEB
FDR_CLUSTERS_OFF = 0x1C
MAX_FILES = 127

# geometry -> (sides, tracks, sectors_per_track, density)
GEOMS = {
    "sssd": (1, 40, 9, 1),
    "dssd": (2, 40, 9, 1),
    "ssdd": (1, 40, 18, 2),
    "dsdd": (2, 40, 18, 2),
}


def be16(buf, off):
    return struct.unpack_from(">H", buf, off)[0]


def put_be16(buf, off, val):
    struct.pack_into(">H", buf, off, val & 0xFFFF)


def total_sectors(sides, tracks, spt):
    return sides * tracks * spt


def geom_from_vib(data):
    return (data[0x12], data[0x11], data[0x0C])  # sides, tracks, spt


def bit_used(data, sector):
    return (data[BITMAP_OFF + sector // 8] >> (sector % 8)) & 1


def set_bit(data, sector, used):
    off = BITMAP_OFF + sector // 8
    mask = 1 << (sector % 8)
    if used:
        data[off] |= mask
    else:
        data[off] &= ~mask & 0xFF


def name_field(name):
    return name.upper().encode("ascii", "replace")[:10].ljust(10, b" ")


def name_str(raw10):
    return raw10.rstrip(b" ").decode("ascii", "replace")


def cmd_mkfs(img, geom="sssd", name="RBTEST"):
    key = str(geom).lower()
    if key not in GEOMS:
        print(f"mkfs: bad geometry {geom!r} (use sssd/dssd/ssdd/dsdd)")
        return 1
    sides, tracks, spt, density = GEOMS[key]
    tsec = total_sectors(sides, tracks, spt)
    data = bytearray(tsec * SECTOR)
    v = data  # VIB is sector 0 == start of image
    v[0:10] = name_field(name)
    put_be16(v, 0x0A, tsec)
    v[0x0C] = spt
    v[0x0D:0x10] = b"DSK"
    v[0x10] = 0x20  # unprotected
    v[0x11] = tracks
    v[0x12] = sides
    v[0x13] = density
    # Bitmap: 0xEC..0x100 reserved = 0xFF; every bit past the disk end = used.
    for b in range(0xEC, 0x100):
        v[b] = 0xFF
    for s in range(tsec, BITMAP_BITS):
        set_bit(v, s, True)
    set_bit(v, VIB_SECTOR, True)
    set_bit(v, FDIR_SECTOR, True)
    with open(img, "wb") as fh:
        fh.write(data)
    print(f"mkfs: {img} {key} {tsec} sectors, name {name!r}")
    return 0


def find_free(data, tsec, need):
    """Return a list of `need` free sector numbers (contiguous first-fit, else
    scattered), or None."""
    free = [s for s in range(FIRST_DATA, tsec) if not bit_used(data, s)]
    if len(free) < need:
        return None
    # Prefer a contiguous run so the cluster chain stays a single entry.
    for i in range(len(free) - need + 1):
        run = free[i : i + need]
        if run[-1] - run[0] == need - 1:
            return run
    return free[:need]


def build_clusters(sectors):
    """Pack a sector list into 3-byte cluster entries (grouped into contiguous
    runs)."""
    clusters = []
    i = 0
    file_au = 0
    while i < len(sectors):
        start = sectors[i]
        j = i
        while j + 1 < len(sectors) and sectors[j + 1] == sectors[j] + 1:
            j += 1
        run_len = j - i + 1
        last_off = file_au + run_len - 1
        b0 = start & 0xFF
        b1 = ((start >> 8) & 0x0F) | ((last_off & 0x0F) << 4)
        b2 = (last_off >> 4) & 0xFF
        clusters.append(bytes([b0, b1, b2]))
        file_au += run_len
        i = j + 1
    return b"".join(clusters)


def read_fdir(data):
    """Return (fdr_sector, name) list from the FDIR, in stored order."""
    out = []
    for i in range(MAX_FILES):
        ptr = be16(data, FDIR_SECTOR * SECTOR + i * 2)
        if ptr == 0:
            break
        off = ptr * SECTOR
        out.append((ptr, name_str(data[off : off + 10])))
    return out


def write_fdir(data, entries):
    base = FDIR_SECTOR * SECTOR
    data[base : base + SECTOR] = bytes(SECTOR)
    for i, (ptr, _name) in enumerate(entries):
        put_be16(data, base + i * 2, ptr)


def cmd_addfile(img, name, hostfile):
    with open(hostfile, "rb") as fh:
        payload = fh.read()
    with open(img, "rb") as fh:
        data = bytearray(fh.read())
    sides, tracks, spt = geom_from_vib(data)
    tsec = be16(data, 0x0A)
    entries = read_fdir(data)
    if len(entries) >= MAX_FILES:
        print("addfile: directory full (127 files)")
        return 1
    if any(n == name.upper() for _, n in entries):
        print(f"addfile: {name!r} already exists")
        return 1
    data_sectors = max(1, (len(payload) + SECTOR - 1) // SECTOR)
    # Need one FDR sector + the data sectors.
    fdr_sec_list = find_free(data, tsec, 1)
    if fdr_sec_list is None:
        print("addfile: no free sector for the FDR")
        return 1
    fdr_sec = fdr_sec_list[0]
    set_bit(data, fdr_sec, True)
    data_secs = find_free(data, tsec, data_sectors)
    if data_secs is None:
        set_bit(data, fdr_sec, False)
        print("addfile: not enough free sectors")
        return 1
    for s in data_secs:
        set_bit(data, s, True)
    # Write the payload across the data sectors.
    remaining = payload
    for s in data_secs:
        chunk = remaining[:SECTOR]
        remaining = remaining[SECTOR:]
        data[s * SECTOR : s * SECTOR + len(chunk)] = chunk
    # Build the FDR.
    fdr = bytearray(SECTOR)
    fdr[0:10] = name_field(name)
    fdr[0x0C] = 0x01  # PROGRAM file
    fdr[0x0D] = 0  # records/sector
    put_be16(fdr, 0x0E, data_sectors)  # sectors allocated (excl. FDR)
    fdr[0x10] = len(payload) % SECTOR  # EOF offset (0 => full last sector)
    fdr[0x11] = 0  # record length
    put_be16(fdr, 0x12, 0)  # record count
    clusters = build_clusters(data_secs)
    fdr[FDR_CLUSTERS_OFF : FDR_CLUSTERS_OFF + len(clusters)] = clusters
    data[fdr_sec * SECTOR : fdr_sec * SECTOR + SECTOR] = fdr
    # Insert into the FDIR, sorted by the padded name.
    entries.append((fdr_sec, name.upper()))
    entries.sort(key=lambda e: name_field(e[1]))
    write_fdir(data, entries)
    with open(img, "wb") as fh:
        fh.write(data)
    print(f"addfile: {name!r} FDR@{fdr_sec} data@{data_secs} ({len(payload)} bytes)")
    return 0


def cmd_delete(img, name):
    with open(img, "rb") as fh:
        data = bytearray(fh.read())
    entries = read_fdir(data)
    target = name.upper()
    keep = []
    removed = False
    for ptr, n in entries:
        if n == target and not removed:
            # Free the FDR + its data sectors.
            free_file_sectors(data, ptr)
            set_bit(data, ptr, False)
            removed = True
        else:
            keep.append((ptr, n))
    if not removed:
        print(f"delete: {name!r} not found")
        return 1
    write_fdir(data, keep)
    with open(img, "wb") as fh:
        fh.write(data)
    print(f"delete: {name!r} removed")
    return 0


def walk_clusters(data, fdr_sec):
    """Yield each physical data sector of the file whose FDR is at fdr_sec."""
    off = fdr_sec * SECTOR
    sectors_alloc = be16(data, off + 0x0E)
    got = 0
    prev_off = -1
    c = FDR_CLUSTERS_OFF
    while c + 3 <= SECTOR and got < sectors_alloc:
        b0, b1, b2 = data[off + c], data[off + c + 1], data[off + c + 2]
        start = b0 | ((b1 & 0x0F) << 8)
        last_off = (b1 >> 4) | (b2 << 4)
        if start == 0 and last_off == 0:
            break
        count = last_off - prev_off
        for k in range(count):
            yield start + k
            got += 1
        prev_off = last_off
        c += 3


def free_file_sectors(data, fdr_sec):
    for s in walk_clusters(data, fdr_sec):
        set_bit(data, s, False)


def file_size(data, fdr_sec):
    off = fdr_sec * SECTOR
    sectors_alloc = be16(data, off + 0x0E)
    eof = data[off + 0x10]
    if sectors_alloc == 0:
        return 0
    return (sectors_alloc - 1) * SECTOR + (eof if eof else SECTOR)


def cmd_list(img):
    with open(img, "rb") as fh:
        data = bytearray(fh.read())
    if data[0x0D:0x10] != b"DSK":
        print("list: not a TI-99 disk (no DSK marker)")
        return 1
    name = name_str(data[0:10])
    tsec = be16(data, 0x0A)
    used = sum(1 for s in range(tsec) if bit_used(data, s))
    print(f"volume {name!r} sectors={tsec} free={tsec - used}")
    for ptr, n in read_fdir(data):
        flags = data[ptr * SECTOR + 0x0C]
        secs = be16(data, ptr * SECTOR + 0x0E)
        print(f"  {n:10} FDR@{ptr} flags=0x{flags:02X} {secs} sectors {file_size(data, ptr)} bytes")
    return 0


def cmd_fsck(img):
    with open(img, "rb") as fh:
        data = bytearray(fh.read())
    problems = []
    if data[0x0D:0x10] != b"DSK":
        print("fsck: missing DSK marker")
        return 1
    tsec = be16(data, 0x0A)
    disk_sectors = len(data) // SECTOR
    if tsec > disk_sectors:
        problems.append(f"VIB total {tsec} > image {disk_sectors} sectors")

    # Recompute allocation from the FDIR -> FDR -> cluster walk.
    computed = bytearray((tsec + 7) // 8)

    def mark(s):
        if 0 <= s < tsec:
            computed[s // 8] |= 1 << (s % 8)

    mark(VIB_SECTOR)
    mark(FDIR_SECTOR)
    entries = read_fdir(data)
    prev_name = b""
    for ptr, n in entries:
        if not (FIRST_DATA <= ptr < tsec):
            problems.append(f"file {n!r}: FDR sector {ptr} out of range")
            continue
        mark(ptr)
        nm = name_field(n)
        if nm < prev_name:
            problems.append(f"FDIR not sorted at {n!r}")
        prev_name = nm
        secs = be16(data, ptr * SECTOR + 0x0E)
        walked = list(walk_clusters(data, ptr))
        if len(walked) != secs:
            problems.append(f"file {n!r}: chain yields {len(walked)} sectors, FDR says {secs}")
        for s in walked:
            if not (FIRST_DATA <= s < tsec):
                problems.append(f"file {n!r}: data sector {s} out of range")
            else:
                mark(s)

    for s in range(tsec):
        on_disk = bit_used(data, s)
        should = (computed[s // 8] >> (s % 8)) & 1
        if on_disk != should:
            problems.append(
                f"bitmap sector {s}: disk={on_disk} computed={should}"
                + (" (leaked)" if on_disk and not should else " (used-but-free)")
            )

    if problems:
        for p in problems[:40]:
            print("fsck: " + p)
        return 1
    print(f"fsck: clean ({len(entries)} files)")
    return 0


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    cmd, img = sys.argv[1], sys.argv[2]
    rest = sys.argv[3:]
    if cmd == "mkfs":
        return cmd_mkfs(img, *rest)
    if cmd == "addfile":
        return cmd_addfile(img, *rest)
    if cmd == "delete":
        return cmd_delete(img, *rest)
    if cmd == "list":
        return cmd_list(img)
    if cmd == "fsck":
        return cmd_fsck(img)
    print(f"unknown command {cmd!r}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
