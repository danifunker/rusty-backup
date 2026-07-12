#!/usr/bin/env python3
"""Clean-room TR-DOS (ZX Spectrum Beta Disk) filesystem oracle.

An independent reference implementation (mkfs / addfile / delete / list / fsck)
derived solely from the published TR-DOS on-disk format, used to cross-validate
rusty-backup's Rust TR-DOS driver in both directions (our writer's output must
pass this fsck; this writer's output must read back through our driver). This
mirrors the "independent clean-room reader/writer" validation the repo already
uses for UCSD p-System and RS-DOS / CoCo Disk BASIC.

On-disk facts:
  - 256-byte sectors, 16 sectors/track. Disk type (info byte 0xE3):
      0x16 = 80 track double sided (160 logical tracks, 640 KiB)
      0x17 = 40 track double sided ( 80 logical tracks, 320 KiB)
      0x18 = 80 track single sided ( 80 logical tracks, 320 KiB)
      0x19 = 40 track single sided ( 40 logical tracks, 160 KiB)
  - File entries use *logical* track numbers, and the .trd stores sectors in
    logical order, so linear_sector = track*16 + sector and byte offset =
    linear_sector*256 (no head/cylinder interleaving math is ever needed).
  - Catalog: track 0, sectors 0..7 = 128 x 16-byte entries. Disk-info sector:
    track 0, sector 8 (byte offset 0x800).
  - Files are CONTIGUOUS and packed from sector 16 (track 1) in catalog order.
    The first-free pointer is a high-water mark that advances on SAVE and never
    retreats on ERASE (space is reclaimed only by the MOVE/compress command), so
    a deleted file keeps its slot and its sectors until a MOVE re-packs.
  - Directory entry (16 bytes): name[0..8] (byte 0 = 0x00 end-of-catalog, 0x01
    deleted tombstone), type char[8], start param[9..11] LE, length bytes[11..13]
    LE, length in sectors[13], start sector[14], start track[15].
  - Disk-info sector: 0xE1 first free sector, 0xE2 first free track, 0xE3 disk
    type, 0xE4 file count (incl. tombstones), 0xE5..E6 free sector count LE,
    0xE7 = 0x10 (TR-DOS id), 0xF4 deleted count, 0xF5..FC 8-char disk label.

Usage:
  trdos-oracle.py mkfs    <img> [type] [label]     # type: 80DS 40DS 80SS 40SS
  trdos-oracle.py addfile <img> <name> <typechar> <host-file>
  trdos-oracle.py delete  <img> <name>
  trdos-oracle.py list    <img>
  trdos-oracle.py fsck    <img>                    # exit 0 = clean, 1 = problems
"""
import struct
import sys

SECTOR = 256
SPT = 16  # sectors per track
CATALOG_SECTORS = 8  # track 0, sectors 0..7
INFO_SECTOR = 8  # track 0, sector 8
DATA_START = SPT  # first data sector: track 1, sector 0 = linear 16
ENTRY = 16
MAX_FILES = 128
INFO_OFF = INFO_SECTOR * SECTOR  # 0x800

# disk-type byte -> number of logical tracks
TYPES = {0x16: 160, 0x17: 80, 0x18: 80, 0x19: 40}
TYPE_NAMES = {"80DS": 0x16, "40DS": 0x17, "80SS": 0x18, "40SS": 0x19}


def total_sectors(disk_type):
    return TYPES[disk_type] * SPT


def read_img(img):
    with open(img, "rb") as fh:
        return bytearray(fh.read())


def info_view(data):
    return data[INFO_OFF : INFO_OFF + SECTOR]


def parse_entries(data):
    """Used catalog slots as (index, raw16, deleted), stopping at end-of-catalog."""
    entries = []
    for i in range(MAX_FILES):
        off = i * ENTRY
        raw = data[off : off + ENTRY]
        b0 = raw[0]
        if b0 == 0x00:
            break  # end of catalogue
        entries.append((i, bytes(raw), b0 == 0x01))
    return entries


def entry_fields(raw):
    type_char = raw[8]
    start_param = struct.unpack_from("<H", raw, 9)[0]
    length_bytes = struct.unpack_from("<H", raw, 11)[0]
    length_sectors = raw[13]
    start_sector = raw[14]
    start_track = raw[15]
    return (
        bytes(raw[0:8]),
        type_char,
        start_param,
        length_bytes,
        length_sectors,
        start_sector,
        start_track,
    )


def name_str(name8):
    return name8.rstrip(b" ").decode("latin-1")


def cmd_mkfs(img, disk_type="80DS", label="RBTEST"):
    dt = TYPE_NAMES.get(str(disk_type).upper())
    if dt is None:
        try:
            dt = int(disk_type, 0)
        except ValueError:
            dt = None
    if dt not in TYPES:
        print(f"mkfs: bad disk type {disk_type!r} (use 80DS/40DS/80SS/40SS)")
        return 1
    tsec = total_sectors(dt)
    data = bytearray(tsec * SECTOR)
    base = INFO_OFF
    data[base + 0x00] = 0x00  # end-of-catalogue marker (catalog is empty)
    data[base + 0xE1] = DATA_START % SPT  # first free sector = 0
    data[base + 0xE2] = DATA_START // SPT  # first free track = 1
    data[base + 0xE3] = dt  # disk type
    data[base + 0xE4] = 0  # number of files
    free = tsec - DATA_START
    struct.pack_into("<H", data, base + 0xE5, free)  # free sectors
    data[base + 0xE7] = 0x10  # TR-DOS id
    data[base + 0xF4] = 0  # deleted-file count
    lab = label.encode("latin-1", "replace")[:8].ljust(8, b" ")
    data[base + 0xF5 : base + 0xFD] = lab  # 8-char disk label
    with open(img, "wb") as fh:
        fh.write(data)
    print(f"mkfs: {img} type={disk_type} {tsec} sectors free={free} label {label!r}")
    return 0


def cmd_addfile(img, name, type_char, hostfile):
    with open(hostfile, "rb") as fh:
        payload = fh.read()
    data = read_img(img)
    info = info_view(data)
    dt = info[0xE3]
    if dt not in TYPES:
        print("addfile: not a TR-DOS disk (bad type byte)")
        return 1
    tsec = total_sectors(dt)
    entries = parse_entries(data)
    num_files = len(entries)
    if num_files >= MAX_FILES:
        print("addfile: catalogue full (128 entries)")
        return 1
    ff_sector = info[0xE1]
    ff_track = info[0xE2]
    start_linear = ff_track * SPT + ff_sector
    need = max(1, (len(payload) + SECTOR - 1) // SECTOR)
    if start_linear + need > tsec:
        print("addfile: not enough free space")
        return 1
    # Write the data, zero-padding the final sector.
    off = start_linear * SECTOR
    buf = payload + b"\x00" * (need * SECTOR - len(payload))
    data[off : off + len(buf)] = buf
    # Build the 16-byte catalogue entry at slot num_files.
    tc = type_char.encode("latin-1")[0] if type_char else ord("C")
    entry = bytearray(ENTRY)
    entry[0:8] = name.upper().encode("latin-1", "replace")[:8].ljust(8, b" ")
    entry[8] = tc
    struct.pack_into("<H", entry, 9, 0)  # start param
    struct.pack_into("<H", entry, 11, len(payload) & 0xFFFF)  # length bytes
    entry[13] = need  # length in sectors
    entry[14] = ff_sector
    entry[15] = ff_track
    slot = num_files * ENTRY
    data[slot : slot + ENTRY] = entry
    # Advance the disk-info high-water mark and free count.
    new_ff = start_linear + need
    data[INFO_OFF + 0xE4] = num_files + 1
    data[INFO_OFF + 0xE1] = new_ff % SPT
    data[INFO_OFF + 0xE2] = new_ff // SPT
    struct.pack_into("<H", data, INFO_OFF + 0xE5, tsec - new_ff)
    with open(img, "wb") as fh:
        fh.write(data)
    print(
        f"addfile: {name!r} type={type_char} at track {ff_track} sector {ff_sector} "
        f"({need} sectors, {len(payload)} bytes)"
    )
    return 0


def cmd_delete(img, name):
    data = read_img(img)
    target = name.upper().encode("latin-1", "replace")[:8].ljust(8, b" ")
    for i, raw, deleted in parse_entries(data):
        if deleted:
            continue
        if raw[0:8] == target:
            data[i * ENTRY] = 0x01  # tombstone: replace first name byte
            data[INFO_OFF + 0xF4] = (data[INFO_OFF + 0xF4] + 1) & 0xFF
            with open(img, "wb") as fh:
                fh.write(data)
            print(f"delete: {name!r} tombstoned at slot {i}")
            return 0
    print(f"delete: {name!r} not found")
    return 1


def cmd_list(img):
    data = read_img(img)
    info = info_view(data)
    dt = info[0xE3]
    if dt not in TYPES:
        print("list: not a TR-DOS disk (bad type byte)")
        return 1
    label = name_str(bytes(info[0xF5:0xFD]))
    free = struct.unpack_from("<H", info, 0xE5)[0]
    print(f"disk label {label!r} type=0x{dt:02X} files={info[0xE4]} free_sectors={free}")
    for i, raw, deleted in parse_entries(data):
        if deleted:
            continue
        name, tc, _sp, lb, ls, ss, st = entry_fields(raw)
        tchar = chr(tc) if 32 <= tc < 127 else "?"
        print(
            f"  {name_str(name):8}.{tchar}  track {st} sector {ss}  "
            f"{ls} sectors  {ls * SECTOR} bytes  (len={lb})"
        )
    return 0


def cmd_fsck(img):
    data = read_img(img)
    info = info_view(data)
    problems = []
    dt = info[0xE3]
    if dt not in TYPES:
        print(f"fsck: bad disk type byte 0x{dt:02X}")
        return 1
    if info[0xE7] != 0x10:
        problems.append(f"TR-DOS id byte 0x{info[0xE7]:02X} != 0x10")
    tsec = total_sectors(dt)
    disk_sectors = len(data) // SECTOR
    if tsec > disk_sectors:
        problems.append(f"disk type implies {tsec} sectors but image has {disk_sectors}")

    entries = parse_entries(data)
    if info[0xE4] != len(entries):
        problems.append(f"file count {info[0xE4]} != {len(entries)} used slots")
    deleted = sum(1 for (_, _, d) in entries if d)
    if info[0xF4] != deleted:
        problems.append(f"deleted count {info[0xF4]} != {deleted} tombstones")

    cursor = DATA_START
    for i, raw, _is_del in entries:
        name, _tc, _sp, lb, ls, ss, st = entry_fields(raw)
        nm = name_str(name)
        start_linear = st * SPT + ss
        if ls == 0:
            problems.append(f"entry {i} {nm!r}: zero length in sectors")
        if start_linear != cursor:
            problems.append(
                f"entry {i} {nm!r}: start {start_linear} != expected {cursor} (non-contiguous)"
            )
        if start_linear + ls > tsec:
            problems.append(f"entry {i} {nm!r}: extends past disk end")
        if lb > ls * SECTOR:
            problems.append(f"entry {i} {nm!r}: length {lb} bytes exceeds its {ls}-sector allocation")
        cursor = start_linear + ls

    ff_linear = info[0xE2] * SPT + info[0xE1]
    if ff_linear != cursor:
        problems.append(f"first-free pointer {ff_linear} != high-water mark {cursor}")
    free = struct.unpack_from("<H", info, 0xE5)[0]
    if free != tsec - cursor:
        problems.append(f"free sectors {free} != {tsec - cursor}")

    if problems:
        for p in problems:
            print("fsck: " + p)
        return 1
    print(f"fsck: clean ({len(entries)} entries, {deleted} deleted, {tsec - cursor} free sectors)")
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
