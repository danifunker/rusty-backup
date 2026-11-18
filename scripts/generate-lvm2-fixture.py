#!/usr/bin/env python3
"""Build tests/fixtures/test_lvm2.img.zst: a 16 MiB MBR disk whose one partition is a
Linux LVM2 physical volume (VG "fixvg", 1 MiB extents) holding:

  root  ext4 (mke2fs -d), 6 extents stored as PEs 6-8 then 0-2: two runs, out of order
  swap  2 extents at PEs 3-4: one contiguous run
  pool  a thin pool, listed with the reason it cannot open

The LVM2 label, PV header, metadata-area header and VG text follow the on-disk format the
reader documents in crates/part-tables/src/lvm/linux.rs. Needs e2fsprogs and zstd.
Usage: scripts/generate-lvm2-fixture.py [out.img.zst]
"""
import os
import struct
import subprocess
import sys
import tempfile
import zlib

MIB = 1 << 20
DISK = 16 * MIB
PART_AT = 1 * MIB  # partition start (LBA 2048)
PART_LEN = 15 * MIB
PE_START = 1 * MIB  # data area, from the PV start
EXTENT = 1 * MIB
SEED = 0xF597A6CF
MDA_AT = 4096
TEXT_AT = 1536  # VG text, from the metadata area start
UUID = "Fx7LvmFixturePvUuid0000000000001"  # 32 characters


def lvm_crc(data, seed=SEED):
    """LVM's CRC-32: zlib's table, seeded, no final inversion."""
    return ~zlib.crc32(data, ~seed & 0xFFFFFFFF) & 0xFFFFFFFF


def mke2fs_root():
    e2 = "/opt/homebrew/opt/e2fsprogs/sbin/mke2fs"
    if not os.path.exists(e2):
        e2 = "mke2fs"
    with tempfile.TemporaryDirectory() as tmp:
        src = os.path.join(tmp, "root")
        os.makedirs(os.path.join(src, "etc"))
        with open(os.path.join(src, "etc", "hostname"), "w") as f:
            f.write("lvm2-fixture\n")
        with open(os.path.join(src, "README.TXT"), "w") as f:
            f.write("root LV: two runs, logical extents 0-2 at PEs 6-8, 3-5 at PEs 0-2\n")
        # Spans every extent, so a misplaced run shows as a wrong byte.
        with open(os.path.join(src, "BIG.BIN"), "wb") as f:
            f.write(bytes((i * 7 + i // 4096) & 0xFF for i in range(4 * MIB + 12345)))
        img = os.path.join(tmp, "root.ext4")
        subprocess.run(
            [e2, "-q", "-F", "-t", "ext4", "-b", "4096", "-L", "fixroot",
             "-E", "root_owner=0:0", "-U", "11111111-2222-3333-4444-555555555555",
             "-d", src, img, "6M"],
            check=True,
            env={**os.environ, "E2FSPROGS_FAKE_TIME": "1700000000"},
        )
        return open(img, "rb").read()


def dashed(u):
    """LVM prints a UUID as 6-4-4-4-4-4-6 characters."""
    cuts = [0, 6, 10, 14, 18, 22, 26, 32]
    return "-".join(u[a:b] for a, b in zip(cuts, cuts[1:]))


def seg(n, start, count, pe):
    return (f"segment{n} {{\nstart_extent = {start}\nextent_count = {count}\n"
            f"type = \"striped\"\nstripe_count = 1\nstripes = [\n\"pv0\", {pe}\n]\n}}\n")


def vg_text():
    pe_count = (PART_LEN - PE_START) // EXTENT
    lvs = (
        "root {\nid = \"root-lv\"\nstatus = [\"READ\", \"WRITE\", \"VISIBLE\"]\nsegment_count = 2\n"
        + seg(1, 0, 3, 6) + seg(2, 3, 3, 0) + "}\n"
        "swap {\nid = \"swap-lv\"\nstatus = [\"READ\", \"WRITE\", \"VISIBLE\"]\nsegment_count = 1\n"
        + seg(1, 0, 2, 3) + "}\n"
        "pool {\nid = \"pool-lv\"\nstatus = [\"READ\", \"WRITE\", \"VISIBLE\"]\nsegment_count = 1\n"
        "segment1 {\nstart_extent = 0\nextent_count = 2\ntype = \"thin-pool\"\n}\n}\n"
    )
    return (
        "fixvg {\nid = \"fixvg-id\"\nseqno = 1\nformat = \"lvm2\"\nstatus = [\"READ\", \"WRITE\"]\n"
        f"extent_size = {EXTENT // 512}\nmax_lv = 0\nmax_pv = 0\n"
        "physical_volumes {\npv0 {\n"
        f"id = \"{dashed(UUID)}\"\ndevice = \"/dev/sda1\"\n"
        f"status = [\"ALLOCATABLE\"]\ndev_size = {PART_LEN // 512}\n"
        f"pe_start = {PE_START // 512}\npe_count = {pe_count}\n}}\n}}\n"
        "logical_volumes {\n" + lvs + "}\n}\n"
        "contents = \"Text Format Volume Group\"\nversion = 1\ndescription = \"\"\n"
        "creation_host = \"fixture\"\ncreation_time = 1700000000\n"
    ).encode() + b"\0"


def pv():
    img = bytearray(PART_LEN)
    label = bytearray(512)
    label[0:8] = b"LABELONE"
    label[8:16] = struct.pack("<Q", 1)
    label[20:24] = struct.pack("<I", 32)
    label[24:32] = b"LVM2 001"
    label[32:64] = UUID.encode()
    label[64:72] = struct.pack("<Q", PART_LEN)
    label[72:88] = struct.pack("<QQ", PE_START, 0)  # data area, then the list's end
    label[88:104] = struct.pack("<QQ", 0, 0)
    label[104:120] = struct.pack("<QQ", MDA_AT, PE_START - MDA_AT)  # metadata area
    label[120:136] = struct.pack("<QQ", 0, 0)
    label[16:20] = struct.pack("<I", lvm_crc(bytes(label[20:512])))
    img[512:1024] = label

    text = vg_text()
    img[MDA_AT + TEXT_AT:MDA_AT + TEXT_AT + len(text)] = text
    h = bytearray(512)
    h[4:20] = b" LVM2 x[5A%r0N*>"
    h[20:24] = struct.pack("<I", 1)
    h[24:40] = struct.pack("<QQ", MDA_AT, PE_START - MDA_AT)
    h[40:64] = struct.pack("<QQII", TEXT_AT, len(text), lvm_crc(text), 0)
    h[0:4] = struct.pack("<I", lvm_crc(bytes(h[4:512])))
    img[MDA_AT:MDA_AT + 512] = h

    root = mke2fs_root()
    assert len(root) == 6 * EXTENT
    for logical, pe in enumerate([6, 7, 8, 0, 1, 2]):
        at = PE_START + pe * EXTENT
        img[at:at + EXTENT] = root[logical * EXTENT:(logical + 1) * EXTENT]
    for pe in (3, 4):
        at = PE_START + pe * EXTENT
        img[at:at + EXTENT] = b"\x5a" * EXTENT
    return img


def mbr():
    sector = bytearray(512)
    start, count = PART_AT // 512, PART_LEN // 512
    sector[446:462] = struct.pack("<B3sB3sII", 0, b"\xfe\xff\xff", 0x8E, b"\xfe\xff\xff",
                                  start, count)
    sector[440:444] = struct.pack("<I", 0x4C564D32)
    sector[510:512] = b"\x55\xaa"
    return sector


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else "tests/fixtures/test_lvm2.img.zst"
    disk = bytearray(DISK)
    disk[0:512] = mbr()
    disk[PART_AT:PART_AT + PART_LEN] = pv()
    raw = out[:-4] if out.endswith(".zst") else out
    with open(raw, "wb") as f:
        f.write(disk)
    if out.endswith(".zst"):
        subprocess.run(["zstd", "-q", "-19", "-f", "--rm", raw, "-o", out], check=True)
    print(out)


if __name__ == "__main__":
    main()
