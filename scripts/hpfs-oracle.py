#!/usr/bin/env python3
# Clean-room HPFS (OS/2 High Performance File System) oracle for rusty-backup.
#
# HPFS has no portable userspace mkfs/fsck on Linux (the kernel `hpfs` driver is
# read-mostly and needs root to mount; `hpfsck` in Debian is actually an HFS+
# checker). So, following the repo precedent for TR-DOS / UCSD / TI-99, this is
# an INDEPENDENT reference implementation written from the on-disk spec
# (Linux `fs/hpfs/hpfs.h` struct layouts + the dnode/anode/alloc algorithms).
# It cross-checks the Rust driver in src/fs/hpfs.rs:
#   - `mkfs` / `build` format + populate a volume  -> Rust reads it (browse).
#   - `ls` / `cat` / `facts` parse a volume         -> reads what Rust wrote.
#   - `fsck` validates the exact invariants the kernel enforces on mount.
#
# On-disk facts (all little-endian, 512-byte sectors):
#   sector 0   boot block   ("HPFS    " sig @54, sig_28h=0x28 @38, 0xAA55 @510)
#   sector 16  super block   magic f995e849, points at root fnode + bitmaps + dirband
#   sector 17  spare block   magic f9911849, dirty flag, hotfix/codepage/spare-dnode
#   free-space bitmaps: 4 sectors/band (1 band = 0x4000 sectors = 8 MiB),
#                       BIT SET = FREE (inverted vs most FS), LE bit order.
#   directory band: preallocated dnode area with its own 4-sector dnode bitmap
#                   (1 bit per 4-sector dnode; bit set = free).
#   dnode  (4 sectors) magic 77e40aae: B-tree directory node, dirents from off 20,
#          each dirent ends the list with a \377 (last) sentinel; dirs also carry
#          a leading \001\001 (first / ".") sentinel.
#   fnode  (1 sector)  magic f7e40aae: per-file/dir alloc B+tree (8 leaf extents)
#          + EAs; dir fnode has flag 0x100 and external[0].disk_secno = root dnode.
#   anode  (1 sector)  magic 37e40aae: overflow alloc B+tree (unused by our writer,
#          read-supported for images written elsewhere).

import json
import os
import struct
import sys

SECTOR = 512
DNODE_SECTORS = 4
BAND_SECTORS = 0x4000  # 16384 sectors = 8 MiB per band

SB_MAGIC = 0xF995E849
SB_MAGIC1 = 0xFA53E9C5
SP_MAGIC = 0xF9911849
SP_MAGIC1 = 0xFA5229C5
DNODE_MAGIC = 0x77E40AAE
FNODE_MAGIC = 0xF7E40AAE
ANODE_MAGIC = 0x37E40AAE

FNODE_FLAG_ANODE = 0x0002
FNODE_FLAG_DIR = 0x0100

# dirent flags byte (offset 2)
DE_FIRST = 0x01
DE_HAS_ACL = 0x02
DE_DOWN = 0x04
DE_LAST = 0x08
DE_HAS_EA = 0x10
# dirent attrib byte (offset 3) = DOS attributes
AT_READONLY = 0x01
AT_HIDDEN = 0x02
AT_SYSTEM = 0x04
AT_DIRECTORY = 0x10
AT_ARCHIVE = 0x20
AT_NOT_8X3 = 0x40

DIRENT_FIXED = 0x1F  # 31 bytes fixed header before name

# HPFS timestamp: seconds since 1970 (local). We stamp a fixed value so images
# are reproducible (the harness forbids wall-clock in generators).
FIXED_TIME = 0x40000000  # 2004-01-10, arbitrary but valid


def de_size(namelen, has_down):
    return ((DIRENT_FIXED + namelen + 3) & ~3) + (4 if has_down else 0)


def not_allowed_char(c):
    return c < 0x20 or c in b'"*/:<>?\\|'


def no_dos_char(c):
    return c in b"+,;=[]"


def is_name_long(name):
    # mirror hpfs_is_name_long
    n = len(name)
    i = 0
    while i < n and name[i] != ord("."):
        if no_dos_char(name[i]):
            return True
        i += 1
    if i == 0 or i > 8:
        return True
    if i == n:
        return False
    for j in range(i + 1, n):
        if name[j] == ord(".") or no_dos_char(name[i]):
            return True
    return (n - i) > 4


def upcase(c):
    # ASCII-only upcase (matches kernel for <128 / ==255; codepage ignored)
    if ord("a") <= c <= ord("z"):
        return c - 0x20
    return c


def compare_names(n1, n2, n2_last):
    # returns <0 if n1 sorts before n2. n2_last => n2 is the \377 sentinel.
    if n2_last:
        return -1
    l = min(len(n1), len(n2))
    for i in range(l):
        c1 = upcase(n1[i])
        c2 = upcase(n2[i])
        if c1 < c2:
            return -1
        if c1 > c2:
            return 1
    if len(n1) < len(n2):
        return -1
    if len(n1) > len(n2):
        return 1
    return 0


class Hpfs:
    def __init__(self, data=None, total_sectors=None):
        if data is not None:
            self.data = bytearray(data)
            self.total_sectors = len(self.data) // SECTOR
        else:
            self.total_sectors = total_sectors
            self.data = bytearray(total_sectors * SECTOR)

    # ---- raw sector access ----
    def rd(self, sec, n=1):
        return bytes(self.data[sec * SECTOR : (sec + n) * SECTOR])

    def wr(self, sec, buf):
        assert len(buf) % SECTOR == 0
        self.data[sec * SECTOR : sec * SECTOR + len(buf)] = buf

    def u16(self, off):
        return struct.unpack_from("<H", self.data, off)[0]

    def u32(self, off):
        return struct.unpack_from("<I", self.data, off)[0]

    def pw32(self, off, v):
        struct.pack_into("<I", self.data, off, v & 0xFFFFFFFF)

    def pw16(self, off, v):
        struct.pack_into("<H", self.data, off, v & 0xFFFF)

    # ================= FORMAT =================
    def format(self, label="RB-HPFS"):
        ts = self.total_sectors
        assert ts >= 2048, "volume too small"
        n_bands = (ts + BAND_SECTORS - 1) // BAND_SECTORS

        # ---- bump-allocate metadata sectors (contiguous from 0x14) ----
        cur = 0x14
        self.bitmap_dir_sec = cur
        cur += 4  # kernel chk_sectors checks 4 sectors here
        self.band_bmp = []
        for _b in range(n_bands):
            self.band_bmp.append(cur)
            cur += 4
        self.dmap_sec = cur
        cur += 4
        self.user_id_sec = cur
        cur += 8
        self.hotfix_sec = cur
        cur += 4
        self.root_fnode = cur
        cur += 1

        # dir band: 4-sector aligned; size multiple of 4, <= 0x4000
        dnodes = max(8, min(ts // 32 // 4, 0x1000))
        dirband_size = dnodes * 4
        cur = (cur + 3) & ~3
        self.dirband_start = cur
        self.dirband_size = dirband_size
        self.dirband_end = self.dirband_start + dirband_size - 1
        cur += dirband_size
        assert cur <= ts, "volume too small for metadata"

        # root dnode = first dnode in the band
        self.root_dno = self.dirband_start

        # ---- init in-memory band bitmaps (bit set = free) ----
        # one bytearray of 2048 bytes per band
        self.bands = [bytearray(b"\xff" * (4 * SECTOR)) for _ in range(n_bands)]
        # mark off-the-end sectors (>= ts) as used
        for s in range(ts, n_bands * BAND_SECTORS):
            self._bmp_set_used(s)
        # system sectors 0..0x11 used
        for s in range(0, 0x12):
            self._bmp_set_used(s)
        # spare sectors 0x12,0x13 used
        self._bmp_set_used(0x12)
        self._bmp_set_used(0x13)
        # bitmap dir
        for s in range(self.bitmap_dir_sec, self.bitmap_dir_sec + 4):
            self._bmp_set_used(s)
        # band bitmaps
        for base in self.band_bmp:
            for s in range(base, base + 4):
                self._bmp_set_used(s)
        # dnode bitmap
        for s in range(self.dmap_sec, self.dmap_sec + 4):
            self._bmp_set_used(s)
        # user id table + hotfix map + root fnode
        for s in range(self.user_id_sec, self.user_id_sec + 8):
            self._bmp_set_used(s)
        for s in range(self.hotfix_sec, self.hotfix_sec + 4):
            self._bmp_set_used(s)
        self._bmp_set_used(self.root_fnode)
        # entire dir band used in main bitmap
        for s in range(self.dirband_start, self.dirband_start + dirband_size):
            self._bmp_set_used(s)

        # ---- dnode bitmap (bit set = free), 1 bit / dnode ----
        self.dmap = bytearray(4 * SECTOR)  # all zero = all used
        n_dnodes = dirband_size // 4
        for d in range(n_dnodes):
            self._dmap_set_free(d)
        self._dmap_set_used(0)  # root dnode

        # ---- boot block ----
        boot = bytearray(SECTOR)
        boot[0:3] = b"\xeb\x3c\x90"
        boot[3:11] = b"IBM 4.50"
        struct.pack_into("<H", boot, 11, SECTOR)  # bytes/sector
        boot[13] = 1  # sectors/cluster
        struct.pack_into("<H", boot, 14, 0)  # reserved sectors
        boot[16] = 0  # n_fats
        struct.pack_into("<H", boot, 17, 0)  # rootdir entries
        struct.pack_into("<H", boot, 19, ts if ts < 0x10000 else 0)  # n_sectors_s
        boot[21] = 0xF8  # media
        struct.pack_into("<H", boot, 22, 0)  # sectors/fat
        struct.pack_into("<H", boot, 24, 63)  # sectors/track
        struct.pack_into("<H", boot, 26, 16)  # heads
        struct.pack_into("<I", boot, 28, 0)  # hidden sectors
        struct.pack_into("<I", boot, 32, ts if ts >= 0x10000 else 0)  # n_sectors_l
        boot[36] = 0x80  # drive number
        boot[38] = 0x28  # sig_28h (HPFS marker, NOT 0x29)
        struct.pack_into("<I", boot, 39, 0x12345678)  # volume serial
        lbl = label.encode("ascii", "replace")[:11].ljust(11, b" ")
        boot[43:54] = lbl
        boot[54:62] = b"HPFS    "
        struct.pack_into("<H", boot, 510, 0xAA55)
        self.wr(0, boot)

        # ---- super block (sector 16) ----
        sb = bytearray(SECTOR)
        struct.pack_into("<I", sb, 0, SB_MAGIC)
        struct.pack_into("<I", sb, 4, SB_MAGIC1)
        sb[8] = 2  # version
        sb[9] = 2  # funcversion
        struct.pack_into("<I", sb, 12, self.root_fnode)  # root fnode
        struct.pack_into("<I", sb, 16, ts)  # n_sectors
        struct.pack_into("<I", sb, 20, 0)  # n_badblocks
        struct.pack_into("<I", sb, 24, self.bitmap_dir_sec)  # bitmaps
        struct.pack_into("<I", sb, 32, 0)  # badblocks list
        struct.pack_into("<I", sb, 40, 0)  # last_chkdsk
        struct.pack_into("<I", sb, 44, 0)  # last_optimize
        struct.pack_into("<I", sb, 48, dirband_size)  # n_dir_band
        struct.pack_into("<I", sb, 52, self.dirband_start)
        struct.pack_into("<I", sb, 56, self.dirband_end)
        struct.pack_into("<I", sb, 60, self.dmap_sec)  # dir_band_bitmap
        struct.pack_into("<I", sb, 96, self.user_id_sec)  # user_id_table
        self.wr(16, sb)

        # ---- spare block (sector 17) ----
        spb = bytearray(SECTOR)
        struct.pack_into("<I", spb, 0, SP_MAGIC)
        struct.pack_into("<I", spb, 4, SP_MAGIC1)
        spb[8] = 0  # flags: clean
        spb[9] = 0
        struct.pack_into("<I", spb, 12, self.hotfix_sec)  # hotfix_map
        struct.pack_into("<I", spb, 16, 0)  # n_spares_used
        struct.pack_into("<I", spb, 20, 0)  # n_spares
        struct.pack_into("<I", spb, 24, 0)  # n_dnode_spares_free
        struct.pack_into("<I", spb, 28, 0)  # n_dnode_spares
        struct.pack_into("<I", spb, 32, 0)  # code_page_dir
        struct.pack_into("<I", spb, 36, 0)  # n_code_pages
        self.wr(17, spb)

        # ---- bitmap directory (sector bitmap_dir_sec) ----
        bd = bytearray(4 * SECTOR)
        for b in range(n_bands):
            struct.pack_into("<I", bd, b * 4, self.band_bmp[b])
        self.wr(self.bitmap_dir_sec, bd)

        # ---- root fnode (directory) ----
        self._write_dir_fnode(self.root_fnode, up=self.root_fnode, name=b"", root_dno=self.root_dno)

        # ---- root dnode: \001\001 + \377 ----
        self._write_empty_dir_dnode(self.root_dno, up=self.root_fnode, self_fnode=self.root_fnode)

        self._flush_bitmaps()

    # ---- main bitmap helpers (bit set = free) ----
    def _bmp_set_used(self, sec):
        band = sec >> 14
        idx = (sec & 0x3FFF) >> 5
        bit = sec & 0x1F
        w = struct.unpack_from("<I", self.bands[band], idx * 4)[0]
        w &= ~(1 << bit)
        struct.pack_into("<I", self.bands[band], idx * 4, w & 0xFFFFFFFF)

    def _bmp_is_free(self, sec):
        band = sec >> 14
        idx = (sec & 0x3FFF) >> 5
        bit = sec & 0x1F
        w = struct.unpack_from("<I", self.bands[band], idx * 4)[0]
        return (w >> bit) & 1

    def _alloc_run(self, n):
        # allocate n contiguous free data sectors (simple first-fit over data area)
        run = 0
        start = 0
        for s in range(0x14, self.total_sectors):
            if self._bmp_is_free(s):
                if run == 0:
                    start = s
                run += 1
                if run == n:
                    for x in range(start, start + n):
                        self._bmp_set_used(x)
                    return start
            else:
                run = 0
        raise RuntimeError("no free run of %d sectors" % n)

    def _flush_bitmaps(self):
        for b, base in enumerate(self.band_bmp):
            self.wr(base, bytes(self.bands[b]))
        self.wr(self.dmap_sec, bytes(self.dmap))

    # ---- dnode bitmap helpers (bit set = free) ----
    def _dmap_set_free(self, d):
        idx = d >> 5
        struct.pack_into(
            "<I", self.dmap, idx * 4,
            (struct.unpack_from("<I", self.dmap, idx * 4)[0] | (1 << (d & 0x1F))) & 0xFFFFFFFF,
        )

    def _dmap_set_used(self, d):
        idx = d >> 5
        struct.pack_into(
            "<I", self.dmap, idx * 4,
            struct.unpack_from("<I", self.dmap, idx * 4)[0] & ~(1 << (d & 0x1F)) & 0xFFFFFFFF,
        )

    def _alloc_dnode(self):
        n = self.dirband_size // 4
        for d in range(n):
            idx = d >> 5
            w = struct.unpack_from("<I", self.dmap, idx * 4)[0]
            if (w >> (d & 0x1F)) & 1:
                self._dmap_set_used(d)
                return self.dirband_start + d * 4
        raise RuntimeError("dir band full")

    # ================= fnode / dnode writers =================
    def _blank_fnode(self, sec, up, name):
        f = bytearray(SECTOR)
        struct.pack_into("<I", f, 0, FNODE_MAGIC)
        nm = name[:15]
        f[12] = len(nm)
        f[13 : 13 + len(nm)] = nm
        struct.pack_into("<I", f, 28, up)
        struct.pack_into("<H", f, 184, 0xC4)  # ea_offs
        return f

    def _write_dir_fnode(self, sec, up, name, root_dno):
        f = self._blank_fnode(sec, up, name)
        struct.pack_into("<H", f, 54, FNODE_FLAG_DIR)  # flags
        # btree header @56: flags, fill[3], n_free, n_used, first_free
        f[60] = 7  # n_free_nodes
        f[61] = 1  # n_used_nodes
        struct.pack_into("<H", f, 62, 0x14)  # first_free
        # external[0] @64: file_secno, length, disk_secno
        struct.pack_into("<I", f, 64, 0xFFFFFFFF)  # file_secno = -1
        struct.pack_into("<I", f, 68, 0)  # length
        struct.pack_into("<I", f, 72, root_dno)  # disk_secno -> root dnode
        struct.pack_into("<I", f, 160, 0)  # file_size
        self.wr(sec, f)

    def _write_file_fnode(self, sec, up, name, size, extents):
        # extents: list of (file_secno, length, disk_secno), up to 8
        f = self._blank_fnode(sec, up, name)
        struct.pack_into("<H", f, 54, 0)  # flags: file
        n_used = len(extents)
        assert n_used <= 8, "writer supports <=8 fnode extents"
        f[60] = 8 - n_used
        f[61] = n_used
        struct.pack_into("<H", f, 62, 8 + n_used * 12)
        for i, (fs, ln, ds) in enumerate(extents):
            base = 64 + i * 12
            struct.pack_into("<I", f, base, fs)
            struct.pack_into("<I", f, base + 4, ln)
            struct.pack_into("<I", f, base + 8, ds)
        struct.pack_into("<I", f, 160, size)  # file_size
        self.wr(sec, f)

    def _write_empty_dir_dnode(self, dno, up, self_fnode):
        d = bytearray(DNODE_SECTORS * SECTOR)
        struct.pack_into("<I", d, 0, DNODE_MAGIC)
        d[8] = 0x01  # root_dnode bit
        struct.pack_into("<I", d, 12, up)
        struct.pack_into("<I", d, 16, dno)  # self
        # build dirents: \001\001 first, then \377 last
        de1 = self._make_dirent(
            name=b"\x01\x01", fnode=self_fnode, size=0,
            flags=DE_FIRST, attrib=AT_DIRECTORY, has_down=False, down=0, last=False,
        )
        de_last = self._make_last_dirent()
        off = 20
        d[off : off + len(de1)] = de1
        off += len(de1)
        d[off : off + len(de_last)] = de_last
        off += len(de_last)
        struct.pack_into("<I", d, 4, off)  # first_free
        self.wr(dno, d)

    def _make_last_dirent(self):
        de = bytearray(32)
        struct.pack_into("<H", de, 0, 32)
        de[2] = DE_LAST
        de[30] = 1  # namelen
        de[31] = 0xFF  # name[0]
        return de

    def _make_dirent(self, name, fnode, size, flags, attrib, has_down, down, last, is_dir=False):
        namelen = len(name)
        length = de_size(namelen, has_down)
        de = bytearray(length)
        struct.pack_into("<H", de, 0, length)
        de[2] = flags | (DE_DOWN if has_down else 0)
        at = attrib
        if is_name_long(name) and not last and namelen:
            at |= AT_NOT_8X3
        de[3] = at
        struct.pack_into("<I", de, 4, fnode)
        struct.pack_into("<I", de, 8, FIXED_TIME)  # write_date
        struct.pack_into("<I", de, 12, size)  # file_size
        struct.pack_into("<I", de, 16, FIXED_TIME)  # read_date
        struct.pack_into("<I", de, 20, FIXED_TIME)  # creation_date
        struct.pack_into("<I", de, 24, 0)  # ea_size
        de[28] = 0  # no_of_acls
        de[29] = 0  # ix
        de[30] = namelen
        de[31 : 31 + namelen] = name
        if has_down:
            struct.pack_into("<I", de, length - 4, down)
        return de

    # ================= populate (single-dnode dirs only) =================
    def add_file(self, parent_dno, name, data):
        name = name.encode() if isinstance(name, str) else name
        size = len(data)
        extents = []
        if size:
            n_sec = (size + SECTOR - 1) // SECTOR
            start = self._alloc_run(n_sec)
            padded = data + b"\x00" * (n_sec * SECTOR - size)
            self.wr(start, padded)
            extents = [(0, n_sec, start)]
        fno = self._alloc_run(1)
        self._write_file_fnode(fno, up=self._dno_fnode(parent_dno), name=name, size=size, extents=extents)
        self._insert_dirent(parent_dno, name, fno, size, attrib=AT_ARCHIVE, is_dir=False)
        return fno

    def add_dir(self, parent_dno, name):
        name = name.encode() if isinstance(name, str) else name
        fno = self._alloc_run(1)
        dno = self._alloc_dnode()
        self._write_dir_fnode(fno, up=self._dno_fnode(parent_dno), name=name, root_dno=dno)
        self._write_empty_dir_dnode(dno, up=fno, self_fnode=fno)
        self._insert_dirent(parent_dno, name, fno, 0, attrib=AT_DIRECTORY, is_dir=True)
        return dno

    def _dno_fnode(self, dno):
        return self.u32(dno * SECTOR + 12)  # dnode.up

    def _insert_dirent(self, dno, name, fnode, size, attrib, is_dir):
        d = bytearray(self.rd(dno, DNODE_SECTORS))
        first_free = struct.unpack_from("<I", d, 4)[0]
        # find insertion offset (sorted); dirents from off 20
        off = 20
        ins = None
        while off < first_free:
            length = struct.unpack_from("<H", d, off)[0]
            deflags = d[off + 2]
            last = bool(deflags & DE_LAST)
            first = bool(deflags & DE_FIRST)
            if last:
                ins = off
                break
            if first:
                off += length
                continue
            namelen = d[off + 30]
            ename = bytes(d[off + 31 : off + 31 + namelen])
            c = compare_names(name, ename, False)
            if c == 0:
                raise RuntimeError("duplicate name")
            if c < 0:
                ins = off
                break
            off += length
        assert ins is not None
        new_de = self._make_dirent(
            name=name, fnode=fnode, size=size, flags=0, attrib=attrib,
            has_down=False, down=0, last=False, is_dir=is_dir,
        )
        newlen = len(new_de)
        if first_free + newlen > DNODE_SECTORS * SECTOR:
            raise RuntimeError("dnode full (oracle writer does not split)")
        d[ins + newlen : first_free + newlen] = d[ins:first_free]
        d[ins : ins + newlen] = new_de
        struct.pack_into("<I", d, 4, first_free + newlen)
        self.wr(dno, d)

    # ================= PARSE / READ =================
    def read_super(self):
        base = 16 * SECTOR
        assert self.u32(base) == SB_MAGIC, "bad super magic"
        assert self.u32(17 * SECTOR) == SP_MAGIC, "bad spare magic"
        self.root_fnode = self.u32(base + 12)
        self.total_sectors = self.u32(base + 16)
        self.bitmap_dir_sec = self.u32(base + 24)
        self.dirband_size = self.u32(base + 48)
        self.dirband_start = self.u32(base + 52)
        self.dirband_end = self.u32(base + 56)
        self.dmap_sec = self.u32(base + 60)
        n_bands = (self.total_sectors + BAND_SECTORS - 1) // BAND_SECTORS
        self.band_bmp = [self.u32(self.bitmap_dir_sec * SECTOR + b * 4) for b in range(n_bands)]

    def fnode_root_dno(self, fno):
        return self.u32(fno * SECTOR + 72)  # external[0].disk_secno

    def _walk_extents(self, fno):
        # yield (file_secno, length, disk_secno) leaves for a file fnode,
        # descending anodes if the btree is internal.
        base = fno * SECTOR
        flags = self.data[base + 56]
        n_used = self.data[base + 61]
        if flags & 0x80:  # internal -> anodes
            for i in range(n_used):
                down = self.u32(base + 64 + i * 8 + 4)
                yield from self._walk_anode(down)
        else:
            for i in range(n_used):
                b = base + 64 + i * 12
                yield (self.u32(b), self.u32(b + 4), self.u32(b + 8))

    def _walk_anode(self, ano):
        base = ano * SECTOR
        assert self.u32(base) == ANODE_MAGIC, "bad anode magic @%x" % ano
        flags = self.data[base + 12]
        n_used = self.data[base + 13]
        if flags & 0x80:
            for i in range(n_used):
                down = self.u32(base + 20 + i * 8 + 4)
                yield from self._walk_anode(down)
        else:
            for i in range(n_used):
                b = base + 20 + i * 12
                yield (self.u32(b), self.u32(b + 4), self.u32(b + 8))

    def read_file(self, fno):
        base = fno * SECTOR
        assert self.u32(base) == FNODE_MAGIC, "bad fnode magic @%x" % fno
        size = self.u32(base + 160)
        out = bytearray(size)
        for (fs, ln, ds) in self._walk_extents(fno):
            for k in range(ln):
                fsec = fs + k
                dsec = ds + k
                pos = fsec * SECTOR
                if pos >= size:
                    continue
                chunk = self.rd(dsec, 1)
                end = min(SECTOR, size - pos)
                out[pos : pos + end] = chunk[:end]
        return bytes(out)

    def _dnode_dirents(self, dno):
        # yield raw dirent dicts in on-disk order for one dnode
        base = dno * SECTOR
        assert self.u32(base) == DNODE_MAGIC, "bad dnode magic @%x" % dno
        first_free = self.u32(base + 4)
        off = 20
        while off < first_free:
            length = self.u16(base + off)
            if length < 32:
                break
            flags = self.data[base + off + 2]
            attrib = self.data[base + off + 3]
            fnode = self.u32(base + off + 4)
            size = self.u32(base + off + 12)
            namelen = self.data[base + off + 30]
            name = bytes(self.data[base + off + 31 : base + off + 31 + namelen])
            down = 0
            if flags & DE_DOWN:
                down = self.u32(base + off + length - 4)
            yield {
                "off": off, "length": length, "flags": flags, "attrib": attrib,
                "fnode": fnode, "size": size, "name": name, "down": down,
                "last": bool(flags & DE_LAST), "first": bool(flags & DE_FIRST),
            }
            off += length

    def list_dir(self, dno):
        # in-order B-tree traversal, skipping first/last sentinels
        out = []
        for de in self._dnode_dirents(dno):
            if de["down"]:
                out.extend(self.list_dir(de["down"]))
            if de["last"] or de["first"]:
                continue
            out.append(de)
        return out

    def tree(self, dno, path=""):
        entries = []
        for de in self.list_dir(dno):
            name = de["name"].decode("latin-1")
            is_dir = bool(de["attrib"] & AT_DIRECTORY)
            node = {
                "name": name, "path": path + "/" + name, "is_dir": is_dir,
                "size": de["size"], "fnode": de["fnode"],
            }
            if is_dir:
                node["children"] = self.tree(self.fnode_root_dno(de["fnode"]), path + "/" + name)
            entries.append(node)
        return entries

    def free_sectors(self):
        cnt = 0
        for band in self.band_bmp:
            buf = self.rd(band, 4)
            for i in range(0, len(buf), 4):
                cnt += bin(struct.unpack_from("<I", buf, i)[0]).count("1")
        return cnt

    # ================= FSCK =================
    def fsck(self):
        errors = []
        base = 16 * SECTOR
        if self.u32(base) != SB_MAGIC:
            errors.append("bad superblock magic")
        if self.u32(17 * SECTOR) != SP_MAGIC:
            errors.append("bad spareblock magic")
        self.read_super()
        # dir band consistency
        if self.dirband_end - self.dirband_start + 1 != self.dirband_size:
            errors.append("dir band size mismatch")
        if self.dirband_size > 0x4000:
            errors.append("dir band too large")
        # walk directory tree, validate each dnode + fnode
        seen = set()

        def chk_dnode(dno, up, is_root):
            b = dno * SECTOR
            if self.u32(b) != DNODE_MAGIC:
                errors.append("bad dnode magic @%x" % dno)
                return
            if self.u32(b + 16) != dno:
                errors.append("dnode self ptr @%x" % dno)
            ff = self.u32(b + 4)
            if ff > 2048:
                errors.append("dnode first_free>2048 @%x" % dno)
                return
            off = 20
            last_seen = False
            while off < ff:
                length = self.u16(b + off)
                if length < 32 or length > 292 or (length & 3) or off + length > 2048:
                    errors.append("bad dirent size @%x+%x" % (dno, off))
                    return
                de_namelen = self.data[b + off + 30]
                de_down = 1 if (self.data[b + off + 2] & DE_DOWN) else 0
                if ((DIRENT_FIXED + de_namelen + de_down * 4 + 3) & ~3) != length:
                    errors.append("namelen/size mismatch @%x+%x" % (dno, off))
                if self.data[b + off + 2] & DE_DOWN:
                    child = self.u32(b + off + length - 4)
                    chk_dnode(child, dno, False)
                if self.data[b + off + 2] & DE_LAST:
                    last_seen = True
                off += length
            if not last_seen:
                errors.append("dnode missing \\377 entry @%x" % dno)

        chk_dnode(self.root_dno if hasattr(self, "root_dno") else self.fnode_root_dno(self.root_fnode),
                  self.root_fnode, True)
        return errors


# ================= CLI =================
def cmd_mkfs(args):
    size_mb = float(args[0])
    out = args[1]
    label = args[2] if len(args) > 2 else "RB-HPFS"
    ts = int(size_mb * 1024 * 1024) // SECTOR
    fs = Hpfs(total_sectors=ts)
    fs.format(label)
    with open(out, "wb") as f:
        f.write(fs.data)
    print("wrote %s (%d sectors, root_fnode=%d root_dno=%d)" % (out, ts, fs.root_fnode, fs.root_dno))


def cmd_build(args):
    # build <img> <size_mb> <srcdir>
    out, size_mb, srcdir = args[0], float(args[1]), args[2]
    ts = int(size_mb * 1024 * 1024) // SECTOR
    fs = Hpfs(total_sectors=ts)
    fs.format("RB-HPFS")

    def recurse(hostdir, dno):
        for name in sorted(os.listdir(hostdir)):
            p = os.path.join(hostdir, name)
            if os.path.isdir(p):
                child = fs.add_dir(dno, name)
                recurse(p, child)
            else:
                with open(p, "rb") as fh:
                    fs.add_file(dno, name, fh.read())

    recurse(srcdir, fs.root_dno)
    fs._flush_bitmaps()
    with open(out, "wb") as f:
        f.write(fs.data)
    print("built %s from %s" % (out, srcdir))


def cmd_ls(args):
    fs = Hpfs(data=open(args[0], "rb").read())
    fs.read_super()
    tree = fs.tree(fs.fnode_root_dno(fs.root_fnode))
    print(json.dumps(tree, indent=2))


def cmd_cat(args):
    fs = Hpfs(data=open(args[0], "rb").read())
    fs.read_super()
    target = args[1]
    parts = [p for p in target.split("/") if p]

    def find(dno, parts):
        for de in fs.list_dir(dno):
            if de["name"].decode("latin-1") == parts[0]:
                if len(parts) == 1:
                    return de
                return find(fs.fnode_root_dno(de["fnode"]), parts[1:])
        return None

    de = find(fs.fnode_root_dno(fs.root_fnode), parts)
    if not de:
        sys.stderr.write("not found: %s\n" % target)
        sys.exit(1)
    sys.stdout.buffer.write(fs.read_file(de["fnode"]))


def cmd_facts(args):
    fs = Hpfs(data=open(args[0], "rb").read())
    fs.read_super()
    facts = {
        "total_sectors": fs.total_sectors,
        "root_fnode": fs.root_fnode,
        "root_dno": fs.fnode_root_dno(fs.root_fnode),
        "bitmap_dir": fs.bitmap_dir_sec,
        "dirband_start": fs.dirband_start,
        "dirband_size": fs.dirband_size,
        "dmap": fs.dmap_sec,
        "free_sectors": fs.free_sectors(),
        "n_bands": len(fs.band_bmp),
    }
    print(json.dumps(facts, indent=2))


def cmd_fsck(args):
    fs = Hpfs(data=open(args[0], "rb").read())
    errors = fs.fsck()
    if errors:
        for e in errors:
            print("ERROR: " + e)
        sys.exit(1)
    print("clean")


def main():
    if len(sys.argv) < 2:
        sys.stderr.write(
            "usage: hpfs-oracle.py {mkfs|build|ls|cat|facts|fsck} ...\n"
            "  mkfs <size_mb> <out.img> [label]\n"
            "  build <out.img> <size_mb> <srcdir>\n"
            "  ls <img> | cat <img> <path> | facts <img> | fsck <img>\n"
        )
        sys.exit(2)
    cmd = sys.argv[1]
    rest = sys.argv[2:]
    {
        "mkfs": cmd_mkfs, "build": cmd_build, "ls": cmd_ls,
        "cat": cmd_cat, "facts": cmd_facts, "fsck": cmd_fsck,
    }[cmd](rest)


if __name__ == "__main__":
    main()
