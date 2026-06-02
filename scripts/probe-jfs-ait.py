#!/usr/bin/env python3
"""Probe the JFS2 fixture for the Aggregate Inode Table (AIT).

Uses the kernel-confirmed offsets:
  SUPER1_OFF = 0x8000        (32768)
  SIZE_OF_SUPER = PSIZE = 4096
  AIMAP_OFF = SUPER1_OFF + SIZE_OF_SUPER = 0x9000 (36864)
  SIZE_OF_MAP_PAGE = PSIZE = 4096
  AITBL_OFF = AIMAP_OFF + (SIZE_OF_MAP_PAGE << 1) = 0xB000 (45056)
  DISIZE = 512
  INOSPEREXT = 32
  INODE_EXTENT_SIZE = INOSPEREXT * DISIZE = 16384 bytes
  AGGREGATE_I = 1, BMAP_I = 2, LOG_I = 3, BADBLOCK_I = 4, FILESYSTEM_I = 16

Dinode field layout (verbatim from jfs_dinode.h):
  +0   di_inostamp  u32
  +4   di_fileset   u32
  +8   di_number    u32
  +12  di_gen       u32
  +16  di_ixpxd     pxd_t (8 bytes)
  +24  di_size      u64
  +32  di_nblocks   u64
  +40  di_nlink     u32
  +44  di_uid       u32
  +48  di_gid       u32
  +52  di_mode      u32
  +224 di_xtroot    xtroot_t (288 bytes, file/symlink)
  +224 di_dtroot    dtroot_t (288 bytes, directory)
"""
import zstandard as zstd
import struct
from pathlib import Path

root = Path(__file__).resolve().parent.parent
with open(root / 'tests' / 'fixtures' / 'test_jfs.img.zst', 'rb') as f:
    data = zstd.ZstdDecompressor().decompress(f.read())

AITBL_OFF = 0xB000
DISIZE = 512
INOSPEREXT = 32

NAMED_INODES = {0: 'reserved', 1: 'AGGREGATE_I', 2: 'BMAP_I', 3: 'LOG_I',
                4: 'BADBLOCK_I', 16: 'FILESYSTEM_I'}

def parse_pxd(buf, off):
    """8-byte pxd_t: word0 = (length:24 + addr_hi:8), word1 = addr_lo:32."""
    word0 = struct.unpack_from('<I', buf, off)[0]
    word1 = struct.unpack_from('<I', buf, off + 4)[0]
    length = word0 & 0x00FF_FFFF
    addr_hi = (word0 >> 24) & 0xFF
    address = (addr_hi << 32) | word1
    return length, address

def fmt_dinode(buf, off, ino_idx):
    if len(buf) < off + 512:
        return None
    inostamp = struct.unpack_from('<I', buf, off + 0)[0]
    fileset = struct.unpack_from('<I', buf, off + 4)[0]
    di_num = struct.unpack_from('<I', buf, off + 8)[0]
    di_gen = struct.unpack_from('<I', buf, off + 12)[0]
    ixpxd_len, ixpxd_addr = parse_pxd(buf, off + 16)
    di_size = struct.unpack_from('<Q', buf, off + 24)[0]
    di_nblocks = struct.unpack_from('<Q', buf, off + 32)[0]
    di_nlink = struct.unpack_from('<I', buf, off + 40)[0]
    di_uid = struct.unpack_from('<I', buf, off + 44)[0]
    di_gid = struct.unpack_from('<I', buf, off + 48)[0]
    di_mode = struct.unpack_from('<I', buf, off + 52)[0]
    return {
        'idx': ino_idx, 'name': NAMED_INODES.get(ino_idx, ''),
        'inostamp': inostamp, 'fileset': fileset, 'di_number': di_num,
        'gen': di_gen, 'ixpxd_len': ixpxd_len, 'ixpxd_addr': ixpxd_addr,
        'size': di_size, 'nblocks': di_nblocks, 'nlink': di_nlink,
        'uid': di_uid, 'gid': di_gid, 'mode': di_mode,
    }


print(f'AIT at byte 0x{AITBL_OFF:X} ({AITBL_OFF})')
print()

# Read all 32 inodes in the first AIT extent.
for ino_idx in range(INOSPEREXT):
    inode_off = AITBL_OFF + ino_idx * DISIZE
    d = fmt_dinode(data, inode_off, ino_idx)
    if d is None:
        break
    # Skip clearly-empty slots (mode = 0).
    if d['mode'] == 0 and d['di_number'] == 0 and d['size'] == 0 and d['nblocks'] == 0:
        continue
    mode_str = f'0o{d["mode"]:08o}' + (f' (IFJOURNAL+IDIR)' if d['mode'] & 0x20010000 == 0x20010000 else '')
    name = f' [{d["name"]}]' if d['name'] else ''
    print(f'  inode {d["idx"]:2}{name} @ 0x{inode_off:X}:')
    print(f'    inostamp={d["inostamp"]:#x} fileset={d["fileset"]} di_number={d["di_number"]} gen={d["gen"]}')
    print(f'    ixpxd len={d["ixpxd_len"]} addr={d["ixpxd_addr"]}')
    print(f'    size={d["size"]} nblocks={d["nblocks"]} nlink={d["nlink"]} mode={mode_str}')
    # If it's an inode-with-xtree, dump the xtree root header at +224.
    xtr_off = inode_off + 224
    if xtr_off + 32 <= len(data):
        xt_next = struct.unpack_from('<Q', data, xtr_off + 0)[0]
        xt_prev = struct.unpack_from('<Q', data, xtr_off + 8)[0]
        xt_flag = data[xtr_off + 16]
        xt_nextindex = struct.unpack_from('<H', data, xtr_off + 18)[0]
        xt_maxentry = struct.unpack_from('<H', data, xtr_off + 20)[0]
        xt_self_len, xt_self_addr = parse_pxd(data, xtr_off + 24)
        # Decode XAD entries (16 bytes each, starting after the 32-byte header)
        print(f'    xtroot: flag=0x{xt_flag:02X} nextindex={xt_nextindex} maxentry={xt_maxentry} self=(len={xt_self_len},addr={xt_self_addr})')
        for xad_idx in range(min(xt_nextindex, 16)):
            xad_off = xtr_off + 32 + xad_idx * 16
            xad_flag = data[xad_off + 0]
            xad_off1 = data[xad_off + 3]
            xad_off2 = struct.unpack_from('<I', data, xad_off + 4)[0]
            xad_log_off = (xad_off1 << 32) | xad_off2
            xad_loc_len, xad_loc_addr = parse_pxd(data, xad_off + 8)
            print(f'      xad[{xad_idx}]: flag=0x{xad_flag:02X} log_off_blk={xad_log_off} loc=(len={xad_loc_len},addr={xad_loc_addr})')
    print()
