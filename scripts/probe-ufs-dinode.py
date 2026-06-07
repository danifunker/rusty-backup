#!/usr/bin/env python3
"""Probe UFS1 / UFS2 fixtures for SB geometry and root-inode layout.

Used during the U.3 implementation to confirm fs_iblkno, fs_maxsymlinklen,
and the on-disk dinode field offsets used by src/fs/ufs.rs::read_inode.
"""
import zstandard as zstd
import struct
import sys
from pathlib import Path

FIXTURES = [
    ('test_ufs1.img.zst', False),
    ('test_ufs2.img.zst', True),
]

root = Path(__file__).resolve().parent.parent
for name, is_ufs2 in FIXTURES:
    with open(root / 'tests' / 'fixtures' / name, 'rb') as f:
        data = zstd.ZstdDecompressor().decompress(f.read())
    sb_off = 8192
    def u32(off): return struct.unpack_from('<I', data, sb_off+off)[0]
    def i32(off): return struct.unpack_from('<i', data, sb_off+off)[0]
    def i64(off): return struct.unpack_from('<q', data, sb_off+off)[0]

    print(f'=== {name} ===')
    print(f'  fs_sblkno     (+8):    {i32(8)}')
    print(f'  fs_cblkno    (+12):    {i32(12)}')
    print(f'  fs_iblkno    (+16):    {i32(16)}')
    print(f'  fs_dblkno    (+20):    {i32(20)}')
    print(f'  fs_old_size  (+36):    {i32(36)}')
    print(f'  fs_ncg       (+44):    {u32(44)}')
    print(f'  fs_bsize     (+48):    {i32(48)}')
    print(f'  fs_fsize     (+52):    {i32(52)}')
    print(f'  fs_frag      (+56):    {i32(56)}')
    print(f'  fs_ipg       (+184):   {u32(184)}')
    print(f'  fs_fpg       (+188):   {i32(188)}')
    print(f'  fs_size      (+1080):  {i64(1080)}')
    print(f'  fs_maxsymlinklen (+1320): {i32(1320)}')
    print(f'  fs_magic     (+1372):  0x{u32(1372):08X}')

    iblkno = i32(16)
    fsize = i32(52)
    ipg = u32(184)
    fpg = i32(188)
    dsize = 256 if is_ufs2 else 128
    for inum in (2,):
        cg = inum // ipg
        in_cg = inum % ipg
        inode_byte = (cg * fpg + iblkno) * fsize + in_cg * dsize
        print(f'  Inode {inum} byte offset: {inode_byte}')
        ino = data[inode_byte:inode_byte+dsize]
        mode = struct.unpack_from('<H', ino, 0)[0]
        nlink = struct.unpack_from('<h', ino, 2)[0]
        print(f'    di_mode  = 0o{mode:o}')
        print(f'    di_nlink = {nlink}')
        if is_ufs2:
            uid = struct.unpack_from('<I', ino, 4)[0]
            gid = struct.unpack_from('<I', ino, 8)[0]
            size = struct.unpack_from('<Q', ino, 16)[0]
            blocks = struct.unpack_from('<Q', ino, 24)[0]
            mtime = struct.unpack_from('<q', ino, 40)[0]
            print(f'    di_uid={uid} di_gid={gid} di_size={size} di_blocks={blocks} di_mtime={mtime}')
            for i in range(12):
                blk = struct.unpack_from('<q', ino, 112+i*8)[0]
                if blk: print(f'    di_db[{i}] = {blk}')
            for i in range(3):
                blk = struct.unpack_from('<q', ino, 208+i*8)[0]
                if blk: print(f'    di_ib[{i}] = {blk}')
        else:
            size = struct.unpack_from('<Q', ino, 8)[0]
            mtime = struct.unpack_from('<i', ino, 24)[0]
            uid = struct.unpack_from('<I', ino, 112)[0]
            gid = struct.unpack_from('<I', ino, 116)[0]
            print(f'    di_size={size} di_mtime={mtime} di_uid={uid} di_gid={gid}')
            for i in range(12):
                blk = struct.unpack_from('<i', ino, 40+i*4)[0]
                if blk: print(f'    di_db[{i}] = {blk}')
            for i in range(3):
                blk = struct.unpack_from('<i', ino, 88+i*4)[0]
                if blk: print(f'    di_ib[{i}] = {blk}')

    # Root dir block: read first directory block (di_db[0] * fsize)
    if is_ufs2:
        db0 = struct.unpack_from('<q', data, sb_off + (8192*0) +  # placeholder
                                 (0))[0]
    print()
