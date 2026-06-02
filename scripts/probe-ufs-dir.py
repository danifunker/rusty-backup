#!/usr/bin/env python3
"""Decode the root + subdir DIRENT2 records from the UFS fixtures.

Confirms the on-disk DIRENT2 layout used by src/fs/ufs.rs::list_directory:
  d_ino    u32
  d_reclen u16
  d_type   u8
  d_namlen u8
  name + NUL pad to 4-byte boundary.
"""
import zstandard as zstd
import struct
from pathlib import Path

D_TYPE = {0: 'UNK', 1: 'FIFO', 2: 'CHR', 4: 'DIR', 6: 'BLK', 8: 'REG', 10: 'LNK', 12: 'SOCK'}

root = Path(__file__).resolve().parent.parent

def parse_dirblock(blk, label):
    print(f'  --- {label} ({len(blk)} B) ---')
    off = 0
    while off + 8 <= len(blk):
        d_ino = struct.unpack_from('<I', blk, off)[0]
        d_reclen = struct.unpack_from('<H', blk, off + 4)[0]
        d_type = blk[off + 6]
        d_namlen = blk[off + 7]
        if d_reclen == 0:
            print(f'    @{off}: zero reclen, stopping')
            break
        name_end = off + 8 + d_namlen
        if name_end > len(blk):
            print(f'    @{off}: name overrun')
            break
        name = blk[off + 8:name_end].decode('latin-1')
        print(f'    @{off}: ino={d_ino} reclen={d_reclen} type={D_TYPE.get(d_type, d_type)} name={name!r}')
        if d_ino == 0:
            # End-of-block marker.
            pass
        off += d_reclen
        if d_reclen % 4 != 0:
            print(f'    WARN: reclen {d_reclen} not 4-aligned')

for name, is_ufs2 in [('test_ufs1.img.zst', False), ('test_ufs2.img.zst', True)]:
    with open(root / 'tests' / 'fixtures' / name, 'rb') as f:
        data = zstd.ZstdDecompressor().decompress(f.read())
    sb_off = 8192
    def u32(off): return struct.unpack_from('<I', data, sb_off+off)[0]
    def i32(off): return struct.unpack_from('<i', data, sb_off+off)[0]
    iblkno = i32(16)
    fsize = i32(52)
    ipg = u32(184)
    fpg = i32(188)
    bsize = i32(48)
    dsize = 256 if is_ufs2 else 128
    print(f'=== {name} ===')

    def read_inode(inum):
        cg = inum // ipg
        in_cg = inum % ipg
        inode_byte = (cg * fpg + iblkno) * fsize + in_cg * dsize
        return data[inode_byte:inode_byte+dsize]

    def db0_byte(inode_bytes):
        if is_ufs2:
            blk = struct.unpack_from('<q', inode_bytes, 112)[0]
        else:
            blk = struct.unpack_from('<i', inode_bytes, 40)[0]
        return blk * fsize

    # Root directory inode 2.
    root_ino = read_inode(2)
    if is_ufs2:
        rsize = struct.unpack_from('<Q', root_ino, 16)[0]
    else:
        rsize = struct.unpack_from('<Q', root_ino, 8)[0]
    root_db = db0_byte(root_ino)
    parse_dirblock(data[root_db:root_db + rsize], f'root @ db {root_db//fsize} ({rsize} B)')

    # Walk through entries: find 'subdir' inode + dump its content.
    subdir_ino = None
    off = 0
    rb = data[root_db:root_db + rsize]
    while off + 8 <= len(rb):
        d_ino = struct.unpack_from('<I', rb, off)[0]
        d_reclen = struct.unpack_from('<H', rb, off + 4)[0]
        d_namlen = rb[off + 7]
        if d_reclen == 0:
            break
        name = rb[off + 8:off + 8 + d_namlen].decode('latin-1')
        if name == 'subdir':
            subdir_ino = d_ino
        elif name == 'link.txt':
            link_ino = d_ino
            ino = read_inode(link_ino)
            if is_ufs2:
                lsize = struct.unpack_from('<Q', ino, 16)[0]
            else:
                lsize = struct.unpack_from('<Q', ino, 8)[0]
            print(f'  link.txt inode={link_ino} size={lsize} (inline limit={"120" if is_ufs2 else "60"})')
            target_start = 112 if is_ufs2 else 40
            target = ino[target_start:target_start + lsize].decode('latin-1')
            print(f'    inline symlink target = {target!r}')
        elif name == 'large.bin':
            big_ino = d_ino
            ino = read_inode(big_ino)
            if is_ufs2:
                bsize_ino = struct.unpack_from('<Q', ino, 16)[0]
                blks = [struct.unpack_from('<q', ino, 112+i*8)[0] for i in range(12)]
                inds = [struct.unpack_from('<q', ino, 208+i*8)[0] for i in range(3)]
            else:
                bsize_ino = struct.unpack_from('<Q', ino, 8)[0]
                blks = [struct.unpack_from('<i', ino, 40+i*4)[0] for i in range(12)]
                inds = [struct.unpack_from('<i', ino, 88+i*4)[0] for i in range(3)]
            print(f'  large.bin inode={big_ino} size={bsize_ino}')
            print(f'    direct = {[b for b in blks if b != 0]}')
            print(f'    indirect = {[b for b in inds if b != 0]}')
        off += d_reclen

    if subdir_ino:
        sino = read_inode(subdir_ino)
        if is_ufs2:
            ssize = struct.unpack_from('<Q', sino, 16)[0]
        else:
            ssize = struct.unpack_from('<Q', sino, 8)[0]
        sdb = db0_byte(sino)
        parse_dirblock(data[sdb:sdb+ssize], f'subdir @ db {sdb//fsize} ({ssize} B)')
    print()
