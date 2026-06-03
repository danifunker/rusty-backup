#!/usr/bin/env python3
"""Probe the fileset-1 root dtree."""
import zstandard as zstd
import struct
from pathlib import Path

root = Path(__file__).resolve().parent.parent
with open(root / 'tests' / 'fixtures' / 'test_jfs.img.zst', 'rb') as f:
    data = zstd.ZstdDecompressor().decompress(f.read())

ROOT_INODE_BYTE = 28 * 4096 + 2 * 512  # inoext[0].addr=28, fileset inum 2
ino = data[ROOT_INODE_BYTE:ROOT_INODE_BYTE + 512]
print(f'=== fileset 1 root inode @ byte 0x{ROOT_INODE_BYTE:X} ===')
print(f'  di_inostamp = 0x{struct.unpack_from("<I", ino, 0)[0]:x}')
print(f'  di_fileset  = {struct.unpack_from("<I", ino, 4)[0]}')
print(f'  di_number   = {struct.unpack_from("<I", ino, 8)[0]}')
print(f'  di_size     = {struct.unpack_from("<Q", ino, 24)[0]}')
print(f'  di_nblocks  = {struct.unpack_from("<Q", ino, 32)[0]}')
print(f'  di_nlink    = {struct.unpack_from("<I", ino, 40)[0]}')
print(f'  di_mode     = 0o{struct.unpack_from("<I", ino, 52)[0]:o}')

dtroot = ino[224:224 + 288]
flag = dtroot[16]
nextindex = dtroot[17]
freecnt = struct.unpack_from('<b', dtroot, 18)[0]
freelist = struct.unpack_from('<b', dtroot, 19)[0]
idotdot = struct.unpack_from('<I', dtroot, 20)[0]
stbl = list(dtroot[24:32])
print('\n=== dtroot @ inode +224 ===')
print(f'  DASD (16 bytes) hex: {dtroot[:16].hex()}')
print(f'  flag          = 0x{flag:02X}')
print(f'  nextindex     = {nextindex}')
print(f'  freecnt       = {freecnt}')
print(f'  freelist      = {freelist}')
print(f'  idotdot       = {idotdot}')
print(f'  stbl[8]       = {stbl}')

print('\n--- entries (in stbl order) ---')
for i in range(nextindex):
    slot_idx = stbl[i]
    if slot_idx < 0 or slot_idx >= 9:
        print(f'  stbl[{i}]={slot_idx}: skip (out of range)')
        continue
    slot = dtroot[32 + slot_idx * 32:32 + (slot_idx + 1) * 32]
    inum = struct.unpack_from('<I', slot, 0)[0]
    nxt = struct.unpack_from('<b', slot, 4)[0]
    namlen = slot[5]
    chars = []
    take = min(11, namlen)
    for c in range(take):
        cp = struct.unpack_from('<H', slot, 6 + c * 2)[0]
        if 0x20 <= cp < 0x7f:
            chars.append(chr(cp))
        else:
            chars.append('?')
    name = ''.join(chars)
    print(f'  stbl[{i}]={slot_idx} -> slot[{slot_idx}]: inum={inum} next={nxt} namlen={namlen} name={name!r}')
    if namlen > 11 and 0 <= nxt < 9:
        cont_slot = dtroot[32 + nxt * 32:32 + (nxt + 1) * 32]
        cont_next = struct.unpack_from('<b', cont_slot, 0)[0]
        cont_cnt = struct.unpack_from('<b', cont_slot, 1)[0]
        cont_chars = []
        for c in range(min(15, cont_cnt)):
            cp = struct.unpack_from('<H', cont_slot, 2 + c * 2)[0]
            if 0x20 <= cp < 0x7f:
                cont_chars.append(chr(cp))
        print(f'    continuation slot {nxt}: next={cont_next} cnt={cont_cnt} chars={"".join(cont_chars)!r}')

# Also dump the other fileset-1 inodes (0-7 from pmap[0]=0xFF000000)
print('\n--- fileset 1 inodes 0..7 mode/size summary ---')
for ino_idx in range(8):
    di_off = 28 * 4096 + ino_idx * 512
    di = data[di_off:di_off + 512]
    di_number = struct.unpack_from('<I', di, 8)[0]
    di_size = struct.unpack_from('<Q', di, 24)[0]
    di_nlink = struct.unpack_from('<I', di, 40)[0]
    di_mode = struct.unpack_from('<I', di, 52)[0]
    ftype = di_mode & 0o170000
    ftype_name = {0o100000: 'REG', 0o040000: 'DIR', 0o120000: 'LNK',
                  0o060000: 'BLK', 0o020000: 'CHR', 0o010000: 'FIFO',
                  0o140000: 'SOCK'}.get(ftype, f'0o{ftype:o}')
    print(f'  inum {ino_idx} (di_number={di_number}): size={di_size} nlink={di_nlink} type={ftype_name}')
