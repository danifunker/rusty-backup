#!/usr/bin/env python3
"""Probe the JFS2 BMAP control inode's data area.

BMAP inode (inum 2 of AIT) points at 6 aggregate blocks starting at
block 16 (= byte 65536). Layout (from jfs_dmap.h):
  Page 0: dbmap_disk (BMAP control header) — 4096 bytes
  Page 1: dmap_disk[0] (first leaf, covers 8192 blocks at L2BPERDMAP=13)
  Pages 2-?: more dmap leaves or dmapctl internal nodes
"""
import zstandard as zstd
import struct
from pathlib import Path

root = Path(__file__).resolve().parent.parent
with open(root / 'tests' / 'fixtures' / 'test_jfs.img.zst', 'rb') as f:
    data = zstd.ZstdDecompressor().decompress(f.read())

PSIZE = 4096
BMAP_BLK = 16  # from inode 2 xtree[0].loc.addr
BMAP_LEN = 6
BMAP_BYTE = BMAP_BLK * PSIZE

print(f'BMAP control inode data @ block {BMAP_BLK} = byte 0x{BMAP_BYTE:X} ({BMAP_BYTE})')

# Page 0: dbmap_disk header
print('\n=== Page 0: dbmap_disk (4096 bytes) ===')
dbmap = data[BMAP_BYTE:BMAP_BYTE + PSIZE]
print(f'  dn_mapsize    : {struct.unpack_from("<q", dbmap, 0)[0]}    (# agg blocks)')
print(f'  dn_nfree      : {struct.unpack_from("<q", dbmap, 8)[0]}    (# free agg blocks)')
print(f'  dn_l2nbperpage: {struct.unpack_from("<i", dbmap, 16)[0]}')
print(f'  dn_numag      : {struct.unpack_from("<i", dbmap, 20)[0]}')
print(f'  dn_maxlevel   : {struct.unpack_from("<i", dbmap, 24)[0]}    (depth of dmap tree)')
print(f'  dn_maxag      : {struct.unpack_from("<i", dbmap, 28)[0]}')
print(f'  dn_agpref     : {struct.unpack_from("<i", dbmap, 32)[0]}')
print(f'  dn_aglevel    : {struct.unpack_from("<i", dbmap, 36)[0]}')
print(f'  dn_agheight   : {struct.unpack_from("<i", dbmap, 40)[0]}')
print(f'  dn_agwidth    : {struct.unpack_from("<i", dbmap, 44)[0]}')
print(f'  dn_agstart    : {struct.unpack_from("<i", dbmap, 48)[0]}')
print(f'  dn_agl2size   : {struct.unpack_from("<i", dbmap, 52)[0]}')
# dn_agfree[MAXAG=128]: 8 bytes each, starts at offset 56
print('  dn_agfree[0..3]:', end=' ')
for i in range(4):
    v = struct.unpack_from('<q', dbmap, 56 + i * 8)[0]
    print(f'AG{i}={v}', end=' ')
print()
# dn_agsize after dn_agfree array (56 + 128*8 = 1080)
agsize_off = 56 + 128 * 8
print(f'  dn_agsize     : {struct.unpack_from("<q", dbmap, agsize_off)[0]}')
print(f'  dn_maxfreebud : {struct.unpack_from("<b", dbmap, agsize_off + 8)[0]}')

# Page 1: dmap_disk (first dmap leaf)
print('\n=== Page 1: dmap_disk (first leaf, 4096 bytes) ===')
dmap = data[BMAP_BYTE + PSIZE:BMAP_BYTE + 2*PSIZE]
print(f'  nblocks  : {struct.unpack_from("<i", dmap, 0)[0]}    (# agg blocks covered by this dmap)')
print(f'  nfree    : {struct.unpack_from("<i", dmap, 4)[0]}    (# free blocks in this dmap)')
print(f'  start    : {struct.unpack_from("<q", dmap, 8)[0]}    (first agg block covered)')
# dmaptree at offset 16 ... let me check
# Actually struct dmap is:
#   nblocks  u32 +0
#   nfree    u32 +4
#   start    u64 +8
#   tree     dmaptree (variable size)
#   pad[1672]
#   wmap[LPERDMAP] u32 each, total 1024 bytes
#   pmap[LPERDMAP] u32 each, total 1024 bytes
# Layout: header (16) + tree (?) + pad(1672) + wmap(1024) + pmap(1024) = 4096
# So tree = 4096 - 16 - 1672 - 1024 - 1024 = 360 bytes
LPERDMAP = 256  # = BPERDMAP / DBWORD = 8192 / 32 = 256
wmap_off = 4096 - 2 * LPERDMAP * 4  # = 4096 - 2048 = 2048
pmap_off = wmap_off + LPERDMAP * 4  # = 3072

print(f'\n  --- pmap[256] (persistent map, each word covers 32 blocks) ---')
print(f'  pmap offset within dmap page: {pmap_off}')
print(f'  First 8 pmap words (covering blocks 0..255):')
for i in range(8):
    w = struct.unpack_from('<I', dmap, pmap_off + i * 4)[0]
    print(f'    pmap[{i:3}] (blocks {i*32:4}..{(i+1)*32-1:4}): 0x{w:08X}  popcount={bin(w).count("1")}')

# Find highest set bit across all pmap words
highest_alloc = -1
for i in range(LPERDMAP):
    w = struct.unpack_from('<I', dmap, pmap_off + i * 4)[0]
    if w:
        for bit in range(31, -1, -1):
            if w & (1 << bit):
                # JFS bit order: MSB of word = lowest block? Or LSB?
                # We'll assume MSB-first within a word (bit 31 = block 0 of word, bit 0 = block 31).
                # Actually JFS uses MSB-first per kernel source:
                #   #define DMAPBIT(b)  (1 << (DBWORD - 1 - ((b) & (DBWORD - 1))))
                # where DBWORD = 32.
                block_in_word = 31 - bit
                global_block = i * 32 + block_in_word
                highest_alloc = max(highest_alloc, global_block)
                break

print(f'\n  Highest allocated block per pmap: {highest_alloc}')
print(f'  → last_data_byte = ({highest_alloc + 1}) * 4096 = {(highest_alloc + 1) * 4096}')

# Also compare with wmap
print(f'\n  First 8 wmap words (working map, same encoding):')
for i in range(8):
    w = struct.unpack_from('<I', dmap, wmap_off + i * 4)[0]
    print(f'    wmap[{i:3}] (blocks {i*32:4}..{(i+1)*32-1:4}): 0x{w:08X}  popcount={bin(w).count("1")}')
