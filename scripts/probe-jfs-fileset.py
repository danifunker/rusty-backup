#!/usr/bin/env python3
"""Probe the JFS2 fileset 1 (user fileset) layout.

Path: AIT inode 16 (FILESYSTEM_I) → xtree[0] → fileset's IMAP +
inode table. From there, fileset's inode 2 = user root directory.

Layout per `jfsutils/include/jfs_imap.h`:
  Page 0 of FILESYSTEM_I data = dinomap_disk (IMAP descriptor)
  Followed by a chain of IAGs (each 4 KiB), each describing 4096
  fileset inodes via a 128-element extent map (extspace[]) plus
  alloc/free bitmaps.

The fileset's "root inode" is fileset inode 2 (FILESYSTEM_I_TO_AGGREGATE
maps it to a position via the first IAG's `inoext[0]` pxd).
"""
import zstandard as zstd
import struct
from pathlib import Path

root = Path(__file__).resolve().parent.parent
with open(root / 'tests' / 'fixtures' / 'test_jfs.img.zst', 'rb') as f:
    data = zstd.ZstdDecompressor().decompress(f.read())

PSIZE = 4096

# FILESYSTEM_I's xtree[0] = (len=2, addr=32) → 2 agg blocks at block 32 = byte 0x20000.
FILESYSTEM_BLK = 32
FILESYSTEM_BYTE = FILESYSTEM_BLK * PSIZE

print(f'FILESYSTEM_I (inum 16) data @ block {FILESYSTEM_BLK} = byte 0x{FILESYSTEM_BYTE:X}')

# Page 0 = dinomap_disk (IMAP control). Per jfs_imap.h.
print('\n=== Page 0: dinomap_disk (4096 bytes) ===')
page = data[FILESYSTEM_BYTE:FILESYSTEM_BYTE + PSIZE]
print(f'  in_freeiag      (+0,   u32): {struct.unpack_from("<I", page, 0)[0]}')
print(f'  in_nextiag      (+4,   u32): {struct.unpack_from("<I", page, 4)[0]}')
print(f'  in_numinos      (+8,   u32): {struct.unpack_from("<I", page, 8)[0]}')
print(f'  in_numfree      (+12,  u32): {struct.unpack_from("<I", page, 12)[0]}')
# Skipping the iagctl[] array; iag offset chains follow
# Hex dump first 256 bytes
print('  first 64 bytes (hex):')
for off in range(0, 64, 16):
    print(f'    +0x{off:02X}: {page[off:off+16].hex()}')

# Page 1 = IAG 0 (first Inode Allocation Group)
print('\n=== Page 1: iag (Inode Allocation Group 0, 4096 bytes) ===')
iag = data[FILESYSTEM_BYTE + PSIZE:FILESYSTEM_BYTE + 2*PSIZE]
print(f'  agstart       (+0,  u64): {struct.unpack_from("<Q", iag, 0)[0]}')
print(f'  iagnum        (+8,  u32): {struct.unpack_from("<I", iag, 8)[0]}')
print(f'  inofreefwd    (+12, u32): {struct.unpack_from("<I", iag, 12)[0]}')
print(f'  inofreeback   (+16, u32): {struct.unpack_from("<I", iag, 16)[0]}')
print(f'  extfreefwd    (+20, u32): {struct.unpack_from("<I", iag, 20)[0]}')
print(f'  extfreeback   (+24, u32): {struct.unpack_from("<I", iag, 24)[0]}')
print(f'  iagfree       (+28, u32): {struct.unpack_from("<I", iag, 28)[0]}')
print(f'  nfreeinos     (+32, u32): {struct.unpack_from("<I", iag, 32)[0]}')
print(f'  nfreeexts     (+36, u32): {struct.unpack_from("<I", iag, 36)[0]}')
# Then inosmap, extsmap, wmap, pmap, inoext arrays follow
# inoext[EXTSPERIAG=128] starts somewhere. Each inoext is a pxd_t (8 bytes).
# Layout per kernel:
#   u64 agstart, u32 iagnum, ... headers ... (~40 bytes)
#   inosmap[SMAPSZ]    (LE u32 array)
#   extsmap[SMAPSZ]    (LE u32 array)
#   wmap[EXTSPERIAG]   (LE u32 array, EXTSPERIAG=128, so 128*4=512 bytes)
#   pmap[EXTSPERIAG]   (LE u32 array)
#   inoext[EXTSPERIAG] (pxd_t array, 128 * 8 = 1024 bytes)

# Print first 8 inoext PXDs to find user-inode locations.
# Header is 40 bytes + inosmap(?) + extsmap(?) + wmap(512) + pmap(512) before inoext.
# Per jfs_imap.h: SMAPSZ = EXTSPERIAG / 32 = 4. So inosmap[4] + extsmap[4] = 16 + 16 = 32 bytes.
# Then wmap[128] + pmap[128] = 512 + 512 = 1024 bytes.
# So inoext starts at 40 + 32 + 1024 = 1096... but we should verify.
# Actually the header per kernel is bigger:
# struct iag {
#   __le64 agstart;             /*  8 */
#   __le32 iagnum;              /*  4 */
#   __le32 inofreefwd;          /*  4 */
#   __le32 inofreeback;         /*  4 */
#   __le32 extfreefwd;          /*  4 */
#   __le32 extfreeback;         /*  4 */
#   __le32 iagfree;             /*  4 */
#   __le32 nfreeinos;           /*  4 */
#   __le32 nfreeexts;           /*  4 */
#   u8 pad[44];                  /* 44 */
#   __le32 inosmap[SMAPSZ];      /* SMAPSZ=4 */
#   __le32 extsmap[SMAPSZ];
#   __le32 inoamap[EXTSPERIAG];  /* EXTSPERIAG=128 */
#   __le32 inopmap[EXTSPERIAG];
#   pxd_t inoext[EXTSPERIAG];
# };
# Header bytes: 8+4+4+4+4+4+4+4+4+44 = 84 bytes
# inosmap+extsmap = 4*4 + 4*4 = 32 bytes
# inoamap+inopmap = 128*4 + 128*4 = 1024 bytes
# inoext[128] = 128 * 8 = 1024 bytes
# Total: 84 + 32 + 1024 + 1024 = 2164 bytes (leaving padding)

inoext_off = 84 + 32 + 1024
print(f'\n  inoext[] starts at offset {inoext_off}')
for i in range(8):
    pxd_off = inoext_off + i*8
    word0 = struct.unpack_from('<I', iag, pxd_off)[0]
    word1 = struct.unpack_from('<I', iag, pxd_off + 4)[0]
    length = word0 & 0x00FFFFFF
    addr_hi = (word0 >> 24) & 0xFF
    address = (addr_hi << 32) | word1
    print(f'    inoext[{i}]: len={length}, addr={address}, byte=0x{address*PSIZE:X}')

# inoext[0] points to the first user-inode extent. It contains 32 dinodes
# (INOSPEREXT=32). Fileset inode 2 = root dir is at index 2 of this extent.
# Each dinode is 512 bytes.
inoext0_word0 = struct.unpack_from('<I', iag, inoext_off)[0]
inoext0_word1 = struct.unpack_from('<I', iag, inoext_off + 4)[0]
inoext0_length = inoext0_word0 & 0x00FFFFFF
inoext0_addr = ((inoext0_word0 >> 24) & 0xFF) << 32 | inoext0_word1
if inoext0_length > 0:
    print(f'\nFirst user-inode extent @ byte 0x{inoext0_addr*PSIZE:X}, len={inoext0_length} blocks')
    print('User inodes 0-7 (in fileset 1):')
    for ino in range(8):
        di_off = inoext0_addr * PSIZE + ino * 512
        di = data[di_off:di_off+512]
        if di == b'\x00' * 512:
            print(f'  inum {ino} @ 0x{di_off:X}: EMPTY')
            continue
        di_number = struct.unpack_from('<I', di, 8)[0]
        di_fileset = struct.unpack_from('<I', di, 4)[0]
        di_size = struct.unpack_from('<Q', di, 24)[0]
        di_nlink = struct.unpack_from('<I', di, 40)[0]
        di_mode = struct.unpack_from('<I', di, 52)[0]
        ftype = di_mode & 0o170000
        ftype_name = {0o100000:'REG', 0o040000:'DIR', 0o120000:'LNK',
                       0o060000:'BLK', 0o020000:'CHR', 0o010000:'FIFO',
                       0o140000:'SOCK'}.get(ftype, f'0o{ftype:o}')
        print(f'  inum {ino} @ 0x{di_off:X}: di_number={di_number} fileset={di_fileset} '
              f'size={di_size} nlink={di_nlink} mode=0o{di_mode:o} type={ftype_name}')
        # If it's the root dir (mode S_IFDIR + di_number=2), dump xtree/dtree info
        if di_number == 2 and ftype == 0o040000:
            print(f'    >>> This is the root directory')
            # dtroot at offset 224, 288 bytes
            dtroot = di[224:224+288]
            # dtroot.header.flag at offset +32 (after DASD struct, kind of, let me probe)
            # Actually struct dtroot header:
            #   struct dasd DASD;
            #   u8 flag, nextindex, freecnt, freelist;
            #   __le32 idotdot;
            #   s8 stbl[8];
            # struct dasd is 16 bytes (per dasd_t size = 16). So:
            #   bytes [0..16]: dasd
            #   bytes [16..20]: flag, nextindex, freecnt, freelist
            #   bytes [20..24]: idotdot
            #   bytes [24..32]: stbl[8]
            # Then slot[9] at bytes [32..288]
            d_flag = dtroot[16]
            d_nextindex = dtroot[17]
            d_freecnt = struct.unpack_from('<b', dtroot, 18)[0]
            d_freelist = struct.unpack_from('<b', dtroot, 19)[0]
            d_idotdot = struct.unpack_from('<I', dtroot, 20)[0]
            d_stbl = list(dtroot[24:32])
            print(f'    dtroot header: flag=0x{d_flag:02X} nextindex={d_nextindex} '
                  f'freecnt={d_freecnt} freelist={d_freelist} idotdot={d_idotdot}')
            print(f'    stbl[8] = {d_stbl}')
            # Each slot is 32 bytes starting at offset 32. Decode entries via stbl[].
            for sidx, slot_idx in enumerate(d_stbl):
                if 0 <= slot_idx < 9:
                    slot = dtroot[32 + slot_idx*32 : 32 + (slot_idx+1)*32]
                    # ldtentry (leaf entry): inumber u32, next s8, namlen u8, name[11] UCS-2, index u32
                    ent_inum = struct.unpack_from('<I', slot, 0)[0]
                    ent_next = struct.unpack_from('<b', slot, 4)[0]
                    ent_namlen = slot[5]
                    name_bytes = slot[6:6+min(22, ent_namlen*2)]
                    # UCS-2 LE; print printable ASCII characters
                    name_chars = []
                    for c in range(min(11, ent_namlen)):
                        cp = struct.unpack_from('<H', slot, 6+c*2)[0]
                        if 0x20 <= cp < 0x7f:
                            name_chars.append(chr(cp))
                        else:
                            name_chars.append(f'\\u{cp:04x}')
                    name = ''.join(name_chars)
                    print(f'      stbl[{sidx}]={slot_idx} -> slot[{slot_idx}]: inum={ent_inum} next={ent_next} namlen={ent_namlen} name={name!r}')
