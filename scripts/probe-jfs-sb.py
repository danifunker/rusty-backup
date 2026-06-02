#!/usr/bin/env python3
"""Probe the JFS2 fixture for Aggregate Superblock fields.

Reference: jfsutils/include/jfs_superblock.h. Used during J.1 to confirm
field offsets and the actual values mkfs.jfs writes.
"""
import zstandard as zstd
import struct
from pathlib import Path

root = Path(__file__).resolve().parent.parent
with open(root / 'tests' / 'fixtures' / 'test_jfs.img.zst', 'rb') as f:
    data = zstd.ZstdDecompressor().decompress(f.read())

# Primary aggregate SB is at byte offset 0x8000 = 32768.
SB_OFF = 0x8000

def u8(off): return data[SB_OFF + off]
def u16(off): return struct.unpack_from('<H', data, SB_OFF + off)[0]
def i16(off): return struct.unpack_from('<h', data, SB_OFF + off)[0]
def u32(off): return struct.unpack_from('<I', data, SB_OFF + off)[0]
def i32(off): return struct.unpack_from('<i', data, SB_OFF + off)[0]
def u64(off): return struct.unpack_from('<Q', data, SB_OFF + off)[0]
def i64(off): return struct.unpack_from('<q', data, SB_OFF + off)[0]
def bytes_at(off, n): return data[SB_OFF + off:SB_OFF + off + n]

print('=== JFS Aggregate Superblock @ byte 32768 ===')
print(f'  s_magic      (+0):    {bytes_at(0, 4)!r}')
print(f'  s_version    (+4):    {u32(4)}')
print(f'  s_size       (+8):    {i64(8)} hardware blocks ({i64(8) * 512} bytes)')
print(f'  s_bsize      (+16):   {i32(16)}  (aggregate block size)')
print(f'  s_l2bsize    (+20):   {i16(20)}')
print(f'  s_l2bfactor  (+22):   {i16(22)}')
print(f'  s_pbsize     (+24):   {i32(24)}  (physical/hardware block size)')
print(f'  s_l2pbsize   (+28):   {i16(28)}')
print(f'  pad          (+30):   {i16(30)}')
print(f'  s_agsize     (+32):   {u32(32)}  (alloc group size in aggr blocks)')
print(f'  s_flag       (+36):   0x{u32(36):08X}')
print(f'  s_state      (+40):   0x{u32(40):08X}')
print(f'  s_compress   (+44):   {i32(44)}')

# Skip ahead to label area
# pxd_t s_ait2 (+48, 8 bytes)
# pxd_t s_aim2 (+56, 8 bytes)
# u32 s_logdev (+64)
# i32 s_logserial (+68)
# pxd_t s_logpxd (+72, 8)
# pxd_t s_fsckpxd (+80, 8)
# struct timestruc_t s_time (+88, 8)
# i32 s_fsckloglen (+96)
# i8 s_fscklog (+100)
# char s_fpack[11] (+101..112)

print(f'  s_fpack      (+101):  {bytes_at(101, 11)!r}')
# s_label is at +152 (after extendfs params + uuid)
# Actually re-read: after s_fpack=112; then s_xsize (i64, +112=8), s_xfsckpxd (pxd_t, +120=8), s_xlogpxd (pxd_t, +128=8); then 128-byte boundary
# Actually we're already past 128 with s_fpack at 101. Let's see: s_fpack runs 101..112; then s_xsize at 112-119, s_xfsckpxd at 120-127, s_xlogpxd at 128-135.
# 128-byte boundary comment in struct.
# Then s_uuid[16] at 136..151, s_label[16] at 152..167, s_loguuid[16] at 168..183.

print(f'  s_uuid       (+136):  {bytes_at(136, 16).hex()}')
print(f'  s_label      (+152):  {bytes_at(152, 16)!r}')

# Total raw image size:
print()
print(f'  Image total bytes:    {len(data)}')
print(f'  s_size * s_pbsize:    {i64(8) * 512}')
print(f'  s_size * s_bsize/8:   {i64(8) * 4096 / 8}')

# Dump the first 256 bytes of the SB region for context.
print()
print('--- raw SB hex (first 256 bytes) ---')
for off in range(0, 256, 16):
    h = bytes_at(off, 16).hex()
    a = ''.join(chr(b) if 32 <= b < 127 else '.' for b in bytes_at(off, 16))
    print(f'  {off:04X}: {h}  {a}')
