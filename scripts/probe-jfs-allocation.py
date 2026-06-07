#!/usr/bin/env python3
"""Probe the JFS2 fixture for its allocation footprint.

Scans the raw image to find the highest non-zero block and reports the
gap structure. Used to decide whether a simple "backward zero scan" is
viable for last_data_byte in J.2.
"""
import zstandard as zstd
from pathlib import Path

root = Path(__file__).resolve().parent.parent
with open(root / 'tests' / 'fixtures' / 'test_jfs.img.zst', 'rb') as f:
    data = zstd.ZstdDecompressor().decompress(f.read())

BLOCK = 4096
n_blocks = len(data) // BLOCK
print(f'Image: {len(data)} bytes = {n_blocks} 4 KiB blocks')

# Find highest non-zero block.
highest_nz = -1
for i in range(n_blocks - 1, -1, -1):
    if data[i * BLOCK:(i + 1) * BLOCK] != b'\x00' * BLOCK:
        highest_nz = i
        break
print(f'Highest non-zero 4 KiB block: {highest_nz} (byte offset {highest_nz * BLOCK})')

# Print the allocation map: zero/nonzero per block.
print('\nPer-block allocation pattern (0=zero, 1=non-zero):')
runs = []
state = None
run_start = 0
for i in range(n_blocks):
    s = 0 if data[i * BLOCK:(i + 1) * BLOCK] == b'\x00' * BLOCK else 1
    if s != state:
        if state is not None:
            runs.append((state, run_start, i - 1))
        state = s
        run_start = i
runs.append((state, run_start, n_blocks - 1))
for s, a, b in runs:
    label = 'data' if s else 'zero'
    print(f'  blocks {a:4d}..{b:4d}  {label:5s}  ({b - a + 1} blocks)')

# Where is the file data likely (where /large.bin = 24 KiB landed)?
# 24 KiB / 4 KiB = 6 blocks. Look for runs >= 6 blocks.
print('\nData runs >= 6 blocks (candidates for the 24 KiB large.bin file):')
for s, a, b in runs:
    if s == 1 and b - a + 1 >= 6:
        print(f'  blocks {a}..{b} ({b - a + 1} blocks)')
        # Verify byte formula for large.bin: data[i] = (i * 37 + 11) & 0xFF
        for blk_idx in range(a, b + 1):
            blk = data[blk_idx * BLOCK:(blk_idx + 1) * BLOCK]
            # Check if this block matches the large.bin formula at block offset 0..6
            for file_blk_idx in range(6):
                expected = bytes([((file_blk_idx * BLOCK + i) * 37 + 11) & 0xFF for i in range(min(16, BLOCK))])
                if blk[:16] == expected[:16]:
                    print(f'    block {blk_idx} matches large.bin block {file_blk_idx}')
                    break
