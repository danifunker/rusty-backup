#!/usr/bin/env python3
"""Build assets/icons/icon-vintage.icns: the app icon in the formats Mac OS X 10.4 reads.

Tiger predates the PNG / ARGB icon elements icon.icns carries (10.5 and later), so it shows a
generic icon for them. This writes the classic elements instead: RLE-packed 24-bit bitmaps with
8-bit masks (is32/s8mk 16 px, il32/l8mk 32 px, ih32/h8mk 48 px, it32/t8mk 128 px), from the
icon-<size>.png files beside icon.icns. No dependencies: the PNGs are 8-bit RGBA, decoded here.

    scripts/make-legacy-icns.py        # rewrites assets/icons/icon-vintage.icns
"""
import os
import struct
import sys
import zlib

ICONS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "assets", "icons")
SIZES = [(16, b"is32", b"s8mk"), (32, b"il32", b"l8mk"), (48, b"ih32", b"h8mk"), (128, b"it32", b"t8mk")]


def read_png_rgba(path):
    """Decode an 8-bit, non-interlaced RGBA PNG into (width, height, bytes)."""
    data = open(path, "rb").read()
    if data[:8] != b"\x89PNG\r\n\x1a\n":
        sys.exit("%s: not a PNG" % path)
    pos, idat, w, h = 8, b"", 0, 0
    while pos < len(data):
        n, kind = struct.unpack(">I4s", data[pos:pos + 8])
        body = data[pos + 8:pos + 8 + n]
        if kind == b"IHDR":
            w, h, depth, ctype, _, _, interlace = struct.unpack(">IIBBBBB", body)
            if (depth, ctype, interlace) != (8, 6, 0):
                sys.exit("%s: need 8-bit RGBA, non-interlaced" % path)
        elif kind == b"IDAT":
            idat += body
        pos += 12 + n
    raw, bpp, stride = zlib.decompress(idat), 4, w * 4
    out, prev = bytearray(), bytearray(stride)
    for y in range(h):
        f, line = raw[y * (stride + 1)], bytearray(raw[y * (stride + 1) + 1:(y + 1) * (stride + 1)])
        for x in range(stride):
            a = line[x - bpp] if x >= bpp else 0
            b, c = prev[x], prev[x - bpp] if x >= bpp else 0
            if f == 1:
                line[x] = (line[x] + a) & 0xFF
            elif f == 2:
                line[x] = (line[x] + b) & 0xFF
            elif f == 3:
                line[x] = (line[x] + (a + b) // 2) & 0xFF
            elif f == 4:
                p = a + b - c
                pa, pb, pc = abs(p - a), abs(p - b), abs(p - c)
                line[x] = (line[x] + (a if pa <= pb and pa <= pc else b if pb <= pc else c)) & 0xFF
        out += line
        prev = line
    return w, h, bytes(out)


def pack_channel(chan):
    """Apple's icon RLE: 0x00-0x7F = n+1 literal bytes follow, 0x80-0xFF = next byte n-0x80+3 times."""
    out, i, n = bytearray(), 0, len(chan)
    while i < n:
        run = 1
        while i + run < n and run < 130 and chan[i + run] == chan[i]:
            run += 1
        if run >= 3:
            out += bytes((0x80 + run - 3, chan[i]))
            i += run
            continue
        start = i
        while i < n and i - start < 128:
            if i + 2 < n and chan[i] == chan[i + 1] == chan[i + 2]:
                break
            i += 1
        out.append(i - start - 1)
        out += chan[start:i]
    return bytes(out)


def element(kind, payload):
    return kind + struct.pack(">I", 8 + len(payload)) + payload


def main():
    body = b""
    for size, rgb_kind, mask_kind in SIZES:
        w, h, px = read_png_rgba(os.path.join(ICONS, "icon-%d.png" % size))
        if (w, h) != (size, size):
            sys.exit("icon-%d.png is %dx%d" % (size, w, h))
        # Each channel is packed on its own, red then green then blue; it32 starts with four zeros.
        rgb = b"".join(pack_channel(px[c::4]) for c in range(3))
        if rgb_kind == b"it32":
            rgb = b"\0\0\0\0" + rgb
        body += element(rgb_kind, rgb) + element(mask_kind, px[3::4])
    out = os.path.join(ICONS, "icon-vintage.icns")
    with open(out, "wb") as fh:
        fh.write(b"icns" + struct.pack(">I", 8 + len(body)) + body)
    print("wrote %s (%d bytes)" % (os.path.normpath(out), 8 + len(body)))


if __name__ == "__main__":
    main()
