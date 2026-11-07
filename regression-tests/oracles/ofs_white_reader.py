#!/usr/bin/env python3
"""BeOS OFS: Steve White's ofs-extractor, ported line for line, reads what rb-cli wrote.

    ofs_white_reader.py <artifact>            add files with rb-cli, read them back here
    ofs_white_reader.py --list <image>        print path, size and md5 of every file

ofs-extractor (2004, hobbitbeos on SourceForge) is the only other reader of this format. This
port follows its C and was not written from rb-cli's driver: the same table of contents, 63-entry
directory blocks, contiguous and extent-list files, and the same refusal of anything but version
1.0. It is a second opinion, never authoritative: no running system reads OFS, so a pass says two
implementations agree. `--list` on the real 1993 Hobbit disk is what keeps the port honest.
RB_EMU_CONTROL=1 damages the root directory first, and that must fail.

Exits 0 pass, 1 finding, 77 cannot run here. Based on ofs-extractor.c, under this licence:

Copyright (c) 2004 Steve White. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted
provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions
   and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of
   conditions and the following disclaimer in the documentation and/or other materials provided
   with the distribution.

THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR OR HIS RELATIVES BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF MIND, USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import hashlib
import os
import shutil
import struct
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "emulator"))
import scenario as sc  # noqa: E402

SECTOR = 512
TOC_FIRST_DIR = 16
TOC_VOLUME_NAME = 44
BLOCK_BYTES = 64 + 63 * 64 + 64
DIRECTORY = 0xFFFFFFFF
CONTIGUOUS = 0x80000000


class Unreadable(Exception):
    pass


# How many extent-list files the last read met; the test must exercise that path.
extent_files = 0


def be32(data, at):
    return struct.unpack_from(">I", data, at)[0]


def read_fat_file(f, first, length):
    """extractFATFile: a list of {offset, count} pairs; pair 0 chains, which it cannot follow."""
    global extent_files
    extent_files += 1
    f.seek(first * SECTOR)
    fats = f.read(SECTOR)
    if len(fats) != SECTOR:
        raise Unreadable("short extent list")
    if be32(fats, 0):
        raise Unreadable("multiple FAT entries")
    out = bytearray()
    i = 1
    while be32(fats, i * 8) != 0xFFFFFFFF:
        offset, count = be32(fats, i * 8), be32(fats, i * 8 + 4)
        f.seek(offset * SECTOR)
        done = 0
        while done < count and length > 0:
            piece = f.read(min(length, SECTOR))
            if not piece:
                raise Unreadable("extent past the end of the image")
            out += piece
            length -= len(piece)
            done += 1
        i += 1
        if i == 64:
            raise Unreadable("extent list has no end marker")
    return bytes(out)


def read_dir(f, sector, prefix, files, seen):
    """readDir: follow the block chain, descending into each directory entry."""
    while True:
        if sector in seen:
            raise Unreadable(f"directory loop at sector {sector}")
        seen.add(sector)
        f.seek(sector * SECTOR)
        block = f.read(BLOCK_BYTES)
        if len(block) != BLOCK_BYTES:
            raise Unreadable(f"short directory block at sector {sector}")
        for i in range(63):
            e = block[64 + i * 64:128 + i * 64]
            first, _last, kind = be32(e, 32), be32(e, 36), be32(e, 40)
            size = be32(e, 52)
            # A leading NUL marks a deleted entry; the C only prints it, as *name.
            if e[0] == 0:
                continue
            name = e[:32].split(b"\0")[0].decode("latin-1")
            path = f"{prefix}/{name}"
            if kind == DIRECTORY:
                read_dir(f, first, path, files, seen)
            elif first & CONTIGUOUS:
                f.seek((first & 0x7FFFFFFF) * SECTOR)
                data = f.read(size)
                if len(data) != size:
                    raise Unreadable(f"{path} runs past the end of the image")
                files[path] = data
            else:
                files[path] = read_fat_file(f, first, size)
        sector = be32(block, 0)
        if sector == 0:
            return


def read_volume(path):
    with open(path, "rb") as f:
        toc = f.read(SECTOR)
        version = be32(toc, 0)
        if (version >> 16) & 0xFFFF != 1 or version & 0xFFF != 0:
            raise Unreadable("only ofs version 1.0 is supported")
        files = {}
        read_dir(f, be32(toc, TOC_FIRST_DIR), "", files, set())
    return toc[TOC_VOLUME_NAME:TOC_VOLUME_NAME + 32].split(b"\0")[0].decode("latin-1"), files


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--list":
        name, files = read_volume(sys.argv[2])
        print(f"volume {name}, {len(files)} file(s)")
        for path in sorted(files):
            print(f"{hashlib.md5(files[path]).hexdigest()} {len(files[path])} {path}")
        return 0
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    work = sc.workdir()
    disk = os.path.join(work, "ofs.img")
    shutil.copyfile(sys.argv[1], disk)
    want = {
        "/rbtest/renamed.txt": b"hello from rusty-backup\n",
        "/rbtest/sub/big.bin": bytes((i * 23) % 251 for i in range(600 * 1024)),
    }
    sc.rb("mkdir", disk, "/rbtest")
    sc.rb("mkdir", disk, "/rbtest/sub")
    sc.rb("put", disk, sc.host_file(work, "note", want["/rbtest/renamed.txt"]), "/rbtest/note.txt")
    sc.rb("put", disk, sc.host_file(work, "gone", b"deleted again\n"), "/rbtest/gone.txt")
    sc.rb("put", disk, sc.host_file(work, "big", want["/rbtest/sub/big.bin"]), "/rbtest/sub/big.bin")
    sc.rb("mv", disk, "/rbtest/note.txt", "/rbtest/renamed.txt")
    sc.rb("rm", disk, "/rbtest/gone.txt")

    # Fill the volume and punch holes, so the next file can only be stored in pieces.
    filler = sc.host_file(work, "filler", b"\xa7" * 180_000)
    for i in range(64):
        put = subprocess.run([sc.rb_cli(), "-q", "put", disk, filler, f"/fill{i}"], capture_output=True)
        if put.returncode != 0:
            break
    for i in range(0, 12, 2):
        sc.rb("rm", disk, f"/fill{i}")
    want["/rbtest/frag.bin"] = bytes((i * 31) % 253 for i in range(500 * 1024))
    sc.rb("put", disk, sc.host_file(work, "frag", want["/rbtest/frag.bin"]), "/rbtest/frag.bin")

    with open(disk, "rb") as f:
        root = be32(f.read(SECTOR), TOC_FIRST_DIR)
    sc.damage(disk, [(root * SECTOR, 64 + 2 * 64)])

    problems = []
    try:
        _, files = read_volume(disk)
        for path, data in want.items():
            if files.get(path) != data:
                problems.append(f"{path} does not read back")
        if "/rbtest/gone.txt" in files or "/rbtest/note.txt" in files:
            problems.append("a deleted or renamed name is still listed")
        if extent_files == 0:
            problems.append("no file needed an extent list, so that path went untested")
    except (Unreadable, struct.error, UnicodeDecodeError) as e:
        problems.append(f"the reader gave up: {e}")
    if problems:
        print("FINDING: " + "; ".join(problems))
        sc.finish(sc.FINDING, work)
    print("PASS: the ofs-extractor port reads rb-cli's volume back")
    sc.finish(sc.PASS, work)


if __name__ == "__main__":
    sys.exit(main())
