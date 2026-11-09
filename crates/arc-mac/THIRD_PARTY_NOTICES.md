# Third-party notices

`arc-mac` is licensed `MIT AND LGPL-2.1-or-later`. Each source file starts with an SPDX line naming its licence.

## XADMaster (LGPL-2.1-or-later)

These files are ports of XADMaster (The Unarchiver's engine) and are distributed under the GNU Lesser General Public License, version 2.1 or later (`LICENSE-LGPL-2.1`):

- `stuffit_arsenic.rs` (Arsenic, including the BWT inverse)
- `compactpro.rs` (Compact Pro parser, RLE and LZH)
- `stuffit5.rs` (StuffIt 5 parser)
- `stuffit_lzw.rs` (the method-2 LZW *decoder*; the `compress(1)` encoder in the same file is original MIT code)
- `stuffit_lzh.rs` and `stuffit13_tables.rs` (method 13)
- `stuffit_lzah.rs` (LZAH)
- `stuffit_huffman.rs` (method 3)

Each of the XADMaster source files followed carries this notice:

```
/*
 *
 * Copyright (c) 2017-present, MacPaw Way Ltd. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
```

## Aaru.Compression.Native (LGPL-2.1-or-later)

`shrinkwrap.rs` (ShrinkWrap 3 "SIT2", the NDIF chunk type `0xF0` codec) is ported from `stuffit/shrinkwrap.c` in [Aaru.Compression.Native](https://github.com/aaru-dps/Aaru.Compression.Native), Copyright (c) 2019-2026 Natalia Portillo, distributed under the GNU Lesser General Public License, version 2.1 or later (`LICENSE-LGPL-2.1`). Its code-length tree reader is XADMaster's `SIT14_ReadTree` (`XADStuffItOldHandles.m`, notice above).

## ResDecompress (MIT)

`tome.rs` (Apple Installer Tome) is original code, but its InstaCompOne copy-length and literal-count prefix codes started from `InstaCompOne.py` in [ResDecompress](https://github.com/maximumspatium/ResDecompress), Copyright (c) 2018-2019 Maxim Poliakovski, MIT licensed. The last copy-length code is corrected there (10 bits, not 11).

## Everything else (MIT)

`stuffit.rs` (the StuffIt 1.5 container and its writer), `bitreader_be.rs`, `detect.rs`, `extract.rs`, `macbinary.rs`, `maczip.rs` and `mar.rs` are original and MIT licensed (`LICENSE-MIT`). `mar.rs` says it is a clean-room implementation of Hampa Hug's `mar` format (whose reference tool is GPL-2.0); that has not yet been compared against the tool's source (rusty-backup `docs/license_audit.md`).
