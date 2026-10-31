# arc-mac

Classic Macintosh archive formats, read back into Mac files with their data fork, resource fork and Finder info (type, creator, flags):

- **StuffIt** 1.5-style `.sit` and self-extracting `.sea` (methods 0 stored, 1 RLE, 2 LZW, 3 Huffman, 5 LZAH, 13 LZ+Huffman, 15 Arsenic), and **StuffIt 5**
- **Compact Pro** `.cpt`, often BinHex-wrapped
- **MacBinary** I / II / III, **BinHex 4.0**, **MacZip** (`__MACOSX` AppleDouble pairs) and Hampa Hug's **mar**
- writing: StuffIt archives and mar

Builds on Rust 1.73 and later. Depends on `fs-mac` for Mac Roman names and resource forks. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

Licensed **MIT AND LGPL-2.1-or-later**, file by file: the decompressors and the StuffIt 5 / Compact Pro parsers are ported from XADMaster (LGPL-2.1-or-later); the rest is MIT. Each file starts with an SPDX line, and `THIRD_PARTY_NOTICES.md` lists them.
