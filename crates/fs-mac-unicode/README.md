# fs-mac-unicode

How HFS and HFS+ sort catalog names, which a writer has to reproduce exactly or Mac OS reports a damaged B-tree:

- **HFS+:** Apple's `FastUnicodeCompare`, meaning the TN1150 case-fold table, canonical decomposition with Apple's exclusions, and the ignorable code points. HFSX binary compare is also supported.
- **Classic HFS:** the Mac Roman `_RelString` order (`HFS_CHARORDER`): case-insensitive, diacritical-sensitive.

No dependencies. Builds on Rust 1.73 and later. Used by `fs-mac`; part of [rusty-backup](https://github.com/danifunker/rusty-backup).

Licensed GPL-2.0-only. The HFS+ tables and lookup follow the Linux kernel's `fs/hfsplus/tables.c` and `unicode.c`, and `HFS_CHARORDER` is hfsutils' `hfs_charorder` (GPL-2.0-or-later). Replacing them with tables generated from TN1150 and Inside Macintosh would let this become MIT (rusty-backup licence decision D2).
