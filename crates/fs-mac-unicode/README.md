# fs-mac-unicode

How HFS and HFS+ sort catalog names, which a writer has to reproduce exactly or Mac OS reports a damaged B-tree:

- **HFS+:** Apple's `FastUnicodeCompare` case folding (with its ignorable code points, and NUL sorting last), on names in the canonical decomposition HFS+ stores. HFSX binary compare is also supported.
- **Classic HFS:** each Mac Roman byte's rank under Apple's `RelString` (case-insensitive, diacritic-sensitive), compared rank by rank.

No dependencies. Builds on Rust 1.73 and later. Used by `fs-mac`; part of [rusty-backup](https://github.com/danifunker/rusty-backup).

## Where the tables come from

Both tables are generated, and the generators are in rusty-backup's `scripts/`:

- `src/hfsplus_tables.rs` ← `gen-hfsplus-tables.py UnicodeData-3.2.0.txt FastUnicodeCompare.c`.
  - Case folding is the mapping in `FastUnicodeCompare.c`, the download attached to Apple's TN1150 ("HFS Plus Volume Format", `downloads/tn1150.1.hqx`). TN1150 says an implementation must use exactly this equivalence. Only the mapping is kept, stored as a sorted list of the code points that do not fold to themselves.
  - Decomposition is Unicode 3.2.0's canonical decomposition (`UnicodeData.txt`), applied fully and put in canonical order. As TN1150 says, U+2000–U+2FFF and U+F900–U+FAFF are left alone, and Hangul syllables become conjoining jamo. Mac OS X 10.3 and later decompose this way; Mac OS 8.1–10.2 used Unicode 2.1, which differs for about 140 code points that Mac encodings rarely produce.
  - Checked against macOS: names holding each of the 1,391 BMP code points that any version decomposes were created on an HFS+ image by the macOS kernel, and the stored UTF-16 matches these tables in every case, including canonical reordering across characters.
- `src/hfs_charorder.rs` ← `gen-hfs-charorder.py` (macOS only). It compiles `hfs-relstring-pairs.c`, asks Apple's `RelString` about every pair of one-byte names, and turns the answers into ranks. Comparing names rank by rank, shorter first, agrees with `RelString` on 300,000 random longer pairs.

MIT licensed. The Unicode data is Copyright © 1991–2002 Unicode, Inc., distributed under the [Unicode licence](https://www.unicode.org/license.txt), which permits this use with that notice.
