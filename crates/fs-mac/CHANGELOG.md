# Changelog

All notable changes to this crate are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## 0.2.0

### Changed
- Built on `disk-core` 0.3 and `fs-mac-unicode` 0.2 (MIT; HFS+ names compare as
  Apple's `FastUnicodeCompare` does).
- New `fork-export` feature: the AppleDouble / MacBinary / BinHex writers
  without HFS. `forks` is `fork-export` plus `hfs`, as before.
- `build_appledouble_with_flags` / `build_macbinary_with_flags`; fork export
  now keeps the Finder flags in all three formats.
- `hfsplus_journal::replay_journal_within` takes the volume length;
  `replay_journal` is bounded by the image end.

### Fixed (review, 2026-11-21)
- MFS keeps existing names longer than 27 bytes on every directory write; its
  MDB geometry (map vs directory, 4094-block limit, free count, volume end) is
  checked at open; file counters no longer overflow.
- HFS repair no longer deletes a directory whose missing thread will not fit,
  puts the old thread back when a corrected one will not fit, refuses to rebuild
  the bitmap when the extents tree is unreadable, re-checks overlapping extents
  without dropping the first owner's blocks, zeroes the rest of a fork after a
  bad extent, and restores the catalog when an index rebuild fails part-way.
- B-tree walks stop on leaf-chain loops; record counts larger than the node,
  header-node bitmap offsets and HFS+ node sizes outside 512..32768 are refused;
  an insert into a damaged index (loop, bad root) or a rootless tree with records
  is refused instead of misplacing the record; large records get a three-way
  split budget.
- Journal replay refuses blocks outside the volume; BinHex, MacBinary and alias
  parsing no longer wrap on 32-bit targets; B-tree file and Lisa image sizes are
  checked before allocation; defrag progress goes to the log, not stderr.
- Alias volume and target names decode as Mac Roman.
