# Changelog

All notable changes to this crate are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## 0.2.2

### Fixed
- HFS, HFS+ and MFS implement `set_type_creator_raw`, writing the four OSType bytes as given, so
  high-bit MacRoman type and creator codes survive an edit (MCM-006); `set_type_creator` now
  converts and calls it.

## 0.2.1

### Fixed
- A shrunk HFS+-in-HFS-wrapper volume resizes the wrapper's bad-block run with it; it was
  left at the old length, past the new volume's end (MAC-016). Checked on a Mac OS 8.1
  volume with fsck_hfs and Mac OS 8.1's Disk First Aid.
- The HFS+ defrag clone works on 512-byte and 1 KiB allocation blocks (Mac OS 8.1's choice
  for small volumes); it used to panic.
- `create_blank_hfsplus` with 512-byte or 1 KiB blocks writes volumes fsck_hfs accepts.
- The classic-HFS checker flags a catalog node whose offset table repeats an offset
  (a zero-length record), as it already did for the extents tree; extents problems
  now carry the same specific codes as catalog ones instead of one catch-all.

### Added
- `mfs::write_blank_mfs_into` and `hfv::write_blank_hfv_into` stream a blank volume
  (MAC-018).

### Changed
- One HFS+ catalog-key comparator (`catalog_compare_keys`); hardlink stubs share one writer.

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
