# Changelog

All notable changes to this crate are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## 0.2.0

### Changed
- Licensed `MIT AND Unicode-3.0` (was GPL-2.0-only); `LICENSE-UNICODE` covers the
  tables derived from Unicode's data files. Every table is regenerated from Apple TN1150's
  `FastUnicodeCompare.c` mapping, Unicode 3.2.0 and Apple's `RelString`, by
  scripts in rusty-backup; none comes from the Linux kernel or hfsutils any more.
- HFS+ decomposition follows Unicode 3.2, as Mac OS X 10.3 and later write it.
  The old tables used the earlier Unicode 2.x set, which differs for 143 code
  points (for example Greek tonos U+0301 instead of U+030D, U+0218 now
  decomposed, U+04D4 no longer, and the four-unit Greek ones such as U+1F82).
  `decompose_str` now also puts combining marks in canonical order across
  characters, as macOS does.
- `compare_hfsplus_keys` now folds only, as Apple's `FastUnicodeCompare` does:
  names must already be decomposed (every key fs-mac builds is). Before, it
  decomposed stored units too, which with the new table put names an older
  writer stored precomposed out of order.
- Case folding and the classic HFS order are unchanged: same result on every
  code point and every byte pair. ASCII skips the table lookups.

## 0.1.0

First release, split out of fs-mac.
