# Changelog

All notable changes to this crate are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## 0.3.2

### Changed
- The `disk-core` feature accepts disk-core 0.1 or 0.2 (`>=0.1, <0.3`). The
  adapter uses only the filesystem and entry interfaces, which 0.2 leaves
  unchanged; 0.2 dropped Unix-only helpers this crate never used. Tested
  against both.

## 0.3.1

### Added
- Optional `disk-core` feature: `qnx2fs::adapter` puts a `Volume` behind the
  [disk-core](https://crates.io/crates/disk-core) `Filesystem` /
  `EditableFilesystem` traits, with `From` conversions for `Error` and
  `Attributes`. It moved here from RustyBackup unchanged. The default build
  still has no dependencies.

## 0.3.0

Modern dates are folded into the ICON's range instead of clamped.

An ICON date holds only the years 1980-1995. Until now a date outside that
range, such as any file from a modern computer, was written as 31 December 1995
11:59:59 pm.

### Added
- `fold_date(unix)`: the same month, day and time of day in **1994**, with
  29 February going to **1992**, the last leap year the ICON can store. Dates
  already in 1980-1995 are kept. 1994 sorts after all the ICON system software
  (the last release is from 1991), and a clock running past New Year's Eve moves
  into 1995, which can still be stored. icon-emu's emulated clock uses the same
  rule.

### Changed
- "Now" (from `set_clock` or the system clock) and `NewEntry::mtime` are folded
  with `fold_date` before they are written. `encode_date` itself still clamps;
  call `encode_date(fold_date(t))` to get the new behaviour from it.
- Without `set_clock` on `wasm32-unknown-unknown`, dates come out as
  1 January 1994 (it was 1 January 1980).



Copying between volumes, and keeping metadata when files move or change.

### Added
- `Attributes`: an entry's QNX metadata exactly as stored (owner attributes,
  group and other permissions, owner, raw date), from `Entry::attributes()`.
- `NewEntry::attributes` (and `NewEntry::like(&entry)`): create an entry with
  exactly that metadata. `mtime`, if also set, replaces the date.
- `Volume::copy_from(&mut src, src_path, dst_dir, replace)`: copy a file or a
  folder tree from another volume, keeping every entry's permissions, owner and
  date (folders get their own date back after being filled). Existing folders
  are merged into; existing files need `replace`. Copying `/` copies a disk's
  contents without its `bitmap`. Returns a `CopyReport`.
- `Volume::replace(path, data, opts)`: overwrite a file keeping its permissions
  and owner, with a new date; checks the space first, so a full disk keeps the
  old file.
- `Volume::set_attributes(&entry, attributes)`.

### Changed (breaking)
- `NewEntry` has a new public field, `attributes`. Code that builds it with a
  struct literal must add `..Default::default()` (or `attributes: None`).

## 0.1.0

First release. Extracted from RustyBackup's QNX 2 support (same author) and
relicensed MIT, so that MIT projects such as icon-emu can use it.

- Read: list directories (deleted entries on request), read files, look up paths.
- Write, as QNX 4.10.01 itself writes (checked with its `chkfsys` in icon-emu):
  create files and directories, delete, rename, set permissions and owners.
- Path-based helpers: `put`, `get`, `mkdir`, `remove`, `rename_path`, `list`.
- `Volume::check`: broken chains, cross-links, bitmap against the blocks files
  hold, recoverable deleted files.
- Text: `rs_to_lf`, `lf_to_rs`, `looks_like_rs_text`. Names: `validate_name`,
  `sanitize_name`. Dates: `decode_date`, `encode_date`, `format_date`.
- No dependencies; Rust 1.73; builds for `wasm32-unknown-unknown` (set the clock
  with `Volume::set_clock` there).
