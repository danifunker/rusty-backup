# Changelog

All notable changes to this crate are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## 0.2.0

### Changed
- Licensed MIT (was `MIT AND BSD-4-Clause`). A side-by-side check found no code
  translated from pfs3aio, so its BSD-4-Clause notice is dropped, and the
  comments now describe the on-disk formats instead of citing driver sources.
- SFS: object-node tree growth, the full bits on its pointers, and admin-space
  growth are rewritten from the on-disk rules.

### Changed (review, 2026-11-21)
- SFS volumes are read-only: create, delete and rename never maintained the
  directory hash tables the SFS handler looks names up in, so files made here
  did not open by name on an Amiga and a delete cut other names' chains. They
  come back once the name hash is verified against a real SFS volume.
- AFFS DirCache volumes (DOS\4 / DOS\5) are read-only and are not resized:
  the cache blocks are not updated.
- Names are ISO-8859-1 on all three filesystems, read and written; a name with
  a character outside it is refused.

### Fixed
- AFFS repair no longer rebuilds the bitmap after a broken directory walk, and
  fsck counts DirCache blocks as in use.
- AFFS resize repoints the root's children at the moved root; the fallback root
  search checks the root's checksum and its first bitmap page.
- PFS3 and SFS take `open_within` (used by the app's editable openers) and refuse
  a volume larger than its partition; PFS3 refuses to delete a directory hard
  link; AFFS and PFS3 refuse files of 4 GiB or more.
- AFFS hash and extension chains, PFS3 anode chains, the SFS extent B-tree and
  the PFS3 clone walk stop on loops; the PFS3 clone flushes every 16 MiB.
- SFS listings carry protection and comments; the AFFS root's creation date is
  read from 0x1E4.
- SFS: growing an object-node tree past two levels could add an empty index
  container and still report DiskFull. Growth now always ends in a new leaf.
