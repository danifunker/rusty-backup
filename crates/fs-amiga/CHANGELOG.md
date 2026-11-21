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

### Fixed
- SFS: growing an object-node tree past two levels could add an empty index
  container and still report DiskFull. Growth now always ends in a new leaf.
