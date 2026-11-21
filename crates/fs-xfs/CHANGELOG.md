# Changelog

All notable changes to this crate are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## 0.2.0

### Changed
- **Licence: MIT** (was GPL-2.0-only). The two parts that tied the crate to the
  GPL were rewritten from SGI's *XFS Algorithms & Data Structures* (CC BY-SA),
  without reference to xfsprogs or kernel source:
  - the directory name hash (`dir_hashname`), from the chapter "Variable Length
    Record B+trees". It is checked against 2,800+ hashes read off block, leaf and
    node directories in `mkfs.xfs`-made v4 and v5 images (now fixtures) and
    against the document's own `xfs_db` dump;
  - the in-place leaf / node directory editor (`dir2_tree`), from the chapter
    "Directories" (Block, Leaf, Node and B+tree Directories) and "Variable Length
    Record B+trees" (Block Headers, Internal Nodes). Validated with `xfs_repair -n`
    after thousands of inserts and removes on v4 and v5 images.
- Comments that cited xfsprogs / kernel files as porting references now describe
  `xfs_repair -n` as the behavioural reference and validation oracle.

## 0.1.0

First release, split out of rusty-backup (GPL-2.0-only).
