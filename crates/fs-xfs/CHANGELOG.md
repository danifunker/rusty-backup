# Changelog

All notable changes to this crate are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## 0.2.1

### Fixed
- Edits, repair, chmod and chown refuse a volume whose log was not cleanly unmounted (the head
  record is not the unmount transaction), since the next mount would replay the log over them.
  `XfsFilesystem::log_state` reports it (XFS-012).
- chmod and chown work on v5 volumes with features the editor does not maintain: they rewrite
  only the inode core.

## 0.2.0

### Fixed (review, 2026-11-21)
- Edits and repair refuse volumes with finobt, rmapbt, reflink, inobtcnt, sparse
  inodes, bigtime, exchange-range, parent pointers or v1 directories, which they
  would have left inconsistent; R3 refuses to rebuild a lost inode btree on v5.
- `open` checks superblock geometry and refuses unknown v5 incompat bits;
  `open_within` refuses a volume larger than its partition.
- Attribute-fork blocks are counted by fsck, R2 and R3 and freed on delete;
  deleting one name of a hard-linked file keeps the inode.
- Directory edits compare and rewrite names as bytes and refuse a directory
  holding a non-UTF-8 name; 8-byte short-form directories get 8-byte entries.
- The allocator checks free-space records and inode free masks before use;
  bmbt leaf chains stop on a loop; the repair map is a bounded bitset.
- v5 remote symlinks no longer include the block header; BIGTIME timestamps are
  decoded; v1 inodes report their link count.

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
