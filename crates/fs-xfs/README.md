# fs-xfs

XFS for every generation that shipped on real disks, from IRIX v4 (dir1 and dir2) to Linux v5 with metadata CRCs:

- read: files, directories (shortform, block, leaf, node), symlinks, extent and B-tree mapped files
- edit: create, delete, rename, with the inode, free-space and directory B-trees kept consistent
- check, and a repair modelled on `xfs_repair`'s phases

Editing and repair are limited to volumes whose structures they maintain: v4 (IRIX and older Linux) with v2 directories, and v5 without finobt, rmapbt, reflink, inobtcnt, sparse inodes, bigtime, exchange-range or parent pointers (the volumes `write_blank_xfs` makes). Anything else (modern `mkfs.xfs` defaults, IRIX v1 directories) opens read-only and every change is refused. A volume that was not cleanly unmounted has its log checked: fsck reports it, `repair` replays it as a mount would (on any feature set: the replay does not need the editor), and `XfsFilesystem::discard_log` throws away a log that cannot be replayed, like `xfs_repair -L`. Edits refuse a dirty log.

Implements the `disk-core` filesystem traits. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

Licensed MIT since 0.2.0 (0.1.x was GPL-2.0-only). The directory name hash and the leaf / node directory editor were rewritten from SGI's *XFS Algorithms & Data Structures*; the repair phases use `xfs_repair -n` as a behavioural reference and validation oracle, with no code taken from it. See `CHANGELOG.md` and `docs/license_audit.md` in the rusty-backup repository.
