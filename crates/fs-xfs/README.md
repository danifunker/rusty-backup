# fs-xfs

XFS for every generation that shipped on real disks, from IRIX v4 (dir1 and dir2) to Linux v5 with metadata CRCs:

- read: files, directories (shortform, block, leaf, node), symlinks, extent and B-tree mapped files
- edit: create, delete, rename, with the inode, free-space and directory B-trees kept consistent
- check, and a repair modelled on `xfs_repair`'s phases

Implements the `disk-core` filesystem traits. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

Licensed GPL-2.0-only, because the directory name hash follows xfsprogs (`xfs_da_hashname`) line for line. The rest of the crate follows the published XFS on-disk format; see `docs/license_audit.md` in the rusty-backup repository.
