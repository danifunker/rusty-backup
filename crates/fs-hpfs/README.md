# fs-hpfs

OS/2 High Performance File System (HPFS): read, edit (create and delete, with the dnode B-tree kept balanced), create blank volumes, and check.

Implements the `disk-core` filesystem traits. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

Licensed GPL-2.0-only, because the directory editing follows the Linux kernel's `fs/hpfs/dnode.c` (same breakdown and split heuristic). The read side follows the published on-disk format.
