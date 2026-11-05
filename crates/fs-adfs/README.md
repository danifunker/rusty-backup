# fs-adfs

Acorn ADFS / FileCore, the filesystem of Acorn Archimedes and RISC OS machines (and of later BBC Micro / Electron ADFS ROMs):

- **New-map** E, F and HD formats: read, edit, create blank volumes, check.
- **Old-map** D format: read.

Implements the `disk-core` filesystem traits. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

Licensed GPL-2.0-only, because the new-map free-space walk follows the Linux kernel's `fs/adfs/map.c` statement for statement. A clean-room rewrite from the RISC OS PRM FileCore chapter would let it become MIT (rusty-backup licence decision D2).
