# fs-cpm

Read and edit CP/M 2.2 and 3.0 disk filesystems. A catalog of per-machine disk definitions (DPBs) covers the formats the MiSTer and other retro cores use.

Implements the `disk-core` filesystem traits, so code written against `disk_core::filesystem::Filesystem` works with it unchanged.

Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup). MIT licensed.
