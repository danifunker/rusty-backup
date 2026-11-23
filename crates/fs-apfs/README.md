# fs-apfs

Read-only Apple File System (APFS): containers, volumes, files, directories and symlinks. With the `crypto` feature, FileVault-encrypted volumes unlock from a password. A container with one volume browses as that volume; with several, the root holds one folder per volume, and a password unlocks every encrypted volume it opens.

Implements the `disk-core` filesystem traits. Builds on Rust 1.73 and later without `crypto`; `crypto` needs Rust 1.85 for its RustCrypto dependencies. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed.
