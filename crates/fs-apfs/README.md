# fs-apfs

Read-only Apple File System (APFS): containers, volumes, files, directories and symlinks. With the `crypto` feature, FileVault-encrypted volumes unlock from a password.

Implements the `disk-core` filesystem traits. Builds on Rust 1.73 and later without `crypto`; `crypto` needs Rust 1.85 for its RustCrypto dependencies. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed.
