# fs-squashfs

SquashFS 4.0 (the Linux kernel's read-only compressed filesystem, found in live CDs, AppImages and router firmware):

- **read:** files, directories, symlinks, devices, extended attributes, fragments
- **build:** write a complete image from an in-memory tree, the way `mksquashfs` does
- **edit:** create, delete and rename through the `disk-core` editing trait; the image is rebuilt on sync, keeping unchanged files byte for byte
- **verify:** a full structural traversal (SquashFS has no checksums, so this is not an fsck)

Compressors: gzip, xz, lzma and lz4, plus zstd through either the C libzstd (`native-zstd`, default) or a pure-Rust, bit-exact port (`pure-zstd`). LZO images are recognised but not decompressed.

Implements the `disk-core` filesystem traits. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed.
