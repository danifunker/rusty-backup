# dsk-clonezilla

Reads [Clonezilla](https://clonezilla.org/) backups:

- **partclone images** (v0001 and v0002), uncompressed or compressed with any of Clonezilla's `-z` choices: zstd, gzip/pigz, bzip2, xz or lz4. Split images (`.aa`, `.ab`, ...) are read as one stream.
- **Image-folder metadata:** the disk and partition layout from a Clonezilla image folder (`parts`, `*-pt.sf`, `*-chs.sf`, ...), including swap partitions.
- **Block cache:** random access into a partclone image, with a background scan that keeps FAT, NTFS, exFAT, ext and btrfs metadata blocks in memory for browsing.

Features: `native-zstd` (default, the C libzstd) or `pure-zstd` (pure Rust, no C to cross-compile). Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed. Copyright Dani Sarfati and Alan Steremberg.
