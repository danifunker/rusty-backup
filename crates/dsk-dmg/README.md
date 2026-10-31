# dsk-dmg

Reads Apple disk images as a seekable stream of the disk inside:

- **UDIF `.dmg`**: raw, zlib, bzip2, LZFSE, LZMA and ADC chunks.
- **NDIF:** classic Mac OS Disk Copy 6 images, reconstructed from the `bcem` resource.
- **`.sparseimage`:** growable band images.
- **Encrypted DMG** (`encrcdsa` v2): AES-CBC, or 3DES on older images, unlocked from a password, behind the `crypto` feature.

Builds on Rust 1.73 and later without `crypto`; `crypto` needs Rust 1.85 for its RustCrypto dependencies. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed.
