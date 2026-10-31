# dsk-ghost

Reads Norton Ghost images (`.gho`, with `.ghs` spans) back into the disk or partition they captured:

- whole-disk and single-partition images, sector-based or file-aware (FAT and NTFS volumes rebuilt from the files Ghost stored)
- Ghost's compression (zlib and the "Z1" Fast-LZ), split / spanned sets, and password-protected images

Builds on Rust 1.73 and later. Depends on `fs-pc` for the FAT and NTFS rebuild. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed. The Fast-LZ decoder is ported from nyarime/gho (MIT); see `THIRD_PARTY_NOTICES.md`.
