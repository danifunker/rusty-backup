# fs-pc

PC filesystems, one Cargo feature each:

| Feature | Filesystem | Support |
|---|---|---|
| `fat` | FAT12 / FAT16 / FAT32 with VFAT long names | read, edit, create, check, resize, compact |
| `exfat` | exFAT | read, edit, create, check, resize, defragmenting clone |
| `ntfs` | NTFS | read, edit, create, check, resize, defragmenting clone |
| `human68k` | Sharp X68000 Human68k | read, edit, check, resize, clone |

Each implements the `disk-core` filesystem traits. HPFS is in the separate, GPL-licensed `fs-hpfs` crate. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed. See `THIRD_PARTY_NOTICES.md` for the origin of the NTFS template blobs.
