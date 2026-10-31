# fs-mac

Classic Macintosh filesystems, one Cargo feature each:

| Feature | What | Support |
|---|---|---|
| `hfs` | HFS (Mac OS 1986–1998), including flat BasiliskII / SheepShaver `.hfv` volumes | read, edit, create, check, clone to a new allocation-block size, boot blocks |
| `mfs` | Macintosh File System (the original 400K floppy format) | read, edit, create |
| `hfsplus` | HFS+ and HFSX | read, edit, create, check, journal replay, defragmenting clone (flat or inside an HFS wrapper) |
| `lisa` | Lisa File System, from DiskCopy 4.2 images with tags | read |
| `forks` | resource forks, BinHex 4.0, fork-aware export | read and write |

Finder info, type / creator codes, dates and resource forks survive every edit. Each filesystem implements the `disk-core` traits. Partition maps (APM) are not included; this crate produces and reads bare volumes. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed. It depends on the GPL-2.0-only `fs-mac-unicode` crate for HFS / HFS+ catalog-name ordering, so a binary that uses `fs-mac` is covered by the GPL as a whole until that crate's tables are replaced (rusty-backup licence decision D2).
