# fs-mac

Classic Macintosh filesystems, one Cargo feature each:

| Feature | What | Support |
|---|---|---|
| `hfs` | HFS (Mac OS 1986–1998), including flat BasiliskII / SheepShaver `.hfv` volumes | read, edit, create, check, clone to a new allocation-block size, boot blocks |
| `mfs` | Macintosh File System (the original 400K floppy format) | read, edit, create |
| `hfsplus` | HFS+ and HFSX | read, edit, create, check, journal replay, defragmenting clone (flat or inside an HFS wrapper) |
| `lisa` | Lisa File System, from DiskCopy 4.2 images with tags | read |
| `codecs` | Mac Roman names, BinHex 4.0, resource forks, MacBinary / AppleDouble, without HFS | read and write |
| `fork-export` | `codecs` plus fork-aware export from any `disk-core` filesystem, without HFS | write |
| `forks` | `fork-export` plus `hfs` (as before 0.2.0) | read and write |

Finder info, type / creator codes, dates and resource forks survive every edit. Each filesystem implements the `disk-core` traits. Partition maps (APM) are not included; this crate produces and reads bare volumes. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed, with every feature. `hfs` and `hfsplus` pull in `fs-mac-unicode` for catalog-name ordering, which is MIT too since its 0.2.0 (its tables were regenerated from Apple TN1150, Unicode 3.2 and Apple's `RelString`).

To leave out the HFS code, depend with `default-features = false` and only `codecs`, `fork-export` and / or `lisa`.
