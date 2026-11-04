# rb-fsexporter

Export a tree from any [`disk-core`](https://crates.io/crates/disk-core) filesystem (anything that implements `disk_core::filesystem::Filesystem`) to a host folder:

- **One walker** for a file, a folder with everything in it, a whole volume (a folder named after the volume, or its contents straight into the destination), or a folder's children.
- **Mac forks** in every `fs-mac` mode: AppleDouble `._name` sidecars, MacBinary `.bin`, BinHex `.hqx`, `.rsrc` sidecars, the macOS named fork, or data forks only. The encoders are `fs-mac`'s; this crate has none of its own.
- **Symlinks** become host symlinks; where the host refuses one (Windows without the privilege, for instance), a small text file holding the target.
- **Policies:** existing files (overwrite, skip, fail), errors (skip the entry, or stop), case collisions on a case-insensitive host (rename with `~N`, skip, refuse), POSIX permission bits, recursion.
- **Progress and cancel** through a `Host` trait, which also receives the per-entry reports and can rename a file or write it itself (a text conversion, per-file compression).

## Compatible crates

Every filesystem in these crates implements `disk_core::filesystem::Filesystem`, so it exports with no glue:

| Crate | Filesystems |
|---|---|
| `fs-mac` | MFS, HFS, HFS+ / HFSX, Lisa |
| `fs-pc` | FAT12/16/32, exFAT, NTFS, Human68k |
| `fs-unix` | ext2/3/4, UFS, EFS (incl. v1), JFS, btrfs, ReiserFS, VxFS, Minix, BeOS BFS / OFS |
| `fs-xfs` | XFS |
| `fs-apfs` | APFS |
| `fs-apple2` | Apple DOS 3.3, ProDOS, UCSD p-System |
| `fs-amiga` | AFFS / OFS, PFS3, SFS |
| `fs-8bit` | CBM, Atari DOS, Acorn DFS, TR-DOS, QDOS (+ microdrive), RS-DOS, DragonDOS, OS-9, TI-99/4A, Oric, HP LIF, ANDOS |
| `fs-adfs` | Acorn ADFS |
| `fs-hpfs` | HPFS |
| `fs-cpm` | CP/M |
| `fs-qnx` | QNX 4, QNX 6 |
| `qnx2fs` | QNX 2 (feature `disk-core`) |
| `fs-squashfs` | SquashFS |
| `fs-xerox` | Xerox Alto (BFS, Pilot) |
| `fs-tar` | tar / ZIP archives browsed as a volume (feature `mem-archive`) |
| `dsk-tape` | tape images |

Optical discs (`opticaldiscs`) have their own filesystem trait; rusty-backup exports them through its adapter, not directly.

## Dependencies

Only `disk-core`, `fs-mac` (feature `forks`) and `anyhow`. `fs-mac` pulls in the GPL-2.0-only `fs-mac-unicode`, so a build with this crate is GPL as a whole.

Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed.
