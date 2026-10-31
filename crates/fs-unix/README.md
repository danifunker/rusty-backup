# fs-unix

Unix-family filesystems, one Cargo feature each:

| Feature | Filesystem | Support |
|---|---|---|
| `ext` | ext2 / ext3 / ext4 | read, edit, create, check |
| `ufs` | UFS1 / UFS2 / 4.3BSD FFS, big or little endian | read, edit, create, check |
| `efs` | SGI EFS (IRIX) and IRIS 3000 EFS v1 | read, edit, check, resize |
| `jfs` | JFS2 (Linux, OS/2, AIX 5); AIX JFS1 | JFS2 read, edit, check; JFS1 read |
| `btrfs` | btrfs | read |
| `reiserfs` | ReiserFS 3.5 / 3.6 | read |
| `vxfs` | Veritas VxFS (HP-UX, UnixWare) | read, edit |
| `minix` | Minix V1 / V2 / V3 | read, edit, create, check |
| `bfs` | BeOS / Haiku BFS | read, edit, create, check |

Each implements the `disk-core` filesystem traits. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed.
