# fs-8bit

Read and edit the disk filesystems of 8-bit home computers. Each filesystem is a Cargo feature, so you only build the ones you use.

| Feature | Filesystem | Read | Write | fsck |
|---|---|---|---|---|
| `oric` | Oric Jasmin (Oric-1 / Atmos / Telestrat, MiSTer Oric core) | Yes | Yes | Yes |

More filesystems (Commodore, Atari DOS, Acorn, TR-DOS, QDOS, CoCo, OS-9, TI-99, HP LIF, …) move here from rusty-backup over time.

Every filesystem implements the traits from [`disk-core`](../disk-core), so code written against `disk_core::filesystem::Filesystem` works with all of them.

Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup). MIT licensed.
