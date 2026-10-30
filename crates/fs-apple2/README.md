# fs-apple2

Apple II era disk filesystems, one Cargo feature each.

| Feature | Filesystem |
|---|---|
| `ucsd` | UCSD p-System (Apple Pascal, and other p-System machines) |

Apple DOS 3.3 and ProDOS will join once they're untangled from rusty-backup's container code and its user-configurable ProDOS file-type table.

Implements the `disk-core` filesystem traits. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup). MIT licensed.
