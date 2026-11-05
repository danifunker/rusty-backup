# dsk-apple

Apple disk-image containers, one Cargo feature each:

| Feature | Container | Support |
|---|---|---|
| `dc42` | DiskCopy 4.2, including Lisa / Twiggy images with their tag bytes | read, write |
| `twomg` | 2IMG / 2MG (Apple II) | read, write |
| `woz` | WOZ 1 / 2 (Apple II) and MOOF (Macintosh 3.5") flux-accurate bitstream images | read, write |
| `dart` | DART, Apple's compressed floppy archive (RLE and LZH chunks) | read |

Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed.
