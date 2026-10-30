# fs-apple2

Apple II disk filesystems, one Cargo feature each. All are on by default.

| Feature | Filesystem |
|---|---|
| `apple-dos` | Apple DOS 3.3 |
| `prodos` | ProDOS, plus the ProDOS file-type name table |
| `ucsd` | UCSD p-System (Apple Pascal, and other p-System machines) |

`sector_order` is always included. It converts between DOS 3.3 and ProDOS sector orders in 140 KB `.dsk` / `.do` / `.po` images.

The ProDOS type table ships embedded. Users can extend it with a `prodos_file_types.json` in their config folder: by default the platform config dir plus `rusty-backup`, or wherever the host app points `prodos_types::set_user_config_dir`.

Implements the `disk-core` filesystem traits. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup). MIT licensed.
