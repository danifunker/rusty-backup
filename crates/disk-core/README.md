# disk-core

The shared base under the rusty-backup filesystem, partition-table and disk-image crates.

**With the `fs` feature (default)**, it provides the interface every filesystem crate implements:

- `Filesystem`: list directories, read files and forks, report sizes and labels, run fsck.
- `EditableFilesystem`: create, write, rename and delete files and directories, set permissions, owners and extended attributes.
- `FileEntry`: one file or directory, including Mac type/creator codes, Unix modes, Amiga protection bits and timestamps.

A program written against these traits works with every supported filesystem. That includes emulators, file browsers and converters.

```rust,ignore
use disk_core::filesystem::{Filesystem, FilesystemError};

fn print_root(fs: &mut dyn Filesystem) -> Result<(), FilesystemError> {
    let root = fs.root()?;
    for entry in fs.list_directory(&root)? {
        println!("{:>10}  {}", entry.size_string(), entry.name);
    }
    Ok(())
}
```

**Always available**, including with `default-features = false`, which is how the partition and disk-image crates use it:

- `io_other`, `repeat_n`: std shims that compile on both current Rust and Rust 1.73.
- `display`: `format_size`, `decode_ostype` (Mac four-character codes), `unix_mode_string`.
- `rust173` (only with the `rust173-polyfill` feature): stand-ins for `is_multiple_of` and `Option::is_none_or`. Never enable this feature on a modern toolchain.

Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup). MIT licensed.
