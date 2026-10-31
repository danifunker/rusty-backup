# fs-tar

Tar archives in and out of any [`disk-core`](https://crates.io/crates/disk-core) filesystem, whichever crate implements it:

- **Export** a whole volume or a selection as `.tar`, `.tar.gz` or `.tar.zst`, with modes, owners, times and symlinks.
- **Import** a tar (plain, gzip, bzip2 or zstd; GNU, ustar and old V7 headers) into any editable filesystem, with a conflict policy (stop, skip or overwrite), `mkdir -p` of implied directories, path-traversal guarding, and attribute inheritance when a file is replaced. Names that aren't UTF-8 keep their raw bytes on filesystems that store them.

Features:

- `native-zstd` (default) / `pure-zstd`: the zstd backend for `.tar.zst`.
- `appledouble`: join macOS `._name` sidecars to their files as a resource fork plus type / creator codes. It pulls in `fs-mac`, and through it the GPL-2.0-only `fs-mac-unicode`, so a build with it on is GPL as a whole. Without it, sidecars are skipped and counted.

Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed.
