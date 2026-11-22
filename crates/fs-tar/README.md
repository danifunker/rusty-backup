# fs-tar

Tar archives in and out of any [`disk-core`](https://crates.io/crates/disk-core) filesystem, whichever crate implements it:

- **Export** a whole volume or a selection as `.tar`, `.tar.gz` or `.tar.zst`, with modes, owners, times and symlinks.
- **Import** a tar (plain, gzip, bzip2 or zstd; GNU, ustar and old V7 headers) into any editable filesystem, with a conflict policy (stop, skip or overwrite), `mkdir -p` of implied directories, path-traversal guarding, and attribute inheritance when a file is replaced. Names that aren't UTF-8 keep their raw bytes on filesystems that store them.

- **Line endings**: `ImportOptions::line_endings` converts text files as they are imported (`line_endings::LineEndings`: `Keep` by default, `Unix` for CRLF to LF, `Mac` for CR, `Dos` for CRLF). The decision reads a file's first 8 KiB: no NUL or stray control byte, and not an archive by name or magic. The conversion is a streaming filter that keeps a CRLF split across two reads whole and leaves a lone CR alone outside `Mac` mode; `ImportStats::line_endings_converted` counts the files it changed, and `measure_tar_expanded_with` / `measure_zip_expanded_with` size the converted members.

Features:

- `native-zstd` (default) / `pure-zstd`: the zstd backend for `.tar.zst`.
- `appledouble`: join macOS `._name` sidecars to their files as a resource fork plus type / creator codes. It uses only `fs-mac`'s `codecs` feature (no HFS, no `fs-mac-unicode`), so the tree stays MIT. Without it, sidecars are skipped and counted.

Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed.
