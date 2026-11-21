# rb-fsimporter

Copy a folder from the host into any editable `disk-core` filesystem (EFS, XFS, FAT, ext, UFS, HFS, ...), the other direction from [`rb-fsexporter`](../rb-fsexporter). Entries are visited in sorted order, so the same tree always lands the same way; the shared `fs-tar` import sink handles path safety, `mkdir -p`, the conflict policy and attribute inheritance.

- `import_dir_with(efs, dest, root, &opts, expander, &host)`: a `Host` hears the running `ImportStats` after every entry and is asked `cancelled()` before each one; a cancelled run returns an error that downcasts to `Cancelled`. `import_dir` is the same with a plain progress callback and no expander.
- `preflight_dir` / `measure_dir` / `measure_dir_for` / `projected_volume_bytes`: what an import would skip and write, and how big a volume it needs, without writing. `measure_dir` and the `_with` forms take the expander.
- Options: expand tar and zip archives (`expand_archives`, `flatten_archives`), strip gzip (`expand_gunzip`), skip AppleDouble sidecars, apply host permissions.
- Mac archives and disk images expand only through an `Expander` the caller passes (rusty-backup supplies one); pass `None` otherwise. Every `Expander` method has a default (detection says no, the rest return an "unsupported" error), so an implementer writes only the half it handles. The types it is handed (`ImportOptions`, `ImportStats`, `Importer`, `FileEntry`, `EditableFilesystem`) are re-exported from the crate root.
- Feature `mac` (off by default) rejoins Mac resource forks from `._name` / `.rsrc` sidecars and MacBinary / AppleSingle wrappers on HFS / MFS / ProDOS targets. It pulls in `fs-mac`, which depends on the GPL-2.0-only `fs-mac-unicode`, so a build with it is GPL as a whole. Without it the crate and its dependencies are MIT.

The caller owns `sync_metadata()` (and, for a container, the commit) after an import. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup). Licensed MIT.
