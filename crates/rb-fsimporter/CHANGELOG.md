# Changelog

All notable changes to this crate are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## 0.1.0

First release. Moved out of rusty-backup's `src/fs/dir_import.rs`, with:
- `Host` (`cancelled`, `progress`), `NoHost`, `Cancelled` and `import_dir_with`,
  so a GUI can watch and stop an import between entries.
- `Expander`: Mac archives, MacZip and disk images are the caller's to expand.
  Every method has a default body (no detection, an "unsupported" error), and
  the types its methods take (`ImportOptions`, `ImportStats`, `Importer`,
  `FileEntry`, `EditableFilesystem`) are re-exported from the crate root.
- `preflight_dir_with` / `measure_dir_with` taking the expander; `measure_dir`
  takes it too.
- Feature `mac` for Mac fork rejoining (off by default).
