# GTK shell desktop data

- `io.github.danifunker.RustyBackup.desktop` is the launcher. Its name matches the app id in
  `src/main.rs`, so GNOME ties the running window to it.
- The icon is `assets/icons/hicolor/256x256` (and the other sizes in `assets/icons/`), installed as
  `share/icons/hicolor/<size>/apps/io.github.danifunker.RustyBackup.png`. The window asks the
  theme for that name, so an uninstalled build shows the generic icon.

`frontends/gtk4/Cargo.toml` carries the `cargo deb` and `cargo generate-rpm` metadata, and the
`build-gtk4` CI job builds the `.deb`, `.rpm` and an Arch `.pkg.tar.zst` from it, named
`rusty-backup-gtk` so they install beside egui's packages.
