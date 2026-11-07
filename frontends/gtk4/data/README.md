# GTK shell desktop data

- `io.github.danifunker.RustyBackup.desktop` is the launcher. Its name matches the app id in
  `src/main.rs`, so GNOME ties the running window to it.
- The icon is `assets/icons/hicolor/256x256` (and the other sizes in `assets/icons/`), installed as
  `share/icons/hicolor/<size>/apps/io.github.danifunker.RustyBackup.png`. The window asks the
  theme for that name, so an uninstalled build shows the generic icon.

Packaging (slice 9 of `docs/gtk4_shell_plan.md`) installs both.
