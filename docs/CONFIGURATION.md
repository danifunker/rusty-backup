# Configuration

Rusty Backup stores user configuration in a platform-specific location:

- **Linux/macOS**: `~/.config/rusty-backup/config.json`
- **Windows**: `%APPDATA%\rusty-backup\config.json`

Configuration can also be placed in the current directory or executable directory, but the user profile location takes priority.

## Settings

All settings can be configured through the GUI (Settings button in top bar) or by manually editing the config file.

### Available Options

```json
{
  "update_check": {
    "enabled": true,
    "repository_url": "https://github.com/danifunker/rusty-backup"
  },
  "last_chd_codecs": null,
  "last_chd_hunk_size": null,
  "file_associations_enabled": false,
  "assoc_registered_version": null
}
```

- **update_check.enabled**: Enable/disable automatic update checking at startup.
- **update_check.repository_url**: GitHub repository URL for update checks.
- **last_chd_codecs**: Last-used CHD codec spec (e.g. `"lzma,zlib,huff,flac"`),
  remembered across launches. `null` = profile default.
- **last_chd_hunk_size**: Last-used CHD hunk size in bytes. `null` = profile
  default.
- **file_associations_enabled** *(Windows only)*: When `true`, the app
  registers itself as a handler for its supported disk-image extensions in
  `HKCU\Software\Classes` on launch. Toggled by the Settings dialog and by
  the Inno installer's optional "Associate disk image files" checkbox.
  Registration is per-user (no elevation), idempotent, and refreshed
  automatically after a self-update adds new extensions. Windows still
  prompts the user before changing the *default* handler — this only
  enrolls the app in "Open with".
- **assoc_registered_version** *(Windows only)*: Internal marker recording
  which `APP_VERSION` registered associations last; lets a self-update
  trigger re-registration when the central extension list has grown.

### Windows install + self-update

`Setup.exe` writes a per-user install to `%LocalAppData%\Programs\Rusty Backup`
by default; in-app "Download & Install Update" replaces both `rusty-backup.exe`
and `rb-cli.exe` in place and refreshes the "Add/Remove Programs"
`DisplayVersion` so the ARP entry never goes stale. Portable ZIP copies also
self-update, but they skip every registry write (no ARP entry, no file
associations) — they remain truly portable.

CHD compression and decompression are built into the binary via
[libchdman-rs](https://github.com/danifunker/libchdman-rs); there is no
external `chdman` dependency to configure. See `docs/chd_native.md` for
details.
