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
  "last_chd_hunk_size": null
}
```

- **update_check.enabled**: Enable/disable automatic update checking at startup.
- **update_check.repository_url**: GitHub repository URL for update checks.
- **last_chd_codecs**: Last-used CHD codec spec (e.g. `"lzma,zlib,huff,flac"`),
  remembered across launches. `null` = profile default.
- **last_chd_hunk_size**: Last-used CHD hunk size in bytes. `null` = profile
  default.

CHD compression and decompression are built into the binary via
[libchdman-rs](https://github.com/danifunker/libchdman-rs); there is no
external `chdman` dependency to configure. See `docs/chd_native.md` for
details.
