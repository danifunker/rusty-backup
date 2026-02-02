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
  "chdman_path": null
}
```

- **update_check.enabled**: Enable/disable automatic update checking at startup
- **update_check.repository_url**: GitHub repository URL for update checks
- **chdman_path**: Full path to chdman executable. Set to `null` or omit to use system PATH

## Example

To specify a custom chdman location:

```json
{
  "update_check": {
    "enabled": true,
    "repository_url": "https://github.com/danifunker/rusty-backup"
  },
  "chdman_path": "/usr/local/bin/chdman"
}
```

Windows example:

```json
{
  "update_check": {
    "enabled": true,
    "repository_url": "https://github.com/danifunker/rusty-backup"
  },
  "chdman_path": "C:\\Tools\\mame\\chdman.exe"
}
```
