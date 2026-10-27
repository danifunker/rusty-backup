# Image-files-only builds (`no-physical-devices`)

`no-physical-devices` is a cargo feature that builds Rusty Backup (GUI, `rb-cli`
and the TUI) without access to physical disks or optical drives. It exists for
sandboxed distribution channels, which refuse raw-device access and privilege
escalation:

| Channel | Why raw access is out |
|---|---|
| Mac App Store | App Sandbox denies `/dev/disk*`; `authopen` / `sudo` escalation is rejected in review |
| Microsoft Store (MSIX) | Packaged apps cannot request `runas` / administrator elevation |
| Flathub / Snap | Confinement blocks block devices without extra interfaces |

The feature is platform-neutral. It works the same way on Linux, macOS, Windows and every
other target, so a future channel with the same restriction needs no new code.

```bash
cargo build --release --features no-physical-devices
```

It composes with any other feature set, including `--no-default-features` slim
builds.

## What changes

Everything that reads disk images keeps working: browse, edit, fsck, backup *from*
an image, restore *to* an image file, CHD/optical image conversion and CD-DA
playback from CHD and BIN/CUE.

These are refused or hidden:

- **Device enumeration** — `os::enumerate_devices()` and the local optical-drive
  list return empty, so the GUI's device pickers show "Physical devices are not
  available in this build" and `rb-cli show devices` errors with that message.
- **Device open** — `os::open_source_for_reading`, `os::open_target_for_writing`,
  `os::open_target_preserving`, the source-reader dispatch and the CLI's
  `open_image_ro` / `open_image_rw` refuse a device path.
- **Elevation** — the GUI's "Unlock / Show Physical Devices" button is replaced by
  an "Image files only" label, and every escalation path (`request_elevation`,
  Linux `pkexec` relaunch, macOS `authopen` / `sudo`) refuses before it runs.
- **Optical drives** — `LocalCdReader`, `PhysicalDiscSource`, drive browsing and
  `eject_disc` refuse; a daemon built this way reports no drives.

Remote daemons are **not** affected. A store build can still connect to an
`rb-cli serve` on another machine and back up its disks or rip its drives,
because that is network I/O, not local device access.

## What counts as a physical device

`os::is_physical_device` decides. A path counts as a device if any of these hold:

- it is named like one: `/dev/...`, `\\.\...`, or `\\?\GLOBALROOT` / `\\?\Volume{` / `\\?\PhysicalDrive`
- on Unix, it resolves to a block or character device node (so a symlink called
  `disk.img` that points at `/dev/sda` is caught)

Stream nodes (`/dev/null`, `/dev/zero`, `/dev/stdin`, `/dev/fd/N`, ...) are not
devices, so scripted `rb-cli` pipelines keep working.

## Scope

This is an in-app gate, not a security boundary. The store sandbox is what
actually enforces the restriction. The gate's job is to never attempt or offer
device access, so the app passes review and users get a clear message instead
of a permission error. The checks sit at the shared chokepoints above. A new
code path that opens a user-supplied path directly with `File::open` should call
`os::ensure_physical_allowed` first.

The physical-device code itself is still compiled in (it is gated at runtime), so
the binary still links the platform disk APIs (IOKit, DiskArbitration, the
Windows ioctl set). These are public APIs, and neither store rejects an app for
linking them. If a channel ever starts objecting to the symbols themselves, the
next step is to compile those modules out behind this feature.

## Not covered yet

Other store restrictions are separate from device access and are not part of this feature:

- **Self-update** (`update.rs`, `self-replace` on Windows): both the Mac App Store and the
  Microsoft Store forbid an app replacing its own binary.
- **File-association registration** (`os::file_assoc`, HKCU writes): MSIX declares
  associations in the package manifest instead.
