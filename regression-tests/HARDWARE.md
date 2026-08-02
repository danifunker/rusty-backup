# Tier 7 — Physical Backup and Restore

**Status: designed, not executed.** Per the Phase 0 decision, the cases and
the safety machinery get authored and committed now; execution stays gated
until dedicated scratch media is set aside. Nothing in this document runs
unless a human passes `--allow-hardware` *and* a device allowlist exists.

Backup from a real device is read-only and comparatively safe. Restore is
not: it overwrites a physical disk. Every interlock below exists because of
the restore direction.

---

## Safety interlocks

All five must hold or the runner refuses the case and records
`skip-hardware`. They are checked in this order, and failing any one is a
hard stop, never a prompt.

1. **Opt-in flag.** `--allow-hardware` absent means no hardware case runs.
   There is no config-file equivalent and no environment variable, so it
   cannot be turned on by accident or inherited from a shell profile.

2. **Explicit device allowlist.** `--device-allowlist <file>` pointing at a
   gitignored TOML that names each permitted device by *stable identity* —
   serial number and reported model, never just `/dev/sdb` or
   `\\.\PhysicalDrive1`, which renumber between boots. A device not in the
   list is never touched.

3. **Identity re-verification at use.** Immediately before any write, the
   runner re-reads the device's serial and model and confirms they still
   match the allowlist entry. Catches the case where media was swapped or
   the enumeration order changed after the list was written.

4. **System-disk refusal.** The runner independently identifies the booting
   disk and any disk holding a mounted filesystem, and refuses to write to
   them even if allowlisted. Allowlisting your own system disk should be a
   typo, not a loaded gun.

5. **Capacity sanity.** The device's reported capacity must be within an
   expected band for the case. A restore case written for a 512 MiB CF card
   refuses to run against a 4 TB disk.

### Allowlist format

`regression-tests/local-devices.toml`, gitignored:

```toml
[[device]]
label    = "scratch-cf-512"
serial   = "..."
model    = "..."
bytes    = 536870912
platform = "windows"
# Free-text acknowledgement that this device's contents are expendable.
expendable = "CF card reserved for regression runs, nothing of value on it"
```

`expendable` is required and free-text on purpose: it forces whoever adds a
device to write a sentence about it rather than copy a template.

---

## Case shapes

### H-1 — Backup from device (read-only)

Read a physical device to every output format, verify checksums, confirm the
partition table and filesystem metadata match what `inspect` reports for the
same media imaged by other means. Safe to run against any allowlisted device.

- Per output format: raw, zstd, VHD fixed, VHD dynamic, single-file CHD
- Per checksum mode: CRC32, SHA256
- Compact-space on and off
- `--defrag` on and off where the filesystem supports it

### H-2 — Device round-trip

Back up a device, restore it to the same device, back it up again, compare
the two backups. The strongest single hardware assertion available, because
it does not need a known-good reference image.

Guard: the first backup is retained until the second comparison passes, so a
failed restore is always recoverable from the run bundle.

### H-3 — Restore at a different size

Restore to a target larger and smaller than the source, at each supported
sizing mode (original, minimum, custom), asserting alignment preservation
across all four alignment modes. The main risk area for vintage-OS
compatibility and therefore the highest-value hardware tier.

### H-4 — Cross-media restore

Restore an image captured from one physical medium onto a different one — CF
to SD, SD to USB stick, spinning disk to SSD — covering geometry differences
the image-only tests cannot reach.

### H-5 — Degraded media

Bad-sector handling, read timeouts, and write-protected media. Hard to
provision honestly; a deliberately damaged scratch card is the realistic
path. Lowest priority.

### H-6 — Platform-specific device paths

`\\.\PhysicalDriveX` plus volume locking and dismounting on Windows;
`/dev/sdX` and `MNT_DETACH` unmounting on Linux; `/dev/diskX` and
DiskArbitration on macOS. Elevation is required on all three, so these cases
also exercise the privilege-request paths in `src/os/` and
`src/privileged/`.

---

## Media wanted

Recorded so the shopping list is explicit when this tier is enabled:

| Medium | Purpose |
|--------|---------|
| Small CF card (128-512 MB) with a USB reader | The canonical vintage target; small enough that a full round-trip is quick. |
| SD card, same size class | Cross-media restore partner for the CF card. |
| USB stick, a different size again | Third point for size-mismatch restores. |
| External USB HDD or SSD | Multi-partition and large-resize scenarios. |
| A card known to have bad sectors | H-5, if one can be found or manufactured. |

The MiSTer's own SD card is deliberately *not* on this list. It is a working
setup, and a failed restore would cost real time to rebuild. If MiSTer SD
round-trips are wanted later, use a duplicate card.

---

## Runtime characteristics

Hardware cases are slow — a 512 MB round-trip is minutes, not seconds — and
they cannot be parallelised across a single device. They therefore run last,
serially, after every image-based tier has finished, so an interrupted run
still yields the full software matrix.

---

## Enabling this tier

1. Set aside media and write `local-devices.toml`.
2. Run H-1 only, against one device, and confirm the identity checks fire
   correctly by deliberately pointing at a non-allowlisted device and
   verifying the refusal.
3. Only then enable H-2 and above.

Step 2 is not optional. The interlocks are worth exactly as much as the one
time someone verified they actually engage.
