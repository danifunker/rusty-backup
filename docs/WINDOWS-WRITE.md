# Windows Physical Drive Write Access

## Problem

On Windows, writing to a physical drive (`\\.\PhysicalDriveN`) fails with
**"Access is denied" (OS error 5)** when the write offset falls within a
partition that has a mounted volume (drive letter). The OS protects mounted
volume sectors even when the process has administrator privileges.

Typical symptom during restore:

```
MBR write (offset 0)                 -> OK
Gap zeros (offset 512..8225280)      -> OK
Partition data (offset 8225280+)     -> Access is denied (os error 5)
```

The MBR and pre-partition gap succeed because they fall outside any volume's
extent. Once the write reaches sectors belonging to a mounted filesystem,
Windows blocks it.

## Solution

Before opening the physical drive for writing, **lock and dismount** every
volume that resides on the target disk using two IOCTLs:

1. `FSCTL_LOCK_VOLUME` (0x00090018) — gains exclusive access to the volume
2. `FSCTL_DISMOUNT_VOLUME` (0x00090020) — forces the filesystem to dismount

The volume handles must remain open for the entire duration of the write
operation. Closing a handle releases the lock and allows Windows to re-mount
the filesystem.

## Implementation

### Volume Locking (`src/os/windows.rs`)

```
lock_and_dismount_volumes(drive_num) -> Result<VolumeLockSet>
```

1. Calls `enumerate_volumes()` to discover all drive-letter volumes
2. Filters to volumes whose `disk_number` matches the target physical drive
3. For each matching volume (`\\.\X:`):
   - Opens the volume handle with `GENERIC_READ | GENERIC_WRITE`
   - Sends `FSCTL_LOCK_VOLUME` via `DeviceIoControl`
   - Sends `FSCTL_DISMOUNT_VOLUME` via `DeviceIoControl`
   - Keeps the handle in a `Vec<SafeHandle>`
4. Returns a `VolumeLockSet` that holds all the locked handles

`VolumeLockSet` is an RAII guard: when dropped, the inner `SafeHandle`s call
`CloseHandle`, releasing the locks and allowing Windows to re-mount volumes.

### Lifetime Management (`src/os/mod.rs`)

`open_target_for_writing` returns a `DeviceWriteHandle`:

```rust
pub struct DeviceWriteHandle {
    pub file: File,
    #[cfg(target_os = "windows")]
    _volume_locks: windows::VolumeLockSet,
}
```

On non-Windows platforms, the struct is just a thin wrapper around `File` with
no extra fields.

### Restore Flow (`src/restore/mod.rs`)

```rust
let device_handle = open_target_for_writing(&target_path)?;
let target_file = device_handle.file;       // partial move
let mut target = SectorAlignedWriter::new(target_file);

// ... write MBR, gaps, partition data ...

drop(target);   // close the physical drive handle

// device_handle._volume_locks is still alive here,
// keeping volumes dismounted for the step-9 reopen

// Step 9: reopen device for FAT flag operations ...

// function returns -> _volume_locks dropped -> locks released
```

The partial move of `device_handle.file` leaves `_volume_locks` alive until
the function returns, ensuring volumes stay dismounted through both the main
write phase and the post-write FAT cleanup phase.

## Alternative Approach (Balena Etcher)

Balena Etcher uses a different strategy: it shells out to **diskpart** to
clean the disk before writing:

```
diskpart /s script.txt
  select disk N
  clean
  rescan
```

This wipes the partition table entirely, which force-dismounts all volumes.
Rusty Backup uses the IOCTL approach instead because:

- It does not destroy the existing partition table before writing the new one
- It requires no external tool invocation
- Locks can be held precisely for the duration needed

## Key Windows API Details

| API | Purpose |
|-----|---------|
| `CreateFileW("\\.\X:", GENERIC_READ \| GENERIC_WRITE, FILE_SHARE_READ \| FILE_SHARE_WRITE)` | Open volume handle (sharing required for lock IOCTLs) |
| `DeviceIoControl(FSCTL_LOCK_VOLUME)` | Exclusive volume lock |
| `DeviceIoControl(FSCTL_DISMOUNT_VOLUME)` | Force filesystem dismount |
| `CloseHandle` | Release lock (called automatically by `SafeHandle::drop`) |

The physical drive itself is opened with `GENERIC_READ | GENERIC_WRITE` and
no special flags (`FILE_FLAG_NO_BUFFERING` is not used, so sector alignment
of buffers is not required).

## Error Handling

If a volume cannot be locked or dismounted (e.g., another process holds it
open), the code logs a warning and continues. In practice, if dismount fails,
the subsequent physical drive write may still fail with "Access is denied" for
sectors in that volume's extent. The user should close any programs using the
target drive (Explorer, antivirus scanners, etc.) and retry.
