# Windows FILE_FLAG_NO_BUFFERING Testing Guide

## Overview

This document describes the Windows-specific changes made to support `FILE_FLAG_NO_BUFFERING` properly and provides a comprehensive testing plan for Windows machines.

## Changes Made

### Problem
Windows `FILE_FLAG_NO_BUFFERING` requires three types of alignment:
1. **Buffer address alignment**: The memory address of the buffer must be sector-aligned
2. **File offset alignment**: All seek positions must be sector-aligned (512 bytes)
3. **Transfer size alignment**: All read/write sizes must be multiples of the sector size

Previous implementation only handled #3, causing "OS error 1" (ERROR_INVALID_FUNCTION).

### Solution
Implemented aligned buffer allocation and tracking similar to Etcher's approach:

#### 1. AlignedBuffer (`src/os/mod.rs`)
- New `aligned_buffer` module (Windows-only)
- Uses `std::alloc::Layout` to allocate memory-aligned buffers
- Provides safe wrapper around raw aligned memory allocation
- Ensures buffer addresses are always sector-aligned (512 bytes)

#### 2. Windows-Specific SectorAlignedWriter (`src/os/mod.rs`)
- Separate implementation for Windows using `AlignedBuffer`
- Tracks file position to ensure offset alignment
- Validates positions before writes/reads
- Aligns seeks to sector boundaries
- Falls back to non-Windows version on macOS/Linux

### Files Modified
- `src/os/mod.rs`: Added `aligned_buffer` module and Windows-specific `SectorAlignedWriter`
- No changes to Windows-specific code in `src/os/windows.rs` (still uses FILE_FLAG_NO_BUFFERING)

## Testing Plan

### Prerequisites
1. Windows 10 or Windows 11
2. Administrator privileges (UAC elevation)
3. USB flash drive or SD card (minimum 1GB, will be erased!)
4. Rust toolchain installed
5. Visual Studio Build Tools (for compilation)

### Build Instructions

```powershell
# Clone or pull latest changes
cd rusty-backup

# Build release version
cargo build --release

# The executable will be at: target\release\rusty-backup.exe
```

### Test 1: Basic Device Enumeration
**Purpose**: Verify the app can see physical drives with FILE_FLAG_NO_BUFFERING

```powershell
# Run the app
.\target\release\rusty-backup.exe

# Expected:
# - UAC prompt appears (accept it)
# - GUI opens without errors
# - Backup tab shows physical drives (PhysicalDrive0, PhysicalDrive1, etc.)
# - No error messages in console or GUI
```

**Pass criteria**:
- [ ] App launches without crashes
- [ ] Physical drives are listed
- [ ] No "OS error 1" messages

### Test 2: Read Physical Drive (Backup Operation)
**Purpose**: Verify reading from physical drive with FILE_FLAG_NO_BUFFERING

**Setup**:
1. Insert a USB flash drive with some data on it
2. Note the drive letter (e.g., E:)
3. Find the PhysicalDrive number (use Disk Management or `wmic diskdrive list brief`)

**Steps**:
```powershell
# 1. Run the app
.\target\release\rusty-backup.exe

# 2. In the GUI:
#    - Go to Backup tab
#    - Select the USB drive's PhysicalDrive
#    - Choose output folder
#    - Click "Start Backup"

# 3. Monitor progress
```

**Pass criteria**:
- [ ] Backup starts without "OS error 1"
- [ ] Progress bar advances
- [ ] Backup completes successfully
- [ ] Output folder contains:
  - [ ] `metadata.json`
  - [ ] `mbr.bin` or `gpt.json`
  - [ ] `partition-N.chd` files
  - [ ] Checksum files

**If it fails**:
- Check the error message
- Check Windows Event Viewer for driver/disk errors
- Note the exact error code and context

### Test 3: Write Physical Drive (Restore Operation)
**Purpose**: Verify writing to physical drive with FILE_FLAG_NO_BUFFERING and aligned buffers

**⚠️ WARNING**: This will ERASE the target drive completely!

**Setup**:
1. Use a spare USB drive or SD card (1GB+)
2. Backup any important data first!
3. Have a backup folder from Test 2

**Steps**:
```powershell
# 1. Run the app
.\target\release\rusty-backup.exe

# 2. In the GUI:
#    - Go to Restore tab
#    - Select the backup folder from Test 2
#    - Select the target USB drive (PhysicalDrive)
#    - Choose restore option (Entire Disk or Minimum)
#    - Click "Start Restore"

# 3. Monitor progress
```

**Pass criteria**:
- [ ] Restore starts without "OS error 1"
- [ ] No alignment errors appear
- [ ] Progress bar advances smoothly
- [ ] Restore completes successfully
- [ ] Drive is bootable/readable afterward (if it was originally)
- [ ] Data integrity matches original

**If it fails**:
- Note exact error message
- Check if error occurs at specific offset/percentage
- Try with different target drives
- Check Event Viewer

### Test 4: Alignment Edge Cases
**Purpose**: Test edge cases with partial sectors and seeks

**Setup**:
1. Create a small test image (100MB)
2. Partition it with FAT32

**Tests**:
a. **Partial sector restore**: Restore with custom partition size (not original)
b. **FAT filesystem resize**: Tests the `inner_mut()` seeking during FAT resize
c. **Multiple partition backup/restore**: Multi-partition disk

**Pass criteria**:
- [ ] No alignment errors during FAT resize operations
- [ ] Seeking works correctly in `inner_mut()`
- [ ] Multi-partition disks restore correctly
- [ ] Partition size changes work without errors

### Test 5: Large Drive Test
**Purpose**: Test with larger drives to catch buffer exhaustion issues

**Setup**:
1. Use a 32GB+ USB drive or larger

**Steps**:
1. Full backup of large drive
2. Monitor memory usage during operation
3. Verify completion

**Pass criteria**:
- [ ] No memory allocation errors
- [ ] AlignedBuffer handles large transfers
- [ ] No buffer overflow issues

### Test 6: Different Sector Sizes
**Purpose**: Test with 4K native drives

**Setup**:
1. Find a drive with 4096-byte sectors (newer SSDs/HDDs)
2. Check with: `fsutil fsinfo ntfsinfo X:` (look for "Bytes Per Physical Sector")

**Steps**:
1. Backup and restore the 4K sector drive
2. Monitor for alignment errors

**Pass criteria**:
- [ ] Works with both 512-byte and 4096-byte sectors
- [ ] No alignment errors
- [ ] Buffer allocation uses correct alignment

## Debugging Tips

### Enable Verbose Logging
Add to the beginning of `main()` in `src/main.rs`:
```rust
env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();
```

### Check Windows Event Viewer
1. Open Event Viewer
2. Windows Logs → System
3. Look for disk/driver errors around the time of failure

### Use Process Monitor
1. Download Sysinternals Process Monitor
2. Filter to rusty-backup.exe
3. Look for failed CreateFile/ReadFile/WriteFile calls
4. Check parameters and error codes

### Common Errors

| Error | Description | Likely Cause |
|-------|-------------|--------------|
| OS error 1 | ERROR_INVALID_FUNCTION | Buffer address not aligned |
| OS error 5 | ERROR_ACCESS_DENIED | Not running as admin |
| OS error 87 | ERROR_INVALID_PARAMETER | Offset not aligned |
| OS error 112 | ERROR_DISK_FULL | Out of space (not alignment) |

## Expected Behavior vs Known Limitations

### Expected:
- Sequential writes to physical drives work perfectly
- Backup operations complete without alignment errors
- Restore operations write correctly to raw devices
- FAT resize operations work (use `inner_mut()`)

### Known Limitations:
1. **Seeking to non-aligned positions**: The `Seek` impl rounds down to sector boundaries. If code expects byte-precise positioning, it may not work correctly.
2. **FILE_FLAG_NO_BUFFERING still required**: The changes don't remove the flag, they just make it work correctly.
3. **Platform-specific code**: Windows implementation differs from macOS/Linux.

## Rollback Plan

If tests fail and the issue is critical:

### Option A: Remove FILE_FLAG_NO_BUFFERING
Edit `src/os/windows.rs` line 570:
```rust
// Change:
FILE_FLAG_NO_BUFFERING,

// To:
FILE_FLAGS_AND_ATTRIBUTES(0),  // No special flags
```

### Option B: Use FILE_FLAG_WRITE_THROUGH
Edit `src/os/windows.rs` line 570:
```rust
// Change:
FILE_FLAG_NO_BUFFERING,

// To:
FILE_FLAG_WRITE_THROUGH,  // Bypass write cache but no alignment needed
```

Both options remove alignment requirements but may have performance impacts.

## Success Criteria

The implementation is considered successful if:
1. ✅ All 6 tests pass
2. ✅ No "OS error 1" occurs during normal operations
3. ✅ Backups complete successfully on various drives
4. ✅ Restores work correctly and data integrity is maintained
5. ✅ No crashes or panics related to alignment
6. ✅ Memory usage is reasonable (no leaks from AlignedBuffer)

## Reporting Issues

If tests fail, please collect:
1. Exact error message and stack trace
2. Drive type and sector size
3. Windows version
4. Operation being performed (backup/restore)
5. Approximate progress when error occurred
6. Event Viewer logs
7. Process Monitor trace (if available)

## Implementation Details for Reference

### AlignedBuffer Safety
- Uses `std::alloc::Layout` for proper alignment
- Implements `Drop` to clean up memory
- Marked `Send` and `Sync` (owns memory exclusively)
- Panics on invalid parameters (capacity/alignment)

### SectorAlignedWriter (Windows)
- Tracks file position separately from File handle
- Validates alignment before each write
- Flushes with padding to maintain sector alignment
- `inner_mut()` flushes before granting access

### Integration Points
- Used in `restore/mod.rs` line 366
- Wraps any File opened with FILE_FLAG_NO_BUFFERING
- Transparent to calling code (same interface)

## Next Steps After Testing

1. If tests pass: Consider this feature complete
2. If tests reveal issues: Debug based on failure patterns
3. Performance testing: Compare with/without NO_BUFFERING
4. Consider adding metrics: alignment hit rate, buffer utilization
