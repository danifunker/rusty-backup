# Windows Testing Quick Start

## TL;DR
Testing the Windows FILE_FLAG_NO_BUFFERING alignment fix on Windows machines.

## Quick Build & Test

```powershell
# 1. Build
cd rusty-backup
cargo build --release

# 2. Run (as Administrator)
.\target\release\rusty-backup.exe

# 3. Quick smoke test
# - Check if app launches without errors
# - Go to Backup tab, verify drives are listed
# - Try backing up a small USB drive
```

## What Changed?
- Added memory-aligned buffer allocation for Windows (`AlignedBuffer`)
- Windows `SectorAlignedWriter` now handles all three alignment requirements:
  - ✅ Buffer address alignment
  - ✅ File offset alignment  
  - ✅ Transfer size alignment

## Expected Outcome
✅ **Should work**: No more "OS error 1" during backup/restore
❌ **If broken**: Error will occur when reading/writing physical drives

## If It Fails

### Quick Fix (Remove Strict Alignment)
Edit `src/os/windows.rs` line 570:
```rust
// Before:
FILE_FLAG_NO_BUFFERING,

// After (Option 1 - No special flags):
FILE_FLAGS_AND_ATTRIBUTES(0),

// After (Option 2 - Write-through only):
FILE_FLAG_WRITE_THROUGH,
```

Then rebuild:
```powershell
cargo build --release
```

## Full Testing Guide
See `docs/windows-alignment-testing.md` for:
- 6 comprehensive test scenarios
- Debugging tips
- Error code reference
- Detailed diagnostics

## Files to Check
- `docs/windows-alignment-testing.md` - Full testing guide
- `docs/windows-alignment-fix-summary.md` - Implementation summary
- `src/os/mod.rs` - AlignedBuffer and SectorAlignedWriter code

## Key Test Cases

### Test 1: Device List ✓
```
Launch app → Backup tab → See PhysicalDrives listed
Pass: No crashes, drives visible
```

### Test 2: Backup ✓
```
Select USB drive → Choose output → Start Backup
Pass: Completes without "OS error 1"
```

### Test 3: Restore ⚠️
```
Select backup → Select target USB → Start Restore
Pass: Completes without "OS error 1", data is correct
WARNING: Erases target drive!
```

## Common Errors

| Error | Meaning | Fix |
|-------|---------|-----|
| OS error 1 | Buffer/offset not aligned | This fix should resolve it! |
| OS error 5 | Not admin | Run as Administrator |
| OS error 87 | Invalid parameter | Offset not aligned (should be fixed) |

## Success = All Green
- [x] Compiles
- [ ] Launches (no crash)
- [ ] Lists devices
- [ ] Backs up successfully
- [ ] Restores successfully
- [ ] No "OS error 1"

## Questions?
Review the detailed testing doc or check session notes in `.copilot/session-state/`.
