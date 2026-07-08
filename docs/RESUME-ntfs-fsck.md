# RESUME: NTFS fsck + repair (the last of completion-plan Step 1)

**Do this on the Windows machine** — `chkdsk` is the definitive NTFS oracle and
is native there (macOS has no NTFS checker). Everything else builds/tests the
same cross-platform.

## Where we are

`filesystem_completion_plan.md` Step 1 = "real check + repair for the 4 most-used
filesystems". **FAT, exFAT, and ext are done and committed** — NTFS is the only
one left. Reference commits (use them as templates, the shape is identical):

| FS | Module | Commit | Oracle |
|----|--------|--------|--------|
| FAT12/16/32 | `src/fs/fat_fsck.rs` | `936d194` | `fsck_msdos` |
| exFAT | `src/fs/exfat_fsck.rs` | `c7f1960` | `fsck_exfat` (via `hdiutil attach`) |
| ext2/3/4 | `src/fs/ext_fsck.rs` | `81fea79` | `e2fsck` (`brew install e2fsprogs`) |

**Read `src/fs/exfat_fsck.rs` first** — NTFS is closest to it (allocation-bitmap
reconciliation against a walked metadata tree).

## The established pattern (replicate it exactly)

1. New module `src/fs/ntfs_fsck.rs`, registered `pub mod ntfs_fsck;` in
   `src/fs/mod.rs`.
2. One `fn analyze(fs) -> Analysis` (Read+Seek) that produces both the
   `FsckResult` and a concrete `Vec<Fix>`. `pub fn fsck_ntfs` (read-only) and
   `pub fn repair_ntfs` (Read+Write+Seek) both call it — check and repair never
   diverge. See the `Analysis` / `Fix` / `analyze` structure in `exfat_fsck.rs`.
3. Shared result types are in `src/fs/fsck.rs` (`FsckResult`, `FsckIssue`,
   `FsckStats`, `RepairReport`). `FsckResult::is_clean()` = no errors (warnings
   tolerated). Set `repairable: true` per-issue only when a `Fix` is queued.
4. Wire the trait overrides on `NtfsFilesystem` (in `src/fs/ntfs.rs`):
   - `fn fsck(&mut self) -> Option<Result<FsckResult, _>>` in the
     `impl<R: Read + Seek + Send> Filesystem` block (line ~1150) →
     `Some(super::ntfs_fsck::fsck_ntfs(self))`.
   - `fn repair(&mut self) -> Result<RepairReport, _>` in the
     `impl<R: Read + Write + Seek + Send> EditableFilesystem` block (line ~2674)
     → `super::ntfs_fsck::repair_ntfs(self)`.
5. Enable the Inspect "Check" button: in `src/fs/mod.rs`, add `"NTFS"` to the
   token list in `is_checkable_fs_name` (NTFS shares exFAT's `0x07` partition
   byte, so it must reach the button through the *name* path, not
   `is_checkable_type`). Update the `checkable_fs_name_covers_probed_unix_families`
   test (move "NTFS" from the not-checkable list to the checkable list).
6. Tests in a `#[cfg(test)] mod tests` at the bottom of `ntfs_fsck.rs`: load the
   committed fixture, assert clean (no false positives), then corrupt → detect →
   repair → re-check clean. Plus a `#[cfg(windows)]`-gated oracle test that runs
   `chkdsk` (see below).
7. **Doc sync in the same commit** (CLAUDE.md pre-commit checklist): README
   Filesystems table (NTFS `fsck` column `—` → `Yes (check + repair)`),
   `docs/filesystem_coverage_audit.md` NTFS row (`validate` → `Yes`),
   `docs/filesystem_completion_plan.md` (1a NTFS row + the 1b "Most-used first"
   tier + the sequencing step 1 — mark Step 1 **complete**), and a "Check phases
   (NTFS example)" section in `docs/fsck.md`.
8. The pre-commit hook runs `cargo fmt --all`, `cargo check --all-targets`, and
   `cargo clippy --all-targets -- -D warnings`. Clippy is strict — run
   `cargo clippy --all-targets` yourself before committing (it catches test-only
   lints `--lib` misses, e.g. `manual_find`).

Commit message: `feat(ntfs): fsck + repair (...)`, ending with
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## NTFS design — what to check + repair

The core, mirroring exFAT/ext, is **`$Bitmap` reconciliation**: compute the set
of in-use clusters from the MFT and compare it to `$Bitmap` (one bit per cluster,
**1 = allocated**).

**Compute the in-use cluster set:**
- Walk MFT records `0..mft_record_capacity()` (that method already exists,
  `src/fs/ntfs.rs:468`). A record is in-use if its header flags have bit
  `0x0001` (IN_USE) set. Apply the **Update Sequence Array (USA) fixups** when
  reading a record (the last 2 bytes of each 512-byte stride are replaced by the
  USA — the driver's `read_mft_record` may already do this; check).
- For each in-use record, for each **non-resident attribute** (`$DATA`,
  `$INDEX_ALLOCATION`, `$BITMAP`, etc.), decode its data runs
  (`decode_data_runs`, already `pub(crate)` at `src/fs/ntfs.rs:140`) and mark
  every real cluster (`DataRun.length` clusters from the run's LCN). **Skip
  sparse/hole runs** (LCN 0 / the sign that a run is a hole — a data run with no
  LCN advance). Compressed files have holes too.
- Records 0–15 are the metadata files ($MFT, $MFTMirr, $LogFile, $Volume,
  $AttrDef, `.`, $Bitmap, $Boot, $BadClus, $Secure, $UpCase, $Extend). Their
  clusters come out of the same walk (they're normal records with non-resident
  $DATA). `$Boot`'s $DATA covers the boot region; `$MFT`'s $DATA is the MFT
  itself; `$Bitmap`'s $DATA is the bitmap. So a straight walk of all in-use
  records yields the complete metadata + user allocation — **no special-casing
  of fixed layout like ext needed** (NTFS has no fixed block-group metadata; it's
  all files).
- Watch for **`$ATTRIBUTE_LIST`**: large files spread attributes across several
  MFT records. If a base record has an `$ATTRIBUTE_LIST` (0x20), the non-resident
  $DATA runs live in child records referenced by it. v1 option: follow the
  attribute list; simpler v1: detect its presence and, if the volume has any,
  surface a "not fully traced" warning rather than risk a wrong bitmap. Decide
  based on whether the fixture triggers it (small fixtures won't).

**Reconcile** the computed set against `$Bitmap` → `BitmapUsedButFree` /
`BitmapLeaked` (same names/idea as exFAT). Also check:
- **$MFTMirr** — the first 4 MFT records ($MFT, $MFTMirr, $LogFile, $Volume) are
  mirrored at the `$MFTMirr` LCN (VBR field). Compare; repair = copy from $MFT.
- **Boot backup** — the *last sector* of the volume is a copy of the VBR
  (sector 0). Compare; repair = copy sector 0 over it.
- **Volume dirty flag** — `$Volume` record's `$VOLUME_INFORMATION` (0x70)
  attribute, flags field, bit `0x0001`. Warn; repair clears it. (This is the bit
  `chkdsk` sets/reads.)

**Repair:** rewrite `$Bitmap`'s $DATA from the computed set, resync `$MFTMirr`,
fix the boot backup, clear the dirty flag. All are non-destructive metadata
rewrites. Surface (don't auto-fix) anything needing MFT-record rewrites
(cross-links, $ATTRIBUTE_LIST cases).

## pub(crate) surface to add on `NtfsFilesystem` (in `src/fs/ntfs.rs`)

Mirror what ext/exFAT did. Reads go in the `impl<R: Read + Seek>` block
(line ~408), writes in the `impl<R: Read + Write + Seek>` block (line ~1782):

- `pub(crate) fn fsck_geometry(&self) -> NtfsGeom` — a Copy struct with
  `partition_offset, bytes_per_sector, bytes_per_cluster, total_clusters,
  total_sectors, mft_lcn, mftmirr_lcn`. (Fields are on the struct / `NtfsVbr`
  already; just snapshot them.)
- Make `read_mft_record` (line ~497) `pub(crate)` (or add a
  `fsck_read_record(n) -> Option<Vec<u8>>` wrapper that returns None for
  not-in-use / unreadable).
- A `pub(crate) fn fsck_record_clusters(&mut self, record_bytes) -> Vec<u64>`
  that decodes every non-resident attribute's data runs into LCNs (reuse the
  attribute-parsing already in the record parser around lines 300–330 +
  `decode_data_runs`).
- `pub(crate) fn read_named_metafile_data(...)` or specifically read `$Bitmap`'s
  $DATA (there's already `$Bitmap` handling near line 810 for `used_bytes` —
  reuse/expose it) and `$Boot`.
- Writes: `pub(crate) fn write_raw(offset, &[u8])`, `fn flush_writer`, and a way
  to overwrite `$Bitmap` (write to its data-run clusters) and `$MFTMirr`.
- `decode_data_runs` is already `pub(crate)`. `NtfsVbr` is already `pub(crate)`.

Look at how `ext.rs` exposed `fsck_geometry` / `fsck_read_inode` /
`fsck_owned_blocks` / `read_block_bitmap` / `write_raw` — do the analogous thing.

## The Windows oracle: chkdsk

`chkdsk` needs the image as a mounted **volume with a drive letter**. Raw `.img`
files aren't natively mountable, so pick one:

- **Fixed VHD (cleanest, scriptable):** convert the raw image to a fixed VHD and
  mount it, or make the fixture a fixed VHD to begin with.
  ```powershell
  # raw .img -> fixed VHD (needs qemu-img, or use the repo's VHD writer)
  # qemu-img convert -f raw -O vpc -o subformat=fixed in.img out.vhd
  $img = Mount-DiskImage -ImagePath C:\path\out.vhd -PassThru
  $letter = ($img | Get-Disk | Get-Partition | Where DriveLetter).DriveLetter
  chkdsk "${letter}:" /f        # /f fixes; omit /f (or use read-only) to just check
  Dismount-DiskImage -ImagePath C:\path\out.vhd
  ```
  `chkdsk` exit codes: **0 = clean**, 1 = fixed, 2 = should re-run, 3 = could not
  check. Assert 0 (read-only, after our repair) or that a fresh check finds
  nothing.
- **OSFMount / ImDisk** can mount a raw `.img` as a drive letter directly, then
  `chkdsk X: /f`. Good if you don't want the VHD conversion.
- **Best fixture:** create a *real* Windows NTFS volume — `Mount-DiskImage` a
  blank fixed VHD, `Initialize-Disk`, `New-Partition`, `Format-Volume -FileSystem
  NTFS` — that's a gold, `chkdsk`-clean base. Corrupt it, repair with `rb-cli`,
  and `chkdsk` it. (The committed `tests/fixtures/test_ntfs.img.zst` was made by
  our own clean-room formatter and is ntfs-3g-clean; **verify `chkdsk` accepts it
  first** — like the exFAT fixture needed a device, ours may have chkdsk quirks.)

Gate the oracle test on `#[cfg(windows)]` + tool availability so CI/macOS skip
it. The hermetic tests (corrupt→our-repair→our-fsck-clean, byte-compare the
rebuilt $Bitmap) run everywhere.

## Fixture + quick manual loop

```
# decompress the committed fixture
zstd -d tests/fixtures/test_ntfs.img.zst -o ntfs.img
target\debug\rb-cli fsck ntfs.img            # should be clean (no false positives)
# fabricate a $Bitmap leak (flip a free cluster's bit), then:
target\debug\rb-cli fsck ntfs.img            # detects BitmapLeaked
target\debug\rb-cli fsck --repair ntfs.img   # rebuilds
target\debug\rb-cli fsck ntfs.img            # clean
# then chkdsk via a mounted VHD as above
```

## Gotchas

- **USA fixups** on every MFT record and index block — apply before trusting
  record bytes (offset/count in the record header). `read_mft_record` likely
  handles this; confirm.
- **Sparse/compressed data runs** have holes (no LCN) — don't count hole
  clusters as allocated.
- **$ATTRIBUTE_LIST** (0x20) — big/fragmented files split across records; either
  follow it or surface-and-skip (see above).
- **64-bit LCNs**; `$Bitmap` can be large — read it via its data runs, not a
  fixed offset.
- Don't touch `$LogFile` — leave it; `chkdsk` will replay/ignore it. Clearing the
  dirty flag is enough.
- After the first commit lands, update `MEMORY.md` and the
  `fat-fsck-fsck_msdos-oracle` memory to mark **Step 1 complete**, and pick the
  next plan item (Step 2: create-blank for ext + UFS).
