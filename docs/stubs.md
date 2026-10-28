# Stubs and deliberate gaps

Things that are knowingly unfinished: refused with a clear message, or detected
but not implemented. Each entry says where the refusal lives and what would
finish it. Remove an entry when its work ships.

## Partition schemes

| Scheme | Gap | Where | To finish |
|---|---|---|---|
| BSD disklabel (`PartitionTable::Bsd`) | Editing slots is refused ("BSD disk-label editing is not yet implemented") | `partition/editor.rs` | Slot edits that rewrite `p_offset` / `p_size` and restamp the XOR checksum, in the label's own byte order |
| BSD disklabel | Resizing partitions on restore is refused ("no restore-time label patcher for BSD tables") | `partition/restore_patch.rs` | A `patch_bsd` that repacks slots like `patch_next`, keeping the LIF volume and the label's offset intact |
| BSD disklabel | Only whole-disk backup through the single-file CHD layout; per-partition layouts are refused | `backup/mod.rs` | Same as the other label schemes; unlikely to change |

## Filesystems

| Filesystem | Gap | Where | To finish |
|---|---|---|---|
| HP-UX HFS (UFS1 with magic `0x095014` / `0x195612`) | Read-only: editable opens are refused ("HP-UX HFS is read-only for now") | `fs::open_ufs_editable` | Verify that UFS writes keep HP's magic, `fs_clean` handling and long-name directories intact; check edits with HP-UX's `fsck` in the hp9000-715 emulator |
| HP LIF (`fs::lif`) | Read-only | `fs/lif.rs` | Write support only if a use case appears (replacing a boot program) |
| AIX JFS2 (big-endian JFS2) | **Shelved**: no AIX 5L+ fixture to verify against | plan item 3.4 | Make `fs/jfs.rs` endian-generic once a fixture exists |

## Optical

| Area | Gap | Where | To finish |
|---|---|---|---|
| HP LIF / PReP boot areas | Probed only on plain 2048-byte-sector `.iso` images, not BIN/CUE or CHD | `optical/boot_area.rs` | Read sector 0 through the `opticaldiscs` sector reader instead of the image file |
