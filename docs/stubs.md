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
| HP-UX LVM (`PartitionTable::HpuxLvm`) | Editing, LV resize and resize-on-restore are refused; whole-disk CHD backup only | `partition/editor.rs`, `partition/restore_patch.rs` | Round-trip the VGDA (both copies, timestamps, VGSA) before touching extents |
| HP-UX LVM, AIX LVM | Multi-disk volume groups: LVs with extents on another PV are listed but refused | `partition::lvm_rows`, `LVM_UNAVAILABLE` | Out of scope by decision (single-disk only) |
| AIX LVM (`PartitionTable::AixLvm`) | Editing, LV resize and resize-on-restore are refused; whole-disk CHD backup only | `partition/editor.rs`, `partition/restore_patch.rs` | Round-trip both VGDA copies and the VGSA before touching partitions |

## Filesystems

| Filesystem | Gap | Where | To finish |
|---|---|---|---|
| AIX JFS (`fs::jfs1`) | Read-only, no fsck; volumes with compression enabled (`s_compress`) refuse file reads | `fs/jfs1.rs` | Decode JFS1's LZ page compression; then writes, verified on a real AIX |
| VxFS (`fs::vxfs`) | Read-only; disk layouts 5 and later (HP-UX 11.23 / 11.31) are refused; no fsck; no compaction (backup copies it raw) | `fs/vxfs.rs` | Layout 5+ differences (64-bit fields, new OLT records), then writes; verify against the hp9000-715 HP-UX `fsck_vxfs` |
| AIX JFS2 (big-endian JFS2) | **Shelved**: no AIX 5L+ fixture to verify against | plan item 3.4 | Make `fs/jfs.rs` endian-generic once a fixture exists |

## Optical

| Area | Gap | Where | To finish |
|---|---|---|---|
| HP LIF / PReP boot areas | Probed only on plain 2048-byte-sector `.iso` images, not BIN/CUE or CHD | `optical/boot_area.rs` | Read sector 0 through the `opticaldiscs` sector reader instead of the image file |
