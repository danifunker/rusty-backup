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
| VxFS (`fs::vxfs`, `fs::vxfs_edit`) | Layout 4 edits only direct-extent files (ten extents) and refuses indirect / typed ones; layouts 5+ refused; no fsck; no compaction (backup copies it raw) | `fs/vxfs.rs` | Layout 5+ differences (64-bit fields, new OLT records), then writes; verify against the hp9000-715 HP-UX `fsck_vxfs` |
| AIX JFS2 (big-endian JFS2) | **Shelved**: no AIX 5L+ fixture to verify against | plan item 3.4 | Make `fs/jfs.rs` endian-generic once a fixture exists |
| QNX 4 (`fs::qnx4`) | Read-only (no edit dispatch arm, no create, no fsck); checked on one real volume (`qnxfs1g.img`) | `fs/mod.rs` `open_editable_filesystem` | Real QNX 4.25 fixtures under QEMU, then edit + create + fsck verified with its `chkfsys` / `dcheck` (`docs/PLAN-qnx.md` Q5) |
| QNX 6 Power-Safe (`fs::qnx6`) | Read-only; tested only on synthetic volumes | `fs/mod.rs` `open_editable_filesystem` | Real QNX 6.5 / 7 media under QEMU to verify read, then edit + create + fsck against `chkqnx6fs` (`docs/PLAN-qnx.md` Q6) |
| QNX 2 (`fs::qnx2`) | No create-blank, and fsck is check-only; 640 KiB ICON disks are untested (no samples) | `qnx2fs` crate | Samples of the 640 KiB format; a formatter and repair verified with QNX `chkfsys` in icon-emu |

## Disk images and flux

| Format | Gap | Where | To finish |
|---|---|---|---|
| Flux captures (KryoFlux, `.scp`, `.hfe`) | GCR (Apple / Commodore) is not decoded from flux; only IBM MFM, IBM FM and AmigaDOS MFM are probed | `rbformats/flux/` | A GCR decoder beside `mfm.rs` / `fm.rs` / `amiga.rs`, checked against `gw convert` output like the others |
| MFM reader capture (`.emu`) | Only the `Intel_iSBC_214` sector format decodes; other formats are refused by name | `rbformats/flux/mfm_emu.rs` | A decoder per format, each checked against a sector dump of the same drive |

## Tapes

| Format | Gap | Where | To finish |
|---|---|---|---|
| Sun QIC tape (SUN2TAPE `.qic`) | Edits (tar / `.tar.Z` / UFS miniroot) are not yet verified on Sun hardware | `fs/sun2tape_edit.rs` | Boot the edited test tapes in `rb-fixtures/to-verify/sun-tape-edit/` on the Sun-2 / Sun-3 MiSTer cores |
| Sun QIC tape | Tape files themselves cannot be added, removed or renamed; the XDR table of contents is copied unchanged | `fs/sun2tape_edit.rs` `TapeEditor` | Rewrite the XDR table of contents alongside the block-0 table |

## Optical

| Area | Gap | Where | To finish |
|---|---|---|---|
| HP LIF / PReP boot areas | Probed only on plain 2048-byte-sector `.iso` images, not BIN/CUE or CHD | `optical/boot_area.rs` | Read sector 0 through the `opticaldiscs` sector reader instead of the image file |
