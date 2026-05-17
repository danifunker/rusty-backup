# Disk Expansion (XFS and Beyond)

This doc tracks the multi-phase work to let users expand the disks in a backup
or restore so an in-OS tool (IRIX `fx` + `xfs_growfs`, Linux `parted` +
`xfs_growfs`, Windows Disk Management, macOS Disk Utility, etc.) can absorb the
new free space.

The motivating filesystem is **XFS** — IRIX's native FS has no offline grow tool
and `xfs_growfs` only ever extends *up to the partition boundary*. But the
design is filesystem-agnostic: we never touch the filesystem superblock. We
just enlarge the container (and optionally the partition table) and let the
guest OS do the rest.

The same machinery also enables "add a new partition to an existing backup" —
useful for any user who wants to format extra space themselves.

## Modes

Two modes, surfaced as a radio under a single top-level "Add free space for
in-OS expansion" checkbox:

- **Mode A — Leave as unpartitioned free space (default).** Grow the container,
  do not touch the partition table. User boots, runs their OS's partitioner
  (`fx`, `parted`, `fdisk`, Disk Management, ...) to extend or add a partition,
  then runs whatever grow tool their filesystem needs. Works for *any*
  filesystem on *any* OS. This is the "honest" mode — it matches what would
  happen on bare metal if the user swapped in a larger drive.

- **Mode B — Extend last partition automatically.** Grow the container *and*
  patch the partition table so the last partition fills the new space. User
  just runs `xfs_growfs` (or equivalent). Convenience mode; v1 only operates on
  the *last* partition because shifting non-trailing partitions is out of
  scope.

Mode A is always available. Mode B requires partition-table writers (Phase 3).

## Phases

### Phase 0 — Partition bar widget

A reusable read-only visualization of a disk's partition layout, used in three
places: inspect tab (current backup), restore tab (current and planned),
expand-disk dialog (current and planned).

**Component shape:**

```rust
struct PartitionBar {
    segments: Vec<Segment>,
    show_legend: bool,
    show_inline_labels: bool,
}

struct Segment {
    label: String,         // volume name (or "Partition N")
    fs: String,            // filesystem name ("FAT16", "XFS", "ext4")
    size_bytes: u64,
    kind: SegmentKind,
}

enum SegmentKind {
    Partition { color_index: usize }, // cycles through a fixed palette
    Free,                              // solid neutral gray
    Dimmed,                            // APM partition-map, SGI volume header
}
```

**Rendering rules:**

- Bar height ~32–36 px.
- Each `Partition` segment uses a stable color from a fixed palette (cycle by
  index across the disk).
- `Free` segments render solid neutral gray, no hatching, no inline label.
- `Dimmed` segments (APM partition-map entry, SGI volume header) render at
  reduced opacity with italic legend label.
- **Inline labels** (`<volume_name> [<fs>]` inside the segment) only render
  when the segment is wide enough — heuristic ~60 px. Otherwise the segment is
  a colored block; the legend below carries the identification.
- **Minimum segment width** ~6 px pip — applied uniformly so GPT/APM disks
  with many tiny partitions stay visually parseable. Sub-1 MB partitions in
  particular get a pip + legend entry.
- Hover tooltip always shows full details (start LBA, end LBA, size,
  type-code, alignment), regardless of inline label.
- **Not clickable** — pure visualization. Selection happens in the partition
  table below.

**Legend** (below the bar, wraps as needed):

```
[color] Volume [FS] Size    [color] Volume [FS] Size    [gray] Free Size
```

- One row per segment, color swatch + name + `[FS]` + size.
- Free uses the gray swatch with a subtle border so it reads as "not a real
  partition."
- Dimmed segments use a dimmed swatch and italic label.

**Inspect tab integration:**

- Inside a **collapsible "Disk layout" section**, open by default.
- Bar shows the current backup's partition layout.
- Same widget instance can be reused for restore/expand views later.

### Phase 1 — XFS recognizer

Minimal `src/fs/xfs.rs`:

- Detect `XFSB` magic at partition offset 0.
- Read superblock fields needed for display only:
  - `sb_blocksize` (4 KiB typical)
  - `sb_dblocks` (total data blocks)
  - `sb_agcount` (allocation group count)
- No browse / no edit / no compact / no resize — XFS stays a black box.

**Dispatch wiring in `fs/mod.rs`:**

- MBR type `0x83` (Linux) — disambiguate vs ext/btrfs/etc. via XFSB magic at
  offset 0.
- GPT Linux-filesystem GUID — same magic check.
- SGI disklabel partition-type `7` (xfs).
- `fs_name_for` returns "XFS".
- `is_browsable_type` / `is_checkable_type` / `is_editable_type` all false.

Inspect tab shows "XFS" + block count + AG count.

### Phase 2 — Mode A (container grow, no partition-table edits)

The smallest useful slice that ships expansion to users.

**Top-level checkbox** in restore tab + new inspect-tab "Expand disk…" dialog:

```
[x] Add free space for in-OS expansion
    Total disk size: [ 1974 ] MB   (current: 950 MB)
    Free space:      [ 1024 ] MB
    [o] Leave as unpartitioned free space (recommended)
    [ ] Extend last partition automatically  (Phase 4)
```

- Two text fields, bidirectionally bound (`free = total - current`).
- **Update on commit** — Enter or focus loss. Avoids flickering bar while the
  user types. Standard egui pattern.
- Visualization is `PartitionBarPair` (two stacked `PartitionBar`s — current
  and after) sharing horizontal scale so the After bar is wider by exactly the
  growth ratio.
- No drag handles; text fields only.

**Model layer:**

- `src/model/expand_runner.rs` — Status struct + runner spawning a worker that
  zero-extends the container.
- For per-partition backups (Zstd/Raw/VHD): grow `metadata.json` `total_size`,
  no per-partition file changes needed (free space is implicit between the
  last partition's end-LBA and the disk end).
- For single-file CHD: rewrite the CHD with the larger virtual size, zero-fill
  the new tail.
- Update `metadata.json` to reflect the new total disk size.

**Restore-time wiring in `restore/mod.rs`:**

- New `RestoreOptions::add_free_space_bytes: u64`.
- When > 0, the reconstructed disk's total size is `partition_table_size +
  partitions + add_free_space_bytes`. Partition table stays as-is.

Phase 2 ships Mode A for all backups. The user does both partition-table edit
and filesystem grow in their guest OS.

### Phase 3 — Partition-table writers

Needed for Mode B and Phase 5. One writer per scheme; all take `(table,
partition_index, new_size_sectors)` and emit patched bytes:

- **MBR** — extend `patch_mbr_entries` to recompute CHS for the resized entry.
- **GPT** — patch `ending_lba`, recompute header + entry-array CRCs, rewrite
  backup GPT at end of disk.
- **SGI disklabel** — new writer. Patch the partition entry's `num_blocks`,
  recompute the disklabel 32-bit checksum (sum-to-zero).
- **APM** — patch `pmPartBlkCnt` on the target entry; no checksum.
- **RDB** — out of scope (XFS doesn't run on Amiga). Refuse expand on RDB.

Each writer also gets an "insert new entry into free space" variant for
Phase 5.

Pure library code with unit tests against synthetic tables. No GUI changes.

### Phase 4 — Mode B (extend last partition automatically)

Activates the second radio option from Phase 2.

- When selected, the After bar renders the new free space in the last
  partition's color (signaling absorption) instead of gray.
- Restore / expand worker calls the appropriate Phase 3 writer to patch the
  partition table after growing the container.
- Refuses if the target partition is not the last one or if there's a gap
  after it.

### Phase 5 — Add new partition in free space

Independent feature that reuses Phase 3.

- Inspect tab gets an "Add partition in free space…" action when the disk has
  trailing free space.
- Dialog: pick start offset (default = first free sector), size, partition
  type. Writer inserts a new entry; container grows if needed; partition data
  is zero-filled.
- User boots and formats with their native tool.

## Constraints and edge cases

- **XFS partition must be last** for Mode B in v1. We will not shift partitions.
- **Non-trailing free space** (gaps between partitions) is renderable in the
  bar but not editable in v1. The free-space input only affects trailing free
  space.
- **RDB disks** refuse expand entirely (no XFS-on-Amiga use case).
- **GPT requires rewriting the backup GPT** at the new end of disk. Don't
  forget to update both copies.
- **APM partition map** has a self-entry whose size must stay consistent with
  the number of map blocks; rewriters must not corrupt this.
- **SGI disklabel checksum** is sum-to-zero over the entire 512-byte label
  with the checksum field zeroed during computation.

## Component reuse

- `PartitionBar` from Phase 0 is reused in inspect tab, restore tab, expand
  dialog. Build it once, use it everywhere.
- Phase 3 writers feed both Phase 4 (extend last) and Phase 5 (insert new),
  no duplicate logic.
- The expand-disk dialog mirrors the existing `ExpandHfsDialog` shape (worker
  + Arc<Mutex<Status>> + progress UI), just with a different worker function.

## Out of scope (v1)

- Shrinking. XFS doesn't support it; for other filesystems we already have
  shrink paths in their own resize functions.
- Shifting non-trailing partitions to make room.
- Touching XFS internals. We never read the XFS superblock for resize
  purposes — only for display.
- Per-partition free space (between two existing partitions). The free-space
  input is always trailing.
