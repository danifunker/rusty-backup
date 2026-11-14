# Resize Image: one guided flow for a bigger (or smaller) disk image

**Status: decided (2026-11-13), being built on `resize-image-wizard`.** Decisions in section 5.

Today growing a disk image takes three separate tools, in the right order, with nothing joining
them: **Expand Image** (pad the file), **Resize Partitions** (move and grow the table's entries,
and patch each filesystem's own size), and per-filesystem resize (`rb-cli resize`). Each works;
the user has to know that all three exist, run them in order, and check the result themselves.
This plan puts them behind one wizard, started from Inspect, and retires Expand Image as its own
dialog.

## 1. What the user asked for

- Start from Inspect, on the image that is open.
- Walk through it: the disk's new size, then the partitions, then the filesystems.
- Check the volumes before and after. Optional, **on by default**, so a partly corrupted disk
  is found before it is changed rather than after.
- Adjust each partition's size, and leave free space on purpose (for a partition added later).
- Absorb Expand Image (#11): growing the file is step 1 of this flow, not a separate dialog.
- Show which partition is which: the number, the volume name, the layout colour Inspect uses,
  and a used / free bar per volume (as Expand HFS Volume now does).

## 2. The steps

1. **Disk size.** The current size, a new size (or "+N MiB"), and what it leaves: the bar shows
   the new free space at the end. Shrinking is allowed down to the end of the last partition's
   minimum; the step says how far that is.
2. **Check first** (on by default). Run each partition's fsck read-only, in a job, one row per
   partition: clean, repairable, damaged, or no checker for this filesystem. A damaged volume
   offers Repair (the existing repair, on the image) or Skip; the flow will not grow a damaged
   filesystem without the user choosing to.
3. **Partitions.** The existing Resize Partitions table and its Current / After bars, now inside
   the wizard: each row's size as Keep / Fill / Minimum / a typed size, in the row's colour, with
   its used / free bar. The space not given to any partition stays free at the end, shown on the
   bar, so "leave room for another partition" is just not filling it.
4. **Review.** One page that says everything that will happen, in order: the file grows to X,
   partition 2 moves from A to B, partition 3 grows from C to D and its HFS volume with it, then
   the checks run again. Writing to a copy is the default for an image file (`name-resized.img`
   beside it), with "change this file in place" as the alternative.
5. **Work.** One job, its steps listed with a tick as each finishes: copy (when writing a copy),
   grow the file, move and resize the entries, grow each filesystem, check again. Cancel stops
   between steps; a step that fails says which and leaves the copy for inspection.
6. **Done.** The checks' results, Open in Inspect, and the log.

## 3. Where the pieces come from

| Step | Today | Change |
|---|---|---|
| Grow the file | Expand Image (`InspectAction::ExpandImage`, `rb-cli grow`) | becomes step 1 |
| Move / resize entries, patch the filesystem's size | `model::partition_resize::run_resize` (`partition::resize::apply_resize`) | reused as is |
| Grow a filesystem past its old size | `rb-cli resize` per filesystem | lifted into a model call the job runs per partition |
| Check / repair | Inspect's Check (fsck + repair) | run per partition as steps 2 and 5 |

The controller is new, `controller::resize_image`, and it owns the steps, the plan and the job.
`controller::resize` (the table page) is reused inside step 3 rather than copied.

## 4. Every front end, and the CLI

- The four GUIs and the TUI, per the fan-out rule. The steps are the same everywhere; the TUI
  draws the bars as text.
- `rb-cli resize-image IMG --size 2G [--part N=SIZE|fill|min ...] [--no-check] [--in-place]`,
  the same plan without the questions. `rb-cli grow` stays as the one-step padding verb.
- A tier 8 flow per shell: grow a two-partition MBR image, leave 64 MiB free, check before and
  after, and verify with `rb-cli` that the second filesystem grew and the free space is there.

## 5. Decisions

1. **Image files only** for the first version. A card is backed up, resized as an image and
   restored; resizing a device in place can follow later.
2. **What can grow or shrink.** The engine (`partition::resize::apply_resize`) already resizes
   FAT, NTFS, exFAT, HFS, HFS+, ext2/3/4, btrfs, SFS, PFS3, AFFS, EFS, EFS v1, QDOS, ProDOS and
   Human68k with their partitions, and refuses any other filesystem before writing
   (`preflight_resizable`). The Review page names each partition's filesystem and says whether
   it resizes; a partition that cannot resize keeps its size.
3. **Grow and shrink** from the start; shrinking stops at each partition's minimum (Inspect's
   Calc min).
4. **A copy by default** (`name-resized.ext` beside the original); changing the file in place
   is the alternative on the Review page.
5. **Expand HFS Volume folds in.** Growing classic HFS in place keeps its allocation block size,
   which caps the volume (65,535 blocks). When a typed size passes that cap, the wizard offers
   the rebuild instead: the HFS partition is cloned at a larger block size (the Expand HFS
   engine, `model::hfs_expand_runner`) into the new disk. The separate dialog then goes.
