# Resume prompt: audit sweep, leg 1 of 3 (Linux)

Run this leg first, on the Linux development box. Leg 2 is
`docs/RESUME-audit-2-windows.md`, leg 3 `docs/RESUME-audit-3-macos.md`;
each starts by pulling `bugfixer1` with the previous leg's commits.

## Prompt

You are continuing the 2026-09-01 codebase audit on branch `bugfixer1`.
The audit produced ~100 findings; every High and most Meds have shipped
(see `git log --oneline e5266da..` for the 2026-09-02 batch, one finding per
commit). This leg finishes the items that can be done and verified on Linux.
Work one finding per commit, in the order below, and commit after each.

Ground rules (all from CONTRIBUTING.md, all bit us this week):

- The pre-commit hook is on: `cargo fmt`, then `cargo clippy --all-targets
  -- -D warnings`. Do not bypass it. Run `cargo test --lib` before each commit.
- Comments: one line, two at most, never three. Say why, not what.
- No non-ASCII in user-visible strings (log lines, labels, dialog text).
- `gui/` never spawns threads; workers go through `model/`. Use
  `model::worker::spawn_guarded` for anything with an `Arc<Mutex<Status>>`.
- Engine code (`src/`) must build on Rust 1.73. Before pushing run
  `bash scripts/preflight.sh` (it is not executable in the checkout; plain
  `scripts/preflight.sh` fails instantly with permission denied and looks
  green through a pipe). It runs the release suite, the MiSTer feature set,
  the 1.73 floor, doc parity and the regression harness.
- Debug builds are line-tables-only and cargo is capped at four jobs
  (`docs/build-memory-crashes.md`). Never raise `-j`, never run two cargo
  builds at once; the OOM killer takes the whole terminal with it.
- Keep tool output bounded (grep / tail). Two earlier sessions died with
  huge outputs in context.

### 1. Hard-rule cleanup: non-ASCII UI strings (C12, A12, B17)

The audit listed em dashes, arrows and bullets in user-visible strings.
As of 2026-09-02 this still finds 59 of them:

```
grep -rnP '"[^"]*[^\x00-\x7F][^"]*"' src/gui src/model --include=*.rs \
  | grep -vP '^\S+:\d+:\s*//'
```

Files: inspect_tab.rs (10), model/hfs_expand_runner.rs (8), metadata_editor.rs
(6), bulk_convert_dialog.rs (6), resize_popup.rs (5), model/text_edit.rs (4),
gui/mod.rs (4), browse_view.rs (4), commander/mod.rs (3), restore_tab.rs (2),
expand_hfs_dialog.rs (2), model/partition_editor.rs (1). Replace with ASCII
(`->`, `-`, `OK`, `Warning`, `...`). Content read from a filesystem and shown
verbatim is exempt. One commit per file group is fine.

### 2. Streaming rule: NTFS create_file buffers the whole file (D16)

`ntfs.rs` (search `fn create_file`) reads the entire source into a `Vec`
before writing clusters. CONTRIBUTING requires 64 KiB to 1 MiB chunks and
never a whole partition or file in RAM. Stream the data run by run.

### 3. Dispatch gaps (F7, F11, F12, F13)

- F7: `fs/mod.rs` has no GPT GUID arms in `compact_partition_reader_by_string`,
  `fs_name_for`, `is_layout_preserving_fs`, `has_defragmenting_writer`,
  `is_expensive_minimum`; type bytes 0x07-as-FAT, 0x11, 0x27, 0xEF fall through.
  Extend the ladders (the F1-F6 commits show the pattern) and add cases to the
  dispatch matrix test.
- F11: `mod.rs` ADFS `density <= 3` gate vs the superfloppy probe (F-format is
  density 4). Confirm with an F-format image before changing.
- F12: type byte 0 is unknown to `is_layout_preserving_fs`, so
  `pick_shrink_target` picks the defragmented size for a bare HFS+ volume.
- F13: HFS signatures at +1024 are accepted before the ext / Minix magics.
  Order the probes so an ext superblock wins.

### 4. Backup / restore (BR9, BR11, BR12, BR14)

- BR9: `model/export_runner.rs` decides "resize needed" from
  `original_size_bytes`, and only FAT resizes in place (A7 now refuses the
  rest). Decide whether Minimum for a compacted layout-preserving partition
  should export the imaged size unchanged; document the rule in the GUI hint.
- BR11: `restore/mod.rs` layout with a subset backup and a non-Original
  alignment can overlap entries that were excluded from the backup. Add a
  layout test with an excluded middle partition.
- BR12: `restore/single.rs` with `target_size: None` never resizes a trimmed
  layout-preserving partition. Test with an HFS+ single-partition restore.
- BR14: `backup/mod.rs` zero-pads a short source silently; log a warning with
  the byte counts.

### 5. Browse / edit / Commander (B15, A8-A11, C11, X11-X14)

- B15: after a text-editor Save the content pane stays on "Loading...".
- A8: Defragment hides only its own button while running.
- A9: `apply_source_event` closes the browse view before the mac-archive
  early return.
- A10: expand-HFS dialog title-bar X while busy orphans the worker.
- A11: dropping a file during a remote inspect is ignored silently; say so.
- C11: the min-size worker has no cancel; Close Drive leaves threads reading
  the device. Add `cancel_requested` to `MinSizeStatus`, check it between
  phases, set it from `close_source`.
- X11: Commander copies drop symlinks silently; log each one.
- X12: host overwrite in `fork_export.rs` / `commander_ops.rs` is unconditional;
  route through the conflict review like image writes.
- X13: `resource_fork.rs` / `binhex.rs` write names as UTF-8 not Mac Roman,
  and `min(63)` can cut mid-character.
- X14: `tar_import.rs` drops `._name` members instead of rejoining them.

### 6. Filesystems (H7, H8, H10, H12-H14, D14, D15, D17-D19)

- H7: alternate MDB / volume header written at `total_blocks*bs-1024`, not
  `partition_end-1024`. Compare with what Disk First Aid expects (leg 3 can
  confirm on a Mac).
- H8: `grow_btree_fork` blocks-per-node wrong when block_size > node_size.
- H10: classic HFS dates are stamped UTC; HFS is local time.
- H12: MFS duplicate detection is byte-exact; MFS is case-insensitive.
- H13: `resize_hfs_in_place` shrink guard compares counts, not the last
  allocated block position.
- H14: MacBinary import drops fdFlags (no `CreateFileOptions` field).
- D14: exFAT `name_hash` upcases ASCII only.
- D15: FAT `data_len as u32` with no 4 GiB guard; refuse with a message.
- D17: NTFS `mft_record_size` 0/1 and unchecked `content_length` panic on
  damaged media; return errors.
- D18: HPFS new files get `FIXED_TIME`; stamp now (see `fs/times.rs`).
- D19: `unix_to_dos_datetime` clamps the year but keeps month/day
  (2108-02-29); clamp the whole date.

### 7. Linux OS and remote (R16, R17), plus hardware checks

- R16: `remote/fs.rs::wire_err` collapses everything to `Other`; keep
  PermissionDenied so the GUI can offer elevation.
- R17: `os/linux.rs` excludes `/dev/fd0` from the device list.
- Hardware: plug a USB stick, mount one partition, start a restore to it.
  R5 (8b2cb2c) must refuse with "still in use" while it is mounted and
  succeed after unmounting. Then restore a backup of a GRUB-booting Linux
  image and check `mbr-gap.bin` exists and the restored disk boots in QEMU
  (BR3, 0ed80ba).

### Close-out

Update this file's status line, run `bash scripts/preflight.sh`, push, then
hand over to `docs/RESUME-audit-2-windows.md`.

Status: DONE 2026-09-02 (e815315..2f0dc50, one finding per commit). BR3 was
then verified in QEMU after grub-pc-bin was installed: a GRUB disk built by
hand (recipe in the session memory `grub-mbr-gap-qemu-recipe`) backs up with
a 245-sector `mbr-gap.bin`, and both the zstd per-partition restore and the
default single-file CHD restore boot to the GRUB marker. R5 needs root (the
guard is umount2 plus an O_EXCL open of the device) and is run by the user
with sudo against the USB stick; result recorded below when done.
Also shipped on the way: the HFS+ trim point reserves the alternate-VH block
(8d07d64), and H7 asks leg 3 to confirm the header placement with Disk First
Aid. `bash scripts/preflight.sh` passed all six stages on e2ac069 (2026-09-02);
its release run caught one fixture the debug runs had hidden behind a pipe.
