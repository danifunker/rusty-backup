# Agent task: make CHD output actually be single-file

## Mission

`CLAUDE.md:103` states an invariant:

> CHD output never produces per-partition CHDs — the single-file layout is
> the only CHD shape rusty-backup writes.

**It is not true today.** For every source that `single_file_chd::is_supported`
rejects, `backup --format chd` falls through to the per-partition loop and
emits `partition-N.chd` files. Three source shapes hit this, one of them
producing *two* CHDs from a single floppy, and one of them producing a `.chd`
that is not a disk image at all.

Your job is to restore the invariant. That is the whole mission — **not** to
add features. Read `CONTRIBUTING.md` and `CLAUDE.md` in full first.

This was found while closing the last `backup` gaps for the disk-label
partition schemes (commits `8d814b7`, `9cd5249`, `ac34203`, `30d4f9b` on
`round2-newfilesystems`). Those commits built the machinery you will reuse.

---

## Ground truth (measured 2026-09-10 — re-verify each before relying on it)

Every claim below was produced by running the commands, not by reading code.
Re-run them; if any disagrees, trust the run and update this file.

### The three leaks

| Source | `backup --format chd` produces |
|---|---|
| Superfloppy (`PartitionTable::None`) — **the common case, every floppy** | `partition-0.chd` |
| DSD (double-sided Acorn DFS) | `partition-1.chd` **and** `partition-2.chd` |
| X68k | `partition-0.chd` |

Reproduce:

```bash
rb-cli new floppy fat /tmp/fl.img --size 1440K
rb-cli backup /tmp/fl.img /tmp/bk --name job --format chd && ls /tmp/bk/job
```

### The superfloppy `.chd` is not a disk image

Same 1.44 MB floppy, two paths:

| Path | CHD logical size | Total units | File size |
|---|---|---|---|
| `convert --format chd` | 1,474,560 | 2,880 | 384 B |
| `backup --format chd` -> `partition-0.chd` | **8,704** | **17** | 312 B |

The compact reader packed the FAT volume down to its used extent and the CHD
recorded *that* as the disk's size. So the file is an 8.5 KB packed fragment
wearing a `.chd`: `chdman info` reports an 8.5 KB hard disk, MAME would see an
8.5 KB drive. It fails the promise `CLAUDE.md` makes for CHD output
("`chdman info` opens it, MAME loads it").

The **backup folder still restores correctly** — `metadata.json` carries both
the compacted and original sizes and re-expands on restore. Only the `.chd`
taken on its own is not a usable disk image. Do not "fix" restore; it is fine.

Note the file sizes: 384 B whole-disk vs 312 B packed. Making it a real disk
image costs essentially nothing, because the padded free space is zeros and
CHD compresses that away.

### The fallback the code claims does not exist

`src/backup/mod.rs:1487` says:

> Superfloppies route through the per-partition loop with
> `effective_compression` forced to `None` (raw .img) ... We don't emit
> per-partition CHDs anywhere; CHD output is single-file or nothing.

`src/backup/mod.rs:1707` is the only assignment:

```rust
let effective_compression = config.compression;
```

Nothing forces anything, ever. The comment describes behaviour that was never
implemented. **Fix or delete that comment as part of your change** — it is what
made a previous reader (me) treat `partition-0.chd` as an intended shape.

### CHD needs no partition table

Verified, so do not go looking for one to infer. `compress_chd`
(`src/rbformats/chd.rs:356`) takes `logical_size`, `hunk_size`,
`unit_size` (512) and codecs — nothing else. Geometry metadata (`GDDD`:
`CYLS/HEADS/SECS/BPS`) is derived from the logical size inside
`libchdman-rs`. A CHD is a hunk-backed block stream; partitioning is the
guest OS's business. `rb-cli convert --format chd` on a bare FAT floppy
already produces a valid CHD with no table anywhere in the path.

### "Superfloppy" is not "floppy"

`PartitionTable::None` covers two populations, and this decides the answer to
"should CHD even be supported here":

```
fs.sfs.workbench-dh0.hd.img        498.6 MiB   Partition table: None   (Amiga HDF)
fs.hfv.populated-macos81.hd.hfv    300.0 MiB   Partition table: None   (BasiliskII HFV)
```

Amiga HDFs, BasiliskII HFVs (up to 2047 MB), bare SquashFS, bare ext4 — these
are **partitionless hard disks**, exactly what CHD exists for. Dropping CHD for
`PartitionTable::None` would take it away from them. Keep it; fix the shape.

---

## Code map

| Location | What it is |
|---|---|
| `src/backup/mod.rs:1029` | `single_file_chd_planned` gate — carries the `!is_superfloppy` exclusion |
| `src/backup/mod.rs:1487` | the stale comment described above |
| `src/backup/mod.rs:1707` | `let effective_compression = config.compression;` |
| `src/backup/single_file_chd.rs:212` | `is_supported` — the scheme allowlist |
| `src/backup/single_file_chd.rs:677` | `debug_assert_eq!` "streams sized to the partition extent" |
| `src/backup/single_file_chd.rs:2159` | `PartitionTable::None \| Dsd` bail inside `build_patched_head_segments` |
| `src/fs/mod.rs:1077` | `packed_partition_reader_padded` |
| `src/fs/mod.rs:1087` | `if partition_type_string.is_some() { return compact_partition_reader(...) }` — **the bug in task B** |
| `src/partition/mod.rs:177` | `partitions_overlap` |
| `src/partition/mod.rs:194` | `whole_disk_body_reason` |
| `src/partition/mod.rs:210` | `whole_disk_partition` |
| `src/restore/mod.rs:1304` | `run_single_file_chd_restore_as_is` — plain `io::copy`, table-agnostic |
| `src/restore/mod.rs:1453` | `run_single_file_chd_restore_resize` |
| `src/rbformats/mod.rs:353` | the `is_x68k` restore branch (zero-fills the boot region) |

---

## Task A — superfloppy (contained; do this one first)

Route `PartitionTable::None` through the single-file layout as one whole-disk
segment. No head region, no table, no inference.

Two edits are needed, and the second is the one that bites:

1. `src/backup/mod.rs:1029` — drop `!is_superfloppy` from the gate.
2. `src/backup/single_file_chd.rs:2159` — `build_patched_head_segments` bails
   for `PartitionTable::None`. It needs an arm returning **no head segments**
   (`Ok((Vec::new(), None))`), the same shape the Sun arm takes when a slice
   starts at byte 0.

I prototyped edit 1 alone and got:

```
error: backup failed: assemble_from_staging: superfloppy / double-sided-DFS
sources are not supported
```

so do not assume the gate flip is sufficient.

**Why this one is clean.** A superfloppy carries
`partition_type_string == None` and `partition_type_byte == 0`, so it falls
through `packed_partition_reader_padded`'s early return into the `0x00` branch,
gets `detect_filesystem_type` -> `fat_compact_reader`, and **is padded back to
the full partition extent**. The stream is the right size, the free space is
zeros, the CHD compresses it away. The padding machinery already does the work.

**Acceptance:**
- `backup --format chd` on a floppy writes one `<name>.chd`, never `partition-0.chd`
- `metadata.json` records `layout: single-file-chd`
- the CHD's logical size equals the source image size (not the packed extent)
- restore round-trips; with `--sector-by-sector` it is byte-identical
- old per-partition backups still restore (restore branches on the recorded
  `layout`, so verify with a folder made *before* your change)
- the same for a large partitionless **hard disk**, not just a floppy — use
  `fs.hfv.populated-macos81.hd` and `fs.sfs.workbench-dh0.hd`

---

## Task B — X68k (needs more; do not bundle it with A)

Task A does **not** fix X68k, and a previous session nearly assumed it would.
X68k is not a superfloppy; it has a real table with a partition at LBA 32, so
it takes the per-partition branch. Two independent blockers:

### B1. The padding early-return

`src/fs/mod.rs:1087` assumes a type string implies a layout-preserving reader:

```rust
if partition_type_string.is_some() {
    return compact_partition_reader(...);   // unpadded
}
```

X68k partitions carry `partition_type_string = Some("human68k")`, and the
Human68k reader **packs** (`compacted_size` 16,832,000 < `original_size`
33,538,048). So the stream is short and trips the `debug_assert` at
`single_file_chd.rs:677`. In release that assert is compiled out and the code
falls back to raw passthrough with a log line — safe, but it silently loses
compaction.

Fix: make the type-string path pad like the byte path does. Audit which other
type-string filesystems pack; do not assume `human68k` is the only one.

### B2. Resize-on-restore regresses — UNSOLVED, and the real work

This works **today** and must not be lost:

```bash
rb-cli new hd x68k /tmp/x.img --size 32M
rb-cli backup /tmp/x.img /tmp/bk --name job --format chd
rb-cli restore /tmp/bk/job /tmp/out.img --size minimum
rb-cli inspect /tmp/out.img     # partition shrunk 32.0 MiB -> 16.1 MiB
```

`patch_x68k_entries` rewrites the table. Under my single-file-CHD prototype the
same command produced an image that **no longer parses at all**
(`Unrecognized media`), and the resize refusal in
`build_patched_head_segments` did **not** fire.

I verified the symptom, not the mechanism. Start here. The guard compares
`o.export_size != p.size_bytes || o.effective_start_lba() != p.start_lba`;
work out why the restore-side overrides compare equal while a resize still
happens further down (the run logged `Patched BPB hidden sectors: 0 -> 32`).

### B3. There is no X68k HDD fixture

The corpus has only `.d88` floppies (`fs.human68k.2dd.floppy`,
`fs.human68k.x68k-game`). Everything above was verified on disks made by
`rb-cli new hd x68k`, which produce the **512-byte synthetic** shape — not the
real SASI (256 B) or SCSI (1024 B) geometries.

**Get a real fixture before shipping B.** It matters because of B4.

### B4. Why the X68k boot region is not just "boot code"

Today's per-partition restore zero-fills everything ahead of the table
(`src/rbformats/mod.rs:353`) — 0x400 on SASI, 0x800 on SCSI. Per
`src/partition/x68k_ipl.rs`, that region holds:

- **SCSI**: the literal `X68SCSI1` signature, an 8-byte geometry descriptor, a
  40-byte Keisoku Giken string, IPL code at 0x400
- **SASI**: byte 0 is a 68000 `BRA.W` whose target holds the IPL menu code

And `x68k::detect_sector_size` (`src/partition/x68k.rs:110`) reads **the first
8 bytes** to choose the sector size: `X68SCSI1` -> 1024, SASI sig -> 256, else
512. Those bytes decide the unit the table's `start_sector` fields are counted
in.

So zeroing them does not merely make a disk unbootable — a real SCSI disk falls
back to 512 and its partitions are reinterpreted at half their true byte
offsets. Demonstrated in the opposite direction: stamping `X68SCSI1` onto a
512-unit disk makes `inspect` fail with `Unrecognized media`.

This is the actual justification for doing task B at all. It is a fidelity bug,
not a nicety.

---

## Task C — DSD (a decision, not a fix)

A `.dsd` is two Acorn DFS sides stored **track-interleaved**, which the reader
de-interleaves. A whole-disk CHD would therefore hold the de-interleaved form,
and restoring it would not reproduce a `.dsd`. So do **not** add DSD to the
single-file layout.

That leaves the question the invariant forces: when a source genuinely cannot
use the single-file layout, should `--format chd`

- **fail** with "CHD output is single-file only; use `--format zstd`" (consistent
  with how the disk-label schemes now behave — see `LABEL_BACKUP_NEEDS_CHD` in
  `src/backup/mod.rs`), or
- **silently substitute** raw, as the stale comment intended?

**Ask the user.** They have prior context on the CHD-is-single-file decision.
My recommendation is refuse — substituting hands back an uncompressed image
nobody asked for. Whichever is chosen, the stale comment at
`src/backup/mod.rs:1487` must end up describing what the code does.

---

## Traps

- **Do not infer a partition table.** CHD does not have the concept. See above.
- **Do not change the restore side to match.** Restore keys off
  `metadata.layout`, so old per-partition folders keep working. Verify that
  with a folder created before your change; it is the regression that would
  hurt real users.
- **`debug_assert` hides in release.** `single_file_chd.rs:677` panics in debug
  and degrades to raw passthrough in release. Test both profiles, or you will
  ship a silent compaction loss.
- **Do not widen `LABEL_BACKUP_NEEDS_CHD` to superfloppies.** Those must keep
  backing up in every format; only the CHD *shape* is wrong.
- `--split-size` disables the single-file layout entirely
  (`src/backup/mod.rs:1029`), so CHD + split silently keeps the old shape.
  Decide whether that is acceptable or should warn.

## Verification

```bash
cargo test --lib
scripts/preflight.sh                         # cargo test --release, MiSTer set,
                                             # Rust 1.73 floor, doc parity
./regression-tests/runner/target/release/rb-regress run
```

Baseline on `30d4f9b`: preflight green, 3,164 lib tests, `rb-regress` **381/381**.
Anything below that is yours.

New regression cases belong in
`regression-tests/cases/tier5/roundtrip-label-schemes.toml` (the backup/restore
tier) — it already holds ten cases covering the five disk-label schemes and is
the pattern to copy. Per `CLAUDE.md`'s pre-commit doc sync, check whether the
README's Image/backup-formats table needs a line about what `--format chd` now
produces for a partitionless volume.
