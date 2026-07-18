# RESUME — Double-sided DFS (`.dsd`) support

Self-contained prompt to implement + complete **double-sided Acorn DFS (`.dsd`)**
read/write. Read top-to-bottom, then start at "Where to start coding". A fresh
session can pick this up with no other context.

Companion memory: `dsd-double-sided-dfs.md`. Prior related work this branch
(`more-filesystems`): MFS create+fsck (8c354d3), MOOF (8c354d3), ADFS E-format
create + new-map fsck (1f50641), ADFS old-map D-format read (4cb4a3c) + write
(3584d26). All committed; tree green.

---

## 0. Mission

`.dsd` is a **double-sided Acorn DFS** disc image: two independent DFS volumes
(BBC "drive 0" + "drive 2") stored **track-interleaved**. Our `fs::dfs` driver
reads single-sided `.ssd` only; `.dsd` currently falls through to "Unknown" /
carve (deliberately — `src/fs/dfs.rs:~11` calls it a follow-up and
`looks_like_dfs` separates `.ssd` from `.dsd` by size).

Goal: de-interleave `.dsd` and present its two sides as **two partitions**
(`disk.dsd@1` = side 0, `disk.dsd@2` = side 1), reusing the existing DFS reader
+ editor unchanged. Read first (validate against real fixtures), then write
(re-interleave on commit).

Design decision already made by the user: **two partitions** (not side-0-only,
not a `--side` flag).

## 1. Confirmed on-disk facts (verified against a real disc)

- `.dsd` track-interleave: logical slot for (track `t`, side `s`) is
  `slot = t*2 + s`. 10 sectors/track × 256 B. An 80-track double-sided disc is
  **409,600 bytes**; each side is **204,800 bytes** (800 sectors).
- **De-interleave side `s`**: concatenate, for `t = 0..tracks`, the 10-sector
  (2560-byte) block at byte offset `(t*2 + s) * 2560`. `tracks = len / (2560*2)`.
- Each de-interleaved side is a **standard DFS volume** the current
  `DfsFilesystem::open(reader, offset)` reads as-is.
- Verified on a real disc (BBC PD music disk): side 0 title "RED_SHIFT", 28
  files (`!BOOT`, `MENU`, …); side 1 = 31 files. Both `total_sectors = 800`.
- DFS catalogue (per side, for detection): sector 0 = title[0..8] + 8×8-byte
  filename entries; sector 1 = title[8..12], `nfiles = s1[5] >> 3`,
  `total_sectors = ((s1[6] & 3) << 8) | s1[7]`. `looks_like_dfs(reader, offset)`
  already encodes this — reuse it.
- `.adl` = **ADFS L-format** (640 KB, 655,360 B, double-sided ADFS) — a
  SEPARATE bonus target (currently errors "Invalid MBR"); out of scope here
  unless you choose to also do it (it's ADFS old-map-ish, not DFS).

## 2. Plan of record → two partitions

Present the de-interleaved image as `[side0 (204800) ‖ side1 (204800)]` and add
a `PartitionTable::Dsd` variant yielding two `PartitionInfo`s at byte 0 and byte
204800 (each `type_name = "Acorn DFS"`, `size_bytes = 204800`). The DFS reader
opens each side at its byte offset.

## 3. Where to start coding (tasks, in order)

### T1 — de-interleave container in `source_reader`
- `src/model/source_reader.rs`: add `pub fn is_dsd_path(path) -> bool`
  (extension `.dsd`, size `409_600` — and de-interleaved side-0 passes
  `looks_like_dfs`; mirror `is_woz_path` / `is_moof_path`).
- In `open_read`, add an `else if is_dsd_path(path)` branch that reads the raw
  `.dsd`, de-interleaves into `side0 ‖ side1`, and returns a `Cursor` over the
  flat buffer (mirror the Twiggy dc42 de-interleave at ~line 1060). Add
  `is_dsd_path` to `is_container_path` (line ~1240).
- Put the de-interleave helper in `src/rbformats/interleave.rs` (where the
  Apple/Twiggy interleave lives) or a small `dfs`-adjacent module:
  `deinterleave_dsd(raw: &[u8]) -> Option<Vec<u8>>` (returns `None` if size
  isn't a clean `tracks*2*2560`).

### T2 — `PartitionTable::Dsd` variant + `partitions()`
- `src/partition/mod.rs`: add variant `Dsd { size_bytes: u64 }` to the enum
  (line ~30). `size_bytes` is the de-interleaved total (409,600).
- In `partitions()` (the arm builder starting line ~827; `None` arm at ~1091):
  add a `Dsd` arm returning **two** `PartitionInfo`s — index 1 at
  `start_lba = 0`, index 2 at `start_lba = 204800/512 = 400`; each
  `type_name = "Acorn DFS"`, `partition_type_string = None`, `size_bytes =
  204800`, `partition_type_byte = 0`. (DFS dispatch is by fs-name, not type
  byte; the resolver opens `DfsFilesystem` at the partition byte offset.)
- `name()` (line ~1162) + `partition_count`/type-byte arms (line ~1169-1177):
  add `Dsd` arms (`name = "DSD"`, type byte 0).

### T3 — detection → return `PartitionTable::Dsd`
- The detect entry that returns `PartitionTable::None { .. }` (lines ~748/783):
  BEFORE the superfloppy `None` fallback, if the image is a de-interleaved
  `.dsd` (length 409,600 AND `looks_like_dfs(reader, 0)` AND
  `looks_like_dfs(reader, 204800)` both `Some`), return `PartitionTable::Dsd`.
  NOTE: detection runs on the ALREADY-de-interleaved stream (T1 de-interleaves
  in `source_reader::open_read` before partition detection sees the bytes), so
  both sides sit at contiguous offsets 0 and 204800 — no interleave math in the
  detector.
- Keep the guard tight so a plain 400K flat image (e.g. an 800K/2-sided GCR or a
  400K FAT floppy) is NOT misread as DSD — requiring DFS catalogues at BOTH
  offsets is the discriminator.

### T4 — thread `Dsd` through the ~29 `PartitionTable::None` match sites
- `grep -rn "PartitionTable::None" src/` → ~29 arms across ~14 files
  (backup/, restore, gui/backup_tab, gui/inspect_tab, gui/physical_disk_export,
  model/source_reader, model/backup_remote, partition/editor, partition/
  alignment, backup/single_file_chd, backup/mod, cli/verbs/optical, fs/
  hfs_boot, fs/make_bootable, fs/hfv).
- Most can **group** `Dsd` with `None` (`PartitionTable::None { .. } |
  PartitionTable::Dsd { .. } =>`) — a `.dsd` side behaves like a superfloppy
  DFS volume for backup/restore/alignment purposes. Compile-error-drive it:
  add the variant, `cargo build`, handle each error, grouping with `None`
  unless a site needs distinct behaviour.

### T5 — validate READ against real fixtures
- Fixtures: `/Volumes/Software/Old Mac Stuff/Acorn BBC - Music Disks
  (2024-07-12)/` — 84 `.dsd`. Use a **PD** one (BBC PD / Negative Charge /
  Bitshifters) so it's committable.
- Commands:
  ```
  rb-cli inspect disk.dsd            # -> 2 partitions, both "Acorn DFS", 200K
  rb-cli ls disk.dsd@1               # side 0 (e.g. RED_SHIFT, 28 files)
  rb-cli ls disk.dsd@2               # side 1
  rb-cli get disk.dsd@1 !BOOT -      # read a real file back
  ```
- Independent oracle for the de-interleave + catalogue (already prototyped in
  this session — reproduce in `scripts/` if you want a committed check):
  Python de-interleave + DFS-catalogue parse; both sides must match `rb-cli ls`.

### T6 — WRITE (re-interleave on commit)
- Editing a side (via `DfsFilesystem` EditableFilesystem, already implemented)
  mutates the flat de-interleaved buffer. On commit, **re-interleave** back to
  `.dsd` order: slot `t*2 + s` ← side `s` track `t`. Add
  `interleave_dsd(flat: &[u8]) -> Vec<u8>` (inverse of `deinterleave_dsd`), and
  wire it into the container-edit commit path (see how `.woz` / `.atr`
  re-encode on commit — `src/model/container_edit.rs`, `is_editable_container_path`
  in source_reader). Add `is_dsd_path` to `is_editable_container_path` once the
  re-encode is wired.
- Validate: `rb-cli put disk.dsd@2 host.txt NEWFILE`, reopen, `ls @2` shows it,
  side 0 untouched, and a Python re-parse of the re-interleaved `.dsd` still
  reads both catalogues.

### T7 — fixtures + docs
- Commit 1-2 PD `.dsd` (zstd-compressed if >~100K, per `tests/fixtures`
  convention) + a read/round-trip test gated on the fixture.
- Add `"dsd"` to `DISK_IMAGE_EXTS` in `src/model/file_types.rs` (+ the
  `association_exts` regression test) if not already present.
- Docs sync (CLAUDE.md pre-commit checklist): README Filesystems / image-formats
  note that `.dsd` double-sided DFS is supported (2 partitions); coverage audit;
  `full_MiSTer_support_status.md` BBCMicro + AcornElectron rows (they currently
  say "Double-sided `.dsd` ... outstanding" — flip to done);
  `filesystem_completion_plan.md` if it mentions `.dsd`.

## 4. Key code pointers (verified this session)

- `src/fs/dfs.rs`: `looks_like_dfs<R>(reader, partition_offset) -> Option<..>`
  (line 259), `DfsGeometry::from_body_len` (102400→400, 204800→800 sectors;
  line 76), `DfsFilesystem::open(reader, partition_offset)` (line 317),
  EditableFilesystem impl (line 656). fs-name = "Acorn DFS"; dispatch string
  "acorndfs" (`src/fs/mod.rs:1455/1820`).
- `src/partition/mod.rs`: `enum PartitionTable` (line 30), `None { size_bytes,
  fs_hint }` (line 64), `partitions()` (line 827; None arm ~1091), detect
  returns `None { .. }` at ~748/783, DFS superfloppy detect
  `looks_like_dfs(reader, 0)` → `"Acorn DFS"` at line 593.
- `src/model/source_reader.rs`: Twiggy de-interleave model ~line 1055-1080;
  `is_woz_path`/`is_moof_path`/`is_atr_path` container-detect patterns;
  `open_read`; `is_container_path` (~1235); `is_editable_container_path` (~1219).
- CLI `@N`: `src/cli/img_at.rs` (`ImageRef`), `src/cli/resolve.rs`
  (`resolve_partition_*`). These read `partitions()` and open the fs at the
  chosen `PartitionInfo::byte_offset()`.

## 5. Gotchas

- **De-interleave BEFORE partition detection** (do it in `source_reader::
  open_read`), so the detector + resolver see contiguous `side0 ‖ side1`. Don't
  try to give the detector interleave math.
- **Tight DSD detection**: require a valid DFS catalogue at BOTH offset 0 and
  204800 (+ size == 409,600). A 400K image with DFS only at side 0 is a plain
  `.ssd`-ish single volume, not a `.dsd`.
- Some `.dsd` are 40-track (2×100K = 204,800) — decide whether to support that
  too (`from_body_len` has 102400→400). Start with the common 80-track/409,600
  case; generalize `tracks = len/(2*2560)` and side size = `len/2`.
- Side 1 title can be empty (real discs do this) — don't reject an empty-title
  side; `looks_like_dfs` should already tolerate it (verify).
- Re-interleave on write must be the exact inverse; round-trip
  `deinterleave(interleave(x)) == x` in a unit test.
- Copyrighted `.dsd` (commercial games/music) stay EXTERNAL like the ADFS
  Repton/Lemmings discs; commit only PD ones.
