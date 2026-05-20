# PPC port — plans for the next four features

Scope: implementation plans for the four items called out as the
near-term queue in `extendppc.md`:

1. RDB partition table (Amiga)
2. ext3 / ext4 leftovers (inline-data, symlink hygiene)
3. `new --fs <fat12|fat16|fat32>` (blank-FAT formatter)
4. NTFS read

Order above is recommended sequencing (cheapest → most ambitious).

User-confirmed conventions baked into the plans:

- The `new` verb takes the filesystem variant directly as `--fs` value:
  `rusty-backup new --fs fat32 --size 1G out.img`. No auto-pick. Same
  shape will extend to `--fs hfs` / `--fs efs` later if we want.
- `get` on a symlink **refuses** with a hint pointing at the target.
  No symlink following, no writing link-target bytes as content.

---

## 1. RDB partition table (Amiga)

### Why

The shipped AFFS reader works on floppy `.adf` superfloppies. Real
Amiga hard-disk images (`.hdf`, `.hdz`) carry a Rigid Disk Block (RDB)
partition table instead — without it the AFFS reader can't find the
partition to mount.

### On-disk shape

All big-endian (free on PPC).

- Scan sectors 0..15 of the disk for the ASCII magic `R D S K` (some
  reformatted disks push it past sector 0).
- The RDSK block holds:
  - `rdb_PartitionList` — block number of first PART block
  - `rdb_CylBlocks`, `rdb_Surfaces`, `rdb_BlocksPerTrack` (geometry)
  - block size (usually 512; some setups use 1024 — read, don't assume)
- Walk the PART chain via `pb_Next`. For each PART:
  - `pb_DriveName` — BSTR (length byte + chars) at the documented offset
  - `pb_Environment[]` — derives `de_LowCyl`, `de_HighCyl`, `de_Surfaces`,
    `de_BlocksPerTrack` → `start_sector = LowCyl * Surfaces * BlocksPerTrack`,
    `block_count = (HighCyl - LowCyl + 1) * Surfaces * BlocksPerTrack`
  - `pb_DosType` — 4 bytes (e.g. `DOS\0`..`DOS\7` for AFFS, `PFS\3`, `SFS\0`)
- Chain terminates when `pb_Next == 0xFFFFFFFF`.

### Implementation in `rust_cli_real.c`

- New types:
  ```c
  typedef struct {
      uint32_t first;          // start sector (512-byte)
      uint32_t blocks;         // sector count
      char     name[32];       // BSTR-decoded
      char     dos_type[5];    // "DOS\0" etc., null-terminated
  } RdbEntry;
  typedef struct {
      int      entry_count;
      RdbEntry entries[MAX_PARTITIONS];
  } RdbTable;
  ```
- Add `PT_RDB` to `PartTableType`, slot into `PartTable.data` union.
- Add detection branch at the top of `detect_partition_table` (RDSK
  magic search across sectors 0..15) — no collisions with existing
  magics.
- `get_partition_list` PT_RDB branch: emit each non-empty entry;
  `type_name` from `dos_type` (e.g. `"AFFS DOS\\3"`).
- Add `pt_name = "RDB"` to the three `show partmap`-style sites.
- **No** changes needed to the FS-detection chain in
  `resolve_image_ref` — AFFS already sniffs by `"DOS\X"` magic at
  partition offset 0, so once the partition enumerates, browse works.

### Validation

- Build a synthetic Amiga HDF (Rust example using the desktop RDB
  code, or hand-poke RDSK + one PART + one AFFS partition).
- Verify with `show partmap`, `ls image@1 /`, `get image@1 /file`.
- Add `rdb.img.gz` to `ppc-tiger/fixtures/` and extend
  `test_fs_readers.sh` (~3 new PASS lines).

### Cost / risk

- ~300 LOC C, low risk. Almost entirely BE byte arithmetic.
- Watch for: BSTR decoding (length byte, no null), PART pointer being
  a *block number* (multiply by block size, not sector size, if RDSK
  reports 1024-byte blocks).

---

## 2. ext3 / ext4 leftovers

The shipped ext2 reader handles ext4 extent trees, 64-bit GDT fields,
classic indirect maps, hashed directories (correctly via linear leaf
walk), and block sizes 1 KiB – 64 KiB. Two real gaps remain.

### Item A — `INLINE_DATA` (ext4)

ext4 stores tiny file contents directly in the inode's `i_block` area
(60 bytes at offset 0x28). Without handling this, the current reader
reads the 60-byte field as if it were extent pointers and either
returns garbage or fails.

Detection:

- Superblock `s_feature_incompat` has `INCOMPAT_INLINE_DATA` (0x8000)
- AND the inode's `i_flags` has `EXT4_INLINE_DATA_FL` (0x10000000)

Implementation:

- In `ext2_block_list`, detect the inline flag *before* the
  extent/indirect branch; for inline inodes set `bl->blocks = NULL`
  and store the inline payload elsewhere.
- In `ext2_read_inode_data` / `fs_get_ext2`, when inline: write
  `min(in.size, 60)` bytes straight from `in.block`.
- Cap support at the 60-byte tier. Larger inline files use the
  `system.data` xattr — out of scope.

### Item B — symlink hygiene in `get`

Today `get /path/symlink OUT` writes the raw link target (e.g. the
literal bytes `"foo/bar"`) into OUT, because a symlink inode reports a
nonzero size and we obediently extract it.

Fix:

- In `fs_get_ext2`, after `ext2_resolve_path` returns the inode, check
  `(in.mode & 0xF000) == 0xA000` → print
  `Error: <path> is a symlink to <target>; pass the target path` and
  return non-zero.
- Read the target via the existing fast-symlink path (target is in
  `in.block` when `size < 60` AND no extent flag); otherwise read it
  via the regular block list capped at `size`.
- Same treatment for the EFS reader (`(in.mode & 0xF000) == 0xA000`)
  for consistency; EFS already shows `l` in `ls`.

### Out of scope (declined)

- htree-as-index optimization (current linear walk is correct; only
  slow on directories with thousands of entries — vintage images
  almost never hit it)
- Journal replay (only matters on dirty-unmount images)
- xattrs / ACLs (not browse-relevant)

### Validation

- Build a fixture with: a 30-byte file via `INLINE_DATA`, a regular
  file, and a symlink. Verify `get` extracts the inline file's exact
  bytes and refuses the symlink with the right hint.
- Extend `test_fs_readers.sh` (~3 new PASS lines).

### Cost / risk

- ~150 LOC total. Low risk. Inline-data fixture is the only fiddly
  bit — easiest path is to build it via the desktop ext4 lib (the
  editable filesystem there probably doesn't emit INLINE_DATA, so we
  may need to hand-poke the flag on one inode).

---

## 3. `new --fs <fat12|fat16|fat32>` (blank FAT image)

### Surface

```
rusty-backup new --fs fat12 --size <BYTES|N{K,M,G}> [--label NAME] OUT
rusty-backup new --fs fat16 --size <BYTES|N{K,M,G}> [--label NAME] OUT
rusty-backup new --fs fat32 --size <BYTES|N{K,M,G}> [--label NAME] OUT
```

- `--fs` value is the FAT variant directly. No auto-pick — user
  chooses. (Reasoning: same pattern extends naturally to `--fs hfs`
  / `--fs efs` / `--fs ext2` later, each with its own valid-sizes
  policy.)
- `--size` accepts `K` / `M` / `G` suffixes. Reject obviously-bad
  combos at parse time (fat12 > 32 MiB, fat16 > 2 GiB, fat32 < 32
  MiB) with a helpful error suggesting the right variant.
- `--label` defaults to empty (no volume-label entry in root) if
  omitted. If supplied, must be ASCII, ≤11 chars, uppercase-folded
  (matching DOS conventions).
- OUT is a host file path; refuse if it exists unless `--force`.

### Layout per variant

Geometry pulled from documented Microsoft tables; standard values:

| Variant | Sector size | Reserved sectors | FATs | Root entries | Cluster size (typical) |
|---|---|---|---|---|---|
| fat12 | 512 | 1 | 2 | 224 (floppy) or 512 | 1 sector for tiny, 4 for ~32 MiB |
| fat16 | 512 | 1 | 2 | 512 | size-tiered (1, 2, 4, 8, 16, 32, 64) |
| fat32 | 512 | 32 | 2 | 0 (root in cluster chain) | size-tiered |

Steps (per variant, mostly shared):

1. Compute geometry — sectors/cluster from a size table per variant,
   then `sectors_per_fat` from the cluster count.
2. Write boot sector (BPB at offset 0, jump+OEM at top, boot sig
   `0x55AA` at 510). For fat32 also write FSInfo at sector 1 and
   backup boot at sector 6.
3. Write both FATs (`reserved_sectors..reserved_sectors+2*spf`).
   FAT[0] = media descriptor pattern, FAT[1] = EOC marker; fat32 also
   marks cluster 2 as EOC for the empty root.
4. Zero root directory area (fat12/16 only); optionally write a
   volume-label entry as the first 8.3 slot.
5. Data area: stays a file hole (use `ftruncate` to set the final
   length without writing zeros) to keep the output sparse.

### Implementation

- New `cmd_new(argc, argv)` in `rust_cli_real.c`, dispatched off
  `--fs` value (`fat12`/`fat16`/`fat32` first; reject others with
  a "not yet supported" message).
- Add a new top-level case in `main`'s verb dispatch.
- Share nothing with the FAT *reader* — formatting is its own pile
  of BPB byte-poking; ~500 LOC.

### Validation

- `rusty-backup show fs-info OUT.img` — must report the right FAT
  type, cluster size, sectors-per-FAT, free space.
- `rusty-backup fsck OUT.img` (existing FAT verifier) — must pass
  clean on a freshly created image.
- On macOS: `hdiutil attach -nomount OUT.img && diskutil info ...`
  must recognize the filesystem; mount and verify writable.
- Byte-compare BPB region against `newfs_msdos -F <variant>` output
  for the same size (acceptable for the layout fields; OEM string
  and timestamps differ).
- Extend `test_fs_readers.sh` with a "create + fsck + show fs-info"
  triplet per variant (~9 new PASS lines).

### Decisions left for implementation time

- OEM string: use `"rb-fat  "` (8 chars) for traceability.
- Whether to default-create as superfloppy (no partition table) or
  inside a minimal MBR with one partition. **Recommend superfloppy**
  — matches `hdiutil`'s default and is simpler; user can wrap with
  `partmap edit` later (Tier D).

### Cost / risk

- ~500 LOC. Risk concentrated in fat32's FSInfo + cluster-2 setup
  (host won't mount it if either is wrong — easy to spot, painful
  to debug). Geometry tables well-documented; the main trap is the
  "1.44 MiB-style" floppy presets vs. modern hard-disk presets.

---

## 4. NTFS read

The biggest browse target left. Full read-only `ls` / `get` for NTFS
partitions inside MBR or GPT disks.

### Scope (in / out)

**In:**

- NTFS 3.0/3.1 (Win2000+) — covers everything you'd reasonably image
  from XP, 7, 10 era machines.
- Resident + non-resident `$DATA` (plain files).
- Directory listing (small via `$INDEX_ROOT`, large via
  `$INDEX_ALLOCATION`).
- Path resolution from root MFT record (#5).
- UTF-16LE → ASCII best-effort name rendering, matching exFAT /
  Joliet conventions.

**Out:**

- Compressed `$DATA` (LZNT1) — skip; rare on system disks.
- Encrypted (EFS) — skip.
- Sparse runs — handle as zero-fill (trivial extension, include).
- Alternate Data Streams (named `$DATA`) — skip (browse only shows
  the unnamed one).
- Reparse points / junctions / mount points — list as plain entries.
- Hardlinks — pick the Win32 / POSIX-namespace `$FILE_NAME`, ignore
  the 8.3 one.

### On-disk shape (LE, byteswap on PPC)

- VBR at sector 0: magic `"NTFS    "` at offset 3. MBR type byte
  `0x07` shared with exFAT — distinguish via VBR magic (same shape
  as the exFAT-vs-FAT distinction we already do).
- Key VBR fields: `bytes_per_sector`, `sectors_per_cluster`,
  `mft_cluster` (LCN of `$MFT`), `mft_mirror_cluster`, **MFT record
  size**, **index record size**. The size fields are *signed*: a
  positive value is in clusters, a negative value is `2^|n|` bytes
  (typical: -10 = 1024 bytes).
- `$MFT` is itself MFT record #0 — its `$DATA` runlist tells you
  where every other record lives. Bootstrap challenge: you have to
  read record #0 directly via VBR fields before the rest of the MFT
  becomes addressable.
- **Update Sequence (USA) fixup** — critical and easy to forget. Every
  multi-sector record (MFT records and INDX blocks) has the last 2
  bytes of each 512-byte sector replaced with an update-sequence
  number. The *original* bytes live in a short USA array at the
  record header. Skip the fixup and you get silent corruption that
  looks like a parser bug.
- Attribute types inside an MFT record:
  - `0x10` `$STANDARD_INFORMATION` (timestamps, flags)
  - `0x30` `$FILE_NAME` (can repeat — namespace byte selects
    Win32 / DOS / POSIX; want the long-name one)
  - `0x80` `$DATA` (resident or non-resident)
  - `0x90` `$INDEX_ROOT` (B+ tree root; small dirs entirely here)
  - `0xA0` `$INDEX_ALLOCATION` (non-resident INDX blocks for big dirs)
  - `0xB0` `$BITMAP` (which INDX blocks are in use)
- **Runlist** decoding — varlength-encoded `(length, lcn_offset)`
  deltas. Negative deltas are 2's-complement; sparse runs have
  zero-length lcn field. Easy to get wrong.

### Slice plan

Five slices, each committable on its own; each unlocks something
testable.

| Slice | Output | What's testable |
|---|---|---|
| **1. VBR + `$MFT` bootstrap** | Can fetch any MFT record by index | A debug `show mft <N>` dump command can confirm USA fixup + record parsing |
| **2. Resident-only `ls /`** | Parse root record (#5), walk `$INDEX_ROOT` only | `ls /` on a small NTFS image where root fits in `$INDEX_ROOT` |
| **3. `$INDEX_ALLOCATION` walk** | INDX-block fetch (USA fixup applies again) + large-dir listing | `ls` on a real NTFS volume with a populated root |
| **4. `$DATA` extraction** | Resident + non-resident → `get` for plain files | Byte-exact `get` of a small file (resident) and a multi-cluster file (non-resident) |
| **5. Path resolution + wire-up** | Repeated dir lookups from root; full `ls` / `get` path | `ls /Windows/System32`, `get` nested files |

### Implementation in `rust_cli_real.c`

- New `NtfsVol` struct holding VBR-derived fields + MFT runlist (built
  during Slice 1 by parsing record #0's `$DATA`).
- `ntfs_read_record(v, mft_index, buf)` — central helper; handles USA
  fixup. Used by every higher-level routine.
- `ntfs_parse_attr(record, type, name)` iterator — yields attribute
  headers, body, runlist if non-resident.
- `ntfs_resolve_runlist(runlist_bytes, len)` → array of
  `(start_lcn, length_clusters)` with sparse runs flagged.
- `ntfs_walk_dir(v, mft_index, cb, user)` — combines `$INDEX_ROOT`
  inline entries + `$INDEX_ALLOCATION` INDX blocks.
- `fs_ls_ntfs` / `fs_get_ntfs` / detection in `resolve_image_ref` and
  superfloppy detection.

### Validation

- Real NTFS image (Windows install partition, or `mkntfs` on a
  loopback under Linux — macOS has no native NTFS formatter).
- Cross-check listings + extraction against the desktop NTFS reader
  in `src/fs/ntfs.rs`, the same way we did with EFS (real fixture
  cross-checked entry counts and byte-exact extractions).
- Bundle a small NTFS fixture under `ppc-tiger/fixtures/`. The
  fixture-build path is awkward (no native macOS NTFS formatter) —
  may need a one-off `ntfs-3g` brew install or a synthetic fixture
  built byte-by-byte from a Rust example.
- Extend `test_fs_readers.sh` once each slice lands.

### Cost / risk

- ~2500–3000 LOC across the 5 slices. Multi-week.
- Highest-risk read filesystem in the queue. Real failure modes:
  - USA fixup omitted → silent corruption
  - Wrong `$FILE_NAME` namespace selected → 8.3 names everywhere
  - Runlist sign-extension bug → wrong LCNs → reads garbage / wrong file
  - Off-by-one in MFT record indexing past the first cluster
- Mitigation: every slice gets cross-checked against the desktop
  reader before moving on. The slice plan keeps each commit small
  enough to debug independently.

---

## Cross-cutting notes

- **Test harness.** Each shipping slice extends
  `ppc-tiger/test_fs_readers.sh` with PASS-line assertions and adds
  any new fixtures to `ppc-tiger/fixtures/` (compressed). The big-
  endian acceptance test still runs end-to-end on real Tiger
  hardware via `./build.sh && sh test_fs_readers.sh`.
- **No new external deps.** All four items stay within the existing
  build (libc + zlib). Zstd / CHD vendoring is a separate Tier C
  conversation.
- **No new edit surface.** All four are read or write-once-create
  (the FAT formatter writes a fresh image; it never mutates an
  existing one). Edit verbs stay parked in Tier D.
