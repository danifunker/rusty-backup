# Partition-table writers: what's done, what's left

`rb-cli new hd <table>` and the GUI Restore tab's **Build Disk** mode both
provision a blank disk with a real partition table and partitions you size and
type yourself. The layout maths and every table writer live in
`src/partition/provision.rs`; `src/cli/verbs/new_partitioned_hd.rs` is only the
CLI grammar over it. This page tracks which tables can be *written* and what
each remaining one needs. Every format here already **parses** correctly — the
gap is only the write side.

## Status

| Table | Write | Entry point |
|---|---|---|
| MBR | Done | `partition::mbr::build_minimal_mbr` |
| GPT | Done | `partition::gpt::build_minimal_gpt` + protective MBR + backup header |
| APM | Done | `partition::apm::build_minimal_apm` |
| SGI (IRIX) | Done | `partition::provision::write_sgi` |
| X68000 | Done | `partition::provision::write_x68k` |
| **Sun (SMI VTOC)** | **Missing** | see below |
| **RDB (Amiga)** | **Missing** | see below |
| **AHDI (Atari ST)** | **Missing** | see below |

Filesystem creation is separate from table creation. A table writer produces
empty partitions; `rb-cli reformat --fs <fs>` fills them. See
[XFS creation](#xfs-creation--done), which has since shipped.

## How to add a writer

All five existing writers follow the same shape, so a new one slots in without
touching the layout engine:

1. Add the variant to `PartitionedHdCommand` and to `HdCommand` in
   `cli/verbs/new.rs`.
2. Add the table to `partition::type_catalog::TableKind` — the compiler will
   point at every match arm that needs updating, which is the safety net.
3. In `partition::provision`, give it `default_type()`, `slot_limit()` and
   `reserved_head()` / `reserved_tail()` entries so the shared `place()` keeps
   partitions clear of the table's own regions.
4. Write `fn write_<table>(out: &mut impl Write + Seek, placed, disk_size)` and
   dispatch to it from `provision::write_table`.
5. Add the kind to `provision::WRITABLE_TABLES` — that is the single list the
   GUI's Build Disk picker and the `every_writable_table_writes_and_reparses`
   round-trip test both read, so a new writer gets a picker entry and a
   write-then-reparse test for free.
6. Populate `<TABLE>_TYPES` in `type_catalog` so `partmap types` and the GUI
   type dropdown work.

`place()` already handles alignment, ordering, the single `rest` partition and
overrun refusal; a writer only serialises.

---

## Sun (SMI VTOC)

**Parser:** `src/partition/sun.rs`. 8 slices, big-endian, magic `0xDABE` at byte
508 of sector 0.

**What a writer needs**

- The 512-byte label: ASCII `info` text, `rpm`, `pcyl`, `apc`, `nhead`,
  `nsect`, `ncyl`, then the 8 × 8-byte slice array (tag, flags, start cylinder,
  sector count), magic and checksum.
- **The checksum is a plain XOR of every 16-bit big-endian word in the label,
  and it must come out zero.** Get this wrong and Solaris refuses the disk.
- Slices are expressed in **cylinders, not sectors**, so the geometry
  (`nhead` × `nsect`) is load-bearing: every partition start and length must be
  a whole number of cylinders. `place()` handles this if the caller passes
  `align = nhead * nsect * 512`, exactly as the SGI path does.
- Slice 2 is conventionally the whole disk ("backup"), the same role SGI's slot
  10 plays. Slice tags: 0 unassigned, 1 boot, 2 root, 3 swap, 4 usr, 5 backup,
  7 var, 8 home.

**Validation.** Do not ship this without an oracle. A wrong VTOC yields a disk
that looks plausible and won't boot. Build a label, then check it round-trips
through our own parser *and* through `prtvtoc`/`fdisk` in a Solaris or
OpenIndiana VM, the way the EFS work was validated against IRIX.

---

## RDB (Amiga Rigid Disk Block)

**Parser:** `src/partition/rdb.rs` — RDSK block plus a linked list of PART
blocks. Note the editor currently supports only `SetBootable` on RDB
(`partition::editor::apply_rdb_edits`); everything else bails.

**What a writer needs**

- An `RDSK` block in the first 16 sectors: block size, cylinders, heads,
  sectors, `rdb_PartitionList` pointing at the first PART block, and the usual
  "highest/lowest cylinder" fields.
- A chain of `PART` blocks, each holding a BSTR drive name (`DH0`), the
  `de_` DosEnvec geometry sub-struct (block size in longs, surfaces, blocks per
  track, low/high cylinder, reserved, `de_DosType`), and `pb_Next`.
- **Every block carries a checksum: the 32-bit sum of all longs in the block
  must be zero.** `rdb.rs` already computes this for `set_partition_bootable` —
  reuse that helper rather than writing a second one.
- Everything is **big-endian**, and partition bounds are in **cylinders**.
- `de_DosType` is the filesystem tag (`DOS\3`, `PFS\3`, `SFS\0`); the values are
  already in `type_catalog`'s `RDB_TYPES`.

**Validation.** Round-trip through our parser, then mount in an emulator
(FS-UAE / vAmiga) with the matching filesystem handler. A wrong DosEnvec
produces a partition AmigaOS sees but cannot mount.

---

## AHDI (Atari ST)

**Parser:** `src/partition/atari.rs`. `AtariPartitionType::to_bytes` exists but
returns only the 3-byte type tag (`GEM`/`BGM`/`XGM`/`RAW`) — there is no table
serialiser.

**What a writer needs**

- The root sector: 4 primary entries at byte 0x1C6, each 12 bytes (flags byte,
  3-byte type tag, 4-byte start sector, 4-byte length), plus the total-sector
  count at 0x1C2. Big-endian, and there is **no boot signature** — detection is
  by checksum and plausibility.
- `GEM` for partitions up to 16 MB, `BGM` above it; `XGM` marks an extended
  chain whose logical entries are relative to the XGM sector, MBR-EBR style.
- The root sector checksum word makes the 16-bit sum of the sector equal
  `0x1234` when the disk is meant to be bootable; non-bootable disks leave it
  alone.

**Validation.** Hatari with a TOS ROM, or `atari-hd-image` output to diff
against.

---

## XFS creation — done

Shipped as `src/fs/xfs/format.rs`, reached from `rb-cli new volume xfs` and the
`batch` builder's `"xfs"` filesystem. It emits a **v5/CRC** filesystem with the
conservative feature set our reader already speaks (`ftype` on; `finobt`,
`rmapbt`, `reflink`, `bigtime`, `sparse` and `nrext64` off), at fixed 4 KiB
blocks / 512-byte sectors / 512-byte inodes.

The layout mirrors `mkfs.xfs`: per AG, block 0 holds the superblock, AGF, AGI
and AGFL sectors, blocks 1-3 the bnobt / cntbt / inobt roots, blocks 4-7 the
four blocks parked on the AGFL, then the log (in the middle AG) and the root
inode chunk (AG 0), and one free extent for the rest. Only populated regions are
written, so the log body and the free space stay sparse.

Two things worth knowing if you touch it:

- The minimum is **32 MiB** — two allocation groups of `XFS_AG_MIN_BLOCKS`. That
  is far below the 300 MB floor `mkfs.xfs` 6.x imposes on itself, which is a
  mkfs policy rather than an on-disk constraint. A single-AG filesystem is
  refused because `xfs_repair` will not validate its geometry without
  `-o force_geometry`.
- Building this surfaced a real bug in `v5_crc::stamp_sblock_hdr_for_ag`: it
  derived a btree block's `bb_blkno` from the `agno << sb_agblklog` fsblock
  encoding, which only equals the physical address when `sb_agblocks` is a power
  of two. Every non-power-of-two-AG filesystem (a 300 MiB one, say) got corrupt
  headers in AGs 1 and up. It now uses `agno * sb_agblocks`, per
  `XFS_AGB_TO_DADDR`. The repair path shared the same helper.

**Validation.** `scripts/xfs-oracle.sh sweep` formats at a range of sizes and
runs the real `xfs_repair -n` on each over SSH; clean from 32 MiB to 16 GiB
against xfsprogs 6.6.0.
