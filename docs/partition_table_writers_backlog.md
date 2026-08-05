# Partition-table writers: what's done, what's left

`rb-cli new hd <table>` and the GUI Restore tab's **Build Disk** mode both
provision a blank disk with a real partition table and partitions you size and
type yourself. The layout maths and every table writer live in
`src/partition/provision.rs`; `src/cli/verbs/new_partitioned_hd.rs` is only the
CLI grammar over it. This page tracked which tables could be *written*; that
list is now complete, so what remains is a record of how each writer is put
together and what was learned building it.

## Status

| Table | Write | Entry point |
|---|---|---|
| MBR | Done | `partition::mbr::build_minimal_mbr` |
| GPT | Done | `partition::gpt::build_minimal_gpt` + protective MBR + backup header |
| APM | Done | `partition::apm::build_minimal_apm` |
| SGI (IRIX) | Done | `partition::provision::write_sgi` |
| X68000 | Done | `partition::provision::write_x68k` |
| RDB (Amiga) | Done | `partition::provision::write_rdb` |
| Sun (SMI VTOC) | Done | `partition::provision::write_sun` |
| AHDI (Atari ST) | Done | `partition::provision::write_ahdi` |

Every table this project parses can now also be written. What is left is
narrower: creating an AHDI **XGM extended chain** (only the four primary slots
are written today), and editing an existing RDB or Sun label rather than
laying down a fresh one.

Filesystem creation is separate from table creation. A table writer produces
empty partitions; `rb-cli reformat --fs <fs>` fills them. See
[XFS creation](#xfs-creation--done), which has since shipped.

## How to add a writer

All eight existing writers follow the same shape, so a new one slots in without
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

## Sun (SMI VTOC) — done

Shipped as `partition::provision::write_sun`, reached from `rb-cli new hd sun`,
the TUI's New wizard and the GUI's Build Disk picker. It writes the single
512-byte label at sector 0: the free-form `info` text, the VTOC tag table, the
geometry words, and the 8 `{start_cylinder, num_sectors}` slices. Field offsets
follow the kernel's `struct sun_disklabel` (`block/partitions/sun.c`), which is
also what `src/partition/sun.rs` reads.

Notes for anyone extending it:

- **Slice 2** is the whole-disk "backup" alias, written unconditionally with
  tag 5 spanning `ncyl * spc`. User slices fill the other seven, hence
  `slot_limit(Sun) == 7`.
- The checksum is a 16-bit XOR over all 256 big-endian words that has to come
  out **zero**, so the stored `csum` is the XOR of the other 255. `fdisk`
  refuses the label outright if this is wrong, which makes it easy to catch.
- Slices are cylinder-granular, so Sun shares RDB's `size_granularity` hook —
  a size that is not a whole number of cylinders cannot be expressed.
- Slice tags accept either a name (`root`, `usr`, `swap`, ...) or a bare number,
  resolved by `sun::tag_from_text`, which `type_catalog::normalize` also uses so
  the two spellings compare equal.
- We start slice 0 at **cylinder 1**, not cylinder 0. Real Sun disks put the
  root slice at cylinder 0 and rely on UFS leaving the label sector alone, but
  this tool pours arbitrary images into partitions, so overlapping the label
  would be a footgun. `sfdisk --verify` reports the first cylinder as an unused
  gap and still exits 0.

**Validation.** Round-trips through our own parser in
`every_writable_table_writes_and_reparses` and
`sun_label_checksums_and_reserves_the_backup_slice`, and cross-checked against
util-linux `fdisk -l` / `sfdisk --dump` / `sfdisk --verify` on a Linux box,
which report our geometry, every slice's start and length, and each tag.
Booting a real Solaris install is still unproven — the label is right by every
tool that reads it, but nobody has put an actual SunOS root on one yet.

---

## RDB (Amiga Rigid Disk Block) — done

Shipped as `partition::provision::write_rdb`, reached from `rb-cli new hd rdb`,
the TUI's New wizard and the GUI's Build Disk picker. It writes an `RDSK` at
sector 0 and one `PART` block per partition at sectors 1..N, chained through
`pb_Next`, with field placement taken from `amitools`' `RDBlock.py` /
`PartitionBlock.py` — the reference AmigaOS-compatible writer.

Notes for anyone extending it:

- Partitions are laid out in **cylinders**, so RDB is the first table whose
  partition *sizes* also have to land on the alignment, not just their starts.
  `place()` grew a `size_granularity` hook for that; every other table still
  works in sectors.
- The RDB owns cylinder 0 (`rdb_RDBBlocksLo` = 0, `rdb_RDBBlocksHi` =
  `cyl_blks - 1`, `lo_cyl` = 1), which is what `rdb_cyls=1` gives you in
  `rdbtool`. `slot_limit` caps partitions at 15 so every `PART` block stays
  inside the first 16 sectors that Amiga tools scan.
- The zero-sum block checksum is `rdb::stamp_checksum`, lifted out of
  `set_partition_bootable` so both paths share one implementation.
- Entries default to the AmigaDOS device names `DH0`, `DH1`, ... rather than the
  generic `Partition N`, via `provision::default_name`.
- The editor is still `SetBootable`-only on an *existing* RDB; that is
  unchanged.

**Validation.** Round-trips through our own parser in
`every_writable_table_writes_and_reparses`, and cross-checked against
`amitools`' `rdbtool` (`python3 bin/rdbtool IMG info` / `free` from a checkout
of <https://github.com/cnvogelg/amitools>), which reads back the geometry, both
DosTypes, the drive names and the exact free cylinder range.

---

## AHDI (Atari ST) — done

Shipped as `partition::provision::write_ahdi`, reached from
`rb-cli new hd atari`, the TUI's New wizard and the GUI's Build Disk picker.
It turned out to be the thinnest of the four: `AhdiTable::root_to_bytes` was
already a complete root-sector serialiser (including the checksum), so the
writer only fills the four primary slots and hands it the table.

Notes for anyone extending it:

- **Only the four primary slots are written.** XGM extended chains parse
  (`AhdiTable::detect_and_walk` walks them, MBR-EBR style) but are not created,
  so `slot_limit(Atari)` is 4. Creating an XGM chain is the one piece of this
  page still open.
- A `GEM` partition over 16 MiB is promoted to `BGM`, because GEM describes its
  size with a 16-bit sector count. The promotion happens in `place()` via
  `effective_type`, not in the writer, so the CLI log, the GUI picker and the
  on-disk bytes all name the same type.
- AHDI has **no magic number**. Detection keys off the 0x1234 word-sum plus at
  least one plausibly-shaped entry, and it only runs on sectors that lack the
  0xAA55 MBR signature — which a table we write never has.
- The root sector's leading 454-byte bootstrap area is left zeroed, so a disk
  we create is not TOS-bootable. Grafting in a real bootstrap is future work.

**Validation.** Round-trips through our own parser in
`every_writable_table_writes_and_reparses` and
`ahdi_stamps_its_checksum_and_promotes_oversized_gem_to_bgm`; independently
re-parsed with a throwaway Python reader modelled on
`scripts/generate-ahdi-fixture.sh` (the script that produced the committed
real-tool fixture), which agrees on the checksum, both tags and both extents;
and exercised end to end by pouring a FAT12 volume into partition 1 with
`--fill`, then `put`-ing and `ls`-ing a file through it. Booting a real TOS
machine is unproven — there is no bootstrap in the root sector to boot from.

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
