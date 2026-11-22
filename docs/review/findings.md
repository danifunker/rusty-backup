# Codebase review: findings ledger

Every finding from the whole-codebase review, checked against
[rubric.md](rubric.md). The review records; fixes land later, one cluster per
change, with `scripts/regress-changed.py` run on each.

**Status values:** `open`, `fixing`, `fixed (<commit>)`, `wontfix (<why>)`.
**Verdict:** `confirmed` (reproduced, or the quoted code shows it) or
`plausible` (says what would confirm it).

IDs are `<slice>-<nnn>`. A finding seen while reviewing one slice but belonging
to another carries the other slice's prefix and is listed under that slice.

## Progress

| Slice | Lines | Reviewed | Open findings |
|---|---:|---|---:|
| crates/disk-core | 4,943 | 2026-11-21, fixed in 0.3.0 | 1 (DC-006, per driver) |
| crates/part-tables | 10,220 | 2026-11-21, fixed in 0.2.0 | 0 |
| crates/fs-pc | 27,123 | 2026-11-21, fixed in 0.1.7 | 0 |
| iris-sgi-crates work (fs-xfs 0.2.0, sgi-disk, rb-fsimporter, provisioning, shells) | ~6,000 changed | 2026-11-21, separate review agent | 15 |
| iris-sgi-crates follow-up (fork-export, fs-mac-unicode 0.2.0, fs-amiga 0.2.0) | ~2,700 changed | 2026-11-21, separate review agent | 9 |
| crates/fs-mac | 42,245 | 2026-11-21 (partial, then the remainder by a separate agent) | 20 |
| crates/fs-xfs | 18,569 | 2026-11-21, separate agent, fixed in 0.2.0 | 2 deferred |
| crates/fs-amiga | 16,461 | 2026-11-21, separate agent | 16 |
| crates/fs-unix | 51,669 | 2026-11-21, separate agent | 61 |
| crates/fs-8bit | 16,369 | 2026-11-21, separate agent | 16 |
| crates/dsk-ghost | 10,087 | 2026-11-21, separate agent | 17 |
| crates/fs-squashfs | 6,425 | (seen in passing) | 1 |
| src/fs | 14,945 | (seen in passing) | 1 |
| src/partition, src/rbformats, src/restore | 36,000 | (seen in passing) | 2 |
| src/model/convert | — | (seen in passing) | 1 |

## Mechanical checks

Run per slice before reading (results go in the slice's section):

```bash
./scripts/similar-fns/target/release/similar-fns --repo . --unit crates/<name>
cargo clippy --offline -p <name> --all-targets -- -D warnings
grep -rnE 'leading_(zeros|ones)|eprintln!|\.unwrap\(\)|\.expect\(|panic!|unreachable!' crates/<name>/src
grep -rnE 'vec!\[0u?8?; *[a-z_]|with_capacity\([a-z_]' crates/<name>/src   # sizes taken from disk?
```

---

## crates/disk-core

Mechanical: clippy clean, 82 tests pass, no `unwrap` / `panic!` / `eprintln!`
outside tests, no narrow `leading_zeros`. Read in full.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| DC-001 | S2 | confirmed | [attrs.rs:326](../../crates/disk-core/src/attrs.rs#L326) | `PreservedMeta` carries no `finder_flags`, so replacing an HFS / HFS+ / MFS file (`rb-cli put` over an existing file, `src/fs/replace.rs`) drops `hasBundle`, custom-icon and invisible bits, the loss `docs/bug_binhex_finder_flags.md` describes. `CreateFileOptions.finder_flags` exists; `preserved_meta` and `apply_to_options` just don't carry it. `put.rs:430` sets it only for a MacBinary / AppleDouble import. | fixed (disk-core 0.3.0) |
| DC-002 | S2 | confirmed | [xattr.rs:31](../../crates/disk-core/src/xattr.rs#L31) | `value_display` / `parse_value` don't round-trip a text value that starts with `0x`: `b"0x41"` displays as `0x41` and parses back as `[0x41]` (`A`). An editor that shows a value and saves it unchanged rewrites the attribute. Text that looks like hex needs an escape, or text values need a marker. | fixed (disk-core 0.3.0) |
| DC-003 | S2 | confirmed | [times.rs:446](../../crates/disk-core/src/times.rs#L446) | Clamped encoders keep the real month and day, which makes invalid dates: ProDOS clamps to 2039 (2040-02-29 is written as 2039-02-29, read back as no date); UCSD clamps to 1999 (any 29 February from 2028 on); LIF clamps to 2069. `unix_to_dos_datetime` already fixed this exact bug (comment at line 239). Pin the whole stamp, as DOS does. | fixed (disk-core 0.3.0) |
| DC-004 | S2 | confirmed | [times.rs:678](../../crates/disk-core/src/times.rs#L678) | `unix_to_qdos_date` wraps with `as u32` instead of clamping: 2100-01-01 encodes as 91474304, which decodes to nothing (or, near the boundary, to a wrong 1960s date). The module doc promises clamping. | fixed (disk-core 0.3.0) |
| DC-005 | S2 | plausible | [filesystem.rs:444](../../crates/disk-core/src/filesystem.rs#L444) | The default `delete_recursive` recurses with no depth limit or visited set. A corrupt image whose directory lists an ancestor (a FAT subdirectory cluster pointing back up) recurses until the stack overflows, and deletes along the way. Confirm with a FAT image whose subdir's first cluster is its parent's. | fixed (disk-core 0.3.0) |
| DC-006 | S3 | confirmed | [filesystem.rs:89](../../crates/disk-core/src/filesystem.rs#L89) | The default `write_file_to` reads the whole file into one `Vec` (`read_file(entry, usize::MAX)`). 20 drivers rely on it. Most are floppy formats under the 3 MB rule, but CP/M (hard disks to 512 MB), HP LIF, QNX 2, Xerox Pilot and the SquashFS editor can hold files far larger, and `stream_pipe` / extract then buffer them whole. List: `grep` for `Filesystem for` impls without `fn write_file_to`. | open: fixed per driver as each crate is reviewed |
| DC-007 | S3 | confirmed | [times.rs:378](../../crates/disk-core/src/times.rs#L378) | Local-time formats (FAT, HFS, ProDOS, exFAT without an offset) convert with today's UTC offset, not the offset in force on that date, so a summer file read in winter (or written then read across a DST change) is an hour out. Common practice (mtools does the same), but undocumented here. Either use chrono's offset for the date in question or say so in the module doc. | fixed (disk-core 0.3.0) |
| DC-008 | S4 | confirmed | [entry.rs:133](../../crates/disk-core/src/entry.rs#L133) | Six full field lists of `FileEntry`'s 27 fields: `root`, `new_directory`, `new_file`, `new_symlink`, `new_special` and `unix_common::unix_entry_from_inode`. Adding a field means editing six places (the `qnx2_attrs` addition did). One private `blank(entry_type)` plus struct-update syntax removes ~170 lines. | fixed (disk-core 0.3.0) |
| DC-009 | S4 | confirmed | [entry.rs:430](../../crates/disk-core/src/entry.rs#L430) | `entry::select_child` and `filesystem::find_child` are the same function with the arguments swapped (8 and 44 callers). Two more copies: `crates/fs-tar/src/import_sink.rs:570` and `src/fs/copy.rs:856`. Keep `find_child`. | fixed (disk-core 0.3.0) |
| DC-010 | S4 | confirmed | [inode.rs:165](../../crates/disk-core/src/unix_common/inode.rs#L165) | `format_unix_timestamp` re-implements the Hinnant civil-date algorithm that `times::ymd_hms` already has, in the same crate. | fixed (disk-core 0.3.0) |
| DC-011 | S4 | confirmed | (crate-wide) | disk-core has no little / big-endian byte readers, so every crate grows its own: `le32` 9 times, `be32` 6, `be16` 5, `read_u32` 5, `le16` 4 (see similar-functions.md, same-name section). A `disk_core::bytes` module (`le16/le32/le64/be16/be32/be64` over `&[u8]` at an offset, plus checked variants returning `Option`) gives the crates one tested version, and a place to add the bounds checks the rubric asks for. | fixed (disk-core 0.3.0) |
| DC-012 | S4 | confirmed | various | Small accuracy and maintenance slips: `bits.rs:43` points at `src/fs/hfsplus.rs` (now `crates/fs-mac`); `times.rs:582` labels the LIF encoder as the OS-9 section; `id_names.rs:115` says a symlinked `/etc/passwd` is followed and then rejects it; the `id_names.rs:163` test claims "unknown user, known group" and asserts the both-known case again; `#[allow(dead_code)]` on `pub` functions in `bits.rs` does nothing; `stream_pipe.rs:110` reports the consumer's wrapped string error instead of the producer's typed `FilesystemError` when the producer caused it. | fixed (disk-core 0.3.0) |

**Fixed in disk-core 0.3.0** (minor bump: `PreservedMeta` gained a public field and
`entry::select_child` is gone; every dependent's requirement moved to `0.3` with
`scripts/crate-versions.py`, the four lockfiles refreshed). Notes on the fixes:
DC-003 keeps each date's month and day and moves only an impossible 29 February
to the 28th, because pinning the whole stamp would date every UCSD file after
1999 as 1999-12-31. DC-005 refuses trees deeper than 1024 folders and skips
`.` / `..`. DC-007 adds `utc_offset_at` and converts each date with its own
offset; tests pass under New York, Berlin, Sydney and UTC. DC-009 adds
`filesystem::lookup_child`, which the fs-tar and `src/fs/copy.rs` copies now
call. DC-011 adds `disk_core::bytes`; moving each crate onto it is part of that
crate's review. DC-012 also stops `stream_pipe` hiding the source's typed error,
while a consumer that fails first still reports its own error rather than the
broken pipe it caused.

**Regression (batch level, 2026-11-21, `84935173`):** preflight green (release tests,
MiSTer feature set, Rust 1.73 vintage build, doc parity, harness tests), layer check
green. 821 cases: 500 pass, 167 fail, 154 skip. None of the failures comes from this
change: 164 are AppKit / Cocoa GUI flows, which cannot drive a desktop session on an
unattended run (the shell is killed waiting for its window); 3 are CV-001 below,
failing on main since the Convert redesign. One oracle verdict failed because
`amitools_affs.py` crashed when `xdftool` is missing instead of reporting itself
unavailable; fixed in the harness.

Checked and fine: `stream_pipe` (no deadlock on early stop or panic; producer
errors never become a silent short read), `raw_name` round trip, `name_legalize`
(host names can't escape the folder: `/` and `..` are refused and repaired),
`bits` arithmetic, the DOS / exFAT / NTFS / Mac / ADFS / OS-9 converters.

## crates/part-tables

Read line by line: `mbr`, `gpt`, `apm`, the RDB restore plan, the X68k patch,
`lvm/{aix,hpux,linux}`, `size_override`. Scanned with the mechanical greps (narrowing
casts, unchecked indexing, disk-sized allocations): `atari`, `next`, `sgi`,
`sgi_dklabel`, `sun`, `solaris_x86`, `bsd`, `lif`; nothing found there that needed a
fix. Fixed in **part-tables 0.2.0** (breaking: `patch_mbr_entries`, `build_ebr_chain`,
`Apm::patch_for_restore` and `patch_x68k_entries` return `Result`; new
`PartitionError::InvalidX68k`).

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| PT-001 | S1 | confirmed | [mbr.rs:319](../../crates/part-tables/src/mbr.rs#L319) | `patch_mbr_entries` wrote `(export_size / 512) as u32`: a "fill remaining" restore of an MBR backup onto a card past 2 TiB wrote a wrapped sector count, so the table described a tiny partition under a large filesystem. Now refused with an error, all-or-nothing, and every restore / VHD / CHD caller propagates it. | fixed (part-tables 0.2.0) |
| PT-002 | S1 | confirmed | [gpt.rs](../../crates/part-tables/src/gpt.rs) | `build_primary_gpt` / `build_backup_gpt` cut the entry array to 16 KiB but computed its CRC over the whole array and kept the header's entry count, so a GPT with more than 128 entries of 128 bytes was rebuilt with a CRC mismatch. The array region is now sized from the array (`array_sectors`), first / last usable LBAs follow it, and callers place the backup by its length instead of `target - 33`. | fixed (part-tables 0.2.0) |
| PT-003 | S2 | confirmed | [gpt.rs](../../crates/part-tables/src/gpt.rs) | `partition_entry_size` from disk sized a `Vec` directly (up to 4 GiB). Now 128..=4096 and a power of two, as the spec allows. | fixed (part-tables 0.2.0) |
| PT-004 | S2 | confirmed | [gpt.rs](../../crates/part-tables/src/gpt.rs) | Neither the header CRC nor the entry-array CRC was checked, and the backup GPT was never consulted. A primary that fails either CRC now gives way to a backup that passes both; with no good backup the primary is still read, as before. | fixed (part-tables 0.2.0) |
| PT-005 | S2 | confirmed | [mbr.rs](../../crates/part-tables/src/mbr.rs) | `parse_ebr_chain` added untrusted u32 LBAs unchecked: a crafted EBR panicked in debug builds and wrapped in release. Now ends the chain. | fixed (part-tables 0.2.0) |
| PT-006 | S2 | confirmed | [apm.rs](../../crates/part-tables/src/apm.rs) | A DDR with block size 0 made restore divide by zero (`src/rbformats/mod.rs`, `src/restore/mod.rs`) and showed every partition at 0. The parser reads it as 512, and the restore path that loads `apm.json` from older backups does the same. | fixed (part-tables 0.2.0) |
| PT-007 | S2 | confirmed | [lvm/aix.rs](../../crates/part-tables/src/lvm/aix.rs) | The AIX VGDA length from disk sized a buffer with no cap (up to 2 TiB). Capped at 65,536 sectors; HP-UX and Linux LVM already capped theirs. | fixed (part-tables 0.2.0) |
| PT-008 | S2 | confirmed | [gpt.rs](../../crates/part-tables/src/gpt.rs) | `GptPartitionEntry::size_bytes` underflowed on last < first; `patch_for_restore` underflowed for a size under one sector; `build_*_gpt` underflowed for tiny disks. | fixed (part-tables 0.2.0) |
| PT-009 | S3 | confirmed | [apm.rs](../../crates/part-tables/src/apm.rs), [rdb.rs](../../crates/part-tables/src/rdb.rs) | Restore patches narrowed with `as u32` (APM block count and start, RDB low cylinder, high cylinder, disk cylinders). Now checked, with errors. | fixed (part-tables 0.2.0) |
| PT-010 | S2 | plausible | [x68k.rs:334](../../crates/part-tables/src/x68k.rs#L334) | Moving an X68k partition overwrote the start word's high byte, which holds the active / flags bits, with zero, and a start past 24 bits wrapped silently. The flags byte is now kept and a too-large start is refused. The app's own `restore_patch::patch_x68k` already kept it (see PR-001). | fixed (part-tables 0.2.0) |
| PT-011 | S1 | confirmed | [mbr.rs](../../crates/part-tables/src/mbr.rs) | `build_ebr_chain` subtracted LBAs unchecked; an impossible layout wrote EBRs at wrapped positions. Now an error; `restore::build_restore_ebr_chain` returns `Result<Option<_>>` so a restore stops instead of dropping the logical partitions. | fixed (part-tables 0.2.0) |
| PT-012 | S4 | confirmed | [mbr.rs](../../crates/part-tables/src/mbr.rs) | Dead code in `Mbr::parse` (`let _ = &mut entries`). Removed. The crate's private `Cursor` / `be16` / `le32` helpers move onto `disk_core::bytes` when each module is next touched (DC-011). | fixed (part-tables 0.2.0) |

## crates/fs-pc

Read: every open / boot-sector parse (FAT, exFAT, NTFS, Human68k), the chain and
run followers that reads and writes go through, NTFS run decoding and fixups, and
the bitmap and FAT buffers sized from on-disk fields. Not yet read line by line:
the FAT resize / repack rebuild (fat.rs 2400-2800), the four fsck modules' repair
writes, `ntfs_format`, the clone modules. Fixed in **fs-pc 0.1.7** (no API change).

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| FP-001 | S2 | confirmed | [fat.rs:132](../../crates/fs-pc/src/fat.rs#L132) | `open` never checked the BPB layout, and `scan_used_clusters` (run at every open) allocated `sectors_per_fat * bytes_per_sector`: a damaged BPB asked for up to TiBs and the allocation failure aborted the app. Now `open` refuses a BPB with no FATs or regions that overrun the volume, and every read that sizes a buffer from the FAT first checks that the FAT is in the image (when the reader knows its length; a raw device that reports 0 is not refused). A head buffer holding only the boot sector still opens, as detection needs; its best-effort used-cluster scan is skipped. | fixed (fs-pc 0.1.7) |
| FP-002 | S1 | confirmed | [fat.rs:427](../../crates/fs-pc/src/fat.rs#L427) | `next_cluster` accepted any link below the format's reserved range, not just up to the last data cluster, and `write_cluster_chain` wrote wherever the link pointed: a corrupt chain sent writes past the FAT volume into whatever follows it. Links past `total_clusters + 1` now end the chain, for every reader and writer. | fixed (fs-pc 0.1.7) |
| FP-003 | S4 | confirmed | [fat.rs](../../crates/fs-pc/src/fat.rs) | `total_clusters as u32 + 2` could wrap in the used-cluster scan. Saturates now. | fixed (fs-pc 0.1.7) |
| FP-004 | S1 | confirmed | [exfat.rs:272](../../crates/fs-pc/src/exfat.rs#L272) | exFAT `next_cluster` range-checked the cluster it was given but returned any `next` up to the end-of-chain marker, which the caller then read or wrote. Now `2 <= next < cluster_count + 2`. | fixed (fs-pc 0.1.7) |
| FP-005 | S2 | confirmed | [exfat.rs:38](../../crates/fs-pc/src/exfat.rs#L38) | The sectors-per-cluster shift was not checked: `1 << shift` panicked in debug for 64+ and gave a wrong cluster size in release. Refused when the two shifts sum past 25 (32 MiB clusters), as the spec caps them. | fixed (fs-pc 0.1.7) |
| FP-006 | S2 | confirmed | [exfat.rs:413](../../crates/fs-pc/src/exfat.rs#L413) | The allocation-bitmap length from the root directory sized a buffer unchecked. Refused past one bit per cluster plus a cluster of slack. | fixed (fs-pc 0.1.7) |
| FP-007 | S2 | confirmed | [ntfs.rs:113](../../crates/fs-pc/src/ntfs.rs#L113) | The index-record size did `1 << -raw` on an i8: `0x80` overflowed the negation (panic) and large shifts panicked or masked. Now decoded by `mft_record_bytes`, with the existing 4096 fallback. | fixed (fs-pc 0.1.7) |
| FP-008 | S1 | confirmed | [ntfs.rs:2391](../../crates/fs-pc/src/ntfs.rs#L2391) | `write_data_to_runs` wrote at each on-disk run's cluster with no check that the run ends inside the volume, so overwriting a file (or $Bitmap) on a corrupt volume could write past the partition; the $Bitmap resize indexed its buffer with those runs (panic). `check_runs` now refuses a negative or past-the-end run before any write or bitmap update. | fixed (fs-pc 0.1.7) |
| FP-009 | S3 | confirmed | [ntfs.rs:177](../../crates/fs-pc/src/ntfs.rs#L177) | `decode_data_runs` added relative offsets unchecked (panic in debug on a crafted run list). Stops the list now. | fixed (fs-pc 0.1.7) |
| FP-010 | S2 | confirmed | [human68k.rs:372](../../crates/fs-pc/src/human68k.rs#L372) | Human68k `open` read the whole FAT into a buffer sized from two u16 BPB fields (up to 4 GiB) with no check. Refused when the FAT overruns the volume or the image. | fixed (fs-pc 0.1.7) |
| FP-011 | S2 | confirmed | [human68k.rs:394](../../crates/fs-pc/src/human68k.rs#L394) | The chain walker capped at `total_sectors / spc + 16`, so a chain could name 16 clusters past the volume. Now capped at the real last cluster (`cluster_limit`). | fixed (fs-pc 0.1.7) |

**Regression (batch level, 2026-11-21, `d2258bde`, covering part-tables 0.2.0 and fs-pc 0.1.7):**
preflight green; 821 cases: 572 pass, 12 fail, 237 skip; oracle verify clean. The 12
failures are 9 vintage Cocoa GUI flows (no desktop session) and the 3 CV-001 convert
cases; the produce step's 6 missing convert artifacts (`fmt.vhd-*`, `fmt.qcow2`,
`fmt.vmdk-sparse`, `fmt.chd`, `optical.chdcd`) are CV-001 as well and failed the same
way before these changes. An earlier attempt was void: the disk filled (see the
unattended-run notes) and the next one caught an over-strict FAT check, fixed in
`d2258bde`.

## iris-sgi-crates review (2026-11-21)

Review of `07d1da77..6d621e8c` by a separate agent against this rubric (read in full: sgi-disk,
dir2_tree non-test code, the rb-fsimporter diff and shim, the provisioning / controller / CLI /
shell diffs). 12 findings confirmed by a temporary test, a CLI run or `cargo tree`, 3 by quoted code.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| SD-001 | S1 | confirmed | crates/sgi-disk/src/open.rs | The opener passes only the partition start to EFS / XFS and never compares the filesystem's own size with the label's partition length; a filesystem larger than its partition (corrupt label, label-only relabel over an old volume) is edited past the partition end (7.8 MB past it in the test). | fixed |
| XF-001 | S2 | confirmed | crates/fs-xfs/src/xfs/edit/dir2_tree.rs (`leaf1_bests`, `leaf1_write`) | `bestcount` from disk is unchecked: `len - 4 - 2*n` underflows, inserting into a leaf-form dir with a corrupt bestcount panics. | fixed |
| XF-002 | S2 | confirmed | dir2_tree.rs (`data_records`) | Guard `pos + 8 > len` then reads `blk[pos + 8]`: a record in the last 8 bytes indexes out of bounds. | fixed |
| XF-003 | S2 | confirmed | dir2_tree.rs (`set_free_best`) | freeindex `nvalid` unclamped; the trim loop reads past the block. | fixed |
| XF-004 | S2 | confirmed | dir2_tree.rs (`collapse_root`) | No depth cap or self-reference check: a root whose single entry points at itself loops forever. | fixed |
| BD-001 | S2 | confirmed | src/cli/verbs/new_sgi_hdd.rs (`--size auto`) | Auto size covers EFS content only; `--swap` is carved out of it and `--fs xfs` overhead is not counted, so the import fails DiskFull with a half-built image. | fixed |
| SD-002 | S2 | confirmed | crates/sgi-disk/src/build.rs, src/model/provision_runner.rs | The disk size always rounds up to whole cylinders, so on a fixed-size target (a device, an emulator's disk) the label describes up to ~504 KiB past the end; Build Disk's device path then fails `check_fits` after building. Predates this work for EFS; the label-only stage inherits it. | fixed |
| FI-001 | S3 | confirmed | Cargo.toml + rb-cli-{vintage,ppc,sol9} | The app takes rb-fsimporter with default features, whose `native-zstd` turns on fs-tar's C libzstd in the "zero C" pure-zstd builds (MiSTer armv7, headless, musl / ppc). | fixed |
| SD-003 | S3 | confirmed | crates/sgi-disk/src/build.rs | Without feature `xfs`, an XFS root is refused only after the volume header has been written. | fixed |
| BD-003 | S4 | confirmed | five Build Disk views | The "Format" choice labels are copied into five views; they belong in the controller. | fixed |
| SD-004 | S4 | confirmed | build.rs, src/partition/provision.rs | Two SGI header writers (`provision::write_sgi` rounds down, always swap slot 1); legacy alias names in the crate's 0.1.0 API; public `build_volume_header` panics / truncates on a hand-built layout. | fixed |
| FI-002 | S4 | confirmed | crates/rb-fsimporter, src/fs/dir_import.rs | `Expander` needs fs-tar types that are not re-exported; every method required; the app probes each image twice; the shim's `measure_dir` duplicates the crate's. | fixed |
| AP-001 | S4 | confirmed | regression-tests/data/change-map.tsv | No rows for crates/sgi-disk or crates/rb-fsimporter. | fixed |
| XF-005 | S4 | confirmed | crates/fs-xfs/src/xfs/repair.rs:19 | Still cites an xfsprogs file path as a reference; reword like the other modules. | fixed |
| BD-004 | S4 | confirmed | src/cli/verbs/new_sgi_hdd.rs | `--help` text and an error message still say EFS only. | fixed |

Also noted, not recorded: the new `Host` cancel is not wired into the app's own imports
(provision_runner, cd_build still use a never-cancelling host; since wired through
`dir_import::import_dir_cancellable`); XFS volumes with the ASCII
case-insensitive directory feature are not refused by the editor (plausible wrong hashes; would
need a `mkfs.xfs -n version=ci` image to confirm).

## crates/fs-mac (partial)

Read so far: the in-memory blank builders (test-only callers, so within the
streaming rule) and every HFS / HFS+ write that goes through on-disk extents.
Still to read: MFS, B-tree node-size and catalog-size handling, fsck repair, the
clone / defrag / journal modules. A regression run for 0.1.11 is still owed.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| FM-001 | S1 | confirmed | [hfs.rs](../../crates/fs-mac/src/hfs.rs) | The HFS catalog, extents-overflow and fork write-backs (`write_hfs_fork_data`, `write_extents_overflow_data`, the catalog's overflow loop) wrote through extents read from the MDB and the extents B-tree with no check that they end inside `total_blocks`, so a corrupt tree sent B-tree writes past the volume. `check_hfs_extent` now refuses them before any write. | fixed (fs-mac 0.1.11) |
| FM-002 | S1 | confirmed | [hfsplus.rs](../../crates/fs-mac/src/hfsplus.rs) | HFS+ `write_fork_data` / `write_fork_data_with_overflow` (catalog, extents and attributes B-trees) had the same gap. They take `total_blocks` and refuse an extent that overflows or ends past it. | fixed (fs-mac 0.1.11) |

## crates/fs-squashfs

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| SQ-001 | S3 | confirmed | [squashfs_edit.rs:1](../../crates/fs-squashfs/src/squashfs_edit.rs#L1) | The editor holds the whole decompressed image in RAM (`FileContent::Bytes`) and rebuilds it into a buffer, against the streaming rule. The module doc calls lazy streaming "the follow-up"; it has no tracking entry. Peak memory is the decompressed image size. | open |

## src/fs

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| FS-001 | S2 | plausible | [export_selection.rs:434](../../src/fs/export_selection.rs#L434) | Zip export builds member paths from raw entry names via `archive_name()`, which rejects only an exact `.` / `..`. Classic HFS and MFS allow `/` in a name, so `a/b` becomes a nested member and `../../x` a traversal member (`zip` does not refuse `..`; the `tar` crate does, so tar export fails instead). Folder export is safe (`sanitize_filename`). Confirm by `put`-ting a file named `../x` onto an HFS image and zipping it. | open |

## src/partition, src/rbformats, src/restore

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| PR-001 | S4 | confirmed | [restore_patch.rs:579](../../src/partition/restore_patch.rs#L579) | Two X68k table patchers: `restore_patch::patch_x68k` (slot matching, repack, keeps flags) and `part_tables::x68k::patch_x68k_entries` (used by `src/rbformats/mod.rs`). The crate one lost the flags byte until PT-010. One should go. | open |
| PR-002 | S4 | confirmed | [vhd.rs:246](../../src/rbformats/vhd.rs#L246) | `rebuild_vhd_ebr_chain` re-implements `restore::build_restore_ebr_chain` ("same algorithm", says its own comment), with its own silent fallback when the chain can't be rebuilt. | open |

## src/model/convert

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| CV-001 | S3 | confirmed | [classify.rs:81](../../src/model/convert/classify.rs#L81) | A 2 MB FAT image classifies as a floppy, and `--format qcow2` / `vmdk-sparse` / `chd` (hard-disk formats) are then ignored without a word: the output is raw. `subcmd.convert.{qcow2,vmdk-sparse,chd}-reopens` fail on main since the Convert redesign (`da41d419`). Either warn when `--format` doesn't apply to a kind, or let a hard-disk format pull a floppy-sized image along. The produce step's `fmt.vhd-fixed`, `fmt.vhd-dynamic`, `fmt.qcow2`, `fmt.vmdk-sparse`, `fmt.chd` and `optical.chdcd` artifacts are missing for the same reason. | open |

## iris-sgi-crates follow-up review (2026-11-21)

Review of `699dcf75..a580e817` by a separate agent against this rubric, under the same clean-room
limits as the rewrite (no kernel / hfsutils / SFS / pfs3aio source, no old `hfs_unicode.rs` body).
Probes in a scratch crate; 7 confirmed, 2 plausible.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| MU-001 | S2 | confirmed | fs-mac-unicode `decompose_str` vs collate `fold_hfsplus_name_for_compare`; fs-mac `find_catalog_record` | Lookup keys are now canonically reordered but the compare fold is not, so a name stored with marks out of canonical order (as fs-mac 0.1.10 wrote) no longer matches its own listed name: delete returns NotFound, and a create with the same name makes a second child. | fixed (0c801287) |
| MU-002 | S2 | confirmed | collate `compare_hfsplus_keys` | The comparator decomposes stored units before folding; with the new table a code point older writers stored precomposed (U+0218) sorts elsewhere, so fsck reports KeyOutOfOrder on a volume that was in order. Apple's `FastUnicodeCompare` folds only. | fixed (0c801287) |
| MU-003 | S4 | confirmed | `case_fold` / `decompose` | Two binary searches per unit on the catalog-compare hot path and no ASCII fast path: 772 ns vs 134 ns per 24-unit compare. | fixed (0c801287) |
| MU-004 | S4 | confirmed | collate.rs `compare_hfs_keys` | Three-line doc comment. | fixed (0c801287) |
| FX-001 | S4 | confirmed | fs-mac lib.rs feature table; rb-fsexporter README / CHANGELOG | `fork-export` missing from the lib.rs table (still under `forks`); rb-fsexporter docs still call fs-mac-unicode GPL. | fixed (0c801287) |
| AM-001 | S2 | plausible | fs-amiga sfs.rs node and admin pointers | `blk << shifts` is never checked against `1 << (32 - shifts)`: on a volume over 2^28 blocks a container placed high gets a truncated pointer (predates the rewrite). | fixed (0c801287) |
| DOC-001 | S4 | confirmed | CLAUDE.md SFS bullet; docs/missing_features_from_regression.md F-014; license_audit.md SFS row | Says object-node tree growth is unimplemented / mirrors the driver's functions; the audit row says "four functions" and names three. | fixed (0c801287) |
| DOC-002 | S4 | confirmed | crates/dsk-apple and 19 more | Crates already on crates.io were moved to disk-core 0.3 (91ccccee) without a version bump, so the publish workflow skips them and crates.io keeps the disk-core 0.2 builds; fs-mac 0.1.11, fs-pc 0.1.7 and rb-fsexporter 0.1.4 change a public dependency's major version in a patch release. | fixed (0c801287) |
| DOC-003 | S4 | plausible | fs-mac-unicode manifest / README | Tables derived from UnicodeData.txt: the Unicode licence's notice is linked, not reproduced, and the manifest says only MIT. The use of Apple's TN1150 sample mapping has no recorded terms check. | fixed (0c801287) |

## crates/fs-xfs

Review by a separate agent, 2026-11-21, read-only (no cargo; confirmed by quoted code and traced
inputs). `dir2_tree.rs` and `dir_hashname` excluded (rewritten clean-room, reviewed above). The
editor and repair were only ever checked against the crate's own fsck, which accepts v4 inobt
magic on v5, skips attribute forks and does not cross-check finobt / rmapbt.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| XFS-001 | S1 | confirmed | edit.rs `try_claim_slot_from_existing_chunks`, `alloc_new_inode_chunk`, `alloc_blocks` | Editor ignores finobt / rmapbt / reflink / inobtcnt and updates none of them: on a ro_compat 0xF volume a create leaves finobt and rmapbt stale. | fixed (a5dc5563) |
| XFS-002 | S1 | confirmed | freespace_rebuild.rs `run_freespace_rebuild`, `build_inuse_map` | R2 runs on finobt volumes but never marks finobt blocks in use, so the rebuilt bnobt lists them free and a later allocation overwrites them. | fixed (a5dc5563) |
| XFS-003 | S1 | confirmed | inobt_repair.rs `rebuild_single_inobt_leaf`, `rebuild_multilevel_inobt` | R3 on v5 (no rmapbt) writes a v4 IABT block (16-byte header, no CRC) that the kernel rejects. | fixed (a5dc5563) |
| XFS-004 | S1 | confirmed | fsck.rs `scan_inode_blocks`; freespace_rebuild.rs `mark_inode_blocks` | Attribute-fork blocks are never claimed: fsck reports UnaccountedBlocks on clean volumes, R2 / R3 free them, delete leaks them. | fixed (a5dc5563) |
| XFS-005 | S1 | confirmed | edit.rs `do_delete_entry` | Delete ignores `di_nlink`: removing one name of a hard-linked file frees the inode and blocks the other name still uses. | fixed (a5dc5563) |
| XFS-006 | S1 | confirmed | edit.rs `sf_remove_entry`, `rebuild_dir_without` | Names go through `from_utf8_lossy` and are written back: a directory rebuild turns non-UTF-8 names into U+FFFD, and deleting one of two names that are lossily equal removes the wrong entry. | fixed (a5dc5563) |
| XFS-007 | S1 | confirmed | edit.rs `current_full_free`, `free_blocks`, slot claim | The allocator trusts bnobt records and `ir_free` unchecked: a record past the AG end sends data writes past `dblocks`; a wrong free mask overwrites a live inode. | fixed (a5dc5563) |
| XFS-008 | S1 | confirmed | xfs/mod.rs `open`; src/fs/mod.rs editable XFS openers | No check of `dblocks * blocksize` against the partition length on MBR / GPT volumes: edits and R2 write past the partition end (SD-001 covered sgi-disk only). | fixed (a5dc5563) |
| XFS-009 | S1 | confirmed | edit.rs `sf_insert_entry`; dir_repair.rs | Editor and R6 do not check `dir_format`: on a dir1 volume (IRIX 6.2 / 6.4) they write dir2 layout into dir1 forks. | fixed (a5dc5563) |
| XFS-010 | S1 | confirmed | edit.rs `sf_insert_entry` | A parent with `i8count > 0` gets a 4-byte inumber appended, so the fork misparses. | fixed (a5dc5563) |
| XFS-011 | S1 | plausible | sb.rs; edit.rs; fsck.rs | Sparse inode chunks are not refused: the slot claim can place an inode in a hole. Confirm with `mkfs.xfs -i sparse=1`, fragment, create, `xfs_repair -n`. | fixed (a5dc5563) |
| XFS-012 | S1 | plausible | edit.rs, repair.rs | No clean-log check before editing or repairing; recovery would replay over our writes. Confirm: shut down with a pending transaction, edit, mount. | deferred (needs a log-head scan; README says to edit only cleanly unmounted volumes) |
| XFS-013 | S2 | confirmed | sb.rs `parse` | Superblock geometry unchecked: v5 with 128-byte inodes panics on a slice, large `dirblklog` shift-panics or allocates 2 GiB, `dblocks < (agcount-1)*agblocks` underflows (R2 loops ~2^64); unknown v5 incompat bits accepted. | fixed (a5dc5563) |
| XFS-014 | S2 | confirmed | xfs/mod.rs `walk_bmbt`, `collect_bmbt_blocks` | A bmbt leaf whose `rightsib` is itself loops forever and grows a Vec until OOM. | fixed (a5dc5563) |
| XFS-015 | S2 | confirmed | freespace_rebuild.rs `InUseMap::new`; inobt_repair.rs | R2 / R3 allocate a `Vec<bool>` of `dblocks` with no cap: 1 GiB for 4 TiB, abort on a hostile `dblocks`. | fixed (a5dc5563) |
| XFS-016 | S2 | confirmed | edit.rs `sf_entries_with_ftype`, `gather_dir_entries` | Short-form dir with too large a count or `di_size` past the inode panics on index in create / rename / delete. | fixed (a5dc5563) |
| XFS-017 | S2 | confirmed | xfs/mod.rs `read_symlink_target` | v5 remote symlinks include the 56-byte XSLM header in the target, and only the first block is read. | fixed (a5dc5563) |
| XFS-018 | S2 | confirmed | inode.rs | BIGTIME timestamps not decoded: mtimes wrong in listings and tar export. | fixed (a5dc5563) |
| XFS-019 | S4 | confirmed | repair.rs, edit.rs, freespace_rebuild.rs, inobt_repair.rs, inode_repair.rs, format.rs | Stale docs (mutations "Unsupported", "v4 only", "v5 gate"); public test-only `create_blank_xfs` builds a volume-sized Vec. | fixed (a5dc5563) |
| XFS-020 | S4 | confirmed | fsck.rs, inobt_repair.rs, v5_crc.rs, edit.rs | Duplicate btree walkers / `mark_*_blocks` / `stamp_*`; each allocation rebuilds the AG's whole bnobt / cntbt; edit.rs is 4,145 lines with five jobs. | deferred (refactor of edit.rs, own change) |

## crates/fs-amiga

Review by a separate agent, 2026-11-21, read-only; the SFS growth functions rewritten that day
excluded. No reference C sources opened.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| AMI-001 | S1 | confirmed | affs_fsck.rs `repair_affs` | AFFS repair rebuilds the bitmap after a broken walk (PFS3 / SFS refuse unless structurally clean): one bad directory checksum frees that subtree's blocks. | fixed (0360cf79) |
| AMI-002 | S1 | confirmed | affs_fsck.rs `walk_directory_tree`; affs.rs | DOS\4 / DOS\5 dircache not modelled: fsck flags every clean DCFS volume and repair frees the cache blocks; create / delete / rename never update the cache and are not refused. | fixed (0360cf79) |
| AMI-003 | S1 | confirmed | affs.rs `resize_affs_in_place` | The resize moves the root but leaves the root children's `parent` (0x1F4) pointing at the old root, which is then freed. | fixed (0360cf79) |
| AMI-004 | S1 | plausible | affs.rs `find_affs_root_block` | The fallback scan accepts the first block that looks like a root (no checksum, no position): an ADF stored on the volume below the real root can be taken for it. | fixed (0360cf79) |
| AMI-005 | S1 | confirmed | sfs.rs `extent_btree_insert` | Extent length stored as `blocks as u16` with no cap: a file over 65,535 blocks is truncated, its tail leaked. | fixed (94e22553) |
| AMI-006 | S1 | confirmed | sfs.rs `is_metadata_block` | Dirty data blocks whose first 4 bytes look like an SFS tag get re-checksummed on read / sync: file content `OBJC...` has bytes 4..8 overwritten. | fixed (94e22553) |
| AMI-007 | S1 | confirmed | sfs.rs object-node create / delete / rename | HTAB hash chains never maintained: the SFS handler cannot open new files by name, delete cuts chains, rename leaves entries in the old bucket. | fixed (94e22553): hash tables maintained; hash measured on the Workbench SFS fixture |
| AMI-008 | S1 | confirmed | pfs3.rs / sfs.rs `open`; src/fs/mod.rs callers | PFS3 / SFS editors take no partition length: a volume claiming more than its RDB partition gets writes in the next partition. | fixed (0360cf79) |
| AMI-009 | S1 | plausible | pfs3.rs `do_delete_entry` | Deleting an ST_LINKDIR reads and frees, as a dirblock, the reserved block whose number is the link's parent anode. | fixed (0360cf79) |
| AMI-010 | S2 | confirmed | affs.rs / pfs3.rs / sfs.rs `create_file` | Sizes of 4 GiB or more stored as `u32` (PFS3 never writes `fsizex`): should be refused. | fixed (0360cf79) |
| AMI-011 | S2 | confirmed | affs_common.rs `read_bstr` vs affs.rs `write_bstr`; pfs3.rs; sfs.rs | Names read as Latin-1 / lossy UTF-8 but written as UTF-8: AFFS hashes the wrong bytes (NotFound on delete / rename of a non-ASCII name, duplicates on create). | fixed (0360cf79) |
| AMI-012 | S2 | confirmed | affs.rs, pfs3.rs, sfs.rs, pfs3_clone.rs chain / tree walks | Hash chains, extension chains, anode chains, SFS BNodes and the PFS3 clone walk have no cycle or depth cap: self-links hang or overflow the stack. | fixed (0360cf79) |
| AMI-013 | S2 | confirmed | pfs3_clone.rs `clone_pfs3_volume`; pfs3.rs `do_create_file` | Clone stages all data in memory and syncs once: peak RAM is the volume's used data. Single-file creates hold the whole file (S3). | fixed (0360cf79) |
| AMI-014 | S2 | confirmed | affs.rs `CompactAffsReader::new` | Uses `open` not `open_sized`: on an RDB disk the root search fails or the compact size covers the following partitions. | deferred (needs the partition size threaded through compact_partition_reader) |
| AMI-015 | S2 | confirmed | sfs.rs `list_directory`, `create_file` | SFS drops protection, comment and dates on read and ignores them on write; softlinks list as empty files; no volume label. | fixed (0360cf79) |
| AMI-016 | S4 | confirmed | affs.rs, sfs.rs, pfs3.rs, *_fsck.rs | Duplicates (`sync_metadata` / `flush_metadata`, `fs_type` / `fs_type_label`, `fold`, fsck helpers x3); stale comments; `create_days` read from 0x1A4 (root alteration date). | partly fixed (0360cf79): create date offset; duplicates left |

## crates/fs-mac (remainder)

Review by a separate agent, 2026-11-21, read-only, of what the partial review above left: MFS,
B-tree handling, fsck / repair, clone / defrag / journal, codecs.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| MAC-001 | S1 | confirmed | mfs.rs `MfsDirEntry::encode` | Directory names up to 255 bytes parse but are cut to 27 on every directory write-back: any edit renames every file with a longer name. | fixed (b9aecbe2) |
| MAC-002 | S1 | confirmed | mfs.rs `MfsMdb::parse`, `alloc_block_offset`, `map_set`, `map_write_back` | MDB geometry unchecked: the map can overlap the directory, allocations can pass the volume end, more than 4094 blocks wrap the 12-bit links, counters overflow. | fixed (b9aecbe2) |
| MAC-003 | S1 | confirmed | hfs_fsck/catalog.rs `repair_catalog_consistency`, `fix_thread_record` | When a missing thread does not fit its leaf, repair deletes the directory record (orphaning the subtree) or removes the old thread first and leaves none. | fixed (b9aecbe2) |
| MAC-004 | S1 | confirmed | hfs.rs `repair`; hfs_fsck/bitmap.rs | The extents-overflow tree is read with `.ok()`: a read error rebuilds the bitmap without overflow extents and frees fragmented files' later extents. | fixed (b9aecbe2) |
| MAC-005 | S2 | confirmed | hfs_fsck/bitmap.rs `repair_extents` | On an overlap the whole range is dropped from the used set, so a later cross-link with the first file survives repair; zeroing extent j keeps j+1. | fixed (b9aecbe2) |
| MAC-006 | S1 | plausible | hfs_fsck/btree.rs `rebuild_index_nodes`; hfs.rs `repair` | A failed index rebuild leaves `root_node` on a zeroed node and the catalog is written anyway. | fixed (b9aecbe2) |
| MAC-007 | S2 | confirmed | hfs.rs `list_children`, file-extents lookup; hfs_clone.rs `walk_catalog` | Classic HFS leaf-chain walks have no visited set: a self-linked leaf hangs list, read and clone. | fixed (b9aecbe2) |
| MAC-008 | S2 | confirmed | hfs.rs, hfs_clone.rs, hfsplus.rs, hfs_common.rs `btree_node_free_space` | `node_size - 2*(i+1)` from on-disk numRecords underflows and panics; HFS+ open accepts node sizes 1-13. | fixed (b9aecbe2) |
| MAC-009 | S2 | confirmed | hfs_common.rs `btree_node_bitmap_range` | Header-node record offsets unchecked: panics on small catalogs. | fixed (b9aecbe2) |
| MAC-010 | S2 | confirmed | hfs_common.rs `btree_find_insert_leaf`, `btree_insert_full` | `root_node` past the file panics; an index cycle inserts out of order (or into the header node); a zeroed root field orphans every leaf. | fixed (b9aecbe2) |
| MAC-011 | S1 | plausible | hfs_common.rs insert budget, `btree_split_leaf_with_insert` | The free-node budget assumes a 2-way split; a 3-way split can run out mid-cascade and the retry inserts twice. | fixed (b9aecbe2) |
| MAC-012 | S2 | confirmed | hfsplus_journal.rs `read_transaction`, `replay_journal` | Journal block targets unbounded (outside the checksum): replay can write past the partition; the multiply can overflow. | fixed (b9aecbe2) |
| MAC-013 | S2 | confirmed | fork_export.rs; resource_fork.rs `build_appledouble`, `build_macbinary` | Export drops Finder flags (invisible, custom icon, bundle) in BinHex, MacBinary and AppleDouble. | fixed (b9aecbe2) |
| MAC-014 | S2 | confirmed | binhex.rs `parse_decoded`; resource_fork.rs `parse_macbinary`; mac_alias.rs | Unchecked `usize` adds of u32 lengths wrap on 32-bit targets and panic. | fixed (b9aecbe2) |
| MAC-015 | S2 | plausible | hfs.rs `read_fork_data`; lisa.rs `open` | Allocations sized by on-disk u32 fields before checking the stream length. | fixed (b9aecbe2) |
| MAC-016 | S2 | plausible | hfsplus_wrapper_clone.rs `patch_mdb_in_place` | A wrapper shrink keeps the bad-block extent at the old length, past the new volume end. | deferred (needs a shrunk OS 8.1 wrapper volume to confirm with fsck_hfs) |
| MAC-017 | S3 | confirmed | hfsplus_defrag.rs `emit_progress` | `eprintln!` in library code; thread-local progress sink; no `cancel_check` for whole-volume streams. | fixed (b9aecbe2) |
| MAC-018 | S3 | confirmed | mfs.rs `create_blank_mfs`; hfv.rs `build_blank_hfv` | Public builders hold the whole volume in RAM, above the 3 MB floppy exception. | deferred (app caps MFS at 800 KB and build_blank_hfv is test-only; streaming builders are their own change) |
| MAC-019 | S4 | confirmed | hfsplus.rs, hfsplus_defrag.rs, hfs_fsck | Three copies of `catalog_compare`; near-duplicate hardlink creators and fsck pairs. | deferred (refactor, own change) |
| MAC-020 | S4 | confirmed | mfs.rs, mac_roman.rs, mac_alias.rs, CLAUDE.md | Docs that no longer match (MFS "extract-only", name-length doc, map offset, GPL note, HFV limit 2047 vs 4063 MB); mac_alias shows Mac Roman as `?`. | fixed (b9aecbe2) |

## crates/dsk-ghost

Review by a separate agent, 2026-11-21, read-only.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| GHO-001 | S1 | confirmed | gho.rs `synthesize_mbr`, `synthesize_ebrs` | Every partition after the first is put in an extended container with an EBR at `hidden - 63`: with two primaries the EBR lands in P1's last track and is served over real data; P2 is unreachable. | fixed (3612a6a6) |
| GHO-002 | S1 | confirmed | gho.rs multi-partition VBR read | Partition size taken from FAT BPB fields only: an NTFS partition gets size 0 and is dropped from the MBR; FAT12 / unknown typed 0x0C. | fixed (3612a6a6) |
| GHO-003 | S2 | confirmed | gho.rs trim / reassign / binary search | Trimmed blocks keep stale offsets in the sorted list, so reads near a partition end hit the wrong block (error or zeros). | open |
| GHO-004 | S2 | confirmed | gho.rs `ensure_block_cached` | A non-final block that decodes short returns Ok(0) mid-image: the output is silently truncated. | open |
| GHO-005 | S2 | confirmed | gho.rs `ensure_cluster_decoded` | Each cluster miss decodes the file from its start: O(n^2), effectively a hang on large files. | open |
| GHO-006 | S2 | confirmed | gho.rs `GhoFileContentReader::fill_next` | Uncompressed body longer than the 33,792-byte buffer panics. | open |
| GHO-007 | S1 | plausible | gho.rs `build_virtual_fat_image` | create errors swallowed, so files and subtrees silently go missing; layout re-derived from size can change cluster size and run out of space. | fixed (3612a6a6) |
| GHO-008 | S2 | confirmed | gho.rs `build_virtual_fat_image` | DFS without a visited set: a dir pointing back at the root nests until the volume is full, all in RAM. | fixed (3612a6a6) |
| GHO-009 | S2 | confirmed | gho.rs `gather_ghs_siblings` | Spans matched by name prefix only: a neighbouring image's spans are merged in; a relative pick can choose the wrong primary; a missing span is not detected. | open |
| GHO-010 | S2 | confirmed | gho.rs NTFS file-aware open | VBR `total_sectors` unchecked: a huge value aborts on allocation, products overflow. | open |
| GHO-011 | S2 | confirmed | gho.rs `open_ntfs_compressed_mft_only`, `extend_ntfs_compressed_index` | Inflate output unbounded (~4 GiB); oversized block shifts later offsets and wrong data is served. | open |
| GHO-012 | S2 | confirmed | gho.rs lazy NTFS scan; trim | I/O errors swallowed: unmapped clusters served as zeros, real errors treated as padding. | open |
| GHO-013 | S2 | plausible | gho.rs uncompressed multi-partition | Acknowledged-unsupported layout served as one raw stream instead of refused; later partitions shifted. | open |
| GHO-014 | S2 | confirmed | gho.rs MBR / EBR synthesis, blocked read | Unchecked subtraction of partition geometry (hidden < 63, overlapping or unsorted) panics or yields garbage. | fixed (3612a6a6) |
| GHO-015 | S3 | confirmed | gho.rs `unsafe impl Send for GhoReader` | Unneeded `unsafe impl Send`. | open |
| GHO-016 | S4 | confirmed | gho.rs | Dead / duplicated paths (materialize, emit copies, four zlib loops, three Seek impls; one in-RAM volume emitter); 9,850-line file with six jobs. | open |
| GHO-017 | S4 | confirmed | gho.rs headers | Stale docs (file-aware "deferred", password "errors", "SECTOR only"). | open |

## crates/fs-8bit

Review by a separate agent, 2026-11-21, read-only.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| B8-001 | S1 | confirmed | cbm.rs `read_bam`, `write_bam`, `create_blank` | D64 40-track BAM for tracks 36-40 overlaps the disk name / ID: blank images get a garbage name, real 40-track disks are misread and their names overwritten. | fixed (409017f1) |
| B8-002 | S1 | confirmed | cbm.rs `recompute_bam` / `repair` | GEOS VLIR record chains and info blocks are freed by repair; GEOS disks should be refused like REL. | fixed (409017f1) |
| B8-003 | S1 | confirmed | trdos.rs `create_file` | New catalogue slot taken from the disk-info count, not the walk: a wrong count overwrites a live entry or hides the new one. | fixed (409017f1) |
| B8-004 | S1 | confirmed | oric.rs `create_file_internal` | Length stored as u16: files over 64 KiB read back truncated. | fixed (409017f1) |
| B8-005 | S1 | confirmed | os9.rs `build_fd`, `allocate_segments` | Segment sector counts stored as u16 with no split: a 16 MiB+ contiguous file reads back empty. | fixed (409017f1) |
| B8-006 | S1 | plausible | atari_dos.rs create / delete | Enhanced-density (VTOC2) disks are edited as if single-VTOC: VTOC2 goes stale. Writes should be refused on ED. | fixed (409017f1) |
| B8-007 | S1 | confirmed | qdos.rs `parse_dir_entry`, `delete_entry` | File type read from the wrong offset: subdirectories list as files and deleting one orphans its contents. | fixed (409017f1) |
| B8-008 | S2 | confirmed | ti99.rs `create_file` | More than 76 cluster runs panics after data was written; `n_data as u16` truncates before the size check. | open |
| B8-009 | S2 | confirmed | os9.rs `dir_add_entry` | Adding an entry rewrites the parent's FD from scratch, zeroing owner, dates and link count. | open |
| B8-010 | S2 | confirmed | os9.rs `allocate_segments` | LSN 0 can be allocated when the bitmap bit is clear, overwriting the identification sector. | fixed (409017f1) |
| B8-011 | S2 | confirmed | os9.rs, qdos.rs, lif.rs reads | Allocations sized from on-disk u32 lengths (4 GiB to ~1 TB). | open |
| B8-012 | S2 | plausible | qdos.rs `FAT_END_LO` | Chains stop at 0xFFF8+ though clusters up to 65,534 are allowed. | open |
| B8-013 | S2 | confirmed | dragondos.rs `parse_geometry` | Track counts past the bitmap's 3,488 LSNs flip bits in the directory. | open |
| B8-014 | S3 | confirmed | cbm.rs, qdos.rs, atari_dos.rs, dragondos.rs, rsdos.rs, trdos.rs | Em dash (U+2014) in user-visible error strings. | open |
| B8-015 | S4 | confirmed | andos.rs, qdos_mdv.rs | Errors point at moved `src/fs/` paths; andos doc says it lists an empty root. | open |
| B8-016 | S4 | confirmed | rsdos.rs, atari_dos.rs, dragondos.rs, trdos.rs | Byte-identical name codecs and sector I/O helpers. | open |

## crates/fs-unix

Review by a separate agent (with four sub-forks), 2026-11-21, read-only. Rows that hit several
drivers are merged.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| UNX-001 | S1 | confirmed | ext.rs, ufs.rs, efs.rs, efs_v1.rs `delete_entry` | No `nlink` check: deleting one hard-link name frees the inode and blocks the other still uses. | fixed (72c94099) |
| UNX-002 | S1 | confirmed | ext.rs `delete_entry`, `inode_data_blocks` | `i_block` read as pointers for fast symlinks / devices / inline data: frees random blocks or panics. | fixed (72c94099) |
| UNX-003 | S1 | confirmed | ext.rs `add_dir_entry`, `try_insert_dir_entry` | htree (dir_index) directories not handled: the first insert overwrites `dx_root_info`; leaf inserts ignore hash order. | fixed (72c94099) |
| UNX-004 | S1 | confirmed | ext.rs `open`, `build_relocation_map` | No incompat / ro_compat gate: meta_bg, RECOVER (unreplayed journal), bigalloc, inline_data all edited wrongly. | fixed (72c94099) |
| UNX-005 | S1 | confirmed | ufs.rs, jfs.rs, efs.rs directory / inode writes | On-disk pointers never checked against the volume size: edits can write into the next partition. | fixed (48ac3f30) |
| UNX-006 | S1 | confirmed | ufs.rs `free_inode_blocks`, `free_frag_run` | Freeing trusts pointers: panics past the CG, frees CG headers / inode tables, frees cross-linked blocks. | partly fixed (f55f13c6): runs past the CG and double frees refused; the metadata-area check is left out (the CG layout of our own test volumes does not match the reviewer's model) |
| UNX-007 | S1 | confirmed | efs.rs directory code, efs_fsck.rs, efs_resize.rs | Indirect-extent directories (numextents > 12) treated as direct: insert overwrites an index block, listing shows empty, repair double-links. | fixed (72c94099) |
| UNX-008 | S1 | confirmed | efs_v1.rs `allocate_inode`, `dir_insert` | Dirents hold a u16 inum but inodes above 65,535 are handed out: the entry points at the wrong inode. | fixed (72c94099) |
| UNX-009 | S1 | confirmed | bfs_fsck.rs `check_inode`, `repair_bfs` | Double-indirect and indirect map blocks never claimed: repair frees them. | fixed (48ac3f30) |
| UNX-010 | S1 | confirmed | ofs_fsck.rs `claim_file`; ofs_write.rs `owned_runs` | Only the first allocation list counted: repair frees chained lists, delete leaks them. | fixed (48ac3f30) |
| UNX-011 | S1 | confirmed | bfs_write.rs `BTree::split_and_promote` | Internal split promotes the wrong key: some keys become unreachable. | fixed (f55f13c6) |
| UNX-012 | S1 | confirmed | bfs_write.rs `write_blank_bfs_into` | 65,537..65,539-block volumes put the root tree past the volume end. | fixed (f55f13c6) |
| UNX-013 | S1 | confirmed | minix.rs `write_blank_minix_into` | `firstdatazone` / `zmap_blocks` stored as u16 unchecked: large V3 volumes overlap data and inodes. | fixed (f55f13c6) |
| UNX-014 | S1 | plausible | minix.rs `parse`, alloc; minix_fsck.rs | Bitmap block counts not checked against zones / inodes: allocation repeats a zone, bit writes hit the inode table. | open |
| UNX-015 | S1 | plausible | ofs_write.rs `write_sectors`, `claim_slot` | Bound allows one sector past the volume; directory chain pointer can write into the bitmap. | open |
| UNX-016 | S1 | plausible | jfs_fsck.rs `check_connectivity`, `repair_jfs` | Directories over 4096 bytes skipped: children reported as orphans and double-linked. | open |
| UNX-017 | S1 | plausible | btrfs.rs `CompactBtrfsReader::new` | Boot area and superblock mirrors zeroed in a compacted backup: restored partition does not boot. | open |
| UNX-018 | S2 | confirmed | ufs.rs, efs.rs, efs_v1.rs orphan adoption | Children of an orphaned directory adopted twice; UFS `..` and lost+found nlink stale; efs `continue` in the wrong loop. | open |
| UNX-019 | S2 | confirmed | ext.rs, btrfs.rs, jfs.rs, reiserfs.rs tree walks | No depth cap / level check: self-pointing nodes overflow the stack or loop. | open |
| UNX-020 | S2 | confirmed | ext, ufs, jfs, jfs1, btrfs, bfs, vxfs | Allocations sized from on-disk fields (symlink size 1<<40 aborts a listing). | open |
| UNX-021 | S2 | confirmed | efs.rs `open`, `inode_byte_offset` | `cgisize` / `cgfsize` / `ncg` not validated: divide by zero on listing. | fixed (72c94099) |
| UNX-022 | S2 | confirmed | ext.rs `try_insert_dir_entry` | `rec_len - actual_len` underflows (panic / wrapped rec_len written). | fixed (72c94099) |
| UNX-023 | S2 | confirmed | ext.rs `delete_entry` | Deleting an ext2/3 file over 12 blocks leaks its indirect blocks. | fixed (72c94099) |
| UNX-024 | S2 | confirmed | ext.rs `free_blocks_list` | No range / state check: underflow, panic, double-free counted. | fixed (72c94099) |
| UNX-025 | S2 | confirmed | ext.rs `open` | Shift before range check, underflow, descriptor Vec sized from disk (up to 128 GiB). | fixed (72c94099) |
| UNX-026 | S2 | confirmed | ext.rs `add_block_to_extent_inode` | `eh_entries` / `eh_max` trusted: indexes past `i_block`. | fixed (72c94099) |
| UNX-027 | S2 | confirmed | ext.rs `rename` | File type recomputed as dir / regular: symlinks, devices, fifos get the wrong type. | fixed (72c94099) |
| UNX-028 | S2 | plausible | ext_fsck.rs; ext.rs | INODE_UNINIT / `itable_unused` ignored: false errors and repairs on lazy-init volumes. | open |
| UNX-029 | S2 | plausible | ext.rs `grow_ext_add_groups` | Grow ignores reserved GDT blocks and the resize inode. | open |
| UNX-030 | S2 | plausible | ext.rs; ext_fsck.rs | Block-bitmap checksum covers the whole block, not blocks_per_group / 8. | open |
| UNX-031 | S2 | plausible | ext.rs `resize_ext_in_place` shrink | Only counts patched: padding bits, descriptor table, inode count stale. | open |
| UNX-032 | S2 | confirmed | ufs.rs `dir_find`, `dir_remove` | Name slices without the `namlen <= reclen - 8` check: panic on create. | open |
| UNX-033 | S2 | confirmed | ufs.rs `create_file`, `create_directory` | Rollback runs with size 0 and frees nothing: blocks leak; `cs_ndir` off by one. | open |
| UNX-034 | S2 | confirmed | ufs.rs `repair_ufs` | Bitmap repair adjusts nffree only: summaries end up wrong. | open |
| UNX-035 | S2 | confirmed | ufs_fsck.rs `record_frags` | Only the first fragment of each block checked: shared fragments, link counts, cg_cs pass. | open |
| UNX-036 | S2 | confirmed | ufs.rs | `frag * fsize` on UFS2 64-bit pointers unchecked. | open |
| UNX-037 | S2 | plausible | ufs.rs `walk_inode_blocks` | Bounded by `di_size` only: a huge size with full triple indirects hangs. | open |
| UNX-038 | S2 | confirmed | jfs.rs `read_fileset_inode` | pmap read per extent, not per inode: after one delete, other inodes in that extent fail to read. | open |
| UNX-039 | S2 | confirmed | jfs_fsck.rs; jfs.rs | lost+found inode written before an insert that then fails: each repair leaks a directory. | open |
| UNX-040 | S2 | confirmed | btrfs.rs `parse_btrfs_superblock` | `sys_chunk_array_size` over 3285 panics at open. | open |
| UNX-041 | S2 | confirmed | btrfs.rs | Compressed extents read as zeros (should be Unsupported); PREALLOC returns stale bytes; short items zero-filled. | open |
| UNX-042 | S2 | plausible | btrfs.rs `resize_btrfs_in_place` | DEV_ITEM and mirror superblocks not updated; I/O errors return Ok. | open |
| UNX-043 | S2 | confirmed | reiserfs.rs `list_directory` | Each child re-walks every leaf: quadratic listing. | open |
| UNX-044 | S2 | plausible | reiserfs.rs | `s_bmap_nr` unchecked: bitmap reads past the volume; compact stream too long. | open |
| UNX-045 | S2 | confirmed | jfs.rs, jfs1.rs | Unchecked size multiplications. | open |
| UNX-046 | S2 | confirmed | bfs.rs `parse`; bfs_fsck.rs | Geometry not cross-checked: fsck loops 2^60 times. | open |
| UNX-047 | S2 | confirmed | bfs_write.rs `delete_entry` | Tree stored and runs freed before the double-indirect refusal; attribute dirs never freed. | open |
| UNX-048 | S2 | confirmed | vxfs_edit.rs | Panics on hostile input (namelen slice, underflow, maps / iau indexing). | open |
| UNX-049 | S2 | confirmed | minix.rs `create_file` | Size stored as u32 (4 GiB+1 records 1); a failure leaks the inode and zones. | open |
| UNX-050 | S2 | confirmed | minix.rs, vxfs_edit.rs `rename` | Old dirent removed before the insert: DiskFull leaves the inode nameless. | open |
| UNX-051 | S2 | confirmed | efs_v1.rs `read_dir_entries` vs efs_v1_fsck.rs | Reader drops out-of-range dirents, so fsck's check can never fire. | open |
| UNX-052 | S2 | plausible | unix_common/compact.rs `MappedBlocks` and callers | One u64 per allocated block (~2 GB for 1 TB); ext's tables add more: breaks the streaming rule. | open |
| UNX-053 | S3 | confirmed | unix_common/compact.rs `load_block` | One seek + read per block, not 64 KiB-1 MiB runs. | open |
| UNX-054 | S3 | confirmed | ufs.rs `open` | `total_frags` not compared with `ncg*fpg` or the slice (generic form of SD-001). | open |
| UNX-055 | S3 | confirmed | btrfs.rs `list_directory` | `Err(_) => continue` drops children silently. | open |
| UNX-056 | S3 | confirmed | jfs.rs, jfs_fsck.rs | `≥`, `⇒`, `—` in user-visible messages. | open |
| UNX-057 | S3 | confirmed | bfs_write.rs, ofs_write.rs, ofs.rs | Whole-source `read_to_end`; OFS buffers a whole extent. | open |
| UNX-058 | S4 | confirmed | bfs_write.rs, minix.rs, vxfs.rs | Each allocation rereads the whole bitmap; VxFS bmap rereads indirects per block. | open |
| UNX-059 | S4 | confirmed | efs_fsck.rs, efs_v1_fsck.rs, jfs_fsck.rs, ufs_fsck.rs (+ fs-xfs) | Five copies of the fsck `Builder`; belongs in `disk_core::fsck`. | open |
| UNX-060 | S4 | confirmed | ufs.rs, efs.rs | Full 27-field `FileEntry` literals (DC-008 class). | open |
| UNX-061 | S4 | confirmed | lib.rs, ufs.rs, efs_resize.rs, ext_format.rs | Stale docs (ofs GPL note, UFS scope, shrink hard-link claim, batch-builder caller). | open |

## crates/fs-apple2

Review by a separate agent, 2026-11-21, read-only.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| AP2-001 | S1 | confirmed | apple_dos.rs `read_file_bytes` + `read_file` | Type-B header stripped twice: some payloads read back as 1 byte. | fixed (39c02adb) |
| AP2-002 | S1 | confirmed | apple_dos.rs `create_file` | B header length `as u16`: files over 64 KiB read back truncated. | fixed (39c02adb) |
| AP2-003 | S1 | confirmed | prodos.rs `create_file` | Exactly 16 MiB accepted; EOF stored as 0, file reads empty, blocks stay allocated. | fixed (39c02adb) |
| AP2-004 | S2 | confirmed | prodos.rs directory-chain walks | No cycle guard: a self-linked dir block hangs list / create / delete. | open |
| AP2-005 | S2 | confirmed | prodos.rs `free_file_blocks` | Index pointers not range-checked before freeing: panic, or the volume directory freed. | open |
| AP2-006 | S2 | confirmed | prodos.rs read / write_to / delete | Storage type 5 (forked) treated as seedling: extract returns the fork descriptor; delete leaks both forks. | open |
| AP2-007 | S2 | plausible | prodos.rs `resize_prodos_in_place` | Shrink guard uses the used-block count, not the highest used block; stale buffer on a failed read. | open |
| AP2-008 | S2 | plausible | prodos.rs subdirectory builders | Subdir metadata off-spec (0-based parent entry number, EOF 0, counts not updated, volume dir grows past 4 blocks). | open |
| AP2-009 | S2 | plausible | prodos.rs `allocate_block`; ucsd.rs `find_gap` | Block 0 allocatable; volume size not checked against the partition. | open |
| AP2-010 | S2 | confirmed | apple_dos.rs `read_file_bytes` | Sparse (0,0) T/S pairs skipped instead of zero-filled: records shift. | open |
| AP2-011 | S2 | confirmed | apple_dos.rs `alloc_one_sector`, `free_sector_count` | Track 34 counted free but never allocated: creates fail half-way. | open |
| AP2-012 | S2 | confirmed | apple_dos.rs `bitmap_mark_free` | Freed T/S pairs not range-checked: panic. | open |
| AP2-013 | S2 | confirmed | apple_dos.rs `encode_apple_name` | Trailing spaces accepted, trimmed on read: failed create, then duplicates. | open |
| AP2-014 | S2 | confirmed | ucsd.rs `byte_size`, `read_file` | `last_byte` not clamped: reads spill into following files. | open |
| AP2-015 | S2 | confirmed | ucsd.rs `repair_ucsd` | BadName repair drops the entry and frees the file's blocks. | open |
| AP2-016 | S2 | confirmed | ucsd.rs `create_file` | Block count `as u16` wraps at 32 MiB; slice panics. | open |
| AP2-017 | S3 | confirmed | prodos.rs, apple_dos.rs, ucsd.rs, sector_order.rs | Em dash / multiplication sign in user-visible errors. | open |
| AP2-018 | S3 | confirmed | prodos_types.rs `parse_json` | Format crate reads the app config dir; a bad override file is silently ignored. | open |
| AP2-019 | S4 | confirmed | apple_dos.rs, prodos.rs | Catalog walk copied 5 times; read_* duplicate write_*_to; fork walkers duplicated. | open |

## crates/fs-xerox

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| XRX-001 | S1 | confirmed | alto/write.rs `build_disk` | VDAs and freePages stored as u16: Trident packs always fail fsck and point at wrong pages past 65535. | fixed (0c22507d): rebuild of packs over 65535 pages refused |
| XRX-002 | S1 | plausible | alto/write.rs `extract_files`, `build_disk` | Every edit / repair rebuilds from SysDir only: sub-directory files dropped, boot page not copied. | open |
| XRX-003 | S2 | confirmed | alto/bfs.rs `sync_metadata`, `repair`; src/model/browse_session.rs | Non-atomic save, always PDI: a Salto `.dsk` overwritten as PDI. | open |
| XRX-004 | S2 | confirmed | alto/pdi.rs `read_header` | Label / data sizes unchecked against the family: panic on open; product overflow. | open |
| XRX-005 | S2 | confirmed | alto/copydisk.rs `read` | Allocation from u16 geometry before reading: OOM / panic. | open |
| XRX-006 | S2 | confirmed | alto/zdisk.rs `inflate` | Uncapped inflate: zlib bomb. | open |
| XRX-007 | S2 | confirmed | alto/pilot.rs `leader_name` | Property read past the 512-byte page: panic on open. | open |
| XRX-008 | S2 | confirmed | alto/pilot.rs `parse_client_directory` | Reads a word past the page. | open |
| XRX-009 | S2 | confirmed | alto/pilot.rs volume / file builders | Loops over on-disk u32 counts unclamped: long hangs, huge run tables. | open |
| XRX-010 | S2 | confirmed | alto/pilot.rs `rebuild_vam`, `delete_file` | Direct sector indexing from descriptor sizes: panic on add / delete. | open |
| XRX-011 | S1 | confirmed | alto/pilot.rs `add_file` | Runs stored `as u16`: files over 32 MiB read back truncated. | fixed (0c22507d) |

## crates/arc-mac

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| ARC-001 | S2 | confirmed | stuffit_huffman.rs, stuffit_lzah.rs, stuffit_lzh.rs; arsenic, compactpro reserves | Decoders don't stop at end of input and reserve the header length up front: 4 GiB bombs. | open |
| ARC-002 | S2 | confirmed | stuffit_lzw.rs `decompress`, `BitCursor::skip` | Pad skip after CLEAR overruns: panic (reachable from dsk-tape `.Z`). | open |
| ARC-003 | S2 | confirmed | stuffit_lzw.rs | `.Z` max bits not capped at 16; output unbounded. | open |
| ARC-004 | S2 | confirmed | maczip.rs `inflate` | Inflate uncapped, size checked after. | open |
| ARC-005 | S2 | plausible | mar.rs, compactpro.rs, stuffit.rs | Unbounded folder nesting recursion; per-level path clones. | open |
| ARC-006 | S2 | confirmed | extract.rs `write_entry` | Entries mapping to the same host path overwrite each other and existing files. | open |
| ARC-007 | S2 | confirmed | extract.rs `write_entry` | MacBinary / AppleDouble output drops dates and Finder flags. | open |
| ARC-008 | S2 | plausible | stuffit.rs, stuffit5.rs | usize overflow on 32-bit targets: infinite loop / panic. | open |
| ARC-009 | S3 | confirmed | stuffit.rs, mar.rs writers | Sizes / counts truncated and names cut silently, returning Ok. | open |
| ARC-010 | S3 | confirmed | extract.rs, mar.rs, maczip.rs | Archives held whole in RAM, several copies. | open |
| ARC-011 | S3 | confirmed | maczip.rs, stuffit5.rs `parse` | Unreadable forks dropped silently; ZIP CRC unchecked; truncated SIT5 unreported. | open |
| ARC-012 | S2 | plausible | stuffit_arsenic.rs `read_block` | Zero-run counters overflow (panic / spin). | open |
| ARC-013 | S4 | confirmed | arsenic, lzah, lzh, detect.rs | Duplicate bit readers / PrefixCode / three CRC-32s; BinHex decoded twice. | open |

## crates/dsk-apple

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| DAP-001 | S2 | confirmed | woz.rs data-field decoders | WOZ sector checksums never checked (3.5" eats a checksum nibble as data); missing 5.25" sectors zeroed silently. | open |
| DAP-002 | S2 | plausible | woz.rs | `bit_count * 2` from the TRK descriptor overflows. | open |
| DAP-003 | S2 | plausible | dc42.rs `encode_dc42_with_tags`; dart.rs | Tag checksum range, format byte and disk_format look off-spec; tagged DARTs all named "Lisa". | open |
| DAP-004 | S4 | confirmed | dc42.rs | Disk names UTF-8 instead of Mac Roman. | open |
| DAP-005 | S4 | plausible | woz_write.rs `FORMAT_35_SSDD` | 400K format byte disagrees with dc42.rs; 3.5" WOZ marked Apple ][. | open |
| DAP-006 | S4 | confirmed | woz.rs, moof.rs | Duplicate readers, extra TRKS copy, WOZ CRC unchecked, dead code. | open |

## crates/fs-squashfs, crates/dsk-vm, crates/fs-tar

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| SQ-002 | S2 | confirmed | squashfs.rs `decompress`, `decompress_zstd` | XZ / LZMA / zstd output uncapped: GiB-scale bombs. | open |
| SQ-003 | S2 | confirmed | squashfs.rs `Superblock::parse` | `block_log` unbounded: shift panic, 2 GiB blocks. | open |
| SQ-004 | S2 | confirmed | squashfs.rs `build_node`; squashfs_verify.rs | No directory-cycle guard: stack overflow / endless verify. | open |
| SQ-005 | S2 | confirmed | squashfs.rs `read_metadata_span` and table loads | Capacities taken from on-disk u32s (4 GiB / 64 GiB). | open |
| SQ-006 | S3 | confirmed | squashfs_write.rs `write_node`; `read_build_tree` | Dir `parent` written wrong; hard links duplicated on rebuild. | open |
| SQ-007 | S4 | confirmed | squashfs.rs, squashfs_write.rs | Comment contradicts code; whole-Vec clone per file; near-duplicate table loaders. | open |
| DVM-001 | S1 | plausible | qcow2.rs `open`, `ensure_free_cluster_index` | Edits accepted on dirty / corrupt images; with lazy refcounts live clusters are reused. | fixed (3612a6a6) |
| DVM-002 | S2 | confirmed | qcow2.rs `export_qcow2` | One-cluster refcount table overflows into the first L2 table at small cluster sizes (latent). | open |
| DVM-003 | S1 | confirmed | vmdk_sparse.rs `export_vmdk_sparse` | Host offset `as u32` sectors: disks past 2 TiB written corrupt. | fixed (3612a6a6) |
| DVM-004 | S2 | confirmed | vmdk.rs descriptor parse / `open_with` | Absolute or `..` extent paths opened read-write: an edit can write into an arbitrary host file. | open |
| DVM-005 | S2 | confirmed | vhd.rs `write`, `allocate_block` | Unchecked BAT index, 4 GiB block buffers, offset overflow, unaligned file length shifts data. | open |
| DVM-006 | S2 | confirmed | vmdk_sparse.rs header parse | Grain size / GT entries unbounded: 8 GiB / 1 TiB allocations. | open |
| DVM-007 | S2 | plausible | vmdk.rs `read_descriptor` | Only 64 KiB of descriptor parsed: many-extent disks fail or silently shorten. | open |
| DVM-008 | S2 | plausible | qcow2.rs `open`, `allocate_cluster` | Unaligned file length gives unaligned new clusters and wrong refcounts. | open |
| DVM-009 | S4 | confirmed | vmdk.rs, lib.rs, qemu_img_test.rs | Test-only public fn; helpers duplicated with src/rbformats. | open |
| TAR-001 | S1 | plausible | tar_import.rs `NormalizedTar::fill` | Data blocks counted from the ustar size only: pax sizes / GNU sparse headers make data be rewritten as headers (silent corruption). | open |
| TAR-002 | S2 | confirmed | tar_import.rs; zip_import.rs `plan` | `skip_appledouble=false` ignored: `._` files always consumed; preflight disagrees. | open |
| TAR-003 | S2 | confirmed | mem_archive.rs | Small compressed archives inflated fully into RAM uncapped; indexed path fills disk; zip capacity from declared size. | open |
| TAR-004 | S2 | plausible | tar_import.rs `open_tar_stream` | Multi-member .tar.gz imports only the first member. | open |
| TAR-005 | S2 | plausible | tar_export.rs `walk` | Export recursion has no depth / visited guard. | open |
| TAR-006 | S4 | confirmed | import_sink.rs, tar_export.rs, mem_archive.rs, zstd_compat.rs, zip_import.rs | Stats merge drops a field; stale docs; pure-zstd can't browse .tar.zst; duplicate date code. | open |

## crates/fs-cpm

Review by a separate agent, 2026-11-21, read-only.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| CPM-001 | S1 | confirmed | cpm.rs `create_file`, `free_blocks`, `is_active` | Slots with user >= 16 (CP/M 3 label / SFCB / password, ZSDOS users 16-31) are treated as free: new files overwrite them and their blocks are reallocated. | fixed (13054e1c) |
| CPM-002 | S2 | confirmed | cpm.rs `analyze`, `run_repair` | fsck calls user >= 16 entries invalid and repair deletes them (password entries, ZSDOS files). | open |
| CPM-003 | S2 | confirmed | cpm.rs `encode_dir_entry`, `dir_write_back` | Every write-back re-encodes all slots lossily: SFCB dates, label mode flags and f1'-f4' attributes are lost. | open |
| CPM-004 | S2 | confirmed | cpm.rs `collect_files`, `read_group` | EXM ignored: on exm > 0 presets (EINSTEIN, SVI328, MULTICOMP) files over 16 KB list short and read truncated. | open |
| CPM-005 | S1 | confirmed | cpm.rs `create_file` | Writer ignores EXM too: real CP/M sees a different file than we wrote. | mitigated (13054e1c): writes refused on exm > 0 presets until EXM is implemented |
| CPM-006 | S1 | confirmed | cpm_diskdefs.rs `ZX_PLUS3`, `SVI328_CPM` | Data area runs past the disk geometry: allocations and blank images go past the image end. | fixed (13054e1c): allocation clamped to the blocks the image holds; preset values left as they are |
| CPM-007 | S2 | confirmed | cpm_diskdefs.rs `ALTAIR_CF`, `SVI328_CPM`; cpm.rs | Directory size from al0/al1 disagrees with drm: ALTAIR_CF creates always fail at sync after writing data. | open |
| CPM-008 | S1 | confirmed | cpm.rs list / find / delete / rename | User number dropped from name handling: same-named files in two users collide; delete removes both. | fixed (13054e1c) |
| CPM-009 | S2 | confirmed | cpm.rs `create_file` | Empty files never get an entry; a mid-loop "directory full" leaves a truncated file to be synced. | open |
| CPM-010 | S2 | plausible | cpm_diskdefs.rs `ALTAIR_8IN`; cpm.rs | No sector skew: ibm-3740 images (skew 6) read garbled. | open |
| CPM-011 | S3 | confirmed | cpm.rs `validate_cpm_name` | Wildcard / separator characters accepted in names. | open |
| CPM-012 | S3 | confirmed | cpm.rs | Non-ASCII glyphs in user-visible errors. | open |
| CPM-013 | S3 | confirmed | cpm.rs `read_file`, `create_file` | Whole-file buffering (DC-006 for this crate; largest preset 4 MiB). | open |
| CPM-014 | S4 | confirmed | cpm.rs, cpm_diskdefs.rs | Dead locals and unread DPB fields; `tracks` documented as a bounds check but unused. | open |

## crates/dsk-dmg, dsk-clonezilla, dsk-floppy, dsk-flux, dsk-imz, dsk-tape

Review by a separate agent, 2026-11-21, read-only.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| DMG-001 | S2 | confirmed | dsk-dmg dmg.rs `detect_dmg`, `ensure_cached`, `parse_mish` | XML length, pack / unpack sizes and zero blocks sized from the header unchecked: OOM; offset product overflows. | open |
| DMG-002 | S2 | confirmed | dsk-dmg dmg.rs `Read for DmgReader`, `copy_match` | A short chunk returns Ok(0) mid-disk (silent truncation); bad ADC distances become zeros. | open |
| DMG-003 | S2 | confirmed | dsk-dmg dmg_crypto.rs `CryptoContext::open` | `wrap_key_bits` slice panics; offset overflow; PBKDF2 iterations uncapped. | open |
| DMG-004 | S2 | confirmed | dsk-dmg sparseimage.rs `detect_sparseimage` | Band table sized from header values: 32 GiB from a 4 KiB file. | open |
| DMG-005 | S2 | confirmed | dsk-dmg ndif.rs `reconstruct_to` | Capacity from an unchecked entry count (~128 GiB). | open |
| CZL-001 | S2 | confirmed | dsk-clonezilla partclone.rs `parse_header`, `parse_bitmap` | `block_size` / `total_blocks` unvalidated: OOM, a block size of 0 hangs, overflow. | open |
| CZL-002 | S2 | plausible | dsk-clonezilla partclone.rs `parse_header` | v1 images likely rejected by the magic check; endianness field ignored. | open |
| CZL-003 | S2 | confirmed | dsk-clonezilla metadata_scan.rs `identify_ext_metadata_blocks` | Off-by-4 bounds check panics the scan thread and poisons the cache mutex. | open |
| CZL-004 | S2 | confirmed | dsk-clonezilla metadata_scan.rs | Unchecked shifts / underflow; BPB-driven ranges insert billions of entries. | open |
| CZL-005 | S4 | confirmed | dsk-clonezilla block_cache.rs | Unneeded `unsafe impl Send`. | open |
| FLP-001 | S2 | confirmed | dsk-floppy edsk.rs, d88.rs flat decode | Missing tracks / sectors shift every later sector (no zero-filled holes). | open |
| FLP-002 | S2 | plausible | dsk-floppy g64.rs `decode_track` | Sectors crossing the GCR wrap lost; bad-checksum sectors zeroed silently. | open |
| FLP-003 | S4 | confirmed | dsk-floppy msa.rs, dim.rs | Public encoder panics via `assert_eq!`; dead `if`. | open |
| FLX-001 | S2 | confirmed | dsk-flux flux/mod.rs `decode_disk` | Heads / cylinders from the max over stray sector IDs: the whole image layout shifts. | open |
| FLX-002 | S2 | confirmed | dsk-flux flux/mfm.rs `vote` | Voting always checks the MFM CRC: FM / Amiga sectors never recover; false clean votes on Amiga. | open |
| FLX-003 | S2 | confirmed | dsk-flux flux/mfm.rs `to_bits_pll` | PLL bit stream unbounded: a 100 KB track runs out of memory. | open |
| FLX-004 | S2 | confirmed | dsk-flux flux/mfm_emu.rs `decode_to` | Seek does not extend the temp file: truncated disk; header-sized allocations unchecked. | open |
| FLX-005 | S2 | confirmed | dsk-flux flux/kryoflux.rs `parse_stream` | u32 overflow counter overflows (panic / wrong interval). | open |
| FLX-006 | S4 | confirmed | dsk-flux mfm_emu.rs, mfm.rs | `crc16` twice; byte helpers duplicate disk_core. | open |
| IMZ-001 | S2 | confirmed | dsk-imz imz.rs `open_with_password` | Capacity from ZIP header sizes; whole entry in RAM with no cap. | open |
| TAP-001 | S3 | confirmed | dsk-tape sun2tape.rs, sun2tape_edit.rs | Tape files read whole into RAM (up to three copies). | open |
| TAP-002 | S2 | plausible | dsk-tape sun2tape.rs, sun2tape_edit.rs `build_image` | Zero-length tape files dropped on rebuild: later files renumbered. | open |
| TAP-003 | S3 | confirmed | dsk-tape tar_section.rs `TarSection::parse` | A leading `./` entry disappears from the rebuilt archive. | open |

## crates/fs-adfs, fs-hpfs, fs-apfs, fs-qnx, qnx2fs

Review by a separate agent (with two sub-forks), 2026-11-21, read-only.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| ADF-001 | S1 | confirmed | fs-adfs adfs.rs `alloc_and_write_data` | Fragment bits not rounded to whole sectors: on E-format the next file overwrites the tail of the previous one. | fixed (313cccb3) |
| ADF-002 | S1 | confirmed | adfs.rs `delete_entry`, `free_fragment` | Delete frees a whole shared fragment (siblings and parent unreadable); multi-fragment files free only the first. | fixed (313cccb3) |
| ADF-003 | S1 | confirmed | adfs.rs `remove_dir_entry` | Entry shift runs into the directory tail: every delete corrupts the parent pointer, title, name and EndMasSeq. | fixed (313cccb3) |
| ADF-004 | S1 | confirmed | adfs.rs `insert_dir_entry` | A 78th entry in a full directory overwrites the tail. | fixed (313cccb3) |
| ADF-005 | S2 | plausible | adfs.rs | Sequence numbers not bumped, check byte not recomputed, entries not sorted. | open |
| ADF-006 | S2 | confirmed | adfs.rs | Free-space chain broken on allocate and never relinked on delete. | open |
| ADF-007 | S2 | confirmed | adfs.rs `adfs_calczonecheck` | Truncated last zone underflows and panics (fsck, repair, every write). | open |
| ADF-008 | S2 | confirmed | adfs.rs `delete_entry` | D-format without FSM panics after rewriting the parent; errors fire after the entry is removed. | fixed (313cccb3) |
| ADF-009 | S2 | confirmed | adfs.rs `DiscRecord::parse` | id_len / log2bpmb / zone_spare unvalidated: shifts overflow. | open |
| ADF-010 | S2 | confirmed | adfs.rs `read_dir_block` | Capacity from on-disk root size (4 GiB). | open |
| ADF-011 | S2 | confirmed | adfs.rs `flush_zone` | Backup map zones 1..n-1 go stale on multi-zone discs. | open |
| ADF-012 | S2 | plausible | adfs.rs | Free runs / sectors not bounded by the partition; overlapping runs accepted. | open |
| ADF-013 | S3 | confirmed | adfs.rs `AdfsFsm::read` | Zone load sized by on-disk count (128 MiB); every edit rewrites all zones; quadratic copy. | open |
| ADF-014 | S3 | confirmed | adfs.rs | Map written before data / entry (leaks on failure); no duplicate-name check. | open |
| HPF-001 | S1 | confirmed | fs-hpfs hpfs.rs `fix_up_ptrs` etc. | Edit paths follow unchecked dnode pointers and write 2 KiB through an unchecked `write_sectors`. | fixed (2d2f9c3d) |
| HPF-002 | S2 | confirmed | hpfs.rs `add_dirent` etc. | Dirent walks don't check `len` / `first_free`: infinite loop, panic. | fixed (2d2f9c3d) |
| HPF-003 | S1 | confirmed | hpfs.rs `flush_bitmaps`, `open` | A bad bitmap pointer is kept: freeing writes 2 KiB to sector 0 or past the partition. | fixed (2d2f9c3d) |
| HPF-004 | S1 | confirmed | hpfs.rs split / move / delete | Not atomic: DiskFull mid-split drops half a directory; delete frees data before the dirent. | fixed (2d2f9c3d) |
| HPF-005 | S2 | confirmed | hpfs.rs `free_anode_tree` | Unguarded B+ tree parse on delete: panic, stack overflow, metadata freed. | open |
| HPF-006 | S2 | confirmed | hpfs.rs | dirband size / start unchecked: panics; overflow. | open |
| HPF-007 | S2 | confirmed | hpfs.rs `validate_hpfs_name` | Validated trimmed, stored untrimmed: oversized dirents. | open |
| HPF-008 | S2 | confirmed | hpfs.rs `create_entry` | Size `as u32` with no 2 GiB limit. | open |
| HPF-009 | S2 | confirmed | hpfs.rs `alloc_file_data` | Fragmented fallback does a full scan per size: effectively a hang. | open |
| HPF-010 | S3 | confirmed | hpfs.rs `run_fsck` | Cross-link set of every sector (~1 GB); `total - free` underflow. | open |
| HPF-011 | S2 | confirmed | hpfs.rs `open` | `bitmap_dir + s` overflow; root fnode not range-checked. | open |
| HPF-012 | S4 | confirmed | hpfs.rs `is_name_long` | Tests the dot, not the character: some long names stored as short. | open |
| HPF-013 | S4 | confirmed | hpfs.rs `alloc_dnode` | Dead branch. | open |
| APF-001 | S2 | confirmed | fs-apfs apfs.rs `open_with_passphrase` | Only volume 0 is browsable; a failing volume is skipped silently. | open |
| APF-002 | S2 | confirmed | apfs.rs `unlock_active_volume` | Only the first KEK tried: other users' passwords / the recovery key fail. | open |
| APF-003 | S2 | confirmed | apfs.rs `load_omap`, `build_catalog` | No visited set: self-pointing index nodes grow the stack Vec to ~8 GB. | open |
| APF-004 | S2 | confirmed | apfs.rs `read_block`, `stream_phys` | Physical addresses unchecked against block_count; products overflow. | open |
| APF-005 | S2 | confirmed | apfs.rs `stream_extents` | 64 GiB cap truncates larger files silently with Ok. | open |
| APF-006 | S2 | confirmed | apfs.rs `parse_kek_blob`; apfs_crypto.rs | PBKDF2 iterations from the keybag unbounded (hour-long hang). | open |
| APF-007 | S2 | confirmed | apfs.rs `der_children` | DER length add overflows on 32-bit: panic. | open |
| APF-008 | S2 | plausible | apfs.rs `stream_phys` | XTS tweak from the physical block, not the extent's crypto_id. | open |
| APF-009 | S3 | plausible | apfs.rs `build_catalog` | Whole catalog / omap flattened into HashMaps on first browse. | open |
| QNX-001 | S2 | confirmed | fs-qnx qnx4.rs `extents` | Self-linked empty extent block loops forever. | open |
| QNX-002 | S2 | confirmed | qnx4.rs `open` | Volume sized to the end of the reader (whole disk for MBR partitions). | open |
| QNX-003 | S2 | confirmed | qnx4.rs `read_data`, qnx6.rs `read_node` | Allocation by on-disk size before reading; endless hole zero-fill. | open |
| QNX-004 | S2 | confirmed | qnx6.rs `bmap` | `levels` uncapped: shift overflow. | open |
| QNX-005 | S3 | confirmed | qnx6.rs `copy_node`, `open` | Block-at-a-time I/O with repeated pointer reads; backup superblock geometry from the primary. | open |
| QN2-001 | S1 | confirmed | qnx2fs lib.rs `extents`, `mark`, `delete` | Extent spans unchecked: delete frees other files' blocks; `mark` panics; directory writes past the partition. | fixed (36314812) |
| QN2-002 | S1 | confirmed | qnx2fs lib.rs `replace`, `copy_entry` | Space check assumes full extents: a full disk loses the old file. | fixed (36314812) |
| QN2-003 | S2 | confirmed | qnx2fs lib.rs `open`, `read_chain` | Bitmap read grows by on-disk extent sizes before reading (4 GiB). | open |
| QN2-004 | S2 | confirmed | qnx2fs lib.rs `deleted_state`, `claim` | Per-block loops over 2^32 spans: billions of iterations / issue strings. | open |
| QN2-005 | S2 | confirmed | qnx2fs lib.rs `copy_entry` | Recursive copy has no depth / visited guard. | open |
| QN2-006 | S3 | confirmed | qnx2fs lib.rs `create_file`, `free_slot`, `delete` | Data allocated before the slot; spill relink before failure; delete frees before rewriting. | open |
