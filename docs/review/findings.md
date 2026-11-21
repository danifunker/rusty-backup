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
| crates/fs-mac | 42,245 | partial (write paths through extents), fixed in 0.1.11 | review not finished |
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
| FM-001 | S4 | confirmed | fs-mac lib.rs feature table; rb-fsexporter README / CHANGELOG | `fork-export` missing from the lib.rs table (still under `forks`); rb-fsexporter docs still call fs-mac-unicode GPL. | fixed (0c801287) |
| AM-001 | S2 | plausible | fs-amiga sfs.rs node and admin pointers | `blk << shifts` is never checked against `1 << (32 - shifts)`: on a volume over 2^28 blocks a container placed high gets a truncated pointer (predates the rewrite). | fixed (0c801287) |
| DOC-001 | S4 | confirmed | CLAUDE.md SFS bullet; docs/missing_features_from_regression.md F-014; license_audit.md SFS row | Says object-node tree growth is unimplemented / mirrors the driver's functions; the audit row says "four functions" and names three. | fixed (0c801287) |
| DOC-002 | S4 | confirmed | crates/dsk-apple and 19 more | Crates already on crates.io were moved to disk-core 0.3 (91ccccee) without a version bump, so the publish workflow skips them and crates.io keeps the disk-core 0.2 builds; fs-mac 0.1.11, fs-pc 0.1.7 and rb-fsexporter 0.1.4 change a public dependency's major version in a patch release. | fixed (0c801287) |
| DOC-003 | S4 | plausible | fs-mac-unicode manifest / README | Tables derived from UnicodeData.txt: the Unicode licence's notice is linked, not reproduced, and the manifest says only MIT. The use of Apple's TN1150 sample mapping has no recorded terms check. | fixed (0c801287) |
