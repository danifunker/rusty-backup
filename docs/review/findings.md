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
| DC-006 | S3 | confirmed | [filesystem.rs:89](../../crates/disk-core/src/filesystem.rs#L89) | The default `write_file_to` reads the whole file into one `Vec` (`read_file(entry, usize::MAX)`). 20 drivers rely on it. Most are floppy formats under the 3 MB rule, but CP/M (hard disks to 512 MB), HP LIF, QNX 2, Xerox Pilot and the SquashFS editor can hold files far larger, and `stream_pipe` / extract then buffer them whole. List: `grep` for `Filesystem for` impls without `fn write_file_to`. | fixed (this branch): LIF, QNX 2, Xerox Pilot and the SquashFS editor stream `write_file_to`; the drivers left on the default are floppy formats under the 3 MB rule |
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
| SQ-001 | S3 | confirmed | [squashfs_edit.rs:1](../../crates/fs-squashfs/src/squashfs_edit.rs#L1) | The editor holds the whole decompressed image in RAM (`FileContent::Bytes`) and rebuilds it into a buffer, against the streaming rule. The module doc calls lazy streaming "the follow-up"; it has no tracking entry. Peak memory is the decompressed image size. | deferred (needs a lazy FileContent that reuses unchanged blocks; a redesign of the editor) |

## src/fs

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| FS-001 | S2 | plausible | [export_selection.rs:434](../../src/fs/export_selection.rs#L434) | Zip export builds member paths from raw entry names via `archive_name()`, which rejects only an exact `.` / `..`. Classic HFS and MFS allow `/` in a name, so `a/b` becomes a nested member and `../../x` a traversal member (`zip` does not refuse `..`; the `tar` crate does, so tar export fails instead). Folder export is safe (`sanitize_filename`). Confirm by `put`-ting a file named `../x` onto an HFS image and zipping it. | fixed (f439045e) |

## src/partition, src/rbformats, src/restore

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| PR-001 | S4 | confirmed | [restore_patch.rs:579](../../src/partition/restore_patch.rs#L579) | Two X68k table patchers: `restore_patch::patch_x68k` (slot matching, repack, keeps flags) and `part_tables::x68k::patch_x68k_entries` (used by `src/rbformats/mod.rs`). The crate one lost the flags byte until PT-010. One should go. | fixed (this branch): restore_patch writes X68k entries through `patch_x68k_entries`; overflow refused, not truncated |
| PR-002 | S4 | confirmed | [vhd.rs:246](../../src/rbformats/vhd.rs#L246) | `rebuild_vhd_ebr_chain` re-implements `restore::build_restore_ebr_chain` ("same algorithm", says its own comment), with its own silent fallback when the chain can't be rebuilt. | fixed (this branch): `restore::pack_logical_partitions`; an unbuildable chain now stops VHD export too |

## src/model/convert

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| CV-001 | S3 | confirmed | [classify.rs:81](../../src/model/convert/classify.rs#L81) | A 2 MB FAT image classifies as a floppy, and `--format qcow2` / `vmdk-sparse` / `chd` (hard-disk formats) are then ignored without a word: the output is raw. `subcmd.convert.{qcow2,vmdk-sparse,chd}-reopens` fail on main since the Convert redesign (`da41d419`). Either warn when `--format` doesn't apply to a kind, or let a hard-disk format pull a floppy-sized image along. The produce step's `fmt.vhd-fixed`, `fmt.vhd-dynamic`, `fmt.qcow2`, `fmt.vmdk-sparse`, `fmt.chd` and `optical.chdcd` artifacts are missing for the same reason. | fixed (73ecadb2) |

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
| XFS-012 | S1 | plausible | edit.rs, repair.rs | No clean-log check before editing or repairing; recovery would replay over our writes. Confirm: shut down with a pending transaction, edit, mount. | fixed (this branch): edits, chmod and chown refuse a volume whose log head is not the unmount record; fsck reports it and repair replays the log first (`log_replay.rs`) |
| XFS-013 | S2 | confirmed | sb.rs `parse` | Superblock geometry unchecked: v5 with 128-byte inodes panics on a slice, large `dirblklog` shift-panics or allocates 2 GiB, `dblocks < (agcount-1)*agblocks` underflows (R2 loops ~2^64); unknown v5 incompat bits accepted. | fixed (a5dc5563) |
| XFS-014 | S2 | confirmed | xfs/mod.rs `walk_bmbt`, `collect_bmbt_blocks` | A bmbt leaf whose `rightsib` is itself loops forever and grows a Vec until OOM. | fixed (a5dc5563) |
| XFS-015 | S2 | confirmed | freespace_rebuild.rs `InUseMap::new`; inobt_repair.rs | R2 / R3 allocate a `Vec<bool>` of `dblocks` with no cap: 1 GiB for 4 TiB, abort on a hostile `dblocks`. | fixed (a5dc5563) |
| XFS-016 | S2 | confirmed | edit.rs `sf_entries_with_ftype`, `gather_dir_entries` | Short-form dir with too large a count or `di_size` past the inode panics on index in create / rename / delete. | fixed (a5dc5563) |
| XFS-017 | S2 | confirmed | xfs/mod.rs `read_symlink_target` | v5 remote symlinks include the 56-byte XSLM header in the target, and only the first block is read. | fixed (a5dc5563) |
| XFS-018 | S2 | confirmed | inode.rs | BIGTIME timestamps not decoded: mtimes wrong in listings and tar export. | fixed (a5dc5563) |
| XFS-019 | S4 | confirmed | repair.rs, edit.rs, freespace_rebuild.rs, inobt_repair.rs, inode_repair.rs, format.rs | Stale docs (mutations "Unsupported", "v4 only", "v5 gate"); public test-only `create_blank_xfs` builds a volume-sized Vec. | fixed (a5dc5563) |
| XFS-020 | S4 | confirmed | fsck.rs, inobt_repair.rs, v5_crc.rs, edit.rs | Duplicate btree walkers / `mark_*_blocks` / `stamp_*`; each allocation rebuilds the AG's whole bnobt / cntbt; edit.rs is 4,145 lines with five jobs. | fixed (this branch): edit.rs split by job into edit/; one `mark_tree_blocks`. The per-allocation free-space rebuild stays (correct, slow; changing it is not a tidy-up) |

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
| AMI-014 | S2 | confirmed | affs.rs `CompactAffsReader::new` | Uses `open` not `open_sized`: on an RDB disk the root search fails or the compact size covers the following partitions. | fixed (this branch): backup compaction and sizing pass the partition length to CompactAffsReader::new_sized |
| AMI-015 | S2 | confirmed | sfs.rs `list_directory`, `create_file` | SFS drops protection, comment and dates on read and ignores them on write; softlinks list as empty files; no volume label. | fixed (0360cf79) |
| AMI-016 | S4 | confirmed | affs.rs, sfs.rs, pfs3.rs, *_fsck.rs | Duplicates (`sync_metadata` / `flush_metadata`, `fs_type` / `fs_type_label`, `fold`, fsck helpers x3); stale comments; `create_days` read from 0x1A4 (root alteration date). | fixed (this branch): shared fsck_common (issue, bucket, BitSet, free-map diff); fs_type / sync_metadata / fold reuse their twins |

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
| MAC-016 | S2 | confirmed | hfsplus_wrapper_clone.rs `patch_mdb_in_place` | A wrapper shrink keeps the bad-block extent at the old length, past the new volume end. | fixed (this branch): the wrapper's bad-block run is resized with the volume. Confirmed on a Mac OS 8.1 wrapped volume (QEMU Quadra 800): fsck_hfs and Mac OS 8.1 Disk First Aid pass the shrunk volume |
| MAC-017 | S3 | confirmed | hfsplus_defrag.rs `emit_progress` | `eprintln!` in library code; thread-local progress sink; no `cancel_check` for whole-volume streams. | fixed (b9aecbe2) |
| MAC-018 | S3 | confirmed | mfs.rs `create_blank_mfs`; hfv.rs `build_blank_hfv` | Public builders hold the whole volume in RAM, above the 3 MB floppy exception. | fixed (this branch): `write_blank_mfs_into`, `write_blank_hfv_into`; `rb-cli new --fs mfs` streams |
| MAC-019 | S4 | confirmed | hfsplus.rs, hfsplus_defrag.rs, hfs_fsck | Three copies of `catalog_compare`; near-duplicate hardlink creators and fsck pairs. | fixed (this branch): one `catalog_compare_keys`, shared link-stub writer and link-count adjuster. The classic-HFS catalog and extents checks now share one strict offset-table check (the catalog side let equal offsets through) and the extents checks report the catalog's specific codes; the record-shape checks stay per tree, as the formats differ |
| MAC-020 | S4 | confirmed | mfs.rs, mac_roman.rs, mac_alias.rs, CLAUDE.md | Docs that no longer match (MFS "extract-only", name-length doc, map offset, GPL note, HFV limit 2047 vs 4063 MB); mac_alias shows Mac Roman as `?`. | fixed (b9aecbe2) |
| MAC-021 | S2 | confirmed | hfsplus_defrag.rs `plan_defrag_layout`, `build_target_metadata` | Defrag clone of a volume with 512 B or 1 KiB blocks (Mac OS 8.1's choice for small volumes) panicked: the volume header was assumed to fit block 0, and a small source's B-trees could be under three of the target's 4 KiB nodes. Found on the real 8.1 volume. | fixed (this branch) |
| MAC-022 | S2 | confirmed | hfsplus.rs `build_blank_hfsplus_front` | `rb-cli new volume hfsplus --block-size 512/1024` wrote volumes fsck_hfs rejects ("Invalid B-tree header"): blocks-per-node was inverted, so each 4 KiB node got one block, and the header overlapped the bitmap. | fixed (this branch) |
| MAC-023 | S1 | confirmed | src/fs/mod.rs APM compaction; src/backup/sizes.rs | A backup of HFS+ inside an HFS wrapper (APM `Apple_HFS`) compacted only the inner volume, so the backup lost the wrapper (and restored without it); shrink-to-minimum was also never offered, as the wrapped minimum was weighed against the inner trim point. | fixed (this branch): the wrapper's head and tail are kept around the compacted volume, never trimmed; the wrapped minimum is computed |

## crates/dsk-ghost

Review by a separate agent, 2026-11-21, read-only.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| GHO-001 | S1 | confirmed | gho.rs `synthesize_mbr`, `synthesize_ebrs` | Every partition after the first is put in an extended container with an EBR at `hidden - 63`: with two primaries the EBR lands in P1's last track and is served over real data; P2 is unreachable. | fixed (3612a6a6) |
| GHO-002 | S1 | confirmed | gho.rs multi-partition VBR read | Partition size taken from FAT BPB fields only: an NTFS partition gets size 0 and is dropped from the MBR; FAT12 / unknown typed 0x0C. | fixed (3612a6a6) |
| GHO-003 | S2 | confirmed | gho.rs trim / reassign / binary search | Trimmed blocks keep stale offsets in the sorted list, so reads near a partition end hit the wrong block (error or zeros). | fixed (fda1fc41) |
| GHO-004 | S2 | confirmed | gho.rs `ensure_block_cached` | A non-final block that decodes short returns Ok(0) mid-image: the output is silently truncated. | fixed (fda1fc41) |
| GHO-005 | S2 | confirmed | gho.rs `ensure_cluster_decoded` | Each cluster miss decodes the file from its start: O(n^2), effectively a hang on large files. | fixed (fda1fc41) |
| GHO-006 | S2 | confirmed | gho.rs `GhoFileContentReader::fill_next` | Uncompressed body longer than the 33,792-byte buffer panics. | fixed (fda1fc41) |
| GHO-007 | S1 | plausible | gho.rs `build_virtual_fat_image` | create errors swallowed, so files and subtrees silently go missing; layout re-derived from size can change cluster size and run out of space. | fixed (3612a6a6) |
| GHO-008 | S2 | confirmed | gho.rs `build_virtual_fat_image` | DFS without a visited set: a dir pointing back at the root nests until the volume is full, all in RAM. | fixed (3612a6a6) |
| GHO-009 | S2 | confirmed | gho.rs `gather_ghs_siblings` | Spans matched by name prefix only: a neighbouring image's spans are merged in; a relative pick can choose the wrong primary; a missing span is not detected. | fixed (fda1fc41) |
| GHO-010 | S2 | confirmed | gho.rs NTFS file-aware open | VBR `total_sectors` unchecked: a huge value aborts on allocation, products overflow. | fixed (fda1fc41) |
| GHO-011 | S2 | confirmed | gho.rs `open_ntfs_compressed_mft_only`, `extend_ntfs_compressed_index` | Inflate output unbounded (~4 GiB); oversized block shifts later offsets and wrong data is served. | fixed (fda1fc41) |
| GHO-012 | S2 | confirmed | gho.rs lazy NTFS scan; trim | I/O errors swallowed: unmapped clusters served as zeros, real errors treated as padding. | fixed (fda1fc41) |
| GHO-013 | S2 | plausible | gho.rs uncompressed multi-partition | Acknowledged-unsupported layout served as one raw stream instead of refused; later partitions shifted. | open: needs an uncompressed **sector-mode** multi-partition sample to detect the layout safely (the compressed sector-mode case is now covered by test_gho_11_sector) |
| GHO-014 | S2 | confirmed | gho.rs MBR / EBR synthesis, blocked read | Unchecked subtraction of partition geometry (hidden < 63, overlapping or unsorted) panics or yields garbage. | fixed (3612a6a6) |
| GHO-015 | S3 | confirmed | gho.rs `unsafe impl Send for GhoReader` | Unneeded `unsafe impl Send`. | fixed (fda1fc41) |
| GHO-016 | S4 | confirmed | gho.rs | Dead / duplicated paths (materialize, emit copies, four zlib loops, three Seek impls; one in-RAM volume emitter); 9,850-line file with six jobs. | fixed (this branch): gho/ split into eleven modules, public API unchanged; two zlib loops share `zlib_decode_block`. The two FAT emitters stay (they differ in what they do) |
| GHO-017 | S4 | confirmed | gho.rs headers | Stale docs (file-aware "deferred", password "errors", "SECTOR only"). | fixed (2441e66e) |
| GHO-018 | S3 | confirmed | gho.rs file-aware tree walk / FAT emit | A file-aware image's volume label (BOOT, FAT16, ...) was rebuilt as an empty file instead of the FAT label. | fixed (this branch) |

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
| B8-008 | S2 | confirmed | ti99.rs `create_file` | More than 76 cluster runs panics after data was written; `n_data as u16` truncates before the size check. | fixed (7170f90a) |
| B8-009 | S2 | confirmed | os9.rs `dir_add_entry` | Adding an entry rewrites the parent's FD from scratch, zeroing owner, dates and link count. | fixed (7170f90a) |
| B8-010 | S2 | confirmed | os9.rs `allocate_segments` | LSN 0 can be allocated when the bitmap bit is clear, overwriting the identification sector. | fixed (409017f1) |
| B8-011 | S2 | confirmed | os9.rs, qdos.rs, lif.rs reads | Allocations sized from on-disk u32 lengths (4 GiB to ~1 TB). | fixed (7170f90a) |
| B8-012 | S2 | plausible | qdos.rs `FAT_END_LO` | Chains stop at 0xFFF8+ though clusters up to 65,534 are allowed. | fixed (7170f90a) |
| B8-013 | S2 | confirmed | dragondos.rs `parse_geometry` | Track counts past the bitmap's 3,488 LSNs flip bits in the directory. | fixed (7170f90a) |
| B8-014 | S3 | confirmed | cbm.rs, qdos.rs, atari_dos.rs, dragondos.rs, rsdos.rs, trdos.rs | Em dash (U+2014) in user-visible error strings. | fixed (d6f2d9b7) |
| B8-015 | S4 | confirmed | andos.rs, qdos_mdv.rs | Errors point at moved `src/fs/` paths; andos doc says it lists an empty root. | fixed (d6f2d9b7) |
| B8-016 | S4 | confirmed | rsdos.rs, atari_dos.rs, dragondos.rs, trdos.rs | Byte-identical name codecs and sector I/O helpers. | fixed (this branch): `fs-8bit::common` |

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
| UNX-006 | S1 | confirmed | ufs.rs `free_inode_blocks`, `free_frag_run` | Freeing trusts pointers: panics past the CG, frees CG headers / inode tables, frees cross-linked blocks. | fixed (this branch): frees that overlap a CG's superblock copy, CG header or inode table (and CG 0's boot area) are refused; the synthetic test volume now reserves its metadata as newfs does |
| UNX-007 | S1 | confirmed | efs.rs directory code, efs_fsck.rs, efs_resize.rs | Indirect-extent directories (numextents > 12) treated as direct: insert overwrites an index block, listing shows empty, repair double-links. | fixed (72c94099) |
| UNX-008 | S1 | confirmed | efs_v1.rs `allocate_inode`, `dir_insert` | Dirents hold a u16 inum but inodes above 65,535 are handed out: the entry points at the wrong inode. | fixed (72c94099) |
| UNX-009 | S1 | confirmed | bfs_fsck.rs `check_inode`, `repair_bfs` | Double-indirect and indirect map blocks never claimed: repair frees them. | fixed (48ac3f30) |
| UNX-010 | S1 | confirmed | ofs_fsck.rs `claim_file`; ofs_write.rs `owned_runs` | Only the first allocation list counted: repair frees chained lists, delete leaks them. | fixed (48ac3f30) |
| UNX-011 | S1 | confirmed | bfs_write.rs `BTree::split_and_promote` | Internal split promotes the wrong key: some keys become unreachable. | fixed (f55f13c6) |
| UNX-012 | S1 | confirmed | bfs_write.rs `write_blank_bfs_into` | 65,537..65,539-block volumes put the root tree past the volume end. | fixed (f55f13c6) |
| UNX-013 | S1 | confirmed | minix.rs `write_blank_minix_into` | `firstdatazone` / `zmap_blocks` stored as u16 unchecked: large V3 volumes overlap data and inodes. | fixed (f55f13c6) |
| UNX-014 | S1 | plausible | minix.rs `parse`, alloc; minix_fsck.rs | Bitmap block counts not checked against zones / inodes: allocation repeats a zone, bit writes hit the inode table. | fixed (e9e11e89) |
| UNX-015 | S1 | plausible | ofs_write.rs `write_sectors`, `claim_slot` | Bound allows one sector past the volume; directory chain pointer can write into the bitmap. | fixed (e9e11e89) |
| UNX-016 | S1 | plausible | jfs_fsck.rs `check_connectivity`, `repair_jfs` | Directories over 4096 bytes skipped: children reported as orphans and double-linked. | fixed (e9e11e89) |
| UNX-017 | S1 | plausible | btrfs.rs `CompactBtrfsReader::new` | Boot area and superblock mirrors zeroed in a compacted backup: restored partition does not boot. | fixed (e9e11e89) |
| UNX-018 | S2 | confirmed | ufs.rs, efs.rs, efs_v1.rs orphan adoption | Children of an orphaned directory adopted twice; UFS `..` and lost+found nlink stale; efs `continue` in the wrong loop. | fixed (47957566) |
| UNX-019 | S2 | confirmed | ext.rs, btrfs.rs, jfs.rs, reiserfs.rs tree walks | No depth cap / level check: self-pointing nodes overflow the stack or loop. | fixed (267f907c) |
| UNX-020 | S2 | confirmed | ext, ufs, jfs, jfs1, btrfs, bfs, vxfs | Allocations sized from on-disk fields (symlink size 1<<40 aborts a listing). | fixed (267f907c) |
| UNX-021 | S2 | confirmed | efs.rs `open`, `inode_byte_offset` | `cgisize` / `cgfsize` / `ncg` not validated: divide by zero on listing. | fixed (72c94099) |
| UNX-022 | S2 | confirmed | ext.rs `try_insert_dir_entry` | `rec_len - actual_len` underflows (panic / wrapped rec_len written). | fixed (72c94099) |
| UNX-023 | S2 | confirmed | ext.rs `delete_entry` | Deleting an ext2/3 file over 12 blocks leaks its indirect blocks. | fixed (72c94099) |
| UNX-024 | S2 | confirmed | ext.rs `free_blocks_list` | No range / state check: underflow, panic, double-free counted. | fixed (72c94099) |
| UNX-025 | S2 | confirmed | ext.rs `open` | Shift before range check, underflow, descriptor Vec sized from disk (up to 128 GiB). | fixed (72c94099) |
| UNX-026 | S2 | confirmed | ext.rs `add_block_to_extent_inode` | `eh_entries` / `eh_max` trusted: indexes past `i_block`. | fixed (72c94099) |
| UNX-027 | S2 | confirmed | ext.rs `rename` | File type recomputed as dir / regular: symlinks, devices, fifos get the wrong type. | fixed (72c94099) |
| UNX-028 | S2 | plausible | ext_fsck.rs; ext.rs | INODE_UNINIT / `itable_unused` ignored: false errors and repairs on lazy-init volumes. | fixed (47957566) |
| UNX-029 | S2 | plausible | ext.rs `grow_ext_add_groups` | Grow ignores reserved GDT blocks and the resize inode. | fixed (ea96f43c) |
| UNX-030 | S2 | plausible | ext.rs; ext_fsck.rs | Block-bitmap checksum covers the whole block, not blocks_per_group / 8. | fixed (47957566) |
| UNX-031 | S2 | plausible | ext.rs `resize_ext_in_place` shrink | Only counts patched: padding bits, descriptor table, inode count stale. | fixed (ea96f43c) |
| UNX-032 | S2 | confirmed | ufs.rs `dir_find`, `dir_remove` | Name slices without the `namlen <= reclen - 8` check: panic on create. | fixed (4fb60164) |
| UNX-033 | S2 | confirmed | ufs.rs `create_file`, `create_directory` | Rollback runs with size 0 and frees nothing: blocks leak; `cs_ndir` off by one. | fixed (4fb60164) |
| UNX-034 | S2 | confirmed | ufs.rs `repair_ufs` | Bitmap repair adjusts nffree only: summaries end up wrong. | fixed (4fb60164) |
| UNX-035 | S2 | confirmed | ufs_fsck.rs `record_frags` | Only the first fragment of each block checked: shared fragments, link counts, cg_cs pass. | fixed (4fb60164) |
| UNX-036 | S2 | confirmed | ufs.rs | `frag * fsize` on UFS2 64-bit pointers unchecked. | fixed (4fb60164) |
| UNX-037 | S2 | plausible | ufs.rs `walk_inode_blocks` | Bounded by `di_size` only: a huge size with full triple indirects hangs. | fixed (4fb60164) |
| UNX-038 | S2 | confirmed | jfs.rs `read_fileset_inode` | pmap read per extent, not per inode: after one delete, other inodes in that extent fail to read. | fixed (b6e84a55) |
| UNX-039 | S2 | confirmed | jfs_fsck.rs; jfs.rs | lost+found inode written before an insert that then fails: each repair leaks a directory. | fixed (b6e84a55) |
| UNX-040 | S2 | confirmed | btrfs.rs `parse_btrfs_superblock` | `sys_chunk_array_size` over 3285 panics at open. | fixed (4fb60164) |
| UNX-041 | S2 | confirmed | btrfs.rs | Compressed extents read as zeros (should be Unsupported); PREALLOC returns stale bytes; short items zero-filled. | fixed (4fb60164) |
| UNX-042 | S2 | plausible | btrfs.rs `resize_btrfs_in_place` | DEV_ITEM and mirror superblocks not updated; I/O errors return Ok. | fixed (4fb60164); DEV_ITEM patch not yet run past btrfs check |
| UNX-043 | S2 | confirmed | reiserfs.rs `list_directory` | Each child re-walks every leaf: quadratic listing. | fixed (b6e84a55) |
| UNX-044 | S2 | plausible | reiserfs.rs | `s_bmap_nr` unchecked: bitmap reads past the volume; compact stream too long. | fixed (b6e84a55) |
| UNX-045 | S2 | confirmed | jfs.rs, jfs1.rs | Unchecked size multiplications. | fixed (b6e84a55) |
| UNX-046 | S2 | confirmed | bfs.rs `parse`; bfs_fsck.rs | Geometry not cross-checked: fsck loops 2^60 times. | fixed (303f658d) |
| UNX-047 | S2 | confirmed | bfs_write.rs `delete_entry` | Tree stored and runs freed before the double-indirect refusal; attribute dirs never freed. | fixed (303f658d) |
| UNX-048 | S2 | confirmed | vxfs_edit.rs | Panics on hostile input (namelen slice, underflow, maps / iau indexing). | fixed (303f658d) |
| UNX-049 | S2 | confirmed | minix.rs `create_file` | Size stored as u32 (4 GiB+1 records 1); a failure leaks the inode and zones. | fixed (303f658d) |
| UNX-050 | S2 | confirmed | minix.rs, vxfs_edit.rs `rename` | Old dirent removed before the insert: DiskFull leaves the inode nameless. | fixed (303f658d) |
| UNX-051 | S2 | confirmed | efs_v1.rs `read_dir_entries` vs efs_v1_fsck.rs | Reader drops out-of-range dirents, so fsck's check can never fire. | fixed (303f658d) |
| UNX-052 | S2 | plausible | unix_common/compact.rs `MappedBlocks` and callers | One u64 per allocated block (~2 GB for 1 TB); ext's tables add more: breaks the streaming rule. | fixed (ea96f43c) |
| UNX-053 | S3 | confirmed | unix_common/compact.rs `load_block` | One seek + read per block, not 64 KiB-1 MiB runs. | fixed (ea96f43c) |
| UNX-054 | S3 | confirmed | ufs.rs `open` | `total_frags` not compared with `ncg*fpg` or the slice (generic form of SD-001). | fixed (5694446c) |
| UNX-055 | S3 | confirmed | btrfs.rs `list_directory` | `Err(_) => continue` drops children silently. | fixed (4fb60164) |
| UNX-056 | S3 | confirmed | jfs.rs, jfs_fsck.rs | `≥`, `⇒`, `—` in user-visible messages. | fixed (5694446c) |
| UNX-057 | S3 | confirmed | bfs_write.rs, ofs_write.rs, ofs.rs | Whole-source `read_to_end`; OFS buffers a whole extent. | fixed (58e7a44e) |
| UNX-058 | S4 | confirmed | bfs_write.rs, minix.rs, vxfs.rs | Each allocation rereads the whole bitmap; VxFS bmap rereads indirects per block. | fixed (this branch): BFS and minix cache their bitmaps (BFS writes back only changed blocks; minix resumes each search past the last zone); VxFS keeps the last indirect extent it read |
| UNX-059 | S4 | confirmed | efs_fsck.rs, efs_v1_fsck.rs, jfs_fsck.rs, ufs_fsck.rs (+ fs-xfs) | Five copies of the fsck `Builder`; belongs in `disk_core::fsck`. | fixed (this branch): `disk_core::fsck::FsckBuilder` |
| UNX-060 | S4 | confirmed | ufs.rs, efs.rs | Full 27-field `FileEntry` literals (DC-008 class). | fixed (this branch): `FileEntry::bare` is public; EFS, UFS, Minix, XFS, HFS, HFS+, ProDOS, NTFS and exFAT fill only their own fields |
| UNX-061 | S4 | confirmed | lib.rs, ufs.rs, efs_resize.rs, ext_format.rs | Stale docs (ofs GPL note, UFS scope, shrink hard-link claim, batch-builder caller). | fixed (705602dc) |

## crates/fs-apple2

Review by a separate agent, 2026-11-21, read-only.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| AP2-001 | S1 | confirmed | apple_dos.rs `read_file_bytes` + `read_file` | Type-B header stripped twice: some payloads read back as 1 byte. | fixed (39c02adb) |
| AP2-002 | S1 | confirmed | apple_dos.rs `create_file` | B header length `as u16`: files over 64 KiB read back truncated. | fixed (39c02adb) |
| AP2-003 | S1 | confirmed | prodos.rs `create_file` | Exactly 16 MiB accepted; EOF stored as 0, file reads empty, blocks stay allocated. | fixed (39c02adb) |
| AP2-004 | S2 | confirmed | prodos.rs directory-chain walks | No cycle guard: a self-linked dir block hangs list / create / delete. | fixed (0735476c) |
| AP2-005 | S2 | confirmed | prodos.rs `free_file_blocks` | Index pointers not range-checked before freeing: panic, or the volume directory freed. | fixed (0735476c) |
| AP2-006 | S2 | confirmed | prodos.rs read / write_to / delete | Storage type 5 (forked) treated as seedling: extract returns the fork descriptor; delete leaks both forks. | fixed (0735476c) |
| AP2-007 | S2 | plausible | prodos.rs `resize_prodos_in_place` | Shrink guard uses the used-block count, not the highest used block; stale buffer on a failed read. | fixed (0735476c) |
| AP2-008 | S2 | plausible | prodos.rs subdirectory builders | Subdir metadata off-spec (0-based parent entry number, EOF 0, counts not updated, volume dir grows past 4 blocks). | fixed (0735476c) |
| AP2-009 | S2 | plausible | prodos.rs `allocate_block`; ucsd.rs `find_gap` | Block 0 allocatable; volume size not checked against the partition. | fixed (0735476c) |
| AP2-010 | S2 | confirmed | apple_dos.rs `read_file_bytes` | Sparse (0,0) T/S pairs skipped instead of zero-filled: records shift. | fixed (0735476c) |
| AP2-011 | S2 | confirmed | apple_dos.rs `alloc_one_sector`, `free_sector_count` | Track 34 counted free but never allocated: creates fail half-way. | fixed (0735476c) |
| AP2-012 | S2 | confirmed | apple_dos.rs `bitmap_mark_free` | Freed T/S pairs not range-checked: panic. | fixed (0735476c) |
| AP2-013 | S2 | confirmed | apple_dos.rs `encode_apple_name` | Trailing spaces accepted, trimmed on read: failed create, then duplicates. | fixed (0735476c) |
| AP2-014 | S2 | confirmed | ucsd.rs `byte_size`, `read_file` | `last_byte` not clamped: reads spill into following files. | fixed (0735476c) |
| AP2-015 | S2 | confirmed | ucsd.rs `repair_ucsd` | BadName repair drops the entry and frees the file's blocks. | fixed (0735476c) |
| AP2-016 | S2 | confirmed | ucsd.rs `create_file` | Block count `as u16` wraps at 32 MiB; slice panics. | fixed (0735476c) |
| AP2-017 | S3 | confirmed | prodos.rs, apple_dos.rs, ucsd.rs, sector_order.rs | Em dash / multiplication sign in user-visible errors. | fixed (d3e79acf) |
| AP2-018 | S3 | confirmed | prodos_types.rs `parse_json` | Format crate reads the app config dir; a bad override file is silently ignored. | fixed (d3e79acf) |
| AP2-019 | S4 | confirmed | apple_dos.rs, prodos.rs | Catalog walk copied 5 times; read_* duplicate write_*_to; fork walkers duplicated. | fixed (this branch): Apple DOS `walk_catalog`; ProDOS `walk_fork_blocks` / `extended_forks`, read_* through write_*_to |

## crates/fs-xerox

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| XRX-001 | S1 | confirmed | alto/write.rs `build_disk` | VDAs and freePages stored as u16: Trident packs always fail fsck and point at wrong pages past 65535. | fixed (0c22507d): rebuild of packs over 65535 pages refused |
| XRX-002 | S1 | plausible | alto/write.rs `extract_files`, `build_disk` | Every edit / repair rebuilds from SysDir only: sub-directory files dropped, boot page not copied. | fixed (4599565b) |
| XRX-003 | S2 | confirmed | alto/bfs.rs `sync_metadata`, `repair`; src/model/browse_session.rs | Non-atomic save, always PDI: a Salto `.dsk` overwritten as PDI. | fixed (42e7e9fa) |
| XRX-004 | S2 | confirmed | alto/pdi.rs `read_header` | Label / data sizes unchecked against the family: panic on open; product overflow. | fixed (42e7e9fa) |
| XRX-005 | S2 | confirmed | alto/copydisk.rs `read` | Allocation from u16 geometry before reading: OOM / panic. | fixed (42e7e9fa) |
| XRX-006 | S2 | confirmed | alto/zdisk.rs `inflate` | Uncapped inflate: zlib bomb. | fixed (42e7e9fa) |
| XRX-007 | S2 | confirmed | alto/pilot.rs `leader_name` | Property read past the 512-byte page: panic on open. | fixed (42e7e9fa) |
| XRX-008 | S2 | confirmed | alto/pilot.rs `parse_client_directory` | Reads a word past the page. | fixed (42e7e9fa) |
| XRX-009 | S2 | confirmed | alto/pilot.rs volume / file builders | Loops over on-disk u32 counts unclamped: long hangs, huge run tables. | fixed (42e7e9fa) |
| XRX-010 | S2 | confirmed | alto/pilot.rs `rebuild_vam`, `delete_file` | Direct sector indexing from descriptor sizes: panic on add / delete. | fixed (42e7e9fa) |
| XRX-011 | S1 | confirmed | alto/pilot.rs `add_file` | Runs stored `as u16`: files over 32 MiB read back truncated. | fixed (0c22507d) |

## crates/arc-mac

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| ARC-001 | S2 | confirmed | stuffit_huffman.rs, stuffit_lzah.rs, stuffit_lzh.rs; arsenic, compactpro reserves | Decoders don't stop at end of input and reserve the header length up front: 4 GiB bombs. | fixed (a6f17e49) |
| ARC-002 | S2 | confirmed | stuffit_lzw.rs `decompress`, `BitCursor::skip` | Pad skip after CLEAR overruns: panic (reachable from dsk-tape `.Z`). | fixed (a6f17e49) |
| ARC-003 | S2 | confirmed | stuffit_lzw.rs | `.Z` max bits not capped at 16; output unbounded. | fixed (a6f17e49) |
| ARC-004 | S2 | confirmed | maczip.rs `inflate` | Inflate uncapped, size checked after. | fixed (a6f17e49) |
| ARC-005 | S2 | plausible | mar.rs, compactpro.rs, stuffit.rs | Unbounded folder nesting recursion; per-level path clones. | fixed (a6f17e49) |
| ARC-006 | S2 | confirmed | extract.rs `write_entry` | Entries mapping to the same host path overwrite each other and existing files. | fixed (a6f17e49) |
| ARC-007 | S2 | confirmed | extract.rs `write_entry` | MacBinary / AppleDouble output drops dates and Finder flags. | fixed (a6f17e49) |
| ARC-008 | S2 | plausible | stuffit.rs, stuffit5.rs | usize overflow on 32-bit targets: infinite loop / panic. | fixed (a6f17e49) |
| ARC-009 | S3 | confirmed | stuffit.rs, mar.rs writers | Sizes / counts truncated and names cut silently, returning Ok. | fixed (6ca2f3b3, 71e8b2a5): sizes and counts refused past the format; long names cut with a warning, since refusing broke whole-disc exports |
| ARC-010 | S3 | confirmed | extract.rs, mar.rs, maczip.rs | Archives held whole in RAM, several copies. | deferred (needs the archive readers to work from a Read + Seek source; a redesign) |
| ARC-011 | S3 | confirmed | maczip.rs, stuffit5.rs `parse` | Unreadable forks dropped silently; ZIP CRC unchecked; truncated SIT5 unreported. | fixed (6ca2f3b3) |
| ARC-012 | S2 | plausible | stuffit_arsenic.rs `read_block` | Zero-run counters overflow (panic / spin). | fixed (a6f17e49) |
| ARC-013 | S4 | confirmed | arsenic, lzah, lzh, detect.rs | Duplicate bit readers / PrefixCode / three CRC-32s; BinHex decoded twice. | fixed (this branch): shared `BitReaderBE`, `prefix_code`, `crc32`; BinHex decoded once (MAR's CRC is another polynomial) |

## crates/dsk-apple

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| DAP-001 | S2 | confirmed | woz.rs data-field decoders | WOZ sector checksums never checked (3.5" eats a checksum nibble as data); missing 5.25" sectors zeroed silently. | fixed (3de2eee2) |
| DAP-002 | S2 | plausible | woz.rs | `bit_count * 2` from the TRK descriptor overflows. | fixed (3de2eee2) |
| DAP-003 | S2 | plausible | dc42.rs `encode_dc42_with_tags`; dart.rs | Tag checksum range, format byte and disk_format look off-spec; tagged DARTs all named "Lisa". | fixed (3de2eee2) |
| DAP-004 | S4 | confirmed | dc42.rs | Disk names UTF-8 instead of Mac Roman. | fixed (27379918) |
| DAP-005 | S4 | plausible | woz_write.rs `FORMAT_35_SSDD` | 400K format byte disagrees with dc42.rs; 3.5" WOZ marked Apple ][. | fixed (27379918) |
| DAP-006 | S4 | confirmed | woz.rs, moof.rs | Duplicate readers, extra TRKS copy, WOZ CRC unchecked, dead code. | fixed (this branch): WOZ CRC checked (27379918); TRKS kept as a range, not copied; dead trks_chunk_offset removed; both readers wrap a Cursor |

## crates/fs-squashfs, crates/dsk-vm, crates/fs-tar

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| SQ-002 | S2 | confirmed | squashfs.rs `decompress`, `decompress_zstd` | XZ / LZMA / zstd output uncapped: GiB-scale bombs. | fixed (32313343) |
| SQ-003 | S2 | confirmed | squashfs.rs `Superblock::parse` | `block_log` unbounded: shift panic, 2 GiB blocks. | fixed (32313343) |
| SQ-004 | S2 | confirmed | squashfs.rs `build_node`; squashfs_verify.rs | No directory-cycle guard: stack overflow / endless verify. | fixed (32313343) |
| SQ-005 | S2 | confirmed | squashfs.rs `read_metadata_span` and table loads | Capacities taken from on-disk u32s (4 GiB / 64 GiB). | fixed (32313343) |
| SQ-006 | S3 | confirmed | squashfs_write.rs `write_node`; `read_build_tree` | Dir `parent` written wrong; hard links duplicated on rebuild. | fixed (0309cc99) |
| SQ-008 | S3 | confirmed | squashfs_edit.rs `read_file` | Hard-linked files (`FileContent::Shared`) failed to read in the editor ("not a regular file"). | fixed (this branch) |
| SQ-007 | S4 | confirmed | squashfs.rs, squashfs_write.rs | Comment contradicts code; whole-Vec clone per file; near-duplicate table loaders. | fixed (this branch): flag comment corrected; fragment / ID / xattr-ID tables share one loader; per-file clone gone (21d43797) |
| DVM-001 | S1 | plausible | qcow2.rs `open`, `ensure_free_cluster_index` | Edits accepted on dirty / corrupt images; with lazy refcounts live clusters are reused. | fixed (3612a6a6) |
| DVM-002 | S2 | confirmed | qcow2.rs `export_qcow2` | One-cluster refcount table overflows into the first L2 table at small cluster sizes (latent). | fixed (f8e6b89b) |
| DVM-003 | S1 | confirmed | vmdk_sparse.rs `export_vmdk_sparse` | Host offset `as u32` sectors: disks past 2 TiB written corrupt. | fixed (3612a6a6) |
| DVM-004 | S2 | confirmed | vmdk.rs descriptor parse / `open_with` | Absolute or `..` extent paths opened read-write: an edit can write into an arbitrary host file. | fixed (f8e6b89b) |
| DVM-005 | S2 | confirmed | vhd.rs `write`, `allocate_block` | Unchecked BAT index, 4 GiB block buffers, offset overflow, unaligned file length shifts data. | fixed (f8e6b89b) |
| DVM-006 | S2 | confirmed | vmdk_sparse.rs header parse | Grain size / GT entries unbounded: 8 GiB / 1 TiB allocations. | fixed (f8e6b89b) |
| DVM-007 | S2 | plausible | vmdk.rs `read_descriptor` | Only 64 KiB of descriptor parsed: many-extent disks fail or silently shorten. | fixed (f8e6b89b) |
| DVM-008 | S2 | plausible | qcow2.rs `open`, `allocate_cluster` | Unaligned file length gives unaligned new clusters and wrong refcounts. | fixed (f8e6b89b) |
| DVM-009 | S4 | confirmed | vmdk.rs, lib.rs, qemu_img_test.rs | Test-only public fn; helpers duplicated with src/rbformats. | fixed (this branch): is_all_zeros / write_zeros live in disk_core::zeros, used by dsk-vm and src/rbformats |
| TAR-001 | S1 | plausible | tar_import.rs `NormalizedTar::fill` | Data blocks counted from the ustar size only: pax sizes / GNU sparse headers make data be rewritten as headers (silent corruption). | fixed (8126d65f) |
| TAR-002 | S2 | confirmed | tar_import.rs; zip_import.rs `plan` | `skip_appledouble=false` ignored: `._` files always consumed; preflight disagrees. | fixed (69648efe) |
| TAR-003 | S2 | confirmed | mem_archive.rs | Small compressed archives inflated fully into RAM uncapped; indexed path fills disk; zip capacity from declared size. | fixed (69648efe) |
| TAR-004 | S2 | plausible | tar_import.rs `open_tar_stream` | Multi-member .tar.gz imports only the first member. | fixed (69648efe) |
| TAR-005 | S2 | plausible | tar_export.rs `walk` | Export recursion has no depth / visited guard. | fixed (69648efe) |
| TAR-006 | S4 | confirmed | import_sink.rs, tar_export.rs, mem_archive.rs, zstd_compat.rs, zip_import.rs | Stats merge drops a field; stale docs; pure-zstd can't browse .tar.zst; duplicate date code. | fixed (this branch): zip dates use disk_core's DOS conversion; tar_export's mtime doc corrected; stats merge and pure-zstd browse fixed earlier |
| TAR-007 | S4 | confirmed | zip_compat.rs | `cargo build -p fs-tar --all-features` fails: `ZipFile<'_>` lacks the reader generic newer `zip` versions take. CI never builds that feature mix. dsk-imz had the same shim and failed the same way. | fixed (this branch): both shims dispatch on what `name()` returns instead of a feature, and CI checks every crate with `--all-features` |

## crates/fs-cpm

Review by a separate agent, 2026-11-21, read-only.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| CPM-001 | S1 | confirmed | cpm.rs `create_file`, `free_blocks`, `is_active` | Slots with user >= 16 (CP/M 3 label / SFCB / password, ZSDOS users 16-31) are treated as free: new files overwrite them and their blocks are reallocated. | fixed (13054e1c) |
| CPM-002 | S2 | confirmed | cpm.rs `analyze`, `run_repair` | fsck calls user >= 16 entries invalid and repair deletes them (password entries, ZSDOS files). | fixed (6cd952f5) |
| CPM-003 | S2 | confirmed | cpm.rs `encode_dir_entry`, `dir_write_back` | Every write-back re-encodes all slots lossily: SFCB dates, label mode flags and f1'-f4' attributes are lost. | fixed (6cd952f5) |
| CPM-004 | S2 | confirmed | cpm.rs `collect_files`, `read_group` | EXM ignored: on exm > 0 presets (EINSTEIN, SVI328, MULTICOMP) files over 16 KB list short and read truncated. | fixed (6cd952f5) |
| CPM-005 | S1 | confirmed | cpm.rs `create_file` | Writer ignores EXM too: real CP/M sees a different file than we wrote. | fixed (this branch): the writer lays entries out per EXM (extent byte = last logical extent, record count of that one); the refusal is gone |
| CPM-006 | S1 | confirmed | cpm_diskdefs.rs `ZX_PLUS3`, `SVI328_CPM` | Data area runs past the disk geometry: allocations and blank images go past the image end. | fixed (13054e1c): allocation clamped to the blocks the image holds; preset values left as they are |
| CPM-007 | S2 | confirmed | cpm_diskdefs.rs `ALTAIR_CF`, `SVI328_CPM`; cpm.rs | Directory size from al0/al1 disagrees with drm: ALTAIR_CF creates always fail at sync after writing data. | fixed (6cd952f5) |
| CPM-008 | S1 | confirmed | cpm.rs list / find / delete / rename | User number dropped from name handling: same-named files in two users collide; delete removes both. | fixed (13054e1c) |
| CPM-009 | S2 | confirmed | cpm.rs `create_file` | Empty files never get an entry; a mid-loop "directory full" leaves a truncated file to be synced. | fixed (6cd952f5) |
| CPM-010 | S2 | plausible | cpm_diskdefs.rs `ALTAIR_8IN`; cpm.rs | No sector skew: ibm-3740 images (skew 6) read garbled. | fixed (this branch): new `ibm_3740` preset reads and writes through the skew-6 table; `altair_8in` stays sequential. Tested on CP/M UG volume 54 (agn453/CPM-UG-Disks, Unlicense) |
| CPM-011 | S3 | confirmed | cpm.rs `validate_cpm_name` | Wildcard / separator characters accepted in names. | fixed (9bd44acf) |
| CPM-012 | S3 | confirmed | cpm.rs | Non-ASCII glyphs in user-visible errors. | fixed (9bd44acf) |
| CPM-013 | S3 | confirmed | cpm.rs `read_file`, `create_file` | Whole-file buffering (DC-006 for this crate; largest preset 4 MiB). | fixed (this branch): create_file streams into its blocks and write_file_to streams out, one block at a time; read_file fills only up to max_bytes |
| CPM-014 | S4 | confirmed | cpm.rs, cpm_diskdefs.rs | Dead locals and unread DPB fields; `tracks` documented as a bounds check but unused. | fixed (this branch): dead locals gone; tracks documented; cks / psh / phm stay as descriptive preset data |

## crates/dsk-dmg, dsk-clonezilla, dsk-floppy, dsk-flux, dsk-imz, dsk-tape

Review by a separate agent, 2026-11-21, read-only.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| DMG-001 | S2 | confirmed | dsk-dmg dmg.rs `detect_dmg`, `ensure_cached`, `parse_mish` | XML length, pack / unpack sizes and zero blocks sized from the header unchecked: OOM; offset product overflows. | fixed (70f1b62d) |
| DMG-002 | S2 | confirmed | dsk-dmg dmg.rs `Read for DmgReader`, `copy_match` | A short chunk returns Ok(0) mid-disk (silent truncation); bad ADC distances become zeros. | fixed (70f1b62d) |
| DMG-003 | S2 | confirmed | dsk-dmg dmg_crypto.rs `CryptoContext::open` | `wrap_key_bits` slice panics; offset overflow; PBKDF2 iterations uncapped. | fixed (70f1b62d) |
| DMG-004 | S2 | confirmed | dsk-dmg sparseimage.rs `detect_sparseimage` | Band table sized from header values: 32 GiB from a 4 KiB file. | fixed (70f1b62d) |
| DMG-005 | S2 | confirmed | dsk-dmg ndif.rs `reconstruct_to` | Capacity from an unchecked entry count (~128 GiB). | fixed (70f1b62d) |
| CZL-001 | S2 | confirmed | dsk-clonezilla partclone.rs `parse_header`, `parse_bitmap` | `block_size` / `total_blocks` unvalidated: OOM, a block size of 0 hangs, overflow. | fixed (64152ec1) |
| CZL-002 | S2 | plausible | dsk-clonezilla partclone.rs `parse_header` | v1 images likely rejected by the magic check; endianness field ignored. | fixed (this branch): v1 (partclone 0.2.x, before ~2018) kept unsupported but named in the error; other format versions and big-endian images refused. Tested on a real v1 header from partclone 0.2.86 |
| CZL-003 | S2 | confirmed | dsk-clonezilla metadata_scan.rs `identify_ext_metadata_blocks` | Off-by-4 bounds check panics the scan thread and poisons the cache mutex. | fixed (64152ec1) |
| CZL-004 | S2 | confirmed | dsk-clonezilla metadata_scan.rs | Unchecked shifts / underflow; BPB-driven ranges insert billions of entries. | fixed (64152ec1) |
| CZL-005 | S4 | confirmed | dsk-clonezilla block_cache.rs | Unneeded `unsafe impl Send`. | fixed (f7ecbb34) |
| FLP-001 | S2 | confirmed | dsk-floppy edsk.rs, d88.rs flat decode | Missing tracks / sectors shift every later sector (no zero-filled holes). | fixed (1c1db841) |
| FLP-002 | S2 | plausible | dsk-floppy g64.rs `decode_track` | Sectors crossing the GCR wrap lost; bad-checksum sectors zeroed silently. | fixed (1c1db841) |
| FLP-003 | S4 | confirmed | dsk-floppy msa.rs, dim.rs | Public encoder panics via `assert_eq!`; dead `if`. | fixed (c327966a) |
| FLX-001 | S2 | confirmed | dsk-flux flux/mod.rs `decode_disk` | Heads / cylinders from the max over stray sector IDs: the whole image layout shifts. | fixed (50641b3b) |
| FLX-002 | S2 | confirmed | dsk-flux flux/mfm.rs `vote` | Voting always checks the MFM CRC: FM / Amiga sectors never recover; false clean votes on Amiga. | fixed (50641b3b) |
| FLX-003 | S2 | confirmed | dsk-flux flux/mfm.rs `to_bits_pll` | PLL bit stream unbounded: a 100 KB track runs out of memory. | fixed (50641b3b) |
| FLX-004 | S2 | confirmed | dsk-flux flux/mfm_emu.rs `decode_to` | Seek does not extend the temp file: truncated disk; header-sized allocations unchecked. | fixed (50641b3b) |
| FLX-005 | S2 | confirmed | dsk-flux flux/kryoflux.rs `parse_stream` | u32 overflow counter overflows (panic / wrong interval). | fixed (50641b3b) |
| FLX-006 | S4 | confirmed | dsk-flux mfm_emu.rs, mfm.rs | `crc16` twice; byte helpers duplicate disk_core. | fixed (18396b2a) |
| IMZ-001 | S2 | confirmed | dsk-imz imz.rs `open_with_password` | Capacity from ZIP header sizes; whole entry in RAM with no cap. | fixed (52f2afa6) |
| TAP-001 | S3 | confirmed | dsk-tape sun2tape.rs, sun2tape_edit.rs | Tape files read whole into RAM (up to three copies). | fixed (this branch): tape files spooled to temp files; streaming .Z codec in arc-mac; `TarSection` over file ranges |
| TAP-002 | S2 | plausible | dsk-tape sun2tape.rs, sun2tape_edit.rs `build_image` | Zero-length tape files dropped on rebuild: later files renumbered. | fixed (fe41fa48) |
| TAP-003 | S3 | confirmed | dsk-tape tar_section.rs `TarSection::parse` | A leading `./` entry disappears from the rebuilt archive. | fixed (06523f71) |

## crates/fs-adfs, fs-hpfs, fs-apfs, fs-qnx, qnx2fs

Review by a separate agent (with two sub-forks), 2026-11-21, read-only.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| ADF-001 | S1 | confirmed | fs-adfs adfs.rs `alloc_and_write_data` | Fragment bits not rounded to whole sectors: on E-format the next file overwrites the tail of the previous one. | fixed (313cccb3) |
| ADF-002 | S1 | confirmed | adfs.rs `delete_entry`, `free_fragment` | Delete frees a whole shared fragment (siblings and parent unreadable); multi-fragment files free only the first. | fixed (313cccb3) |
| ADF-003 | S1 | confirmed | adfs.rs `remove_dir_entry` | Entry shift runs into the directory tail: every delete corrupts the parent pointer, title, name and EndMasSeq. | fixed (313cccb3) |
| ADF-004 | S1 | confirmed | adfs.rs `insert_dir_entry` | A 78th entry in a full directory overwrites the tail. | fixed (313cccb3) |
| ADF-005 | S2 | plausible | adfs.rs | Sequence numbers not bumped, check byte not recomputed, entries not sorted. | fixed (a1f98e4a); check byte not yet compared with RISC OS |
| ADF-006 | S2 | confirmed | adfs.rs | Free-space chain broken on allocate and never relinked on delete. | fixed (a1f98e4a) |
| ADF-007 | S2 | confirmed | adfs.rs `adfs_calczonecheck` | Truncated last zone underflows and panics (fsck, repair, every write). | fixed (a1f98e4a) |
| ADF-008 | S2 | confirmed | adfs.rs `delete_entry` | D-format without FSM panics after rewriting the parent; errors fire after the entry is removed. | fixed (313cccb3) |
| ADF-009 | S2 | confirmed | adfs.rs `DiscRecord::parse` | id_len / log2bpmb / zone_spare unvalidated: shifts overflow. | fixed (a1f98e4a) |
| ADF-010 | S2 | confirmed | adfs.rs `read_dir_block` | Capacity from on-disk root size (4 GiB). | fixed (a1f98e4a) |
| ADF-011 | S2 | confirmed | adfs.rs `flush_zone` | Backup map zones 1..n-1 go stale on multi-zone discs. | fixed (a1f98e4a) |
| ADF-012 | S2 | plausible | adfs.rs | Free runs / sectors not bounded by the partition; overlapping runs accepted. | fixed (a1f98e4a) |
| ADF-013 | S3 | confirmed | adfs.rs `AdfsFsm::read` | Zone load sized by on-disk count (128 MiB); every edit rewrites all zones; quadratic copy. | fixed (a1f98e4a) |
| ADF-014 | S3 | confirmed | adfs.rs | Map written before data / entry (leaks on failure); no duplicate-name check. | fixed (a1f98e4a) |
| ADF-015 | S3 | confirmed | adfs.rs `run_fsck`, `delete_entry` | An empty file stored with disc address 0 (as RISC OS writes it) was reported as a dangling reference, and deleting it was refused as a reserved fragment. On old-map discs the refusal came after the entry was already removed. Seen on a real Archimedes demo disc. | fixed (this branch) |
| HPF-001 | S1 | confirmed | fs-hpfs hpfs.rs `fix_up_ptrs` etc. | Edit paths follow unchecked dnode pointers and write 2 KiB through an unchecked `write_sectors`. | fixed (2d2f9c3d) |
| HPF-002 | S2 | confirmed | hpfs.rs `add_dirent` etc. | Dirent walks don't check `len` / `first_free`: infinite loop, panic. | fixed (2d2f9c3d) |
| HPF-003 | S1 | confirmed | hpfs.rs `flush_bitmaps`, `open` | A bad bitmap pointer is kept: freeing writes 2 KiB to sector 0 or past the partition. | fixed (2d2f9c3d) |
| HPF-004 | S1 | confirmed | hpfs.rs split / move / delete | Not atomic: DiskFull mid-split drops half a directory; delete frees data before the dirent. | fixed (2d2f9c3d) |
| HPF-005 | S2 | confirmed | hpfs.rs `free_anode_tree` | Unguarded B+ tree parse on delete: panic, stack overflow, metadata freed. | fixed (9f89a11a) |
| HPF-006 | S2 | confirmed | hpfs.rs | dirband size / start unchecked: panics; overflow. | fixed (9f89a11a) |
| HPF-007 | S2 | confirmed | hpfs.rs `validate_hpfs_name` | Validated trimmed, stored untrimmed: oversized dirents. | fixed (9f89a11a) |
| HPF-008 | S2 | confirmed | hpfs.rs `create_entry` | Size `as u32` with no 2 GiB limit. | fixed (9f89a11a) |
| HPF-009 | S2 | confirmed | hpfs.rs `alloc_file_data` | Fragmented fallback does a full scan per size: effectively a hang. | fixed (9f89a11a) |
| HPF-010 | S3 | confirmed | hpfs.rs `run_fsck` | Cross-link set of every sector (~1 GB); `total - free` underflow. | fixed (9f89a11a) |
| HPF-011 | S2 | confirmed | hpfs.rs `open` | `bitmap_dir + s` overflow; root fnode not range-checked. | fixed (9f89a11a) |
| HPF-012 | S4 | confirmed | hpfs.rs `is_name_long` | Tests the dot, not the character: some long names stored as short. | fixed (9f89a11a) |
| HPF-013 | S4 | confirmed | hpfs.rs `alloc_dnode` | Dead branch. | fixed (9f89a11a) |
| APF-001 | S2 | confirmed | fs-apfs apfs.rs `open_with_passphrase` | Only volume 0 is browsable; a failing volume is skipped silently. | fixed (this branch): a multi-volume container's root lists one folder per volume; a password unlocks every encrypted volume it opens |
| APF-002 | S2 | confirmed | apfs.rs `unlock_active_volume` | Only the first KEK tried: other users' passwords / the recovery key fail. | fixed (42f7a075) |
| APF-003 | S2 | confirmed | apfs.rs `load_omap`, `build_catalog` | No visited set: self-pointing index nodes grow the stack Vec to ~8 GB. | fixed (42f7a075) |
| APF-004 | S2 | confirmed | apfs.rs `read_block`, `stream_phys` | Physical addresses unchecked against block_count; products overflow. | fixed (42f7a075) |
| APF-005 | S2 | confirmed | apfs.rs `stream_extents` | 64 GiB cap truncates larger files silently with Ok. | fixed (42f7a075) |
| APF-006 | S2 | confirmed | apfs.rs `parse_kek_blob`; apfs_crypto.rs | PBKDF2 iterations from the keybag unbounded (hour-long hang). | fixed (42f7a075) |
| APF-007 | S2 | confirmed | apfs.rs `der_children` | DER length add overflows on 32-bit: panic. | fixed (42f7a075) |
| APF-008 | S2 | plausible | apfs.rs `stream_phys` | XTS tweak from the physical block, not the extent's crypto_id. | wontfix (not reproducible): FileVault volumes filled after encryption and converted in place by `diskutil apfs encryptVolume` both decrypt with the physical-sector tweak; crypto_id never differed |
| APF-009 | S3 | plausible | apfs.rs `build_catalog` | Whole catalog / omap flattened into HashMaps on first browse. | deferred (needs a lazy B-tree walk in place of the flattened maps; a redesign) |
| QNX-001 | S2 | confirmed | fs-qnx qnx4.rs `extents` | Self-linked empty extent block loops forever. | fixed (172f817a) |
| QNX-002 | S2 | confirmed | qnx4.rs `open` | Volume sized to the end of the reader (whole disk for MBR partitions). | fixed (172f817a) |
| QNX-003 | S2 | confirmed | qnx4.rs `read_data`, qnx6.rs `read_node` | Allocation by on-disk size before reading; endless hole zero-fill. | fixed (172f817a) |
| QNX-004 | S2 | confirmed | qnx6.rs `bmap` | `levels` uncapped: shift overflow. | fixed (172f817a) |
| QNX-005 | S3 | confirmed | qnx6.rs `copy_node`, `open` | Block-at-a-time I/O with repeated pointer reads; backup superblock geometry from the primary. | fixed (172f817a): I/O; backup-superblock geometry still from the primary |
| QN2-001 | S1 | confirmed | qnx2fs lib.rs `extents`, `mark`, `delete` | Extent spans unchecked: delete frees other files' blocks; `mark` panics; directory writes past the partition. | fixed (36314812) |
| QN2-002 | S1 | confirmed | qnx2fs lib.rs `replace`, `copy_entry` | Space check assumes full extents: a full disk loses the old file. | fixed (36314812) |
| QN2-003 | S2 | confirmed | qnx2fs lib.rs `open`, `read_chain` | Bitmap read grows by on-disk extent sizes before reading (4 GiB). | fixed (1dbd1562) |
| QN2-004 | S2 | confirmed | qnx2fs lib.rs `deleted_state`, `claim` | Per-block loops over 2^32 spans: billions of iterations / issue strings. | fixed (1dbd1562) |
| QN2-005 | S2 | confirmed | qnx2fs lib.rs `copy_entry` | Recursive copy has no depth / visited guard. | fixed (1dbd1562) |
| QN2-006 | S3 | confirmed | qnx2fs lib.rs `create_file`, `free_slot`, `delete` | Data allocated before the slot; spill relink before failure; delete frees before rewriting. | fixed (1dbd1562) |

---

# src/ review (branch `review-src`, started 2026-11-25)

Slices, write paths first: (1) restore, partition, rbformats, backup; (2) os,
privileged, device.rs; (3) fs glue, optical, remote; (4) model; (5) controller;
(6) cli; (7) gui (light, egui is retiring) and root files. Frontends come last, on
their own OSes. Findings come from one read-only review agent per module and are
re-checked before they are fixed; "confirmed" here is the reviewer's verdict until
a status says otherwise.

Mechanical (slice 1): `eprintln!` in partition/x68k_hdd_builder.rs (3) and
backup/single_file_chd.rs (3); allocations sized from input listed under RBF.

## src/restore

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| RST-001 | S1 | confirmed | [mod.rs:1218](../../src/restore/mod.rs#L1218) | Step 8 resizes at the layout's start (`ov.effective_start_lba()`), but the EBR plan repacked logicals one sector apart once any resized, so data and EBRs went elsewhere: the resize read the wrong sectors (FAT `Ok(false)`, BPB left at the old size, overrunning the next logical), and Modern 1 MiB alignment was lost for logicals. | fixed (this branch): the EBR plan uses the layout's starts; test `logical_partitions_keep_their_laid_out_starts_after_a_shrink` (32769 vs 34816 before) |
| RST-002 | S1 | confirmed | [mod.rs:2990](../../src/restore/mod.rs#L2990) | Clonezilla: Minimum = partclone `used_size()`, received as `Custom`; `copy_limit = export_size.min(size)` cuts a block-positioned partclone stream, dropping every used block past it, with no floor check. An fs the resizer doesn't know (XFS, JFS, UFS) then hits `resize_fat_in_place` → `Ok(false)`: silent loss. | fixed (this branch): the Clonezilla restore's finalize ladder now resizes through `resize_filesystem_from`, so a cut through used blocks or an fs with no resizer fails instead of passing; Minimum from `used_size()` is still too low a floor (see RST-002b) |
| RST-003 | S1 | confirmed | [mod.rs:527](../../src/restore/mod.rs#L527) | APM Original alignment keeps every start but never checks a grown partition against the next; Custom on a non-tail row, or FillRemaining on the last row by index when index order differs from LBA order, overlaps entries and the grow writes over the neighbour. | fixed (this branch): each partition is bounded by the next start; Fill fills to it; test `an_apm_partition_can_grow_only_to_the_next_one` |
| RST-004 | S1 | confirmed | [superfloppy_wrap.rs:124](../../src/restore/superfloppy_wrap.rs#L124) | GPT fit check `end_byte > target_size_bytes` ignores the 33-sector backup GPT; FillRemaining (`disk_write_plan.rs:137`) sizes to `target - first_byte`, so `write_backup_gpt` overwrites the partition's last 33 sectors and the entry passes the last usable LBA. | fixed (this branch): the wrap check and Fill keep the last 33 sectors for GPT; test `a_gpt_fill_leaves_room_for_the_backup_table` |
| RST-005 | S2 | confirmed | [single.rs:352](../../src/restore/single.rs#L352) | New-disk GPT on an image: disk sized `data + 2048*512`, start 2048, so the partition ends on the last sector; the backup GPT (written first) is overwritten by data and `end_lba` passes the last usable LBA. | fixed (this branch): an image is sized start + data + 33 sectors for GPT, and the writer refuses an entry past the last usable LBA |
| RST-006 | S2 | plausible | [mod.rs:2410](../../src/restore/mod.rs#L2410) | `build_ebr_chain(..).ok()?` swallows the error PT-011 made fatal; on `None` `write_clonezilla_disk` writes no EBRs, so logicals vanish. | fixed (this branch): `clonezilla_ebr_layout` returns the chain error instead of `None` |
| RST-007 | S2 | confirmed | [mod.rs:911](../../src/restore/mod.rs#L911) | Single-file CHD as-is vs resize looks only at size choices: all rows Original plus Modern1MB/Custom alignment copies the image verbatim, silently ignoring the alignment. | fixed (this branch): a non-Original alignment takes the rebuild path |
| RST-008 | S2 | confirmed | [single.rs:531](../../src/restore/single.rs#L531) | `let _ = patch_hidden_sectors_for(..)`: a failed patch after a move leaves DOS/Win9x unbootable with nothing logged (the CHD and wrap paths propagate it). | fixed (this branch): the hidden-sector patch error propagates |
| RST-009 | S2 | confirmed | [single.rs:307](../../src/restore/single.rs#L307) | New-disk MBR writes `start_lba as u32, part_size_sectors as u32` and APM `(disk_size_bytes / 512) as u32` (L363): wraps past 2 TiB (the CHD path uses `u32::try_from`). | fixed (this branch): new-disk MBR/APM fields go through `u32::try_from` |
| RST-010 | S2 | confirmed | [mod.rs:990](../../src/restore/mod.rs#L990) | A GPT/APM backup missing gpt.json/apm.json only warns, then writes every partition onto a disk with no table; a Clonezilla GPT parse failure (L2614) also falls into the MBR branch. Should bail. | fixed (this branch): a missing gpt.json/apm.json, or an unparsable Clonezilla GPT, stops the restore; restore shares `load_table_sidecars` |
| RST-011 | S3 | confirmed | [mod.rs:1363](../../src/restore/mod.rs#L1363) | `let _ = validate_filesystem_for(..)` (also L2746, single.rs:615-660): a validator error after a resize is dropped and the restore reports complete. | fixed (this branch): a failed check is logged as a Warning instead of dropped |
| RST-012 | S2 | plausible | [mod.rs:269](../../src/restore/mod.rs#L269) | Superfloppy layout returns `pm.original_size_bytes`, ignoring Custom / FillRemaining / Minimum. Confirm whether the controller offers size modes for a table-less backup. | fixed (this branch): the superfloppy override takes the size choice (Original/Minimum/Custom/Fill) |
| RST-013 | S3 | confirmed | [mod.rs:2232](../../src/restore/mod.rs#L2232) | Clonezilla Modern1MB/Custom pass heads 0 / spt 0; the per-partition path falls back to 255/63 because zero geometry leaves CHS stale after a move. | fixed (this branch): zero geometry falls back to the image's, then 255/63 |
| RST-014 | S3 | confirmed | [mod.rs:2691](../../src/restore/mod.rs#L2691) | The Clonezilla resize ladder drifted from `run_restore`'s: NTFS skips `ensure_backup_boot_sector`, FAT skips `resize_human68k_in_place`, no `needs_resize` guard. | fixed (this branch): the Clonezilla ladder is `resize_filesystem_from` plus the NTFS backup boot sector |
| RST-015 | S3 | confirmed | [mod.rs:993](../../src/restore/mod.rs#L993) | Em-dash glyphs in user-visible log/error strings (L993, 1019, 1191, 1250, 1285, 2627). | fixed (this branch): glyphs replaced across slice-1 strings |
| RST-016 | S4 | confirmed | [mod.rs:1623](../../src/restore/mod.rs#L1623) | Doc says logicals are unsupported and bail; the code rebuilds the EBR chain (L1900) and is tested. | fixed (this branch): the doc says the EBR chain is rebuilt |
| RST-017 | S4 | confirmed | [mod.rs:190](../../src/restore/mod.rs#L190) | `calculate_restore_layout`'s doc sits on `restore_size_floor`. | fixed (this branch): the doc moved onto `calculate_restore_layout` |
| RST-018 | S4 | confirmed | [mod.rs:249](../../src/restore/mod.rs#L249) | `repack_hint` says Original keeps positions; only APM does, MBR/GPT repack from `first_partition_lba`. | fixed (this branch): doc and hint say Original keeps the backup's grid, and absolute starts only for APM |
| RST-019 | S4 | confirmed | [mod.rs:1686](../../src/restore/mod.rs#L1686) | The CHD-resize floor pre-flight can never fire (`calculate_restore_layout` already bailed on a stricter floor). | deferred (refactor pass): dead pre-flight, harmless |
| RST-020 | S4 | confirmed | [single.rs:25](../../src/restore/single.rs#L25) | Says it re-uses the log/progress helpers; copies them from mod.rs:116-188. | fixed (this branch): single.rs uses the parent's helpers |
| RST-021 | S4 | confirmed | [mod.rs:1292](../../src/restore/mod.rs#L1292) | Three hand-written FS-resize ladders (L1292, 1560, 2691) beside `crate::fs::resize_filesystem_for`; RST-014 is the drift. | partly fixed (this branch): the Clonezilla ladder is now `resize_filesystem_from` (RST-014); the per-partition and CHD ladders carry compacted/defrag-clone rules the shared resizer doesn't, so merging them is its own refactor |
| RST-022 | S4 | confirmed | [mod.rs:1086](../../src/restore/mod.rs#L1086) | The open-target block (device vs R+W truncate) is pasted five times. | deferred (refactor pass) |
| RST-023 | S4 | confirmed | [mod.rs:1923](../../src/restore/mod.rs#L1923) | The extended-entry `total_sectors` patch loop duplicates `rbformats/mod.rs:552`. | deferred (refactor pass) |
| RST-024 | S4 | confirmed | [single.rs:613](../../src/restore/single.rs#L613) | Per-FS validate match re-implements `validate_filesystem_for`. | fixed (this branch): single.rs calls `validate_filesystem_for` |
| RST-025 | S3 | plausible | [mod.rs:760](../../src/restore/mod.rs#L760) | `plan_restore_ebr_chain` treats `index >= 4` as logical, `calculate_restore_layout` only `is_logical`; matters only for metadata older than 7e4ff1a9. | deferred (plausible): only metadata older than 7e4ff1a9 |

## src/partition

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| PAR-001 | S1 | confirmed | [resize.rs:412](../../src/partition/resize.rs#L412) | `apply_resize` moves data (`shift_region`) before `editor::apply_edits` writes the table; the editor refuses AHDI, X68k, Sun, TS, BSD, LVM (and RDB except SetBootable), and `validate_edits` is never called first. Shrinking partition 0 of an AHDI disk moves partition 1, then the editor bails and the table points at stale data. | fixed (this branch): the edit is validated and rehearsed on an in-memory overlay before any data moves; test `a_table_the_editor_refuses_leaves_the_data_unmoved` |
| PAR-002 | S1 | confirmed | [editor.rs:519](../../src/partition/editor.rs#L519) | GPT/APM `DeleteEntry` uses `entries.remove(i)`, shifting later indices; edits after it in one Apply land on the wrong partition (APM L642). | fixed (this branch): GPT/APM deletes are applied after the loop; test `a_gpt_delete_leaves_later_edits_on_their_own_partitions` |
| PAR-003 | S1 | confirmed | [resize.rs:91](../../src/partition/resize.rs#L91) | `compute_resize_plan` bounds by the disk size only, ignoring GPT's backup area and last usable LBA: growing the last GPT partition lets `apply_gpt_edits` write the backup array inside it. | fixed (this branch): the GPT writer refuses an entry past the last usable LBA, so the resize rehearsal stops it; test `a_gpt_partition_cannot_grow_into_the_backup_table` |
| PAR-004 | S1 | confirmed | [editor.rs:317](../../src/partition/editor.rs#L317) | Unchecked `as u32` on MBR entries (`new_size_bytes / 512`, starts at L351/416); same in SGI, SGI-DkLabel, APM edits (L1188, 1356, 605): wraps past 2 TiB. | fixed (this branch): every editor narrowing goes through `fit32`; test `an_mbr_size_past_32_bits_is_refused` |
| PAR-005 | S2 | confirmed | [restore_patch.rs:59](../../src/partition/restore_patch.rs#L59) | `layout_limits` reports tail 0 for Sun and nothing for SGI-DkLabel, while the patchers reserve `acyl` / cap at `altstart`; a builder "rest" row is then refused on every Sun donor with alternate cylinders. | fixed (this branch) for Sun: the tail is the alternate cylinders; SGI-DkLabel caps at an absolute `altstart`, which a tail can't express, so the patcher's own check stays the guard |
| PAR-006 | S2 | confirmed | [resize.rs:445](../../src/partition/resize.rs#L445) | A shrink truncates the image to `new_data_end`, cutting off the backup GPT just written and any tail. | fixed (this branch): `resized_data_end` keeps a GPT disk at its size, used by the footer and the truncation; test `a_shrunk_gpt_disk_keeps_its_backup_table` |
| PAR-007 | S2 | confirmed | [restore_patch.rs:316](../../src/partition/restore_patch.rs#L316) | Restore patchers write `as u32` unchecked: SGI, SGI-DkLabel, AHDI, TS, Sun (X68k and NeXT refuse). | fixed (this branch): every patcher narrowing goes through `fit32` |
| PAR-008 | S2 | confirmed | [x68k_hdd_builder.rs:485](../../src/partition/x68k_hdd_builder.rs#L485) | `entry_start as u32` into a word whose high byte is flags, no 0xFFFFFF check: 8 GiB SASI disks corrupt the flags of partitions 5-8. | fixed (this branch): the builder refuses an entry past sector 0xFFFFFF |
| PAR-009 | S2 | confirmed | [provision.rs:624](../../src/partition/provision.rs#L624) | `place` doesn't cap MBR, APM, AHDI or X68k at their field widths (`as u32` wraps); BSD and SGI use `try_from`. | fixed (this branch): `write_table` refuses a disk past the table's sector-field width (24-bit X68k, 32-bit the rest) |
| PAR-010 | S2 | confirmed | [editor.rs:571](../../src/partition/editor.rs#L571) | The GPT parser drops empty entries and `apply_gpt_edits` rebuilds from the compacted list: any edit renumbers partitions after a gap. | deferred: keeping GPT slot positions needs `part_tables::gpt` to keep empty slots, and every GPT index in the app to change meaning with it; its own PR |
| PAR-011 | S2 | plausible | [restore_patch.rs:90](../../src/partition/restore_patch.rs#L90) | `repack` lays out only given overrides; an unrestored listed slot (Sun, SGI, AHDI) keeps its extent and a moved neighbour can overlap it. | deferred (plausible): needs each patcher to list its untouched slots for an overlap check; no failing case yet |
| PAR-012 | S3 | confirmed | [x68k_hdd_builder.rs:459](../../src/partition/x68k_hdd_builder.rs#L459) | `eprintln!` (L459, 725, 791) carries warnings the user must act on, invisible in the GUI; belongs in a log callback or `BuildSummary.warnings`. | fixed (this branch): warnings travel in `BuildSummary.warnings`; the CLI and the provision runner show them |
| PAR-013 | S3 | confirmed | [editor.rs:511](../../src/partition/editor.rs#L511) | Unapplicable edits are logged and skipped and `apply_edits` returns Ok (bad GUID, no free MBR slot, logical resize, SetBootable on MBR/APM). | fixed (this branch): logical resize/move, no free MBR slot and a bad GUID now fail; SetBootable no-ops stay logged |
| PAR-014 | S3 | confirmed | [editor.rs:474](../../src/partition/editor.rs#L474) | A size under 512 bytes gives `last_lba < first_lba`; `validate_edits` accepts 0. | fixed (this branch): `apply_edits` refuses a size under one sector |
| PAR-015 | S3 | confirmed | [x68k_hdd_builder.rs:418](../../src/partition/x68k_hdd_builder.rs#L418) | U+2014 in user-visible strings (also L436, 607, 623; editor.rs:1242; provision.rs:1067; resize.rs:692; mod.rs:271, 287). | fixed (this branch): glyphs replaced |
| PAR-016 | S3 | plausible | [x68k_hdd_builder.rs:679](../../src/partition/x68k_hdd_builder.rs#L679) | `start_sector as usize * sector_size` overflows on 32-bit Win32. | fixed (this branch): the donor offset is computed in u64 |
| PAR-017 | S4 | confirmed | [mod.rs:439](../../src/partition/mod.rs#L439) | mod.rs (3,632 lines) has several jobs; split the 560-line `detect_superfloppy` and the per-scheme row builders. | deferred (refactor pass) |
| PAR-018 | S4 | confirmed | [editor.rs:1411](../../src/partition/editor.rs#L1411) | Comment swaps SGI slots 8 and 10. | fixed (this branch) |
| PAR-019 | S4 | confirmed | [mac_cd_builder.rs:243](../../src/partition/mac_cd_builder.rs#L243) | `build_mac_hfs_cd` and `build_sgi_efs_hdd` are public in-RAM builders used only by tests. | deferred (refactor pass): test-only builders; move behind `#[cfg(test)]` with their callers |

## src/rbformats

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| RBF-001 | S1 | confirmed | [vhd.rs:336](../../src/rbformats/vhd.rs#L336) | The fixed-VHD footer is written at the cursor; after `reconstruct_disk_from_backup`'s `patch_hidden_sectors_for` the cursor sits at last partition + 512, so a backup-folder VHD export writes `conectix` over the last partition's sector 1 and EOF has no footer (also the label path, vhd.rs:510, and RDB without tail pad). | fixed (this branch): `append_vhd_footer` seeks to the data end; test `vhd_export_from_backup` |
| RBF-002 | S1 | confirmed | [export.rs:748](../../src/rbformats/export.rs#L748) | 2MG export from a backup folder writes a 64-byte placeholder, but reconstruct places data at absolute offsets: the volume overwrites the placeholder and `patch_header_length` then overwrites the volume's first 64 bytes. | fixed (this branch): the disk is written through a window past the header; test `a_2mg_from_a_backup_keeps_header_and_disk_apart` |
| RBF-003 | S1 | confirmed | [mod.rs:735](../../src/rbformats/mod.rs#L735) | Exports from a backup folder never resize the filesystem; data is cut to `export_size` and the table patched, so "Minimum" on a compacted FAT gives a partition smaller than its BPB (`SIZES_HINT` promises a resize). | fixed (this branch): exports go through `reconstruct_disk_for_export`, which fits each resized (or compacted) volume with `resize_filesystem_from`; test `a_shrunk_partition_takes_its_fat_with_it` |
| RBF-004 | S1 | confirmed | [mod.rs:1345](../../src/rbformats/mod.rs#L1345) | RDB skip test is always true, so every in-place shrink skips `resize_filesystem_for`; resize errors only logged. | fixed (this branch): the skip uses `original_size`; a failed resize stops the export; `test_rdb_sfs_export_shrink_round_trip` now checks the volume size (3.2 MB left in a 1.6 MB partition before) |
| RBF-005 | S1 | confirmed | [mod.rs:1009](../../src/rbformats/mod.rs#L1009) | APM raw export resizes only `Apple_HFS` and only logs a failure; shrinking HFS+ copies `min(original, export)` and leaves the header at the old size. | fixed (this branch): any size change goes through `resize_filesystem_from` and a failure stops the export; a wrapped HFS+ is refused on shrink and left alone on grow |
| RBF-006 | S1 | confirmed | [compress.rs:345](../../src/rbformats/compress.rs#L345) | On i686 Windows, `(remaining as usize).min(CHUNK)` truncates: remaining = 4 GiB reads 0 and stops (8 GiB partition stops at 4 GiB); same at vhd.rs:693/721, mod.rs:985/1309/2195, export.rs:969/1237; mod.rs:2213 loops forever. | fixed (this branch): all 35 `(x as usize).min(n)` sites in src/ now take the min in u64 first |
| RBF-007 | S1 | confirmed | [export.rs:1148](../../src/rbformats/export.rs#L1148) | Clonezilla whole-disk export to QCOW2 / dynamic VHD / VMDK goes through `convert_from_vhd_temp`, which writes raw bytes: a raw image named `.qcow2`/`.vmdk`. | fixed (this branch): container formats re-encode the raw temp through the raw-image exporter; test `a_clonezilla_disk_exported_to_qcow2_is_a_qcow2` |
| RBF-008 | S1 | confirmed | [mod.rs:690](../../src/rbformats/mod.rs#L690) | No overlap check: if a partition grew past the next start, the next is written at the cursor (prev + 512) instead of its LBA (VHD raw path at `total_written`). | fixed (this branch): reconstruct and the VHD raw path refuse a start inside written data; test `a_partition_grown_over_its_neighbour_is_refused` |
| RBF-009 | S1 | plausible | [vhd.rs:954](../../src/rbformats/vhd.rs#L954) | Clonezilla EBR written at `part_offset - 512`; for the extended container the first EBR lands one sector early and its real LBA is zero-filled. | fixed (this branch): the export rebuilds the EBR chain with restore's `clonezilla_ebr_layout` and honours moved starts; test `a_clonezilla_vhd_export_puts_each_ebr_at_its_lba` |
| RBF-010 | S2 | confirmed | [export.rs:1289](../../src/rbformats/export.rs#L1289) | Dynamic VHD / QCOW2 / VMDK exports from a backup size the disk at the last partition's end: the GPT backup header is dropped and the primary's alternate LBA points past the end. | fixed (this branch): the temp-backed exports size the disk by the whole temp file |
| RBF-011 | S2 | confirmed | [mod.rs:2226](../../src/rbformats/mod.rs#L2226) | `seek(source_data_size - 1); write_all(&[0])` zeroes the last byte of a partition running to the disk end. | fixed (this branch): only a short image is extended |
| RBF-012 | S2 | confirmed | [mod.rs:711](../../src/rbformats/mod.rs#L711) | A missing partition data file is replaced by zeros with a log line; restore/export reports success. | fixed (this branch): a missing data file stops the restore/export |
| RBF-013 | S2 | confirmed | [cbk.rs:373](../../src/rbformats/cbk.rs#L373) | `vec![0u8; index_len]` and `with_capacity(chunk_count)` from footer u32s before any check; short index panics. | fixed (this branch): the footer and every index read are bounds-checked; test `a_footer_pointing_outside_the_file_is_refused` |
| RBF-014 | S2 | plausible | [cbk.rs:442](../../src/rbformats/cbk.rs#L442) | Traversal guard doesn't reject `:` (Windows `C:x` drive-relative join). | fixed (this branch): `:` and empty names are refused |
| RBF-015 | S2 | confirmed | [export.rs:1211](../../src/rbformats/export.rs#L1211) | WOZ/MOOF/DC42 from Clonezilla allocate `data_size` with no `check_floppy_export_size`. | fixed (this branch): `check_floppy_export_size` before the read |
| RBF-016 | S2 | confirmed | [mod.rs:1598](../../src/rbformats/mod.rs#L1598) | WOZ/MOOF, Twiggy (L1995) and DART (L1983) detection `read_to_end` the whole file once the magic matches. | fixed (this branch): `read_small_container` caps the in-memory read at 64 MiB |
| RBF-017 | S2 | confirmed | [gz_index.rs:75](../../src/rbformats/gz_index.rs#L75) | `with_capacity(count)` from the `.idx` sidecar before the truncation check. | fixed (this branch): capacity follows the bytes present; test `a_count_past_the_data_is_truncation_not_an_allocation` |
| RBF-018 | S2 | plausible | [chd_edit.rs:109](../../src/rbformats/chd_edit.rs#L109) | `position / sector_size` would panic on a CHD unit size of 0. | fixed (this branch): a zero unit size is refused at open |
| RBF-019 | S2 | plausible | [chd_edit.rs:503](../../src/rbformats/chd_edit.rs#L503) | `flatten_to_parent` re-encodes without geometry / ident (custom GDDD/IDNT lost) and loses codecs/hunk when `opts` is None. | deferred: carrying GDDD/IDNT through needs `compress_chd` to take source metadata from libchdman; its own PR |
| RBF-020 | S3 | confirmed | [chd.rs:560](../../src/rbformats/chd.rs#L560) | Model returns `crate::cli::exit::usage(..)`: model depends on the CLI. | fixed (this branch): the model bails plainly; the CLI checks the extension first for its usage code |
| RBF-021 | S4 | confirmed | [export.rs:1265](../../src/rbformats/export.rs#L1265) | Four near-copies of open → sidecars → reconstruct → encode (RBF-010 in all four). | deferred (refactor pass): RBF-010 is fixed in all four copies |
| RBF-022 | S4 | confirmed | [vhd.rs:619](../../src/rbformats/vhd.rs#L619) | The extended-entry patch loop and reconstruct re-implementations (VHD raw, Clonezilla VHD) differ in gap/overlap/resize rules; `zstd_compat.rs` and `qemu_img_test.rs` duplicate crate code. | deferred (refactor pass): the Clonezilla VHD path now shares restore's EBR layout (RBF-009) and the overlap rule (RBF-008) |

## src/backup

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| BKP-001 | S1 | confirmed | [single_file_chd.rs:552](../../src/backup/single_file_chd.rs#L552) | With a CHD resize plan, `compute_resize_plan` plans primaries only, so staging skips logicals: an MBR disk with a logical swap gets zeros there in the CHD and metadata omits it, reported as success. | fixed (this branch): a resized single-file CHD with logicals is refused (staging them is a follow-up); test `a_resized_chd_with_logical_partitions_is_refused` |
| BKP-002 | S2 | confirmed | [single_file_chd.rs:2121](../../src/backup/single_file_chd.rs#L2121) | MBR head segments are sector 0 plus the pre-primary gap; no EBRs are written, so the CHD's extended partition is empty for chdman/MAME, and metadata writes `extended_container: None`. | mitigated (this branch): the staging path is now refused for a disk with logicals (BKP-001), so no CHD is written without its EBRs; writing EBR head segments is the follow-up that lifts the refusal |
| BKP-003 | S2 | confirmed | [single_file_chd.rs:2016](../../src/backup/single_file_chd.rs#L2016) | Staging resize cuts the body to `target_len` then calls `resize_filesystem_for`, which is Ok and a no-op for UFS, XFS, minix...: the table shrinks, the superblock doesn't. | fixed (this branch): `resize_filesystem_from` refuses a shrink of a filesystem with no resizer (test `shrinking_a_filesystem_without_a_resizer_is_refused`) |
| BKP-004 | S1 | plausible | [single_file_chd.rs:411](../../src/backup/single_file_chd.rs#L411) | `run_export`'s resize path decodes only `.chd`; dynamic VHD, 2MG, QCOW2 sources are read raw while sector 0 came through the decoding `open_disk`. | fixed (this branch): any non-raw container is decoded to the temp image |
| BKP-005 | S2 | confirmed | [mod.rs:2073](../../src/backup/mod.rs#L2073) | Defrag-clone pipe deadlocks if the consumer stops early: `reader` is alive at `producer.join()`, so cancel or disk-full hangs (also single_file_chd.rs:1933). | fixed (this branch): the pipe reader is dropped before the join, so the producer's send fails and it returns |
| BKP-006 | S2 | confirmed | [apm_extras.rs:47](../../src/backup/apm_extras.rs#L47) | A failed `apm-extras.bin` export leaves a partial file (only a warning); restore then bails and the bundled-driver fallback is skipped. | fixed (this branch): a failed export removes the partial sidecar |
| BKP-007 | S2 | confirmed | [single_file_chd.rs:1294](../../src/backup/single_file_chd.rs#L1294) | `vec![0u8; head_end]` sized by the first partition's offset (also L1322 for label schemes): a multi-GB allocation. | fixed (this branch): head regions past 256 MiB are refused rather than allocated |
| BKP-008 | S3 | confirmed | [verify.rs:433](../../src/backup/verify.rs#L433) | Single-file CHD partitions have empty `compressed_files`, so their recorded checksums are never verified. | fixed (this branch): restore compares the CHD header SHA-1 with `container_sha1` and warns on a mismatch (an edit whose metadata refresh failed leaves it stale); decode checks each hunk's CRC |
| BKP-009 | S3 | plausible | [single_file_chd.rs:1164](../../src/backup/single_file_chd.rs#L1164) | A short staged `.zst` is zero-padded by `DiskImageStream` but the hash covers fewer bytes, unwarned. | deferred (plausible): needs a failing case |
| BKP-010 | S3 | confirmed | [single_file_chd.rs:1759](../../src/backup/single_file_chd.rs#L1759) | `eprintln!` clone warnings (L1759, 1792, 1825) bypass the progress log. | fixed (this branch): clone warnings go to the clone log |
| BKP-011 | S3 | confirmed | [mod.rs:671](../../src/backup/mod.rs#L671) | Container-decode loop has no cancel check; `sizes.rs` none at all (minute-long HFS+ walks). | fixed (this branch) for the decode loop; `sizes.rs` walks still have no cancel (S4 follow-up) |
| BKP-012 | S3 | confirmed | [mod.rs:2519](../../src/backup/mod.rs#L2519) | `progress.lock().expect(..)` panics on poison where every other lock tolerates it. | fixed (this branch): poison is tolerated |
| BKP-013 | S3 | plausible | [mod.rs:2680](../../src/backup/mod.rs#L2680) | CHD metadata always writes `defragmented_clone: false`; restore uses it to skip `rebuild_outgrown_hfs`. | fixed (this branch): recorded from the clone targets |
| BKP-014 | S3 | plausible | [mod.rs:2688](../../src/backup/mod.rs#L2688) | CHD path records `config.source_path` where per-partition uses `source_display` (the `rb://` label). | deferred (plausible): cosmetic `source_device` label |
| BKP-015 | S3 | confirmed | [sizes.rs:261](../../src/backup/sizes.rs#L261) | Glyphs in log text (ellipsis sizes.rs:261/269; em dashes mod.rs:1113, 1135, 1170, 1473, 1505, 1540). | fixed (this branch): glyphs replaced |
| BKP-016 | S4 | confirmed | [mod.rs:2540](../../src/backup/mod.rs#L2540) | Three identical push closures, all at Info, so staging warnings lose their level. | deferred (refactor pass) |
| BKP-017 | S4 | confirmed | [mod.rs:1877](../../src/backup/mod.rs#L1877) | The 5-shape clone producer is duplicated in single_file_chd.rs:1738; the shape-label match thrice. | deferred (refactor pass) |
| BKP-018 | S4 | confirmed | [mod.rs:1106](../../src/backup/mod.rs#L1106) | 11 copy-pasted sidecar arms; `run_backup_inner` is ~1,600 lines. | deferred (refactor pass) |
| BKP-019 | S4 | confirmed | [single_file_chd.rs:20](../../src/backup/single_file_chd.rs#L20) | Stale module docs (sparse temp path, direct stream, re-reading for checksums). | fixed (this branch): the module docs describe the staging path and streamed hashing |
| BKP-020 | S4 | confirmed | [single_file_chd.rs:1397](../../src/backup/single_file_chd.rs#L1397) | Dead code: `staging_path_unsupported_reason` (its DVD gate never runs), `let _ = is_superfloppy`, an unreachable `SolarisX86` arm, unread `mbr-min.bin`. | deferred (refactor pass) |
| BKP-021 | S4 | confirmed | [mod.rs:242](../../src/backup/mod.rs#L242) | `log` / `is_cancelled` duplicated across five units. | deferred (refactor pass) |

# src/ review, slice 2 (branch `review-src-2`, started 2026-11-26)

os, privileged, device.rs. Mechanical: no `eprintln!`; input-sized allocations are fixed-size; em dashes in eight `macos.rs` log lines (OSD-013).

## src/os common, privileged, device.rs

| ID | Sev | Status | Where | Finding | Fix |
|---|---|---|---|---|---|
| OSC-001 | S1 | confirmed | [mod.rs:321](../../src/os/mod.rs#L321) | `SectorAlignedReader`'s fast path rounds a short read down to a whole sector, so a tail under 512 bytes returns `Ok(0)`: backing up an image whose length isn't a sector multiple (a 174848-byte .d64) drops the last 256 bytes. | fixed: a sub-sector tail is returned as read; test `a_file_tail_under_one_sector_is_not_taken_for_eof` |
| OSC-002 | S1 | plausible | [mod.rs:595](../../src/os/mod.rs#L595) | Unix `flush_padded` zero-pads a partial last sector on every seek/read/flush, overwriting up to 511 following bytes on a preserving target and moving the cursor. | fixed: a regular file passes straight through on Unix; only devices are padded |
| OSC-003 | S1 | plausible | [mod.rs:817](../../src/os/mod.rs#L817) | Windows `SectorAlignedWriter::seek` silently aligns the target down; `Seek(1000)` + write lands at 512. Unix passes it through. | fixed: an unaligned Windows seek is refused instead of rounded down |
| OSC-004 | S1 | plausible | [mod.rs:683](../../src/os/mod.rs#L683) | Windows `flush_sectors` writes at the handle's cursor while the other paths seek to `self.position`; after `inner_mut()` the two diverge. | fixed: `flush_sectors` writes at the tracked position |
| OSC-005 | S2 | confirmed | [mod.rs:1001](../../src/os/mod.rs#L1001) | `is_device_path` only knows `/dev/` and `\\.\`; a symlink to a device, `\\?\PhysicalDriveN` or a GLOBALROOT path takes the regular-file branch (create+truncate, no unmount/claim/lock). | fixed: `normalize_device_path` resolves symlinks and spells drive paths `\\.\PhysicalDriveN`; a device that still isn't one is refused, not opened as a file |
| OSC-006 | S2 | plausible | [mod.rs:352](../../src/os/mod.rs#L352) | `SeekFrom::End` goes to the inner reader; on a bare macOS raw device it resolves to 0. | deferred (plausible): needs a device-size source in `browse_session`; the backup and inspect paths already wrap with `KnownLen` |
| OSC-007 | S2 | plausible | [mod.rs:1206](../../src/os/mod.rs#L1206) | `get_file_size` falls back to `seek(End)` = 0 on a macOS device, giving an empty source instead of an error. | deferred (plausible): logged by OSC-009; making it an error needs the macOS size path to never return None for a real disk |
| OSC-008 | S2 | plausible | [mod.rs:810](../../src/os/mod.rs#L810) | Windows `seek(End)` uses `metadata().len()` = 0 on a physical drive and wraps signed offsets unchecked. | fixed: a Windows End-seek with no length is refused; offsets are checked |
| OSC-009 | S3 | confirmed | [mod.rs:98](../../src/os/mod.rs#L98) | `known_len_reader` / `known_len_source` turn a failed size query into 0. | fixed: the failure is logged; callers still see 0 (a Result would ripple through seven callers) |
| OSC-010 | S3 | confirmed | [mod.rs:622](../../src/os/mod.rs#L622) | Unix `write` buffers then errors with the bytes still queued; a retry writes them twice. | deferred: a failed `write_all` mid-flush leaves an unknown amount on the device, so there is no correct retry state to restore |
| OSC-011 | S3 | plausible | [mod.rs:564](../../src/os/mod.rs#L564) | No `Drop` on `SectorAlignedWriter`: an unflushed buffer is silently lost. | fixed: `Drop` flushes what is buffered |
| OSC-012 | S3 | confirmed | [bulk_buf_reader.rs:137](../../src/bulk_buf_reader.rs#L137) | `set_len` on uninitialised memory handed to an arbitrary `Read` (UB); the type has no users. | fixed: `bulk_buf_reader.rs` removed (no users) |
| OSC-013 | S3 | confirmed | [file_assoc.rs:96](../../src/os/file_assoc.rs#L96) | A failed association registration still records the version, so it is never retried. | fixed: the version is recorded only on success; the installer verbs exit 1 on failure |
| OSC-014 | S4 | plausible | [wakelock.rs:80](../../src/os/wakelock.rs#L80) | Windows execution state is per-thread; a guard dropped on another thread, or nested guards, release wrongly. | deferred (plausible): every guard is created and dropped on one thread today |
| OSC-015 | S4 | confirmed | [privileged/mod.rs:81](../../src/privileged/mod.rs#L81) | The `privileged` module is dead, with a fixed /tmp progress path and unchecked `count*512` allocations. | fixed: `src/privileged` removed with the Linux and Windows impls |
| OSC-016 | S4 | confirmed | [mod.rs:512](../../src/os/mod.rs#L512) | The writer's doc paragraph sits on `sync_committed`. | fixed |
| OSC-017 | S4 | confirmed | [mod.rs:122](../../src/os/mod.rs#L122) | `SourceHandle::try_clone` claims an independent offset; `File::try_clone` shares it. | fixed: the doc says a File clone shares its offset |
| OSC-018 | S4 | confirmed | [mod.rs:373](../../src/os/mod.rs#L373) | Redundant `unsafe impl Send for SectorAlignedReader`. | fixed: removed |

## src/os macOS

| ID | Sev | Status | Where | Finding | Fix |
|---|---|---|---|---|---|
| OSD-001 | S2 | plausible | [macos.rs:1180](../../src/os/macos.rs#L1180) | Socketpair and stderr pipe lack `FD_CLOEXEC`; a concurrent child inherits them and `drain_stderr_pipe` blocks until it exits. | fixed: the socketpair and stderr pipe are close-on-exec |
| OSD-002 | S2 | confirmed | [macos.rs:1306](../../src/os/macos.rs#L1306) | Raw-device fds (SCM_RIGHTS, direct opens, dup) lack `FD_CLOEXEC`; diskutil/hdiutil children inherit a root O_RDWR disk handle. | fixed: every raw-device fd (received, opened, dup) is close-on-exec |
| OSD-003 | S2 | plausible | [macos.rs:696](../../src/os/macos.rs#L696) | Main-target claim timeout frees the callback state while the main thread may be inside `claim_callback`. | deferred (plausible): needs a DA unschedule that waits for an in-flight callback; tracked for a macOS device pass |
| OSD-004 | S2 | plausible | [macos.rs:1331](../../src/os/macos.rs#L1331) | Restore's authopen fd stays in `ELEVATED_DEVICES` for the app's life: no re-probe, volumes don't mount, eject says busy. | fixed: `run_restore` releases the device's cached descriptor when it ends |
| OSD-005 | S2 | plausible | [macos.rs:804](../../src/os/macos.rs#L804) | Every `dup_as_file` shares one offset; two concurrent File users of one device seek under each other. | deferred (plausible): no concurrent File users of one device today |
| OSD-006 | S2 | plausible | [macos.rs:875](../../src/os/macos.rs#L875) | The elevated-fd cache is keyed by path only; a new card on the same diskN gets the revoked fd. | deferred (plausible): needs media identity on the cache key (DA media UUID) |
| OSD-007 | S3 | confirmed | [macos.rs:1337](../../src/os/macos.rs#L1337) | A failed unmount only warns; the user then sees "Permission denied" instead of "busy". | deferred: the busy cause is in the log; surfacing it needs `open_device` to carry the unmount error |
| OSD-008 | S2 | plausible | [macos_stub.rs:290](../../src/os/macos_stub.rs#L290) | The stub reads a mounted volume through `/dev/diskN` without unmounting (PowerPC backups image a live volume). | deferred: the stub serves PowerPC/10.5 builds, which need their own test machine |
| OSD-009 | S2 | plausible | [darwin_devices.rs:168](../../src/os/darwin_devices.rs#L168) | Nested slices (`disk2s2s1`) are skipped by `mounts_to_unmount`. | deferred (plausible): nested-slice parsing in `darwin_devices`, stub builds only |
| OSD-010 | S3 | confirmed | [sudo.rs:138](../../src/os/macos/sudo.rs#L138) | Predictable /tmp askpass path through a symlink-following create (module dead, OSD-016). | fixed: `sudo.rs` removed |
| OSD-011 | S3 | plausible | [sudo.rs:126](../../src/os/macos/sudo.rs#L126) | A cancelled askpass reads as an auth failure; `sudo` resolved via PATH with `-E`. | fixed: `sudo.rs` removed |
| OSD-012 | S3 | confirmed | [macos.rs:1251](../../src/os/macos.rs#L1251) | The forked child calls `exit(-1)` after a failed `execv`; must be `_exit(127)`. | fixed: `_exit(127)` |
| OSD-013 | S3 | confirmed | [macos.rs:587](../../src/os/macos.rs#L587) | Em dashes in eight log messages. | fixed |
| OSD-014 | S3 | confirmed | [macos.rs:1538](../../src/os/macos.rs#L1538) | Two 7-line comment blocks added after the comment cap; one is wrong. | fixed: both blocks cut to two lines; the wrong claim is gone |
| OSD-015 | S4 | confirmed | [macos.rs:537](../../src/os/macos.rs#L537) | Unmount retries 6 times, not the documented 5. | fixed: five attempts, as documented |
| OSD-016 | S4 | confirmed | [sudo.rs:53](../../src/os/macos/sudo.rs#L53) | `sudo.rs` is dead. | fixed: `sudo.rs` removed |
| OSD-017 | S4 | confirmed | [macos_stub.rs:211](../../src/os/macos_stub.rs#L211) | Stub and real module public surfaces have drifted. | deferred (refactor pass) |
| OSD-018 | S4 | confirmed | [darwin_devices.rs:78](../../src/os/darwin_devices.rs#L78) | `is_read_only` doc calls `false` the safer default. | fixed: the doc says false means writable |
| OSD-019 | S4 | plausible | [macos.rs:144](../../src/os/macos.rs#L144) | `dict_get_*` cast CF values with no type-ID check. | deferred (plausible): IOKit returns the documented types for the keys read |
| OSD-020 | S4 | plausible | [macos.rs:888](../../src/os/macos.rs#L888) | The cache lock is dropped between lookup and insert; two dialogs, duplicate entries. | deferred (plausible): concurrent escalations of one device are not issued today |
| OSD-021 | S4 | confirmed | [macos.rs:1287](../../src/os/macos.rs#L1287) | authopen's stderr is discarded when the reply fails. | deferred |

## src/os Windows

| ID | Sev | Status | Where | Finding | Fix |
|---|---|---|---|---|---|
| OSW-001 | S1 | confirmed | [windows.rs:121](../../src/os/windows.rs#L121) | A volume that fails to open, lock or dismount only warns and the write goes ahead; Windows then refuses sectors inside the still-mounted volume partway through. | fixed: a volume that can't be opened or dismounted stops the write; the lock is retried for 2.5 s and a failed lock forces the dismount |
| OSW-002 | S1 | confirmed | [partition_resize.rs:60](../../src/model/partition_resize.rs#L60) | `open_target_for_writing(path)?.file` drops the handle (and its volume locks) before any write. | fixed: the handle lives to the end of the resize |
| OSW-003 | S1 | confirmed | [windows.rs:337](../../src/os/windows.rs#L337) | Lower-case `\\.\physicaldrive0` passes the device checks but misses the case-sensitive preflight lookup, skipping the system-disk refusal. | fixed: preflight compares the normalized path |
| OSW-004 | S1 | plausible | [mod.rs:996](../../src/os/mod.rs#L996) | `\\?\PhysicalDriveN` is accepted by preflight but written as a regular file (no lock/dismount); same as OSC-005. | fixed with OSC-005 |
| OSW-005 | S1 | plausible | [windows.rs:546](../../src/os/windows.rs#L546) | A failed volume enumeration or extents query drops the volume silently, so "No volumes found" lets the write proceed. | fixed: a failed volume listing stops the write |
| OSW-006 | S2 | plausible | [windows.rs:267](../../src/os/windows.rs#L267) | The runas relaunch passes no working directory; relative arguments resolve against System32. | fixed: `args_for_relaunch` makes existing relative paths absolute before runas |
| OSW-007 | S4 | confirmed | [windows.rs:285](../../src/os/windows.rs#L285) | `shell_execute_ex_w` calls `ShellExecuteW`; the error comes from `last_os_error` and always blames UAC. | deferred (refactor pass): the misnamed wrapper works; only its error text is off |
| OSW-008 | S4 | confirmed | [windows.rs:664](../../src/os/windows.rs#L664) | `QueryDosDeviceW` retries on any error, growing to 32 MB. | fixed: only ERROR_INSUFFICIENT_BUFFER retries |
| OSW-009 | S4 | confirmed | [windows.rs:846](../../src/os/windows.rs#L846) | Four disagreeing "is a physical drive" tests (root of OSW-003/004). | partly fixed: the duplicate `is_device_path`s delegate to `os`; normalization covers the casings |
| OSW-010 | S4 | confirmed | [windows.rs:901](../../src/os/windows.rs#L901) | `WindowsDiskAccess` is an all-bail stub; docs claim automatic elevation. | fixed: `WindowsDiskAccess` removed |
| OSW-011 | S4 | confirmed | [windows.rs:309](../../src/os/windows.rs#L309) | `bus_type_to_string` misses SCSI, RAID, iSCSI, SAS, virtual. | fixed |

## src/os Linux, Solaris

| ID | Sev | Status | Where | Finding | Fix |
|---|---|---|---|---|---|
| OSL-001 | S2 | confirmed | [linux.rs:134](../../src/os/linux.rs#L134) | The mount map keeps one mount per device: btrfs `/` + `/home` on one partition loses `/`, so `is_system` is false and only one mount is unmounted. | fixed: `/` wins per source; the unmount walks every mount, deepest first; test `a_source_mounted_twice_keeps_root_and_unmounts_both` |
| OSL-002 | S2 | confirmed | [linux.rs:232](../../src/os/linux.rs#L232) | A root on LVM/LUKS or on a whole disk is never flagged as the system disk. | fixed: the root's source is traced through `/sys/class/block/*/slaves` to its disks |
| OSL-003 | S2 | confirmed | [linux.rs:305](../../src/os/linux.rs#L305) | Every mount on the parent disk is detached before `O_EXCL` decides; a partition target unmounts its siblings. | deferred: refusing before unmounting needs a mounted-but-not-held probe; O_EXCL still blocks a live write |
| OSL-004 | S2 | confirmed | [linux.rs:296](../../src/os/linux.rs#L296) | `/dev/disk/by-id/...` isn't canonicalised, so nothing is unmounted and the open fails EBUSY. | fixed with OSC-005 (by-id symlinks resolve before the unmount) |
| OSL-005 | S2 | confirmed | [linux.rs:689](../../src/os/linux.rs#L689) | `std::env::args()` panics on a non-UTF-8 argument during relaunch. | fixed: `args_os` |
| OSL-006 | S2 | plausible | [linux.rs:740](../../src/os/linux.rs#L740) | pkexec changes to /root, so relative arguments resolve wrongly. | fixed: the same helper before pkexec |
| OSL-007 | S2 | plausible | [solaris.rs:299](../../src/os/solaris.rs#L299) | ZFS/SVM roots never flag the boot disk (display only; writes refused on Solaris). | deferred: Solaris writes are refused, display only |
| OSL-008 | S2 | plausible | [solaris.rs:147](../../src/os/solaris.rs#L147) | SPARC whole disk is `s2`; EFI disks skipped; size from kstat can exceed `s2`. | deferred: needs a SPARC/EFI test machine |
| OSL-009 | S2 | plausible | [solaris.rs:300](../../src/os/solaris.rs#L300) | `is_removable` opens every disk, contradicting the no-open enumeration. | deferred (plausible) |
| OSL-010 | S3 | confirmed | [linux.rs:783](../../src/os/linux.rs#L783) | Elevated runs `umask(0)`: backups are world-writable. | deferred: the fix is handing files to `SUDO_UID`, not a tighter umask (which would leave the user unable to delete their own backups) |
| OSL-011 | S4 | confirmed | [linux.rs:692](../../src/os/linux.rs#L692) | pkexec lookup shells out to `which`. | fixed: PATH is searched directly |
| OSL-012 | S4 | confirmed | [linux.rs:531](../../src/os/linux.rs#L531) | `LinuxDiskAccess` is dead and duplicates the unmount loop. | fixed: `LinuxDiskAccess` removed |
| OSL-013 | S4 | confirmed | [linux.rs:79](../../src/os/linux.rs#L79) | `unescape_mountinfo` is quadratic. | fixed: a peek |
| OSL-014 | S4 | plausible | [linux.rs:59](../../src/os/linux.rs#L59) | eMMC boot partitions, md, mtdblock, zd listed as disks; Solaris `is_read_only` hard-coded. | deferred (plausible): extra rows, no wrong writes |

# src/ review, slice 3 (branch `review-src-3`, started 2026-11-26)

fs glue, optical, remote. Four read-only reviewers: `src/fs/mod.rs` (FSG), the rest of `src/fs` (FSX), `src/optical` (OPT), `src/remote` (RMT).

## src/fs/mod.rs

| ID | Sev | Status | Where | Finding | Fix |
|---|---|---|---|---|---|
| FSG-001 | S1 | confirmed | [mod.rs:192](../../src/fs/mod.rs#L192) | `resize_filesystem_for` passes `bytes/512` to the FAT/NTFS/exFAT resizers, which read it in the volume's own sector size: a 2048-bps FAT or 4Kn NTFS/exFAT grows to claim 4-8x its partition. | fixed: `native_sectors` converts to the BPB/VBR sector size at the dispatch and in restore; test `resize_counts_are_in_the_volumes_own_sectors` |
| FSG-002 | S1 | plausible | [mod.rs:219](../../src/fs/mod.rs#L219) | The resize dispatch runs every resizer; a FAT whose FAT table holds 0x4244/0xEF53 at +1024 also gets the HFS or ext resizer. Dispatch on the detected type. | fixed: detection runs before any resizer, and the HFS/HFS+ and ext resizers run only on the volume it names |
| FSG-003 | S1 | plausible | [mod.rs:518](../../src/fs/mod.rs#L518) | `in_place_resize_support` maps "unknown" to `NoFilesystem`, so a shrink of a filesystem detection misses (CP/M, LIF, Lisa) is let through. | deferred (plausible): needs `in_place_resize_support` to tell "no filesystem" from "one detection doesn't know" (CP/M, LIF, Lisa have no magic at the partition start) |
| FSG-004 | S2 | confirmed | [mod.rs:192](../../src/fs/mod.rs#L192) | `(bytes/512) as u32` wraps at 2 TiB for FAT. | fixed: the FAT count saturates |
| FSG-005 | S2 | confirmed | [mod.rs:120](../../src/fs/mod.rs#L120) | The FAT hidden-sector patch accepts NTFS and writes only the primary boot sector; the NTFS patcher then sees nothing to do, so the backup boot sector keeps the stale value. | deferred: patching NTFS first wrote a backup sector at the old TotalSectors before the resize (past a shrunk partition), so the order is back; restore's `ensure_backup_boot_sector` rewrites the backup after the resize, and the export paths still leave it stale |
| FSG-006 | S2 | confirmed | [mod.rs:3365](../../src/fs/mod.rs#L3365) | `wrapped_hfsplus_compact_reader` allocates the wrapper head from an unchecked MDB offset (up to ~2^48). | fixed: the wrapper head is capped at 256 MiB |
| FSG-007 | S2 | plausible | [mod.rs:3383](../../src/fs/mod.rs#L3383) | The same reader holds the partition tail after the embedded volume in RAM. | fixed: the tail is capped too; a bigger wrapper is refused with a sector-by-sector hint |
| FSG-008 | S2 | confirmed | [mod.rs:820](../../src/fs/mod.rs#L820) | UFS/ADFS/JFS/DOS 3.3 detection does unaligned sub-sector reads, which fail on a raw device. | deferred: the detection reads go through `SectorAlignedReader` on every device path found; making each probe sector-sized is a sweep across ~20 probes |
| FSG-009 | S2 | plausible | [mod.rs:4015](../../src/fs/mod.rs#L4015) | `fit_apple_volume_to_partition` reads 2 bytes at +1024 on a raw device. | fixed: reads a whole sector |
| FSG-010 | S2 | plausible | [mod.rs:268](../../src/fs/mod.rs#L268) | `recount_fat32_free`'s last FAT chunk isn't a sector multiple. | deferred (plausible): devices reach `recount_fat32_free` through the aligned reader |
| FSG-011 | S2 | plausible | [mod.rs:362](../../src/fs/mod.rs#L362) | `grow_exfat_past_its_fat` rewrites in place with the only copy in an unnamed tempfile. | deferred (plausible): needs a named, kept copy of the volume before the in-place rewrite |
| FSG-012 | S4 | confirmed | [mod.rs:1420](../../src/fs/mod.rs#L1420) | `packed_partition_reader_padded` misses FAT types 0x11, 0xEF, 0x27. | fixed: 0x11 and 0xEF added (0x27 is NTFS) |
| FSG-013 | S4 | confirmed | [mod.rs:3485](../../src/fs/mod.rs#L3485) | `Apple_UNIX_SVR2` compaction handles only ext/btrfs. | deferred (refactor pass) |
| FSG-014 | S4 | confirmed | [mod.rs:1781](../../src/fs/mod.rs#L1781) | `EXPENSIVE_MINIMUM_STRINGS` misses `Apple_HFS+`, `Apple_UNIX_SRVR2`, ProDOS. | deferred (refactor pass) |
| FSG-015 | S4 | confirmed | [mod.rs:1604](../../src/fs/mod.rs#L1604) | `fs_name_for` says "unknown" for strings the routers open. | deferred (refactor pass) |
| FSG-016 | S4 | confirmed | [mod.rs:3209](../../src/fs/mod.rs#L3209) | PFS3/SFS read/compact arms ignore `partition_size`; 0x07 drops the passphrase. | deferred (refactor pass) |
| FSG-017 | S4 | confirmed | [mod.rs:2309](../../src/fs/mod.rs#L2309) | Detected `human68k` has no 0x00 auto-detect arm. | deferred (refactor pass) |
| FSG-018 | S4 | confirmed | [mod.rs:3903](../../src/fs/mod.rs#L3903) | `is_checkable_type` misses 0x11/0xEF; its doc contradicts `is_checkable_fs_name`. | fixed: 0x11 and 0xEF added |
| FSG-019 | S4 | confirmed | [mod.rs:691](../../src/fs/mod.rs#L691) | Detection treats only 0x482B as wrapped; the guard also takes 0x4858. | fixed: an HFSX-embedded wrapper detects as hfsplus |
| FSG-020 | S4 | confirmed | [mod.rs:136](../../src/fs/mod.rs#L136) | `resize_filesystem_from` passes no type string, so its Amiga branch is dead. | deferred (refactor pass): with FSG-003 |
| FSG-021 | S4 | confirmed | [mod.rs:734](../../src/fs/mod.rs#L734) | QNX4 probed twice; the FAT comment sits above it. | fixed: the second probe is gone |
| FSG-022 | S3 | confirmed | [mod.rs:3227](../../src/fs/mod.rs#L3227) | Em dashes in user-visible errors. | fixed |
| FSG-023 | S4 | confirmed | [mod.rs:552](../../src/fs/mod.rs#L552) | `fs_display_name` covers half the detected tokens. | deferred (refactor pass) |
| FSG-024 | S4 | confirmed | [mod.rs:235](../../src/fs/mod.rs#L235) | FAT/exFAT on-disk logic in app glue; belongs in `fs-pc`. | deferred (refactor pass): moving FAT/exFAT logic into fs-pc is a crate change |

## src/fs operations

| ID | Sev | Status | Where | Finding | Fix |
|---|---|---|---|---|---|
| FSX-001 | S1 | confirmed | [replace.rs:215](../../src/fs/replace.rs#L215) | A failed temp-name create returns `Ok(None)`; the fallback deletes the original and re-reads an already consumed source. | fixed: the new contents are spooled; a staged write that fails falls back to delete-then-create only when the new copy fits the old one's place, else errors with the original intact; tests `a_replace_that_cannot_fit_keeps_the_original`, `a_replace_on_a_nearly_full_volume_still_works` |
| FSX-002 | S1 | confirmed | [copy.rs:373](../../src/fs/copy.rs#L373) | `Force` deletes the destination before the source is read or written. | fixed: a file in the way is replaced through the staged `create_or_replace`; test `a_forced_copy_that_cannot_fit_keeps_the_destination_file` |
| FSX-003 | S2 | confirmed | [export_selection.rs:168](../../src/fs/export_selection.rs#L168) | `relative_dir` from on-disk names (`..`, `/`, `C:`) is joined onto the export folder unsanitised; same in zip and StuffIt member names. | fixed: folder components are legalized one by one and zip members can't be `.`/`..`; test `image_folder_names_stay_inside_the_export_folder`. StuffIt names stay Mac names inside the archive |
| FSX-004 | S2 | confirmed | [export_selection.rs:257](../../src/fs/export_selection.rs#L257) | `read_data_fork` sizes a Vec from the image's entry size; StuffIt export holds every file in RAM. | fixed for the reservation (capped at 16 MiB); a whole-file read is still how per-file gzip/zip work |
| FSX-005 | S2 | confirmed | [copy.rs:390](../../src/fs/copy.rs#L390) | `Vec::with_capacity(rsrc_size)` from the image. | fixed: reservation capped |
| FSX-006 | S2 | confirmed | [zstd_stream.rs:72](../../src/fs/zstd_stream.rs#L72) | `fill_to` keeps every decompressed byte; End-seek returns the buffered length. | deferred: `ZstdStreamReader` needs a seekable-zstd index to drop the prefix it keeps |
| FSX-007 | S2 | confirmed | [optical_fs.rs:272](../../src/fs/optical_fs.rs#L272) | Optical file reads load the whole file; `read_file_range` exists. | deferred: `optical_fs` should call `read_file_range` in chunks; needs a test disc with a large file |
| FSX-008 | S2 | confirmed | [hfs_clone.rs:276](../../src/fs/hfs_clone.rs#L276) | Driver partitions buffered from an unchecked `block_count`. | fixed: driver partitions over 64 MiB are refused |
| FSX-009 | S2 | confirmed | [mac_scsi_bless.rs:161](../../src/fs/mac_scsi_bless.rs#L161) | Donor driver read sized by an unchecked `block_count`; `ddSize as u16` truncates. | fixed for the allocation (64 MiB cap); the `ddSize as u16` truncation remains (drivers are far smaller) |
| FSX-010 | S2 | confirmed | [copy.rs:252](../../src/fs/copy.rs#L252) | Flatten mode on a flat volume silently overwrites same-named files from different folders. | deferred: flatten on a flat volume needs a rename-on-collision policy (a UI choice) |
| FSX-011 | S2 | plausible | [mac_scsi_bless.rs:277](../../src/fs/mac_scsi_bless.rs#L277) | Bless writes into the first `is_driver_type` match (also Patches/ATA/FW) and replaces the DDR with one descriptor. | deferred (plausible): needs an ATA-era APM fixture |
| FSX-012 | S2 | plausible | [copy.rs:287](../../src/fs/copy.rs#L287) | Directory walks recurse with no depth or cycle cap. | fixed for copy and tree (a 512-level cap); `project`, `zip_recurse`, `sit_nodes` and `measure_dir_into` still lean on the drivers' own cycle guards |
| FSX-013 | S2 | plausible | [archive_fs.rs:129](../../src/fs/archive_fs.rs#L129) | An empty archive member name maps to `/`, so the root lists itself. | deferred (plausible): depends on whether arc-mac drops empty names |
| FSX-014 | S2 | confirmed | [mac_archive_import.rs:44](../../src/fs/mac_archive_import.rs#L44) | `looks_like_mac_archive` reads the whole file to sniff it. | fixed: files over 256 MiB are not sniffed (BinHex detection needs the whole file) |
| FSX-015 | S3 | confirmed | [dir_import.rs:49](../../src/fs/dir_import.rs#L49) | `is_maczip` reads zips up to 256 MiB into RAM. | deferred: a streaming MacZip probe |
| FSX-016 | S3 | confirmed | [tree.rs:58](../../src/fs/tree.rs#L58) | Control characters shown as U+2400 glyphs. | fixed: control characters show as `?` |
| FSX-017 | S3 | confirmed | [make_bootable.rs:253](../../src/fs/make_bootable.rs#L253) | The donor is validated after the driver and DDR are written. | deferred: validate the donor before the first write |
| FSX-018 | S3 | confirmed | [export_selection.rs:367](../../src/fs/export_selection.rs#L367) | Tar/zip exports leave a truncated archive on error or cancel. | deferred: remove the archive on error or cancel |
| FSX-019 | S3 | confirmed | [tree.rs:87](../../src/fs/tree.rs#L87) | Unchecked size arithmetic from disk. | fixed: saturating |
| FSX-020 | S4 | confirmed | [hfs_clone.rs:1](../../src/fs/hfs_clone.rs#L1) | Module docs claim re-exports that don't exist. | deferred (refactor pass) |
| FSX-021 | S4 | confirmed | [hfs_clone.rs:61](../../src/fs/hfs_clone.rs#L61) | `emit_apm_disk_with_hfs` is an in-RAM API used only by a test. | deferred (refactor pass) |
| FSX-022 | S4 | confirmed | [mac_scsi_bless.rs:74](../../src/fs/mac_scsi_bless.rs#L74) | `is_driver_type` duplicated in hfs_clone.rs. | deferred (refactor pass): belongs in part_tables::apm |

## src/optical

| ID | Sev | Status | Where | Finding | Fix |
|---|---|---|---|---|---|
| OPT-001 | S1 | confirmed | [convert.rs:539](../../src/optical/convert.rs#L539) | Per-track flush errors are dropped with `.ok()`, then the single-bin is deleted: a full disk loses the only complete copy. | fixed: a failed track flush stops before the single-bin is deleted |
| OPT-002 | S1 | confirmed | [convert.rs:149](../../src/optical/convert.rs#L149) | Convert never refuses an output that is the source; `a.iso -> a.iso --format bincue` overwrites the ISO with the cue. | fixed: `rb-cli optical convert` applies `output_refusal` |
| OPT-003 | S1 | confirmed | [rip.rs:399](../../src/optical/rip.rs#L399) | `--output disc.bin --format bincue` writes the cue over the ripped BIN. | fixed: `rb-cli optical rip` applies it too |
| OPT-004 | S2 | confirmed | [rip.rs:422](../../src/optical/rip.rs#L422) | Every data track is read as Mode 1, so Mode 2 discs (PSX, VCD, CD-i) can't be ripped. | deferred: Mode 2 ripping is a feature (cd-da-reader `detect_track_format` + `Mode2Raw`) |
| OPT-005 | S2 | confirmed | [rip.rs:304](../../src/optical/rip.rs#L304) | The ISO rip reads to the lead-out, through a mixed-mode disc's audio tracks, and fails. | fixed: the ISO rip ends at the next track (less the 2 s gap before audio) |
| OPT-006 | S2 | confirmed | [convert.rs:192](../../src/optical/convert.rs#L192) | `bincue_to_iso` includes the next audio track's pregap. | deferred: needs a mixed-mode single-BIN fixture to pin the pregap rule |
| OPT-007 | S2 | confirmed | [convert.rs:197](../../src/optical/convert.rs#L197) | `bin_size - file_byte_offset` unchecked. | fixed: checked |
| OPT-008 | S2 | confirmed | [rip.rs:389](../../src/optical/rip.rs#L389) | `toc.tracks[0]` and unchecked LBA subtractions on an empty or unsorted TOC (remote TOCs unvalidated). | fixed: `check_toc` refuses an empty or unsorted TOC |
| OPT-009 | S2 | plausible | [rip.rs:450](../../src/optical/rip.rs#L450) | Short sector reads aren't checked against the requested count. | fixed: both rip loops refuse a short sector read |
| OPT-010 | S2 | confirmed | [convert.rs:403](../../src/optical/convert.rs#L403) | CHD extracts ignore cancel. | deferred: libchdman extract calls take no cancel hook |
| OPT-011 | S2 | confirmed | [source.rs:386](../../src/optical/source.rs#L386) | DVD/BD reads have no retry or chunk reduction. | deferred: DVD/BD retries need the chunk-reduction loop the CD path has |
| OPT-012 | S2 | confirmed | [boot_area.rs:162](../../src/optical/boot_area.rs#L162) | `read_file` allocates an unchecked length. | fixed: boot-area files over 64 MiB are refused |
| OPT-013 | S2 | confirmed | [convert.rs:114](../../src/optical/convert.rs#L114) | MSF minutes wrap past 99; a trailing partial sector is dropped. | fixed: an ISO past 99:59:74 or with a partial sector is refused |
| OPT-014 | S2 | plausible | [convert.rs:125](../../src/optical/convert.rs#L125) | Synthesised MODE1/2352 frames carry zero EDC/ECC. | deferred (plausible): EDC/ECC generation is a feature |
| OPT-015 | S2 | plausible | [rip.rs:415](../../src/optical/rip.rs#L415) | CD-Extra's session gap is read as part of the last audio track. | deferred (plausible): needs a real Enhanced CD |
| OPT-016 | S3 | confirmed | [rip.rs:324](../../src/optical/rip.rs#L324) | Failed or cancelled rips/converts leave their output (and a temp BIN). | fixed for rips: a failed rip removes only outputs it changed, so an early error keeps a previous rip |
| OPT-017 | S3 | plausible | [source.rs:272](../../src/optical/source.rs#L272) | Windows eject quoting. | deferred (plausible) |
| OPT-018 | S4 | confirmed | [convert.rs:600](../../src/optical/convert.rs#L600) | chd-only helpers not cfg-gated. | deferred (refactor pass) |
| OPT-019 | S4 | confirmed | [rip.rs:515](../../src/optical/rip.rs#L515) | Three LBA-to-MSF copies; helpers copied between rip and convert. | deferred (refactor pass) |
| OPT-020 | S4 | plausible | [rip.rs:240](../../src/optical/rip.rs#L240) | Any TOC failure sends a CD down the DVD path. | deferred (plausible) |

## src/remote

| ID | Sev | Status | Where | Finding | Fix |
|---|---|---|---|---|---|
| RMT-001 | S1 | confirmed | [server.rs:773](../../src/remote/server.rs#L773) | The daemon has no authentication, binds 0.0.0.0:7341 and is writable by default: any LAN peer can write images and non-system disks. | parked by the user (2026-11-26): authentication, the default bind and the default writable mode change the MiSTer workflow |
| RMT-002 | S1 | confirmed | [server.rs:816](../../src/remote/server.rs#L816) | The chunked-upload path ignores `--read-only`. | fixed: a read-only daemon drops a Family-B PUT |
| RMT-003 | S1 | confirmed | [service.rs:70](../../src/remote/service.rs#L70) | `rb-daemon.ini`'s `token` is parsed but never enforced. | partly fixed: the daemon log warns that a configured token is not enforced; enforcing one needs a protocol field and a token prompt in every client (needs a decision with RMT-001) |
| RMT-004 | S1 | confirmed | [server.rs:2329](../../src/remote/server.rs#L2329) | `sandbox_join_create` follows a final-component symlink out of the root. | fixed: a final-component symlink is refused |
| RMT-005 | S1 | plausible | [server.rs:742](../../src/remote/server.rs#L742) | Member names allow `C:x` on a Windows daemon. | fixed: `:` is refused in container and member names |
| RMT-006 | S2 | confirmed | [server.rs:148](../../src/remote/server.rs#L148) | Unbounded threads, no socket timeouts. | partly fixed: at most 32 concurrent connections; idle timeouts wait on a client heartbeat |
| RMT-007 | S2 | confirmed | [client.rs:57](../../src/remote/client.rs#L57) | The client clears its read timeout and has no write timeout. | deferred: a long daemon job can go quiet for minutes, so a read timeout needs progress frames first |
| RMT-008 | S2 | confirmed | [client.rs:666](../../src/remote/client.rs#L666) | `read_block` buffers an unbounded reply before checking its length. | fixed: `CappedSink` refuses past the requested length |
| RMT-009 | S2 | confirmed | [server.rs:686](../../src/remote/server.rs#L686) | Uploaded manifest/metadata members read whole. | fixed: client-sent manifests and metadata over 16 MiB are skipped |
| RMT-010 | S2 | confirmed | [server.rs:1135](../../src/remote/server.rs#L1135) | `WriteHostFile` truncates before the body arrives and doesn't check the size. | fixed: the upload goes to a `.name.rb-part` sibling, is checked against the announced size and renamed over |
| RMT-011 | S2 | confirmed | [server.rs:2079](../../src/remote/server.rs#L2079) | `open_write_target` drops the volume locks/DA claim at once. | fixed: the `DeviceWriteHandle` lives in the block handle |
| RMT-012 | S2 | plausible | [server.rs:363](../../src/remote/server.rs#L363) | Concurrent PUTs of one name share a staging dir. | deferred (plausible): needs a per-name staging lock |
| RMT-013 | S2 | plausible | [server.rs:1196](../../src/remote/server.rs#L1196) | No cross-connection exclusion on an image or device. | deferred (plausible): needs a daemon-wide open-target registry |
| RMT-014 | S2 | confirmed | [connection.rs:68](../../src/remote/connection.rs#L68) | `heartbeat` ignores a write failure. | fixed: an I/O error counts as a dead daemon |
| RMT-015 | S2 | confirmed | [block_reader.rs:252](../../src/remote/block_reader.rs#L252) | Unchecked signed seek arithmetic. | fixed: unsigned checked seek |
| RMT-016 | S2 | plausible | [server.rs:773](../../src/remote/server.rs#L773) | No per-connection handle limit. | deferred (plausible): a per-connection handle cap |
| RMT-017 | S3 | confirmed | [server.rs:800](../../src/remote/server.rs#L800) | `Hello` isn't required before other requests. | deferred: with RMT-001 (a required Hello is where a token would go) |
| RMT-018 | S3 | confirmed | [server.rs:2239](../../src/remote/server.rs#L2239) | `host_stat` is an existence oracle outside the root. | deferred: answering both outside-root cases alike needs a lexical check first |
| RMT-019 | S3 | confirmed | [server.rs:849](../../src/remote/server.rs#L849) | Em dashes in replies and logs. | fixed |
| RMT-020 | S3 | confirmed | [service.rs:341](../../src/remote/service.rs#L341) | Unquoted exe path in `user-startup.sh`. | fixed: the exe path is single-quoted |
| RMT-021 | S3 | plausible | [service.rs:471](../../src/remote/service.rs#L471) | `stop` trusts a PID file in /tmp. | deferred (plausible): the PID file wants a non-world-writable home |
| RMT-022 | S4 | confirmed | [server.rs:1438](../../src/remote/server.rs#L1438) | `reply_err`'s doc sits on `in_job`. | fixed |

# src/ review, slice 4 (branch `review-src-4`, started 2026-11-26)

`src/model`, 74 files. Four read-only reviewers: commander/browse/edit (MCM), sources and readers (MSR), write runners (MWR), jobs, limits and convert (MJB).

## src/model commander, browse and edit (MCM)

| ID | Sev | Status | Where | Finding | Fix |
|---|---|---|---|---|---|
| MCM-001 | S1 | confirmed | [archive_edit.rs:336](../../src/model/archive_edit.rs#L336) | Recompressing an edited backup partition writes over the original `partition-N.zst` first; a failed or cancelled compress destroys it. | fixed: compressed to a `.stem-rbnew` sibling and renamed into place on success |
| MCM-002 | S1 | confirmed | [commander_ops.rs:1675](../../src/model/commander_ops.rs#L1675) | Uploads to a remote host folder always force, silently overwriting the daemon's files with no conflict review. | fixed: the remote jobs carry `on_conflict`; the controller lists the destination's existing names and holds the upload for the same review a local copy gets, and each write checks the daemon (`put_remote`: Skip records, Fail stops, Replace overwrites). The TUI's blocking uploads keep overwriting. Test: the Skip case in `upload_host_tree_to_remote_host_over_the_wire` |
| MCM-003 | S1 | fixed | [container_edit.rs:206](../../src/model/container_edit.rs#L206) | A WOZ (and D88/DIM) commit rebuilds from flat sectors, dropping protection, timing, META and write-protect on the first edit. | edits stay allowed (owner's decision); every write path (Apply, a no-staging pane, Commander's save-and-close, Repair) first confirms what a flat re-encode drops (`container_edit::reencode_loss`); the CLI warns when it saves |
| MCM-004 | S1 | confirmed | [commander_ops.rs:1286](../../src/model/commander_ops.rs#L1286) | Host-to-host conflict check follows symlinks; a dangling link reads as no conflict and the copy writes through it. | fixed: the check uses `symlink_metadata`, and Replace removes the link rather than writing through it |
| MCM-005 | S1 | plausible | [archive_import.rs:221](../../src/model/archive_import.rs#L221) | A failed tar import returns without `sync_metadata`, leaving cached bitmap/directory state unwritten. | fixed: a failed import syncs and commits what landed and says so |
| MCM-006 | S2 | confirmed | [edit_queue.rs:385](../../src/model/edit_queue.rs#L385) | Staged SetTypeCreator goes through `from_utf8_lossy`; high-bit OSTypes become U+FFFD. | fixed: `disk-core` 0.3.3 adds `set_type_creator_raw` (ASCII default delegates to the text method); HFS, HFS+ and MFS write the bytes verbatim (`fs-mac` 0.2.2); the edit queue applies through it; test `high_bit_type_and_creator_codes_are_written_verbatim` |
| MCM-007 | S2 | fixed | [commander_ops.rs:1595](../../src/model/commander_ops.rs#L1595) | Remote directory paths are built from unsanitized image folder names (`..`, `/`). | image folder names are host-sanitized before the remote mkdir |
| MCM-008 | S2 | fixed | [edit_queue.rs:361](../../src/model/edit_queue.rs#L361) | Delete and metadata edits use the entry captured at staging; after an earlier Replace it is stale. | deletes, renames and metadata edits re-resolve their entry at apply time, the same node (`location`) first |
| MCM-009 | S2 | deferred | [dir_listing.rs:485](../../src/model/dir_listing.rs#L485) | Selection is by name, so duplicate names select and act on every match. | moves to the shell pass: selection by identity changes the listing API every shell uses |
| MCM-010 | S2 | fixed | [multi_volume_fs.rs:191](../../src/model/multi_volume_fs.rs#L191) | A malformed path lists the adapter root, which walkers re-enter forever. | a malformed path is NotFound, not the adapter root |
| MCM-011 | S2 | fixed | [commander_ops.rs:633](../../src/model/commander_ops.rs#L633) | Image walkers recurse with no depth cap. | walkers, the remote upload included, stop past 256 folders deep |
| MCM-012 | S2 | fixed | [mac_archive_read.rs:83](../../src/model/mac_archive_read.rs#L83) | Opening a file as an archive reads it whole (and clones it). | archives over `DECODE_MAX` (512 MiB) are refused before the whole read |
| MCM-013 | S2 | fixed | [archive_import.rs:56](../../src/model/archive_import.rs#L56) | Mac-archive extensions skip the `INLINE_MAX` guard. | a named archive over `DECODE_MAX` is staged as a plain file |
| MCM-014 | S2 | confirmed | [extract.rs:175](../../src/model/extract.rs#L175) | `Plan::run` handles only Skip; Fail overwrites files created since the pre-check. | fixed: a Fail run with conflicts stops before writing; Skip reaches the exporter for merged folders |
| MCM-015 | S2 | fixed | [commander_ops.rs:867](../../src/model/commander_ops.rs#L867) | Unreadable host folders copy as empty and report success. | an unreadable folder is left out and reported by name; the rest copies |
| MCM-016 | S2 | fixed | [edit_queue.rs:286](../../src/model/edit_queue.rs#L286) | MacBinary/BinHex imports never get their preserved Mac dates. | the MacBinary / BinHex path applies the staged Mac dates |
| MCM-017 | S2 | confirmed | [archive_edit.rs:195](../../src/model/archive_edit.rs#L195) | A checksum failure after recompress is dropped; metadata keeps the old checksum. | fixed: reported as an error |
| MCM-018 | S2 | fixed | [text_edit.rs:309](../../src/model/text_edit.rs#L309) | Forced UTF-8 on invalid bytes saves U+FFFD over them. | UTF-8 and Shift-JIS (forced or conventional) refuse invalid bytes instead of decoding lossily |
| MCM-019 | S2 | confirmed | [host_ops.rs:40](../../src/model/host_ops.rs#L40) | A case-only rename on a case-insensitive host fails as "already exists". | fixed: a rename onto the same file (case only) is allowed |
| MCM-020 | S2 | fixed | [commander_source.rs:216](../../src/model/commander_source.rs#L216) | Backup edit uses the first member only (split backups) and trusts metadata file names. | `commander_source::editable_member` refuses a split set or unsafe name, for the GUI and both CLI paths |
| MCM-021 | S2 | fixed | [edit_queue.rs:667](../../src/model/edit_queue.rs#L667) | Space preflight credits nothing for a recursive delete. | `bytes_needed_on` credits the deleted folder's tree on the volume |
| MCM-022 | S3 | fixed | [container_edit.rs:229](../../src/model/container_edit.rs#L229) | The atomic replace doesn't fsync and replaces the original's permissions. | the new container is fsynced and takes the original's permissions |
| MCM-023 | S3 | fixed | [commander_ops.rs:286](../../src/model/commander_ops.rs#L286) | Staged MacBinary imports hold the data fork in RAM and are cloned for preflight. | preflight reads the edits in place (`scan_edits`, `bytes_needed_on`) |
| MCM-024 | S3 | fixed | [browse_session.rs:777](../../src/model/browse_session.rs#L777) | Alto/Trident packs and NDIF forks are read whole. | a Trident pack (76 / 289 MB) opens file-backed in fs-xerox 0.3.0 (`open_pack_file`; sectors read on demand through a VDA map) for browse, inspect, fsck and resize; the small containers still load whole; an edit loads the pack (the BFS writer rebuilds the volume); a nested NDIF rebuilds through temp files |
| MCM-025 | S4 | fixed | [archive_edit.rs:118](../../src/model/archive_edit.rs#L118) | Async and sync recompress/extract paths have diverged; name sanitizing hand-rolled three times. | the threaded extract / recompress call the sync bodies; `model::host_component` replaces three name sanitizers (none caught `..`) |

## src/model sources and readers (MSR)

| ID | Sev | Status | Where | Finding | Fix |
|---|---|---|---|---|---|
| MSR-001 | S1 | confirmed | [resize_remote.rs:64](../../src/model/resize_remote.rs#L64) | A remote resize past the partition only warns, then grows metadata into the next partition. | fixed: refused, as the local verb does |
| MSR-002 | S1 | confirmed | [resize_remote.rs:71](../../src/model/resize_remote.rs#L71) | A remote shrink skips the `last_data_byte` floor. | fixed: the filesystem is asked where its data ends first |
| MSR-003 | S2 | fixed | [source_reader.rs:699](../../src/model/source_reader.rs#L699) | The lazy `.cbk` reader serves zstd members as raw partition bytes. | only gzip / uncompressed members open lazily; others take the full path |
| MSR-004 | S2 | fixed | [source_reader.rs:782](../../src/model/source_reader.rs#L782) | The lazy `.cbk` reader zero-fills a short or split member silently. | a split member falls back; a short member is an UnexpectedEof error |
| MSR-005 | S2 | fixed | [source_scan.rs:193](../../src/model/source_scan.rs#L193) | Minimum-size probes read the raw container file, not the decoded disk. | probes open through the image-format wrapper |
| MSR-006 | S2 | fixed | [inspect_runner.rs:255](../../src/model/inspect_runner.rs#L255) | Device probes skip the sector-aligned reader, losing labels on raw devices. | device probes go through `SectorAlignedReader` |
| MSR-007 | S2 | fixed | [inspect_runner.rs:253](../../src/model/inspect_runner.rs#L253) | Every probe re-decodes a streaming container. | probes share one decoder; each handle seeks to its own position |
| MSR-008 | S2 | fixed | [chd_edit_open.rs:37](../../src/model/chd_edit_open.rs#L37) | A crash mid-copy leaves a truncated `.chd_backup` that the next edit trusts. | backup copies go through a `.part` name, size-checked, then renamed |
| MSR-009 | S2 | fixed | [cache_runner.rs:76](../../src/model/cache_runner.rs#L76) | Cancel leaves the scan thread running; a reopen starts a second one. | dsk-clonezilla 0.2.2 `scan_metadata_cancellable`; a cancel stops the scan and waits for it |
| MSR-010 | S2 | fixed | [cache_runner.rs:112](../../src/model/cache_runner.rs#L112) | The seekable cache is written in place and may swallow a late write error. | the cache is built under `.part`, synced, then renamed |
| MSR-011 | S2 | fixed | [optical_ops.rs:241](../../src/model/optical_ops.rs#L241) | CHD rips stage to fixed names, so two rips into one folder collide. | rip temps are named after the CHD |
| MSR-012 | S2 | fixed | [source_reader.rs:1178](../../src/model/source_reader.rs#L1178) | Single-member gzip decoders read only the first member of a multi-member file. | `source_reader::gz_members` reads every member and stops at trailing padding |
| MSR-013 | S3 | fixed | [source_reader.rs:1232](../../src/model/source_reader.rs#L1232) | Model imports from `crate::cli`. | `looks_like_device_path` moved to `os`; the CLI re-exports it |
| MSR-014 | S3 | confirmed | [source_reader.rs:1033](../../src/model/source_reader.rs#L1033) | `eprintln!` in model code. | fixed: `log::warn!` |
| MSR-015 | S3 | fixed | [image_prep.rs:181](../../src/model/image_prep.rs#L181) | The `.hdz` gunzip can't be cancelled. | the gunzip checks the job's cancel per read |
| MSR-016 | S3 | fixed | [cd_build.rs:239](../../src/model/cd_build.rs#L239) | SGI/HFS CD layouts truncate the output before validating. | the blank disc is written to `.rbpart` and renamed only when whole |
| MSR-017 | S3 | fixed | [restore_remote.rs:61](../../src/model/restore_remote.rs#L61) | Remote restore stages to the system temp dir (tmpfs) unchecked. | free space is checked against the backup's imaged data (the stage is sparse) |
| MSR-018 | S3 | fixed | [optical_devices.rs:66](../../src/model/optical_devices.rs#L66) | `rb://` device args miss a separator for Windows paths. | `remote_device_url` puts a `/` before a Windows device; `resolve` drops it again |
| MSR-019 | S4 | confirmed | [backup_loader.rs:146](../../src/model/backup_loader.rs#L146) | Every GPT backup logs "Could not parse mbr.bin". | fixed: a GPT backup's protective-MBR parse failure is not reported |
| MSR-020 | S4 | fixed | [checksum.rs:188](../../src/model/checksum.rs#L188) | `checksum::spawn` is dead; `hash_reader` aborts on Interrupted. | the dead `spawn` is gone; `hash_reader` retries `Interrupted` |

## src/model write runners (MWR)

| ID | Sev | Status | Where | Finding | Fix |
|---|---|---|---|---|---|
| MWR-001 | S1 | confirmed | [physical_write_runner.rs:182](../../src/model/physical_write_runner.rs#L182) | A whole-disk write truncates its target before reading the source; the source as target is emptied. | fixed: refused via `model::same_file` |
| MWR-002 | S1 | confirmed | [resize_image.rs:858](../../src/model/resize_image.rs#L858) | In place, Rebuild zeroes the old HFS head while the rebuilt volume lives in an anonymous tempfile; a later failure loses it. | fixed: the rebuilt volume is a named file beside the image, kept and reported on failure |
| MWR-003 | S1 | confirmed | [hfs_expand_runner.rs:300](../../src/model/hfs_expand_runner.rs#L300) | APM output truncates before the source is reopened; output = source destroys it. | fixed: refused |
| MWR-004 | S1 | confirmed | [repack_runner.rs:68](../../src/model/repack_runner.rs#L68) | The Human68k defragment writes the BPB's size, unbounded by the partition. | fixed: the partition size is passed and enforced |
| MWR-005 | S1 | plausible | [provision_runner.rs:842](../../src/model/provision_runner.rs#L842) | A file target is truncated before its sources or donor are read; no same-file check. | fixed: refused |
| MWR-006 | S1 | fixed | [restore_plan.rs:667](../../src/model/restore_plan.rs#L667) | A new disk is sized from a container's file length, not its decoded length. | a VHD / 2MG source is sized from its header; other files by length |
| MWR-007 | S1 | by design | [resize_image.rs:663](../../src/model/resize_image.rs#L663) | In-place `apply_resize` has no rollback on an I/O error mid-move. | in place stays (owner's decision); Review, the confirm and the CLI plan say it isn't crash-safe and point to a copy |
| MWR-008 | S2 | fixed | [resize_image.rs:782](../../src/model/resize_image.rs#L782) | CHD copy packs to a stripped-extension sibling that may clobber another file. | a copy packs to a hidden `.NAME.resize-pack` and renames onto the target |
| MWR-009 | S2 | plausible | [resize_image.rs:572](../../src/model/resize_image.rs#L572) | A failed Copy step removes a pre-existing target. | fixed: a target that existed before the run is never removed |
| MWR-010 | S2 | confirmed | [disk_builder.rs:114](../../src/model/disk_builder.rs#L114) | `Donor::read` allocates the donor's head unbounded. | fixed: a head past 256 MiB (or the disk) is refused |
| MWR-011 | S2 | fixed | [export_runner.rs:868](../../src/model/export_runner.rs#L868) | Shrinking exports run general resizers on a cut file. | a shrinking export copies the whole volume, resizes, then cuts the file (image source, and Raw from a backup; a backup to VHD/2MG still streams to size) |
| MWR-012 | S2 | confirmed | [export_runner.rs:857](../../src/model/export_runner.rs#L857) | Per-partition export reads at `start_lba*512`, not `byte_offset()`. | fixed for the raw export (`byte_offset`); the HFV branch and HFS-ceiling probe still read at `start_lba*512` |
| MWR-013 | S2 | fixed | [partition_editor.rs:419](../../src/model/partition_editor.rs#L419) | A pending row's edited size isn't what is written. | a new row's size field is parsed and written |
| MWR-014 | S2 | fixed | [disk_write_plan.rs:228](../../src/model/disk_write_plan.rs#L228) | Hidden sectors read from the raw container file. | hidden sectors read through `open_peeled_read_with_entry` |
| MWR-015 | S2 | fixed | [swab_runner.rs:44](../../src/model/swab_runner.rs#L44) | No canonical same-file check before `File::create(dst)`. | an output that is the input is refused before it is created |
| MWR-016 | S2 | confirmed | [chd_expand_runner.rs:63](../../src/model/chd_expand_runner.rs#L63) | `old_logical + add_bytes` unchecked. | fixed: checked |
| MWR-017 | S3 | fixed | [partition_editor.rs:411](../../src/model/partition_editor.rs#L411) | An unparseable MBR type on a new row becomes 0x83 silently. | an unparseable MBR type on a new row is an error |
| MWR-018 | S3 | fixed | [provision_runner.rs:792](../../src/model/provision_runner.rs#L792) | A failed partitioned build leaves its half-built image. | a failed build removes the image it created or truncated |
| MWR-019 | S3 | fixed | [alto_resize.rs:65](../../src/model/alto_resize.rs#L65) | Files are read whole before validation. | a file over 512 MiB (past a Trident T-300) is refused before it is read |
| MWR-020 | S4 | fixed | [physical_write_runner.rs:100](../../src/model/physical_write_runner.rs#L100) | `target_size_bytes` is never read. | a device write refuses a region past `target_size_bytes` |
| MWR-021 | S4 | fixed | [export_runner.rs:954](../../src/model/export_runner.rs#L954) | A doc comment merged into its neighbour's. | the doc comment is back on `refuse_unpatched_shrink` |
| MWR-022 | S4 | fixed | [hfs_expand_runner.rs:121](../../src/model/hfs_expand_runner.rs#L121) | `lock().expect` on a status mutex. | `lock_status` instead of `lock().expect` |

## src/model jobs, limits and convert (MJB)

| ID | Sev | Status | Where | Finding | Fix |
|---|---|---|---|---|---|
| MJB-001 | S1 | confirmed | [run.rs:48](../../src/model/convert/run.rs#L48) | A refused output-is-source conversion runs `remove_partial`, deleting the source. | fixed: the cleanup never removes the source or its BIN |
| MJB-002 | S2 | fixed | [min_size_runner.rs:142](../../src/model/min_size_runner.rs#L142) | Concurrent minimum calcs share one file cursor through `try_clone`. | workers read through `os::PositionalFile` (positioned reads, own cursor) |
| MJB-003 | S2 | fixed | [mod.rs:576](../../src/model/convert/mod.rs#L576) | `disk.vhd` and `disk.qcow2` both map to `disk.img`; the second overwrites the first. | repeated destinations (case-folded) get a `-{srcext}` suffix, shown on the row |
| MJB-004 | S2 | fixed | [stage_names.rs:83](../../src/model/stage_names.rs#L83) | A legal name isn't checked against names already taken. | a legal name that clashes with an earlier rename in the batch is made unique |
| MJB-005 | S2 | fixed | [fs_limits.rs:252](../../src/model/fs_limits.rs#L252) | exFAT limit arithmetic unchecked. | checked arithmetic; a corrupt head gives no limit |
| MJB-006 | S2 | fixed | [classify.rs:110](../../src/model/convert/classify.rs#L110) | Kind decided by size alone; a small hard disk is a floppy. | a floppy-sized image with an MBR / GPT / RDB / APM table is a hard disk |
| MJB-007 | S2 | fixed | [classify.rs:233](../../src/model/convert/classify.rs#L233) | gzip ISIZE is modulo 4 GiB. | under 4 MiB the trailer is exact; a larger gzip is counted by decoding |
| MJB-008 | S2 | fixed | [verify.rs:205](../../src/model/convert/verify.rs#L205) | Disc verify passes when nothing was compared. | nothing compared (no filesystem, no files) reports Not verified |
| MJB-009 | S2 | fixed | [verify.rs:138](../../src/model/convert/verify.rs#L138) | Volume verify skips unopenable partitions and compares names only. | contents compared by CRC-32 where readable, cancellable per file; unreadable partitions are reported |
| MJB-010 | S2 | fixed | [job.rs:245](../../src/model/job.rs#L245) | Rate and ETA freeze during a stall. | snapshots re-read the rate as of now (`rate_as_of`) |
| MJB-011 | S2 | confirmed | [mod.rs:416](../../src/model/convert/mod.rs#L416) | `minimum / 100 * p` unchecked. | fixed: saturating |
| MJB-012 | S2 | fixed | [disk_tree.rs:53](../../src/model/disk_tree.rs#L53) | One unresolvable position fails the whole tree. | an unresolvable position is reported on its row |
| MJB-013 | S3 | fixed | [disk_tree.rs:9](../../src/model/disk_tree.rs#L9) | Model imports `crate::cli` and logs to stderr. | `resolve`, `backup_edit`, `img_at`, `optical_hint` and the image open helpers live in `model` (the CLI re-exports them); notes go through `model::notice`, refusals through `model::fault`; no model file imports `crate::cli` |
| MJB-014 | S3 | fixed | [min_size_runner.rs:104](../../src/model/min_size_runner.rs#L104) | Raw `thread::spawn` instead of a Job; poison leaves it unfinished. | `spawn_guarded`, and every status lock survives poison |
| MJB-015 | S3 | fixed | [job.rs:298](../../src/model/job.rs#L298) | `ask` spins forever if the Job handle is dropped without cancel. | `ask` gives up once no `Job` handle is left |
| MJB-016 | S4 | confirmed | [file_types.rs:25](../../src/model/file_types.rs#L25) | `DISK_IMAGE_EXTS` lacks `msa` and `st`. | fixed: `msa` and `st` added with a regression test |
| MJB-017 | S4 | fixed | [classify.rs:67](../../src/model/convert/classify.rs#L67) | A third extension list drifting from the others. | the list names only extensions the engine opens; a test keeps it inside the picker and disc-crate lists |
| MJB-018 | S4 | fixed | [file_types.rs:123](../../src/model/file_types.rs#L123) | `.iso`/`.bin` registered as associations. | `.iso` and `.bin` are not associations (Windows, AppKit plist); unregister clears every old marker |
| MJB-019 | S4 | fixed | [disk_tree.rs:160](../../src/model/disk_tree.rs#L160) | Remote tree omits LVM rows. | the remote count includes LVM volumes |
| MJB-020 | S4 | fixed | [run.rs:536](../../src/model/convert/run.rs#L536) | HFS ceiling probed through container bytes. | the HFS ceiling is probed through a CHD's decoder (other containers stay as files: this runs on the UI thread) |
| MJB-021 | S4 | fixed | [mod.rs:611](../../src/model/convert/mod.rs#L611) | Folder walk follows symlink loops. | linked folders are not followed |
| MJB-022 | S4 | fixed | [classify.rs:151](../../src/model/convert/classify.rs#L151) | The text size gate is dead. | the cue-sheet size gate reads the file's size |
