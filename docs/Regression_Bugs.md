# Regression Findings (R-001 … R-027)

Defects and documentation drift turned up while building the regression suite
(`regression-tests/`), 2026-08-01/02. The suite work was deliberately kept
separate from bug fixing, and nothing outside `regression-tests/` was modified
while finding them. Fixes land afterwards, one finding at a time; a fixed entry
is struck through in the table and keeps its original report below the
**FIXED** note, so the reproduction stays readable.

Capability gaps — things the engine never claimed to do — are tracked
separately in [`missing_features_from_regression.md`](missing_features_from_regression.md).
This file is for defects: code disagreeing with its own documentation or
with reality.

Every entry was reproduced against `target/release/rb-cli` on Windows. Where a
finding depends on a fixture, the fixture is named.

## Summary

| ID | Severity | Area | Finding |
|----|----------|------|---------|
| [R-019](#r-019) | Low — **accepted** | `src/rbformats/vhd.rs` | VHD Creator Host OS makes output non-reproducible across platforms; behaviour kept, parity declares it |
| [R-023](#r-023) | **High** | `src/cli/verbs/repack.rs` | `repack` loses every file in the volume |
| [R-022](#r-022) | **High** | `src/fs/hpfs.rs` | HPFS sector-by-sector backup -> restore is not byte-identical |
| [R-021](#r-021) | **High** | `src/cli/verbs/resize.rs` | `resize --size` reports success and changes nothing |
| [R-024](#r-024) | Medium | `src/fs/affs.rs` | AFFS `put` leaves the volume failing its own fsck |
| [R-025](#r-025) | Medium | `src/fs/squashfs_edit.rs` | `squashfs put` fails to replace the image on Windows |
| [R-026](#r-026) | Low | `src/cli/verbs/show.rs` | `show partmap` cannot read an SGI disk that `inspect` reads fine |
| [R-027](#r-027) | Medium | `src/rbformats/zip_disk.rs` | A Finder-made `.zip` holding one `.dmg` is rejected as ambiguous |
| [R-020](#r-020) | **High** | `src/fs/affs.rs` | `new volume affs` output is "Not a DOS disk" on a real Amiga, at every size |
| [R-016](#r-016) | **High** | `src/cli/verbs/backup.rs` | `backup` accepts only flat-layout sources: CHD, dynamic VHD, QCOW2 and VMDK all fail |
| ~~R-018~~ | ~~Blocker~~ **FIXED** | `CONTRIBUTING.md` | ~~The documented Rust-1.73 verification build does not compile on Windows~~ — missing `windows-legacy` feature, 2026-08-07 |
| ~~R-017~~ | ~~High~~ **FIXED** | `src/partition/mod.rs` | ~~Superfloppy detection also misses SFS (extends R-009)~~ — probe added 2026-08-07 |
| [R-015](#r-015) | Medium | `src/optical/` (cue parser) | A `.cue` with unpadded track numbers (`TRACK 1`) is rejected |
| ~~R-014~~ | ~~Blocker~~ **FIXED** | `src/cli/verbs/squashfs.rs` | ~~Pre-existing clippy failure blocks every commit via the pre-commit hook~~ — boxed 2026-08-07 |
| [R-008b](#r-008b) | **High** | `src/fs/affs.rs` | `new volume affs --size 4M` panics; no file produced, exit 101 |
| ~~R-007~~ | ~~High~~ **FIXED** | `src/fs/ntfs_format.rs` | ~~Freshly formatted NTFS fails its own fsck~~ — verified clean 2026-08-07 |
| ~~R-009~~ | ~~High~~ **FIXED** | `src/partition/mod.rs` | ~~Bare JFS / UFS1 / UFS2 / ReiserFS images cannot be opened at all~~ — probes added 2026-08-07 |
| [R-013](#r-013) | **High** | `src/fs/ufs.rs` | Solaris UFS directories reported as files, one with a garbage size |
| [R-005](#r-005) | Medium | `src/cli/output.rs` | No error envelope emitted under `--format json` |
| [R-008a](#r-008a) | Medium | `src/fs/affs.rs` | AFFS volumes above 4066 blocks have uncovered tail blocks |
| [R-012](#r-012) | Medium | `src/optical/` | `optical info` rejects any disc with no data track (pure CD-DA) |
| [R-003](#r-003) | Medium | `src/cli/output.rs` | Docs claim `ls` supports `--format`; it does not |
| [R-010](#r-010) | Medium | `src/cli/verbs/inspect.rs` | `inspect` has no `--fs-type`, so CP/M images cannot be inspected |
| [R-006](#r-006) | Medium | `src/cli/verbs/new.rs` | `new volume prodos` always fails with default arguments |
| [R-004](#r-004) | Low | `src/cli/output.rs` | CSV/TSV rejection exits 1, documented as 2 |
| [R-011](#r-011) | Unknown | `src/rbformats/` | G64 decoding fails on copy-protected / patched dumps |
| [R-001](#r-001) | Doc | `README.md` | Partition-table list missing AHDI and X68000 |
| [R-002](#r-002) | Doc | `src/fs/README.md` | Capability table stale — ext listed as "planned" |

---

## Blocker

### R-018 — the vintage-build check does not compile on Windows {#r-018}

**FIXED 2026-08-07 — and it was documentation drift, not a code defect.** The
command was missing `windows-legacy` from its `--features` list. That feature
already exists in both manifests and already does exactly the right thing:
`src/os/windows.rs` carries `#[cfg]`-split call sites for `CreateWellKnownSid`
and `ShellExecuteW`, and CI's Windows vintage leg has been passing the flag all
along. Only the CONTRIBUTING.md command had drifted from it.

Adding the flag builds clean — zero errors. CONTRIBUTING.md now quotes CI's
exact feature list (`native-zstd,remote,tui,rust173-polyfill,windows-legacy,yaml`),
verified locally, with a note that the two must stay together. `windows-legacy`
gates only `#[cfg(windows)]` code, so it is inert on the macOS 10.7 leg and the
single command is correct on every platform.

Correcting the original report below: it concluded "both call sites need a
`#[cfg]`-split shim in `src/os/windows.rs`". They already had one. The error
text names the call sites, which reads as missing shim code, and the report
stopped at the symptom instead of checking whether the feature that fixes it
existed. Worth recording — the finding was real and the diagnosis was not.

Original report follows.

CONTRIBUTING.md § "Rust 1.73 floor for engine code" gives this as the command
to run before pushing any change under `src/`. It fails on a clean `HEAD`:

```
cargo build --manifest-path rb-cli-vintage/Cargo.toml \
  --no-default-features --features native-zstd,remote,tui,rust173-polyfill \
  --ignore-rust-version

error[E0308]: mismatched types
   --> ..\src\os\windows.rs:178:13    expected `PSID`, found `Option<PSID>`
error[E0271]: type mismatch resolving `<Option<HWND> as TypeKind>::TypeKind == CopyType`
   --> ..\src\os\windows.rs:233:9
error: could not compile `rb-cli-vintage` (lib) due to 4 previous errors
```

Not a 1.73 std-API violation — a `windows`-crate version skew. The vintage
manifest pins a release whose `CreateWellKnownSid` / `ShellExecuteW` take bare
`PSID` / `HWND`, while `src/os/windows.rs` passes the `Option<..>` forms the
modern crate wants. Both call sites are `#[cfg(windows)]`, so this is a
Windows-only failure; the macOS-10.7 leg of the vintage target is unaffected,
which is presumably why it has survived.

The effect is that the *only* documented way to catch a 1.73 violation is
itself red on Windows, so a real violation is indistinguishable from this
noise and the check gets skipped. Both call sites need a `#[cfg]`-split shim
in `src/os/windows.rs`, the same pattern `crate::compat` already uses.
*(Wrong — see the correction above. The shims were already there.)*

Confirmed pre-existing: identical four errors with all local work stashed.

Discovered 2026-08-07 running the check after the R-009 / R-017 fix.

### R-014 — clippy fails on `SquashfsCommand`, blocking every commit {#r-014}

**FIXED 2026-08-07.** `Put` now holds a `Box<SquashfsPutArgs>`; clippy's own
suggestion, applied verbatim. `cargo clippy --all-targets -- -D warnings`
passes clean — boxing only `Put` did *not* move the complaint to `Rm`, so the
192-byte variant was left alone. clap parses the boxed variant unchanged
(`clap::Args` is implemented for `Box<T>`), and the single match arm needed no
edit: `Box` supports the partial field moves it already did. Original report
follows.

`cargo clippy --all-targets -- -D warnings` fails on a clean tree:

```
error: large size difference between variants
  --> src\clierbs\squashfs.rs:28:1
   |
28 | pub enum SquashfsCommand {
33 |     Put(SquashfsPutArgs),
   |     -------------------- the largest variant contains at least 400 bytes
35 |     Rm(SquashfsRmArgs),
   |     ------------------ the second-largest variant contains at least 192 bytes
   |
   = note: `-D clippy::large-enum-variant` implied by `-D warnings`
error: could not compile `rusty-backup` (lib) due to 1 previous error
```

The repository's pre-commit hook runs exactly that command, so **no commit
can be made until this is resolved** — including commits that touch nothing
but `tests/`.

Confirmed pre-existing: it reproduces on a clean `HEAD` with all local work
stashed. The lint URL names `rust-1.96.0`, so this is a newer clippy
tightening `large_enum_variant` against code that has not changed, rather
than a recent regression.

Clippy's own suggestion is a one-line fix:

```rust
Put(Box<SquashfsPutArgs>),
```

`Rm` at 192 bytes may want the same treatment, and boxing only `Put` may
simply move the complaint to the next-largest variant — worth checking the
lint passes rather than assuming.

Not fixed when found: this file is engine code, and the session that found it
was scoped to `regression-tests/` with an explicit instruction not to change
anything outside it. Flagged rather than silently widening that scope, then
fixed in the following session once the scope was lifted.

Discovered 2026-08-07 while committing `tests/cli_suite/cli_native_slots.rs`,
which was written, passing and mutation-verified but could not be committed
until this cleared.

---

## Low

### R-019 — VHD output is not byte-reproducible across platforms {#r-019}

Found by the first three-OS `parity` run, 2026-08-07. 101 of 105 comparisons
matched; all four divergences were VHD, and all four were the same four bytes.

```
DIFF  fmt.vhd-dynamic   macos vs windows   8 byte(s) outside the mask
        @0x00000024  4d != 57      "M" vs "W"
        @0x00000025  61 != 69      "a" vs "i"
        @0x00000026  63 != 32      "c" vs "2"
        @0x00000027  20 != 6b      " " vs "k"
```

Footer offset 0x24 is **Creator Host OS**. macOS writes `"Mac "`, Windows and
Linux write `"Wi2k"`. Dynamic VHDs carry the footer twice, so they diverge in
eight bytes rather than four.

This is deliberate — `src/rbformats/vhd.rs:82-86` writes it explicitly — and
spec-legal: the field is *meant* to record the creating host. So it is not a
correctness defect, and both `qemu-img info` and our own reader accept every
variant. It is recorded because of the consequence and the inconsistency:

1. **A VHD built on a Mac never checksums equal to the same VHD built on
   Windows.** Anything comparing images across machines has to know that.
2. **The policy is applied unevenly.** Linux writes `"Wi2k"` with the comment
   "use Windows ID (most compatible)" — a deliberate lie for compatibility.
   macOS writes the truthful `"Mac "`. Whichever principle is right,
   compatibility or fidelity, only one of the two platforms is following it.

**Resolved 2026-08-07 by keeping the behaviour and declaring it.** The field
is spec-legal and the divergence is intentional, so `vhd.rs` is untouched.
`produce.toml` now carries an `expect_divergence` declaration for both VHD
recipes and `parity` honours it.

The exemption is deliberately narrow and loud: it covers 4 bytes rather than
the footer or the format, a byte one past the declared range is still a
finding, and every match that used it prints the reason. Counts stay separate
— `identical outside 18 masked + 4 expected byte(s)` — so an exemption can
never be mistaken for agreement.

Point 2 above still stands as an open inconsistency: Linux lies for
compatibility, macOS tells the truth. Nothing depends on resolving it, but if
uniformity is ever wanted, the declaration is one line to delete.

The creation timestamp at footer offset 0x18 also differs, as expected, and
was correctly absorbed by the volatile-range mask — 18 masked bytes on
`fmt.vhd-fixed`. That is the mask doing its job, and the reason these four
bytes stood out at all.

---

## Found by the tier-3 sweep, 2026-08-08

Six findings from the first run of the round-trip, edit and resize cases —
the surface COMMAND-COVERAGE.md showed was untested. Recorded, not diagnosed:
each names its reproduction and stops there.

### R-021 — `resize --size` reports success and does nothing {#r-021}

```
rb-cli new volume fat --size 8M v.img
rb-cli resize v.img --size 16M     -> "resize complete", exit 0
rb-cli inspect v.img               -> FAT   8.0 MiB
```

Exit 0 and a success message for work that did not happen. Silent no-ops are
the worst shape of bug in a tool whose job is moving data: nothing downstream
has any reason to check. Case `resize.to-explicit-size`.

### R-022 — HPFS does not survive a sector-by-sector round-trip {#r-022}

`backup --sector-by-sector` then `restore` returns bytes that differ from the
source. Every other filesystem tested — FAT, NTFS, ext4, HFS, minix3, EFS,
ProDOS — comes back byte-identical through the same path, so this is specific
to HPFS rather than to the backup format. `--sector-by-sector` asks for a
faithful image, so any difference is a defect. Case `roundtrip.hpfs.raw`.

### R-023 — `repack` loses every file {#r-023}

```
rb-cli put v.img payload.bin /payload.bin
rb-cli repack v.img                -> exit 0
rb-cli get v.img /payload.bin      -> error: path component not found: payload.bin
```

Exit 0, and the data is gone. Case `resize.repack.keeps-data`.

### R-024 — AFFS `put` leaves the volume failing its own fsck {#r-024}

A single `put` into a freshly formatted 3 MB AFFS volume — the largest size
R-008 leaves working — makes `fsck --checkonly` report
`1 error(s), 1 warning(s) (some repairable)`. The file reads back correctly,
so the damage is to the allocation structures rather than the data. Related to
but distinct from R-008a/b (sizing) and R-020 (root block); this is the
editor, not the formatter. Case `edit.affs.put-get`.

### R-025 — `squashfs put` cannot replace the image on Windows {#r-025}

```
error: sync_metadata: I/O error: replacing <path>
```

SquashFS edits rebuild the whole image and swap it in atomically (temp +
fsync + rename). The rename fails on Windows, so the edit path is unusable
there. Case `subcmd.squashfs.put-rebuilds`. `squashfs create` and `verify`
both pass, so it is specific to the replace step.

### R-026 — `show partmap` cannot read an SGI disk {#r-026}

```
rb-cli new hd sgi-efs --size 16M d.img
rb-cli inspect d.img        -> reads the SGI volume header fine
rb-cli show partmap d.img   -> error: parsing APM: Invalid APM: bad DDR signature: 0x0BE5
```

0x0BE5 is the leading half of the SGI volume-header magic, so `show partmap`
is trying APM on a disk it should have recognised. `inspect` gets it right on
the same file, so the detection that exists is simply not being used here.
Case `subcmd.show.partmap`.

### R-027 — a Mac-made `.zip` holding one `.dmg` is called ambiguous {#r-027}

Found 2026-08-08, the first time `fs.apfs.apple-gpt.hd` was ever executed —
it was catalogued and checksummed but no run had reached it.

```
rb-cli inspect fs.apfs.apple-gpt.hd.dmg.zip
  -> error: ZIP has no obvious disk image and contains multiple files;
     pass --inside <name> to pick one. Entries:
       APFS_Image.dmg (524288000 bytes)
       __MACOSX/._APFS_Image.dmg (219 bytes)
```

Control: `--inside APFS_Image.dmg` reads the disk correctly (GPT, Apple APFS
at LBA 40, 500 MiB), so neither the archive nor the ZIP reader is at fault.

Two things line up to produce it, both in `is_disk_image_entry` /
`pick_entry` in `src/rbformats/zip_disk.rs`. `.dmg` is absent from the `EXTS`
list, so the archive's only real image is not recognised as one; and the
zero-candidate fallback accepts a single-entry archive without first
discarding AppleDouble stubs, which the `__MACOSX/._` sidecar defeats. Either
alone would be survivable — together, every disk image zipped in Finder needs
`--inside`. `is_apple_double` already exists and is applied in the wrong
place. Note also that `EXTS` is a second hardcoded extension list living
apart from `DISK_IMAGE_EXTS` in `src/model/file_types.rs`.

Case `read.apfs.apple-gpt`.

---

## High

### R-020 — every AFFS volume we write is unmountable on a real Amiga {#r-020}

Found 2026-08-07 by the first FS-UAE emulator-oracle run — the first result
from an oracle that is not a command-line tool, and the reason that oracle
was built.

`rb-cli new volume affs` output, attached to an A1200 under Kickstart 3.1:

```
Mounted disks:
Unit       Size     Used     Free Full Errs   Status   Name
DH1:      Not a DOS disk
```

**The control rules out the harness.** A real MiSTer FFS volume
(`Mister-3-2.hdf`, bare, no RDB, same shape as ours) mounted through the
identical config and geometry:

```
DH1:      1999M   278618  3817380   7%   0  Read/Write Amiga32
```

Same emulator, same config, same mount path — only the image differs.

**It is not R-008a.** That finding says AFFS breaks above 4066 blocks. A
volume of *exactly* 4066 blocks (`--size 2033K`), which our own fsck reports
as `0 files / 1 dirs checked`, exit 0, is equally "Not a DOS disk". The defect
is present at every size, independent of the bitmap bug.

**Root cause.** Comparing our root block against the working volume's:

| offset | field | ours | working |
|--------|-------|------|---------|
| 0x00 | type | `00000002` (T_HEADER) | `00000002` |
| **0x04** | **header_key** | **`00000800` = 2048** | **`00000000`** |
| 0x0C | ht_size | `00000048` (72) | `00000048` |
| 0x1FC | sec_type | `00000001` (ST_ROOT) | `00000001` |

An AFFS root block's `header_key` must be **zero**. We write the root block's
own block number into it. AmigaDOS's FFS validates that field and rejects the
volume outright — which is why the failure is "Not a DOS disk" (identification)
rather than a structural complaint.

**Why nothing caught it.** `rb-cli fsck` passes these volumes, and the tier-1
case `fs.new-volume.affs` only ever asked rb-cli to read back what rb-cli
wrote. Formatter and fsck share the same wrong assumption about `header_key`,
so they agree with each other and disagree with every real Amiga. This is
exactly the failure mode `README.md` warns tier 1 cannot detect — "a bug on
both sides cancels out" — demonstrated rather than hypothesised.

The fix must land in both places: the formatter must write 0, and `affs_fsck`
must reject a non-zero `header_key` so the case cannot silently pass again.

Reproduction: `regression-tests/oracles/fsuae/` (config template and guest
probe). The guest writes its verdict to a host directory mounted as an Amiga
volume, so no screen-scraping is involved.

### R-008b — `new volume affs` panics at 4 MB and above {#r-008b}

AFFS puts the root block at the volume midpoint. Once that index passes the
single bitmap page the "mark in-use" loop indexes past the 512-byte block.
Unlike the loop immediately below it, this one has no `word_idx >= 128` guard.

```
rb-cli new volume affs --size 4M v.img
  thread 'main' panicked at src/fs/affs.rs: range end index 516 out of range
  for slice of length 512
  -> exit 101, no file created
```

The arithmetic matches exactly:

| size | blocks | root | bit | word_idx | slice access | message |
|------|-------:|-----:|----:|---------:|--------------|---------|
| 4 MB | 8192 | 4096 | 4094 | 128 | `bm[512..516]` | "range end index 516" |
| 8 MB | 16384 | 8192 | 8190 | 256 | `bm[1024..1028]` | "range start index 1024" |

Works up to 3 MB; broken at 4 MB and above. A user asking for an ordinary
4 MB Amiga volume gets a stack trace and no file. Exit 101 is not in the
exit-code contract (`src/cli/exit.rs`) at all.

### R-007 — FIXED: a freshly formatted NTFS volume failed its own fsck {#r-007}

**Fixed.** Re-verified 2026-08-07: `new volume ntfs --size 2M` then
`fsck --checkonly` reports `0 files / 0 dirs checked`, exit 0. The three
cases (`fs.new-volume.ntfs`, `.2m-fsck`, `.32m-fsck`) went green without
being touched — the suite noticing a fix is as useful as it noticing a break.
They stay in place as regression guards.

Original report follows.

```
rb-cli new volume ntfs --size 2M v.img
rb-cli fsck v.img --checkonly
  ERROR [BitmapLeaked] 16 cluster(s) marked allocated in $Bitmap but referenced by nothing
  ERROR [BackupBootMismatch] backup boot sector (last sector) does not match the VBR
  -> exit 1
```

Either the formatter or the fsck is wrong; they cannot both be right, and
`format -> fsck` on an untouched volume must be clean.

`BackupBootMismatch` is the more serious half: real NTFS keeps a copy of the
VBR in the volume's last sector and Windows checks it. A volume whose backup
boot sector does not mirror the VBR is not one `chkdsk` will accept, which
undercuts the point of writing NTFS.

### R-016 — `backup` accepts only flat-layout containers {#r-016}

`backup --help` documents SOURCE as "an image file or a block-device path",
and `inspect` reads every container we write. `backup` reads only the ones
whose data begins at offset 0:

| source | `inspect` | `backup` |
|--------|-----------|----------|
| raw `.img` | `Partition table: MBR` | **ok** |
| fixed VHD | `Partition table: MBR` | **ok** |
| **CHD** | `Partition table: MBR` | **fails** |
| **dynamic VHD** | `Partition table: MBR` | **fails** |
| **QCOW2** | `Partition table: MBR` | **fails** |
| **VMDK sparse** | `Partition table: MBR` | **fails** |

Fixed VHD only passes because it *is* raw data with a trailing footer. Every
container with a non-flat internal layout fails, in one of two ways:

```
rb-cli backup o-chd/disk.chd ./out --format raw --sector-by-sector
  -> error: backup failed: cannot read first sector: failed to fill whole buffer

rb-cli backup o-qcow2/disk.qcow2 ./out --format raw --sector-by-sector
  -> error: backup failed: failed to detect partition table:
     Invalid MBR: invalid boot signature: expected 0xAA55, got 0x...
```

Same root cause — the container is not decoded, so raw file bytes are read as
though they were the disk. Which message appears depends on whether the read
runs off the end of a small file or lands on header bytes that resemble a bad
MBR. `--sector-by-sector` does not help; the failure precedes all partition
logic.

**Why it matters.** These are four of our own output formats, CHD being the
default for `convert`. A user can convert a disk to any of them and then find
they cannot back it up — archive to QCOW2, later try to make a working copy,
and the tool refuses. `inspect` reading them fine makes the failure look
arbitrary from outside.

Reproduces on a 64 MB synthetic image; no fixture required.

**Two traps when verifying this**, both of which caught me:

1. `backup` prints `rb-cli backup: SRC -> DEST` *before* doing any work, so a
   grep for `->` reports success on a run that then fails. **Check the exit
   code**, not the output.
2. `--format raw` writes `partition-N.img` files, so a `find` over several
   directories can pick up an unrelated `.img` and attribute the wrong result
   to the wrong container. Use explicit paths per case.

### R-017 — superfloppy detection also misses SFS {#r-017}

**FIXED 2026-08-07**, with [R-009](#r-009). SFS needed more than a magic:
`detect_filesystem_type` has no SFS probe, so the auto-detect-at-open path the
other four rely on does not exist for it. The hint therefore travels as a
DosType in `partition_type_string`, the same route a bare AmigaDOS floppy
takes, which reaches the driver through the string dispatcher.
`fs::sfs::looks_like_sfs` gates on the `SFS\0` id, ownblock == 0 *and* the
whole-block checksum, so a custom bootblock carrying the magic is not claimed.
`rb-cli ls` on the Workbench fixture now walks the tree with no `--fs-type`.
Original report follows.

Same shape as [R-009](#r-009), found separately and worth its own line because
it extends the affected list.

A bare SFS volume is not recognised:

```
rb-cli inspect dh0.img
  -> error: detecting partition table: Invalid MBR: invalid boot signature:
     expected 0xAA55, got 0x0000
```

The volume is unambiguously SFS — `SFS\0` is the first four bytes:

```
00000000: 5346 5300 f85c 2753 0000 0000 0003 0000  SFS..'S........
```

and the engine reads it perfectly once told what it is:

```
rb-cli ls dh0.img / --fs-type "SFS\0"
  DIR  Utilities    DIR  WBStartup    DIR  Expansion
  DIR  L            DIR  Fonts        FILE Devs.info
```

So the driver is fine; only the magic is missing from `detect_superfloppy`.
The affected set is now **JFS, UFS1, UFS2, ReiserFS and SFS** — five
filesystems with working drivers that cannot be opened from a bare image.

Fixture: `fs.sfs.workbench-dh0.hd` (annex).

### R-009 — bare JFS / UFS / ReiserFS images cannot be opened {#r-009}

**FIXED 2026-08-07.** `detect_superfloppy` gained the three missing probes, at
the offsets `detect_filesystem_type` was already using: ReiserFS at 0x10000+52
(folded into the existing btrfs read, which covers the same sector), UFS at
8192/65536 + 1372 in both byte orders, JFS at 0x8000. No dispatch string is
needed — a type-byte-0 partition auto-detects at open, and those drivers were
always reachable that way. `inspect` and `ls` both work on all four fixtures
with no `--fs-type`. Each probe was mutation-verified individually: breaking
any one magic reproduces this finding's exact error for that fixture alone.

The four hints also had to be added to `fs::is_browsable_superfloppy`, whose
doc comment requires it to cover every hint `detect_superfloppy` emits or the
GUI silently refuses the filesystem. Auditing that list found two unrelated
pre-existing omissions — `squashfs` and `Oric Jasmin` — fixed in the same
pass, and the guard test now carries the full set instead of a subset.

Original report follows.

```
rb-cli inspect test_jfs.img
  -> error: detecting partition table: Invalid MBR: invalid boot signature:
     expected 0xAA55, got 0x0000
```

Same for `test_ufs1.img`, `test_ufs2.img`, `test_reiserfs_v3_6.img`.

Not a general superfloppy failure — the neighbouring cases work:

| filesystem | bare-image `inspect` |
|------------|----------------------|
| btrfs, exFAT, APFS, XFS v4/v5 | works |
| ext, NTFS, HFS, HFS+, HPFS, Minix V2/V3, AFFS, ProDOS, EFS, FAT | works |
| **JFS, UFS1, UFS2, ReiserFS** | **fails** |

`detect_superfloppy` (`src/partition/mod.rs`) probes magics for SquashFS, the
Xerox Alto family, Pilot/Cedar, Dwarf, Lisa, NTFS, exFAT, XFS, ext and btrfs.
JFS (`JFS1` at 32768), UFS (`0x011954` at 8192 or 65536) and ReiserFS
(`ReIsEr2Fs`) are simply absent.

Why it went unnoticed: the `cargo test` suites for these four call
`open_filesystem` directly and never route through partition-table detection,
so the drivers are well covered while the path a user actually takes is not.

User impact: anyone who dumps a raw JFS/UFS/ReiserFS partition and points
rb-cli at it gets an MBR error, with no hint the filesystem is supported.

### R-013 — UFS directories reported as files, with a garbage size {#r-013}

Reading the root of an installed Solaris disk (Sun label, UFS slice):

```
rb-cli ls disk-0.qcow2@1 /
  DIR            0             lost+found
  FILE  14989422569311248440   usr
  FILE           0             export
  FILE           0             var
  FILE           9             bin
  FILE           0             etc
```

Two defects in one listing:

1. Every directory except `lost+found` is typed `FILE`. `usr`, `var`, `etc`,
   `bin`, `dev`, `kernel`, `lib`, `platform`, `export` are all directories.
2. `usr` reports 14,989,422,569,311,248,440 bytes — about 1.5e19, consistent
   with an unrelated field read as the size, or a sign/endianness slip.

`lost+found` being correct is the useful detail: it is the first entry, which
points at the walk going wrong *after* the first record rather than a
wholesale layout misread.

Existing UFS fixtures are NetBSD `makefs` images read via `open_filesystem`
directly, so neither the Solaris on-disk variant nor this listing path was
ever exercised. Fixture: `part.sun.solaris-disk.multipart` (annex).

---

## Medium

### R-015 — cue sheets with unpadded track numbers are rejected {#r-015}

A CUE sheet written with `TRACK 1` rather than `TRACK 01` fails to parse:

```
rb-cli optical info BOOKSHELF.cue
  Container:   unknown
  Filesystems: (none recognized)
  warning: unrecognized disc image: CUE error:
    Error(Msg("Expeceted number but found String(\"1\") instead"), ...)
```

Confirmed as zero-padding and nothing else. The same disc parses when the
track number is padded, and every cue currently in the corpus happens to use
the padded form:

| cue | track line | result |
|-----|-----------|--------|
| `mixedmode-both.cue` | `TRACK 01 MODE1/2352` | parses |
| `BOOKSHELF.cue` | `TRACK 1 MODE1/2352` | **rejected** |

The CUE format does not require zero-padding, and real tools emit both —
this sheet came from CloneCD alongside a `.ccd`/`.sub` pair. So any disc
ripped by such a tool is unreadable through the cue path, and the failure
surfaces as "unrecognized disc image" rather than as a parse problem the user
could act on.

Note the upstream message also carries a typo ("Expeceted"), which places the
fault in the cue-parsing dependency rather than in our own code — worth
checking whether it is fixed upstream before working around it locally.

Found 2026-08-07 while triaging the fixture drop folder. No fixture is needed
to reproduce: take any working cue and remove the leading zero from a track
number.

---


### R-005 — no error envelope under `--format json` {#r-005}

`src/cli/output.rs` documents that on failure the envelope still returns, with
`status.error: true`, `status.code` carrying the exit code, `status.message` a
short description and `result` null.

Observed: a failing verb under `--format json` writes **nothing** to stdout
and a plain-text error to stderr.

```
rb-cli inspect ./missing.img --format json
  -> exit 1, stdout empty, stderr: "error: open ./missing.img: ..."
```

A JSON consumer gets no parseable output on exactly the path the envelope
exists to serve.

### R-008a — AFFS tail blocks above 4066 are uncovered {#r-008a}

One AFFS bitmap block covers `(512 - 4) / 4 * 32 = 4064` bits, addressing
blocks 2..4065. `src/fs/affs.rs` writes exactly one, commented "the bitmap
covers a full 4064-bit page even if the volume is smaller" — true only while
the volume *is* smaller.

A 2 MB volume is 4096 blocks, so 4066..4095 fall outside the page:

```
ERROR [AffsBitmapMismatch] 30 block(s) marked allocated but unreachable
WARN  [AffsOrphanBlock]    30 block(s) unreachable from root: [4066, 4067, ...]
```

Confirmed as a boundary: at `--size 2033K` (exactly 4066 blocks) fsck is
clean; every larger size fails.

Same root cause as R-008b. Both need the bitmap to span as many blocks as the
volume requires, chained through the root block's `bm_pages` slots, with the
marking loop bounded.

### R-012 — `optical info` rejects discs with no data track {#r-012}

```
rb-cli optical info Audio-only.cue
  Container:   unknown
  Filesystems: (none recognized)
  warning: unrecognized disc image: No data track found
```

The rule is exactly "has a data track", nothing subtler:

| cue | tracks | data tracks | result |
|-----|-------:|------------:|--------|
| Akumajou Dracula MIDI Collection | 21 | 1 | `bincue` |
| usbode-audio-sampler | 2 | 1 | `bincue` |
| mixedmode-both (MODE1 + AUDIO) | 2 | 1 | `bincue` |
| FF_CD | 5 | **0** | **rejected** |
| Audio-only | 1 | **0** | **rejected** |
| synthetic 2-track audio | 2 | **0** | **rejected** |

Minimal repro, no fixture required:

```bash
head -c $((800*2352)) /dev/zero > cdda.bin
printf 'FILE "cdda.bin" BINARY\r\n  TRACK 01 AUDIO\r\n    INDEX 01 00:00:00\r\n  TRACK 02 AUDIO\r\n    INDEX 01 00:05:25\r\n' > cdda.cue
rb-cli optical info cdda.cue
```

`src/optical/cd_audio.rs` exists specifically to read audio tracks and
`optical rip` handles CD-DA, so the disc a user is most likely to point at is
the one shape `optical info` refuses. Mixed-mode is unaffected.

### R-003 — docs claim `ls` supports `--format`; it does not {#r-003}

`src/cli/output.rs` lists `ls` among the verbs that "can emit their results in
one of five formats", and its CSV/TSV note says those formats apply to "`ls`,
`show partmap`, `show devices`, `fsck` issue lists".

`rb-cli ls --help` exposes no `--format`, and it is not global either — both
`rb-cli ls img --format json` and `rb-cli --format json ls img` exit 2 with
"unexpected argument". Verified present on `inspect`, `fsck`, `du`, `locate`.

Either the flag was dropped and the doc is stale, or structured `ls` output is
a real gap. `ls` is among the most script-facing verbs in the CLI.

### R-010 — `inspect` has no `--fs-type` {#r-010}

`--fs-type` exists on `ls`, `fsck` and `du`, and `docs/cli-reference.md`
describes it as the mechanism for CP/M images "which have no on-disk
signature". `inspect` does not take it — its usage line is bare
`inspect <IMAGE>`:

```
rb-cli inspect ManicMiner.dsk --fs-type cpm:amstrad_data
  -> error: unexpected argument '--fs-type' found

rb-cli inspect ManicMiner.dsk
  -> error: detecting partition table: Invalid MBR: invalid boot signature:
     expected 0xAA55, got 0xE5E5
```

`rb-cli ls ManicMiner.dsk --fs-type cpm:amstrad_data` works and lists
`MANIC.BAS` / `MANIC.BIN`, so the engine is fine — `inspect` cannot reach it.
Every CP/M disk (nine DPB presets) is therefore un-inspectable.

### R-006 — `new volume prodos` fails with default arguments {#r-006}

The default volume name is `rusty-backup`. ProDOS forbids `-`, so the default
is invalid for that filesystem and the verb always fails:

```
rb-cli new volume prodos --size 2M vp.img
  -> error: invalid data: filename contains '-' — ProDOS allows only letters
     (A-Z), digits (0-9), and '.'; rename the file
```

`--name TESTVOL` succeeds, so the builder is fine — only the default is
unusable. The message compounds it by saying "rename the file" when the
offending string is the volume name.

Every other `new volume` filesystem accepts the default.

---

## Low / undecided

### R-004 — CSV/TSV rejection exits 1, documented as 2 {#r-004}

`src/cli/output.rs` states nested-result verbs "error out with
`crate::cli::exit::USAGE_ERROR`" — exit 2. Observed exit 1.

```
rb-cli inspect vol.img --format csv   ->  exit 1
error: inspect returns nested data; --format csv only supports flat tabular
results. Use --format json or --format yaml instead.
```

The message is right, the code is not. `require_non_flat`
(`src/cli/output.rs:161`) uses `anyhow::bail!`, which propagates as a generic
failure. The unit test `require_non_flat_rejects_csv_for_nested` only asserts
`is_err()`, so it cannot catch this.

### R-011 — G64 fails on copy-protected / patched dumps {#r-011}

Of three real G64 files, one opened and two failed:

| file | result |
|------|--------|
| `C'est la Vie.g64` (278 KB) | opens |
| `Protector II (must be write protected).g64` (191 KB) | fails |
| `American Express (Magic Disk 64 1989-09 Side 2) [patched].g64` (278 KB) | fails |

Both failures are copy-protected or patched dumps with non-standard GCR, and
both report the generic `Invalid MBR: invalid boot signature` rather than
anything GCR-specific.

**Needs a decision before it can be graded.** Preserving copy-protected disks
is the reason G64 exists rather than D64, so partial support may be a real
limitation rather than an acceptable boundary. `.d64` and `.g71` unaffected.

---

## Documentation drift

### R-001 — README partition-table list missing two schemes {#r-001}

`src/partition/mod.rs` defines nine `PartitionTable` variants: `Mbr`, `Gpt`,
`Apm`, `Rdb`, `Sgi`, `Sun`, `Ahdi`, `X68k`, `None`. README § Partition tables
lists only MBR, GPT, APM, RDB, SGI, Sun and "None (superfloppy)" — **Atari
AHDI and Sharp X68000 are absent**, despite both being fully implemented
(`src/partition/atari.rs`, `src/partition/x68k.rs`) and both having CLI
builders (`rb-cli new hd x68k`).

Exactly the drift class CLAUDE.md's pre-commit documentation-sync section
warns about.

### R-002 — `src/fs/README.md` capability table is stale {#r-002}

Still lists ext2/3/4 as "No (planned)" for browsing, compaction and resize,
and covers only six partition type bytes. The engine implements ext fully
(`src/fs/ext.rs`, `ext_format.rs`, `ext_fsck.rs`, `ext_csum.rs`) plus about
forty other filesystems. Either regenerate from `fs/mod.rs` dispatch or delete
it and point at the README.

---

## Regression coverage

Which finding is guarded by which case, so a fix cannot silently regress.
Run `rb-regress run --tiers 0-4` to check them all.

| Finding | Case | State |
|---------|------|-------|
| R-003 | `cli.envelope.ls-supports-format` | red |
| R-004 | `cli.exit.{csv,tsv}-on-nested-verb-is-usage-error` | red |
| R-005 | `cli.envelope.error-envelope-on-failure` | red |
| R-006 | `fs.new-volume.prodos-default-name` | red |
| R-007 | `fs.new-volume.ntfs{,.2m-fsck,.32m-fsck}` | **green — fixed** |
| R-008a | `fs.new-volume.affs.bitmap-boundary-plus-one` | red |
| R-008b | `fs.new-volume.affs.{4m,32m}` | red |
| R-009 | `fs.read.{jfs,reiserfs,ufs1,ufs2}` | **green — fixed** |
| R-010 | `cli.flags.inspect-accepts-fs-type` | red |
| R-011 | `fmt.g64.standard-dump-opens` | green — **pins the working half only** |
| R-012 | `optical.cdda.no-data-track-opens` | red |
| R-013 | `fs.detect.ufs-{solaris-entry-types,no-absurd-sizes}` | red |
| R-015 | `optical.cue.unpadded-track-number` | red |
| R-016 | `backup.container.{chd,vhd-dynamic,qcow2,vmdk-sparse}` | red |
| R-017 | `fs.detect.sfs-bare-volume` | **green — fixed** |

Cases assert the **intended** behaviour, so each is red until its finding is
fixed and green afterwards. Never "fix" one by asserting the broken
behaviour — the red case is the tracking mechanism, and the same case guards
against the bug returning.

Several findings are paired with a **working-half** case
(`fs.detect.sfs-reads-when-told`, `optical.cdda.mixed-mode-still-opens`,
`backup.container.{raw,vhd-fixed}`, `cli.flags.ls-accepts-fs-type`). Those are
green now and exist so a fix for the red one cannot break the path that
already works.

**Not covered:**

- **R-001, R-002** — documentation drift. Not expressible as a CLI case; they
  need a source-parity test comparing the README tables against the
  `PartitionTable` enum and the `fs/mod.rs` dispatch.
- **R-011** — only the working half is pinned. No case asserts that
  copy-protected G64 dumps open, because whether they should is undecided;
  asserting either way would prejudge it.
- **R-018** — a documentation failure, not runtime behaviour, and the suite
  runs the modern binary. A docs-parity test comparing CONTRIBUTING.md's
  feature list against the workflow's would guard it; that is the same
  source-parity test R-001 / R-002 need, so it belongs with them.
- **R-014** — a lint failure, not runtime behaviour. The pre-commit hook is
  itself the regression guard: it runs `clippy --all-targets -- -D warnings`
  on every commit, so a reintroduction cannot be committed.

## Suggested order

0. ~~**R-014**~~ — done; commits work without `--no-verify` again.
0b. ~~**R-018**~~ — done; the verification command works on Windows again.
1. ~~**R-009** / **R-017**~~ — done; five filesystems' worth of tier-2
   coverage went green.
2. **R-008b** — a panic with no file produced is the worst failure mode here,
   and R-008a shares its fix.
3. ~~**R-007**~~ — done; the formatter was already correct when re-verified.
4. **R-013** — wrong entry types and an absurd size are user-visible
   immediately.
5. **R-005**, **R-004**, **R-003** — the CLI contract group; cheap, and the
   regression harness depends on that contract being true.
6. **R-006** — a one-line default change.
7. **R-001**, **R-002** — fold into the next docs commit.
8. **R-011** — decide scope first.
