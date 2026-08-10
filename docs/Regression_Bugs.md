# Regression Findings (R-001 … R-037)

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
| ~~R-023~~ | ~~**High**~~ **FIXED** | `src/cli/verbs/repack.rs` | ~~`repack` loses every file in the volume~~ — scope guard; nothing was lost, a FAT long filename was dropped, 2026-08-09 |
| ~~R-022~~ | ~~**High**~~ **FIXED** | `src/partition/mod.rs` | ~~HPFS sector-by-sector backup -> restore is not byte-identical~~ — detection, not fidelity: a bare HPFS volume backed up to nothing at all. Probe added, 2026-08-09 |
| ~~R-021~~ | ~~**High**~~ **FIXED** | `src/cli/verbs/resize.rs` | ~~`resize --size` reports success and changes nothing~~ — grows the file when the volume is the file, refuses otherwise, 2026-08-09 |
| ~~R-024~~ | ~~Medium~~ **FIXED** | `src/fs/affs.rs` | ~~AFFS `put` leaves the volume failing its own fsck~~ — was the truncated bitmap, not the editor; same fix, 2026-08-10 |
| ~~R-025~~ | ~~Medium~~ **FIXED** | `src/fs/squashfs_edit.rs` | ~~`squashfs put` fails to replace the image on Windows~~ — handle released before the rename, 2026-08-08 |
| ~~R-026~~ | ~~Low~~ **FIXED** | `src/cli/verbs/show.rs` | ~~`show partmap` cannot read an SGI disk that `inspect` reads fine~~ — detects the table first, 2026-08-08 |
| ~~R-027~~ | ~~Medium~~ **FIXED** | `src/rbformats/zip_disk.rs` | ~~A Finder-made `.zip` holding one `.dmg` is rejected as ambiguous~~ — extension list derived from the canonical one, 2026-08-08 |
| ~~R-030~~ | ~~**High**~~ **FIXED** | `src/fs/affs.rs` | ~~A real Workbench 1.3 AFFS volume cannot be opened at all~~ — the root block was located from the end of the file, not the partition, 2026-08-10 |
| [R-029](#r-029) | **High** | `src/fs/efs.rs` | EFS computes block addresses far outside the image; `fsck` fails on an unmodified volume |
| [R-031](#r-031) | Medium | `src/partition/mod.rs` | A real Apple DOS 3.3 disk is detected as `unknown`, though our own output is not |
| ~~R-028~~ | ~~Medium~~ **FIXED** | `src/fs/apple_dos.rs` | ~~Apple DOS 3.3 reports three different sizes for one file~~ — length stored in a type-B header; all three now agree, 2026-08-10 |
| [R-032](#r-032) | Low | `src/fs/sfs.rs` | SFS `put` fails on any volume with a multi-leaf extent btree — i.e. any real one |
| ~~R-033~~ | ~~High~~ **FIXED** | `src/partition/mod.rs` | ~~A QL Microdrive `.mdv` fails at MBR detection, though its own probe matches it exactly~~ — probe added beside the HPFS one, 2026-08-10 |
| ~~R-034~~ | ~~Medium~~ **FIXED** | `src/fs/mod.rs` | ~~Refusing a write to a read-only filesystem says `unknown` and exits 1, not 4~~ — names the filesystem, exits 4, 2026-08-08 |
| ~~R-035~~ | ~~Medium~~ **FIXED** | `src/backup/` | ~~`.cbk` embeds the producing host's absolute path, so it can never be byte-identical across machines~~ — path normalised to a leaf, 2026-08-09 |
| ~~R-036~~ | ~~Medium~~ **FIXED** | `src/cli/resolve.rs` | ~~A missing image gets three different exit codes across the verb surface~~ — one guard in the shared resolver, 2026-08-10 |
| ~~R-037~~ | ~~**High**~~ **FIXED** | `src/cli/verbs/resize.rs` | ~~Shrinking rewrote the filesystem over live data and returned truncated files~~ — data floor + `--confirm-shrink` + truncation, 2026-08-09 |
| [R-020](#r-020) | **High** | `src/fs/affs.rs` | `new volume affs` output is "Not a DOS disk" on a real Amiga, at every size |
| ~~R-016~~ | ~~**High**~~ **RECLASSIFIED** | `src/cli/verbs/backup.rs` | ~~`backup` accepts only flat-layout sources: CHD, dynamic VHD, QCOW2 and VMDK all fail~~ — not a defect; moved to [F-008](missing_features_from_regression.md#f-008), 2026-08-09 |
| ~~R-018~~ | ~~Blocker~~ **FIXED** | `CONTRIBUTING.md` | ~~The documented Rust-1.73 verification build does not compile on Windows~~ — missing `windows-legacy` feature, 2026-08-07 |
| ~~R-017~~ | ~~High~~ **FIXED** | `src/partition/mod.rs` | ~~Superfloppy detection also misses SFS (extends R-009)~~ — probe added 2026-08-07 |
| [R-015](#r-015) | Medium | `src/optical/` (cue parser) | A `.cue` with unpadded track numbers (`TRACK 1`) is rejected |
| ~~R-014~~ | ~~Blocker~~ **FIXED** | `src/cli/verbs/squashfs.rs` | ~~Pre-existing clippy failure blocks every commit via the pre-commit hook~~ — boxed 2026-08-07 |
| ~~R-008b~~ | ~~**High**~~ **FIXED** | `src/fs/affs.rs` | ~~`new volume affs --size 4M` panics; no file produced, exit 101~~ — the formatter writes as many bitmap pages as the volume needs, 2026-08-10 |
| ~~R-007~~ | ~~High~~ **FIXED** | `src/fs/ntfs_format.rs` | ~~Freshly formatted NTFS fails its own fsck~~ — verified clean 2026-08-07 |
| ~~R-009~~ | ~~High~~ **FIXED** | `src/partition/mod.rs` | ~~Bare JFS / UFS1 / UFS2 / ReiserFS images cannot be opened at all~~ — probes added 2026-08-07 |
| ~~R-013~~ | ~~**High**~~ **FIXED** | `src/fs/ufs.rs` | ~~Solaris UFS directories reported as files, one with a garbage size~~ — UFS1's rotational cylinder-group offset was ignored, 2026-08-10 |
| ~~R-005~~ | ~~Medium~~ **FIXED** | `src/cli/output.rs` | ~~No error envelope emitted under `--format json`~~ — format recorded before dispatch, envelope emitted from `main`, 2026-08-09 |
| ~~R-008a~~ | ~~Medium~~ **FIXED** | `src/fs/affs.rs` | ~~AFFS volumes above 4066 blocks have uncovered tail blocks~~ — same fix as R-008b, 2026-08-10 |
| ~~R-012~~ | ~~Medium~~ **FIXED** | upstream `opticaldiscs` | ~~`optical info` rejects any disc with no data track (pure CD-DA)~~ — fixed upstream, pin bumped to 0.15.0, 2026-08-10 |
| ~~R-003~~ | ~~Medium~~ **FIXED** | `src/cli/output.rs` | ~~Docs claim `ls` supports `--format`; it does not~~ — flag implemented, all five formats, 2026-08-09 |
| ~~R-010~~ | ~~Medium~~ **FIXED** | `src/cli/verbs/inspect.rs` | ~~`inspect` has no `--fs-type`, so CP/M images cannot be inspected~~ — flag added and honoured, 2026-08-08 |
| ~~R-006~~ | ~~Medium~~ **FIXED** | `src/cli/verbs/new.rs` | ~~`new volume prodos` always fails with default arguments~~ — per-filesystem default, 2026-08-08 |
| ~~R-004~~ | ~~Low~~ **FIXED** | `src/cli/exit.rs` | ~~CSV/TSV rejection exits 1, documented as 2~~ — errors carry their exit code now, 2026-08-08 |
| [R-011](#r-011) | **Partial** | `src/rbformats/containers/g64.rs` | Copy-protected G64 dumps: read-only refusal fixed; decode of non-standard GCR unverified (no fixture) |
| ~~R-001~~ | ~~Doc~~ **FIXED** | `README.md` | ~~Partition-table list missing AHDI and X68000~~ — X68k and DSD rows added, guarded by a parity test, 2026-08-09 |
| ~~R-002~~ | ~~Doc~~ **FIXED** | `src/fs/README.md` | ~~Capability table stale — ext listed as "planned"~~ — table deleted for a pointer at the live dispatch, 2026-08-09 |

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

**FIXED 2026-08-09**, and it was not quite a no-op — which is worse. `resize`
warned "the FS may refuse", then proceeded: the FAT resize rewrote the BPB for
16303 clusters, appended 32 KB of new FAT sectors, printed `resize complete`
and exited 0. The result was a filesystem describing twice the blocks its
container held. `inspect` still said 8 MiB because the file length is what a
superfloppy's size comes from, which is what made it look like nothing had
happened.

The warning was the bug. Two situations were being treated as one:

- **The volume is the whole file** — a bare superfloppy in a plain image.
  Nothing else lives there and there is no table to keep in step, so appending
  zeros *is* what the caller asked for. `resize` now grows the image first,
  then resizes into it. `PartitionContext::whole_file_path` is exactly this
  condition and already existed.
- **Anything else** — a partition inside a larger disk, a decoded container.
  Its length is set by something `resize` is not editing, so overrunning it is
  corruption. Now a hard refusal, exit 2, naming `partmap resize` and `grow` as
  the verbs that can move the boundary.

Verified both ways: an 8 MiB FAT superfloppy with a file in it grows to 16 MiB,
fsck-clean, file intact; an X68k partition asked for 64 MiB in a 16 MiB slot is
refused with the image byte-for-byte untouched and its three files still there.

Growing only, at the time. **That half of this note was wrong and is corrected
by [R-037](#r-037):** a shrink was not "trailing slack, not damage". It rewrote
the filesystem for the smaller size with live data still beyond it, and the
files came back truncated. Shrinking is handled properly as of R-037.

---

```
rb-cli new volume fat --size 8M v.img
rb-cli resize v.img --size 16M     -> "resize complete", exit 0
rb-cli inspect v.img               -> FAT   8.0 MiB
```

Exit 0 and a success message for work that did not happen. Silent no-ops are
the worst shape of bug in a tool whose job is moving data: nothing downstream
has any reason to check. Case `resize.to-explicit-size`.

### R-022 — HPFS does not survive a sector-by-sector round-trip {#r-022}

**FIXED 2026-08-09, and the finding understated it by a lot.** This was not a
fidelity bug. The restored 2 MB image held **44 non-zero bytes against the
source's 604** — the boot sector and nothing else. Superblock, spareblock,
bitmaps, root fnode: gone. `backup` wrote no partition file at all, and exited
0.

The backup folder contained `metadata.json`, `mbr.bin`, `mbr.json` and no
`partition-*`. Its metadata says why:

```json
"partition_table_type": "MBR",
"partitions": []
```

**Cause is detection, not the round-trip.** An HPFS boot sector is an x86 VBR:
`EB 3C 90`, OEM `IBM 4.50`, a BPB, and an 0xAA55 signature. `detect_superfloppy`
had no HPFS probe, and HPFS fails the FAT probe because `reserved_sectors` and
`num_fats` are both 0 — HPFS has no FATs. So it fell through to the MBR parse,
which took the 0xAA55 at face value, read an all-zero partition array, and
reported "MBR, no partitions". `backup` iterates that list, so it had nothing
to copy. Nothing anywhere in the chain was wrong about its own job.

`ls` and `inspect` masked it: both fall back to "raw filesystem @ byte 0" and
read the volume fine, so the disk looked healthy right up until it was backed
up.

The fix is a probe in `detect_superfloppy`, beside the NTFS / exFAT pair HPFS
shares MBR type 0x07 with. `fs::hpfs::looks_like_hpfs` already existed and
wants both the superblock magic at sector 16 and the spareblock magic at
sector 17, so a chance match needs 64 bits to line up. A bare HPFS volume now
reports `Partition table: None` with one HPFS partition, and the round trip is
byte-identical.

Two new cases pin the mechanism rather than the symptom, so a detection
regression is named as one instead of resurfacing as a mysterious fidelity
failure: `fs.detect.hpfs-bare-volume` and `fs.detect.hpfs-backup-is-not-empty`.
The second exists because `roundtrip.hpfs.raw` cannot tell "backed up nothing"
from "backed up slightly wrong".

**The control that mattered** was the other direction: a *partitioned* HPFS
disk — the ao486 shape, graded **Yes** in
[full_MiSTer_support_status.md](full_MiSTer_support_status.md) — must not be
hijacked by the new probe. The probe reads absolute sectors 16 and 17, which on
a partitioned disk sit in the pre-partition gap, so it should not fire. It was
first checked against a synthesized MBR holding the same 2 MB volume.

**That control is now a real fixture and a real case.** `fs.hpfs.os2-warp45.hd`
— an OS/2 Warp 4.52 install, MBR type-0x07 HPFS at LBA 63 — was admitted
2026-08-09, and `fs.detect.hpfs-partitioned-stays-mbr` asserts the disk still
reports MBR with its partition at 63. `read.hpfs.os2-warp45` reads the same
volume end to end: 4722 files across 471 directories, long names with spaces,
fsck clean. A synthetic `new volume hpfs` is empty and 2 MB and can exercise
none of that.

---

`backup --sector-by-sector` then `restore` returns bytes that differ from the
source. Every other filesystem tested — FAT, NTFS, ext4, HFS, minix3, EFS,
ProDOS — comes back byte-identical through the same path, so this is specific
to HPFS rather than to the backup format. `--sector-by-sector` asks for a
faithful image, so any difference is a defect. Case `roundtrip.hpfs.raw`.

### R-023 — `repack` loses every file {#r-023}

**FIXED 2026-08-09 — and the diagnosis in the original report was wrong.**
Nothing was lost. `payload.bin` came back as `PAYLOAD.BIN`, and the `get` in
the next step asked for the lowercase name and missed. Recording that, because
"exit 0 and the data is gone" and "exit 0 and a filename changed case" call for
different fixes, and only the second one was happening.

The real defect is scope. `repack` is documented Human68k-only, but it opened
`Human68kFilesystem` directly instead of asking how the read path had routed
the volume — and a plain FAT16 superfloppy opens cleanly as Human68k, because
the two share a BPB layout on purpose (`Human68kBpb::parse` accepts the
standard little-endian MS-DOS form, which is what X68000 floppies use). So
`repack` rebuilt a FAT volume through a driver with no long-filename concept.
The short name is all the Human68k driver can see, and FAT short names are
upper-case, so the LFN was dropped on the way through.

`repack` now requires `type_string == Some("human68k")` — the same dispatch
identity that chose the driver in the first place, and one that both shapes
carry (X68k partition entries and bare Human68k superfloppies, which are gated
on the 68000 `BRA.S` opcode). A plain FAT volume is refused with exit 2 and a
message naming `resize` instead.

Control, run both ways before believing any of this: `repack` on a real
Human68k volume (`new hd x68k`, four files including a lowercase
`payload.bin`) already worked perfectly — all four files, name case intact,
byte-identical, fsck clean. That is what proved the clone sound and the input
wrong.

The case was rewritten deliberately: it built a plain FAT volume, which
`repack` was never for. It now uses a real Human68k volume, and a new sibling
`resize.repack.refuses-plain-fat` pins the input it used to accept — including
that the refusal leaves the volume untouched, long filename and all.

---

```
rb-cli put v.img payload.bin /payload.bin
rb-cli repack v.img                -> exit 0
rb-cli get v.img /payload.bin      -> error: path component not found: payload.bin
```

Exit 0, and the data is gone. Case `resize.repack.keeps-data`.

### R-024 — AFFS `put` leaves the volume failing its own fsck {#r-024}

**FIXED 2026-08-10 — and this report's diagnosis was wrong.** It reads "this is
the editor, not the formatter" and says to keep it separate from R-008. It was
the formatter: `put` allocated into blocks past the end of the single bitmap
page R-008a describes, and fsck correctly reported a bitmap that disagreed with
the directory walk. Fixing the formatter's bitmap sizing turned this case green
with no change to the edit path. See [R-008a](#r-008a).


A single `put` into a freshly formatted 3 MB AFFS volume — the largest size
R-008 leaves working — makes `fsck --checkonly` report
`1 error(s), 1 warning(s) (some repairable)`. The file reads back correctly,
so the damage is to the allocation structures rather than the data. Related to
but distinct from R-008a/b (sizing) and R-020 (root block); this is the
editor, not the formatter. Case `edit.affs.put-get`.

### R-025 — `squashfs put` cannot replace the image on Windows {#r-025}

**FIXED 2026-08-08.** `commit_by_replacing` now closes its handle on the target
before renaming the rebuilt temp over it.

The original code held the handle across the rename and said so explicitly,
reasoning that nothing reads through it afterwards. That is true, and it is
still not safe on Windows: a file marked for deletion **keeps its name until
the last handle closes**, so the rename cannot reuse a name we are still
holding. Unix frees the name immediately, which is why every developer machine
and both other CI platforms passed.

`FILE_SHARE_DELETE` looks like the fix and is not — tried first, and it changed
nothing. It permits the delete to *begin*; it does not let the name be reused
while a handle is open. An isolating test made that unambiguous: replacing an
unheld file succeeded, replacing a held one failed with the same `os error 5`,
with no editor code in the picture.

The handle is now `Option<RW>`, released before `persist`. `commit_in_place` —
the only other reader — runs solely when `backing_file` is `None`, which is
exactly when the replacement path never ran.



```
error: sync_metadata: I/O error: replacing <path>
```

SquashFS edits rebuild the whole image and swap it in atomically (temp +
fsync + rename). The rename fails on Windows, so the edit path is unusable
there. Cases `subcmd.squashfs.put-rebuilds` and `meta.xattr.set-list-rm` —
`xattr set` reaches the same replace path, so it is not xattr-specific.
`squashfs create` and `verify` both pass, so it is specific to the replace
step.

**Confirmed Windows-only, 2026-08-08.** Both cases pass on macOS. That is
what `platforms = ["windows"]` in known-failures.toml now records — before it
existed, the first macOS run reported them as XPASS, which is supposed to
mean "fixed, remove the entry" rather than "never applied here".

### R-026 — `show partmap` cannot read an SGI disk {#r-026}

**FIXED 2026-08-08.** `show partmap` went straight to `Apm::parse` on any
image, so every non-Apple table reported whatever its magic looked like as a
bad DDR signature. It detects the table first now. APM keeps its full DDR and
driver-descriptor rendering; every other table gets the generic partition
list, which is all those tables have — faking a DDR for them would be worse
than omitting it. `PartmapPayload` already carried a `kind` field, so the
structured output anticipated this.


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

**FIXED 2026-08-08 — and the AppleDouble sidecar was not the cause.** The
archive holds `APFS_Image.dmg` and `__MACOSX/._APFS_Image.dmg`, so the stub
looked like the obvious culprit. It was already filtered. The real reason was
that `.dmg` was not in this file's private extension list at all, so *neither*
entry counted as a disk image and the single-entry fallback then saw two files.

The list had drifted from `DISK_IMAGE_EXTS`, which has carried `dmg` all
along — and `adf`, `2mg`, `woz`, `imz`, `dc42`, `moof`, `d88`, `xdf`, `hdm`,
`dim`, `po`, `do`, `gho`, `hfv` and `squashfs` besides. Every one of those was
the same bug waiting for a differently-shaped archive. It is now derived from
the canonical list, so the two cannot drift again; `bin` is excluded, being far
too common in a mixed archive to identify a disk image by.

AppleDouble entries are now dropped at collection rather than only in the
image filter, so the single-entry fallback and the "Entries:" listing ignore
them too.

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

**Hypothesis CONFIRMED 2026-08-10, and the formatter half fixed.** This entry
recorded "root block `header_key` must be 0 and we write the block number" as
**unconfirmed**, needing an emulator. It did not: a real disk answers it. The
Workbench 1.3 fixture admitted for [R-030](#r-030) has a root block reading

```
REAL Workbench1.3 root :  type=2  header_key=0     secType=1
OUR 4M volume root     :  type=2  header_key=4096  secType=1
```

`create_blank_affs` now writes 0. Volumes still format, fsck clean, and accept
a `put` that reads back.

**Still open, and still needs an oracle.** That a correct `header_key` makes the
volume *mount* on a real Amiga is a separate claim, and no automated run here
can make it — all 62 emulator / MiSTer-core oracles are `skip-manual`. What has
changed is that the remaining question is "does this fix it", not "is the
hypothesis right". The other half of the entry — that `affs_fsck` agrees with
the formatter and is wrong the same way — found nothing to change: fsck never
inspected the root's `header_key` at all.

---


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

**RECLASSIFIED 2026-08-09 — not a defect.** `backup` has never claimed to
decode containers; `--help` documents SOURCE as "an image file or a
block-device path" and says nothing about non-flat internal layouts. Code that
does not do something it never claimed is a capability gap, and this file's own
rule for the split ("a bug means the code disagrees with its own documentation
or with reality") puts it on the other side of the line.

The full report, the four-container table, the two verification traps and the
route to a fix now live at
[F-008](missing_features_from_regression.md#f-008). The four cases
`backup.container.{chd,vhd-dynamic,qcow2,vmdk-sparse}` are unchanged — they
assert the behaviour we want and stay red — and their `known-failures.toml`
entries now cite F-008. `rb-regress validate` accepts an F-nnn citation as of
the same date; before that a known failure could only name a defect, which is
what made a feature gap look like the only available filing.

What made it read as a defect for so long is real and worth keeping in mind:
`backup.container.inspect-reads-what-backup-cannot` is green and proves
`inspect` opens exactly what `backup` refuses. An asymmetry between two verbs
on the same file looks like a bug from outside even when neither verb is
misbehaving.

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

**FIXED 2026-08-10. Cylinder-group layout, as the entry guessed — not
endianness.**

The tell was in the symptom: `lost+found` was the only correct entry, and it is
the only one whose inode lives in cylinder group 0.

UFS1 places each CG's *metadata* — superblock replica, CG header, inode table —
at `cgbase(c) + fs_cgoffset * (c & !fs_cgmask)`, a rotational skew from the days
when it mattered which track a cylinder group's tables landed on. `cgbase(c)` is
only where the CG's *data* begins. Our addressing used `cgbase` throughout, and
that term is **zero for c = 0 whatever the values** — so CG 0 read perfectly and
every later CG read garbage.

Confirmed by probing the disk rather than reasoning about it:

```
fs_cgoffset = 32   fs_cgmask = 0xffffff00

ino     2  cg 0:  plain 0o040755   with-cgoffset 0o040755   (same)
ino 18816  cg 1:  plain 0o100242   with-cgoffset 0o040755   DIFFER
ino 37632  cg 2:  plain 0o027056   with-cgoffset 0o040755   DIFFER
```

`cgstart_frag()` now applies the skew, and the superblock replica, CG header and
inode table all address through it. It is inert where it should be: UFS2 never
has the term, and a UFS1 image written after the rotational tables were dropped
carries `fs_cgoffset = 0`. The existing UFS1/UFS2/NeXTSTEP read and edit cases
stayed green, which is the check that matters for a change to inode addressing.

`bin` and `lib` still list as 9-byte files — correct: they are Solaris symlinks
to `./usr/bin` and `./usr/lib`, both 9 characters.

**One symptom did not clear**, and is not covered by either case: `show fs-info`
reports `Free: 0 B` on this volume. That comes from the cylinder-group summary,
not from inode addressing, so it is a separate defect on the same filesystem —
worth its own finding rather than being folded in here silently.

---


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

### R-033 — a QL Microdrive cartridge never reaches its own detector {#r-033}

**FIXED 2026-08-10, and it was R-022's shape exactly** — which is what the
resume note predicted. Both the driver and the detection existed; nothing
consulted them on the path the user takes. `detect_superfloppy` had no MDV
probe, so a cartridge fell through to the MBR parse, which read the `ff ff`
sync at 0x0A as a bad 0xAA55 and refused the whole file.

The probe now sits beside the HPFS one added for R-022, gated on **both** the
exact 174,930-byte cartridge length and the sector-0 header, so it cannot claim
anything else. Two unit tests pin both halves of that gate — a header at the
wrong length and cartridge-sized bytes without a header must both be rejected.
A probe that fires too eagerly is worse than one that never fires.

`inspect` now reports `qdos_mdv`, 170.8 KiB, no partition table.

**What this does not do.** Reaching the driver reveals that its directory walk
is a stub: `ls` returns "QDOS microdrive directory walk not implemented". That
is not a new finding — it is the `.mdv` QDOS microdrive reader already tracked
in [OPEN-WORK.md](OPEN-WORK.md) §7, estimated at ~300 LOC against the two
anchored fixtures. This finding was about reachability, and reachability is
what it fixed.


```
rb-cli inspect fs.qdos.microdrive.mdv.mdv
  -> error: detecting partition table: Invalid MBR: invalid boot signature:
     expected 0xAA55, got 0x00FF
```

The probe that should catch this is already written and already matches. From
`src/fs/mod.rs:659`, a `.mdv` is recognised when the file is exactly
`MDV_CART_BYTES` and `looks_like_mdv_sector_zero` holds. The fixture is
174,930 bytes — exactly `MDV_CART_BYTES` — and its sector 0 is:

```
00000000  00 00 00 00 00 00 00 00 00 00 ff ff ff 00 54 65  ..............Te
00000010  73 74 20 20 20 20 20 20 4b d5 8e 13 00 00 00 00  st      K.......
```

Ten zero bytes of preamble, `ff ff ff` sync at 0x0A, ASCII cartridge name
"Test" at 0x0E — every condition the function tests. So the driver is not
missing and the detection is not wrong; `inspect` fails before consulting it,
in partition-table detection, which reports the sync bytes (`0x00FF`) as a bad
MBR signature.

`--fs-type qdos` is not a workaround: it routes to the QXL.WIN reader
(`src/fs/qdos.rs`) and fails on a signature mismatch. The two QDOS drivers
exist, and neither is reachable for a real cartridge.

Same shape as [R-026](#r-026) and the R-009 / R-017 group — detection that
exists but is not consulted on the path the user takes. Case
`read.qdos.microdrive`.

### R-035 — `.cbk` embeds the producing host's absolute path {#r-035}

**FIXED 2026-08-09. Decision: normalise the path**, rather than keep it or
replace it with a device identity. A device identity would have to be
synthesised for the common case (an image file, which has no serial number),
and inventing one is a bigger change than the finding asks for.

`backup::metadata::normalize_source_device` reduces the source to a label that
names it without naming the machine. Three shapes, because they are not all
paths:

- **Device paths** (`\.\PhysicalDrive2`, `/dev/sda`, `/dev/disk2`) are kept
  verbatim — a device node is already a leaf, and it is the useful provenance.
- **`rb://` sources** are kept verbatim — the host in a remote backup's label
  is the *source*, chosen by the user, not the producing host's layout.
- **Everything else** is a path to an image file and keeps only its file name.

Both production write sites in `src/backup/mod.rs` go through it. The four
other assignments are test fixtures and were left alone.

This resolves the finding rather than masking it, which matters because the
report is explicit that masking was not available: `expect_divergence` covers
byte *ranges*, and a shorter string changes the file's *length*. Two hosts
backing up the same image now write the same `source_device`, so the `.cbk`
lengths agree and there is nothing for `parity` to align. The information leak
goes with it.

Verified end to end: backing up `C:\Temp\...	arget\sc35\disk.img` records
`disk.img`.

---

Found 2026-08-08 by the first three-way `parity` run over all 53 produced
formats. 157 comparisons matched; this was the only real divergence.

```
DIFF  fmt.cbk   linux vs windows   size 951 != 959
DIFF  fmt.cbk   macos vs windows   size 951 != 959
```

Linux and macOS agree with each other and Windows is 8 bytes larger, which
looks like a Windows defect and is not. Unpacking both and diffing the
metadata shows what actually varies:

```
< "source_device": "/home/dani/repos/.../scratch/produce/fmt.cbk-a/src.img"
> "source_device": "C:\Temp\mistercore\...\scratch\produce\fmt.cbk-a/src.img"
```

The container records the absolute path of whatever it was made from. The two
paths are different lengths, gzip turns that into an 8-byte difference in the
compressed stream, and the sizes diverge. So the 8 bytes are a red herring:
the real property is that **a `.cbk` is not reproducible across machines at
all** — two Linux boxes with different checkout paths would differ too, and
nothing about the OS is involved.

Whether to keep the field is a decision, not a bug: a backup container that
records what it backed up is reasonable, and [R-019](#r-019) set the precedent
for a deliberate, spec-legal field that records the producing host (VHD's
Creator Host OS), declared to `parity` rather than removed. Two things make
this one different:

- `expect_divergence` masks byte *ranges*. Here the *length* changes, so
  `parity` cannot align the two files to mask anything. The existing mechanism
  cannot express this, whatever we decide.
- The value is a filesystem path from the producing machine, so it travels
  inside any `.cbk` that is shared. That is a mild information leak on top of
  the reproducibility question.

Recording the source as a device identity or a relative name would make the
format reproducible and drop the leak. Case: none yet — this is a `parity`
finding, not a case failure; `fmt.cbk` is produced by `produce.toml` and
`cbk.pack-unpack-roundtrip` passes on all three platforms.

### R-030 — a real Workbench 1.3 AFFS volume cannot be opened at all {#r-030}

**FIXED 2026-08-10.** Neither OFS-vs-FFS nor an older root-block layout — two
of the three readings this entry asked to distinguish. It was arithmetic.

AFFS records its size nowhere: the root block sits at the volume's midpoint, so
the root block's *position* is the size. `AffsFilesystem::open` derived the
block count from `seek(End) - partition_offset`, i.e. the end of the **file**.
For a bare ADF that is the end of the volume. For a partition inside an RDB
disk it is the end of the disk, so the candidate lands past the real root:

```
partition: 4040 blocks at LBA 2020        -> true root block 2020 ("Workbench1.3")
computed from the file tail (4124 blocks) -> 2062, not a root block
```

The candidate is always an upper bound, so `locate_root_block` steps down from
it until a block validates on tag, secType, hash-table size **and its own
checksum** — a loose check would mount a directory block as the volume. The
search is bounded (2048 blocks) so a non-AFFS partition cannot become a
whole-disk scan, and every bare ADF and superfloppy still hits on the first
try. `total_blocks` is then trusted from the root's position rather than the
reader's length.

`rb-cli ls` walks the real disk; `fsck --checkonly` reports 337 files / 31 dirs
clean.

**The architectural note.** The real fix is for the read-open path to carry the
partition length, which the resolver already knows (`ctx.size`) and the *write*
path already threads (`EditContext::partition_len`). AFFS is the only driver
that cares today, being the only one that derives its geometry rather than
reading it from a superblock — so the search is a contained workaround, not the
end state.

---


Found 2026-08-08, the first time the tier-3 edit cases were executed against
the reference volumes rather than against volumes we had formatted ourselves.

```
rb-cli ls   fs.affs.workbench13.hd.hdf@1 /
rb-cli fsck fs.affs.workbench13.hd.hdf@1 --checkonly
rb-cli put  fs.affs.workbench13.hd.hdf ... /PAYLOAD.BIN
  -> Partition @1 / @s0 (RDB): AmigaDOS FFS (DH0) DOS\1 @ LBA 2020, 2068480 bytes
     error: opening filesystem: parse error: root block: type != T_HEADER
```

All three fail identically, so this is not a write-path problem — the volume
cannot be opened at all. The RDB above it parses fine: the partition, its
DosType and its extent are all reported correctly, so the failure is inside
AFFS with a correct offset handed to it. Either the root block's position is
being computed wrong for this geometry, or a 1.3-era root block is being
rejected on a field a 3.x one carries.

Worth reading beside [R-020](#r-020), which is the mirror image: every AFFS
volume *we write* is unmountable on a real Amiga because we put the block
number in `header_key` where 0 belongs. Both are the reader and the writer
disagreeing with the real format about the root block; they may or may not
share a fix. Cases `edit.real.affs-workbench13`.

### R-029 — EFS computes block addresses far outside the image {#r-029}

```
rb-cli ls    fs.efs.small.hd.img /            -> lists the tree fine
rb-cli fsck  fs.efs.small.hd.img --checkonly
  -> error: EFS short read at byte 50065408: got 0 of 512
rb-cli put   fs.efs.small.hd.img payload.bin /PAYLOAD.BIN
  -> error: EFS short read at byte 344826880: got 0 of 512
```

The image is 4,194,304 bytes. The two offsets are roughly 12x and 82x its
size, and they differ between the two operations, so this is a computed
address rather than a fixed constant. Browsing works, which puts the fault in
whatever converts an inode's extent list into a byte offset on the paths
`fsck` and `create_file` take. Case `edit.real.efs-small`; `fsck` failing on
an unmodified fixture means read-only use is affected too.

### R-028 — Apple DOS 3.3 reports three different sizes for one file {#r-028}

**FIXED 2026-08-10.** All three numbers were explainable, and two were wrong.

- **512 from `ls`** — the catalog's `length_sectors` counts *every* sector a
  file occupies, including its track/sector lists. A one-sector file is 1 data +
  1 T/S list = 2, reported as 512 bytes. Now `data_sectors()` subtracts the
  lists (one per 122 data sectors).
- **256 from `get`** — the data sector, unpadded. DOS 3.3 records no byte
  length, so a headerless store loses it permanently.
- **104, the truth** — recoverable only if the length is written to the disk.

`create_file` stored type **T** with the comment that this "avoids stamping a
binary header". That is what lost the length. It now stores type **B** with the
standard 4-byte header (load address, then length, little-endian), which is what
a real Apple II tool writes for arbitrary bytes — so the length is recoverable
by anything reading the disk, not just by us. `read_file` strips the header and
cuts to the declared length; `list_directory` reads it for the reported size,
falling back to allocated bytes when the header is missing or implausible.

104 in, 104 from `ls`, 104 from `get`, byte-identical.

The unit test covering this asserted only a *prefix* match
(`&read_back[..len] == payload`), which is exactly why a 104-in/256-out could
pass it. It now asserts full equality and that the listed size matches.

---


```
rb-cli new floppy apple-dos v.dsk
rb-cli put v.dsk payload.bin /PAYLOAD     # payload.bin is 104 bytes
rb-cli ls  v.dsk /                        -> FILE 512  PAYLOAD
rb-cli get v.dsk /PAYLOAD out.bin         -> out.bin is 256 bytes
```

104 in, 512 reported, 256 out. The first 104 bytes of `out.bin` are correct
and the rest is padding, so no data is lost — but nothing round-trips, and a
tool that copies a file out of one image and into another silently grows it.
Apple DOS 3.3's catalog stores a sector count rather than a byte length, so
some rounding is inherent; three *different* numbers is not. Case
`edit.apple-dos.put-get`.

### R-034 — a read-only filesystem is refused as 'unknown', with the wrong code {#r-034}

**FIXED 2026-08-08.** Both halves, and they had different causes.

*The name.* `fs_name_for` had no entry for `lisafs`, and the Alto image
carries no type string at all — its name lives in `PartitionInfo::type_name`,
which the write path never received. So the fix is in two places: the missing
`fs_name_for` entries, and `type_name` carried on `PartitionContext` so a
refusal can say what the read path just said. Only a message that failed to
name the filesystem is rewritten; every other `Unsupported` is more specific
than anything this could substitute.

*The code.* Nothing could carry an exit code out of a handler at all — see
[R-004](#r-004), fixed first. `Unsupported` from a write-open is now
`exit::permission_denied`, which is what `exit.rs` reserves code 4 for.

    error: opening filesystem for write: unsupported: editing not yet
    supported for filesystem type 'Apple Lisa File System'   [exit 4]

**It does NOT also fix [R-031](#r-031)**, which this entry suggested checking.
`edit.real.apple-dos-invaders` is still red: a real Apple DOS 3.3 disk arrives
at the write path as `unknown` for a different reason, and needs its own
diagnosis.


Found 2026-08-08 writing the negative cases PLAN.md § Phase 4 asks for, which
did not exist at all until now.

```
rb-cli ls  fs.lisa.los31-blank.floppy.dsk /        -> lists fine
rb-cli put fs.lisa.los31-blank.floppy.dsk payload.bin /PAYLOAD
  -> error: opening filesystem for write: unsupported:
     editing not yet supported for filesystem type 'unknown'
     exit 1
```

Identical on `fs.alto-bfs.mesa5.hd`. Both are `we_write = false` in
formats.toml, so refusing is correct — two things about *how* are not.

The filesystem is called `unknown`, though `ls` and `inspect` identify it
correctly on the same file a moment earlier. Whatever the write path uses to
resolve a filesystem is not what the read path uses, so a user is told the
disk is unreadable when it was read. This is the same shape as
[R-031](#r-031), where a real Apple DOS 3.3 disk also arrives at the write
path as `unknown`, and it is worth checking whether one fix covers both.

And the code is 1. `exit.rs` gives PERMISSION_DENIED (4) explicitly for
"hitting a read-only filesystem on a write path" — this branch and no other.
A script cannot currently tell "this filesystem is read-only" from any other
generic failure. Compare [R-004](#r-004): the same habit of letting
`anyhow::bail!` pick the exit code.

Cases `edit.readonly.{lisa,alto}-refuses-a-write`.

### R-031 — a real Apple DOS 3.3 disk is detected as 'unknown' {#r-031}

**NOT A DEFECT — closed 2026-08-10.** The premise is wrong: the disk is not an
Apple DOS 3.3 disk. `Unknown` is the right answer.

`fs.apple-dos.invaders.floppy.dsk` has **no filesystem**. Track 17, where the
DOS 3.3 VTOC lives, is 4096 zero bytes. No VTOC-shaped sector exists anywhere in
the image even under loose criteria (any DOS version, 13- or 16-sector, 35- or
40-track). There is no ProDOS volume directory at block 2 and no Pascal volume
name. Meanwhile 68% of the disk is non-zero, tracks 0-3 are dense and tracks 17+
are empty — a bootloader that reads raw sectors, which is what an Apple II game
disk of this vintage usually is.

Sector ordering does not explain it: DOS-order and ProDOS-order `.dsk` images
differ in the arrangement of sectors *within* a track, so track 17 is empty
either way.

The entry advised trying [R-034](#r-034)'s fix first, on the grounds of "same
shape". They are not the same: R-034 was a *write refusal* naming the wrong
type; this is *detection*, and detection is right. What R-034 did give us is the
honest refusal this disk now produces — exit 4, "editing not yet supported for
filesystem type 'Unknown'".

The case asserted a `put`/`get` round-trip on a disk with no filesystem. It now
asserts the disk opens and that a write is refused, and records why.

---


```
rb-cli put fs.apple-dos.invaders.floppy.dsk payload.bin /PAYLOAD.BIN
  -> Partition @1 (None): Unknown 0x00 @ LBA 0, 143360 bytes
     error: editing not yet supported for filesystem type 'unknown'
```

Our own `new floppy apple-dos` output on the same path reports `DOS 3.3`, so
the detector works on what we write and not on a real disk of the same size
and geometry. The error message is about editing, which is misleading — the
volume was never identified. Case `edit.real.apple-dos-invaders`.

### R-032 — SFS `put` fails on any volume with a multi-leaf extent btree {#r-032}

```
rb-cli ls  fs.sfs.workbench-dh0.hd.img /    -> lists the tree fine
rb-cli put fs.sfs.workbench-dh0.hd.img payload.bin /PAYLOAD.BIN
  -> error: create_file: parse error: extent_btree_insert: only single-leaf BNDC supported
```

CLAUDE.md records the single-leaf restriction, so this is a documented limit
rather than a surprise — logged because a 499 MiB reference volume is the
normal case, not an edge one, and "SFS: edit" in the README reads as
unqualified. Read is unaffected. Case `edit.sfs.put-get`.

---

## Medium

### R-015 — cue sheets with unpadded track numbers are rejected {#r-015}

**FIXED 2026-08-10, upstream.** Fixed in `opticaldiscs` 0.15.0; bumping the
pin from 0.13.0 was the whole change on our side.

---


**Blocked upstream, confirmed 2026-08-08.** The cue parser is in the
`opticaldiscs` crate, not this repository. Bumping 0.13.0 -> 0.14.0 builds
without any API change and does **not** fix it, so the bump was reverted
rather than carried for nothing.

Minimal reproduction for upstream — `TRACK 1` instead of `TRACK 01`:

```
FILE "BOOKSHELF.img" BINARY
   TRACK 1 MODE1/2352
   INDEX 1 00:00:00
```

```
CUE error: Error(Msg("Expeceted number but found String(\"1\") instead"))
```

(The typo `Expeceted` is upstream's too, and pins the message's origin.)

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

**FIXED 2026-08-09.** The obstacle was structural, as with [R-004](#r-004):
`--format` is a per-verb argument, so by the time an error reached `main` the
selected format was gone with the verb's args.

`rb-cli` now parses through `clap::ArgMatches`, walks to the deepest
subcommand for a `--format`, and records it before dispatch
(`output::record_active_format`). The error arm calls
`output::emit_error_envelope_for`, which emits the documented envelope on
stdout when — and only when — the caller asked for a structured format;
`status.code` comes from `exit::code_for`, so the envelope and the process
agree. The plain-text line still goes to stderr, which is the human channel.

Reading clap's own matches rather than rescanning `argv` means a file named
`--format` cannot be mistaken for the flag, and a verb added later is covered
without touching the entry point.

The case asserted `expect_exit = 1`, which contradicted
`cli.exit.missing-image-file` once [R-010](#r-010) landed — a missing image is
`NOT_FOUND`, and `exit.rs` has reserved 3 for it all along. Corrected to 3
deliberately, not weakened to suit the fix.

---

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

**FIXED 2026-08-10. One fix closed R-008a, R-008b and R-024**, which the bug
list had filed as three problems across two components.

One bitmap block holds 127 words of bits, so it accounts for 4064 blocks. The
formatter wrote exactly one, whatever the volume size. A 4 MB volume is 8192
blocks, so marking the root block in use computed word index 128 and indexed
`bm[512..516]` in a 512-byte block — the panic, exit 101, no file (R-008b).
Below the panic threshold the same bug was silent: every block past 4066 simply
had no bit, so the allocator could hand out blocks the bitmap did not describe
(R-008a).

**R-024 was the same bug wearing a third hat.** "One `put` into a fresh 3 MB
volume makes `fsck --checkonly` report errors" was filed against the *editor*,
with the note that R-008 was the formatter and the two were distinct. They were
not: `put` allocated into the uncovered tail, and fsck correctly reported a
bitmap that disagreed with the directory walk. Nothing in the editor was wrong.
It went green with no change to the edit path at all.

The formatter now computes `ceil((total_blocks - 2) / 4064)` pages, writes them
all, records the first 25 in the root block's `bm_pages`, and chains any beyond
that through bitmap extension blocks (127 pointers plus a `next` each). Bits
past the end of the volume are cleared in the final page so the allocator can
never hand out a block that does not exist.

Verified at 1M, 3M, 4M, 8M and 32M: all create, all fsck clean. Five cases went
green — `fs.new-volume.affs{,.4m,.32m,.bitmap-boundary-plus-one}` and
`edit.affs.put-get`.

Note this does **not** address [R-020](#r-020): these volumes are still
"Not a DOS disk" on a real Amiga, and that needs an emulator or hardware oracle
to confirm either way.

---


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

**FIXED 2026-08-10, upstream.** `opticaldiscs` 0.15.0 adds
`FilesystemType::None`: a disc carrying no data track now reports that instead
of being refused. It is deliberately distinct from `Unknown`, which means
"there is a filesystem here and we did not recognise it". Our side was one
match arm in `fs_token` — the new variant made the match non-exhaustive, so the
compiler pointed at the exact place the fix had to land.
`optical.cdda.mixed-mode-still-opens`, the working-half case, stays green.

---


**Blocked upstream, confirmed 2026-08-08.** Same crate as [R-015](#r-015) and
same result on 0.14.0: still `No data track found`.

A pure CD-DA disc legitimately has no data track, so refusing to describe the
image is the defect — `optical info` should report the audio tracks and total
time. Minimal reproduction:

```
FILE "cdda-noaudiodata.bin" BINARY
  TRACK 01 AUDIO
    INDEX 01 00:00:00
  TRACK 02 AUDIO
    INDEX 01 00:05:25
```

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

**FIXED 2026-08-09.** Decided in favour of implementing the flag rather than
correcting the doc: `ls` is the most script-facing verb in the CLI, and the
doc described the more useful shape.

`ls` now takes `--format` with all five values. Text output is untouched —
it still runs through `print_entry`, so no existing case's column layout
moves. The structured paths collect an `LsRow` per entry instead: `kind`,
`name`, `size`, `type_code`, `creator_code`, `mode`, `uid`, `gid`, `owner`.
Every field is always present so the CSV header is stable across volumes, and
a filesystem carrying none of a column leaves it null. `--owner` still governs
whether ids are resolved to names, since that costs a read of the image's own
`passwd`/`group`.

CSV/TSV emission was private to `show.rs`; it moved to `output::emit_csv_or_tsv`
and `show` now delegates, rather than the two verbs growing separate copies.
The `rb://` remote listing path emits the same shapes, with `mode`/`uid`/`gid`
null because the wire protocol does not carry them.

---

`src/cli/output.rs` lists `ls` among the verbs that "can emit their results in
one of five formats", and its CSV/TSV note says those formats apply to "`ls`,
`show partmap`, `show devices`, `fsck` issue lists".

`rb-cli ls --help` exposes no `--format`, and it is not global either — both
`rb-cli ls img --format json` and `rb-cli --format json ls img` exit 2 with
"unexpected argument". Verified present on `inspect`, `fsck`, `du`, `locate`.

Either the flag was dropped and the doc is stale, or structured `ls` output is
a real gap. `ls` is among the most script-facing verbs in the CLI.

### R-010 — `inspect` has no `--fs-type` {#r-010}

**FIXED 2026-08-08.** Accepting the flag was not enough on its own, and it is
worth recording why. A signature-less filesystem has no partition table
either, so `PartitionTable::detect` failed before the forced type could be
applied — `inspect` took the flag and still refused the disk. It now does what
`ls` already did: with `--fs-type`, a detection failure means "raw filesystem
at byte 0" rather than an error.

```
rb-cli inspect ManicMiner.dsk --fs-type cpm:amstrad_data
  Partition table: None
    1  cpm:amstrad_data   0   180.0 KiB
```

A missing image also exits 3 now, which the case requires to tell "the flag
parsed and the file was absent" from "the flag was rejected". That made
`cli.exit.missing-image-file` contradict it: that case asserted 1 and called
itself "current documented-free behaviour", pinning the status quo rather than
the contract `exit.rs` states. Corrected to 3 deliberately, not weakened to
suit the fix.


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

**FIXED 2026-08-08.** `new volume prodos` with no arguments now writes a
volume named `RUSTY.BACKUP`.

The shared `--name` default is a named constant, and ProDOS substitutes its
own when that default is untouched. An explicitly passed `--name my-vol` still
fails, and should — the user asked for something ProDOS cannot store.

The message was the other half. It said "rename the file" about a *volume*
name, because one validator serves both and hard-coded the noun. It takes the
noun from the caller now:

    volume name contains '-' - ProDOS allows only letters (A-Z), digits (0-9),
    and '.' (spaces and most punctuation are not allowed)


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

**FIXED 2026-08-08.** The cause was structural, not a wrong constant: nothing
could carry an exit code out of a handler. `main` mapped every `Err` to
`GENERIC_FAILURE`, so any message naming a specific code was describing
something the process could not do.

`exit::CodedError` now carries one, with `exit::usage()` and
`exit::permission_denied()` constructors and `exit::code_for()` reading it back
in `main`. `code_for` walks the whole `anyhow` chain, so a later
`.context(..)` — added all over the CLI — cannot silently downgrade a coded
error back to 1.

Both instances use it: `require_non_flat` and the `.chd` output-extension
check. The unit test that missed this asserted only `is_err()`; it now asserts
the code.


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

**A second instance, found 2026-08-08** — the same shape in a different verb,
which makes it a pattern rather than one slip:

```
rb-cli shrink v.img nope.img   ->  exit 1
error: output path must end in .chd (got .../nope.img)
```

`exit.rs` reserves USAGE_ERROR "for handler-side usage-bad-input branches",
which is exactly what refusing a wrong output extension is. Every one of these
is an `anyhow::bail!` in a handler, so the fix is the same everywhere: a usage
rejection has to carry the code, not the message alone. Case
`shrink.rejects-non-chd-output`.

### R-011 — G64 fails on copy-protected / patched dumps {#r-011}

**DECIDED 2026-08-10:** copy-protected dumps *should* open — preserving those
disks is the reason G64 exists rather than D64 — and should be **un-editable**.

**Half of that shipped, and the investigation corrected the report.** G64 was
already un-editable: `is_editable_container_path` never listed it. But the
write path then fell through to opening the *undecoded container bytes*, which
reported `Invalid MBR: invalid boot signature` — the same error this report
attributes to copy protection. It reproduces on the **working** dump:

```
rb-cli ls  fmt.g64.gcr.floppy.g64   -> lists CEST LA VIE, CLV.O, CLV
rb-cli put fmt.g64.gcr.floppy.g64 f /HI
  -> error: detecting partition table: Invalid MBR: invalid boot signature
```

So that error was never evidence of a GCR problem. Any verb that took the write
path produced it on any G64. Now the whole class — G64/G71, MSA, EDSK,
Apple-II `.dsk`: containers the read path decodes and the write path cannot
re-encode — is refused with `PERMISSION_DENIED` (4) and a message naming the
reason and the conversion that gets you an editable copy. Same shape as
[R-034](#r-034). Cases `fmt.g64.{standard-dump-opens,is-read-only}`.

**What is still open, and why it cannot be closed here.** Whether the two named
dumps *decode* is unverified: neither is in the corpus or the dropbox, only the
working one is. `decode_g64_1541` already tolerates partial decode — it bails
only when *no* track yields sectors — so a protected disk with a readable
directory track should open today. Confirming that needs the files.

**To close this:** drop `Protector II (must be write protected).g64` and
`American Express (Magic Disk 64 1989-09 Side 2) [patched].g64` into the fixture
dropbox. The decision above says what the answer must be; only the evidence is
missing.

---


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

**FIXED 2026-08-09**, and the report had aged in both directions by the time it
was picked up. AHDI had been added incidentally on 2026-08-04 by
`e0c9bf6` ("write Atari AHDI root sectors"), two days after this was filed —
so that half was already closed. Meanwhile a tenth variant, `Dsd`, had been
added and was missing too. The same drift, still drifting, which is the
argument for the test rather than the edit.

README § Partition tables now has a row for **X68k** and for **DSD**.

`tests/doc_parity.rs::readme_documents_every_partition_table_scheme` reads
`PartitionTable::ALL_TYPE_NAMES` and requires a row per scheme, so this cannot
recur silently. Two guards make that list trustworthy: `type_name`'s match is
exhaustive, so a new variant is a compile error, and a `#[cfg(test)]`
exhaustiveness guard in `src/partition/mod.rs` pins the variant count against
`ALL_TYPE_NAMES.len()`, so extending one without the other fails to build or
fails the test. Verified by deleting the X68k row and watching the test name it.

---

`src/partition/mod.rs` defines nine `PartitionTable` variants: `Mbr`, `Gpt`,
`Apm`, `Rdb`, `Sgi`, `Sun`, `Ahdi`, `X68k`, `None`. README § Partition tables
lists only MBR, GPT, APM, RDB, SGI, Sun and "None (superfloppy)" — **Atari
AHDI and Sharp X68000 are absent**, despite both being fully implemented
(`src/partition/atari.rs`, `src/partition/x68k.rs`) and both having CLI
builders (`rb-cli new hd x68k`).

Exactly the drift class CLAUDE.md's pre-commit documentation-sync section
warns about.

### R-002 — `src/fs/README.md` capability table is stale {#r-002}

**FIXED 2026-08-09 — by deleting the table, which was the second of the two
options this report offered.** Regenerating it from the `fs/mod.rs` dispatch
would have produced a forty-row table two directory levels below the code it
describes, i.e. the same race with a longer starting line. The file now points
at the two live sources instead: the top-level README's Filesystems table for
capabilities, and the dispatch functions in `mod.rs` for routing.

One routing fact was worth keeping rather than deleting, and stayed: type byte
`0x07` covers both NTFS and exFAT, and `open_filesystem` disambiguates on the
OEM ID magic rather than the byte.

`tests/doc_parity.rs::fs_readme_has_no_hand_kept_capability_table` fails if a
`(planned)` claim or a capability table grows back there.

---

Still lists ext2/3/4 as "No (planned)" for browsing, compaction and resize,
and covers only six partition type bytes. The engine implements ext fully
(`src/fs/ext.rs`, `ext_format.rs`, `ext_fsck.rs`, `ext_csum.rs`) plus about
forty other filesystems. Either regenerate from `fs/mod.rs` dispatch or delete
it and point at the README.

---

### R-036 — a missing image gets three different exit codes {#r-036}

**FIXED 2026-08-10.** `inspect` was right and alone: it carried a per-verb
`if !args.image.exists()` check added with R-010, which no other verb copied.
The fix is one guard in the shared CLI resolver — `resolve::require_source_exists`,
called from the read-only, streaming and read-write entry points — so every
verb inherits it and one added later does too. `inspect`'s local check is gone.

Three things are exempt, none of them a file whose absence can be judged: a raw
device (`\.\PhysicalDrive0` does not `exists()`, and backing up a disk is
the app's core job), an `rb://` remote reference, and a backup folder, which is
a directory and passes `exists()` anyway.

`ls`, `du`, `fsck`, `show fs-info`, `locate` and `tar` now exit 3 with
`nosuch.img: no such file` instead of 1 with a platform-specific
`io::Error`. Six cases, red the same day they were written and green the next.


Found 2026-08-09 while closing R-005, which needed a verb whose missing-file
failure had a settled exit code. `exit.rs` reserves `NOT_FOUND` (3) for exactly
this — "image file missing" is the first example in its own doc comment — and
[R-010](#r-010) made `inspect` obey it. Nothing else does:

| verb | exit | message |
|---|---|---|
| `inspect` | **3** | `nosuch.img: no such file` |
| `ls`, `fsck`, `du`, `get`, `locate`, `tar`, `resize`, `repack`, `backup`, `show fs-info` | **1** | `open nosuch.img: The system cannot find the file specified. (os error 2)` |

**Correction to the first version of this table**, which listed `locate` and
`tar` at exit 2 and called that the worst of three answers. It was two answers,
not three: the survey invoked `tar` with two positionals when it takes three
(`IMAGE SRC OUT`), so clap rejected the command before the image was opened.
That 2 was correct — for a malformed command, not a missing file. Given correct
arity both verbs exit 1 like the rest. The case built on the bad invocation was
corrected with it.

Two things follow from the message, not just the code:

- It is a raw `std::io::Error`, so the text is **platform-specific**. Windows
  says "The system cannot find the file specified. (os error 2)"; Unix says "No
  such file or directory (os error 2)". Any case asserting on it has to be
  written per-platform or not at all.
- It names the syscall (`open`) rather than the thing the user got wrong.
  `inspect`'s "nosuch.img: no such file" is the shape to copy.

**Why it matters.** This is the CLI contract the regression harness itself
depends on: a script cannot tell "the disk is missing" from "the disk is
corrupt" without switching on the code, which is the entire reason the table in
`exit.rs` exists. It is also the cheapest class of fix left — `exit::not_found`
already exists and is already used by `inspect`.

**Cased 2026-08-10**, one per verb because each is a separate contract a
script switches on: `cli.exit.{ls,du,fsck,show-fs-info,locate,tar}-missing-image-is-not-found`,
all red, all citing this finding. `cli.exit.missing-image-file` is the green
counterpart pinning `inspect` at 3.

---

### R-037 — `resize` cannot shrink safely, and shrinking destroyed data {#r-037}

**FIXED 2026-08-09.** Found by reading [R-021](#r-021)'s closing note, which
claimed a shrink left "trailing slack, not damage". It did not, and that note
is corrected in place.

The reproduction, on a 64 MB FAT volume holding a 30 MB file:

```
rb-cli resize v.img --size 16M
  -> "FAT16: clusters 32695 -> 8119"
     "resize complete", exit 0

rb-cli get v.img /D30.BIN out.bin
  -> exit 0, out.bin is 16,629,760 bytes   (the source is 30,720,000)

rb-cli fsck v.img --checkonly
  -> ERROR [ChainPointerInvalid] /D30.BIN: cluster 8120 has an invalid forward link
     ERROR [SizeExceedsChain]    /D30.BIN: size claims 15000 clusters but only 8119 are allocated
```

The FAT was rewritten for the smaller volume while the file's chain still ran
past the new end. `resize` exits 0, `ls` still reports the full size, and `get`
**also exits 0** while returning a short file — so nothing in the chain tells
the caller the data is gone. The image was never truncated either, so the
freed space was not even given back: the operation managed to be destructive
and useless at the same time.

**The fix has three parts**, all in `src/cli/verbs/resize.rs`:

1. **A data floor.** The filesystem is the only thing that knows where its data
   ends, so it is asked: `Filesystem::last_data_byte` — "bytes from the
   partition start needed to hold everything allocated". A shrink below that is
   refused outright, `--confirm-shrink` or not, and the message names the
   smallest safe size. The trait's default implementation returns `total_size`,
   so a driver that does not override it refuses every shrink; that is the
   correct default, because without an answer no shrink can be shown to be safe.
2. **`--confirm-shrink`.** A shrink that *is* safe still truncates the image,
   which cannot be undone, so it has to be asked for. Growing needs no flag.
3. **Truncation.** After the filesystem resize commits, the image is truncated
   to the new size — but only when the volume is the whole file
   (`whole_file_path`), the same condition R-021 established for growing. A
   partition inside a larger disk has data after it, so there the filesystem
   shrinks and the image keeps its length, and the log says to follow up with
   `partmap resize`.

Verified in all three directions on a 64 MB FAT volume: shrinking to 16 MB with
40 MB of data is refused (exit 2, image untouched, fsck clean); shrinking to
40 MB without the flag is refused (exit 2, image untouched); shrinking to 40 MB
with the flag resizes the filesystem, truncates the image to 40 MiB, and the
file reads back byte-identical with a clean fsck.

Cases: `resize.shrink.{refuses-cutting-live-data,needs-confirmation,keeps-data-and-truncates}`.

**Two things this does not cover**, both deliberate and both worth a follow-up:

- **The remote path.** `resize rb://host/img --size` goes through
  `resize_remote_partition` and does not consult `last_data_byte`. Adding an
  unverified guard to a path with no daemon running to test it against would be
  worse than recording the gap.
- **The GUI.** "Resize Partitions…" calls `resize_filesystem_for` directly, so
  the floor lives in the CLI verb rather than in the shared engine. Per
  CLAUDE.md's shared-logic rule the check belongs in a core module both
  surfaces call; that refactor is larger than this fix.

---

## Regression coverage

Which finding is guarded by which case, so a fix cannot silently regress.
Run `rb-regress run --tiers 0-4` to check them all.

| Finding | Case | State |
|---------|------|-------|
| R-003 | `cli.envelope.ls-supports-format` | **green — fixed** |
| R-004 | `cli.exit.{csv,tsv}-on-nested-verb-is-usage-error` | **green — fixed** |
| R-005 | `cli.envelope.error-envelope-on-failure` | **green — fixed** |
| R-006 | `fs.new-volume.prodos-default-name` | **green — fixed** |
| R-007 | `fs.new-volume.ntfs{,.2m-fsck,.32m-fsck}` | **green — fixed** |
| R-008a | `fs.new-volume.affs{,.bitmap-boundary-plus-one}` | **green — fixed** |
| R-008b | `fs.new-volume.affs.{4m,32m}` | **green — fixed** |
| R-009 | `fs.read.{jfs,reiserfs,ufs1,ufs2}` | **green — fixed** |
| R-010 | `cli.flags.inspect-accepts-fs-type` | **green — fixed** |
| R-013 | `fs.detect.ufs-{solaris-entry-types,no-absurd-sizes}` | **green — fixed** |
| R-028 | `edit.apple-dos.put-get` | **green — fixed** |
| R-031 | `edit.real.apple-dos-invaders` | **green — not a defect** |
| R-030 | `edit.real.affs-workbench13` | **green — fixed** |
| R-011 | `fmt.g64.standard-dump-opens` | green — **pins the working half only** |
| R-012 | `optical.cdda.no-data-track-opens` | **green — fixed upstream** |
| R-013 | `fs.detect.ufs-{solaris-entry-types,no-absurd-sizes}` | red |
| R-015 | `optical.cue.unpadded-track-number` | **green — fixed upstream** |
| F-008 | `backup.container.{chd,vhd-dynamic,qcow2,vmdk-sparse}` | red — a feature gap, was R-016 |
| R-001 | `doc_parity::readme_documents_every_partition_table_scheme` | **green — fixed** |
| R-002 | `doc_parity::fs_readme_has_no_hand_kept_capability_table` | **green — fixed** |
| R-018 | `doc_parity::contributing_vintage_features_match_ci` | **green — fixed** |
| R-021 | `resize.to-explicit-size` | **green — fixed** |
| R-023 | `resize.repack.{keeps-data,refuses-plain-fat}` | **green — fixed** |
| R-022 | `roundtrip.hpfs.raw`, `fs.detect.hpfs-{bare-volume,backup-is-not-empty}` | **green — fixed** |
| R-036 | `cli.exit.{ls,du,fsck,show-fs-info,locate,tar}-missing-image-is-not-found` | **green — fixed** |
| R-011 | `fmt.g64.{standard-dump-opens,is-read-only}` | **green — read-only half fixed** |
| R-037 | `resize.shrink.{refuses-cutting-live-data,needs-confirmation,keeps-data-and-truncates}` | **green — fixed** |
| R-017 | `fs.detect.sfs-bare-volume` | **green — fixed** |
| R-025 | `subcmd.squashfs.put-rebuilds`, `meta.xattr.set-list-rm` | red — Windows only |
| R-026 | `subcmd.show.partmap` | **green — fixed** |
| R-027 | `read.apfs.apple-gpt` | **green — fixed** |
| R-034 | `edit.readonly.{lisa,alto}-refuses-a-write` | **green — fixed** |

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

- ~~**R-001, R-002**~~ — **now covered.** `tests/doc_parity.rs` is the
  source-parity test this entry asked for. It is a `cargo test`, not an
  `rb-regress` case, because the claim is *about* the binary rather than
  something the binary does — the suite runs `rb-cli` and cannot see a stale
  markdown table. R-018 is covered by the same file.
- **R-011** — only the working half is pinned. No case asserts that
  copy-protected G64 dumps open, because whether they should is undecided;
  asserting either way would prejudge it.
- ~~**R-018**~~ — **now covered**, by exactly the test that entry predicted:
  `doc_parity::contributing_vintage_features_match_ci` asserts CONTRIBUTING.md's
  vintage feature list appears verbatim in `.github/workflows/release.yml`.
- **R-014** — a lint failure, not runtime behaviour. The pre-commit hook is
  itself the regression guard: it runs `clippy --all-targets -- -D warnings`
  on every commit, so a reintroduction cannot be committed.

## Suggested order

Rewritten 2026-08-09; 21 of 35 findings are closed and the ordering that
remains is different from the one that got us here.

1. **R-008b** — a panic with no file produced is the worst failure mode left,
   and R-008a shares its fix.
2. ~~**R-033**~~ — done 2026-08-10. The prediction held exactly: R-022's
   shape, a bare volume falling through to the MBR parse because no probe in
   `detect_superfloppy` claimed it. The fix was a dozen lines beside the HPFS
   probe.
3. **R-013** — wrong entry types and an absurd size are user-visible
   immediately.
4. **R-024** — the AFFS editor. Distinct from R-008 (formatter) and R-020
   (root block); do not conflate them.
5. **R-011** — decide scope first. The last open decision.
6. Everything else is Tranche C in
   [`regression-fix-prompt.md`](regression-fix-prompt.md): the symptom is
   recorded, the cause is not, and the investigation is the deliverable.

**Two of the closed findings had the wrong cause on file** — R-023 ("every file
is gone"; nothing was lost) and R-022 ("not byte-identical"; the backup was
empty). Both had a one-command control that settled it. Reproduce and *measure*
before fixing anything below.
