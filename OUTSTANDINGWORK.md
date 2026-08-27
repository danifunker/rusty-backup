# Outstanding work — regression tooling for the new filesystems and tables

Branch `feature/new-fs-beos-next-solaris`. Written 2026-08-24, **revised
2026-08-25** after the corpus drop landed on the NAS. Companion to
`~/nextstep-test/RESUME-PROMPT.md`, which is still the authority on the five
original tasks.

The engine work is finished. Everything below is tooling, corpus and oracles.

---

## What landed on 2026-08-23/24 (context, not work)

- **All three Solaris disks boot under QEMU**, reproducibly, from scripted
  `-snapshot` runs. Each runs Sun's own `fsck` against UFS during startup.
  That is a real oracle, not a spike.
- **BeOS/PPC has no emulator** — proven, not assumed. A physical Mac is the
  only oracle big-endian BFS can have, and the test artifact for it is built.
- **EFS v1 shipped, with IRIX 3.7 itself as the oracle** (cc5300b, 8c891c9).
  `resume-prompt-irix37-efs.md` in the repo root is the *spent* prompt for
  that work — it is untracked and can be deleted.
- Memory notes carry the detail: `solaris-qemu-boot-recipes`,
  `emulator-firmware-inventory`, `new-fs-fixture-status`,
  `iris3130-motion-oracle`.

---

## 1. Corpus drop — **done**; only the registry rows (§1c) still open

The eight-image drop is copied and catalogued. Verified 2026-08-25 against
`smb://daninas.local/software/rb-fixtures`: all eight files present under
`fixtures/` + `fixtures-large/`, and all eight rows in the NAS
`fixture-map.tsv` (now 100 data rows). A ninth has since been added — see §2.

| ID | Size | Reads as |
|---|---:|---|
| `fs.ofs.hobbit-1993.hd` | 162 MiB | bare BeOS OFS v1, no partition table |
| `part.next.ns33-intel.hd` | 235 MiB | NeXT label (no MBR) + LE 4.3BSD UFS |
| `part.sun.solaris24-sparc.multipart` | 364 MiB | Sun SMI VTOC, 4 slices, BE UFS |
| `part.next.ns33-m68k.hd` | 512 MiB | NeXT label + BE 4.3BSD UFS |
| `part.solaris-x86.solaris26-intel.multipart` | 889 MiB | MBR 0xBF + **nested** VTOC + LE UFS |
| `fs.bfs.r5-intel.hd` | 977 MiB | MBR 0xEB + LE BFS |
| `fs.bfs.ppc-bigendian.multipart` | 999 MiB | APM: HFS + two **big-endian** BFS |
| `part.sgi.riscos-mips.multipart` | 1001 MiB | CHD; SGI volume header, 9 slots, UFS |

**Do not minimise these.** Five are bootable OS installs, which
`EMULATOR-IMAGES.md` exempts from the 250 MB target precisely because shrinking
one destroys the thing that makes it valuable. If a small fixture is wanted for
a fast tier-2 read case, carve a new one rather than shrinking these.

### 1a. Local corpus — **done 2026-08-25**

All eight files are in this box's `fixtures/` + `fixtures-large/`, **hardlinked**
from `~/NewFixtures/_staged/` rather than copied: both trees are on the same
filesystem, so the corpus cost zero extra bytes on a disk that was already 90%
full, and the staged drop is still intact. `ln`, not `mv` — nothing was moved
or deleted.

The nine catalogue rows are appended here too (92 -> 101), matching the NAS
exactly. `local.toml` still claims *"this host cannot reach the distribution
share"*; **that is no longer true** — it mounts through gvfs at

```
/run/user/1000/gvfs/smb-share:server=daninas.local,share=software/rb-fixtures
```

Worth correcting in `local.toml`, and worth knowing the share is append-only
from here.

### 1b. The orphan inode — **resolved differently**

`part.next.ns33-m68k.hd` carries a pre-existing orphan inode:

```
ERROR  [OrphanInode] inode 20654 (mode=0o100644, size=7133) is unreachable from root
```

This file previously said it needed a `data/known-failures.toml` row. **It does
not, and should not have one.** That file is for cases failing on a recorded
defect *of ours*; this is a property of third-party media. The read and edit
cases now assert it directly — `expect_exit = 1` plus the inode number — so the
damage is pinned rather than excused. The edit case re-asserts it *after*
writing, which turns it into a real assertion: still exactly one error means
our writer added no second one.

Same treatment for the two other damaged volumes found while authoring:
`fs.ofs.hobbit-1993` (546 leaked sectors, as the README documents) and
`fs.bfs.ppc-bigendian.multipart@2` (3 leaked blocks, newly found).

### 1c. The appended rows made Task A concrete

`rb-regress validate` was clean before; with the nine rows in place it names
exactly which registry entries are missing:

```
registry: 125 format(s), 46 oracle(s), 0 host(s)
  problem: registry: fixture 'fs.bfs.r5-intel.hd' implies format 'fs.bfs', which is not in formats.toml
  problem: registry: fixture 'fs.bfs.ppc-bigendian.multipart' implies format 'fs.bfs', ...
  problem: registry: fixture 'fs.ofs.hobbit-1993.hd' implies format 'fs.ofs', ...
  problem: registry: fixture 'part.next.ns33-intel.hd' implies format 'part.next', ...
  problem: registry: fixture 'part.next.ns33-m68k.hd' implies format 'part.next', ...
  problem: registry: fixture 'part.solaris-x86.solaris26-intel.multipart' implies format 'part.solaris-x86', ...
```

Four format IDs to add to `data/formats.toml`: **`fs.bfs`**, **`fs.ofs`**,
**`part.next`**, **`part.solaris-x86`**. `part.sun`, `part.sgi` and `fs.efs-v1`
already resolve, so they need nothing. This is Task A in §7, reduced to a list.

### 1d. Cases — **written 2026-08-25**

The nine fixtures were referenced by no case at all. Two manifests now cover
them, both green except where a real defect is pinned:

* **`cases/tier2/read-new-fs-fixtures.toml`** — 9 cases. `inspect` -> `ls /` ->
  `fsck`, the shallow tier-2 contract. All 9 pass; the whole of tier 2 is
  103/103.
* **`cases/tier3/edit-new-fs-fixtures.toml`** — 10 cases. The gamut is
  `put` -> `ls` -> `get` -> byte-compare -> `mkdir` -> `chmod`/`chown` -> `rm`
  -> `fsck`, all on `{fixture_copy}`. 7 pass, 3 xfail against R-043.

Two assertions in there are worth more than they look:

* **The damaged volumes are pinned, not excused.** 546 leaked sectors before an
  edit and 546 after is the assertion that our allocator leaked nothing new;
  "it was already dirty" is otherwise exactly where a real defect hides.
* **`edit.new.efs-v1-irix37-root-untouched`** writes to the `/usr` slice and
  then asserts the *root* slice is unchanged. That disk's slot `f` aliases slot
  `c`, so a write that resolves the wrong slot lands in the wrong filesystem.

### 1e. R-043 — every edit verb refuses a dynamic VHD

Found while authoring §1d, because three of the new fixtures are dynamic VHDs
and not one accepted a write.

```
rb-cli ls   part.next.ns33-intel.hd.vhd@1 /   -> lists the NeXTSTEP root
rb-cli fsck part.next.ns33-intel.hd.vhd@1     -> 14476 files / 3037 dirs, clean
rb-cli put  part.next.ns33-intel.hd.vhd@1 ... -> Invalid MBR: expected 0xAA55, got 0x0000
```

`open_rw` in `src/cli/resolve.rs` dispatches on container type and has branches
for CHD, QCOW2, the read-only GCR/MSA/EDSK family and the editable floppy/gzip
containers — but **none for VHD**. The file falls through to a plain `File`, and
a dynamic VHD starts with a *copy of its footer*, so byte 510 is `0x00`. The
QCOW2 branch immediately above carries a comment describing this exact failure
for its own format.

Scope, probed rather than assumed: raw / QCOW2 / CHD / **fixed** VHD all write
fine; only **dynamic** VHD fails. Filesystem-independent — reproduced on UFS
behind a NeXT label, BFS behind an MBR, and a plain FAT volume. Same root cause
as F-008, which shipped for `backup` on 2026-08-15 and did not touch the edit
path; the fix shape is either a `ContainerEditSession` or a `Read + Write + Seek`
VHD reader in the manner of `Qcow2Reader`.

Filed as **R-043** in `docs/Regression_Bugs.md` with a fixture-free repro; the
three cases assert intended behaviour and are listed in
`data/known-failures.toml` until it lands.

---

## 2. The IRIS 3130 disk — **now tracked** (was not)

`~/repos/motion/scratch/hd/3130-si0-gui.img` was not in the catalogue, on the
NAS, or referenced anywhere in the repo. It is now `fs.efs-v1.populated-irix37.multipart`
— on the NAS under `fixtures/`, in this host's corpus, and in both catalogues.

```
zstd -19       5,921,229 bytes   367ccfeb243965fa2ef4c5443623a5d6a93a446ff8c8de38a2551fde3ab0aaf6
raw           60,135,936 bytes   bc10aed52e9a7b46e4d82e49341564ef4038ab9830947cdb4bab54c551d3b025
```

**5.6 MB compressed**, so it goes in the core corpus, not the annex. It is
exactly the fixture `FIXTURES.md` § "Populated-system fixtures" was asking for
— *"a populated … EFS volume, small enough to admit"* — and that section has
been updated to admit it. Verified with the release `rb-cli` before cataloguing:

```
Partition table: SGI-DkLabel (byte-swapped)
  1  slot 0  SGI root (EFS v1)   LBA    119   8.7 MiB  boot
  2  slot 1  SGI swap            LBA  17969   8.7 MiB
  3  slot 2  SGI slice (EFS v1)  LBA  35700  38.9 MiB
```

both EFS v1 volumes listing real trees (`/usr` has 2754 files, `mex` included).

Two things a future reader will otherwise re-derive the hard way:

- **The label has four populated slots, not three.** Slot `f` points at the
  *same extent* as slot `c`, because `/etc/brc` maps a 3130 to `usr=si0f` where
  a 3115 gets `md0c`. `inspect` shows three because `is_wrapper_slot`
  (`src/partition/sgi_dklabel.rs:334`) treats a later slot with an identical
  `{base,size}` as a duplicate wrapper. **That is correct behaviour, not a
  missing slice** — worth knowing before someone files it as a bug.
- **It boots unattended to a login prompt**, unlike `~/3130.img`: `initdefault`
  is `2`, `/etc/rc.getdate` is replaced with an executable `date +...`, and
  `bcheckrc` has `3130` added to its auto-check case. That is what makes it
  usable as an oracle disk with no console typing.

### Why this matters beyond one more fixture

It is a *populated* EFS v1 volume — hundreds of directories, thousands of real
files, real Unix owner/permission bits, a meaningfully full bitmap. Tier 3
mutation cases (`put` / `rm` / `mkdir` / `chmod` / resize with the assertion
that *nothing else changed*) have had nothing like it for EFS v1; the synthetic
`fs.efs-v1` volumes have no fragmentation and nothing valuable to corrupt.

Still to do: write those tier-3 cases, and decide whether the Motion oracle
should switch its `$IRIS_DISK` default to this image (it boots without
intervention, which the current default does not).

---

## 3. The Motion oracle's two headline limitations are **stale**

`regression-tests/oracles/motion/README.md` § "Five things that cost time" and
the `motion` entry in `data/oracles.toml` both say the emulator opens the disk
read-only and wires drive 0 only. Both were true when written. Both are fixed
on Motion's **`ai-main3`** branch, read 2026-08-25:

| Claim in our docs | On `ai-main3` |
|---|---|
| *"`dsd5217.cpp` calls `Profile::OpenDisk(0)` and nothing else; `profileDisk1Path` exists but is unwired"* | Both controllers loop over **two** drives — `dsd5217.cpp:55` over `DSD5217_MAX_DISK_DRIVES` (2) and `storager2.cpp:39` over `STORAGER2_MAX_WINCHESTERS` (2) — and `Profile::OpenDisk` resolves id 0/1 to `profileDisk0Path`/`profileDisk1Path`. |
| *"the emulator holds the disk read-only"*, so guest writes are unavailable | `src/base/filesystem/disk_image.cpp` adds a write mode. `diskWriteMode` defaults to **`direct`** — "so that a machine still keeps what it writes". `overlay` is copy-on-write, and `diskCommitOnExit 1` commits an overlay on close. |

**Check the branch before believing any of this.** `~/repos/motion` was mid
`git rebase ai-main3` while this was written, with a conflict left in
`coherent_core.cpp`, and the working tree flipped between the two shapes inside
ten minutes — `storager2.cpp` existed for one grep and was gone for the next.
`main` has none of it: no Storager, no write modes, `OpenDisk` still returning
a `FileStream*`. So verify with `git grep <pattern> ai-main3 -- src` against a
ref, never against the working tree, and re-derive the line numbers once the
rebase lands.

Each of those unblocks something we currently record as impossible:

- **Attach a whole rb-cli-built disk as the second drive.** This is what makes
  `part.sgi-dklabel` *"declared but untested"* in the oracle README and
  "unproven" in `docs/partition_table_writers_backlog.md:306`. With drive 1
  wired, IRIX can be pointed at a label we wrote from scratch — no splicing a
  volume into the reference disk's `/usr` slot, and the partitioning itself
  finally gets checked rather than bypassed.
- **Let the guest write.** This is the step that closed R-039 on the Indy and
  that the Motion README's last line says is unavailable here. `overlay` +
  `diskCommitOnExit 1` gets the writes back out without ever risking the
  fixture; `direct` is the one to avoid on a corpus file.

Update the README, the `oracles.toml` `notes` and the backlog entry together —
all three repeat the same stale claim — and say which Motion revision the new
claim is true of, since this one clearly moves.

---

## 4. Register the three Sun boots as oracles

All three verified 2026-08-23 with stock Ubuntu QEMU 8.2.2, always `-snapshot`
so the fixture is never written. Recipes in full in the
`solaris-qemu-boot-recipes` memory note; scripts in
`~/nextstep-test/boot-spike/`.

| Disk | Status | Firmware | Verdict channel |
|---|---|---|---|
| Solaris 9 SPARC (`~/solaris9/disk-0.qcow2`) | boots to multi-user | OpenBIOS, stock | `checking ufs filesystems` -> `/dev/rdsk/c0t0d0s7: is stable.` |
| Solaris 2.4 SPARC (new fixture) | boots to `The system is ready.` | `~/solaris9/ss5.bin`, drive at `unit=3` | same, on `c0t3d0s7` |
| Solaris 2.6 x86 (new fixture) | kernel loads; console lost | SeaBIOS, stock | **see §5** |

There is no `previous` entry and no Sun/Solaris entry in `data/oracles.toml`
today. `86box-os2` is the precedent for an emulator oracle. `cases/` has
`tier0`..`tier5` only — **tiers 6 and 7 have no case directories at all**, so
this also establishes the shape for tier 7.

Follow the pattern the three proven oracles use (`oracles/fsuae/affs_mount.py`,
`oracles/iris/`, `oracles/motion/`): **the guest writes a result file and the
host polls for it. No screen scraping.** Make the case `skip-tool` when QEMU or
the disk is absent, per the prime directive. A boot is minutes, so budget
accordingly.

### The three traps, each of which reads as a corrupt disk

- **OpenBIOS cannot load Solaris 2.4's a.out kernel.** `Not a bootable ELF
  image` -> `Loading a.out image…` -> `Unhandled Exception 0x00000009`. It needs
  the real `ss5.bin` PROM.
- **`-prom-env boot-device=disk` is silently ignored by a real PROM.** It works
  on OpenBIOS only. Drive the `ok` prompt over serial instead — pipe
  `sleep 35; printf 'boot disk\r'` into a `-nographic` QEMU.
- **`boot disk` is SCSI target 3, not target 0**, and the 2.4 image *wants* to
  be target 3 — its own `vfstab` mounts `/usr` from `c0t3d0s6`. At `unit=0` you
  must say `boot disk0`, and the kernel then loads but halts with
  `WARNING: /usr/sbin/fsck not found`. Attach at `unit=3` and plain `boot disk`
  works.

---

## 5. Give Solaris 2.6 x86 a serial console

The only Sun disk not yet usable headless. It boots correctly — reaches
`SunOS Secondary Boot version 3.00`, runs the Configuration Assistant,
autoboots from `/isa/ata@1,1f0/cmdk@0,0:a` and loads the kernel — then **serial
goes quiet because Solaris x86 hands the console to VGA**. That is not a hang;
it sat there until the `timeout` fired.

Needs the console requested explicitly (a `-b console=ttya`-style boot argument
or an `eeprom` setting on the image) before it can report a verdict. Worth
doing: it is the only fixture with a **nested** Solaris VTOC inside an MBR, and
the only little-endian UFS oracle.

Command that gets it to the kernel today, for a starting point:

```sh
qemu-system-i386 -M pc -m 256 -snapshot -nographic \
  -drive file=part.solaris-x86.solaris26-intel.multipart.vhd,format=vpc,if=ide,index=0,media=disk
```

---

## 6. The BeOS Mac — big-endian BFS on physical hardware

**`~/beos-oracle/bfs-bigendian-apm.img`** — 110 MiB, uncompressed raw, APM disk
with one big-endian `Be_BFS` volume named `RBTest`. Every byte written by
`rb-cli`; nothing on it came from BeOS. Our fsck calls it clean (405 files /
10 dirs) and all 400 `manyfiles/` entries read back, **so any disagreement from
BeOS is the finding.**

Contents chosen to hit where the writer is most likely wrong: `manyfiles/` with
400 entries (directory B+tree splits — the highest-risk path), an 8 MB file
(indirect + double-indirect runs), five levels of nesting, a 200-character
filename, a UTF-8 filename, and an empty `RESULT/`.

The volume carries a `README.TXT` so it is self-describing once mounted.
`~/beos-oracle/README.md` has the full handoff. The short version:

1. Write to media, mount on the Mac, run `chkbfs -v` and `ls -lR` into
   `RESULT/`.
2. **Copy a file of your own into `RESULT/`** — a write by BeOS is what proves
   our allocator left the volume in a state BeOS can extend.
3. Unmount cleanly, bring the media back, re-image, then:

```sh
rb-cli get  <image>@1 /RESULT ./          # verdict, read back through our driver
rb-cli fsck <image>@1 --checkonly         # our opinion after BeOS touched it
rb-cli ls   <image>@1 /manyfiles | wc -l  # still 400?
```

Reading the verdict through our own driver exercises the read path at the same
time, which is why sneakernet is the *stronger* version of the shared-folder
pattern here, not a compromise.

This lands at tier 7B in `EMULATORS.md` — the "Real hardware" row. Not
`HARDWARE.md`; that document is about physical backup/restore of media, a
different axis.

**Why there is no emulator alternative:** `qemu-system-ppc -M g3beige` +
OpenBIOS reaches `Trying hd:,\:tbxi… No valid state has been set by load or
init-program` — it never finds a Mac bootloader. Confirmed, not assumed;
`beos-ppc-g3beige.log` in the repo root is the (one-line) timeout record, and
can be deleted once that sentence is trusted.

If the Mac can also run BeOS R5 for Intel, the little-endian counterpart is a
five-minute build: `new volume bfs` without `--big-endian`, wrapped in an MBR
type `0xEB` instead of APM.

---

## 7. Still open from RESUME-PROMPT.md, untouched

Listed so this file is not mistaken for the whole picture. Full detail in
`~/nextstep-test/RESUME-PROMPT.md`.

- **Task A — the registries are stale.** `data/formats.toml` and
  `data/oracles.toml` do not know we write UFS1, BFS, OFS, NeXT labels or
  Solaris x86 VTOCs. **§1c now names the four missing format IDs.** `COVERAGE.md` and `VERIFICATION-MATRIX.md` are generated
  from them, so fixing layer 1 fixes the docs. `rb-regress query
  unverified-writes` returns empty today because the registry does not know;
  after Task A it should *grow*, then shrink as oracles land.
- **Task B — tier 2/3 cases.** §1 above is only the corpus half. Tier 2
  (read/inspect/fsck over third-party images) is where read correctness is
  actually established; the committed tier-1 cases prove nothing about format
  correctness by the suite's own doctrine. §2 adds the first real tier-3
  target for EFS v1.
- **Task C — Previous / NeXTSTEP as a tier-7 oracle.** The harness works and
  found three real bugs. Step 1 (append to `/private/etc/rc` on a scratch boot
  disk) worked; step 2 was blocked by our own summary-area defect, now fixed
  and **untested end to end**. Retrying that is the first thing to do.
- **Task D — teach `fsck_ufs` the invariants NeXTSTEP checks.** The cheapest
  high-value item, and it needs no emulator: `cg_cs`/`fs_cs`/`fs_cstotal` must
  agree; counters must not go negative (`cs_nffree` reached `-5`); directory
  chunking must match the volume's `DEV_BSIZE` (1024 on NeXTSTEP, not 512).
  Confirm each new case fails against `git stash`-ed pre-fix code before calling
  it done.
- **Task E — 86Box.** Partly superseded: Solaris 2.6 x86 boots under plain
  `qemu-system-i386` with no extra firmware, so 86Box is no longer the only
  route to the nested VTOC. It remains the likely route to **NeXTSTEP/Intel**,
  which is picky about hardware and was produced under 86Box (the drive string
  in its label is literally `86Box 86B_HD00 3.50-512`). 86Box needs its binary
  *and* its separately-distributed `roms/` bundle; neither is on this box.

---

## 8. The GUI browse gate — surveyed and fixed 2026-08-27

Reported as "no Browse button on a UFS volume". It was never UFS-specific:
`partition_is_browsable` (`src/fs/mod.rs`) is the single gate the Inspect grid,
both Commander panes and the nested-image picker all call, and it had drifted
behind the engine on four separate axes.

**Method.** The predicate was mirrored in Python and run over every partition
row the corpus produces — all 100 catalogued fixtures (115 rows) plus a freshly
built volume for each of the 22 `new volume` filesystems — then every miss was
re-checked by asking whether `ls` actually opens it. 37 rows failed the gate;
28 of them were wrong.

**What was hidden, and why:**

| Cause | Examples |
|---|---|
| Superfloppy name list | bare `HPFS`, `BFS`, `BeOS OFS`, `SGI EFS v1` |
| Type-byte allowlist | `0xEB` BeOS BFS; `0xA5`/`0xA6`/`0xA9`/`0xBF` BSD & Solaris UFS |
| APM type-string list | `Be_BFS` (both big-endian volumes on the PPC disk) |
| Labels with no type byte | `NeXT a (4.3BSD)`, `Sun root (UFS?)`, `Solaris s0 (root)`, `SGI BSD` |

So every BeOS filesystem we shipped this cycle was unbrowsable, as was HPFS,
as was a bare EFS v1 volume — which browses fine *inside* an SGI-DkLabel,
because that path mints type byte `0xA2`. **The README already advertised
`Browse: Yes` for all five**, so the docs were right and the gate was the drift.

**The fix.** Three list additions, plus `is_browsable_scheme_slice` for the
four Unix disk labels that deliberately emit `partition_type_byte: 0` so
`open_filesystem` probes the superblock. Reserved roles are excluded through a
shared `is_reserved_slice_role` — swap, boot, altsctr, and the volume-header /
replacement wrappers.

**Excluding swap is load-bearing, not cosmetic.** A Solaris swap slice falls
through to the synthetic carve view; the IRIS 3130's swap slot surfaces a
*stale directory tree* that is not root (`.profile` is 2478 bytes there against
root's 149). Both would have looked like real volumes.

Separately, `SgiPartitionType::is_skipped_from_browse` was missing `TrkRepl`
and `SecRepl`, so a RISC/os disk listed a 0-byte `SGI SECREPL` row whose base
aliases slot 0 — browsing it re-listed slot 0's filesystem. Fixed at source, so
those rows never reach the partition list.

**Not changed, deliberately.** The Check button is nested inside the browse
gate, and `is_checkable_fs_name`'s own doc comment says that is intended
("these all reach the Check button through the browsable path"). Un-nesting it
would give swap a Check button, since `is_checkable_fs_name("Sun swap (UFS?)")`
matches on the substring `UFS`. The nesting is correct now that the gate is.

**Still open from this:**

- **The scheme-slice predicate reads a display string.** It is the only signal
  those rows carry today. The sturdy version is a role field set by the label
  parsers; it was not done here because `PartitionInfo` has 45 construction
  sites across 12 files and the churn outweighed the benefit mid-branch. The
  tests pin every string the corpus actually produces, so a label-format change
  fails in `cargo test` rather than in the GUI.
- **Three loose ends the survey exposed, all separate from the gate:** the
  synthetic carve view ignores partition bounds (a 313 MB Solaris slice offers
  a 334 MB `whole-disk.img`; an 8 MB slice offers 3.2 GB); a 0-byte slot could
  open a neighbour's filesystem; and EFS detection fires on an IRIS swap slot.
- **No GUI/CLI parity test exists for the gate.** `rb-cli inspect` exposes a
  `browsable` column for SGI volume headers only. Widening that to every scheme
  would let the regression suite assert the gate through the CLI, which is what
  would have caught this drift.


## Constraints

- Build with `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0
  CARGO_PROFILE_TEST_DEBUG=0`. 16 GB / 6 cores; `cargo test` at full parallelism
  gets the lib test binary **OOM-killed (SIGKILL)** — use `CARGO_BUILD_JOBS=1
  RUST_TEST_THREADS=3`, and read a bare SIGKILL as memory pressure, not a bug.
- Engine code under `src/` must compile on **Rust 1.73**; verify with the
  command in CONTRIBUTING.md § "Rust 1.73 floor for engine code". A normal build
  will not catch a violation and clippy actively suggests some.
- The pre-commit hook is `.git/hooks/pre-commit` — do **not** pass
  `-c core.hooksPath=.githooks`, which silently disables fmt / check / clippy /
  CLI-doc regeneration.
- Comments in `.rs` are one line, two at the absolute most.
- `COVERAGE.md` and `VERIFICATION-MATRIX.md` are **generated**. Edit layer 1
  (`data/*.toml`) and regenerate; never hand-edit the markdown.
- **`fixture-map.tsv` contains NUL bytes** — the `fs.sfs.workbench-dh0.hd`
  notes quote the literal `SFS\0` magic — so `file` calls it `data` and `grep`
  finds nothing in it, silently. Read it with python, not grep.
- The NAS is reachable from this host through gvfs at
  `/run/user/1000/gvfs/smb-share:server=daninas.local,share=software`. It is
  the master copy of the corpus: append, never delete.
- Keep working files out of `/tmp` — the box's owner asked for that explicitly.
  `~/nextstep-test` holds the harness; `~/nextstep-test/boot-spike` holds the
  QEMU work.
- No archive tool on this box. `pip install --target ~/nextstep-test/pylibs
  py7zr` is how the `.7z` drop was opened.

## Verification

```bash
cargo build --manifest-path regression-tests/runner/Cargo.toml
R=./regression-tests/runner/target/debug/rb-regress
$R validate                                    # 336 cases, 0 problems; 6 registry -> §1c
$R query counts                                # 125 formats / 46 oracles (2026-08-25)
$R query unverified-writes                     # empty today; should grow then shrink
$R fixtures                                    # 101 rows, 0 missing
$R oracles --detect                            # should find the Sun oracles after §4
$R run --tiers 1                               # 48/48 as of 2026-08-24
$R run --tiers 2                               # 103/103 as of 2026-08-25
$R run --filter edit.new.                      # 7 pass + 3 xfail (R-043)
```
