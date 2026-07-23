# New Additional Filesystems — Implementation Targets & Oracles

Candidate NEW filesystems to add to rusty-backup, from the repo's own gap
analysis (`docs/filesystem_coverage_audit.md` §6 "High value" band and
`docs/filesystem_completion_plan.md` Part 2) and verified absent in `src/fs/`
(no dedicated module exists — `find src -iname '*.rs'` returns nothing for
zfs/hpfs/trsdos/oric/n88/sparta).

Target for each is the quartet **Browse → Edit → Create → fsck**, except
read-first cases (ZFS). Every write path is validated against an independent
**oracle**, per the repo precedent:

- External tools: MAME `imgtool` (TI-99, DragonDOS), `cpmtools` (CP/M),
  `fsck.minix`, `c1541` (CBM), NetBSD `makefs` (UFS), `e2fsck`, `fsck_hfs`,
  `xfs_repair`.
- Clean-room Python oracles where no tool exists: `scripts/ucsd-oracle.py`,
  `scripts/trdos-oracle.py`, `scripts/ti99-oracle.py`.

## Priority order
1. **OS/2 — HPFS** — DONE (full quartet: `src/fs/hpfs.rs`)
2. **Atari 8-bit — SpartaDOS / MyDOS / DOS 3** (extends existing `src/fs/atari_dos.rs`)
3. ~~**ZFS** (read-first)~~ — **DROPPED.** ZFS is a *pool*, not a single-disk
   filesystem; a multi-disk pool can't be one `.img` at all, so only the
   single-disk-pool sliver would ever fit rusty-backup's model — and even that
   carries the full pooling machinery (MOS/DSL/DMU/block-pointers/compression/
   CoW) for browse-only value on a modern (Solaris 11+, 2011) FS the retro
   audience rarely has. The preservation-worthy Sun disks (SunOS 4 → Solaris 10)
   are **UFS on a Sun disk label**, which we already read — see the **Sun disk
   label (SMI VTOC)** work below, done in its place (`src/partition/sun.rs`).
4. TRSDOS / LDOS / NEWDOS (TRS-80)
5. **Sedoric / Oric DOS (Oric)** — Jasmin variant DONE (`src/fs/oric.rs`); Sedoric pending
6. N88-BASIC (NEC PC-8801)

**Also shipped (Sun-lineage, replaces ZFS):** Sun disk label / SMI VTOC
partition scheme (`src/partition/sun.rs`) — parses the 8 big-endian slices on a
SPARC Solaris / SunOS disk image and routes them to the existing UFS reader
(browse / inspect / extract). Spec = local kernel `block/partitions/sun.c`;
oracle = `fdisk`/`sfdisk` (non-sudo). Full-disk backup + label editing deferred.

---

## 1. HPFS (OS/2) — PRIORITY 1
- **Systems / era:** OS/2 1.2 → Warp 4, eComStation, ArcaOS. Defining OS/2 FS; a
  PC-preservation gap (we cover FAT/exFAT/NTFS/ext and IBM JFS2, not HPFS).
- **Why it matters:** Completes the OS/2 story.
- **Quartet feasibility:** Browse + edit realistic; fsck medium. B-tree
  directories, band-bitmap allocation.
- **Prior art:** Linux kernel `fs/hpfs`; TotalImage (C#).

### Oracles
| Role | Tool | Linux availability | Notes |
|---|---|---|---|
| **Read** | Linux kernel `hpfs` | native (`mount -t hpfs -o loop,ro`) | Canonical read oracle. In-tree but deprecated; **write is experimental — NOT a write oracle.** |
| **Read + write** | **TotalImage** (.NET/C#) | via `.NET`/Mono | Reads & writes HPFS; cited as prior art in the audit. Verify current upstream repo. |
| **fsck / repair** | OS/2 `CHKDSK C: /F` | under 86Box/PCem/QEMU/VirtualBox (ArcaOS or OS/2 Warp) | Only authoritative HPFS fsck; no portable checker exists. |
| Secondary | `hpfsutils` (historic) | build from source | Thin; secondary read only. |

### Linux setup
- `modinfo hpfs` then `sudo mount -t hpfs -o loop,ro image.img /mnt/hpfs`
  (some distros drop the module).
- fsck oracle: **86Box** (Flathub `net.86box.86Box`) or **QEMU** running OS/2;
  script CHKDSK output and diff vs our fsck.

---

## 2. SpartaDOS / MyDOS / DOS 3 (Atari 8-bit) — PRIORITY 2
- **Systems / era:** Atari 400/800/XL/XE. Alternative DOSes beyond DOS 2 (2.0S/2.5)
  already in `src/fs/atari_dos.rs`.
- **Why it matters:** MiSTer Atari800 core; extends an existing driver.
- **Quartet feasibility:** S–M. SpartaDOS = hierarchical (sector map, subdirs,
  timestamps); MyDOS = DOS 2 + subdirs + larger media; DOS 3 = "blocks" alloc.
- **Prior art:** atari-tools; Altirra disk explorer.

### Oracles
| Role | Tool | Linux availability | Notes |
|---|---|---|---|
| **Read/write (all three)** | **Altirra** | Windows — via **Wine** | Gold standard. Disk Explorer reads/writes Sparta/MyDOS/DOS2/DOS3; in-emulator **CHKDSK**. |
| **Read (CLI)** | **atari-tools** (`atr`, dmsc/atari-tools) | native (C, `make`) | Scriptable CI oracle. |
| **Image build** | **dir2atr / atrtools** (HiassofT/AtariSIO) | native | Round-trip `.atr`. |
| **Behavioral** | **atari800** | native (`apt install atari800`) | Boot SpartaDOS X / MyDOS / DOS 3, run `DIR`/`CHKDSK`. |
| Secondary | MAME `a800` | native (`mame-tools`) | Second reference. |

### Linux setup
- `sudo apt install atari800 wine`
- `git clone https://github.com/dmsc/atari-tools && cd atari-tools && make`
- `git clone https://github.com/HiassofT/AtariSIO` (dir2atr)

---

## 3. ZFS (read-first) — ~~PRIORITY 3~~ DROPPED
> **Dropped** — see the Priority-order note above. ZFS is a pool, not a
> single-disk filesystem (model conflict), it's browse-only, and it's a modern
> (Solaris 11+) FS the retro audience rarely has. Replaced by the **Sun disk
> label (SMI VTOC)** parser, which unlocks the *actually* common Sun disk:
> UFS on a Sun-labeled SPARC image. The notes below are retained for reference.
- **Systems / era:** Solaris/OpenSolaris/illumos, FreeBSD, Linux (OpenZFS). The
  Sun → Oracle NAS/workstation FS.
- **Why it matters:** Workstation & NAS image preservation; biggest Oracle-lineage gap.
- **Quartet feasibility:** **Read-first; write out of scope** (CoW + checksums +
  pool semantics). Only a **read oracle** needed.
- **Prior art:** OpenZFS.

### Oracles
| Role | Tool | Linux availability | Notes |
|---|---|---|---|
| **Structure dump** | **`zdb`** | native (`zfsutils-linux`) | `zdb -dddd`, `zdb -R`, `zdb -l`. Dumps uberblock/MOS/DMU/DSL/blkptrs. |
| **File diff** | `zpool import -o readonly=on -d <dir>` | native | Read-only import, diff contents/checksums. |
| Cross-impl | FreeBSD OpenZFS | via QEMU | Same codebase. |

### Linux setup
- `sudo apt install zfsutils-linux`
- `sudo zpool import -o readonly=on -d /path/to/imagedir <poolname>`
- `sudo zdb -dddd <poolname>` / `sudo zdb -l /path/to/vdev.img`

Baseline = OpenZFS (v28 + feature flags). Oracle Solaris 11 proprietary
features: **detect, don't decode.**

---

## 4. TRSDOS / LDOS / NEWDOS (TRS-80) — PRIORITY 4
- **Systems / era:** TRS-80 Model I/III/4. MiSTer TRS-80 core.
- **Quartet feasibility:** Browse + edit; TRSDOS/LDOS/NEWDOS directory+GAT
  variants complicate a single driver (M).
- **Prior art:** `trs80-tool`.

### Oracles
| Role | Tool | Linux availability | Notes |
|---|---|---|---|
| **Read (CLI)** | **`trs80-tool`** (lkesteloot, Go) | native binary | Lists/extracts TRSDOS from `.dsk`/`.jv1`/`.jv3`/`.dmk`. MAME imgtool has **no** TRSDOS module. |
| **Behavioral/fsck** | **trs80gp** | Windows — via Wine | Boot real TRSDOS/LDOS/NEWDOS; `DIR`/`FREE`/`BACKUP`/`FORMAT`. |
| Behavioral | **sdltrs** | native (some distros) | Second reference. |

### Linux setup
- `go install github.com/lkesteloot/trs80-tool@latest` (or release binary)
- **Fallback:** clean-room `scripts/trsdos-oracle.py` (UCSD/TR-DOS precedent) if
  no tool covers all three variants.

---

## 5. Sedoric / Oric DOS (Oric) — PRIORITY 5
- **Systems / era:** Oric Atmos/Telestrat. MiSTer Oric core.
- **Quartet feasibility:** Full quartet (M).

### Oracles
| Role | Tool | Linux availability | Notes |
|---|---|---|---|
| **Behavioral** | **Oricutron** | native (build from source) | Run Sedoric 3.0; `DIR`/`STATUS`. |
| Behavioral | **Euphoric** | via DOSBox/Wine | Alternative. |
| **Image r/w** | **ManageDSK/dsk_manager** + **OSDK** | native (build) | Sedoric MFM `.dsk` cross-check. |
| Secondary | MAME `oric` | native | Second reference. |

### Linux setup
- `git clone https://github.com/pete-gordon/oricutron && make`
- **Fallback:** clean-room `scripts/sedoric-oracle.py` (weakest tool support of the six).

---

## 6. N88-BASIC (NEC PC-8801) — PRIORITY 6
- **Systems / era:** NEC PC-8801. MiSTer PC88 core. Shares the `.d88` container
  rusty-backup already handles.
- **Quartet feasibility:** Full quartet (M).

### Oracles
| Role | Tool | Linux availability | Notes |
|---|---|---|---|
| **Read + write** | **L3DiskEx** (Sasaji, wxWidgets) | native (buildable) | Supports N88-BASIC (+ Hu-BASIC/MZ/L3); reads/writes dir+FAT; speaks `.d88`. |
| **Behavioral** | **Quasi88** (SDL) | native (build) | `FILES` command. |
| Behavioral | **M88** / MAME `pc8801` | Wine / native | Secondary. |

### Linux setup
- `git clone https://github.com/bml3mk5/L3diskEx` (build w/ wxWidgets)
- Quasi88 from source (SDL).

---

## Consolidated Linux oracle toolbox
Install-first, CI-friendly tools in **bold**.

| Filesystem | Primary Linux oracle | Install |
|---|---|---|
| HPFS | Linux `hpfs` ro-mount; **TotalImage** (r/w); OS/2 CHKDSK (fsck) | `modprobe hpfs`; `.NET`/Mono; 86Box `net.86box.86Box` |
| Atari Sparta/MyDOS/DOS3 | **atari-tools**; Altirra (r/w+CHKDSK); **atari800** | `git clone dmsc/atari-tools && make`; `apt install atari800 wine` |
| ZFS | **`zdb`** + **`zpool import -o readonly=on`** | `apt install zfsutils-linux` |
| TRSDOS/LDOS/NEWDOS | **`trs80-tool`**; trs80gp/sdltrs | `go install …/trs80-tool@latest` |
| Sedoric/Oric | Oricutron; OSDK/ManageDSK | build from source |
| N88-BASIC | **L3DiskEx**; Quasi88 | build from source |
| (shared) | **MAME `imgtool`** (existing TI-99/DragonDOS path) | `apt install mame-tools` |

### Emulators to install on the Linux box
- **QEMU** + **86Box** (Flathub) / **PCem** — OS/2 (HPFS CHKDSK)
- **atari800** (native) + **Altirra** (Wine) — Atari DOSes
- **zfsutils-linux** — ZFS (no emulator; native)
- **sdltrs** (native) + **trs80gp** (Wine) — TRS-80
- **Oricutron** (native) — Oric
- **Quasi88** (native) + **MAME** (`pc8801`,`a800`,`oric`) — long tail

---

## Where to wire a new filesystem into the codebase
All dispatch lives in `src/fs/mod.rs`. A new FS touches, in order:

1. `pub mod <fs>;` at the top of `src/fs/mod.rs` (module list, lines 1–95).
2. **Detection** — add a magic/geometry probe in
   `detect_filesystem_type()` — `src/fs/mod.rs:201`.
3. **Browse dispatch** — add a `"<fs>" => …::open(...)` arm in
   `open_filesystem()` — `src/fs/mod.rs:1365` (and the `0x83` /
   `Apple_UNIX_SVR2` sub-matches if it's a Linux/Unix type).
4. **Edit dispatch** — add the arm in `open_editable_filesystem()` —
   `src/fs/mod.rs:1653` (only if it implements `EditableFilesystem`).
5. **Backup compaction** (optional) — `compact_partition_reader()` —
   `src/fs/mod.rs:705`.
6. **Type-name label** — `fs_name_for()` — `src/fs/mod.rs:1055`; and
   `probe_0x83_fs_type()` — `src/fs/mod.rs:559` for Linux-typed partitions.
7. **fsck GUI/CLI gating** — `is_checkable_type()` (`:2501`),
   `is_checkable_fs_name()` (`:2540`), `is_checkable_retro_fs()` (`:2561`).

## Existing patterns to mirror
- **Extend, don't rewrite (Atari)** — `src/fs/atari_dos.rs` is the template for
  the Sparta/MyDOS/DOS 3 work:
  - detection: `looks_like_atari_dos()` — `src/fs/atari_dos.rs:190`
  - struct/open: `AtariDosFilesystem` (`:216`), `open()` (`:224`)
  - VTOC constant: `VTOC_SECTOR` (`:62`)
  - edit impl: `EditableFilesystem` (`:715`) — `create_file` (`:716`),
    `delete_entry` (`:853`)
- **Clean-room oracle scripts** to model new ones on:
  `scripts/ti99-oracle.py` (397 lines), `scripts/trdos-oracle.py` (302),
  `scripts/ucsd-oracle.py` (256), `scripts/xfs-oracle.sh` (66).
- **Fixture generators / fixtures** (Linux/BSD-gated, like UFS):
  `scripts/generate-ufs-fixtures.sh` (87), `scripts/generate-cpm-fixture.sh`
  (58); compressed fixtures under `tests/fixtures/` as `test_*.<ext>.zst`.
- **Detected-not-read gate** to reuse for out-of-scope variants: the `"JFS1"`
  ASCII-magic gate — `src/fs/jfs.rs:15-17,59-60`.

## Appendix — rest of the gap analysis (later)
Medium: ADFS write/`.dsd` (DONE), AIX JFS1 (detected-not-read,
`src/fs/jfs.rs:15-17`), Veritas VxFS, Tru64 AdvFS, Solaris QFS, OpenVMS ODS-2 on
raw disk (optical-only today), Apple SOS / DOS 3.2 / Pascal, optical UDF 2.50+.
Medium also: **SquashFS write** — browse/extract shipped (`src/fs/squashfs.rs`:
v4.0, gzip/XZ/LZMA/LZ4/zstd; LZO refused by name) for AppImages (we ship one),
Raspberry Pi OS / DietPi / Buildroot, live-media `filesystem.squashfs`,
OpenWrt, SteamOS. Edit is rebuild-only by construction and fsck/resize don't
decompose the usual way — read the SquashFS scoping note in
`docs/filesystem_coverage_audit.md` §6 and the plan in `docs/squashfs_edit.md`
before touching it.
Low: finish ANDOS + QL Microdrive scaffolds (`src/fs/andos.rs`,
`src/fs/qdos_mdv.rs`), CSIDOS, MicroDOS (Vector-06C), Specialist-MX; Reiser4 /
NILFS / F2FS / bcachefs / HAMMER (browse-only where magics collide).
Out of scope: flash-translation FS (JFFS2/YAFFS/UBIFS), cramfs,
network/clustered (NFS/SMB/Ceph/OCFS2/ZFS-as-pool), pseudo/virtual
(procfs/sysfs/tmpfs), mainframe (z/OS VSAM), Windows ReFS.
