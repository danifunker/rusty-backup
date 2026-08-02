# Emulator Images

A third asset class, alongside fixtures and emulator profiles. An **emulator
image** is a bootable OS installation. It plays two roles at once, and the
second is the valuable one:

1. **As a fixture** — a large, real, messy volume to read, browse and edit.
2. **As an oracle** — boot it, and it runs *the vendor's own tools* against
   our output. IRIX checking an EFS volume with IRIX's own `fsck` is a
   stronger statement than any Linux read-only driver can make.

That second role is why these are worth their size. `VERIFICATION-MATRIX.md`
lists a number of filesystems whose only host-side oracle is a read-only
Linux module, or nothing at all. A booted vintage OS closes several of those
outright.

## The three asset classes

| Class | Lives in | Purpose |
|-------|----------|---------|
| Fixture | `fixtures/`, `fixtures-large/` | Input we read and verify against |
| **Emulator image** | `emulator-images/` | Bootable OS: fixture *and* oracle |
| Emulator profile | `emulator-profiles/` | Config to boot one (see `ORACLE-HOSTS.md`) |

An image plus a profile equals a runnable oracle.

## Size policy: none

Fixtures have a 250 MB target. **Emulator images are exempt.** A real OS
install is what it is — you cannot shrink an IRIX 6.5 system disk without
destroying the thing that makes it useful. The IRIX 5.3 disk here has
already been through a `_shrunk` pass and is still 762 MB.

Because they are large and already live on the NAS, images are **referenced
in place** rather than duplicated into the corpus. The manifest records the
canonical path, size, sha256 and what the image can verify. Copy one into
`emulator-images/` only when its source location is volatile.

## Current images

### SGI IRIX 6.5 — `irix65.chd` (5.4 GB CHD, 93.75 GiB logical)

Path: `VintageSystemBackups/IRIX/irix65.chd`

Reads correctly today: `Partition table: SGI`, one partition, `SGI XFS`,
93.6 GiB.

**The most valuable image we have**, because IRIX 6.5 mounts *both* XFS and
EFS. Booted, it becomes the authoritative oracle for both SGI filesystems and
for the SGI volume header — replacing two weak entries in the matrix
(`mount efs (ro)` on full Linux, untested; and `xfs_repair` on Linux, which
checks Linux-flavoured XFS rather than SGI's).

**Emulator: Iris**, not MAME. Iris is a dedicated SGI emulator written in
Rust, and Dani is on its development team — which makes it both the better
technical target and the one where a problem can actually get fixed rather
than worked around. Being Rust, it should also be straightforward to drive
headlessly from the harness and to build on any host the suite runs on.
Confirm its scripting/automation surface when building the profile.

What it can verify once booted:

- EFS volumes we write — IRIX `fsck` for EFS
- XFS volumes we write — IRIX `xfs_check` / `xfs_repair -n`
- SGI volume headers — `prtvtoc`, `dvhtool`
- Our `new hd sgi-efs` and `optical new sgi-efs` output, end to end
- Whether an EFS CD we build actually mounts on IRIX

### SGI IRIX 5.3 — `ULTRA64_2GIG_SCSI_IRIX53_shrunk.chd` (762 MB CHD, 3.6 GiB)

Path: `VintageSystemBackups/IRIX/ULTRA64_2GIG_SCSI_IRIX53_shrunk.chd`

Reads correctly: SGI volume header, `SGI EFS` 3.6 GiB, full slot table —
`EFS` at block 32130, `RAW`, `VOLHDR` (32130 blocks), `VOLUME`. Geometry
473 cyl x 255 head x 63 sec.

The EFS-era counterpart. IRIX 5.3 is the version our EFS support targets, so
this is the closer match for EFS specifically; 6.5 is the better all-rounder.

**Broken sibling:** `ULTRA64_2GIG_SCSI_IRIX53_confirmed2.chd` in the same
folder is **1952 bytes** — a truncated stub, not a disk. It fails to open, as
it should. Do not catalogue it; the `.zip` beside it (822 MB) is presumably
the real article.

### Mac OS 8.1 — `fs.hfv.populated-macos81.hd` (300 MB, in `fixtures-large/`)

Already admitted as a fixture. Also boots under Basilisk II, which makes it
an oracle for HFS via Apple's own Disk First Aid.

### Others on the NAS, not yet catalogued

| Image | Size | Would verify |
|-------|-----:|--------------|
| `GatewayWin98OrigHdd` | 3.1 GB CHD | FAT32 via Windows 98 `SCANDISK` |
| `Kens Old Dell Optiplex 2008` | 2.3 GB CHD | NTFS via native `chkdsk` |

| G3 / G4 / G5 clones | various | HFS+ via Disk Utility |

**Not usable:** `VintageSystemBackups/Amiga/amiga128gb.chd` (21 GB) is a
custom/modern Amiga-ish build, not a canonical AmigaOS install, so it is not
a trustworthy reference. Rejected — see the MiSTer images below instead.

### AmigaOS — on the MiSTer, four real installs

Far better than the 21 GB custom build, and already on the board. Inspected
in place using the `rb-cli` already deployed at `/media/fat/Scripts/rb-cli`,
so no gigabytes crossed the network:

| Image | Size | Reads as |
|-------|-----:|----------|
| `Amiga500HD/1.3-HD.hdf` | 960 MB | **RDB + PFS**, DH0 954.8 MiB, bootable — Workbench 1.3 |
| `Amiga600HD/2.1-HD.hdf` | 960 MB | **RDB + PFS**, DH0 954.8 MiB, bootable — Workbench 2.1 |
| `Amiga1200HD/2.1-HD.hdf` | 960 MB | **RDB + PFS**, DH0, bootable |
| `Amiga1200HD/Mister-3-2.hdf` | 2.0 GB | **AFFS FFS-Intl**, no partition table — AmigaOS 3.2 |
| `Amiga/AmigaVision.hdf` | 9.1 GB | **RDB + PFS x2** — DH0 629.5 MiB + DH1 8.3 GiB, multi-partition |

**This closes the PFS3 oracle gap at 960 MB instead of 21 GB.** A booted
Workbench 1.3 or 2.1 runs PFS3's own tooling against volumes we write.

**SFS is still uncovered.** Every Amiga disk on the board is PFS or AFFS —
nothing uses SFS. It remains the one Amiga filesystem with no oracle
anywhere.

## Manifest

`emulator-images/manifest.tsv` on the NAS, one row per image:

| Column | Meaning |
|--------|---------|
| `id` | Logical ID, `emu.<os>.<version>.<arch>` |
| `path` | Canonical NAS path (referenced, usually not copied) |
| `bytes` / `sha256` | Integrity |
| `fs` | Filesystems present on the disk |
| `verifies` | What it can act as an oracle for, once booted |
| `emulator` | Which emulator boots it, and the profile name |
| `status` | `reads-ok` / `boots` / `oracle-wired` |

`status` tracks the three stages honestly: we can read a disk long before we
can boot it, and boot it long before its native tools are wired into the
harness.

## Next steps

1. **Boot IRIX 6.5 under Iris.** Highest value single action in this
   document — it converts SGI EFS, SGI XFS and the SGI volume header from
   weak or absent oracles into vendor-authoritative ones. Iris is Rust and
   in-house, so headless automation should be tractable and any blocker is
   fixable at source rather than worked around.
2. Wire the guest-writes-a-result-file pattern (`ORACLE-HOSTS.md`
   § Emulator profiles): run the native checker inside IRIX, drop output on
   a shared volume, read it back on the host.
3. Boot `Amiga500HD/1.3-HD.hdf` (or 2.1) under MiSTer Minimig or FS-UAE —
   closes PFS3, which had no oracle at all until this pass.
4. Catalogue the Win98 and Dell images for FAT32 / NTFS vendor checks.
5. **Find an SFS volume.** It is now the only Amiga filesystem with no
   oracle and no fixture; nothing on the MiSTer or the NAS uses it. Most
   likely route is to build one under AmigaOS with the SFS handler
   installed, which also makes the resulting disk the oracle.
