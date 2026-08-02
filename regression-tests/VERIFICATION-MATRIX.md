# Verification Matrix

Which independent tool can verify each thing rusty-backup writes, and on
which host. This is the tier-6 plan (`PLAN.md` phase 7) and, per
`README.md` § "Why tier 1 is not coverage", **the only valid check on
anything we write** short of booting it.

Two rules govern every row:

1. **The oracle is never rusty-backup.** If we write it and we read it, a
   bug on both sides cancels out.
2. **`mount` is the strongest oracle we have.** If a real kernel mounts our
   volume and the files read back byte-correct, that is about as
   independent as verification gets. Everything else is a fallback.

Verified on this hardware 2026-08-02 unless marked *expected*.

---

## Host capability

| Oracle | Win native | Win via WSL | Linux | macOS | MiSTer |
|--------|:----------:|:-----------:|:-----:|:-----:|:------:|
| `qemu-img` | **yes** `C:\Program Files\qemu` | yes | yes | brew | no |
| `7z` | **yes** `C:\Program Files\7-Zip` | apt | yes | brew | no |
| `fsck.ext4` | no | **yes** | yes | brew e2fsprogs | **yes** |
| `fsck.vfat` | no | **yes** | yes | — | **yes** |
| `unsquashfs` | no | **yes** | yes | brew | no |
| `xfs_repair` | no | **yes** | yes | no | no |
| `jfs_fsck` | no | **yes** | yes | no | no |
| `fsck.minix` | no | **yes** | yes | no | no |
| `mdir` (mtools) | no | **yes** | yes | brew | no |
| `chkdsk` | **yes** native | — | no | no | no |
| `fsck_msdos` | no | no | no | **yes** native | no |
| `fsck_hfs` | no | no | no | **yes** native | no |
| `fsck_apfs` | no | no | no | **yes** native | no |
| `hdiutil` | no | no | no | **yes** native | no |
| `ghostexp.exe` | **yes** ×3 on this box | no | no | no | no |
| `chdman` | install | install | install | install | install |
| `xorriso` | install | apt | yes | brew | no |
| `cpmtools` | install | apt | yes | brew | no |
| `python3` | yes | yes | yes | yes | **yes** |

**Kernel `mount` support**, which decides the strongest oracle per host:

| Host | Mountable |
|------|-----------|
| WSL Ubuntu 24.04 | ext2/3/4, squashfs, vfat, udf, xfs; modules for btrfs, exfat, isofs, cramfs, erofs, f2fs |
| **MiSTer** (5.15 armv7) | ext2/3/4, vfat, exfat, iso9660, **affs**, udf |
| Full Linux + `linux-modules-extra` | all of the above plus hfs, hfsplus, minix, jfs, ntfs3, ufs (ro), efs (ro), reiserfs |
| macOS | hfs, hfs+, apfs, fat, exfat |

Two things fall out of that table immediately:

- **MiSTer mounts AFFS.** WSL does not. That makes the board the cheapest
  Amiga oracle we have, and it is already on the network.
- **WSL is materially weaker than a real Linux box** — no hfs, hfsplus,
  affs, minix, jfs, ntfs3. A full Linux host with `linux-modules-extra` is
  the single highest-value machine in the matrix.

---

## Container formats

Proven this session: `qemu-img info` accepts every container we write.
Built a 4 MB FAT volume, converted it six ways, ran the oracle on each.

| We write | Oracle | Host | Status |
|----------|--------|------|--------|
| raw | `qemu-img info` -> `raw` | any | **verified** |
| qcow2 | `qemu-img check` -> clean | any | **verified** |
| vhd-dynamic | `qemu-img info` -> `vpc` | any | **verified** |
| vhd (fixed) | `qemu-img info` -> `raw` | any | **verified, see note** |
| vmdk-flat | descriptor -> `vmdk`, extent -> `raw` | any | **verified** |
| vmdk-sparse | `qemu-img check` -> "No errors" | any | **verified** |
| chd / chd-cd / chd-dvd | `chdman verify` | any | **verified across chdman 0.174 / 0.189 / 0.273 / 0.288** — "Raw SHA1 verification successful" on all four |
| bincue | `chdman createcd` round-trip; MAME | any | untested |
| zstd / gzip / lz4 | `zstd -t`, `gzip -t`, `lz4 -t` | any | untested, trivial |
| ZIP-disk | `7z t` | Win / Linux / macOS | untested, trivial |
| squashfs | `unsquashfs -s` + mount | WSL, Linux, MiSTer | untested |
| DMG / sparseimage / NDIF | `hdiutil verify` / `hdiutil attach` | **macOS only** | untested |
| GHO / GHS | **`ghostexp.exe`** | **Windows only** | untested; binary present ×3 |
| 2MG / WOZ / MOOF / DC42 | AppleCommander (Java), CiderPress (Win), Applesauce (macOS) | mixed | no oracle chosen yet |
| D88 / XDF / HDM / DIM | MAME, or the X68000/PC-98 MiSTer cores | mixed | no oracle chosen yet |
| HFV | Basilisk II / SheepShaver boot | tier 7 | manual only |
| cbk | none identified | — | **gap** |
| AppImage | run it; `--appimage-extract` | Linux | untested |

**Note on fixed VHD.** `qemu-img` reports our fixed VHD as `raw` rather
than `vpc`. That is most likely qemu's probe scoring — a fixed VHD is raw
data with a 512-byte footer, and qemu's `vpc` prober ranks it below `raw`.
Worth one check against a Hyper-V-produced fixed VHD before treating it as
either correct or a defect; do not assume either way.

---

## Filesystems

| We write | Oracle | Host | Notes |
|----------|--------|------|-------|
| FAT12/16/32 | `fsck.vfat`, `mdir`, mount, `chkdsk` | WSL, Linux, MiSTer, Win | best-covered format we have |
| exFAT | mount, `fsck.exfat` | Linux, MiSTer, macOS | |
| NTFS | `chkdsk` native; `ntfsfix` / mount `ntfs3` | Windows, full Linux | not in WSL |
| ext2/3/4 | `fsck.ext4` + mount | WSL, Linux, **MiSTer** | |
| HFS / HFS+ | `fsck_hfs` + mount | **macOS**, full Linux (hfsplus) | **Mac effectively mandatory** |
| APFS | `fsck_apfs` | **macOS only** | no Linux write/verify story |
| HFV | Basilisk II boot | tier 7 | flat HFS, so `fsck_hfs` may work on macOS — worth trying |
| XFS | `xfs_repair -n`; **IRIX 6.5 native `xfs_check`** for SGI-flavoured XFS | WSL, Linux; Iris (SGI emulator) | Linux xfs_repair checks Linux XFS - IRIX is the authority for ours |
| JFS | `jfs_fsck` | WSL, Linux | |
| Minix V1/2/3 | `fsck.minix` | WSL, Linux | |
| SquashFS | `unsquashfs -s` + mount | WSL, Linux | |
| HPFS | mount `hpfs` (ro) | full Linux only | not in WSL; **thin** |
| AFFS | mount `affs` | **MiSTer**, full Linux | plus WinUAE at tier 7 |
| PFS3 | **booted Workbench 1.3 / 2.1** (`emu.amigaos.13/21.m68k`, 960 MB on the MiSTer) | MiSTer Minimig, FS-UAE | RDB + PFS, bootable |
| SFS | none anywhere | — | **still a gap** — no SFS volume found on any source |
| ProDOS / Apple DOS 3.3 | AppleCommander (Java) | any | candidate, not yet wired |
| CP/M (9 DPBs) | `cpmtools` (`cpmls`) | install anywhere | largest sub-axis; oracle exists, just install it |
| SGI EFS | **IRIX 5.3/6.5 native `fsck`** (emulator image); mount `efs` (ro) as fallback | Iris (SGI emulator); full Linux | image reads OK, boot not yet wired |
| CBM DOS | VICE, `cc1541`, DirMaster | any | tier 7 mostly |
| UFS | mount `ufs` (ro) | full Linux | R-013 lives here |
| Acorn DFS / ADFS | BeebEm / Arculator | tier 7 | **no structural oracle** |
| OS-9 / RS-DOS / DragonDOS | `os9` toolkit, `decb` (ToolShed) | any | candidate |
| Human68k | X68000 MiSTer core | tier 7 | **no structural oracle** |
| QDOS | `qxltool` | any | same tool that would build the fixture |
| TI-99 / TR-DOS / Oric / UCSD / MFS / Lisa | niche emulator only | tier 7 | **no structural oracle** |
| Alto BFS / Pilot-Cedar | ContrAlto / Salto | tier 7 | **no structural oracle** |

---

## Partition schemes

| Scheme | Oracle | Host |
|--------|--------|------|
| MBR / GPT | `sfdisk -l`, `fdisk -l`, `parted print` | WSL, Linux |
| APM | `diskutil list`, `gpt` | macOS |
| Sun disk label | `sfdisk -l` reads SunOS labels | Linux |
| SGI volume header | `sfdisk`/`parted` partial | Linux |
| RDB | WinUAE / MiSTer Minimig | tier 7 |
| AHDI / X68000 | Hatari / MiSTer core | tier 7 |

---

## Gaps — no structural oracle on any host

Ranked by how much of the matrix they leave unverified.

1. **SFS** — no host tool mounts it and no SFS volume exists in any source
   we have; every Amiga disk on the MiSTer is PFS or AFFS. The one Amiga
   filesystem still without an oracle. **PFS3 is now covered** by the
   bootable Workbench 1.3 / 2.1 disks on the board (960 MB each).
2. **cbk (cb-dos container)** — our own format, no third-party reader
   exists. Structurally unverifiable by definition; the honest answer is a
   documented exemption plus a strong self-consistency test, and to say so
   rather than pretend.
3. **Acorn DFS / ADFS, Human68k, TI-99, TR-DOS, Oric, UCSD, MFS, Lisa,
   Alto BFS, Pilot/Cedar** — emulator-only. Reasonable: these are the
   formats where an emulator *is* the reference implementation.
4. **HPFS** — one read-only Linux module, absent from WSL. Thin.
5. **Apple floppy containers (2MG/WOZ/MOOF/DC42)** — tools exist
   (AppleCommander, CiderPress, Applesauce) but none is wired up or chosen.

## Action items

1. **`chdman` — done.** Newest local copy is **0.288** at
   `C:\Tools\chdman\chdman.exe` (0.273 in the MAME folders is two years
   old; 0.189 and 0.174 also present). Our CHD output verifies on **all
   four**, which makes the version spread a coverage axis worth keeping —
   users run whatever MAME build they have, so "opens on 0.174 through
   0.288" is a stronger claim than "opens on the newest".
2. **Stand up one full Linux box** with `linux-modules-extra`. It is the
   only host that mounts hfs, hfsplus, affs, minix, jfs, ntfs3 and ufs —
   converting a large block of "no oracle" into `mount` checks.
3. **macOS is not optional.** `fsck_hfs`, `fsck_apfs` and `hdiutil` exist
   nowhere else, so HFS, HFS+, APFS, DMG, sparseimage and NDIF cannot be
   verified without a Mac.
4. `apt install cpmtools` in WSL — closes the largest sub-axis (nine DPBs)
   for one command.
5. Wire `ghostexp.exe` as the GHO oracle; the binary is already on this box
   in three places, including Ghost 11.5 which matches our fixtures.
6. Pick an Apple-floppy oracle. AppleCommander is Java, so it runs on all
   three OSes — probably the right call over per-OS tools.
7. Settle the fixed-VHD `raw`-vs-`vpc` question against a Hyper-V-produced
   file.

## Consequence for the run

No single host can verify everything, so a full regression is inherently
multi-machine:

| Host | Uniquely verifies |
|------|-------------------|
| **macOS** | HFS, HFS+, APFS, DMG, sparseimage, NDIF, APM |
| **Windows** | GHO/GHS (`ghostexp`), NTFS via `chkdsk` |
| **Full Linux** | hfs/hfsplus/affs/minix/jfs/ntfs3/ufs/efs mounts, xfs, squashfs |
| **MiSTer** | AFFS mount, plus every MiSTer core at tier 7 |
| any | qemu-img containers, chdman, 7z, cpmtools |

The runner already models this: a case declares `requires = [...]`, an
absent tool degrades to `skip-tool`, and the report names every skipped
oracle. So a Windows-only run is *valid* — it simply reports a large
`oracle-skips.md`, and the matrix is only complete once the per-platform
bundles are read together.
