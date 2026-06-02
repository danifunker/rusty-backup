# Fixtures Needed

Real-image fixtures required to unblock parked work items. Each entry
lists the OPEN-WORK section, the suggested fixture filename(s) (so the
person generating them knows where to drop them), the minimum content
the fixture must contain, and the tool / OS needed to produce it.

Fixture files live under `tests/fixtures/` and are committed in zstd-
compressed form (`.img.zst`) unless they're already small enough
uncompressed.

Add a new entry here every time you park an item with "need fixture".

---

## ReiserFS

- **§1.1 R.3b/c/d — S+tree walker, list_directory, read_file (v3.6 — shipped)**
  - File: `tests/fixtures/test_reiserfs_v3_6.img.zst` — 64 MiB, v3.6
    (`ReIsEr2Fs` magic), label `reiser36_test`. Carries `/hello.txt`,
    `/subdir/nested.txt`, `/link.txt -> hello.txt`, `/tiny.txt` (10
    bytes, exercises the tail-packing direct-item path), and
    `/large.bin` (24 KiB deterministic, forces an indirect item with
    6 block pointers). Producer: `scripts/generate-reiserfs-fixtures.sh`.

- **§1.1 R.3 v3.5 fixture — parked (modern kernel won't mount)**
  - Files: `tests/fixtures/test_reiserfs_v3_5.img.zst` (would need ≥ 32 MiB)
  - Modern Linux kernels (6.x) refuse to mount a v3.5 ReiserFS volume
    even when `reiserfsprogs` happily formats one — `mount` returns
    `EUCLEAN "Structure needs cleaning"` and `mount -o conv` no longer
    auto-upgrades. To produce a populated v3.5 fixture we'd need an
    older kernel (~5.x). The R.1/R.2/R.3a synth tests in
    `src/fs/reiserfs.rs` already cover the v3.5 superblock and V1 key
    format (both `from_v1` sentinel u32s and V1 ItemHead version=0)
    end-to-end. Grab `reiserfsprogs` while it's still in distros — the
    Linux kernel is on the removal track and userspace is going next.

## UFS — **fixtures shipped 2026-06-02**

- **§1.2 U.1 / U.2 / U.3 — UFS1 + UFS2 detect, sizing, browse (now in tree)**
  - Files (shipped via `scripts/generate-ufs-fixtures.sh`, ~1.4 KB zstd each):
    - `tests/fixtures/test_ufs1.img.zst` — 16 MiB, UFS1, makefs default layout
      (SB at byte 8192, magic `0x011954`).
    - `tests/fixtures/test_ufs2.img.zst` — 16 MiB, UFS2; NetBSD makefs places
      the SB at byte 8192 even for `-O 2` (FreeBSD `newfs` would put it at
      byte 65536 — our parser handles both).
  - Contents (matching ReiserFS / ext fixtures):
    `/hello.txt`, `/subdir/nested.txt`, `/link.txt` symlink, `/tiny.txt`
    (10 bytes), `/large.bin` (24 KiB deterministic; exercises the
    direct[0..3] block walk under U.3 — fits within the 12-direct
    ceiling at bsize=8192, so single-indirect coverage rides on a
    synthetic-image test in `synthetic_single_indirect_walk`).
  - Producer notes: Ubuntu 24.04 has no `mkfs.ufs`/`newfs`, but the `makefs`
    package in universe (NetBSD's tool) builds an FFS image directly from a
    host directory tree. Install via `sudo apt-get install -y makefs`; the
    script then runs `makefs -t ffs -B le -s 16m -o version={1,2}`. No
    kernel UFS write support needed — userspace pass only.

## JFS — **fixture shipped 2026-06-02**

- **§1.3 J.1 — JFS2 detect, sizing, inline-log-aware backup extent (now in tree)**
  - File (shipped via `scripts/generate-jfs-fixtures.sh`, 4.4 KiB zstd):
    - `tests/fixtures/test_jfs.img.zst` — 16 MiB, JFS2 (Aggregate
      Superblock at byte 32768, magic `JFS1`, on-disk `s_version=1`
      as Linux mkfs.jfs writes; kernel constant is `2` and either
      value mounts cleanly).
  - Contents (matching ReiserFS / ext / UFS fixtures):
    `/hello.txt`, `/subdir/nested.txt`, `/link.txt` symlink,
    `/tiny.txt` (10 bytes), `/large.bin` (24 KiB deterministic;
    consumed by J.3 once the AIT + xtree walker lands).
  - Producer notes: Ubuntu has `jfsutils` (universe) which gives
    `mkfs.jfs`; the kernel module isn't always loaded in WSL, so
    `guestfish` mounts via the libguestfs appliance kernel +
    `linux-modules-extra-$(uname -r)` (same pattern as the ReiserFS
    fixture). 16 MiB is the JFS minimum — anything smaller and
    mkfs.jfs refuses.
- **§1.3 J.2 / J.3 — BMAP B+tree walker + Tier B browse** still
  parked, but **not fixture-blocked** — the fixture above already
  carries the file tree J.3 will need. Parked for scope (multi-
  session walker, ~2000 lines of kernel reference for J.2 alone).
  Pick up if real demand surfaces for trimming free blocks out of
  JFS backups.

## NTFS file-aware GHO (compressed)

- **§3.2 NTFS GHO compressed path**
  - File: `tests/fixtures/ntfs_fileaware_compressed.gho.zst` — a real Norton
    Ghost backup of an NTFS partition with the compressed file-aware container
    format. Even a small (~50 MiB) source partition is enough to exercise
    the compressed-block reader.
  - Producer: Norton Ghost 11.x or 12.x (the last versions to ship the
    file-aware NTFS path), running on a Windows XP / 7 VM. Back up a
    small NTFS partition with **file-aware + compressed** selected in
    the wizard.
  - Today the code bails with `"compressed NTFS not yet supported"` in
    `GhoReader::open` (~line 1102); the format is reverse-engineered
    from the uncompressed samples, but the compression-wrapper framing
    isn't observed yet. Most likely the same Fast-LZ chunking used by
    compressed FAT GHO, but verify against a real fixture before
    plumbing.

## Clonezilla LVM

- **§4.1 Clonezilla LVM import**
  - Files: a directory tree under
    `tests/fixtures/clonezilla_lvm/<image-name>/` containing:
    - `<disk>-pt.sf` (sfdisk output with `label: dos`)
    - `lvm_<vg>.conf` (text VG metadata dump)
    - `lvm_logv.list`, `lvm_vg_dev.list`
    - One or more `<vg>-<lv>.<fs>-ptcl-img.*` partclone images for the LVs
  - Producer: A Linux VM with an LVM-backed partition (PV/VG/LV), backed up
    with stock Clonezilla in "savedisk" mode. Tar/zstd the whole image dir.

## Clonezilla RAID

- **§4.2 Clonezilla RAID import**
  - Files: a directory tree under
    `tests/fixtures/clonezilla_raid/<image-name>/` containing:
    - `<disk>-pt.sf` (with `0xFD` Linux RAID Autodetect partitions)
    - The mdadm config dump (filename varies by Clonezilla version —
      usually `mdadm.conf` or similar)
    - Per-member partclone images
  - Producer: A Linux VM with software RAID (mdadm) — RAID-1 is enough
    to validate the detection + sidecar parsing; RAID-0/5 are needed to
    exercise the "refuse single-member restore" path.

## HFS — extending a raw partition image

- **§3.3 HFS raw extend**
  - File: `tests/fixtures/hfs_raw_partition.img.zst` — a raw HFS partition
    image with no APM wrapper (sector 0 = HFS boot blocks, byte 1024 =
    MDB). Roughly 4 MiB is plenty; the case to test is whether
    `emit_apm_disk_with_hfs` can be bypassed when the source has no
    wrapper.
  - Producer: extract a single HFS partition from any APM-wrapped image
    we already have, or use `dd` on a real Mac to skip the APM region.
    The existing `~/Documents/partition-0.img` (referenced in the §3.3
    note) is the original blocker.

## HFV emulator verification

- **§7 HFV in BasiliskII / MAME** (user-side, not a unit-test fixture)
  - Files: any output of `rb-cli expand --to-hfv` or `rb-cli new --fs hfv`
    plus the two known-bootable samples already in user's possession:
    `Mac OS 8.1.HFV`, `Starterdisk.hfv`.
  - Producer: BasiliskII / SheepShaver / MAME 68k Mac emulator setup
    on the user's host. This isn't a code fixture — it's a manual
    "boot it, see if it mounts" check.

## CD CHD ISO9660 browse verification

- **§7 CD CHD ISO9660 browse** (user-side)
  - File: any CD CHD ripped via `rb-cli optical rip` (or external `chdman`)
    plus the source ISO. Check that a single file extracted via the
    Browse view matches the same file extracted from a mounted ISO.
