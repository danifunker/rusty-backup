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

- **§1.1 R.3b/c/d — S+tree walker, list_directory, read_file**
  - Files:
    - `tests/fixtures/test_reiserfs_v3_5.img.zst` — ≥ 32 MiB, v3.5 (`ReIsErFs` magic)
    - `tests/fixtures/test_reiserfs_v3_6.img.zst` — ≥ 32 MiB, v3.6 (`ReIsEr2Fs` magic)
  - Minimum contents (same layout on both, so tests can share code):
    - `/hello.txt` containing exactly `Hello, ReiserFS!`
    - `/subdir/nested.txt` containing exactly `nested file`
    - `/link.txt` symlink → `hello.txt`
    - One file with size > 16 KiB to force an indirect item (vs tail-packed direct)
    - One file < 64 bytes to verify tail packing
  - Producer: Linux box with `reiserfsprogs`:
    ```
    dd if=/dev/zero of=test_reiserfs_v3_6.img bs=1M count=64
    mkfs.reiserfs -f -v 2 test_reiserfs_v3_6.img        # -v 2 = format 3.6
    # ... loop-mount, populate, unmount ...
    zstd test_reiserfs_v3_6.img -o test_reiserfs_v3_6.img.zst
    ```
    Replace `-v 2` with `-v 1` for the v3.5 image. Grab `reiserfsprogs`
    **soon** — Linux kernel is on the removal track and distros will
    start dropping the userspace next.

## UFS

- **§1.2 U.1 / U.2 / U.3 — UFS1 + UFS2 detect, sizing, browse**
  - Files:
    - `tests/fixtures/test_ufs1.img.zst` — ≥ 16 MiB, UFS1 (superblock at byte 8192, magic `0x011954`)
    - `tests/fixtures/test_ufs2.img.zst` — ≥ 16 MiB, UFS2 (superblock at byte 65536, magic `0x19540119`)
  - Minimum contents: same shape as ReiserFS — `/hello.txt`, `/subdir/nested.txt`,
    `/link.txt` symlink, one >16 KiB file (indirect block walk), one inline file.
  - Producer: FreeBSD VM (`newfs -O 1 ...` for UFS1, `newfs -O 2 ...` for UFS2),
    or Linux with the `ufstool` / `mkfs.ufs` package.

## JFS

- **§1.3 J.1 / J.2 / J.3 — JFS2 detect, BMAP walk, browse**
  - Files:
    - `tests/fixtures/test_jfs2.img.zst` — ≥ 16 MiB, JFS2 (Aggregate Superblock at byte 32768, magic `JFS1` with `s_version=2`)
  - Minimum contents: ReiserFS-shape (hello/subdir/link/large/small).
  - Producer: Linux box with `jfsutils`:
    ```
    dd if=/dev/zero of=test_jfs2.img bs=1M count=32
    mkfs.jfs -q test_jfs2.img
    ```

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
