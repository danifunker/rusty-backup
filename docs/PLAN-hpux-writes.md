# Plan: write support for the HP 9000 / Sun formats

Follows `docs/PLAN-to-fix-add-fixtures.md` (all read-side phases done). Every
write path ships only after the native OS's own checker accepts our output.

| Phase | Format | Status | Oracle |
|---|---|---|---|
| W1 | HP LIF boot volumes | Done (`03004ea`) | HP-UX 11.11 `lifls` / `lifcp`, boot through a moved ISL |
| W2 | HP-UX HFS, plus the UFS allocator fixes it exposed | Done (`0387690`) | HP-UX 11.11 `fsck -F hfs`, mount |
| W2b | FreeBSD 12+ UFS check-hashes, cluster maps, lazy inode init | Done (`f32349b`) | FreeBSD 15.1 `fsck_ffs`, mount |
| W3 | VxFS disk layout 4: create / delete / rename files and directories | Done (first cut: direct extents) | HP-UX 11.11 `fsck -F vxfs -o full -n`, `fsdb -F vxfs`, mount |
| W4 | Sun QIC tapes: rebuild on edit, original kept as a backup | Code done 2026-10-29 (`src/fs/sun2tape_edit.rs`); awaiting a Sun-2 / Sun-3 MiSTer boot of an edited tape | SunOS 4.1.x reading the tape back (sun3 / sun4m emulation), `tar t` |
| W5 | BSD disklabel slot edits + resize on restore | Planned | NetBSD/hppa `disklabel` under QEMU |
| W6 | HP-UX LVM LV resize / resize on restore | Planned | HP-UX `vgcfgbackup`, `vgdisplay`, `lvdisplay` |
| — | AIX LVM / JFS1 writes | Blocked on the 43P MiSTer core reaching AIX | AIX 4.3.3 `fsck`, `lsvg` |
| — | VxFS layouts 5+ | Blocked on VxFS 3.5 media (the 11.11 DVD has 3.3 only) | |

## W3: VxFS layout 4 writes

Scope: regular files, directories and symlinks in the primary fileset —
create (with data), delete, rename — on a cleanly unmounted volume. Refused:
a dirty intent log, quotas, attribute inodes we would have to move, layouts
other than 4.

Method: a lab volume made by HP-UX's own `mkfs -F vxfs -o version=4`
(`/run/media/dani/BigDisk/rb-tmp/vxlab/vx.img`, hot-plugged at SCSI ID 4).
Learn each allocation structure from `fsdb -F vxfs` field dumps and from the
byte diff of one controlled HP-UX change (create one file, remove it, mkdir),
then mirror it:

1. Structural files from the OLT and the structural fileset: IAU (inode
   allocation), extent-map / extent-summary per allocation unit, the current
   usage table (CUT), the fileset header's counts, the superblock free counts.
2. Allocator: free extents from the per-AU maps; inodes from the IAU bitmaps.
3. Inode writer (direct extents, immediate data for small files and dirs),
   directory-block insert / remove with the hash chains.
4. fsck `-o full -n` clean after every edit kind; then a mount reads it back.

## W4: Sun tapes

Editing a tape rewrites the whole container, so the flow is SquashFS's
(`src/fs/squashfs_edit.rs`): load the tape into memory, edit tar sections
(recompressing `compress(1)` sections) and the UFS miniroot in place, rebuild
into a new file, and only after the rebuild succeeds rename the original to a
backup (`<name>.bak`, never overwritten: `<name>.bak.1`, ...) and move the new
tape into its name.
