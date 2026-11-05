# Bootable disks backlog

Out of scope for Build Disk's first phases (decision 3 in `docs/build_disk_plan.md`).
Build Disk keeps today's boot pieces: the X68000 IPL stub, the fixed builders (SGI volume
header, the CD builders) and `rb-cli make-bootable` for Mac disks.

## What to work out, per filesystem and scheme

- What "bootable" means: boot code in the table sector (MBR), boot blocks in the volume
  (HFS, AFFS, FAT boot sector), a system file or driver the OS loader needs (Mac System
  Folder blessing and the APM driver, Amiga RDB filesystem handlers, SGI sash / volume
  header files, Sun bootblock, NeXT boot area, X68000 IPL).
- Which part rusty-backup can write itself and which must come from elsewhere.

## Donor disks

- Let the user point Build Disk at a disk image that is known to boot, and copy its
  boot pieces (boot code, boot blocks, drivers, handlers) into the new disk.
- Validate the donor first (the pieces are present and match the target scheme and
  filesystem), and refuse a donor that doesn't fit.
- Verify each scheme in an emulator before it ships (QEMU, FS-UAE, Previous, the Iris
  emulator, Basilisk II / SheepShaver), as the regression oracles do.
