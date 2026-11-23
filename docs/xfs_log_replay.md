# XFS log replay

`crates/fs-xfs/src/xfs/log_replay.rs` recovers an XFS volume that was not cleanly unmounted,
the way a Linux or IRIX mount would. fsck reports the dirty log. Repair replays it before any
other pass. `rb-cli fsck --repair --discard-log` (`XfsFilesystem::discard_log`) throws away a
log that cannot be replayed, like `xfs_repair -L`.

## Clean room

The code was written from two sources only:

- the public format document, SGI's *XFS Algorithms & Data Structures* ("Journaling Log"
  chapter: record header, operation header, transaction header, item formats);
- black-box output: `xfs_logprint` decoding the test images, and the bytes the kernel's own
  recovery leaves on disk.

No kernel or xfsprogs source was read. The document names the flags without giving their
values, so these were pinned by matching raw log bytes against `xfs_logprint`:

| Field | Values |
|---|---|
| `oh_flags` | START 0x01, COMMIT 0x02, CONTINUE 0x04, WAS_CONT 0x08, END 0x10, UNMOUNT 0x20 |
| `oh_clientid` | TRANS 0x69, LOG 0xAA |
| `blf_flags` | INODE_BUF 0x1, CANCEL 0x2; the buffer type sits in bits 11-15 |
| `blf_flags` quota buffers | user 0x4, project 0x8, group 0x10 |
| `sb_qflags` | user ACCT 0x1, ENFD 0x2, CHKD 0x4; project ACCT 0x8; other (v4 group / project) ENFD 0x10, CHKD 0x20; group ACCT 0x40, ENFD 0x80, CHKD 0x100; project ENFD 0x200, CHKD 0x400 |
| `ilf_fields` | CORE 0x1, DDATA 0x2, DEXT 0x4, DBROOT 0x8, DEV 0x10, UUID 0x20, ADATA 0x40, AEXT 0x80, ABROOT 0x100 |
| `h_fmt` | 1 little-endian Linux, 2 big-endian Linux, 3 big-endian IRIX |

## What replay does

1. **Find the head.** The head is the record header with the highest LSN whose own block
   number matches its position, and whose data sectors all carry its cycle stamp. A torn
   write fails the stamp check, and the next-newest record is used. A head that is the
   unmount record means the log is clean, whatever its tail says.
2. **Walk from the tail to the head.** The walk starts at the head's `h_tail_lsn`, wraps at
   the end of the log, and undoes the cycle stamps from `h_cycle_data` (continued in
   extended headers past 32 KiB). Operations are grouped by transaction id, and a `WAS_CONT`
   operation extends the previous region. A transaction is applied when its commit record
   arrives. Transactions with no commit by the head are dropped, as a mount drops them.
3. **First pass (no writes).** Counts buffer cancellations, pairs intents with their done
   items (the id at offset 8 of both), and refuses anything it cannot finish:
   unfinished rmap, refcount, bmap, attr or exchange intents, unknown item types, and a
   missing or torn record between the tail and the head. A refused log is left untouched.
4. **Second pass.** Items are applied in commit order:
   - **Buffers.** Each run of set bits in the 128-byte-chunk map takes the next region. A
     buffer cancelled later in the log is skipped. An `INODE_BUF` item updates only the
     `di_next_unlinked` fields. On v5, blocks are restamped by magic: LSN plus CRC.
   - **Inodes.** The logged core is host-endian. Each field is swapped to big-endian;
     timestamps are one 64-bit count with bigtime (`di_flags2` bit 3) and two 32-bit halves
     otherwise. The on-disk `di_next_unlinked` is kept. Forks are copied in place. A btree
     root is logged as an in-memory long-format block (72-byte v5 header, 24-byte v4), with
     keys and pointers sized to the logged length, and is rewritten as an on-disk `bmdr`
     sized to the fork. Device numbers come from `ilf_u`. v5 inodes get the transaction's
     LSN and a fresh CRC.
   - **Inode-create items.** The chunk is written as free inodes: magic, version, the
     logged generation, a null unlinked pointer, and, on v5, the inode number, metadata
     UUID and CRC.
   - **Quota items.** The format region names the dquot buffer (`qlf_blkno`, in sectors)
     and the record's byte offset in it (`qlf_boffset`); `qlf_len` is not a sector count
     and is ignored. The second region is the 104-byte on-disk `xfs_disk_dquot`, already
     big-endian, and is written over the record. On v5 the 136-byte `xfs_dqblk` then gets
     the LSN at 112 and a CRC at 108; dquot blocks logged as buffers get the same per
     record. A quota item that does not hold a dquot (no `DQ` magic or no known type), and
     the quota items and buffers of a type a quota-off item turns off, are not replayed:
     every `sb_qflags` "checked" bit is cleared instead, so the next quota mount recounts.
     Quota-off items are handled from the format document alone: current kernels no
     longer write them, so no oracle image holds one.
   - **Extent-free intents without a done item** are counted, not applied. Their blocks
     stay allocated but unowned, and the free-space rebuild that runs next in repair
     reclaims them.
5. **Finish.** The lazy superblock counters (`sb_icount`, `sb_ifree`, `sb_fdblocks`) are
   summed from the AG headers, as a mount does (the "checked" flags are cleared too when
   quotas need a recount). The log is then rewritten as one unmount
   record at block 0 with cycle *c*+1 (*c* being the old head's cycle, so no v5 metadata
   LSN is ahead of the log), and every other sector is stamped with cycle *c*.
   `xfs_repair` needs that stamp to find the head.

After a replay, repair also frees the inodes on the AGI unlinked lists (files deleted while
still open at the crash), as a mount does. The freed inodes' blocks are left to the
free-space rebuild. An inode on a list that still has links only leaves the list.

Repair replays the log even on volumes whose features the editor does not maintain (finobt,
reflink, sparse inodes and so on). The unlinked-list pass and the other repair passes are then
skipped, and the repair report says so.

## Validation

The oracle is a privileged Debian container (`xfsprogs`, `attr`) that loop-mounts images
(`scripts/xfs-log-oracle/`):

1. make a volume and settle it (`sync`, then a forced shutdown and a remount);
2. run a workload whose changes live only in the log: 1200 of 1500 files deleted, a
   100-extent sparse file (btree bmap), two xattrs, a device node, a hard link, a truncate
   and an unlink;
3. `xfs_io -x -c "shutdown -f"`, then unmount;
4. the copy is the dirty image; mounting the original gives the kernel's recovery.

On v4 (`-m crc=0`), plain v5 and default-feature v5 (bigtime, finobt, reflink, sparse
inodes), our replay matches the kernel's recovery:

- `xfs_repair -n` is clean;
- the kernel mounts the result with "Ending clean mount";
- the file tree, every file's MD5, the xattrs and `ls -ln` are identical.

The v4 and v5 rich images are the test fixtures
(`tests/fixtures/sgi/xfs_v{4,5}_dirty_log.img.zst`, about 200 KB each). A third,
`xfs_v4_dirty_log_unlinked.img.zst`, holds a file deleted while open at the crash; after
repair it too is `xfs_repair -n` clean and identical to the kernel's result. After discarding the
log, the kernel again mounts the volume clean, holding the pre-crash state. Discarding also
clears the quota "checked" flags, since the quota records on disk need not match the files.

Docker Desktop's kernel has no XFS quota support, so the quota images are made and checked
in a Debian 6.1 kernel under QEMU inside the container (`scripts/xfs-log-oracle/qemu/`). On
a v5 volume with user, group and project quotas and a v4 volume with user and group quotas
(files created, chowned and deleted, a limit set, then a crash), our replay leaves
`xfs_repair -n` clean, the kernel mounts with no quotacheck, and `xfs_quota report -ugbin`,
the tree and every MD5 match the kernel's recovery (`xfs_v{4,5}_dirty_log_quota.img.zst`).

## Not done

- The deferred-operation intents of rmap, reflink, bmap, attr and exchange-range are
  refused, not replayed.
- The v5 "skip a block whose on-disk LSN is newer" check is not made. Replaying every
  committed transaction in order reaches the same final state, because nothing on disk can
  be newer than the head.
- The record CRC (`h_crc`) is not verified; the cycle stamps catch torn writes. Our unmount
  record carries a zero CRC, as `mkfs.xfs` writes it.
- External logs (`sb_logstart == 0`) are not touched.
