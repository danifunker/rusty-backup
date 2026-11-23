# XFS log replay

`crates/fs-xfs/src/xfs/log_replay.rs` recovers an XFS volume that was not cleanly unmounted,
the way a Linux or IRIX mount would. fsck reports the dirty log. Repair replays it before any
other pass. `rb-cli fsck --repair --discard-log` (`XfsFilesystem::discard_log`) throws away a
log that cannot be replayed, like `xfs_repair -L`; it refuses a log that is clean or that
`--repair` can replay.

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

## IRIX logs

IRIX 6.5 writes version-1 records (one 512-byte header, up to 32 KiB of data) and leaves
`h_fmt`, `h_fs_uuid` and `h_size` zero, and its items are big-endian, as IRIX's MIPS hosts
are. Every transaction's byte order is therefore read from its own `TRAN` header magic, which
only matches one way round; `h_fmt` is the fallback, with 0 taken as big-endian. The item
layouts were checked against real IRIX 6.5 logs: inode items are the 56-byte 64-bit format,
buffer items carry a 64-bit `blkno` and a dirty map, EFI / EFD use 64-bit extents, and the
inode core is the 96-byte v2 core already in on-disk order. IRIX never unmounts `/` cleanly at
shutdown, so every IRIX root disk comes back with a short pending log (root-directory
timestamp updates) that the next boot replays.

IRIX's own unmount record is a v1 record of 32 bytes whose one op (client `0xAA`, flags
`0x20`) holds the 20 bytes `Unmount filesystem--`; the clean log written here keeps the old
log's version, zero trailer and big-endian `0x556e` payload, which IRIX 6.5.22 mounts with no
recovery.

## What replay does

1. **Find the head.** The head is the record header with the highest LSN whose own block
   number matches its position, and whose data sectors all carry its cycle stamp. A torn
   write fails the stamp check, and the next-newest record is used. A head that is the
   unmount record means the log is clean, whatever its tail says.
2. **Walk from the tail to the head.** The walk starts at the head's `h_tail_lsn`, wraps at
   the end of the log, and undoes the cycle stamps from `h_cycle_data` (continued in
   extended headers past 32 KiB). Operations are grouped by transaction id, and a `WAS_CONT`
   operation extends the previous region. A transaction is applied when its commit record
   arrives. Transactions with no commit by the head are dropped, as a mount drops them. A
   header is only taken as a record if its data sectors fit its cycle-data slots and `h_size`
   is at most 256 KiB.
3. **First pass (no writes).** Validates every item completely, so that the second pass can
   fail only on I/O: format sizes, region counts, dirty-map runs inside their buffer, the
   inode core's length and version, inode-create items naming one chunk inside an AG, and
   every write target inside the volume. It also counts buffer cancellations, pairs intents
   with their done items (the id at offset 8 of both), and refuses anything it cannot finish:
   a malformed item,
   unfinished rmap, refcount, bmap, attr or exchange intents, unknown item types, a missing,
   torn or out-of-sequence record between the tail and the head (a record that runs past the
   end of the log has its wrapped sectors stamped with the next cycle, which is expected) (the walk must start at the
   tail's LSN and climb to the head's), more uncommitted data than the log's size or 1 GiB, and a log holding record headers
   but no complete record (damaged, not clean). A refused log is left untouched.
4. **Second pass.** Items are applied in commit order:
   - **Buffers.** Each run of set bits in the 128-byte-chunk map takes the next region. A
     buffer cancelled later in the log is skipped. An `INODE_BUF` item updates only the
     `di_next_unlinked` fields. On v5, blocks are restamped by magic: LSN plus CRC.
   - **Inodes.** An item is skipped when the disk copy is newer: a higher `di_flushiter`
     (mod 2^16) on a v1 / v2 inode, or a higher `di_lsn` on a v3 one. IRIX flushes inodes
     with changes it never logged, so replaying an older core would roll them back.
     The logged core is host-endian. Each field is swapped to big-endian;
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
   record at block 0 with cycle *c*+1 (*c* being the old head's cycle, or for a discard the
   highest cycle any header claims, so no v5 metadata LSN is ahead of the log), and every other sector is stamped with cycle *c*.
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

IRIX logs are checked against IRIX 6.5.22 itself, run under the IRIS emulator
(`scripts/xfs-log-oracle/irix/`). A 128 MB XFS made by IRIX's `mkfs`, given a workload (400
creates, 250 deletes, a 2.5 MB file emptied, a hard link, a symlink, a chown, a rename, nested
directories) and stopped mid-flight with the emulator killed, has a 1292-transaction log that
wraps past the end of the log mid-record. IRIX mounting a copy recovers it; our replay of
another copy mounts on IRIX with no recovery, IRIX's `xfs_repair -n` is clean before and after,
and `ls -ldni` and `sum -r` of every entry are identical to IRIX's result. An IRIX 6.5 Indy root
disk replayed by us boots with no recovery line in `/var/adm/SYSLOG`. The fixtures are
`xfs_irix65_dirty_log.img.zst` (with IRIX's recovery of it, `xfs_irix65_recovered.img.zst`)
and `xfs_irix65_root_log.img.zst` (the root disk with only its superblock, AG headers, root
inode cluster and log kept).

## Not done

- The deferred-operation intents of rmap, reflink, bmap, attr and exchange-range are
  refused, not replayed.
- The v5 "skip a block whose on-disk LSN is newer" check is not made. Replaying every
  committed transaction in order reaches the same final state, because nothing on disk can
  be newer than the head.
- The record CRC (`h_crc`) is not verified; the cycle stamps catch torn writes. Our unmount
  record carries a zero CRC, as `mkfs.xfs` writes it.
- External logs (`sb_logstart == 0`) are not touched.
