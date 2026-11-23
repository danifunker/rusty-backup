# Changelog

All notable changes to this crate are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## 0.2.2

### Fixed
- Logs written by IRIX replay. IRIX 6.5 writes version-1 records with `h_fmt` 0, which were
  read as little-endian, so `check_log` refused them ("item 0x5254 is missing regions").
  Each transaction's byte order now comes from its `TRAN` header magic, and an `h_fmt` of 0
  counts as big-endian. IRIX's item layouts (56-byte inode items, buffer items, EFI / EFD)
  were checked against real IRIX 6.5 logs and match the format document, big-endian.
- A record that runs past the end of the log was taken for a torn write, so a log that had
  wrapped mid-record (on IRIX or Linux) could not be replayed. Sectors past the end carry the
  next cycle's stamp, and are now expected to.
- An inode item is skipped when the copy on disk is newer (a higher `di_flushiter` on v1 / v2
  inodes, a higher `di_lsn` on v3), rather than rolling the inode back. IRIX flushes inodes
  with changes it never logged.
- The clean log written after a replay or a discard keeps the old log's record version,
  `h_fmt`, UUID and payload byte order, so IRIX accepts it: a v1 log stays v1. A v2 clean
  record always says a 32 KiB log buffer, whatever size the old log used.

Validated under the IRIS emulator against IRIX 6.5.22 itself: on a volume made by IRIX's own
mkfs and crashed mid-workload, the replayed volume mounts on IRIX with no log recovery, IRIX's
`xfs_repair -n` is clean, and every entry's inode number, mode, owner, size, mtime and
checksum match IRIX's own recovery of the same disk. An IRIX root disk replayed this way boots
with no log recovery.

## 0.2.1

### Added
- Log recovery (`xfs::log_replay`): fsck reports a volume that was not cleanly unmounted, and
  `repair` replays its log first, as a mount would: committed buffer, inode and inode-create
  and quota items from the tail to the head, buffer cancellations, v5 checksums and LSNs, the
  lazy superblock counters, inodes deleted while open, then a clean unmount record. The result
  matches the kernel's own recovery (`xfs_repair -n` clean, same tree, contents, xattrs,
  ownership and quota report) on v4 and v5. A quota item it cannot read, or one for a quota
  turned off later in the log, clears the "quotas checked" flags so the next quota mount
  recounts, as does discarding the log. A log it cannot finish (unfinished rmap / refcount / bmap / attr intents, a
  torn record before the head) is left untouched and reported; `XfsFilesystem::discard_log`
  (`xfs_repair -L`) is the explicit way out. Every item is validated (sizes, region counts,
  dirty maps, write targets inside the volume, LSN order from tail to head) before anything is
  written, so a malformed or hostile log is refused untouched rather than half applied or a
  panic. A log holding record headers but no complete record is reported damaged, not clean. Written from the public format document and black-box output only; see
  `docs/xfs_log_replay.md`.

### Fixed
- `log_state` finds the head as replay does (skipping a torn newest record), so edits and
  repair agree on whether the log is clean.
- The v5 project-quota inode (`sb_pquotino`) counts as a superblock root, so fsck no longer
  reports it as an orphan and repair no longer moves it to lost+found.
- Edits, repair, chmod and chown refuse a volume whose log was not cleanly unmounted (the head
  record is not the unmount transaction), since the next mount would replay the log over them.
  `XfsFilesystem::log_state` reports it (XFS-012).
- chmod and chown work on v5 volumes with features the editor does not maintain: they rewrite
  only the inode core.

## 0.2.0

### Fixed (review, 2026-11-21)
- Edits and repair refuse volumes with finobt, rmapbt, reflink, inobtcnt, sparse
  inodes, bigtime, exchange-range, parent pointers or v1 directories, which they
  would have left inconsistent; R3 refuses to rebuild a lost inode btree on v5.
- `open` checks superblock geometry and refuses unknown v5 incompat bits;
  `open_within` refuses a volume larger than its partition.
- Attribute-fork blocks are counted by fsck, R2 and R3 and freed on delete;
  deleting one name of a hard-linked file keeps the inode.
- Directory edits compare and rewrite names as bytes and refuse a directory
  holding a non-UTF-8 name; 8-byte short-form directories get 8-byte entries.
- The allocator checks free-space records and inode free masks before use;
  bmbt leaf chains stop on a loop; the repair map is a bounded bitset.
- v5 remote symlinks no longer include the block header; BIGTIME timestamps are
  decoded; v1 inodes report their link count.

### Changed
- **Licence: MIT** (was GPL-2.0-only). The two parts that tied the crate to the
  GPL were rewritten from SGI's *XFS Algorithms & Data Structures* (CC BY-SA),
  without reference to xfsprogs or kernel source:
  - the directory name hash (`dir_hashname`), from the chapter "Variable Length
    Record B+trees". It is checked against 2,800+ hashes read off block, leaf and
    node directories in `mkfs.xfs`-made v4 and v5 images (now fixtures) and
    against the document's own `xfs_db` dump;
  - the in-place leaf / node directory editor (`dir2_tree`), from the chapter
    "Directories" (Block, Leaf, Node and B+tree Directories) and "Variable Length
    Record B+trees" (Block Headers, Internal Nodes). Validated with `xfs_repair -n`
    after thousands of inserts and removes on v4 and v5 images.
- Comments that cited xfsprogs / kernel files as porting references now describe
  `xfs_repair -n` as the behavioural reference and validation oracle.

## 0.1.0

First release, split out of rusty-backup (GPL-2.0-only).
