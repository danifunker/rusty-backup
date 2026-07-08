# Release Notes

This release adds **native Macintosh archive support** (BinHex + StuffIt, no
emulator required), a complete **XFS check / repair / edit** stack for vintage
IRIX disks, and a new **Archives** tab in the GUI — plus CLI verbs for all of
it.

## Native Mac archives (BinHex & StuffIt)

Decode and create the classic Mac fork-preserving archive formats end-to-end,
without launching an emulator. Both forks (data + resource) and full Finder
info (type/creator/flags) are preserved throughout.

- **BinHex 4.0 (`.hqx`)** — native encode **and** decode (CRC-16-CCITT, RLE90,
  6-bit alphabet).
- **StuffIt classic (`.sit`)** and **self-extracting (`.sea`)** — reader with
  per-fork decompression for methods:
  - 0 — None (stored)
  - 1 — RLE90
  - 2 — LZW ("Compress")
  - 3 — Huffman
  - 5 — LZAH
  - 13 — LZ + Huffman
  - 15 — Arsenic (BWT + arithmetic coder)
- **StuffIt 5 (`.sit`)** — reader, including the Arsenic (method 15) codec.
- **StuffIt write** — create archives with stored + RLE90 compression. Writing
  to a `.hqx` output produces the classic **`.sit.hqx`** (StuffIt wrapped in
  BinHex), validated against `unar`.
- **StuffIt X (`.sitx`)** — detected and reported with a clear "not supported"
  message rather than failing obscurely (decoding is out of scope for now).

Decoded payloads chain into the existing pipeline — an `.hqx`/`.sit` wrapping a
DiskCopy 4.2 / NDIF image flows straight into HFS/HFS+ browse, inspect, backup,
and restore.

### GUI — new "Archives" tab

Pick a StuffIt (`.sit` / `.sea`, classic or StuffIt 5) or BinHex-wrapped
`.sit.hqx` archive, view its entries, and extract them all to a folder in a
chosen fork-preserving container: **BinHex / MacBinary / AppleDouble / raw**.
Read-only; all decode logic is shared with the CLI.

### CLI — new verbs

- `rb-cli sit list ARCHIVE` — list entries in a `.sit` / `.sea` / `.sit.hqx`.
- `rb-cli sit extract ARCHIVE DEST` — unpack an archive to a folder.
- `rb-cli sit create OUT[.sit|.hqx] FILE...` — build a StuffIt archive from
  host files (`.hqx`, `.bin`, or plain); a `.hqx` output BinHex-wraps it.
- `rb-cli put-binhex IMG[@N] HOST.hqx` — decode a BinHex document into a
  filesystem.
- `rb-cli get-binhex IMG[@N] SRC OUT.hqx` — extract a file from a filesystem
  and encode it as BinHex 4.0, preserving both forks + Finder info.

## XFS filesystem: check, repair, and edit

First-class support for SGI IRIX **XFS** volumes (v4 and v5 on-disk formats),
including read, a multi-phase `fsck`-style verifier, in-place repair, and
editing. XFS is wired into the APM `Apple_UNIX_SVR2` dispatch for read + edit.

### Verifier (`fsck`)

- **Phase 1** — superblock + per-AG header (AGF/AGI/AGFL) consistency, including
  secondary-superblock replica checks.
- **Phase 2** — inode-btree walk + whole-volume block-ownership map
  (double-allocation and extent-past-volume detection).
- **Phase 3** — directory connectivity + orphan detection.
- Free-space btree walk cross-checked against AGF `freeblks`
  (`AgfFreeblksMismatch`), free-block-vs-inode cross-check (`FreeBlockClaimed`),
  and full ownership-map accounting (`UnaccountedBlocks`).
- Validated against a Docker-based `mkfs.xfs`/`xfs_repair` oracle, including a
  real IRIX V1-inode disk.

### Repair

- **R2** — live free-space (bno/cnt) btree rebuild.
- **R3** — inode btree (inobt): in-place free-mask/freecount repair **and**
  full single- and multi-level structure rebuild.
- **R4** — rewrite damaged secondary superblocks from the primary.
- **R4b** — recompute corrupt AGF/AGI summary counts.
- **R5** — recompute inode-core `di_nblocks` / `di_nextents` from the extent
  list.
- **R6** — drop dangling short-form directory entries.
- **R7** — recompute inode link counts from the directory graph, and reconnect
  orphaned inodes into `/lost+found`.

### Editing

- Allocate inodes/blocks and **create files** (single- and multi-extent, into
  fragmented free space) and **directories**.
- **Delete** entries from short-form and single-block directories, freeing the
  inode and its blocks.
- Automatic **short-form → single-block directory conversion** when a directory
  outgrows the inline form.

### Tooling

- New examples: `xfs_check`, `xfs_mkdir`, `xfs_mkfile`.
- Oracle harness: `scripts/xfs-oracle.sh`.
- Plans/docs: `docs/xfs_fsck.md`, `docs/xfs_edit_and_repair.md`,
  `docs/native_mac_archives.md`.

## Fixes & infrastructure

- **32-bit Windows (i686) CI** is green again. The XFS test fixture is a 512 MiB
  volume; the repair round-trip tests no longer hold two copies at once
  (read-only pre-checks now borrow the image), and the i686 test binary runs
  single-threaded so concurrent large fixtures can't exhaust the ~2 GiB 32-bit
  address space. No test coverage was dropped from either target.
- A 512 MiB HFS resize-overflow unit test is gated to 64-bit hosts (the logic
  under test is pointer-width-independent).
