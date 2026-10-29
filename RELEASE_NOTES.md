# Release Notes

This release adds **QNX 2 read and write** for the Unisys ICON and its
LEXICON fileserver hard disks (checked with real QNX), reads **real QNX 4
media**, decodes **SCP / HFE flux images and MFM-reader `.emu` hard-disk
captures**, makes **Sun QIC tapes editable**, and finishes an audit that keeps
large files out of RAM.

## QNX 2 (Unisys ICON / LEXICON): read and write

- **Browse**: owners as `group.member`, the ICON attribute letters (`rwaem`
  for files, `rcbm` for directories) mapped onto Unix modes, and dates checked
  against QNX's own `files`. Dates written with the ICON clock unset (QNX shows
  `0-----80`) read as 1 January 1980; the 4-bit year holds 1980-1995, so newer
  dates are written as 31-Dec-95.
- **Deleted entries** list on request as `NAME [deleted]`: `rb-cli ls --deleted`,
  `rb-cli get --deleted`, or the GUI's **Show deleted** checkbox.
- **RS to LF**: `rb-cli get --rs-to-lf` and the GUI's **Text: RS to LF** extract
  option turn RS (0x1E) line ends into LF in files that look like text (up to
  16 MiB; bigger files stream out unchanged). The GUI preview converts too.
- **Edit**: create files (in 128-block extents, as QNX does), mkdir, rename,
  delete (as QNX's `frel`), chmod and chown; directories grow past one block.
  Verified with real QNX 4.10.01 `chkfsys` in the icon-emu emulator on the
  10 MB disk, both partitions of a 64 MB disk and an 800 KiB floppy; files
  Rusty Backup wrote were copied by QNX and came back byte-identical.
- **fsck** (`rb-cli fsck`): extent chains, back links, cross-links, entry block
  counts, the bitmap against the blocks files hold, and orphaned blocks.
- **LEXICON hard disks**: the ICON header marks MBR types 07 / 08 / 09 as QNX 2
  partitions 1-3 with 1-based starts (read one sector earlier). Inspect labels
  them "QNX 2" instead of NTFS, and a never-formatted partition shows as
  "QNX 2 (not formatted)".
- **The real ST-251**: an MFM reader image that pads every cylinder to 8 heads
  (a 6-head Seagate ST-251 stored as 820 x 8 x 17) is read and written with the
  drive's own geometry, and bad-block-table entries are remapped. Checked on a
  real LEXICON ST-251 (431 files, 20 directories); QNX accepted Rusty Backup's
  writes to the padded image.
- The QDOS `/dos/work.ph` (a FAT disk stored inside QNX) opens as FAT, and
  Commander descends into `.ph` / `.pf` files.

## QNX 4 on real media

A QNX 4 boot block carries a FAT-like BPB (OEM `QNX4FS`) and was taken for
FAT. QNX 4 is now probed first, and the Linux qnx4fs driver's sample volume
(`qnxfs1g.img`) lists correctly. QNX 4 stays read-only; QNX 6 is still only
tested on synthetic volumes.

## Flux and low-level disk captures (read-only)

- **New formats**: SCP (Greaseweazle's native format), HFE v1 and v3 (HxC),
  and David Gesswein's MFM reader `.emu` captures of ST-506 hard disks (the
  Intel iSBC 214 format of ICON / LEXICON drives; a whole ST-251 decodes
  byte-identical to its sector dump). They join KryoFlux stream sets, and
  `.scp`, `.hfe` and `.emu` are in the file picker.
- **Encodings**: IBM MFM, IBM FM (including Acorn DFS's records from 0) and
  AmigaDOS MFM, picked automatically by probing a few tracks.
- **Better recovery**: a phase-tracking PLL follows the bit cell, and when no
  copy of a sector passes its CRC, a byte-wise vote of three or more copies is
  kept if its CRC holds.
- **Fault report**: unreadable sectors are zero-filled, named in a CLI warning,
  and listed in the GUI's Inspect tab ("Unreadable sectors (N)", with LBAs).
- Checked against Greaseweazle-written SCP / HFE images of ICON, Amiga ADF and
  Acorn DFS disks: byte-identical to the sector dumps.
- Not yet: Apple / Commodore GCR from flux.

## Sun QIC tapes: editing

Sun `SUN2TAPE` tapes (`.qic`, the Sun-2 / Sun-3 MiSTer cores' format) are now
editable: add, mkdir, rename, delete, chmod and chown inside their tar,
`compress(1)`-ed tar (`.tar.Z`) and UFS miniroot tape files.

- A tape has no free space, so every edit rebuilds the whole tape. The original
  is kept as `<name>.bak` (then `.bak.1`, ... never overwritten); each `rb-cli`
  edit command is its own session and makes its own backup.
- Untouched members and tape files stay byte-identical; new tar members get V7
  headers in the archive's own `./` style, and `.Z` sections are recompressed
  with a new `compress(1)`-compatible encoder (checked with host `gzip -d`).
- **Not yet verified on Sun hardware**: booting an edited tape on a Sun-2 /
  Sun-3 MiSTer core is still to do.

## Memory: large files stay out of RAM

Nothing over about 200 MB is loaded into memory any more:

- NTFS / exFAT defrag backups and HFS / PFS3 clones pipe file bodies through a
  bounded channel instead of reading each file whole.
- `.cbk` pack and unpack stream (chunk CRCs are still checked).
- ZIP / tar archives over 64 MiB are indexed instead of decoded into memory
  (compressed tars inflate to a temp file).
- `rb-cli new`, `batch` and `new hd x68k` write blank volumes sparse: a 4 GiB
  FAT32 now takes 3 MB on disk.
- NDIF / SMI images rebuild to a temp file, Arculator `.hdf` reads as a window
  on the file, and format sniffs read only the header.
- File extraction streams in UFS, JFS2, SquashFS, ReiserFS, Minix, QNX 4,
  QNX 6, ADFS, OS-9, Human68k and QDOS.
- NTFS-compressed Ghost images parse their MFT incrementally.
- `rb-cli edit` refuses files over 64 MiB.

## Fixes

- **HPFS**: formatting a volume over 1 GiB crashed; 2 GiB volumes now format
  and check clean.
- **Ghost**: a `.GHO` given as a bare filename (no directory) failed to open.
- **`rb-cli convert`** copied flux files and padded ICON images byte-for-byte as
  if they were raw; it now exports what Rusty Backup decodes.

---

## Previous release

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
