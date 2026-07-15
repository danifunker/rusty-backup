# APFS Support Plan

Goal: **read-only browse driver now, full parity (edit / shrink / fsck) later.**
Targets: **unencrypted modern APFS first**; snapshots and encryption are
explicitly deferred to a later phase (see Phase 6).

> **Status (Phases 0–6 encryption shipped).** The read-only browse driver is
> live in `src/fs/apfs.rs`: detect, container/omap/volume parse, catalog browse,
> and file/symlink extract, all verified against a real macOS-formatted fixture
> (`tests/fixtures/test_apfs.img.zst`) whose file tree round-trips byte-for-byte
> (SHA-256 oracle). **FileVault decryption is shipped** (`src/fs/apfs_crypto.rs`
> + the keybag/VEK chain in `apfs.rs`): supply the volume password or personal
> recovery key (`rb-cli ls/get --password`) and the encrypted fixture
> (`test_apfs_encrypted.img.zst`) browses + extracts with every file matching
> its oracle. Wired into GUI Inspect + Commander and the CLI via the shared
> dispatch. **Still deferred:** snapshots (Phase 6, below), a GUI passphrase
> prompt (interactive, GUI-only), and edit / shrink / fsck.

APFS did not exist before macOS 10.13 (2017), so this sits outside the usual
"vintage" remit — it is included for users capturing modern Mac CF/SD/SSD
media alongside their retro volumes.

## Current state (baseline — no work needed)

- **Raw backup/restore already works.** APFS partitions stream through the
  raw / Zstd / VHD path and the whole-disk CHD path as opaque bytes
  (`src/backup/mod.rs`). No free-space zeroing, no shrink, no browse — but
  data is captured and restored faithfully.
- `src/partition/gpt.rs:100` already maps the APFS GPT GUID
  (`7C3457EF-0000-11AA-AA11-00306543ECAC`) to the label "Apple APFS".
- `src/gui/browse_view.rs:718` already lists "APFS" in the Mac-family
  browsable gate with a placeholder comment — the UI anticipates a driver.
- **No APFS filesystem code exists** anywhere in the repo today.

## On-disk format primer (why this is the hard part)

APFS is a copy-on-write, checksummed, object-addressed filesystem. Reading a
single file means walking several indirection layers:

1. **NXSB — container superblock** (block 0, magic `NXSB`). Points at the
   checkpoint descriptor ring. The *latest valid* NXSB in the checkpoint ring
   is the one to trust (highest `xid`, valid Fletcher-64 checksum).
2. **Checkpoint** → container object map (**omap**) and the list of volume
   object IDs (`nx_fs_oid[]`).
3. **omap B-tree** — maps `(oid, xid)` → physical block address. Almost every
   object reference is virtual and must be resolved through an omap.
4. **APSB — volume superblock** (magic `APSB`), one per volume. Points at the
   volume's own omap and its **catalog** and **extent-ref** trees.
5. **Catalog B-tree** — the file/directory records
   (`APFS_TYPE_INODE`, `_DIR_REC`, `_FILE_EXTENT`, `_DSTREAM`, `_XATTR`).
6. **File extents** — resolve `j_file_extent_val` records to physical block
   ranges to read file bytes.

Cross-cutting hazards:
- **Fletcher-64 checksum** in the first 8 bytes of every `obj_phys` block.
  Verify on read; it disambiguates stale checkpoint copies.
- **Everything is little-endian** (unlike the Amiga/HFS big-endian FS here).
- **Block size** is `nx_block_size` (usually 4096) — do not assume 512.
- **Shared/cloned extents** (reference-counted) — fine for read, a trap for
  any future shrink/edit work.

## Phase 0 — Fixtures & oracle (do first)

Without a reference image, every later phase is guesswork.

- Create small unencrypted APFS images to test against. On macOS:
  `hdiutil create -size 64m -fs APFS -volname TEST test.dmg` then attach and
  populate; or `diskutil` on a scratch disk image. Capture a raw image.
- Oracle for correctness: mount the same image on macOS and diff the file
  tree / file contents against what the driver reads. `fsck_apfs -n` and
  `apfs.util` can characterize the image. Consider adding an
  `examples/apfs_dump.rs` like the existing FS dump examples.
- Store fixtures the way other FS tests do (small, committed or generated at
  test time). Follow the CBM/JFS oracle pattern in the memory notes.

## Phase 1 — Detect-only scaffold

Copy the `src/fs/andos.rs` template (154 lines) to `src/fs/apfs.rs`:

- `detect_apfs_signature(bytes) -> bool` — check `NXSB` magic + Fletcher-64
  at block 0.
- `open()` that validates the container superblock and stores block size.
- `Filesystem` impl with the 7 required methods returning stub/empty values
  for now (`root` = empty dir, `list_directory`/`read_file` = `Unsupported`,
  `fs_type` = "APFS", sizes from NXSB `nx_block_count * nx_block_size`).

Wire into `src/fs/mod.rs`:
- `fs_name_for()` — GPT/APM APFS type → "APFS".
- `open_filesystem_by_string()` — dispatch the APFS GPT GUID string.
- `detect_filesystem_type()` — content-sniff `NXSB` for superfloppy/type-0.

At this point `used_size`/`total_size` are real; the tab shows APFS as a
recognized (if not yet browsable) volume.

## Phase 2 — Container parse (NXSB → omap → volume list)

- Parse `obj_phys` header + Fletcher-64 verification helper (shared by all
  object reads).
- Read the checkpoint descriptor ring, pick the latest valid NXSB by `xid`.
- Parse the container omap B-tree; implement generic **B-tree walk**
  (fixed vs variable KV, `btn_flags`, `BTNODE_FIXED_KV_SIZE`, toc, leaf vs
  index nodes). This B-tree code is reused for the catalog tree — get it
  right once.
- Enumerate volumes via `nx_fs_oid[]`, resolve each through the omap, parse
  each **APSB**. Expose volume label from `apfs_volname`.

Deliverable: `volume_label()` returns the real name; multi-volume containers
are enumerated (pick first volume, or expose the list — decide UX).

## Phase 3 — Catalog walk (browse)

- Parse the volume omap + catalog B-tree.
- Decode catalog records keyed by `(oid, type)`: `INODE`, `DIR_REC`
  (directory entries, hashed or unhashed names), `DSTREAM_ID`, `XATTR`.
- Implement `root()` (root inode oid = 2), `list_directory()` (walk
  `DIR_REC` children of a dir inode), and `FileEntry` construction
  (name, size from `DSTREAM`, dates, mode, dir/file flag).
- Handle name normalization / case-folding (note the existing APFS
  case-folding comments in `zip_disk.rs` / `tar_export.rs`).

Deliverable: browsing an APFS volume in the Inspect tab works.

## Phase 4 — File extents (read/extract)

- Resolve `FILE_EXTENT` records → physical block ranges via the extent tree /
  dstream; implement `read_file()` and `write_file_to()` streaming.
- Handle sparse files (holes → zero-fill) and multi-extent files.
- Symlinks (target stored as an xattr / inline data) → `FileEntry` link.

Deliverable: file extract / export works; CLI `browse` + `--format json`
parity with other read-only drivers.

## Phase 5 — Integration polish

- Remove the placeholder comment in `browse_view.rs`; APFS is now genuinely
  browsable.
- Doc sync (per CLAUDE.md pre-commit checklist): README Image-formats /
  Filesystems tables, `docs/full_MiSTer_support_status.md`, and
  `DISK_IMAGE_EXTS` if a new picker extension is involved (`.dmg`/APFS
  sparsebundle are out of scope for now — raw partition images only).
- Optional: filesystem-aware **compaction** for backup (a
  `compact_partition_reader` that zeroes free container blocks using the
  space-manager bitmap) — this is the first real win beyond raw backup, and a
  natural bridge toward the resize work in the parity phase.

## Phase 6 — Deferred: snapshots & encryption (full-parity groundwork)

Explicitly out of scope for the read-only cut, tracked here per request:

- **Snapshots**: a volume may hold multiple checkpoints / a snapshot metadata
  tree (`snap_meta`). The reader initially resolves only the current `xid`.
  Later: enumerate snapshots and allow browsing a chosen one. The omap's
  `(oid, xid)` keying already contemplates this — design the omap resolver so
  a target `xid` can be threaded through from the start to avoid a rewrite.
- **Encryption (FileVault)** — the active next phase. Scope decided:
  **offline decrypt from a user-supplied secret only** — the **volume
  password** or the **personal recovery key** (which is cryptographically the
  same passphrase → KEK path). No Apple ID / iCloud-escrow integration (that
  key isn't on the disk in a usable form; it must be retrieved interactively
  from Apple) and no Secure-Enclave path (machine-bound, unavailable to an
  offline reader). This keeps decryption universal across machines.

  **Bring-up spike done** — the crypto parameters are confirmed against a real
  macOS-encrypted fixture (`tests/fixtures/test_apfs_encrypted.img.zst`; see
  `tests/fixtures/apfs/ENCRYPTED_FIXTURE.md` for the passphrase and the full
  keybag walk). Confirmed facts:
  - Container metadata (NXSB, checkpoints, keybags-as-blocks) is **not**
    encrypted; only the volume's data and volume-tree blocks are.
  - **Keybag decryption**: AES-128-XTS, 512-byte units, tweak = the 512-byte
    logical-sector index (`block_addr * block_size/512 + unit`), XTS key =
    `UUID || UUID`. Container keybag (`nx_keylocker`) uses the **container**
    UUID; the volume keybag (found via a container-keybag tag-3 `prange`
    entry) uses the **volume** UUID.
  - Container keybag yields the **wrapped VEK** (tag 2) and the volume-keybag
    location (tag 3). Volume keybag yields the crypto user's **KEK blob**
    (tag 3, a DER structure with PBKDF2 salt + iteration count + wrapped KEK).

  Implementation steps:
  1. Add `aes` + `xts-mode` crates (spike-verified) and an RFC-3394
     AES-key-unwrap + PBKDF2-HMAC-SHA256 helper (`sha2`/`hmac`/`pbkdf2` or
     hand-rolled).
  2. Detect encryption from the volume APSB flags / presence of a keybag; add
     an optional `passphrase` to `ApfsFilesystem::open` (CLI `--password` /
     `--recovery-key`, GUI prompt). Absent passphrase on an encrypted volume →
     a clear "volume is encrypted, passphrase required" error, not a panic.
  3. Decrypt container keybag → parse the DER KEK blob → PBKDF2(passphrase) →
     unwrap KEK → unwrap VEK.
  4. Wrap the block reader so volume data blocks are AES-XTS-decrypted with the
     VEK (per-block tweak) before checksum/parse. Container-level reads stay
     plaintext.
  5. e2e test: decrypt the fixture with its documented passphrase, walk the
     tree, and match SHA-256s against `oracle_encrypted_checksums.txt` — same
     harness as the unencrypted read path.

## Deferred: edit / shrink / fsck (full parity)

Each is a separate large effort, all left at `Unsupported` /
layout-preserving defaults until the read path is solid:

- **Edit** (`EditableFilesystem`): COW allocation, space-manager bitmap
  updates, checkpoint writing, checksum recompute. Shared-extent
  reference-counting makes deletes non-trivial.
- **Shrink / expand**: relocate objects out of the truncated region and
  rewrite the space manager — comparable to the HFS+ defrag-clone effort
  (`hfsplus_defrag.rs`, 2,902 lines) but harder due to COW.
- **fsck**: `fsck_apfs`-grade checking is notoriously complex; a
  self-consistency checker (checksum + B-tree invariants + refcount) on the
  CBM-VALIDATE template is the realistic first target.

## Effort calibration

- HFS+ (the closest analog, fully-featured) is ~28,500 lines across ~9 files.
- A read-only, unencrypted, current-xid APFS driver is far smaller — estimate
  a few thousand lines, most of it in the B-tree walker and catalog decode.
- The andos scaffold is ~110 lines and covers only Phase 1.
