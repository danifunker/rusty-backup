# SquashFS edit / write — scoping

Read-only SquashFS shipped in `bc14508` (`src/fs/squashfs.rs`). This document
scopes the **write** side, and in particular the **size prompt** that has to
come before any edit — the one piece of this feature with no analogue anywhere
else in the tree.

Companion: the SquashFS scoping note in
[`filesystem_coverage_audit.md`](filesystem_coverage_audit.md) §6, which records
why "full support" doesn't decompose into the usual four README columns.

---

## 1. The constraint everything follows from

SquashFS is a **read-only, densely-packed, compressed** filesystem. There is no
free-space bitmap, no allocation metadata, and no slack anywhere. `mksquashfs`
rewrites the entire image even to append one file, and so must we.

Three consequences drive the whole design:

1. **There is no free space to report.** Every other `EditableFilesystem` in the
   tree answers "will this file fit?" from a bitmap / FAT / allocator.
   SquashFS has nothing to consult — the image is exactly as large as its
   contents happen to compress to.
2. **The post-edit size is unknowable until the rebuild finishes.** It depends
   on how the *new* content compresses, which we cannot know in advance.
3. **The image usually lives somewhere with a fixed size** — a partition, an
   ISO, an AppImage — so growth may not be accommodatable at all.

(1) and (2) together are why the user must declare a size budget up front:
we cannot compute one, and discovering the answer after a multi-minute rebuild
— with the original already replaced — is the failure mode to design out.

---

## 2. The size prompt

### 2.1 What the number actually means

**Not** slack inside the filesystem. Padding a SquashFS with zeroes buys
nothing: you cannot allocate into it later without another full rebuild.

The number is the **size budget for the SquashFS region** — the maximum the
rebuilt image is permitted to occupy. The slack, where it exists, lives
*between the end of the image and the end of its container*, and its purpose is
to let *future* rebuilds grow without re-cutting the container.

### 2.2 Modes

| Mode | CLI | Meaning |
|---|---|---|
| **Fit** | `--size fit` | Accept whatever the rebuild produces |
| **Absolute** | `--size 512M` | Rebuilt image must be ≤ 512 MiB |
| **Headroom** | `--grow 64M` | Budget = current image size + 64 MiB |

Suffix parsing (`K`/`M`/`G`) reuses whatever `rb-cli expand --size` already
uses, so the grammar stays consistent.

### 2.3 Default per container

The right default is a property of where the image lives:

| Container | Can it grow? | Default budget |
|---|---|---|
| Bare `.squashfs` file (superfloppy) | Freely — the file *is* the FS | `fit` |
| Partition in a disk image | Only into free space after it | the partition's current size |
| Inside ISO 9660 (live CDs) | Not practically | *phase 3* |
| AppImage (ELF stub + appended FS) | Yes, it is the tail | *phase 3* |

For a partition, defaulting to "must still fit where it is" means the common
case (replace a config file, delete something) never prompts for a decision the
user doesn't have to make. `--grow` on a partition additionally requires a
partition-table edit and free space following the partition; refuse up front
when that isn't available rather than half-way through.

### 2.4 Two-stage enforcement

**The original is never touched until a rebuild has succeeded and fits.**

1. **Pre-flight**, before any edit is accepted: compute current image size,
   container capacity, and the requested budget. Refuse immediately when the
   budget exceeds what the container can be made to hold. Show the numbers.
2. **Post-rebuild**, before replacing anything: the rebuild lands in a temp
   file. If it exceeds the budget, abort, report actual-vs-budget, and leave
   the original untouched.

### 2.5 Making the prompt informed rather than a blind guess

We can project the rebuilt size *before* running the rebuild, and fairly
tightly, because of the block-reuse design in §4:

- For every **unchanged** file, the compressed bytes are copied verbatim from
  the source image — so their contribution is known **exactly**.
- For **new or modified** content, estimate using the source image's observed
  overall compression ratio as a prior.
- Metadata tables are rewritten wholesale but are small; estimate from the
  current tables scaled by entry-count delta.

Present as a range, labelled an estimate. This turns "how much space do you
want?" into a question the user can actually answer.

### 2.6 Surfaces (GUI / CLI / TUI parity)

- **GUI** — a dialog before entering Edit Mode on a SquashFS volume: current
  size, used, container capacity, observed ratio; a radio for fit / absolute /
  headroom; the §2.5 projection updating live.
- **CLI** — `--size` / `--grow` flags on the edit verbs. One-shot verbs can't
  prompt interactively, so absent a flag they take the §2.3 default. A
  `rb-cli squashfs plan <image>` verb prints the same numbers without editing.
- **TUI** — the GUI dialog's content, ASCII-only.

---

## 3. Prerequisite — the reader must retain more

`src/fs/squashfs.rs` is deliberately "reduced to what browsing needs", and a
faithful rebuild needs more than that. **This is phase 0 and blocks everything
else.** Today:

- `read_inode` (`:405`) reads the 16-byte common header — *type, permissions,
  uid, gid, mtime, inode number* — and keeps only `kind` (`:410-412`). Mode,
  uid, gid, mtime and inode number are all discarded.
- Device nodes (`INODE_BLKDEV` / `INODE_CHRDEV` and extended forms) hit a `{}`
  arm at `:476`, so major/minor are dropped.
- Hard-link counts are dropped, so hardlinked files would be silently
  duplicated on rebuild.
- xattr indices are dropped.
- `Superblock` (`:103`) does not parse `inode_count` (@4), `mod_time` (@8),
  `no_ids` (@26), `id_table_start` (@48), `xattr_id_table_start` (@56) or
  `export_table_start` (@88). The ID table is never loaded, so uid/gid can't
  even be resolved.

Rebuilding without these silently rewrites ownership, permissions and
timestamps across the whole tree — unacceptable for a filesystem whose main use
is appliance root images, where a wrong mode on `/etc/shadow` or a lost setuid
bit is a broken system.

---

## 4. Rebuild engine

A `mksquashfs`-equivalent writer, emitting: superblock, inode table, directory
table (+ index), fragment table, ID table, xattr table (preserve or drop —
see open question Q3), data blocks and fragments.

**Design choice — overlay, not explode-to-disk.** Keep the source image open
read-only and maintain an overlay of pending changes (added files, deleted
paths, new directories, renames). `sync_metadata()` walks the merged tree and
emits a fresh image. The alternative — unsquash everything to a temp directory
and rebuild from it — is simpler but wrong here:

- a 700 MB live-CD SquashFS explodes to several GB;
- it round-trips every file's metadata through the host filesystem, which on
  macOS cannot represent device nodes without root and loses xattrs — exactly
  the fidelity §3 exists to protect.

**Block-level reuse.** For unchanged files, copy the compressed data blocks
byte-for-byte from the source. Only new or modified content is compressed. This
makes rebuild cost proportional to the *change*, not the image, and it's what
makes the §2.5 projection exact for the unchanged majority.

Caveat: a file's tail may share a **fragment** block with other files' tails.
Changing one file in a fragment forces that fragment to be rebuilt, so group
work by fragment when planning reuse.

---

## 5. Phasing

| Phase | Scope | State |
|---|---|---|
| **0** | Extend the reader per §3 — full inode metadata, ID table, device nodes, link counts. Verifiable on its own against `unsquashfs -lls`. | **done** — oracle-diffed at 123k entries |
| **1a** | Writer: emit a valid image from an in-memory tree. Validated by `unsquashfs` + `mksquashfs`-produced references. | **done** — gzip/xz/zstd accepted by `unsquashfs` |
| **1b** | Fragment packing (compression-ratio parity with mksquashfs). | **done** — 1.004 size ratio on real /etc |
| **2a** | `FileContent` seam: stream content, don't hold the whole image in RAM. | **done** — host files stream |
| **2b** | Source→tree bridge: read an image into an editable tree. | **done** — 2513-node rebuild round-trip |
| **2c** | `EditableFilesystem` over bare `.squashfs`: create/delete/mkdir/rename/symlink over an in-memory tree, rebuild on sync, D6 attributes. Wired into the edit dispatch + CLI. | **done** — `rb-cli put` edits a real image; `unsquashfs`-accepted |
| **2d** | Partition-hosted images + the §2 size-budget prompt; in-place commit (temp + size check + atomic rename); xattr-inheritance-on-replace. | **next** |
| **2-opt** | Lazy `FileContent::Source` streaming + verbatim block reuse. Not needed for correctness; bounds rebuild memory and cost. | deferred |
| **3** | ISO 9660 and AppImage containers. | |
| **4** | `rb-cli new volume squashfs` (create blank) and a structural verifier (§6 of the audit: there is no repair to be had). | |

---

## 6. Oracle

The repo pattern is that every write path is validated against an independent
oracle. For SquashFS that's `squashfs-tools`:

- `unsquashfs -lls <img>` — read back our output; compare modes, uid/gid,
  mtimes, symlink targets, device nodes.
- `unsquashfs -stat <img>` — superblock fields.
- `mksquashfs` — build reference images per compressor to read against.

**Not currently installed on this machine.** Available as the Homebrew formula
`squashfs` (4.7.5, bottled). Phase 1 can't be validated without it.

Existing read-side fixture: the Ubuntu 12.04 PowerPC live CD, via
`RB_SQUASHFS_ISO` (see the fixture-gated tests at `src/fs/squashfs.rs:875`).

---

## 7. Decisions

- **D1 — first slice is the full `EditableFilesystem`.** `put` / `rm` /
  `mkdir` / `rename` and GUI Edit Mode behave as they do on every other
  filesystem; the rebuild happens on `sync_metadata()`. No separate repack
  verb — the rebuild is an implementation detail of committing, not a
  user-facing concept.
- **D2 — edits land in place**, via rebuild-to-temp + size check + `fsync` +
  atomic rename over the original. The original is never touched unless the
  rebuild both succeeds and fits the budget. `--output` remains available to
  divert the result instead.
- **D3 — compression settings are preserved by default, with an override.**
  The rebuild reuses the source's compressor, block size and flags.
  `--compressor` / `--block-size` may override, but **any override disables
  verbatim block reuse** (§4) and forces a full recompress — warn loudly and
  say so, because the cost difference is order-of-magnitude.
- **D4 — xattrs are preserved.** The xattr table is parsed in phase 0 and
  carried through the rebuild. Appliance images encode capabilities there
  (`security.capability` on `ping`, `dumpcap`, …); silently dropping them
  produces an image that boots but has subtly broken binaries. This makes the
  phase-0 reader work strictly larger, and it is not optional.
- **D5 — LZO stays refused, for write indefinitely.** Reading an LZO image is
  a decoder away; *writing* one needs an LZO compressor, and no pure-Rust crate
  provides one at the maturity of the decoders.
- **D6 — added files inherit their POSIX attributes; they are never defaulted
  silently.** Resolution lives in `src/fs/attrs.rs` and is shared by every
  filesystem and front end. See §8.

---

## 8. Where an added file's mode / uid / gid come from

Not a SquashFS question — a whole-tree one, surfaced by this work. Every
editable Unix filesystem (ext, UFS, XFS, EFS, Minix) already honoured
`CreateFileOptions`'s `mode` / `uid` / `gid`, but **nothing ever set them**:
`rb-cli put` left all three `None`, so each driver's own `unwrap_or` made every
added file **root:root 0644**. Replacing a `0600` secret with a world-readable
host copy silently widened it.

`src/fs/attrs.rs` is now the single place that decides, so CLI, GUI and TUI
cannot drift. Highest priority first:

| Attribute | Precedence |
|---|---|
| `mode` | explicit flag -> file being replaced -> host file's own bits -> fallback |
| `uid` | explicit flag -> file being replaced -> parent directory -> 0 |
| `gid` | explicit flag -> file being replaced -> parent directory -> 0 |

**Replacing inherits from the replaced file**, because that is the common edit
and the file already carries the answer. **A new file takes ownership from its
parent directory.** The host file's *permission bits* are consulted for `mode`
(macOS carries those faithfully) but its **uid/gid never are** — a macOS 501:20
is meaningless inside a Linux image. Every resolved value records where it came
from and the operation logs it, so nothing is decided silently.

### Bulk imports

`AttrOverrides` is a plain `Copy` struct, so one decision covers a whole import
— pass the same value to every member rather than re-deriving per file. The
flags live on the verb (`--mode` / `--uid` / `--gid`), not per-file.

### Directories

Two differences, both deliberate:

- a new directory inherits its mode from the **parent directory**, not from a
  blanket `0755`, so a subdirectory created inside a `0700` tree stays `0700`;
- any non-explicit directory mode gets `add_execute_where_read` applied — the
  `chmod a+X` rule — because a directory's execute bit means "may be
  traversed", and a directory that can be read but not entered is useless.

Note what this deliberately does *not* do: it never grants execute to a class
that has no read. Forcing `o+x` on every directory would widen `/root` (0700),
`~/.ssh` (0700) and `/etc/ssl/private` — a security regression, not a
convenience. An explicit `--mode` is obeyed verbatim either way; silently
"fixing" what the caller asked for is worse than honouring a strange request.
