# SquashFS — resume prompt

Pick-up notes for finishing the SquashFS work. The plan and every design
decision live in [`squashfs_edit.md`](squashfs_edit.md); this file is the
"where we stopped and what's left" companion.

**Branch:** `edit-squashfs` — **not pushed**. ~25 commits ahead of `main`.

---

## Resume prompt (paste this)

> Continue the SquashFS work on branch `edit-squashfs`. Read
> `docs/squashfs_resume.md` for state and `docs/squashfs_edit.md` for the plan
> and decisions D1–D6. Phases 0 through 2c are done and oracle-validated against
> real `squashfs-tools`; **start with item A (stale doc), then phase 2d.**
> Per-slice commits. Every commit must keep `cargo build --all-targets` at zero
> warnings, `cargo clippy --all-targets -- -D warnings` clean, `cargo test --lib`
> green, and the Rust 1.73 vintage build compiling (see CONTRIBUTING.md
> § "Rust 1.73 floor for engine code" — clippy's autofixes will try to break it).

---

## What is done (all oracle-validated against `squashfs-tools`)

| Phase | What |
|---|---|
| 0 | Reader retains full inode metadata, ID + xattr tables, device nodes. Diffed against `unsquashfs -lls` at **123,645 entries, 0 mismatches**. |
| 1a | Writer emits valid v4.0 images; gzip / XZ / zstd accepted by `unsquashfs`. |
| 1b | Fragment packing — **1.004 size ratio** vs `mksquashfs` on a real `/etc`. |
| 2a | `FileContent` seam; host files stream instead of loading whole. |
| 2b | Source→tree bridge; **2,513-node** read→rebuild→reread round-trip identical. |
| 2c | `EditableFilesystem` on bare `.squashfs` — create/delete/mkdir/rename/symlink, rebuild on sync. `rb-cli put` edits a real image. |

Adjacent work that landed alongside: `src/fs/attrs.rs` (D6 attribute
resolution), `src/fs/id_names.rs` (uid/gid → name from the image's own
`/etc/passwd`), and attribute view/edit UI across Inspect, Commander and the TUI.

**Oracle:** `brew install squashfs` (installed). Fixture: the Ubuntu 12.04 PPC
live CD at `~/Downloads/ubuntu-12.04-desktop-powerpc.iso`, overridable with
`RB_SQUASHFS_ISO`. Tests skip cleanly when either is absent.

---

## Pending work, in the order I'd do it

### A. Stale doc — do this first, it's small and it's a rule violation

`docs/full_MiSTer_support_status.md:15-18` still says SquashFS is
*"read-only browse + extract … no edit or resize by design, since it is built
offline by `mksquashfs`"*. Untrue since 2c. CLAUDE.md's pre-commit doc-sync
requires this file be walked whenever filesystem capability changes; it was
missed. README's Filesystems table **was** updated (Edit = "Yes (whole-image
rebuild)"), so the two now disagree.

### B. Phase 2d — the main remaining plan item

1. **Partition-hosted images.** `SquashfsEditor` targets a bare superfloppy
   (offset 0). It already carries an `offset` field and writes at it, but this
   is untested for a SquashFS inside a partition, and a rebuild that grows past
   the partition must be refused rather than scribbling over the next one.
2. **The size-budget prompt** (`squashfs_edit.md` §2 — the piece with no
   analogue elsewhere in the tree). Modes `fit` / absolute / `--grow N`;
   per-container defaults (bare file → `fit`; partition → current partition
   size); **two-stage enforcement** (pre-flight refusal, then post-rebuild
   size check before anything is replaced); and the §2.5 projection, which can
   be near-exact because unchanged files' compressed sizes are known.
   Surfaces: GUI dialog, `--size`/`--grow` on the CLI, TUI.
3. **Commit as temp + rename (D2).** `sync_metadata` currently rebuilds into a
   RAM buffer and overwrites in place — safe against a *failed* rebuild, but it
   neither truncates (a shrunk image leaves trailing bytes past `bytes_used`;
   valid, but untidy) nor gives atomic replacement. Wire the temp + fsync +
   atomic-rename path.
4. **xattr-inheritance-on-replace** — the one known fidelity gap (D4). Replacing
   a file (delete + create) loses its xattrs, because `CreateFileOptions` has no
   channel to carry them. Narrow (only capability-bearing binaries, only when
   overwritten) but real. Either add an xattrs field to `CreateFileOptions` or
   have the editor capture-and-restore across a replace.

### C. Attribute-editing breadth (spun out of the GUI work; not in the original plan)

The trait surface exists and the UI is wired everywhere, but **only SquashFS
implements the write side**, so the controls render and then fail at apply time
on other filesystems:

- `set_owner`: **squashfs only**. Needed on ext, UFS, XFS, EFS, Minix — all
  carry uid/gid inode fields.
- `set_permissions`: **ext + squashfs only**. Same list.
- `supports_xattrs` / `list_xattrs` / `set_xattr` / `remove_xattr`:
  **squashfs only**. Should also cover **ext** and **XFS** (the other
  xattr-capable filesystems). Minix and EFS have no xattr concept — leave them
  returning `Unsupported`, which correctly hides the UI section.
- **HFS+ is a special case worth a look.** It has real xattr storage, but via
  *inherent* CNID-keyed methods (`hfsplus.rs:1206`, `:4144`, `:4168`) — not the
  new `FileEntry`-keyed trait methods. So HFS+ xattrs exist and the new UI won't
  show them. A thin entry→CNID adapter would surface them.
- **The UI was compile-verified, not driven.** Nobody has clicked through the
  Inspect rows or keyed the TUI overlay (`v` / F3, then `m`/`o`/`x`). Worth a
  real run.

### D. Phase 2-opt — deferred, but the memory ceiling is real

Lazy `FileContent::Source` streaming + verbatim block reuse. Today
`read_build_tree` reads every file eagerly into `FileContent::Bytes`, so a
whole-image rebuild peaks at the decompressed image — **~1.5 GB for the 558 MB
Ubuntu rootfs**. Fine for AppImages and appliance images; not fine as routine.
Block reuse (copying an unchanged file's already-compressed blocks instead of
decompress→recompress) is what makes rebuild cost scale with the *edit* rather
than the image, and it slots in behind the existing `FileContent` seam without
changing signatures. Watch out: a tail sharing a **fragment** with other files
can't be copied verbatim in isolation — group reuse by fragment.

### E. Phase 3 — containers

SquashFS inside **ISO 9660** (`casper/filesystem.squashfs` on every live CD) and
**AppImage** (ELF stub + appended filesystem — the squashfs is the tail, so it
can grow). Both need the size budget from 2d first.

### F. Phase 4 — create + verify

- `rb-cli new volume squashfs` — `BuildNode::from_host_dir` already does the
  tree-building half, so this is mostly CLI wiring.
- A structural verifier ("does every metadata block, inode, dirent and data
  block decompress and cross-reference"). Note this is **not** an fsck: SquashFS
  carries no checksums, so corruption shows up as a decompression failure and
  there is nothing to repair *from*.

### G. LZO — optional, currently refused for both read and write

An LZO-compressed image is refused by name today. **Reading** one is one
dependency away (`lzokay`, MIT, Rust 1.81 — mind the 1.73 floor) and would close
the last read gap. **Writing** stays refused indefinitely per D5: no pure-Rust
LZO encoder is as mature as the decoders.

---

## Key files

| Path | Role |
|---|---|
| `src/fs/squashfs.rs` | Reader + `read_build_tree` / `read_build_subtree` bridge |
| `src/fs/squashfs_write.rs` | Writer (`write_squashfs`, `BuildNode`, `FileContent`, fragments) |
| `src/fs/squashfs_edit.rs` | `SquashfsEditor` — `EditableFilesystem`, rebuild on sync |
| `src/fs/attrs.rs` | D6 mode/uid/gid resolution (shared by all filesystems) |
| `src/fs/xattr.rs` | Shared `Xattr` type + hex/text value parsing |
| `src/fs/id_names.rs` | uid/gid → name from the image's `/etc/passwd` + `/etc/group` |
| `src/fs/mod.rs` | Dispatch — `"squashfs"` arms in read + editable openers |
| `src/model/edit_queue.rs` | `StagedEdit::{SetPermissions,SetOwner,SetXattr,RemoveXattr}` |

## Verification commands

```bash
cargo build --all-targets            # must be zero warnings
cargo clippy --all-targets -- -D warnings
cargo test --lib squashfs            # includes the fixture-gated oracle tests
cargo test --lib                     # 2519 green as of this writing

# The 1.73 floor — a modern green does NOT imply a vintage green.
cargo build --manifest-path rb-cli-vintage/Cargo.toml \
  --no-default-features --features native-zstd,remote,tui,rust173-polyfill \
  --ignore-rust-version

# Size parity against mksquashfs on a real tree (optional corpus override)
RB_SQUASHFS_SIZE_CORPUS=/path/to/tree cargo test --lib image_size_is_comparable
```
