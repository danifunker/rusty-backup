# SquashFS — resume prompt

Pick-up notes for finishing the SquashFS work. The plan and every design
decision live in [`squashfs_edit.md`](squashfs_edit.md); this file is the
"where we stopped and what's left" companion.

**Branch:** `edit-squashfs` — **not pushed**. ~32 commits ahead of `main`.

---

## Resume prompt (paste this)

> Continue the SquashFS work on branch `edit-squashfs`. Read
> `docs/squashfs_resume.md` for state and `docs/squashfs_edit.md` for the plan
> and decisions D1–D6. **Phases 0 through 3 are done** and oracle-validated
> against real `squashfs-tools`, so pick up at **phase 4 (`rb-cli new volume
> squashfs` + a structural verifier)** — or 2-opt first if a big image's rebuild
> memory is biting.
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

### A. Stale doc — **DONE** (`61cf404`)

`docs/full_MiSTer_support_status.md` claimed SquashFS was read-only "by design",
contradicting the README. Rewritten to read + edit.

### B. Phase 2d — half done

1. **Partition-hosted images — DONE** (`6746e07`, `1aaf509`). `SquashfsEditor`
   now carries a container `capacity` alongside its `offset`, the `0x83`
   editable arm routes SquashFS (it previously had no arm at all, so partition
   editing was unreachable), and reaching the editor at a non-zero offset with
   no declared capacity caps growth at the image's current size rather than
   assuming it may grow. `open_editable_filesystem_within` carries the
   partition length; `PartitionContext::open_editable` (CLI) and
   `BrowseSession::partition_size` (GUI) supply it.
2. **The size budget — DONE** (`6746e07` core, `4fbd39d` surfaces).
   `SizeBudget::{Fit, Limit, Grow}`, both stages of §2.4 enforcement, the §2.5
   projection, and all three surfaces: `rb-cli squashfs plan|put|rm` with
   `--size` / `--grow`, a GUI dialog before entering Edit Mode, and the same
   numbers in the TUI File Info overlay on a SquashFS root.
   Note the projection is *not* the near-exact one §2.5 describes: that needs
   per-file compressed sizes, which only exist once phase 2-opt lands block
   reuse. Today it anchors on the source size and estimates the content delta
   at the image's own observed ratio, reported as a range — and on a small
   image that ratio is withheld entirely, because fixed overhead dwarfs the
   file data and the number would be worse than no number.
3. **Commit as temp + rename (D2) — DONE** (`2292b0e`). A bare `.squashfs`
   commits by sibling temp + fsync + atomic rename, gated on
   `EditContext::whole_file_path` so a partition or container temp is never
   renamed over. Everything else commits in place and now zero-fills what a
   shrunk rebuild no longer occupies.
4. **xattr-inheritance-on-replace — DONE** (`e4d1fef`). `attrs::inherited_xattrs`
   captures before the delete, `CreateFileOptions::xattrs` carries them to the
   replacement; used by `put --force` and tar-import overwrite.

**Phases 2d and 3 are complete.** Next is phase 4 (create + verify) or 2-opt
(lazy streaming / block reuse) — see below.

### C. Attribute-editing breadth (spun out of the GUI work; not in the original plan)

The trait surface exists and the UI is wired everywhere, but **only SquashFS
implements the write side**, so the controls render and then fail at apply time
on other filesystems:

- `set_owner`: **squashfs only**. Needed on ext, UFS, XFS, EFS, Minix — all
  carry uid/gid inode fields.
- `set_permissions`: **ext + squashfs only**. Same list.
- `create_symlink` was the same shape of gap and is partly closed: EFS
  (`9ab650f`), Minix and UFS (`bb60b1a`) now write symlinks, joining PFS3 and
  SquashFS. Still missing on **ext** (needs the inline fast-symlink form real
  volumes use for nearly every link), **AFFS** (a distinct `ST_SOFTLINK` block
  type, not a mode bit), **HFS+** (`slnk`/`rhap` files) and **XFS** (no
  `EditableFilesystem` impl at all).
- `supports_xattrs` / `list_xattrs` / `set_xattr` / `remove_xattr`:
  **squashfs only**. Should also cover **ext** and **XFS** (the other
  xattr-capable filesystems). Minix and EFS have no xattr concept — leave them
  returning `Unsupported`, which correctly hides the UI section.
  The xattr side now has a CLI surface too (`rb-cli xattr list|set|rm`), which
  is how the read-only reader's missing `supports_xattrs` / `list_xattrs`
  override came to light — a *browsed* SquashFS reported no xattrs at all, so
  the GUI File Info panel had been hiding them outside edit mode.
- **HFS+ is a special case worth a look.** It has real xattr storage, but via
  *inherent* CNID-keyed methods (`hfsplus.rs:1206`, `:4144`, `:4168`) — not the
  new `FileEntry`-keyed trait methods. So HFS+ xattrs exist and the new UI won't
  show them. A thin entry→CNID adapter would surface them.
- **The UI was compile-verified, not driven.** Nobody has clicked through the
  Inspect rows or keyed the TUI overlay (`v` / F3, then `m`/`o`/`x`). Worth a
  real run — and the reader bug above is exactly the kind of thing that survives
  a compile-only check, so treat the permissions and owner rows with the same
  suspicion until someone has actually driven them.

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

### E. Phase 3 — containers — **DONE** (`ddef88a` AppImage, `73ca347` ISO)

Both landed on `src/rbformats/payload_slice.rs`, a read/write window whose byte
0 is the payload, so every layer above stays unaware of the wrapper. Two shapes:
a **tail** (AppImage — the appended squashfs may grow the file) and a
**bounded** window (ISO — the payload sits between other files and cannot grow).

- **AppImage** (`src/rbformats/appimage.rs`): full browse + edit. Detected by the
  `AI\x02` ELF marker; payload offset = `e_shoff + e_shentsize*e_shnum`,
  *verified* by a superblock parse. Type-1 (ISO payload) recognised, not opened.
- **ISO 9660** (`src/rbformats/iso_squashfs.rs`, `optical`-gated): browse +
  extract only. Locator enumerates the disc via opticaldiscs (Rock Ridge names)
  and picks the file whose extent begins with a squashfs superblock; plain `.iso`
  only (offset = LBA*2048). Edit refused up front — the payload can't grow.

Gotcha this shook out (`resolve_image_rw` now returns a `HandleShape`): a payload
window is `RwCommit::None` just like a raw file, and the resolver had inferred
"whole file → atomic-rename-replace" from that — which replaced an entire
AppImage with only its payload, deleting the ELF stub. The shape is now stated,
not inferred.

**Follow-up if wanted:** ISO edit *is* achievable (shrink-or-equal only) but
needs a capacity signal that fights the `offset==0 → grow freely` shortcut in
the squashfs dispatch arm (`fs/mod.rs`); deferred as marginal since growth is
impossible and extract-then-rebuild is the real workflow.

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
