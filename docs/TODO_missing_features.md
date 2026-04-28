# Missing Features

A running log of feature gaps surfaced during cleanup work in `docs/codecleanup.md`. Items here are *adds*, not refactors — they represent on-disk state or capabilities the codebase doesn't yet expose.

Conventions:
- Group by area (filesystem, GUI, etc.)
- Each item: what's missing + why we know it should exist + suggested shape
- Not prioritized; pull from this list when adding features

---

## Filesystems

### ProDOS — access-bit setter
- **Gap:** ProDOS directory entries carry an *access byte* at offset 30 (read/write/destroy/rename/backup-needed flags; `$C3` unlocked, `$21` locked). The codebase reads and writes this byte (`src/fs/prodos.rs:602, 637, 664`) but the `EditableFilesystem` trait offers no way to change it after creation.
- **Why surfaced:** §2 of `codecleanup.md` — `set_permissions(mode: u32)` is Unix-shaped and only ext implements it. ProDOS access bits are an 8-bit format with different semantics, so they can't piggy-back on `set_permissions`.
- **Suggested shape:** Mirror `set_prodos_type` — add a ProDOS-specific method:
  ```rust
  fn set_prodos_access(&mut self, entry: &FileEntry, access: u8) -> Result<(), FilesystemError>
  ```
  or a higher-level `set_locked(entry, bool)` if a single locked/unlocked toggle covers the GUI case.
- **GUI hookup:** new `StagedEdit::SetProdosAccess` variant in `src/gui/browse_view.rs` (parallel to `SetProdosType`); button gated on `fs_type == "ProDOS"`.
