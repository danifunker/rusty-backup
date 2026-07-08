# Filesystem Check & Repair (fsck)

## Architecture

Fsck is split into two layers:

### Shared types (`src/fs/fsck.rs`)

Filesystem-agnostic result types used by the `Filesystem` / `EditableFilesystem`
traits and the GUI. Any filesystem's checker produces these:

| Type | Purpose |
|------|---------|
| `FsckResult` | Errors, warnings, stats, orphaned entries, and `repairable` flag |
| `FsckIssue` | Single issue: `code` (string), `message`, and `repairable` bool |
| `FsckStats` | Files/dirs checked + `extra` key-value pairs for fs-specific stats |
| `OrphanedEntry` | File/dir whose parent directory is missing (id, name, type, parent id) |
| `RepairReport` | Applied fixes, failed fixes, and unrepairable count |

The `code` field is a short string like `"BadSignature"` or `"MissingParent"`.
Each filesystem defines its own internal code enum and converts to string via
`Debug` formatting. The `repairable` bool is set per-issue by the filesystem's
checker, so the GUI never needs filesystem-specific knowledge to decide what
can be repaired.

### Per-filesystem checker (e.g. `src/fs/hfs_fsck.rs`)

Contains the actual check and repair logic. Internally uses a private enum
(`HfsFsckCode`) for pattern matching during repair classification, but only
produces shared `FsckIssue` values via a helper:

```rust
fn hfs_issue(code: HfsFsckCode, message: impl Into<String>) -> FsckIssue {
    FsckIssue {
        code: format!("{:?}", code),
        message: message.into(),
        repairable: is_repairable(code),
    }
}
```

## Trait integration

The `Filesystem` trait has:
```rust
fn fsck(&mut self) -> Option<Result<FsckResult, FilesystemError>>;
```

The `EditableFilesystem` trait has:
```rust
fn repair(&mut self) -> Result<RepairReport, FilesystemError>;
```

Both return `None` / `Err(Unsupported)` by default. Each filesystem opts in by
overriding these methods.

## Adding fsck for a new filesystem

1. Create `src/fs/<name>_fsck.rs` (e.g. `hfsplus_fsck.rs`, `ext_fsck.rs`).
2. Define an internal issue code enum (e.g. `ExtFsckCode`) with `#[derive(Debug)]`.
3. Write an `is_repairable(code) -> bool` function for your codes.
4. Write a helper like `ext_issue(code, message) -> FsckIssue` using the shared type.
5. Implement `check_<name>_integrity(...)` returning `FsckResult`.
6. Implement `repair_<name>(...)` returning `RepairReport` (if any repairs are possible).
7. Override `fsck()` and `repair()` on your filesystem's `Filesystem` / `EditableFilesystem` impl.
8. Add `pub mod <name>_fsck;` to `src/fs/mod.rs`.

The GUI (both browse_view and inspect_tab) automatically handles display of
errors, warnings, orphaned entries, stats, and repair reports using the shared
types -- no GUI changes needed.

## Check phases (HFS example)

The HFS checker runs four phases:

| Phase | What it checks |
|-------|---------------|
| 1 - MDB | Signature (0x4244), block size (multiple of 512) |
| 2 - B-tree | Header/root node kinds, offset tables, leaf chain integrity |
| 3 - Catalog | Thread/record consistency, parent existence, file/folder counts, valence |
| 4 - Extents | Out-of-range extents, overlapping extents, bitmap vs catalog cross-check |

## Unrepairable issues

Some errors cannot be automatically fixed:

- **MissingParent**: A file or directory references a parent CNID that no longer
  exists in the catalog. The parent was deleted or corrupted. These entries are
  collected in `FsckResult.orphaned_entries` and displayed grouped by missing
  parent in the GUI. Real Mac tools would move these to a "lost+found" directory,
  but that requires creating synthetic directory records which risks further
  corruption on vintage volumes.

- **BadBlockSize**, **OffsetTableNotMonotonic**, **OffsetTableOutOfBounds**:
  Fundamental structural damage that makes the B-tree unreadable.

The GUI shows a warning after repair listing how many issues remain unrepairable.

## Check phases (FAT example)

`src/fs/fat_fsck.rs` is an allocation-level checker rather than a B-tree one. It
reads the primary FAT once into memory, enumerates the directory tree through the
public `Filesystem` interface, and traces every file/dir cluster chain into an
ownership map:

| Pass | What it checks |
|------|---------------|
| 0 - Geometry | FAT large enough to hold an entry per data cluster (`FatTooSmallForClusters`) |
| 1 - Header | FAT[0] media identifier; FAT[1] clean-shutdown / hard-error flags (warnings) |
| 2 - Chains | Loops, links into free/bad/reserved/out-of-range clusters, cross-links, size-vs-chain |
| 3 - Lost clusters | Allocated clusters no chain references, grouped into chains |
| 4 - FAT mirrors | Secondary FAT copies vs. the primary |

A single `analyze()` pass produces both the `FsckResult` (for `fsck_fat`) and a
concrete fix list (for `repair_fat`), so check and repair never diverge. Repair
touches **only the FAT table** — free lost chains, truncate over-long / looped /
broken chains, resync mirrors, rewrite FAT[0]. Faults needing a directory-entry
rewrite (cross-links, size-exceeds-chain, a start pointer into free/bad space)
are surfaced for diagnosis but not auto-repaired. Detection and repair are
byte-verified against the system `fsck_msdos` on real fixtures.

FAT does not use the `<name>_issue` helper / internal-enum pattern above; it
builds `FsckIssue` values directly and pairs each repairable one with its fix in
the shared `analyze()` pass.

## Check phases (exFAT example)

`src/fs/exfat_fsck.rs` follows the same `analyze()`-shared-by-check-and-repair
shape as FAT, but the central structure is exFAT's **allocation bitmap** rather
than a FAT:

| Pass | What it checks |
|------|---------------|
| Boot region | Main boot checksum (sector 11), backup region (sectors 12-23) vs main, VolumeDirty flag |
| Allocation | Directory tree walked; each file/dir traced contiguously or through the FAT per its NoFatChain flag; plus the root chain, bitmap file, and up-case table |
| Bitmap | Traced allocation reconciled against the on-disk bitmap (`BitmapUsedButFree`, `BitmapLeaked`); cross-links / out-of-range refs surfaced |
| Up-case | Table checksum vs its directory entry (surfaced) |

Repair rebuilds the allocation bitmap from the traced allocation (the exFAT
analogue of CBM VALIDATE's BAM rewrite), then leaves the boot regions fully
consistent — recompute PercentInUse, clear VolumeDirty, recompute the main
checksum, and copy the main region over the backup. Verified against the system
`fsck_exfat` (which needs a loopback device, so the oracle test attaches the
image with `hdiutil`).
