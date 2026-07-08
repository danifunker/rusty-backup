# Bump `opticaldiscs` to 0.6.0 (file metadata + Joliet/Rock Ridge)

> **Task prompt** for a Claude Code session working in this repo
> (`rusty-backup`). Self-contained: follow it top to bottom, then run the
> verification steps before reporting done. All line numbers are approximate —
> `rg` for the symbols rather than trusting them. This work is gated behind the
> `optical` feature, so build/test **with `--features optical`**.

## Goal

Bump the `opticaldiscs` dependency from **`0.5`** to **`0.6`**. The core bump is
nearly free — 0.6.0's breaking change is *additive* (two new public fields on
`FileEntry`), which does **not** break code that only *reads* `FileEntry` (which
is all rusty-backup does). The value is new metadata the optical path can now
carry through: **per-file dates**, **POSIX ownership/permissions**, and
**Joliet / Rock Ridge** long-name + symlink support that now surfaces
automatically in the optical browser.

> rusty-backup currently pins `opticaldiscs = "0.5"` (`^0.5` = `<0.6.0`), so the
> pin **must** be bumped or Cargo won't pick up 0.6.0.

## Why bump

1. **Correct Mac file dates on extraction (the headline win).** 0.6.0 exposes the
   on-disc creation/modification timestamps via `FileEntry::timestamps`. For
   HFS/HFS+ these are **raw seconds since the Mac epoch (1904-01-01)** — the
   *exact* encoding MacBinary II uses — so they drop straight into the MacBinary
   date fields with no conversion. Today rusty-backup writes no real dates.
2. **Joliet + Rock Ridge (automatic).** The ISO 9660 browser now prefers a Joliet
   tree (UTF-16BE Unicode names) when present, and reads Rock Ridge/SUSP for
   POSIX metadata, long names, and symlink targets. The optical browse/extract UI
   gets correct long/Unicode names and `symlink_target` for these discs with **no
   code change**.
3. **POSIX metadata** (`FileEntry::posix`: mode/uid/gid) is now available for
   HFS+, EFS, and Rock Ridge discs — usable if/when extraction wants to preserve
   permissions.

## The (non-)breaking change

`FileEntry` gained two public fields:

```rust
pub timestamps: Option<FileTimestamps>,   // raw, tagged by filesystem
pub posix:      Option<PosixMetadata>,     // mode/uid/gid where available
```

Adding public fields to a non-`#[non_exhaustive]` struct is a *minor* semver
break in Rust: it only breaks code that (a) builds `FileEntry` with a struct
literal or (b) exhaustively destructures it with `FileEntry { .. }` **without**
a trailing `..`. rusty-backup does neither (it consumes `FileEntry` from
opticaldiscs' own browser), so **expect a clean compile with only the pin bump.**

`MasterDirectoryBlock` and `HfsPlusVolumeHeader` also gained date fields (same
additive rule). No function signatures changed; `new_hfs_file` is unchanged.

New public API you may use (all re-exported at the crate root):

```rust
pub enum FileTimestamps {
    Hfs     { created: u32, modified: u32, backup: u32 },                    // secs since 1904 (local)
    HfsPlus { created: u32, content_modified: u32, attribute_modified: u32,
              accessed: u32, backup: u32 },                                  // secs since 1904 (GMT)
    Iso9660 { recorded: Iso9660DateTime, created: Option<Iso9660DateTime>,
              modified: Option<Iso9660DateTime>, accessed: Option<Iso9660DateTime> },
    Unix    { atime: i64, mtime: i64, ctime: i64 },                          // secs since 1970 (EFS)
}
pub struct PosixMetadata { pub mode: u32, pub uid: u32, pub gid: u32 }        // + permission_bits(), is_symlink()
pub const MAC_EPOCH_UNIX_OFFSET: i64 = 2_082_844_800;                        // secs between 1904 and 1970 epochs
// also: Iso9660DateTime (fields + year(), to_iso8601()), JolietVolumeDescriptor
```

## Required edits

### 1. `Cargo.toml`

```toml
opticaldiscs = { version = "0.6", features = ["drives"], optional = true }
```

> If `opticaldiscs` 0.6.0 isn't on crates.io yet, point at the local checkout for
> testing and flip back once published:
>
> ```toml
> opticaldiscs = { path = "../opticaldiscs-rs", features = ["drives"], optional = true }
> ```

Then run `cargo tree -i libchdman-rs` and confirm opticaldiscs 0.6.0 (still on
`libchdman-rs 0.288.8`, unchanged from 0.5.0) and rusty-backup's direct
`libchdman-rs` dependency still resolve to a **single** copy.

### 2. Confirm nothing broke (usually nothing)

```sh
rg -n 'FileEntry\s*\{|MasterDirectoryBlock\s*\{|HfsPlusVolumeHeader\s*\{' src
```

If any hit is a *construction* or an exhaustive destructure without `..`, add the
new fields (or a trailing `, ..`). Expect **no hits** in rusty-backup's own code.

## Optional follow-ups (do the dates one; it's the point of the bump)

### A. Stamp real Mac dates into MacBinary / AppleDouble (recommended)

The 0.5.0 bump wired extraction to use raw `type_code`/`creator_code`
(`resource_fork::build_macbinary` / `build_appledouble`, called from
`src/cli/verbs/optical.rs` and `src/optical/browse_view.rs`). Extend those to
carry dates:

- **MacBinary II** stores creation date at header offset 91 and modification date
  at 95, both as **u32 secs since 1904** — i.e. exactly what
  `FileTimestamps::Hfs`/`HfsPlus` already hold. Map directly:

  ```rust
  let (created, modified) = match entry.timestamps {
      Some(FileTimestamps::Hfs { created, modified, .. }) => (created, modified),
      Some(FileTimestamps::HfsPlus { created, content_modified, .. }) => (created, content_modified),
      _ => (0, 0),
  };
  // pass created/modified (secs-since-1904) straight into the MacBinary date fields
  ```

- **AppleDouble** "File Dates Info" (entry ID 8) uses **secs since 2000-01-01**
  (signed i32). Convert from the Mac-1904 value:
  `let secs_2000 = (mac_1904 as i64) - MAC_EPOCH_UNIX_OFFSET - 946_684_800;`
  (i.e. Mac-1904 → Unix → subtract the 2001 epoch offset), clamped to `i32`.

  If `resource_fork::build_macbinary` / `build_appledouble` don't yet take date
  args, add optional params (default to 0 / "now" when `timestamps` is `None`)
  rather than changing every caller's behavior silently.

### B. Symlinks / invisibles in the browser (optional)

Rock Ridge/HFS symlinks now populate `entry.symlink_target`; `entry.posix`
carries mode bits (`is_symlink()`, `permission_bits()`). Consider surfacing these
in the browse view or skipping invisible entries.

## Verification (run all; report output)

```sh
cargo build  --features optical
cargo test   --features optical
cargo clippy --features optical -- -D warnings
cargo fmt --check
cargo tree -i libchdman-rs        # expect a single 0.288.8
```

If you have a Mac HFS/HFS+ optical image, extract a file as MacBinary and confirm
(with a hex dump or `macbinary`-aware tool) that the creation/modification dates
now match the on-disc dates instead of being zero. For a Joliet or Rock Ridge ISO,
confirm long/Unicode names and any symlinks appear in the browse listing.

## Done criteria

- `Cargo.toml` → `opticaldiscs = "0.6"` (crates.io), or the path dep if 0.6.0
  isn't published yet, with a note to flip it back.
- Clean compile (no `FileEntry` literal/exhaustive-match breakage).
- All five verification commands clean; the optical test suite passes.
- (If done) MacBinary/AppleDouble extraction carries real Mac dates.
