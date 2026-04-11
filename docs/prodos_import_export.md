# ProDOS Import/Export & GUI Filetype Plan

Status: **Planned** — accepted defaults, ready for implementation.

This document captures the full design for preserving ProDOS file type and
aux type information when exporting files out to a host filesystem and
importing them back, plus the GUI changes needed to view and edit ProDOS
types inside the browse view.

The plan below **bakes in the accepted defaults** from the design review:

- Export format: **CiderPress `#TTAAAA` filename suffix** (no sidecar)
- GUI scope: **Medium** — view, edit-on-staged-add, and edit-existing
- `FileEntry::aux_type`: **new dedicated field** (not overloaded on `special_type`)

## Background

ProDOS files carry two pieces of metadata that other filesystems don't have:

| Field | Size | Meaning |
|-------|------|---------|
| `file_type` | u8 | Coarse type (e.g. `$04 TXT`, `$FC BAS`, `$B3 S16`) |
| `aux_type`  | u16 | Type-specific auxiliary (load address for BIN, `$0801` for BAS, record length for random TXT, etc.) |

Today the ProDOS layer populates `FileEntry::type_code` (display-formatted
as `$XX ABC` via `prodos_types::format_type_code`) but **drops `aux_type`
entirely**. Exporting a file to the host loses both, so a round-trip back
into ProDOS would default everything to `$06 BIN` / aux `$0000` unless the
user manually retypes every file.

We already have a rich type table (`assets/prodos_file_types.json` +
`src/fs/prodos_types.rs`) with extension-based auto-detection for create.
The missing pieces are:

1. A lossless way to encode `(type_byte, aux_type)` onto an exported
   filename so it survives a copy to any modern filesystem.
2. A `FileEntry::aux_type` field so the GUI and export logic can see the
   aux value.
3. A GUI for viewing and setting ProDOS type/aux, for both pending
   (staged) file additions and already-committed files.
4. An `EditableFilesystem` hook to actually mutate type/aux on disk for
   existing files.

## 1. Export format — CiderPress `#TTAAAA` suffix

### Chosen scheme

Append a hash-prefixed hex suffix to the host filename:

```
<name>#<TT><AAAA>
```

- `TT` — 2 hex digits, the ProDOS type byte (uppercase)
- `AAAA` — 4 hex digits, the aux type (uppercase, big-endian as written)

Examples:

| ProDOS name | type | aux | Host filename                |
|-------------|------|------|------------------------------|
| `README.TXT`  | `$04` | `$0000` | `README.TXT#040000`        |
| `HELLO.BAS`   | `$FC` | `$0801` | `HELLO.BAS#FC0801`         |
| `FINDER.DATA` | `$C9` | `$0000` | `FINDER.DATA#C90000`       |
| `ANTETRIS`    | `$B3` | `$DB7F` | `ANTETRIS#B3DB7F`          |

This is the long-standing convention from CiderPress, NuLib2, and the
Apple II community. It survives copy-to-anywhere (macOS, Windows, Linux,
SMB, cloud drives, zip archives) because it's just characters in the
filename, and it round-trips back into ProDOS without ambiguity.

### Why not the alternatives

| Scheme              | Pros                           | Cons                                                                 |
|---------------------|--------------------------------|----------------------------------------------------------------------|
| JSON sidecar file   | Pretty; extensible             | Two files per export; easy to lose; clutters host directory         |
| NuFX / ShrinkIt     | Native ProDOS archive          | Not human-browsable; requires an extractor; not what "export" means |
| macOS xattrs        | Invisible to user              | macOS-only; stripped by zip/SMB/rsync; invisible == fragile         |
| Overload `special_type` | No new FileEntry field       | Conflates display and data; breaks cross-fs use of the field        |

The suffix is the only scheme that lets a user drag a file into Finder,
email it, zip it, push it to GitHub, and then drag it back — with type
and aux intact.

### API

New helper module (probably `src/fs/prodos_export.rs` or inline in
`prodos_types.rs`):

```rust
/// Encode a ProDOS (type, aux) pair as a CiderPress-style suffix,
/// including the leading `#`. Example: (0xFC, 0x0801) -> "#FC0801".
pub fn encode_cp_suffix(type_byte: u8, aux: u16) -> String;

/// Look for a trailing CiderPress suffix on a host filename. Returns
/// (stem, type_byte, aux) if a valid suffix is present, else None.
/// The stem is the filename with the suffix removed.
pub fn decode_cp_suffix(filename: &str) -> Option<(&str, u8, u16)>;
```

Rules for `decode_cp_suffix`:

- Match `#[0-9A-Fa-f]{6}` at the very end of the filename.
- Case-insensitive on hex digits.
- If the filename has a trailing dot-extension **after** the suffix
  (e.g. `FOO#FC0801.bak` from someone's backup tool), the suffix is
  **not** matched — we only decode when `#TTAAAA` is the literal tail.
  Users who want to keep the suffix through backup stages should avoid
  appending further extensions.
- If no suffix matches, return `None` and caller should fall back to
  extension-based detection (`type_for_filename`).

### Export flow (future "Export mode" dropdown)

The existing HFS resource-fork export has four modes
(`DataForkOnly`, `AppleDouble`, `MacBinary`, `SeparateRsrc`).
ProDOS export gets a parallel dropdown with (initially) two modes:

- `ProdosExportMode::WithTypeSuffix` — **default**, appends `#TTAAAA`
- `ProdosExportMode::Plain` — writes the bare filename (lossy)

Additional modes (`JsonSidecar`, `NuFx`) can be added later without
reworking the call sites.

## 2. `FileEntry::aux_type: Option<u16>`

Add a new optional field to `FileEntry` in `src/fs/entry.rs`:

```rust
pub struct FileEntry {
    // ...existing fields...
    pub type_code: Option<String>,     // display string, e.g. "$04 TXT"
    pub creator_code: Option<String>,  // HFS-only
    pub aux_type: Option<u16>,         // NEW — ProDOS aux
}
```

- `list_prodos_directory` populates it for every non-directory entry.
- Other filesystems leave it `None`.
- Serializers / debug formatters treat `None` as absent.

Why a new field rather than reusing `special_type`/`type_code`:

- `type_code` is already a display string; parsing `$XX ABC` back into
  bytes is a lossy round-trip.
- Aux is a real first-class ProDOS concept, not a debug/display value.
- Other filesystems may want their own 16-bit typed extras later; keeping
  ProDOS on its own named field avoids collisions.

## 3. GUI changes — Medium scope

### 3.a Auto-detection on add (already works)

`ProDosFilesystem::parse_file_type` already calls
`prodos_types::type_for_filename` to auto-populate `(type_byte, aux_type)`
from the extension when the user doesn't specify one. No change needed —
dragging `HELLO.BAS` into a ProDOS image already stores it as
`$FC / $0801`.

### 3.b "Set ProDOS Type…" dialog

Triggered from:

- Context menu on a staged `AddFile` entry → "Set ProDOS Type…"
- Context menu on an existing file in edit mode → "Set ProDOS Type…"
- Optional future: multi-select bulk edit

Dialog contents:

1. **Type dropdown**, sorted by hex byte. Each row shows
   `$04 TXT — ASCII Text`, pulled from `prodos_types::table()`. The
   current type is pre-selected.
2. **Custom type** hex input (`$XX`) for types not in the embedded table.
   Entering a value here unsets the dropdown selection.
3. **Aux type** hex input (`$XXXX`), defaulting to the current value or
   `$0000`. Validated as 4-digit hex.
4. **Apply / Cancel** buttons.

Behavior:

- **Staged add**: mutates the pending `StagedEdit::AddFile` in
  `browse_view.rs`. The dialog updates its `type_byte`/`aux_type` fields
  (new) so that `apply_staged_edits` passes them through to
  `CreateFileOptions`.
- **Existing file**: pushes a new
  `StagedEdit::SetProdosType { entry_id, type_byte, aux_type }` onto the
  staged edits queue. Applied via a new
  `EditableFilesystem::set_prodos_type()` method (see §3.d).

### 3.c Type column / badge in file list

Add a "Type" column to the ProDOS browse view, showing the
`$XX ABC` string from `format_type_code`. Hover tooltip shows the full
description and aux type (`Applesoft BASIC • $0801`).

Other filesystems either hide the column or show a generic value.
Simplest implementation: only show the column when the open filesystem
reports ProDOS (or HFS/HFS+, which already have type columns today).

### 3.d `EditableFilesystem::set_prodos_type()`

New trait method, default `Err(FsError::Unsupported)`:

```rust
fn set_prodos_type(
    &mut self,
    entry_id: u64,
    type_byte: u8,
    aux_type: u16,
) -> Result<()>;
```

ProDOS implementation:

- Find the catalog entry for `entry_id`.
- Update `file_type` (offset `+16` in the 39-byte entry) and `aux_type`
  (offset `+31`, LE u16).
- Caller still runs `sync_metadata()` to flush — consistent with all
  other staged edits.

New `StagedEdit::SetProdosType { entry_id: u64, type_byte: u8, aux_type: u16 }`
variant in `browse_view.rs`, matched in `apply_staged_edits` to call the
new method. Virtual overlay in the browse view shows the pending new
type (e.g. italics or a color accent) until Apply.

## 4. Import — parsing CiderPress suffix on drop

In `add_host_file()` in `browse_view.rs`:

1. If the target filesystem is ProDOS, call
   `prodos_export::decode_cp_suffix(host_filename)`.
2. If it returns `Some((stem, tt, aaaa))`:
   - Use `stem` as the ProDOS-side filename (the `#TTAAAA` is stripped).
   - Seed `CreateFileOptions { type_code: Some(format!("${:02X}", tt)), aux_type: Some(aaaa), .. }`.
3. If it returns `None`, fall back to extension-based detection, same
   as today.
4. The GUI "Set ProDOS Type…" dialog is still available to override
   before Apply.

This makes the happy path of "drag out, drag back in" fully lossless
without any user intervention.

## 5. Tests

### Unit tests

- `encode_cp_suffix` / `decode_cp_suffix` round-trip for a representative
  sample:
  - `(0x04, 0x0000)` → `#040000`
  - `(0xFC, 0x0801)` → `#FC0801`
  - `(0xB3, 0xDB7F)` → `#B3DB7F`
  - `(0xFF, 0xFFFF)` → `#FFFFFF`
- Decode rejects:
  - Filename with no `#`
  - `#TTAAA` (5 digits)
  - `#TTAAAAX` (7 digits)
  - `FOO#FC0801.bak` (trailing extension after suffix)
- Decode is case-insensitive: `#fc0801` and `#FC0801` both parse.

### Integration tests (`tests/filesystem_e2e.rs`)

- Open the `test_prodos.hdv` fixture, locate `ANTETRIS` (`$B3 S16`).
  Verify `FileEntry::aux_type` is populated (non-`None`).
- Simulated "export" — compute the host filename via `encode_cp_suffix`,
  assert it matches `ANTETRIS#B3XXXX` where `XXXX` is the actual aux.
- Simulated "import" — feed `HELLO.BAS#FC0801` through
  `decode_cp_suffix`, assert `(stem="HELLO.BAS", 0xFC, 0x0801)`.
- End-to-end set-type: open editable, create a stub file as `$06 BIN`,
  call `set_prodos_type(entry, 0x04, 0x1234)`, sync, reopen, verify the
  catalog entry now reports `$04` and aux `$1234`.

## 6. Implementation order

1. `encode_cp_suffix` / `decode_cp_suffix` + unit tests (pure, no GUI).
2. `FileEntry::aux_type` field + populate in `list_prodos_directory` +
   round-trip integration test.
3. `EditableFilesystem::set_prodos_type` (trait default + ProDOS impl)
   + integration test.
4. `StagedEdit::SetProdosType` variant + `apply_staged_edits` wiring.
5. "Set ProDOS Type…" dialog in `browse_view.rs`.
6. Type column / badge.
7. `add_host_file` CiderPress-suffix parsing.
8. Export mode dropdown (`ProdosExportMode::WithTypeSuffix` default)
   wired into the existing extract flow.

Each step is independently committable and leaves the build green.

## Open questions (non-blocking)

- Should the suffix survive being renamed via the GUI? Proposed: yes —
  stripping the suffix on display and re-adding on export would be more
  user-friendly, but it's extra complexity we can defer. For now the
  suffix lives in the host-side filename only and ProDOS names never
  contain `#TTAAAA`.
- Should multi-select bulk edit be part of the first cut? Deferred to
  "Maximum scope" — not part of Medium.
- Should we warn when a user sets a type that doesn't match the
  extension (e.g. `HELLO.BAS` typed as `$06 BIN`)? Deferred — ProDOS
  itself doesn't enforce this and power users may have reasons.
