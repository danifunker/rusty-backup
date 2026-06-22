# Native Mac archives GUI — draft wording (v2)

Draft strings for the four workflows in OPEN-WORK §6.1 plus a new
**Workflow E** for browsing into archives that already live on a disk
image. ASCII-only (per CLAUDE.md the default egui font has no unicode
glyphs).

## Design-checkpoint decisions (v2 — updated per user)

1. **Detection: magic-sniff, not extension.** Every file the user picks
   in Workflow A (browse-tab Add File...), every file selected inside
   the disk image in Workflows C / E, and every file picked in
   Workflow D's Archives tab is sniffed for archive content. The
   filename extension is informational only and never gates the modal.
   Detection order: BinHex (`(This file must be converted with BinHex
   4.0)` header) → if decoded → re-sniff payload as SIT / SIT5 / SEA.
   Standalone files try SIT / SIT5 / SEA directly. The first match
   wins. If nothing matches, the file is treated as opaque bytes (no
   modal, fall through to the existing add-as-binary path).

2. **Inline tooltips in dropdowns** (no `?` affordance).

3. **No "always X" settings yet** — defer until requested.

4. **Default extraction container on HFS/HFS+ targets: BinHex.**
   Preserves forks on the host roundtrip; the destination is Mac-aware
   so we should never strip forks on import.

5. **`.sea.hqx` is a first-class double-wrap** alongside `.sit.hqx`.
   Same shape: decode HQX → get .sea → optionally extract entries.

## Placeholder convention

`{filename}` in any modal body is a placeholder for the actual picked
file's basename at runtime (e.g. `MyApp.sit.hqx`, `Document.hqx`). The
implementation interpolates the real name when rendering the modal.

## Detected archive kinds

The implementation will surface a single `MacArchiveKind` enum returned
by a `detect_mac_archive(&[u8]) -> Option<MacArchiveKind>` routine in
`src/macarchive/detect.rs`:

| Kind                | Detected via                                                        | Maps to modal              |
|---------------------|---------------------------------------------------------------------|----------------------------|
| `BinHexSingleFile`  | BinHex header parses; decoded data fork is not a SIT/SEA            | "Convert HQX" modal        |
| `BinHexOverSit`     | BinHex header parses; decoded data fork sniffs as SIT/SIT5          | "Convert and/or expand" modal (.sit.hqx) |
| `BinHexOverSea`     | BinHex header parses; decoded data fork sniffs as SEA-bearing app   | "Convert and/or expand" modal (.sea.hqx) |
| `Sit`               | SIT magic at offset 0                                               | "Expand contents" modal    |
| `Sit5`              | SIT5 magic at offset 0                                              | "Expand contents" modal    |
| `Sea`               | Mac app data fork with SEA-style SIT find                           | "Expand contents" modal    |
| `CompactPro`        | `0x01` marker + CRC-validated index                                 | "Expand contents" modal    |
| `Mar`               | `MAR\x80` magic + valid 128-byte header CRC                         | "Expand contents" modal    |

The same enum drives Workflows A / C / D / E uniformly.

## Workflow A — Importing an archive into a disk image

Triggered when the edit-mode browse view's **Add File...** picks (or
drag-drops) a file that sniffs as a Mac archive AND the disk image is
HFS / HFS+. Other filesystems (Amiga, classic-Unix) fall through to
the normal "add as binary" path silently — those filesystems don't
have resource forks to preserve anyway.

### `BinHexSingleFile` (.hqx, single file payload)

**Title:** Convert HQX file?

**Body:** *Would you like to convert the file to binary?*

**Buttons:**

| Label              | Action                                                                                                 |
|--------------------|--------------------------------------------------------------------------------------------------------|
| **Convert**        | Decode BinHex; add the resulting Mac file (data + resource fork + type/creator) to this directory.     |
| **Add as-is**      | Add the .hqx file unchanged.                                                                           |
| **Cancel**         | -                                                                                                      |

### `Sit` / `Sit5` / `Sea` / `CompactPro` / `Mar` (.sit, .sea, .cpt, .mar)

**Title:** Expand archive?

**Body:** *Would you like to expand the contents of `{filename}`?*

**Buttons:**

| Label              | Action                                                                                                 |
|--------------------|--------------------------------------------------------------------------------------------------------|
| **Expand**         | Decompress the archive and add each enclosed file to this directory (forks preserved on HFS/HFS+).     |
| **Add as-is**      | Add the archive file unchanged.                                                                       |
| **Cancel**         | -                                                                                                      |

### `BinHexOverSit` (.sit.hqx) / `BinHexOverSea` (.sea.hqx)

**Title:** Convert and/or expand?

**Body:** *Would you like to convert the file to binary and/or expand
the contents of `{filename}`?* (or `.sea` for the SEA variant)

**Buttons (three actions):**

| Label                         | Action                                                                                                 |
|-------------------------------|--------------------------------------------------------------------------------------------------------|
| **Convert and expand**        | Decode BinHex, decompress the inner archive, add each enclosed file to this directory.                 |
| **Convert only (.sit / .sea)**| Strip the BinHex wrapper, add the .sit / .sea file. Useful when the target Mac has StuffIt Expander but no BinHex tool. |
| **Add as-is (.sit.hqx)**      | Add the wrapped archive unchanged.                                                                     |
| **Cancel**                    | -                                                                                                      |

### Auto-unwrap hook (DC42 / raw HFS payload)

When **Convert** (or **Convert and expand**) decodes a .hqx whose
payload sniffs as DiskCopy 4.2 or raw HFS, the modal additionally
surfaces:

| Label                                  | Action                                                                                                                                          |
|----------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| **Mount in new Inspect tab**           | Open the decoded payload as a disk image in a new Inspect tab instead of dropping it on this disk image as a file.                              |

Same affordance shows in Workflow D when a standalone .hqx decodes to
a disk image.

### Inline errors (shown in the same modal)

| Condition                                         | Inline message                                                                                                                                  |
|---------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| Sniffed as archive but decode fails               | Couldn't decode as <kind>: <error>. You can still add as-is.                                                                                    |
| Expand chosen but disk image is full              | Not enough free space to expand <N> file(s) (need <X>, have <Y>). Try Add as-is or pick a larger target partition.                              |
| Expand chosen but one or more names collide       | <N> file(s) already exist in this directory: <first three names>. Cancel and rename, or pick Add as-is.                                         |

## Workflow B — Exporting selected files as a SIT / HQX archive

Triggered when the user has one or more entries selected in the
edit-mode browse view and opens the **Save As...** dropdown.

### Dropdown entries (added alongside "Raw bytes" / "MacBinary" / "AppleDouble")

| Label                                   | Enabled when                          | Tooltip                                                                                                                                          |
|-----------------------------------------|---------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| **BinHex 4.0 (.hqx)**                   | Exactly one file selected             | Encodes the file's data fork, resource fork, and type/creator codes in printable ASCII. The only flat format that survives a non-HFS roundtrip. |
| **StuffIt classic (.sit)**              | Any selection (files and/or folders)  | Multi-file compressed archive. Works on System 6 and up. Preserves forks on HFS or inside a .hqx wrapper; loses them on other filesystems.       |
| **StuffIt-over-BinHex (.sit.hqx)**      | Any selection                         | StuffIt bundles, BinHex wraps in ASCII for email and web-safe transport. The classic "I emailed you a Mac app" format.                           |

### File-picker default name

| Selection                          | Default name                                              |
|------------------------------------|-----------------------------------------------------------|
| One file `foo.bin`                 | `foo.bin.hqx` / `foo.bin.sit` etc.                        |
| One folder `MyApp`                 | `MyApp.sit` / `MyApp.sit.hqx`                             |
| Multiple entries from `/Apps`      | `Apps.sit` / `Apps.sit.hqx` (parent folder name)          |
| Multiple entries from `/`          | `archive.sit` / `archive.sit.hqx` (volume root fallback)  |

Completion toast: `Wrote <path> (<N> files, <bytes_human>).`

## Workflow C — Extracting an archived file *out of* a disk image

Triggered when the user selects a file inside the browse view that
sniffs as a Mac archive and clicks **Save As...**.

### Modal

**Title:** Save archive

**Body:** *The selected file is a `<kind>` archive. How would you like
to save it?*

**Buttons:**

| Label                                          | Action                                                                                                                                          |
|------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| **Save as-is**                                 | Write the raw archive bytes to the host. Useful when you're passing the archive along without unpacking.                                        |
| **Decode and save contents to a folder...**    | Decompress on the host. The follow-up dialog asks which fork-preserving container to use (BinHex / MacBinary / AppleDouble / raw).              |
| **Cancel**                                     | -                                                                                                                                               |

The "Decode and save contents" path opens the same fork-container
follow-up dialog the Archives tab uses (`ForkFormat` selector). When
the user picks a folder, the extract pipeline runs in a worker thread
with the existing progress affordance.

## Workflow D — Browsing / extracting standalone archives (Archives tab)

The Archives tab (`src/gui/archives_tab.rs`) is in tree and supports
`.sit` / `.sea` / `.sit.hqx`. This slice adds:

### D.1 — Accept loose .hqx and any sniffed archive

The picker drops its extension filter and uses magic sniff instead.
A .hqx whose payload is not a SIT/SEA still carries one file (data
fork + resource fork + Finder info); decode it as a single-file BinHex
archive and display the one entry in the existing entry list.

### D.2 — Auto-unwrap hint for DC42 / raw HFS payloads

When the decoded payload sniffs as DiskCopy 4.2 or raw HFS, surface a
new button alongside "Extract All...":

| Label                                  | Tooltip                                                                                                                                          |
|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| **Mount in new Inspect tab**           | The decoded archive payload is itself a disk image. Open it in a new Inspect tab instead of extracting it as a file.                             |

### D.3 — Per-entry checkboxes for mixed selection extract

Today the tab has "Extract All..." which dumps every entry. Add a
checkbox column on the left of the existing entry grid (default: all
checked). A new button "Extract Selected..." next to "Extract All..."
runs on the checked subset only. The fork-container dropdown applies
to both buttons identically.

### D.4 — File-picker extensions in inspect / restore / backup tabs

Inspect, Restore, and Backup tabs' file pickers should accept
`MAC_ARCHIVE_EXTS` alongside `DISK_IMAGE_EXTS` so a user can route a
`.sit.hqx` (or any sniffed Mac archive) directly through those tabs
without retyping the path. The Archives tab stays the primary home
for archive-only operations; from the other tabs, the archive is
sniffed and — if its payload is a disk image — opened in the right tab
context.

## Workflow E — Browse into archive contents on the disk image (new feature)

When the browse view encounters a file on the disk image that sniffs
as a Mac archive (`Sit` / `Sit5` / `Sea` / `BinHexSingleFile` /
`BinHexOverSit` / `BinHexOverSea`), treat it as a navigable folder:
the user can double-click into it or press **Open archive...** to
descend into its entries, then save individual entries back out via the
existing Save As flow.

### Scope (read-only first)

- **In:** browse entries inside the archive; show name / size /
  type/creator; descend into nested folders inside the archive; Save
  As... a single entry (uses the same fork-container follow-up dialog
  as Workflow C); double-wrap descent (sit.hqx → sit → individual files
  in one continuous browse path).
- **Out (deferred):** edit operations inside the archive (Add / Delete
  / Rename). Repacking an archive after edits is a real piece of work
  and the read-only browse already covers the dominant
  "fish-one-file-out-of-the-bundle" use case.

### UI shape

- The browse-view directory grid gains a new "Open archive..." button
  in the toolbar, enabled when the selected entry sniffs as an
  archive. Double-clicking the entry does the same thing as the
  button.
- After descending, the breadcrumb at the top of the browse pane
  shows the archive's path on the disk plus the navigation inside it,
  e.g. `/Apps/MyApp.sit.hqx > Decoded > MyApp Folder > readme.txt`.
- **Back** in the breadcrumb pops one level; **Up** in the toolbar
  exits the archive entirely back to the surrounding directory.
- The edit toolbar's edit-only buttons (Add File... / New Folder... /
  Delete / Bless Folder / Lock / Unlock / etc.) are disabled while
  inside an archive (read-only view).
- Save As... on an entry inside the archive opens the Workflow C
  fork-container follow-up dialog directly — no "save as-is" choice,
  since the archive entry doesn't have a "raw bytes" form.

### Architecture sketch

- A new `BrowseSource` enum in browse_view.rs:
  ```
  enum BrowseSource {
      Filesystem,          // existing — backed by Filesystem trait
      Archive {            // new
          archive_path_on_disk: String,    // the .sit / .hqx / etc.
          decoded_bytes: Arc<Vec<u8>>,     // BinHex-decoded if HQX-wrapped
          archive: Arc<StuffItArchive>,    // parsed entries
          inner_path: Vec<String>,         // current depth inside the archive
      },
  }
  ```
- `list_directory()` for `BrowseSource::Archive` reads from
  `archive.entries`, filtered to entries directly under `inner_path`.
- `read_file()` for `BrowseSource::Archive` returns the entry's
  decompressed data fork (and surfaces resource fork + type/creator
  separately for the fork-container saver).
- Switching out of archive mode just restores the previous
  `BrowseSource::Filesystem` and re-renders.

### What this enables

The motivating workflow: user has `MyApp.sit.hqx` on a vintage HFS
disk image, doesn't want to write through the .hqx wrapping every time
they need a peek. They double-click the .hqx → see decoded contents →
double-click again into the .sit → see the bundled files →
**Save As...** the one document they actually needed.

## Implementation order

D → A → B → C → E, with each slice landing as its own commit:

1. **D.4** (one-liner add to three file pickers) — smallest, no UI risk.
2. **D.1** (drop extension filter, sniff-on-load) — adds the
   `detect_mac_archive` routine the rest of the work reuses.
3. **D.2** (DC42 / HFS auto-unwrap hint) — adds one button when the
   sniff fires; reuses the inspect-tab open path.
4. **D.3** (mixed selection checkboxes) — UI-only, no new decode work.
5. **A** (import modal) — first new modal; establishes the
   extract-and-add path the disk-image side will reuse.
6. **B** (export dropdown additions) — three new SIT/HQX writer paths,
   reuses `build_archive_tree` + `binhex::encode`.
7. **C** (extract-out modal) — reuses A's extract pipeline plus B's
   fork-container follow-up dialog.
8. **E** (browse into archive) — last because it's the most novel UI;
   benefits from having the sniff routine + decode pipeline well
   shaken out by the time we land it.
