# Native Mac archive support — implementation plan

Living plan + progress tracker for **native** (no classic-Mac-required) handling
of the Macintosh fork-preserving archive/encoding formats:

- **BinHex 4.0** (`.hqx`) — read **and** write
- **StuffIt classic** (`.sit`) — read **and** write
- **Self-extracting archives** (`.sea`) — read (StuffIt SEA dispatch)
- **StuffIt X** (`.sitx`) — **deferred** (out of scope for now; see §7)

Goal: decode these formats end-to-end without launching an emulator, and emit
HQX/SIT from files pulled off an HFS/HFS+ volume **preserving both forks + full
Finder info**. Surfaces in **both GUI and CLI** (parity rule, CLAUDE.md).

Update the checkboxes and the **Progress log** at the bottom as work lands. Keep
each step small enough to be one commit (engine layer first, then model, then
view, then tests).

Status legend: `[ ]` not started · `[~]` in progress · `[x]` done · `[-]` dropped/out of scope

---

## 0. What we already have (leverage, do not rebuild)

The fork-container infrastructure is already in the tree — this plan rides on it:

| Capability | Location | State |
|---|---|---|
| MacBinary I/II/III read | `src/fs/resource_fork.rs` `parse_macbinary` | done |
| MacBinary write | `src/fs/resource_fork.rs` `build_macbinary` | done |
| AppleDouble read/write + sidecar detect | `src/fs/resource_fork.rs` `parse_appledouble`/`build_appledouble` | done |
| `ResourceForkMode` enum, `ImportedResourceFork` | `src/fs/resource_fork.rs` | done |
| Finder-info xattr extraction | `src/fs/resource_fork.rs` `read_finder_info_xattr` | done |
| DiskCopy 4.2 parse + encode | `src/rbformats/dc42.rs` | done |
| Extract data + resource fork from a live HFS/HFS+ volume | `Filesystem::read_file` / `write_resource_fork_to`, `FileEntry.{type_code,creator_code,resource_fork_size}` | done |
| `put-macbinary` CLI verb (pattern to mirror) | `src/cli/verbs/put_macbinary.rs` | done |

So the "write forks to a portable single file" half of the request is already
solved for MacBinary/AppleDouble. **BinHex slots in right next to them**; SIT is
the only genuinely new container.

### Reference sources

- **Format docs (on disk):** `~/repos/CiderPress2/ndocs/formatdoc/{StuffIt,MacBinary,AppleSingle}-notes.html` — fadden's notes, authoritative.
- **BinHex 4.0:** well-documented public spec (Peter Lewis); CRC-16-CCITT + RLE90 + 6-bit alphabet.
- **StuffIt decompressors:** The Unarchiver / **XADMaster** `StuffItHandle` + per-method codecs is the canonical reverse-engineered reference. **Not present locally — clone it as a reference repo** (amigasources-style) before starting Phase 2.

---

## 1. The real-world use case (drives the auto-unwrap chain)

Most "compressed floppy" `.hqx`/`.sit` files do **not** contain loose files —
they wrap a **DiskCopy 4.2 / NDIF disk image**. So the practical pipeline is a
chain, and we already own every link except the first:

```
file.hqx → BinHex decode → payload (type 'dImg'/'DDim'/etc.)
         → dc42.rs parse → raw HFS bytes
         → fs::open_filesystem(offset 0) → existing HFS browse/inspect/backup
```

Build the **auto-unwrap dispatch hook in Phase 1** so a decoded HQX whose payload
sniffs as a disk image flows straight into the existing image pipeline, and one
whose payload is a plain file flows into the fork-export path.

---

## 2. BinHex 4.0 (`.hqx`) — read + write   [Phase 1]

### Format (single file, both forks + Finder info)

- Text wrapper: line `(This file must be converted with BinHex 4.0)`, blank line,
  then a stream bracketed by `:` … `:`, wrapped at 64 chars/line.
- Body: **6-bit alphabet** encoding (64-char table, *not* base64) →
  **RLE90** decompression (`0x90` = run marker; `b 0x90 n` = `b`×n; `0x90 0x00`
  = literal `0x90`).
- Decoded payload layout:
  `nameLen(1) · name(n) · 0x00 · type(4) · creator(4) · flags(2) ·
   dataLen(4) · rsrcLen(4) · headerCRC(2) · data(dataLen) · dataCRC(2) ·
   rsrc(rsrcLen) · rsrcCRC(2)`
- CRC = **CRC-16-CCITT** (poly 0x1021), with the CRC field itself taken as 0
  during computation. Reuse / generalize the existing `macbinary_crc16` helper.

### Steps

- [x] `src/fs/binhex.rs`: `parse_binhex(&[u8]) -> Result<BinHexFile>` (richer than `ImportedResourceFork` — carries name + flags too)
- [x] `build_binhex(&BinHexFile) -> String`
- [x] RLE90 encode/decode + 6-bit codec + augmented CRC-16 as private helpers (unit-tested round-trip)
- [x] CLI: `put-binhex IMG[@N] HOST [--dst-dir/--rename/--force]` and `get-binhex IMG[@N] SRC OUT.hqx` (mirror `put_macbinary.rs`); registered in `cli/mod.rs` + `cli/verbs/mod.rs`
- [x] Tests: round-trip unit tests; validated against 5 real `.hqx` samples (all 3 CRCs verify); full CLI E2E (`.hqx` → HFS → `.hqx` → HFS, data forks byte-identical, type/creator/rsrc preserved)
- [ ] GUI: "Export to BinHex (.hqx)…" + "Import .hqx" in browse view alongside existing fork-export options
- [ ] Auto-unwrap hook: decoded payload sniffed via `dc42::detect_dc42` (and raw-HFS sniff) → route to image pipeline
- [-] `ResourceForkMode::BinHex` variant — not needed; BinHex has its own `BinHexFile` type and dedicated verbs rather than riding the host-sidecar import path

**Estimate: a few days. This alone satisfies the stated extract-and-preserve goal.**

---

## 3. StuffIt classic (`.sit`) — read   [Phase 2]

### Format (multi-file archive, tree of Mac files)

- Archive header (~22 B): magic `SIT!` @0, `numFiles`(2), `archiveLength`(4),
  secondary signature `rLau` @10, version, reserved. (Confirm exact offsets
  against CiderPress2 `StuffIt-notes.html`.)
- Per-entry header (~112 B): rsrc method(1), data method(1), name, type(4),
  creator(4), Finder flags(2), dates, **uncompressed** rsrc/data lengths,
  **compressed** rsrc/data lengths, per-fork CRCs, header CRC. Folders use the
  special start/end method markers (32/33) → reconstruct the directory tree.
- **Compression methods** (the actual work — port from XADMaster):
  `0` none · `1` RLE90 · `2` LZW (12-bit) · `3` Huffman · `13` LZAH (LZ +
  dynamic Huffman) · `15` Arsenic (BWT + arithmetic). 80%+ of real files use
  0/1/13/15 — implement those first, add 2/3 as needed.

### Architecture

Multi-file → new module `src/macarchive/` (or `src/fs/sit.rs` if it stays small)
exposing each entry as a `FileEntry` carrying `resource_fork` + `type_code`/
`creator_code`. Browse via a **read-only view modeled on `src/optical/browse_view.rs`**
so "browse a `.sit` → select files → export to HQX/MacBinary" reuses existing
machinery. Auto-unwrap chain from §1 applies (a `.sit` of a DiskCopy image →
image pipeline).

### Steps

- [ ] Clone XADMaster reference; document codec pointers here (amigasources-style)
- [ ] `macarchive`: archive-header + entry-header parser, folder tree reconstruction
- [ ] Decompressors: method 0, 1 (RLE90 reuse), 13 (LZAH), 15 (Arsenic/BWT) — each unit-tested against known vectors
- [ ] Decompressors: method 2 (LZW), 3 (Huffman) — as needed
- [ ] Per-fork CRC verification on extract
- [ ] Bridge entries → `FileEntry`; read-only browse view (GUI) + `list`/`extract` (CLI)
- [ ] Tests against real `.sit` samples (collect a corpus first)

**Estimate: 1–2 weeks, dominated by codec porting + corpus testing.**

---

## 4. StuffIt classic (`.sit`) — write   [Phase 3]

User wants SIT emission too. Pragmatic path — **valid archives first, good
compression later**:

- [ ] Container writer: archive header + entry headers + folder markers + CRCs
- [ ] Method **0 (stored)** and **1 (RLE90)** writers — trivial, produce archives StuffIt Expander / unar accept
- [ ] CLI `put-sit` / `make-sit` (multi-file in) + GUI "Export selection to StuffIt…"
- [ ] Round-trip test: write SIT → read back with our Phase 2 reader → forks/Finder info intact
- [ ] Validate against The Unarchiver (`lsar`/`unar`) externally
- [ ] *(optional)* method 13 (LZAH) compressor for real space savings — defer unless needed

> Note: writing a *good* StuffIt compressor (method 13/15) is substantial and low
> value for preservation. Stored/RLE90 is correct and sufficient; treat real
> compression as a stretch goal.

---

## 5. Self-extracting archives (`.sea`) — read   [Phase 2, after SIT reader]

A StuffIt `.sea` is a classic Mac **application** whose **data fork is essentially
a `.sit` archive** (the resource fork holds the extractor engine we ignore). So:

- [ ] Detect SEA: file/MacBinary/AppleDouble wrapping an app whose data fork begins with a StuffIt archive signature
- [ ] Locate embedded archive offset → dispatch to the Phase 2 SIT reader
- [ ] Tests with a real StuffIt SEA sample

> Compact Pro SEAs use a different engine/layout — out of scope; detect and
> report "unsupported SEA variant" rather than mis-parsing.

---

## 6. GUI + CLI surfacing (parity)

Per CLAUDE.md, every user-facing op lands in both. Shared logic in core modules
(`src/fs/binhex.rs`, `src/macarchive/`), with `gui/` and `cli/` calling in — no
duplication.

- [ ] CLI verbs: `put-binhex`, `get-binhex`, `make-sit`, plus archive `list`/`extract` for `.sit`/`.sea` (naming reviewed against `docs/cli-todo.md`)
- [ ] File picker: add `.hqx`, `.sit`, `.sea` extensions
- [ ] GUI browse-view export options: "Export to BinHex / StuffIt"
- [ ] GUI: open `.hqx`/`.sit`/`.sea` → read-only archive browse (or auto-unwrap to image pipeline when payload is a disk image)
- [ ] ASCII-only UI strings (no Unicode glyphs)

---

## 7. Deferred / out of scope

- `[-]` **StuffIt X (`.sitx`)** — different container + arithmetic/Brimstone/BWT
  codecs; biggest lift, least common for floppy preservation. Revisit only on
  demand; XADMaster is the reference if/when we do.
- `[-]` **SIT method 13/15 *compressors*** — read-only for those; we write
  stored/RLE90.
- `[-]` **Compact Pro / DiskDoubler / other SEA engines** — detect-and-decline.

---

## Progress log

- 2026-06-02: Plan drafted. Scope set: BinHex r/w, SIT r/w, SEA read, SITX deferred; GUI+CLI together. Confirmed MacBinary/AppleDouble/DiskCopy-4.2/fork-extraction already exist and are the foundation.
