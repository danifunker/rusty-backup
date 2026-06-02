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
- [x] GUI export: `ResourceForkMode::BinHex` variant — "BinHex 4.0 (.hqx)" appears in the HFS/HFS+ resource-fork dropdown; extract writes a single `.hqx` per file (both forks + type/creator), with overwrite-detection candidate path
- [ ] GUI import: "Import .hqx" into a disk in edit mode (decode → create_file)
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

- [x] Clone XADMaster reference (at `~/repos/XADMaster`)
- [x] `src/macarchive/stuffit.rs`: archive-header + 112-byte entry-header parser, folder tree reconstruction, fork offsets, CRC-16/ARC header + fork verification
- [x] Decompressors: method 0 (none), 1 (RLE90, reused from binhex), **13 (LZ+Huffman)** — both dynamic (in-stream tables via metacode) and static (selectors 1–5; tables auto-generated into `src/macarchive/stuffit13_tables.rs` from XAD source). Ported from `XADStuffIt13Handle.m` + `XADLZSSHandle.m` + `XADPrefixCode.m`.
- [x] Per-fork CRC verification on extract (CRC-16/ARC)
- [x] `.sea` dispatch: `find_sea_archive` scans for the embedded `SIT!`/`rLau` stream
- [x] Tests against real `.sit` samples: **102 forks across 4 archives decompressed + CRC-verified** (decoded on the fly from `.sit.hqx`), exercising both method-13 sub-modes
- [ ] Decompressors: method 3 (Huffman), 2 (LZW/Compress), 15 (Arsenic/BWT) — return a clear "unsupported codec" error today
- [-] StuffIt 5 format (`SIT5` / "StuffIt (c)…") — newer container, out of scope (detected + skipped)
- [x] CLI: `sit list ARCHIVE` + `sit extract ARCHIVE DEST --format binhex|macbinary|appledouble|raw`. Transparently decodes BinHex-wrapped `.sit.hqx` and `.sea`; rebuilds the directory tree; preserves forks + type/creator/flags. Verified end-to-end on real archives (49-file MacWeb tree extracted; decompressed HTML + GIF content confirmed valid).
- [ ] GUI: read-only archive browse view (model on `optical/browse_view.rs`) + "extract selection to HQX" — next slice
- [ ] Bridge entries → `FileEntry` for the GUI browse view

**Status: classic `.sit`/`.sea` reading + extraction works end-to-end (CLI) for the dominant codecs (0/1/13). Remaining: codecs 3/2/15, and the GUI browse surface.**

---

## 4. StuffIt classic (`.sit`) — write   [Phase 3]

- [x] Container writer (`stuffit::build_archive`): archive header + 112-byte entry headers + correct **entry-header CRC** and **per-fork CRC-16/ARC** (the checksums real readers verify)
- [x] Method **0 (stored)** and **1 (RLE90)** writers (`WriteMethod`; RLE falls back to store when it doesn't shrink)
- [x] CLI `sit create OUT.sit INPUT… [--rle]` — inputs may be `.hqx` (full fidelity), `.bin` MacBinary, or plain files (+ `._`/`.rsrc` sidecars)
- [x] Round-trip tests: unit test (write → parse → fork CRCs verify) + full CLI workflow (`.hqx` → `sit create` → `sit extract` yields byte-identical data forks)
- [x] External validation against `lsar`/`unar`: our `.sit` is recognized as "StuffIt" and extracts with correct content; our `.sit.hqx` is recognized as "StuffIt in BinHex" and extracts correctly. Writer is interoperable (the offset-20 header field is accepted).
- [x] `.sit.hqx` export (sit-over-BinHex): `sit create OUT.sit.hqx …` wraps the archive in BinHex (name `*.sit`, type `SITD`, creator `SIT!`). The classic distribution format.
- [ ] Folder (nested directory) emission — writer is flat-file only today
- [-] method 13 compressor — out of scope (stored/RLE90 is sufficient for preservation)

> **Caveat (documented in code):** the 16-bit field at archive-header offset 20
> is of unknown derivation — XAD/The Unarchiver ignore it, and we write 0. The
> per-entry and per-fork CRCs (which unar *does* verify) are correct, so our
> archives round-trip through our own reader and should be accepted by unar.
> StuffIt Expander's stricter behavior on that header field is unverified.

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

## 6a. StuffIt 5 (`.sit` v5) — read   [done]

- [x] `src/macarchive/stuffit5.rs`: container parser (`XADStuffIt5Parser` port) producing the shared `StuffItEntry`/`ForkInfo`, so forks flow through `decompress_fork`. Handles directory tree (`diroffs` map), phantom marker entries, second-block file type/creator/finder flags, resource forks. Encrypted archives rejected.
- [x] `src/macarchive/stuffit_arsenic.rs`: **method 15 (Arsenic)** — MSB-first arithmetic decoder + adaptive models + inverse MTF + inverse BWT + de-randomization + 4-byte RLE, with trailing CRC-32 verification. Ported from `XADStuffItArsenicHandle.m` + `BWT.c`.
- [x] CLI `sit list`/`extract` route SIT5 automatically (alongside classic `SIT!`).
- [x] **Validated end-to-end against the real Apple Mac OS 7.1 800K set**: all 10 disk images decompress with CRC-32 verification and are **byte-for-byte identical to `unar`'s output**; extracted `.img` files are valid HFS volumes `rb-cli` reads.

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
- 2026-06-02: Shipped BinHex r/w (engine+CLI+GUI export), classic StuffIt read (methods 0/1/13, container+`.sea`), StuffIt write (stored/RLE90), CLI `sit list/extract/create`, and `.sit.hqx` export. Externally validated with `lsar`/`unar`. Discovered StuffIt 5 is common in the wild (Apple's MacOS 7.1 800k set) → promoted to top read follow-up.
- 2026-06-02: Shipped native **StuffIt 5** read + **Arsenic (method 15)** codec. The Apple Mac OS 7.1 800K `.sit.hqx` extracts entirely natively — all 10 disk images byte-identical to `unar`. Remaining codecs: 2 (LZW), 3 (Huffman), 5 (LZAH); plus StuffIt 13 static selectors are done. GUI archive browse still pending.
