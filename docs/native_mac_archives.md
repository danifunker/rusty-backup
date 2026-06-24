# Native Mac Archive Support — format reference & roadmap

This document is the self-contained reference and implementation plan for the
`src/macarchive/` family. It is written so the plan can be executed without
reading any third-party source tree (CiderPress II, The Unarchiver, etc.); the
relevant format details are embedded inline.

Status legend: **Done** (shipping), **Plan** (this document's roadmap), **Out of
scope** (noted boundary).

---

## 1. What `src/macarchive/` already does (Done)

rusty-backup has a working, multi-codec Mac archive decoder family. The pieces:

- **Shared data model** (`src/macarchive/stuffit.rs`): every parser, regardless
  of source format, returns a `StuffItArchive { entries: Vec<StuffItEntry> }`.
  - `StuffItEntry { path, name, is_dir, type_code, creator_code, finder_flags,
    create_date, mod_date, data: Option<ForkInfo>, rsrc: Option<ForkInfo> }`
  - `ForkInfo { method, codec: ForkCodec, encrypted, uncompressed_len,
    compressed_len, crc, crc32, offset }`
  - `ForkCodec { StuffIt, CompactPro { lzh: bool } }` — the tag that routes a
    fork to the right decompressor at extract time.
  - This triple is the **de-facto archive abstraction**. There is no `Archive`
    trait; new formats join the family by producing a `StuffItArchive`.

- **Magic classifier** (`src/macarchive/detect.rs`):
  `detect_mac_archive(bytes) -> Option<MacArchiveKind>`. Extension-independent,
  content-driven. Today's `MacArchiveKind` variants: `BinHexSingleFile`,
  `BinHexOverSit`, `BinHexOverSea`, `BinHexOverCompactPro`, `Sit`, `Sit5`,
  `Sea`, `CompactPro`, `Mar`. **There is no `MacBinary` variant yet — that is
  the gap this plan closes.**

- **Orchestrator** (`src/macarchive/extract.rs`):
  - `open(path)` / `open_bytes(raw) -> Result<(Vec<u8>, StuffItArchive)>` —
    the single entry point. It calls `detect_mac_archive`, then dispatches to
    the right parser, transparently peeling BinHex wrappers.
  - `synth_single_file_archive(bh)` — manufactures a one-entry `StuffItArchive`
    (method 0 / store) from a decoded single Mac file. **This is the exact
    pattern a MacBinary file will follow** — a MacBinary holds exactly one
    Mac file's two forks plus Finder info.
  - `extract_all` / `extract_filtered` — write entries to disk in a chosen
    `ForkFormat { BinHex, MacBinary, AppleDouble, Raw }`. Format-agnostic once
    a `StuffItArchive` exists.

- **Per-format decoders** (all real, no stubs):
  - StuffIt classic `SIT!` — `stuffit::parse`; codecs: store, RLE90, LZW,
    Huffman, LZAH, LZH, Arsenic (methods 0/1/2/3/5/13/15).
  - StuffIt 5 — `stuffit5::parse` (shares the StuffIt codecs).
  - SEA self-extracting — `stuffit::find_sea_archive` scans the app data fork
    for an embedded `SIT!` / `rLau`.
  - Compact Pro `.cpt` — `compactpro::parse`; RLE + optional LZH, CRC-32 checked.
  - MacArc `.mar` — `mar::parse` (stored read/write; LZW + LZ4 frames recognized
    but `bail!` as unsupported).
  - BinHex 4.0 `.hqx` — `src/fs/binhex.rs` (`parse_binhex`, `rle90_decode`).
  - StuffIt X `.sitx` — recognized (`extract::is_stuffitx`) but intentionally
    `bail!`s ("use `unar`"); Brimstone/PPMd-G codec not implemented.

- **GUI** (`src/gui/archives_tab.rs`, "Mac Archives" tab): Browse picker
  (filtered by `MAC_ARCHIVE_EXTS`), entry grid (name / type+creator / size /
  method), per-entry Extract checkboxes, Extract All / Extract Selected, and a
  "Mount in new Inspect tab" button driven by `detect_mountable_image` (sniffs a
  single decompressed payload for DiskCopy 4.2 / raw HFS / raw HFS+). Reachable
  directly or via redirect from the source picker (`is_mac_archive_path`).

- **CLI** (`rb-cli`):
  - `archive list|extract|create` (`src/cli/verbs/archive.rs`, alias `sit`).
  - `put-binhex` / `get-binhex` (`src/cli/verbs/binhex.rs`).
  - `put-macbinary` (`src/cli/verbs/put_macbinary.rs`) — writes a MacBinary file
    *into* an HFS volume. Note it carries its **own full-fidelity MacBinary
    parser** (`parse_macbinary_header` + `struct MacBinaryHeader`), separate
    from the leaner `resource_fork::parse_macbinary`.

- **Two existing MacBinary parsers** (the duplication this plan consolidates):
  - `src/fs/resource_fork.rs::parse_macbinary` — lean. Accepts MacBinary
    **II/III only** (version byte 129/130), **requires a valid CRC**, and
    **returns `None` when the resource fork is empty**. Extracts both forks +
    type/creator but drops filename, Finder flags, location, folder ID, dates.
  - `src/cli/verbs/put_macbinary.rs::parse_macbinary_header` — full fidelity
    (flags, location, folder, create/modify dates, MacBinary version) but
    MacBinary **I/II** focused and buried in a CLI verb.

---

## 2. MacBinary I / II / III format (embedded spec)

MacBinary is a **transport wrapper**: it serializes one Mac file's data fork +
resource fork + Finder metadata into a single flat stream. It is *not* a
multi-file archive — exactly one file per MacBinary stream. This is why it slots
into the family as a single-entry `StuffItArchive`.

### 2.1 Layout

```
[ 128-byte header ]
[ data fork      ]  padded with zeros up to a 128-byte boundary
[ resource fork  ]  padded with zeros up to a 128-byte boundary
[ optional "Get Info comment" ]  (MacBinary III era; rarely present)
```

All multi-byte integers are **big-endian**. Some writers omit the final padding,
so do not require the total length to be a 128-byte multiple.

### 2.2 Header (128 bytes)

| Offset | Size | Field | Notes |
|-------:|-----:|-------|-------|
| `$00` | 1 | version byte | **must be 0** |
| `$01` | 1 | filename length | 1–63 (II) / 1–31 (III); validate 1–63, then sanity-check bytes |
| `$02` | 63 | filename | Mac OS Roman; only `length` bytes significant; no colons |
| `$41` | 4 | fdType (file type) | 4 chars, or zero |
| `$45` | 4 | fdCreator (creator) | 4 chars, or zero |
| `$49` | 2 | fdFlags | high byte of Finder flags |
| `$4B` | 2 | fdLocationV | icon vertical position |
| `$4D` | 2 | fdLocationH | icon horizontal position |
| `$4F` | 2 | fdFldr | window/folder ID |
| `$51` | 1 | protected flag | 0 or 1 |
| `$52` | 1 | reserved | **must be 0** |
| `$53` | 4 | **data fork length** | bytes; range `0..=0x7FFFFF` |
| `$57` | 4 | **resource fork length** | bytes; range `0..=0x7FFFFF` |
| `$5B` | 4 | creation date | seconds since 1904-01-01, local time |
| `$5F` | 4 | modification date | seconds since 1904-01-01, local time |
| `$63` | 2 | (II) Get Info comment length | |
| `$65` | 1 | (II) low byte of Finder flags | bits 8–15 |
| `$66` | 4 | (III) signature `'mBIN'` | present only in III |
| `$6A` | 1 | (III) filename script (FXInfo `fdScript`) | |
| `$6B` | 1 | (III) extended Finder flags (FXInfo `fdXFlags`) | |
| `$6C` | 8 | reserved | **must be 0** |
| `$74` | 4 | (II) total unpacked length | compression hint, "never used" |
| `$78` | 2 | (II) secondary header length | skip this many bytes (128-rounded) after header if nonzero; effectively never used |
| `$7A` | 1 | (II) version of MacBinary the writer targets | `$81` = II, `$82` = III |
| `$7B` | 1 | (II) minimum version to read | `$81` |
| `$7C` | 2 | (II) **CRC-16/XMODEM of bytes `$00..$7B`** (first 124 bytes) | |
| `$7E` | 2 | computer/OS ID | must be 0; dropped in II |

### 2.3 Version discrimination (cheap when present)

- **MacBinary III** — byte `$7A == 0x82` **and** `'mBIN'` (`6D 42 49 4E`) at `$66`.
- **MacBinary II** — byte `$7A == 0x81` **and** the CRC-16/XMODEM over bytes
  `$00..=$7B` equals the stored value at `$7C`.
- **MacBinary I** — no signature, no CRC. Identify only by heuristics.

### 2.4 Detection algorithm (the genuinely hard part)

MacBinary I has no magic, and lazy II writers leave the CRC zeroed, so detection
is a **confidence-scored heuristic**, never a single magic check. Require *all*
of these as the floor:

1. byte `$00 == 0` and byte `$52 == 0` and the 8 reserved bytes at `$6C == 0`.
2. filename length at `$01` in `1..=63`, and those bytes are plausible
   (printable Mac OS Roman, i.e. values `>= 0x20` excluding `:`).
3. fork lengths at `$53` / `$57` each `<= 0x7FFFFF`, and **at least one nonzero**.
4. type and creator at `$41` / `$45` are each four ASCII-ish bytes or all-zero.
5. **file size consistency**: `128 + round128(dataLen) + round128(rsrcLen)`
   matches the actual file length, allowing for the known "last block unpadded"
   case (i.e. accept exact, or within one 128-byte block short).

Then **escalate confidence**:
- If `'mBIN'` at `$66` → certain (III).
- Else if CRC at `$7C` validates → certain (II).
- Else (I, or CRC-lazy II) → accept only when *all* floor checks pass **and** the
  file-size consistency check is exact. Be conservative: a false positive
  hijacks an unrelated `.bin` (see §4 on the `.bin` overload).

All four sample files in `Examples`/`TestData` (see §6) validate via the CRC
path, so the high-confidence route covers them.

### 2.5 CRC-16/XMODEM

Polynomial `0x1021`, initial value `0x0000`, no reflection, no final XOR,
computed over header bytes `$00..=$7B` (124 bytes), compared against the
big-endian word at `$7C`. (`resource_fork::macbinary_crc16` already implements
this; reuse it.)

### 2.6 Timestamps

Unsigned 32-bit seconds since 1904-01-01 00:00 local time (MFS/HFS epoch). Map to
the model's `create_date` / `mod_date` the same way the StuffIt parsers do.

---

## 3. Implementation plan — MacBinary as a first-class Mac archive (Plan)

Goal: a `.bin` MacBinary I/II/III file behaves exactly like every other member
of the family — it is detected by content, listed in the Archives tab, extracted
via `archive extract`, and (when it wraps a disk image or another archive)
offers the same "peel / mount" affordances as `.sit.hqx` does today.

### Phase 0 — Consolidate the MacBinary parser (prerequisite)

There are two parsers today (§1). Unify into **one full-fidelity parser** in a
new `src/macarchive/macbinary.rs`:

- `is_macbinary(bytes) -> Option<MacBinaryVersion>` — the §2.4 heuristic,
  returning which version was identified (and `None` otherwise).
- `parse(bytes) -> Result<MacBinaryFile>` where `MacBinaryFile` carries
  filename, type/creator, both forks, Finder flags (full 16-bit, combining
  `$49` high byte + `$65` low byte), location, folder, create/mod dates, version.
- Reuse `resource_fork::macbinary_crc16` for the CRC.
- Migrate `cli/verbs/put_macbinary.rs` and `resource_fork::parse_macbinary`'s
  call sites onto this one parser. Keep `resource_fork::build_macbinary`
  (the writer) as-is; optionally have it delegate field layout to the new module.
- **Behavioral fix carried by consolidation**: the new parser must accept an
  **empty resource fork** (data-fork-only Mac files — e.g. a wrapped disk image
  or a text file — are legitimate MacBinary). The old `resource_fork` parser
  returned `None` in that case, which would wrongly reject sample file #1.

### Phase 1 — Teach the classifier about MacBinary

In `src/macarchive/detect.rs`:

- Add `MacArchiveKind::MacBinary` (with `label() = "MacBinary"`,
  `is_multi_file() = false`, `is_binhex_wrapped() = false`).
- In `detect_mac_archive`, add a MacBinary probe. **Ordering matters**: run it
  *after* the StuffIt/CompactPro/MAR magic checks (those have strong magic and
  must win), and gate MacBinary behind the §2.4 heuristic so it never
  false-matches a raw disk image. Because BinHex is checked first and MacBinary
  is binary, there is no conflict with the BinHex branch.
- Consider `BinHexOverMacBinary`? **No** — BinHex already carries Finder info
  directly; a `.bin` inside `.hqx` is not a real-world shape. Skip it.

### Phase 2 — Route MacBinary through the orchestrator

In `src/macarchive/extract.rs::open_bytes`:

- Add a `Some(MacArchiveKind::MacBinary)` arm that calls
  `macbinary::parse(&raw)` and feeds the result to a `synth_single_file_archive`
  variant (generalize the existing helper, or add `synth_from_macbinary`). The
  single entry uses method 0 / store with the two forks laid out back-to-back,
  exactly like the BinHex single-file path. After this, **the entire
  Archives-tab and `archive list|extract` path works for `.bin` with no further
  changes** — they are format-agnostic over `StuffItArchive`.

### Phase 3 — Nested payload handling (peel / mount)

A MacBinary's *data fork* is frequently itself something we can go deeper on.
Mirror the existing BinHex-over-SIT and `detect_mountable_image` behavior:

- **MacBinary-over-archive**: after parsing, sniff the data fork with
  `detect_mac_archive`. If it is a SIT/SIT5/SEA/CPT, treat like
  `BinHexOverSit` — peel and present the *inner* archive's entries. (Sample
  file #4 is a MacBinary III wrapping a StuffIt 5 archive; this makes it expand
  to the StuffIt entries rather than a single opaque `.sit` blob.)
- **MacBinary-over-disk-image**: feed the data fork to
  `detect::detect_mountable_image`. If it returns DiskCopy 4.2 / raw HFS /
  raw HFS+, the Archives tab surfaces "Mount in new Inspect tab" exactly as it
  does for archive payloads today. (Sample file #1 is a MacBinary II wrapping an
  800K Mac disk image → mountable.)
- **MacBinary-over-application** (no inner archive, no inner disk image — e.g.
  the `.smi` self-mounting installer and the font self-extractor, sample files
  #2 and #3): no peel, no mount. The forks extract normally; the user gets the
  Mac application back. The embedded NDIF disk image inside a `.smi` is **out of
  scope** (see §5).

### Phase 4 — Pickers, routing, and the `.bin` overload (§4)

- Add MacBinary to the source-picker / inspect routing so a content-sniffed
  `.bin` MacBinary is redirected to the Archives tab (via the existing
  `is_mac_archive_path` → `pending_open_archive` mechanism) **only when
  `detect_mac_archive` confirms MacBinary**, not by extension alone.
- Do **not** blindly add `"bin"` to `MAC_ARCHIVE_EXTS` — see §4.

### Phase 5 — Docs, pickers, tests (CLAUDE.md pre-commit sync)

- **README.md**: add a MacBinary row to the Image/backup-formats table and note
  it in the Mac-archives coverage; mention I/II/III.
- **This file** (`docs/native_mac_archives.md`): flip MacBinary entries to Done.
- **`DISK_IMAGE_EXTS` / `MAC_ARCHIVE_EXTS`** (`src/model/file_types.rs`): see §4
  for the exact decision. Add the `*_family_present`-style regression test for
  whatever list changes.
- **`docs/cli-reference.md`**: no new verb (MacBinary flows through
  `archive list|extract`), but document that `.bin` MacBinary is an accepted
  input to those verbs.

---

## 4. The `.bin` overload problem (must-resolve design point)

`.bin` is heavily overloaded in this codebase: it is already in `DISK_IMAGE_EXTS`
and `OPTICAL_EXTS` (raw disk/optical images), and it is **not** in
`MAC_ARCHIVE_EXTS`. A raw `.bin` disk image and a MacBinary `.bin` are
indistinguishable by extension.

**Resolution: route by content, not extension.**

- Keep `.bin` in `DISK_IMAGE_EXTS` (raw images stay the default interpretation).
- When any `.bin` (or any file) is opened, run `detect_mac_archive` first. Only
  if it returns `MacArchiveKind::MacBinary` (high-confidence per §2.4) do we
  treat it as a Mac archive; otherwise it stays a raw disk/optical image.
- This is consistent with the module's existing philosophy: detection is
  "magic-driven rather than extension-driven" (`detect.rs` doc comment). The
  extension lists are only *picker filters*; the classifier is the source of
  truth.
- **Do not** add `"bin"` to `MAC_ARCHIVE_EXTS` purely to make the Archives-tab
  picker show `.bin` — that would also make every raw `.bin` disk image show up
  in the Mac-archive picker. Instead, broaden the Archives-tab Browse filter to
  include `.bin`/`.macbin` *and* keep the content sniff as the gate when a file
  is actually selected. Add a regression test asserting a raw (non-MacBinary)
  `.bin` is **not** classified as a Mac archive.

---

## 5. Scope boundaries (Out of scope for this plan)

- **NDIF / `.smi` self-mounting images**: sample file #3 (`CarbonLib 1.5.smi`) is
  a MacBinary wrapping a Mac *application* whose resource fork contains an NDIF
  (New Disk Image Format) compressed disk image. Unwrapping MacBinary yields the
  app; getting at the disk image requires an NDIF decoder, which we do not have.
  Treat `.smi` as "extract the app forks" only. NDIF support is a separate,
  larger effort (its own `rbformats`/`Disk` module).
- **StuffIt X `.sitx`**: already recognized-and-declined; unchanged.
- **MAR LZW/LZ4 compressed members**: already recognized-and-declined; unchanged.
- **MacBinary *writing*** beyond the existing `build_macbinary` (MacBinary III
  writer) and `ForkFormat::MacBinary` extract target: no new write features.

---

## 6. Test fixtures (the four sample files)

From `~/Downloads/ProblemFiles` (consider copying sanitized fixtures into
`TestData/macbinary/`). All four are MacBinary; CRC validated:

| File | MacBinary ver | Inner payload | Exercises |
|------|--------------:|---------------|-----------|
| `800K_MacSnap_.img_.bin` | II (CRC ok) | 800K Mac disk image (data fork) | Phase 3 mount path → `detect_mountable_image` |
| `Adobe_Screen_Fonts.bin` | II (CRC ok) | self-extracting font app | data+rsrc extract, no peel/mount |
| `CarbonLib_1.5.smi_.bin` | II (CRC ok) | `.smi` self-mounting app (NDIF inside) | data+rsrc extract; NDIF out of scope (§5) |
| `MacOSIM-2.0b1.sit_.bin` | III (`'mBIN'`) | StuffIt 5 archive | Phase 3 peel path → inner StuffIt 5 entries |

Test matrix to add:
1. `is_macbinary` accepts each sample and reports the right version.
2. `is_macbinary` rejects a raw HFS `.bin` disk image and random bytes (§4).
3. `parse` round-trips forks + type/creator/dates for a data-fork-only file
   (empty rsrc must succeed — the old-parser bug).
4. `open_bytes` on sample #4 yields the inner StuffIt 5 entry list, not a
   single opaque entry.
5. `detect_mountable_image` on sample #1's data fork returns a mountable kind.
6. `*_family_present`-style picker/association regression test for whatever
   extension list §4 settles on.

---

## 7. Primary format sources (for future maintainers)

- MacBinary I: "Macintosh Binary Transfer Format Standard Proposal", rev 3,
  1985-05-06.
- MacBinary II: "MacBinary II Standard", 1987-07-24.
- MacBinary III: "MacBinary III Standard", 1996-12 (adds `'mBIN'` at `$66`,
  version `$82`).
- CiderPress II format notes (cloned at `../CiderPress2`,
  `DiskArc/Arc/MacBinary-notes.md`) — the offset table in §2.2 matches it.
- StuffIt: reverse-engineered via The Unarchiver / XAD (no public spec).
- CRC: CRC-16/XMODEM (poly `0x1021`, init 0).
