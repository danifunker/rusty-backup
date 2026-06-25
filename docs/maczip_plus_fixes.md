# MacZip support + two Mac-fidelity fixes — execution plan

A short detour bundling three user-facing items, all landing on the current
working branch (no branch switch). Execution order is **A → B → C** (lowest-risk
and most independent first; the two `macarchive`-family tasks back-to-back so the
shared files — `detect.rs`, `extract.rs`, `model/file_types.rs`, README, MiSTer
status — are edited in one convenient sweep).

Source of truth confirmed during planning:
- **MAC3 (`0x334D`) extra-field layout** verified against the MacZip *source*
  (`zip/macos/source/extrafld.c` writer + `unzip/macos/source/macos.c` reader)
  in `~/MacZipSrc106b1.zip`, and byte-checked against a real central-dir sample.
- `~/MacZipSrc106b1.zip` is itself a **MacZip-format archive** (991 `0x334D`
  fields / 993 entries, `XtraStuf.mac/` resource-fork dirs) — a real oracle for
  the Task C reader (diff against system `unzip`).
- `zip` v8 (deflate + aes) and `flate2` are already dependencies.

Gates per task: `cargo build --all-targets` (zero warnings), `cargo test --lib`,
`cargo clippy`, `cargo fmt`. Per-commit doc sync per CLAUDE.md.

---

## Task A — Fix: Finder flags (`fdFlags`) dropped on copy / `get-binhex`

Ref: `docs/bug_binhex_finder_flags.md`. Root cause: `FileEntry` has no
Finder-flags field, so `get-binhex` (and `cp`) hard-code `flags: 0`.

- [x] **A1.** `src/fs/entry.rs`: add `pub finder_flags: Option<u16>`
      (doc-comment: HFS/HFS+ `FInfo.fdFlags`; `None` on non-Mac FS).
- [x] **A2.** Set `finder_flags: None` in the 5 `FileEntry` constructors
      (`root`, `new_directory`, `new_file`, `new_symlink`, `new_special`).
- [x] **A3.** Add `finder_flags: None` to every other explicit `FileEntry { … }`
      literal (~39 sites across `src/fs/*`, tests included) so it compiles.
- [x] **A4.** Populate the real value where Mac catalogs build entries:
      `hfs.rs` (already parses `finder_flags` at the catalog site — thread it on),
      `hfsplus.rs` (userInfo `fdFlags`), `mfs.rs` (`finder_info[8..10]`).
- [x] **A5.** `src/cli/verbs/binhex.rs` `get-binhex`: `flags:
      entry.finder_flags.unwrap_or(0)` (preserve high byte; no full-word zero).
- [x] **A6.** Copy engine (`src/fs/copy.rs` / `cp`): carry `src.finder_flags`
      into the destination Finder info via `set_finder_info`, like
      `put-macbinary`. Honour `AttrPolicy::Skip`.
- [x] **A7.** Tests: catalog read yields `Some(0x2000)`; `get-binhex` header
      flags `= 0x2000`; `get-binhex`→`put-binhex` round-trip preserves the high
      byte; disk-to-disk `cp` preserves flags.

## Task B — MacBinary as a first-class Mac archive

Ref: `docs/native_mac_archives.md` (Phases 0–5). Self-contained spec.

- [x] **B0.** New `src/macarchive/macbinary.rs`: one full-fidelity parser.
      `is_macbinary() -> Option<MacBinaryVersion>` (the §2.4 confidence
      heuristic); `parse() -> MacBinaryFile` (filename, type/creator, both forks,
      full 16-bit Finder flags = `$49` hi + `$65` lo, location, folder,
      create/mod dates, version). Reuse `resource_fork::macbinary_crc16`.
      **Accept an empty resource fork** (fixes the old reject bug).
- [x] **B0b.** Migrate `cli/verbs/put_macbinary.rs::parse_macbinary_header` and
      `resource_fork::parse_macbinary` call sites onto the new parser; keep the
      `build_macbinary` writer.
- [x] **B1.** `detect.rs`: add `MacArchiveKind::MacBinary` (`label`/`is_multi_file
      = false`/`is_binhex_wrapped = false`); probe **after** strong-magic formats,
      gated by the heuristic.
- [x] **B2.** `extract.rs`: `open_bytes` arm → `synth_from_macbinary`
      (single-entry, method 0/store).
- [x] **B3.** Nested payloads: peel when the data fork is a SIT/SIT5/SEA/CPT;
      surface "Mount" when `detect_mountable_image` matches; plain app otherwise
      (NDIF `.smi` stays out of scope).
- [x] **B4.** `.bin` overload: route by content, not extension. Broaden the
      Archives-tab Browse filter to include `.bin`/`.macbin`; do **not** add
      `bin` to `MAC_ARCHIVE_EXTS`. Source-picker/inspect redirect only when
      `detect_mac_archive` confirms MacBinary.
- [x] **B5.** Docs/tests: flip `native_mac_archives.md` MacBinary entries to
      Done; README image-formats + Mac-archive coverage; MiSTer status if
      affected. Tests per §6 matrix (use `~/Downloads/ProblemFiles` fixtures if
      present, else synthesize; include a "raw `.bin` is not MacBinary"
      regression).

## Task C — Native MacZip support

New format joining the `macarchive` family, mirroring Task B's integration.

- [ ] **C1.** New `src/macarchive/maczip.rs`:
  - [ ] Walk the ZIP central directory + local headers (via `zip` v8; `flate2`
        for inflate).
  - [ ] Pair each data-fork entry with its resource-fork entry, identified by a
        `XtraStuf.mac/` path component (`ResourceMark = "XtraStuf.mac:"`); cross-
        check with the MAC3 `EB_M3_FL_DATFRK` (0x01) info bit.
  - [ ] Parse the **MAC3 (`0x334D`)** extra field: type/creator from the
        always-uncompressed 18-byte header; Finder flags + dates + comment from
        the local-header blob — `memextract` = `[method u16][crc u32]` + raw
        deflate, or raw bytes when `EB_M3_FL_UNCMPR` (0x04) is set.
  - [ ] Best-effort JLEE (`0x07c8`) fallback for older Info-ZIP Mac archives
        (type/creator/flags only).
  - [ ] Emit a `StuffItArchive` (method 0/store; forks laid back-to-back), so
        the whole extract/browse path is reused unchanged.
- [ ] **C2.** `detect.rs`: `MacArchiveKind::MacZip` = ZIP magic **and**
      (a `XtraStuf.mac/` entry **or** a `0x334D` extra field) — distinguishes a
      MacZip archive from a plain disk-image-in-zip.
- [ ] **C3.** `extract.rs`: `open_bytes` arm.
- [ ] **C4.** `.zip` overload (like `.bin`): raw `.zip` stays a disk image;
      content-sniff routes MacZip archives to the Archives tab. Broaden the
      Archives picker with a content gate; keep `zip` picker-only / non-associated.
- [ ] **C5.** Validation: extract `MacZipSrc106b1.zip` with the new reader and
      diff forks + type/creator against system `unzip` (993-entry oracle). Unit
      tests + a "plain disk-image zip is not MacZip" regression.
- [ ] **C6.** Docs: README formats/filesystems/Mac-archive coverage; MiSTer
      status; a MacZip note in `native_mac_archives.md`; `MAC_ARCHIVE_EXTS` /
      picker regression test.

---

## Notes
- One branch (current), no switching — this is a detour bundled together.
- Commit each task separately; confirm before committing unless told to just go.
</content>
</invoke>
