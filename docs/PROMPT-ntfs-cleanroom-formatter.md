# Work prompt: clean-room NTFS formatter with dynamic geometry

> Paste everything below the line into a fresh Claude Code session running in the
> `rusty-backup` repo on the other system. It is self-contained.

---

## Mission

Replace the current template-blob NTFS formatter with a **clean-room
reimplementation of `mkntfs`-equivalent formatting** that produces correct,
mountable NTFS volumes for **arbitrary, caller-chosen geometry** — variable
sector size and variable cluster size — instead of the single hardcoded
512 B-sector / 4096 B-cluster / 1024 B-MFT-record shape we ship today. Users
must be able to (a) pick a cluster size explicitly, (b) get a sensible default
derived from volume size, and (c) inherit geometry from the source volume or the
destination physical drive.

The end state removes the `src/fs/ntfs_template/*.bin` blobs (or reduces them to
documented, geometry-independent *data tables* with clear provenance — see
§"Template blobs"). No raw bytes lifted from a `mkntfs`-produced image as the
formatter's backbone.

This is a correctness-critical filesystem writer. **Do not declare it done
without the automated cross-validation harness in §"Validation" passing across
the full geometry matrix.** The whole point of this rewrite is that the previous
approach had exactly one 32 MB test and no automated `ntfsfix`/`ntfs-3g` check.

## Read first

1. `CONTRIBUTING.md` (coding style — mandatory before touching code).
2. `CLAUDE.md` — note the **GUI/CLI feature parity** rule, the **shared
   business logic** rule, the **no-Unicode-glyphs** rule, and the **pre-commit
   documentation sync** checklist (README formats/filesystems tables, etc.).
3. The files you are changing or depending on:
   - `src/fs/ntfs_format.rs` — the formatter being rewritten (~566 lines today).
   - `src/fs/ntfs_clone.rs` — the only caller of `create_blank_ntfs`
     (`ntfs_clone.rs:168`); `stream_defragmented_ntfs` derives `target_size` and
     `mft_records` from the source.
   - `src/fs/ntfs.rs` — the **reader/editor**. It already parses variable
     geometry: `bytes_per_sector`, `sectors_per_cluster`, and the signed
     `mft_record_size` encoding (`ntfs.rs:61-92`), and `apply_fixup` is
     parameterised by `bytes_per_sector` (`ntfs.rs:337`). Treat the reader as the
     reference for what a correct on-disk layout looks like.
   - `src/fs/mod.rs` — NTFS dispatch / `EditableFilesystem` routing.
   - `examples/make_blank_ntfs.rs`, `examples/ntfs_clone_to_file.rs` — example
     callers to update.

## Current state (what's wrong)

`ntfs_format.rs` hardcodes:

```rust
const BPS: u64 = 512;       // bytes per sector
const SPC: u64 = 8;         // sectors per cluster
const CLUSTER: u64 = 4096;  // cluster size
const REC: usize = 1024;    // MFT record size
```

and pulls seven `include_bytes!` blobs from `src/fs/ntfs_template/` that were
extracted from `mkntfs -F -s 512 -c 4096`:

| Blob | Bytes | Geometry-dependent? |
|------|-------|---------------------|
| `boot.bin` | 8192 | **Yes** — BPB fields + sized in sectors |
| `mft_records.bin` | 16384 | **Yes** — record size, runlists, fixups |
| `root_index.bin` | 4096 | **Yes** — INDX record size, allocation runs |
| `attrdef.bin` | 2560 | No — fixed Microsoft `$AttrDef` table (data) |
| `upcase.bin` | 131072 | No — Unicode uppercase table (data) |
| `secure_sds.bin` | 262396 | No — standard `$Secure:$SDS` descriptors (data) |
| `root_secdesc.bin` | 4140 | No — root dir security descriptor (data) |

The geometry-dependent blobs are the unsafe shortcut: changing cluster/sector
size invalidates the embedded runlists, BPB, record sizes, and fixup spacing.
Only the bottom four are genuinely geometry-independent.

## License discipline — this is a clean room, and it matters

`mkntfs` is part of **ntfsprogs/ntfs-3g, licensed GPL-2**. This project is
**AGPL-3.0**. GPL-2-only code is **not** license-compatible with AGPL-3 —
copying or line-by-line translating `mkntfs.c` into this repo would be a license
violation. So:

- **Do not read `mkntfs.c` / `ntfs-3g` source and translate it.** Work from:
  - The public NTFS on-disk format documentation — "NTFS Documentation" by
    Richard Russon & Yuval Fledel (the Linux-NTFS project doc, a.k.a. the
    flatcap.org / linux-ntfs.org NTFS docs), which is a spec, not GPL code.
    Reference offsets for the BPB, `FILE` record header, attribute headers,
    runlist (mapping-pairs) encoding, `$STANDARD_INFORMATION`, `$FILE_NAME`,
    `$DATA`, `$BITMAP`, `$INDEX_ROOT`/`$INDEX_ALLOCATION`, `$SECURITY_DESCRIPTOR`.
  - Microsoft's published NTFS boot-sector / BPB field documentation.
  - **Black-box observation** of `mkntfs` *output*: run `mkntfs` with various
    `-s`/`-c` values inside the oracle container, dump structures with
    `ntfsinfo`/`hexdump`, and match the on-disk *result*. Observing output is
    not deriving from source.
- Keep a short `docs/ntfs_format_cleanroom_notes.md` recording which spec
  sections / observed behaviours each structure was derived from, so the
  clean-room provenance is auditable. Tables of facts ($UpCase mappings,
  $AttrDef definitions) are data, not copyrightable code, but document where
  each came from anyway.

## Geometry model

Introduce an explicit geometry type, e.g.:

```rust
pub struct NtfsGeometry {
    pub bytes_per_sector: u32,   // 512 or 4096 (power of two, 512..=4096)
    pub sectors_per_cluster: u32,// power of two; cluster_size = bps * spc
    pub mft_record_size: u32,    // default 1024 (independent of cluster size)
    pub index_record_size: u32,  // default 4096
}
```

with constructors:

1. `NtfsGeometry::for_volume_size(bytes)` — replicate the documented `mkntfs`
   default cluster-size-by-size table (cite the values as facts in the notes
   doc), e.g. ≤512 MB→512, ≤1 GB→1024, ≤2 GB→2048, >2 GB→4096 (and the modern
   "4096 up to 16 TB, larger clusters beyond" behaviour — confirm exact
   thresholds against `mkntfs` output and document them).
2. `NtfsGeometry::with_cluster_size(cluster, sector)` — explicit, validated.
3. From a drive / source: sector size from the physical device geometry (the
   `src/os/` layer exposes logical sector size; wire it through), cluster size
   either matched to a source volume or chosen.

**Validation rules** (return `FilesystemError::InvalidData` on violation):
- `bytes_per_sector` ∈ {512, 1024, 2048, 4096}, power of two.
- `cluster_size = bytes_per_sector * sectors_per_cluster`, power of two,
  `>= bytes_per_sector`, and within NTFS limits (512 B … 2 MB).
- `mft_record_size` / `index_record_size` power-of-two multiples of sector size.

## Geometry-dependent encodings to get right (the hard parts)

These are exactly the things the blob approach hid. Verify each against
`mkntfs` output at multiple geometries:

1. **BPB (`$Boot`, offset 0x0B+):** bytes-per-sector @0x0B, sectors-per-cluster
   @0x0D, total-sectors @0x28, `$MFT` LCN @0x30, `$MFTMirr` LCN @0x38,
   clusters-per-MFT-record @0x40, clusters-per-index-record @0x44.
2. **The signed power-of-two encoding** for fields 0x40 and 0x44: when the
   structure (record/index) is **larger** than a cluster, the byte is the
   positive cluster count; when **smaller** than a cluster, it's the *signed
   negative* `-log2(size_in_bytes)` (the reader already decodes this at
   `ntfs.rs:88-92` — match it). For >64 KB **clusters**, sectors-per-cluster
   @0x0D itself uses the same negative-power encoding; verify against `mkntfs`.
3. **Fixup / update-sequence arrays:** one fixup entry per **sector**, so the
   USA length and the per-sector tail patching depend on `bytes_per_sector`
   (today's `fixup_write` hardcodes 512). Each multi-sector structure (`FILE`
   records, `INDX` records) carries `ceil(struct_size / sector_size) + 1` USN
   words.
4. **Runlists (mapping pairs):** lengths and LCNs change with cluster size; the
   existing `enc_run` encoding logic is geometry-independent but feed it the
   right cluster-relative values.
5. **Cluster allocation bitmap (`$Bitmap`):** size = `ceil(total_clusters/8)`;
   mark metadata clusters + tail-padding bits past volume end.
6. **`$MFTMirr`:** mirror of records 0–3, placed at its own LCN.
7. **`$MFT` zone:** decide whether to replicate `mkntfs`'s reserved MFT-growth
   zone (default ~12.5% of volume). For a clone *target* it's optional, but for
   a general-purpose formatter document the choice.
8. **INDX records for directories:** root `$INDEX_ROOT` + `$INDEX_ALLOCATION`
   sized to `index_record_size`, not a fixed 4096 blob.

## Template blobs — fate

- **Synthesize** `$Boot`, the 16 system MFT records, and the root INDX
  parametrically from the geometry. Delete `boot.bin`, `mft_records.bin`,
  `root_index.bin`.
- For `$UpCase`, `$AttrDef`, the standard `$Secure:$SDS` and root security
  descriptor: these are geometry-**independent** data tables. Prefer
  **generating** them (e.g. `$UpCase` from the documented Unicode uppercase
  algorithm; `$AttrDef` from the fixed published table) so the blob dependency
  is gone entirely. If generation is impractical for a table, you may retain it
  as a `const` byte array with provenance documented in the notes file — but it
  must be cluster/sector-size independent and never the structural backbone.

## API / caller migration

- Replace `create_blank_ntfs(target, size, mft_records, label)` with a
  geometry-aware entry point, e.g.
  `create_ntfs(target: &mut W, params: &NtfsFormatParams)` where
  `NtfsFormatParams { total_size, geometry: NtfsGeometry, mft_records_hint, label }`.
  Keep a thin back-compat shim if it reduces churn, but update the real callers.
- Update `ntfs_clone.rs::stream_defragmented_ntfs` (`ntfs_clone.rs:144-187`) to
  choose target geometry: by default inherit the **source** volume's
  cluster/sector size (so a defrag clone preserves geometry), with an override.
  `mft_record_capacity()` / `volume_label()` already supply the other inputs.
- Update `examples/make_blank_ntfs.rs` and `examples/ntfs_clone_to_file.rs`.
- **Audit the write path** in `ntfs.rs` (`create_file`, `create_directory`,
  `sync_metadata`, runlist/MFT allocation) for any hidden assumption of
  4096-byte clusters or 1024-byte records. Reads already use `self.cluster_size`
  / `self.mft_record_size`; confirm writes do too. Fix any that don't.

## User-facing surface (GUI/CLI parity — required by CLAUDE.md)

- **CLI** (`rb-cli`): add a cluster-size option to the NTFS creation/format and
  defragment/clone paths, plus `--sector-size`, with an `auto` default
  (volume-size-derived) and an `inherit` mode (from source/drive). Check
  `rb-cli --help` and `docs/cli-reference.md` for naming conventions before
  choosing flag names; mirror any existing `--cluster-size`-style flags.
- **GUI**: a cluster-size selector (dropdown: Auto, 512, 1 K, 2 K, 4 K, 8 K,
  16 K, 32 K, 64 K, …) in the NTFS-relevant dialogs (Defragment / Expand / New).
  ASCII labels only.
- **Inherit-from-drive**: when the destination is a physical device, query its
  logical sector size via the `src/os/` layer and feed it into the geometry.
- Put geometry-selection logic in a **shared core helper** both GUI and CLI call
  — do not duplicate.

## Validation harness (the non-negotiable deliverable)

Add automated tests (and CI wiring) that prove correctness across geometry. Use
the existing NTFS Docker oracle if present (memory references `rb-ntfs-oracle`;
search the repo / `scripts/` / `.github/` and `docker`-related files for it; if
it doesn't exist, create a minimal container with `ntfs-3g` + `ntfsprogs`).

Matrix to cover:
- **sector size** ∈ {512, 4096}
- **cluster size** ∈ {512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}
  (and at least one >64 KB case to exercise the negative-power encoding)
- **volume size** ∈ {a few MB, ~100 MB, ~1 GB, and at least one deliberately
  **non-round / odd** size that is cluster-aligned but not a power of two}

For each cell:
1. Format the volume with the new formatter.
2. **Cross-check with real NTFS tooling**: `ntfsfix -n` (read-only check) and/or
   mount under `ntfs-3g` and run `ntfsinfo` — fail the test on any reported
   inconsistency. This is the check that was missing before.
3. **Field-diff vs `mkntfs`**: format an equivalent volume with `mkntfs` at the
   same `-s`/`-c`, and compare the geometry-dependent BPB/record fields (not a
   raw byte-diff — layouts legitimately differ — but assert our derived fields
   match `mkntfs`'s intent).
4. **Round-trip**: with our own reader, create resident + multi-cluster
   non-resident files + nested directories, `sync_metadata`, reopen, and verify
   byte-exact contents. Then reopen the *same* image under `ntfs-3g` and verify
   the files are readable there too.
5. The existing `stream_defragmented_ntfs_round_trips` test must still pass and
   should be extended to run at >1 geometry.

If any geometry can't be supported yet, **`log`/document the limit explicitly**
(per CLAUDE.md "no silent caps") rather than silently formatting wrong.

## Documentation sync (CLAUDE.md pre-commit checklist)

- `README.md` — if NTFS gains user-visible options (cluster size), reflect it in
  the Filesystems table notes.
- `docs/cli-reference.md` — document the new flags.
- `docs/ntfs_format_cleanroom_notes.md` — new; clean-room provenance + the
  cluster-size default table + supported/unsupported geometry.
- If a new picker-visible extension is introduced (probably none), update
  `DISK_IMAGE_EXTS` in `src/model/file_types.rs` + its regression test.

## Acceptance criteria

- [ ] `ntfs_template/{boot,mft_records,root_index}.bin` deleted; formatter
      synthesizes them from geometry. Remaining blobs (if any) are
      geometry-independent data tables with documented provenance.
- [ ] `NtfsGeometry` + validated constructors (explicit, volume-derived,
      drive-inherited).
- [ ] Variable sector size **and** variable cluster size produce volumes that
      pass `ntfsfix -n` / mount clean under `ntfs-3g` across the full matrix.
- [ ] Our reader and `ntfs-3g` both round-trip files written into formatted
      volumes at every tested geometry.
- [ ] CLI + GUI expose cluster-size selection (auto / explicit / inherit) via a
      shared core helper; ASCII-only UI text.
- [ ] `cargo fmt --check`, `cargo clippy`, `cargo test` all green; validation
      harness wired into CI.
- [ ] Clean-room notes doc + CLAUDE.md doc-sync checklist completed.
- [ ] No GPL-2 `mkntfs`/`ntfs-3g` source copied or translated.

## Suggested order of work

1. Stand up the oracle container + a failing matrix harness against the *current*
   formatter (establishes the baseline and the diffing tooling).
2. `NtfsGeometry` + parametric `$Boot`/BPB synthesis; get 4096/512 byte-equal to
   the old path (proves the rewrite is faithful before generalizing).
3. Parametric MFT records + runlists + bitmap + fixups for arbitrary geometry.
4. Generate/relocate the data tables; delete structural blobs.
5. Caller migration + write-path audit.
6. CLI/GUI surface + shared helper.
7. Full matrix green + docs.
```
