# Output Formats Module (`src/rbformats/`)

Compress/decompress handlers for backup output formats, plus disk reconstruction for restore and VHD export.

## Architecture

- **`mod.rs`** — Orchestration functions (`compress_partition`, `decompress_to_writer`, `reconstruct_disk_from_backup`) and shared utilities (`SplitWriter`, `write_zeros`, `is_all_zeros`, `output_path`, `file_name`, `CHUNK_SIZE`).
- **`vhd.rs`** — VHD format: Fixed (`build_vhd_footer`, `export_whole_disk_vhd`, `export_partition_vhd`) and Dynamic/sparse (`DynamicVhdReader`, `export_whole_disk_vhd_dynamic`). Shared: `vhd_chs_geometry`, `vhd_checksum`, `VHD_COOKIE`.
- **`sparse.rs`** — Shared sparse-allocation bookkeeping (`SparseAllocator`, `is_zero_unit`) reused by dynamic VHD (and later QCOW2, VMDK sparse).
- **`qcow2.rs`** — QCOW2: `Qcow2Reader` (v2/v3, read + in-place allocate-on-write edit) and `export_qcow2` (writes v3 with `refcount_order = 4`, single-file, uncompressed clusters). Self-referential refcount-block sizing keeps `qemu-img check` clean; the editor grows L2 tables + refcount blocks on demand and reuses free host clusters before extending the file.
- **`vmdk.rs`** — VMDK flat: `VmdkFlatReader` (parses the ASCII descriptor, concatenates `FLAT`/`ZERO` extents from `monolithicFlat` and `twoGbMaxExtentFlat` layouts), `export_vmdk_flat` (writes a single-extent `monolithicFlat`: `<base>.vmdk` descriptor + `<base>-flat.vmdk` raw extent), and `open_flat_extent_for_edit` (resolves the single FLAT extent and returns it `R+W` for in-place edit; rejects multi-extent / ZERO / sparse layouts). Sparse VMDKs (`KDMV` magic) detected by the flat reader are rejected with a precise message.
- **`vmdk_sparse.rs`** — VMDK sparse (`monolithicSparse`): `VmdkSparseReader` parses the binary `KDMV` `SparseExtentHeader`, loads the grain directory into RAM, lazily reads grain tables with a single-entry LRU (mirrors `Qcow2Reader`'s L2 cache and `ChdReader`'s hunk cache), and serves `Read + Seek` over the virtual disk; unallocated grains read as zeros. Also implements `Read + Write + Seek` allocate-on-write — writes into an unallocated grain append a fresh grain at EOF, set the GT entry on disk + in RAM, bump `host_end`. `export_vmdk_sparse` streams a whole-disk source into a single self-contained `monolithicSparse` file: header @ sector 0, embedded descriptor, GD, GTs, grain data; non-zero grains allocated on demand via `SparseAllocator`, zero grains omitted. Compressed / marker (`streamOptimized`) variants are detected and rejected — they're not seekable.
- **`chd.rs`** — CHD (MAME) format via the in-process `libchdman-rs` crate: `compress_chd`, `compress_chd_dvd`, `ChdReader`, `CdCookedReader`.
- **`zstd.rs`** — Zstd streaming compression: `compress_zstd`.
- **`raw.rs`** — Raw streaming with optional file splitting and sparse zero-skipping: `stream_with_split`.
- **`imz.rs`** — WinImage IMZ (`.imz`) read-only convert-from. `materialize_imz_to_temp` unwraps the ZIP-wrapped floppy image (`.ima` / `.img`) into a fresh `tempfile::TempDir` and returns the temp path + guard. Password-protected entries (legacy ZipCrypto) are detected via `ZipFile::encrypted()` and rejected with a precise error. Wired through `gui::prepare_disk_image_path` so the GUI file-pick flow handles `.imz` uniformly with `.adz`/`.hdz`; downstream detection treats the materialized file as Raw.
- **`gho.rs`** — Norton Ghost (`.gho` / `.ghs`) read-only convert-from. `materialize_gho_to_temp` decodes **SECTOR-mode** (raw sector-by-sector) backups — including multi-file span sets — to a raw disk image in a tempdir (None / zlib / Fast-LZ compression), wired through `gui::prepare_disk_image_path` alongside `.imz`/`.adz`/`.hdz`. `discover_gho_span_set` + `SpanReader` handle three corpus naming patterns: stem-prefix `.GHS` siblings (with 8.3 truncation), hyphenated stem-prefix, and `.GHO.NNN` numeric suffix; the user can pick the primary OR any span sibling and the whole chain decodes as one continuous stream. The container header, record-stream parser, and Fast-LZ decoder are clean-room ports from the MIT-licensed `nyarime/gho`. **File-aware** ("truncated full backup") mode is supported via `GhoReader` (FAT record-stream + NTFS GHPR reconstruction). **Password-protected** backups decrypt transparently — pass a password to `GhoReader::open_with_password` / `materialize_gho_to_temp_with_password`; the cipher was reverse-engineered from `ghostexp.exe` (see `docs/gho_password.md`). File-aware and uncompressed-SECTOR images are encrypted and decrypt on read; compressed-SECTOR images are stored unencrypted by Ghost 11.5 (the password is still verified).

## Available Formats

| Format | Extension | Compression | Splitting | Notes |
|--------|-----------|-------------|-----------|-------|
| Raw    | `.raw`    | None        | Yes       | Supports sparse zero-skipping |
| VHD (Fixed)   | `.vhd` | None  | No        | Raw data + 512-byte footer |
| VHD (Dynamic) | `.vhd` | None  | No        | Sparse: BAT + per-block bitmap; all-zero blocks omitted |
| QCOW2         | `.qcow2` | None | No       | Read (v2 + v3) + write (v3 uncompressed). Sparse: zero clusters omitted. `qemu-img check` clean. |
| VMDK (Flat)   | `.vmdk` | None | No       | Read (`monolithicFlat` / `twoGbMaxExtentFlat`, FLAT + ZERO extents). Write: `monolithicFlat` — descriptor + sibling `-flat.vmdk` raw extent. |
| VMDK (Sparse) | `.vmdk` | None | No       | Read + write + in-place edit (`monolithicSparse`, `KDMV` magic). Two-level grain map; zero grains omitted on write, allocate-on-write for edits. |
| WinImage IMZ  | `.imz` | ZIP/Deflate | No  | Read-only convert-from (decode-to-temp). Password-protected archives rejected with a clear error. |
| Norton Ghost  | `.gho` / `.ghs` | None / zlib / Fast-LZ | Yes (auto-discover) | Read-only convert-from (decode-to-temp). SECTOR-mode (raw) only; file-aware reconstruction + password not yet supported. Multi-file span sets auto-discovered + concatenated. |
| Zstd   | `.zst`    | Zstd level 3 | Yes (post-hoc) | Good compression ratio |
| CHD    | `.chd`    | MAME CHD    | Yes (post-hoc) | Native via `libchdman-rs` (no external tool) |

## DMG (UDIF) reader — big-endian caveat

`dmg.rs` is a read-only source-container reader (not a backup output). It
decodes UDIF blocks of type raw/UDRW, zlib (UDZO), bzip2 (UDBZ), ADC (UDCO),
LZFSE (ULFO), and LZMA/xz (ULMO) into a `Read + Seek` virtual disk.

The two newest codecs differ in big-endian safety, which matters only if
rusty-backup is ever *run on* a big-endian host (e.g. a PowerPC G5 on Debian's
`powerpc`/`ppc64` big-endian port — not the little-endian `ppc64el`). The
decode always runs on the machine executing rusty-backup, never on the vintage
box being imaged.

- **ULMO / LZMA (`lzma-rs`)** — big-endian safe. The crate declares
  `#![forbid(unsafe_code)]` and reads every field through explicit
  `byteorder` little/big tags, so decode is correct on any host endianness.
- **ULFO / LZFSE (`lzfse_rust`)** — big-endian **unverified**. The crate uses
  `unsafe` native-endian reads (`read_unaligned::<u32>()`,
  `usize::from_ne_bytes`) whose big-endian correctness is unproven and almost
  certainly untested upstream; ULFO decode could silently produce wrong bytes
  there. Verified only on little-endian (x86 / arm64 / ppc64el).

Both crates are left ungated today (every CI target is little-endian). If we
target a big-endian host, gate `lzfse_rust` behind a Cargo feature (so a
big-endian build can drop just that crate while keeping ULMO and the other DMG
codecs) and round-trip-test ULFO on that host against an `hdiutil`-produced
fixture before trusting it.

## Other Apple source-container readers

These are read-only source-container readers (like `dmg.rs`), not backup
outputs. They decode a wrapped image into a `Read + Seek` flat disk that flows
through the normal partition / filesystem detection.

- **`dmg_crypto.rs` — encrypted disk images (`encrcdsa` v2).** AES-128/256
  per-image encryption (Apple "FileVault 1", 10.5+ and still produced by
  `hdiutil create -encryption`). Passphrase → `PBKDF2-HMAC-SHA1` → CBC unwrap of
  the volume-key blob (AES-192 on modern macOS, 3DES on older images; strip
  PKCS#7 + `CKIE\0`) → per-512-byte-block `AES-CBC` with an `HMAC-SHA1(be32(n))`
  IV. Verified end-to-end (payload round-trip) against `hdiutil` AES-128 and
  AES-256 images. Wired into the password-carrying source path
  (`source_reader::open_peeled_read_with_entry`); password-less paths get a
  clear "provide the password" error instead of a cryptic MBR failure.

  > **Note — vintage relevance.** Per-image encryption is uncommon in classic
  > Mac preservation (it's a modern security feature; its heaviest real use is
  > iOS firmware and personal vaults). It's supported here so an old encrypted
  > `.dmg` a user still holds the passphrase for can be opened offline and
  > cross-machine, with no Apple ID / Secure-Enclave dependency. The 3DES
  > key-wrap path (the truly old images) is wired but has **not** been verified
  > against a real vintage sample — modern `hdiutil` wraps with AES-192 even for
  > AES-128 volumes, so we have no 3DES fixture yet.

- **`sparseimage.rs` — Apple sparse images (`.sparseimage`, UDSP).** A growable
  single file (Mac OS X 10.3+) storing only written 1 MiB bands via a `sprs`
  band map; unallocated bands read as zeros. Verified byte-exact against
  `hdiutil` images. Handles up to ~1 GiB of *written* data (the in-header band
  map addresses ≤ 1008 physical bands); larger images use a band-index
  continuation scheme that returns a clear error rather than a misread.

- **`ndif.rs` — NDIF / self-mounting images (`.smi`).** The pre-UDIF Disk Copy 6
  format (Mac OS 8/9): data-fork chunks described by a `bcem` block map in the
  resource fork. Delivered through a fork carrier (MacBinary `.smi.bin`,
  AppleDouble, or a native resource fork); `model/source_reader` decodes the
  carrier and reconstructs the flat image (gated on `bcem`, so a plain `.bin`
  raw image is never touched). Zero / raw / ADC chunks are decoded (verified
  against a real Disk First Aid 8.6.1 SMI); the RLE (0x81), LZH (0x82), KenCode
  (0x80), and StuffIt (0xF0) chunk codecs are recognized and reported with a
  clear per-codec error — they await real sample images that use them (KenCode
  is undocumented and needs reverse-engineering).

## How to Add a New Output Format

1. Create `src/rbformats/myformat.rs` with a compression function:
   ```rust
   pub(crate) fn compress_myformat(
       reader: &mut impl Read,
       output_base: &Path,
       split_size: Option<u64>,
       progress_cb: &mut impl FnMut(u64),
       cancel_check: &impl Fn() -> bool,
   ) -> Result<Vec<String>>
   ```

2. Add a decompression path in `decompress_to_writer()` in `mod.rs` (match on `compression_type` string from metadata).

3. Add a variant to the `CompressionType` enum in `backup/mod.rs` with `as_str()` and `file_extension()` methods.

4. Add routing case in `compress_partition()` in `mod.rs`.

5. Add `pub mod myformat;` to `rbformats/mod.rs`.

## Conventions

- All compression functions return `Vec<String>` of output filenames (relative to the backup folder).
- All functions accept `progress_cb` (called with bytes read from source) and `cancel_check` (returns `true` to abort).
- Shared utilities: `SplitWriter` for file splitting, `write_zeros` for gap filling, `output_path` for consistent naming, `CHUNK_SIZE` (256 KB) for I/O buffers.
