# Apple II Floppy Disk Image Format Support

Add read support for common Apple II / Apple IIgs floppy disk image formats,
enabling ProDOS filesystem browsing, backup, and compaction from these containers.

---

## Background

Rusty Backup already supports ProDOS filesystem browsing/compaction/editing and
the 2MG container format. However, the Apple II ecosystem has several other
common disk image formats that users may have. This plan adds support for the
most important ones.

### Formats by Priority

| Format | Magic/Detection | Complexity | Notes |
|--------|----------------|------------|-------|
| **.po** | Raw, 140KB/800KB | Trivial | ProDOS-order sectors, no header |
| **.do/.dsk** | Raw, 140KB | Trivial | DOS 3.3-order, needs sector interleave |
| **.dc42** | `0x0084` + name | Easy | DiskCopy 4.2, header + checksums |
| **.woz** | `WOZ1`/`WOZ2` | Medium-hard | Nibble/flux-level, needs denibblization |
| **.2mg** | `2IMG` | **Already done** | — |

### Architecture Fit

The existing `ImageFormat` enum + `detect_image_format()` + `wrap_image_reader()`
pipeline is designed for exactly this. Each new format just needs to:

1. Be detected by magic bytes or heuristics
2. Produce a `Read + Seek` stream of logical 512-byte sectors in ProDOS order
3. The existing ProDOS filesystem code handles everything from there

---

## Step 1: Raw ProDOS-Order Images (.po)

**Goal:** Recognize `.po` files as raw ProDOS-order disk images.

**Why:** `.po` files are the simplest ProDOS disk image — raw 512-byte sectors in
order, no header. They're already readable as `ImageFormat::Raw`, but we should
recognize the extension to provide a better description and avoid false negatives
with size-based heuristics.

**Files:**
- `src/rbformats/mod.rs` — update `detect_image_format()` and `ImageFormat`

**Changes:**

1. No structural change needed — `.po` files are byte-identical to raw images.
   The existing `Raw` fallback already handles them correctly.

2. Optionally, add extension-aware description in the GUI so users see
   "ProDOS-order disk image (140 KB)" instead of "Raw disk image" when they
   open a `.po` file. This is cosmetic and can be deferred.

**Estimated effort:** ~30 minutes (cosmetic only), or zero (already works).

---

## Step 2: DOS 3.3-Order Images (.do/.dsk)

**Goal:** Read `.do` and `.dsk` files by applying the DOS 3.3 → ProDOS sector
interleave map.

**Why:** Many Apple II disk images are distributed in DOS 3.3 sector order. The
physical sectors are the same but numbered differently. ProDOS expects sectors in
a different logical order, so we need to remap.

**Files:**
- `src/rbformats/interleave.rs` (new) — sector interleave logic
- `src/rbformats/mod.rs` — add `ImageFormat::Dos33Order`, detection, wrapping

**Changes:**

1. Create `interleave.rs` with:
   - `DOS33_TO_PRODOS_MAP: [u8; 16]` — the 16-sector interleave table
   - `Dos33Reader` struct implementing `Read + Seek` that wraps a raw reader
     and remaps sector reads within each track (16 sectors × 256 bytes per
     sector in DOS 3.3, but we present 512-byte ProDOS blocks)

2. Detection: For files with `.do` or `.dsk` extension AND size exactly 143,360
   bytes (35 tracks × 16 sectors × 256 bytes), detect as DOS 3.3 order.
   - **Ambiguity:** `.dsk` files can be either DOS 3.3 or ProDOS order. Use
     heuristic: try reading ProDOS volume header at block 2 in both orderings.
     If ProDOS magic found in ProDOS order, treat as `.po`. Otherwise assume
     DOS 3.3.

3. Add `ImageFormat::Dos33Order` variant, wire into `wrap_image_reader()`.

**Note:** DOS 3.3 images rarely contain ProDOS filesystems (they typically have
DOS 3.3 filesystem), but the interleave infrastructure is needed for `.dsk`
disambiguation and will be reused by the WOZ reader.

**Estimated effort:** ~2-3 hours. ~100-150 lines.

---

## Step 3: DiskCopy 4.2 Images (.dc42)

**Goal:** Read DiskCopy 4.2 images used by classic Mac and Apple IIgs software.

**Why:** DiskCopy 4.2 was the standard Macintosh/Apple IIgs disk image format.
Many Apple IIgs ProDOS floppies were archived as `.dc42` files.

**Files:**
- `src/rbformats/dc42.rs` (new) — DiskCopy 4.2 parser
- `src/rbformats/mod.rs` — add variant, detection, wrapping

**Changes:**

1. Create `dc42.rs` with:
   - `Dc42Header` struct: disk name (Pascal string, 64 bytes), data size (u32 BE),
     tag size (u32 BE), data checksum (u32 BE), tag checksum (u32 BE),
     disk format byte, format byte, private (u16)
   - `parse_dc42_header()` — validate: byte at offset 0 is string length (1-63),
     byte at offset 82 must be `0x01` or `0x02` (disk format), and total file
     size should match `84 + data_size + tag_size`
   - Data begins at offset 84

2. Detection in `detect_image_format()`:
   - Read first 84 bytes, check format byte at offset 82 is `0x01` or `0x02`,
     verify file size matches `84 + data_size + tag_size`

3. Add `ImageFormat::DiskCopy42(Dc42Header)` variant. `wrap_image_reader()`
   returns a `SectionReader` starting at offset 84 with length `data_size`.

4. DiskCopy 4.2 stores data in ProDOS block order for 3.5" disks (800KB GCR),
   so no reordering needed — just skip the header.

**Estimated effort:** ~3-4 hours. ~150-200 lines.

---

## Step 4: WOZ Images (.woz)

**Goal:** Read WOZ 1.0 and WOZ 2.0 floppy disk images for ProDOS browsing.

**Why:** WOZ is the modern preservation format for Apple II floppies, used by
all major emulators (MAME, OpenEmulator, MicroM8). It stores the physical
nibble/flux representation, which is important for copy-protected disks but
means we need to decode back to logical sectors.

This is the most complex step because WOZ stores the raw magnetic
representation, not logical sectors.

**Files:**
- `src/rbformats/woz.rs` (new) — WOZ parser + denibblizer
- `src/rbformats/mod.rs` — add variant, detection, wrapping

### Sub-step 4a: WOZ Container Parser

Parse the WOZ file structure (both v1 and v2):

```
Bytes 0-3:   Magic ("WOZ1" or "WOZ2")
Bytes 4-7:   0xFF 0x0A 0x0D 0x0A (high-bit verification)
Bytes 8-11:  CRC32 of everything after this point

Chunks (each: 4-byte ID + 4-byte size + data):
  INFO — disk metadata (disk type, write protected, etc.)
  TMAP — track map (160 entries, quarter-tracks → TRKS index)
  TRKS — track data (nibbles for WOZ1, bit streams for WOZ2)
  META — optional key-value metadata (language, title, etc.)
  WRIT — optional write hints (WOZ2)
```

**Key fields from INFO chunk:**
- `disk_type`: 1 = 5.25" (140KB), 2 = 3.5" (800KB)
- `boot_sector_format` (WOZ2): 0 = unknown, 1 = 16-sector, 2 = 13-sector, 3 = both

### Sub-step 4b: 5.25" Disk Denibblization (GCR 6-and-2)

For 5.25" disks (the most common Apple II format):

1. **Find sync bytes** — scan bitstream for `0xFF` sync patterns
2. **Find address fields** — `D5 AA 96` prologue, then volume/track/sector/checksum
   in 4-and-4 encoding, then `DE AA EB` epilogue
3. **Find data fields** — `D5 AA AD` prologue, then 342 bytes of 6-and-2 encoded
   data + 1 checksum byte, then `DE AA EB` epilogue
4. **Decode 6-and-2** — translate nibbles back to 256 raw bytes per sector
5. **Apply ProDOS interleave** — map physical sectors to ProDOS logical blocks
   (reuse map from Step 2)

For 16-sector disks: 35 tracks × 16 sectors × 256 bytes = 140 KB (280 ProDOS blocks).

### Sub-step 4c: 3.5" Disk Denibblization (GCR)

For 3.5" disks (Apple IIgs 800KB floppies):

1. Variable speed zones — 3.5" disks have 5 speed zones with different numbers
   of sectors per track (12, 11, 10, 9, 8 sectors from outer to inner)
2. **Find address/data fields** — different prologue/epilogue patterns than 5.25"
3. **Decode GCR** — 6-and-2 encoding but with 524-byte sectors (512 data + 12 tag)
4. **Extract 512-byte sectors** — discard tag bytes, keep data

80 tracks × variable sectors = 1600 sectors × 512 bytes = 800 KB.

### Sub-step 4d: WozReader

Implement `WozReader` struct (similar pattern to `ChdReader`):

```rust
pub struct WozReader {
    /// Decoded logical sectors in ProDOS block order.
    /// Fully decoded at open time (140KB-800KB fits easily in RAM).
    sectors: Vec<u8>,
    position: u64,
}

impl WozReader {
    pub fn open(path: &Path) -> Result<Self> { ... }
}

impl Read for WozReader { ... }
impl Seek for WozReader { ... }
```

Since floppy images are small (140KB-800KB), we decode all sectors into a flat
`Vec<u8>` at open time rather than doing on-demand track decoding. This keeps
the `Read + Seek` implementation trivial and avoids re-scanning bitstreams
on every seek.

### Sub-step 4e: Integration

1. Detection: check first 4 bytes for `WOZ1` or `WOZ2` magic (plus high-bit
   verification bytes). Add before the VHD check in `detect_image_format()`.

2. Add `ImageFormat::Woz(WozReader)` variant. `wrap_image_reader()` returns
   `(Box::new(woz_reader), woz_reader.len())`.

3. ProDOS filesystem auto-detection works automatically once we provide the
   flat sector stream.

**Estimated effort:** ~3-5 days. ~600-900 lines.

### Known Limitations (acceptable for v1)

- **Read-only** — no writing back to WOZ format (WOZ writing requires
  nibblization, which is the reverse process and significantly more complex)
- **No copy-protection support** — we decode standard GCR encoding only.
  Copy-protected disks with non-standard nibble patterns may fail to decode
  some sectors. This is fine — we're a backup tool, not an emulator.
- **13-sector disks** — DOS 3.2 used 13 sectors per track with 5-and-3
  encoding. These are rare and won't contain ProDOS. Skip for now.

---

## Step 5: Export Support (Optional, Deferred)

Once we can read all these formats, users may want to export/convert between
them. This is a natural follow-on but not required for the initial
implementation:

- Export ProDOS filesystem to `.po` (trivial — already works via raw export)
- Export ProDOS filesystem to `.2mg` (already implemented)
- Export to `.dc42` (straightforward — write header + raw data)
- Export to `.woz` (hard — requires nibblization, defer)

---

## Implementation Order

```
Step 1 (.po)  ─── already works, cosmetic only
     │
Step 2 (.do/.dsk) ─── interleave infrastructure, needed by Step 4
     │
Step 3 (.dc42) ─── independent, can be done in parallel with Step 2
     │
Step 4 (.woz)  ─── depends on Step 2 for interleave map
  ├── 4a: container parser
  ├── 4b: 5.25" denibblization
  ├── 4c: 3.5" denibblization
  ├── 4d: WozReader
  └── 4e: integration
```

**Total estimated effort:** ~5-7 days for all four formats.

---

## Testing Strategy

- Obtain sample images for each format from public domain Apple II software
  archives (e.g., Asimov archive, WOZ test suite from applesaucefdc.com)
- Verify ProDOS volume header detection and directory listing for each format
- Compare decoded sector data against known-good `.po` reference images
- WOZ: use the official WOZ test images which include standard and edge-case
  disk encodings
- Integration tests: round-trip backup from each image format, verify
  metadata and checksums match

## References

- [WOZ 2.0 Spec](https://applesaucefdc.com/woz/reference2/) — official format reference
- [Beneath Apple ProDOS](https://archive.org/details/Beneath_Apple_ProDOS) — ProDOS disk layout
- [Understanding the Apple II, ch. 9](https://archive.org/details/Understanding_the_Apple_II) — GCR encoding
- [DiskCopy 4.2 format](https://wiki.68kmla.org/DiskCopy_4.2) — header structure
- [Ciderpress source](https://github.com/fadden/ciderpress) — reference implementation for all formats
