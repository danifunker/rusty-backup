# WOZ Writing Implementation Plan

Add WOZ2 write support: both exporting compatible disk images to WOZ format and
saving edits back to WOZ files.

---

## Strategy: regenerate, don't patch

CiderPress2 patches WOZ files at the bit level (preserves the original bitstream,
overwrites just the data field when a sector changes). That's the right call for a
preservation tool; it's overkill for a backup tool.

For rusty-backup, both use cases (export from raw disks, save after edit) converge
on one writer: **regenerate a clean WOZ2 from the flat sector buffer that
`WozReader` already produces**. This drops CircularBitBuffer's in-place complexity.

Cost: editing a preservation-grade WOZ will round-trip to a "standard-format" WOZ
(lossless for data, lossy for copy-protection artifacts). Acceptable — we're not
an emulator.

### Reference implementation

CiderPress2 (`../CiderPress2/`) key files:
- `DiskArc/Disk/Wozoof.cs` — container read/write, `DoCreateDisk525/35`, `GenerateStream`
- `DiskArc/Disk/TrackInit.cs` — `GenerateTrack525`, `GenerateTrack35`
- `DiskArc/SectorCodec.cs` — `EncodeSector62_256`, `EncodeSector62_524`,
  `WriteAddressField_525/35`, 4-and-4 helpers, `sDiskBytes62` table
- `DiskArc/Disk/Woz_Info.cs` — INFO chunk field layout
- `DiskArc/Disk/Woz-notes.md` — format notes

---

## Session 1 — BitWriter + 6-and-2 encode tables (~2-3 hr)

**Goal:** Build the low-level bit-writing primitives and encoding tables.

**File:** `src/rbformats/woz_write.rs` (new)

**Deliverables:**
- `BitWriter` struct: append bits MSB-first into a `Vec<u8>`, track bit position.
  Much simpler than CiderPress2's `CircularBitBuffer` because we're building fresh,
  not editing in place.
- `ENCODE_62: [u8; 64]` — the `sDiskBytes62` table, inverse of our existing
  `DECODE_62` in `woz.rs`. Maps 6-bit values (0x00-0x3F) to disk nibbles.
- `write_octet(byte)` — write 8 bits.
- `write_self_sync(count)` — write `count` self-sync FF bytes as 10-bit patterns
  (for WOZ2 non-byte-aligned storage).
- `fill_self_sync(total_bytes)` — fill remaining buffer space with self-sync.
- 4-and-4 encoders: `to_44_lo(val) -> u8`, `to_44_hi(val) -> u8`.
- Unit tests: round-trip a byte through encode→decode using existing `DECODE_62`.

**CiderPress2 reference:**
- `SectorCodec.sDiskBytes62` (line ~52)
- `SectorCodec.To44Lo/To44Hi` (lines ~1191-1200)
- `CircularBitBuffer.WriteOctet/WriteByte/Fill`

---

## Session 2 — 5.25" track generator (~3 hr)

**Goal:** Encode 16-sector 5.25" tracks from raw 256-byte sectors.

**File:** `src/rbformats/woz_write.rs`

**Deliverables:**
- `encode_address_field_525(writer, vol, track, sector)` — writes D5 AA 96
  prologue, 4-and-4 encoded volume/track/sector/checksum, DE AA EB epilogue.
- `encode_data_field_525(writer, data: &[u8; 256])` — writes D5 AA AD prologue,
  342 nibbles of 6-and-2 encoded data (86 auxiliary + 256 primary) with XOR-chain
  checksum, DE AA EB epilogue.
- `generate_track_525(vol, track, sectors: &[[u8; 256]; 16]) -> (Vec<u8>, u32)`
  — builds a ~6336-byte track buffer with bit count:
  - For each physical sector: gap3 (20 self-sync FF) → address field → gap2
    (5 self-sync FF) → data field.
  - Remaining space filled with self-sync (gap1).
- Round-trip tests: `generate_track_525` → `decode_525_track` (existing reader),
  verify all 16 sectors match input.

**CiderPress2 reference:**
- `SectorCodec.WriteAddressField_525` (line ~574)
- `SectorCodec.EncodeSector62_256` (line ~816)
- `TrackInit.GenerateTrack525_16` (line ~116)

**Encoding algorithm (6-and-2, 256 bytes):**
1. Split each input byte into high 6 bits (`top[i]`) and low 2 bits.
2. Pack the low 2 bits into 86 auxiliary bytes (3 input bytes per aux byte,
   bit-reversed).
3. XOR-chain: write aux[85]^0, aux[84]^aux[85], ..., then top[0]^aux[0],
   top[1]^top[0], ..., then final checksum.
4. Each 6-bit value is mapped through `ENCODE_62` to a disk nibble.

---

## Session 3 — 3.5" track generator (~4 hr)

**Goal:** Encode variable-sector 3.5" tracks from raw 512-byte sectors.

**File:** `src/rbformats/woz_write.rs`

**Deliverables:**
- `encode_address_field_35(writer, track, sector, side, format)` — writes D5 AA 96,
  GCR-encoded track/sector/side/format/checksum, DE AA epilogue + pad byte.
- `encode_data_field_35(writer, sector: u8, data: &[u8; 512])` — writes D5 AA AD,
  sector nibble, 700 GCR nibbles encoding 524 bytes (12 zero tag + 512 data) via
  the three-register rotating checksum, 4-byte checksum, DE AA epilogue + pad byte.
- `interleave_table(num_sectors, interleave) -> Vec<u8>` — generates the physical
  sector ordering (2:1 for ProDOS, 4:1 for GS/OS).
- `generate_track_35(track, side, format, sectors: &[Option<[u8; 512]>]) -> (Vec<u8>, u32)`
  — builds a variable-length track (8-12 sectors depending on speed zone):
  - For each sector (in interleave order): gap3 (36 self-sync) → address → pad →
    gap2 (5 self-sync) → data → pad.
  - 64-byte gap1 prefix, remainder filled with self-sync.
- Round-trip tests: `generate_track_35` → `decode_35_track`, verify all sectors.

**CiderPress2 reference:**
- `SectorCodec.WriteAddressField_35` (line ~932)
- `SectorCodec.EncodeSector62_524` (line ~965)
- `TrackInit.GenerateTrack35` (line ~171)
- `TrackInit.GetInterleaveTable` (line ~252)

**Encoding algorithm (6-and-2, 524 bytes → 703 nibbles):**
1. Split 524 input bytes into three parallel streams (part0, part1, part2) using
   the three-register rotating checksum (c1, c2, c3). Each output byte is
   `input XOR register`, with carries propagating between registers.
2. For each of 175 groups: extract high 2 bits from each part into a `twos` nibble,
   write twos + part0_lo6 + part1_lo6 + part2_lo6 (last group omits part2).
3. Write 4-byte checksum (twos + chk2_lo6 + chk1_lo6 + chk0_lo6).

---

## Session 4 — WOZ2 container builder (~3 hr)

**Goal:** Assemble tracks into a valid WOZ2 file.

**File:** `src/rbformats/woz_write.rs`

**Deliverables:**
- `write_info_chunk(buf, disk_type, sides, largest_track_blocks)` — writes 60-byte
  INFO chunk with version=2 fields. `disk_type`: 1=5.25", 2=3.5".
  `optimal_bit_timing`: 32 for 5.25", 16 for 3.5".
- `write_tmap_525(buf, num_tracks)` — 160-byte TMAP: whole tracks at quarter-track
  indices 0,4,8,...; adjacent quarter-tracks point to same index; half-tracks = 0xFF.
- `write_tmap_35(buf, num_sides)` — 160-byte TMAP: 2 entries per track
  (side0, side1 or 0xFF for single-sided).
- `build_woz2_file(disk_type, sides, tracks: &[(Vec<u8>, u32)]) -> Vec<u8>`:
  - Header: 8-byte signature (WOZ2 + FF 0A 0D 0A) + 4-byte CRC placeholder.
  - INFO chunk at offset 12 (header at 12, data at 20).
  - TMAP chunk at offset 80 (header at 80, data at 88).
  - TRKS chunk at offset 248 (header at 248, data at 256): 160×8-byte descriptors
    + track bit data starting at byte 1536, each track padded to 512-byte blocks.
  - CRC32 (via `crc32fast`) over bytes 12..end, written at offset 8.
- Tests: build blank 5.25" (35 tracks) and 3.5" (160 tracks) images, open with
  existing `WozReader`, verify zero-filled sectors decode correctly and
  `is_woz()` returns true.

**WOZ2 file layout:**
```
Offset  Content
0       "WOZ2" FF 0A 0D 0A (8 bytes)
8       CRC32 of bytes 12..EOF (4 bytes)
12      INFO chunk header (ID=0x4F464E49, len=60)
20      INFO data (60 bytes)
80      TMAP chunk header (ID=0x50414D54, len=160)
88      TMAP data (160 bytes)
248     TRKS chunk header (ID=0x534B5254, len=variable)
256     TRKS descriptors (160 × 8 = 1280 bytes)
1536    Track bit data (variable, 512-byte aligned)
```

---

## Session 5 — Public API + integration (~3 hr)

**Goal:** Wire the writer into rusty-backup's export and edit pipelines.

**Files:**
- `src/rbformats/woz_write.rs` — public API functions
- `src/rbformats/mod.rs` — export integration
- `src/gui/browse_view.rs` — save-as option

**Deliverables:**

### Public API
- `pub fn sectors_to_woz_525(sectors: &[u8]) -> Result<Vec<u8>>` — takes 143,360
  bytes (35 tracks × 16 sectors × 256 bytes in ProDOS order), returns WOZ2 file
  bytes. Applies ProDOS→physical sector mapping (reverse of `PHYS_TO_PRODOS`).
- `pub fn sectors_to_woz_35(sectors: &[u8], sides: u8) -> Result<Vec<u8>>` — takes
  400K or 800K of flat sector data (same layout as `decode_35_woz` output), returns
  WOZ2 file bytes. Uses 2:1 interleave, format byte 0x22 (DSDD) or 0x12 (SSDD).
- `pub fn write_woz(path: &Path, sectors: &[u8], disk_type: u8, sides: u8) -> Result<()>`
  — convenience wrapper that auto-detects 5.25" vs 3.5" from size.

### Export integration
- In `rbformats/mod.rs`, add WOZ as an export option for floppy-sized images
  (140K, 400K, 800K).
- For the existing archive-edit flow: when a WOZ file is decompressed for editing,
  the decoded sector data is stored in a temp file; on save, re-encode via the
  writer rather than emitting raw sectors.

### GUI
- Browse view "Save as..." menu gains WOZ option for floppy images.
- When editing a WOZ via archive-edit, the recompress step produces a WOZ file
  (not raw).

### Testing
- Round-trip: `.po` → `sectors_to_woz_525` → `WozReader::from_bytes` → compare.
- Round-trip: 800K raw → `sectors_to_woz_35` → `WozReader::from_bytes` → compare.
- Integration test with real WOZ file (if available): open → decode → re-encode →
  decode again → verify identical sector data.

---

## Summary

| Session | Scope | Est. hours | Dependencies |
|---------|-------|------------|--------------|
| 1 | BitWriter + encode tables | 2-3 | None |
| 2 | 5.25" track generator | 3 | Session 1 |
| 3 | 3.5" track generator | 4 | Session 1 |
| 4 | WOZ2 container builder | 3 | Sessions 2, 3 |
| 5 | Public API + integration | 3 | Session 4 |

Total: ~15-18 hours across 5 sessions.

All new code goes in `src/rbformats/woz_write.rs`. The existing read path in
`woz.rs` is untouched. Both use cases (export from compatible disks + save edits)
share the same regenerate-from-sectors code path.
