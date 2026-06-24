# crusty-backup (`cb-dos`) — backup & restore directly from MS-DOS — scoping plan

Living scope + plan for **crusty-backup**, a separate, small **C** utility
(binary: `cb-dos`) that backs up and restores FAT disks **on the vintage machine
itself**, with compression and (phased) defragmentation. The "C" in the name is
the point — it's the C-based offshoot of rusty-backup.

This is **not** part of the Rust codebase. Rust/LLVM has no real-mode/16-bit x86
target and none of our deps (`chd`, `zstd`, threads, `nix`/`windows`, `tokio`)
build for DOS. crusty-backup shares a **file format** with the desktop tool —
and that format is the desktop's **own native PerPartition backup format**, so
the desktop already knows how to restore (and resize!) what cb-dos produces. The
only desktop addition is a gzip codec (see §3).

Update the checkboxes and the **Progress log** at the bottom as work lands.
Keep each step small enough to be one commit.

Status legend: `[ ]` not started · `[~]` in progress · `[x]` done · `[-]` dropped/out of scope

---

## 0. Confirmed scope decisions

From the scoping discussion (2026-06-02):

| Decision | Choice |
|----------|--------|
| Name / binary | **crusty-backup** / `cb-dos` |
| Target hardware | **486+**, **LFN-capable DOS** — satisfied for free by shipping our own **bootable FreeDOS media** (native kernel LFN); MS-DOS + `doslfn` TSR is the bring-your-own alternative |
| Distribution | **Deferred — design later.** Likely a bootable FreeDOS floppy / Gotek `.img` (FreeDOS + CWSDPMI + `cb-dos.exe`); driver/destination matrix (IDE/CF vs +CD vs +USB) TBD. See *Distribution* for the reference analysis |
| Granularity | **Per-partition AND per-disk** backup; **per-partition selective** restore |
| Restore sizing | **Restore to a different-sized disk** — entire-disk / minimum / custom per partition. **FAT resize on DOS**; NTFS resize routed through the desktop initially |
| Filesystems | **NTFS + FAT16/32** (no FAT12). **ext2/3 = stretch goal.** Compaction needs only each FS's allocation bitmap, not a full driver |
| User interface | **Lightweight text UI** (DJGPP `conio`, keyboard-only, function-key action bar at the bottom — no mouse) **+ CLI**, both over a shared C engine. **POC-first to measure binary size** |
| File format | **Reuse the desktop's native PerPartition backup format.** No `.RBK`, no `.igz` |
| Compression | **gzip/DEFLATE** — add a `Gzip` codec to the native format (`partition-N.gz`) |
| Filenames | **Native names verbatim** (LFN required); zero desktop naming changes |
| Transfer / destination | **Removable media / second drive** (a plain DOS path). Network **deferred**; CD **writing** dropped (CD restore-source is a freebie via MSCDEX) |
| Interop | **Full round-trip** — desktop already restores cb-dos backups; desktop can emit gzip backups for cb-dos to restore |

Out of scope (for now): non-FAT filesystems (no HFS/Amiga/NTFS/ext on a DOS
box), CHD/VHD output, **CD burning** on DOS, any **built-in** network transport
(see §6).

---

## Why the native format (the due-diligence result)

We surveyed every format the app supports before committing to anything new.
Result: **the desktop's native PerPartition backup format already does
everything we need, and is the only format that does.**

- **Per-partition backup** — one `partition-N.<ext>` per partition
  (`src/backup/metadata.rs`); selectable via `partition_filter`.
- **Per-disk backup** — the same layout captures all partitions + the
  MBR/GPT/APM table.
- **Restore to a different-sized disk** — `src/restore/mod.rs` offers
  entire-disk / minimum / custom-per-partition sizing and calls
  `resize_fat_in_place` for FAT. **Filesystem-aware resize — exactly the
  requirement.**

Critically, the **foreign** formats the app can read (Norton Ghost `.gho`,
WinImage `.imz`, VHD, Clonezilla, CHD, QCOW2, VMDK) are **convert/export only —
none support resize-on-restore.** So none of them satisfy the "restore to a
different-sized disk" requirement. The native format is the answer; a bespoke
`.RBK` container buys us nothing the native format doesn't already give.

A whole-disk gzipped raw image (the earlier `.igz` idea) is likewise
disqualified: it has no partition/filesystem awareness, so it can neither do
per-partition selective restore nor resize.

---

## 1. Toolchain

**DJGPP** (GCC for DOS) is the chosen base:

- 32-bit protected mode + DPMI extender (CWSDPMI) → **flat address space and all
  extended memory**. This is what makes compression viable at all; otherwise the
  640 KB conventional-memory ceiling fights us.
- Real libc; **zlib compiles unmodified**.
- Raw disk access via real-mode `int 13h` extended read/write (AH=42h/43h, LBA)
  through `__dpmi_int` + the transfer buffer (`__tb`, `dosmemget`/`dosmemput`).
  CF/SD-over-IDE presents as a normal BIOS drive (0x80, 0x81…).
- **LFN**: cb-dos writes long filenames via the DOS LFN API (int 21h/71xxh).
  Many FreeDOS kernels provide LFN natively (no TSR). **Reality check
  (2026-06-24):** the **FreeDOS 1.4 FULL *installer* image kernel reports no LFN**
  — `71A0h` returns ax `0x7100` (multiplex not installed), and DJGPP then mangles
  `partition-0.gz` → `PARTITIO.GZ` (and collides the `.crc32` sidecar). The fix is
  to **load `DOSLFN.COM`** (shipped at `\FREEDOS\BIN\DOSLFN.COM`) before running
  cb-dos; once loaded, names round-trip verbatim. So the boot media must either
  ship a kernel built with LFN **or** `DEVLOAD`/run `DOSLFN` at boot — do not
  assume kernel-native LFN. On stock MS-DOS the user loads a `doslfn`-style TSR
  the same way.

Alternative considered: **OpenWatcom C/C++** + DOS/4GW. DJGPP preferred for
gcc/zlib familiarity.

Target runtime: **FreeDOS** (shipped as boot media; or MS-DOS 5.0+/PC-DOS with
an LFN TSR), **486+**.

**Local build setup (not yet present on this machine — checked 2026-06-02):** no
DJGPP cross-compiler, OpenWatcom, or DOS emulator is installed. To compile and
**measure** a POC we need either the **DJGPP cross-compiler**
(`i586-pc-msdosdjgpp-*`, e.g. via the `andrewwutw/build-djgpp` Homebrew tap or a
from-source build) or a DOS box with native DJGPP, plus **DOSBox-X / 86Box** for
testing without real hardware. This setup is the gating step for the size POC.

## Distribution — bootable FreeDOS floppy / Gotek image

> **Status: deferred — design later.** The concrete boot-disk composition and
> the destination/driver matrix (IDE/CF vs +CD vs +USB) are **not decided**. The
> material below is captured as reference for when we pick this up. v1 of cb-dos
> can be developed and tested without finalized boot media — run it from any
> FreeDOS / LFN-equipped DOS install (see *Fallback*). **Decoupling note**: the
> cb-dos engine (read FAT → zero-then-gzip → native folder → restore+resize) is
> independent of how it's booted, so this can be settled any time before release.

Ship cb-dos as a **bootable FreeDOS disk image** so the user never has to
assemble an environment:

- **Contents**: FreeDOS kernel + `COMMAND.COM`, `CWSDPMI.EXE` (DPMI host),
  `cb-dos.exe`, and an `AUTOEXEC.BAT` that lands the user in the tool.
- **Why it's the easy path**: FreeDOS gives **LFN for free** (kernel-native, no
  TSR) and guarantees the DPMI host is present — the two environment
  prerequisites cb-dos has. Zero "did you load X?" support burden.
- **Media**: a plain `.img` works on a **real 1.44 MB floppy or a Gotek**
  (FlashFloppy/HxC read `.img`/`.hfe`). Target a **2.88 MB image** for headroom
  once CWSDPMI + zlib-linked cb-dos are added; 1.44 MB is tight.
- **Boot model**: booting from floppy/Gotek frees the IDE/CF bus — **source card
  on one channel, destination on a second drive / USB-IDE** (matches §2b).
- **CI**: build `cb-dos-boot.img` as a release asset alongside the existing
  per-platform bundles in `.github/workflows/release.yml`.
- **Fallback**: `cb-dos.exe` (+ `CWSDPMI.EXE`) also ship as a plain ZIP for users
  who want to run it from their own FreeDOS / LFN-equipped DOS install.

### Self-contained, compressed executable

Two stacking tricks make cb-dos a single small self-contained file — useful both
for floppy budget and for the "load + run from memory" goal:

- **Embed the DPMI host (CWSDSTUB)** — DJGPP can bundle CWSDPMI *into* the exe
  via `stubedit` / the cwsdstub, so **no separate `CWSDPMI.EXE`** is needed on
  the media. (This djgpp tree ships `stubedit`/`stubify` but not the CWSDPMI
  binary — a quick fetch when we wire this up.)
- **UPX-compress the exe** — a UPX'd DJGPP binary **self-decompresses into RAM
  at launch and runs, with no temp file and no RAM disk**. This is exactly the
  "compressed exe that unzips into memory and executes" pattern. Measured on the
  POC: **111,104 → 56,436 bytes (50.8%, `djgpp2/coff`)** with `upx --best`.

Combined target: one self-contained `cb-dos.exe` carrying its own DPMI host,
UPX-compressed. (Validate that UPX + cwsdstub compose cleanly when we add the
embedded host — minor risk.)

### Bundled drivers — what each destination costs

The boot media must carry drivers for whatever I/O the user wants. The cost
varies sharply by path, so we **tier** it via a **menu-driven CONFIG.SYS**
(FreeDOS boot-menu) rather than loading everything always:

| Destination | Drivers bundled | Notes |
|-------------|-----------------|-------|
| **Second IDE/CF drive** (primary) | **none** | BIOS `int 13h`; always works, fastest |
| **CD-ROM** (restore source) | ATAPI CD driver (`VIDE-CDD.SYS`/`GCDROM.SYS`) + `SHSUCDX` | small, reliable, well-trodden |
| **USB mass storage** (best-effort) | `USBASPI.SYS` (UHCI/OHCI/EHCI, hw-specific) + `DI1000DD.SYS`/`USBDISK.SYS` | fragile, slow, controller-specific |

**Boot menu profiles**: (1) Plain IDE/CF — no extra drivers; (2) + CD-ROM;
(3) + USB (best-effort).

**USB reality check**: real **486 hardware almost never has USB** (USB arrived
in the Pentium/Socket-7 era, ~1996+). DOS USB drivers are controller-specific,
USB 1.1/2.0 mass-storage only, no UAS, and slow. So **do not gate the design on
USB** — it's an optional profile for later boards / PCI USB cards, not a v1
commitment. The second IDE/CF drive is the always-works path.

**Memory**: load drivers high with FreeDOS `HIMEMX`/`JEMMEX`. cb-dos is a
DJGPP/DPMI program living in **extended** memory, so it doesn't compete with
these conventional-memory TSRs.

**Size pressure**: even a 2.88 MB image gets tight once CD + USB drivers are
added. Escape hatches if it overflows: a **bootable CF/HDD image** (boot from a
small CF, image a second drive) or an **El Torito bootable CD** for machines
that can boot from CD — both far roomier than a Gotek floppy.

---

## 2. Architecture — two abstractions

### 2a. Source

- **Whole physical disk** by BIOS drive number (0x80, 0x81…) → `int 13h`.
  - LBA path (AH=42h/43h) when the BIOS reports extension support (AH=41h).
  - **CHS fallback** (AH=02h/03h) for pre-1996 BIOSes; covers ≤8 GB, which is
    essentially all vintage CF.
- **An existing backup folder** (a DOS path) → DOS file I/O, for restore.

Read the MBR + FAT BPB to find partitions and the cluster allocation. Small,
self-contained C — same on-disk layout as our Rust `partition/`/`fs/fat.rs`, but
no shared code.

### 2b. Destination = "a DOS path"

`cb-dos` writes the backup **folder** to / reads it from a DOS path:

| Physical destination | How it's a DOS path |
|----------------------|---------------------|
| Second drive / spare partition / removable (CF, USB-IDE, Zip) | A FAT drive letter (`D:\BACKUPS\MYDISK\`) — **primary, easy path** |
| CD-ROM (**restore source only**) | `MSCDEX` mounts it read-only; burning is done on the host |

Removable media is the easy, cross-platform answer: the host just copies the
folder off the card/USB. **No network code in `cb-dos`.**

---

## 2c. Filesystem scope

cb-dos supports **NTFS + FAT16/32** (no FAT12 — floppy-era, irrelevant for CF).
**ext2/3 is a stretch goal.** Compaction only needs each filesystem's
**allocation bitmap** — *not* a full driver:

| FS | What cb-dos parses for compaction | Min-size | Effort |
|----|-----------------------------------|----------|--------|
| **FAT16/32** | BPB + FAT table → allocated clusters | last used cluster | trivial |
| **NTFS** | boot sector → `$MFT` → `$Bitmap` `$DATA` runs → cluster bitmap | last set bit | moderate (no full NTFS driver — we don't enumerate files) |
| **ext2/3** *(stretch)* | superblock → block-group descriptors → block bitmaps | last set block | moderate |

**Resize-on-restore split**: the desktop has `resize_fat_in_place` *and*
`resize_ntfs_in_place`, so restoring a cb-dos backup to a different-sized disk on
the **desktop** works for both. Doing **NTFS resize on DOS** is a heavy lift, so
cb-dos's on-DOS restore does **FAT resize only**; NTFS restores same-size on DOS
and is resized via the desktop. File-level defrag (Phase B, §4) is likewise
**FAT-only**.

## 2d. User interface — text UI + CLI over a shared engine

Mirror rusty-backup's GUI/CLI parity: a **core C engine** (disk I/O, FS bitmap
parse, compact, gzip, metadata) with **two thin front-ends**.

- **Text UI** — **no framework**. DJGPP **`conio`** (`gotoxy`, `cprintf`,
  `getch`, `window()`) + **`int 16h`** keyboard. Keyboard-only; the **controls
  live in a function-key action bar at the bottom** (`F2 Backup  F3 Restore
  F10 Quit`), DOS-`EDIT`/Norton-Commander style — arrow keys + Enter to navigate
  a list, function keys to act. No mouse handling at all.
  - PDCurses (DOS port) is a **fallback only** if we later want richer
    dialogs/windows — it costs binary size + a bundled dependency.
  - Turbo Vision (magiblot port) is explicitly rejected: heavyweight,
    mouse-centric.
- **CLI** — scriptable flags mirroring the engine (backup / restore / inspect,
  partition selection, sizing). Same verbs as `rb-cli` where they overlap.

**Size POC — DONE (2026-06-02).** `crusty-backup/src/crustybk.c` (conio screen
draw + `int 16h` keys + bottom function-key action bar + dummy disk list),
built with `i586-pc-msdosdjgpp-gcc -Os -s`, measures **111,104 bytes (~108 KB)**.
For reference a bare DJGPP hello-world is ~146 KB unstripped; stripping (`-s`)
brings the richer POC *under* that. Projection holds: +zlib later → ~150–300 KB;
CWSDPMI ~20 KB — comfortably within 1.44 MB (the OS + drivers dominate the
floppy, not cb-dos). Build with `cd crusty-backup && make size`.

## 2e. Cloning bootable drives + direct disk-to-disk

Cloning the **bootable** hard drive is already covered by the boot-media plan —
no RAM tricks required:

- **Boot cb-dos from floppy/Gotek**, so the HDD is *not* the running system and
  is just another `int 13h` device. cb-dos reads it **raw**, capturing sector 0
  (MBR + boot code) and each partition's VBR. The native format stores `mbr.bin`
  + full partition contents → restore reconstructs a **bootable** disk. (Phase B
  defrag must preserve `IO.SYS`/`MSDOS.SYS` placement — already flagged.)
- **Live mode** (imaging the drive you booted from) is technically possible on
  single-tasking DOS with raw reads, but only safe if nothing is writing
  (TSRs/temp/swap). Support best-effort; **recommend external boot** for fidelity.

RAM-disk / in-memory reality check:
- A RAM disk (`XMSDSK`, FreeDOS `RDISK`, `RAMDRIVE.SYS`) is useful only as small
  **scratch** space. It is **volatile and tiny** → never a backup destination.
- "Load the whole disk into RAM" doesn't scale (486 RAM ≪ card size). DJGPP's
  flat 32-bit + XMS already lets cb-dos hold multi-MB streaming buffers; that's
  the right use of memory, not whole-disk caching.
- Running the app from memory is handled by the **UPX self-extracting exe**
  (see *Distribution*), not a RAM disk.

**Direct disk-to-disk clone (a planned mode):** for the single-drive-plus-spare
case, cb-dos reads source → compacts/defrags/resizes on the fly → writes
straight to a **target disk**, with no intermediate `.gz` file. Boot from floppy,
source on one IDE channel, target on the other, clone live to a different-sized
disk. Sidesteps the destination-space problem entirely (the target *is* the
destination). Reuses the same engine as backup; add as a mode after the
file-based round-trip works.

## 3. File format — the native PerPartition layout + a gzip codec

cb-dos emits the **desktop's native PerPartition backup folder, verbatim**:

```
MYDISK\                      (a normal folder; LFN-capable DOS)
  metadata.json              top-level metadata (partition info, alignment,
                             original/minimum sizes, checksums)
  mbr.bin                    raw MBR (and mbr.json / gpt.json sidecar)
  partition-0.gz             gzip of partition 0 (free clusters zeroed in-stream)
  partition-0.gz.crc32       checksum sidecar
  partition-1.gz
  partition-1.gz.crc32
  ...
```

### The one desktop change: a `Gzip` codec — **SHIPPED (Phase 1, 2026-06-24)**
`CompressionType` gained a **`Gzip` variant → `.gz`** (`"gzip"` /
`file_extension()` `"gz"` in `src/backup/mod.rs`). Implemented as:

- `src/rbformats/gzip.rs` — `compress_gzip` (mirrors `zstd.rs`: streamed
  `flate2::write::GzEncoder`, optional hasher tee for the `.gz.crc32` sidecar,
  one contiguous member per partition).
- `src/rbformats/compress.rs` — `Gzip` arm in `compress_partition_hashed`, a
  `"gzip"` decompress arm in `decompress_to_writer()` using
  **`flate2::read::MultiGzDecoder`** (so a multi-member `.cbk`-style file decodes
  too, not just a single cb-dos member), and a `"gzip"` arm in
  `compress_file_to_archive`.
- `rb-cli backup --format gzip` (and `gzip`/`gz` accepted in the config-file
  `[backup] format` parser).

The **restore + resize path is reused 100%** — verified end-to-end: a
`rb-cli backup --format gzip` of an MBR FAT16 disk produced `partition-0.gz` +
`metadata.json` (`"compression_type": "gzip"`, `compacted: true`), and
`rb-cli restore` rebuilt a byte-faithful disk (only the MBR CHS + FAT BPB
fields restore deliberately patches differ). Output is **metadata-identical to
the `.zst` path** for the same source, so the engine can't tell gzip from zstd.

### Compaction = per-partition zero-then-gzip
For each FAT partition, cb-dos walks the FAT and streams **real bytes for
allocated clusters, zeros for free clusters**, into gzip. The member stays a
**faithful full-size partition** (so restore/resize need no special handling),
while gzip crushes the zero runs → **compaction for free**. The source disk is
never modified.

### Compression is streamed on the fly (not a later pass)

gzip runs **as the data is read**: cb-dos feeds each 64 KB–1 MB block of used
clusters straight into zlib's streaming `deflate()` and writes compressed bytes
incrementally — there is never an uncompressed staging copy (matches the
"never load a whole partition into RAM" rule). zlib's deflate needs ~256 KB
working memory, trivially covered by DJGPP extended memory.

Consequence for the UI/preflight: because we compress while streaming, the
**destination only needs room for the compressed output**. So "minimum space
required" reported to the user is the **estimated compressed size** (+ margin),
not the used-data size. (Smart mode images used data only; a sector-by-sector
mode images every sector and needs proportionally more.)

### What cb-dos must put in `metadata.json` — **FROZEN (Phase 1)**
Frozen against `src/backup/metadata.rs` (`BackupMetadata` / `PartitionMetadata`
/ `AlignmentMetadata`). Field names are the JSON keys verbatim (serde, no
`rename_all` on these structs → snake_case). A field is **required** unless it
carries `#[serde(default)]` / `skip_serializing_if` in the struct. The minimal
shape cb-dos must emit for a gzipped MBR-FAT backup:

```json
{
  "version": 1,
  "created": "2026-06-24T09:45:00Z",            // ISO-8601; any parseable string
  "source_device": "0x80",                       // free-form label
  "source_size_bytes": 50331648,
  "partition_table_type": "MBR",                 // "MBR" | "GPT" | "None"
  "checksum_type": "crc32",                      // "crc32" | "sha256"
  "compression_type": "gzip",
  "split_size_mib": null,                        // REQUIRED KEY (may be null)
  "sector_by_sector": false,
  "layout": "per-partition",
  "alignment": {                                 // all five keys required
    "detected_type": "DOS Traditional (255x63)",
    "first_partition_lba": 2048,
    "alignment_sectors": 2048,
    "heads": 255,
    "sectors_per_track": 63
  },
  "partitions": [
    {
      "index": 0,
      "type_name": "FAT16",
      "partition_type_byte": 6,                  // 0x06 FAT16, 0x0E/0x0C FAT16/32-LBA …
      "start_lba": 2048,
      "original_size_bytes": 49283072,           // partition window size
      "imaged_size_bytes": 4233728,              // = minimum_size_bytes when compacted
      "compressed_files": ["partition-0.gz"],
      "checksum": "cbf421a8",                     // lowercase hex CRC32 of the .gz
      "resized": false,
      "compacted": true,                          // free clusters zeroed pre-gzip
      "is_logical": false,
      "minimum_size_bytes": 4233728               // FAT last-used-cluster byte size
    }
  ]
}
```

Required top-level keys (no serde default): `version`, `created`,
`source_device`, `source_size_bytes`, `partition_table_type`, `checksum_type`,
`compression_type`, **`split_size_mib`** (emit `null`, not omit), `alignment`,
`partitions`. Required per-partition keys: `index`, `type_name`, `start_lba`,
`original_size_bytes`, `compressed_files`, `checksum`, `resized`. Everything
else (`sector_by_sector`, `layout`, `partition_type_byte`, `imaged_size_bytes`,
`compacted`, `is_logical`, `minimum_size_bytes`, …) has a serde default and may
be omitted — but cb-dos **should** emit `partition_type_byte`, `compacted`,
`imaged_size_bytes`, and `minimum_size_bytes` so the desktop's
shrink-to-minimum restore works (it offers the minimum size only when present).
The per-`.gz` checksum sidecar is `partition-N.gz.crc32` containing the same
lowercase-hex CRC32. JSON is trivial to emit on DOS (`printf`).

---

## 4. Defrag (phased)

### Phase A — zero-then-gzip compaction (MVP)
The free-cluster zeroing in §3 **is** the compaction. Exact on-disk layout (boot
sector, hidden sectors, FAT) preserved → **bootability is safe**.

### Phase B — file-level repack / defrag
Reorder files contiguously within the emitted partition image before zeroing the
now-contiguous free tail → **defrag**.
- Risk: `IO.SYS` / `MSDOS.SYS` must be **first and contiguous** at the start of
  the data area or the disk won't boot (what the DOS `SYS` command babysits).
  FAT entries, attributes, dates, and LFN entries must stay consistent.
- Ships **after** the round-trip is proven, as an optional mode. Still emits a
  plain `partition-N.gz` — just a defragmented one.

---

## 5. Memory & I/O model

- DJGPP flat 32-bit: compression window + cluster buffers in extended memory;
  only a small **bounce buffer** in conventional memory for the `int 13h`
  transfer, looped.
- Stream in 32–64 KB blocks (zlib window 32 KB).
- DOS is single-tasking → **no volume-locking dance** like the modern `src/os/`
  layer. Nothing else is writing the card.
- Safety: refuse to write to the source drive; require explicit confirm /
  `--yes` for restore (destructive); print source/destination geometry first.

---

## 6. Network transport — deferred (and we'd own both ends)

EtherDFS was considered and **dropped**: its server side is Linux-centric, so
"support macOS/Windows/Linux hosts" would mean owning a cross-platform
raw-Ethernet listener anyway — not easy. For v1 there is **no network code**;
removable media is the path. Revisit only after the local round-trip ships.

When we do, the design is scoped in detail in
**[`cb_dos_network_and_state.md`](cb_dos_network_and_state.md)**: **TCP/IP over a
Crynwr packet driver** (mTCP as reference/borrow candidate) with the host as an
`rb-cli net-serve` subcommand on a **plain unprivileged socket** — chosen over raw
L2 because TCP gives reliability for free and keeps the host pcap/root-free. That
doc also covers the coupled **disk-state model** the transport needs (chunked
`.cbk` resume container, the "same source?" fingerprint, the file manifest +
attribute round-trip, boot protection, and swap-file exclusion).

**Pragmatic interim (shipped):** the cb-dos **CD** can carry mTCP (GPLv3) + a
packet driver so you image to a file and then **FTP it off the box** today — no
cb-dos network code, no host listener, just a standard DOS TCP/IP stack. Setup,
the packet-driver library, and the build wiring are in
**[`cb_dos_networking.md`](cb_dos_networking.md)**. The integrated `net-serve`
transport above is still the eventual goal; this just unblocks moving backups
over the wire now.

---

## 7. Open risks / unknowns

- **BIOS LBA support** varies even on 486 boards → keep the CHS fallback (≤8 GB).
- **`metadata.json` field parity** — cb-dos must emit exactly what
  `src/restore/` reads (original/minimum sizes, alignment, codec, CRC32). Pin
  this against `src/backup/metadata.rs` before writing any DOS JSON.
- **LFN availability** — FreeDOS is fine; on stock MS-DOS the user must load an
  LFN TSR. Document clearly; detect at runtime and refuse gracefully if absent.
- **FAT resize correctness** — confirm `resize_fat_in_place` handles the
  cb-dos-produced minimum/custom sizes for FAT16/32 across target geometries.
- **NTFS bitmap parsing on DOS** — reading `$MFT` → `$Bitmap` `$DATA` runs in C
  is moderate but the riskiest FS parser; validate min-size + compaction against
  the desktop's NTFS reader on identical images.
- **TUI binary size** — unmeasured until the Phase 0a POC; estimate says fine,
  but confirm before committing to floppy-only distribution.
- Validating round-trip needs a **real DOS test rig** *and* an emulator
  (86Box / DOSBox-X with emulated IDE-CF) — establish in Phase 0.

---

## 8. Phased plan

- [~] **Phase 0a — Toolchain + UI size POC.** Install DJGPP cross-compiler +
      DOSBox-X/86Box. Build a minimal **text-UI POC** (`conio`: screen draw +
      `int 16h` keys + bottom function-key action bar + dummy disk list) and
      **measure the `.exe`** to confirm the floppy budget. Establish
      engine/front-end project layout (core C lib + TUI + CLI stubs).
      *DJGPP + DOSBox-X installed; POC built (~108 KB / 55 KB UPX), measured, and
      now run under DOSBox-X. Remaining: core-C-lib / CLI-stub project layout.*
- [~] **Phase 0b — Disk spike.** "hello disk": enumerate BIOS drives, `int 13h`
      ext read with CHS fallback, dump MBR, parse FAT/NTFS BPB + read the
      allocation bitmap, write a long-named file via the LFN API. Prove on real
      486 + emulator.
      *Done & DOSBox-X-verified for FAT12/16/32 **and NTFS** (`disk_spike.c`) on
      both a superfloppy and an MBR-partitioned disk; FAT cluster usage and NTFS
      `$Bitmap` and **exFAT** allocation-bitmap counts all **bit-exact vs a host
      scan**; LFN write/detect proven (`lfn_test.c`). NTFS reads MFT record 6 +
      fixup + `$DATA` runs; exFAT scans the root for the 0x81 bitmap entry and
      counts the contiguous bitmap. Remaining: real-**486 hardware** and a
      **booted-FreeDOS** run.*
- [x] **Phase 1 — Desktop `Gzip` codec. DONE (2026-06-24).** Added
      `CompressionType::Gzip` + `src/rbformats/gzip.rs` + the three `compress.rs`
      dispatch arms (`MultiGzDecoder` on decode) + `rb-cli backup --format gzip`.
      Verified end-to-end: gzip backup → restore of an MBR FAT16 disk round-trips
      and is metadata-identical to the `.zst` path (resize machinery reused 100%).
      `metadata.json` field set **frozen** in §3 above.
- [x] **Phase 2 — Backup MVP (DOS). DONE (2026-06-24) — format round-trips.**
      `crusty-backup/src/cbbackup.c` (`CBBACKUP.EXE`) reads a FAT disk via int13h,
      smart-compacts each partition (image up to the last used cluster, zero the
      interior free clusters), streams it through **zlib gzwrite**, and writes the
      native folder (`metadata.json`, `mbr.bin`, `partition-N.gz`,
      `partition-N.gz.crc32`) to a DOS path. Proven end-to-end on **real FreeDOS in
      qemu**: imaged a 47 MB MBR FAT16 disk, copied the folder off, and
      `rb-cli restore` rebuilt a byte-faithful disk (FS mounts, file intact).
      zlib is cross-built by `deps/fetch-zlib.sh`; `make backup`. **Two findings:**
      (1) the FD 1.4 *installer* kernel reports **no LFN** (`71A0h` → ax 0x7100),
      so `DOSLFN` must be loaded or names mangle to 8.3 — see §1; (2) the tools
      used to hang at *process termination* under CWSDPMI — **root-caused and
      fixed 2026-06-24** (a 1-paragraph DOS-buffer under-allocation let the
      int13h DAP overrun the next MCB; DOS hung walking the arena at exit). The
      tools now exit cleanly (qemu poweroff). Remaining polish: NTFS/logical
      partitions (skipped), real-486.
- [x] **Phase 3 — Restore MVP (DOS). DONE (2026-06-24) — byte-identical restore.**
      `crusty-backup/src/cbrestore.c` (`CBRESTORE.EXE`) reads the native folder,
      writes `mbr.bin` to sector 0 and streams each `partition-N.gz` (zlib
      `gzread`) back to its `start_lba` via **int13h write** (AH=43h/03h),
      zero-padding compacted partitions out to their original window. Restores at
      **original size** (a minimal `metadata.json` field-scanner reads `start_lba`
      / `original_size_bytes` / `imaged_size_bytes` / `compressed_files`). `/Y`
      gates the destructive write. **Proof:** restored a Phase-2 folder onto a
      *blank* disk on real FreeDOS in qemu — the result is **byte-for-byte
      identical to the original source disk** (50,331,648 bytes), mounts as
      `CBDOSFAT16`, file intact. `make restore`. (The CWSDPMI exit-hang that
      blocked chaining backup→restore in one boot is now fixed — see the log.)
      **On-DOS resize SHIPPED (2026-06-24):** `/SIZE:ORIGINAL|MINIMUM|ENTIRE|CUSTOM`
      (`/CUSTOM:<bytes>`) does a full bidirectional FAT12/16/32 resize on DOS — the
      C port of the desktop's `resize_fat_in_place`: grow = forward-shift data +
      zero-extend the FAT (capped at the FAT16 cluster ceiling, since wider needs
      FAT32 re-clustering); shrink = backward-shift data + truncate the FAT (floored
      at a valid FAT16); no-op when the window already matches (keeps ORIGINAL
      byte-identical). FAT16/32 clean-shutdown flags + FAT32 FSInfo reset after.
      Handles **both** producer shapes: cbbackup's full-size BPB and the desktop's
      pre-minimized gzip BPB (so ORIGINAL *grows* the latter back). The MBR window +
      CHS are patched only when a partition's size changes (verbatim otherwise).
      Verified on real FreeDOS/qemu across every direction with a 16 MB file intact
      through large bidirectional shifts, read back by *both* mtools and the
      desktop's own FAT driver. Remaining: CD/MSCDEX source, NTFS/logical, real-486.
- [x] **Phase 4 — Per-partition selective backup/restore (DOS). DONE
      (2026-06-24).** `/PARTS:i,j` on **both** `cbbackup` (image only those slots)
      and `cbrestore` (restore only those indices); default = all. Indices are the
      **0-based MBR primary slots** — the same `index` written in `metadata.json`
      and printed as "part N" (cb-dos is MBR-primary-only, so the physical slot is
      the natural key). Note this is *cleaner than* the desktop's `--partitions`,
      which is documented "1-based" but actually filters on a renumbered 0-based
      `part.index` and rejects 0 — leaving the first partition unselectable (a
      desktop CLI bug worth a separate fix). Selective backup keeps the whole
      `mbr.bin`; selective restore writes the saved MBR verbatim and only the
      chosen partitions' data (unselected regions stay blank). Verified on real
      FreeDOS/qemu with a 2-partition FAT16 disk: `cbbackup /PARTS:1` emits only
      `partition-1.gz`; `cbrestore /PARTS:0` / `/PARTS:1` populate exactly one slot
      (file checksums match) and leave the other blank. Fixed a latent
      multi-partition `metadata.json` parse bug uncovered here: `index` was read
      via `u64_after(idx,"index")`, which grabbed the *next* partition's index and
      swapped them (harmless while every backup had one partition).
- [ ] **Phase 4b — Direct disk-to-disk clone mode (DOS).** Source → on-the-fly
      compact/resize → target disk, no intermediate file (see §2e). Reuses the
      backup engine.
- [ ] **Phase 5 — File-level repack/defrag** (Phase B), boot-file aware.
- [ ] **Phase 6 (optional)** — LZ4 codec for slower machines (needs a matching
      desktop `Lz4` variant).
- [ ] **Phase 7 (deferred)** — built-in network transport (host + DOS client,
      both ours), only if removable media proves insufficient. Full design +
      sub-phases (7a–7i) in
      [`cb_dos_network_and_state.md`](cb_dos_network_and_state.md): TCP/IP over a
      packet driver, chunked `.cbk` resume container, disk-state fingerprint, file
      manifest + idempotency, boot protection, swap exclusion.

---

## 9. Reference pointers

- **DJGPP** + **CWSDPMI** — toolchain + DPMI host. LFN via int 21h/71xxh.
- **zlib** — codec (compiles under DJGPP unmodified). Desktop decoder already
  present: `flate2::GzDecoder` in `src/rbformats/gho.rs` + `imz.rs`.
- Desktop touch points: `CompressionType` (`src/backup/mod.rs:28`),
  `src/rbformats/compress.rs` (add gzip compress/decompress),
  `src/backup/metadata.rs` (field parity), `src/restore/mod.rs` (resize path —
  reused unchanged), `resize_fat_in_place` (FAT resize), CRC32 via `crc32fast`.
- DOS bootability gotcha (Phase 5): the `SYS` command's IO.SYS/MSDOS.SYS
  placement rules.
- `int 13h` extensions (AH=41h detect, 42h/43h LBA r/w) with CHS (02h/03h)
  fallback.

---

## Progress log

- 2026-06-24 — **Phase 4 — per-partition selective backup/restore on DOS.**
  Added `/PARTS:i,j` to **both** `cbbackup` (image only the listed MBR slots) and
  `cbrestore` (restore only the listed indices); default stays "all". Indices are
  the **0-based MBR primary slot** = the `metadata.json` "index" = cbbackup's
  "part N" output — the natural key for an MBR-primary-only tool. (Deliberately
  *not* the desktop's `--partitions` scheme, which empirically filters on a
  renumbered 0-based `part.index` while its help says "1-based" and `parse_indices`
  rejects 0 → the first partition is unselectable: a desktop CLI off-by-one worth
  fixing separately.) Selective backup keeps the whole `mbr.bin` and lists only
  the imaged partitions in metadata; selective restore writes the saved MBR
  verbatim and only the chosen partitions' data, leaving unselected regions blank.
  **Found + fixed** a latent multi-partition parse bug in `cbrestore` while
  testing: `p->index` was read with `u64_after(idx,"index")`, which matched the
  *next* partition's `"index"` and swapped the two (invisible while every prior
  backup had a single partition) — now read in place with a new `u64_at`.
  **Verified on real FreeDOS/qemu** with a 2-partition FAT16 disk (each FS sized
  to its slot — note `mformat -i img@@off` sizes to EOF, so build each partition
  FS in its own exact-size file and `dd` it in): `cbbackup /PARTS:1` emitted only
  `partition-1.gz` + an index-1-only metadata; `cbrestore /PARTS:0` and `/PARTS:1`
  each populated exactly one slot (P0/P1 checksums match) and left the other
  unformatted. `make backup restore`. **Next:** Phase 4b (direct disk-to-disk
  clone), then the `.cbk` wire (7b).
- 2026-06-24 — **On-DOS FAT resize shipped — cbrestore grows/shrinks the
  filesystem on the 486 itself.** `cbrestore.c` gained
  `/SIZE:ORIGINAL|MINIMUM|ENTIRE|CUSTOM` (`/CUSTOM:<bytes>`), a full bidirectional
  FAT16/32 resizer that is the C port of the desktop's `resize_fat_in_place`.
  Added the missing disk primitives (`read_lba`, AH=48h true disk size for
  ENTIRE), `compute_fat_sectors`, forward + backward sector-region shifts, the
  FAT16/32 clean-shutdown flags, and FAT32 FSInfo reset. **Algorithm:** grow =
  shift the data region forward + zero-extend the FAT, **capped** at the FAT16
  cluster ceiling (65 524 — wider would need FAT32 re-clustering we don't do on
  DOS); shrink = write the truncated FAT then shift the data region backward onto
  it, **floored** at a valid FAT16 (4 085 clusters); same-size = no-op (keeps the
  proven byte-identical ORIGINAL restore). The MBR window + CHS-end are patched
  only for partitions whose size changed (verbatim otherwise). **Key discovery:**
  the two producers store *different* gz representations — `cbbackup` keeps the
  original BPB with free clusters zeroed (`ts32`=full), while the desktop's
  compacted gzip (`CompactFatReader`) rewrites the BPB to a pre-minimized FS
  (`ts32`=minimum). cbrestore reads the gz's actual BPB total and resizes against
  it, so **ORIGINAL grows the desktop's minimal gz back to full size** while
  staying a no-op for cbbackup's full-size gz. Non-512-byte/non-FAT partitions
  fall back to original size with a note (resize on the desktop). **Verified on
  real FreeDOS in qemu** across every path — ORIGINAL (byte-identical from a
  cbbackup folder; grow from a desktop folder), MINIMUM (no-op on a minimal gz;
  251→16 spf backward-shift + floor clamp on a full gz), ENTIRE (forward-shift
  grow capped at the FAT16 limit), CUSTOM (mid-range 251→96 spf backward shift of
  ~16 MB of data, and a 32 MB grow) — each mounts and reads back **bit-identical
  files** (incl. a 16 MB random blob) under *both* mtools and the desktop's own
  FAT driver. **Test-rig note:** redirecting `cbbackup`'s stdout to a file on the
  same drive it writes the backup folder to corrupts `mbr.bin` (its
  "wrote metadata.json" banner bleeds into the boot-code area) — a FreeCOM
  redirection quirk (gotcha #3), *not* a cbrestore bug; run `cbbackup` without
  `>` (the Phase-2 recipe never redirected it). `make restore`. **Next:** Phase 4
  (per-partition selection), 4b (disk-to-disk clone), then the `.cbk` wire (7b).
- 2026-06-24 — **Fixed the CWSDPMI termination hang (root-caused).** The DOS
  tools (cbbackup, cbrestore, disk_spike) finished their work but then hung at
  *process exit* under CWSDPMI on real FreeDOS — backups were always complete
  and restorable, the process just never returned (so it couldn't chain
  backup→restore in one boot, and a real user would have to power-cycle).
  Isolated it with a minimal mode-switching probe (`t_exit.c`): a bare int13h
  **AH=08** exits cleanly, but an **AH=42 LBA read** hangs. Cause: `xfer_init`
  allocated the DOS transfer buffer as exactly `XFER_BYTES`, but `read_lba`
  places the 16-byte int13h **Disk Address Packet** at offset `XFER_BYTES` — one
  byte past the block — overrunning the adjacent **memory-control block**. DOS
  doesn't notice until it walks the arena to free memory at exit, so the
  corruption surfaces as a hang *at termination* (and only after a DAP-using
  read; AH=08 never touches it). DOSBox-X's DPMI host happened to tolerate it.
  Fix: allocate `XFER_BYTES + 16` (one extra paragraph) so the DAP lives inside
  the owned block. Verified the probe (buggy alloc hangs, fixed alloc exits 0)
  and the real tool: cbbackup now returns to the batch and **qemu powers off
  cleanly (rc 0)** while still producing a byte-faithful, restorable backup.
- 2026-06-24 — **Phase 3 complete — cb-dos restores a folder to a disk on real
  DOS, byte-identically.** Wrote `crusty-backup/src/cbrestore.c`
  (`CBRESTORE.EXE`): a minimal `metadata.json` field-scanner (no JSON lib),
  `mbr.bin` → sector 0, and each `partition-N.gz` streamed via zlib `gzread`
  straight to its `start_lba` with **int13h write** (AH=43h ext / AH=03h CHS),
  zero-padding the compacted tail out to the original window. `/Y` confirms the
  destructive write. **Proof:** staged a Phase-2 backup folder onto FreeDOS,
  booted qemu with a *blank* 48 MB target as 0x81, ran `CBRESTORE C:\BK 81 /Y`,
  and the reconstructed disk is **byte-for-byte identical to the original source**
  (whole-disk `cmp` clean, 50,331,648 bytes) — mounts as `CBDOSFAT16`, `HELLO.TXT`
  intact, MBR partition entry exact. int13h writes commit immediately, so the
  restore lands even though the process hits the documented CWSDPMI exit-hang
  (which also blocks chaining backup→restore in one boot — done as two boots).
  The full DOS-native cycle now works both ways. **Next:** on-DOS resize, then
  Phase 4 (per-partition selection) / the `.cbk` container.
- 2026-06-24 — **Phase 2 complete — cb-dos images a FAT disk on real DOS and the
  desktop restores it.** Wrote `crusty-backup/src/cbbackup.c` (`CBBACKUP.EXE`):
  int13h read (LBA + CHS fallback, mirroring disk_spike), whole-FAT load,
  smart-compaction (image to the last used cluster, zero interior free clusters),
  streamed **zlib `gzwrite`** to `partition-N.gz`, CRC32 of the compressed file
  into the `.gz.crc32` sidecar, raw `mbr.bin`, and a `metadata.json` matching the
  Phase-1 frozen schema. zlib is cross-built for DJGPP by `deps/fetch-zlib.sh`
  (gitignored), linked via `make backup`. **Proof:** booted FreeDOS headless in
  qemu (hda=boot, hdb=the FAT16 disk as 0x81), ran `CBBACKUP C:\BK 81`, pulled
  `C:\BK` off with mtools, and `rb-cli restore` rebuilt a 50331648-byte disk —
  FS mounts as `CBDOSFAT16`, `HELLO.TXT` intact, partition table correct. The
  DOS→desktop format interop is real. **Two findings worth keeping:** (1) the FD
  1.4 installer kernel has **no LFN** (`71A0h`→`0x7100`); must `DOSLFN` first or
  names truncate to 8.3 (and the `.gz` / `.gz.crc32` 8.3-collide) — §1 updated;
  (2) under **CWSDPMI**, an int13h-reading DJGPP program **hangs at termination**
  after finishing — `disk_spike` repros it, DOSBox-X's DPMI doesn't, so it's a
  CWSDPMI exit quirk, not a backup bug; the folder is fully written before the
  hang. Candidate fix: ship an alternate DPMI host (HDPMI32/JEMMEX). **Next:
  Phase 3** (cb-dos restores the folder back to a disk on DOS, with resize).
- 2026-06-24 — **Phase 1 complete — desktop `Gzip` codec shipped + metadata
  frozen.** Added `CompressionType::Gzip` (`"gzip"` / `.gz`) and a new
  `src/rbformats/gzip.rs` (`compress_gzip`, a streamed `GzEncoder` mirroring the
  zstd module, with the optional checksum tee). Wired three dispatch points in
  `src/rbformats/compress.rs`: the `Gzip` compress arm, a `"gzip"` decode arm on
  **`MultiGzDecoder`** (forward-compatible with the multi-member `.cbk` shape),
  and the recompress arm. Exposed `rb-cli backup --format gzip` (+ `gzip`/`gz`
  in the config parser). Built a host MBR FAT16 disk and proved the full
  round-trip: `--format gzip` backup → `partition-0.gz` (4.4 KB from a 47 MB
  mostly-empty partition) + `metadata.json` `"compression_type": "gzip"` →
  `rb-cli restore` rebuilds a byte-faithful disk (only the MBR CHS + FAT BPB
  bytes the restorer deliberately patches differ; FS mounts, file intact). A
  zstd backup of the same disk is **metadata-identical**, confirming the
  restore + resize path is reused 100% — the engine can't distinguish gzip from
  zstd. Froze the exact `metadata.json` field set cb-dos must emit in §3 (which
  keys are serde-required vs defaulted). Unit test `rbformats::gzip::
  test_compress_gzip`; clippy clean. **Next: Phase 2** (cb-dos writes the native
  folder on DOS), the first phase that runs on the 486 itself.
- 2026-06-20 — **Boots into the TUI on real FreeDOS.** The cb-dos disk now
  auto-launches the text UI at boot (FDAUTO.BAT runs `CRUSTYBK.EXE`, renamed from
  `tui_poc`), and the FreeDOS installer's `SETUP.BAT` is stripped. **Key fix:**
  DJGPP binaries need a DPMI host — DOSBox-X fakes one, but a real FreeDOS boot
  disk has none (they failed with "Load error: no DPMI"). `mkmedia.sh` now ships
  **CWSDPMI.EXE** (freely redistributable; the DJGPP stub auto-loads it from
  `A:\`), so the tools run on actual FreeDOS for the first time — verified in
  qemu booting straight into the disk/partition TUI. `DISKSPK.EXE` (int13h disk
  dump) and `LFNTEST.EXE` (LFN write check) remain as diagnostics at the prompt.
- 2026-06-02 — Due-diligence pass over all desktop formats. **Decision: reuse
  the native PerPartition backup format** (it already does per-partition +
  per-disk + resize-on-restore; foreign formats don't resize). Dropped `.RBK`
  and the whole-disk `.igz` idea. Only desktop change is a **`Gzip` codec**
  (decoder already in-tree via Ghost/IMZ). Chose **LFN-required** DOS (FreeDOS /
  `doslfn`) so cb-dos writes native filenames verbatim — zero desktop naming
  work. gzip-only, 486+. No code yet.
- 2026-06-02 — **Phase 0a POC built + measured.** `crusty-backup/src/crustybk.c`
  (conio text UI, bottom function-key bar, keyboard-only) compiles with DJGPP
  gcc 12.2.0 → **111,104 B (~108 KB)** stripped; **56,436 B (~55 KB)** after
  `upx --best`. DJGPP toolchain at `~/djgpp` (had to clear macOS quarantine —
  SIGKILL/exit 137 until `xattr -dr com.apple.quarantine`). Code lives in
  `crusty-backup/` subfolder. Established **UPX self-extracting exe** + planned
  **CWSDSTUB-embedded DPMI** as the self-contained-exe path, and **direct
  disk-to-disk clone** + external-boot raw read as the bootable-clone strategy
  (RAM disk = scratch only). No emulator installed yet — POC unrun.
- 2026-06-02 — UI + FS decisions. **Lightweight text UI** (DJGPP `conio`,
  keyboard-only, bottom function-key action bar — no mouse) **+ CLI** over a
  shared C engine; no TUI framework (PDCurses = fallback, Turbo Vision
  rejected). **POC-first to measure binary size** (estimate ~150–300 KB w/ zlib;
  floppy-fine). FS scope locked to **NTFS + FAT16/32**, **ext2/3 stretch**;
  compaction needs only the allocation bitmap. NTFS resize-on-restore routed
  through desktop; FAT resize on DOS. Checked local machine — **no DOS toolchain
  installed**, so toolchain setup is the gating step for the POC.
- 2026-06-02 — Explored shipping a **bootable FreeDOS floppy / Gotek image**
  (FreeDOS gives kernel-native LFN, satisfying the LFN requirement for free).
  Captured the bundled-driver analysis (IDE/CF needs none; CD restore = ATAPI +
  SHSUCDX; USB = fragile/optional, mostly moot on real 486 hw). **Boot-disk
  design deferred** — engine work is independent of boot media, so it's settled
  later. No v1 destination matrix chosen yet.
- 2026-06-19 — **Phase 0a POC run (gap closed) + Phase 0b disk spike done,
  emulator-verified.** Installed/located **DOSBox-X 2026.05.02**
  (`/Applications/dosbox-x.app`); the previously-unrun `crustybk.exe` now boots
  and renders correctly under it. Wrote **`crusty-backup/src/disk_spike.c`** — the
  "hello disk" spike: enumerate BIOS drives (`int 13h AH=08h`), LBA-extensions
  check (`AH=41h`), sector read with **ext (`AH=42h`) + CHS (`AH=02h`) fallback**
  via a DPMI DOS-memory transfer buffer, sector-0 **MBR-vs-FAT-boot-sector**
  classification, partition-table dump, **FAT12/16/32 BPB parse**, and a full
  **FAT walk counting used/free/bad clusters** (the allocation-bitmap proxy).
  Verified two ways in DOSBox-X (headless `-exit`, results written to
  `C:\SPIKE.LOG` on the host mount): (1) the bare **FAT16 superfloppy** fixture
  (`tests/fixtures/test_fat16.img`) — clusters 8167 / used 3 / free 8164,
  **bit-exact** against an independent host-side Python FAT scan; (2) a
  **host-built MBR-partitioned** FAT16 disk (type 0x06 @ LBA 63) — partition
  table dumped and the nested boot sector parsed *through* the partition offset.
  Also wrote **`crusty-backup/src/lfn_test.c`**: raw **LFN API** create
  (`int 21h AX=716Ch`) + volume-info (`71A0h`). It correctly **detects LFN
  absence** — DOSBox-X integrated DOS reports DOS 5.0, so LFN is off (`AX=0x7100`)
  and the spike refuses rather than 8.3-mangling; with `-set "dos lfn=true"` the
  long name **"Crusty Backup Long Name partition-1.gz"** round-trips verbatim to
  the host FS with correct content. **Still pending:** prove all three on **real
  486 hardware**, and a **booted-FreeDOS** run (with/without `doslfn`) to validate
  the actual deployment environment — DOSBox-X integrated DOS only proves the API.
  Test disks live under the gitignored `crusty-backup/build/disks/`; run harness
  is `crusty-backup/run-dosbox.sh` + the headless `dosbox-x -exit -c …` pattern.
- 2026-06-19 — **Phase 0b: NTFS added to the spike.** `disk_spike.c` now detects
  the NTFS VBR (OEM `NTFS    `), parses its BPB (bytes/sec, sec/clus, total
  sectors, MFT LCN, MFT-record size from the signed `clusters_per_mft` byte),
  reads **MFT record 6 (`$Bitmap`)** — applying the update-sequence **fixup** —
  walks attributes to the `$DATA` (0x80) attribute, **decodes its data runs**
  (resident + non-resident), and reads the bitmap counting set bits (set =
  allocated). Ported the minimal subset of `src/fs/ntfs.rs` (same offsets/fixup/
  run-decode logic). MBR routing now also picks **type 0x07** partitions
  (`is_imageable_part_type`); exFAT (OEM `EXFAT`) is detected and skipped.
  Verified on a host-built MBR disk wrapping `tests/fixtures/test_ntfs.img`
  (type 0x07 @ LBA 63): **511 clusters / 189 allocated / 322 free**, bit-exact
  against an independent host-side Python `$Bitmap` decode. (DOSBox-X `IMGMOUNT`
  rejected the bare 2 MiB image's auto-geometry / `-t hdd` left it "not active",
  so the NTFS test disk is wrapped to ~16 MiB like the FAT one — the small-image
  `IMGMOUNT` quirk is worth remembering.) Remaining 0b work unchanged: real-486,
  booted-FreeDOS, exFAT (stretch).
- 2026-06-19 — **Phase 0b: exFAT added to the spike.** `disk_spike.c` now parses
  the exFAT VBR/BPB (BytesPerSectorShift/SectorsPerClusterShift at 0x6C/0x6D,
  cluster-heap offset, cluster count, root cluster), reads the start of the root
  directory, finds the **allocation-bitmap directory entry (type 0x81)** (start
  cluster @+20, size @+24), and reads the bitmap **contiguously** counting set
  bits (set = allocated) — matching `src/fs/exfat.rs`. Handles non-512-byte
  logical sectors via a `bytes_per_sec/512` BIOS-LBA ratio. The MBR type-0x07
  path already routed here; `report_volume` distinguishes NTFS vs exFAT by OEM
  string. Verified on a host-built MBR disk wrapping `tests/fixtures/
  test_exfat.img` (type 0x07 @ LBA 63): **992 clusters / 9 allocated / 983
  free**, bit-exact vs an independent host decode. **Phase 0b filesystem
  coverage is now complete (FAT12/16/32 + NTFS + exFAT).** Remaining 0b:
  real-486 hardware + booted-FreeDOS.
