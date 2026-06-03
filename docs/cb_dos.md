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
  The **FreeDOS kernel provides LFN natively** (no TSR), which is the main reason
  we ship our own FreeDOS boot media (see *Distribution* below). On a stock
  MS-DOS install the user loads a `doslfn`-style TSR instead.

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

**Size POC — DONE (2026-06-02).** `crusty-backup/src/tui_poc.c` (conio screen
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

### The one desktop change: a `Gzip` codec
Today `CompressionType` is `Chd / Dvd / Vhd / Zstd / None` — **no gzip**
(`src/backup/mod.rs:28`). zstd is impractical on DOS and raw loses compression,
so we add a **`Gzip` variant → `.gz`**. This is small because the **decoder
already exists in-tree**: `flate2::GzDecoder` is used by the Ghost (`gho.rs`) and
IMZ (`imz.rs`) paths. Touch points:

- `CompressionType::{as_str, file_extension}` (`src/backup/mod.rs:28`) → `"gzip"`
  / `.gz`.
- `src/rbformats/compress.rs` — add a `gzip` compressor alongside zstd/raw and a
  decompress case in `decompress_to_writer()` (`GzDecoder`).
- Restore reads the codec from `metadata.json`; no restore-pipeline change.

Once `Gzip` exists, the **desktop restore + resize path is reused 100%** — it
doesn't care whether a `partition-N` member is `.zst` or `.gz`.

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

### What cb-dos must put in `metadata.json`
The restore/resize path keys off metadata, so cb-dos must emit the fields it
reads: per-partition `original_size`, `minimum_size` (FAT last-used-cluster —
cb-dos is already reading the FAT), partition type byte, alignment, codec
(`"gzip"`), and the per-file CRC32. JSON is trivial to emit on DOS (`printf`).
**Freeze the exact field set against `src/backup/metadata.rs` in Phase 1.**

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

- [ ] **Phase 0a — Toolchain + UI size POC.** Install DJGPP cross-compiler +
      DOSBox-X/86Box. Build a minimal **text-UI POC** (`conio`: screen draw +
      `int 16h` keys + bottom function-key action bar + dummy disk list) and
      **measure the `.exe`** to confirm the floppy budget. Establish
      engine/front-end project layout (core C lib + TUI + CLI stubs).
- [ ] **Phase 0b — Disk spike.** "hello disk": enumerate BIOS drives, `int 13h`
      ext read with CHS fallback, dump MBR, parse FAT/NTFS BPB + read the
      allocation bitmap, write a long-named file via the LFN API. Prove on real
      486 + emulator.
- [ ] **Phase 1 — Desktop `Gzip` codec.** Add `CompressionType::Gzip` (+
      `compress.rs` compress/decompress). Verify the desktop can back up *and*
      restore a FAT disk with gzip, incl. resize-to-different-size. Freeze the
      `metadata.json` field set cb-dos must emit.
- [ ] **Phase 2 — Backup MVP (DOS).** cb-dos reads a FAT disk, zero-then-gzip per
      partition, writes the native folder (`metadata.json`, `mbr.bin`,
      `partition-N.gz`, CRC32) to a DOS path. Restore it **on the desktop** to
      prove the format round-trips.
- [ ] **Phase 3 — Restore MVP (DOS).** cb-dos restores the native folder back to
      a disk, with **resize options** (entire / minimum / custom per partition);
      verify the restored card boots. Restore from a DOS path **or CD (MSCDEX)**.
- [ ] **Phase 4 — Per-partition selective backup/restore (DOS).** Mirror
      `partition_filter` — pick which partitions to image/restore.
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

- 2026-06-02 — Due-diligence pass over all desktop formats. **Decision: reuse
  the native PerPartition backup format** (it already does per-partition +
  per-disk + resize-on-restore; foreign formats don't resize). Dropped `.RBK`
  and the whole-disk `.igz` idea. Only desktop change is a **`Gzip` codec**
  (decoder already in-tree via Ghost/IMZ). Chose **LFN-required** DOS (FreeDOS /
  `doslfn`) so cb-dos writes native filenames verbatim — zero desktop naming
  work. gzip-only, 486+. No code yet.
- 2026-06-02 — **Phase 0a POC built + measured.** `crusty-backup/src/tui_poc.c`
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
