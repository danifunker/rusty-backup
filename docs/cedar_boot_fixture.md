# Cedar/Dorado boot disk — the BasicCedar structural fixture

This documents `CedarDorado-boot.pdi`: a Xerox **Pilot/Cedar** physical volume that
carries a disk-resident **germ** + **physical-volume boot file**, written by
Rusty Backup's Pilot writer (`src/fs/alto/pilot.rs`, tool: `examples/pilot_probe.rs`).

It is a **structural boot fixture** for bringing up the disk-germ boot path in a
Dorado emulator. It is byte-exact and self-consistent through our own reader; it
is **not yet bootable on any emulator** (the emulator's disk read data-path and
Mesa VM are the open work — see "Honest scope" below). The point of this doc is
to state *exactly* what a booting emulator must read off this disk, with the
field offsets, so the emulator side has a precise target to validate against.

The on-disk format itself is normative in the companion repo's
`CedarDisk/PARC_PILOT_FORMAT.md`; this file is the concrete, verified instance.

## Identity (what's on it)

| Item | Value |
|---|---|
| Container | PDI (`PARCDISK`), `fsFamily = 2` (Pilot), Cedar-nucleus 32-bit FileIDs |
| Volume name | `CedarDorado` (PV root + LV root), VolumeType `3` = cedar |
| Geometry | 1400 logical pages of 512 data bytes + a 20-byte (10-word) out-of-band label |
| Germ | `Dorado.germ!4` — 16384 B (32 pages), SHA-256 `2dd215c8…f004` |
| Boot file | `BasicCedarDorado.boot!22` (Cedar 6.1) — 542720 B (1060 pages), SHA-256 `c1778650…e435` |
| Microcode slot | **empty** (see gaps) |

Both payloads are **position-independent** Mesa VM memory images: the germ reads
the boot file's location from `bootingInfo` at run time, so placement is free and
the image embeds no disk address (unlike an Alto `Sys.Boot` snapshot).

## Page map (virtual disk address = logical page index)

In the PDI logical-sector model the disk address *is* the VDA. A geometry-accurate
consumer maps VDA <-> (cylinder, head, sector) for its own pack and regenerates
ECC; the PDI stores logical sectors, not physical geometry.

| VDA range | Pages | Label `attributes` | Contents |
|---|---:|---|---|
| 0 | 1 | `1` PHYSICAL_ROOT | Physical-volume root. Seal `121212`₈ @word 0; **`bootingInfo` @word 8** |
| 1 | 1 | `2` BAD_PAGE_LIST | Bad-page list (empty) |
| 2 | 1 | `9730` DATA | Credentials (blank) |
| 3 .. 83 | 81 | `0` | PV reserve region (`OTHELLO_PV_RESERVE = 3*28 = 84` pages, 0-based 0..83) |
| 84 | 1 | `5` LOGICAL_ROOT | Logical-volume root (= subvolume logical page 0). Seal `131313`₈ @word 0 |
| 85 | 1 | `9729` HEADER | VAM run-table header |
| 86 | 1 | `9730` DATA | VAM bitmap (free-page map) |
| 87 .. 118 | 32 | `9730` DATA | **germ** (`fileID 0002:0000`, filePage 0..31) |
| 119 .. 1178 | 1060 | `9730` DATA | **boot file** (`fileID 0003:0000`, filePage 0..1059) |
| 1179 .. 1398 | 220 | `9728` FREE_PAGE | free |
| 1399 | 1 | `4` SUB_VOLUME_MARKER | subvolume end marker. Seal `141414`₈ |

Subvolume: `pvPage = 84`, `nPages = 1315` (logical pages 0..1314 → VDA 84..1398).

## What the emulator reads, stage by stage

A disk-germ Cedar boot is a three-stage chain (`DoradoBooting.tioga` §1.3):

**Stage A — Initial microcode (hardware-addressed).** The Dorado boot hardware
loads the Initial microcode from a fixed hardware-addressed region. **This disk
does not carry it** (clone-only; out of scope — see gaps). The reference
emulator currently bypasses this with a germ disk-intercept ("Route B"), so this
stage is not exercised by the fixture.

**Stage B — microcode loads the germ.** Read the PV root (VDA 0), take the
`bootingInfo[germ]` slot, and follow its boot chain to load 32 pages into memory.

**Stage C — germ loads the boot file.** The running germ takes
`bootingInfo[bootFile]`, follows that chain to load the 1060-page Cedar world
image, and transfers to it. That world image *is* BasicCedar (Viewers + Tioga
editor + the Cedar nucleus) — there are no separate "editor"/"app" files to find;
they live inside the boot world.

The boot path **cannot use the file system yet**, so it locates pages by the
per-page label boot chain, never by scanning for a `fileID` and never via the VAM
run table.

### `bootingInfo` — the array each stage indexes

In the PV root (VDA 0), at **word offset 8** (`10`₈), an
`ARRAY File.VolumeFile[checkpoint..bootFile] OF BootFile.DiskFileID`. Each slot
is **9 words** (`DISK_FILE_ID_WORDS`):

| Slot offset (words) | Field | Width | Meaning |
|---|---|---|---|
| +0 | `fID` | 5 words | FileID; all-zero = slot empty |
| +5 | `firstPage` | 2 words (INT) | logical page number of the file's first page (here `0`) |
| +7 | `firstLink` | 2 words | VDA of the first page = the initial boot-chain link (low word first) |

Slot ordinals: `checkpoint = 0`, `microcode = 1`, `germ = 2`, `bootFile = 3`
(slot word = `8 + ordinal*9`). The disk-boot microcode reads only the **PV
root's** copy. On this fixture: `germ` → firstLink VDA 87, `bootFile` → firstLink
VDA 119, `microcode`/`checkpoint` empty.

### The boot chain — per-page label `dontCare`

Each 10-word sector label is:

| Label words | Field |
|---|---|
| 0..4 | `fileID` (5 words) |
| 5..6 | `filePage` (INT, the 0-based page index within the file) |
| 7 | `attributes` (`9730` = data) |
| 8..9 | `dontCare` = **`bootChainLink`** (the boot-path forward pointer) |

To read a boot file: start at `firstLink`, and at each page validate
`label.fileID == slot.fID` and `label.filePage == firstPage + i`, then advance:

- `dontCare == [0, 0]` → next page is **VDA + 1** (interior of a contiguous run)
- `dontCare == [lo, hi]` (else) → next page is **VDA = lo | hi<<16** (run break)
- `dontCare == [0xFFFF, 0xFFFF]` → **end of file** (`DiskBootTransfer.mc`)

This fixture is laid out as two contiguous runs, so every interior `dontCare` is
`[0,0]` and only the final page of each file carries `[0xFFFF,0xFFFF]`. The
follower in `pilot::read_boot_file` performs this exact walk — a successful read
is the structural proof the install is well-formed.

## Re-verifying the fixture

`pilot_probe verify` audits structure (seals + VAM), the `bootingInfo` slots, the
boot chains, byte-exactness against the canonical sources, and a PDI writer
round-trip. It exits non-zero on any failure.

```sh
cargo run --example pilot_probe -- verify CedarDorado-boot.pdi \
    germ=/path/to/Dorado.germ!4 \
    bootfile=/path/to/BasicCedarDorado.boot!22
```

Current output on the shipped fixture (abridged) — **PASS**:

```
VERIFY  CedarDorado-boot.pdi  (745312 bytes)
  PV label : "CedarDorado"   LV label : "CedarDorado"  type=3
  free pages : 220   VAM : 220 free (agrees with labels)
  bootingInfo @PV-root word 8 (slot stride 9 words):
    germ      : fileID 00020000  firstLink VDA 87   (32 pages,  VDA 87..=118)   chain OK
                vs Dorado.germ!4: IDENTICAL (16384 bytes)
    bootFile  : fileID 00030000  firstLink VDA 119  (1060 pages, VDA 119..=1178) chain OK
                vs BasicCedarDorado.boot!22: IDENTICAL (542720 bytes)
  round-trip: write_pdi reproduces the file byte-for-byte (745312 bytes)
  ==> PASS
```

The synthetic install/extract/chain contract (including a fragmented run break)
is pinned in CI by `install_boot_file_round_trips_via_chain` and
`boot_chain_follows_run_breaks` in `src/fs/alto/pilot.rs`.

## Honest scope — what's missing for an actual boot

This is the **best structural fixture** for the germ disk-boot path, but a fully
bootable, fully-installed disk needs content/work we cannot synthesize here:

1. **Initial microcode region (Stage A)** — hardware-addressed, clone-only. Absent.
2. **`bootingInfo[microcode]` soft-microcode** — the Cedar microcode we have is
   `EB` format; the install path wants `MB` framing, so the slot is left empty.
3. **A fuller world / SYSOUT** — BasicCedar is the core Cedar (editor included);
   the heavyweight dev tools (Compiler, Binder) and apps are separate `.bcd`
   packages not present as bits, and a from-nothing OS image is gated on real
   source.
4. **The emulator can't boot a disk yet** — the `xerox-dorado` disk read
   data-path (FIFO read stream, sequence-PROM/ECC/timing) is a stub and the Mesa
   VM is mid-bringup. Even a perfect disk won't boot until that lands; that work
   is on the emulator, not on producing this disk.

Named user files (apps/games) would additionally need the Cedar `client`
FS/PFS name-directory B-tree (`rootFile[8]`) writer, which is not yet
implemented — the nucleus has no name directory, so boot-installed files like the
germ/boot surface only by FileID, not by name.
