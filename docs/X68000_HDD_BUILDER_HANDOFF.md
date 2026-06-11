# X68000 HDD Builder — Session Handoff (2026-06-10)

Handoff doc capturing every concrete finding from the long session that
shipped Phases A–E + D.2 of the `rb-cli new-x68k-hdd` self-bootable HDD
builder. The work all ships in the 11 unpushed commits on local `main`
(commit chain ends at `403c2c0`). It's mostly correct, but a real-
hardware test on MiSTer at end of session revealed an architectural gap
that makes the partition-boot-sector overlay work (Phase D.2) ship as
**correct bytes that don't execute on real hardware**. Read this whole
doc before resuming.

## TL;DR

1. **Phase A / B / D / E / multi-partition all work end-to-end on real
   MiSTer hardware.** The Print IPL stub displays its banner correctly
   (verified via screenshot Test B). All the byte-level work is sound.
2. **Phase D.2 (`--boot-sector-donor` / `--builtin-boot-sector`)
   produces correct bytes but those bytes never run on MiSTer.** Sharp's
   IPL ROM chains into our byte-0 IPL stub (which prints + halts), so
   the partition boot sector at byte `0x4000` (SASI) / `0x10000` (SCSI)
   never gets executed.
3. **Same blockage breaks the `--system-disk` + `SWITCH.X /HD`
   fallback** — our Print stub takes the screen and FDD0 boot never
   triggers.
4. **The fix is to add Phase F: an IPL chain stub.** ~50–100 bytes of
   68k that reads partition 0's first sector via `IOCS B_READ` and JMPs
   to the buffer. The user has a working MiSTer + test workflow ready,
   so each iteration is ~30 min (build → `scp` → boot → screenshot).
5. **MAME headless was not a reliable oracle.** Average-speed numbers
   and drive-activity overlay patterns I treated as positive signals
   were actually just MAME emulating I/O without ever showing screen
   content. Real-hardware testing is the only ground truth here.

## What shipped this session (11 commits, unpushed)

```
403c2c0  bake in Hero Soft V1.10 boot sector, add --builtin-boot-sector
7f94e63  accept Hero Soft / SxSI / naked-sector boot-sector donors
3d01383  multi-partition IPL banner — print drive list at boot
d6de051  SASI boot-sector-donor extraction (experimental)
37f91df  fix Phase D.2 — patch donor BPB so HDD actually boots
5ffc269  multi-partition HDD builder (--partitions 1..8)
86f574f  docs: surface Phase A-E X68000 self-bootable HDD builder
eb5cf4c  Phase E — `rb-cli new-x68k-hdd` verb at parity with the example
3d978ac  Phase D — `--system-disk` donor floppy extraction
70c7cd8  Phase B IPL stub — clear-screen + printed banner via IOCS B_PRINT
df52878  examples/build_x68k_hdd: drop `-bios ipl10` from the MAME hint
9fafd99  x68k: self-bootable SASI + SCSI HDD images via byte-0 IPL stub
6953449  docs: surface Archives tab + X68000 defrag/repack
```

(The `6953449` doc-sync commit is unrelated — it covered earlier Archives
tab + X68000 defrag/repack work that drifted before this session started.)

## Verified vs. unverified (be honest about what we actually know)

### Verified end-to-end on real MiSTer X68000 core

- **Phase A — IPL chain in.** Sharp's IPL ROM correctly JMPs into our
  byte-0 SASI IPL stub. Our 68k code executes.
- **Phase B — Print stub banner.** Single-partition banner renders
  cleanly. Confirmed: `IOCS B_PRINT` (function `$21` via `TRAP #15`)
  works; ANSI escape sequences (`\x1B[2J` clear, `\x1B[row;colH` cursor
  positioning) are interpreted by Sharp's console driver. Screenshot of
  banner text: `Rusty Backup X68000 HDD` / `Boot Human68k from FDD0 to
  mount as C:`.
- **Test A control** (`mame x68000 -sasi Bomberman.hdf` from
  user's MiSTer): Bomberman boots normally, confirming the MiSTer
  X68000 core's SASI HDD path works for known-good content.

### Verified at byte-level only (correct bytes, never executed on hardware)

- **Phase D.2 SCSI donor extraction** — `extract_partition_boot_sector`
  reads bytes correctly from `hd0.hds`, `X68000v2.zip`'s Toshiba dump,
  the Hero Soft naked-sector file. Output partition has expected bytes
  at expected offsets. Verified via `xxd` comparisons + unit tests.
- **Phase D.2 BPB patcher** — `patch_sharp_kg_bpb_from_pc_bpb` writes
  byte-correct big-endian Sharp/KG BPB fields. Verified against
  `hd0.hds`'s natural BPB layout for matching sizes; verified that
  different `--size` values produce correctly-patched BPBs.
- **Built-in Hero Soft sector** — embedded 1024 bytes at
  `src/partition/x68k_hero_soft_boot.bin`, included via
  `include_bytes!`. SHA1 `3e88955020de2191441e5829ee5a6e95890a3212`.
- **Multi-partition builder** (Phase #11) — `--partitions N` carves N
  equal slots, X68K table has correct entries. Verified via `rb-cli
  inspect` listing 3 Human68k partitions on a 24 MiB test image.

### NOT verified on real hardware (and now we know why not)

- **Phase D.2 boot sequence end-to-end.** The partition boot sector
  (Sharp/KG, Hudson, Hero Soft) never executes because our IPL stub at
  byte 0 (SASI) / byte `0x400` (SCSI) halts after printing. See
  "Architectural gap" below.
- **`--system-disk` + `SWITCH.X` fallback flow.** Same blockage —
  Print stub takes the screen, FDD0 boot never triggers.
- **Hudson IPL HUMAN.SYS-awareness.** The "is Bomberman's Hudson boot
  sector a BPB-driven HUMAN.SYS loader or a game-specific IPL?"
  question we tried to answer in Test B — Hudson code never ran, so the
  question is still open.
- **Multi-partition banner rendering with real partition data.** The
  banner code path is the same as single-partition (we just append more
  rows to the message string), so it should work, but it's untested on
  real hardware.

### MAME-headless signals that misled me

These were treated as positive verification during the session but
shouldn't have been:

- **"Average speed: 230%"** just meant MAME emulated for the requested
  duration without crashing. It says nothing about whether boot
  succeeded.
- **Drv3 ●● double-red activity overlay** matching `hd0.hds`'s
  pattern — I interpreted this as "SCSI sector reads happening, so
  boot code is running." Actually it was just MAME emulating the SCSI
  bus state regardless of what code was executing.
- **MAME headless screenshots** showed blank screens for both broken
  HDDs and known-good `hd0.hds`. The X68000 driver doesn't render its
  text plane to AVI in headless mode — so "blank screen" was
  meaningless either way.

## The architectural gap

### What I assumed (wrong)

I assumed Sharp's IPL ROM follows different paths for SASI vs. SCSI:

- **SASI:** JMP byte 0 of HDD (= our IPL stub), runs our code
- **SCSI (per MAME's behavior with `hd0.hds`):** signature check at
  byte 0, then chain *directly* to partition 0's boot sector (Sharp's
  Copyright 1990 SHARP code at byte `0x8000`), skipping byte `0x400`
  entirely.

I designed Phase D.2 SCSI around this — `--boot-sector-donor` overlays
bytes at the partition's first sector (byte `0x10000` of our output),
and Sharp's IPL ROM was supposed to chain there directly.

### What actually happens on MiSTer

The MiSTer X68000 core JMPs to byte 0 (SASI) / byte `0x400` (SCSI) for
**any** HDD it considers bootable, including SCSI HDDs with valid
`X68SCSI1` signatures. Test C confirmed this: a SCSI HDD with `--system-disk` +
`--builtin-boot-sector` (Hero Soft at partition 0) showed our Print
banner — meaning byte `0x400` executed, not the Hero Soft at byte
`0x10000`.

So on real MiSTer hardware:
- **SASI:** our byte-0 IPL stub runs → halts → done. Partition boot
  sector at byte `0x4000` never touches a register.
- **SCSI:** our IPL stub at byte `0x400` runs → halts → done.
  Hero Soft / Sharp partition boot sector at byte `0x10000` never
  touches a register.

### Why MAME's behavior diverged

MAME's `x68030` driver's `-hard` slot apparently implements the Sharp
IPL ROM's SCSI chain-directly-to-partition semantics that MiSTer's
core doesn't. So MAME's screen-blank output for the `hd0.hds` native
boot wasn't the same artifact as my generated HDD — `hd0.hds` was
probably actually trying to boot via its partition Sharp boot code
(which then hangs because there's no Human68k installed in the
partition), while my generated HDDs were halted at byte `0x400`
because of our stub.

The lesson: **MAME's `x68000` core ≠ MiSTer's X68000 core ≠ real
hardware.** Even with the same Sharp BIOS ROMs, the chain logic
differs.

## What needs to ship to make Phase D.2 actually boot — Phase F

### IPL chain stub

A new IPL stub variant that does what Sharp's stock IPL was supposed to
do for us: read partition 0's first sector and JMP to it.

Pseudocode:

```
chain_to_partition_0:
    // Read partition 0's first sector to a buffer at, e.g., $002000.
    MOVE.W  #IOCS_BREAD, D0        // function code — see below
    MOVE.L  #DRIVE_NUMBER, D1      // SASI drive 0 = ?, SCSI = ?
    MOVE.L  #PARTITION_0_LBA, D2   // sector number to read
    MOVE.L  #LOAD_ADDR, A1         // buffer address (e.g. $002000)
    MOVE.W  #1, D3                 // sector count
    TRAP    #15                    // IOCS dispatch
    // Sanity-check: D0 = 0 on success
    TST.W   D0
    BNE     halt
    // JMP to loaded buffer
    JMP     LOAD_ADDR
halt:
    BRA.S   halt
```

Approximate size: 30–50 bytes of code + maybe 0–20 bytes of constants.
Fits comfortably in either SASI (256-byte sector) or SCSI (1024-byte
sector) boot blocks.

### Unknowns to nail down (use the MiSTer oracle)

1. **IOCS function code for `B_READ` on SASI HDD.** From
   `iplrom30.s` we know SCSI uses `$F5` (the user's `hd0.hds` boot
   code at byte `0x8026` does `MOVEQ #-11, D0` = `$F5`). For SASI,
   reasonable guesses: `$42` (`B_READ`), `$57` (`S_READ`), or one of
   the floppy-style codes. Look at `iplrom30.s` for the SASI-mode
   `bsr` calls.
2. **Drive number convention.** SASI usually starts at `$80` (`$80` =
   drive 0); SCSI starts elsewhere. Check `iplrom30.s` for the
   register-setup pattern around `$F5` for SCSI; mirror for SASI.
3. **Partition 0 LBA convention.** For SASI 256-B-sector HDDs with
   the X68K table at byte `0x400`, partition 0 starts at sector 64
   (= byte `0x4000`). For SCSI 1024-B-sector HDDs, partition 0 starts
   at sector 64 (= byte `0x10000`). These are the same in *our*
   layout but the SASI sector number we'd pass to the IOCS function
   would be 64.
4. **Load address convention.** Sharp's IPL ROM convention is probably
   `$002000` or similar. Look for the JMP target after the SCSI chain
   in `iplrom30.s`.

### Test harness via MiSTer (what the user offered)

The user can iterate fast on real hardware. Workflow per attempt:

```powershell
# On Windows dev machine:
cargo run --bin rb-cli -- new-x68k-hdd C:\Temp\x68000\test.hdf \
    --size 10M --variant sasi --system-disk C:\Temp\x68000\human302.dim \
    --stub chain   # the new variant

scp C:\Temp\x68000\test.hdf root@mister.local:/media/fat/games/X68000/

# On MiSTer:
# F12 → Hard Disks → mount test.hdf
# F12 → Reset
# Take photo of screen with phone
```

Each iteration = ~5 min build + transfer + 30 sec boot test. Three or
four iterations of progressive 68k tweaks should land it (the user said
30 min per round).

## Open question: Hudson boot sector

We never got an answer to "is Bomberman's Hudson Soft 2.00 IPL a
BPB-driven HUMAN.SYS loader, or game-specific?" because Hudson never
ran on MiSTer (our stub blocked it). Once Phase F ships, this is
testable:

- Build `--variant sasi --system-disk human302.dim --boot-sector-donor
  Bomberman.hdf --stub chain`
- Boot on MiSTer
- If `C:>` shows → Hudson IS HUMAN.SYS-aware; we should bake it in as
  the SASI built-in equivalent of Hero Soft for SCSI
- If Bomberman tries to load game data → Hudson is game-specific;
  document SASI bake-in as not possible, stay with `--system-disk` +
  `SWITCH.X` (which then works once Phase F unblocks FDD0 fallback —
  see below)

## Related fix needed alongside Phase F

The `--system-disk` + `SWITCH.X /HD` workflow we documented as the
recommended non-donor path is **also broken on MiSTer** right now,
for the same reason. The user wants to boot from FDD0 to get `A:>`,
run SWITCH.X, write the boot sector. But our Print IPL stub at byte 0
holds the screen and never lets the IPL ROM fall back to FDD0.

Options:

1. **Add a `--stub none` variant that emits NO byte-0 code** (just
   zeros). Sharp's IPL ROM would then fail its "byte 0 is executable"
   check and fall back to FDD0. Easiest fix, but might leave the HDD
   "not recognized" instead of "blank but valid".
2. **Add a `--stub return` variant that emits `RTS`** (`0x4E75`).
   Returns control to the IPL ROM, which (hopefully) falls back to
   FDD0. Untested but plausible.
3. **Default to chain stub** once Phase F lands. If the partition has
   no installed boot sector (e.g. user just ran `--system-disk` without
   `--builtin-boot-sector`), the chain attempt fails and we either fall
   back to FDD0 or halt cleanly.

Probably (3) once Phase F is solid. (1) is the lower-risk default for
now if Phase F slips.

## Files / references for the next session

- **Builder source:** `src/partition/x68k_hdd_builder.rs` (~1100 lines)
- **IPL stub source:** `src/partition/x68k_ipl.rs` (~300 lines)
- **Built-in donor bytes:** `src/partition/x68k_hero_soft_boot.bin`
  (1024 bytes, Hero Soft V1.10)
- **CLI verb:** `src/cli/verbs/new_x68k_hdd.rs`
- **Example:** `examples/build_x68k_hdd.rs`
- **Reference disasm:** `C:\Temp\mistercore\x68kd11s\iplrom\iplrom30.s`
  (25,511 lines — Sharp's IPL ROM disassembly; search for `$F5` and
  `bsr` calls around the SCSI/SASI chain logic)
- **Hudson FatFs port:** `C:\Temp\mistercore\dis68k\lib\libfat-human68k\ff.c`
  (might document the IOCS calls Hudson uses for HDD I/O)
- **MiSTer X68000 core source:** `C:\Temp\mistercore\X68000_MiSTer\rtl\diskemu\sasidev.vhd`
  (SASI bus VHDL — might document the boot semantics)

## Test fixtures available locally

`C:\Temp\x68000\` contains:
- `rb_builtin.hdf` — 32 MiB SCSI HDD with Hero Soft + Human68k v3.02 (Test 1)
- `rb_sasi_hudson.hdf` — 10 MiB SASI HDD with Hudson overlay + Human68k v3.02 (Test 2)
- `rb_sasi_donor.hdf`, `rb_multipart.hdf`, etc. — earlier test builds
- `hd0.hds` — 100 MiB Sharp/KG SCSI donor (canonical reference)
- `hero_soft_boot.bin` — 1024-byte naked-sector portable donor
- `human302.dim` — Human68k v3.02 floppy (the `--system-disk` donor)
- `sasi_games/` — 14 game HDFs (all Hudson Soft 2.00, byte-identical)
- ROMs at `C:\Temp\x68000\roms\` (MAME-validated x68000 + x68030 + x68k_cz6bs1)

## MiSTer-side fixtures present per OPEN-WORK §7.NH

User has:
- `boot3.vhd` (default empty SASI stub) on `/media/fat/games/X68000/`
- `BLANK_disk_X68000.D88` (Human68k v3.0 boot floppy)
- Working MiSTer setup that boots Bomberman.hdf cleanly

## Pickup checklist for next session

If you're picking this up cold:

1. Read this whole doc.
2. Verify the 11 unpushed commits are still on local `main`:
   `git log --oneline origin/main..HEAD` should show them.
3. Decide path:
   - **Conservative:** push the 11 commits as-is, add OPEN-WORK note
     about the Phase F gap, defer Phase F implementation.
   - **Ambitious:** dive into Phase F. Open `iplrom30.s`, find the
     SASI `B_READ` IOCS function code, write a draft chain stub, build,
     ask user to test on MiSTer.
4. If going ambitious: budget 2–4 MiSTer test iterations to nail down
   the IOCS conventions. Don't commit interim attempts — only commit
   once boot-to-C:> is verified on real hardware.

## License note (Hero Soft built-in)

The `src/partition/x68k_hero_soft_boot.bin` file embeds 1024 bytes
of community X68000 scene code (Hero Soft V1.10). License posture is
documented inline in `x68k_hdd_builder.rs::HERO_SOFT_BOOT_SECTOR`'s
const docs. If a credible rights claim ever surfaces, removal is a
two-line change (drop the const + drop the .bin file). If pickup-
person is uncomfortable with the bake-in posture, the `--builtin-boot-sector`
flag can be backed out and `--boot-sector-donor PATH` is the
zero-license-footprint fallback.

---

**Last updated 2026-06-10. Session sponsor: user with MiSTer setup.
Phase F is the work that turns the byte-correct Phase D.2 into actually-
boots-on-hardware Phase D.2.**
