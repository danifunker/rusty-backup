# crusty-backup (`cb-dos`)

C-based, DOS-native offshoot of rusty-backup: back up / restore FAT16/32 + NTFS
disks **on the vintage machine itself**, with gzip compression and (phased)
defrag, producing the desktop tool's **native PerPartition backup format** (so
rusty-backup / rb-cli can restore + resize the result).

This is an independent C/DJGPP program — it shares a **file format** with the
Rust codebase, not code. Full scope + plan: [`../docs/cb_dos.md`](../docs/cb_dos.md).

## Status

Phase 0a (UI/size POC) + Phase 0b (disk spike) — both DOSBox-X-verified.
Current artifacts:

- `src/disk_spike.c` — Phase 0b "hello disk" spike. Enumerates BIOS drives
  (`int 13h AH=08h`), checks LBA extensions (`AH=41h`), reads sectors with
  ext (`AH=42h`) + CHS (`AH=02h`) fallback through a DPMI DOS-memory transfer
  buffer, classifies sector 0 (MBR vs FAT/NTFS boot sector), dumps the partition
  table, and parses the volume:
  - **FAT12/16/32** — parses the BPB and walks the FAT counting used/free/bad
    clusters.
  - **NTFS** — parses the BPB, reads MFT record 6 (`$Bitmap`) with fixup,
    decodes the `$DATA` runs, and counts allocated clusters. (exFAT detected,
    not parsed.)
  Writes findings to both the screen and `C:\SPIKE.LOG`. Cluster counts are
  bit-exact vs an independent host scan for both a superfloppy and an
  MBR-partitioned disk (FAT16 and NTFS).
- `src/lfn_test.c` — Phase 0b LFN write spike. Queries LFN support
  (`int 21h AX=71A0h`) and creates a long-named file via the raw LFN API
  (`AX=716Ch`). Correctly detects LFN *absence* (refuses rather than mangling);
  with LFN on, a long name round-trips verbatim to the host filesystem.
- `src/tui_poc.c` — lightweight keyboard-only text UI (conio + `int 16h`,
  double-buffered via `ScreenUpdate` so there's no flicker). Features:
  - scrollable disk/partition list with a bottom function-key action bar
  - multi-select: Space marks/unmarks (`[X]`), a marked disk covers its
    partitions (`[-]`); Enter executes on the marked set
  - DOS-red **About** box (F1; auto-shown ~8s at startup)
  - mock **Backup** (F2) / **Restore** (F3) screens with a selectable Mode/
    Sizing option, size + space-required fields, and an animated progress bar
    with elapsed/ETA + MB counter
  No mouse, no framework.

### Measured sizes (DJGPP gcc 12.2.0, `-Os -s`)

| Binary | Size | Notes |
|--------|------|-------|
| `tui_poc.exe` | **~115 KB** | full text-UI POC, stripped |
| UPX'd (`upx --best`) | **~57 KB** | self-extracts into RAM at launch |
| `disk_spike.exe` | **~106 KB** | Phase 0b disk spike, stripped |
| `lfn_test.exe` | **~102 KB** | Phase 0b LFN write spike, stripped |

Bare DJGPP hello-world is ~146 KB unstripped, for reference. Conclusion: size is
not a floppy-budget concern; the OS + drivers dominate, not cb-dos.

## Build

Requires the DJGPP cross-compiler (local install assumed at `~/djgpp`).

```sh
make            # build POC binaries into build/
make size       # build + report .exe sizes
make clean

# override the toolchain location if needed:
make DJGPP=/path/to/djgpp
```

To compress (optional, self-extracting): `upx --best build/tui_poc.exe`.

## Run / test

**Interactive (the TUI POC):**

```sh
./run-dosbox.sh                 # mounts build/ as C: and runs TUI_POC.EXE
```

Arrow keys move the selection; `F2`/`F3`/`F5` are stub actions; `F10`/`Esc` quit.
Override the emulator path with `DOSBOXX=/path/to/dosbox-x ./run-dosbox.sh`
(default: `/Applications/dosbox-x.app/Contents/MacOS/dosbox-x`).

**Headless (the disk / LFN spikes):** the spikes write their results to a log
file on the mounted `C:` (the host `build/` folder), so they can run
non-interactively with `dosbox-x -exit` and the output read back on the host.

```sh
# disk_spike.exe against a raw disk image attached as BIOS drive 0x80:
#   IMGMOUNT 2 <img> -fs none   ->  int 13h drive 0x80
dosbox-x -fastlaunch -exit \
  -c 'MOUNT C build' \
  -c 'IMGMOUNT 2 build/disks/HDD.IMG -fs none' \
  -c 'C:' -c 'DISKSPK.EXE' -c 'exit'
cat build/SPIKE.LOG

# lfn_test.exe (LFN must be enabled in DOSBox-X's integrated DOS — it
# reports DOS 5.0 and gates LFN off by default):
dosbox-x -fastlaunch -exit -set 'dos lfn=true' \
  -c 'MOUNT C build' -c 'C:' -c 'LFNTEST.EXE' -c 'exit'
cat build/LFN.LOG
```

Test disk images live under `build/disks/` (gitignored). A partitioned FAT16
disk can be built on the host by wrapping a bare FAT volume in an MBR; see the
2026-06-19 entry in [`../docs/cb_dos.md`](../docs/cb_dos.md). Real-486 and
booted-FreeDOS validation are still pending.
