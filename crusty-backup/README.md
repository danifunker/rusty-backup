# crusty-backup (`cb-dos`)

C-based, DOS-native offshoot of rusty-backup: back up / restore FAT16/32 + NTFS
disks **on the vintage machine itself**, with gzip compression and (phased)
defrag, producing the desktop tool's **native PerPartition backup format** (so
rusty-backup / rb-cli can restore + resize the result).

This is an independent C/DJGPP program — it shares a **file format** with the
Rust codebase, not code. Full scope + plan: [`../docs/cb_dos.md`](../docs/cb_dos.md).

## Status

Phase 0a — proof of concept. Current artifacts:

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

No DOS emulator is installed locally yet. To run the POC, use DOSBox-X or 86Box
(or real hardware): boot DOS and run `TUI_POC.EXE`. Arrow keys move the
selection; `F2`/`F3`/`F5` are stub actions; `F10`/`Esc` quit.
