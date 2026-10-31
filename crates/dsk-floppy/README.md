# dsk-floppy

Wrappers that emulators and archives put around vintage floppy and hard-disk images, decoded to a flat sector image (and, for most, encoded back):

| Module | Container | Machines |
|---|---|---|
| `atr` | ATR | Atari 8-bit |
| `msa` | MSA (Magic Shadow Archiver) | Atari ST / TT / Falcon |
| `g64` | G64 / G71 GCR track images | Commodore 1541 / 1571 |
| `edsk` | EDSK / DSK | Amstrad CPC, Spectrum +3 |
| `d88` | D88 | Japanese computers (PC-88 / PC-98 / X1 / FM-7) |
| `dim` | DIM (DIFC header) | X68000, PC-98 |
| `xdf` | XDF | X68000 |
| `hdm` | HDM | PC-98 |
| `hdf` | HDF | Acorn ADFS hard disks |

EDSK and HDF are read-only; the others can be written as well. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed.
