# fs-8bit

Read and edit the disk filesystems of 8-bit home computers. Each filesystem is a Cargo feature, so you only build the ones you use. All are on by default.

| Feature | Filesystem | Machines |
|---|---|---|
| `acorn` | Acorn DFS | BBC Micro, Acorn Electron |
| `andos` | ANDOS | Elektronika BK0011M |
| `atari-dos` | Atari DOS 2.0S / 2.5 | Atari 400/800/XL/XE |
| `cbm` | Commodore DOS | 1541 / 1571 / 1581 drives, PET 8050 / 8250 |
| `coco` | RS-DOS (Disk BASIC), DragonDOS | TRS-80 Color Computer, Dragon 32/64 |
| `lif` | HP LIF volumes (read, edit), keeping types, implementation words and the IPL pointer | HP 9000, HP-UX / PA-RISC boot areas |
| `oric` | Oric Jasmin | Oric-1 / Atmos / Telestrat |
| `os9` | OS-9 / NitrOS-9 RBF (enables `coco`) | CoCo, Dragon and other 6809 OS-9 systems |
| `qdos` | QDOS QXL.WIN hard-disk volumes (read, edit); Microdrive `.mdv` detection | Sinclair QL (QXL, QL-SD, MiSTer) |
| `ti99` | TI-99/4A disk | TI-99/4A |
| `trdos` | TR-DOS | ZX Spectrum (Beta Disk) |

Read support, write support and fsck vary by filesystem; see each module's documentation.

More families (Acorn ADFS, QDOS, HP LIF) move here from rusty-backup after their license review and test untangling.

Every filesystem implements the traits from [`disk-core`](../disk-core), so code written against `disk_core::filesystem::Filesystem` works with all of them.

```toml
[dependencies]
fs-8bit = { version = "0.1", default-features = false, features = ["cbm"] }
```

Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup). MIT licensed.
