# dsk-flux

Reads flux captures, which record every magnetic transition, and decodes them to a flat sector image. For each sector it uses whichever revolution has a valid CRC, and it reports the sectors it could not recover.

- **Floppy capture formats:** KryoFlux stream sets, SuperCard Pro / Greaseweazle `.scp`, and HxC `.hfe`.
- **Hard disks:** David Gesswein's MFM reader / emulator `.emu` captures of ST-506 drives.
- **Floppy encodings:** IBM MFM, IBM FM and AmigaDOS MFM, detected by probing a few tracks.

Read-only. No dependencies beyond `disk-core`'s shims. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed.
