# fs-xerox

Xerox Alto and D-machine (Dolphin / Dorado / Dandelion) filesystems:

- **Alto Basic File System (BFS):** read, write and repair.
- **Trident file system (TFS)** packs.
- **Pilot / Cedar** nucleus filesystem.
- Their disk-image containers: PARC Disk Image (PDI), Salto `.dsk`, Trident packs, CopyDisk streams, Dwarf `.zdisk`.

Implements the `disk-core` filesystem traits. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed. Copyright Dani Sarfati and Alan Steremberg.
