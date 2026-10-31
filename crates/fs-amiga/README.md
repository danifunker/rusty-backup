# fs-amiga

Classic Amiga filesystems, one Cargo feature each:

- **`affs`:** AmigaDOS OFS / FFS (`DOS\0` to `DOS\7`, including international and directory-cache modes). Read, edit and a Disk Validator style check and repair. Files keep their protection bits and comments.
- **`pfs3`:** Professional File System 3 (`PFS\3`, `PDS\3`, muFS). Read, edit, check, and a defragmenting clone into a new volume.
- **`sfs`:** Smart File System (`SFS\0`, `SFS\2`). Read and edit at any B-tree depth, and check.

Each implements the `disk-core` filesystem traits and can format a blank volume. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

Licensed `MIT AND BSD-4-Clause`. The PFS3 driver carries pfs3aio's 4-clause BSD notice; see `THIRD_PARTY_NOTICES.md`.
