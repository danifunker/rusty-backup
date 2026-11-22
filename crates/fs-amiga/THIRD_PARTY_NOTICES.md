# Third-party notices

`fs-amiga` is MIT licensed (see `LICENSE`). It contains no third-party code. The drivers were written from the on-disk formats, and the reference drivers below were used only to learn those formats and to cross-check behaviour.

## PFS3

The PFS3 driver (`src/pfs3.rs`, `src/pfs3_fsck.rs`, `src/pfs3_clone.rs`) follows the PFS3 on-disk format, as documented by pfs3aio (BSD-4-Clause, Michiel Pelt). Until 0.1.2 this crate also carried pfs3aio's licence as a precaution. A side-by-side comparison in 2026-11 found that nothing was translated: the allocators, directory and anode handling, and the formatter are all independent designs. So 0.2.0 drops the notice, and the comments now describe the format rather than citing pfs3aio's files.

## Smart File System (SFS)

The SFS driver (`src/sfs.rs`, `src/sfs_fsck.rs`) follows the on-disk format of the SmartFilesystem sources, which were released as freeware without licence terms. Only format facts are used. The object-node tree growth and the admin-space growth were rewritten in 0.2.0 from the on-disk rules, because their earlier structure followed the driver's functions closely. Nothing uses AROS `bitfuncs` (GPL-2.0-or-later).

## AFFS

The AFFS driver follows the AmigaDOS on-disk format (the AmigaDOS reference manual and the ADF FAQ). ADFlib and amitools were used as cross-checks only; no code was copied.
