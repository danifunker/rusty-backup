//! Xerox Alto and D-machine filesystems, in the [`alto`] module: the Alto Basic File
//! System (BFS), the Trident file system (TFS), the Pilot / Cedar nucleus filesystem,
//! and the disk-image containers they come in (PDI, Salto `.dsk`, Trident packs,
//! CopyDisk streams, Dwarf `.zdisk`).

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod alto;
