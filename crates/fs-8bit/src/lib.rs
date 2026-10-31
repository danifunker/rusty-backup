//! Filesystems of 8-bit home computers, each behind its own Cargo feature.
//!
//! Every filesystem implements [`disk_core::filesystem::Filesystem`] (and
//! [`disk_core::filesystem::EditableFilesystem`] where writing is supported).
//!
//! | Feature | Module | Filesystem |
//! |---|---|---|
//! | `acorn` | [`dfs`] | Acorn DFS (BBC Micro) |
//! | `andos` | [`andos`] | ANDOS (Elektronika BK) |
//! | `atari-dos` | [`atari_dos`] | Atari DOS 2.0S / 2.5 |
//! | `cbm` | [`cbm`] | Commodore DOS (1541 / 1571 / 1581 / 8050 / 8250) |
//! | `coco` | [`rsdos`], [`dragondos`] | Color Computer RS-DOS, DragonDOS |
//! | `lif` | [`lif`] | HP LIF (Logical Interchange Format): HP-UX / PA-RISC boot areas, HP 9000 media |
//! | `oric` | [`oric`] | Oric Jasmin (Oric-1 / Atmos / Telestrat) |
//! | `os9` | [`os9`] | OS-9 / NitrOS-9 RBF (pulls in `coco`) |
//! | `qdos` | [`qdos`], [`qdos_mdv`] | Sinclair QL: QXL.WIN hard-disk volumes; Microdrive `.mdv` detection |
//! | `ti99` | [`ti99`] | TI-99/4A |
//! | `trdos` | [`trdos`] | TR-DOS (ZX Spectrum Beta Disk) |

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

#[cfg(feature = "andos")]
pub mod andos;
#[cfg(feature = "atari-dos")]
pub mod atari_dos;
#[cfg(feature = "cbm")]
pub mod cbm;
#[cfg(feature = "acorn")]
pub mod dfs;
#[cfg(feature = "coco")]
pub mod dragondos;
#[cfg(feature = "lif")]
pub mod lif;
#[cfg(feature = "oric")]
pub mod oric;
#[cfg(feature = "os9")]
pub mod os9;
#[cfg(feature = "qdos")]
pub mod qdos;
#[cfg(feature = "qdos")]
pub mod qdos_mdv;
#[cfg(feature = "coco")]
pub mod rsdos;
#[cfg(feature = "ti99")]
pub mod ti99;
#[cfg(feature = "trdos")]
pub mod trdos;
