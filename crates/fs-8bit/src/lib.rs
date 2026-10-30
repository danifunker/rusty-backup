//! Filesystems of 8-bit home computers, each behind its own Cargo feature.
//!
//! Every filesystem implements [`disk_core::filesystem::Filesystem`] (and
//! [`disk_core::filesystem::EditableFilesystem`] where writing is supported).
//!
//! | Feature | Module | Filesystem |
//! |---|---|---|
//! | `oric` | [`oric`] | Oric Jasmin (Oric-1 / Atmos / Telestrat) |

#[cfg(feature = "oric")]
pub mod oric;
