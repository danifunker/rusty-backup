//! Tape images, one Cargo feature per format. Today: `sun2tape`, Sun QIC tapes.
//!
//! - [`sun2tape`]: browse a `SUN2TAPE` `.qic` image as one tree (tar sections, `compress(1)`-ed
//!   tar sections, and UFS miniroots).
//! - [`sun2tape_edit`]: edit it (add, mkdir, rename, delete, chmod, chown), rebuilding the tape.
//! - [`tar_section`]: an editable tar archive held in memory, for the tape's tar sections.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

#[cfg(feature = "sun2tape")]
pub mod sun2tape;
#[cfg(feature = "sun2tape")]
pub mod sun2tape_edit;
#[cfg(feature = "sun2tape")]
pub mod tar_section;
