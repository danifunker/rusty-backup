//! QNX filesystems, each behind its own Cargo feature.
//!
//! | Feature | Module | Filesystem |
//! |---|---|---|
//! | `qnx2` | [`qnx2`] | QNX 2 (Unisys ICON / LEXICON), read/write, over the `qnx2fs` crate |
//! | `qnx4` | [`qnx4`] | QNX 4 (also Neutrino 6.0–6.3 `fs-qnx4`), read-only |
//! | `qnx6` | [`qnx6`] | QNX 6 Power-Safe (Neutrino 6.4+), read-only |

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

#[cfg(feature = "qnx2")]
pub mod qnx2;
#[cfg(feature = "qnx4")]
pub mod qnx4;
#[cfg(feature = "qnx6")]
pub mod qnx6;
