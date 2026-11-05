//! Apple disk images: [`dmg`] (UDIF), [`ndif`] (the classic Mac OS format), [`sparseimage`],
//! and, with the `crypto` feature, [`dmg_crypto`] for encrypted images.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod dmg;
#[cfg(feature = "crypto")]
pub mod dmg_crypto;
pub mod ndif;
pub mod sparseimage;
