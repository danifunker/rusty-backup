//! APFS, read-only, in [`apfs`]. With the `crypto` feature, [`apfs_crypto`] unlocks
//! FileVault-encrypted volumes from a password.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod apfs;
#[cfg(feature = "crypto")]
pub mod apfs_crypto;
