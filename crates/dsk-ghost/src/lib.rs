//! Norton Ghost images, in [`gho`]; [`gho_crypto`] removes Ghost's password encryption.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod gho;
pub mod gho_crypto;
