//! Native readers for classic Macintosh archive formats.
//!
//! These archives are trees of Mac files, each preserving a data fork, a
//! resource fork, and Finder info (type/creator/flags). The reader exposes
//! entries that map cleanly onto the rest of rusty-backup's fork-aware tooling
//! (BinHex export, MacBinary, HFS import).
//!
//! - [`stuffit`] — classic StuffIt (`.sit`, `SIT!` magic) and StuffIt
//!   self-extracting archives (`.sea`).
//! - [`compactpro`] — Compact Pro (`.cpt`, and `.sea` self-extracting),
//!   commonly distributed BinHex-wrapped.
//! - [`mar`] — Hampa Hug's `mar` archive (`.mar`): a single Mac file or folder
//!   tree, read + written (stored).
//! - [`tome`] — Apple Installer Tome (type `idcp`, creator `kakc`): read only, stored,
//!   InstaCompOne and the text codec.
//!
//! See `docs/native_mac_archives.md` in rusty-backup for the format references and roadmap.
//!
//! Licensing is per file (each starts with an SPDX line): the StuffIt / Compact Pro
//! decompressors and parsers ported from XADMaster are LGPL-2.1-or-later, everything else is
//! MIT. See `THIRD_PARTY_NOTICES.md`.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod bitreader_be;
pub mod compactpro;
pub mod detect;
pub mod extract;
pub mod macbinary;
pub mod maczip;
pub mod mar;
pub mod shrinkwrap;
pub mod stuffit;
pub mod stuffit13_tables;
pub mod stuffit5;
pub mod stuffit_arsenic;
pub mod stuffit_huffman;
pub mod stuffit_lzah;
pub mod stuffit_lzh;
pub mod stuffit_lzw;
pub mod tome;
