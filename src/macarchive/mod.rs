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
//!
//! See `docs/native_mac_archives.md` for the format references and roadmap.

pub mod bitreader_be;
pub mod compactpro;
pub mod detect;
pub mod extract;
pub mod macbinary;
pub mod mar;
pub mod stuffit;
pub mod stuffit13_tables;
pub mod stuffit5;
pub mod stuffit_arsenic;
pub mod stuffit_huffman;
pub mod stuffit_lzah;
pub mod stuffit_lzh;
pub mod stuffit_lzw;
