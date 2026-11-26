//! Norton Ghost `.GHO`/`.GHS` reader (sessions 5.5a + 5.5b + 5.5c + 5.6 + 5.6-span).
//!
//! Layered scope:
//! - **5.5a:** outer container header — 12-byte wrapper, optional 16-byte
//!   password verifier, optional Pascal-style description at offset 0xFF.
//! - **5.5b:** inner **record-stream parser** — `GhoRecordHeader`
//!   (10-byte: `u16 type | u16 marker | u32 magic | u16 body_len`),
//!   `find_inner_stream_start` (scan-forward for record magic, absorbs
//!   the per-version padding difference: 7.5 starts at sector 6, 11.5 at
//!   sector 11), `GhoRecordIter` walking the stream.
//! - **5.5c:** standalone Fast-LZ block decoder (`fast_lz_decompress` +
//!   `fast_lz_hash`) ported from the MIT-licensed clean-room reference
//!   `nyarime/gho`.
//! - **5.6:** **SECTOR-mode decode-to-temp** (`materialize_gho_to_temp`).
//!   SECTOR-mode (raw sector-by-sector) backups — uncompressed, zlib
//!   (`High`), or Fast-LZ (`Fast`) — decode to a raw disk image. Data
//!   starts at the sector after the last `FE EF` sub-header; compressed
//!   streams are `[u16 stored_len][block]` chunks (stored_len includes
//!   itself) terminated by a record header. Wired into the GUI via
//!   `prepare_disk_image_path`.
//!
//!   **File-aware mode** (boot-sector / FAT-entry / directory / file-extent
//!   metadata records, types 0x0017, 0x0004, 0x0102/3/4, interleaved with
//!   0x0002 cluster-data records) is rebuilt as well: `parse_gho_image`
//!   walks the stream, and [`GhoReader`] lays each partition out at its
//!   place behind a synthesized partition table.
//! - **5.6-span:** **multi-file span sets.**
//!   `discover_gho_span_set(picked)` walks the picked file's directory
//!   and returns the ordered set (primary + numbered spans), handling
//!   three corpus naming patterns: stem-prefix `.GHS` siblings (incl.
//!   8.3 truncation like `SECTOR.GHO` + `SECTO00N.GHS`), hyphenated
//!   stem-prefix, and `.GHO.NNN` numeric suffix. `SpanReader: Read +
//!   Seek` virtualises the chain — primary verbatim, container-header
//!   skipped on every continuation — so SECTOR-mode decode reads one
//!   continuous stream. The user can pick the primary OR any span
//!   sibling and the whole set decodes.
//! - **Passwords:** `gho_crypto` derives and checks the seed; the readers
//!   decrypt per record body (file-aware) or per 32 KiB chunk (SECTOR).
//!
//! Layout reverse-engineered from our own fixture corpus (12+ files
//! spanning Ghost 7.5 and 11.5, with every combination of compression /
//! image-type / password). See `docs/virtualization-formats.md` §5.5 for
//! the container-header confirmation table.
//!
//! Key 5.5b finding: the record stream is **structurally identical**
//! between Ghost 7.5 and 11.5 — same record sequence, same `body_len`
//! values record-for-record over the first 128 records of the matching
//! fixtures. Only the 2-byte `marker` field differs: 7.5 is always
//! `0x0000`; 11.5 is mostly `0x0000` with ~10% of records carrying
//! `0x95FD` (purpose TBD — likely a per-record class flag or chained CRC;
//! 5.6 will catalogue).

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{anyhow, bail, Context, Result};

use crate::gho_crypto::{gho_decrypt_body, gho_seed_from_password, gho_verify_seed};

mod emit;
mod fastlz;
mod file_aware;
mod header;
mod image;
mod ntfs;
mod reader;
mod record_types;
mod records;
mod span;
#[cfg(test)]
mod tests;

pub use emit::*;
pub use fastlz::*;
pub use file_aware::*;
pub use header::*;
pub use image::*;
use ntfs::*;
pub use reader::*;
pub use record_types::*;
pub use records::*;
pub use span::*;
