//! Container-decode layer for vintage floppy / disk image wrappers that
//! encode per-track geometry or compression on top of the raw sector stream.
//!
//! A container is conceptually:
//!
//! ```text
//! wrapped bytes (.msa / .td0 / .edsk / …)  →  flat 512-byte-sector stream
//! ```
//!
//! Each decoder in this module exposes a `Read + Seek + Send` view over the
//! decoded sector stream so the partition + filesystem layers downstream see
//! a normal raw image. This is the same model `ChdReader` uses for `.chd`
//! files; container decoders just live in their own sub-module for
//! organisation.
//!
//! Dispatch is content-magic-first via [`detect_container_kind`]; the file
//! extension is used only as a tiebreaker for formats whose magic is weak or
//! absent (e.g. raw `.st` Atari floppies have no header to sniff).
//!
//! Per the MiSTer-FS plan (§3.2 of `docs/mister_filesystem_implementation_
//! plan.md`), all decoders are ported MIT source or hand-written; no new
//! crate dependency.

pub mod d88;
pub mod edsk;
pub mod msa;
pub mod sector_order;

use std::io::{Cursor, Read, Seek};
use std::path::Path;

use anyhow::Result;

use super::ReadSeek;

/// Container formats we can decode into a flat sector stream. `Raw` is the
/// passthrough — the input is already a flat sector stream and needs no
/// decoding. Add new variants here as decoders land.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerKind {
    /// Atari ST MSA (Magic Shadow Archiver) — `$0E0F` magic, per-track RLE.
    Msa,
    /// CPCEMU DSK / EDSK — Amstrad CPC / PCW / Einstein / Oric / etc.
    /// CP/M floppies. Either "MV - CPCEMU Disk-File" (DSK) or
    /// "EXTENDED CPC DSK File" (EDSK) header magic.
    Edsk,
    /// Sharp `.d88` floppy container — X68000 / PC-88 / PC-98 / MSX /
    /// FM-7. 32-byte disk-info header + 164-entry track-offset table.
    D88,
    /// Pass-through: the bytes are already a flat sector stream.
    Raw,
}

impl ContainerKind {
    /// Plain-ASCII display name for log lines / inspect rows.
    pub fn display_name(self) -> &'static str {
        match self {
            ContainerKind::Msa => "Atari MSA",
            ContainerKind::Edsk => "CPCEMU DSK/EDSK",
            ContainerKind::D88 => "Sharp .d88",
            ContainerKind::Raw => "Raw",
        }
    }
}

/// Sniff the container kind from the first few bytes. Returns `Raw` for any
/// input we don't recognise — the caller can then pass the file through to
/// the partition layer untouched.
///
/// `path` is an optional hint used only to tiebreak against extension when
/// the magic is empty or ambiguous (none today, but kept for future
/// container formats that lack a header).
pub fn detect_container_kind(head: &[u8], _path: Option<&Path>) -> ContainerKind {
    if msa::looks_like_msa_header(head) {
        return ContainerKind::Msa;
    }
    if edsk::looks_like_edsk_header(head) {
        return ContainerKind::Edsk;
    }
    if d88::looks_like_d88_header(head) {
        return ContainerKind::D88;
    }
    ContainerKind::Raw
}

/// Open a container from raw bytes. If the bytes look like a recognised
/// wrapper they are decoded into a flat sector stream wrapped in a `Cursor`;
/// otherwise the input passes through verbatim.
///
/// Memory-resident: floppy containers are tiny (a 1.44 MB MSA decodes to
/// at most ~1.5 MB raw). Hard-disk containers, if any are added later, may
/// need a streaming variant.
pub fn open_container_bytes(
    bytes: Vec<u8>,
    path_hint: Option<&Path>,
) -> Result<(ContainerKind, Box<dyn ReadSeek>)> {
    let kind = detect_container_kind(&bytes[..bytes.len().min(64)], path_hint);
    match kind {
        ContainerKind::Msa => {
            let flat = msa::decode_msa_bytes(&bytes)?;
            Ok((ContainerKind::Msa, Box::new(Cursor::new(flat))))
        }
        ContainerKind::Edsk => {
            let flat = edsk::decode_edsk_bytes(&bytes)?;
            Ok((ContainerKind::Edsk, Box::new(Cursor::new(flat))))
        }
        ContainerKind::D88 => {
            let flat = d88::decode_d88_bytes(&bytes)?;
            Ok((ContainerKind::D88, Box::new(Cursor::new(flat))))
        }
        ContainerKind::Raw => Ok((ContainerKind::Raw, Box::new(Cursor::new(bytes)))),
    }
}

/// Open a container from a `Read + Seek` source by buffering everything into
/// memory. Convenience wrapper around [`open_container_bytes`]. Use the
/// bytes form when you already have the buffer; this one is for callers
/// that hand us a file or `Cursor`.
pub fn open_container_reader<R: Read + Seek>(
    mut reader: R,
    path_hint: Option<&Path>,
) -> Result<(ContainerKind, Box<dyn ReadSeek>)> {
    use std::io::SeekFrom;
    reader.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    open_container_bytes(bytes, path_hint)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_passthrough_round_trips() {
        let payload = vec![0xABu8; 4096];
        let (kind, mut reader) = open_container_bytes(payload.clone(), None).unwrap();
        assert_eq!(kind, ContainerKind::Raw);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, payload);
    }

    #[test]
    fn detect_container_kind_unknown_is_raw() {
        let head = [0xCA, 0xFE, 0xBA, 0xBE, 0, 0, 0, 0];
        assert_eq!(detect_container_kind(&head, None), ContainerKind::Raw);
    }
}
