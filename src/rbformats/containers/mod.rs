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
pub mod dim;
pub mod edsk;
pub mod floppy_geom;
pub mod hdf;
pub mod hdm;
pub mod msa;
pub mod sector_order;
pub mod xdf;

use std::io::{Cursor, Read, Seek};
use std::path::Path;

use anyhow::{Context, Result};

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
    /// X68000 XDF — raw headerless flat-sector dump. Geometry inferred
    /// from file size. Routed by extension; the decoder fails if the
    /// size doesn't match a supported floppy geometry.
    Xdf,
    /// PC-98 / DiskExplorer HDM — byte-identical to XDF on disk; kept as
    /// a distinct variant so the routing and log labels track the user's
    /// chosen extension.
    Hdm,
    /// DiskExplorer DIM — 256-byte header + payload. Read path handles
    /// both DIFC (signed `DIFC HEADER` at byte 0xAB) and generic
    /// `header + flat` variants; write path always emits DIFC.
    Dim,
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
            ContainerKind::Xdf => "X68000 XDF",
            ContainerKind::Hdm => "PC-98 HDM",
            ContainerKind::Dim => "DiskExplorer DIM",
            ContainerKind::Raw => "Raw",
        }
    }
}

/// Sniff the container kind from the first few bytes. Returns `Raw` for any
/// input we don't recognise — the caller can then pass the file through to
/// the partition layer untouched.
///
/// `path` is an optional hint used to tiebreak against file extension when
/// the magic is absent (XDF / HDM — raw headerless dumps with size-based
/// geometry inference) or weak. Magic-strong formats win first.
///
/// **Head buffer size:** callers should pass at least 256 bytes so DIM's
/// DIFC signature (at offset 0xAB) can be detected. The `open_container_*`
/// helpers slice that much when invoking this function.
///
/// **Note:** the headerless floppy formats (XDF and friends) need the full
/// file *length* to validate the geometry, not just a header window. The
/// detection here returns the bucket; the decoder validates the size and
/// returns a typed error if it's wrong.
pub fn detect_container_kind(head: &[u8], path: Option<&Path>) -> ContainerKind {
    if msa::looks_like_msa_header(head) {
        return ContainerKind::Msa;
    }
    if edsk::looks_like_edsk_header(head) {
        return ContainerKind::Edsk;
    }
    if d88::looks_like_d88_header(head) {
        return ContainerKind::D88;
    }
    if dim::looks_like_dim_header(head) {
        return ContainerKind::Dim;
    }
    // Extension tiebreak for headerless raw floppy formats and generic DIM.
    if let Some(ext) = path.and_then(|p| p.extension()).and_then(|e| e.to_str()) {
        if ext.eq_ignore_ascii_case("xdf") {
            return ContainerKind::Xdf;
        }
        if ext.eq_ignore_ascii_case("hdm") {
            return ContainerKind::Hdm;
        }
        if ext.eq_ignore_ascii_case("dim") {
            return ContainerKind::Dim;
        }
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
    // 256 bytes is enough to see the DIFC signature at offset 0xAB; smaller
    // formats only consult their first dozen bytes.
    let kind = detect_container_kind(&bytes[..bytes.len().min(256)], path_hint);
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
        ContainerKind::Xdf => {
            let (flat, _media) = xdf::decode_xdf_bytes(&bytes)?;
            Ok((ContainerKind::Xdf, Box::new(Cursor::new(flat))))
        }
        ContainerKind::Hdm => {
            let (flat, _media) = hdm::decode_hdm_bytes(&bytes)?;
            Ok((ContainerKind::Hdm, Box::new(Cursor::new(flat))))
        }
        ContainerKind::Dim => {
            let (flat, _media) = dim::decode_dim_bytes(&bytes)?;
            Ok((ContainerKind::Dim, Box::new(Cursor::new(flat))))
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

/// Floppy-container conversion engine: shared by `rb-cli floppy convert`,
/// the GUI's `FloppyConvertDialog`, and any future caller. Reads `in_path`,
/// detects/decodes the source format, encodes to the format implied by the
/// `target` argument, and writes the result to `out_path`.
///
/// Identity conversions (source kind == target) byte-copy the file instead
/// of round-tripping through the flat-stream intermediate.
pub fn convert_floppy_container(
    in_path: &Path,
    out_path: &Path,
    target: ContainerKind,
) -> Result<ConvertReport> {
    let bytes =
        std::fs::read(in_path).with_context(|| format!("reading source {}", in_path.display()))?;
    let head_window = bytes.len().min(256);
    let source = detect_container_kind(&bytes[..head_window], Some(in_path));

    if !is_floppy_container(source) {
        anyhow::bail!(
            "input {} is not a recognised floppy container (got {})",
            in_path.display(),
            source.display_name()
        );
    }
    if !is_floppy_container(target) {
        anyhow::bail!(
            "target {} is not a floppy container — expected one of XDF, HDM, DIM, D88",
            target.display_name()
        );
    }

    let (flat, media) = decode_any_floppy(&bytes, source)?;

    if source == target {
        std::fs::copy(in_path, out_path).with_context(|| {
            format!(
                "identity copy {} -> {}",
                in_path.display(),
                out_path.display()
            )
        })?;
        return Ok(ConvertReport {
            source,
            target,
            media,
            identity: true,
            bytes_written: bytes.len() as u64,
        });
    }

    let encoded = encode_any_floppy(&flat, media, target)?;
    std::fs::write(out_path, &encoded)
        .with_context(|| format!("writing target {}", out_path.display()))?;
    Ok(ConvertReport {
        source,
        target,
        media,
        identity: false,
        bytes_written: encoded.len() as u64,
    })
}

/// Returned by [`convert_floppy_container`] so the caller can log a single
/// human-friendly summary line.
#[derive(Debug, Clone)]
pub struct ConvertReport {
    pub source: ContainerKind,
    pub target: ContainerKind,
    pub media: floppy_geom::FloppyMedia,
    pub identity: bool,
    pub bytes_written: u64,
}

/// True if `kind` is one of the four floppy containers handled by
/// [`convert_floppy_container`].
pub fn is_floppy_container(kind: ContainerKind) -> bool {
    matches!(
        kind,
        ContainerKind::Xdf | ContainerKind::Hdm | ContainerKind::Dim | ContainerKind::D88
    )
}

/// Pick the floppy [`ContainerKind`] implied by an output file extension.
/// Returns an error for any extension not in the four-format set.
pub fn floppy_kind_from_extension(path: &Path) -> Result<ContainerKind> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "output path {} has no extension; cannot infer floppy format",
                path.display()
            )
        })?;
    match ext.as_str() {
        "xdf" => Ok(ContainerKind::Xdf),
        "hdm" => Ok(ContainerKind::Hdm),
        "dim" => Ok(ContainerKind::Dim),
        "d88" => Ok(ContainerKind::D88),
        other => anyhow::bail!(
            "output extension .{other} is not a floppy container \
             (expected .xdf / .hdm / .dim / .d88)"
        ),
    }
}

fn decode_any_floppy(
    bytes: &[u8],
    kind: ContainerKind,
) -> Result<(Vec<u8>, floppy_geom::FloppyMedia)> {
    match kind {
        ContainerKind::Xdf => xdf::decode_xdf_bytes(bytes),
        ContainerKind::Hdm => hdm::decode_hdm_bytes(bytes),
        ContainerKind::Dim => dim::decode_dim_bytes(bytes),
        ContainerKind::D88 => {
            let flat = d88::decode_d88_bytes(bytes)?;
            let media = floppy_geom::require_media_from_size(flat.len())?;
            Ok((flat, media))
        }
        other => anyhow::bail!(
            "decode_any_floppy: {} is not a floppy container",
            other.display_name()
        ),
    }
}

fn encode_any_floppy(
    flat: &[u8],
    media: floppy_geom::FloppyMedia,
    target: ContainerKind,
) -> Result<Vec<u8>> {
    let geom = media.geometry();
    match target {
        ContainerKind::Xdf => xdf::encode_xdf_bytes(flat, geom),
        ContainerKind::Hdm => hdm::encode_hdm_bytes(flat, geom),
        ContainerKind::Dim => dim::encode_dim_bytes(flat, geom),
        ContainerKind::D88 => {
            let d88_media = match media {
                floppy_geom::FloppyMedia::Hd1232 | floppy_geom::FloppyMedia::Hd1440 => {
                    d88::D88Media::Dd2hd
                }
                floppy_geom::FloppyMedia::Dd720 | floppy_geom::FloppyMedia::Dd640 => {
                    d88::D88Media::Dd2dd
                }
            };
            d88::encode_d88_bytes(
                flat,
                geom.cyls,
                geom.heads,
                geom.spt,
                geom.sec_size as usize,
                d88_media,
            )
        }
        other => anyhow::bail!(
            "encode_any_floppy: {} is not a floppy container",
            other.display_name()
        ),
    }
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

    #[test]
    fn detect_container_kind_xdf_by_extension() {
        use std::path::PathBuf;
        // Head that does NOT match any existing magic. Byte 0x1B is the D88
        // media-type byte — picking 0xFE (a typical FAT BPB media descriptor)
        // keeps the D88 sniffer from claiming this buffer.
        let mut head = [0u8; 64];
        head[0x1B] = 0xFE;
        assert_eq!(detect_container_kind(&head, None), ContainerKind::Raw);
        let path = PathBuf::from("disk.xdf");
        assert_eq!(
            detect_container_kind(&head, Some(path.as_path())),
            ContainerKind::Xdf
        );
        let path_upper = PathBuf::from("DISK.XDF");
        assert_eq!(
            detect_container_kind(&head, Some(path_upper.as_path())),
            ContainerKind::Xdf
        );
    }

    #[test]
    fn open_container_bytes_decodes_xdf() {
        use crate::rbformats::containers::floppy_geom::FloppyMedia;
        let geom = FloppyMedia::Hd1232.geometry();
        // Pattern where byte 0x1B is 0xFE so the D88 sniff doesn't claim it.
        let pattern: Vec<u8> = (0..geom.flat_size())
            .map(|i| if i == 0x1B { 0xFE } else { (i & 0xFF) as u8 })
            .collect();
        let path = std::path::PathBuf::from("disk.xdf");
        let (kind, mut reader) = open_container_bytes(pattern.clone(), Some(&path)).unwrap();
        assert_eq!(kind, ContainerKind::Xdf);
        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).unwrap();
        assert_eq!(decoded, pattern);
    }

    #[test]
    fn open_container_bytes_rejects_wrong_size_xdf() {
        let path = std::path::PathBuf::from("disk.xdf");
        // 1000 bytes, byte 0x1B = 0xFE so D88 sniff doesn't capture it.
        let mut bytes = vec![0u8; 1000];
        bytes[0x1B] = 0xFE;
        // `Ok` payload contains `Box<dyn ReadSeek>` which is not `Debug`, so we
        // hand-pattern the result instead of `unwrap_err`.
        let err = match open_container_bytes(bytes, Some(&path)) {
            Ok(_) => panic!("expected decode failure"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("no supported floppy geometry"),
            "unexpected error: {err}"
        );
    }
}
