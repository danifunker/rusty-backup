//! `partition-N.gz.idx` — a pre-computed **seek layout** for a source-span
//! multi-member gzip backup member.
//!
//! A backup's `partition-N.gz` is written as a sequence of independent gzip
//! members, each covering ~[`GZ_SPAN_BYTES`] of *uncompressed* data. This sidecar
//! records, for each member, its uncompressed start offset and its byte offset
//! within the `.gz` file, so:
//!   - the `.cbk` packer can split the member into per-span chunks (each carrying
//!     the right `src_offset`) without decompressing, and
//!   - a lazy reader can jump straight to the member covering a wanted offset
//!     instead of decoding from byte 0.
//!
//! It is purely an accelerator: byte-identical to the `.gz` it describes,
//! regenerable, and optional — a missing or stale `.idx` just means a consumer
//! falls back to sequential decode. It is written only when a partition is large
//! enough to span more than one member; small partitions stay single-member and
//! get no `.idx` (byte-identical to the historical single-member output).
//!
//! Wire format (little-endian): magic `"RBGX"` u32, version u16, `span_count`
//! u32, then per span `uncompressed_offset` u64 + `compressed_offset` u64. The
//! compressed extent of the last span runs to EOF of the `.gz`.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

pub const GZ_IDX_MAGIC: u32 = 0x5242_4758; // "RBGX"
pub const GZ_IDX_VERSION: u16 = 1;

/// Uncompressed bytes per gzip member — the seek granularity. A deep seek
/// decompresses at most this much to reach its target within a member.
pub const GZ_SPAN_BYTES: u64 = 4 * 1024 * 1024;

/// One source-span gzip member: where it starts in the decompressed stream and
/// where its bytes begin in the `.gz` file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GzSpan {
    pub uncompressed_offset: u64,
    pub compressed_offset: u64,
}

/// The conventional `.idx` path for a `.gz` member (`foo.gz` -> `foo.gz.idx`).
pub fn gz_index_path(gz: &Path) -> PathBuf {
    let mut s = gz.as_os_str().to_owned();
    s.push(".idx");
    PathBuf::from(s)
}

/// True if `name` is a `.gz.idx` seek-layout sidecar.
pub fn is_gz_index_name(name: &str) -> bool {
    name.ends_with(".gz.idx")
}

/// Serialize a span table.
pub fn encode_gz_index(spans: &[GzSpan]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10 + spans.len() * 16);
    buf.extend_from_slice(&GZ_IDX_MAGIC.to_le_bytes());
    buf.extend_from_slice(&GZ_IDX_VERSION.to_le_bytes());
    buf.extend_from_slice(&(spans.len() as u32).to_le_bytes());
    for s in spans {
        buf.extend_from_slice(&s.uncompressed_offset.to_le_bytes());
        buf.extend_from_slice(&s.compressed_offset.to_le_bytes());
    }
    buf
}

/// Parse a span table (the inverse of [`encode_gz_index`]).
pub fn decode_gz_index(b: &[u8]) -> Result<Vec<GzSpan>> {
    if b.len() < 10 {
        bail!("gz index too short");
    }
    if u32::from_le_bytes(b[0..4].try_into().unwrap()) != GZ_IDX_MAGIC {
        bail!("bad gz index magic");
    }
    let ver = u16::from_le_bytes(b[4..6].try_into().unwrap());
    if ver != GZ_IDX_VERSION {
        bail!("unsupported gz index version {ver}");
    }
    let count = u32::from_le_bytes(b[6..10].try_into().unwrap()) as usize;
    let mut spans = Vec::with_capacity(count);
    let mut p = 10usize;
    for _ in 0..count {
        if p + 16 > b.len() {
            bail!("truncated gz index");
        }
        spans.push(GzSpan {
            uncompressed_offset: u64::from_le_bytes(b[p..p + 8].try_into().unwrap()),
            compressed_offset: u64::from_le_bytes(b[p + 8..p + 16].try_into().unwrap()),
        });
        p += 16;
    }
    Ok(spans)
}

/// Write a `.idx` sidecar.
pub fn write_gz_index(path: &Path, spans: &[GzSpan]) -> Result<()> {
    std::fs::write(path, encode_gz_index(spans))
        .with_context(|| format!("writing {}", path.display()))
}

/// Read a `.idx` sidecar.
pub fn read_gz_index(path: &Path) -> Result<Vec<GzSpan>> {
    let b = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    decode_gz_index(&b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let spans = vec![
            GzSpan {
                uncompressed_offset: 0,
                compressed_offset: 0,
            },
            GzSpan {
                uncompressed_offset: 4 << 20,
                compressed_offset: 1_234,
            },
            GzSpan {
                uncompressed_offset: 8 << 20,
                compressed_offset: 2_468,
            },
        ];
        let bytes = encode_gz_index(&spans);
        assert_eq!(decode_gz_index(&bytes).unwrap(), spans);
        assert!(decode_gz_index(b"nope").is_err());
    }
}
