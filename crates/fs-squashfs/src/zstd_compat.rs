//! zstd through whichever backend the `native-zstd` / `pure-zstd` feature picks.

use std::io;

#[cfg(not(any(feature = "native-zstd", feature = "pure-zstd")))]
compile_error!("enable one zstd backend: `native-zstd` (C libzstd) or `pure-zstd` (pure Rust)");

/// Decompress at most `max` bytes; more is an error (a bomb, or a misframed block).
#[cfg(feature = "native-zstd")]
pub(crate) fn decode_capped(data: &[u8], max: usize) -> io::Result<Vec<u8>> {
    use io::Read as _;
    let mut out = Vec::new();
    zstd::stream::read::Decoder::new(data)?
        .take(max as u64 + 1)
        .read_to_end(&mut out)?;
    if out.len() > max {
        return Err(disk_core::io_other(format!(
            "zstd output passes {max} bytes"
        )));
    }
    Ok(out)
}

/// Decompress at most `max` bytes. The pure decoder works in one shot, so a frame that
/// declares a larger content size is refused before it is decoded.
#[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
pub(crate) fn decode_capped(data: &[u8], max: usize) -> io::Result<Vec<u8>> {
    if frame_content_size(data).is_some_and(|n| n > max as u64) {
        return Err(disk_core::io_other(format!(
            "zstd frame declares more than {max} bytes"
        )));
    }
    let out = decode_all(data)?;
    if out.len() > max {
        return Err(disk_core::io_other(format!(
            "zstd output passes {max} bytes"
        )));
    }
    Ok(out)
}

/// The Frame_Content_Size a zstd frame header declares, when it has one.
#[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
fn frame_content_size(data: &[u8]) -> Option<u64> {
    if data.len() < 5 || data[..4] != [0x28, 0xB5, 0x2F, 0xFD] {
        return None;
    }
    let fhd = data[4];
    let single = fhd & 0x20 != 0;
    let fcs_len = match fhd >> 6 {
        0 if single => 1,
        0 => return None,
        1 => 2,
        2 => 4,
        _ => 8,
    };
    let at = 5 + usize::from(!single) + [0, 1, 2, 4][(fhd & 3) as usize];
    let b = data.get(at..at + fcs_len)?;
    let mut v = 0u64;
    for (i, &x) in b.iter().enumerate() {
        v |= (x as u64) << (8 * i);
    }
    Some(if fcs_len == 2 { v + 256 } else { v })
}

/// Compress a full buffer in one shot at `level`.
#[cfg(feature = "native-zstd")]
pub(crate) fn encode_all(data: &[u8], level: i32) -> io::Result<Vec<u8>> {
    zstd::encode_all(data, level)
}

/// Decompress a full in-memory zstd frame sequence.
#[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
pub(crate) fn decode_all(data: &[u8]) -> io::Result<Vec<u8>> {
    libzstd_bitexact_rs::decompress(data).map_err(|e| disk_core::io_other(e.to_string()))
}

/// Compress a full buffer in one shot at `level`.
#[cfg(all(not(feature = "native-zstd"), feature = "pure-zstd"))]
pub(crate) fn encode_all(data: &[u8], level: i32) -> io::Result<Vec<u8>> {
    libzstd_bitexact_rs::compress(data, level).map_err(|e| disk_core::io_other(e.to_string()))
}
