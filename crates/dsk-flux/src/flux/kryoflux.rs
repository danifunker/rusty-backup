//! KryoFlux stream files (`<name>NN.S.raw`, one per cylinder NN and side S).
//!
//! A stream is a byte code sequence (KryoFlux's published stream format):
//! `0x00-0x07` a two-byte flux value (`code << 8 | next`), `0x08-0x0A` padding of one to
//! three bytes in all, `0x0B` adds 0x10000 to the next value, `0x0C` a
//! three-byte value (`next << 8 | after`), `0x0D` an out-of-band block (type,
//! u16 length, payload; type `0x0D` ends the stream), and `0x0E-0xFF` a
//! one-byte value. Values count sample-clock ticks (`sck`, about 24.03 MHz).

use std::path::{Path, PathBuf};

use super::FluxTrack;

/// The KryoFlux sample clock, ((18.432 MHz * 73) / 14) / 2.
const SCK_HZ: f64 = 18_432_000.0 * 73.0 / 14.0 / 2.0;

/// Transition intervals of one stream file, in nanoseconds.
pub fn parse_stream(data: &[u8]) -> Vec<u32> {
    let mut out = Vec::with_capacity(data.len());
    let mut ovl = 0u32;
    let mut i = 0usize;
    let mut push = |ticks: u32, ovl: &mut u32| {
        out.push(((ticks + *ovl) as f64 * 1e9 / SCK_HZ) as u32);
        *ovl = 0;
    };
    while i < data.len() {
        let c = data[i];
        match c {
            0x00..=0x07 if i + 1 < data.len() => {
                push(((c as u32) << 8) | data[i + 1] as u32, &mut ovl);
                i += 2;
            }
            0x08 => i += 1,
            0x09 => i += 2,
            0x0A => i += 3,
            0x0B => {
                ovl += 0x1_0000;
                i += 1;
            }
            0x0C if i + 2 < data.len() => {
                push(((data[i + 1] as u32) << 8) | data[i + 2] as u32, &mut ovl);
                i += 3;
            }
            0x0D if i + 3 < data.len() => {
                if data[i + 1] == 0x0D {
                    break;
                }
                let len = u16::from_le_bytes([data[i + 2], data[i + 3]]) as usize;
                i += 4 + len;
            }
            0x0E..=0xFF => {
                push(c as u32, &mut ovl);
                i += 1;
            }
            _ => break,
        }
    }
    out
}

/// `(cylinder, side)` from a stream file name ending `NN.S.raw`.
pub fn track_of(path: &Path) -> Option<(u32, u32)> {
    let name = path.file_name()?.to_str()?;
    let stem = name
        .strip_suffix(".raw")
        .or_else(|| name.strip_suffix(".RAW"))?;
    let (rest, side) = stem.rsplit_once('.')?;
    let side: u32 = side.parse().ok().filter(|&s| s <= 1)?;
    let digits: String = rest
        .chars()
        .rev()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    if digits.len() < 2 {
        return None;
    }
    let cyl: u32 = digits.chars().rev().collect::<String>().parse().ok()?;
    Some((cyl, side))
}

/// Whether `path` is a KryoFlux stream: the name pattern, and an out-of-band block or flux code up front.
pub fn is_stream_path(path: &Path) -> bool {
    if track_of(path).is_none() {
        return false;
    }
    let mut head = [0u8; 16];
    let Ok(mut f) = std::fs::File::open(path) else {
        return false;
    };
    use std::io::Read;
    let n = f.read(&mut head).unwrap_or(0);
    n >= 4 && head[..n].contains(&0x0D)
}

/// Every stream file of the set `path` belongs to (same name before the `NN.S.raw`).
pub fn sibling_streams(path: &Path) -> Vec<(u32, u32, PathBuf)> {
    let Some(dir) = path.parent() else {
        return Vec::new();
    };
    let prefix = |p: &Path| -> Option<String> {
        let name = p.file_name()?.to_str()?;
        let stem = name
            .strip_suffix(".raw")
            .or_else(|| name.strip_suffix(".RAW"))?;
        let (rest, _) = stem.rsplit_once('.')?;
        Some(
            rest.trim_end_matches(|c: char| c.is_ascii_digit())
                .to_string(),
        )
    };
    let Some(want) = prefix(path) else {
        return Vec::new();
    };
    let mut out: Vec<(u32, u32, PathBuf)> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .filter(|p| prefix(p).as_deref() == Some(want.as_str()))
        .filter_map(|p| track_of(&p).map(|(c, s)| (c, s, p)))
        .collect();
    out.sort();
    out
}

/// Read and parse every stream of the set `path` belongs to.
pub fn read_set(path: &Path) -> std::io::Result<Vec<FluxTrack>> {
    sibling_streams(path)
        .into_iter()
        .map(|(cyl, head, p)| {
            Ok(FluxTrack {
                cyl,
                head,
                intervals_ns: parse_stream(&std::fs::read(&p)?),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_code_and_stops_at_eof() {
        // One-byte 0x30, two-byte 0x0123, overflow + three-byte 0x0001, padding, an info block, EOF.
        let s = [
            0x30, 0x01, 0x23, 0x0B, 0x0C, 0x00, 0x01, 0x09, 0xAA, 0x0D, 0x04, 0x02, 0x00, b'h',
            b'i', 0x0D, 0x0D, 0x00, 0x00, 0x40,
        ];
        let ns = parse_stream(&s);
        let ticks: Vec<u32> = ns
            .iter()
            .map(|&n| (n as f64 * SCK_HZ / 1e9).round() as u32)
            .collect();
        assert_eq!(ticks, [0x30, 0x0123, 0x1_0001]);
    }

    #[test]
    fn names_carry_cylinder_and_side() {
        assert_eq!(track_of(Path::new("disk.img07.1.raw")), Some((7, 1)));
        assert_eq!(track_of(Path::new("track79.0.raw")), Some((79, 0)));
        assert_eq!(track_of(Path::new("disk.raw")), None);
    }
}
