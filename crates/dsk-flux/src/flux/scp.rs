//! SuperCard Pro `.scp` flux images, the native capture format of Greaseweazle.
//!
//! Layout (checked against Greaseweazle's `image/scp.py`): a 16-byte header
//! (`"SCP"`, version, disk type, revolutions +5, start / end track, flags +8,
//! cell width +9, single-sided +10, resolution +11, checksum +12), then 168
//! little-endian track offsets at 0x10 (the table ends early when a track header
//! starts before 0x2B0). A track header is `"TRK"`, its number, then per
//! revolution `{index ticks, flux count, data offset}` (u32 LE, the offset from
//! the header). Flux values are big-endian u16 ticks of 25 ns × (resolution + 1);
//! a 0 adds 65536 to the next value. Track `n` is cylinder `n / 2`, side `n % 2`;
//! legacy single-sided images that number tracks consecutively are remapped.

use super::FluxTrack;

const TABLE_AT: usize = 0x10;
const TABLE_END: usize = 0x2B0;

/// Whether `head` starts like an SCP image.
pub fn looks_like_scp(head: &[u8]) -> bool {
    head.len() >= 16 && &head[0..3] == b"SCP" && head[5] > 0 && head[9] == 0
}

fn le32(b: &[u8], o: usize) -> Option<u32> {
    Some(u32::from_le_bytes(b.get(o..o + 4)?.try_into().ok()?))
}

/// Every track's flux, all revolutions back to back, in nanoseconds.
pub fn parse(data: &[u8]) -> Result<Vec<FluxTrack>, String> {
    if !looks_like_scp(data) {
        return Err("not an SCP image".into());
    }
    let revs = data[5] as usize;
    let single_sided = data[10];
    let tick_ns = 25.0 * (data[11] as f64 + 1.0);
    let mut offsets: Vec<usize> = (0..168)
        .map(|i| le32(data, TABLE_AT + i * 4).unwrap_or(0) as usize)
        .collect();
    if let Some(first) = offsets
        .iter()
        .copied()
        .filter(|&o| o != 0 && o < TABLE_END)
        .min()
    {
        offsets.truncate(first.saturating_sub(TABLE_AT) / 4);
    }
    let mut tracks: Vec<(usize, Vec<u32>)> = Vec::new();
    for (n, &off) in offsets.iter().enumerate() {
        if off == 0 {
            continue;
        }
        if data.get(off..off + 3) != Some(b"TRK") {
            return Err(format!("track {n}: missing TRK header at {off:#x}"));
        }
        // Revolutions with no flux at the end of the header are unused slots.
        let entries: Vec<(u32, u32, u32)> = (0..revs)
            .filter_map(|r| {
                let at = off + 4 + r * 12;
                Some((le32(data, at)?, le32(data, at + 4)?, le32(data, at + 8)?))
            })
            .collect();
        let used: Vec<&(u32, u32, u32)> = entries
            .iter()
            .filter(|(ticks, count, _)| *ticks != 0 && *count != 0)
            .collect();
        let (Some(first), Some(last)) = (used.first(), used.last()) else {
            continue;
        };
        let start = off + first.2 as usize;
        let end = off + last.2 as usize + last.1 as usize * 2;
        let flux = data
            .get(start..end)
            .ok_or_else(|| format!("track {n}: flux runs past the end of the file"))?;
        let mut intervals = Vec::with_capacity(flux.len() / 2);
        let mut carry = 0u64;
        for pair in flux.chunks_exact(2) {
            let v = u16::from_be_bytes([pair[0], pair[1]]) as u64;
            if v == 0 {
                carry += 65536;
                continue;
            }
            intervals.push(((carry + v) as f64 * tick_ns) as u32);
            carry = 0;
        }
        tracks.push((n, intervals));
    }
    let sides = [0, 1].map(|s| tracks.iter().filter(|(n, _)| n % 2 == s).count());
    let legacy = single_sided != 0 && sides[0] > 0 && sides[1] > 0;
    Ok(tracks
        .into_iter()
        .map(|(n, intervals_ns)| {
            let (cyl, head) = if legacy {
                (n as u32, u32::from(single_sided - 1))
            } else {
                ((n / 2) as u32, (n % 2) as u32)
            };
            FluxTrack {
                cyl,
                head,
                intervals_ns,
            }
        })
        .collect())
}

/// Write `tracks` as a one-revolution SCP image at 25 ns resolution (tests and fixtures).
#[cfg(test)]
pub fn write(tracks: &[FluxTrack]) -> Vec<u8> {
    let mut out = vec![0u8; TABLE_END];
    out[0..3].copy_from_slice(b"SCP");
    out[3] = 0x19;
    out[4] = 0x80;
    out[5] = 1;
    out[8] = 0x01;
    for t in tracks {
        let n = (t.cyl * 2 + t.head) as usize;
        let off = out.len();
        out[TABLE_AT + n * 4..TABLE_AT + n * 4 + 4].copy_from_slice(&(off as u32).to_le_bytes());
        let mut flux = Vec::new();
        for &ns in &t.intervals_ns {
            let mut ticks = ((ns as f64) / 25.0).round().max(1.0) as u64;
            while ticks >= 65536 {
                flux.extend_from_slice(&[0, 0]);
                ticks -= 65536;
            }
            flux.extend_from_slice(&(ticks as u16).to_be_bytes());
        }
        let total: u64 = t.intervals_ns.iter().map(|&n| n as u64 / 25).sum();
        out.extend_from_slice(b"TRK");
        out.push(n as u8);
        out.extend_from_slice(&(total as u32).to_le_bytes());
        out.extend_from_slice(&((flux.len() / 2) as u32).to_le_bytes());
        out.extend_from_slice(&16u32.to_le_bytes());
        out.extend_from_slice(&flux);
    }
    let sum: u32 = out[16..]
        .iter()
        .fold(0u32, |a, &b| a.wrapping_add(b as u32));
    out[12..16].copy_from_slice(&sum.to_le_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_tracks_and_long_intervals() {
        let tracks = vec![
            FluxTrack {
                cyl: 0,
                head: 1,
                intervals_ns: vec![2000, 3000, 4000, 2_000_000],
            },
            FluxTrack {
                cyl: 3,
                head: 0,
                intervals_ns: vec![2000; 10],
            },
        ];
        let got = parse(&write(&tracks)).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!((got[0].cyl, got[0].head), (0, 1));
        assert_eq!(got[0].intervals_ns, tracks[0].intervals_ns);
        assert_eq!((got[1].cyl, got[1].head), (3, 0));
    }

    #[test]
    fn rejects_other_files() {
        assert!(parse(&[0u8; 1024]).is_err());
        assert!(!looks_like_scp(b"SCX"));
    }
}
