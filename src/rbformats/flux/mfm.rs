//! IBM-style MFM: flux intervals to bits with a PLL, then `A1 A1 A1` sync marks,
//! ID fields (`FE c h r n` + CRC) and data fields (`FB` / `F8` + data + CRC).
//!
//! The bit cell is not assumed: the shortest common flux interval is two cells,
//! so it is taken from the interval histogram, and the track is decoded at a few
//! cells around it. Every copy of a sector (each revolution, each cell) is
//! offered and a copy whose CRCs hold is kept.

use super::Sector;

/// `A1` with the missing clock bit, three times: the MFM sync pattern.
const SYNC: u64 = 0x4489_4489_4489;
const SYNC_MASK: u64 = 0xFFFF_FFFF_FFFF;
const IDAM: u8 = 0xFE;

fn crc16(data: &[u8]) -> u16 {
    let mut crc = 0xFFFFu16;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// The cell length (ns) implied by the shortest well-populated interval peak.
fn estimate_cell(intervals: &[u32]) -> Option<f64> {
    const BIN: u32 = 50;
    let mut hist = vec![0u32; 400];
    for &i in intervals {
        if let Some(h) = hist.get_mut((i / BIN) as usize) {
            *h += 1;
        }
    }
    let floor = intervals.len() as u32 / 50;
    let peak = (1..hist.len() - 1)
        .find(|&b| hist[b] >= floor && hist[b] >= hist[b - 1] && hist[b] >= hist[b + 1])?;
    // Weighted centre of the peak and its neighbours.
    let (sum, n) = (peak - 1..=peak + 1).fold((0f64, 0f64), |(s, n), b| {
        (
            s + (b as f64 + 0.5) * BIN as f64 * hist[b] as f64,
            n + hist[b] as f64,
        )
    });
    (n > 0.0).then(|| sum / n / 2.0)
}

/// Raw MFM bits (clock and data) from the intervals, with a gently adapting cell.
fn to_bits(intervals: &[u32], cell: f64) -> Vec<u8> {
    let mut bits = Vec::with_capacity(intervals.len() * 3);
    let mut c = cell;
    for &t in intervals {
        let t = t as f64;
        let n = (t / c).round().clamp(1.0, 8.0);
        bits.extend(crate::compat::repeat_n(0, n as usize - 1));
        bits.push(1);
        c += (t / n - c) * 0.05;
        c = c.clamp(cell * 0.85, cell * 1.15);
    }
    bits
}

fn byte_at(bits: &[u8], at: usize) -> Option<u8> {
    let raw = bits.get(at..at + 16)?;
    Some((0..8).fold(0u8, |v, j| (v << 1) | raw[j * 2 + 1]))
}

fn bytes_at(bits: &[u8], at: usize, n: usize) -> Option<Vec<u8>> {
    (0..n).map(|k| byte_at(bits, at + k * 16)).collect()
}

/// Every sync-mark position (index of the first bit after the third `A1`).
fn syncs(bits: &[u8]) -> Vec<usize> {
    let mut out = Vec::new();
    let mut w = 0u64;
    for (i, &b) in bits.iter().enumerate() {
        w = ((w << 1) | b as u64) & SYNC_MASK;
        if w == SYNC {
            out.push(i + 1);
        }
    }
    out
}

fn decode_bits(bits: &[u8]) -> Vec<Sector> {
    let marks = syncs(bits);
    let mut out = Vec::new();
    for (k, &at) in marks.iter().enumerate() {
        let Some(id) = bytes_at(bits, at, 7) else {
            continue;
        };
        if id[0] != IDAM {
            continue;
        }
        let header_ok = crc16(&[[0xA1, 0xA1, 0xA1].as_slice(), &id[..5]].concat())
            == u16::from_be_bytes([id[5], id[6]]);
        if !header_ok || id[4] > 6 {
            continue;
        }
        let size = 128usize << id[4];
        // The data field is the next sync within a short gap.
        let Some(&dat) = marks.get(k + 1).filter(|&&d| d > at && d - at < 16 * 64) else {
            continue;
        };
        let Some(field) = bytes_at(bits, dat, 1 + size + 2) else {
            continue;
        };
        if field[0] != 0xFB && field[0] != 0xF8 {
            continue;
        }
        let ok = crc16(&[[0xA1, 0xA1, 0xA1].as_slice(), &field[..1 + size]].concat())
            == u16::from_be_bytes([field[1 + size], field[2 + size]]);
        out.push(Sector {
            cyl: id[1],
            head: id[2],
            record: id[3],
            data: field[1..1 + size].to_vec(),
            ok,
        });
    }
    out
}

/// Every sector copy found on the track, at a few cells around the estimate.
pub fn decode_track(intervals: &[u32]) -> Vec<Sector> {
    let Some(cell) = estimate_cell(intervals) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for f in [1.0, 0.97, 1.03, 0.94, 1.06] {
        let found = decode_bits(&to_bits(intervals, cell * f));
        let all_ok = !found.is_empty() && found.iter().all(|s| s.ok);
        out.extend(found);
        if all_ok && f == 1.0 {
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// MFM-encode `bytes` (with `A1` sync marks where flagged) into flux intervals at `cell` ns.
    fn encode(bytes: &[(u8, bool)], cell: u32) -> Vec<u32> {
        let mut bits = Vec::new();
        let mut prev = 0u8;
        for &(b, sync) in bytes {
            for j in (0..8).rev() {
                let d = (b >> j) & 1;
                let clock = u8::from(prev == 0 && d == 0);
                bits.push(if sync && j == 2 { 0 } else { clock });
                bits.push(d);
                prev = d;
            }
        }
        let mut out = Vec::new();
        let mut run = 0u32;
        for b in bits {
            run += 1;
            if b == 1 {
                out.push(run * cell);
                run = 0;
            }
        }
        out
    }

    fn sector(c: u8, h: u8, r: u8, data: &[u8]) -> Vec<(u8, bool)> {
        let mut v: Vec<(u8, bool)> = vec![(0x4E, false); 20];
        v.extend([(0, false); 12]);
        v.extend([(0xA1, true); 3]);
        let id = [IDAM, c, h, r, 2];
        let crc = crc16(&[[0xA1, 0xA1, 0xA1].as_slice(), &id].concat());
        v.extend(
            id.iter()
                .chain(crc.to_be_bytes().iter())
                .map(|&b| (b, false)),
        );
        v.extend([(0x4E, false); 22]);
        v.extend([(0, false); 12]);
        v.extend([(0xA1, true); 3]);
        let mut field = vec![0xFB];
        field.extend_from_slice(data);
        let crc = crc16(&[[0xA1, 0xA1, 0xA1].as_slice(), &field].concat());
        v.extend(
            field
                .iter()
                .chain(crc.to_be_bytes().iter())
                .map(|&b| (b, false)),
        );
        v
    }

    #[test]
    fn decodes_synthetic_sectors_at_an_odd_cell() {
        let a: Vec<u8> = (0..512).map(|i| i as u8).collect();
        let b = vec![0xE5u8; 512];
        let mut stream = sector(3, 1, 1, &a);
        stream.extend(sector(3, 1, 2, &b));
        stream.extend([(0x4E, false); 40]);
        let secs = decode_track(&encode(&stream, 870));
        let got: Vec<_> = secs
            .iter()
            .filter(|s| s.ok)
            .map(|s| (s.cyl, s.head, s.record))
            .collect();
        assert!(
            got.contains(&(3, 1, 1)) && got.contains(&(3, 1, 2)),
            "{got:?}"
        );
        assert_eq!(secs.iter().find(|s| s.record == 1 && s.ok).unwrap().data, a);
    }

    #[test]
    fn a_flipped_byte_is_reported_as_crc_bad() {
        let mut stream = sector(0, 0, 1, &[0x11; 512]);
        stream[300].0 ^= 0x40;
        stream.extend([(0x4E, false); 40]);
        let secs = decode_track(&encode(&stream, 1000));
        assert!(secs.iter().any(|s| s.record == 1 && !s.ok));
    }
}
