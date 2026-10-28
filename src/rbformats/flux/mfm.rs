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

/// Raw MFM bits through a PLL that carries its phase error from one transition to the next.
fn to_bits_pll(intervals: &[u32], cell: f64, period_adj: f64, phase_adj: f64) -> Vec<u8> {
    let mut bits = Vec::with_capacity(intervals.len() * 3);
    let (lo, hi) = (cell * 0.9, cell * 1.1);
    let mut clock = cell;
    let mut ticks = 0f64;
    for &t in intervals {
        ticks += t as f64;
        if ticks < clock / 2.0 {
            continue;
        }
        let mut zeros = 0;
        loop {
            ticks -= clock;
            if ticks < clock / 2.0 {
                break;
            }
            bits.push(0);
            zeros += 1;
        }
        bits.push(1);
        // In lock, steer the period by the phase error; after a long gap, drift back to nominal.
        clock += if zeros <= 3 {
            ticks * period_adj
        } else {
            (cell - clock) * period_adj
        };
        clock = clock.clamp(lo, hi);
        ticks *= 1.0 - phase_adj;
    }
    bits
}

/// A byte-wise majority of three or more damaged copies, kept only when its CRC holds.
pub fn vote(copies: &[Sector]) -> Option<Sector> {
    let size = copies.first()?.data.len();
    let same: Vec<&Sector> = copies.iter().filter(|c| c.data.len() == size).collect();
    if same.len() < 3 {
        return None;
    }
    let pick = |f: &dyn Fn(&Sector) -> u8| {
        let mut counts = [0u16; 256];
        for c in &same {
            counts[f(c) as usize] += 1;
        }
        (0..256).max_by_key(|&b| counts[b]).unwrap_or(0) as u8
    };
    let data: Vec<u8> = (0..size).map(|i| pick(&|c: &Sector| c.data[i])).collect();
    let mark = pick(&|c: &Sector| c.mark);
    let crc = u16::from_be_bytes([
        pick(&|c: &Sector| (c.crc >> 8) as u8),
        pick(&|c: &Sector| c.crc as u8),
    ]);
    let ok = crc16(&[[0xA1, 0xA1, 0xA1, mark].as_slice(), &data].concat()) == crc;
    ok.then(|| Sector {
        data,
        ok,
        mark,
        crc,
        ..same[0].clone()
    })
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
            mark: field[0],
            crc: u16::from_be_bytes([field[1 + size], field[2 + size]]),
        });
    }
    out
}

/// Every sector copy found on the track, at a few cells around the estimate.
pub fn decode_track(intervals: &[u32]) -> Vec<Sector> {
    let Some(cell) = estimate_cell(intervals) else {
        return Vec::new();
    };
    let mut out: Vec<Sector> = Vec::new();
    // Passes from cheapest to most forgiving; stop once every record seen has a clean copy.
    let passes: [(f64, Option<(f64, f64)>); 8] = [
        (1.0, Some((0.05, 0.6))),
        (1.0, None),
        (1.0, Some((0.02, 0.3))),
        (1.0, Some((0.1, 0.8))),
        (0.97, Some((0.05, 0.6))),
        (1.03, Some((0.05, 0.6))),
        (0.94, None),
        (1.06, None),
    ];
    for (f, pll) in passes {
        let bits = match pll {
            Some((period, phase)) => to_bits_pll(intervals, cell * f, period, phase),
            None => to_bits(intervals, cell * f),
        };
        out.extend(decode_bits(&bits));
        if track_complete(&out) {
            break;
        }
    }
    out
}

/// Whether every record from 1 to the highest seen has a copy whose CRCs held.
fn track_complete(found: &[Sector]) -> bool {
    let Some(max) = found.iter().map(|s| s.record).max() else {
        return false;
    };
    (1..=max).all(|r| found.iter().any(|s| s.record == r && s.ok))
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
    fn the_pll_follows_a_drifting_clock() {
        let a: Vec<u8> = (0..512).map(|i| (i * 7) as u8).collect();
        let mut stream = sector(0, 0, 1, &a);
        stream.extend([(0x4E, false); 40]);
        // Stretch the track by up to 6% from start to end, as a slow motor would.
        let flux = encode(&stream, 1000);
        let n = flux.len() as f64;
        let drifted: Vec<u32> = flux
            .iter()
            .enumerate()
            .map(|(i, &t)| (t as f64 * (1.0 + 0.06 * i as f64 / n)) as u32)
            .collect();
        let bits = to_bits_pll(&drifted, 1000.0, 0.05, 0.6);
        let found = decode_bits(&bits);
        assert!(found.iter().any(|s| s.ok && s.data == a), "{found:?}");
    }

    #[test]
    fn a_vote_of_damaged_copies_is_kept_only_when_its_crc_holds() {
        let data = vec![0x5Au8; 512];
        let crc = crc16(&[[0xA1, 0xA1, 0xA1, 0xFB].as_slice(), &data].concat());
        let copy = |flip: usize| {
            let mut d = data.clone();
            d[flip] ^= 0x10;
            Sector {
                cyl: 0,
                head: 0,
                record: 1,
                data: d,
                ok: false,
                mark: 0xFB,
                crc,
            }
        };
        let fixed = vote(&[copy(3), copy(100), copy(400)]).expect("majority restores it");
        assert!(fixed.ok && fixed.data == data);
        assert!(vote(&[copy(3), copy(3), copy(400)]).is_none());
        assert!(vote(&[copy(3), copy(100)]).is_none());
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
