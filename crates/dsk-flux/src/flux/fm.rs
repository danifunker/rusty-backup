//! IBM-style FM (single density): clock cell before every data cell, address marks with clock `C7`.
//!
//! In cells, the ID mark (data `FE`, clock `C7`) is `F57E`; the data marks `FB`
//! and `F8` are `F56F` and `F56A`. Fields and CRCs match MFM's except that the
//! CRC starts at the mark byte (no `A1` prefix). The shortest flux interval is
//! one FM cell, so the cell is twice what the MFM estimate returns. Acorn DFS,
//! for one, numbers its records from 0.

use super::mfm::{bytes_at, crc16, estimate_cell, to_bits_pll};
use super::Sector;

const IDAM: u16 = 0xF57E;
const DAM: u16 = 0xF56F;
const DDAM: u16 = 0xF56A;

/// Positions just past every 16-cell window equal to `mark`.
fn marks(bits: &[u8], want: &[u16]) -> Vec<(usize, u16)> {
    let mut out = Vec::new();
    let mut w = 0u16;
    for (i, &b) in bits.iter().enumerate() {
        w = (w << 1) | b as u16;
        if want.contains(&w) {
            out.push((i + 1, w));
        }
    }
    out
}

fn decode_bits(bits: &[u8]) -> Vec<Sector> {
    let all = marks(bits, &[IDAM, DAM, DDAM]);
    let mut out = Vec::new();
    for (k, &(at, m)) in all.iter().enumerate() {
        if m != IDAM {
            continue;
        }
        let Some(id) = bytes_at(bits, at, 6) else {
            continue;
        };
        if crc16(&[[0xFE].as_slice(), &id[..4]].concat()) != u16::from_be_bytes([id[4], id[5]])
            || id[3] > 6
        {
            continue;
        }
        let size = 128usize << id[3];
        let Some(&(dat, dm)) = all
            .get(k + 1)
            .filter(|&&(d, dm)| dm != IDAM && d > at && d - at < 16 * 64)
        else {
            continue;
        };
        let Some(field) = bytes_at(bits, dat, size + 2) else {
            continue;
        };
        let mark = if dm == DAM { 0xFB } else { 0xF8 };
        let crc = u16::from_be_bytes([field[size], field[size + 1]]);
        out.push(Sector {
            cyl: id[0],
            head: id[1],
            record: id[2],
            data: field[..size].to_vec(),
            ok: crc16(&[[mark].as_slice(), &field[..size]].concat()) == crc,
            mark,
            crc,
        });
    }
    out
}

/// Every FM sector copy found on the track.
pub fn decode_track(intervals: &[u32]) -> Vec<Sector> {
    let Some(half) = estimate_cell(intervals) else {
        return Vec::new();
    };
    let cell = half * 2.0;
    let mut out = Vec::new();
    for (f, period, phase) in [
        (1.0, 0.05, 0.6),
        (1.0, 0.02, 0.3),
        (0.97, 0.05, 0.6),
        (1.03, 0.05, 0.6),
    ] {
        out.extend(decode_bits(&to_bits_pll(
            intervals,
            cell * f,
            period,
            phase,
        )));
        if super::mfm::track_complete(&out) {
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FM cells: a clock then a data cell per bit; `mark` bytes use clock `C7`.
    fn fm(bytes: &[(u8, bool)]) -> Vec<u8> {
        let mut cells = Vec::new();
        for &(b, mark) in bytes {
            let clock = if mark { 0xC7 } else { 0xFF };
            for j in (0..8).rev() {
                cells.push((clock >> j) & 1);
                cells.push((b >> j) & 1);
            }
        }
        cells
    }

    fn to_flux(cells: &[u8], cell: u32) -> Vec<u32> {
        let mut out = Vec::new();
        let mut run = 0;
        for &c in cells {
            run += cell;
            if c == 1 {
                out.push(run);
                run = 0;
            }
        }
        out
    }

    #[test]
    fn decodes_an_fm_sector_numbered_from_zero() {
        let data: Vec<u8> = (0..=255).collect();
        let id = [3u8, 0, 0, 1];
        let mut v: Vec<(u8, bool)> = vec![(0xFF, false); 16];
        v.extend([(0, false); 6]);
        v.push((0xFE, true));
        let crc = crc16(&[[0xFE].as_slice(), &id].concat());
        v.extend(id.iter().chain(&crc.to_be_bytes()).map(|&b| (b, false)));
        v.extend([(0xFF, false); 11]);
        v.extend([(0, false); 6]);
        v.push((0xFB, true));
        let crc = crc16(&[[0xFB].as_slice(), &data].concat());
        v.extend(data.iter().chain(&crc.to_be_bytes()).map(|&b| (b, false)));
        v.extend([(0xFF, false); 20]);
        let found = decode_track(&to_flux(&fm(&v), 4000));
        let s = found.iter().find(|s| s.ok).expect("sector");
        assert_eq!((s.cyl, s.head, s.record), (3, 0, 0));
        assert_eq!(s.data, data);
    }
}
