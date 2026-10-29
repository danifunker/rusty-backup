//! AmigaDOS MFM tracks: `4489 4489` syncs, then odd/even-split header, label, checksums and data.
//!
//! Layout (from Greaseweazle's `codec/amiga/amigados.py`): after the two sync
//! words, raw MFM bytes hold the 4-byte info `{0xFF, track, sector, to-gap}`
//! (8 raw bytes), a 16-byte label (32), the header checksum (8), the data
//! checksum (8) and 512 data bytes (1024). Each field is stored as its odd bits
//! then its even bits; the data bits sit at mask `0x55` of each raw byte. A
//! checksum is the XOR of the field's big-endian longs, folded with
//! `(c ^ c >> 1) & 0x55555555`. Track `t` is cylinder `t / 2`, head `t % 2`;
//! sectors are numbered from 0 and become records from 1.

use super::mfm::{estimate_cell, to_bits_pll};
use super::Sector;

const SYNC: u32 = 0x4489_4489;

/// Raw MFM bytes (clock and data cells) from bit `at`.
fn raw_bytes(bits: &[u8], at: usize, n: usize) -> Option<Vec<u8>> {
    let cells = bits.get(at..at + n * 8)?;
    Some(
        cells
            .chunks_exact(8)
            .map(|c| c.iter().fold(0u8, |v, &b| (v << 1) | b))
            .collect(),
    )
}

/// Join a field's odd-bit half and even-bit half.
fn join(raw: &[u8]) -> Vec<u8> {
    let (odd, even) = raw.split_at(raw.len() / 2);
    odd.iter()
        .zip(even)
        .map(|(&o, &e)| (o << 1 & 0xAA) | (e & 0x55))
        .collect()
}

fn checksum(data: &[u8]) -> u32 {
    let c = data.chunks_exact(4).fold(0u32, |c, l| {
        c ^ u32::from_be_bytes([l[0], l[1], l[2], l[3]])
    });
    (c ^ (c >> 1)) & 0x5555_5555
}

fn be32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

fn decode_bits(bits: &[u8]) -> Vec<Sector> {
    let mut out = Vec::new();
    let mut w = 0u32;
    for (i, &b) in bits.iter().enumerate() {
        w = (w << 1) | b as u32;
        if w != SYNC {
            continue;
        }
        // 1084 raw bytes follow the sync: 8 + 32 + 8 + 8 + 1024 + 4 of gap.
        let Some(raw) = raw_bytes(bits, i + 1, 1080) else {
            break;
        };
        let info = join(&raw[0..8]);
        if info[0] != 0xFF {
            continue;
        }
        let label = join(&raw[8..40]);
        let head_ok = be32(&join(&raw[40..48])) == checksum(&[info.as_slice(), &label].concat());
        if !head_ok {
            continue;
        }
        let data = join(&raw[56..1080]);
        let stored = be32(&join(&raw[48..56]));
        let ok = stored == checksum(&data);
        out.push(Sector {
            cyl: info[1] / 2,
            head: info[1] % 2,
            record: info[2].wrapping_add(1),
            data,
            ok,
            mark: 0xFF,
            crc: stored as u16,
        });
    }
    out
}

/// Every Amiga sector copy found on the track.
pub fn decode_track(intervals: &[u32]) -> Vec<Sector> {
    let Some(cell) = estimate_cell(intervals) else {
        return Vec::new();
    };
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

    /// MFM cells for data bytes, continuing from the previous data bit.
    fn mfm(bytes: &[u8], prev: &mut u8, cells: &mut Vec<u8>) {
        for &b in bytes {
            for j in (0..8).rev() {
                let d = (b >> j) & 1;
                cells.push(u8::from(*prev == 0 && d == 0));
                cells.push(d);
                *prev = d;
            }
        }
    }

    fn split(dat: &[u8]) -> Vec<u8> {
        dat.iter()
            .map(|&x| (x >> 1) & 0x55)
            .chain(dat.iter().map(|&x| x & 0x55))
            .collect()
    }

    /// The cells of one sector with its sync, as AmigaDOS writes it.
    fn sector(track: u8, id: u8, data: &[u8]) -> Vec<u8> {
        let mut cells = Vec::new();
        let mut prev = 0;
        mfm(&[0; 4], &mut prev, &mut cells);
        for _ in 0..2 {
            cells.extend((0..16).rev().map(|j| (0x4489u16 >> j & 1) as u8));
        }
        prev = 1;
        let info = [0xFF, track, id, 11 - id];
        let label = [0u8; 16];
        // The split fields are themselves data bytes, MFM-encoded in order.
        let mut fields = split(&info);
        fields.extend(split(&label));
        fields.extend(split(
            &checksum(&[info.as_slice(), &label].concat()).to_be_bytes(),
        ));
        fields.extend(split(&checksum(data).to_be_bytes()));
        fields.extend(split(data));
        fields.extend([0u8; 4]);
        // Only the data cells of these bytes matter; encode them as MFM data bits.
        let mut raw_cells = Vec::new();
        for &f in &fields {
            for j in (0..8).rev() {
                raw_cells.push((f >> j) & 1);
            }
        }
        // Each raw byte already carries data at 0x55; rebuild its clock cells.
        for pair in raw_cells.chunks_exact(2) {
            let d = pair[1];
            cells.push(u8::from(prev == 0 && d == 0));
            cells.push(d);
            prev = d;
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
    fn decodes_amigados_sectors() {
        let data: Vec<u8> = (0..512).map(|i| (i * 3) as u8).collect();
        let mut cells = sector(5, 0, &data);
        cells.extend(sector(5, 1, &[0xAA; 512]));
        cells.extend([0u8, 1].repeat(64));
        let found = decode_track(&to_flux(&cells, 2000));
        let s = found
            .iter()
            .find(|s| s.record == 1 && s.ok)
            .expect("sector 0");
        assert_eq!((s.cyl, s.head), (2, 1));
        assert_eq!(s.data, data);
        assert!(found.iter().any(|s| s.record == 2 && s.ok));
    }
}
