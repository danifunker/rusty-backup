//! HxC `.hfe` bitstream images (v1 `HXCPICFE` and v3 `HXCHFEV3`), as Greaseweazle writes them.
//!
//! Layout (checked against Greaseweazle's `image/hfe.py`): a header with the
//! signature, revision +8, cylinders +9, sides +10, encoding +11, the bit rate in
//! kbit/s +12 (u16) and the track table's 512-byte block +18 (u16). The table
//! holds `{u16 block, u16 length}` per cylinder; a track's bytes run in 512-byte
//! blocks whose first 256 bytes are side 0 and next 256 side 1, `length / 2` per
//! side. Bits are LSB first, one MFM/FM cell each, `2000 × bit rate` cells per
//! second. v3 adds opcodes (`0xF0` nop, `F1` index, `F2` rate in 36 MHz ticks
//! per cell, `F3` skip bits, `F4` weak byte) in the bit-reversed byte stream.
//! Cells become flux intervals so the encoding layer decodes them like a capture.

use super::FluxTrack;

/// Whether `head` starts like an HFE image.
pub fn looks_like_hfe(head: &[u8]) -> bool {
    head.len() >= 20 && (&head[0..8] == b"HXCPICFE" || &head[0..8] == b"HXCHFEV3")
}

/// Every track's cells as flux intervals in nanoseconds.
pub fn parse(data: &[u8]) -> Result<Vec<FluxTrack>, String> {
    if !looks_like_hfe(data) {
        return Err("not an HFE image".into());
    }
    let v3 = &data[0..8] == b"HXCHFEV3";
    let (cyls, sides) = (data[9] as usize, data[10] as usize);
    let bitrate = u16::from_le_bytes([data[12], data[13]]) as f64;
    let table = u16::from_le_bytes([data[18], data[19]]) as usize * 512;
    if data[8] > 1 || cyls == 0 || !(1..=2).contains(&sides) || bitrate == 0.0 {
        return Err("HFE: bad header".into());
    }
    let cell_ns = 1e6 / (2.0 * bitrate);
    let mut tracks = Vec::new();
    for cyl in 0..cyls {
        let at = table + cyl * 4;
        let entry = data
            .get(at..at + 4)
            .ok_or_else(|| format!("HFE: track table entry {cyl} past the end"))?;
        let block = u16::from_le_bytes([entry[0], entry[1]]) as usize;
        let len = u16::from_le_bytes([entry[2], entry[3]]) as usize;
        for side in 0..sides {
            let mut bytes = Vec::with_capacity(len / 2);
            let mut todo = len / 2;
            let mut b = block;
            while todo > 0 {
                let n = todo.min(256);
                let off = b * 512 + side * 256;
                let chunk = data
                    .get(off..off + n)
                    .ok_or_else(|| format!("HFE: cylinder {cyl} runs past the end"))?;
                // Reverse each byte so bits read MSB first, as the v3 opcodes expect.
                bytes.extend(chunk.iter().map(|x| x.reverse_bits()));
                todo -= n;
                b += 1;
            }
            let cells = if v3 {
                v3_cells(&bytes, cell_ns)?
            } else {
                bytes
                    .iter()
                    .flat_map(|&x| (0..8).rev().map(move |j| (x >> j & 1, cell_ns)))
                    .collect()
            };
            tracks.push(FluxTrack {
                cyl: cyl as u32,
                head: side as u32,
                intervals_ns: to_intervals(&cells),
            });
        }
    }
    Ok(tracks)
}

/// Cells of a v3 track as `(bit, duration ns)`, applying its opcodes.
fn v3_cells(bytes: &[u8], default_ns: f64) -> Result<Vec<(u8, f64)>, String> {
    let mut out: Vec<(u8, f64)> = Vec::with_capacity(bytes.len() * 8);
    let mut rate: Option<f64> = None;
    let mut first_rated = 0;
    let mut i = 0;
    let push = |out: &mut Vec<(u8, f64)>, x: u8, n: usize, ns: f64| {
        out.extend((0..n).rev().map(|j| (x >> j & 1, ns)));
    };
    while i < bytes.len() {
        let x = bytes[i];
        i += 1;
        let ns = rate.unwrap_or(default_ns);
        match x {
            0xF0 | 0xF1 => {}
            0xF2 => {
                let r = *bytes.get(i).ok_or("HFE v3: truncated bit-rate opcode")?;
                i += 1;
                if rate.is_none() {
                    first_rated = out.len();
                }
                rate = Some(r as f64 * 1e9 / 36e6);
            }
            0xF3 => {
                let (nr, mut v) = match bytes.get(i..i + 2) {
                    Some(p) => (p[0] as usize, p[1]),
                    None => return Err("HFE v3: truncated skip-bits opcode".into()),
                };
                i += 2;
                if !(1..8).contains(&nr) {
                    return Err(format!("HFE v3: bad skip-bits count {nr}"));
                }
                if v == 0xF4 {
                    v = 0;
                }
                push(&mut out, v, 8 - nr, ns);
            }
            0xF4 => push(&mut out, 0, 8, ns),
            _ if x & 0xF0 == 0xF0 => return Err(format!("HFE v3: unknown opcode {x:#04x}")),
            _ => push(&mut out, x, 8, ns),
        }
    }
    // Cells before the track's first rate opcode take the rate it ends with.
    if let Some(r) = rate {
        for c in &mut out[..first_rated] {
            c.1 = r;
        }
    }
    Ok(out)
}

/// Flux intervals from cells: each `1` ends an interval spanning the cells since the last one.
fn to_intervals(cells: &[(u8, f64)]) -> Vec<u32> {
    let mut out = Vec::new();
    let mut run = 0f64;
    for &(bit, ns) in cells {
        run += ns;
        if bit == 1 {
            out.push(run as u32);
            run = 0.0;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A one-cylinder, one-sided v1 image whose track is `bytes` (MSB-first cells).
    fn v1(bytes: &[u8]) -> Vec<u8> {
        let mut img = vec![0u8; 1024];
        img[0..8].copy_from_slice(b"HXCPICFE");
        img[9] = 1;
        img[10] = 1;
        img[12..14].copy_from_slice(&250u16.to_le_bytes());
        img[18..20].copy_from_slice(&1u16.to_le_bytes());
        img[512..514].copy_from_slice(&2u16.to_le_bytes());
        img[514..516].copy_from_slice(&((bytes.len() * 2) as u16).to_le_bytes());
        let mut track = vec![0u8; 512];
        for (k, &b) in bytes.iter().enumerate() {
            track[k] = b.reverse_bits();
        }
        img.extend_from_slice(&track);
        img
    }

    #[test]
    fn v1_cells_become_two_microsecond_flux() {
        // 0b0100_1001: transitions after 2, 3 and 3 cells of 2 us at 250 kbit/s.
        let tracks = parse(&v1(&[0b0100_1001])).unwrap();
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].intervals_ns, vec![4000, 6000, 6000]);
    }

    #[test]
    fn v3_opcodes_set_the_rate_and_skip_bits() {
        let mut img = v1(&[]);
        img[0..8].copy_from_slice(b"HXCHFEV3");
        // Rate 72 ticks at 36 MHz = 2 us; then 0x41; then skip 4 bits of 0x0F (keeps 1111).
        let ops: [u8; 6] = [0xF2, 72, 0x41, 0xF3, 4, 0x0F];
        img[514..516].copy_from_slice(&((ops.len() * 2) as u16).to_le_bytes());
        for (k, &b) in ops.iter().enumerate() {
            img[1024 + k] = b.reverse_bits();
        }
        let tracks = parse(&img).unwrap();
        assert_eq!(
            tracks[0].intervals_ns,
            vec![4000, 12000, 2000, 2000, 2000, 2000]
        );
    }

    #[test]
    fn rejects_other_files() {
        assert!(parse(&[0u8; 1024]).is_err());
    }
}
