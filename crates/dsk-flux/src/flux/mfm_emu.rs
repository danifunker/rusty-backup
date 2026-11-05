//! David Gesswein's MFM reader/emulator `.emu` files: raw MFM cells of ST-506 hard disks.
//!
//! Header (little-endian u32s after the 8-byte magic `EE 'M' 'F' 'M' 0D 0A 1A 00`): a
//! version word +8, header length +12, track length +16, track-header length +20,
//! cylinders +24, heads +28, sample rate +32 (10 MHz: one MFM cell per bit), then the
//! length and text of the command line the capture was made with (`--format ...
//! --sectors 17,0 ...`). Each track follows as `{0x12345678, cylinder, head}` and the
//! cell stream, stored as 32-bit little-endian words read most significant bit first;
//! a track header with cylinder and head `0xFFFFFFFF` (or without the magic) ends the file.
//!
//! Sector formats are named by the command line. `Intel_iSBC_214` (used by ICON /
//! LEXICON drives) was worked out against a real ST-251 whose sector dump exists:
//! an `A1` sync, then the ID `{FE ^ cyl_high, cyl_low, 0x20 | head, sector}` with a
//! CRC-CCITT (init FFFF, over `A1` and the four bytes); the data field is another
//! sync, `F8`, 512 bytes and a 32-bit ECC (polynomial 0x140A0445, init FFFFFFFF, over
//! `A1 F8` and the data), big-endian. Other formats are refused by name.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use super::{DecodedDisk, SectorFault};

const MAGIC: [u8; 8] = [0xEE, b'M', b'F', b'M', 0x0D, 0x0A, 0x1A, 0x00];
const TRACK_MAGIC: u32 = 0x1234_5678;
const SYNC: u16 = 0x4489;

/// What the `.emu` header says.
#[derive(Debug, Clone)]
pub struct EmuHeader {
    pub header_len: u64,
    pub track_len: u64,
    pub track_header_len: u64,
    pub cylinders: u32,
    pub heads: u32,
    pub command: String,
}

fn le32(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}

/// Whether `head` starts like an `.emu` file.
pub fn looks_like_emu(head: &[u8]) -> bool {
    head.len() >= 8 && head[..8] == MAGIC
}

/// Parse the header at the start of `r`.
pub fn read_header<R: Read + Seek>(r: &mut R) -> std::io::Result<EmuHeader> {
    let bad = |m: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, m.to_string());
    let mut h = [0u8; 40];
    r.seek(SeekFrom::Start(0))?;
    r.read_exact(&mut h)?;
    if !looks_like_emu(&h) {
        return Err(bad("not an MFM emulator (.emu) file"));
    }
    let (cmd_len, header_len) = (le32(&h, 36) as usize, le32(&h, 12) as u64);
    let mut cmd = vec![0u8; cmd_len.min(4096)];
    r.read_exact(&mut cmd)?;
    let end = cmd.iter().position(|&b| b == 0).unwrap_or(cmd.len());
    let header = EmuHeader {
        header_len,
        track_len: le32(&h, 16) as u64,
        track_header_len: le32(&h, 20) as u64,
        cylinders: le32(&h, 24),
        heads: le32(&h, 28),
        command: String::from_utf8_lossy(&cmd[..end]).into_owned(),
    };
    if header.track_header_len < 12 || header.track_len == 0 || header.heads == 0 {
        return Err(bad("MFM emulator file: implausible header"));
    }
    Ok(header)
}

/// The value after `--name` in the capture's command line.
/// The reader can run options together (`--rate 10000000--format X`), so split at `--`.
fn option<'a>(command: &'a str, name: &str) -> Option<&'a str> {
    let key = name.trim_start_matches('-');
    command.split("--").find_map(|opt| {
        let mut words = opt.split_whitespace();
        (words.next() == Some(key)).then(|| words.next()).flatten()
    })
}

fn crc16(data: &[u8]) -> u16 {
    let mut c = 0xFFFFu16;
    for &b in data {
        c ^= (b as u16) << 8;
        for _ in 0..8 {
            c = if c & 0x8000 != 0 {
                (c << 1) ^ 0x1021
            } else {
                c << 1
            };
        }
    }
    c
}

fn ecc32(data: &[u8]) -> u32 {
    let mut c = 0xFFFF_FFFFu32;
    for &b in data {
        c ^= (b as u32) << 24;
        for _ in 0..8 {
            c = if c & 0x8000_0000 != 0 {
                (c << 1) ^ 0x140A_0445
            } else {
                c << 1
            };
        }
    }
    c
}

/// MFM cells of a track: 32-bit little-endian words, most significant bit first.
fn cells(track: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(track.len() * 8);
    for w in track.chunks_exact(4) {
        for &b in w.iter().rev() {
            out.extend((0..8).rev().map(|i| (b >> i) & 1));
        }
    }
    out
}

/// Every `(sector, data, ecc ok)` an iSBC 214 track yields, with the cylinder and head its IDs name.
fn decode_isbc214(cells: &[u8]) -> Vec<(u32, u32, u32, Vec<u8>, bool)> {
    let byte = |at: usize| -> Option<u8> {
        let raw = cells.get(at..at + 16)?;
        Some((0..8).fold(0u8, |v, k| (v << 1) | raw[k * 2 + 1]))
    };
    let bytes =
        |at: usize, n: usize| -> Option<Vec<u8>> { (0..n).map(|k| byte(at + k * 16)).collect() };
    let mut marks = Vec::new();
    let mut w = 0u16;
    for (i, &c) in cells.iter().enumerate() {
        w = (w << 1) | c as u16;
        if w == SYNC {
            marks.push(i + 1);
        }
    }
    let mut out = Vec::new();
    for (k, &at) in marks.iter().enumerate() {
        let Some(id) = bytes(at, 6) else { continue };
        if id[0] & 0xF8 != 0xF8 || id[0] == 0xF8 {
            continue;
        }
        if crc16(&[[0xA1].as_slice(), &id[..4]].concat()) != u16::from_be_bytes([id[4], id[5]]) {
            continue;
        }
        let cyl = (u32::from(0xFE ^ id[0]) << 8) | u32::from(id[1]);
        let (head, sector) = (u32::from(id[2] & 0x0F), u32::from(id[3]));
        let Some(&dat) = marks.get(k + 1).filter(|&&d| d > at && d - at < 16 * 64) else {
            continue;
        };
        let Some(field) = bytes(dat, 1 + 512 + 4) else {
            continue;
        };
        if field[0] != 0xF8 {
            continue;
        }
        let stored = u32::from_be_bytes([field[513], field[514], field[515], field[516]]);
        let ok = ecc32(&[[0xA1].as_slice(), &field[..513]].concat()) == stored;
        out.push((cyl, head, sector, field[1..513].to_vec(), ok));
    }
    out
}

/// Decode an `.emu` capture into `out` (cylinders x heads x sectors x 512, zero-filled where
/// unreadable), one track at a time. Returns the geometry and the faults, without the image.
pub fn decode_to<R: Read + Seek, W: Write + Seek>(
    r: &mut R,
    out: &mut W,
) -> std::io::Result<DecodedDisk> {
    let bad = |m: String| std::io::Error::new(std::io::ErrorKind::InvalidData, m);
    let h = read_header(r)?;
    let format = option(&h.command, "--format").unwrap_or("");
    if !format.starts_with("Intel_iSBC_214") {
        return Err(bad(format!(
            "MFM emulator file: sector format {format:?} is not supported yet (Intel_iSBC_214 is)"
        )));
    }
    let spt: u32 = option(&h.command, "--sectors")
        .and_then(|s| s.split(',').next())
        .and_then(|n| n.parse().ok())
        .ok_or_else(|| bad("MFM emulator file: no --sectors in the command line".into()))?;
    let first = option(&h.command, "--sectors")
        .and_then(|s| s.split(',').nth(1))
        .and_then(|n| n.parse::<u32>().ok())
        .unwrap_or(0);
    let mut seen = vec![0u8; (h.cylinders * h.heads * spt) as usize];
    r.seek(SeekFrom::Start(h.header_len))?;
    let mut th = vec![0u8; h.track_header_len as usize];
    let mut track = vec![0u8; h.track_len as usize];
    loop {
        // The file ends with a track header whose cylinder and head are 0xFFFFFFFF.
        if r.read_exact(&mut th).is_err() || le32(&th, 0) != TRACK_MAGIC || le32(&th, 4) == u32::MAX
        {
            break;
        }
        r.read_exact(&mut track)?;
        for (c, hd, s, data, ok) in decode_isbc214(&cells(&track)) {
            if c >= h.cylinders || hd >= h.heads || s < first || s - first >= spt {
                continue;
            }
            let lba = ((c * h.heads + hd) * spt + (s - first)) as usize;
            // A clean copy wins; a bad one is kept only until a clean one turns up.
            if seen[lba] == 2 || (seen[lba] == 1 && !ok) {
                continue;
            }
            out.seek(SeekFrom::Start(lba as u64 * 512))?;
            out.write_all(&data)?;
            seen[lba] = if ok { 2 } else { 1 };
        }
    }
    let total = seen.len() as u64 * 512;
    out.seek(SeekFrom::Start(total))?;
    let mut faults = Vec::new();
    for (lba, &state) in seen.iter().enumerate() {
        let lba = lba as u32;
        let (c, rest) = (lba / (h.heads * spt), lba % (h.heads * spt));
        let fault = match state {
            0 => Some(SectorFault::Missing),
            1 => Some(SectorFault::Crc),
            _ => None,
        };
        if let Some(f) = fault {
            faults.push((c, rest / spt, rest % spt + first, f));
        }
    }
    Ok(DecodedDisk {
        image: Vec::new(),
        cylinders: h.cylinders,
        heads: h.heads,
        sectors_per_track: spt,
        sector_size: 512,
        first_record: first,
        faults,
    })
}

/// Whether `path` is an `.emu` capture, by extension and magic.
pub fn is_emu_path(path: &Path) -> bool {
    let ext = path.extension().and_then(|e| e.to_str());
    if !ext.is_some_and(|e| e.eq_ignore_ascii_case("emu")) {
        return false;
    }
    let mut head = [0u8; 8];
    std::fs::File::open(path)
        .and_then(|mut f| f.read_exact(&mut head))
        .is_ok_and(|_| looks_like_emu(&head))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// MFM cells for data bytes (a `sync` byte drops the clock bit that makes A1 a mark).
    fn encode(bytes: &[(u8, bool)]) -> Vec<u8> {
        let mut cells = Vec::new();
        let mut prev = 0u8;
        for &(b, sync) in bytes {
            for j in (0..8).rev() {
                let d = (b >> j) & 1;
                let clock = u8::from(prev == 0 && d == 0);
                cells.push(if sync && j == 2 { 0 } else { clock });
                cells.push(d);
                prev = d;
            }
        }
        cells
    }

    /// One iSBC 214 sector as cells.
    fn sector(cyl: u32, head: u8, s: u8, data: &[u8; 512]) -> Vec<(u8, bool)> {
        let mut v: Vec<(u8, bool)> = vec![(0x4E, false); 16];
        v.extend([(0, false); 12]);
        v.push((0xA1, true));
        let id = [0xFE ^ (cyl >> 8) as u8, cyl as u8, 0x20 | head, s];
        let crc = crc16(&[[0xA1].as_slice(), &id].concat());
        v.extend(id.iter().chain(&crc.to_be_bytes()).map(|&b| (b, false)));
        v.extend([(0x4E, false); 12]);
        v.extend([(0, false); 12]);
        v.push((0xA1, true));
        let mut field = vec![0xF8];
        field.extend_from_slice(data);
        let ecc = ecc32(&[[0xA1].as_slice(), &field].concat());
        v.extend(field.iter().chain(&ecc.to_be_bytes()).map(|&b| (b, false)));
        v
    }

    /// Pack cells into 32-bit little-endian words, MSB first, padded to `len` bytes.
    fn pack(cells: &[u8], len: usize) -> Vec<u8> {
        let mut bytes: Vec<u8> = cells
            .chunks(8)
            .map(|c| {
                c.iter()
                    .enumerate()
                    .fold(0u8, |v, (i, &b)| v | b << (7 - i))
            })
            .collect();
        bytes.resize(len, 0);
        bytes
            .chunks_exact(4)
            .flat_map(|w| w.iter().rev().copied().collect::<Vec<_>>())
            .collect()
    }

    #[test]
    fn decodes_isbc214_tracks_into_the_flat_image() {
        let (cyls, heads, spt, tl) = (2u32, 1u32, 2u32, 4096usize);
        let cmd = b"--format Intel_iSBC_214_512B --sectors 2,0\0";
        let mut f = Vec::new();
        f.extend_from_slice(&MAGIC);
        f.extend_from_slice(&0x0202_0200u32.to_le_bytes());
        let header_len = 40 + cmd.len();
        for v in [
            header_len as u32,
            tl as u32,
            12,
            cyls,
            heads,
            10_000_000,
            cmd.len() as u32,
        ] {
            f.extend_from_slice(&v.to_le_bytes());
        }
        f.extend_from_slice(cmd);
        for c in 0..cyls {
            // Sectors in reverse order: their ID, not their place on the track, decides.
            let mut cells = Vec::new();
            for s in [1u8, 0] {
                cells.extend(encode(&sector(c, 0, s, &[(c as u8) * 10 + s; 512])));
            }
            for v in [TRACK_MAGIC, c, 0] {
                f.extend_from_slice(&v.to_le_bytes());
            }
            f.extend_from_slice(&pack(&cells, tl));
        }
        f.extend_from_slice(&TRACK_MAGIC.to_le_bytes());
        f.extend_from_slice(&[0xFF; 8]);
        let mut out = Cursor::new(Vec::new());
        let disk = decode_to(&mut Cursor::new(f), &mut out).unwrap();
        assert!(disk.faults.is_empty(), "{:?}", disk.faults);
        let img = out.into_inner();
        assert_eq!(img.len(), (cyls * heads * spt * 512) as usize);
        assert_eq!(img[512], 1);
        assert_eq!(img[2 * 512], 10);
        assert_eq!(img[3 * 512 + 511], 11);
    }

    #[test]
    fn options_run_together_are_still_found() {
        let cmd = "--heads 6 --rate 10000000--format Intel_iSBC_214_512B --sectors 17,0 --heads 8";
        assert_eq!(option(cmd, "--format"), Some("Intel_iSBC_214_512B"));
        assert_eq!(option(cmd, "--sectors"), Some("17,0"));
        assert_eq!(option(cmd, "--rate"), Some("10000000"));
    }

    #[test]
    fn high_cylinder_bits_ride_in_the_mark() {
        let cells = encode(&sector(819, 5, 3, &[7; 512]));
        let got = decode_isbc214(&cells);
        assert_eq!(got.len(), 1);
        assert_eq!((got[0].0, got[0].1, got[0].2, got[0].4), (819, 5, 3, true));
    }
}
