//! StuffIt compression **method 13** ("LZ+Huffman"): an LZSS scheme with
//! per-block Huffman-coded literals/lengths/offsets. This is the dominant
//! codec in classic `.sit` archives.
//!
//! Ported from XADMaster `XADStuffIt13Handle.m` + `XADLZSSHandle.m` +
//! `XADPrefixCode.m`. Two code-selection modes exist:
//!
//! - **Dynamic (selector nibble 0):** the three Huffman code tables (first,
//!   second, offset) are described in-stream, RLE-encoded through a fixed
//!   "metacode". Implemented here.
//! - **Static (selector nibble 1–5):** one of five hard-coded length-table
//!   sets (see [`super::stuffit13_tables`]).
//!
//! Bit order matches XAD's "LE" reader: bits are pulled LSB-first from each
//! byte, while Huffman codewords are canonical (assigned MSB-first), so the
//! first bit read is a codeword's most-significant bit.

use anyhow::{bail, Result};
use std::collections::HashMap;

/// LSB-first ("LE") bit reader over a byte slice.
struct BitReader<'a> {
    data: &'a [u8],
    pos: usize,
    buf: u64,
    nbits: u32,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        BitReader {
            data,
            pos: 0,
            buf: 0,
            nbits: 0,
        }
    }

    fn fill(&mut self) {
        while self.nbits <= 56 && self.pos < self.data.len() {
            self.buf |= (self.data[self.pos] as u64) << self.nbits;
            self.pos += 1;
            self.nbits += 8;
        }
    }

    fn next_bit(&mut self) -> u32 {
        if self.nbits == 0 {
            self.fill();
            if self.nbits == 0 {
                return 0; // past EOF: pad with zeros
            }
        }
        let bit = (self.buf & 1) as u32;
        self.buf >>= 1;
        self.nbits -= 1;
        bit
    }

    /// Read `n` bits; the first bit read lands in the LSB of the result.
    fn next_bits(&mut self, n: u32) -> u32 {
        if n == 0 {
            return 0;
        }
        if self.nbits < n {
            self.fill();
        }
        let take = n.min(self.nbits);
        let mask = if take >= 32 {
            u32::MAX
        } else {
            (1u32 << take) - 1
        };
        let val = (self.buf as u32) & mask;
        self.buf >>= take;
        self.nbits -= take;
        val
    }
}

/// A prefix (Huffman) code: maps `(length, codeword)` — codeword accumulated
/// MSB-first — to a symbol value.
struct PrefixCode {
    map: HashMap<(u32, u32), i32>,
    maxlen: u32,
}

impl PrefixCode {
    /// Canonical code from per-symbol bit lengths (lengths < 1 mean "absent").
    fn from_lengths(lengths: &[i32]) -> PrefixCode {
        let maxlen = lengths
            .iter()
            .copied()
            .filter(|&l| l > 0)
            .max()
            .unwrap_or(0) as u32;
        let mut map = HashMap::new();
        let mut code: u32 = 0;
        for len in 1..=maxlen {
            for (sym, &l) in lengths.iter().enumerate() {
                if l == len as i32 {
                    map.insert((len, code), sym as i32);
                    code += 1;
                }
            }
            code <<= 1;
        }
        PrefixCode { map, maxlen }
    }

    /// Explicit code from `(symbol, codeword_low_bit_first, length)` triples
    /// (used for the fixed metacode). The codeword is reversed to MSB-first.
    fn from_explicit(entries: &[(i32, u32, u32)]) -> PrefixCode {
        let mut map = HashMap::new();
        let mut maxlen = 0;
        for &(sym, code_lbf, len) in entries {
            let code = reverse_bits(code_lbf, len);
            map.insert((len, code), sym);
            maxlen = maxlen.max(len);
        }
        PrefixCode { map, maxlen }
    }

    fn decode(&self, br: &mut BitReader) -> Result<i32> {
        let mut acc: u32 = 0;
        for len in 1..=self.maxlen {
            acc = (acc << 1) | br.next_bit();
            if let Some(&sym) = self.map.get(&(len, acc)) {
                return Ok(sym);
            }
        }
        bail!("StuffIt method 13: invalid prefix code in bitstream");
    }
}

fn reverse_bits(val: u32, len: u32) -> u32 {
    let mut out = 0;
    for i in 0..len {
        if val & (1 << i) != 0 {
            out |= 1 << (len - 1 - i);
        }
    }
    out
}

// Fixed "metacode" used to RLE-decode the dynamic length tables. Codes are
// given low-bit-first (XADStuffIt13Handle.m MetaCodes / MetaCodeLengths).
const META_CODES: [u32; 37] = [
    0x5d8, 0x058, 0x040, 0x0c0, 0x000, 0x078, 0x02b, 0x014, 0x00c, 0x01c, 0x01b, 0x00b, 0x010,
    0x020, 0x038, 0x018, 0x0d8, 0xbd8, 0x180, 0x680, 0x380, 0xf80, 0x780, 0x480, 0x080, 0x280,
    0x3d8, 0xfd8, 0x7d8, 0x9d8, 0x1d8, 0x004, 0x001, 0x002, 0x007, 0x003, 0x008,
];
const META_LENGTHS: [u32; 37] = [
    11, 8, 8, 8, 8, 7, 6, 5, 5, 5, 5, 6, 5, 6, 7, 7, 9, 12, 10, 11, 11, 12, 12, 11, 11, 11, 12, 12,
    12, 12, 12, 5, 2, 2, 3, 4, 5,
];

fn build_metacode() -> PrefixCode {
    let entries: Vec<(i32, u32, u32)> = (0..37)
        .map(|i| (i as i32, META_CODES[i], META_LENGTHS[i]))
        .collect();
    PrefixCode::from_explicit(&entries)
}

/// Read an RLE-encoded length table of `numcodes` symbols through the metacode,
/// then build the canonical prefix code. Mirrors `allocAndParseCodeOfSize`.
fn parse_code(br: &mut BitReader, metacode: &PrefixCode, numcodes: usize) -> Result<PrefixCode> {
    let mut lengths = vec![0i32; numcodes];
    let mut length: i32 = 0;
    let mut i = 0usize;
    while i < numcodes {
        let val = metacode.decode(br)?;
        match val {
            31 => length = -1,
            32 => length += 1,
            33 => length -= 1,
            34 => {
                if br.next_bit() == 1 {
                    lengths[i] = length;
                    i += 1;
                }
            }
            35 => {
                let mut count = br.next_bits(3) + 2;
                while count > 0 && i < numcodes {
                    lengths[i] = length;
                    i += 1;
                    count -= 1;
                }
            }
            36 => {
                let mut count = br.next_bits(6) + 10;
                while count > 0 && i < numcodes {
                    lengths[i] = length;
                    i += 1;
                    count -= 1;
                }
            }
            v => length = v + 1,
        }
        if i < numcodes {
            lengths[i] = length;
        }
        i += 1;
    }
    Ok(PrefixCode::from_lengths(&lengths))
}

const WINDOW_SIZE: usize = 65536;
const WINDOW_MASK: usize = WINDOW_SIZE - 1;

/// Decompress a StuffIt method-13 stream to exactly `want` bytes.
pub fn decompress(comp: &[u8], want: usize) -> Result<Vec<u8>> {
    if want == 0 {
        return Ok(Vec::new());
    }
    if comp.is_empty() {
        bail!("StuffIt method 13: empty input");
    }

    let selector = comp[0];
    let code = selector >> 4;
    let mut br = BitReader::new(&comp[1..]);

    let (firstcode, secondcode, offsetcode) = if code == 0 {
        let metacode = build_metacode();
        let firstcode = parse_code(&mut br, &metacode, 321)?;
        let secondcode = if selector & 0x08 != 0 {
            // secondcode == firstcode
            None
        } else {
            Some(parse_code(&mut br, &metacode, 321)?)
        };
        let offset_size = (selector & 0x07) as usize + 10;
        let offsetcode = parse_code(&mut br, &metacode, offset_size)?;
        (firstcode, secondcode, offsetcode)
    } else if (1..=5).contains(&code) {
        use super::stuffit13_tables as t;
        let idx = (code - 1) as usize;
        let firstcode = PrefixCode::from_lengths(t::FIRST[idx]);
        let secondcode = Some(PrefixCode::from_lengths(t::SECOND[idx]));
        let offsetcode = PrefixCode::from_lengths(t::OFFSET[idx]);
        (firstcode, secondcode, offsetcode)
    } else {
        bail!("StuffIt method 13: invalid selector nibble {code}");
    };

    let mut window = vec![0u8; WINDOW_SIZE];
    let mut out = Vec::with_capacity(want);
    let mut pos: usize = 0;
    let mut match_len: usize = 0;
    let mut match_off: usize = 0;
    let mut use_first = true;

    while out.len() < want {
        if match_len == 0 {
            let curr = if use_first {
                &firstcode
            } else {
                secondcode.as_ref().unwrap_or(&firstcode)
            };
            let sym = curr.decode(&mut br)?;

            if sym < 0x100 {
                use_first = true;
                let byte = sym as u8;
                window[pos & WINDOW_MASK] = byte;
                out.push(byte);
                pos += 1;
                continue;
            }

            use_first = false;
            let length = if sym < 0x13e {
                (sym - 0x100 + 3) as usize
            } else if sym == 0x13e {
                br.next_bits(10) as usize + 65
            } else if sym == 0x13f {
                br.next_bits(15) as usize + 65
            } else {
                break; // end-of-stream symbol (0x140)
            };

            let bitlength = offsetcode.decode(&mut br)?;
            let offset = if bitlength == 0 {
                1
            } else if bitlength == 1 {
                2
            } else {
                (1usize << (bitlength - 1)) + br.next_bits((bitlength - 1) as u32) as usize + 1
            };

            if offset > pos {
                bail!("StuffIt method 13: back-reference before start of output");
            }
            match_len = length;
            match_off = pos - offset;
        }

        // Emit one byte from the current match.
        match_len -= 1;
        let byte = window[match_off & WINDOW_MASK];
        match_off += 1;
        window[pos & WINDOW_MASK] = byte;
        out.push(byte);
        pos += 1;
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reverse_bits_works() {
        assert_eq!(reverse_bits(0b001, 3), 0b100);
        assert_eq!(reverse_bits(0b1011, 4), 0b1101);
        assert_eq!(reverse_bits(0, 5), 0);
    }

    #[test]
    fn bitreader_le_order() {
        // Byte 0b1010_0011: first bit out is LSB = 1.
        let mut br = BitReader::new(&[0b1010_0011]);
        assert_eq!(br.next_bit(), 1);
        assert_eq!(br.next_bit(), 1);
        assert_eq!(br.next_bit(), 0);
        // next 3 bits as a string -> 0b100 read LSB-first = 0,0,1 -> value 0b100=4
        assert_eq!(br.next_bits(3), 0b100);
    }
}
