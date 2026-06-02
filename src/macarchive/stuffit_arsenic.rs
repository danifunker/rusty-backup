//! StuffIt compression **method 15** ("Arsenic"): an arithmetic-coded
//! BWT + MTF + RLE scheme (conceptually similar to bzip2). Used by StuffIt's
//! "maximum" setting and by most StuffIt 5 archives.
//!
//! Ported from XADMaster `XADStuffItArsenicHandle.m` + `BWT.c`. The bitstream
//! is read **MSB-first** (big-endian), unlike method 13's LSB-first reader.
//!
//! Pipeline per block: arithmetic decode → inverse MTF → inverse BWT →
//! de-randomize → a 4-byte run-length expansion. A trailing CRC-32 (zlib /
//! `0xedb88320`) covers the whole output and is verified.

use anyhow::{bail, Result};

/// MSB-first bit reader over a byte slice.
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
            self.buf = (self.buf << 8) | self.data[self.pos] as u64;
            self.pos += 1;
            self.nbits += 8;
        }
    }

    /// Read `n` bits MSB-first (1..=32). Past EOF reads as zero bits.
    fn next_bits(&mut self, n: u32) -> u32 {
        if n == 0 {
            return 0;
        }
        if self.nbits < n {
            self.fill();
        }
        if self.nbits < n {
            // Pad with zero bits at EOF.
            let avail = self.nbits;
            let v = if avail == 0 {
                0
            } else {
                ((self.buf << (n - avail)) & mask(n)) as u32
            };
            self.buf = 0;
            self.nbits = 0;
            return v;
        }
        let v = ((self.buf >> (self.nbits - n)) & mask(n)) as u32;
        self.nbits -= n;
        v
    }

    fn next_bit(&mut self) -> u32 {
        self.next_bits(1)
    }
}

fn mask(n: u32) -> u64 {
    if n >= 64 {
        u64::MAX
    } else {
        (1u64 << n) - 1
    }
}

// ---- adaptive arithmetic model + decoder ----

struct Model {
    increment: i64,
    frequency_limit: i64,
    total_frequency: i64,
    num_symbols: usize,
    first_symbol: i64,
    freq: Vec<i64>,
}

impl Model {
    fn new(first: i64, last: i64, increment: i64, frequency_limit: i64) -> Model {
        let num_symbols = (last - first + 1) as usize;
        let mut m = Model {
            increment,
            frequency_limit,
            total_frequency: 0,
            num_symbols,
            first_symbol: first,
            freq: vec![0; num_symbols],
        };
        m.reset();
        m
    }

    fn reset(&mut self) {
        self.total_frequency = self.increment * self.num_symbols as i64;
        for f in &mut self.freq {
            *f = self.increment;
        }
    }

    fn increase(&mut self, idx: usize) {
        self.freq[idx] += self.increment;
        self.total_frequency += self.increment;
        if self.total_frequency > self.frequency_limit {
            self.total_frequency = 0;
            for f in &mut self.freq {
                *f += 1;
                *f >>= 1;
                self.total_frequency += *f;
            }
        }
    }
}

const NUM_BITS: u32 = 26;
const ONE: i64 = 1 << (NUM_BITS - 1);
const HALF: i64 = 1 << (NUM_BITS - 2);

struct Decoder {
    range: i64,
    code: i64,
}

impl Decoder {
    fn new(br: &mut BitReader) -> Decoder {
        Decoder {
            range: ONE,
            code: br.next_bits(NUM_BITS) as i64,
        }
    }

    fn read_next(&mut self, br: &mut BitReader, symlow: i64, symsize: i64, symtot: i64) {
        let renorm = self.range / symtot;
        let lowincr = renorm * symlow;
        self.code -= lowincr;
        if symlow + symsize == symtot {
            self.range -= lowincr;
        } else {
            self.range = symsize * renorm;
        }
        while self.range <= HALF {
            self.range <<= 1;
            self.code = (self.code << 1) | br.next_bit() as i64;
        }
    }

    fn next_symbol(&mut self, br: &mut BitReader, model: &mut Model) -> i64 {
        let frequency = self.code / (self.range / model.total_frequency);
        let mut cumulative = 0i64;
        let mut n = 0usize;
        while n < model.num_symbols - 1 {
            if cumulative + model.freq[n] > frequency {
                break;
            }
            cumulative += model.freq[n];
            n += 1;
        }
        self.read_next(br, cumulative, model.freq[n], model.total_frequency);
        model.increase(n);
        model.first_symbol + n as i64
    }

    /// Read `bits` binary symbols (LSB-first) through `model`.
    fn next_bitstring(&mut self, br: &mut BitReader, model: &mut Model, bits: u32) -> u32 {
        let mut res = 0u32;
        for i in 0..bits {
            if self.next_symbol(br, model) != 0 {
                res |= 1 << i;
            }
        }
        res
    }
}

// ---- move-to-front ----

struct Mtf {
    table: [u8; 256],
}

impl Mtf {
    fn new() -> Mtf {
        let mut table = [0u8; 256];
        for (i, t) in table.iter_mut().enumerate() {
            *t = i as u8;
        }
        Mtf { table }
    }

    fn reset(&mut self) {
        for (i, t) in self.table.iter_mut().enumerate() {
            *t = i as u8;
        }
    }

    fn decode(&mut self, symbol: usize) -> u8 {
        let res = self.table[symbol];
        for i in (1..=symbol).rev() {
            self.table[i] = self.table[i - 1];
        }
        self.table[0] = res;
        res
    }
}

/// Inverse BWT: fills `transform` so that following the chain reconstructs the
/// original order (counting-sort method from `BWT.c`).
fn inverse_bwt(block: &[u8]) -> Vec<u32> {
    let n = block.len();
    let mut counts = [0u32; 256];
    for &b in block {
        counts[b as usize] += 1;
    }
    let mut cumulative = [0u32; 256];
    let mut total = 0u32;
    for i in 0..256 {
        cumulative[i] = total;
        total += counts[i];
        counts[i] = 0;
    }
    let mut transform = vec![0u32; n];
    for (i, &b) in block.iter().enumerate() {
        let b = b as usize;
        transform[(cumulative[b] + counts[b]) as usize] = i as u32;
        counts[b] += 1;
    }
    transform
}

// CRC-32 (zlib, reflected poly 0xedb88320), table-free per-byte form.
fn crc32_update(crc: u32, byte: u8) -> u32 {
    let mut c = (crc ^ byte as u32) & 0xff;
    for _ in 0..8 {
        if c & 1 != 0 {
            c = (c >> 1) ^ 0xedb88320;
        } else {
            c >>= 1;
        }
    }
    (crc >> 8) ^ c
}

const RANDOMIZATION_TABLE: [u16; 256] = [
    0xee, 0x56, 0xf8, 0xc3, 0x9d, 0x9f, 0xae, 0x2c, 0xad, 0xcd, 0x24, 0x9d, 0xa6, 0x101, 0x18,
    0xb9, 0xa1, 0x82, 0x75, 0xe9, 0x9f, 0x55, 0x66, 0x6a, 0x86, 0x71, 0xdc, 0x84, 0x56, 0x96, 0x56,
    0xa1, 0x84, 0x78, 0xb7, 0x32, 0x6a, 0x3, 0xe3, 0x2, 0x11, 0x101, 0x8, 0x44, 0x83, 0x100, 0x43,
    0xe3, 0x1c, 0xf0, 0x86, 0x6a, 0x6b, 0xf, 0x3, 0x2d, 0x86, 0x17, 0x7b, 0x10, 0xf6, 0x80, 0x78,
    0x7a, 0xa1, 0xe1, 0xef, 0x8c, 0xf6, 0x87, 0x4b, 0xa7, 0xe2, 0x77, 0xfa, 0xb8, 0x81, 0xee, 0x77,
    0xc0, 0x9d, 0x29, 0x20, 0x27, 0x71, 0x12, 0xe0, 0x6b, 0xd1, 0x7c, 0xa, 0x89, 0x7d, 0x87, 0xc4,
    0x101, 0xc1, 0x31, 0xaf, 0x38, 0x3, 0x68, 0x1b, 0x76, 0x79, 0x3f, 0xdb, 0xc7, 0x1b, 0x36, 0x7b,
    0xe2, 0x63, 0x81, 0xee, 0xc, 0x63, 0x8b, 0x78, 0x38, 0x97, 0x9b, 0xd7, 0x8f, 0xdd, 0xf2, 0xa3,
    0x77, 0x8c, 0xc3, 0x39, 0x20, 0xb3, 0x12, 0x11, 0xe, 0x17, 0x42, 0x80, 0x2c, 0xc4, 0x92, 0x59,
    0xc8, 0xdb, 0x40, 0x76, 0x64, 0xb4, 0x55, 0x1a, 0x9e, 0xfe, 0x5f, 0x6, 0x3c, 0x41, 0xef, 0xd4,
    0xaa, 0x98, 0x29, 0xcd, 0x1f, 0x2, 0xa8, 0x87, 0xd2, 0xa0, 0x93, 0x98, 0xef, 0xc, 0x43, 0xed,
    0x9d, 0xc2, 0xeb, 0x81, 0xe9, 0x64, 0x23, 0x68, 0x1e, 0x25, 0x57, 0xde, 0x9a, 0xcf, 0x7f, 0xe5,
    0xba, 0x41, 0xea, 0xea, 0x36, 0x1a, 0x28, 0x79, 0x20, 0x5e, 0x18, 0x4e, 0x7c, 0x8e, 0x58, 0x7a,
    0xef, 0x91, 0x2, 0x93, 0xbb, 0x56, 0xa1, 0x49, 0x1b, 0x79, 0x92, 0xf3, 0x58, 0x4f, 0x52, 0x9c,
    0x2, 0x77, 0xaf, 0x2a, 0x8f, 0x49, 0xd0, 0x99, 0x4d, 0x98, 0x101, 0x60, 0x93, 0x100, 0x75,
    0x31, 0xce, 0x49, 0x20, 0x56, 0x57, 0xe2, 0xf5, 0x26, 0x2b, 0x8a, 0xbf, 0xde, 0xd0, 0x83, 0x34,
    0xf4, 0x17,
];

struct Arsenic<'a> {
    br: BitReader<'a>,
    dec: Decoder,
    initial: Model,
    selector: Model,
    mtfmodel: Vec<Model>,
    mtf: Mtf,
    blockbits: u32,
    blocksize: usize,
    block: Vec<u8>,
    transform: Vec<u32>,
    transformindex: usize,
    numbytes: usize,
    bytecount: usize,
    endofblocks: bool,
    randomized: bool,
    randcount: usize,
    randindex: usize,
    repeat: i64,
    count: i64,
    last: u8,
    crc: u32,
    compcrc: u32,
}

impl<'a> Arsenic<'a> {
    fn new(comp: &'a [u8]) -> Result<Arsenic<'a>> {
        let mut br = BitReader::new(comp);
        let dec = Decoder::new(&mut br);
        let mut a = Arsenic {
            br,
            dec,
            initial: Model::new(0, 1, 1, 256),
            selector: Model::new(0, 10, 8, 1024),
            mtfmodel: vec![
                Model::new(2, 3, 8, 1024),
                Model::new(4, 7, 4, 1024),
                Model::new(8, 15, 4, 1024),
                Model::new(16, 31, 4, 1024),
                Model::new(32, 63, 2, 1024),
                Model::new(64, 127, 2, 1024),
                Model::new(128, 255, 1, 1024),
            ],
            mtf: Mtf::new(),
            blockbits: 0,
            blocksize: 0,
            block: Vec::new(),
            transform: Vec::new(),
            transformindex: 0,
            numbytes: 0,
            bytecount: 0,
            endofblocks: false,
            randomized: false,
            randcount: 0,
            randindex: 0,
            repeat: 0,
            count: 0,
            last: 0,
            crc: 0xffffffff,
            compcrc: 0,
        };

        if a.dec.next_bitstring(&mut a.br, &mut a.initial, 8) != b'A' as u32 {
            bail!("Arsenic: bad magic (expected 'A')");
        }
        if a.dec.next_bitstring(&mut a.br, &mut a.initial, 8) != b's' as u32 {
            bail!("Arsenic: bad magic (expected 's')");
        }
        a.blockbits = a.dec.next_bitstring(&mut a.br, &mut a.initial, 4) + 9;
        a.blocksize = 1 << a.blockbits;
        a.block = vec![0u8; a.blocksize];
        a.endofblocks = a.dec.next_symbol(&mut a.br, &mut a.initial) != 0;
        Ok(a)
    }

    fn read_block(&mut self) -> Result<()> {
        self.mtf.reset();
        self.randomized = self.dec.next_symbol(&mut self.br, &mut self.initial) != 0;
        self.transformindex =
            self.dec
                .next_bitstring(&mut self.br, &mut self.initial, self.blockbits)
                as usize;
        self.numbytes = 0;

        loop {
            let mut sel = self.dec.next_symbol(&mut self.br, &mut self.selector);
            if sel == 0 || sel == 1 {
                // Run of zeros (MTF index 0), bijective base-2 encoded.
                let mut zerostate = 1usize;
                let mut zerocount = 0usize;
                while sel < 2 {
                    if sel == 0 {
                        zerocount += zerostate;
                    } else {
                        zerocount += 2 * zerostate;
                    }
                    zerostate *= 2;
                    sel = self.dec.next_symbol(&mut self.br, &mut self.selector);
                }
                if self.numbytes + zerocount > self.blocksize {
                    bail!("Arsenic: zero run overflows block");
                }
                let b = self.mtf.decode(0);
                for _ in 0..zerocount {
                    self.block[self.numbytes] = b;
                    self.numbytes += 1;
                }
            }

            if sel == 10 {
                break;
            }
            let symbol = if sel == 2 {
                1
            } else {
                self.dec
                    .next_symbol(&mut self.br, &mut self.mtfmodel[(sel - 3) as usize])
            };
            if self.numbytes >= self.blocksize {
                bail!("Arsenic: block overflow");
            }
            self.block[self.numbytes] = self.mtf.decode(symbol as usize);
            self.numbytes += 1;
        }

        if self.transformindex >= self.numbytes {
            bail!("Arsenic: BWT index out of range");
        }
        self.selector.reset();
        for m in &mut self.mtfmodel {
            m.reset();
        }
        if self.dec.next_symbol(&mut self.br, &mut self.initial) != 0 {
            self.compcrc = self.dec.next_bitstring(&mut self.br, &mut self.initial, 32);
            self.endofblocks = true;
        }
        self.transform = inverse_bwt(&self.block[..self.numbytes]);
        Ok(())
    }

    fn produce_byte(&mut self) -> Result<Option<u8>> {
        let outbyte;
        if self.repeat > 0 {
            self.repeat -= 1;
            outbyte = self.last;
        } else {
            loop {
                if self.bytecount >= self.numbytes {
                    if self.endofblocks {
                        return Ok(None);
                    }
                    self.read_block()?;
                    self.bytecount = 0;
                    self.count = 0;
                    self.last = 0;
                    self.randindex = 0;
                    self.randcount = RANDOMIZATION_TABLE[0] as usize;
                }

                self.transformindex = self.transform[self.transformindex] as usize;
                let mut byte = self.block[self.transformindex];

                if self.randomized && self.randcount == self.bytecount {
                    byte ^= 1;
                    self.randindex = (self.randindex + 1) & 255;
                    self.randcount += RANDOMIZATION_TABLE[self.randindex] as usize;
                }

                self.bytecount += 1;

                if self.count == 4 {
                    self.count = 0;
                    if byte == 0 {
                        continue; // retry
                    }
                    self.repeat = byte as i64 - 1;
                    outbyte = self.last;
                    break;
                } else {
                    if byte == self.last {
                        self.count += 1;
                    } else {
                        self.count = 1;
                        self.last = byte;
                    }
                    outbyte = byte;
                    break;
                }
            }
        }

        self.crc = crc32_update(self.crc, outbyte);
        Ok(Some(outbyte))
    }
}

/// Decompress a StuffIt Arsenic (method 15) stream to `want` bytes, verifying
/// the trailing CRC-32.
pub fn decompress(comp: &[u8], want: usize) -> Result<Vec<u8>> {
    let mut a = Arsenic::new(comp)?;
    let mut out = Vec::with_capacity(want);
    while let Some(b) = a.produce_byte()? {
        out.push(b);
        if out.len() > want.saturating_add(1 << 20) {
            bail!("Arsenic: output exceeds expected size");
        }
    }
    if a.compcrc != !a.crc {
        bail!(
            "Arsenic: CRC mismatch (got {:#010x}, expected {:#010x})",
            !a.crc,
            a.compcrc
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_matches_known() {
        let mut crc = 0xffffffffu32;
        for &b in b"123456789" {
            crc = crc32_update(crc, b);
        }
        assert_eq!(!crc, 0xCBF43926); // standard CRC-32 check value
    }

    #[test]
    fn bitreader_msb_first() {
        let mut br = BitReader::new(&[0b1011_0000, 0xFF]);
        assert_eq!(br.next_bits(4), 0b1011);
        assert_eq!(br.next_bits(4), 0b0000);
        assert_eq!(br.next_bits(8), 0xFF);
    }

    #[test]
    fn inverse_bwt_roundtrip() {
        // BWT of "banana" with primary index, inverted, should give "banana".
        // Last column of sorted rotations of "banana\x00"-style is tricky; use a
        // direct check: build transform and follow it.
        let block = b"nnbaaa"; // BWT last column of "banana" (primary index 3)
        let transform = inverse_bwt(block);
        let mut idx = 3usize;
        let mut out = Vec::new();
        for _ in 0..block.len() {
            idx = transform[idx] as usize;
            out.push(block[idx]);
        }
        assert_eq!(&out, b"banana");
    }
}
