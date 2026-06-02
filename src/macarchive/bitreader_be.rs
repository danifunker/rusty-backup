//! Big-endian (MSB-first) bit reader shared by the StuffIt Huffman (method 3)
//! and LZAH (method 5) codecs. Matches XADMaster's `CSInputNextBit` /
//! `CSInputNextBitString` (the non-`LE` variants).

/// MSB-first bit reader over a byte slice. Past EOF reads as zero bits.
pub struct BitReaderBE<'a> {
    data: &'a [u8],
    pos: usize,
    buf: u64,
    nbits: u32,
}

impl<'a> BitReaderBE<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        BitReaderBE {
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

    /// Read `n` bits (1..=32), MSB-first.
    pub fn next_bits(&mut self, n: u32) -> u32 {
        if n == 0 {
            return 0;
        }
        if self.nbits < n {
            self.fill();
        }
        if self.nbits < n {
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

    pub fn next_bit(&mut self) -> u32 {
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
