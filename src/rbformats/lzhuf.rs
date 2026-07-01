//! LZHUF decompression (Haruhiko Okumura's public-domain LZHUF: LZSS + adaptive
//! Huffman), as used by DART's "best" compression.
//!
//! This is the decode half only. It is a faithful port of the classic algorithm
//! (cross-checked against CiderPress2's `LZHufStream`, Apache-2.0), parameterised
//! for DART's variant: the leading 4-byte length word is omitted (the caller
//! supplies the expected output length) and the ring buffer is initialised with a
//! caller-chosen byte (0x00 for DART) instead of the traditional 0x20.

const N: usize = 4096; // ring buffer size
const F: usize = 60; // lookahead / max match length
const THRESHOLD: usize = 2; // minimum match length is THRESHOLD + 1
const N_CHAR: usize = 256 - THRESHOLD + F; // 314 codes: literals + match lengths
const T: usize = N_CHAR * 2 - 1; // 627 tree nodes
const R: usize = T - 1; // 626, root
const MAX_FREQ: u16 = 0x8000; // rebuild tree when the root frequency hits this

/// Upper 6 bits of a match position, recovered from the first byte read.
#[rustfmt::skip]
const D_CODE: [u8; 256] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
    0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
    0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03,
    0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
    0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07,
    0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x09, 0x09, 0x09, 0x09, 0x09, 0x09, 0x09, 0x09,
    0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0B, 0x0B, 0x0B, 0x0B, 0x0B, 0x0B, 0x0B, 0x0B,
    0x0C, 0x0C, 0x0C, 0x0C, 0x0D, 0x0D, 0x0D, 0x0D, 0x0E, 0x0E, 0x0E, 0x0E, 0x0F, 0x0F, 0x0F, 0x0F,
    0x10, 0x10, 0x10, 0x10, 0x11, 0x11, 0x11, 0x11, 0x12, 0x12, 0x12, 0x12, 0x13, 0x13, 0x13, 0x13,
    0x14, 0x14, 0x14, 0x14, 0x15, 0x15, 0x15, 0x15, 0x16, 0x16, 0x16, 0x16, 0x17, 0x17, 0x17, 0x17,
    0x18, 0x18, 0x19, 0x19, 0x1A, 0x1A, 0x1B, 0x1B, 0x1C, 0x1C, 0x1D, 0x1D, 0x1E, 0x1E, 0x1F, 0x1F,
    0x20, 0x20, 0x21, 0x21, 0x22, 0x22, 0x23, 0x23, 0x24, 0x24, 0x25, 0x25, 0x26, 0x26, 0x27, 0x27,
    0x28, 0x28, 0x29, 0x29, 0x2A, 0x2A, 0x2B, 0x2B, 0x2C, 0x2C, 0x2D, 0x2D, 0x2E, 0x2E, 0x2F, 0x2F,
    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E, 0x3F,
];

/// Number of bits in a match-position code, indexed by the first byte read.
#[rustfmt::skip]
const D_LEN: [u8; 256] = [
    0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03,
    0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03,
    0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04,
    0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04,
    0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04,
    0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
    0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
    0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
    0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
    0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06,
    0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06,
    0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06,
    0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07,
    0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07,
    0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07,
    0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
];

struct Lzhuf<'a> {
    input: &'a [u8],
    pos: usize,
    getbuf: u16,
    getlen: i32,
    freq: [u16; T + 1],
    prnt: [u16; T + N_CHAR],
    son: [u16; T],
    text_buf: [u8; N + F - 1],
}

impl<'a> Lzhuf<'a> {
    fn new(input: &'a [u8]) -> Self {
        Lzhuf {
            input,
            pos: 0,
            getbuf: 0,
            getlen: 0,
            freq: [0; T + 1],
            prnt: [0; T + N_CHAR],
            son: [0; T],
            text_buf: [0; N + F - 1],
        }
    }

    /// Next input byte, or 0 past end-of-input (matching the reference, which
    /// pads with zero bits so a truncated stream fails cleanly on length).
    fn read_byte(&mut self) -> u16 {
        if self.pos < self.input.len() {
            let b = self.input[self.pos];
            self.pos += 1;
            b as u16
        } else {
            0
        }
    }

    fn get_bit(&mut self) -> u16 {
        while self.getlen <= 8 {
            let i = self.read_byte();
            self.getbuf |= i << (8 - self.getlen);
            self.getlen += 8;
        }
        let i = self.getbuf;
        self.getbuf <<= 1;
        self.getlen -= 1;
        i >> 15
    }

    fn get_byte(&mut self) -> u16 {
        while self.getlen <= 8 {
            let i = self.read_byte();
            self.getbuf |= i << (8 - self.getlen);
            self.getlen += 8;
        }
        let i = self.getbuf;
        self.getbuf <<= 8;
        self.getlen -= 8;
        i >> 8
    }

    fn start_huff(&mut self) {
        for i in 0..N_CHAR {
            self.freq[i] = 1;
            self.son[i] = (i + T) as u16;
            self.prnt[i + T] = i as u16;
        }
        let mut i = 0usize;
        let mut j = N_CHAR;
        while j <= R {
            self.freq[j] = self.freq[i].wrapping_add(self.freq[i + 1]);
            self.son[j] = i as u16;
            self.prnt[i] = j as u16;
            self.prnt[i + 1] = j as u16;
            i += 2;
            j += 1;
        }
        self.freq[T] = 0xffff;
        self.prnt[R] = 0;
    }

    fn reconst(&mut self) {
        // Collect leaves in the first half, halving frequencies.
        let mut j = 0usize;
        for i in 0..T {
            if self.son[i] as usize >= T {
                self.freq[j] = self.freq[i].wrapping_add(1) / 2;
                self.son[j] = self.son[i];
                j += 1;
            }
        }
        // Rebuild internal nodes by merging the two lowest-frequency subtrees.
        let mut i = 0usize;
        let mut j = N_CHAR;
        while j < T {
            let f = self.freq[i].wrapping_add(self.freq[i + 1]);
            self.freq[j] = f;
            let mut k = j - 1;
            while k > 0 && f < self.freq[k] {
                k -= 1;
            }
            if f >= self.freq[k] {
                k += 1;
            }
            let l = j - k;
            self.freq.copy_within(k..k + l, k + 1);
            self.freq[k] = f;
            self.son.copy_within(k..k + l, k + 1);
            self.son[k] = i as u16;
            i += 2;
            j += 1;
        }
        // Re-link parents.
        for i in 0..T {
            let k = self.son[i] as usize;
            self.prnt[k] = i as u16;
            if k < T {
                self.prnt[k + 1] = i as u16;
            }
        }
    }

    fn update(&mut self, c: u16) {
        if self.freq[R] == MAX_FREQ {
            self.reconst();
        }
        let mut c = self.prnt[c as usize + T] as usize;
        loop {
            self.freq[c] = self.freq[c].wrapping_add(1);
            let k = self.freq[c];
            // If order is disturbed, swap with the next node.
            let mut l = c + 1;
            if k > self.freq[l] {
                l += 1;
                while k > self.freq[l] {
                    l += 1;
                }
                l -= 1;
                self.freq[c] = self.freq[l];
                self.freq[l] = k;

                let i = self.son[c];
                self.prnt[i as usize] = l as u16;
                if (i as usize) < T {
                    self.prnt[i as usize + 1] = l as u16;
                }
                let m = self.son[l];
                self.son[l] = i;
                self.prnt[m as usize] = c as u16;
                if (m as usize) < T {
                    self.prnt[m as usize + 1] = c as u16;
                }
                self.son[c] = m;
                c = l;
            }
            c = self.prnt[c] as usize;
            if c == 0 {
                break;
            }
        }
    }

    fn decode_char(&mut self) -> u16 {
        let mut c = self.son[R];
        while (c as usize) < T {
            c += self.get_bit();
            c = self.son[c as usize];
        }
        c -= T as u16;
        self.update(c);
        c
    }

    fn decode_position(&mut self) -> usize {
        let i0 = self.get_byte();
        let mut i = i0;
        let c = (D_CODE[i0 as usize] as usize) << 6;
        let j = D_LEN[i0 as usize] as usize - 2;
        for _ in 0..j {
            i = (i << 1) + self.get_bit();
        }
        c | (i as usize & 0x3f)
    }
}

/// Decompress an LZHUF stream into exactly `expected_len` bytes. `window_init`
/// seeds the ring buffer (0x00 for DART). Errors only on an impossible tree
/// state; a truncated stream yields zero-padding and still returns `expected_len`
/// bytes (the caller validates against a checksum / structure).
pub fn lzhuf_decompress(
    input: &[u8],
    expected_len: usize,
    window_init: u8,
) -> Result<Vec<u8>, String> {
    let mut s = Lzhuf::new(input);
    s.start_huff();
    for b in s.text_buf.iter_mut().take(N - F) {
        *b = window_init;
    }
    let mut r = N - F;
    let mut out = Vec::with_capacity(expected_len);

    while out.len() < expected_len {
        let c = s.decode_char();
        if (c as usize) < 256 {
            out.push(c as u8);
            s.text_buf[r] = c as u8;
            r = (r + 1) & (N - 1);
        } else {
            let dist = s.decode_position();
            let base = (r + N - dist - 1) & (N - 1);
            let len = c as usize - 255 + THRESHOLD;
            for k in 0..len {
                if out.len() >= expected_len {
                    break;
                }
                let ch = s.text_buf[(base + k) & (N - 1)];
                out.push(ch);
                s.text_buf[r] = ch;
                r = (r + 1) & (N - 1);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn does_not_panic_on_zero_input() {
        // Exercises the control flow on all-zero input (decoded as a run of the
        // first literal) and confirms it produces exactly the requested length.
        let out = lzhuf_decompress(&[0u8; 64], 100, 0x00).expect("decode");
        assert_eq!(out.len(), 100);
    }

    #[test]
    fn decodes_real_dart_lzh_chunk() {
        // Chunk 0 of CiderPress2's `test1-best` (Apache-2.0 test data): a real
        // DART "best" (LZHUF) 20960-byte chunk from a Macintosh 800K disk. This
        // is the byte-exact golden vector that guards the D_LEN position table
        // and the decode loop against regression.
        let comp = include_bytes!("../../tests/fixtures/dart/lzh_chunk0.bin");
        let out = lzhuf_decompress(comp, 20960, 0x00).expect("decode");
        assert_eq!(out.len(), 20960);
        // CRC32 of the correct decompressed chunk (40 512-byte data blocks + 40
        // zero-filled tag sets).
        assert_eq!(crc32fast::hash(&out), 0xd882_b699);
    }
}
