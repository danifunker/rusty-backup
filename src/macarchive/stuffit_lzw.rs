//! StuffIt compression **method 2** ("Compress"): the classic Unix
//! `compress(1)` LZW with block mode (code 256 = table clear). StuffIt invokes
//! it with flags `0x8e` → block mode, 14-bit maximum code width.
//!
//! Ported from XADMaster `XADCompressHandle.m` + `LZW.c`. Codes are read
//! LSB-first; the width grows from 9 bits as the dictionary fills, and after a
//! clear the encoder pads to an 8-code boundary (the well-known `compress`
//! quirk).

#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
use anyhow::{bail, Result};

/// LSB-first bit cursor over the compressed bytes.
struct BitCursor<'a> {
    data: &'a [u8],
    cursor: usize, // in bits
}

impl<'a> BitCursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        BitCursor { data, cursor: 0 }
    }
    fn remaining(&self) -> usize {
        self.data.len() * 8 - self.cursor
    }
    fn next_bits(&mut self, n: u32) -> u32 {
        let mut v = 0u32;
        for i in 0..n {
            let idx = self.cursor;
            let bit = (self.data[idx / 8] >> (idx % 8)) & 1;
            v |= (bit as u32) << i;
            self.cursor += 1;
        }
        v
    }
    fn skip(&mut self, n: usize) {
        self.cursor += n;
    }
}

const CLEAR: i32 = 256;

struct Lzw {
    chr: Vec<u8>,
    parent: Vec<i32>,
    numsymbols: i32,
    maxsymbols: i32,
    reserved: i32,
    prevsymbol: i32,
    symbolsize: u32,
}

impl Lzw {
    fn new(maxsymbols: i32, reserved: i32) -> Lzw {
        let mut chr = vec![0u8; maxsymbols as usize];
        let mut parent = vec![-1i32; maxsymbols as usize];
        for i in 0..256 {
            chr[i] = i as u8;
            parent[i] = -1;
        }
        let mut l = Lzw {
            chr,
            parent,
            numsymbols: 0,
            maxsymbols,
            reserved,
            prevsymbol: -1,
            symbolsize: 9,
        };
        l.clear();
        l
    }

    fn clear(&mut self) {
        self.numsymbols = 256 + self.reserved;
        self.prevsymbol = -1;
        self.symbolsize = 9;
    }

    fn full(&self) -> bool {
        self.numsymbols == self.maxsymbols
    }

    fn first_byte(&self, mut symbol: i32) -> u8 {
        while self.parent[symbol as usize] >= 0 {
            symbol = self.parent[symbol as usize];
        }
        self.chr[symbol as usize]
    }

    /// Advance the dictionary with `symbol`; sets `prevsymbol`. Returns the
    /// decoded output string (in forward order).
    fn next_symbol(&mut self, symbol: i32, out: &mut Vec<u8>) -> Result<()> {
        if self.prevsymbol < 0 {
            if symbol >= self.numsymbols {
                bail!("LZW: invalid first code {symbol}");
            }
            self.prevsymbol = symbol;
            self.emit(out);
            return Ok(());
        }

        let postfix = if symbol < self.numsymbols {
            self.first_byte(symbol)
        } else if symbol == self.numsymbols {
            self.first_byte(self.prevsymbol)
        } else {
            bail!(
                "LZW: invalid code {symbol} (numsymbols {})",
                self.numsymbols
            );
        };

        let parent = self.prevsymbol;
        self.prevsymbol = symbol;

        if !self.full() {
            let idx = self.numsymbols as usize;
            self.parent[idx] = parent;
            self.chr[idx] = postfix;
            self.numsymbols += 1;
            if !self.full() && (self.numsymbols & (self.numsymbols - 1)) == 0 {
                self.symbolsize += 1;
            }
        }
        self.emit(out);
        Ok(())
    }

    /// Append the string for `prevsymbol` (reverse the parent chain).
    fn emit(&self, out: &mut Vec<u8>) {
        let start = out.len();
        let mut symbol = self.prevsymbol;
        while symbol >= 0 {
            out.push(self.chr[symbol as usize]);
            symbol = self.parent[symbol as usize];
        }
        out[start..].reverse();
    }
}

/// Decompress a StuffIt method-2 (Compress/LZW) stream to `want` bytes.
/// `flags` is the StuffIt compress-flags byte (0x8e in practice).
pub fn decompress(comp: &[u8], want: usize, flags: u8) -> Result<Vec<u8>> {
    let blockmode = flags & 0x80 != 0;
    let maxsymbols = 1i32 << (flags & 0x1f);
    if maxsymbols <= 256 {
        bail!("LZW: bad max code width in flags {flags:#x}");
    }
    let reserved = if blockmode { 1 } else { 0 };
    let mut lzw = Lzw::new(maxsymbols, reserved);

    let mut br = BitCursor::new(comp);
    // `want` can be unbounded (Unix .Z has no length), so cap the up-front reservation.
    let mut out = Vec::with_capacity(want.min(comp.len().saturating_mul(4)));
    let mut symbolcounter: usize = 0;

    while out.len() < want {
        // Read the next non-clear code.
        let symbol = loop {
            if br.remaining() < lzw.symbolsize as usize {
                return Ok(out); // ran out of input
            }
            let s = br.next_bits(lzw.symbolsize) as i32;
            symbolcounter += 1;
            if s == CLEAR && blockmode {
                // Pad to an 8-code group boundary, then clear the table.
                let symsize = lzw.symbolsize as usize;
                if !symbolcounter.is_multiple_of(8) {
                    br.skip(symsize * (8 - symbolcounter % 8));
                }
                lzw.clear();
                symbolcounter = 0;
            } else {
                break s;
            }
        };

        lzw.next_symbol(symbol, &mut out)?;
    }

    Ok(out)
}

/// Decompress a Unix `compress(1)` `.Z` stream: magic `1F 9D`, a flags byte, then LZW to EOF.
pub fn decompress_unix_z(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < 3 || data[0..2] != [0x1F, 0x9D] {
        bail!("not a compress(1) .Z stream");
    }
    decompress(&data[3..], usize::MAX, data[2])
}

/// Compress `data` as a Unix `compress(1)` `.Z` stream (16-bit, block mode), byte-compatible with
/// classic `compress.c`: codes grow from 9 bits, and each width change pads to a whole code group.
pub fn compress_unix_z(data: &[u8]) -> Vec<u8> {
    const MAXBITS: u32 = 16;
    const MAXMAXCODE: u32 = 1 << MAXBITS;
    const CLEAR: u32 = 256;
    const FIRST: u32 = 257;

    struct Out {
        bytes: Vec<u8>,
        buf: [u8; 16],
        offset: u32,
        n_bits: u32,
        maxcode: u32,
    }
    impl Out {
        /// `compress.c`'s `output()`: pack a code LSB-first; flush a group of eight codes, and
        /// flush the (zero-padded) group whenever the width is about to change.
        fn code(&mut self, code: u32, free_ent: u32, clear: &mut bool) {
            for i in 0..self.n_bits {
                if code >> i & 1 != 0 {
                    let bit = self.offset + i;
                    self.buf[(bit / 8) as usize] |= 1 << (bit % 8);
                }
            }
            self.offset += self.n_bits;
            if self.offset == self.n_bits * 8 {
                self.bytes
                    .extend_from_slice(&self.buf[..self.n_bits as usize]);
                self.buf = [0; 16];
                self.offset = 0;
            }
            if free_ent > self.maxcode || *clear {
                if self.offset > 0 {
                    self.bytes
                        .extend_from_slice(&self.buf[..self.n_bits as usize]);
                    self.buf = [0; 16];
                }
                self.offset = 0;
                if *clear {
                    self.n_bits = 9;
                    *clear = false;
                } else {
                    self.n_bits += 1;
                }
                self.maxcode = if self.n_bits == MAXBITS {
                    MAXMAXCODE
                } else {
                    (1 << self.n_bits) - 1
                };
            }
        }

        fn finish(&mut self) {
            let n = self.offset.div_ceil(8) as usize;
            self.bytes.extend_from_slice(&self.buf[..n]);
        }
    }

    let mut out = Out {
        bytes: vec![0x1F, 0x9D, 0x80 | MAXBITS as u8],
        buf: [0; 16],
        offset: 0,
        n_bits: 9,
        maxcode: (1 << 9) - 1,
    };
    let Some((&first, rest)) = data.split_first() else {
        return out.bytes;
    };
    let mut table: std::collections::HashMap<(u32, u8), u32> = std::collections::HashMap::new();
    let mut free_ent = FIRST;
    let mut clear = false;
    let mut ent = u32::from(first);
    for &c in rest {
        if let Some(&code) = table.get(&(ent, c)) {
            ent = code;
            continue;
        }
        out.code(ent, free_ent, &mut clear);
        if free_ent < MAXMAXCODE {
            table.insert((ent, c), free_ent);
            free_ent += 1;
        } else {
            // A full table: block mode lets the stream start a fresh dictionary.
            table.clear();
            free_ent = FIRST;
            clear = true;
            out.code(CLEAR, free_ent, &mut clear);
        }
        ent = u32::from(c);
    }
    out.code(ent, free_ent, &mut clear);
    out.finish();
    out.bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_plain_lzw_roundtrip() {
        // Encode "TOBEORNOTTOBEORTOBEORNOT" with a tiny non-block-mode LZW so we
        // can check the decoder independently of the compress block quirks.
        // Build the stream by hand using 9-bit growing codes is complex; instead
        // verify the dictionary mechanics on a trivial repeat via block-mode
        // real data is covered by the integration test against real .sit files.
        // Here we just confirm a single-literal run decodes.
        // Codes: 'A'(65) 'A'(65) <256+0 = "AA"> with 9-bit LSB-first, no clear.
        // bits: 65 -> 9 bits, 65 -> 9 bits, 257 -> 9 bits
        let mut bits: Vec<u8> = Vec::new();
        let mut acc = 0u32;
        let mut nbits = 0u32;
        let push = |code: u32, width: u32, bits: &mut Vec<u8>, acc: &mut u32, nbits: &mut u32| {
            *acc |= code << *nbits;
            *nbits += width;
            while *nbits >= 8 {
                bits.push((*acc & 0xff) as u8);
                *acc >>= 8;
                *nbits -= 8;
            }
        };
        push(65, 9, &mut bits, &mut acc, &mut nbits);
        push(65, 9, &mut bits, &mut acc, &mut nbits);
        push(257, 9, &mut bits, &mut acc, &mut nbits);
        if nbits > 0 {
            bits.push((acc & 0xff) as u8);
        }
        // Non-block-mode (flags without 0x80): maxsymbols 1<<9 = 512.
        let out = decompress(&bits, 4, 0x09).unwrap();
        assert_eq!(&out, b"AAAA");
    }

    #[test]
    fn compress_round_trips_through_our_decoder() {
        let mut data = Vec::new();
        // Text, then incompressible bytes, enough to pass 16-bit codes and force a CLEAR.
        for i in 0..40_000u32 {
            data.extend_from_slice(format!("line {i} of a SunOS tar section\n").as_bytes());
        }
        let mut x = 0x1234_5678u32;
        for _ in 0..300_000 {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            data.push(x as u8);
        }
        let z = compress_unix_z(&data);
        assert_eq!(&z[..3], &[0x1F, 0x9D, 0x90]);
        assert!(z.len() < data.len());
        assert_eq!(decompress_unix_z(&z).unwrap(), data);
        // gzip reads .Z with its own LZW decoder: an independent check when it is installed.
        if let Ok(mut child) = std::process::Command::new("gzip")
            .arg("-dc")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
        {
            let mut stdin = child.stdin.take().unwrap();
            let z2 = z.clone();
            let writer = std::thread::spawn(move || {
                use std::io::Write;
                stdin.write_all(&z2).unwrap();
            });
            let out = child.wait_with_output().unwrap();
            writer.join().unwrap();
            assert!(out.status.success(), "gzip -dc rejected our .Z");
            assert_eq!(out.stdout, data, "gzip -dc decoded different bytes");
        }
        assert_eq!(decompress_unix_z(&compress_unix_z(b"")).unwrap(), b"");
        assert_eq!(decompress_unix_z(&compress_unix_z(b"a")).unwrap(), b"a");
    }
}
