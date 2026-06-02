//! StuffIt compression **method 2** ("Compress"): the classic Unix
//! `compress(1)` LZW with block mode (code 256 = table clear). StuffIt invokes
//! it with flags `0x8e` → block mode, 14-bit maximum code width.
//!
//! Ported from XADMaster `XADCompressHandle.m` + `LZW.c`. Codes are read
//! LSB-first; the width grows from 9 bits as the dictionary fills, and after a
//! clear the encoder pads to an 8-code boundary (the well-known `compress`
//! quirk).

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
    let mut out = Vec::with_capacity(want);
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
}
