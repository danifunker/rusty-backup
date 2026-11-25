// SPDX-License-Identifier: MIT
//! A prefix (Huffman) code mapping `(length, codeword)`, codewords read MSB-first, to a
//! symbol. Shared by StuffIt LZAH (method 5) and LZH (method 13); each reads bits its own way.

use std::collections::HashMap;

pub(crate) struct PrefixCode {
    map: HashMap<(u32, u32), i32>,
    pub(crate) maxlen: u32,
}

impl PrefixCode {
    /// Canonical code from per-symbol bit lengths (lengths < 1 mean "absent").
    pub(crate) fn from_lengths(lengths: &[i32]) -> PrefixCode {
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

    /// Explicit code from `(symbol, codeword_low_bit_first, length)` triples.
    pub(crate) fn from_explicit(entries: &[(i32, u32, u32)]) -> PrefixCode {
        let mut map = HashMap::new();
        let mut maxlen = 0;
        for &(sym, code_lbf, len) in entries {
            map.insert((len, reverse_bits(code_lbf, len)), sym);
            maxlen = maxlen.max(len);
        }
        PrefixCode { map, maxlen }
    }

    /// The next symbol, reading one bit at a time; `None` for a codeword not in the code.
    pub(crate) fn decode(&self, mut next_bit: impl FnMut() -> u32) -> Option<i32> {
        let mut acc: u32 = 0;
        for len in 1..=self.maxlen {
            acc = (acc << 1) | next_bit();
            if let Some(&sym) = self.map.get(&(len, acc)) {
                return Some(sym);
            }
        }
        None
    }
}

/// `val`'s low `len` bits in reverse order.
pub(crate) fn reverse_bits(val: u32, len: u32) -> u32 {
    let mut out = 0;
    for i in 0..len {
        if val & (1 << i) != 0 {
            out |= 1 << (len - 1 - i);
        }
    }
    out
}
