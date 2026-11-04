// SPDX-License-Identifier: LGPL-2.1-or-later
// Derived from Aaru.Compression.Native and XADMaster; see THIRD_PARTY_NOTICES.md.
//! ShrinkWrap 3 **"SIT2"** decompressor: the StuffIt codec behind Disk Copy 6 / NDIF
//! chunk type `0xF0` (images made by Aladdin ShrinkWrap 3, creator `Wrap`).
//!
//! It is StuffIt method 14 ("Installer") cut down: one block per chunk, 276 literal /
//! length symbols, 16 distance symbols, no sliding-window wrap. Ported from Aaru's
//! `stuffit/shrinkwrap.c` (reverse-engineered from ShrinkWrap 3.5.1's `Dcmp` 240), whose
//! tree reader is XADMaster's `SIT14_ReadTree`.
//!
//! Chunk layout: a 16-byte header (`u32` compressed length, `u32` LE uncompressed length
//! twice, `0xFFFFFFFF`), then the LSB-first bitstream: the literal/length tree, the
//! distance tree (each byte-aligned after), then the codes.

use anyhow::{bail, Result};

use crate::stuffit_lzh::BitReader;

const LITLEN_SYMBOLS: usize = 276;
const DIST_SYMBOLS: usize = 16;
const HEADER_LEN: usize = 16;
/// Nested code-length trees never go deeper than one level in real streams.
const MAX_TREE_DEPTH: u32 = 4;

/// Uncompressed length a ShrinkWrap chunk declares in its header, if it has one.
pub fn declared_len(chunk: &[u8]) -> Option<usize> {
    let b = chunk.get(4..8)?;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
}

/// Decompress one ShrinkWrap SIT2 chunk (header included), refusing outputs over `max_out`.
pub fn decompress(chunk: &[u8], max_out: usize) -> Result<Vec<u8>> {
    if chunk.len() < HEADER_LEN {
        bail!("ShrinkWrap: chunk shorter than its 16-byte header");
    }
    let want = declared_len(chunk).unwrap_or(0);
    if want > max_out {
        bail!("ShrinkWrap: chunk declares {want} bytes, more than the {max_out} it may hold");
    }
    let mut len_base = [0u32; 20];
    let mut len_extra = [0u32; 20];
    let mut k = 0u32;
    for i in 0..20 {
        len_extra[i] = if i >= 2 { (i as u32 - 2) >> 1 } else { 0 };
        len_base[i] = k + 4;
        k += 1 << len_extra[i];
    }
    let mut dist_base = [0u32; 16];
    let mut k = 1u32;
    for (i, base) in dist_base.iter_mut().enumerate() {
        *base = k;
        k += 1 << i;
    }

    let mut br = BitReader::new(&chunk[HEADER_LEN..]);
    let litlen = read_tree(&mut br, LITLEN_SYMBOLS, 0)?;
    let dist = read_tree(&mut br, DIST_SYMBOLS, 0)?;

    let mut out = Vec::with_capacity(want);
    while out.len() < want {
        let sym = walk(&mut br, litlen.as_slice(), LITLEN_SYMBOLS)?;
        if sym < 256 {
            out.push(sym as u8);
            continue;
        }
        let li = sym - 256;
        if li >= 20 {
            bail!("ShrinkWrap: length symbol {li} out of range");
        }
        let mut len = len_base[li] + br.next_bits(len_extra[li]);
        let di = walk(&mut br, dist.as_slice(), DIST_SYMBOLS)?;
        let distance = dist_base[di] as usize + br.next_bits(di as u32) as usize;
        if distance == 0 || distance > out.len() {
            bail!(
                "ShrinkWrap: back-reference {distance} before the start of the output ({})",
                out.len()
            );
        }
        while len > 0 && out.len() < want {
            let b = out[out.len() - distance];
            out.push(b);
            len -= 1;
        }
    }
    Ok(out)
}

/// Follow one codeword through a tree built by [`read_tree`]; returns the symbol.
fn walk(br: &mut BitReader, tree: &[u16], symbols: usize) -> Result<usize> {
    let leaf = symbols * 2;
    let mut node = 0usize;
    for _ in 0..=32 {
        node = match tree.get(node + br.next_bit() as usize) {
            Some(&n) if n != 0 => n as usize,
            _ => bail!("ShrinkWrap: invalid Huffman code in the stream"),
        };
        if node >= leaf {
            return Ok(node - leaf);
        }
    }
    bail!("ShrinkWrap: Huffman code longer than 32 bits")
}

/// Read a code-length table (method 14's `SIT14_ReadTree`) and build its decode tree.
fn read_tree(br: &mut BitReader, codesize: usize, depth: u32) -> Result<Vec<u16>> {
    let zero_flag = br.next_bits(1);
    let width = br.next_bits(2) + 2;
    let offset = br.next_bits(3) + 1;
    let size = 1usize << width;
    let repeat = size - 1;
    // With the flag set, the value one below `repeat` stands for an absent symbol.
    let absent = if zero_flag != 0 {
        Some(repeat - 1)
    } else {
        None
    };
    let mut code = vec![0u8; codesize];
    let mut i = 0usize;
    if br.next_bits(2) & 1 != 0 {
        if depth >= MAX_TREE_DEPTH {
            bail!("ShrinkWrap: code-length trees nested too deeply");
        }
        let sub = read_tree(br, size, depth + 1)?;
        while i < codesize {
            let l = walk(br, sub.as_slice(), size)?;
            if Some(l) == absent {
                code[i] = 0;
                i += 1;
            } else if l == repeat {
                let n = walk(br, sub.as_slice(), size)? + 3;
                i = repeat_previous(code.as_mut_slice(), i, n)?;
            } else {
                code[i] = (l as u32 + offset) as u8;
                i += 1;
            }
        }
    } else {
        while i < codesize {
            let l = br.next_bits(width) as usize;
            if Some(l) == absent {
                code[i] = 0;
                i += 1;
            } else if l == repeat {
                let n = br.next_bits(width) as usize + 3;
                i = repeat_previous(code.as_mut_slice(), i, n)?;
            } else {
                code[i] = (l as u32 + offset) as u8;
                i += 1;
            }
        }
    }
    let tree = build_tree(code.as_slice())?;
    br.align_to_byte();
    Ok(tree)
}

/// Copy the previous code length `n` times from `i`; returns the new position.
fn repeat_previous(code: &mut [u8], i: usize, n: usize) -> Result<usize> {
    if i == 0 {
        bail!("ShrinkWrap: code-length repeat with no previous length");
    }
    let end = (i + n).min(code.len());
    let prev = code[i - 1];
    for c in &mut code[i..end] {
        *c = prev;
    }
    Ok(end)
}

/// Canonical codes in method 14's order (its quicksort is unstable, so it is kept verbatim).
fn build_tree(code: &[u8]) -> Result<Vec<u16>> {
    let n = code.len();
    if code.iter().any(|&c| c > 32) {
        bail!("ShrinkWrap: code length over 32 bits");
    }
    let mut sorted = code.to_vec();
    let mut order: Vec<u16> = (0..n as u16).collect();
    sort_codes(0, n, sorted.as_mut_slice(), order.as_mut_slice());

    let mut bits = vec![0u64; n];
    let mut i = sorted.iter().position(|&c| c != 0).unwrap_or(n);
    let mut j = 0u64;
    while i < n {
        if i > 0 {
            j <<= sorted[i] - sorted[i - 1];
        }
        let mut m = 0u64;
        let mut l = j;
        for _ in 0..sorted[i] {
            m = (m << 1) | (l & 1);
            l >>= 1;
        }
        bits[order[i] as usize] = m;
        i += 1;
        j += 1;
    }

    let mut tree = vec![0u16; n * 2];
    let mut next = 2usize;
    for (sym, &len) in code.iter().enumerate() {
        let mut node = 0usize;
        let mut m = bits[sym];
        for k in 0..len {
            node += (m & 1) as usize;
            let slot = match tree.get_mut(node) {
                Some(s) => s,
                None => bail!("ShrinkWrap: over-subscribed Huffman code"),
            };
            if k + 1 == len {
                *slot = (n * 2 + sym) as u16;
            } else {
                if *slot == 0 {
                    if next >= n * 2 {
                        bail!("ShrinkWrap: Huffman code needs more nodes than it has symbols");
                    }
                    *slot = next as u16;
                    next += 2;
                }
                node = *slot as usize;
            }
            m >>= 1;
        }
    }
    Ok(tree)
}

/// `SIT14_Update`: the in-place quicksort over `[first, last)` that sets the code order.
fn sort_codes(mut first: usize, mut last: usize, code: &mut [u8], order: &mut [u16]) {
    while last - first > 1 {
        let mut i = first;
        let mut j = last;
        loop {
            i += 1;
            while i < last && code[first] > code[i] {
                i += 1;
            }
            j -= 1;
            while j > first && code[first] < code[j] {
                j -= 1;
            }
            if j > i {
                code.swap(i, j);
                order.swap(i, j);
            } else {
                break;
            }
        }
        if first != j {
            code.swap(first, j);
            order.swap(first, j);
            let i = j + 1;
            if last - i <= j - first {
                sort_codes(i, last, code, order);
                last = j;
            } else {
                sort_codes(first, j, code, order);
                first = i;
            }
        } else {
            first += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// LSB-first bit writer, the mirror of the decoder's reader.
    struct BitWriter {
        out: Vec<u8>,
        acc: u32,
        n: u32,
    }

    impl BitWriter {
        fn put(&mut self, v: u32, bits: u32) {
            for b in 0..bits {
                self.acc |= ((v >> b) & 1) << self.n;
                self.n += 1;
                if self.n == 8 {
                    self.out.push(self.acc as u8);
                    self.acc = 0;
                    self.n = 0;
                }
            }
        }
        fn align(&mut self) {
            if self.n > 0 {
                self.out.push(self.acc as u8);
                self.acc = 0;
                self.n = 0;
            }
        }
    }

    /// Codeword bits per symbol for `lengths`, the way [`build_tree`] assigns them.
    fn codes_for(lengths: &[u8]) -> Vec<u64> {
        let n = lengths.len();
        let mut sorted = lengths.to_vec();
        let mut order: Vec<u16> = (0..n as u16).collect();
        sort_codes(0, n, &mut sorted, &mut order);
        let mut bits = vec![0u64; n];
        let mut i = sorted.iter().position(|&c| c != 0).unwrap_or(n);
        let mut j = 0u64;
        while i < n {
            if i > 0 {
                j <<= sorted[i] - sorted[i - 1];
            }
            let (mut m, mut l) = (0u64, j);
            for _ in 0..sorted[i] {
                m = (m << 1) | (l & 1);
                l >>= 1;
            }
            bits[order[i] as usize] = m;
            i += 1;
            j += 1;
        }
        bits
    }

    /// A flat (non-nested) tree header: every symbol one `width`-bit length (`len - 1`).
    fn put_flat_tree(w: &mut BitWriter, lengths: &[u8], width: u32) {
        w.put(0, 1); // no "absent" value
        w.put(width - 2, 2);
        w.put(0, 3); // offset 1
        w.put(0, 2); // flat
        for &l in lengths {
            w.put(l as u32 - 1, width);
        }
        w.align();
    }

    /// Encode `data` as literals plus one back-reference, then decode it back.
    #[test]
    fn round_trips_literals_and_a_match() {
        // A complete code, as real streams carry: 236 symbols of 8 bits and 40 of 9.
        let litlen: Vec<u8> = (0..LITLEN_SYMBOLS)
            .map(|s| if s < 236 { 8 } else { 9 })
            .collect();
        let dist: Vec<u8> = vec![4; DIST_SYMBOLS];
        let (lc, dc) = (codes_for(&litlen), codes_for(&dist));
        let mut w = BitWriter {
            out: Vec::new(),
            acc: 0,
            n: 0,
        };
        put_flat_tree(&mut w, &litlen, 4);
        put_flat_tree(&mut w, &dist, 3);
        let text = b"NDIF chunk, ";
        for &b in text.iter() {
            w.put(lc[b as usize] as u32, 8);
        }
        // Length symbol 258 is 6 with no extra bits; distance symbol 3 is 8 plus 3 extra bits.
        w.put(lc[258] as u32, 9);
        w.put(dc[3] as u32, 4);
        w.put(4, 3);
        w.align();

        let mut expect = text.to_vec();
        for _ in 0..6 {
            expect.push(expect[expect.len() - 12]);
        }
        let mut chunk = (w.out.len() as u32 + 16).to_le_bytes().to_vec();
        chunk.extend_from_slice(&(expect.len() as u32).to_le_bytes());
        chunk.extend_from_slice(&(expect.len() as u32).to_le_bytes());
        chunk.extend_from_slice(&[0xff; 4]);
        chunk.extend_from_slice(&w.out);
        assert_eq!(decompress(&chunk, 1 << 20).unwrap(), expect);
        assert!(decompress(&chunk, 4).is_err(), "over the caller's cap");
        assert!(
            decompress(&chunk[..10], 1 << 20).is_err(),
            "truncated header"
        );
    }

    /// Garbage must error out rather than panic or spin.
    #[test]
    fn garbage_errors_cleanly() {
        let mut chunk = vec![
            0u8, 0, 0, 0, 0, 0x10, 0, 0, 0, 0x10, 0, 0, 0xff, 0xff, 0xff, 0xff,
        ];
        chunk.extend((0..200u32).map(|i| (i.wrapping_mul(37) ^ 0x5a) as u8));
        let _ = decompress(&chunk, 1 << 20);
        let _ = decompress(&[0u8; 16], 1 << 20);
    }
}
