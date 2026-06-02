//! StuffIt compression **method 5** ("LZAH"): an LHArc-style LZSS over a 4 KiB
//! preset window, with an *adaptive* (FGK) Huffman code for literals/lengths
//! and a static prefix code for the high bits of the match offset. Ported from
//! XADMaster `XADLZHDynamicHandle.m` (+ `XADLZSSHandle.m`). Big-endian
//! bitstream.
//!
//! Rare (StuffIt 1.x era); no local sample to validate against, so this is a
//! faithful port. The per-fork CRC-16/ARC check makes a wrong decode fail
//! loudly rather than produce silent garbage.

use anyhow::{bail, Result};
use std::collections::HashMap;

use super::bitreader_be::BitReaderBE;

const NUM_LEAVES: usize = 314;
const NUM_NODES: usize = NUM_LEAVES * 2 - 1; // 627
const FREQ_LIMIT: i32 = 0x8000;
const WINDOW_SIZE: usize = 4096;
const WINDOW_MASK: usize = WINDOW_SIZE - 1;

/// Static lengths for the 64-symbol offset-high-bits prefix code.
const DISTANCE_LENGTHS: [i32; 64] = [
    3, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
];

/// Canonical prefix code (MSB-first codewords) decoded from a BE bitstream.
struct PrefixCode {
    map: HashMap<(u32, u32), i32>,
    maxlen: u32,
}

impl PrefixCode {
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

    fn decode(&self, br: &mut BitReaderBE) -> Result<i32> {
        let mut acc = 0u32;
        for len in 1..=self.maxlen {
            acc = (acc << 1) | br.next_bit();
            if let Some(&s) = self.map.get(&(len, acc)) {
                return Ok(s);
            }
        }
        bail!("LZAH: invalid distance code");
    }
}

/// One node of the adaptive Huffman tree. Links are storage indices (-1 = none).
#[derive(Clone, Copy)]
struct Node {
    parent: i32,
    left: i32,
    right: i32,
    index: i32, // position in `order`
    freq: i32,
    value: i32,
}

impl Default for Node {
    fn default() -> Self {
        Node {
            parent: -1,
            left: -1,
            right: -1,
            index: 0,
            freq: 0,
            value: 0,
        }
    }
}

struct AdaptiveTree {
    storage: Vec<Node>, // backing store (tree links reference these)
    order: Vec<usize>,  // order[pos] = storage index, kept sorted by freq
}

impl AdaptiveTree {
    fn new() -> AdaptiveTree {
        let mut t = AdaptiveTree {
            storage: vec![Node::default(); NUM_NODES],
            order: (0..NUM_NODES).collect(),
        };
        // Leaves occupy the high storage indices.
        for i in 0..NUM_LEAVES {
            let idx = NUM_NODES - 1 - i;
            t.storage[idx].index = idx as i32;
            t.storage[idx].freq = 1;
            t.storage[idx].value = i as i32;
        }
        // Internal nodes 0..=312.
        for i in (0..NUM_LEAVES - 1).rev() {
            t.storage[i].index = i as i32;
            let l = 2 * i + 1;
            let r = 2 * i + 2;
            t.storage[i].left = l as i32;
            t.storage[i].right = r as i32;
            t.storage[l].parent = i as i32;
            t.storage[r].parent = i as i32;
            t.storage[i].freq = t.storage[l].freq + t.storage[r].freq;
        }
        t
    }

    fn is_leaf(&self, s: usize) -> bool {
        self.storage[s].left < 0 && self.storage[s].right < 0
    }

    /// Decode the next value by walking from the root (storage[0]).
    fn next_value(&mut self, br: &mut BitReaderBE) -> Result<i32> {
        let mut node = 0usize; // root == nodestorage[0]
        while self.storage[node].left >= 0 || self.storage[node].right >= 0 {
            let bit = br.next_bit();
            // bit 1 -> left child, bit 0 -> right child (per XAD).
            let next = if bit == 1 {
                self.storage[node].left
            } else {
                self.storage[node].right
            };
            if next < 0 {
                bail!("LZAH: walked into empty branch");
            }
            node = next as usize;
        }
        let value = self.storage[node].value;
        self.update(node);
        Ok(value)
    }

    fn update(&mut self, mut node: usize) {
        if self.storage[0].freq == FREQ_LIMIT {
            self.reconstruct();
        }
        loop {
            self.storage[node].freq += 1;
            if self.storage[node].parent < 0 {
                break;
            }
            self.rearrange(node);
            node = self.storage[node].parent as usize;
        }
    }

    fn rearrange(&mut self, p: usize) {
        let p_index = self.storage[p].index;
        let mut q_index = p_index;
        while q_index > 0
            && self.storage[self.order[(q_index - 1) as usize]].freq < self.storage[p].freq
        {
            q_index -= 1;
        }
        if q_index < p_index {
            let q = self.order[q_index as usize];
            let new_q_parent = self.storage[p].parent;
            let new_p_parent = self.storage[q].parent;
            let p_is_right = self.storage[self.storage[p].parent as usize].right == p as i32;
            let q_is_right = self.storage[self.storage[q].parent as usize].right == q as i32;

            let pp = self.storage[p].parent as usize;
            if p_is_right {
                self.storage[pp].right = q as i32;
            } else {
                self.storage[pp].left = q as i32;
            }
            let qp = self.storage[q].parent as usize;
            if q_is_right {
                self.storage[qp].right = p as i32;
            } else {
                self.storage[qp].left = p as i32;
            }

            self.storage[p].parent = new_p_parent;
            self.storage[q].parent = new_q_parent;

            self.order[p_index as usize] = q;
            self.storage[q].index = p_index;
            self.order[q_index as usize] = p;
            self.storage[p].index = q_index;
        }
    }

    fn reconstruct(&mut self) {
        // Gather leaves in `order` order, halving their frequencies.
        let mut leafs: Vec<usize> = Vec::with_capacity(NUM_LEAVES);
        for i in 0..NUM_NODES {
            let s = self.order[i];
            if self.is_leaf(s) {
                self.storage[s].freq = (self.storage[s].freq + 1) / 2;
                leafs.push(s);
            }
        }

        let mut leaf_index: i32 = NUM_LEAVES as i32 - 1;
        let mut branch_index: i32 = NUM_LEAVES as i32 - 2;
        let mut node_index: i32 = NUM_NODES as i32 - 1;
        let mut pair_index: i32 = NUM_NODES as i32 - 2;

        while node_index >= 0 {
            while node_index >= pair_index {
                let leaf = leafs[leaf_index as usize];
                self.order[node_index as usize] = leaf;
                self.storage[leaf].index = node_index;
                node_index -= 1;
                leaf_index -= 1;
            }

            let branch = branch_index as usize;
            branch_index -= 1;
            let l = self.order[pair_index as usize];
            let r = self.order[(pair_index + 1) as usize];
            self.storage[branch].left = l as i32;
            self.storage[branch].right = r as i32;
            self.storage[l].parent = branch as i32;
            self.storage[r].parent = branch as i32;
            self.storage[branch].freq = self.storage[l].freq + self.storage[r].freq;

            while leaf_index >= 0
                && self.storage[leafs[leaf_index as usize]].freq <= self.storage[branch].freq
            {
                let leaf = leafs[leaf_index as usize];
                self.order[node_index as usize] = leaf;
                self.storage[leaf].index = node_index;
                node_index -= 1;
                leaf_index -= 1;
            }

            self.order[node_index as usize] = branch;
            self.storage[branch].index = node_index;
            node_index -= 1;
            pair_index -= 2;
        }
        self.storage[0].parent = -1;
    }
}

/// Build the LHA preset 4 KiB window.
fn preset_window() -> Vec<u8> {
    let mut w = vec![0u8; WINDOW_SIZE];
    for i in 0..256 {
        for j in 0..13 {
            w[i * 13 + 18 + j] = i as u8;
        }
    }
    let base = 256 * 13;
    for i in 0..256 {
        w[base + 18 + i] = i as u8;
    }
    for i in 0..256 {
        w[base + 256 + 18 + i] = (255 - i) as u8;
    }
    // [base+512+18 .. +128] already zero.
    for i in 0..(128 - 18) {
        w[base + 512 + 128 + 18 + i] = b' ';
    }
    w
}

/// Decompress a StuffIt method-5 (LZAH) stream to `want` bytes.
pub fn decompress(comp: &[u8], want: usize) -> Result<Vec<u8>> {
    if want == 0 {
        return Ok(Vec::new());
    }
    let mut br = BitReaderBE::new(comp);
    let distancecode = PrefixCode::from_lengths(&DISTANCE_LENGTHS);
    let mut tree = AdaptiveTree::new();
    let mut window = preset_window();

    let mut out = Vec::with_capacity(want);
    let mut pos: usize = 0;
    let mut match_len: usize = 0;
    let mut match_off: usize = 0;

    while out.len() < want {
        if match_len == 0 {
            let lit = tree.next_value(&mut br)?;
            if lit < 0x100 {
                let byte = lit as u8;
                window[pos & WINDOW_MASK] = byte;
                out.push(byte);
                pos += 1;
                continue;
            }
            let length = (lit - 0x100 + 3) as usize;
            let highbits = distancecode.decode(&mut br)? as usize;
            let lowbits = br.next_bits(6) as usize;
            let offset = (highbits << 6) + lowbits + 1;
            match_len = length;
            // pos - offset, wrapping into the preset window for early matches.
            match_off = pos.wrapping_sub(offset);
        }

        match_len -= 1;
        let byte = window[match_off & WINDOW_MASK];
        match_off = match_off.wrapping_add(1);
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
    fn tree_and_window_init() {
        // Sanity: tree builds, root has children, leaves carry values 0..313.
        let t = AdaptiveTree::new();
        assert!(t.storage[0].left >= 0 && t.storage[0].right >= 0);
        assert_eq!(t.storage[NUM_NODES - 1].value, 0);
        assert_eq!(
            t.storage[NUM_NODES - NUM_LEAVES].value,
            NUM_LEAVES as i32 - 1
        );
        let w = preset_window();
        assert_eq!(w.len(), WINDOW_SIZE);
        assert_eq!(w[18], 0); // first patterned byte
        assert_eq!(w[18 + 13], 1);
    }

    #[test]
    fn distance_code_builds() {
        let c = PrefixCode::from_lengths(&DISTANCE_LENGTHS);
        assert_eq!(c.maxlen, 8);
    }
}
