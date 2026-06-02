//! StuffIt compression **method 3** ("Huffman"): a plain byte-wise Huffman
//! code whose tree is serialized at the head of the stream. Ported from
//! XADMaster `XADStuffItHuffmanHandle.m`. Big-endian (MSB-first) bitstream.
//!
//! Rare (StuffIt 1.x era); no local sample to validate against, so this is a
//! faithful port. The per-fork CRC-16/ARC check makes a wrong decode fail
//! loudly rather than produce silent garbage.

use anyhow::{bail, Result};

use super::bitreader_be::BitReaderBE;

enum Node {
    Leaf(u8),
    Branch(Box<Node>, Box<Node>),
}

/// Parse the serialized tree: a `1` bit introduces a leaf (next 8 bits = byte),
/// a `0` bit introduces an internal node (zero-branch then one-branch).
fn parse_tree(br: &mut BitReaderBE, depth: u32) -> Result<Node> {
    if depth > 64 {
        bail!("StuffIt Huffman: tree too deep");
    }
    if br.next_bit() == 1 {
        Ok(Node::Leaf(br.next_bits(8) as u8))
    } else {
        let zero = parse_tree(br, depth + 1)?;
        let one = parse_tree(br, depth + 1)?;
        Ok(Node::Branch(Box::new(zero), Box::new(one)))
    }
}

/// Decompress a StuffIt method-3 (Huffman) stream to `want` bytes.
pub fn decompress(comp: &[u8], want: usize) -> Result<Vec<u8>> {
    if want == 0 {
        return Ok(Vec::new());
    }
    let mut br = BitReaderBE::new(comp);
    let root = parse_tree(&mut br, 0)?;

    let mut out = Vec::with_capacity(want);
    while out.len() < want {
        let mut node = &root;
        loop {
            match node {
                Node::Leaf(b) => {
                    out.push(*b);
                    break;
                }
                Node::Branch(zero, one) => {
                    // CSInputNextSymbolUsingCode walks branches[bit]: bit 0 -> left.
                    node = if br.next_bit() == 0 { zero } else { one };
                }
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn huffman_simple_tree() {
        // Build a stream by hand: tree for symbols 'A' (code 0) and 'B' (code 1).
        // Tree serialization (MSB-first): 0 (branch) 1 'A'(8b) 1 'B'(8b)
        // Then data: A B A A -> bits 0 1 0 0
        let mut bits: Vec<bool> = Vec::new();
        let put_byte = |bits: &mut Vec<bool>, v: u8| {
            for i in (0..8).rev() {
                bits.push((v >> i) & 1 == 1);
            }
        };
        bits.push(false); // branch
        bits.push(true); // leaf
        put_byte(&mut bits, b'A');
        bits.push(true); // leaf
        put_byte(&mut bits, b'B');
        // data: A(0) B(1) A(0) A(0)
        bits.push(false);
        bits.push(true);
        bits.push(false);
        bits.push(false);

        // pack MSB-first into bytes
        let mut bytes = Vec::new();
        let mut acc = 0u8;
        let mut n = 0;
        for b in bits {
            acc = (acc << 1) | (b as u8);
            n += 1;
            if n == 8 {
                bytes.push(acc);
                acc = 0;
                n = 0;
            }
        }
        if n > 0 {
            bytes.push(acc << (8 - n));
        }

        let out = decompress(&bytes, 4).unwrap();
        assert_eq!(&out, b"ABAA");
    }
}
