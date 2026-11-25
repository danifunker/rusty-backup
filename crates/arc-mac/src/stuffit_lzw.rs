// SPDX-License-Identifier: LGPL-2.1-or-later AND MIT
// The LZW decoder is derived from XADMaster (LGPL); the compress(1) encoder is original (MIT).
//! StuffIt compression **method 2** ("Compress"): the classic Unix
//! `compress(1)` LZW with block mode (code 256 = table clear). StuffIt invokes
//! it with flags `0x8e` → block mode, 14-bit maximum code width.
//!
//! Ported from XADMaster `XADCompressHandle.m` + `LZW.c`. Codes are read
//! LSB-first; the width grows from 9 bits as the dictionary fills, and after a
//! clear the encoder pads to an 8-code boundary (the well-known `compress`
//! quirk).

use anyhow::{bail, Result};
#[cfg(feature = "rust173-polyfill")]
use disk_core::rust173::IntIsMultipleOf as _;

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
        // Padding past the end just ends the stream; remaining() must not underflow.
        self.cursor = (self.cursor + n).min(self.data.len() * 8);
    }
}

/// Where the LZW decoder takes its LSB-first bits from: a slice, or a stream.
trait Bits {
    /// Whether `n` more bits are available.
    fn has(&mut self, n: u32) -> bool;
    fn take(&mut self, n: u32) -> u32;
    /// Skip `n` bits; past the end just ends the stream.
    fn pass(&mut self, n: usize);
}

impl Bits for BitCursor<'_> {
    fn has(&mut self, n: u32) -> bool {
        self.remaining() >= n as usize
    }
    fn take(&mut self, n: u32) -> u32 {
        self.next_bits(n)
    }
    fn pass(&mut self, n: usize) {
        self.skip(n)
    }
}

/// LSB-first bits read from a stream, buffered a chunk at a time.
struct StreamBits<'r> {
    r: &'r mut dyn std::io::Read,
    buf: Vec<u8>,
    bit: usize,
    eof: bool,
    err: Option<std::io::Error>,
}

impl<'r> StreamBits<'r> {
    fn new(r: &'r mut dyn std::io::Read) -> Self {
        StreamBits {
            r,
            buf: Vec::new(),
            bit: 0,
            eof: false,
            err: None,
        }
    }

    /// Buffer until `need` bits are available or the stream ends.
    fn fill(&mut self, need: usize) {
        while self.buf.len() * 8 - self.bit < need && !self.eof {
            let done = self.bit / 8;
            self.buf.drain(..done);
            self.bit -= done * 8;
            let at = self.buf.len();
            self.buf.resize(at + (64 << 10), 0);
            match self.r.read(&mut self.buf[at..]) {
                Ok(0) => {
                    self.buf.truncate(at);
                    self.eof = true;
                }
                Ok(k) => self.buf.truncate(at + k),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => self.buf.truncate(at),
                Err(e) => {
                    self.buf.truncate(at);
                    self.err = Some(e);
                    self.eof = true;
                }
            }
        }
    }
}

impl Bits for StreamBits<'_> {
    fn has(&mut self, n: u32) -> bool {
        self.fill(n as usize);
        self.buf.len() * 8 - self.bit >= n as usize
    }
    fn take(&mut self, n: u32) -> u32 {
        let mut v = 0u32;
        for i in 0..n {
            let bit = (self.buf[self.bit / 8] >> (self.bit % 8)) & 1;
            v |= (bit as u32) << i;
            self.bit += 1;
        }
        v
    }
    fn pass(&mut self, n: usize) {
        self.fill(n);
        self.bit = (self.bit + n).min(self.buf.len() * 8);
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
    // `want` can be unbounded (Unix .Z has no length), so cap the up-front reservation.
    let mut out = Vec::with_capacity(want.min(comp.len().saturating_mul(4)));
    let mut sink = |b: &[u8]| -> Result<()> {
        out.extend_from_slice(b);
        Ok(())
    };
    decode_lzw(&mut BitCursor::new(comp), want as u64, flags, &mut sink)?;
    Ok(out)
}

/// The LZW decode loop, handing output to `sink` a chunk at a time; returns the bytes produced.
fn decode_lzw(
    br: &mut dyn Bits,
    want: u64,
    flags: u8,
    sink: &mut dyn FnMut(&[u8]) -> Result<()>,
) -> Result<u64> {
    let blockmode = flags & 0x80 != 0;
    // compress(1) codes are 9 to 16 bits; a wider flag would size the tables at gigabytes.
    let bits = flags & 0x1f;
    if !(9..=16).contains(&bits) {
        bail!("LZW: max code width {bits} in flags {flags:#x} (9 to 16 allowed)");
    }
    let maxsymbols = 1i32 << bits;
    let reserved = if blockmode { 1 } else { 0 };
    let mut lzw = Lzw::new(maxsymbols, reserved);

    let mut out = Vec::new();
    let mut total: u64 = 0;
    let mut symbolcounter: usize = 0;

    'decode: while total < want {
        // Read the next non-clear code.
        let symbol = loop {
            if !br.has(lzw.symbolsize) {
                break 'decode; // ran out of input
            }
            let s = br.take(lzw.symbolsize) as i32;
            symbolcounter += 1;
            if s == CLEAR && blockmode {
                // Pad to an 8-code group boundary, then clear the table.
                let symsize = lzw.symbolsize as usize;
                if !symbolcounter.is_multiple_of(8) {
                    br.pass(symsize * (8 - symbolcounter % 8));
                }
                lzw.clear();
                symbolcounter = 0;
            } else {
                break s;
            }
        };

        let before = out.len();
        lzw.next_symbol(symbol, &mut out)?;
        total += (out.len() - before) as u64;
        if total > MAX_LZW_OUT {
            bail!("LZW: output passes {MAX_LZW_OUT} bytes (a decompression bomb?)");
        }
        if out.len() >= 64 << 10 {
            sink(&out)?;
            out.clear();
        }
    }
    sink(&out)?;
    Ok(total)
}

/// Decompress a Unix `compress(1)` `.Z` stream from `r` into `w` without holding either whole;
/// returns the bytes written.
pub fn decompress_unix_z_to(r: &mut dyn std::io::Read, w: &mut dyn std::io::Write) -> Result<u64> {
    let mut head = [0u8; 3];
    if r.read_exact(&mut head).is_err() || head[0..2] != [0x1F, 0x9D] {
        bail!("not a compress(1) .Z stream");
    }
    let mut br = StreamBits::new(r);
    let mut sink = |b: &[u8]| -> Result<()> { Ok(w.write_all(b)?) };
    let n = decode_lzw(&mut br, u64::MAX, head[2], &mut sink)?;
    if let Some(e) = br.err.take() {
        return Err(e.into());
    }
    Ok(n)
}

/// Largest stream this in-memory decoder will produce; a .Z has no stored length.
const MAX_LZW_OUT: u64 = 1 << 32;

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
    let mut out = Vec::new();
    compress_unix_z_to(&mut &data[..], &mut out).expect("writing to a Vec cannot fail");
    out
}

/// [`compress_unix_z`] from a stream to a stream, holding only the dictionary; returns the
/// bytes written.
pub fn compress_unix_z_to(
    r: &mut dyn std::io::Read,
    w: &mut dyn std::io::Write,
) -> std::io::Result<u64> {
    use std::io::Read as _;
    const MAXBITS: u32 = 16;
    const MAXMAXCODE: u32 = 1 << MAXBITS;
    const CLEAR: u32 = 256;
    const FIRST: u32 = 257;

    struct Out<'w> {
        w: &'w mut dyn std::io::Write,
        written: u64,
        buf: [u8; 16],
        offset: u32,
        n_bits: u32,
        maxcode: u32,
    }
    impl Out<'_> {
        fn put(&mut self, bytes: &[u8]) -> std::io::Result<()> {
            self.written += bytes.len() as u64;
            self.w.write_all(bytes)
        }

        /// `compress.c`'s `output()`: pack a code LSB-first; flush a group of eight codes, and
        /// flush the (zero-padded) group whenever the width is about to change.
        fn code(&mut self, code: u32, free_ent: u32, clear: &mut bool) -> std::io::Result<()> {
            for i in 0..self.n_bits {
                if code >> i & 1 != 0 {
                    let bit = self.offset + i;
                    self.buf[(bit / 8) as usize] |= 1 << (bit % 8);
                }
            }
            self.offset += self.n_bits;
            if self.offset == self.n_bits * 8 {
                let group = self.buf;
                self.put(&group[..self.n_bits as usize])?;
                self.buf = [0; 16];
                self.offset = 0;
            }
            if free_ent > self.maxcode || *clear {
                if self.offset > 0 {
                    let group = self.buf;
                    self.put(&group[..self.n_bits as usize])?;
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
            Ok(())
        }

        fn finish(&mut self) -> std::io::Result<()> {
            let n = self.offset.div_ceil(8) as usize;
            let group = self.buf;
            self.put(&group[..n])
        }
    }

    let mut out = Out {
        w,
        written: 0,
        buf: [0; 16],
        offset: 0,
        n_bits: 9,
        maxcode: (1 << 9) - 1,
    };
    out.put(&[0x1F, 0x9D, 0x80 | MAXBITS as u8])?;
    let mut input = std::io::BufReader::with_capacity(64 << 10, r).bytes();
    let first = match input.next() {
        Some(b) => b?,
        None => return Ok(out.written),
    };
    let mut table: std::collections::HashMap<(u32, u8), u32> = std::collections::HashMap::new();
    let mut free_ent = FIRST;
    let mut clear = false;
    let mut ent = u32::from(first);
    for c in input {
        let c = c?;
        if let Some(&code) = table.get(&(ent, c)) {
            ent = code;
            continue;
        }
        out.code(ent, free_ent, &mut clear)?;
        if free_ent < MAXMAXCODE {
            table.insert((ent, c), free_ent);
            free_ent += 1;
        } else {
            // A full table: block mode lets the stream start a fresh dictionary.
            table.clear();
            free_ent = FIRST;
            clear = true;
            out.code(CLEAR, free_ent, &mut clear)?;
        }
        ent = u32::from(c);
    }
    out.code(ent, free_ent, &mut clear)?;
    out.finish()?;
    Ok(out.written)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_code_width_past_16_bits_is_refused() {
        assert!(decompress(&[0u8; 16], 100, 0x9f).is_err());
    }

    #[test]
    fn padding_past_the_end_after_a_clear_ends_the_stream() {
        // A CLEAR (256) as the first 9-bit code, then too little input for the pad.
        let out = decompress(&[0x00, 0x01], 100, 0x90).unwrap();
        assert!(out.is_empty());
    }

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

    /// A reader that hands out at most 7 bytes per call, to exercise buffer refills.
    struct Trickle<'a>(&'a [u8]);
    impl std::io::Read for Trickle<'_> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = buf.len().min(7).min(self.0.len());
            buf[..n].copy_from_slice(&self.0[..n]);
            self.0 = &self.0[n..];
            Ok(n)
        }
    }

    #[test]
    fn streamed_codec_matches_the_in_memory_one() {
        // Past 64 KiB of output and past a full 16-bit table, so both chunking and clears run.
        let data: Vec<u8> = (0..600_000u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8 % 37)
            .collect();
        let packed = compress_unix_z(&data);
        let mut streamed = Vec::new();
        let n = compress_unix_z_to(&mut Trickle(&data), &mut streamed).unwrap();
        assert_eq!(n, packed.len() as u64);
        assert!(streamed == packed, "streamed encoder output differs");
        let mut back = Vec::new();
        let m = decompress_unix_z_to(&mut Trickle(&packed), &mut back).unwrap();
        assert_eq!(m, data.len() as u64);
        assert!(back == data, "streamed decode differs");
        assert!(decompress_unix_z(&packed).unwrap() == data);
        let mut nothing = Vec::new();
        assert!(decompress_unix_z_to(&mut &b"no"[..], &mut nothing).is_err());
    }

    /// Against the host's `compress(1)` when it has one; skipped otherwise.
    #[test]
    fn system_compress_agrees_with_the_streamed_codec() {
        use std::io::Write as _;
        use std::process::{Command, Stdio};
        let data: Vec<u8> = (0..300_000u32)
            .flat_map(|i| format!("line {} of the test {}\n", i % 977, i % 13).into_bytes())
            .collect();
        let run = |cmd: &str, args: &[&str], input: &[u8]| -> Option<Vec<u8>> {
            let mut child = Command::new(cmd)
                .args(args)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .ok()?;
            let mut stdin = child.stdin.take()?;
            let input = input.to_vec();
            let feeder = std::thread::spawn(move || stdin.write_all(&input));
            let out = child.wait_with_output().ok()?;
            feeder.join().ok()?.ok()?;
            out.status.success().then_some(out.stdout)
        };
        let Some(theirs) = run("compress", &["-c"], &data) else {
            return;
        };
        let mut back = Vec::new();
        decompress_unix_z_to(&mut &theirs[..], &mut back).unwrap();
        assert!(back == data, "our decoder misreads compress(1) output");
        let mut ours = Vec::new();
        compress_unix_z_to(&mut &data[..], &mut ours).unwrap();
        let unpacked = run("uncompress", &["-c"], &ours).expect("uncompress runs");
        assert!(unpacked == data, "uncompress misreads our output");
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
