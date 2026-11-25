//! Ghost's Fast-LZ block decoder.

use super::*;

// ============================================================================
// 5.5c — Fast-LZ block decoder
// ============================================================================
//
// Norton Ghost's "Z1" Fast-LZ is a custom LZ77 variant ported from the
// MIT-licensed clean-room reference `github.com/nyarime/gho/fastlz.go`
// (itself reverse-engineered from Ghost 11.5.1 `sub_4DDD70` via IDA — no
// Symantec source consulted, see the provenance rule in
// `docs/virtualization-formats.md` §5).
//
// Each on-disk block carries a 4-byte prefix; if the first byte is 1 the
// remaining `comp_len - 4` bytes are stored verbatim, otherwise the payload
// is compressed.
//
// Compressed stream uses 16-bit control words: each bit selects literal (0)
// or 2-byte match token (1). A match token packs a 12-bit hash-table index
// (b1 | ((b0 & 0xF0) << 4)) and a 4-bit extra-length (b0 & 0x0F); copy
// length is `3 + extra_len` bytes from the previous output at the hashed
// position. Hash function:
//
//     h = ((-24993 * (b2 ^ (16 * (b1 ^ (16 * b0))))) >> 4) & 0xFFF
//
// Token loop runs 16 tokens per control word, except within 32 bytes of
// `src_end` where it falls back to 1 token at a time (per the reference).
// Hash-table entries start as a "sentinel" pointing at the 18-byte literal
// `"123456789012345678"` — matches resolved against the sentinel copy from
// that string (zero-padded past byte 17). Decode-to-temp wiring lands in 5.6.

/// Fast-LZ hash-table size (12-bit hash → 4096 entries).
pub const FAST_LZ_HASH_SIZE: usize = 4096;

/// Sentinel literal pre-populated into hash slots Ghost has never resolved
/// against an actual position. Reads past the sentinel return zero.
pub const FAST_LZ_SENTINEL: &[u8; 18] = b"123456789012345678";

/// Decoded block size Ghost picks for partition data (32 KiB). Caller
/// supplies the destination buffer; the decoder never allocates.
pub const FAST_LZ_BLOCK_SIZE: usize = 32 * 1024;

/// Compute the 12-bit Fast-LZ hash for 3 consecutive bytes.
///
/// `h = ((-24993 * (b2 ^ (16 * (b1 ^ (16 * b0))))) >> 4) & 0xFFF`.
/// Multiplication wraps modulo 2^32 (matches Ghost's i32×i32→i32 cast +
/// reinterpret-as-u32 + unsigned shift).
#[inline]
pub fn fast_lz_hash(b0: u8, b1: u8, b2: u8) -> usize {
    let v = (b2 as i32) ^ (16 * ((b1 as i32) ^ (16 * (b0 as i32))));
    let prod = (-24993_i32).wrapping_mul(v) as u32;
    ((prod >> 4) & 0xFFF) as usize
}

/// Decompress one Fast-LZ block into `dst`. Returns the number of bytes
/// written.
///
/// `data` must contain at least `comp_len` valid bytes (the on-disk block
/// payload, excluding the 2-byte `stored_len` prefix from the outer
/// container framing). `dst` must be pre-allocated to at least
/// `FAST_LZ_BLOCK_SIZE` bytes; the decoder zero-fills `dst` before
/// writing, so the sentinel-fallback paths in the reference reproduce
/// faithfully (Ghost's reference also relies on its destination buffer
/// being pre-zeroed by `make`).
pub fn fast_lz_decompress(data: &[u8], comp_len: usize, dst: &mut [u8]) -> Result<usize> {
    if comp_len == 0 || data.len() < comp_len {
        bail!(
            "fast-lz: truncated input (comp_len={}, data_len={})",
            comp_len,
            data.len()
        );
    }

    // Mirror the reference: zero-fill the destination so out-of-range
    // hash-table-driven reads return 0 rather than uninitialized memory.
    for b in dst.iter_mut() {
        *b = 0;
    }

    // Uncompressed block: prefix byte == 1 → straight copy of bytes [4..comp_len).
    if data[0] == 1 {
        if comp_len < 4 {
            bail!("fast-lz: uncompressed block too short ({} < 4)", comp_len);
        }
        let n = comp_len - 4;
        if n > dst.len() {
            bail!(
                "fast-lz: uncompressed payload {} exceeds dst {}",
                n,
                dst.len()
            );
        }
        dst[..n].copy_from_slice(&data[4..4 + n]);
        return Ok(n);
    }

    // Compressed path.
    let mut hash_table: [i32; FAST_LZ_HASH_SIZE] = [-1; FAST_LZ_HASH_SIZE];

    let mut src: usize = 4;
    let src_end: usize = comp_len;
    let mut out_pos: usize = 0;

    // Sentinel control value forces an initial control-word reload.
    let mut control: u32 = 1;
    let mut literal_run: u16 = 0;
    let mut prev_literal_run: u16 = 0;

    'outer: while src < src_end {
        if control == 1 {
            if src + 1 >= src_end {
                break;
            }
            control = (data[src] as u32) | ((data[src + 1] as u32) << 8) | 0x10000;
            src += 2;
        }

        // Near-end safeguard from the reference: within 32 bytes of the
        // end of the compressed payload we decode one token at a time
        // (avoids over-reading the 2-byte match token at a boundary).
        let near_end = src_end < src + 32;
        let token_count = if near_end { 1 } else { 16 };

        for _ in 0..token_count {
            if src >= src_end {
                break;
            }

            if control & 1 != 0 {
                // Match reference: 2-byte token.
                if src + 1 >= src_end {
                    break 'outer;
                }
                let b0 = data[src];
                let b1 = data[src + 1];

                let hash_idx = (b1 as usize) | (((b0 as usize) & 0xF0) << 4);
                let extra_len = (b0 & 0x0F) as usize;

                let match_pos = hash_table[hash_idx];
                let match_start = out_pos;
                let total_copy = 3 + extra_len;

                // j indexes three independent sources (sentinel slice,
                // dst at match_pos + j, fallback zero) chosen by branch —
                // .enumerate() over any single source would lose clarity.
                #[allow(clippy::needless_range_loop)]
                for j in 0..total_copy {
                    if out_pos >= dst.len() {
                        bail!("fast-lz: output overflow at match copy");
                    }
                    let byte = if match_pos < 0 {
                        // Sentinel slot: copy from the literal "123…",
                        // zero past byte 17 (Ghost reads from a global buffer
                        // initialised that way).
                        if j < FAST_LZ_SENTINEL.len() {
                            FAST_LZ_SENTINEL[j]
                        } else {
                            0
                        }
                    } else {
                        let src_idx = match_pos as usize + j;
                        if src_idx < dst.len() {
                            dst[src_idx]
                        } else {
                            0
                        }
                    };
                    dst[out_pos] = byte;
                    out_pos += 1;
                }

                src += 2;

                // Register hash entries for the literal run that ended just
                // before this match (matches the reference's 1- or 2-entry
                // backfill).
                if literal_run > 0 {
                    let pos_isz = match_start as isize - literal_run as isize;
                    if pos_isz >= 0 {
                        let pos = pos_isz as usize;
                        if pos + 2 < out_pos {
                            let h = fast_lz_hash(dst[pos], dst[pos + 1], dst[pos + 2]);
                            hash_table[h] = pos as i32;
                            if prev_literal_run == 2 && pos + 3 < out_pos {
                                let h2 = fast_lz_hash(dst[pos + 1], dst[pos + 2], dst[pos + 3]);
                                hash_table[h2] = (pos + 1) as i32;
                            }
                        }
                    }
                    literal_run = 0;
                    prev_literal_run = 0;
                }

                hash_table[hash_idx] = match_start as i32;
            } else {
                // Literal byte.
                if out_pos >= dst.len() {
                    bail!("fast-lz: output overflow at literal copy");
                }
                literal_run += 1;
                dst[out_pos] = data[src];
                out_pos += 1;
                src += 1;
                prev_literal_run = literal_run;

                if literal_run == 3 {
                    let pos = out_pos - 3;
                    let h = fast_lz_hash(dst[pos], dst[pos + 1], dst[pos + 2]);
                    hash_table[h] = pos as i32;
                    literal_run = 2;
                    prev_literal_run = 2;
                }
            }

            control >>= 1;
            if control == 1 {
                break;
            }
        }
    }

    Ok(out_pos)
}
