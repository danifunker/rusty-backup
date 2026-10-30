//! Big-endian (MSB-first) allocation-bitmap helpers, as used by HFS, HFS+ and ProDOS.

/// Test whether the bit at `index` is set in a big-endian bitmap.
#[inline]
#[allow(dead_code)]
pub fn bitmap_test_bit_be(data: &[u8], index: u32) -> bool {
    let byte_idx = index as usize / 8;
    let bit_idx = 7 - (index % 8);
    byte_idx < data.len() && (data[byte_idx] >> bit_idx) & 1 == 1
}

/// Set the bit at `index` in a big-endian bitmap.
#[inline]
pub fn bitmap_set_bit_be(data: &mut [u8], index: u32) {
    let byte_idx = index as usize / 8;
    let bit_idx = 7 - (index % 8);
    data[byte_idx] |= 1u8 << bit_idx;
}

/// Clear the bit at `index` in a big-endian bitmap.
#[inline]
pub fn bitmap_clear_bit_be(data: &mut [u8], index: u32) {
    let byte_idx = index as usize / 8;
    let bit_idx = 7 - (index % 8);
    data[byte_idx] &= !(1u8 << bit_idx);
}

#[allow(dead_code)]
/// Find the first set (1) bit in a big-endian bitmap within `max_bits`.
/// ProDOS uses bit=1 for FREE blocks, so this finds the first free block.
pub fn bitmap_find_set_bit_be(data: &[u8], max_bits: u32) -> Option<u32> {
    (0..max_bits).find(|&bit| bitmap_test_bit_be(data, bit))
}

#[allow(dead_code)]
/// Find the first clear (0) bit in a big-endian bitmap within `max_bits`.
pub fn bitmap_find_clear_bit_be(data: &[u8], max_bits: u32) -> Option<u32> {
    (0..max_bits).find(|&bit| !bitmap_test_bit_be(data, bit))
}

/// Collect every contiguous run of clear bits in a big-endian bitmap as
/// `(start, length)` pairs. Used by the HFS+ multi-extent allocator
/// (largest-run-first) — see `allocate_extents` in `src/fs/hfsplus.rs`.
pub fn bitmap_collect_clear_runs_be(data: &[u8], max_bits: u32) -> Vec<(u32, u32)> {
    let mut runs = Vec::new();
    let mut run_start = 0u32;
    let mut run_len = 0u32;
    for bit in 0..max_bits {
        if bitmap_test_bit_be(data, bit) {
            if run_len > 0 {
                runs.push((run_start, run_len));
                run_len = 0;
            }
            run_start = bit + 1;
        } else {
            if run_len == 0 {
                run_start = bit;
            }
            run_len += 1;
        }
    }
    if run_len > 0 {
        runs.push((run_start, run_len));
    }
    runs
}

/// Find a contiguous run of `count` clear bits in a big-endian bitmap.
/// Returns the starting bit index, or None if no such run exists.
pub fn bitmap_find_clear_run_be(data: &[u8], max_bits: u32, count: u32) -> Option<u32> {
    if count == 0 {
        return Some(0);
    }
    let mut run_start = 0u32;
    let mut run_len = 0u32;
    for bit in 0..max_bits {
        if bitmap_test_bit_be(data, bit) {
            run_start = bit + 1;
            run_len = 0;
        } else {
            run_len += 1;
            if run_len >= count {
                return Some(run_start);
            }
        }
    }
    None
}
