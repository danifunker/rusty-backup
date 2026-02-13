//! Bitmap utilities for Unix-like filesystems.
//!
//! Provides efficient bit-level operations on block and inode bitmaps.
//! Uses little-endian bit order within each byte (bit 0 = LSB), matching
//! the standard Linux bitmap convention used by ext2/3/4, xfs AG bitmaps,
//! UFS cylinder group bitmaps, and Minix zone maps.

/// A read-only view over a bitmap stored as a byte slice.
///
/// `bit_count` specifies the number of valid bits, which may be less than
/// `data.len() * 8` (e.g. when the last byte is only partially used).
pub struct BitmapReader<'a> {
    data: &'a [u8],
    bit_count: u64,
}

impl<'a> BitmapReader<'a> {
    /// Create a new bitmap reader.
    ///
    /// `bit_count` is the number of valid bits. Bits beyond `bit_count`
    /// (in the last partial byte) are ignored by counting and iteration methods.
    pub fn new(data: &'a [u8], bit_count: u64) -> Self {
        Self { data, bit_count }
    }

    /// Returns the total number of valid bits in this bitmap.
    pub fn bit_count(&self) -> u64 {
        self.bit_count
    }

    /// Test whether the bit at `index` is set (1).
    ///
    /// Returns `false` if `index` is out of range.
    #[inline]
    pub fn is_bit_set(&self, index: u64) -> bool {
        if index >= self.bit_count {
            return false;
        }
        let byte_idx = (index / 8) as usize;
        let bit_idx = (index % 8) as u32;
        if byte_idx >= self.data.len() {
            return false;
        }
        self.data[byte_idx] & (1u8 << bit_idx) != 0
    }

    /// Count the number of set (1) bits in the valid range.
    pub fn count_set_bits(&self) -> u64 {
        if self.bit_count == 0 {
            return 0;
        }

        let full_bytes = (self.bit_count / 8) as usize;
        let remaining_bits = (self.bit_count % 8) as u32;

        let mut count: u64 = 0;

        // Count full bytes using popcount
        for &byte in &self.data[..full_bytes] {
            count += byte.count_ones() as u64;
        }

        // Handle the partial last byte
        if remaining_bits > 0 && full_bytes < self.data.len() {
            let mask = (1u8 << remaining_bits) - 1;
            count += (self.data[full_bytes] & mask).count_ones() as u64;
        }

        count
    }

    /// Count the number of clear (0) bits in the valid range.
    pub fn count_clear_bits(&self) -> u64 {
        self.bit_count - self.count_set_bits()
    }

    /// Find the index of the highest set bit, scanning from the end.
    ///
    /// Returns `None` if no bits are set.
    pub fn highest_set_bit(&self) -> Option<u64> {
        if self.bit_count == 0 {
            return None;
        }

        let full_bytes = (self.bit_count / 8) as usize;
        let remaining_bits = (self.bit_count % 8) as u32;

        // Check partial last byte first
        if remaining_bits > 0 && full_bytes < self.data.len() {
            let mask = (1u8 << remaining_bits) - 1;
            let masked = self.data[full_bytes] & mask;
            if masked != 0 {
                let top_bit = 7 - masked.leading_zeros();
                return Some(full_bytes as u64 * 8 + top_bit as u64);
            }
        }

        // Scan full bytes from end to start
        for i in (0..full_bytes).rev() {
            if self.data[i] != 0 {
                let top_bit = 7 - self.data[i].leading_zeros();
                return Some(i as u64 * 8 + top_bit as u64);
            }
        }

        None
    }

    /// Find the index of the lowest clear (0) bit, scanning from the start.
    ///
    /// Returns `None` if all bits are set.
    pub fn lowest_clear_bit(&self) -> Option<u64> {
        if self.bit_count == 0 {
            return None;
        }

        let full_bytes = (self.bit_count / 8) as usize;
        let remaining_bits = (self.bit_count % 8) as u32;

        // Scan full bytes
        for i in 0..full_bytes {
            if self.data[i] != 0xFF {
                let bit = self.data[i].trailing_ones();
                return Some(i as u64 * 8 + bit as u64);
            }
        }

        // Check partial last byte
        if remaining_bits > 0 && full_bytes < self.data.len() {
            let mask = (1u8 << remaining_bits) - 1;
            let masked = self.data[full_bytes] & mask;
            if masked != mask {
                // There's a clear bit in the valid range
                let bit = masked.trailing_ones();
                if (bit as u32) < remaining_bits {
                    return Some(full_bytes as u64 * 8 + bit as u64);
                }
            }
        }

        None
    }

    /// Iterate over the indices of all set (1) bits in ascending order.
    pub fn iter_set_bits(&self) -> SetBitsIter<'_> {
        SetBitsIter {
            bitmap: self,
            index: 0,
        }
    }

    /// Iterate over the indices of all clear (0) bits in ascending order.
    pub fn iter_clear_bits(&self) -> ClearBitsIter<'_> {
        ClearBitsIter {
            bitmap: self,
            index: 0,
        }
    }
}

/// Iterator over set bit indices in a `BitmapReader`.
pub struct SetBitsIter<'a> {
    bitmap: &'a BitmapReader<'a>,
    index: u64,
}

impl Iterator for SetBitsIter<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        while self.index < self.bitmap.bit_count {
            let i = self.index;
            self.index += 1;
            if self.bitmap.is_bit_set(i) {
                return Some(i);
            }
        }
        None
    }
}

/// Iterator over clear bit indices in a `BitmapReader`.
pub struct ClearBitsIter<'a> {
    bitmap: &'a BitmapReader<'a>,
    index: u64,
}

impl Iterator for ClearBitsIter<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        while self.index < self.bitmap.bit_count {
            let i = self.index;
            self.index += 1;
            if !self.bitmap.is_bit_set(i) {
                return Some(i);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_byte_set_bits() {
        let data = [0b10100101u8]; // bits 0,2,5,7 set
        let bm = BitmapReader::new(&data, 8);
        assert!(bm.is_bit_set(0));
        assert!(!bm.is_bit_set(1));
        assert!(bm.is_bit_set(2));
        assert!(!bm.is_bit_set(3));
        assert!(!bm.is_bit_set(4));
        assert!(bm.is_bit_set(5));
        assert!(!bm.is_bit_set(6));
        assert!(bm.is_bit_set(7));
        assert_eq!(bm.count_set_bits(), 4);
        assert_eq!(bm.count_clear_bits(), 4);
        assert_eq!(bm.highest_set_bit(), Some(7));
    }

    #[test]
    fn test_partial_byte() {
        let data = [0xFF];
        let bm = BitmapReader::new(&data, 5); // only 5 bits valid
        assert_eq!(bm.count_set_bits(), 5); // not 8
        assert_eq!(bm.count_clear_bits(), 0);
        assert_eq!(bm.highest_set_bit(), Some(4));
    }

    #[test]
    fn test_partial_byte_with_clear_bits() {
        // 0b11110011 = bits 0,1,4,5,6,7 set; bits 2,3 clear
        let data = [0b11110011u8];
        let bm = BitmapReader::new(&data, 6); // only bits 0-5 valid
                                              // Valid bits: 0=1, 1=1, 2=0, 3=0, 4=1, 5=1 → 4 set
        assert_eq!(bm.count_set_bits(), 4);
        assert_eq!(bm.count_clear_bits(), 2);
        assert_eq!(bm.highest_set_bit(), Some(5));
    }

    #[test]
    fn test_iter_set_bits() {
        let data = [0b00000101u8]; // bits 0, 2
        let bm = BitmapReader::new(&data, 8);
        let bits: Vec<u64> = bm.iter_set_bits().collect();
        assert_eq!(bits, vec![0, 2]);
    }

    #[test]
    fn test_iter_clear_bits() {
        let data = [0b11111010u8]; // bits 1,3,4,5,6,7 set; bits 0,2 clear
        let bm = BitmapReader::new(&data, 8);
        let bits: Vec<u64> = bm.iter_clear_bits().collect();
        assert_eq!(bits, vec![0, 2]);
    }

    #[test]
    fn test_iter_set_bits_partial() {
        let data = [0b11111111u8];
        let bm = BitmapReader::new(&data, 3); // only bits 0,1,2 valid
        let bits: Vec<u64> = bm.iter_set_bits().collect();
        assert_eq!(bits, vec![0, 1, 2]);
    }

    #[test]
    fn test_multi_byte() {
        let data = [0xFF, 0x00, 0b00000011]; // byte 0: all set, byte 1: none, byte 2: bits 0,1
        let bm = BitmapReader::new(&data, 24);
        assert_eq!(bm.count_set_bits(), 10); // 8 + 0 + 2
        assert_eq!(bm.count_clear_bits(), 14);
        assert_eq!(bm.highest_set_bit(), Some(17)); // byte 2, bit 1 → index 16+1=17
    }

    #[test]
    fn test_multi_byte_partial() {
        let data = [0xFF, 0xFF, 0b00001111];
        let bm = BitmapReader::new(&data, 20); // 16 full + 4 partial
        assert_eq!(bm.count_set_bits(), 20); // 8+8+4
        assert_eq!(bm.highest_set_bit(), Some(19));
    }

    #[test]
    fn test_highest_set_bit_middle() {
        let data = [0x00, 0b10000000, 0x00]; // only bit 15 set (byte 1, bit 7)
        let bm = BitmapReader::new(&data, 24);
        assert_eq!(bm.highest_set_bit(), Some(15));
    }

    #[test]
    fn test_highest_set_bit_none() {
        let data = [0x00, 0x00];
        let bm = BitmapReader::new(&data, 16);
        assert_eq!(bm.highest_set_bit(), None);
    }

    #[test]
    fn test_lowest_clear_bit() {
        let data = [0xFF, 0b11111110]; // byte 0: all set, byte 1: bit 0 clear
        let bm = BitmapReader::new(&data, 16);
        assert_eq!(bm.lowest_clear_bit(), Some(8)); // first clear bit is at index 8
    }

    #[test]
    fn test_lowest_clear_bit_first() {
        let data = [0b11111110]; // bit 0 clear
        let bm = BitmapReader::new(&data, 8);
        assert_eq!(bm.lowest_clear_bit(), Some(0));
    }

    #[test]
    fn test_lowest_clear_bit_none() {
        let data = [0xFF, 0xFF];
        let bm = BitmapReader::new(&data, 16);
        assert_eq!(bm.lowest_clear_bit(), None);
    }

    #[test]
    fn test_lowest_clear_bit_partial() {
        // All bits set in the full byte, partial byte has a clear bit
        let data = [0xFF, 0b11111011]; // bit 2 of byte 1 is clear
        let bm = BitmapReader::new(&data, 12); // 8 full + 4 partial
                                               // Valid bits in byte 1: 0=1, 1=1, 2=0, 3=1 → bit 10 is clear
        assert_eq!(bm.lowest_clear_bit(), Some(10));
    }

    #[test]
    fn test_empty_bitmap() {
        let data = [];
        let bm = BitmapReader::new(&data, 0);
        assert_eq!(bm.count_set_bits(), 0);
        assert_eq!(bm.count_clear_bits(), 0);
        assert_eq!(bm.highest_set_bit(), None);
        assert_eq!(bm.lowest_clear_bit(), None);
        assert!(!bm.is_bit_set(0));
        assert_eq!(bm.iter_set_bits().count(), 0);
        assert_eq!(bm.iter_clear_bits().count(), 0);
    }

    #[test]
    fn test_out_of_range() {
        let data = [0xFF];
        let bm = BitmapReader::new(&data, 8);
        assert!(!bm.is_bit_set(8));
        assert!(!bm.is_bit_set(100));
    }

    #[test]
    fn test_bit_count() {
        let data = [0xFF; 4];
        let bm = BitmapReader::new(&data, 30);
        assert_eq!(bm.bit_count(), 30);
    }

    #[test]
    fn test_large_bitmap() {
        // Simulate a 1024-block bitmap (128 bytes), all allocated
        let data = vec![0xFF; 128];
        let bm = BitmapReader::new(&data, 1024);
        assert_eq!(bm.count_set_bits(), 1024);
        assert_eq!(bm.count_clear_bits(), 0);
        assert_eq!(bm.highest_set_bit(), Some(1023));
        assert_eq!(bm.lowest_clear_bit(), None);
    }

    #[test]
    fn test_large_bitmap_sparse() {
        // 1024-bit bitmap with only every 8th bit set (bit 0 of each byte)
        let data = vec![0x01; 128];
        let bm = BitmapReader::new(&data, 1024);
        assert_eq!(bm.count_set_bits(), 128);
        assert_eq!(bm.count_clear_bits(), 896);
        assert_eq!(bm.highest_set_bit(), Some(1016)); // byte 127, bit 0 → 127*8=1016
        assert_eq!(bm.lowest_clear_bit(), Some(1)); // bit 1 of byte 0
    }

    #[test]
    fn test_iter_set_bits_multi_byte() {
        let data = [0b00000001, 0b10000000]; // bit 0 and bit 15
        let bm = BitmapReader::new(&data, 16);
        let bits: Vec<u64> = bm.iter_set_bits().collect();
        assert_eq!(bits, vec![0, 15]);
    }

    #[test]
    fn test_iter_clear_bits_all_set() {
        let data = [0xFF];
        let bm = BitmapReader::new(&data, 8);
        let bits: Vec<u64> = bm.iter_clear_bits().collect();
        assert!(bits.is_empty());
    }

    #[test]
    fn test_iter_set_bits_all_clear() {
        let data = [0x00];
        let bm = BitmapReader::new(&data, 8);
        let bits: Vec<u64> = bm.iter_set_bits().collect();
        assert!(bits.is_empty());
    }
}
