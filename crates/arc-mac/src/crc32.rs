// SPDX-License-Identifier: MIT
//! The reflected CRC-32 (zlib polynomial `0xedb88320`) that Compact Pro and StuffIt Arsenic
//! checksum with. Callers pick the seed and any final XOR (ARC-013).

const fn build_table() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut n = 0usize;
    while n < 256 {
        let mut c = n as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xedb8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        t[n] = c;
        n += 1;
    }
    t
}

static TABLE: [u32; 256] = build_table();

/// Fold `data` into the running CRC `crc` (no seed or final XOR applied).
pub(crate) fn crc32_update(mut crc: u32, data: &[u8]) -> u32 {
    for &b in data {
        crc = TABLE[((crc ^ b as u32) & 0xff) as usize] ^ (crc >> 8);
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_standard_check_value() {
        assert_eq!(!crc32_update(!0, b"123456789"), 0xCBF4_3926);
    }
}
