//! All-zero checks and zero fills, shared by the container writers that skip or pad runs of
//! zeros (sparse VMDK / VHD, raw backups).

use std::io::Write;

/// Bytes written per `write_all` while zero-filling.
const ZERO_CHUNK: usize = 256 * 1024;

/// Whether every byte of `data` is zero.
pub fn is_all_zeros(data: &[u8]) -> bool {
    data.iter().all(|&b| b == 0)
}

/// Write `count` zero bytes to `writer`, a chunk at a time.
pub fn write_zeros(writer: &mut dyn Write, count: u64) -> std::io::Result<()> {
    let zeros = vec![0u8; count.min(ZERO_CHUNK as u64) as usize];
    let mut remaining = count;
    while remaining > 0 {
        // Compared as u64: a 32-bit `as usize` would truncate a 4 GiB run to 0 and loop forever.
        let n = remaining.min(zeros.len() as u64) as usize;
        writer.write_all(&zeros[..n])?;
        remaining -= n as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_exactly_count_zeros_across_chunks() {
        let mut out = Vec::new();
        write_zeros(&mut out, ZERO_CHUNK as u64 * 2 + 7).unwrap();
        assert_eq!(out.len(), ZERO_CHUNK * 2 + 7);
        assert!(is_all_zeros(&out));
        out.push(1);
        assert!(!is_all_zeros(&out));
        let mut none = Vec::new();
        write_zeros(&mut none, 0).unwrap();
        assert!(none.is_empty());
    }
}
