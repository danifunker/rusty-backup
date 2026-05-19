//! Shared argument parsers — size strings, Mac paths, and a small
//! zero-byte reader used by `put --zero N`.

use anyhow::{anyhow, bail, Result};
use std::io::Read;

/// Parse a human-friendly size string. Accepts plain bytes, optional
/// `B`/`K`/`KiB`/`M`/`MiB`/`G`/`GiB` suffixes.
pub fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        bail!("empty size");
    }
    let (num_part, mult): (&str, u64) =
        if let Some(rest) = s.strip_suffix("KiB").or_else(|| s.strip_suffix('K')) {
            (rest, 1024)
        } else if let Some(rest) = s.strip_suffix("MiB").or_else(|| s.strip_suffix('M')) {
            (rest, 1024 * 1024)
        } else if let Some(rest) = s.strip_suffix("GiB").or_else(|| s.strip_suffix('G')) {
            (rest, 1024 * 1024 * 1024)
        } else if let Some(rest) = s.strip_suffix('B') {
            (rest, 1)
        } else {
            (s, 1)
        };
    let n: u64 = num_part
        .trim()
        .parse()
        .map_err(|_| anyhow!("invalid size {s:?}"))?;
    n.checked_mul(mult)
        .ok_or_else(|| anyhow!("size {s:?} overflows u64"))
}

/// Pick the smallest 512-byte-multiple block size that keeps total_blocks
/// at or below 65535 (the HFS limit).
pub fn pick_block_size(volume_bytes: u64) -> u32 {
    let mut bs: u32 = 512;
    while volume_bytes / bs as u64 > 65535 {
        bs = bs.saturating_mul(2);
        if bs == 0 {
            return 1 << 16;
        }
    }
    bs
}

/// Split a `/`-delimited Mac path into `(parent_dir, basename)`. Trailing
/// slashes are stripped; the root `/` returns `("/", "")`.
pub fn split_mac_path(p: &str) -> Result<(String, String)> {
    let normalized = p.trim_end_matches('/');
    if normalized.is_empty() || normalized == "/" {
        return Ok(("/".into(), String::new()));
    }
    let (parent, name) = match normalized.rsplit_once('/') {
        Some((par, n)) => (par, n),
        None => ("", normalized),
    };
    let parent = if parent.is_empty() {
        "/".into()
    } else {
        parent.to_string()
    };
    Ok((parent, name.to_string()))
}

/// Reader that yields `remaining` zero bytes. Used by `put --zero N` to
/// pre-allocate a zero-filled file inside a filesystem.
pub struct ZeroReader {
    pub remaining: u64,
}

impl Read for ZeroReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        let n = buf.len().min(self.remaining as usize);
        for b in &mut buf[..n] {
            *b = 0;
        }
        self.remaining -= n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_size_bytes() {
        assert_eq!(parse_size("819200").unwrap(), 819200);
        assert_eq!(parse_size("800K").unwrap(), 819200);
        assert_eq!(parse_size("800KiB").unwrap(), 819200);
        assert_eq!(parse_size("5M").unwrap(), 5 * 1024 * 1024);
        assert_eq!(parse_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert!(parse_size("").is_err());
        assert!(parse_size("nope").is_err());
    }

    #[test]
    fn split_mac_path_cases() {
        assert_eq!(split_mac_path("/foo").unwrap(), ("/".into(), "foo".into()));
        assert_eq!(
            split_mac_path("/foo/bar.txt").unwrap(),
            ("/foo".into(), "bar.txt".into())
        );
        assert_eq!(split_mac_path("/").unwrap(), ("/".into(), String::new()));
    }

    #[test]
    fn pick_block_size_floppy_and_large() {
        assert_eq!(pick_block_size(819_200), 512);
        // 60 MiB / 512 = 122880 > 65535, so bs jumps to 1024.
        assert_eq!(pick_block_size(60 * 1024 * 1024), 1024);
    }
}
