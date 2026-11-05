//! XFS symlink target extraction. Symlinks come in two flavors:
//! - `di_format == Local` — target lives inline in the data fork.
//! - `di_format == Extents` — target lives in the first (and always only)
//!   data extent; XFS symlinks are bounded by `XFS_SYMLINK_MAXLEN` (1024).

use super::types::DiFormat;

/// Decode an inline (local-format) symlink target from the fork buffer.
/// `size` is `di_size`. Trailing NULs are trimmed for compatibility with the
/// way some IRIX userspace stores targets.
pub fn decode_local_target(fork: &[u8], size: u64) -> String {
    let take = (size as usize).min(fork.len());
    String::from_utf8_lossy(&fork[..take])
        .trim_end_matches('\0')
        .to_string()
}

/// Decode an extent-format symlink target from a pre-read block buffer. The
/// caller is expected to read the first extent (count blocks × blocksize) and
/// pass `di_size` so we know how many bytes are actual target data.
pub fn decode_extent_target(block: &[u8], size: u64) -> String {
    decode_local_target(block, size)
}

/// Convenience: which path will be used for this inode's symlink?
pub fn target_kind(format: DiFormat) -> Option<&'static str> {
    match format {
        DiFormat::Local => Some("local"),
        DiFormat::Extents => Some("extents"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_local_target_trims_nuls() {
        let fork = b"/usr/sbin/init\0\0\0\0\0";
        assert_eq!(decode_local_target(fork, 14), "/usr/sbin/init");
    }

    #[test]
    fn decodes_local_respects_di_size() {
        let fork = b"/etc/passwd\0junk-after-size";
        // di_size says 11 bytes — we must not return the junk.
        assert_eq!(decode_local_target(fork, 11), "/etc/passwd");
    }

    #[test]
    fn decodes_extent_target_same_path_as_local() {
        let block = b"../shared/lib\0\0\0";
        assert_eq!(decode_extent_target(block, 13), "../shared/lib");
    }
}
