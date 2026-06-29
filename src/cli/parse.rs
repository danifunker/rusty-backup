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

/// Split an in-image path into its decoded components.
///
/// Two grammars, picked by the caller via `colon`:
///
/// - **slash** (`colon == false`, every filesystem): `/`-delimited. A literal
///   `/` inside a component is written `\/`; a literal `\` is written `\\`.
///   Any other `\x` is preserved verbatim (the backslash is kept) so a stray
///   backslash in a real name survives a round trip.
/// - **colon** (`colon == true`, only filesystems where
///   [`crate::fs::filesystem::Filesystem::uses_colon_paths`] is true): `:`-delimited,
///   and `/` is an ordinary filename byte needing no escape. `:` can never
///   appear in an HFS/HFS+ name, so this grammar is unambiguous on those
///   volumes.
///
/// Leading / trailing separators are ignored and empty components dropped, so
/// `/a/b`, `a/b`, and `/a/b/` all yield `["a", "b"]`.
pub fn split_image_path(path: &str, colon: bool) -> Vec<String> {
    if colon {
        return path
            .split(':')
            .filter(|c| !c.is_empty())
            .map(|c| c.to_string())
            .collect();
    }
    let mut components = Vec::new();
    let mut cur = String::new();
    let mut escaped = false;
    for c in path.chars() {
        if escaped {
            match c {
                '/' | '\\' => cur.push(c),
                other => {
                    // Not an escape we recognise — keep the backslash literal.
                    cur.push('\\');
                    cur.push(other);
                }
            }
            escaped = false;
        } else if c == '\\' {
            escaped = true;
        } else if c == '/' {
            components.push(std::mem::take(&mut cur));
        } else {
            cur.push(c);
        }
    }
    if escaped {
        // Trailing lone backslash: keep it as data.
        cur.push('\\');
    }
    components.push(cur);
    components.into_iter().filter(|c| !c.is_empty()).collect()
}

/// Split an in-image path into `(parent_components, basename)`, decoding with
/// the same escape / colon rules as [`split_image_path`]. The root path yields
/// `([], "")`.
pub fn split_image_parent(path: &str, colon: bool) -> (Vec<String>, String) {
    let mut components = split_image_path(path, colon);
    match components.pop() {
        Some(name) => (components, name),
        None => (Vec::new(), String::new()),
    }
}

/// Encode a single path component for the slash grammar: a literal `\` becomes
/// `\\` and a literal `/` becomes `\/`. The inverse of one component of
/// [`split_image_path`] in slash mode. Callers that build a slash path from
/// known components (or external tooling mirroring this convention) use it so
/// the result round-trips back through [`split_image_path`].
pub fn escape_path_component(component: &str) -> String {
    let mut out = String::with_capacity(component.len());
    for c in component.chars() {
        if c == '\\' || c == '/' {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Split a `/`-delimited Mac path into `(parent_dir, basename)`. Trailing
/// slashes are stripped; the root `/` returns `("/", "")`.
///
/// This is the plain `/`-splitter still used on the remote (`rb://`) path,
/// where addressing is slash-only and the daemon does its own resolution.
/// Local verbs go through [`split_image_parent`] /
/// [`crate::cli::verbs::ls::resolve_parent`] instead, which understand the `\/`
/// escape and colon grammar.
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
    fn split_image_path_slash_basic() {
        assert_eq!(split_image_path("/a/b/c", false), vec!["a", "b", "c"]);
        assert_eq!(split_image_path("a/b/c", false), vec!["a", "b", "c"]);
        assert_eq!(split_image_path("/a/b/c/", false), vec!["a", "b", "c"]);
        assert!(split_image_path("/", false).is_empty());
        assert!(split_image_path("", false).is_empty());
    }

    #[test]
    fn split_image_path_escaped_slash_is_one_component() {
        // "Oxyd 3.6" / "Oxyd b/w" addressed with a \/ escape.
        assert_eq!(
            split_image_path(r"/Games/Oxyd 3.6/Oxyd b\/w", false),
            vec!["Games", "Oxyd 3.6", "Oxyd b/w"]
        );
        // A component that is literally just "/".
        assert_eq!(split_image_path(r"/a/\//b", false), vec!["a", "/", "b"]);
    }

    #[test]
    fn split_image_path_double_backslash_is_literal_backslash() {
        assert_eq!(
            split_image_path(r"/a/b\\c/d", false),
            vec!["a", r"b\c", "d"]
        );
        // An unrecognised escape keeps the backslash.
        assert_eq!(split_image_path(r"/a/b\xc", false), vec!["a", r"b\xc"]);
        // Trailing lone backslash survives as data.
        assert_eq!(split_image_path(r"/a/b\", false), vec!["a", r"b\"]);
    }

    #[test]
    fn split_image_path_colon_mode_slash_is_data() {
        assert_eq!(
            split_image_path(":Games:Oxyd 3.6:Oxyd b/w", true),
            vec!["Games", "Oxyd 3.6", "Oxyd b/w"]
        );
        // No leading colon also works; trailing colon dropped.
        assert_eq!(
            split_image_path("Games:Oxyd 3.6:", true),
            vec!["Games", "Oxyd 3.6"]
        );
        assert!(split_image_path(":", true).is_empty());
    }

    #[test]
    fn split_image_parent_separates_basename() {
        assert_eq!(
            split_image_parent(r"/Apps/Oxyd 3.6/Oxyd b\/w", false),
            (vec!["Apps".into(), "Oxyd 3.6".into()], "Oxyd b/w".into())
        );
        assert_eq!(split_image_parent("/", false), (Vec::new(), String::new()));
        assert_eq!(
            split_image_parent(":Apps:Oxyd b/w", true),
            (vec!["Apps".into()], "Oxyd b/w".into())
        );
    }

    #[test]
    fn escape_path_component_round_trips() {
        for name in ["plain", "Oxyd b/w", r"a\b", r"weird/\name"] {
            let escaped = escape_path_component(name);
            // A single escaped component re-parses to exactly that name.
            assert_eq!(split_image_path(&escaped, false), vec![name.to_string()]);
        }
    }

    #[test]
    fn pick_block_size_floppy_and_large() {
        assert_eq!(pick_block_size(819_200), 512);
        // 60 MiB / 512 = 122880 > 65535, so bs jumps to 1024.
        assert_eq!(pick_block_size(60 * 1024 * 1024), 1024);
    }
}
