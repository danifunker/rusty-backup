//! Shared extended-attribute type, used across every filesystem that carries
//! POSIX-style xattrs (SquashFS, ext, XFS).
//!
//! An xattr is a `name` in a namespace (`user.`, `trusted.`, `security.`,
//! `system.`) mapped to an arbitrary **binary** `value`. The canonical example
//! is `security.capability` — a packed struct granting a binary its Linux
//! capabilities (e.g. `cap_net_raw` on `ping`), which is why xattrs must survive
//! a SquashFS rebuild and why an editor exposes the raw bytes rather than
//! pretending the value is text.

// `hex.len().is_multiple_of(2)` is an inherent method only since Rust 1.87; the
// vintage 1.73 build gets it from this trait. See CONTRIBUTING.md § Rust 1.73.
#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;

/// One extended attribute: a fully-qualified name and its raw value bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Xattr {
    /// Fully-qualified name including its namespace prefix, e.g.
    /// `security.capability` or `user.mime_type`.
    pub name: String,
    /// Raw value bytes. May be arbitrary binary (a capability struct) or UTF-8
    /// text (`user.*` attributes often are) — the caller decides how to render.
    pub value: Vec<u8>,
}

impl Xattr {
    /// Render the value for display: as text if it is valid, printable UTF-8,
    /// otherwise as a `0x`-prefixed hex string. Lets a browse view show
    /// `user.mime_type = text/plain` but `security.capability = 0x0100...`.
    pub fn value_display(&self) -> String {
        if let Ok(s) = std::str::from_utf8(&self.value) {
            if !s.is_empty() && s.chars().all(|c| !c.is_control() || c == '\n' || c == '\t') {
                return s.to_string();
            }
        }
        format!("0x{}", hex_encode(&self.value))
    }
}

/// Lowercase hex, no separators.
pub fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0xf) as u32, 16).unwrap());
    }
    s
}

/// Parse a value written by a user: `0x`-prefixed hex is decoded to raw bytes,
/// anything else is taken as its UTF-8 bytes. The inverse of
/// [`Xattr::value_display`] for round-tripping through an editor.
pub fn parse_value(input: &str) -> Result<Vec<u8>, String> {
    let trimmed = input.trim();
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        let hex: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
        if !hex.len().is_multiple_of(2) {
            return Err("hex value must have an even number of digits".into());
        }
        let mut out = Vec::with_capacity(hex.len() / 2);
        let bytes = hex.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let hi = (bytes[i] as char)
                .to_digit(16)
                .ok_or_else(|| format!("invalid hex digit {:?}", bytes[i] as char))?;
            let lo = (bytes[i + 1] as char)
                .to_digit(16)
                .ok_or_else(|| format!("invalid hex digit {:?}", bytes[i + 1] as char))?;
            out.push(((hi << 4) | lo) as u8);
            i += 2;
        }
        Ok(out)
    } else {
        Ok(input.as_bytes().to_vec())
    }
}

/// A recognized xattr namespace prefix. A name without one of these cannot be
/// represented on disk, so an editor rejects it up front.
pub const NAMESPACES: &[&str] = &["user.", "trusted.", "security.", "system."];

/// True when `name` starts with a representable namespace prefix.
pub fn has_valid_namespace(name: &str) -> bool {
    NAMESPACES.iter().any(|p| name.starts_with(p))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_values_display_as_text_binary_as_hex() {
        let t = Xattr {
            name: "user.mime_type".into(),
            value: b"text/plain".to_vec(),
        };
        assert_eq!(t.value_display(), "text/plain");
        let b = Xattr {
            name: "security.capability".into(),
            value: vec![1, 0, 0, 2, 0xff],
        };
        assert_eq!(b.value_display(), "0x01000002ff");
    }

    #[test]
    fn parse_round_trips_hex_and_text() {
        assert_eq!(parse_value("0x0100ff").unwrap(), vec![1, 0, 0xff]);
        assert_eq!(parse_value("0x01 00 ff").unwrap(), vec![1, 0, 0xff]);
        assert_eq!(parse_value("text/plain").unwrap(), b"text/plain".to_vec());
        assert!(parse_value("0x123").is_err(), "odd hex length rejected");
        assert!(parse_value("0xzz").is_err(), "bad hex digit rejected");
    }

    #[test]
    fn namespace_validation() {
        assert!(has_valid_namespace("security.capability"));
        assert!(has_valid_namespace("user.foo"));
        assert!(!has_valid_namespace("bogus.foo"));
        assert!(!has_valid_namespace("capability"));
    }
}
