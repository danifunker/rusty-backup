//! Byte-exact on-disk names through the `&str` filesystem API.
//!
//! Unix filesystems store a name as bytes, not text. NeXTSTEP wrote its names
//! in the 8-bit **NeXTSTEP encoding** (ASCII below 0x80; 0x80-0xFD are Latin
//! letters and punctuation laid out unlike ISO 8859-1 — `0xF6` is `u` with a
//! diaeresis, `0xBC` a horizontal ellipsis; see Unicode's
//! `VENDORS/NEXT/NEXTSTEP.TXT`), and Japanese NeXTSTEP-J wrote EUC-JP. Neither
//! is UTF-8, and which one a given disk used is not recorded anywhere, so a
//! name cannot be transcoded safely — only carried.
//!
//! The filesystem traits pass names as `String`, so undecodable bytes travel as
//! placeholder characters: byte `b` becomes `U+10FF00 + b`, the last 256 code
//! points of Supplementary Private Use Area-B, which no real name uses.
//! [`decode`] turns on-disk bytes into such a name (valid UTF-8 runs are kept
//! as text), and [`encode`] turns it back into the exact bytes, so a name read
//! off one disk and written to another lands byte for byte. A host file whose
//! name carries the placeholders (an earlier extract) re-imports the same way.

use std::borrow::Cow;

/// First placeholder code point; byte `b` maps to `RAW_BASE + b`.
const RAW_BASE: u32 = 0x0010_FF00;

/// On-disk name bytes as a name: UTF-8 kept as text, every other byte a placeholder.
pub fn decode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    let mut rest = bytes;
    loop {
        match std::str::from_utf8(rest) {
            Ok(s) => {
                out.push_str(s);
                return out;
            }
            Err(e) => {
                let (good, bad) = rest.split_at(e.valid_up_to());
                out.push_str(std::str::from_utf8(good).unwrap_or_default());
                let n = e.error_len().unwrap_or(bad.len());
                out.extend(bad[..n].iter().map(|&b| placeholder(b)));
                rest = &bad[n..];
            }
        }
    }
}

/// The bytes to store for `name`: placeholders become their byte, everything else UTF-8.
pub fn encode(name: &str) -> Cow<'_, [u8]> {
    if !name.chars().any(|c| raw_byte(c).is_some()) {
        return Cow::Borrowed(name.as_bytes());
    }
    let mut out = Vec::with_capacity(name.len());
    let mut buf = [0u8; 4];
    for c in name.chars() {
        match raw_byte(c) {
            Some(b) => out.push(b),
            None => out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes()),
        }
    }
    Cow::Owned(out)
}

/// The byte a placeholder stands for, or `None` for an ordinary character.
pub fn raw_byte(c: char) -> Option<u8> {
    let v = u32::from(c);
    (RAW_BASE..=RAW_BASE + 0xFF)
        .contains(&v)
        .then(|| (v - RAW_BASE) as u8)
}

fn placeholder(b: u8) -> char {
    char::from_u32(RAW_BASE + u32::from(b)).expect("Private Use Area-B is valid")
}

/// `name` with each placeholder shown as `\xNN`, for terminal output.
pub fn display(name: &str) -> Cow<'_, str> {
    if !name.chars().any(|c| raw_byte(c).is_some()) {
        return Cow::Borrowed(name);
    }
    let mut out = String::with_capacity(name.len());
    for c in name.chars() {
        match raw_byte(c) {
            Some(b) => out.push_str(&format!("\\x{b:02X}")),
            None => out.push(c),
        }
    }
    Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_names_pass_through_untouched() {
        assert_eq!(decode(b"NeXTSTEP_3.3"), "NeXTSTEP_3.3");
        assert_eq!(decode("caf\u{e9}".as_bytes()), "caf\u{e9}");
        assert!(matches!(encode("plain"), Cow::Borrowed(_)));
    }

    /// NeXTSTEP-encoded `u` with diaeresis (0xF6) and an EUC-JP run both survive the round trip.
    #[test]
    fn nextstep_and_euc_bytes_round_trip() {
        for raw in [
            &b"Steroidgrundger\xf6st.lookMol"[..],
            &b"Eject\xbcE.tiff"[..],
            &b"01OmniWeb \xa4\xd8\xa4\xe8\xa4\xa6\xa4\xb3\xa4\xbd.wdesk"[..],
            &b"\xff\xfe"[..],
        ] {
            let name = decode(raw);
            assert_eq!(&*encode(&name), raw, "{name:?}");
        }
    }

    /// Through a real UFS: the dirent holds the NeXT byte itself, and listing gives the name back.
    #[test]
    fn a_ufs_dirent_stores_the_raw_byte() {
        use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
        let img =
            crate::fs::ufs_format::create_blank_ufs1(&crate::fs::ufs_format::Ufs1FormatParams {
                size_bytes: 8 * 1024 * 1024,
                cg_layout: crate::fs::ufs::CgLayout::Bsd43,
                endian: crate::fs::ufs::UfsEndian::Big,
                ..Default::default()
            })
            .unwrap();
        let mut cur = std::io::Cursor::new(img);
        let mut fs = crate::fs::ufs::UfsFilesystem::open(&mut cur, 0).unwrap();
        let root = fs.root().unwrap();
        let name = decode(b"Steroidgrundger\xf6st.lookMol");
        let opts = CreateFileOptions::default();
        fs.create_file(&root, &name, &mut &b"C27"[..], 3, &opts)
            .unwrap();
        fs.sync_metadata().unwrap();
        let listed = fs.list_directory(&root).unwrap();
        assert!(listed.iter().any(|e| e.name == name), "{listed:?}");

        drop(fs);
        let img = cur.into_inner();
        let raw = b"Steroidgrundger\xf6st.lookMol";
        assert!(img.windows(raw.len()).any(|w| w == raw), "raw 0xF6 on disk");
        let utf8 = name.as_bytes();
        assert!(
            !img.windows(utf8.len()).any(|w| w == utf8),
            "no placeholder bytes on disk"
        );
    }

    #[test]
    fn display_spells_out_the_raw_bytes() {
        assert_eq!(display(&decode(b"Eject\xbcE.tiff")), "Eject\\xBCE.tiff");
        assert_eq!(display("plain"), "plain");
    }
}
