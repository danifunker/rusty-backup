//! Round-tripping a text file through an editor without destroying how the
//! system that wrote it expects to read it back.
//!
//! Handing a vintage text file straight to an editor corrupts it two ways, and
//! neither is the editor's fault:
//!
//! * **Line endings.** vim detects `fileformat` and preserves CRLF, but its
//!   default `fileformats` on Unix is `unix,dos` — a classic-Mac **CR-only**
//!   file loads as one long line of `^M` and is mangled on save. `ed`, `sed` and
//!   old notepad do not preserve anything. Every editor normalises a
//!   mixed-ending file.
//! * **Encoding.** A DOS text file is CP437, not UTF-8; classic Mac is
//!   MacRoman; Human68k is Shift-JIS. A UTF-8 editor shows mojibake for the
//!   high bytes and then writes UTF-8 back, so the file stops being readable on
//!   the machine it belongs to.
//!
//! So the editor is never shown the vintage form. [`decode_for_edit`] hands out
//! clean UTF-8 with LF endings — the one shape every editor gets right — and
//! [`encode_after_edit`] puts the original encoding and endings back. What the
//! editor does in between stops mattering.
//!
//! This is deliberately shared: the CLI's `edit` verb (which shells out to
//! `$EDITOR`) and the in-app editors in the TUI and GUI all need exactly this,
//! and a second implementation would be a second set of corruption bugs.

use crate::fs::fat::{char_to_cp437, cp437_to_char};
use crate::fs::hfs::{mac_roman_to_utf8, utf8_to_mac_roman};

/// How a file's bytes represent characters.
///
/// Not recorded anywhere on disk — it is inferred from the filesystem's
/// convention plus the bytes themselves. See [`decode_for_edit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextEncoding {
    /// Modern Unix volumes, and any file whose bytes are already valid UTF-8.
    Utf8,
    /// DOS / FAT: the OEM codepage.
    Cp437,
    /// Classic Mac OS: HFS and HFS+.
    MacRoman,
    /// Amiga (AFFS / PFS3 / SFS) and other ISO-8859-1 volumes.
    Latin1,
    /// Sharp X68000 / Human68k.
    ShiftJis,
}

impl TextEncoding {
    /// Name for logs and error messages.
    pub fn label(self) -> &'static str {
        match self {
            Self::Utf8 => "UTF-8",
            Self::Cp437 => "CP437",
            Self::MacRoman => "MacRoman",
            Self::Latin1 => "Latin-1",
            Self::ShiftJis => "Shift-JIS",
        }
    }

    /// Parse an `--encoding` value.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().replace(['-', '_'], "").as_str() {
            "utf8" => Some(Self::Utf8),
            "cp437" | "437" | "oem" | "dos" => Some(Self::Cp437),
            "macroman" | "mac" => Some(Self::MacRoman),
            "latin1" | "iso88591" | "amiga" => Some(Self::Latin1),
            "shiftjis" | "sjis" | "x68000" | "human68k" => Some(Self::ShiftJis),
            _ => None,
        }
    }
}

/// Which byte sequence separates lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineEnding {
    /// Unix.
    Lf,
    /// DOS / Windows, and most vintage PC formats.
    CrLf,
    /// Classic Mac OS (pre-OS X).
    Cr,
}

impl LineEnding {
    pub fn label(self) -> &'static str {
        match self {
            Self::Lf => "LF",
            Self::CrLf => "CRLF",
            Self::Cr => "CR",
        }
    }

    /// Parse a `--line-endings` value.
    ///
    /// Named the way people describe them rather than by control character:
    /// someone repairing a file thinks "make it DOS", not "make it 0x0D 0x0A".
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().replace(['-', '_'], "").as_str() {
            "lf" | "unix" | "linux" | "n" => Some(Self::Lf),
            "crlf" | "dos" | "windows" | "rn" => Some(Self::CrLf),
            "cr" | "mac" | "classicmac" | "r" => Some(Self::Cr),
            _ => None,
        }
    }

    fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Lf => b"\n",
            Self::CrLf => b"\r\n",
            Self::Cr => b"\r",
        }
    }
}

/// Everything needed to put a file back the way it was found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextShape {
    pub encoding: TextEncoding,
    pub ending: LineEnding,
    /// The original used more than one ending convention. Restoring it exactly
    /// is not possible once normalised, so the dominant one is applied
    /// throughout — callers should say so, because it changes bytes the user
    /// did not edit.
    pub mixed_endings: bool,
}

/// A file decoded into the shape an editor should see, plus how to undo that.
#[derive(Debug, Clone)]
pub struct DecodedText {
    /// UTF-8, LF-only.
    pub text: String,
    pub shape: TextShape,
}

/// Why a text round-trip could not be completed.
#[derive(Debug, Clone)]
pub enum TextEditError {
    /// The content does not look like text. Editing it as text would corrupt
    /// it, so this is refused rather than attempted.
    NotText { reason: String },
    /// A character in the edited text has no representation in the file's
    /// encoding. Reported with a position so it can be found and fixed.
    Unrepresentable {
        ch: char,
        line: usize,
        col: usize,
        encoding: TextEncoding,
    },
}

impl std::fmt::Display for TextEditError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotText { reason } => {
                write!(f, "does not look like a text file ({reason})")
            }
            Self::Unrepresentable {
                ch,
                line,
                col,
                encoding,
            } => write!(
                f,
                "line {line}, col {col}: U+{:04X} {} is not representable in {}",
                *ch as u32,
                char_name(*ch),
                encoding.label(),
            ),
        }
    }
}

impl std::error::Error for TextEditError {}

/// A short description of a character for error messages. Deliberately not a
/// full Unicode name table — the codepoint is the actionable part, and a name
/// table would be a megabyte of data in a disk utility.
fn char_name(c: char) -> String {
    match c {
        '\u{2014}' => "EM DASH".to_string(),
        '\u{2013}' => "EN DASH".to_string(),
        '\u{2018}' | '\u{2019}' => "CURLY QUOTE".to_string(),
        '\u{201C}' | '\u{201D}' => "CURLY DOUBLE QUOTE".to_string(),
        '\u{2026}' => "ELLIPSIS".to_string(),
        '\u{00A0}' => "NO-BREAK SPACE".to_string(),
        c if c.is_control() => "control character".to_string(),
        c => format!("'{c}'"),
    }
}

/// The encoding a filesystem's text files conventionally use.
///
/// A hint, not a fact — nothing on disk records it. `fs_type` is the string
/// [`crate::fs::Filesystem::fs_type`] reports.
pub fn conventional_encoding_for(fs_type: &str) -> TextEncoding {
    let t = fs_type.to_ascii_lowercase();
    if t.starts_with("fat") || t.contains("exfat") || t.contains("ntfs") {
        TextEncoding::Cp437
    } else if t.contains("hfs") || t.contains("mfs") || t.contains("prodos") {
        TextEncoding::MacRoman
    } else if t.contains("human68k") {
        TextEncoding::ShiftJis
    } else if t.contains("affs") || t.contains("pfs") || t.contains("sfs") || t.contains("amiga") {
        TextEncoding::Latin1
    } else {
        // ext / btrfs / xfs / UFS / JFS / ISO-9660 and anything unrecognised.
        TextEncoding::Utf8
    }
}

/// Whether these bytes should be refused as non-text.
///
/// A NUL is the decisive signal: no text encoding here uses it, and every
/// binary format is full of them. Beyond that, a high proportion of control
/// bytes means the same thing.
pub fn looks_binary(bytes: &[u8]) -> Option<String> {
    if let Some(pos) = bytes.iter().position(|&b| b == 0) {
        return Some(format!("NUL byte at offset {pos}"));
    }
    if bytes.is_empty() {
        return None;
    }
    let odd = bytes
        .iter()
        .filter(|&&b| b < 0x09 || (0x0E..0x20).contains(&b))
        .count();
    // 2% of control bytes is already far more than any real text file.
    if odd * 50 > bytes.len() {
        return Some(format!(
            "{odd} control bytes in {} - {}%",
            bytes.len(),
            odd * 100 / bytes.len()
        ));
    }
    None
}

/// The dominant line ending, and whether the file mixes conventions.
pub fn detect_line_ending(bytes: &[u8]) -> (LineEnding, bool) {
    let mut crlf = 0usize;
    let mut lone_lf = 0usize;
    let mut lone_cr = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\r' => {
                if bytes.get(i + 1) == Some(&b'\n') {
                    crlf += 1;
                    i += 2;
                    continue;
                }
                lone_cr += 1;
            }
            b'\n' => lone_lf += 1,
            _ => {}
        }
        i += 1;
    }
    let kinds = [crlf, lone_lf, lone_cr].iter().filter(|&&n| n > 0).count();
    let mixed = kinds > 1;
    // Ties go to CRLF then LF: a file with one of each is more likely a DOS file
    // with a stray LF than a Unix file with a stray CR.
    let dominant = if crlf >= lone_lf && crlf >= lone_cr && crlf > 0 {
        LineEnding::CrLf
    } else if lone_cr > lone_lf {
        LineEnding::Cr
    } else {
        LineEnding::Lf
    };
    (dominant, mixed)
}

/// Decode a file's bytes into UTF-8 with LF endings, recording how to restore
/// the original form.
///
/// Encoding selection, in order:
/// 1. `forced`, when the caller passed `--encoding`.
/// 2. If every byte is ASCII, the filesystem's convention. ASCII decodes
///    identically under all of these, and choosing the convention here means a
///    non-ASCII character *added* during the edit is written in the form the
///    volume expects (or refused) rather than silently becoming UTF-8.
/// 3. Valid UTF-8 with non-ASCII bytes: UTF-8. It is self-validating, so this is
///    not a guess.
/// 4. Otherwise the filesystem's convention.
pub fn decode_for_edit(
    bytes: &[u8],
    fs_type: &str,
    forced: Option<TextEncoding>,
) -> Result<DecodedText, TextEditError> {
    if let Some(reason) = looks_binary(bytes) {
        return Err(TextEditError::NotText { reason });
    }
    let conventional = conventional_encoding_for(fs_type);
    let encoding = match forced {
        Some(e) => e,
        None => {
            if bytes.iter().all(|&b| b < 0x80) {
                conventional
            } else if std::str::from_utf8(bytes).is_ok() {
                TextEncoding::Utf8
            } else {
                conventional
            }
        }
    };

    let decoded = match encoding {
        TextEncoding::Utf8 => String::from_utf8_lossy(bytes).into_owned(),
        TextEncoding::Cp437 => bytes.iter().map(|&b| cp437_to_char(b)).collect(),
        TextEncoding::MacRoman => mac_roman_to_utf8(bytes),
        TextEncoding::Latin1 => bytes.iter().map(|&b| b as char).collect(),
        TextEncoding::ShiftJis => {
            let (text, _, _) = encoding_rs::SHIFT_JIS.decode(bytes);
            text.into_owned()
        }
    };

    let (ending, mixed_endings) = detect_line_ending(bytes);
    // Normalise to LF for the editor. CRLF first so a CRLF file does not leave
    // stray CRs behind.
    let text = decoded.replace("\r\n", "\n").replace('\r', "\n");

    Ok(DecodedText {
        text,
        shape: TextShape {
            encoding,
            ending,
            mixed_endings,
        },
    })
}

/// Re-encode edited UTF-8/LF text into the file's original form.
///
/// With `substitute` false (the default, and what callers should prefer) a
/// character the encoding cannot represent is an error naming its position,
/// and nothing is written. Substituting silently is how a text file quietly
/// stops saying what it said.
pub fn encode_after_edit(
    text: &str,
    shape: &TextShape,
    substitute: bool,
) -> Result<Vec<u8>, TextEditError> {
    // Normalise whatever came back before re-applying the file's convention.
    // The editor was handed LF-only text, but plenty of editors write CRLF
    // regardless (every Windows one, and anything configured with
    // `fileformat=dos`). Re-applying CRLF on top of a CR the editor added
    // produces `\r\r\n` - a doubled carriage return on every line it
    // touched, which is exactly the silent corruption this module exists to
    // prevent.
    let normalised = text.replace("\r\n", "\n").replace('\r', "\n");
    let text: &str = &normalised;

    let mut out = Vec::with_capacity(text.len() + 16);
    let eol = shape.ending.as_bytes();
    let mut line = 1usize;
    let mut col = 1usize;

    for ch in text.chars() {
        if ch == '\n' {
            out.extend_from_slice(eol);
            line += 1;
            col = 1;
            continue;
        }
        match encode_char(ch, shape.encoding) {
            Some(bytes) => out.extend_from_slice(&bytes),
            None if substitute => out.push(b'?'),
            None => {
                return Err(TextEditError::Unrepresentable {
                    ch,
                    line,
                    col,
                    encoding: shape.encoding,
                })
            }
        }
        col += 1;
    }
    Ok(out)
}

/// Encode one character, or `None` when the encoding has no representation.
fn encode_char(ch: char, encoding: TextEncoding) -> Option<Vec<u8>> {
    match encoding {
        TextEncoding::Utf8 => {
            let mut buf = [0u8; 4];
            Some(ch.encode_utf8(&mut buf).as_bytes().to_vec())
        }
        TextEncoding::Cp437 => char_to_cp437(ch).map(|b| vec![b]),
        TextEncoding::MacRoman => {
            // Per-character so the failure names the character. The HFS helper
            // reports only that *something* failed.
            let s = ch.to_string();
            utf8_to_mac_roman(&s).ok().filter(|b| !b.is_empty())
        }
        TextEncoding::Latin1 => {
            let cp = ch as u32;
            (cp < 0x100).then(|| vec![cp as u8])
        }
        TextEncoding::ShiftJis => {
            let s = ch.to_string();
            let (bytes, _, had_errors) = encoding_rs::SHIFT_JIS.encode(&s);
            (!had_errors).then(|| bytes.into_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_endings_are_detected_and_restored() {
        for (raw, want) in [
            (&b"a\nb\n"[..], LineEnding::Lf),
            (&b"a\r\nb\r\n"[..], LineEnding::CrLf),
            (&b"a\rb\r"[..], LineEnding::Cr),
        ] {
            let (got, mixed) = detect_line_ending(raw);
            assert_eq!(got, want, "detect {raw:?}");
            assert!(!mixed, "{raw:?} is not mixed");

            // The editor always sees LF...
            let d = decode_for_edit(raw, "ext4", None).expect("decode");
            assert_eq!(d.text, "a\nb\n", "editor view of {raw:?}");
            // ...and the file gets its own convention back, byte for byte.
            let back = encode_after_edit(&d.text, &d.shape, false).expect("encode");
            assert_eq!(back, raw, "round trip of {raw:?}");
        }
    }

    /// An editor that writes CRLF into the LF text it was handed must not
    /// produce a doubled CR when the file's own convention is re-applied.
    #[test]
    fn endings_the_editor_introduced_are_normalised_before_re_encoding() {
        let shape = TextShape {
            encoding: TextEncoding::Cp437,
            ending: LineEnding::CrLf,
            mixed_endings: false,
        };
        // What a Windows editor hands back after appending a line.
        let edited = "PATH C:\\DOS\r\nREM added\r\n";
        let bytes = encode_after_edit(edited, &shape, false).expect("encode");
        assert!(
            !bytes.windows(2).any(|w| w == b"\r\r"),
            "no doubled CR: {bytes:?}"
        );
        assert_eq!(bytes, b"PATH C:\\DOS\r\nREM added\r\n");

        // A CR-only file gets CR-only back, whatever the editor used.
        let mac = TextShape {
            ending: LineEnding::Cr,
            ..shape
        };
        let bytes = encode_after_edit("one\r\ntwo\n", &mac, false).expect("encode");
        assert_eq!(bytes, b"one\rtwo\r");
    }

    #[test]
    fn a_mixed_ending_file_is_flagged() {
        let (_, mixed) = detect_line_ending(b"a\r\nb\nc\r\n");
        assert!(mixed, "CRLF plus a lone LF is mixed");
        let d = decode_for_edit(b"a\r\nb\nc\r\n", "fat16", None).unwrap();
        assert!(d.shape.mixed_endings);
        assert_eq!(d.shape.ending, LineEnding::CrLf, "CRLF dominates");
    }

    /// A DOS file's high bytes must survive the trip, which is the whole point:
    /// treating CP437 as UTF-8 loses them.
    #[test]
    fn cp437_round_trips_through_the_editor_form() {
        // 0xB3 is a box-drawing vertical bar in CP437, and invalid UTF-8.
        let raw = b"DIR \xb3 FILE\r\n";
        let d = decode_for_edit(raw, "fat16", None).expect("decode");
        assert_eq!(d.shape.encoding, TextEncoding::Cp437);
        assert!(
            d.text.contains('\u{2502}'),
            "0xB3 should decode to a box-drawing bar, got {:?}",
            d.text
        );
        let back = encode_after_edit(&d.text, &d.shape, false).expect("encode");
        assert_eq!(back, raw, "CP437 bytes must come back unchanged");
    }

    /// The case the user asked about: adding a character the target cannot hold
    /// must fail loudly, naming where it is, and write nothing.
    #[test]
    fn an_unrepresentable_character_is_refused_with_its_position() {
        let d = decode_for_edit(b"REM hello\r\n", "fat16", None).unwrap();
        assert_eq!(d.shape.encoding, TextEncoding::Cp437);

        let edited = "REM hello\nREM em\u{2014}dash\n";
        let err = encode_after_edit(edited, &d.shape, false).expect_err("must refuse");
        match err {
            TextEditError::Unrepresentable {
                ch,
                line,
                col,
                encoding,
            } => {
                assert_eq!(ch, '\u{2014}');
                assert_eq!(line, 2, "second line");
                assert_eq!(col, 7, "after 'REM em'");
                assert_eq!(encoding, TextEncoding::Cp437);
            }
            other => panic!("wrong error: {other:?}"),
        }
        // And with an explicit opt-in it proceeds.
        let forced = encode_after_edit(edited, &d.shape, true).expect("substitute");
        assert!(forced.ends_with(b"dash\r\n"));
    }

    /// An ASCII file on a DOS volume must be treated as CP437, not UTF-8, so a
    /// character added during the edit is written the way DOS reads it - or
    /// refused. Tagging it UTF-8 would silently write UTF-8 bytes instead.
    #[test]
    fn ascii_content_adopts_the_filesystem_convention() {
        let d = decode_for_edit(b"PATH C:\\DOS\r\n", "fat12", None).unwrap();
        assert_eq!(d.shape.encoding, TextEncoding::Cp437);

        let d = decode_for_edit(b"# fstab\n", "ext4", None).unwrap();
        assert_eq!(d.shape.encoding, TextEncoding::Utf8);

        let d = decode_for_edit(b"README\r", "hfs", None).expect("decode");
        assert_eq!(d.shape.encoding, TextEncoding::MacRoman);
        assert_eq!(d.shape.ending, LineEnding::Cr);
    }

    /// Valid UTF-8 with non-ASCII is self-validating, so it is trusted over the
    /// filesystem's convention.
    #[test]
    fn valid_utf8_wins_over_the_convention() {
        let raw = "café\n".as_bytes();
        let d = decode_for_edit(raw, "fat32", None).unwrap();
        assert_eq!(d.shape.encoding, TextEncoding::Utf8);
        assert_eq!(encode_after_edit(&d.text, &d.shape, false).unwrap(), raw);
    }

    #[test]
    fn binary_content_is_refused_rather_than_mangled() {
        let err = decode_for_edit(b"MZ\x90\x00\x03", "fat16", None).expect_err("refuse");
        match err {
            TextEditError::NotText { reason } => assert!(reason.contains("NUL")),
            other => panic!("wrong error: {other:?}"),
        }
        // A forced encoding does not override the binary check: the point is to
        // avoid writing a mangled executable back.
        assert!(decode_for_edit(b"\x00\x01", "ext4", Some(TextEncoding::Utf8)).is_err());
    }

    /// Endings are always read from the file, never assumed from the
    /// filesystem - a DOS-formatted volume holds LF-only files all the time,
    /// and rewriting them to CRLF because of where they live would corrupt
    /// files nobody asked to change.
    #[test]
    fn endings_come_from_the_content_not_the_filesystem() {
        // An LF file on a FAT volume stays LF.
        let d = decode_for_edit(b"one\ntwo\n", "fat16", None).unwrap();
        assert_eq!(d.shape.ending, LineEnding::Lf);
        assert_eq!(
            encode_after_edit(&d.text, &d.shape, false).unwrap(),
            b"one\ntwo\n"
        );

        // A CRLF file on ext stays CRLF.
        let d = decode_for_edit(b"one\r\ntwo\r\n", "ext4", None).unwrap();
        assert_eq!(d.shape.ending, LineEnding::CrLf);

        // And a deliberate conversion is just a different shape.
        let converted = TextShape {
            ending: LineEnding::Lf,
            ..d.shape
        };
        assert_eq!(
            encode_after_edit(&d.text, &converted, false).unwrap(),
            b"one\ntwo\n"
        );
    }

    #[test]
    fn line_ending_names_parse_the_way_a_user_would_type_them() {
        assert_eq!(LineEnding::parse("crlf"), Some(LineEnding::CrLf));
        assert_eq!(LineEnding::parse("dos"), Some(LineEnding::CrLf));
        assert_eq!(LineEnding::parse("unix"), Some(LineEnding::Lf));
        assert_eq!(LineEnding::parse("LF"), Some(LineEnding::Lf));
        assert_eq!(LineEnding::parse("mac"), Some(LineEnding::Cr));
        assert_eq!(LineEnding::parse("ebcdic"), None);
    }

    #[test]
    fn encoding_names_parse_the_way_a_user_would_type_them() {
        assert_eq!(TextEncoding::parse("cp437"), Some(TextEncoding::Cp437));
        assert_eq!(TextEncoding::parse("CP-437"), Some(TextEncoding::Cp437));
        assert_eq!(TextEncoding::parse("dos"), Some(TextEncoding::Cp437));
        assert_eq!(
            TextEncoding::parse("mac_roman"),
            Some(TextEncoding::MacRoman)
        );
        assert_eq!(TextEncoding::parse("sjis"), Some(TextEncoding::ShiftJis));
        assert_eq!(TextEncoding::parse("utf-8"), Some(TextEncoding::Utf8));
        assert_eq!(TextEncoding::parse("ebcdic"), None);
    }

    /// Shift-JIS is multi-byte, so a per-character encoder has to handle a
    /// character becoming more than one byte.
    #[test]
    fn shift_jis_multibyte_round_trips() {
        let shape = TextShape {
            encoding: TextEncoding::ShiftJis,
            ending: LineEnding::CrLf,
            mixed_endings: false,
        };
        let bytes = encode_after_edit("日本\n", &shape, false).expect("encode");
        assert!(bytes.len() > 4, "kanji are two bytes each plus CRLF");
        assert!(bytes.ends_with(b"\r\n"));
        let back = decode_for_edit(&bytes, "human68k", None).expect("decode");
        assert_eq!(back.text, "日本\n");
    }
}
