//! Optional line-ending conversion for imported text files (`dos2unix` and friends).
//!
//! [`LineEndings`] picks the target convention. A file is converted only when it looks like text
//! and is not an archive: see [`is_text`] and [`is_archive`]. The decision reads the first
//! [`SNIFF_BYTES`] of the file and nothing more.
//!
//! Conversion is a streaming filter ([`LineEndingReader`]) with one byte of state, so a CR at the
//! end of one read and the LF at the start of the next still make one CRLF. What each mode does:
//!
//! | Mode | CRLF | lone LF | lone CR |
//! |---|---|---|---|
//! | [`LineEndings::Unix`] | LF | LF | kept |
//! | [`LineEndings::Dos`] | CRLF | CRLF | kept |
//! | [`LineEndings::Mac`] | CR | CR | CR |
//!
//! A lone CR is a classic Mac line break; `Unix` and `Dos` leave it alone (as `dos2unix` does),
//! so a CR-only file is not run together into one line.

use std::io::{self, Read};

/// How many leading bytes decide whether a file is text.
pub const SNIFF_BYTES: usize = 8192;

/// Which line endings imported text files get.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LineEndings {
    /// Copy every file byte for byte.
    #[default]
    Keep,
    /// CRLF to LF (`dos2unix`), for Unix targets such as IRIX.
    Unix,
    /// CRLF and LF to CR, for classic Mac OS.
    Mac,
    /// LF to CRLF (`unix2dos`); an existing CRLF stays as it is.
    Dos,
}

impl LineEndings {
    /// Parse `keep`, `unix` (or `lf`), `mac` (or `cr`) and `dos` (or `crlf`).
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_lowercase().as_str() {
            "keep" => Self::Keep,
            "unix" | "lf" => Self::Unix,
            "mac" | "cr" => Self::Mac,
            "dos" | "crlf" => Self::Dos,
            _ => return None,
        })
    }
}

/// Whether `head` reads as text: no NUL or stray control byte (tab, line and page breaks, BEL,
/// BS, ESC and SUB are fine); bytes from 0x80 pass, so Latin-1 and UTF-8 do and UTF-16 does not.
pub fn is_text(head: &[u8]) -> bool {
    head.iter().all(|&b| {
        b >= 0x20
            || matches!(
                b,
                b'\t' | b'\n' | 0x0b | 0x0c | b'\r' | 0x07 | 0x08 | 0x1a | 0x1b
            )
    })
}

/// Archives and disk images that look like text, or are named as one, are copied byte for byte.
const ARCHIVE_EXTENSIONS: &[&str] = &[
    "tar", "tgz", "gz", "z", "bz2", "tbz", "tbz2", "xz", "txz", "zst", "tzst", "zip", "7z", "rar",
    "lha", "lzh", "arc", "arj", "zoo", "cab", "tardist", "sit", "sea", "cpt", "hqx", "mar", "bin",
    "shar", "uu", "uue", "xxe", "cpio", "rpm", "deb", "dmg", "img", "iso", "dsk", "hfv", "adf",
];

/// Whether a file is an archive by its name or its first bytes; such a file is never converted.
pub fn is_archive(name: &str, head: &[u8]) -> bool {
    let named = name.rsplit_once('.').is_some_and(|(_, ext)| {
        ARCHIVE_EXTENSIONS
            .iter()
            .any(|a| a.eq_ignore_ascii_case(ext))
    });
    let magic: &[&[u8]] = &[
        b"PK\x03\x04",
        b"\x1f\x8b",
        b"BZh",
        b"\xfd7zXZ\x00",
        b"\x28\xb5\x2f\xfd",
        b"7z\xbc\xaf",
        b"Rar!",
        b"SIT!",
        b"StuffIt",
        b"(This file must be converted with BinHex",
        b"#!/bin/sh\n# This is a shell archive",
    ];
    let ustar = head.get(257..262) == Some(b"ustar");
    named || ustar || magic.iter().any(|m| head.starts_with(m))
}

/// Whether a file with this name and first bytes is converted under `mode`.
pub fn converts(mode: LineEndings, name: &str, head: &[u8]) -> bool {
    mode != LineEndings::Keep && is_text(head) && !is_archive(name, head)
}

/// A reader that yields `inner`'s bytes with their line endings converted to `mode`.
pub struct LineEndingReader<R> {
    inner: R,
    mode: LineEndings,
    /// A CR read but not yet written: whether it ends a CRLF depends on the next byte.
    pending_cr: bool,
    /// Output waiting for the caller's buffer (at most two bytes per input byte).
    out: Vec<u8>,
    out_at: usize,
    buf: Vec<u8>,
    eof: bool,
    changed: bool,
}

impl<R: Read> LineEndingReader<R> {
    pub fn new(inner: R, mode: LineEndings) -> Self {
        Self {
            inner,
            mode,
            pending_cr: false,
            out: Vec::new(),
            out_at: 0,
            buf: vec![0; 64 * 1024],
            eof: false,
            changed: false,
        }
    }

    /// Whether any line ending read so far came out different.
    pub fn changed(&self) -> bool {
        self.changed
    }

    /// Convert one input byte into `out`.
    fn push(&mut self, b: u8) {
        let (cr_lf, lf): (&[u8], &[u8]) = match self.mode {
            LineEndings::Keep => unreachable!("Keep never builds a converter"),
            LineEndings::Unix => (b"\n", b"\n"),
            LineEndings::Mac => (b"\r", b"\r"),
            LineEndings::Dos => (b"\r\n", b"\r\n"),
        };
        if self.pending_cr {
            self.pending_cr = false;
            if b == b'\n' {
                self.out.extend_from_slice(cr_lf);
                self.changed |= cr_lf != b"\r\n";
                return;
            }
            self.out.push(b'\r');
        }
        match b {
            b'\r' => self.pending_cr = true,
            b'\n' => {
                self.out.extend_from_slice(lf);
                self.changed |= lf != b"\n";
            }
            _ => self.out.push(b),
        }
    }

    /// Refill `out` from the next chunk of input; false at the end of the stream.
    fn fill(&mut self) -> io::Result<bool> {
        self.out.clear();
        self.out_at = 0;
        while self.out.is_empty() {
            if self.eof {
                return Ok(false);
            }
            let n = self.inner.read(&mut self.buf)?;
            if n == 0 {
                self.eof = true;
                // A CR at the very end is a lone CR, kept as one (as CR in Mac mode too).
                if std::mem::take(&mut self.pending_cr) {
                    self.out.push(b'\r');
                }
                continue;
            }
            for i in 0..n {
                let b = self.buf[i];
                self.push(b);
            }
        }
        Ok(true)
    }
}

impl<R: Read> Read for LineEndingReader<R> {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        if dst.is_empty() {
            return Ok(0);
        }
        if self.out_at == self.out.len() && !self.fill()? {
            return Ok(0);
        }
        let n = (self.out.len() - self.out_at).min(dst.len());
        dst[..n].copy_from_slice(&self.out[self.out_at..self.out_at + n]);
        self.out_at += n;
        Ok(n)
    }
}

/// Read up to [`SNIFF_BYTES`] from `r` (fewer only at the end of the stream).
pub fn read_head(r: &mut dyn Read) -> io::Result<Vec<u8>> {
    let mut head = Vec::with_capacity(SNIFF_BYTES);
    r.take(SNIFF_BYTES as u64).read_to_end(&mut head)?;
    Ok(head)
}

/// The length a file of `name` imports as under `mode`, reading `r` to its end, and whether the
/// conversion changes it; a file [`converts`] turns down keeps its raw length.
pub fn imported_len(mode: LineEndings, name: &str, r: &mut dyn Read) -> io::Result<(u64, bool)> {
    let head = read_head(r)?;
    if !converts(mode, name, &head) {
        return Ok((head.len() as u64 + io::copy(r, &mut io::sink())?, false));
    }
    let mut conv = LineEndingReader::new(io::Cursor::new(head).chain(r), mode);
    let len = io::copy(&mut conv, &mut io::sink())?;
    Ok((len, conv.changed()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Convert through a reader that hands out `chunk` bytes per read, to split CRLFs.
    fn convert(input: &[u8], mode: LineEndings, chunk: usize) -> Vec<u8> {
        struct Chunks<'a>(&'a [u8], usize);
        impl Read for Chunks<'_> {
            fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
                let n = self.0.len().min(self.1).min(dst.len());
                dst[..n].copy_from_slice(&self.0[..n]);
                self.0 = &self.0[n..];
                Ok(n)
            }
        }
        let mut out = Vec::new();
        LineEndingReader::new(Chunks(input, chunk), mode)
            .read_to_end(&mut out)
            .unwrap();
        out
    }

    #[test]
    fn unix_mode() {
        for chunk in [1, 2, 3, 64 * 1024] {
            assert_eq!(convert(b"a\r\nb\r\n", LineEndings::Unix, chunk), b"a\nb\n");
            assert_eq!(convert(b"a\rb\r", LineEndings::Unix, chunk), b"a\rb\r");
            assert_eq!(convert(b"a\nb\n", LineEndings::Unix, chunk), b"a\nb\n");
            assert_eq!(
                convert(b"a\r\nb\nc\rd\r\r\n", LineEndings::Unix, chunk),
                b"a\nb\nc\rd\r\n"
            );
            assert_eq!(convert(b"", LineEndings::Unix, chunk), b"");
            assert_eq!(convert(b"\r", LineEndings::Unix, chunk), b"\r");
        }
    }

    #[test]
    fn crlf_split_across_reads() {
        // The CR ends the first read and the LF starts the second.
        assert_eq!(convert(b"ab\r\ncd", LineEndings::Unix, 3), b"ab\ncd");
        assert_eq!(convert(b"ab\r\ncd", LineEndings::Dos, 3), b"ab\r\ncd");
        assert_eq!(convert(b"ab\r\ncd", LineEndings::Mac, 3), b"ab\rcd");
        // And across the converter's own 64 KiB buffer.
        let mut big = vec![b'x'; 64 * 1024 - 1];
        big.extend_from_slice(b"\r\nend");
        let mut want = vec![b'x'; 64 * 1024 - 1];
        want.extend_from_slice(b"\nend");
        assert_eq!(convert(&big, LineEndings::Unix, 64 * 1024), want);
    }

    #[test]
    fn mac_and_dos_modes() {
        assert_eq!(convert(b"a\r\nb\nc\rd", LineEndings::Mac, 2), b"a\rb\rc\rd");
        assert_eq!(
            convert(b"a\r\nb\nc\rd", LineEndings::Dos, 2),
            b"a\r\nb\r\nc\rd"
        );
        assert_eq!(convert(b"\n\n", LineEndings::Dos, 1), b"\r\n\r\n");
    }

    #[test]
    fn text_and_archive_tests() {
        assert!(is_text(b"plain ASCII\r\n"));
        assert!(is_text("caf\u{e9} na\u{ef}ve\n".as_bytes())); // UTF-8
        assert!(is_text(b"caf\xe9\n")); // Latin-1
        assert!(is_text(b""));
        assert!(!is_text(b"a\x00b"));
        assert!(!is_text(b"\x7fELF\x02\x01\x01\x00"));
        assert!(!is_text(b"MZ\x90\x00\x03\x00\x00\x00"));
        assert!(is_archive(
            "notes.hqx",
            b"(This file must be converted with BinHex 4.0)\r"
        ));
        assert!(is_archive(
            "x",
            b"(This file must be converted with BinHex 4.0)\r"
        ));
        assert!(is_archive("pkg.TAR", b"anything"));
        assert!(!is_archive("README", b"hello\r\n"));
        assert!(!converts(LineEndings::Keep, "README", b"hello\r\n"));
        assert!(converts(LineEndings::Unix, "README", b"hello\r\n"));
    }

    #[test]
    fn imported_len_matches_the_bytes_written() {
        let text = b"one\r\ntwo\r\nthree\n".repeat(5000);
        let len = imported_len(LineEndings::Unix, "a.txt", &mut &text[..]).unwrap();
        assert_eq!(
            len,
            (convert(&text, LineEndings::Unix, 4096).len() as u64, true)
        );
        // Text already in the target convention converts to itself and reports no change.
        let len = imported_len(LineEndings::Unix, "c.txt", &mut &b"a\nb\n"[..]).unwrap();
        assert_eq!(len, (4, false));
        // A binary file with CRLF bytes in it keeps its length.
        let bin = b"\x00\x01\r\n\x02\r\n".repeat(10);
        let len = imported_len(LineEndings::Unix, "b", &mut &bin[..]).unwrap();
        assert_eq!(len, (bin.len() as u64, false));
    }
}
