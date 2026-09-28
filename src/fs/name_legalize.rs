//! Make a filename legal on a destination filesystem, whatever its rules.
//!
//! Every editable driver already states its naming rules through
//! [`Filesystem::validate_name`](crate::fs::filesystem::Filesystem::validate_name)
//! (length, charset, encoding, reserved names, 8.3 shape, ...). Rather than
//! restate those rules in a second table that would drift, [`legalize_name`]
//! treats `validate_name` as an oracle and searches for the smallest repair it
//! accepts:
//!
//! 1. Per-character: each character is probed alone (`A<c>A`). One the
//!    destination refuses is replaced by its uppercase form, then an ASCII
//!    transliteration (`e` for `é`, `TM` for `™`), then `_`, `-` or `.`,
//!    and is dropped only when none of those are accepted.
//! 2. Structural: trailing spaces / dots are trimmed, extra dots collapsed,
//!    reserved names (`CON`, `..`) suffixed, a leading letter added for
//!    filesystems that need one (ProDOS, Atari DOS).
//! 3. Length: the stem is shortened first so the extension survives, then the
//!    extension, then the whole name.
//!
//! [`unique_name`] then appends `_1`, `_2`, ... (shortening the stem to make
//! room) when the legal name is already taken in the same folder.
//!
//! The small validators at the bottom ([`validate_posix_name`],
//! [`validate_amiga_name`], [`is_dos_device_name`]) are shared by drivers that
//! had no name check of their own.

use std::collections::HashMap;

use crate::fs::filesystem::FilesystemError;

/// A name check: `Ok` when the destination accepts the name as-is.
pub type NameValidator<'a> = dyn Fn(&str) -> Result<(), FilesystemError> + 'a;

/// Return `name` unchanged when `validate` accepts it, else the closest legal
/// name; `Err` (the destination's own reason) when no repair is accepted.
pub fn legalize_name(validate: &NameValidator<'_>, name: &str) -> Result<String, FilesystemError> {
    let original_err = match validate(name) {
        Ok(()) => return Ok(name.to_string()),
        Err(e) => e,
    };
    let mut probe = CharProbe::new(validate);
    let mapped = probe.map_chars(name);
    let candidates = structural_candidates(&mapped, &mut probe);
    if let Some(ok) = candidates.iter().find(|c| validate(c).is_ok()) {
        return Ok(ok.clone());
    }
    candidates
        .iter()
        .find_map(|c| shorten_to_fit(validate, c))
        .ok_or(original_err)
}

/// `name` if it is free, else `stem_N.ext` for the first free legal N.
pub fn unique_name(
    validate: &NameValidator<'_>,
    name: &str,
    is_taken: &dyn Fn(&str) -> bool,
) -> Option<String> {
    if !is_taken(name) {
        return Some(name.to_string());
    }
    let (stem, ext) = split_ext(name);
    let stem: Vec<char> = stem.chars().collect();
    for n in 1..10_000u32 {
        let suffix = format!("_{n}");
        for keep in (0..=stem.len()).rev() {
            let head: String = stem[..keep].iter().collect();
            let candidate = format!("{head}{suffix}{ext}");
            if validate(&candidate).is_ok() {
                if !is_taken(&candidate) {
                    return Some(candidate);
                }
                break;
            }
        }
    }
    None
}

/// Per-character acceptance, memoised: a long copy probes the same few characters.
struct CharProbe<'a, 'b> {
    validate: &'a NameValidator<'b>,
    cache: HashMap<String, bool>,
}

impl<'a, 'b> CharProbe<'a, 'b> {
    fn new(validate: &'a NameValidator<'b>) -> Self {
        Self {
            validate,
            cache: HashMap::new(),
        }
    }

    fn accepts(&mut self, piece: &str) -> bool {
        if let Some(&ok) = self.cache.get(piece) {
            return ok;
        }
        // Surrounding letters keep leading-letter and empty-name rules out of the probe.
        let ok = (self.validate)(&format!("A{piece}A")).is_ok()
            || (self.validate)(&format!("A{piece}")).is_ok();
        self.cache.insert(piece.to_string(), ok);
        ok
    }

    /// The first replacement the destination accepts, or "" to drop the char.
    fn replacement(&mut self) -> String {
        for r in ["_", "-", "."] {
            if self.accepts(r) {
                return r.to_string();
            }
        }
        String::new()
    }

    fn map_chars(&mut self, name: &str) -> String {
        let mut out = String::with_capacity(name.len());
        for c in name.chars() {
            let s = c.to_string();
            if self.accepts(&s) {
                out.push(c);
                continue;
            }
            let upper: String = c.to_uppercase().collect();
            if upper != s && self.accepts(&upper) {
                out.push_str(&upper);
                continue;
            }
            if let Some(ascii) = ascii_fold(c) {
                let folded: String = ascii
                    .chars()
                    .map(|a| {
                        if self.accepts(&a.to_string()) {
                            a.to_string()
                        } else if self.accepts(&a.to_ascii_uppercase().to_string()) {
                            a.to_ascii_uppercase().to_string()
                        } else {
                            String::new()
                        }
                    })
                    .collect();
                if !folded.is_empty() {
                    out.push_str(&folded);
                    continue;
                }
            }
            out.push_str(&self.replacement());
        }
        out
    }
}

/// Whole-name repairs to try in order, each on top of the character mapping.
fn structural_candidates(mapped: &str, probe: &mut CharProbe<'_, '_>) -> Vec<String> {
    let mut out = vec![mapped.to_string()];
    let trimmed = mapped
        .trim_start_matches(' ')
        .trim_end_matches([' ', '.'])
        .to_string();
    let base = if trimmed.is_empty() {
        let r = probe.replacement();
        if r.is_empty() {
            "X".to_string()
        } else {
            r
        }
    } else {
        trimmed
    };
    out.push(base.clone());
    // Filesystems that split on the first '.' refuse a second one (Atari DOS, RS-DOS).
    if let Some(last) = base.rfind('.') {
        let (head, tail) = base.split_at(last);
        if head.contains('.') {
            let r = probe.replacement();
            let r = if r == "." { String::new() } else { r };
            out.push(format!("{}{tail}", head.replace('.', &r)));
        }
        out.push(format!("{}{tail}", head.replace('.', "")));
    }
    // Reserved names (CON, NUL, '..') and names that must start with a letter.
    let with_suffix: Vec<String> = out.iter().map(|c| format!("{c}_")).collect();
    let with_prefix: Vec<String> = out.iter().map(|c| format!("X{c}")).collect();
    out.extend(with_suffix);
    out.extend(with_prefix);
    out.dedup();
    out
}

/// Shorten `name` until `validate` accepts it: stem first, then extension, then the whole.
fn shorten_to_fit(validate: &NameValidator<'_>, name: &str) -> Option<String> {
    let (stem, ext) = split_ext(name);
    let stem: Vec<char> = stem.chars().collect();
    let ext: Vec<char> = ext.chars().collect();
    // Full extension first; then shorter extensions (".html" on an 8.3 volume).
    for ext_keep in (2..=ext.len()).rev() {
        let tail: String = ext[..ext_keep].iter().collect();
        for keep in (1..=stem.len()).rev() {
            let head: String = stem[..keep].iter().collect();
            let candidate = format!("{}{tail}", head.trim_end_matches([' ', '.']));
            if validate(&candidate).is_ok() {
                return Some(candidate);
            }
        }
    }
    let all: Vec<char> = name.chars().collect();
    for keep in (1..all.len()).rev() {
        let candidate: String = all[..keep].iter().collect();
        let candidate = candidate.trim_end_matches([' ', '.']).to_string();
        if !candidate.is_empty() && validate(&candidate).is_ok() {
            return Some(candidate);
        }
    }
    None
}

/// Split at the last '.', keeping the dot with the extension; a leading dot is not an extension.
fn split_ext(name: &str) -> (&str, &str) {
    match name.rfind('.') {
        Some(i) if i > 0 && name.len() - i <= 6 => name.split_at(i),
        _ => (name, ""),
    }
}

/// Closest plain-ASCII spelling for the non-ASCII characters vintage names carry.
pub fn ascii_fold(c: char) -> Option<&'static str> {
    Some(match c {
        'À' | 'Á' | 'Â' | 'Ã' | 'Ä' | 'Å' => "A",
        'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'ª' => "a",
        'Ç' => "C",
        'ç' | '¢' => "c",
        'È' | 'É' | 'Ê' | 'Ë' => "E",
        'è' | 'é' | 'ê' | 'ë' => "e",
        'Ì' | 'Í' | 'Î' | 'Ï' => "I",
        'ì' | 'í' | 'î' | 'ï' | 'ı' => "i",
        'Ñ' => "N",
        'ñ' => "n",
        'Ò' | 'Ó' | 'Ô' | 'Õ' | 'Ö' | 'Ø' => "O",
        'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ø' | 'º' => "o",
        'Ù' | 'Ú' | 'Û' | 'Ü' => "U",
        'ù' | 'ú' | 'û' | 'ü' | 'µ' => "u",
        'Ÿ' => "Y",
        'ÿ' | '¥' => "y",
        'ß' => "ss",
        'Æ' => "AE",
        'æ' => "ae",
        'Œ' => "OE",
        'œ' => "oe",
        'ƒ' => "f",
        'ﬁ' => "fi",
        'ﬂ' => "fl",
        'π' => "pi",
        'Ω' => "Omega",
        '™' => "TM",
        '©' => "(c)",
        '®' => "(R)",
        '£' => "L",
        '€' => "EUR",
        '§' => "S",
        '¶' => "P",
        '°' | '˚' => "o",
        '±' => "+-",
        '≠' => "!=",
        '≤' => "<=",
        '≥' => ">=",
        '≈' | '˜' => "~",
        '÷' | '⁄' => "-",
        '•' | '·' | '–' | '—' | '†' | '‡' => "-",
        '…' => "...",
        '“' | '”' | '„' | '«' | '»' => "\"",
        '‘' | '’' | '‚' | '‹' | '›' | '´' | '`' => "'",
        '\u{A0}' => " ",
        '\u{F8FF}' => "Apple",
        '∞' => "inf",
        '√' => "v",
        '¬' => "-",
        '¿' => "?",
        '¡' => "!",
        _ => return None,
    })
}

/// DOS / Windows device names, refused as a base name on FAT-family volumes.
pub fn is_dos_device_name(name: &str) -> bool {
    let base = name.split('.').next().unwrap_or("").trim_end();
    let upper = base.to_ascii_uppercase();
    matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL" | "CLOCK$")
        || (upper.len() == 4
            && (upper.starts_with("COM") || upper.starts_with("LPT"))
            && matches!(upper.as_bytes()[3], b'1'..=b'9'))
}

/// Host folder names: the portable set everywhere, plus Win32's extra rules on Windows.
pub fn validate_host_name(name: &str) -> Result<(), FilesystemError> {
    validate_host_name_for(name, cfg!(windows))
}

fn validate_host_name_for(name: &str, windows: bool) -> Result<(), FilesystemError> {
    validate_posix_name(name, 255, "the host")?;
    if let Some(c) = name.chars().find(|c| r#"<>:"\|?*"#.contains(*c)) {
        return Err(FilesystemError::InvalidData(format!(
            "'{c}' is not allowed in a host filename"
        )));
    }
    if windows {
        // `Icon\r` is a real Finder file on macOS, so control codes only fail on Windows.
        if name.chars().any(|c| (c as u32) < 0x20) {
            return Err(FilesystemError::InvalidData(
                "Windows filenames cannot hold control characters".into(),
            ));
        }
        if name.ends_with([' ', '.']) || is_dos_device_name(name) {
            return Err(FilesystemError::InvalidData(format!(
                "'{name}' is not a usable Windows filename"
            )));
        }
    }
    Ok(())
}

/// Unix-style names: 1..=`max_bytes` UTF-8 bytes, no `/` or NUL, not `.` / `..`.
pub fn validate_posix_name(name: &str, max_bytes: usize, fs: &str) -> Result<(), FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData("name cannot be empty".into()));
    }
    if name == "." || name == ".." {
        return Err(FilesystemError::InvalidData(format!(
            "'.' and '..' are reserved on {fs}"
        )));
    }
    if name.contains('/') || name.contains('\0') {
        return Err(FilesystemError::InvalidData(format!(
            "{fs} names cannot contain '/' or NUL"
        )));
    }
    if name.len() > max_bytes {
        return Err(FilesystemError::InvalidData(format!(
            "name is too long ({} bytes); {fs} allows up to {max_bytes}",
            name.len()
        )));
    }
    Ok(())
}

/// Amiga-style names: 1..=`max_bytes` bytes, no `/`, `:` or NUL (the AmigaDOS path syntax).
pub fn validate_amiga_name(name: &str, max_bytes: usize, fs: &str) -> Result<(), FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData("name cannot be empty".into()));
    }
    if name.contains(['/', ':', '\0']) {
        return Err(FilesystemError::InvalidData(format!(
            "{fs} names cannot contain '/', ':' or NUL"
        )));
    }
    if name.len() > max_bytes {
        return Err(FilesystemError::InvalidData(format!(
            "name is too long ({} bytes); {fs} allows up to {max_bytes}",
            name.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn legal(v: &NameValidator<'_>, name: &str) -> String {
        let out = legalize_name(v, name).unwrap();
        assert!(v(&out).is_ok(), "{name:?} -> {out:?} still invalid");
        out
    }

    #[test]
    fn a_legal_name_is_untouched() {
        let v = |n: &str| validate_posix_name(n, 255, "ext");
        assert_eq!(legal(&v, "plain.txt"), "plain.txt");
        assert_eq!(legal(&v, "Icon\r"), "Icon\r");
    }

    #[test]
    fn slash_and_colon_become_underscores() {
        let v = |n: &str| validate_amiga_name(n, 30, "SFS");
        assert_eq!(legal(&v, "Schwarz/Weiss"), "Schwarz_Weiss");
        assert_eq!(legal(&v, "a:b"), "a_b");
    }

    #[test]
    fn long_names_keep_their_extension() {
        let v = |n: &str| validate_amiga_name(n, 12, "SFS");
        assert_eq!(legal(&v, "Anti-Aliased PICT.rsrc"), "Anti-Al.rsrc");
    }

    #[test]
    fn dos_device_names_are_detected() {
        assert!(is_dos_device_name("con"));
        assert!(is_dos_device_name("LPT1.txt"));
        assert!(!is_dos_device_name("CONFIG.SYS"));
        assert!(!is_dos_device_name("COM0"));
    }

    #[test]
    fn unique_name_suffixes_before_the_extension() {
        let v = |n: &str| validate_posix_name(n, 255, "ext");
        let taken = |n: &str| n == "a_b.txt" || n == "a_b_1.txt";
        assert_eq!(unique_name(&v, "a_b.txt", &taken).unwrap(), "a_b_2.txt");
        let short = |n: &str| validate_posix_name(n, 8, "ext");
        let taken = |n: &str| n == "abcdefgh";
        assert_eq!(unique_name(&short, "abcdefgh", &taken).unwrap(), "abcdef_1");
    }

    #[test]
    fn nothing_legal_reports_the_destination_reason() {
        let v = |_: &str| Err(FilesystemError::InvalidData("read-only".into()));
        assert!(legalize_name(&v, "x").is_err());
    }

    #[test]
    fn host_rules_are_stricter_on_windows() {
        assert!(validate_host_name_for("Icon\r", false).is_ok());
        assert!(validate_host_name_for("Icon\r", true).is_err());
        assert!(validate_host_name_for("Read Me.", true).is_err());
        assert!(validate_host_name_for("aux.c", true).is_err());
        assert!(validate_host_name_for("a:b", false).is_err());
    }

    /// Names seen on real classic-Mac and DOS volumes, pushed through the real validators.
    #[test]
    fn vintage_names_become_legal_on_every_real_destination() {
        use crate::fs::filesystem::Filesystem;
        use std::io::Cursor;
        let corpus = [
            "Acquire/Export",
            "Schwarz/Weiss",
            "MacDRUMS Instruments/Tracks",
            "Icon\r",
            "Read Me!",
            "R\u{e9}sum\u{e9}\u{2122} \u{192}",
            "\u{F8FF} Menu Items",
            "a:b",
            "CON",
            "lpt1.txt",
            "..",
            "   ",
            "trailing.",
            "*?<>|",
            "1st file",
            "file.tar.gz",
            "Anti-Aliased PICT",
            "An extraordinarily long name that exceeds every limit.html",
        ];
        let fat = crate::fs::fat::FatFilesystem::open(
            Cursor::new(crate::fs::fat::create_blank_fat(737280, Some("T")).unwrap()),
            0,
        )
        .unwrap();
        let hfs = crate::fs::hfs::HfsFilesystem::open(
            Cursor::new(crate::fs::hfs::create_blank_hfs(8 << 20, 4096, "T").unwrap()),
            0,
        )
        .unwrap();
        let prodos = crate::fs::prodos::ProDosFilesystem::open(
            Cursor::new(crate::fs::prodos::create_blank_prodos(143_360, "T").unwrap()),
            0,
        )
        .unwrap();
        let mfs = crate::fs::mfs::MfsFilesystem::open(
            Cursor::new(crate::fs::mfs::create_blank_mfs(409_600, "T").unwrap()),
            0,
        )
        .unwrap();
        type Check<'a> = Box<NameValidator<'a>>;
        let validators: Vec<(&str, Check<'_>)> = vec![
            ("FAT", Box::new(|n: &str| fat.validate_name(n))),
            ("HFS", Box::new(|n: &str| hfs.validate_name(n))),
            ("ProDOS", Box::new(|n: &str| prodos.validate_name(n))),
            ("MFS", Box::new(|n: &str| mfs.validate_name(n))),
            (
                "Atari DOS",
                Box::new(|n: &str| crate::fs::atari_dos::encode_name(n).map(|_| ())),
            ),
            (
                "CBM",
                Box::new(|n: &str| crate::fs::cbm::encode_petscii_name(n).map(|_| ())),
            ),
            ("HPFS", Box::new(crate::fs::hpfs::validate_hpfs_name)),
            (
                "ext",
                Box::new(|n: &str| validate_posix_name(n, 255, "ext")),
            ),
        ];
        for (fs, v) in &validators {
            for name in corpus {
                let out = legalize_name(v.as_ref(), name)
                    .unwrap_or_else(|e| panic!("{fs}: no legal name for {name:?}: {e}"));
                assert!(v(&out).is_ok(), "{fs}: {name:?} -> {out:?} still invalid");
            }
        }
        // HFS keeps slashes (legal there); FAT does not.
        let hfs_v = |n: &str| hfs.validate_name(n);
        assert_eq!(
            legalize_name(&hfs_v, "Acquire/Export").unwrap(),
            "Acquire/Export"
        );
        let fat_v = |n: &str| fat.validate_name(n);
        assert_eq!(
            legalize_name(&fat_v, "Acquire/Export").unwrap(),
            "Acquire_Export"
        );
        assert_eq!(legalize_name(&fat_v, "Icon\r").unwrap(), "Icon_");
        assert_eq!(legalize_name(&fat_v, "CON").unwrap(), "CON_");
        let prodos_v = |n: &str| prodos.validate_name(n);
        assert_eq!(legalize_name(&prodos_v, "Read Me!").unwrap(), "Read.Me.");
    }
}
