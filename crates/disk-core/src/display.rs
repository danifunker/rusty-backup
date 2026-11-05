//! Small, filesystem-neutral formatters used when showing entries to a person.

/// Format a byte count as a short human-readable size (`512 B`, `1.5 MiB`, …).
pub fn format_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    const TIB: u64 = 1024 * GIB;

    if bytes >= TIB {
        format!("{:.1} TiB", bytes as f64 / TIB as f64)
    } else if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Render a Mac four-character `OSType` lossily: non-printable bytes become `.`.
///
/// Display-only (e.g. `PoƒP` = `50 6f C4 50` shows as `Po.P`); carry the raw
/// `[u8; 4]` wherever fidelity matters.
pub fn decode_ostype(code: &[u8; 4]) -> String {
    code.iter()
        .map(|&b| {
            if b.is_ascii_graphic() || b == b' ' {
                b as char
            } else {
                '.'
            }
        })
        .collect()
}

const S_IFMT: u32 = 0o170000;
const S_IFSOCK: u32 = 0o140000;
const S_IFLNK: u32 = 0o120000;
const S_IFBLK: u32 = 0o060000;
const S_IFDIR: u32 = 0o040000;
const S_IFCHR: u32 = 0o020000;
const S_IFIFO: u32 = 0o010000;
const S_ISUID: u32 = 0o4000;
const S_ISGID: u32 = 0o2000;
const S_ISVTX: u32 = 0o1000;

/// Render raw Unix mode bits as a 10-character permission string (e.g. `drwxr-xr-x`).
pub fn unix_mode_string(mode: u32) -> String {
    let file_type_char = match mode & S_IFMT {
        S_IFDIR => 'd',
        S_IFLNK => 'l',
        S_IFBLK => 'b',
        S_IFCHR => 'c',
        S_IFIFO => 'p',
        S_IFSOCK => 's',
        _ => '-',
    };
    let mut chars = ['-'; 9];
    // Owner
    if mode & 0o400 != 0 {
        chars[0] = 'r';
    }
    if mode & 0o200 != 0 {
        chars[1] = 'w';
    }
    if mode & 0o100 != 0 {
        chars[2] = if mode & S_ISUID != 0 { 's' } else { 'x' };
    } else if mode & S_ISUID != 0 {
        chars[2] = 'S';
    }
    // Group
    if mode & 0o040 != 0 {
        chars[3] = 'r';
    }
    if mode & 0o020 != 0 {
        chars[4] = 'w';
    }
    if mode & 0o010 != 0 {
        chars[5] = if mode & S_ISGID != 0 { 's' } else { 'x' };
    } else if mode & S_ISGID != 0 {
        chars[5] = 'S';
    }
    // Other
    if mode & 0o004 != 0 {
        chars[6] = 'r';
    }
    if mode & 0o002 != 0 {
        chars[7] = 'w';
    }
    if mode & 0o001 != 0 {
        chars[8] = if mode & S_ISVTX != 0 { 't' } else { 'x' };
    } else if mode & S_ISVTX != 0 {
        chars[8] = 'T';
    }
    let mut s = String::with_capacity(10);
    s.push(file_type_char);
    s.extend(chars.iter());
    s
}
