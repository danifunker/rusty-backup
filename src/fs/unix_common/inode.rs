//! Shared helpers for Unix-like filesystems (ext2/3/4, btrfs, xfs, UFS, etc.).
//!
//! Provides inode mode parsing, file type detection, timestamp formatting,
//! and FileEntry construction from raw inode fields.

use crate::fs::entry::{EntryType, FileEntry};

// ---- Unix mode bit masks ----

const S_IFMT: u32 = 0o170000;
const S_IFSOCK: u32 = 0o140000;
const S_IFLNK: u32 = 0o120000;
const S_IFREG: u32 = 0o100000;
const S_IFBLK: u32 = 0o060000;
const S_IFDIR: u32 = 0o040000;
const S_IFCHR: u32 = 0o020000;
const S_IFIFO: u32 = 0o010000;

const S_ISUID: u32 = 0o4000;
const S_ISGID: u32 = 0o2000;
const S_ISVTX: u32 = 0o1000;

/// Unix file type extracted from mode bits.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UnixFileType {
    Regular,
    Directory,
    Symlink,
    BlockDevice,
    CharDevice,
    Fifo,
    Socket,
    Unknown,
}

/// Extract the file type from raw Unix mode bits.
pub fn unix_file_type(mode: u32) -> UnixFileType {
    match mode & S_IFMT {
        S_IFREG => UnixFileType::Regular,
        S_IFDIR => UnixFileType::Directory,
        S_IFLNK => UnixFileType::Symlink,
        S_IFBLK => UnixFileType::BlockDevice,
        S_IFCHR => UnixFileType::CharDevice,
        S_IFIFO => UnixFileType::Fifo,
        S_IFSOCK => UnixFileType::Socket,
        _ => UnixFileType::Unknown,
    }
}

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

    let mut result = String::with_capacity(10);
    result.push(file_type_char);
    for c in &chars {
        result.push(*c);
    }
    result
}

/// Build a `FileEntry` from raw Unix inode fields.
///
/// The caller is responsible for filling `symlink_target` afterwards for symlinks
/// (since reading the link target requires filesystem I/O).
pub fn unix_entry_from_inode(
    name: &str,
    path: &str,
    mode: u32,
    size: u64,
    inode_num: u64,
    mtime: i64,
    uid: u32,
    gid: u32,
) -> FileEntry {
    let ft = unix_file_type(mode);
    let modified = if mtime != 0 {
        Some(format_unix_timestamp(mtime))
    } else {
        None
    };

    let (entry_type, special_type) = match ft {
        UnixFileType::Regular | UnixFileType::Unknown => (EntryType::File, None),
        UnixFileType::Directory => (EntryType::Directory, None),
        UnixFileType::Symlink => (EntryType::Symlink, None),
        UnixFileType::BlockDevice => (EntryType::Special, Some("block device".to_string())),
        UnixFileType::CharDevice => (EntryType::Special, Some("char device".to_string())),
        UnixFileType::Fifo => (EntryType::Special, Some("fifo".to_string())),
        UnixFileType::Socket => (EntryType::Special, Some("socket".to_string())),
    };

    let display_size = match entry_type {
        EntryType::Special | EntryType::Directory => 0,
        _ => size,
    };

    FileEntry {
        name: name.to_string(),
        path: path.to_string(),
        entry_type,
        size: display_size,
        location: inode_num,
        modified,
        type_code: None,
        creator_code: None,
        symlink_target: None,
        special_type,
        mode: Some(mode),
        uid: Some(uid),
        gid: Some(gid),
        resource_fork_size: None,
    }
}

/// Extract major and minor device numbers from a raw Linux device number.
///
/// Linux encodes devices as: `major = (dev >> 8) & 0xFFF`, `minor = (dev & 0xFF) | ((dev >> 12) & 0xFFF00)`.
/// For simple cases (major < 256, minor < 256): `major = (dev >> 8) & 0xFF`, `minor = dev & 0xFF`.
pub fn device_major_minor(dev: u32) -> (u32, u32) {
    let major = (dev >> 8) & 0xFFF;
    let minor = (dev & 0xFF) | ((dev >> 12) & 0xFFF00);
    (major, minor)
}

/// Convert Unix epoch seconds to a human-readable `YYYY-MM-DD HH:MM:SS` string.
pub fn format_unix_timestamp(epoch: i64) -> String {
    if epoch < 0 {
        return "1970-01-01 00:00:00".to_string();
    }
    let secs = epoch as u64;

    let sec_of_day = secs % 86400;
    let hour = sec_of_day / 3600;
    let minute = (sec_of_day % 3600) / 60;
    let second = sec_of_day % 60;

    // Calculate date from day count since 1970-01-01
    let mut days = (secs / 86400) as i64;

    // Algorithm adapted from Howard Hinnant's civil_from_days
    // http://howardhinnant.github.io/date_algorithms.html
    days += 719468; // shift epoch from 1970-01-01 to 0000-03-01
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u32; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // year of era [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // month prime [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // day [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // month [1, 12]
    let year = if m <= 2 { y + 1 } else { y };

    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        year, m, d, hour, minute, second
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mode_string_regular_file() {
        assert_eq!(unix_mode_string(0o100644), "-rw-r--r--");
    }

    #[test]
    fn test_mode_string_executable() {
        assert_eq!(unix_mode_string(0o100755), "-rwxr-xr-x");
    }

    #[test]
    fn test_mode_string_directory() {
        assert_eq!(unix_mode_string(0o040755), "drwxr-xr-x");
    }

    #[test]
    fn test_mode_string_symlink() {
        assert_eq!(unix_mode_string(0o120777), "lrwxrwxrwx");
    }

    #[test]
    fn test_mode_string_setuid() {
        assert_eq!(unix_mode_string(0o104755), "-rwsr-xr-x");
    }

    #[test]
    fn test_mode_string_setuid_no_exec() {
        assert_eq!(unix_mode_string(0o104644), "-rwSr--r--");
    }

    #[test]
    fn test_mode_string_setgid() {
        assert_eq!(unix_mode_string(0o102755), "-rwxr-sr-x");
    }

    #[test]
    fn test_mode_string_setgid_no_exec() {
        assert_eq!(unix_mode_string(0o102644), "-rw-r-Sr--");
    }

    #[test]
    fn test_mode_string_sticky() {
        assert_eq!(unix_mode_string(0o041755), "drwxr-xr-t");
    }

    #[test]
    fn test_mode_string_sticky_no_exec() {
        assert_eq!(unix_mode_string(0o041754), "drwxr-xr-T");
    }

    #[test]
    fn test_mode_string_block_device() {
        assert_eq!(unix_mode_string(0o060660), "brw-rw----");
    }

    #[test]
    fn test_mode_string_char_device() {
        assert_eq!(unix_mode_string(0o020666), "crw-rw-rw-");
    }

    #[test]
    fn test_mode_string_fifo() {
        assert_eq!(unix_mode_string(0o010644), "prw-r--r--");
    }

    #[test]
    fn test_mode_string_socket() {
        assert_eq!(unix_mode_string(0o140755), "srwxr-xr-x");
    }

    #[test]
    fn test_mode_string_no_permissions() {
        assert_eq!(unix_mode_string(0o100000), "----------");
    }

    #[test]
    fn test_mode_string_all_permissions() {
        assert_eq!(unix_mode_string(0o100777), "-rwxrwxrwx");
    }

    #[test]
    fn test_unix_file_type() {
        assert_eq!(unix_file_type(0o100644), UnixFileType::Regular);
        assert_eq!(unix_file_type(0o040755), UnixFileType::Directory);
        assert_eq!(unix_file_type(0o120777), UnixFileType::Symlink);
        assert_eq!(unix_file_type(0o060660), UnixFileType::BlockDevice);
        assert_eq!(unix_file_type(0o020666), UnixFileType::CharDevice);
        assert_eq!(unix_file_type(0o010644), UnixFileType::Fifo);
        assert_eq!(unix_file_type(0o140755), UnixFileType::Socket);
        assert_eq!(unix_file_type(0o000644), UnixFileType::Unknown);
    }

    #[test]
    fn test_format_timestamp_epoch() {
        assert_eq!(format_unix_timestamp(0), "1970-01-01 00:00:00");
    }

    #[test]
    fn test_format_timestamp_known_date() {
        // 2023-11-14 22:13:20 UTC = 1700000000
        assert_eq!(format_unix_timestamp(1700000000), "2023-11-14 22:13:20");
    }

    #[test]
    fn test_format_timestamp_y2k() {
        // 2000-01-01 00:00:00 UTC = 946684800
        assert_eq!(format_unix_timestamp(946684800), "2000-01-01 00:00:00");
    }

    #[test]
    fn test_format_timestamp_negative() {
        assert_eq!(format_unix_timestamp(-1), "1970-01-01 00:00:00");
    }

    #[test]
    fn test_format_timestamp_2038() {
        // 2038-01-19 03:14:07 UTC = 2147483647 (max i32)
        assert_eq!(format_unix_timestamp(2147483647), "2038-01-19 03:14:07");
    }

    #[test]
    fn test_format_timestamp_1971() {
        // 1971-01-01 00:00:00 UTC = 31536000
        assert_eq!(format_unix_timestamp(31536000), "1971-01-01 00:00:00");
    }

    #[test]
    fn test_device_major_minor() {
        // /dev/sda = major 8, minor 0 → encoded as 0x0800
        assert_eq!(device_major_minor(0x0800), (8, 0));
        // /dev/null = major 1, minor 3 → encoded as 0x0103
        assert_eq!(device_major_minor(0x0103), (1, 3));
    }

    #[test]
    fn test_unix_entry_from_inode_regular() {
        let fe = unix_entry_from_inode(
            "hello.txt",
            "/hello.txt",
            0o100644,
            42,
            100,
            1700000000,
            1000,
            1000,
        );
        assert_eq!(fe.entry_type, EntryType::File);
        assert_eq!(fe.size, 42);
        assert_eq!(fe.location, 100);
        assert_eq!(fe.mode, Some(0o100644));
        assert_eq!(fe.uid, Some(1000));
        assert_eq!(fe.gid, Some(1000));
        assert!(fe.modified.is_some());
        assert!(fe.symlink_target.is_none());
        assert!(fe.special_type.is_none());
    }

    #[test]
    fn test_unix_entry_from_inode_directory() {
        let fe = unix_entry_from_inode("subdir", "/subdir", 0o040755, 4096, 200, 1700000000, 0, 0);
        assert_eq!(fe.entry_type, EntryType::Directory);
        assert_eq!(fe.size, 0); // directories show 0 size
    }

    #[test]
    fn test_unix_entry_from_inode_symlink() {
        let fe = unix_entry_from_inode("link", "/link", 0o120777, 10, 300, 1700000000, 0, 0);
        assert_eq!(fe.entry_type, EntryType::Symlink);
        assert!(fe.symlink_target.is_none()); // caller fills this later
    }

    #[test]
    fn test_unix_entry_from_inode_block_device() {
        let fe = unix_entry_from_inode("sda", "/dev/sda", 0o060660, 0, 400, 0, 0, 6);
        assert_eq!(fe.entry_type, EntryType::Special);
        assert_eq!(fe.special_type.as_deref(), Some("block device"));
    }

    #[test]
    fn test_unix_entry_from_inode_char_device() {
        let fe = unix_entry_from_inode("null", "/dev/null", 0o020666, 0, 401, 0, 0, 0);
        assert_eq!(fe.entry_type, EntryType::Special);
        assert_eq!(fe.special_type.as_deref(), Some("char device"));
    }

    #[test]
    fn test_unix_entry_from_inode_fifo() {
        let fe = unix_entry_from_inode("pipe", "/tmp/pipe", 0o010644, 0, 500, 0, 1000, 1000);
        assert_eq!(fe.entry_type, EntryType::Special);
        assert_eq!(fe.special_type.as_deref(), Some("fifo"));
    }

    #[test]
    fn test_unix_entry_from_inode_socket() {
        let fe = unix_entry_from_inode("sock", "/tmp/sock", 0o140755, 0, 600, 0, 0, 0);
        assert_eq!(fe.entry_type, EntryType::Special);
        assert_eq!(fe.special_type.as_deref(), Some("socket"));
    }
}
