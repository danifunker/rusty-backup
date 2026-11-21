//! Shared helpers for Unix-like filesystems (ext2/3/4, btrfs, xfs, UFS, etc.).
//!
//! Provides inode mode parsing, file type detection, timestamp formatting,
//! and FileEntry construction from raw inode fields.

use crate::entry::{EntryType, FileEntry};

// ---- Unix mode bit masks ----

const S_IFMT: u32 = 0o170000;
const S_IFSOCK: u32 = 0o140000;
const S_IFLNK: u32 = 0o120000;
const S_IFREG: u32 = 0o100000;
const S_IFBLK: u32 = 0o060000;
const S_IFDIR: u32 = 0o040000;
const S_IFCHR: u32 = 0o020000;
const S_IFIFO: u32 = 0o010000;

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
pub use crate::display::unix_mode_string;

/// Build a `FileEntry` from raw Unix inode fields.
///
/// The caller is responsible for filling `symlink_target` afterwards for symlinks
/// (since reading the link target requires filesystem I/O).
#[allow(clippy::too_many_arguments)] // FileEntry fields all need to come in from the caller
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
    // Numeric twin of `modified` for tar_export / cross-image copies.
    let modified_unix: Option<u64> = if mtime > 0 { Some(mtime as u64) } else { None };

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
        entry_type,
        modified,
        modified_unix,
        special_type,
        mode: Some(mode),
        uid: Some(uid),
        gid: Some(gid),
        ..FileEntry::new_file(name.to_string(), path.to_string(), display_size, inode_num)
    }
}

/// Check that a POSIX id fits the width its on-disk inode field has,
/// refusing rather than truncating.
///
/// The narrow fields are real: EFS and Minix V2/V3 store 16-bit uid and
/// gid, and Minix V1 stores gid in a single byte. A silently wrapped id
/// is far worse than a rejected one — uid 65536 on a 16-bit field
/// becomes 0, which is root. `what` names the field for the error
/// ("uid" / "gid").
pub fn check_id_width(
    value: u32,
    bits: u32,
    what: &str,
) -> Result<u32, crate::filesystem::FilesystemError> {
    let max = if bits >= 32 {
        u32::MAX
    } else {
        (1u32 << bits) - 1
    };
    if value > max {
        return Err(crate::filesystem::FilesystemError::InvalidData(format!(
            "{what} {value} does not fit this filesystem's {bits}-bit {what} field (max {max})"
        )));
    }
    Ok(value)
}

/// Replace the permission bits of `mode`, keeping its file-type bits.
///
/// Every `set_permissions` implementation needs this: the caller passes
/// `0o7777` permission bits, and writing them straight into the inode
/// would clear `S_IFREG` / `S_IFDIR` and leave a typeless inode behind.
pub fn with_permission_bits(current_mode: u32, new_perms: u32) -> u32 {
    (current_mode & S_IFMT) | (new_perms & 0o7777)
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
    let (year, m, d, hour, minute, second) = crate::times::civil_from_unix(epoch as u64);
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
