/// A file or directory entry within a partition's filesystem.
#[derive(Debug, Clone)]
pub struct FileEntry {
    pub name: String,
    pub path: String,
    pub entry_type: EntryType,
    pub size: u64,
    /// Starting cluster (FAT) or inode/extent for other filesystems.
    pub location: u64,
    /// Human-readable modification date string.
    pub modified: Option<String>,
    /// HFS/HFS+ file type code (e.g. "TEXT", "PICT"). Four ASCII characters.
    pub type_code: Option<String>,
    /// HFS/HFS+ creator code (e.g. "MSWD", "ttxt"). Four ASCII characters.
    pub creator_code: Option<String>,
    /// Symlink target path (only set for `EntryType::Symlink`).
    pub symlink_target: Option<String>,
    /// Special file type description (e.g. "block device", "char device", "socket", "fifo").
    /// Only set for `EntryType::Special`.
    pub special_type: Option<String>,
    /// Raw Unix mode bits (e.g. 0o100755). Present on ext/btrfs/xfs/UFS filesystems.
    pub mode: Option<u32>,
    /// Unix user ID. Present on ext/btrfs/xfs/UFS filesystems.
    pub uid: Option<u32>,
    /// Unix group ID. Present on ext/btrfs/xfs/UFS filesystems.
    pub gid: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EntryType {
    File,
    Directory,
    /// Symbolic link. The target path is stored in `FileEntry::symlink_target`.
    Symlink,
    /// Special file: block device, char device, socket, or FIFO.
    /// The kind is stored in `FileEntry::special_type`.
    Special,
}

impl FileEntry {
    pub fn root() -> Self {
        Self {
            name: "/".into(),
            path: "/".into(),
            entry_type: EntryType::Directory,
            size: 0,
            location: 0,
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target: None,
            special_type: None,
            mode: None,
            uid: None,
            gid: None,
        }
    }

    pub fn new_directory(name: String, path: String, location: u64) -> Self {
        Self {
            name,
            path,
            entry_type: EntryType::Directory,
            size: 0,
            location,
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target: None,
            special_type: None,
            mode: None,
            uid: None,
            gid: None,
        }
    }

    pub fn new_file(name: String, path: String, size: u64, location: u64) -> Self {
        Self {
            name,
            path,
            entry_type: EntryType::File,
            size,
            location,
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target: None,
            special_type: None,
            mode: None,
            uid: None,
            gid: None,
        }
    }

    pub fn new_symlink(
        name: String,
        path: String,
        size: u64,
        location: u64,
        target: String,
    ) -> Self {
        Self {
            name,
            path,
            entry_type: EntryType::Symlink,
            size,
            location,
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target: Some(target),
            special_type: None,
            mode: None,
            uid: None,
            gid: None,
        }
    }

    pub fn new_special(
        name: String,
        path: String,
        location: u64,
        special_type_str: String,
    ) -> Self {
        Self {
            name,
            path,
            entry_type: EntryType::Special,
            size: 0,
            location,
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target: None,
            special_type: Some(special_type_str),
            mode: None,
            uid: None,
            gid: None,
        }
    }

    pub fn is_directory(&self) -> bool {
        self.entry_type == EntryType::Directory
    }

    pub fn is_file(&self) -> bool {
        self.entry_type == EntryType::File
    }

    pub fn is_symlink(&self) -> bool {
        self.entry_type == EntryType::Symlink
    }

    pub fn is_special(&self) -> bool {
        self.entry_type == EntryType::Special
    }

    /// Returns a human-readable Unix permission string (e.g. "drwxr-xr-x"),
    /// or `None` if mode bits are not available (non-Unix filesystems).
    pub fn mode_string(&self) -> Option<String> {
        self.mode
            .map(crate::fs::unix_common::inode::unix_mode_string)
    }

    pub fn size_string(&self) -> String {
        if self.is_directory() || self.is_special() {
            return String::new();
        }
        crate::partition::format_size(self.size)
    }
}
