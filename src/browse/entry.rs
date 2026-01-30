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
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EntryType {
    File,
    Directory,
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
        }
    }

    pub fn is_directory(&self) -> bool {
        self.entry_type == EntryType::Directory
    }

    pub fn is_file(&self) -> bool {
        self.entry_type == EntryType::File
    }

    pub fn size_string(&self) -> String {
        if self.is_directory() {
            return String::new();
        }
        crate::partition::format_size(self.size)
    }
}
