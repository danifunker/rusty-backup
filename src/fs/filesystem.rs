use std::fmt;
use std::io::Write;
use std::path::PathBuf;

use super::entry::FileEntry;

/// Trait for browsing a filesystem within a partition.
pub trait Filesystem: Send {
    /// Get the root directory entry.
    fn root(&mut self) -> Result<FileEntry, FilesystemError>;

    /// List the contents of a directory.
    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError>;

    /// Read file contents (up to `max_bytes`).
    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError>;

    /// Volume label, if available.
    fn volume_label(&self) -> Option<&str>;

    /// Filesystem type name (e.g., "FAT16", "FAT32").
    fn fs_type(&self) -> &str;

    /// Total filesystem size in bytes.
    fn total_size(&self) -> u64;

    /// Used space in bytes.
    fn used_size(&self) -> u64;

    /// Returns the minimum number of bytes from the partition start needed to
    /// capture all filesystem data. Used for smart backup trimming.
    ///
    /// Default implementation returns `total_size()` (no trimming).
    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.total_size())
    }

    /// Stream file data to a writer. Returns the number of bytes written.
    ///
    /// Default loads the entire file into RAM via `read_file(entry,
    /// usize::MAX)` and then writes it. This is acceptable for vintage
    /// disk images (typical files <100 MiB) but is not true streaming.
    /// TODO: when a chunked `read_file_at(entry, offset, len)` API is
    /// added, override this per-FS to stream extent-by-extent and avoid
    /// the full-file allocation.
    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let data = self.read_file(entry, usize::MAX)?;
        writer.write_all(&data)?;
        Ok(data.len() as u64)
    }

    /// Write resource fork data to a writer. Returns the number of bytes written.
    /// Default returns `Ok(0)` (no resource fork).
    fn write_resource_fork_to(
        &mut self,
        _entry: &FileEntry,
        _writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        Ok(0)
    }

    /// Returns the resource fork size for a file entry. Default returns `0`.
    fn resource_fork_size(&mut self, _entry: &FileEntry) -> u64 {
        0
    }

    /// Returns the CNID and name of the blessed (bootable) system folder, if set.
    /// Only meaningful for HFS/HFS+.
    fn blessed_system_folder(&mut self) -> Option<(u64, String)> {
        None
    }

    /// Run filesystem integrity check. Returns `None` if not supported for
    /// this filesystem type. Override in implementations that support fsck.
    fn fsck(&mut self) -> Option<Result<super::fsck::FsckResult, FilesystemError>> {
        None
    }

    /// Validate that `name` is legal for a new file or directory on this
    /// filesystem. Returns `Err(InvalidData)` with a human-readable reason
    /// when the name violates length, character, or encoding rules.
    ///
    /// Default implementation accepts any non-empty name; filesystems that
    /// support editing should override this with their actual rules so that
    /// GUI callers can validate at staging time rather than at apply time.
    fn validate_name(&self, name: &str) -> Result<(), FilesystemError> {
        if name.is_empty() {
            return Err(FilesystemError::InvalidData("name cannot be empty".into()));
        }
        Ok(())
    }
}

/// Errors from filesystem operations.
#[derive(Debug)]
pub enum FilesystemError {
    Io(std::io::Error),
    NotADirectory(String),
    NotFound(String),
    Parse(String),
    Unsupported(String),
    InvalidData(String),
    AlreadyExists(String),
    DiskFull(String),
}

impl fmt::Display for FilesystemError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilesystemError::Io(e) => write!(f, "I/O error: {e}"),
            FilesystemError::NotADirectory(p) => write!(f, "not a directory: {p}"),
            FilesystemError::NotFound(p) => write!(f, "not found: {p}"),
            FilesystemError::Parse(msg) => write!(f, "parse error: {msg}"),
            FilesystemError::Unsupported(msg) => write!(f, "unsupported: {msg}"),
            FilesystemError::InvalidData(msg) => write!(f, "invalid data: {msg}"),
            FilesystemError::AlreadyExists(p) => write!(f, "already exists: {p}"),
            FilesystemError::DiskFull(msg) => write!(f, "disk full: {msg}"),
        }
    }
}

impl std::error::Error for FilesystemError {}

impl From<std::io::Error> for FilesystemError {
    fn from(e: std::io::Error) -> Self {
        FilesystemError::Io(e)
    }
}

/// Options for creating a file on an editable filesystem.
#[derive(Debug, Clone, Default)]
pub struct CreateFileOptions {
    /// Unix mode bits (default 0o100666). Ignored on FAT/exFAT.
    pub mode: Option<u32>,
    /// Unix user ID (default 0). Ignored on FAT/exFAT/NTFS.
    pub uid: Option<u32>,
    /// Unix group ID (default 0). Ignored on FAT/exFAT/NTFS.
    pub gid: Option<u32>,
    /// HFS/HFS+ type code (e.g. "TEXT") or ProDOS type as "$XX" (e.g. "$04").
    /// Auto-detected from extension if not set.
    pub type_code: Option<String>,
    /// HFS/HFS+ creator code (e.g. "MSWD"). Auto-detected from extension if not set.
    pub creator_code: Option<String>,
    /// ProDOS auxiliary type (16-bit). Semantics depend on the file type:
    /// $0801 for Applesoft BASIC load address, $2000 for typical BIN,
    /// record length for random-access TXT, etc. Auto-detected from
    /// extension if not set. Ignored on non-ProDOS filesystems.
    pub aux_type: Option<u16>,
    /// Optional resource fork data source (HFS/HFS+ only).
    pub resource_fork: Option<ResourceForkSource>,
}

/// Options for creating a directory on an editable filesystem.
#[derive(Debug, Clone, Default)]
pub struct CreateDirectoryOptions {
    /// Unix mode bits (default 0o40777). Ignored on FAT/exFAT.
    pub mode: Option<u32>,
    /// Unix user ID (default 0). Ignored on FAT/exFAT/NTFS.
    pub uid: Option<u32>,
    /// Unix group ID (default 0). Ignored on FAT/exFAT/NTFS.
    pub gid: Option<u32>,
}

/// Source for resource fork data (HFS/HFS+ only).
#[derive(Debug, Clone)]
pub enum ResourceForkSource {
    /// Read resource fork from a file on the host.
    File(PathBuf),
    /// Resource fork data provided directly.
    Data(Vec<u8>),
}

/// Trait for filesystems that support write operations (add/delete files and folders).
///
/// Individual mutation methods (`create_file`, `delete_entry`, etc.) modify in-memory
/// state only. The caller MUST call `sync_metadata()` after all mutations are complete
/// to flush changes to disk. This enables batching multiple edits into a single atomic
/// write.
pub trait EditableFilesystem: Filesystem {
    /// Create a file in the given parent directory.
    ///
    /// `data` is a reader providing the file contents; `data_len` is the total size.
    /// Returns the new file's entry.
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError>;

    /// Create a subdirectory in the given parent directory.
    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError>;

    /// Delete a file, empty directory, or symlink.
    ///
    /// Returns an error if the entry is a non-empty directory.
    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError>;

    /// Recursively delete a directory and all its contents.
    ///
    /// Default implementation lists children, recurses, then calls `delete_entry`.
    fn delete_recursive(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if entry.is_directory() {
            let children = self.list_directory(entry)?;
            for child in &children {
                if child.is_directory() {
                    self.delete_recursive(entry, child)?;
                } else {
                    self.delete_entry(entry, child)?;
                }
            }
        }
        self.delete_entry(parent, entry)
    }

    /// Set Unix permission bits on an entry. Override on Unix-style
    /// filesystems (ext); other filesystems return `Unsupported` so callers
    /// don't silently lose mode information.
    fn set_permissions(&mut self, _entry: &FileEntry, _mode: u32) -> Result<(), FilesystemError> {
        Err(FilesystemError::Unsupported(
            "set_permissions not supported for this filesystem".into(),
        ))
    }

    /// Set HFS/HFS+ type and creator codes. Override on HFS/HFS+; other
    /// filesystems return `Unsupported` so callers can branch on capability
    /// rather than relying on silent no-ops.
    fn set_type_creator(
        &mut self,
        _entry: &FileEntry,
        _type_code: &str,
        _creator_code: &str,
    ) -> Result<(), FilesystemError> {
        Err(FilesystemError::Unsupported(
            "set_type_creator not supported for this filesystem".into(),
        ))
    }

    /// Set ProDOS file type byte and aux type on an existing file.
    /// Default returns `Unsupported` — override on ProDOS.
    fn set_prodos_type(
        &mut self,
        _entry: &FileEntry,
        _type_byte: u8,
        _aux_type: u16,
    ) -> Result<(), FilesystemError> {
        Err(FilesystemError::Unsupported(
            "set_prodos_type not supported for this filesystem".into(),
        ))
    }

    /// Write resource fork data. Override on HFS/HFS+; other filesystems
    /// return `Unsupported` rather than silently dropping the fork.
    fn write_resource_fork(
        &mut self,
        _entry: &FileEntry,
        _data: &mut dyn std::io::Read,
        _len: u64,
    ) -> Result<(), FilesystemError> {
        Err(FilesystemError::Unsupported(
            "write_resource_fork not supported for this filesystem".into(),
        ))
    }

    /// Attempt to repair filesystem issues found by fsck.
    /// Default returns an error indicating repair is not supported.
    fn repair(&mut self) -> Result<super::fsck::RepairReport, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "repair not supported for this filesystem".into(),
        ))
    }

    /// Flush metadata (superblock, bitmaps, FAT tables, etc.) to disk.
    fn sync_metadata(&mut self) -> Result<(), FilesystemError>;

    /// Returns the number of free bytes available on the filesystem.
    fn free_space(&mut self) -> Result<u64, FilesystemError>;

    /// Mark a folder as the blessed (bootable) system folder. Override on
    /// HFS/HFS+; other filesystems return `Unsupported` so the GUI can
    /// gate the action and surface unexpected calls.
    fn set_blessed_folder(&mut self, _entry: &FileEntry) -> Result<(), FilesystemError> {
        Err(FilesystemError::Unsupported(
            "set_blessed_folder not supported for this filesystem".into(),
        ))
    }
}
