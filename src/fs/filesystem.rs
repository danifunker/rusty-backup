use std::fmt;
use std::io::Write;

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
    /// Default delegates to `read_file(entry, usize::MAX)`.
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
        }
    }
}

impl std::error::Error for FilesystemError {}

impl From<std::io::Error> for FilesystemError {
    fn from(e: std::io::Error) -> Self {
        FilesystemError::Io(e)
    }
}
