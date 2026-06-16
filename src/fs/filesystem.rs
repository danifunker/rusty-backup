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

    /// Allocation unit (cluster / block) size in bytes, when the
    /// filesystem has a single fixed one. The cross-image copy preflight
    /// uses this to project real on-disk consumption — many small files
    /// each round up to one unit, so summing raw byte sizes badly
    /// underestimates usage on large-cluster volumes. `None` means
    /// "unknown"; the copy engine then falls back to a 512-byte floor.
    fn allocation_unit(&self) -> Option<u64> {
        None
    }

    /// Returns the minimum number of bytes from the partition start needed to
    /// capture all filesystem data **without moving any data on disk** — i.e.
    /// the position of the last allocated byte. Used for smart backup trimming.
    ///
    /// Default implementation returns `total_size()` (no trimming).
    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.total_size())
    }

    /// Returns the minimum partition size that could host all live filesystem
    /// data **after a defragmenting clone** (data packed from offset 0).
    ///
    /// For most filesystems this is identical to `last_data_byte()` — there is
    /// no clone-shrink path. HFS+ overrides this because a fragmented volume's
    /// in-place trim point hugs the partition tail even when most blocks are
    /// free. See `docs/hfsplus_enhancements.md` Phase 1.
    ///
    /// Callers should treat the value as an approximate target: the actual
    /// clone may need a small additional margin for fresh B-tree headers and
    /// alignment.
    fn defragmented_minimum_size(&mut self) -> Result<u64, FilesystemError> {
        self.last_data_byte()
    }

    /// Returns per-fork fragmentation statistics for this volume, or `None`
    /// when the filesystem can't compute them (e.g. cluster-allocation
    /// filesystems whose CompactReader already repacks during the backup
    /// stream so "fragmentation" doesn't translate into a backup-time
    /// decision the user can make).
    ///
    /// Only HFS+/HFS implement this today; FAT/NTFS/exFAT return `None`
    /// because the packing CompactReader inherently emits a defragmented
    /// layout for them, and the UI doesn't surface a defrag toggle on
    /// those rows.
    fn fragmentation_stats(&mut self) -> Option<Result<FragmentationStats, FilesystemError>> {
        None
    }

    /// Stream file data to a writer. Returns the number of bytes written.
    ///
    /// All built-in filesystems (FAT/exFAT/NTFS/ext/btrfs/HFS/HFS+/ProDOS)
    /// override this with an extent-by-extent implementation that doesn't
    /// allocate the full file. The default falls back to loading the entire
    /// file into RAM via `read_file(entry, usize::MAX)`; new filesystems
    /// should override unless their files are bounded by design.
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

    /// Parse and summarize the volume's journal, if it has one. Returns
    /// `Ok(None)` when the filesystem has no journal or is not journaled.
    /// Only HFS+/HFSX override this. See `docs/hfsplus_enhancements.md`
    /// Phase 9, Step 29.
    fn read_journal(
        &mut self,
    ) -> Result<Option<super::hfsplus_journal::JournalState>, FilesystemError> {
        Ok(None)
    }

    /// Decode the journal into per-transaction detail for the GUI history
    /// viewer. `Ok(None)` when not journaled / not applicable.
    fn journal_detail(
        &mut self,
    ) -> Result<Option<super::hfsplus_journal::JournalDetail>, FilesystemError> {
        Ok(None)
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

/// Aggregate fragmentation counts for a volume's user data forks.
///
/// `files_with_data` counts forks that have at least one allocated extent;
/// `fragmented_files` counts forks whose data spans more than one extent
/// (including the extents-overflow B-tree). The UI derives a percentage
/// from these two counts.
#[derive(Debug, Clone, Copy, Default)]
pub struct FragmentationStats {
    pub files_with_data: u64,
    pub fragmented_files: u64,
}

impl FragmentationStats {
    pub fn percent(&self) -> Option<f32> {
        if self.files_with_data == 0 {
            return None;
        }
        Some(100.0 * self.fragmented_files as f32 / self.files_with_data as f32)
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
    XattrTooLarge(String),
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
            FilesystemError::XattrTooLarge(msg) => write!(f, "extended attribute too large: {msg}"),
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
    /// AmigaDOS protection bits (`access` word) for AFFS/FFS. The
    /// on-disk default of 0 displays as `----rwed`. Ignored on
    /// non-Amiga filesystems.
    pub amiga_protection: Option<u32>,
    /// AmigaDOS filenote (comment, up to 79 bytes). Ignored on
    /// non-Amiga filesystems.
    pub amiga_comment: Option<String>,
    /// AmigaDOS raw datestamp triple `(days, minutes, ticks)` since
    /// 1978-01-01. When set, written verbatim into the on-disk entry so
    /// dates round-trip through clone/restore. Ignored on non-Amiga
    /// filesystems. Default `None` leaves the date zero (which display
    /// tools render as the epoch).
    pub amiga_dates: Option<(i32, i32, i32)>,
    /// When true, allocate clusters and create the directory entry but
    /// skip writing file content.  The allocated clusters are left
    /// uninitialized (zero on fresh images).  Used by the GHO
    /// file-aware virtual-FAT builder to avoid writing 1 GB+ of zeros
    /// that are immediately evicted.
    pub skip_data_write: bool,
    /// When true, skip the duplicate-name check and SFN collision scan
    /// before creating the file.  The caller guarantees uniqueness
    /// (e.g. entries come from a pre-deduplicated tree on a fresh FS).
    /// Eliminates the O(n) parent-directory read per file.
    pub skip_name_checks: bool,
    /// When true, skip the per-operation FSInfo update (FAT32).
    /// Caller must call `sync_metadata` when done to write a correct
    /// FSInfo sector.  Eliminates two full FAT-table reads per file.
    pub skip_fsinfo_update: bool,
    /// Standard DOS attribute bits to stamp on the new file (read-only
    /// `0x01`, hidden `0x02`, system `0x04`, archive `0x20`). Honored by
    /// FAT and exFAT; ignored elsewhere. `None` lets the filesystem pick
    /// its conventional default (FAT/exFAT use archive). See
    /// [`crate::fs::entry::FileEntry::dos_attributes`].
    pub dos_attributes: Option<u16>,
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
    /// AmigaDOS protection bits. See `CreateFileOptions::amiga_protection`.
    pub amiga_protection: Option<u32>,
    /// AmigaDOS filenote (comment). See `CreateFileOptions::amiga_comment`.
    pub amiga_comment: Option<String>,
    /// AmigaDOS raw datestamp triple. See `CreateFileOptions::amiga_dates`.
    pub amiga_dates: Option<(i32, i32, i32)>,
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

    /// Set the ProDOS access byte on an existing file or directory.
    ///
    /// The access byte (offset 30 of a directory entry) is a bitmask:
    /// `0x80` = enable destroy, `0x40` = enable rename, `0x20` = backup
    /// required (set when the file changes; cleared by backup software),
    /// `0x02` = enable write, `0x01` = enable read. Conventional values:
    /// `0xC3` = unlocked (read/write/destroy/rename), `0x21` = locked
    /// (read + backup-required).
    ///
    /// Default returns `Unsupported` — override on ProDOS.
    fn set_prodos_access(
        &mut self,
        _entry: &FileEntry,
        _access: u8,
    ) -> Result<(), FilesystemError> {
        Err(FilesystemError::Unsupported(
            "set_prodos_access not supported for this filesystem".into(),
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

    /// Stage the 1024-byte HFS boot-block region (sectors 0–1) for the
    /// volume, written verbatim at `sync_metadata` time. Used to make a
    /// classic-HFS volume bootable by copying a donor's boot blocks. Override
    /// on HFS/HFS+ (the only filesystems with a Mac boot region); others
    /// return `Unsupported` so the GUI can gate the action.
    fn write_boot_blocks(&mut self, _blocks: &[u8; 1024]) -> Result<(), FilesystemError> {
        Err(FilesystemError::Unsupported(
            "write_boot_blocks not supported for this filesystem".into(),
        ))
    }

    /// Rename the volume. Override on filesystems where the volume name is
    /// stored in metadata that the implementation can rewrite in place
    /// (HFS today; others return `Unsupported`).
    fn set_volume_name(&mut self, _new_name: &str) -> Result<(), FilesystemError> {
        Err(FilesystemError::Unsupported(
            "set_volume_name not supported for this filesystem".into(),
        ))
    }

    /// Overwrite the full 16-byte FInfo and 16-byte FXInfo Finder metadata
    /// on a file's catalog record. HFS today; others `Unsupported`. Used
    /// by the MacBinary put path to apply Finder flags / location / folder
    /// in one shot.
    fn set_finder_info(
        &mut self,
        _entry: &FileEntry,
        _finfo: [u8; 16],
        _fxinfo: [u8; 16],
    ) -> Result<(), FilesystemError> {
        Err(FilesystemError::Unsupported(
            "set_finder_info not supported for this filesystem".into(),
        ))
    }

    /// Overwrite the create/modify/backup dates on a file's catalog
    /// record. Dates are Mac epoch (seconds since 1904-01-01 00:00 UTC).
    /// HFS today; others `Unsupported`.
    fn set_dates(
        &mut self,
        _entry: &FileEntry,
        _create: u32,
        _modify: u32,
        _backup: u32,
    ) -> Result<(), FilesystemError> {
        Err(FilesystemError::Unsupported(
            "set_dates not supported for this filesystem".into(),
        ))
    }

    /// Create a symbolic link in `parent` pointing at `target` (a path
    /// string interpreted by the filesystem's own resolver semantics).
    /// Returns the new symlink's entry. Default returns `Unsupported` —
    /// override on filesystems that support symlinks (PFS3, AFFS,
    /// ext, etc).
    fn create_symlink(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _target: &str,
        _options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "create_symlink not supported for this filesystem".into(),
        ))
    }

    /// Create a hard link in `parent` pointing at an existing entry
    /// already on this filesystem. `target` must be an entry returned
    /// by a previous `create_file` / `create_directory` / `list_directory`
    /// call on this same volume — its `location` identifies the inode /
    /// anode / CNID that the new link references. Returns the new
    /// hardlink's entry. Default returns `Unsupported` — override on
    /// filesystems that support hardlinks (PFS3, HFS+, ext, etc).
    fn create_hardlink(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _target: &FileEntry,
        _options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "create_hardlink not supported for this filesystem".into(),
        ))
    }
}
