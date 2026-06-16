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
    /// Resource fork size (HFS/HFS+ only). None or Some(0) means no resource fork.
    pub resource_fork_size: Option<u64>,
    /// ProDOS auxiliary type (16-bit). Semantics depend on the file type
    /// (e.g. `$0801` = Applesoft BASIC load address, load addr for BIN,
    /// record length for random-access TXT). Only set for ProDOS entries.
    pub aux_type: Option<u16>,
    /// HFS+ hardlink target inode CNID. Set on entries whose catalog row
    /// is a hardlink stub (`fdType='hlnk' fdCreator='hfs+'`); the link's
    /// data and resource forks live on the inode at this CNID under
    /// `HFS+ Private Data`. `read_file` / `write_file_to` follow the
    /// indirection automatically. `None` for ordinary files.
    pub link_target_cnid: Option<u64>,
    /// AmigaDOS protection word (`access` field of an AFFS file header).
    /// `0` displays as `----rwed`. Only set for AmigaDOS / Fast File
    /// System entries.
    pub amiga_protection: Option<u32>,
    /// AmigaDOS filenote (comment), up to 79 bytes. Only set for
    /// AmigaDOS / Fast File System entries; `None` outside that family.
    pub amiga_comment: Option<String>,
    /// AmigaDOS raw datestamp triple `(days, minutes, ticks)` from the
    /// on-disk entry. Days since 1978-01-01, minutes within the day,
    /// and ticks (1/50s) within the minute. Preserved verbatim for
    /// byte-exact round-trips through clone/restore. AFFS stores these
    /// as i32 on disk; PFS3 direntries narrow them to u16 — we widen
    /// to i32 so both round-trip losslessly. The display string lives
    /// in `modified`. Only set for AmigaDOS / PFS3 / SFS entries;
    /// `None` outside that family.
    pub amiga_date: Option<(i32, i32, i32)>,
    /// Standard DOS attribute bits (read-only `0x01`, hidden `0x02`,
    /// system `0x04`, archive `0x20`; the volume-label `0x08` and
    /// directory `0x10` bits are represented by `entry_type` and not
    /// carried here). Populated for FAT (the on-disk `u8` widened) and
    /// exFAT (the directory entry's `u16` FileAttributes). `None` for
    /// filesystems that have no DOS attribute concept. The copy engine
    /// carries this across FAT/exFAT destinations.
    pub dos_attributes: Option<u16>,
    /// HFS/HFS+/MFS raw catalog dates `(create, modify, backup)` in Mac-epoch
    /// seconds (since 1904-01-01 UTC). Kept raw so the detail pane can both
    /// display them (via `hfs_common::format_mac_date`) and stage an in-place
    /// edit (`StagedEdit::SetDates`). The `modify` value is also surfaced as a
    /// formatted string in `modified`. `None` outside the HFS family.
    pub mac_dates: Option<(u32, u32, u32)>,
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
            resource_fork_size: None,
            aux_type: None,
            link_target_cnid: None,
            amiga_protection: None,
            amiga_comment: None,
            amiga_date: None,
            dos_attributes: None,
            mac_dates: None,
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
            resource_fork_size: None,
            aux_type: None,
            link_target_cnid: None,
            amiga_protection: None,
            amiga_comment: None,
            amiga_date: None,
            dos_attributes: None,
            mac_dates: None,
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
            resource_fork_size: None,
            aux_type: None,
            link_target_cnid: None,
            amiga_protection: None,
            amiga_comment: None,
            amiga_date: None,
            dos_attributes: None,
            mac_dates: None,
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
            resource_fork_size: None,
            aux_type: None,
            link_target_cnid: None,
            amiga_protection: None,
            amiga_comment: None,
            amiga_date: None,
            dos_attributes: None,
            mac_dates: None,
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
            resource_fork_size: None,
            aux_type: None,
            link_target_cnid: None,
            amiga_protection: None,
            amiga_comment: None,
            amiga_date: None,
            dos_attributes: None,
            mac_dates: None,
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
