//! The Options step's rules, so every view edits a filesystem's options the same way.

use super::{FormatOptions, FormatTarget, FsOption, NtfsVersion};

/// The AFFS DosType variants, by the byte `FormatOptions::affs_variant` holds.
pub const AFFS_VARIANTS: [(u8, &str); 8] = [
    (0, "OFS (DOS\\0)"),
    (1, "FFS (DOS\\1)"),
    (2, "OFS international (DOS\\2)"),
    (3, "FFS international (DOS\\3)"),
    (4, "OFS dir cache (DOS\\4)"),
    (5, "FFS dir cache (DOS\\5)"),
    (6, "OFS long names (DOS\\6)"),
    (7, "FFS long names (DOS\\7)"),
];

/// The NTFS versions on offer, newest first, with the Windows that mount each.
pub const NTFS_VERSIONS: [(NtfsVersion, &str); 2] = [
    (NtfsVersion::V3_1, "3.1 (Windows XP and later)"),
    (NtfsVersion::V1_2, "1.2 (Windows NT 3.5 to 4.0)"),
];

/// The NTFS sector sizes on offer; 512 is the default, stored as `None`.
pub const SECTOR_SIZES: [u32; 4] = [512, 1024, 2048, 4096];

/// One edit of a filesystem's options; a size of `None` means automatic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionEdit {
    Label(String),
    BlockSize(Option<u64>),
    BigEndian(bool),
    Fat32(bool),
    CaseSensitive(bool),
    AffsVariant(u8),
    CpmPreset(String),
    Geometry(String),
    Inodes(Option<u64>),
    BytesPerInode(Option<u64>),
    ClusterSize(Option<u64>),
    SectorSize(u32),
    NtfsVersion(NtfsVersion),
}

fn clamp32(v: Option<u64>) -> Option<u32> {
    v.map(|v| v.min(u64::from(u32::MAX)) as u32)
}

impl OptionEdit {
    /// Inode count and density are two ways to say one thing, so setting one clears the other.
    pub fn apply(self, o: &mut FormatOptions) {
        match self {
            OptionEdit::Label(t) => o.label = t,
            OptionEdit::BlockSize(v) => o.block_size = clamp32(v),
            OptionEdit::BigEndian(on) => o.big_endian = on,
            OptionEdit::Fat32(on) => o.fat32 = on,
            OptionEdit::CaseSensitive(on) => o.case_sensitive = on,
            OptionEdit::AffsVariant(v) => o.affs_variant = v,
            OptionEdit::CpmPreset(p) => o.cpm_preset = Some(p),
            OptionEdit::Geometry(g) => o.geometry = Some(g),
            OptionEdit::Inodes(v) => {
                o.inodes = v;
                if v.is_some() {
                    o.bytes_per_inode = None;
                }
            }
            OptionEdit::BytesPerInode(v) => {
                o.bytes_per_inode = v;
                if v.is_some() {
                    o.inodes = None;
                }
            }
            OptionEdit::ClusterSize(v) => o.cluster_bytes = clamp32(v),
            OptionEdit::SectorSize(s) => o.sector_size = (s != 512).then_some(s),
            // 1.2 is 512-byte sectors only, so picking it takes the sector size back to 512.
            OptionEdit::NtfsVersion(v) => {
                o.ntfs_version = Some(v);
                if v == NtfsVersion::V1_2 {
                    o.sector_size = None;
                }
            }
        }
    }
}

impl FormatTarget {
    /// The options a view offers: the B-tree sizes stay with the CLI.
    pub fn fields(&self) -> Vec<FsOption> {
        let shown = |o: &&FsOption| !matches!(o, FsOption::BtreeSizes | FsOption::MinCatalog);
        self.caps.options.iter().filter(shown).copied().collect()
    }

    /// Its options with `edit` made, for `SetRowOptions` / `SetVolumeOptions`.
    pub fn edited(&self, edit: OptionEdit) -> FormatOptions {
        let mut o = self.opts.clone();
        edit.apply(&mut o);
        o
    }
}
