//! What a filesystem can store, inferred from its `fs_type` string: the one table
//! copy, import and convert consult before carrying metadata across.

/// Coarse per-filesystem capability flags, used to decide which metadata
/// axes can cross and whether directories are supported.
///
/// Inferred from [`Filesystem::fs_type`](crate::filesystem::Filesystem::fs_type) in [`Capabilities::infer`]. This
/// is the single place to update when a new filesystem gains relevant
/// support; an incomplete entry degrades to spurious "dropped" warnings or
/// a loud `create_directory` error, never to silent data loss. (Promoting
/// this to a `Filesystem` trait method later is mechanical.)
#[derive(Debug, Clone, Copy)]
pub struct Capabilities {
    pub subdirectories: bool,
    pub resource_forks: bool,
    pub type_creator: bool,
    pub unix_permissions: bool,
    pub dos_attributes: bool,
    pub amiga_metadata: bool,
    pub symlinks: bool,
    /// Raw QNX 2 attributes, owner and date words (QNX 2 only).
    pub qnx2_attributes: bool,
}

impl Capabilities {
    pub fn infer(fs_type: &str) -> Self {
        let t = fs_type;
        // Hierarchical baseline; flip individual axes below.
        let mut c = Capabilities {
            subdirectories: true,
            resource_forks: false,
            type_creator: false,
            unix_permissions: false,
            dos_attributes: false,
            amiga_metadata: false,
            symlinks: false,
            qnx2_attributes: false,
        };

        // Classic Mac forks + Finder type/creator.
        if t.starts_with("HFS") {
            c.resource_forks = true;
            c.type_creator = true;
        } else if t == "MFS" {
            c.resource_forks = true;
            c.type_creator = true;
            c.subdirectories = false; // MFS is a flat volume
        } else if t == "ProDOS" {
            c.resource_forks = true; // extended (forked) files
            c.type_creator = true; // file type + aux
        }

        // Unix families: mode/uid/gid + symlinks.
        if t.starts_with("ext")
            || t == "btrfs"
            || t.starts_with("XFS")
            || t.starts_with("UFS")
            || t.starts_with("ReiserFS")
            || t == "EFS"
            || t.starts_with("JFS")
        {
            c.unix_permissions = true;
            c.symlinks = true;
        }

        // QNX 2 maps Unix modes onto its attribute letters; between QNX 2 volumes they copy raw.
        if t == "QNX 2" {
            c.unix_permissions = true;
            c.qnx2_attributes = true;
        }

        // DOS attribute bits.
        if t.starts_with("FAT") || t == "exFAT" || t.starts_with("Human68k") {
            c.dos_attributes = true;
        }

        // AmigaDOS family: protection / comment / datestamp + symlinks.
        if t.starts_with("OFS")
            || t.starts_with("FFS")
            || t.starts_with("PFS")
            || t == "AFS"
            || t == "SFS"
        {
            c.amiga_metadata = true;
            c.symlinks = true;
        }

        // Flat filesystems with no subdirectory concept.
        if t == "CP/M"
            || t == "Acorn DFS"
            || t == "DOS 3.3"
            || t == "Atari DOS 2"
            || t == "RS-DOS"
            || t == "DragonDOS"
            || t == "QDOS Microdrive"
            || t.starts_with("ANDOS")
            || t.starts_with("CBM DOS")
        {
            c.subdirectories = false;
        }

        c
    }
}
