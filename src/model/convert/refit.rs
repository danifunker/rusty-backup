//! Refit a floppy to another standard size: format a blank volume of the same filesystem at
//! the new size, copy every file across, and carry the boot code over (docs/convert_tab.md).
//! Floppies are at most 2.88 MB, so they are refitted in memory (the streaming rule's exception).

use std::io::{Cursor, Read, Seek, SeekFrom, Write};

use crate::fs::copy::{copy_into, AttrPolicy, ConflictMode, CopyOptions, CopyStats, NamePolicy};
use crate::model::format::{format_into, FormatOptions, FsKind};

/// A family of floppy drives a filesystem was written for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    Pc,
    Mac,
    Amiga,
}

impl Family {
    pub const fn label(self) -> &'static str {
        match self {
            Family::Pc => "PC",
            Family::Mac => "Mac",
            Family::Amiga => "Amiga",
        }
    }
}

/// A size a floppy can be refitted to.
pub struct RefitSize {
    pub bytes: u64,
    pub label: &'static str,
    /// Command-line spellings, the first one canonical.
    pub ids: &'static [&'static str],
    pub families: &'static [Family],
}

pub const SIZES: &[RefitSize] = &[
    RefitSize {
        bytes: 368_640,
        label: "360 KB",
        ids: &["360k"],
        families: &[Family::Pc],
    },
    RefitSize {
        bytes: 737_280,
        label: "720 KB",
        ids: &["720k"],
        families: &[Family::Pc],
    },
    RefitSize {
        bytes: 819_200,
        label: "800 KB",
        ids: &["800k"],
        families: &[Family::Mac],
    },
    RefitSize {
        bytes: 901_120,
        label: "880 KB",
        ids: &["880k"],
        families: &[Family::Amiga],
    },
    RefitSize {
        bytes: 1_228_800,
        label: "1.2 MB",
        ids: &["1.2m", "1200k"],
        families: &[Family::Pc],
    },
    RefitSize {
        bytes: 1_474_560,
        label: "1.44 MB",
        ids: &["1.44m", "1440k"],
        families: &[Family::Pc, Family::Mac],
    },
    RefitSize {
        bytes: 1_802_240,
        label: "1.76 MB",
        ids: &["1.76m", "1760k"],
        families: &[Family::Amiga],
    },
    RefitSize {
        bytes: 2_949_120,
        label: "2.88 MB",
        ids: &["2.88m", "2880k"],
        families: &[Family::Pc],
    },
];

/// A refit size's label, or its byte count.
pub fn size_label(bytes: u64) -> String {
    SIZES
        .iter()
        .find(|s| s.bytes == bytes)
        .map(|s| s.label.to_string())
        .unwrap_or_else(|| format!("{bytes} bytes"))
}

/// The family a filesystem name (`Filesystem::fs_type`) belongs to.
pub fn family_of(fs_type: &str) -> Option<Family> {
    if fs_type.starts_with("FAT") {
        Some(Family::Pc)
    } else if fs_type == "HFS" {
        Some(Family::Mac)
    } else if fs_type.starts_with("OFS") || fs_type.starts_with("FFS") {
        Some(Family::Amiga)
    } else {
        None
    }
}

/// What a refit did, for the log.
pub struct Refitted {
    pub image: Vec<u8>,
    pub files: u64,
    pub bytes: u64,
}

/// The note on a refit: only the files cross.
pub const ONLY_FILES: &str = "Only the files are copied. Disks that depend on their exact \
    layout, such as copy-protected games, won't work after a refit.";

/// Refit the floppy `flat` (its plain sectors) to `target` bytes.
pub fn refit(flat: &[u8], target: u64, log: &dyn Fn(&str)) -> Result<Refitted, String> {
    let mut src =
        crate::fs::open_filesystem(Cursor::new(flat.to_vec()), 0, 0, None).map_err(|_| {
            "No filesystem the engine can copy from was found, so it can't be refitted \
             (copy-protected and raw-track disks keep their size)."
                .to_string()
        })?;
    let fs_type = src.fs_type().to_string();
    let family = family_of(fs_type.as_str())
        .ok_or_else(|| format!("Refitting isn't available for {fs_type} disks."))?;
    let size = SIZES
        .iter()
        .find(|s| s.bytes == target)
        .ok_or_else(|| format!("{target} bytes is not a standard floppy size."))?;
    if !size.families.contains(&family) {
        return Err(format!(
            "{} is not a size for a {} disk.",
            size.label,
            family.label()
        ));
    }
    let label = src.volume_label().map(str::to_string);
    let blank = blank_volume(family, flat, target, label.as_deref())?;

    let tmp = tempfile::tempfile().map_err(|e| format!("Cannot make a scratch file: {e}"))?;
    let mut file = tmp;
    file.write_all(blank.as_slice())
        .map_err(|e| format!("Cannot write the blank disk: {e}"))?;
    let reopen = file
        .try_clone()
        .map_err(|e| format!("Cannot reopen the blank disk: {e}"))?;
    let mut dst = crate::fs::open_editable_filesystem(reopen, 0, 0, None)
        .map_err(|e| format!("Cannot open the blank {} disk: {e}", size.label))?;

    let root = src
        .root()
        .map_err(|e| format!("Cannot read the disk: {e}"))?;
    let children = src
        .list_directory(&root)
        .map_err(|e| format!("Cannot list the disk: {e}"))?;
    let caps = crate::fs::copy::Capabilities::infer(dst.fs_type());
    let unit = dst.as_filesystem().allocation_unit();
    let mut needed = 0u64;
    for child in &children {
        needed = needed.saturating_add(
            crate::fs::copy::project(src.as_mut(), child, &caps, unit, true)
                .map_err(|e| format!("Cannot size the files: {e:#}"))?,
        );
    }
    let free = dst
        .free_space()
        .map_err(|e| format!("Cannot read the new disk's free space: {e}"))?;
    if needed > free {
        return Err(format!(
            "{} of files won't fit on {}.",
            super::classify::size_text(needed),
            size.label
        ));
    }

    let opts = CopyOptions {
        recursive: true,
        conflict: ConflictMode::Error,
        names: NamePolicy::Error,
        attrs: AttrPolicy::Preserve,
        flatten: false,
        preserve_times: true,
    };
    let mut stats = CopyStats::default();
    let dst_root = dst
        .as_filesystem_mut()
        .root()
        .map_err(|e| format!("Cannot read the new disk: {e}"))?;
    dst.begin_bulk();
    for child in &children {
        copy_into(
            src.as_mut(),
            child,
            dst.as_mut(),
            &dst_root,
            child.name.as_str(),
            &opts,
            &mut stats,
            log,
        )
        .map_err(|e| format!("Copying {} failed: {e:#}", child.name))?;
    }
    dst.end_bulk();
    dst.sync_metadata()
        .map_err(|e| format!("Cannot finish the new disk: {e}"))?;
    drop(dst);

    let mut image = vec![0u8; target as usize];
    file.seek(SeekFrom::Start(0))
        .and_then(|_| file.read_exact(image.as_mut_slice()))
        .map_err(|e| format!("Cannot read the new disk back: {e}"))?;
    carry_boot_code(family, flat, image.as_mut_slice(), target);
    Ok(Refitted {
        image,
        files: stats.files,
        bytes: stats.bytes,
    })
}

fn blank_volume(
    family: Family,
    flat: &[u8],
    target: u64,
    label: Option<&str>,
) -> Result<Vec<u8>, String> {
    if family == Family::Pc {
        return fs_pc::fat::create_dos_floppy(target, label).map_err(|e| format!("{e:#}"));
    }
    let (kind, variant) = match family {
        Family::Mac => (FsKind::Hfs, 0),
        _ => (FsKind::Affs, flat.get(3).copied().unwrap_or(1).min(5)),
    };
    let opts = FormatOptions {
        label: label
            .filter(|l| !l.trim().is_empty())
            .unwrap_or(kind.default_label())
            .to_string(),
        affs_variant: variant,
        ..FormatOptions::default()
    };
    let mut out = Cursor::new(vec![0u8; target as usize]);
    format_into(&mut out, target, kind, &opts).map_err(|e| format!("{e:#}"))?;
    let mut image = out.into_inner();
    image.resize(target as usize, 0);
    Ok(image)
}

/// Copy the source's boot code into the new disk, keeping the new disk's own size fields.
fn carry_boot_code(family: Family, src: &[u8], dst: &mut [u8], target: u64) {
    match family {
        Family::Pc => {
            // A DOS boot sector jumps over its BPB; everything from the jump target is code.
            let boots = src.len() >= 512 && src[510] == 0x55 && src[511] == 0xAA;
            if !boots || src[0] != 0xEB {
                return;
            }
            let code = (src[1] as usize + 2).clamp(0x1E, 0x1FE);
            dst[0..3].copy_from_slice(&src[0..3]);
            dst[3..11].copy_from_slice(&src[3..11]);
            dst[code..0x1FE].copy_from_slice(&src[code..0x1FE]);
        }
        Family::Mac => {
            // Boot blocks start "LK"; the MDB at 1024 is the new disk's own.
            if src.len() >= 1024 && src[0..2] == *b"LK" {
                dst[0..1024].copy_from_slice(&src[0..1024]);
            }
        }
        Family::Amiga => {
            // Keep the new DOS type and root block, take the code, and redo the checksum.
            if src.len() < 1024 || src[0..3] != *b"DOS" || src[12..1024].iter().all(|&b| b == 0) {
                return;
            }
            dst[12..1024].copy_from_slice(&src[12..1024]);
            let root = (target / 512 / 2) as u32;
            dst[8..12].copy_from_slice(&root.to_be_bytes());
            dst[4..8].copy_from_slice(&[0; 4]);
            let mut sum: u32 = 0;
            for word in dst[0..1024].chunks_exact(4) {
                let w = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
                let (s, carry) = sum.overflowing_add(w);
                sum = s + carry as u32;
            }
            dst[4..8].copy_from_slice(&(!sum).to_be_bytes());
        }
    }
}
