//! Well-known partition type values, keyed by partition-table flavor.
//!
//! The partition-table editor's Type field means something different on every
//! table: a hex byte on MBR, a type GUID on GPT, a name string on APM, an
//! AmigaDOS DosType on RDB, a type keyword on SGI. This module is the single
//! source of truth for "what may I type in that box", shared by the GUI's
//! type dropdown and `rb-cli partmap types`.
//!
//! The lists are curated, not exhaustive — they cover what a retro-computing
//! user actually creates. Both surfaces keep free-form entry, so an unlisted
//! value is always still reachable by typing it.

use super::PartitionTable;

/// Which partition-table flavor a type value belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableKind {
    Mbr,
    Gpt,
    Apm,
    Rdb,
    Sgi,
    /// Sun disk label (SMI VTOC). Entries carry a numeric VTOC tag.
    Sun,
    /// Atari ST AHDI. Entries carry a 3-character ASCII type tag.
    Atari,
    /// Sharp X68000. Entries carry a name, not a type code.
    X68k,
    /// Tables whose type field the editor does not expose a catalog for.
    Other,
}

impl TableKind {
    /// Short display name, matching the editor popup's "Table type:" line.
    pub fn label(self) -> &'static str {
        match self {
            TableKind::Mbr => "MBR",
            TableKind::Gpt => "GPT",
            TableKind::Apm => "APM",
            TableKind::Rdb => "RDB",
            TableKind::Sgi => "SGI",
            TableKind::Sun => "Sun",
            TableKind::Atari => "AHDI",
            TableKind::X68k => "X68000",
            TableKind::Other => "Unknown",
        }
    }

    /// What the Type field holds on this table, for hover help.
    pub fn field_hint(self) -> &'static str {
        match self {
            TableKind::Mbr => "MBR partition type byte, in hex (e.g. 0C, 83, AF)",
            TableKind::Gpt => "GPT partition type GUID",
            TableKind::Apm => "APM partition type string (e.g. Apple_HFS)",
            TableKind::Rdb => "AmigaDOS DosType tag (e.g. DOS\\3, PFS\\3, SFS\\0)",
            TableKind::Sgi => "SGI partition type keyword (e.g. XFS, EFS)",
            TableKind::Sun => "Sun VTOC slice tag, by name or number (e.g. root, usr, 4)",
            TableKind::Atari => "AHDI 3-character type tag (GEM, BGM, XGM, RAW)",
            TableKind::X68k => "X68000 partition name (e.g. Human68k)",
            TableKind::Other => "Partition type value",
        }
    }
}

/// One row of the catalog: the literal value the editor expects, plus the
/// human name shown beside it.
#[derive(Debug, Clone, Copy)]
pub struct TypeChoice {
    /// Exactly what goes in the Type field.
    pub value: &'static str,
    pub label: &'static str,
}

/// Classify a loaded partition table.
pub fn kind_of(table: &PartitionTable) -> TableKind {
    match table {
        PartitionTable::Mbr(_) => TableKind::Mbr,
        PartitionTable::Gpt { .. } => TableKind::Gpt,
        PartitionTable::Apm(_) => TableKind::Apm,
        PartitionTable::Rdb(_) => TableKind::Rdb,
        PartitionTable::Sgi(_) => TableKind::Sgi,
        PartitionTable::Sun(_) => TableKind::Sun,
        PartitionTable::Ahdi(_) => TableKind::Atari,
        PartitionTable::X68k { .. } => TableKind::X68k,
        _ => TableKind::Other,
    }
}

/// Curated type values for `kind`. Empty for [`TableKind::Other`].
pub fn choices(kind: TableKind) -> &'static [TypeChoice] {
    match kind {
        TableKind::Mbr => MBR_TYPES,
        TableKind::Gpt => GPT_TYPES,
        TableKind::Apm => APM_TYPES,
        TableKind::Rdb => RDB_TYPES,
        TableKind::Sgi => SGI_TYPES,
        TableKind::Sun => SUN_TYPES,
        TableKind::Atari => ATARI_TYPES,
        // X68k entries are named, not typed, so there is nothing to offer.
        TableKind::X68k => &[],
        TableKind::Other => &[],
    }
}

/// Human name for `value` on `kind`, or `None` when it isn't in the catalog.
/// Matching is forgiving: MBR accepts `0xAF` / `af` / `AF`, GPT and APM are
/// case-insensitive.
pub fn describe(kind: TableKind, value: &str) -> Option<&'static str> {
    let normalized = normalize(kind, value);
    if normalized.is_empty() {
        return None;
    }
    choices(kind)
        .iter()
        .find(|c| normalize(kind, c.value) == normalized)
        .map(|c| c.label)
}

/// Canonical comparison form for a type value on `kind`.
fn normalize(kind: TableKind, value: &str) -> String {
    let trimmed = value.trim();
    match kind {
        TableKind::Mbr => {
            let hex = trimmed
                .strip_prefix("0x")
                .or_else(|| trimmed.strip_prefix("0X"))
                .unwrap_or(trimmed);
            // Compare on the parsed byte so "F" and "0F" agree.
            match u8::from_str_radix(hex, 16) {
                Ok(b) => format!("{:02X}", b),
                Err(_) => String::new(),
            }
        }
        TableKind::Gpt | TableKind::Apm | TableKind::Sgi | TableKind::Atari => {
            trimmed.to_ascii_uppercase()
        }
        // A Sun slice tag is a number; the names are just aliases for one.
        TableKind::Sun => match crate::partition::sun::tag_from_text(trimmed) {
            Some(tag) => tag.to_string(),
            None => String::new(),
        },
        TableKind::Rdb | TableKind::X68k | TableKind::Other => trimmed.to_string(),
    }
}

const MBR_TYPES: &[TypeChoice] = &[
    TypeChoice {
        value: "01",
        label: "FAT12",
    },
    TypeChoice {
        value: "04",
        label: "FAT16 (<32 MB)",
    },
    TypeChoice {
        value: "05",
        label: "Extended (CHS)",
    },
    TypeChoice {
        value: "06",
        label: "FAT16 (>32 MB)",
    },
    TypeChoice {
        value: "07",
        label: "NTFS / HPFS / exFAT",
    },
    TypeChoice {
        value: "0B",
        label: "FAT32 (CHS)",
    },
    TypeChoice {
        value: "0C",
        label: "FAT32 (LBA)",
    },
    TypeChoice {
        value: "0E",
        label: "FAT16 (LBA)",
    },
    TypeChoice {
        value: "0F",
        label: "Extended (LBA)",
    },
    TypeChoice {
        value: "82",
        label: "Linux swap",
    },
    TypeChoice {
        value: "83",
        label: "Linux (ext2/3/4, XFS, ...)",
    },
    TypeChoice {
        value: "8E",
        label: "Linux LVM",
    },
    TypeChoice {
        value: "A5",
        label: "FreeBSD",
    },
    TypeChoice {
        value: "A6",
        label: "OpenBSD",
    },
    TypeChoice {
        value: "AF",
        label: "Apple HFS / HFS+",
    },
    TypeChoice {
        value: "EE",
        label: "GPT protective",
    },
    TypeChoice {
        value: "EF",
        label: "EFI System",
    },
    TypeChoice {
        value: "FD",
        label: "Linux RAID autodetect",
    },
];

const GPT_TYPES: &[TypeChoice] = &[
    TypeChoice {
        value: "C12A7328-F81F-11D2-BA4B-00A0C93EC93B",
        label: "EFI System",
    },
    TypeChoice {
        value: "21686148-6449-6E6F-7468-656564454649",
        label: "BIOS Boot",
    },
    TypeChoice {
        value: "EBD0A0A2-B9E5-4433-87C0-68B6B72699C7",
        label: "Microsoft Basic Data (NTFS / FAT / exFAT)",
    },
    TypeChoice {
        value: "E3C9E316-0B5C-4DB8-817D-F92DF00215AE",
        label: "Microsoft Reserved",
    },
    TypeChoice {
        value: "DE94BBA4-06D1-4D40-A16A-BFD50179D6AC",
        label: "Windows Recovery",
    },
    TypeChoice {
        value: "0FC63DAF-8483-4772-8E79-3D69D8477DE4",
        label: "Linux Filesystem",
    },
    TypeChoice {
        value: "0657FD6D-A4AB-43C4-84E5-0933C84B4F4F",
        label: "Linux Swap",
    },
    TypeChoice {
        value: "E6D6D379-F507-44C2-A23C-238F2A3DF928",
        label: "Linux LVM",
    },
    TypeChoice {
        value: "A19D880F-05FC-4D3B-A006-743F0F84911E",
        label: "Linux RAID",
    },
    TypeChoice {
        value: "933AC7E1-2EB4-4F13-B844-0E14E2AEF915",
        label: "Linux Home",
    },
    TypeChoice {
        value: "48465300-0000-11AA-AA11-00306543ECAC",
        label: "Apple HFS / HFS+",
    },
    TypeChoice {
        value: "7C3457EF-0000-11AA-AA11-00306543ECAC",
        label: "Apple APFS",
    },
    TypeChoice {
        value: "55465300-0000-11AA-AA11-00306543ECAC",
        label: "Apple UFS",
    },
    TypeChoice {
        value: "426F6F74-0000-11AA-AA11-00306543ECAC",
        label: "Apple Boot (Recovery HD)",
    },
    TypeChoice {
        value: "516E7CB4-6ECF-11D6-8FF8-00022D09712B",
        label: "FreeBSD Data",
    },
    TypeChoice {
        value: "83BD6B9D-7F41-11DC-BE0B-001560B84F0F",
        label: "FreeBSD Boot",
    },
];

const APM_TYPES: &[TypeChoice] = &[
    TypeChoice {
        value: "Apple_HFS",
        label: "Mac OS HFS / HFS+",
    },
    TypeChoice {
        value: "Apple_HFSX",
        label: "HFS+ case-sensitive",
    },
    TypeChoice {
        value: "Apple_UFS",
        label: "Mac OS X UFS",
    },
    TypeChoice {
        value: "Apple_Free",
        label: "Unused space",
    },
    TypeChoice {
        value: "Apple_partition_map",
        label: "Partition map itself",
    },
    TypeChoice {
        value: "Apple_Driver43",
        label: "SCSI Manager 4.3 driver",
    },
    TypeChoice {
        value: "Apple_Driver_ATA",
        label: "ATA driver",
    },
    TypeChoice {
        value: "Apple_Patches",
        label: "Patch partition",
    },
    TypeChoice {
        value: "Apple_Bootstrap",
        label: "yaboot (Linux/PPC)",
    },
    TypeChoice {
        value: "Apple_ProDOS",
        label: "ProDOS",
    },
    TypeChoice {
        value: "Apple_MFS",
        label: "Macintosh File System",
    },
    TypeChoice {
        value: "Apple_Scratch",
        label: "Empty / scratch",
    },
];

/// AHDI type tags. `GEM` is the FAT12 / small-FAT16 partition every TOS
/// version understands; `BGM` is its over-16-MiB counterpart.
const ATARI_TYPES: &[TypeChoice] = &[
    TypeChoice {
        value: "GEM",
        label: "GEMDOS (FAT12 / small FAT16)",
    },
    TypeChoice {
        value: "BGM",
        label: "Big GEM (FAT16 over 16 MiB)",
    },
    TypeChoice {
        value: "RAW",
        label: "Raw / non-filesystem",
    },
    TypeChoice {
        value: "XGM",
        label: "Extended container",
    },
];

/// Sun VTOC slice tags. The value is the name; `normalize` maps both it and a
/// bare number onto the tag, so either form is accepted.
const SUN_TYPES: &[TypeChoice] = &[
    TypeChoice {
        value: "unassigned",
        label: "Unassigned",
    },
    TypeChoice {
        value: "boot",
        label: "Boot",
    },
    TypeChoice {
        value: "root",
        label: "SunOS root",
    },
    TypeChoice {
        value: "swap",
        label: "SunOS swap",
    },
    TypeChoice {
        value: "usr",
        label: "SunOS usr",
    },
    TypeChoice {
        value: "backup",
        label: "Whole disk",
    },
    TypeChoice {
        value: "stand",
        label: "SunOS stand",
    },
    TypeChoice {
        value: "var",
        label: "SunOS var",
    },
    TypeChoice {
        value: "home",
        label: "SunOS home",
    },
];

const RDB_TYPES: &[TypeChoice] = &[
    TypeChoice {
        value: "DOS\\0",
        label: "OFS (Old File System)",
    },
    TypeChoice {
        value: "DOS\\1",
        label: "FFS (Fast File System)",
    },
    TypeChoice {
        value: "DOS\\2",
        label: "OFS international",
    },
    TypeChoice {
        value: "DOS\\3",
        label: "FFS international",
    },
    TypeChoice {
        value: "DOS\\4",
        label: "OFS dir-cache",
    },
    TypeChoice {
        value: "DOS\\5",
        label: "FFS dir-cache",
    },
    TypeChoice {
        value: "DOS\\6",
        label: "OFS long filenames",
    },
    TypeChoice {
        value: "DOS\\7",
        label: "FFS long filenames",
    },
    TypeChoice {
        value: "PFS\\3",
        label: "PFS3",
    },
    TypeChoice {
        value: "PDS\\3",
        label: "PFS3 (PDS variant)",
    },
    TypeChoice {
        value: "SFS\\0",
        label: "SmartFilesystem v1",
    },
    TypeChoice {
        value: "SFS\\2",
        label: "SmartFilesystem v2",
    },
];

const SGI_TYPES: &[TypeChoice] = &[
    TypeChoice {
        value: "XFS",
        label: "XFS",
    },
    TypeChoice {
        value: "EFS",
        label: "EFS",
    },
    TypeChoice {
        value: "Raw",
        label: "Raw data / swap",
    },
    TypeChoice {
        value: "VolHdr",
        label: "Volume header",
    },
    TypeChoice {
        value: "Volume",
        label: "Whole volume",
    },
    TypeChoice {
        value: "XfsLog",
        label: "XFS external log",
    },
    TypeChoice {
        value: "Xlv",
        label: "XLV logical volume",
    },
    TypeChoice {
        value: "Xvm",
        label: "XVM logical volume",
    },
    TypeChoice {
        value: "Bsd",
        label: "BSD 4.2 filesystem",
    },
    TypeChoice {
        value: "SysV",
        label: "System V filesystem",
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hfs_plus_value_per_table() {
        assert_eq!(describe(TableKind::Mbr, "AF"), Some("Apple HFS / HFS+"));
        assert_eq!(
            describe(TableKind::Gpt, "48465300-0000-11AA-AA11-00306543ECAC"),
            Some("Apple HFS / HFS+"),
        );
        assert_eq!(
            describe(TableKind::Apm, "Apple_HFS"),
            Some("Mac OS HFS / HFS+"),
        );
    }

    #[test]
    fn mbr_lookup_is_prefix_and_case_tolerant() {
        for spelling in ["AF", "af", "0xAF", "0XAF", " af "] {
            assert_eq!(
                describe(TableKind::Mbr, spelling),
                Some("Apple HFS / HFS+"),
                "spelling {spelling}",
            );
        }
        // Single-digit hex normalizes to the padded form.
        assert_eq!(describe(TableKind::Mbr, "b"), Some("FAT32 (CHS)"));
    }

    #[test]
    fn gpt_and_apm_lookups_ignore_case() {
        assert!(describe(TableKind::Gpt, "0fc63daf-8483-4772-8e79-3d69d8477de4").is_some());
        assert!(describe(TableKind::Apm, "apple_free").is_some());
    }

    #[test]
    fn unknown_values_describe_as_none() {
        assert_eq!(describe(TableKind::Mbr, "zz"), None);
        assert_eq!(describe(TableKind::Mbr, ""), None);
        assert_eq!(describe(TableKind::Gpt, "not-a-guid"), None);
    }

    #[test]
    fn every_catalog_value_round_trips_through_describe() {
        for kind in [
            TableKind::Mbr,
            TableKind::Gpt,
            TableKind::Apm,
            TableKind::Rdb,
            TableKind::Sgi,
        ] {
            for choice in choices(kind) {
                assert_eq!(
                    describe(kind, choice.value),
                    Some(choice.label),
                    "{} value {}",
                    kind.label(),
                    choice.value,
                );
            }
        }
    }

    #[test]
    fn other_kind_has_no_choices() {
        assert!(choices(TableKind::Other).is_empty());
    }
}
