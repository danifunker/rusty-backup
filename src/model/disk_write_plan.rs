//! The rules of writing one image to a disk (Physical Disk Export, Write Image File).
//!
//! Every shell shares them through `controller::disk_write`: what the source is (decoded size,
//! whether it is a superfloppy and its VBR's hidden-sectors hint), the defaults of the optional
//! partition table synthesized around a superfloppy, the partitions on a target, and the
//! checks that turn a form into a [`PhysicalWriteRequest`]. The write itself is
//! `physical_write_runner::run_worker`; the table synthesis is `restore::superfloppy_wrap`.
//! See docs/phase2_commander_plan.md § "Slice 14".

use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::model::physical_write_runner::{PhysicalWriteRequest, PhysicalWriteSource, WriteExtent};
use crate::model::size_mode::{SizeMode, CUSTOM_MIB_MAX};
use crate::partition::PartitionInfo;
use crate::restore::superfloppy_wrap::{self, WrapAlignment, WrapParams, WrapTable};
use disk_core::display::format_size;

/// What a screen hands the disk writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteOpen {
    pub path: PathBuf,
    /// The decoded size when the caller measured it; `None` measures it in the open job.
    pub size_bytes: Option<u64>,
    pub fs_hint: String,
    pub has_partition_table: bool,
    /// A table-less volume: its VBR's hidden-sectors field seeds the alignment.
    pub superfloppy: bool,
    /// "Synthesize partition table" starts ticked (it is offered whenever there is no table).
    pub synthesize: bool,
}

impl WriteOpen {
    /// A plain image whose decoded size is known (Write Image File).
    pub fn image(path: PathBuf, size_bytes: u64) -> Self {
        WriteOpen {
            path,
            size_bytes: Some(size_bytes),
            fs_hint: "image".to_string(),
            has_partition_table: true,
            superfloppy: false,
            synthesize: false,
        }
    }

    /// Write Image File's source: the decoded size, and a superfloppy offers (unticked) a table.
    pub fn probe_image(path: PathBuf) -> Self {
        let mut open = WriteOpen::image(path, 0);
        let Ok(mut reader) = crate::model::source_reader::open_peeled_read_with_entry(
            open.path.as_path(),
            None,
            None,
        ) else {
            return open;
        };
        let size = reader.seek(SeekFrom::End(0)).unwrap_or(0);
        open.size_bytes = Some(size);
        if size == 0 || reader.seek(SeekFrom::Start(0)).is_err() {
            return open;
        }
        if let Ok(crate::partition::PartitionTable::None { fs_hint, .. }) =
            crate::partition::PartitionTable::detect(&mut reader)
        {
            open.fs_hint = fs_hint;
            open.has_partition_table = false;
            open.superfloppy = true;
        }
        open
    }
}

/// The source once opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteSource {
    pub path: PathBuf,
    /// The decoded disk size (a container's file size is not its disk size).
    pub size_bytes: u64,
    pub fs_hint: String,
    pub has_partition_table: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableKind {
    Mbr,
    Gpt,
}

/// Where the synthesized partition starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WrapAlign {
    /// LBA 63.
    DosTraditional,
    /// LBA 2048.
    Modern1MB,
    /// `WrapForm::custom_alignment_sectors`.
    Custom,
}

/// The "Synthesize partition table" group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WrapForm {
    pub synthesize: bool,
    pub table_kind: TableKind,
    pub alignment: WrapAlign,
    pub custom_alignment_sectors: u64,
    pub type_byte: u8,
    pub size_mode: SizeMode,
    pub custom_size_mib: u32,
}

impl Default for WrapForm {
    fn default() -> Self {
        WrapForm {
            synthesize: false,
            table_kind: TableKind::Mbr,
            alignment: WrapAlign::Modern1MB,
            custom_alignment_sectors: 2048,
            type_byte: 0x07,
            size_mode: SizeMode::Original,
            custom_size_mib: 0,
        }
    }
}

impl WrapForm {
    fn alignment(&self) -> WrapAlignment {
        match self.alignment {
            WrapAlign::DosTraditional => WrapAlignment::DosTraditional,
            WrapAlign::Modern1MB => WrapAlignment::Modern1MB,
            WrapAlign::Custom => WrapAlignment::Custom(self.custom_alignment_sectors),
        }
    }

    /// The synthesized partition's size, rounded down to a sector.
    fn partition_size(&self, source_size: u64, target_size: u64, first_byte: u64) -> u64 {
        let raw = if self.size_mode == SizeMode::FillRemaining {
            target_size.saturating_sub(first_byte)
        } else {
            self.size_mode
                .effective_size(source_size, source_size, self.custom_size_mib)
        };
        (raw / 512) * 512
    }

    /// The size modes the synthesized partition offers (nothing is compacted, so no Minimum).
    pub fn size_choices() -> [SizeMode; 3] {
        [
            SizeMode::Original,
            SizeMode::Custom,
            SizeMode::FillRemaining,
        ]
    }

    /// A Custom size's range in MiB: the source (at least 1) up to 2 TiB; a smaller one is refused.
    pub fn custom_bounds_mib(source_size: u64) -> (u32, u32) {
        let lo = (source_size / (1024 * 1024)).clamp(1, CUSTOM_MIB_MAX as u64) as u32;
        (lo, CUSTOM_MIB_MAX)
    }
}

/// The common MBR type bytes the type picker offers (any byte may be typed).
pub const TYPE_BYTE_PRESETS: &[(u8, &str)] = &[
    (0x01, "0x01 FAT12"),
    (0x06, "0x06 FAT16"),
    (0x07, "0x07 NTFS/exFAT"),
    (0x0B, "0x0B FAT32 (CHS)"),
    (0x0C, "0x0C FAT32 (LBA)"),
    (0x83, "0x83 Linux"),
    (0xAF, "0xAF HFS/HFS+"),
];

/// Measure the source and seed the form (a slow read for a container; run it in a job).
pub fn prepare(open: WriteOpen) -> (WriteSource, WrapForm) {
    let size_bytes = open.size_bytes.unwrap_or_else(|| {
        crate::model::source_reader::decoded_image_size(open.path.as_path())
            .unwrap_or_else(|| std::fs::metadata(open.path.as_path()).map_or(0, |m| m.len()))
    });
    let hidden = if open.superfloppy {
        hidden_sectors(open.path.as_path())
    } else {
        None
    };
    // The target size is not known yet: the source size keeps the heuristic from forcing GPT.
    let suggested = superfloppy_wrap::suggest_wrap_params(
        open.fs_hint.as_str(),
        size_bytes,
        size_bytes.max(1),
        hidden,
    );
    let (table_kind, type_byte, align) = match suggested.table {
        WrapTable::Mbr {
            type_byte,
            alignment,
            ..
        } => (TableKind::Mbr, type_byte, alignment),
        WrapTable::Gpt { alignment, .. } => (
            TableKind::Gpt,
            superfloppy_wrap::mbr_type_byte_for_fs(open.fs_hint.as_str()),
            alignment,
        ),
    };
    let mut form = WrapForm {
        synthesize: open.synthesize && !open.has_partition_table,
        table_kind,
        type_byte,
        size_mode: SizeMode::Original,
        custom_size_mib: (size_bytes / (1024 * 1024)).max(1) as u32,
        ..WrapForm::default()
    };
    form.alignment = match align {
        WrapAlignment::DosTraditional => WrapAlign::DosTraditional,
        WrapAlignment::Modern1MB => WrapAlign::Modern1MB,
        WrapAlignment::Custom(n) => {
            form.custom_alignment_sectors = n;
            WrapAlign::Custom
        }
    };
    let source = WriteSource {
        path: open.path,
        size_bytes,
        fs_hint: open.fs_hint,
        has_partition_table: open.has_partition_table,
    };
    (source, form)
}

/// The VBR's hidden-sectors field (offset 0x1C), or `None` when the first sector cannot be read.
fn hidden_sectors(path: &Path) -> Option<u32> {
    use std::io::Read;
    let mut f = std::fs::File::open(path).ok()?;
    let mut sector = [0u8; 512];
    f.read_exact(&mut sector).ok()?;
    Some(u32::from_le_bytes([
        sector[0x1C],
        sector[0x1D],
        sector[0x1E],
        sector[0x1F],
    ]))
}

/// Partitions on a write target, or empty when it has no readable table.
pub fn read_target_partitions(target: &Path) -> Vec<PartitionInfo> {
    let Ok(mut reader) =
        crate::model::source_reader::open_peeled_read_with_entry(target, None, None)
    else {
        return Vec::new();
    };
    match crate::partition::PartitionTable::detect(&mut reader) {
        Ok(table) => table
            .partitions()
            .into_iter()
            .filter(|p| !p.is_extended_container && p.size_bytes > 0)
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Where the write goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteTarget {
    pub path: PathBuf,
    /// The device's size, or an image file's current length.
    pub size_bytes: u64,
    /// A device's size is a hard limit; an image file grows to hold the source.
    pub is_device: bool,
}

/// Why the source does not fit the partition, for the form's warning line.
pub fn partition_fit_warning(source_size: u64, part: &PartitionInfo) -> Option<String> {
    (source_size > part.size_bytes).then(|| {
        format!(
            "Source is {} but the partition holds {}",
            format_size(source_size),
            format_size(part.size_bytes),
        )
    })
}

/// The form as a request for `physical_write_runner`, or why it cannot be written.
pub fn build_request(
    source: &WriteSource,
    form: &WrapForm,
    target: &WriteTarget,
    partition: Option<&PartitionInfo>,
) -> Result<PhysicalWriteRequest, String> {
    let image = PhysicalWriteSource::Image(source.path.clone());
    let target_size = target.size_bytes;
    // Writing into one partition leaves the table alone, so nothing is synthesized.
    if let Some(part) = partition {
        if source.size_bytes > part.size_bytes {
            return Err(format!(
                "Source is {} but partition {} holds only {}",
                format_size(source.size_bytes),
                part.index + 1,
                format_size(part.size_bytes),
            ));
        }
        return Ok(PhysicalWriteRequest {
            source: image,
            target_device_path: target.path.clone(),
            target_size_bytes: target_size,
            extent: WriteExtent::partition_at(part.byte_offset(), part.size_bytes),
            wrap: None,
        });
    }
    let capacity = if target.is_device {
        target_size
    } else {
        target_size.max(source.size_bytes)
    };
    let wrap = if form.synthesize {
        Some(wrap_params(source, form, target_size)?)
    } else {
        None
    };
    Ok(PhysicalWriteRequest {
        source: image,
        target_device_path: target.path.clone(),
        target_size_bytes: target_size,
        extent: WriteExtent::whole_disk(capacity),
        wrap,
    })
}

fn wrap_params(
    source: &WriteSource,
    form: &WrapForm,
    target_size: u64,
) -> Result<WrapParams, String> {
    let alignment = form.alignment();
    let first_byte = alignment.first_partition_lba() * 512;
    let partition_size = form.partition_size(source.size_bytes, target_size, first_byte);
    if partition_size < source.size_bytes {
        return Err(format!(
            "Partition size ({} bytes) is smaller than source filesystem ({} bytes)",
            partition_size, source.size_bytes,
        ));
    }
    if first_byte + partition_size > target_size {
        return Err(format!(
            "Partition end ({} bytes) exceeds target disk size ({} bytes)",
            first_byte + partition_size,
            target_size,
        ));
    }
    let table = match form.table_kind {
        TableKind::Mbr => WrapTable::Mbr {
            type_byte: form.type_byte,
            bootable: false,
            alignment,
        },
        TableKind::Gpt => {
            // An unbounded target makes the heuristic pick GPT and its type GUID for this fs.
            let gpt = superfloppy_wrap::suggest_wrap_params(
                source.fs_hint.as_str(),
                source.size_bytes,
                u64::MAX,
                None,
            );
            match gpt.table {
                WrapTable::Gpt {
                    type_guid, name, ..
                } => WrapTable::Gpt {
                    type_guid,
                    name,
                    alignment,
                },
                WrapTable::Mbr { .. } => return Err("No GPT type for this filesystem".into()),
            }
        }
    };
    Ok(WrapParams {
        table,
        partition_size_bytes: partition_size,
        target_size_bytes: target_size,
        source_fs_hint: source.fs_hint.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(size: u64, table: bool) -> WriteSource {
        WriteSource {
            path: PathBuf::from("/x.img"),
            size_bytes: size,
            fs_hint: "FAT16".into(),
            has_partition_table: table,
        }
    }

    fn device(size: u64) -> WriteTarget {
        WriteTarget {
            path: PathBuf::from("/dev/x"),
            size_bytes: size,
            is_device: true,
        }
    }

    #[test]
    fn a_plain_write_fills_the_device_and_an_image_file_grows() {
        let s = src(10 << 20, true);
        let f = WrapForm::default();
        let r = build_request(&s, &f, &device(64 << 20), None).unwrap();
        assert_eq!(r.extent, WriteExtent::whole_disk(64 << 20));
        assert!(r.wrap.is_none());
        let file = WriteTarget {
            path: PathBuf::from("/t.img"),
            size_bytes: 0,
            is_device: false,
        };
        let r = build_request(&s, &f, &file, None).unwrap();
        assert_eq!(r.extent, WriteExtent::whole_disk(10 << 20));
    }

    #[test]
    fn a_synthesized_table_must_fit_the_source_and_the_target() {
        let s = src(10 << 20, false);
        let mut f = WrapForm {
            synthesize: true,
            ..WrapForm::default()
        };
        let r = build_request(&s, &f, &device(64 << 20), None).unwrap();
        let wrap = r.wrap.unwrap();
        assert_eq!(wrap.partition_size_bytes, 10 << 20);
        assert_eq!(wrap.table.first_partition_lba(), 2048);
        let err = build_request(&s, &f, &device(10 << 20), None).unwrap_err();
        assert!(err.starts_with("Partition end"), "{err}");
        f.size_mode = SizeMode::Custom;
        f.custom_size_mib = 5;
        let err = build_request(&s, &f, &device(64 << 20), None).unwrap_err();
        assert!(err.contains("smaller than source"), "{err}");
        f.size_mode = SizeMode::FillRemaining;
        let wrap = build_request(&s, &f, &device(64 << 20), None)
            .unwrap()
            .wrap
            .unwrap();
        assert_eq!(wrap.partition_size_bytes, (64 << 20) - 2048 * 512);
    }

    #[test]
    fn a_partition_write_is_refused_when_the_source_is_larger() {
        let part = PartitionInfo {
            index: 1,
            type_name: "FAT16".into(),
            partition_type_byte: 6,
            start_lba: 2048,
            start_byte: None,
            size_bytes: 4 << 20,
            bootable: false,
            is_logical: false,
            is_extended_container: false,
            partition_type_string: None,
            hfs_block_size: None,
            rdb_part_block: None,
            drv_name: None,
        };
        let err = build_request(
            &src(10 << 20, true),
            &WrapForm::default(),
            &device(64 << 20),
            Some(&part),
        )
        .unwrap_err();
        assert!(err.contains("partition 2 holds only"), "{err}");
        assert!(partition_fit_warning(10 << 20, &part).is_some());
        let r = build_request(
            &src(1 << 20, true),
            &WrapForm::default(),
            &device(64 << 20),
            Some(&part),
        )
        .unwrap();
        assert_eq!(r.extent, WriteExtent::partition_at(2048 * 512, 4 << 20));
    }
}
