//! Restore planning rules shared by every restore front end.
//!
//! [`load_backup`] reads a backup folder (native `metadata.json`, a Clonezilla image, or a
//! `.cbk` unpacked to a temp folder) into a [`LoadedBackup`] with one [`SizeRow`] per partition
//! and its smallest safe size. The rest turn the user's choices into the engine's input:
//! [`partition_sizes`] (per-partition sizes, with "extend last partition" forcing Fill),
//! [`resolve_alignment`], [`ExpandPlan`] (trailing free space on an image target), [`layout_fit`]
//! (the engine's own layout projected before Start), and the single-partition and new-disk
//! configs. The layouts themselves (alignment preservation, the disk-label schemes'
//! `restore_patch`, the single-file CHD path) stay in `crate::restore`. `controller::restore`
//! drives these; see docs/phase2_commander_plan.md § "Slice 13".

use std::path::{Path, PathBuf};

use crate::backup::metadata::{BackupLayout, BackupMetadata, PartitionMetadata};
use crate::model::fs_limits::{caveats, Caveat, CaveatLevel, VolumeFacts, HEAD_BYTES};
use crate::model::size_mode::{SizeMode, CUSTOM_MIB_MAX};
use crate::restore::single::{
    NewDiskConfig, NewTableType, SinglePartitionRestoreConfig, SinglePartitionSource,
};
use crate::restore::{RestoreAlignment, RestorePartitionSize, RestoreSizeChoice};
#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
use disk_core::display::format_size;
use dsk_clonezilla::metadata::ClonezillaImage;

const MIB: u64 = 1024 * 1024;

/// The largest alignment, in sectors, the alignment fields take.
pub const ALIGN_SECTORS_MAX: u32 = 65535;

/// Where partitions start: the backup's own layout, 1 MB boundaries, or a sector multiple.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlignmentChoice {
    Original,
    Modern1MB,
    Custom,
}

/// One partition's restore size: what the backup recorded and what the user chose.
#[derive(Debug, Clone, PartialEq)]
pub struct SizeRow {
    /// The table slot, the key the engine matches on.
    pub index: usize,
    pub type_name: String,
    pub start_lba: u64,
    pub original_size: u64,
    pub minimum_size: u64,
    pub mode: SizeMode,
    pub custom_mib: u32,
    /// What the partition's own first bytes say it holds; `None` if unread or not in the table.
    pub facts: Option<VolumeFacts>,
}

impl SizeRow {
    fn new(index: usize, type_name: String, start_lba: u64, original: u64, minimum: u64) -> Self {
        SizeRow {
            index,
            type_name,
            start_lba,
            original_size: original,
            minimum_size: minimum,
            mode: SizeMode::Original,
            custom_mib: (original / MIB) as u32,
            facts: None,
        }
    }

    /// The size this row restores to; a Custom size outside [`Self::custom_bounds_mib`] is clamped.
    pub fn effective_size(&self) -> u64 {
        self.mode.effective_size(
            self.original_size,
            self.minimum_size,
            self.custom_mib_effective(),
        )
    }

    /// The Custom size in MiB once clamped into [`Self::custom_bounds_mib`].
    pub fn custom_mib_effective(&self) -> u32 {
        let (lo, hi) = self.custom_bounds_mib();
        self.custom_mib.clamp(lo, hi)
    }

    /// The size modes this row offers: Minimum below the original, Fill on the last row only.
    pub fn mode_choices(&self, is_last: bool) -> Vec<SizeMode> {
        let mut modes = vec![SizeMode::Original];
        if self.minimum_size < self.original_size {
            modes.push(SizeMode::Minimum);
        }
        modes.push(SizeMode::Custom);
        if is_last {
            modes.push(SizeMode::FillRemaining);
        }
        modes
    }

    /// A Custom size's range in MiB: the minimum (at least 1) up to 2 TiB.
    pub fn custom_bounds_mib(&self) -> (u32, u32) {
        let lo = (self.minimum_size / MIB).clamp(1, CUSTOM_MIB_MAX as u64) as u32;
        (lo, CUSTOM_MIB_MAX)
    }

    /// The engine's size choice for this row.
    pub fn choice(&self) -> RestoreSizeChoice {
        match self.mode {
            SizeMode::Original => RestoreSizeChoice::Original,
            SizeMode::Minimum => RestoreSizeChoice::Custom(self.minimum_size),
            SizeMode::MinPlus20 => RestoreSizeChoice::Custom(self.effective_size()),
            SizeMode::Custom => RestoreSizeChoice::Custom(self.custom_mib_effective() as u64 * MIB),
            SizeMode::FillRemaining => RestoreSizeChoice::FillRemaining,
        }
    }
}

/// A backup read for restoring.
pub struct LoadedBackup {
    /// The folder the engine reads (a `.cbk`'s temp folder for a container).
    pub folder: PathBuf,
    pub metadata: Option<BackupMetadata>,
    pub clonezilla: Option<ClonezillaImage>,
    pub rows: Vec<SizeRow>,
    /// The "Loaded backup: ..." line for the log.
    pub loaded_line: String,
    /// Keeps a `.cbk`'s unpacked folder alive.
    pub guard: Option<tempfile::TempDir>,
    /// The disk had a Mac driver and this backup did not keep it.
    pub mac_driver_missing: bool,
}

impl LoadedBackup {
    /// The source disk's size, the default size of an image target.
    pub fn source_size(&self) -> u64 {
        self.metadata
            .as_ref()
            .map(|m| m.source_size_bytes)
            .or_else(|| self.clonezilla.as_ref().map(|c| c.source_size_bytes))
            .unwrap_or(0)
    }
}

/// Read the backup at `path`; a `.cbk` file is unpacked to a temp folder first.
pub fn load_backup(path: &Path) -> Result<LoadedBackup, String> {
    let (folder, guard) = if path.is_file() && crate::rbformats::cbk::is_cbk(path) {
        let tmp = tempfile::TempDir::new().map_err(|e| format!("Cannot open .cbk: {e}"))?;
        crate::rbformats::cbk::materialize_cbk_to_folder(path, tmp.path())
            .map_err(|e| format!("Cannot open .cbk: {e}"))?;
        (tmp.path().to_path_buf(), Some(tmp))
    } else {
        (path.to_path_buf(), None)
    };
    let metadata_path = folder.join("metadata.json");
    let mut loaded = if metadata_path.exists() {
        load_native(folder.as_path(), metadata_path.as_path())?
    } else if let Some(cz) = dsk_clonezilla::metadata::resolve_clonezilla_folder(folder.as_path()) {
        load_clonezilla(folder.as_path(), cz.as_path())?
    } else {
        return Err("No backup found (metadata.json or Clonezilla image)".to_string());
    };
    loaded.guard = guard;
    Ok(loaded)
}

fn load_native(folder: &Path, metadata_path: &Path) -> Result<LoadedBackup, String> {
    let file = std::fs::File::open(metadata_path)
        .map_err(|e| format!("Cannot open metadata.json: {e}"))?;
    let metadata: BackupMetadata =
        serde_json::from_reader(file).map_err(|e| format!("Invalid metadata.json: {e}"))?;
    let rows = metadata
        .partitions
        .iter()
        .map(|pm| {
            // The smallest known-safe floor: defragmented, then in place, then the imaged size.
            let minimum = pm
                .defragmented_min_size_bytes
                .filter(|&s| s > 0)
                .or(pm.minimum_size_bytes.filter(|&s| s > 0))
                .unwrap_or(if pm.imaged_size_bytes > 0 {
                    pm.imaged_size_bytes
                } else {
                    pm.original_size_bytes
                });
            let mut row = SizeRow::new(
                pm.index,
                pm.type_name.clone(),
                pm.start_lba,
                pm.original_size_bytes,
                minimum,
            );
            row.facts = partition_head(folder, &metadata, pm).and_then(|head| {
                VolumeFacts::from_head(&head, pm.partition_type_string.as_deref())
            });
            row
        })
        .collect();
    let mac_driver_missing = mac_driver_missing(folder, &metadata);
    let loaded_line = format!(
        "Loaded backup: {} partition(s) from {}",
        metadata.partitions.len(),
        metadata.source_device,
    );
    Ok(LoadedBackup {
        folder: folder.to_path_buf(),
        metadata: Some(metadata),
        clonezilla: None,
        rows,
        loaded_line,
        guard: None,
        mac_driver_missing,
    })
}

/// A whole-disk CHD holds the driver itself; a per-partition backup needs the sidecar.
fn mac_driver_missing(folder: &Path, metadata: &BackupMetadata) -> bool {
    if metadata.partition_table_type != "APM" || metadata.layout == BackupLayout::SingleFileChd {
        return false;
    }
    std::fs::File::open(folder.join("apm.json"))
        .ok()
        .and_then(|f| serde_json::from_reader(f).ok())
        .is_some_and(|apm| crate::backup::apm_extras::driver_missing(folder, &apm))
}

/// The first [`HEAD_BYTES`] of a backed-up partition; `None` when they cannot be read.
fn partition_head(
    folder: &Path,
    metadata: &BackupMetadata,
    pm: &PartitionMetadata,
) -> Option<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    let mut head = Vec::with_capacity(HEAD_BYTES);
    if metadata.layout == BackupLayout::SingleFileChd {
        let chd = folder.join(metadata.container.as_deref()?);
        let mut reader = crate::rbformats::chd::ChdReader::open(&chd).ok()?;
        reader.seek(SeekFrom::Start(pm.byte_offset())).ok()?;
        reader.take(HEAD_BYTES as u64).read_to_end(&mut head).ok()?;
    } else {
        let members: Vec<PathBuf> = pm.compressed_files.iter().map(|f| folder.join(f)).collect();
        crate::rbformats::decompress_members_to_writer(
            &members,
            &metadata.compression_type,
            &mut head,
            Some(HEAD_BYTES as u64),
            &mut |_| {},
            &|| false,
            &mut |_| {},
        )
        .ok()?;
    }
    Some(head)
}

/// One caveat, and the partition it concerns (1-based, as `inspect` numbers them).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowCaveat {
    pub position: Option<usize>,
    pub caveat: Caveat,
}

impl RowCaveat {
    /// "Partition 2: ..." for a view or the log.
    pub fn line(&self) -> String {
        let text = crate::model::fs_limits::line(&self.caveat);
        match self.position {
            Some(n) => format!("Partition {n}: {text}"),
            None => text,
        }
    }
}

/// What a whole-disk restore of `loaded` at these sizes means for each volume.
pub fn plan_caveats(
    loaded: &LoadedBackup,
    alignment: &RestoreAlignment,
    sizes: &[RestorePartitionSize],
    target_size: u64,
    install_mac_driver: bool,
) -> Vec<RowCaveat> {
    // The layout is what knows how big Fill makes a row.
    let layout = loaded
        .metadata
        .as_ref()
        .filter(|_| target_size > 0)
        .and_then(|m| {
            crate::restore::calculate_restore_layout(m, alignment, sizes, target_size).ok()
        });
    let mut out = Vec::new();
    if loaded.mac_driver_missing {
        let text = if install_mac_driver {
            format!(
                "This backup did not keep the disk's Mac driver, so the bundled {} goes in its \
                 place; it covers SCSI disks, and an IDE Mac needs its own",
                crate::fs::mac_scsi_bless::BUILTIN_DRIVER_NAME
            )
        } else {
            "This backup did not keep the disk's Mac driver and none will be added, so a Mac \
             cannot start up from this disk"
                .to_string()
        };
        out.push(RowCaveat {
            position: None,
            caveat: Caveat {
                level: CaveatLevel::Warning,
                text,
            },
        });
    }
    for (pos, row) in loaded.rows.iter().enumerate() {
        let Some(facts) = row.facts else { continue };
        let size = layout
            .as_ref()
            .and_then(|l| l.iter().find(|o| o.index == row.index))
            .map_or_else(|| row.effective_size(), |o| o.export_size);
        out.extend(caveats(&facts, size).into_iter().map(|caveat| RowCaveat {
            position: Some(pos + 1),
            caveat,
        }));
    }
    out
}

/// What one volume means at `size`, for a single-partition or new-disk restore.
pub fn volume_caveats(facts: Option<VolumeFacts>, size: u64) -> Vec<RowCaveat> {
    facts
        .map(|f| caveats(&f, size))
        .unwrap_or_default()
        .into_iter()
        .map(|caveat| RowCaveat {
            position: None,
            caveat,
        })
        .collect()
}

/// The facts of an image file used as a partition source.
pub fn image_facts(path: &Path) -> Option<VolumeFacts> {
    use std::io::Read;
    let mut head = Vec::with_capacity(HEAD_BYTES);
    std::fs::File::open(path)
        .ok()?
        .take(HEAD_BYTES as u64)
        .read_to_end(&mut head)
        .ok()?;
    VolumeFacts::from_head(&head, None)
}

fn load_clonezilla(folder: &Path, cz_folder: &Path) -> Result<LoadedBackup, String> {
    let image = dsk_clonezilla::metadata::load(cz_folder)
        .map_err(|e| format!("Failed to load Clonezilla image: {e}"))?;
    let rows: Vec<SizeRow> = image
        .partitions
        .iter()
        .filter(|p| !p.is_extended)
        .map(|p| {
            // The partclone header knows how much is in use.
            let minimum = if p.partclone_files.is_empty() {
                p.size_bytes()
            } else {
                match dsk_clonezilla::partclone::read_partclone_header(p.partclone_files.as_slice())
                {
                    Ok(header) => header.used_size(),
                    Err(_) => p.size_bytes(),
                }
            };
            SizeRow::new(p.index, p.type_name(), p.start_lba, p.size_bytes(), minimum)
        })
        .collect();
    let loaded_line = format!(
        "Loaded Clonezilla image: {} partition(s) from /dev/{}",
        rows.len(),
        image.disk_name,
    );
    Ok(LoadedBackup {
        folder: folder.to_path_buf(),
        metadata: None,
        clonezilla: Some(image),
        rows,
        loaded_line,
        guard: None,
        mac_driver_missing: false,
    })
}

/// The two summary lines under the folder: what the backup holds and where it came from.
pub fn summary(loaded: &LoadedBackup) -> Option<(String, String)> {
    if let Some(meta) = &loaded.metadata {
        let date = &meta.created[..meta.created.len().min(10)];
        return Some((
            format!(
                "{} partition(s), {}, {}, {}",
                meta.partitions.len(),
                meta.partition_table_type,
                meta.compression_type,
                date,
            ),
            format!(
                "{} ({})",
                meta.source_device,
                format_size(meta.source_size_bytes)
            ),
        ));
    }
    let cz = loaded.clonezilla.as_ref()?;
    Some((
        format!(
            "Clonezilla image, {} partition(s), MBR",
            cz.partitions.len()
        ),
        format!(
            "/dev/{} ({})",
            cz.disk_name,
            format_size(cz.source_size_bytes)
        ),
    ))
}

/// The "Original" alignment radio's label.
pub fn original_alignment_label(loaded: &LoadedBackup) -> String {
    if let Some(meta) = &loaded.metadata {
        format!(
            "Original ({}, LBA {})",
            meta.alignment.detected_type, meta.alignment.first_partition_lba,
        )
    } else if let Some(cz) = &loaded.clonezilla {
        let first_lba = cz
            .partitions
            .iter()
            .filter(|p| !p.is_extended)
            .map(|p| p.start_lba)
            .min()
            .unwrap_or(63);
        format!("Original (LBA {first_lba})")
    } else {
        "Original".to_string()
    }
}

/// The alignment the engine takes for `choice`.
pub fn resolve_alignment(choice: AlignmentChoice, custom_sectors: u32) -> RestoreAlignment {
    match choice {
        AlignmentChoice::Original => RestoreAlignment::Original,
        AlignmentChoice::Modern1MB => RestoreAlignment::Modern1MB,
        AlignmentChoice::Custom => {
            RestoreAlignment::Custom(custom_sectors.clamp(1, ALIGN_SECTORS_MAX) as u64)
        }
    }
}

/// Trailing free space added to an image target (docs/disk_expansion.md).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExpandPlan {
    pub enabled: bool,
    pub free_mib: u32,
    /// Mode B: the last partition grows over the free space.
    pub extend_last: bool,
}

impl ExpandPlan {
    /// The bytes added; a device's size is fixed by the hardware.
    pub fn free_bytes(&self, target_is_device: bool) -> u64 {
        if self.enabled && !target_is_device {
            self.free_mib as u64 * MIB
        } else {
            0
        }
    }

    /// Mode B applies: the last partition fills the target.
    pub fn extends_last(&self, target_is_device: bool) -> bool {
        self.enabled && self.extend_last && !target_is_device
    }

    /// The largest free space offered, in MiB.
    pub fn cap_mib(source_size: u64) -> u32 {
        let source_mib = (source_size / MIB).max(1) as u32;
        source_mib.saturating_mul(8).max(source_mib + 8192)
    }
}

/// The engine's per-partition sizes; `extend_last` forces the last row to Fill.
pub fn partition_sizes(rows: &[SizeRow], extend_last: bool) -> Vec<RestorePartitionSize> {
    let last = rows.last().map(|r| r.index);
    rows.iter()
        .map(|r| RestorePartitionSize {
            index: r.index,
            size_choice: if extend_last && Some(r.index) == last {
                RestoreSizeChoice::FillRemaining
            } else {
                r.choice()
            },
        })
        .collect()
}

/// The sum of the rows' fixed sizes (a Fill row counts nothing).
pub fn fixed_total(rows: &[SizeRow]) -> u64 {
    rows.iter()
        .map(|r| {
            if r.mode == SizeMode::FillRemaining {
                0
            } else {
                r.effective_size()
            }
        })
        .sum()
}

/// Whether the engine's layout fits the target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LayoutFit {
    Fits,
    /// The layout ends past the target.
    Overrun {
        end: u64,
        target: u64,
    },
    /// The engine refused the layout; `original` says Original alignment caused it.
    Refused {
        message: String,
        original: bool,
    },
}

/// Project the layout the engine will build, so a misfit shows before Start.
pub fn layout_fit(
    metadata: &BackupMetadata,
    alignment: &RestoreAlignment,
    sizes: &[RestorePartitionSize],
    target_size: u64,
) -> LayoutFit {
    match crate::restore::calculate_restore_layout(metadata, alignment, sizes, target_size) {
        Err(e) => LayoutFit::Refused {
            message: format!("{e}"),
            original: matches!(alignment, RestoreAlignment::Original),
        },
        Ok(overrides) => {
            let end = overrides
                .iter()
                .map(|o| o.effective_start_lba() * 512 + o.export_size)
                .max()
                .unwrap_or(0);
            if end > target_size {
                LayoutFit::Overrun {
                    end,
                    target: target_size,
                }
            } else {
                LayoutFit::Fits
            }
        }
    }
}

/// A partition in the expansion preview bar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanSegment {
    pub label: String,
    pub fs: String,
    pub size_bytes: u64,
}

/// The backup's top-level partitions in disk order (logical ones live in their container).
pub fn plan_segments(loaded: &LoadedBackup) -> Vec<PlanSegment> {
    if let Some(meta) = &loaded.metadata {
        return meta
            .partitions
            .iter()
            .filter(|pm| !pm.is_logical)
            .enumerate()
            .map(|(k, pm)| PlanSegment {
                label: format!("Partition {}", k + 1),
                fs: pm.type_name.clone(),
                size_bytes: pm.original_size_bytes,
            })
            .collect();
    }
    let Some(cz) = &loaded.clonezilla else {
        return Vec::new();
    };
    cz.partitions
        .iter()
        .filter(|p| !p.is_logical && !p.is_extended)
        .map(|p| PlanSegment {
            label: format!("Partition {}", p.index + 1),
            fs: p.filesystem_type.clone(),
            size_bytes: p.size_sectors * 512,
        })
        .collect()
}

/// The first partition's alignment on a new disk.
pub fn new_disk_alignment_sectors(choice: AlignmentChoice, custom: u32) -> u64 {
    match choice {
        AlignmentChoice::Original => 63,
        AlignmentChoice::Modern1MB => 2048,
        AlignmentChoice::Custom => custom.clamp(1, ALIGN_SECTORS_MAX) as u64,
    }
}

/// Where the partition starts on a new disk: GPT needs LBAs 0-33 before it.
pub fn new_disk_start_lba(table: NewTableType, alignment_sectors: u64) -> u64 {
    match table {
        NewTableType::Gpt => {
            let min_lba = 34u64;
            if alignment_sectors > 0 && !min_lba.is_multiple_of(alignment_sectors) {
                min_lba + alignment_sectors - min_lba % alignment_sectors
            } else {
                min_lba
            }
        }
        _ => alignment_sectors,
    }
}

/// The source of a single-partition or new-disk restore.
pub enum PartitionSource<'a> {
    Image(&'a Path),
    Backup {
        folder: &'a Path,
        metadata: Option<&'a BackupMetadata>,
        index: usize,
    },
}

impl<'a> PartitionSource<'a> {
    fn engine(&self) -> SinglePartitionSource {
        match self {
            PartitionSource::Image(path) => SinglePartitionSource::ImageFile {
                path: path.to_path_buf(),
            },
            PartitionSource::Backup { folder, index, .. } => SinglePartitionSource::Backup {
                folder: folder.to_path_buf(),
                partition_index: *index,
            },
        }
    }

    fn partition(&self) -> Option<&'a crate::backup::metadata::PartitionMetadata> {
        match self {
            PartitionSource::Backup {
                metadata: Some(m),
                index,
                ..
            } => m.partitions.iter().find(|p| p.index == *index),
            _ => None,
        }
    }

    fn start_lba(&self) -> u64 {
        self.partition().map(|p| p.start_lba).unwrap_or(0)
    }

    /// The bytes to write: the partition's imaged size, or the image file's length.
    pub fn data_size(&self) -> Option<u64> {
        match self {
            PartitionSource::Image(path) => std::fs::metadata(path).ok().map(|m| m.len()),
            PartitionSource::Backup { .. } => self.partition().map(|p| p.imaged_size_bytes),
        }
    }
}

/// Overwrite one partition of a device in place; the table is left alone.
pub fn single_partition_config(
    source: &PartitionSource<'_>,
    device: &Path,
    start_lba: u64,
    size_bytes: u64,
) -> SinglePartitionRestoreConfig {
    SinglePartitionRestoreConfig {
        source: source.engine(),
        target_path: device.to_path_buf(),
        target_is_device: true,
        target_offset_bytes: start_lba * 512,
        target_size_bytes: Some(size_bytes),
        target_start_lba: start_lba,
        source_start_lba: source.start_lba(),
        new_disk: None,
    }
}

/// The new-disk form's choices.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NewDiskChoice {
    pub table: NewTableType,
    pub alignment: AlignmentChoice,
    pub custom_alignment: u32,
    pub bootable: bool,
}

/// Lay a fresh one-partition table on `target` and write the partition into it.
pub fn new_disk_config(
    source: &PartitionSource<'_>,
    target: &Path,
    target_is_device: bool,
    device_size: Option<u64>,
    choice: &NewDiskChoice,
) -> SinglePartitionRestoreConfig {
    // An image target is sized from the source plus room for the alignment.
    let disk_size_bytes = match (target_is_device, device_size) {
        (true, Some(n)) => n,
        _ => source.data_size().unwrap_or(0) + 2048 * 512,
    };
    let alignment_sectors = new_disk_alignment_sectors(choice.alignment, choice.custom_alignment);
    let start_lba = new_disk_start_lba(choice.table, alignment_sectors);
    let part = source.partition();
    SinglePartitionRestoreConfig {
        source: source.engine(),
        target_path: target.to_path_buf(),
        target_is_device,
        target_offset_bytes: start_lba * 512,
        target_size_bytes: None,
        target_start_lba: start_lba,
        source_start_lba: source.start_lba(),
        new_disk: Some(NewDiskConfig {
            table_type: choice.table,
            alignment_sectors,
            // FAT32 LBA when the source does not say.
            partition_type_byte: part.map(|p| p.partition_type_byte).unwrap_or(0x0C),
            partition_type_guid: None,
            partition_type_string: part.and_then(|p| p.partition_type_string.clone()),
            bootable: choice.bootable,
            disk_size_bytes,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(index: usize, original: u64, minimum: u64, mode: SizeMode) -> SizeRow {
        let mut r = SizeRow::new(index, "FAT".into(), 63, original, minimum);
        r.mode = mode;
        r
    }

    #[test]
    fn sizes_map_to_the_engine_choice() {
        let rows = vec![
            row(0, 100 * MIB, 10 * MIB, SizeMode::Minimum),
            row(1, 100 * MIB, 10 * MIB, SizeMode::Original),
        ];
        let sizes = partition_sizes(rows.as_slice(), false);
        assert!(matches!(sizes[0].size_choice, RestoreSizeChoice::Custom(n) if n == 10 * MIB));
        assert!(matches!(sizes[1].size_choice, RestoreSizeChoice::Original));
        let sizes = partition_sizes(rows.as_slice(), true);
        assert!(matches!(
            sizes[1].size_choice,
            RestoreSizeChoice::FillRemaining
        ));
        assert_eq!(fixed_total(rows.as_slice()), 110 * MIB);
    }

    #[test]
    fn expansion_is_for_image_targets_only() {
        let plan = ExpandPlan {
            enabled: true,
            free_mib: 4,
            extend_last: true,
        };
        assert_eq!(plan.free_bytes(false), 4 * MIB);
        assert_eq!(plan.free_bytes(true), 0);
        assert!(plan.extends_last(false));
        assert!(!plan.extends_last(true));
        assert_eq!(ExpandPlan::cap_mib(100 * MIB), 100 + 8192);
    }

    #[test]
    fn a_gpt_new_disk_starts_past_its_entries() {
        assert_eq!(new_disk_start_lba(NewTableType::Gpt, 2048), 2048);
        assert_eq!(new_disk_start_lba(NewTableType::Gpt, 63), 63);
        assert_eq!(new_disk_start_lba(NewTableType::Gpt, 8), 40);
        assert_eq!(new_disk_start_lba(NewTableType::Mbr, 63), 63);
    }

    #[test]
    fn a_folder_without_a_backup_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let err = load_backup(dir.path()).err().unwrap();
        assert_eq!(err, "No backup found (metadata.json or Clonezilla image)");
    }
}
