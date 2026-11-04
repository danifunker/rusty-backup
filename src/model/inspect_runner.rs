//! The Inspect tab's open: read a disk, image or remote image's partition table and probe each partition.
//!
//! [`inspect_source`] runs inside a job. It opens the source (elevated for a device or plain image,
//! streamed for a container, over the block tier for a remote image), detects the image format and
//! the partition table, and probes each partition for what the grid shows: the HFS variant and
//! label on an APM disk, the real filesystem behind an MBR / GPT Linux type, the HFS allocation block
//! size, volume labels (FAT, AFFS, PFS3, SFS) and cheap minimum sizes (expensive ones come back
//! deferred). A table-less image that is a browsable optical disc comes back as
//! [`Inspected::Optical`] so the shell can hand it to the Optical tab.

use std::collections::HashMap;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::fs::Filesystem;
use crate::model::job::JobCtx;
use crate::partition::{detect_alignment, PartitionAlignment, PartitionInfo, PartitionTable};
use crate::rbformats::ReadSeek;

/// A remote image open on an `rb-cli serve` daemon: the shared connection and the daemon-side path.
#[cfg(feature = "remote")]
pub type RemoteImage = (
    std::sync::Arc<std::sync::Mutex<crate::remote::RemoteConnection>>,
    String,
);

/// What to inspect.
pub struct InspectRequest {
    /// The device or image path; for a remote image, the daemon-side path (for messages only).
    pub path: PathBuf,
    #[cfg(feature = "remote")]
    pub remote: Option<RemoteImage>,
}

impl InspectRequest {
    pub fn local(path: PathBuf) -> Self {
        InspectRequest {
            path,
            #[cfg(feature = "remote")]
            remote: None,
        }
    }

    fn is_remote(&self) -> bool {
        #[cfg(feature = "remote")]
        {
            self.remote.is_some()
        }
        #[cfg(not(feature = "remote"))]
        {
            false
        }
    }

    /// A fresh block reader on the remote image; `None` for a local source or when it fails.
    fn remote_reader(&self) -> Option<anyhow::Result<Box<dyn ReadSeek>>> {
        #[cfg(feature = "remote")]
        {
            let (conn, rpath) = self.remote.as_ref()?;
            Some(
                crate::remote::RemoteBlockReader::open(std::sync::Arc::clone(conn), rpath.as_str())
                    .map(|r| Box::new(r) as Box<dyn ReadSeek>),
            )
        }
        #[cfg(not(feature = "remote"))]
        {
            None
        }
    }
}

/// A source's partition table and what the probes found.
pub struct InspectOutcome {
    pub table: PartitionTable,
    pub alignment: PartitionAlignment,
    pub partitions: Vec<PartitionInfo>,
    pub min_sizes: HashMap<usize, u64>,
    pub defrag_min_sizes: HashMap<usize, u64>,
    pub volume_labels: HashMap<usize, String>,
    /// Partitions whose minimum needs a filesystem walk, with the filesystem's name.
    pub deferred_min_sizes: HashMap<usize, &'static str>,
    /// The detected image format ("Fixed VHD", "Norton Ghost (GHO)", ...).
    pub format_label: Option<String>,
    /// macOS only: the elevated device handle and its claim, kept so a browse never re-prompts.
    pub device_file: Option<crate::os::SourceHandle>,
    pub device_guard: Option<crate::os::TempFileGuard>,
}

pub enum Inspected {
    Disk(Box<InspectOutcome>),
    /// No partition table, but an optical disc the Optical tab can browse.
    Optical(PathBuf),
}

/// The label a streamed container shows for its extension.
fn streaming_label(ext: Option<&str>) -> &'static str {
    match ext {
        Some("gho") | Some("ghs") => "Norton Ghost (GHO)",
        Some("imz") => "WinImage (IMZ)",
        Some("msa") => "Atari ST MSA floppy",
        Some("dsk") => "CPCEMU DSK/EDSK floppy",
        Some("d88") => "Sharp .d88 floppy",
        Some("dim") => "DiskExplorer DIM floppy",
        Some("xdf") => "X68000 XDF floppy",
        Some("hdm") => "PC-98 HDM floppy",
        Some("hdf") => "Acorn ADFS image",
        Some("do") | Some("po") => "Apple II floppy",
        _ => "Streaming image",
    }
}

/// Inspect `req`; `log` receives the lines the Inspect log shows as they happen.
pub fn inspect_source(
    req: &InspectRequest,
    job: &JobCtx,
    log: &dyn Fn(String),
) -> Result<Inspected, String> {
    let path = req.path.as_path();
    let remote = req.is_remote();
    let is_device = !remote
        && (path.to_string_lossy().starts_with("/dev/")
            || path.to_string_lossy().starts_with("\\\\.\\"));
    // Containers decode to a flat stream without elevation; CHD keeps its own detection below.
    let uses_streaming_reader = !remote
        && !is_device
        && crate::model::source_reader::is_container_path(path)
        && !crate::model::source_reader::is_chd_path(path);

    job.set_phase("Requesting exclusive disk access...");
    // The guard holds the volume locks / disk claim for the whole inspect.
    let (device_file, guard) = if remote || uses_streaming_reader {
        (None, None)
    } else {
        let elevated = crate::os::open_source_for_reading(path)
            .map_err(|e| format!("Cannot open {}: {e:#}", path.display()))?;
        let (f, g) = elevated.into_parts();
        (Some(f), Some(g))
    };

    job.set_phase("Reading partition table...");
    let mut reader: Box<dyn ReadSeek> = if let Some(r) = req.remote_reader() {
        r.map_err(|e| format!("Cannot open remote image {}: {e:#}", path.display()))?
    } else if uses_streaming_reader {
        crate::model::source_reader::open_read(path)
            .map_err(|e| format!("Cannot open {}: {e:#}", path.display()))?
    } else {
        let f = device_file
            .as_ref()
            .ok_or_else(|| format!("Cannot open {}", path.display()))?
            .try_clone()
            .map_err(|e| format!("Cannot clone file handle: {e}"))?;
        if is_device {
            // A device cannot answer seek(End); carry the length the OS reports instead.
            Box::new(crate::os::known_len_source(f, path))
        } else {
            Box::new(BufReader::new(f))
        }
    };

    let mut format_label = None;
    let detect_result = if remote || is_device {
        PartitionTable::detect(&mut reader)
    } else if uses_streaming_reader {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|s| s.to_ascii_lowercase());
        let label = streaming_label(ext.as_deref());
        log(format!("Detected format: {}", label));
        if matches!(ext.as_deref(), Some("gho") | Some("ghs")) {
            if let Ok(info) = dsk_ghost::gho::format_gho_info(path) {
                for line in info.lines() {
                    if !line.is_empty() {
                        log(line.to_string());
                    }
                }
            }
            job.set_phase("Reconstructing partitions from Ghost image...");
        }
        format_label = Some(label.to_string());
        PartitionTable::detect(&mut reader)
    } else {
        let handle = device_file
            .as_ref()
            .expect("an image file is opened elevated");
        match handle.try_clone().and_then(|h| h.into_file()) {
            Ok(clone) => match crate::rbformats::detect_image_format_with_path(clone, Some(path)) {
                Ok(format) => {
                    let desc = format.description();
                    log(format!("Detected format: {}", desc));
                    format_label = Some(desc.clone());
                    let file = handle
                        .try_clone()
                        .and_then(|h| h.into_file())
                        .or_else(|_| std::fs::File::open(path))
                        .map_err(|e| format!("Cannot open {}: {e}", path.display()))?;
                    match crate::rbformats::wrap_image_reader(file, format) {
                        Ok((mut wrapped, _)) => PartitionTable::detect(&mut wrapped),
                        Err(e) => {
                            log(format!("Format wrap failed, trying raw: {e}"));
                            let _ = reader.seek(SeekFrom::Start(0));
                            PartitionTable::detect(&mut reader)
                        }
                    }
                }
                Err(e) => {
                    log(format!("Format detection failed, trying raw: {e}"));
                    PartitionTable::detect(&mut reader)
                }
            },
            Err(_) => PartitionTable::detect(&mut reader),
        }
    };

    let mut table = match detect_result {
        Ok(t) => t,
        Err(e) => {
            if !remote && crate::cli::optical_hint::is_browsable_optical(path) {
                return Ok(Inspected::Optical(path.to_path_buf()));
            }
            return Err(no_table(req, e));
        }
    };

    // seek(End(0)) returns 0 for macOS devices; take the size the OS reports.
    if let PartitionTable::None { size_bytes, .. } = &mut table {
        if *size_bytes == 0 {
            if let Some(real) = device_file.as_ref().and_then(|df| df.byte_len(path)) {
                *size_bytes = real;
            }
        }
    }

    let alignment = detect_alignment(&table);
    let mut partitions = table.partitions();
    let is_chd = !remote && crate::model::source_reader::is_chd_path(path);
    // A raw .chd is compressed at partition offsets, so probes read through a fresh decoder.
    let probe = || -> Option<Box<dyn ReadSeek>> {
        if let Some(r) = req.remote_reader() {
            r.ok()
        } else if uses_streaming_reader || is_chd {
            crate::model::source_reader::open_read(path).ok()
        } else {
            device_file
                .as_ref()?
                .try_clone()
                .ok()
                .map(|f| Box::new(BufReader::new(f)) as Box<dyn ReadSeek>)
        }
    };

    if matches!(table, PartitionTable::Apm(_)) {
        relabel_apm_hfs(&mut partitions, &probe);
    }
    relabel_linux_types(&mut partitions, &probe);
    probe_hfs_block_sizes(&mut partitions, &table, &probe);

    if let PartitionTable::None { fs_hint, .. } = &table {
        log(format!(
            "Warning: image has no partition table (no MBR/GPT/APM/SGI signature). Detected bare {} filesystem at sector 0; synthesizing a single-partition view in memory so the volume can be browsed. The source file is not modified.",
            fs_hint
        ));
    } else {
        let order = table
            .byte_order_name()
            .map(|o| format!(", {o} 16-bit word order"))
            .unwrap_or_default();
        log(format!(
            "Detected {} partition table with {} partition(s){order}",
            table.type_name(),
            partitions.len()
        ));
    }
    log(format!(
        "Alignment: {} (first LBA: {})",
        alignment.alignment_type, alignment.first_lba
    ));

    let volume_labels = probe_volume_labels(&partitions, &table, &probe);

    job.set_phase("Analyzing filesystem sizes...");
    let mut min_sizes = HashMap::new();
    let mut defrag_min_sizes = HashMap::new();
    let mut deferred_min_sizes = HashMap::new();
    let is_gho = path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("gho") || e.eq_ignore_ascii_case("ghs"));
    for part in &partitions {
        if part.is_extended_container || part.partition_type_byte == 0xEE {
            continue;
        }
        let min = |r: Box<dyn ReadSeek>| {
            crate::fs::partition_minimum_size(
                r,
                part.byte_offset(),
                part.partition_type_byte,
                part.partition_type_string.as_deref(),
                part.size_bytes,
                false,
                None,
                &|_| {},
            )
        };
        let result = if let Some(r) = req.remote_reader() {
            match r {
                Ok(r) => min(r),
                Err(_) => continue,
            }
        } else if (uses_streaming_reader || is_chd) && is_gho {
            // A compressed NTFS inside a GHO decompresses the whole file to measure; defer it.
            crate::fs::MinimumResult::Deferred {
                fs_name: crate::fs::fs_name_for(
                    part.partition_type_byte,
                    part.partition_type_string.as_deref(),
                ),
            }
        } else if uses_streaming_reader || is_chd {
            match crate::model::source_reader::open_read(path) {
                Ok(r) => min(r),
                Err(_) => continue,
            }
        } else {
            let Some(Ok(f)) = device_file.as_ref().map(|d| d.try_clone()) else {
                continue;
            };
            if is_device {
                // SectorAlignedReader delegates End to inner, so wrap first.
                min(Box::new(crate::os::SectorAlignedReader::new(
                    crate::os::known_len_source(f, path),
                )))
            } else {
                min(Box::new(BufReader::new(f)))
            }
        };
        match result {
            crate::fs::MinimumResult::Computed {
                in_place: Some(min_size),
                defragmented,
                ..
            } => {
                let clamped = min_size.min(part.size_bytes);
                min_sizes.insert(part.index, clamped);
                if let Some(d) = defragmented {
                    let d_clamped = d.min(part.size_bytes);
                    if d_clamped < clamped {
                        defrag_min_sizes.insert(part.index, d_clamped);
                    }
                }
                log(format!(
                    "Partition {}: minimum size {} (original {})",
                    part.index,
                    disk_core::display::format_size(clamped),
                    disk_core::display::format_size(part.size_bytes),
                ));
            }
            crate::fs::MinimumResult::Computed { in_place: None, .. } => {}
            crate::fs::MinimumResult::Deferred { fs_name } => {
                log(format!(
                    "Partition {}: need to calculate minimum size due to filesystem {fs_name} (click \"Calc min\" to compute)",
                    part.index,
                ));
                deferred_min_sizes.insert(part.index, fs_name);
            }
        }
    }

    // Only macOS hands the elevated handle back: elsewhere File::open works for the browse.
    #[cfg(target_os = "macos")]
    let (device_file, device_guard) = (device_file, guard);
    #[cfg(not(target_os = "macos"))]
    let (device_file, device_guard) = {
        drop((device_file, guard));
        (None, None)
    };
    Ok(Inspected::Disk(Box::new(InspectOutcome {
        table,
        alignment,
        partitions,
        min_sizes,
        defrag_min_sizes,
        volume_labels,
        deferred_min_sizes,
        format_label,
        device_file,
        device_guard,
    })))
}

/// The parse error, with the NKit or optical hint for a local disc image.
fn no_table(req: &InspectRequest, e: impl std::fmt::Display) -> String {
    let base = anyhow::anyhow!("Failed to parse partition table: {e}");
    if req.is_remote() {
        return format!("{base:#}");
    }
    let path = req.path.as_path();
    let hinted = if crate::cli::optical_hint::is_nkit_image(path) {
        crate::cli::optical_hint::with_nkit_hint(base, path)
    } else {
        crate::cli::optical_hint::with_optical_hint(base, path)
    };
    format!("{hinted:#}")
}

type Probe<'a> = dyn Fn() -> Option<Box<dyn ReadSeek>> + 'a;

/// Name APM `Apple_HFS` / `Apple_HFSX` rows by their HFS variant and volume label.
fn relabel_apm_hfs(partitions: &mut [PartitionInfo], probe: &Probe) {
    for part in partitions.iter_mut() {
        let Some(type_string) = part.partition_type_string.clone() else {
            continue;
        };
        let type_string = type_string.as_str();
        if type_string != "Apple_HFS" && type_string != "Apple_HFSX" {
            continue;
        }
        // The APM name from the "Apple_HFS (NAME)" form `partitions()` builds.
        let apm_name = part
            .type_name
            .strip_prefix(type_string)
            .and_then(|s| s.trim_start().strip_prefix('('))
            .and_then(|s| s.strip_suffix(')'))
            .unwrap_or("")
            .to_string();
        let part_offset = part.byte_offset();
        let detected = probe()
            .map(|mut br| crate::fs::probe_apple_hfs_type(&mut br, part_offset).to_string())
            .unwrap_or_default();
        let variant_tag: Option<&'static str> = match detected.as_str() {
            "HFS+" => Some("HFS+"),
            "HFSX" => Some("HFSX"),
            _ => None,
        };
        let label = if variant_tag.is_some() {
            probe().and_then(|mut br| {
                fs_mac::hfsplus::probe_hfsplus_volume_label(&mut br, part_offset)
            })
        } else {
            None
        };
        // macOS Disk Utility writes "Apple_HFS_Untitled_N" when no name was given; that says nothing.
        let apm_is_default = apm_name.starts_with("Apple_HFS_Untitled");
        let display_name = match (label.as_deref(), apm_name.as_str()) {
            (Some(lab), _) if !lab.is_empty() => lab.to_string(),
            (_, n) if !n.is_empty() && !apm_is_default => n.to_string(),
            _ => String::new(),
        };
        let trailing_apm = match (label.as_deref(), apm_name.as_str()) {
            (Some(lab), n) if !lab.is_empty() && !n.is_empty() && !apm_is_default && lab != n => {
                Some(n.to_string())
            }
            _ => None,
        };
        let mut new_name = match variant_tag {
            Some(tag) if tag == "HFSX" && type_string == "Apple_HFSX" => type_string.to_string(),
            Some(tag) => format!("{type_string} ({tag})"),
            None => type_string.to_string(),
        };
        if !display_name.is_empty() {
            new_name.push(' ');
            new_name.push_str(display_name.as_str());
        }
        if let Some(extra) = trailing_apm {
            new_name.push_str(&format!(" [APM: {extra}]"));
        }
        part.type_name = new_name;
    }
}

/// Name MBR 0x83 / GPT Linux rows by the filesystem really there; FAT at 0x83 is an MSX disk.
fn relabel_linux_types(partitions: &mut [PartitionInfo], probe: &Probe) {
    for part in partitions.iter_mut() {
        if part.is_extended_container {
            continue;
        }
        let is_linux_mbr = part.partition_type_byte == 0x83;
        let is_linux_gpt =
            part.partition_type_string.as_deref() == Some("0FC63DAF-8483-4772-8E79-3D69D8477DE4");
        if !is_linux_mbr && !is_linux_gpt {
            continue;
        }
        let Some(mut br) = probe() else {
            continue;
        };
        match crate::fs::probe_0x83_fs_type(&mut br, part.byte_offset()) {
            Some("FAT") if is_linux_mbr => {
                let offset = part.byte_offset();
                let subtype = probe().and_then(|br2| {
                    fs_pc::fat::FatFilesystem::open(br2, offset)
                        .ok()
                        .map(|fs| fs.fs_type().to_string())
                });
                part.type_name = match subtype {
                    Some(t) => format!("{t} (MSX)"),
                    None => "FAT (MSX)".to_string(),
                };
            }
            Some(other) => part.type_name = other.to_string(),
            None => {}
        }
    }
}

/// The HFS / HFS+ allocation block size, for APM Apple_HFS, MBR 0xAF and HFS superfloppies.
fn probe_hfs_block_sizes(partitions: &mut [PartitionInfo], table: &PartitionTable, probe: &Probe) {
    let hfs_superfloppy = matches!(
        table,
        PartitionTable::None { fs_hint, .. } if fs_hint == "HFS" || fs_hint == "HFS+"
    );
    for part in partitions.iter_mut() {
        if part.is_extended_container {
            continue;
        }
        let is_hfs_apm = part
            .partition_type_string
            .as_deref()
            .is_some_and(|s| s.eq_ignore_ascii_case("Apple_HFS"));
        if !(is_hfs_apm || part.partition_type_byte == 0xAF || hfs_superfloppy) {
            continue;
        }
        if let Some(mut br) = probe() {
            part.hfs_block_size = crate::fs::hfs_block_size_at_offset(&mut br, part.byte_offset());
        }
    }
}

/// Volume labels from boot / root block reads; an Amiga row shows "DH0 - Workbench".
fn probe_volume_labels(
    partitions: &[PartitionInfo],
    table: &PartitionTable,
    probe: &Probe,
) -> HashMap<usize, String> {
    let fat_superfloppy = matches!(table, PartitionTable::None { fs_hint, .. } if fs_hint == "FAT");
    let mut labels = HashMap::new();
    for part in partitions {
        if part.is_extended_container {
            continue;
        }
        let is_fat_byte = matches!(
            part.partition_type_byte,
            0x01 | 0x04 | 0x06 | 0x0B | 0x0C | 0x0E | 0x14 | 0x16 | 0x1B | 0x1C | 0x1E
        );
        let ts = part.partition_type_string.as_deref();
        let offset = part.byte_offset();
        let label: Option<String> = if is_fat_byte || fat_superfloppy {
            probe().and_then(|br| {
                fs_pc::fat::FatFilesystem::open(br, offset)
                    .ok()
                    .and_then(|fs| fs.volume_label().map(str::to_string))
            })
        } else if ts.is_some_and(crate::fs::is_amiga_dos_type) {
            probe().and_then(|br| {
                fs_amiga::affs::AffsFilesystem::open(br, offset)
                    .ok()
                    .and_then(|fs| fs.volume_label().map(str::to_string))
            })
        } else if ts.is_some_and(crate::fs::is_amiga_pfs3_type) {
            probe().and_then(|br| {
                fs_amiga::pfs3::Pfs3Filesystem::open(br, offset)
                    .ok()
                    .and_then(|fs| fs.volume_label().map(str::to_string))
            })
        } else if ts.is_some_and(crate::fs::is_amiga_sfs_type) {
            probe().and_then(|br| {
                fs_amiga::sfs::SfsFilesystem::open(br, offset)
                    .ok()
                    .and_then(|fs| fs.volume_label().map(str::to_string))
            })
        } else {
            None
        };
        let combined = match (part.drv_name.as_deref(), label.as_deref()) {
            (Some(drv), Some(vol)) if !drv.is_empty() && !vol.trim().is_empty() => {
                Some(format!("{drv} - {}", vol.trim()))
            }
            (Some(drv), _) if !drv.is_empty() => Some(drv.to_string()),
            (_, Some(vol)) if !vol.trim().is_empty() => Some(vol.trim().to_string()),
            _ => None,
        };
        if let Some(label) = combined {
            labels.insert(part.index, label);
        }
    }
    labels
}

/// Apply a probed volume label to a partition's type name, dropping the APM noise older backups carry.
pub fn apply_volume_label(type_name: &str, label: &str) -> String {
    let trimmed = strip_apm_noise_parenthetical(type_name);
    if trimmed.trim_end().ends_with(label) {
        trimmed
    } else {
        format!("{} {}", trimmed.trim_end(), label)
    }
}

/// `Apple_HFS (HFS+) (Untitled_1)` -> `Apple_HFS (HFS+)`; non-HFS names are left alone.
fn strip_apm_noise_parenthetical(s: &str) -> String {
    for tag in ["(HFS+)", "(HFSX)", "(HFS)"] {
        if let Some(idx) = s.find(tag) {
            return s[..idx + tag.len()].to_string();
        }
    }
    s.to_string()
}

/// Largest floppy container: 1.44 MB plus D88's per-sector overhead, with headroom; no HDD is this small.
pub const MAX_FLOPPY_CONTAINER_BYTES: u64 = 2 * 1024 * 1024;

/// True when `path` is a floppy container (XDF / HDM / DIM / D88) the floppy converter takes.
/// The size gate keeps a zeroed-header HDD image (an X68000 `.hdf`) off the loose D88 sniffer.
pub fn is_floppy_container_file(path: &Path) -> bool {
    match std::fs::metadata(path) {
        Ok(m) if m.len() <= MAX_FLOPPY_CONTAINER_BYTES => {}
        _ => return false,
    }
    let mut head = [0u8; 256];
    let n = match std::fs::File::open(path).and_then(|mut f| std::io::Read::read(&mut f, &mut head))
    {
        Ok(n) => n,
        Err(_) => return false,
    };
    let kind = crate::rbformats::containers::detect_container_kind(&head[..n], Some(path));
    crate::rbformats::containers::is_floppy_container(kind)
}

/// One partition of a table read by [`read_table_unelevated`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainPartition {
    pub type_name: String,
    /// The table's own type string (an Amiga DosType, an APM type), when it has one.
    pub type_string: Option<String>,
    pub size_bytes: u64,
}

/// A table read with a plain open: its name and partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainTable {
    pub type_name: String,
    pub partitions: Vec<PlainPartition>,
}

/// The size of `path`; a block device reports 0 in its metadata, so seek to its end instead.
pub fn size_unelevated(path: &Path) -> std::io::Result<u64> {
    let m = std::fs::metadata(path)?;
    if m.len() > 0 {
        return Ok(m.len());
    }
    let mut f = std::fs::File::open(path)?;
    f.seek(SeekFrom::End(0))
}

/// Read `path`'s partition table with a plain open (never elevates); `None` when it has none.
pub fn read_table_unelevated(path: &Path) -> std::io::Result<Option<PlainTable>> {
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    Ok(PartitionTable::detect(&mut reader)
        .ok()
        .map(|pt| PlainTable {
            type_name: pt.type_name().to_string(),
            partitions: pt
                .partitions()
                .iter()
                .map(|p| PlainPartition {
                    type_name: p.type_name.clone(),
                    type_string: p.partition_type_string.clone(),
                    size_bytes: p.size_bytes,
                })
                .collect(),
        }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_volume_label_strips_legacy_apm_garbage() {
        assert_eq!(
            apply_volume_label(
                "Apple_HFS (HFS+) (Apple_HFS (HFS+)_Untitled_1)",
                "Ariel-backup",
            ),
            "Apple_HFS (HFS+) Ariel-backup",
        );
    }

    #[test]
    fn apply_volume_label_appends_to_clean_name() {
        assert_eq!(
            apply_volume_label("Apple_HFS (HFS+)", "Ariel-backup"),
            "Apple_HFS (HFS+) Ariel-backup",
        );
    }

    #[test]
    fn apply_volume_label_idempotent_when_already_present() {
        assert_eq!(
            apply_volume_label("Apple_HFS (HFS+) Ariel-backup", "Ariel-backup"),
            "Apple_HFS (HFS+) Ariel-backup",
        );
    }

    #[test]
    fn apply_volume_label_handles_hfsx_and_classic_hfs() {
        assert_eq!(
            apply_volume_label("Apple_HFS (HFSX) (Untitled_2)", "Foo"),
            "Apple_HFS (HFSX) Foo",
        );
        assert_eq!(
            apply_volume_label("Apple_HFS (HFS) (Old)", "Bar"),
            "Apple_HFS (HFS) Bar",
        );
    }

    #[test]
    fn apply_volume_label_non_hfs_left_alone() {
        assert_eq!(apply_volume_label("Linux", "label"), "Linux label");
    }

    /// A 10 MB X68000 `.hdf` with a zeroed header trips the loose D88 sniffer but is no floppy.
    #[test]
    fn a_zeroed_hdf_is_not_a_floppy_container() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("Populous.hdf");
        let mut bytes = vec![0u8; 10 * 1024 * 1024];
        bytes[..4].copy_from_slice(&[0x60, 0x00, 0x00, 0xca]);
        std::fs::write(&path, &bytes).unwrap();
        assert!(!is_floppy_container_file(&path));
    }

    #[test]
    fn a_floppy_sized_xdf_is_a_floppy_container() {
        use dsk_floppy::floppy_geom::FloppyMedia;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("game.xdf");
        let flat = vec![0u8; FloppyMedia::Hd1232.geometry().flat_size()];
        assert!(flat.len() as u64 <= MAX_FLOPPY_CONTAINER_BYTES);
        std::fs::write(&path, &flat).unwrap();
        assert!(is_floppy_container_file(&path));
    }
}
