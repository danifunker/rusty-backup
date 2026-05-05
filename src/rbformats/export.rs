//! Unified disk image export layer.
//!
//! Provides [`ExportFormat`] (VHD, Raw, 2MG) and format-agnostic export
//! functions that delegate to the appropriate format-specific code.

use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};

use super::dc42;
use super::twomg::build_twomg_header;
use super::vhd::build_vhd_footer;
use super::woz_write;
use super::{decompress_to_writer, reconstruct_disk_from_backup, write_zeros, CHUNK_SIZE};
use crate::backup::metadata::BackupMetadata;
use crate::partition::PartitionSizeOverride;

/// Output format for disk image export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    /// Fixed VHD (Virtual Hard Disk) — raw data + 512-byte footer.
    Vhd,
    /// Raw disk image — no header/footer.
    Raw,
    /// 2MG (Apple II) — 64-byte header + raw data.
    TwoMg,
    /// WOZ 2.0 (Apple II GCR bitstream) — floppy-only: 140K, 400K, or 800K sources.
    Woz,
    /// DiskCopy 4.2 (Mac / Apple IIgs) — floppy-only: 400K / 720K / 800K / 1440K sources.
    Dc42,
    /// MAME CHD, hard-disk profile (512-byte unit).
    Chd,
    /// MAME CHD, DVD profile (2048-byte unit, MAME 0.287+).
    ChdDvd,
    /// MAME CHD, CD profile (2448-byte frame). Input must be ISO or CUE.
    ChdCd,
    /// BIN/CUE pair extracted from a CD CHD. Single-bin by default; the
    /// bulk-convert worker can request multi-bin via a separate flag.
    BinCue,
}

impl ExportFormat {
    /// File extension for this format.
    pub fn extension(&self) -> &'static str {
        match self {
            Self::Vhd => "vhd",
            Self::Raw => "img",
            Self::TwoMg => "2mg",
            Self::Woz => "woz",
            Self::Dc42 => "dsk",
            Self::Chd | Self::ChdDvd | Self::ChdCd => "chd",
            Self::BinCue => "cue",
        }
    }

    /// Human-readable description.
    pub fn description(&self) -> &'static str {
        match self {
            Self::Vhd => "Fixed VHD",
            Self::Raw => "Raw Image",
            Self::TwoMg => "2MG (Apple II)",
            Self::Woz => "WOZ (Apple II)",
            Self::Dc42 => "DiskCopy 4.2",
            Self::Chd => "CHD (Hard Disk)",
            Self::ChdDvd => "DVD CHD",
            Self::ChdCd => "CD CHD",
            Self::BinCue => "BIN/CUE",
        }
    }

    /// Default filename for a save dialog.
    pub fn default_filename(&self, stem: &str) -> String {
        format!("{}.{}", stem, self.extension())
    }

    /// File dialog filter label and extensions.
    pub fn dialog_filter(&self) -> (&'static str, &'static [&'static str]) {
        match self {
            Self::Vhd => ("VHD Files", &["vhd", "hda"]),
            Self::Raw => ("Raw Images", &["img", "raw", "bin", "dd"]),
            Self::TwoMg => ("2MG Files", &["2mg"]),
            Self::Woz => ("WOZ Files", &["woz"]),
            Self::Dc42 => ("DiskCopy 4.2", &["dsk", "image", "dc42", "img"]),
            Self::Chd | Self::ChdDvd | Self::ChdCd => ("MAME CHD", &["chd"]),
            Self::BinCue => ("BIN/CUE Sheet", &["cue"]),
        }
    }

    /// True if this format can only wrap a floppy-sized image.
    /// WOZ: 140K / 400K / 800K. DiskCopy 4.2: 400K / 720K / 800K / 1440K.
    pub fn is_floppy_only(&self) -> bool {
        matches!(self, Self::Woz | Self::Dc42)
    }

    /// Write the format header (if any) to `writer`. Returns bytes written.
    fn write_header(&self, writer: &mut impl Write, data_length: u64) -> Result<u64> {
        match self {
            Self::TwoMg => {
                let hdr = build_twomg_header(data_length);
                writer
                    .write_all(&hdr)
                    .context("failed to write 2MG header")?;
                Ok(64)
            }
            _ => Ok(0),
        }
    }

    /// Write the format footer (if any) to `writer`. Returns bytes written.
    fn write_footer(&self, writer: &mut impl Write, data_length: u64) -> Result<u64> {
        match self {
            Self::Vhd => {
                let footer = build_vhd_footer(data_length);
                writer
                    .write_all(&footer)
                    .context("failed to write VHD footer")?;
                Ok(512)
            }
            _ => Ok(0),
        }
    }

    /// For 2MG, we need to go back and patch the data_length field after
    /// writing all data. This patches the header at the start of the file.
    fn patch_header_length(
        &self,
        writer: &mut (impl Write + Seek),
        data_length: u64,
    ) -> Result<()> {
        match self {
            Self::TwoMg => {
                // Rewrite full header with correct data_length
                writer.seek(SeekFrom::Start(0))?;
                let hdr = build_twomg_header(data_length);
                writer
                    .write_all(&hdr)
                    .context("failed to patch 2MG header")?;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Encode raw sector bytes as a DiskCopy 4.2 file and write to `dest_path`.
///
/// `sectors.len()` must be 400K / 720K / 800K / 1440K — the encoder rejects anything else.
fn write_dc42_from_sectors(
    sectors: &[u8],
    dest_path: &Path,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    let name = dest_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("Untitled");
    let bytes = dc42::encode_dc42(name, sectors)
        .map_err(|e| anyhow::anyhow!("DiskCopy 4.2 export failed: {e}"))?;
    std::fs::write(dest_path, &bytes)
        .with_context(|| format!("failed to write {}", dest_path.display()))?;
    log_cb(&format!(
        "DiskCopy 4.2 export complete: {} ({} input bytes -> {} DC42 bytes)",
        dest_path.display(),
        sectors.len(),
        bytes.len(),
    ));
    Ok(())
}

/// Encode raw sector bytes as a WOZ 2.0 file and write to `dest_path`.
///
/// `sectors.len()` must be 143,360 (140K 5.25"), 409,600 (400K 3.5"), or
/// 819,200 (800K 3.5") — `sectors_to_woz` auto-detects and rejects others.
fn write_woz_from_sectors(
    sectors: &[u8],
    dest_path: &Path,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    let bytes = woz_write::sectors_to_woz(sectors)
        .context("WOZ export failed: source is not a recognised floppy size")?;
    std::fs::write(dest_path, &bytes)
        .with_context(|| format!("failed to write {}", dest_path.display()))?;
    log_cb(&format!(
        "WOZ export complete: {} ({} input bytes -> {} WOZ bytes)",
        dest_path.display(),
        sectors.len(),
        bytes.len(),
    ));
    Ok(())
}

/// Slurp a source path (via decompress_to_writer) into an in-memory buffer.
/// Intended for floppy-sized content only — bounded by `max_bytes`.
fn read_source_to_memory(
    source_path: &Path,
    compression_type: &str,
    max_bytes: Option<u64>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    decompress_to_writer(
        source_path,
        compression_type,
        &mut buf,
        max_bytes,
        progress_cb,
        cancel_check,
        log_cb,
    )?;
    Ok(buf)
}

/// Export a whole disk image in the specified format.
///
/// For backup folders: reconstructs the disk from MBR + partition data files.
/// For raw images/devices: reconstructs with partition size overrides.
pub fn export_whole_disk(
    format: ExportFormat,
    source_path: &Path,
    backup_metadata: Option<&BackupMetadata>,
    mbr_bytes: Option<&[u8; 512]>,
    partition_sizes: &[PartitionSizeOverride],
    dest_path: &Path,
    mut progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    // For VHD format, delegate to existing full implementation which handles
    // all the complex reconstruction with EBR chain rebuilding etc.
    if format == ExportFormat::Vhd {
        return super::vhd::export_whole_disk_vhd(
            source_path,
            backup_metadata,
            mbr_bytes,
            partition_sizes,
            dest_path,
            progress_cb,
            cancel_check,
            log_cb,
        );
    }

    // CHD outputs go through libchdman-rs. Bulk-convert is the only caller
    // today and uses a separate entry point that threads `ChdOptions`; the
    // generic export path here defaults the codecs/hunk-size from chdman.
    if format == ExportFormat::Chd || format == ExportFormat::ChdDvd {
        let profile = if format == ExportFormat::Chd {
            super::chd_options::ChdProfile::Hd
        } else {
            super::chd_options::ChdProfile::Dvd
        };
        return export_whole_disk_chd(
            source_path,
            backup_metadata,
            mbr_bytes,
            partition_sizes,
            dest_path,
            profile,
            None,
            progress_cb,
            cancel_check,
            log_cb,
        );
    }

    if format == ExportFormat::ChdCd {
        return export_whole_disk_chd_cd(source_path, dest_path, None, cancel_check, log_cb);
    }

    if format == ExportFormat::BinCue {
        return export_whole_disk_bincue(source_path, dest_path, false, cancel_check, log_cb);
    }

    // WOZ and DiskCopy 4.2: floppy-only.  Reconstruct (or slurp) the source
    // into memory, then encode.  Raw-image path covers superfloppies (2MG,
    // .dsk, DC42, etc.); backup-folder path reconstructs the whole disk first.
    if format == ExportFormat::Woz || format == ExportFormat::Dc42 {
        let buf = if let Some(meta) = backup_metadata {
            let mut buf: Vec<u8> = Vec::new();
            reconstruct_disk_from_backup(
                source_path,
                meta,
                mbr_bytes,
                partition_sizes,
                meta.source_size_bytes,
                &mut std::io::Cursor::new(&mut buf),
                false,
                false,
                None,
                None,
                &mut progress_cb,
                &cancel_check,
                &mut log_cb,
            )?;
            buf
        } else {
            // Unwrap any container (WOZ, 2MG, DMG, VHD, DiskCopy 4.2, DOS-order
            // .do/.dsk) so we feed the encoder flat sector data.
            let file = File::open(source_path)
                .with_context(|| format!("failed to open {}", source_path.display()))?;
            let (mut reader, data_size) = {
                let file2 = File::open(source_path)?;
                let fmt = super::detect_image_format_with_path(file, Some(source_path))?;
                super::wrap_image_reader(file2, fmt)?
            };
            let mut buf = vec![0u8; data_size as usize];
            reader
                .read_exact(&mut buf)
                .context("failed to read source")?;
            progress_cb(data_size);
            buf
        };
        return if format == ExportFormat::Woz {
            write_woz_from_sectors(&buf, dest_path, &mut log_cb)
        } else {
            write_dc42_from_sectors(&buf, dest_path, &mut log_cb)
        };
    }

    // Raw and 2MG formats: reconstruct disk, then wrap with header/footer.
    let mut total_written: u64 = 0;

    if let Some(meta) = backup_metadata {
        // Backup folder reconstruction
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dest_path)
            .with_context(|| format!("failed to create {}", dest_path.display()))?;

        // For 2MG: write placeholder header, then data
        let header_size = if format == ExportFormat::TwoMg {
            file.write_all(&[0u8; 64])
                .context("failed to write 2MG header placeholder")?;
            64u64
        } else {
            0u64
        };

        total_written = reconstruct_disk_from_backup(
            source_path,
            meta,
            mbr_bytes,
            partition_sizes,
            meta.source_size_bytes,
            &mut file,
            false,
            false,
            None,
            None,
            &mut progress_cb,
            &cancel_check,
            &mut log_cb,
        )?;

        // Patch 2MG header with actual data length
        if format == ExportFormat::TwoMg {
            format.patch_header_length(&mut file, total_written)?;
            // Seek back to end
            file.seek(SeekFrom::Start(header_size + total_written))?;
        }

        file.flush()?;

        log_cb(&format!(
            "{} export complete: {} ({} data bytes)",
            format.description(),
            dest_path.display(),
            total_written,
        ));
    } else {
        // Raw image/device path.
        //
        // Unwrap any container (VHD, 2MG, DMG, DiskCopy 4.2, DOS-order .dsk, WOZ)
        // so Raw/2MG exports always emit flat sector data — not the source's header/footer bytes.
        let (mut reader, source_data_size) = {
            let file = File::open(source_path)
                .with_context(|| format!("failed to open {}", source_path.display()))?;
            let file2 = File::open(source_path)?;
            let fmt = super::detect_image_format_with_path(file, Some(source_path))?;
            super::wrap_image_reader(file2, fmt)?
        };

        // If the source has an APM partition table and the caller supplied
        // size overrides, reconstruct the disk with patched APM entries and
        // per-partition resize (currently covers classic HFS). Only Raw
        // format gets this path — 2MG wrapping APM isn't a realistic case.
        let apm = if format == ExportFormat::Raw && !partition_sizes.is_empty() {
            super::detect_raw_apm(&mut reader)
        } else {
            None
        };

        if apm.is_some() {
            // Open destination with Read+Write+Seek for the APM reconstruction.
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(dest_path)
                .with_context(|| format!("failed to create {}", dest_path.display()))?;

            total_written = super::reconstruct_raw_apm_disk(
                &mut reader,
                source_data_size,
                &mut file,
                partition_sizes,
                &mut progress_cb,
                &cancel_check,
                &mut log_cb,
            )?;
            file.flush()?;

            log_cb(&format!(
                "{} export complete: {} ({} data bytes, APM reconstructed)",
                format.description(),
                dest_path.display(),
                total_written,
            ));
        } else {
            // No APM override or non-APM source: stream bytes.
            let mut writer = BufWriter::new(
                File::create(dest_path)
                    .with_context(|| format!("failed to create {}", dest_path.display()))?,
            );

            // Write header (2MG only — writes placeholder, patched later)
            let header_size = format.write_header(&mut writer, 0)?;

            let mut buf = vec![0u8; CHUNK_SIZE];
            let mut remaining = source_data_size;
            while remaining > 0 {
                if cancel_check() {
                    bail!("export cancelled");
                }
                let to_read = (remaining as usize).min(CHUNK_SIZE);
                let n = reader
                    .read(&mut buf[..to_read])
                    .context("failed to read source")?;
                if n == 0 {
                    break;
                }
                writer
                    .write_all(&buf[..n])
                    .context("failed to write data")?;
                total_written += n as u64;
                remaining -= n as u64;
                progress_cb(total_written);
            }

            writer.flush()?;

            // Patch 2MG header with actual data length
            if format == ExportFormat::TwoMg {
                format.patch_header_length(writer.get_mut(), total_written)?;
                writer.seek(SeekFrom::Start(header_size + total_written))?;
            }

            // Write footer (VHD only — but we already returned for VHD above)
            format.write_footer(&mut writer, total_written)?;
            writer.flush()?;

            log_cb(&format!(
                "{} export complete: {} ({} data bytes)",
                format.description(),
                dest_path.display(),
                total_written,
            ));
        }
    }

    Ok(())
}

/// Export a single partition in the specified format.
pub fn export_partition(
    format: ExportFormat,
    source_path: &Path,
    compression_type: &str,
    dest_path: &Path,
    max_bytes: Option<u64>,
    mut progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    // For VHD, delegate to existing implementation
    if format == ExportFormat::Vhd {
        return super::vhd::export_partition_vhd(
            source_path,
            compression_type,
            dest_path,
            max_bytes,
            progress_cb,
            cancel_check,
            log_cb,
        );
    }

    // WOZ / DiskCopy 4.2: slurp into memory, encode, write.  Requires the
    // decompressed partition size to be a recognised floppy size.
    if format == ExportFormat::Woz || format == ExportFormat::Dc42 {
        let buf = read_source_to_memory(
            source_path,
            compression_type,
            max_bytes,
            &mut progress_cb,
            &cancel_check,
            &mut log_cb,
        )?;
        return if format == ExportFormat::Woz {
            write_woz_from_sectors(&buf, dest_path, &mut log_cb)
        } else {
            write_dc42_from_sectors(&buf, dest_path, &mut log_cb)
        };
    }

    let mut writer = BufWriter::new(
        File::create(dest_path)
            .with_context(|| format!("failed to create {}", dest_path.display()))?,
    );

    // Write header placeholder (2MG)
    let header_size = format.write_header(&mut writer, 0)?;

    let bytes_written = decompress_to_writer(
        source_path,
        compression_type,
        &mut writer,
        max_bytes,
        &mut progress_cb,
        &cancel_check,
        &mut log_cb,
    )?;

    let data_size = max_bytes.unwrap_or(bytes_written).max(bytes_written);

    // Pad with zeros if needed
    if bytes_written < data_size {
        let pad = data_size - bytes_written;
        write_zeros(&mut writer, pad)?;
    }

    writer.flush()?;

    // Patch 2MG header with actual data length
    if format == ExportFormat::TwoMg {
        format.patch_header_length(writer.get_mut(), data_size)?;
        writer.seek(SeekFrom::Start(header_size + data_size))?;
    }

    // Write footer (VHD — but already returned above for VHD)
    format.write_footer(&mut writer, data_size)?;
    writer.flush()?;

    log_cb(&format!(
        "{} partition export complete: {} ({} data bytes)",
        format.description(),
        dest_path.display(),
        data_size,
    ));

    Ok(())
}

/// Export a whole disk from a Clonezilla image in the specified format.
pub fn export_clonezilla_disk(
    format: ExportFormat,
    cz_image: &crate::clonezilla::metadata::ClonezillaImage,
    backup_folder: &Path,
    output_path: &Path,
    partition_sizes: &[PartitionSizeOverride],
    progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    // For VHD, delegate to existing implementation
    if format == ExportFormat::Vhd {
        return super::vhd::export_clonezilla_disk_vhd(
            cz_image,
            backup_folder,
            output_path,
            partition_sizes,
            progress_cb,
            cancel_check,
            log_cb,
        );
    }

    // For Raw/2MG/WOZ: wrap the VHD export logic but skip footer / add header.
    // Since the Clonezilla disk export is complex (EBR, gap filling, etc.),
    // we call the VHD version to a temp file and then strip/convert.
    // However, that's wasteful. Instead, replicate the core logic.
    //
    // Actually, the simplest correct approach: use the VHD export, then
    // post-process to strip the 512-byte footer and optionally prepend 2MG header.
    // This avoids duplicating the complex Clonezilla reconstruction logic.

    // Export as VHD first
    let temp_vhd = output_path.with_extension("vhd.tmp");
    super::vhd::export_clonezilla_disk_vhd(
        cz_image,
        backup_folder,
        &temp_vhd,
        partition_sizes,
        progress_cb,
        cancel_check,
        &mut log_cb,
    )?;

    // Convert: strip VHD footer, optionally add 2MG header or encode as WOZ.
    convert_from_vhd_temp(&temp_vhd, output_path, format, &mut log_cb)?;

    // Clean up temp
    let _ = std::fs::remove_file(&temp_vhd);

    Ok(())
}

/// Export a single partition from a Clonezilla image in the specified format.
pub fn export_clonezilla_partition(
    format: ExportFormat,
    partclone_files: &[std::path::PathBuf],
    dest_path: &Path,
    export_size: Option<u64>,
    progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    if format == ExportFormat::Vhd {
        return super::vhd::export_clonezilla_partition_vhd(
            partclone_files,
            dest_path,
            export_size,
            progress_cb,
            cancel_check,
            log_cb,
        );
    }

    // Export as VHD first, then convert
    let temp_vhd = dest_path.with_extension("vhd.tmp");
    super::vhd::export_clonezilla_partition_vhd(
        partclone_files,
        &temp_vhd,
        export_size,
        progress_cb,
        cancel_check,
        &mut log_cb,
    )?;

    convert_from_vhd_temp(&temp_vhd, dest_path, format, &mut log_cb)?;
    let _ = std::fs::remove_file(&temp_vhd);

    Ok(())
}

/// Convert a temporary VHD file to Raw or 2MG by stripping the 512-byte
/// footer and optionally prepending a 2MG header.
fn convert_from_vhd_temp(
    vhd_path: &Path,
    dest_path: &Path,
    format: ExportFormat,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    let vhd_size = std::fs::metadata(vhd_path)
        .with_context(|| format!("failed to stat {}", vhd_path.display()))?
        .len();
    let data_size = vhd_size.saturating_sub(512); // strip VHD footer

    // WOZ / DC42 path: read the stripped data into memory, encode, write.
    if format == ExportFormat::Woz || format == ExportFormat::Dc42 {
        let mut f = File::open(vhd_path)
            .with_context(|| format!("failed to open {}", vhd_path.display()))?;
        let mut buf = vec![0u8; data_size as usize];
        f.read_exact(&mut buf).context("failed to read VHD temp")?;
        return if format == ExportFormat::Woz {
            write_woz_from_sectors(&buf, dest_path, log_cb)
        } else {
            write_dc42_from_sectors(&buf, dest_path, log_cb)
        };
    }

    let mut reader = std::io::BufReader::new(
        File::open(vhd_path).with_context(|| format!("failed to open {}", vhd_path.display()))?,
    );
    let mut writer = BufWriter::new(
        File::create(dest_path)
            .with_context(|| format!("failed to create {}", dest_path.display()))?,
    );

    // Write header if needed
    format.write_header(&mut writer, data_size)?;

    // Copy data (without VHD footer)
    let mut buf = vec![0u8; CHUNK_SIZE];
    let mut remaining = data_size;
    while remaining > 0 {
        let to_read = (remaining as usize).min(CHUNK_SIZE);
        let n = reader.read(&mut buf[..to_read])?;
        if n == 0 {
            break;
        }
        writer.write_all(&buf[..n])?;
        remaining -= n as u64;
    }

    writer.flush()?;

    log_cb(&format!(
        "{} export complete: {} ({} data bytes)",
        format.description(),
        dest_path.display(),
        data_size,
    ));

    Ok(())
}

/// Export a whole disk as a MAME CHD (HD or DVD profile) via libchdman-rs.
///
/// Takes any source `wrap_image_reader` understands (raw image, VHD, 2MG, DMG,
/// DiskCopy 4.2, WOZ, an existing CHD, …) and writes a CHD with the chosen
/// profile + optional codec/hunk overrides. `backup_metadata` is rejected for
/// now — the bulk-convert pipeline (the only caller today) operates on raw
/// image files; full backup-folder reconstruction into CHD can be added later
/// if needed.
#[allow(clippy::too_many_arguments)]
pub fn export_whole_disk_chd(
    source_path: &Path,
    backup_metadata: Option<&BackupMetadata>,
    _mbr_bytes: Option<&[u8; 512]>,
    _partition_sizes: &[PartitionSizeOverride],
    dest_path: &Path,
    profile: super::chd_options::ChdProfile,
    chd_options: Option<super::chd_options::ChdOptions>,
    mut progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    if backup_metadata.is_some() {
        bail!("CHD export from backup folders is not implemented; export to VHD/Raw first");
    }

    let file = File::open(source_path)
        .with_context(|| format!("failed to open {}", source_path.display()))?;
    let file2 = File::open(source_path)?;
    let fmt = super::detect_image_format_with_path(file, Some(source_path))?;
    let (mut reader, source_data_size) = super::wrap_image_reader(file2, fmt)?;

    // compress_chd writes "<base>.chd" — strip the trailing extension from
    // dest_path so the output filename matches what the caller asked for.
    let output_base = dest_path.with_extension("");

    let names = match profile {
        super::chd_options::ChdProfile::Hd => super::chd::compress_chd(
            &mut reader,
            &output_base,
            source_data_size,
            None,
            chd_options,
            &mut progress_cb,
            &cancel_check,
            &mut log_cb,
        )?,
        super::chd_options::ChdProfile::Dvd => super::chd::compress_chd_dvd(
            &mut reader,
            &output_base,
            source_data_size,
            None,
            chd_options,
            &mut progress_cb,
            &cancel_check,
            &mut log_cb,
        )?,
        super::chd_options::ChdProfile::Cd => {
            bail!("CD CHD output is not supported by export_whole_disk_chd; use the optical tab")
        }
    };

    log_cb(&format!(
        "CHD export complete: {} file(s) written ({} data bytes)",
        names.len(),
        source_data_size,
    ));

    Ok(())
}

/// Export an ISO or BIN/CUE source as a CD CHD via libchdman-rs.
///
/// Routes through `optical::convert::to_chd` so all the existing logic for
/// `parse_toc` (multi-FILE cues, audio tracks, mixed mode) is reused. Only
/// `.iso` and `.cue` source paths produce CD CHDs cleanly — anything else
/// fails with a clear error from `opticaldiscs::DiscFormat::from_path`.
///
/// Intra-file progress isn't surfaced to the caller's `progress_cb` here —
/// `to_chd` writes into a shared `ConvertProgress` on its own clock. The
/// bulk-convert UI shows per-file progress (the per-CHD worker bumps the
/// file index when it returns) which is granular enough for the dialog.
pub fn export_whole_disk_chd_cd(
    source_path: &Path,
    dest_path: &Path,
    chd_options: Option<super::chd_options::ChdOptions>,
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    use crate::optical::convert::{to_chd, ConvertProgress};
    use std::sync::{Arc, Mutex};

    let shared = Arc::new(Mutex::new(ConvertProgress::new()));
    if cancel_check() {
        if let Ok(mut s) = shared.lock() {
            s.cancel_requested = true;
        }
    }
    to_chd(source_path, dest_path, chd_options, Arc::clone(&shared))?;

    log_cb(&format!(
        "CD CHD export complete: {} -> {}",
        source_path.display(),
        dest_path.display(),
    ));
    Ok(())
}

/// Export a CD CHD as a BIN/CUE pair (single-bin or multi-bin).
///
/// Single-bin output mirrors chdman's `extractcd`. Multi-bin output writes one
/// `<base> (Track NN).bin` per track and a multi-FILE cue — a feature beyond
/// chdman built on top of libchdman-rs's track metadata.
pub fn export_whole_disk_bincue(
    source_chd: &Path,
    dest_cue: &Path,
    multi_bin: bool,
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    use crate::optical::convert::{chd_to_bincue, chd_to_bincue_multi, ConvertProgress};
    use std::sync::{Arc, Mutex};

    let shared = Arc::new(Mutex::new(ConvertProgress::new()));
    if cancel_check() {
        if let Ok(mut s) = shared.lock() {
            s.cancel_requested = true;
        }
    }
    if multi_bin {
        chd_to_bincue_multi(source_chd, dest_cue, Arc::clone(&shared))?;
    } else {
        chd_to_bincue(source_chd, dest_cue, Arc::clone(&shared))?;
    }

    log_cb(&format!(
        "BIN/CUE export complete ({}): {}",
        if multi_bin { "multi-bin" } else { "single-bin" },
        dest_cue.display(),
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_export_format_extension() {
        assert_eq!(ExportFormat::Vhd.extension(), "vhd");
        assert_eq!(ExportFormat::Raw.extension(), "img");
        assert_eq!(ExportFormat::TwoMg.extension(), "2mg");
        assert_eq!(ExportFormat::Woz.extension(), "woz");
        assert_eq!(ExportFormat::Dc42.extension(), "dsk");
    }

    #[test]
    fn test_export_format_is_floppy_only() {
        assert!(!ExportFormat::Vhd.is_floppy_only());
        assert!(!ExportFormat::Raw.is_floppy_only());
        assert!(!ExportFormat::TwoMg.is_floppy_only());
        assert!(ExportFormat::Woz.is_floppy_only());
        assert!(ExportFormat::Dc42.is_floppy_only());
    }

    #[test]
    fn test_export_format_description() {
        assert_eq!(ExportFormat::Vhd.description(), "Fixed VHD");
        assert_eq!(ExportFormat::Raw.description(), "Raw Image");
        assert_eq!(ExportFormat::TwoMg.description(), "2MG (Apple II)");
    }

    #[test]
    fn test_export_format_default_filename() {
        assert_eq!(ExportFormat::Vhd.default_filename("disk"), "disk.vhd");
        assert_eq!(ExportFormat::Raw.default_filename("disk"), "disk.img");
        assert_eq!(ExportFormat::TwoMg.default_filename("disk"), "disk.2mg");
        assert_eq!(ExportFormat::Chd.default_filename("disk"), "disk.chd");
        assert_eq!(ExportFormat::ChdDvd.default_filename("disk"), "disk.chd");
    }

    /// Bulk-convert path: raw image -> HD CHD -> read back via `ChdReader`,
    /// must round-trip byte-equal up to the partition's logical size.
    #[test]
    fn test_export_whole_disk_chd_round_trip() {
        use crate::rbformats::chd::ChdReader;
        use crate::rbformats::chd_options::ChdProfile;
        use std::io::Write;

        let tmp = tempfile::tempdir().expect("tempdir");
        let src_path = tmp.path().join("src.img");
        let dst_path = tmp.path().join("out.chd");

        // 1 MiB pseudo-random source, 512-byte aligned.
        let logical_size: usize = 1024 * 1024;
        let mut src = vec![0u8; logical_size];
        for (i, b) in src.iter_mut().enumerate() {
            *b = (i.wrapping_mul(31337) ^ (i >> 3)) as u8;
        }
        std::fs::File::create(&src_path)
            .unwrap()
            .write_all(&src)
            .unwrap();

        export_whole_disk_chd(
            &src_path,
            None,
            None,
            &[],
            &dst_path,
            ChdProfile::Hd,
            None,
            |_| {},
            || false,
            |_| {},
        )
        .expect("export_whole_disk_chd should succeed");

        assert!(dst_path.exists(), "dest CHD should exist");

        let mut reader = ChdReader::open(&dst_path).expect("open CHD");
        let mut roundtrip = vec![0u8; logical_size];
        std::io::Read::read_exact(&mut reader, &mut roundtrip).unwrap();
        assert_eq!(roundtrip, src, "CHD round-trip must be byte-equal");
    }
}
