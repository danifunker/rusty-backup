use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use cd_da_reader::{CdReader, RetryConfig, SectorReadMode};

use crate::backup::{LogLevel, LogMessage};

/// Output format for disc ripping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RipFormat {
    /// Single .iso file (2048 bytes/sector, data tracks only).
    Iso,
    /// BIN/CUE pair (2352 bytes/sector raw, all tracks).
    BinCue,
}

/// Configuration for a disc rip operation.
#[derive(Debug, Clone)]
pub struct RipConfig {
    /// Device path (e.g. "/dev/sr0", "disk6", r"\\.\E:").
    pub device_path: PathBuf,
    /// Output path: .iso file for Iso format, .cue file for BinCue (derives .bin).
    pub output_path: PathBuf,
    /// Output format.
    pub format: RipFormat,
    /// Eject the disc after ripping completes.
    pub eject_after: bool,
}

/// Shared progress state between the rip thread and the GUI.
pub struct RipProgress {
    pub current_bytes: u64,
    pub total_bytes: u64,
    pub current_sector: u64,
    pub total_sectors: u64,
    pub operation: String,
    pub finished: bool,
    pub error: Option<String>,
    pub cancel_requested: bool,
    pub log_messages: VecDeque<LogMessage>,
}

impl RipProgress {
    pub fn new() -> Self {
        Self {
            current_bytes: 0,
            total_bytes: 0,
            current_sector: 0,
            total_sectors: 0,
            operation: String::new(),
            finished: false,
            error: None,
            cancel_requested: false,
            log_messages: VecDeque::new(),
        }
    }
}

fn log(progress: &Arc<Mutex<RipProgress>>, level: LogLevel, message: impl Into<String>) {
    if let Ok(mut p) = progress.lock() {
        p.log_messages.push_back(LogMessage {
            level,
            message: message.into(),
        });
    }
}

fn set_operation(progress: &Arc<Mutex<RipProgress>>, op: impl Into<String>) {
    if let Ok(mut p) = progress.lock() {
        p.operation = op.into();
    }
}

fn is_cancelled(progress: &Arc<Mutex<RipProgress>>) -> bool {
    progress.lock().map(|p| p.cancel_requested).unwrap_or(false)
}

fn set_progress(progress: &Arc<Mutex<RipProgress>>, current_bytes: u64, current_sector: u64) {
    if let Ok(mut p) = progress.lock() {
        p.current_bytes = current_bytes;
        p.current_sector = current_sector;
    }
}

/// Main rip orchestrator. Runs on a background thread.
pub fn run_rip(config: RipConfig, progress: Arc<Mutex<RipProgress>>) -> Result<()> {
    let result = match config.format {
        RipFormat::Iso => rip_iso(&config, &progress),
        RipFormat::BinCue => rip_bin_cue(&config, &progress),
    };

    if let Err(ref e) = result {
        if let Ok(mut p) = progress.lock() {
            p.error = Some(format!("{e:#}"));
            p.finished = true;
        }
        return result;
    }

    if config.eject_after {
        if let Err(e) = eject_disc(&config.device_path) {
            log(
                &progress,
                LogLevel::Warning,
                format!("Failed to eject disc: {e}"),
            );
        }
    }

    if let Ok(mut p) = progress.lock() {
        p.finished = true;
    }

    Ok(())
}

/// Sectors to read per chunk for READ CD commands.
const SECTORS_PER_CHUNK: u32 = 128;

fn rip_iso(config: &RipConfig, progress: &Arc<Mutex<RipProgress>>) -> Result<()> {
    let device_str = config.device_path.to_string_lossy();
    set_operation(progress, "Opening drive...");
    let reader = CdReader::open(&device_str)
        .with_context(|| format!("Failed to open drive: {device_str}"))?;

    set_operation(progress, "Reading TOC...");
    let toc = reader
        .read_toc()
        .with_context(|| "Failed to read table of contents")?;

    log(
        progress,
        LogLevel::Info,
        format!(
            "TOC: {} tracks, leadout at LBA {}",
            toc.tracks.len(),
            toc.leadout_lba
        ),
    );

    // Find the first data track
    let first_data = toc
        .tracks
        .iter()
        .find(|t| !t.is_audio)
        .context("No data tracks found on disc")?;

    let start_lba = first_data.start_lba;
    let total_sectors = toc.leadout_lba - start_lba;
    let sector_size = SectorReadMode::DataCooked.sector_size() as u64;
    let total_bytes = total_sectors as u64 * sector_size;

    log(
        progress,
        LogLevel::Info,
        format!(
            "Ripping data track: LBA {start_lba} - {}, {total_sectors} sectors ({:.1} MB)",
            toc.leadout_lba,
            total_bytes as f64 / 1_048_576.0
        ),
    );

    if let Ok(mut p) = progress.lock() {
        p.total_bytes = total_bytes;
        p.total_sectors = total_sectors as u64;
    }

    set_operation(progress, "Ripping to ISO...");
    let mut out = File::create(&config.output_path)
        .with_context(|| format!("Failed to create {}", config.output_path.display()))?;

    let retry_cfg = RetryConfig::default();
    let mut bytes_written: u64 = 0;
    let mut sectors_done: u64 = 0;
    let mut lba = start_lba;
    let mut remaining = total_sectors;

    while remaining > 0 {
        if is_cancelled(progress) {
            bail!("Rip cancelled by user");
        }

        let count = remaining.min(SECTORS_PER_CHUNK);
        let data = reader
            .read_data_sectors(lba, count, SectorReadMode::DataCooked, &retry_cfg)
            .with_context(|| format!("Read error at LBA {lba}"))?;

        out.write_all(&data)
            .with_context(|| "Failed to write ISO data")?;

        lba += count;
        remaining -= count;
        bytes_written += data.len() as u64;
        sectors_done += count as u64;
        set_progress(progress, bytes_written, sectors_done);
    }

    out.flush()?;

    log(
        progress,
        LogLevel::Info,
        format!(
            "ISO rip complete: {} ({:.1} MB)",
            config.output_path.display(),
            bytes_written as f64 / 1_048_576.0
        ),
    );

    Ok(())
}

fn rip_bin_cue(config: &RipConfig, progress: &Arc<Mutex<RipProgress>>) -> Result<()> {
    let device_str = config.device_path.to_string_lossy();
    set_operation(progress, "Opening drive...");
    let reader = CdReader::open(&device_str)
        .with_context(|| format!("Failed to open drive: {device_str}"))?;

    set_operation(progress, "Reading TOC...");
    let toc = reader
        .read_toc()
        .with_context(|| "Failed to read table of contents")?;

    log(
        progress,
        LogLevel::Info,
        format!(
            "TOC: {} tracks, leadout at LBA {}",
            toc.tracks.len(),
            toc.leadout_lba
        ),
    );

    // Calculate total sectors across all tracks
    let total_sectors = toc.leadout_lba - toc.tracks[0].start_lba;
    let sector_size = 2352u64; // BIN is always raw 2352 bytes/sector
    let total_bytes = total_sectors as u64 * sector_size;

    if let Ok(mut p) = progress.lock() {
        p.total_bytes = total_bytes;
        p.total_sectors = total_sectors as u64;
    }

    // Derive .bin path from .cue path
    let bin_path = config.output_path.with_extension("bin");
    let bin_filename = bin_path
        .file_name()
        .context("Invalid output path")?
        .to_string_lossy()
        .to_string();

    set_operation(progress, "Ripping to BIN...");
    let mut bin_file = File::create(&bin_path)
        .with_context(|| format!("Failed to create {}", bin_path.display()))?;

    let retry_cfg = RetryConfig::default();
    let mut bytes_written: u64 = 0;
    let mut sectors_done: u64 = 0;

    // Rip each track sequentially
    for (idx, track) in toc.tracks.iter().enumerate() {
        let next_lba = if idx + 1 < toc.tracks.len() {
            toc.tracks[idx + 1].start_lba
        } else {
            toc.leadout_lba
        };

        let track_sectors = next_lba - track.start_lba;
        let mode = if track.is_audio {
            SectorReadMode::Audio
        } else {
            SectorReadMode::DataRaw
        };

        log(
            progress,
            LogLevel::Info,
            format!(
                "Track {}: {} ({} sectors, LBA {}-{})",
                track.number,
                if track.is_audio { "AUDIO" } else { "DATA" },
                track_sectors,
                track.start_lba,
                next_lba - 1
            ),
        );

        let mut lba = track.start_lba;
        let mut track_remaining = track_sectors;

        while track_remaining > 0 {
            if is_cancelled(progress) {
                bail!("Rip cancelled by user");
            }

            let count = track_remaining.min(SECTORS_PER_CHUNK);
            let data = reader
                .read_data_sectors(lba, count, mode, &retry_cfg)
                .with_context(|| format!("Read error at LBA {lba} (track {})", track.number))?;

            bin_file
                .write_all(&data)
                .with_context(|| "Failed to write BIN data")?;

            lba += count;
            track_remaining -= count;
            bytes_written += data.len() as u64;
            sectors_done += count as u64;
            set_progress(progress, bytes_written, sectors_done);
        }
    }

    bin_file.flush()?;

    // Generate CUE sheet
    set_operation(progress, "Writing CUE sheet...");
    let cue_text = generate_cue_sheet(&bin_filename, &toc);
    std::fs::write(&config.output_path, &cue_text)
        .with_context(|| format!("Failed to write {}", config.output_path.display()))?;

    log(
        progress,
        LogLevel::Info,
        format!(
            "BIN/CUE rip complete: {} ({:.1} MB)",
            config.output_path.display(),
            bytes_written as f64 / 1_048_576.0
        ),
    );

    Ok(())
}

/// Generate a CUE sheet from the TOC.
fn generate_cue_sheet(bin_filename: &str, toc: &cd_da_reader::Toc) -> String {
    let first_track_lba = toc.tracks.first().map(|t| t.start_lba).unwrap_or(0);
    let mut lines = Vec::new();
    lines.push(format!("FILE \"{bin_filename}\" BINARY"));

    for track in &toc.tracks {
        let track_type = if track.is_audio {
            "AUDIO"
        } else {
            "MODE1/2352"
        };
        lines.push(format!("  TRACK {:02} {track_type}", track.number));

        // INDEX 01 is relative to the start of the BIN file
        let relative_lba = track.start_lba - first_track_lba;
        let (m, s, f) = lba_to_msf(relative_lba);
        lines.push(format!("    INDEX 01 {m:02}:{s:02}:{f:02}"));
    }

    lines.push(String::new()); // trailing newline
    lines.join("\n")
}

/// Convert an LBA offset to MSF (minute:second:frame) for CUE sheets.
/// Note: CUE MSF does NOT use the 150-frame pregap offset that Red Book uses.
fn lba_to_msf(lba: u32) -> (u8, u8, u8) {
    let minutes = (lba / 75 / 60) as u8;
    let seconds = ((lba / 75) % 60) as u8;
    let frames = (lba % 75) as u8;
    (minutes, seconds, frames)
}

/// Eject the disc from the drive.
fn eject_disc(path: &Path) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        let status = std::process::Command::new("eject")
            .arg(path)
            .status()
            .context("Failed to run eject command")?;
        if !status.success() {
            bail!("eject command failed with status {status}");
        }
    }

    #[cfg(target_os = "macos")]
    {
        let path_str = path.to_string_lossy();
        let status = std::process::Command::new("diskutil")
            .args(["eject", &path_str])
            .status()
            .context("Failed to run diskutil eject")?;
        if !status.success() {
            bail!("diskutil eject failed with status {status}");
        }
    }

    #[cfg(target_os = "windows")]
    {
        let path_str = path.to_string_lossy();
        // Use PowerShell to eject. The path should be like "D:" or "E:"
        let drive_letter = path_str.trim_start_matches(r"\\.\").trim_end_matches(':');
        let ps_script = format!(
            "(New-Object -ComObject Shell.Application).NameSpace(17).ParseName('{drive_letter}:').InvokeVerb('Eject')"
        );
        let status = std::process::Command::new("powershell")
            .args(["-NoProfile", "-Command", &ps_script])
            .status()
            .context("Failed to run PowerShell eject")?;
        if !status.success() {
            bail!("PowerShell eject failed with status {status}");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lba_to_msf() {
        // LBA 0 -> 00:00:00
        assert_eq!(lba_to_msf(0), (0, 0, 0));
        // LBA 75 -> 00:01:00 (75 frames = 1 second)
        assert_eq!(lba_to_msf(75), (0, 1, 0));
        // LBA 4500 -> 01:00:00 (75*60 = 4500 frames = 1 minute)
        assert_eq!(lba_to_msf(4500), (1, 0, 0));
        // LBA 150 -> 00:02:00
        assert_eq!(lba_to_msf(150), (0, 2, 0));
        // LBA 4575 -> 01:01:00
        assert_eq!(lba_to_msf(4575), (1, 1, 0));
        // LBA 37 -> 00:00:37
        assert_eq!(lba_to_msf(37), (0, 0, 37));
    }

    #[test]
    fn test_generate_cue_sheet() {
        let toc = cd_da_reader::Toc {
            first_track: 1,
            last_track: 3,
            tracks: vec![
                cd_da_reader::Track {
                    number: 1,
                    start_lba: 0,
                    start_msf: (0, 2, 0),
                    is_audio: false,
                },
                cd_da_reader::Track {
                    number: 2,
                    start_lba: 15000,
                    start_msf: (3, 22, 0),
                    is_audio: true,
                },
                cd_da_reader::Track {
                    number: 3,
                    start_lba: 30000,
                    start_msf: (6, 42, 0),
                    is_audio: true,
                },
            ],
            leadout_lba: 45000,
        };

        let cue = generate_cue_sheet("disc.bin", &toc);

        assert!(cue.starts_with("FILE \"disc.bin\" BINARY\n"));
        assert!(cue.contains("  TRACK 01 MODE1/2352\n"));
        assert!(cue.contains("    INDEX 01 00:00:00\n"));
        assert!(cue.contains("  TRACK 02 AUDIO\n"));
        // 15000 frames = 200 seconds = 3:20:00
        assert!(cue.contains("    INDEX 01 03:20:00\n"));
        assert!(cue.contains("  TRACK 03 AUDIO\n"));
        // 30000 frames = 400 seconds = 6:40:00
        assert!(cue.contains("    INDEX 01 06:40:00\n"));
    }

    #[test]
    fn test_rip_config_construction() {
        let config = RipConfig {
            device_path: PathBuf::from("/dev/sr0"),
            output_path: PathBuf::from("/tmp/disc.iso"),
            format: RipFormat::Iso,
            eject_after: false,
        };
        assert_eq!(config.format, RipFormat::Iso);
        assert!(!config.eject_after);
    }

    #[test]
    fn test_rip_progress_construction() {
        let progress = RipProgress::new();
        assert_eq!(progress.current_bytes, 0);
        assert_eq!(progress.total_bytes, 0);
        assert_eq!(progress.current_sector, 0);
        assert_eq!(progress.total_sectors, 0);
        assert!(!progress.finished);
        assert!(progress.error.is_none());
        assert!(!progress.cancel_requested);
        assert!(progress.log_messages.is_empty());
    }

    #[test]
    fn test_sector_read_mode_properties() {
        assert_eq!(SectorReadMode::Audio.sector_size(), 2352);
        assert_eq!(SectorReadMode::DataCooked.sector_size(), 2048);
        assert_eq!(SectorReadMode::DataRaw.sector_size(), 2352);

        assert_eq!(SectorReadMode::DataCooked.cdb_byte1(), 0x04);
        assert_eq!(SectorReadMode::DataCooked.cdb_byte9(), 0x10);
        assert_eq!(SectorReadMode::DataRaw.cdb_byte9(), 0xF8);
    }
}
