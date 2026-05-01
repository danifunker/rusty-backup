use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};

use crate::backup::{LogLevel, LogMessage};
use crate::update::UpdateConfig;

/// Shared progress state between a conversion thread and the GUI.
#[derive(Default)]
pub struct ConvertProgress {
    pub current_bytes: u64,
    pub total_bytes: u64,
    pub operation: String,
    pub finished: bool,
    pub error: Option<String>,
    pub cancel_requested: bool,
    pub log_messages: VecDeque<LogMessage>,
}

impl ConvertProgress {
    pub fn new() -> Self {
        Self::default()
    }
}

fn log(progress: &Arc<Mutex<ConvertProgress>>, level: LogLevel, message: impl Into<String>) {
    if let Ok(mut p) = progress.lock() {
        p.log_messages.push_back(LogMessage {
            level,
            message: message.into(),
        });
    }
}

fn set_operation(progress: &Arc<Mutex<ConvertProgress>>, op: impl Into<String>) {
    if let Ok(mut p) = progress.lock() {
        p.operation = op.into();
    }
}

fn is_cancelled(progress: &Arc<Mutex<ConvertProgress>>) -> bool {
    progress.lock().map(|p| p.cancel_requested).unwrap_or(false)
}

fn set_progress(progress: &Arc<Mutex<ConvertProgress>>, current_bytes: u64) {
    if let Ok(mut p) = progress.lock() {
        p.current_bytes = current_bytes;
    }
}

fn get_chdman_command() -> String {
    UpdateConfig::load()
        .chdman_path
        .unwrap_or_else(|| "chdman".to_string())
}

/// Mode 1 CD-ROM sync pattern (12 bytes at the start of each raw sector).
const SYNC_PATTERN: [u8; 12] = [
    0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00,
];

/// Convert an ISO image to BIN/CUE format.
///
/// Each 2048-byte ISO sector is wrapped in a 2352-byte Mode 1 raw frame:
/// 12-byte sync + 4-byte header + 2048-byte data + 288-byte zero ECC/EDC.
pub fn iso_to_bincue(
    iso_path: &Path,
    bin_path: &Path,
    cue_path: &Path,
    progress: Arc<Mutex<ConvertProgress>>,
) -> Result<()> {
    set_operation(&progress, "Converting ISO to BIN/CUE...");

    let iso_file = File::open(iso_path)
        .with_context(|| format!("Failed to open ISO: {}", iso_path.display()))?;
    let iso_size = iso_file.metadata()?.len();
    let sector_count = iso_size / 2048;

    if let Ok(mut p) = progress.lock() {
        p.total_bytes = sector_count * 2352;
    }

    log(
        &progress,
        LogLevel::Info,
        format!(
            "Converting {} sectors ({:.1} MB) from ISO to BIN/CUE",
            sector_count,
            iso_size as f64 / 1_048_576.0
        ),
    );

    let mut reader = BufReader::new(iso_file);
    let mut writer = BufWriter::new(
        File::create(bin_path)
            .with_context(|| format!("Failed to create BIN: {}", bin_path.display()))?,
    );

    let mut iso_buf = [0u8; 2048];
    let mut frame = vec![0u8; 2352];
    let mut bytes_written: u64 = 0;

    for sector in 0..sector_count {
        if is_cancelled(&progress) {
            bail!("Conversion cancelled by user");
        }

        reader
            .read_exact(&mut iso_buf)
            .with_context(|| format!("Failed to read sector {sector}"))?;

        // Build Mode 1 raw frame
        // Sync pattern (bytes 0-11)
        frame[..12].copy_from_slice(&SYNC_PATTERN);
        // Header (bytes 12-15): MSF + mode byte
        let abs_sector = sector + 150; // 2-second pregap offset
        let minutes = (abs_sector / (75 * 60)) as u8;
        let seconds = ((abs_sector / 75) % 60) as u8;
        let frames = (abs_sector % 75) as u8;
        frame[12] = to_bcd(minutes);
        frame[13] = to_bcd(seconds);
        frame[14] = to_bcd(frames);
        // Mode byte
        frame[15] = 1;
        // User data (bytes 16-2063)
        frame[16..2064].copy_from_slice(&iso_buf);
        // ECC/EDC padding (bytes 2064-2351) — zero-filled
        frame[2064..2352].fill(0);

        writer
            .write_all(&frame)
            .with_context(|| "Failed to write BIN data")?;

        bytes_written += 2352;
        if sector % 1000 == 0 {
            set_progress(&progress, bytes_written);
        }
    }

    writer.flush()?;
    set_progress(&progress, bytes_written);

    // Generate CUE sheet
    let bin_filename = bin_path
        .file_name()
        .context("Invalid BIN path")?
        .to_string_lossy();
    let cue_text = format!(
        "FILE \"{}\" BINARY\n  TRACK 01 MODE1/2352\n    INDEX 01 00:00:00\n",
        bin_filename
    );
    std::fs::write(cue_path, &cue_text)
        .with_context(|| format!("Failed to write CUE: {}", cue_path.display()))?;

    log(
        &progress,
        LogLevel::Info,
        format!(
            "ISO to BIN/CUE conversion complete: {} ({:.1} MB)",
            bin_path.display(),
            bytes_written as f64 / 1_048_576.0
        ),
    );

    if let Ok(mut p) = progress.lock() {
        p.finished = true;
    }

    Ok(())
}

/// Convert a BIN/CUE image to ISO format.
///
/// Reads cooked 2048-byte sectors from the first data track and writes them
/// sequentially to a plain .iso file.
pub fn bincue_to_iso(
    cue_path: &Path,
    iso_path: &Path,
    progress: Arc<Mutex<ConvertProgress>>,
) -> Result<()> {
    set_operation(&progress, "Converting BIN/CUE to ISO...");

    let tracks = opticaldiscs::bincue::parse_cue_tracks(cue_path)
        .with_context(|| format!("Failed to parse CUE: {}", cue_path.display()))?;

    let data_track = tracks
        .iter()
        .find(|t| t.is_data())
        .context("No data track found in CUE")?;

    let mut reader = opticaldiscs::BinCueSectorReader::open(data_track)
        .with_context(|| "Failed to open BIN/CUE sector reader")?;

    // Determine sector count from track or BIN file size
    let sector_count = if data_track.frame_count > 0 {
        data_track.frame_count
    } else {
        let bin_size = data_track.bin_path.metadata()?.len();
        let sector_size = data_track.sector_size();
        (bin_size - data_track.file_byte_offset) / sector_size
    };

    let total_bytes = sector_count * 2048;
    if let Ok(mut p) = progress.lock() {
        p.total_bytes = total_bytes;
    }

    log(
        &progress,
        LogLevel::Info,
        format!(
            "Converting {} sectors ({:.1} MB) from BIN/CUE to ISO",
            sector_count,
            total_bytes as f64 / 1_048_576.0
        ),
    );

    let mut writer = BufWriter::new(
        File::create(iso_path)
            .with_context(|| format!("Failed to create ISO: {}", iso_path.display()))?,
    );

    let mut bytes_written: u64 = 0;

    for lba in 0..sector_count {
        if is_cancelled(&progress) {
            bail!("Conversion cancelled by user");
        }

        let sector = reader
            .read_sector(lba)
            .with_context(|| format!("Failed to read sector {lba}"))?;

        writer
            .write_all(&sector)
            .with_context(|| "Failed to write ISO data")?;

        bytes_written += sector.len() as u64;
        if lba % 1000 == 0 {
            set_progress(&progress, bytes_written);
        }
    }

    writer.flush()?;
    set_progress(&progress, bytes_written);

    log(
        &progress,
        LogLevel::Info,
        format!(
            "BIN/CUE to ISO conversion complete: {} ({:.1} MB)",
            iso_path.display(),
            bytes_written as f64 / 1_048_576.0
        ),
    );

    if let Ok(mut p) = progress.lock() {
        p.finished = true;
    }

    Ok(())
}

/// Convert any supported optical format (ISO or BIN/CUE) to CD CHD via libchdman-rs.
///
/// BIN/CUE feeds straight into `cd::create_from_cue`; ISO uses
/// `cd::create_from_iso` (which synthesises a tempfile CUE next to the
/// source — no manual `.rusty-backup-temp.cue` to clean up).
pub fn to_chd(
    input_path: &Path,
    output_path: &Path,
    progress: Arc<Mutex<ConvertProgress>>,
) -> Result<()> {
    set_operation(&progress, "Converting to CHD...");

    let format = opticaldiscs::DiscFormat::from_path(input_path)
        .with_context(|| format!("Unrecognized format: {}", input_path.display()))?;

    let opts = libchdman_rs::cd::CdCreateOptions::default();
    log(
        &progress,
        LogLevel::Info,
        format!(
            "Creating CD CHD: {} -> {} (hunk {}, codecs {:?})",
            input_path.display(),
            output_path.display(),
            opts.hunk_size,
            opts.codecs,
        ),
    );

    let progress_for_cb = Arc::clone(&progress);
    let mut on_progress = move |p: libchdman_rs::CompressionProgress| {
        if let Ok(mut s) = progress_for_cb.lock() {
            s.current_bytes = p.bytes_done;
            s.total_bytes = p.bytes_total;
        }
    };
    let progress_for_cancel = Arc::clone(&progress);
    let cancel = move || is_cancelled(&progress_for_cancel);

    let result = match format {
        opticaldiscs::DiscFormat::BinCue => libchdman_rs::cd::create_from_cue(
            input_path,
            output_path,
            opts,
            &mut on_progress,
            &cancel,
        ),
        opticaldiscs::DiscFormat::Iso => libchdman_rs::cd::create_from_iso(
            input_path,
            output_path,
            opts,
            &mut on_progress,
            &cancel,
        ),
        _ => {
            bail!("Cannot convert {} to CHD directly", format.display_name());
        }
    };

    result.map_err(|e| match e {
        libchdman_rs::ChdError::Cancelled => anyhow::anyhow!("conversion cancelled"),
        other => anyhow::anyhow!("CD CHD create failed: {:?}", other),
    })?;

    log(
        &progress,
        LogLevel::Info,
        format!("CHD creation complete: {}", output_path.display()),
    );

    if let Ok(mut p) = progress.lock() {
        p.finished = true;
    }

    Ok(())
}

/// Convert a CHD image to BIN/CUE using chdman extractcd.
pub fn chd_to_bincue(
    chd_path: &Path,
    cue_path: &Path,
    progress: Arc<Mutex<ConvertProgress>>,
) -> Result<()> {
    set_operation(&progress, "Extracting CHD to BIN/CUE...");

    let bin_path = cue_path.with_extension("bin");
    let chdman = get_chdman_command();

    log(
        &progress,
        LogLevel::Info,
        format!(
            "Running: {} extractcd -i {} -o {} -ob {}",
            chdman,
            chd_path.display(),
            cue_path.display(),
            bin_path.display()
        ),
    );

    let output = Command::new(&chdman)
        .arg("extractcd")
        .arg("-i")
        .arg(chd_path)
        .arg("-o")
        .arg(cue_path)
        .arg("-ob")
        .arg(&bin_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("Failed to run {chdman}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    for line in stdout.lines().chain(stderr.lines()) {
        if !line.is_empty() {
            log(&progress, LogLevel::Info, line.to_string());
        }
    }

    if !output.status.success() {
        let msg = format!("chdman extractcd failed (exit {})", output.status);
        log(&progress, LogLevel::Error, &msg);
        bail!("{msg}");
    }

    log(
        &progress,
        LogLevel::Info,
        format!("CHD extraction complete: {}", cue_path.display()),
    );

    if let Ok(mut p) = progress.lock() {
        p.finished = true;
    }

    Ok(())
}

/// Convert a CHD image to ISO (via CHD → BIN/CUE → ISO).
pub fn chd_to_iso(
    chd_path: &Path,
    iso_path: &Path,
    progress: Arc<Mutex<ConvertProgress>>,
) -> Result<()> {
    set_operation(&progress, "Converting CHD to ISO...");

    // Extract to temporary BIN/CUE next to the output
    let parent = iso_path
        .parent()
        .context("output path has no parent directory")?;
    let temp_cue = parent.join(".rusty-backup-temp-chd.cue");
    let temp_bin = parent.join(".rusty-backup-temp-chd.bin");

    // Create a sub-progress for the extraction step
    let extract_progress = Arc::new(Mutex::new(ConvertProgress::new()));
    chd_to_bincue(chd_path, &temp_cue, Arc::clone(&extract_progress))?;

    // Drain log messages from sub-step
    if let Ok(mut ep) = extract_progress.lock() {
        while let Some(msg) = ep.log_messages.pop_front() {
            if let Ok(mut p) = progress.lock() {
                p.log_messages.push_back(msg);
            }
        }
    }

    // Now convert the temp BIN/CUE to ISO
    let convert_progress = Arc::new(Mutex::new(ConvertProgress::new()));
    bincue_to_iso(&temp_cue, iso_path, Arc::clone(&convert_progress))?;

    // Drain log messages from sub-step
    if let Ok(mut cp) = convert_progress.lock() {
        while let Some(msg) = cp.log_messages.pop_front() {
            if let Ok(mut p) = progress.lock() {
                p.log_messages.push_back(msg);
            }
        }
    }

    // Clean up temp files
    let _ = std::fs::remove_file(&temp_cue);
    let _ = std::fs::remove_file(&temp_bin);

    log(
        &progress,
        LogLevel::Info,
        format!("CHD to ISO conversion complete: {}", iso_path.display()),
    );

    if let Ok(mut p) = progress.lock() {
        p.finished = true;
    }

    Ok(())
}

/// Convert a decimal value to BCD (binary-coded decimal).
fn to_bcd(val: u8) -> u8 {
    ((val / 10) << 4) | (val % 10)
}

use opticaldiscs::SectorReader;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_bcd() {
        assert_eq!(to_bcd(0), 0x00);
        assert_eq!(to_bcd(1), 0x01);
        assert_eq!(to_bcd(9), 0x09);
        assert_eq!(to_bcd(10), 0x10);
        assert_eq!(to_bcd(15), 0x15);
        assert_eq!(to_bcd(59), 0x59);
        assert_eq!(to_bcd(99), 0x99);
    }

    #[test]
    fn test_sync_pattern() {
        assert_eq!(SYNC_PATTERN.len(), 12);
        assert_eq!(SYNC_PATTERN[0], 0x00);
        assert_eq!(SYNC_PATTERN[1], 0xFF);
        assert_eq!(SYNC_PATTERN[11], 0x00);
    }

    #[test]
    fn test_convert_progress_new() {
        let p = ConvertProgress::new();
        assert_eq!(p.current_bytes, 0);
        assert_eq!(p.total_bytes, 0);
        assert!(!p.finished);
        assert!(p.error.is_none());
        assert!(!p.cancel_requested);
        assert!(p.log_messages.is_empty());
    }

    #[test]
    fn test_iso_to_chd_native() {
        // Single-track MODE1/2048 ISO → CHD via the new libchdman-rs
        // create_from_iso path. Verify the CHD opens, reports CD format,
        // and that extract_to_iso round-trips byte-equal.
        let dir = tempfile::tempdir().unwrap();

        let iso_path = dir.path().join("source.iso");
        let mut iso_data = vec![0u8; 2048 * 64];
        for (i, b) in iso_data.iter_mut().enumerate() {
            *b = ((i * 41) ^ (i >> 5)) as u8;
        }
        std::fs::write(&iso_path, &iso_data).unwrap();

        let chd_path = dir.path().join("out.chd");
        let progress = Arc::new(Mutex::new(ConvertProgress::new()));
        to_chd(&iso_path, &chd_path, Arc::clone(&progress)).unwrap();
        assert!(chd_path.exists(), "CHD output missing");

        let chd = libchdman_rs::Chd::open(chd_path.to_str().unwrap(), false, None).unwrap();
        let info = chd.info().unwrap();
        assert!(info.is_cd, "expected is_cd flag on CD CHD");

        let restored = dir.path().join("restored.iso");
        libchdman_rs::cd::extract_to_iso(&chd_path, &restored, &mut |_| {}).unwrap();
        let restored_bytes = std::fs::read(&restored).unwrap();
        assert_eq!(restored_bytes, iso_data, "ISO -> CHD -> ISO mismatch");
    }

    #[test]
    fn test_bincue_to_chd_native() {
        // Build a BIN/CUE from a synthetic ISO via iso_to_bincue, then
        // convert that through the native to_chd path and verify the
        // CHD comes back as a CD with the right cooked sector count.
        let dir = tempfile::tempdir().unwrap();

        let iso_path = dir.path().join("seed.iso");
        let mut iso_data = vec![0u8; 2048 * 32];
        for (i, b) in iso_data.iter_mut().enumerate() {
            *b = ((i * 7) ^ (i >> 3)) as u8;
        }
        std::fs::write(&iso_path, &iso_data).unwrap();

        let bin_path = dir.path().join("seed.bin");
        let cue_path = dir.path().join("seed.cue");
        iso_to_bincue(
            &iso_path,
            &bin_path,
            &cue_path,
            Arc::new(Mutex::new(ConvertProgress::new())),
        )
        .unwrap();

        let chd_path = dir.path().join("out.chd");
        to_chd(
            &cue_path,
            &chd_path,
            Arc::new(Mutex::new(ConvertProgress::new())),
        )
        .unwrap();

        let chd = libchdman_rs::Chd::open(chd_path.to_str().unwrap(), false, None).unwrap();
        assert!(chd.info().unwrap().is_cd);

        let restored = dir.path().join("restored.iso");
        libchdman_rs::cd::extract_to_iso(&chd_path, &restored, &mut |_| {}).unwrap();
        let restored_bytes = std::fs::read(&restored).unwrap();
        assert_eq!(restored_bytes, iso_data, "BIN/CUE -> CHD -> ISO mismatch");
    }

    #[test]
    fn test_iso_to_bincue_roundtrip() {
        let dir = tempfile::tempdir().unwrap();

        // Create a minimal ISO file (10 sectors of zeros)
        let iso_path = dir.path().join("test.iso");
        let iso_data = vec![0u8; 2048 * 10];
        std::fs::write(&iso_path, &iso_data).unwrap();

        let bin_path = dir.path().join("test.bin");
        let cue_path = dir.path().join("test.cue");

        let progress = Arc::new(Mutex::new(ConvertProgress::new()));
        iso_to_bincue(&iso_path, &bin_path, &cue_path, Arc::clone(&progress)).unwrap();

        // BIN should be 10 sectors * 2352 bytes
        assert_eq!(bin_path.metadata().unwrap().len(), 10 * 2352);

        // CUE should reference the BIN
        let cue_text = std::fs::read_to_string(&cue_path).unwrap();
        assert!(cue_text.contains("test.bin"));
        assert!(cue_text.contains("MODE1/2352"));
        assert!(cue_text.contains("INDEX 01 00:00:00"));

        // Verify sync pattern in BIN
        let bin_data = std::fs::read(&bin_path).unwrap();
        assert_eq!(&bin_data[..12], &SYNC_PATTERN);

        // Now convert back to ISO
        let iso2_path = dir.path().join("test2.iso");
        let progress2 = Arc::new(Mutex::new(ConvertProgress::new()));
        bincue_to_iso(&cue_path, &iso2_path, Arc::clone(&progress2)).unwrap();

        // Roundtripped ISO should match original
        let iso2_data = std::fs::read(&iso2_path).unwrap();
        assert_eq!(iso_data, iso2_data);
    }
}
